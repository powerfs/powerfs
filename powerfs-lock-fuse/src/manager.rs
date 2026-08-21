//! `FuseLockManager` — the FUSE userspace Rust impl of
//! `powerfs_lock::LockManager`.
//!
//! This is the conservative adapter layer (see
//! `docs/lock-optimization-plan.md` §4.1). It does NOT replace
//! `cache.rs::HoldState` or `VolumeLeaseManager`; it wraps the unified
//! `LockManager` trait around a fresh `ClientLeaseState` (lease token +
//! expiry cache) plus a `FuseLockBackend` (RPC facade abstraction).
//!
//! # Routing (matches `powerfs_lock::LockRequest` semantics)
//!
//! - `req.is_inode_level()` → `FuseLockBackend::acquire_inode_lease`
//!   (Filer-managed, 方案 A)
//! - `req.is_range_level()` → `FuseLockBackend::acquire_range_lease`
//!   (Volume-managed, 方案 D). Requires a `lookup_volume_id` first if
//!   the caller didn't supply one.
//!
//! # Cache hit / miss
//!
//! Both inode and range paths consult `ClientLeaseState` first; on hit
//! they return a `LockGrant` echoing the cached token with no RPC.
//! Misses call the backend, then populate the cache.
//!
//! # Lazy sweep (phase 4 P1 — see `docs/lock-optimization-plan.md` §五)
//!
//! `acquire` used to call `sweep_expired` unconditionally on every
//! invocation, contributing ~200ns to the cache-hit hot path (per the
//! `powerfs-bench` baseline). The lazy-sweep optimization skips the
//! sweep on cache **hits** when the last sweep was within
//! `sweep_threshold` (default 100ms). Cache **misses** still sweep
//! unconditionally — the upcoming RPC dominates latency, so the sweep
//! overhead is negligible and the cache stays clean before inserting
//! the new entry. `ClientLeaseState::get_inode`/`get_range` already
//! filter expired entries per-lookup, so skipping the global sweep
//! never serves a stale lease.
//!
//! # Metrics
//!
//! Every code path bumps `LockMetrics` counters so Prometheus (exposed
//! in 阶段一C step 5) has a single source of truth for hit rate,
//! conflict rate, and error rate.

use crate::backend::FuseLockBackend;
use crate::lockify::Lockify;
use crate::metrics::LockMetrics;
use crate::state::{ClientLeaseState, InodeLeaseEntry, RangeLeaseEntry};
use async_trait::async_trait;
use powerfs_lock::{LockError, LockEventHandler, LockGrant, LockManager, LockMode, LockRequest};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

/// Default lazy-sweep threshold (phase 4 P1). A cache-hit acquire skips
/// `sweep_expired` when the last sweep was within this window. Picked
/// to be small enough that expired entries don't linger long, but
/// large enough to make back-to-back cache hits effectively free.
pub const DEFAULT_SWEEP_THRESHOLD: Duration = Duration::from_millis(100);

/// FUSE userspace Rust impl of `LockManager`.
///
/// Owns:
/// - `state: Arc<ClientLeaseState>` — inode + range lease cache.
/// - `backend: Arc<dyn FuseLockBackend>` — RPC facade.
/// - `metrics: Arc<LockMetrics>` — counters.
/// - `handler: Mutex<Option<Weak<dyn LockEventHandler>>>` — server-push
///   callback (held as `Weak` to avoid reference cycles; see
///   `LockManager::register_handler` docs).
/// - `last_sweep: Mutex<Instant>` — timestamp of the last sweep, used
///   by the lazy-sweep fast path (phase 4 P1).
/// - `sweep_threshold: Duration` — cache hits within this window since
///   `last_sweep` skip `sweep_expired`.
/// - `lockify: Option<Arc<Lockify>>` — phase 4 §5.1 fast path for
///   metadata creation (creat/mkdir/mknod). `None` disables Lockify;
///   callers fall back to the regular `acquire` RPC path. Set via
///   [`Self::with_lockify`].
pub struct FuseLockManager {
    state: Arc<ClientLeaseState>,
    backend: Arc<dyn FuseLockBackend>,
    metrics: Arc<LockMetrics>,
    client_id: Arc<String>,
    /// Default lease TTL when `LockRequest::timeout` is zero.
    default_lease_ms: u64,
    handler: Mutex<Option<Weak<dyn LockEventHandler>>>,
    /// Timestamp of the last `sweep_expired` call. Initialized to
    /// `Instant::now()` so the very first acquire (a cache miss on an
    /// empty cache) skips the sweep — there's nothing to sweep.
    last_sweep: Mutex<Instant>,
    /// Cache-hit acquires within this window since `last_sweep` skip
    /// `sweep_expired`. See `DEFAULT_SWEEP_THRESHOLD`.
    sweep_threshold: Duration,
    /// Phase-4 §5.1 Lockify fast path. `None` by default — callers
    /// must opt in via [`Self::with_lockify`].
    lockify: Option<Arc<Lockify>>,
}

impl FuseLockManager {
    /// Construct a new `FuseLockManager`.
    ///
    /// `default_lease_ms` is used when a `LockRequest` carries
    /// `timeout == Duration::ZERO`. Pass a sensible default (the
    /// existing FUSE client uses 30s). The lazy-sweep threshold
    /// defaults to [`DEFAULT_SWEEP_THRESHOLD`]; override with
    /// [`Self::with_sweep_threshold`].
    pub fn new(
        backend: Arc<dyn FuseLockBackend>,
        client_id: String,
        default_lease_ms: u64,
    ) -> Self {
        Self {
            state: ClientLeaseState::new_shared(),
            backend,
            metrics: LockMetrics::new_shared(),
            client_id: Arc::new(client_id),
            default_lease_ms,
            handler: Mutex::new(None),
            last_sweep: Mutex::new(Instant::now()),
            sweep_threshold: DEFAULT_SWEEP_THRESHOLD,
            lockify: None,
        }
    }

    /// Construct with all components injected (for tests / advanced
    /// callers that want to share state or metrics across managers).
    pub fn with_state_and_metrics(
        state: Arc<ClientLeaseState>,
        backend: Arc<dyn FuseLockBackend>,
        metrics: Arc<LockMetrics>,
        client_id: String,
        default_lease_ms: u64,
    ) -> Self {
        Self {
            state,
            backend,
            metrics,
            client_id: Arc::new(client_id),
            default_lease_ms,
            handler: Mutex::new(None),
            last_sweep: Mutex::new(Instant::now()),
            sweep_threshold: DEFAULT_SWEEP_THRESHOLD,
            lockify: None,
        }
    }

    /// Override the lazy-sweep threshold (phase 4 P1). Pass
    /// `Duration::ZERO` to disable lazy sweep and force a sweep on
    /// every acquire (useful for tests / benchmarking the legacy
    /// behavior).
    #[must_use]
    pub fn with_sweep_threshold(mut self, threshold: Duration) -> Self {
        self.sweep_threshold = threshold;
        self
    }

    /// Enable the phase-4 §5.1 Lockify fast path. After this call,
    /// [`Self::acquire_local`] is available and will return a local
    /// token without an RPC, spawning an async ownership sync in the
    /// background.
    ///
    /// The `Lockify` helper is constructed from the manager's own
    /// `state`/`backend`/`metrics`/`client_id`, so self-declared
    /// leases share the same cache and metrics as regular acquires.
    /// Pass an optional `tokio::runtime::Handle` to spawn the async
    /// sync task on a runtime even when the calling thread has none
    /// (e.g. when the FUSE driver calls `acquire_local` from a
    /// synchronous context).
    #[must_use]
    pub fn with_lockify(mut self, runtime: Option<tokio::runtime::Handle>) -> Self {
        let mut lockify = Lockify::new(
            Arc::clone(&self.backend),
            Arc::clone(&self.client_id),
            Arc::clone(&self.metrics),
            Arc::clone(&self.state),
        );
        if let Some(handle) = runtime {
            lockify = lockify.with_runtime(handle);
        }
        self.lockify = Some(Arc::new(lockify));
        self
    }

    // ---------- accessors ----------

    pub fn state(&self) -> &Arc<ClientLeaseState> {
        &self.state
    }

    pub fn metrics(&self) -> &Arc<LockMetrics> {
        &self.metrics
    }

    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Borrow the optional [`Lockify`] helper, if enabled via
    /// [`Self::with_lockify`]. `None` means the manager was not
    /// configured for the §5.1 metadata fast path; callers should
    /// fall back to [`LockManager::acquire`].
    pub fn lockify(&self) -> Option<&Arc<Lockify>> {
        self.lockify.as_ref()
    }

    /// Phase-4 §5.1 Lockify fast path: self-declare ownership of a
    /// fresh inode without an RPC.
    ///
    /// Convenience wrapper around [`Lockify::self_declare`] — the
    /// caller doesn't need to hold the `Lockify` Arc directly. The
    /// manager must have been built with [`Self::with_lockify`] first.
    ///
    /// # Errors
    ///
    /// - [`LockError::Internal`] if `duration_ms == 0` or if Lockify
    ///   was never enabled (caller bug — call [`Self::with_lockify`]
    ///   during construction).
    ///
    /// Backend errors are surfaced asynchronously via metrics and
    /// never synchronously on this call.
    pub fn acquire_local(
        &self,
        inode: u64,
        mode: LockMode,
        duration_ms: u64,
    ) -> Result<LockGrant, LockError> {
        let Some(lockify) = &self.lockify else {
            return Err(LockError::Internal(
                "acquire_local called without with_lockify".to_string(),
            ));
        };
        lockify.self_declare(inode, mode, duration_ms)
    }

    /// Resolve the effective lease duration for a request.
    fn effective_duration_ms(&self, req: &LockRequest) -> u64 {
        if req.timeout == Duration::ZERO {
            self.default_lease_ms
        } else {
            req.timeout.as_millis().max(1) as u64
        }
    }

    /// Run a sweep of expired cache entries. Returns the number
    /// removed (recorded in `sweep_removed_total`). Also stamps
    /// `last_sweep` so subsequent cache-hit acquires within
    /// `sweep_threshold` can skip the sweep (phase 4 P1).
    ///
    /// Cheap if nothing's expired — `ClientLeaseState::sweep_expired`
    /// just walks two `HashMap`s and retains non-expired entries.
    /// Called from `acquire` on cache misses (where the upcoming RPC
    /// hides the sweep cost) and as a public escape hatch for
    /// background / explicit cleanup.
    pub fn sweep_expired(&self) -> usize {
        let removed = self.state.sweep_expired();
        *self.last_sweep.lock().unwrap() = Instant::now();
        self.metrics.record_sweep_removed(removed);
        removed
    }

    /// Lazy-sweep fast path for cache hits (phase 4 P1). If the last
    /// sweep was within `sweep_threshold`, skip the sweep and bump
    /// `sweep_skipped_total`. Otherwise run the sweep. Callers must
    /// have already verified the cache hit (this method doesn't peek
    /// the cache — it only throttles the sweep).
    ///
    /// Rationale: on a cache hit the entire acquire is sub-microsecond
    /// and the ~200ns sweep is a meaningful fraction of the hot path.
    /// On a cache miss the upcoming RPC dominates, so the sweep runs
    /// unconditionally (see `acquire_inode` / `acquire_range`).
    fn maybe_sweep_lazy(&self) {
        let last = *self.last_sweep.lock().unwrap();
        if last.elapsed() < self.sweep_threshold {
            self.metrics.record_sweep_skipped();
            return;
        }
        let removed = self.state.sweep_expired();
        *self.last_sweep.lock().unwrap() = Instant::now();
        self.metrics.record_sweep_removed(removed);
    }

    /// Drop all cached inode/range leases for an inode, returning the
    /// range leases that need server-side release.
    ///
    /// Used by the FUSE client's `release()` path (close-time cleanup)
    /// — mirrors the existing `VolumeLeaseManager::release_all_for_inode`
    /// API so the caller doesn't have to change shape during the
    /// conservative-adapter migration.
    ///
    /// Returns `Vec<(stripe_start, token, volume_id)>` for range leases
    /// that were cached. The caller should release each via
    /// `FuseLockBackend::release_range_lease` (or just call
    /// `FuseLockManager::release` per token).
    pub fn release_all_ranges_for_inode(&self, inode: u64) -> Vec<(u64, String, u64)> {
        self.state.drain_ranges_for_inode(inode)
    }

    /// Map a backend string error to a typed `LockError`.
    ///
    /// Heuristics:
    /// - contains "conflict" (case-insensitive) → `Conflict`
    /// - contains "quarantin" → `Quarantined`
    /// - contains "network"/"timeout"/"unreachable"/"transport" → `Network`
    /// - contains "not found" → `NotFound`
    /// - otherwise → `Internal`
    fn map_backend_error(s: String) -> LockError {
        let lower = s.to_lowercase();
        if lower.contains("conflict") {
            LockError::Conflict(s)
        } else if lower.contains("quarantin") {
            LockError::Quarantined(s)
        } else if lower.contains("network")
            || lower.contains("timeout")
            || lower.contains("unreachable")
            || lower.contains("transport")
            || lower.contains("connection")
        {
            LockError::Network(s)
        } else if lower.contains("not found") {
            LockError::NotFound
        } else {
            LockError::Internal(s)
        }
    }

    /// Determine whether a backend error indicates a conflict, for
    /// metric classification.
    fn is_conflict_err(s: &str) -> bool {
        s.to_lowercase().contains("conflict")
    }

    // ---------- inode-level ----------

    async fn acquire_inode(
        &self,
        inode: u64,
        mode: LockMode,
        duration_ms: u64,
    ) -> Result<LockGrant, LockError> {
        // 1. Cache hit?
        if let Some(cached) = self.state.get_inode(inode) {
            if cached.mode == mode {
                self.metrics.record_cache_hit();
                // Lazy sweep (phase 4 P1): skip when the last sweep was
                // recent. `get_inode` already filtered expired entries,
                // so we never serve a stale lease.
                self.maybe_sweep_lazy();
                log::debug!(
                    "lock: inode cache hit inode={} mode={}",
                    inode,
                    mode.as_str()
                );
                return Ok(LockGrant {
                    inode,
                    token: cached.token,
                    sn: 0,
                    lease_ms: cached
                        .expire_at
                        .saturating_duration_since(Instant::now())
                        .as_millis() as u64,
                    mode,
                    range: None,
                });
            }
            // Mode mismatch — drop cached entry, fall through to RPC.
            self.state.invalidate_inode(inode);
        }

        // 2. Cache miss → sweep (clean churn before adding new entry),
        //    then backend RPC. The RPC dominates latency so the sweep
        //    overhead is negligible.
        self.sweep_expired();
        self.metrics.record_cache_miss();
        let (token, expire_at_ms) = self
            .backend
            .acquire_inode_lease(inode, &self.client_id, duration_ms)
            .await
            .map_err(|e| {
                if Self::is_conflict_err(&e) {
                    self.metrics.record_conflict();
                } else {
                    self.metrics.record_error();
                }
                Self::map_backend_error(e)
            })?;

        // 3. Cache and return.
        let expire_at = Instant::now() + Duration::from_millis(expire_at_ms.max(duration_ms));
        self.state.put_inode(
            inode,
            InodeLeaseEntry {
                token: token.clone(),
                expire_at,
                mode: mode.clone(),
            },
        );

        Ok(LockGrant {
            inode,
            token,
            sn: 0,
            lease_ms: expire_at_ms.max(duration_ms),
            mode,
            range: None,
        })
    }

    // ---------- range-level ----------

    async fn acquire_range(
        &self,
        inode: u64,
        mode: LockMode,
        range: powerfs_lock::Range,
        duration_ms: u64,
    ) -> Result<LockGrant, LockError> {
        // The cache is keyed by (inode, stripe_start, stripe_count, exclusive).
        // We use `range.start` as the stripe anchor and a placeholder count
        // of 1 stripe (the existing FUSE client does 1-stripe-per-lease today;
        // see `fuse.rs` `lease_manager.acquire(... stripe_count ...)`). A
        // multi-stripe request splits into N single-stripe leases upstream.
        let stripe_start = range.start;
        let stripe_count = range
            .end
            .map(|end| end.saturating_sub(stripe_start))
            .unwrap_or(1);
        let exclusive = mode.is_exclusive();

        // 1. Cache hit?
        if let Some(cached) = self
            .state
            .get_range(inode, stripe_start, stripe_count, exclusive)
        {
            self.metrics.record_cache_hit();
            // Lazy sweep (phase 4 P1): skip when the last sweep was
            // recent. `get_range` already filtered expired entries.
            self.maybe_sweep_lazy();
            log::debug!(
                "lock: range cache hit inode={} stripe=[{},{}) exclusive={}",
                inode,
                stripe_start,
                stripe_start + stripe_count,
                exclusive
            );
            return Ok(LockGrant {
                inode,
                token: cached.token,
                sn: 0,
                lease_ms: cached
                    .expire_at
                    .saturating_duration_since(Instant::now())
                    .as_millis() as u64,
                mode,
                range: Some(range),
            });
        }

        // 2. Cache miss → sweep, resolve volume_id, then backend RPC.
        self.sweep_expired();
        self.metrics.record_cache_miss();
        let volume_id = self.backend.lookup_volume_id(inode).await.map_err(|e| {
            self.metrics.record_error();
            Self::map_backend_error(e)
        })?;

        let token = self
            .backend
            .acquire_range_lease(
                volume_id,
                inode,
                stripe_start,
                stripe_count,
                &self.client_id,
                exclusive,
                duration_ms,
            )
            .await
            .map_err(|e| {
                if Self::is_conflict_err(&e) {
                    self.metrics.record_conflict();
                } else {
                    self.metrics.record_error();
                }
                Self::map_backend_error(e)
            })?;

        // 3. Cache and return.
        let expire_at = Instant::now() + Duration::from_millis(duration_ms);
        self.state.put_range_for_inode(
            inode,
            stripe_start,
            stripe_count,
            RangeLeaseEntry {
                token: token.clone(),
                expire_at,
                mode: mode.clone(),
                stripe_start,
                stripe_count,
                volume_id,
            },
        );

        Ok(LockGrant {
            inode,
            token,
            sn: 0,
            lease_ms: duration_ms,
            mode,
            range: Some(range),
        })
    }
}

#[async_trait]
impl LockManager for FuseLockManager {
    async fn acquire(&self, req: LockRequest) -> Result<LockGrant, LockError> {
        self.metrics.record_acquire();
        // Lazy sweep (phase 4 P1): the sweep now runs inside
        // `acquire_inode` / `acquire_range`, conditional on cache
        // hit/miss. See `maybe_sweep_lazy` for rationale.

        let duration_ms = self.effective_duration_ms(&req);

        if req.is_inode_level() {
            self.acquire_inode(req.inode, req.mode, duration_ms).await
        } else if let Some(range) = req.effective_range() {
            self.acquire_range(req.inode, req.mode, range, duration_ms)
                .await
        } else {
            // Should be unreachable given `is_range_level` semantics, but
            // fail safe rather than panicking on a malformed request.
            self.metrics.record_error();
            Err(LockError::Internal(
                "range-level request missing effective range".to_string(),
            ))
        }
    }

    async fn release(&self, inode: u64, token: &str) -> Result<(), LockError> {
        self.metrics.record_release();

        // 1. Try inode-level: if the cache has an entry for this inode
        //    with a matching token, release it server-side.
        if self.state.remove_inode_by_token(inode, token) {
            if let Err(e) = self
                .backend
                .release_inode_lease(inode, &self.client_id, token)
                .await
            {
                self.metrics.record_error();
                return Err(Self::map_backend_error(e));
            }
            return Ok(());
        }

        // 2. Try range-level: the token may be a range lease. Look it
        //    up by token (O(N) scan — range cache is small).
        if let Some((volume_id, stripe_start, stripe_count, _exclusive)) =
            self.state.remove_range_by_token(token)
        {
            let _ = (inode, stripe_count); // unused, kept for clarity
            if let Err(e) = self
                .backend
                .release_range_lease(volume_id, inode, stripe_start, &self.client_id, token)
                .await
            {
                self.metrics.record_error();
                return Err(Self::map_backend_error(e));
            }
            return Ok(());
        }

        // 3. Idempotent release: token not cached. The server may have
        //    already released it (e.g. TTL expired), or the caller is
        //    releasing a token from a previous process lifetime. Best-
        //    effort: try the inode-level RPC anyway (cheap) and ignore
        //    "not found" errors. This matches `LockManager::release`'s
        //    contract: "releasing an already-released or expired lease
        //    returns Ok(())".
        match self
            .backend
            .release_inode_lease(inode, &self.client_id, token)
            .await
        {
            Ok(()) => Ok(()),
            Err(e) => {
                let lower = e.to_lowercase();
                if lower.contains("not found")
                    || lower.contains("no such")
                    || lower.contains("unknown token")
                {
                    // Treat as already-released — idempotent success.
                    Ok(())
                } else {
                    self.metrics.record_error();
                    Err(Self::map_backend_error(e))
                }
            }
        }
    }

    async fn renew(&self, inode: u64, token: &str, timeout: Duration) -> Result<(), LockError> {
        self.metrics.record_renew();
        let duration_ms = if timeout == Duration::ZERO {
            self.default_lease_ms
        } else {
            timeout.as_millis().max(1) as u64
        };

        // Only inode-level leases are renewable through this trait
        // (range-level renew is rare; the existing FUSE client re-
        // acquires per-stripe on each read). If the token isn't in the
        // inode cache, return `NotFound` so the caller can re-acquire.
        let cached_mode = self.state.get_inode(inode).map(|e| e.mode);
        let Some(mode) = cached_mode else {
            self.metrics.record_error();
            return Err(LockError::NotFound);
        };

        self.backend
            .renew_inode_lease(inode, &self.client_id, token, duration_ms)
            .await
            .map_err(|e| {
                self.metrics.record_error();
                Self::map_backend_error(e)
            })?;

        // Refresh the cache entry's expiry.
        self.state.put_inode(
            inode,
            InodeLeaseEntry {
                token: token.to_string(),
                expire_at: Instant::now() + Duration::from_millis(duration_ms),
                mode,
            },
        );
        Ok(())
    }

    fn register_handler(&self, handler: Arc<dyn LockEventHandler>) {
        // Downgrade to Weak to avoid reference cycles (per trait docs).
        let weak: Weak<dyn LockEventHandler> = Arc::downgrade(&handler);
        let mut guard = self.handler.lock().unwrap();
        *guard = Some(weak);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use powerfs_lock::{LockEventHandler, LockMode, LockRequest, Range};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex as StdMutex;
    use std::time::Duration;

    // ---------- mock backend ----------

    /// Mock backend that records every call and returns canned responses.
    struct MockBackend {
        acquire_inode_calls: AtomicU64,
        release_inode_calls: AtomicU64,
        renew_inode_calls: AtomicU64,
        acquire_range_calls: AtomicU64,
        release_range_calls: AtomicU64,
        lookup_volume_calls: AtomicU64,
        /// Override responses by setting these. Default = success.
        acquire_inode_resp: StdMutex<Result<(String, u64), String>>,
        acquire_range_resp: StdMutex<Result<String, String>>,
        lookup_volume_resp: StdMutex<Result<u64, String>>,
        release_inode_resp: StdMutex<Result<(), String>>,
        release_range_resp: StdMutex<Result<(), String>>,
        renew_inode_resp: StdMutex<Result<(), String>>,
    }

    impl MockBackend {
        fn new() -> Self {
            Self {
                acquire_inode_calls: AtomicU64::new(0),
                release_inode_calls: AtomicU64::new(0),
                renew_inode_calls: AtomicU64::new(0),
                acquire_range_calls: AtomicU64::new(0),
                release_range_calls: AtomicU64::new(0),
                lookup_volume_calls: AtomicU64::new(0),
                acquire_inode_resp: StdMutex::new(Ok(("tok-inode".to_string(), 30_000))),
                acquire_range_resp: StdMutex::new(Ok("tok-range".to_string())),
                lookup_volume_resp: StdMutex::new(Ok(42)),
                release_inode_resp: StdMutex::new(Ok(())),
                release_range_resp: StdMutex::new(Ok(())),
                renew_inode_resp: StdMutex::new(Ok(())),
            }
        }
    }

    #[async_trait]
    impl FuseLockBackend for MockBackend {
        async fn acquire_inode_lease(
            &self,
            _inode: u64,
            _client_id: &str,
            _duration_ms: u64,
        ) -> Result<(String, u64), String> {
            self.acquire_inode_calls.fetch_add(1, Ordering::SeqCst);
            self.acquire_inode_resp.lock().unwrap().clone()
        }
        async fn release_inode_lease(
            &self,
            _inode: u64,
            _client_id: &str,
            _token: &str,
        ) -> Result<(), String> {
            self.release_inode_calls.fetch_add(1, Ordering::SeqCst);
            self.release_inode_resp.lock().unwrap().clone()
        }
        async fn renew_inode_lease(
            &self,
            _inode: u64,
            _client_id: &str,
            _token: &str,
            _duration_ms: u64,
        ) -> Result<(), String> {
            self.renew_inode_calls.fetch_add(1, Ordering::SeqCst);
            self.renew_inode_resp.lock().unwrap().clone()
        }
        async fn acquire_range_lease(
            &self,
            _volume_id: u64,
            _inode: u64,
            _stripe_start: u64,
            _stripe_count: u64,
            _client_id: &str,
            _exclusive: bool,
            _duration_ms: u64,
        ) -> Result<String, String> {
            self.acquire_range_calls.fetch_add(1, Ordering::SeqCst);
            self.acquire_range_resp.lock().unwrap().clone()
        }
        async fn release_range_lease(
            &self,
            _volume_id: u64,
            _inode: u64,
            _stripe_start: u64,
            _client_id: &str,
            _token: &str,
        ) -> Result<(), String> {
            self.release_range_calls.fetch_add(1, Ordering::SeqCst);
            self.release_range_resp.lock().unwrap().clone()
        }
        async fn lookup_volume_id(&self, _inode: u64) -> Result<u64, String> {
            self.lookup_volume_calls.fetch_add(1, Ordering::SeqCst);
            self.lookup_volume_resp.lock().unwrap().clone()
        }
    }

    fn make_manager() -> (FuseLockManager, Arc<MockBackend>) {
        let backend = Arc::new(MockBackend::new());
        let manager = FuseLockManager::new(
            Arc::clone(&backend) as Arc<dyn FuseLockBackend>,
            "client-A".to_string(),
            30_000,
        );
        (manager, backend)
    }

    // ---------- inode-level acquire/release ----------

    #[tokio::test]
    async fn test_inode_acquire_then_cache_hit_avoids_rpc() {
        let (mgr, backend) = make_manager();
        let inode = 100u64;

        // First acquire → cache miss → RPC.
        let req = LockRequest::new(inode, LockMode::Shared, Duration::from_secs(30));
        let grant = mgr.acquire(req).await.expect("acquire must succeed");
        assert_eq!(grant.token, "tok-inode");
        assert_eq!(backend.acquire_inode_calls.load(Ordering::SeqCst), 1);

        // Second acquire → cache hit → no RPC.
        let req2 = LockRequest::new(inode, LockMode::Shared, Duration::from_secs(30));
        let grant2 = mgr.acquire(req2).await.expect("cache hit must succeed");
        assert_eq!(grant2.token, "tok-inode");
        assert_eq!(
            backend.acquire_inode_calls.load(Ordering::SeqCst),
            1,
            "second acquire must be a cache hit"
        );

        // Metrics: 2 acquires, 1 miss, 1 hit.
        let snap = mgr.metrics().snapshot();
        assert_eq!(snap.acquire_total, 2);
        assert_eq!(snap.acquire_cache_miss, 1);
        assert_eq!(snap.acquire_cache_hit, 1);
    }

    #[tokio::test]
    async fn test_inode_release_clears_cache_and_calls_backend() {
        let (mgr, backend) = make_manager();
        let inode = 5u64;

        let req = LockRequest::new(inode, LockMode::Exclusive, Duration::from_secs(30));
        let grant = mgr.acquire(req).await.expect("acquire must succeed");

        mgr.release(inode, &grant.token)
            .await
            .expect("release must succeed");
        assert_eq!(backend.release_inode_calls.load(Ordering::SeqCst), 1);

        // After release, the next acquire must miss the cache and hit the backend.
        let req2 = LockRequest::new(inode, LockMode::Exclusive, Duration::from_secs(30));
        mgr.acquire(req2).await.expect("acquire must succeed");
        assert_eq!(
            backend.acquire_inode_calls.load(Ordering::SeqCst),
            2,
            "after release the next acquire must miss cache"
        );
    }

    #[tokio::test]
    async fn test_inode_release_idempotent_on_unknown_token() {
        let (mgr, backend) = make_manager();
        // Releasing a token we never acquired should still return Ok
        // (idempotent contract).
        let result = mgr.release(999, "never-seen").await;
        assert!(
            result.is_ok(),
            "release of unknown token must be idempotent"
        );
        // The manager still tries the inode-level RPC once.
        assert_eq!(backend.release_inode_calls.load(Ordering::SeqCst), 1);
    }

    // ---------- range-level acquire/release ----------

    #[tokio::test]
    async fn test_range_acquire_calls_lookup_then_acquire_range() {
        let (mgr, backend) = make_manager();
        let inode = 7u64;
        let range = Range::new(0, Some(4096));
        let req = LockRequest::new(inode, LockMode::Range(range), Duration::from_secs(30));

        let grant = mgr.acquire(req).await.expect("acquire must succeed");
        assert_eq!(grant.token, "tok-range");
        assert_eq!(backend.lookup_volume_calls.load(Ordering::SeqCst), 1);
        assert_eq!(backend.acquire_range_calls.load(Ordering::SeqCst), 1);

        // Second acquire of the same range → cache hit → no further RPCs.
        let req2 = LockRequest::new(inode, LockMode::Range(range), Duration::from_secs(30));
        mgr.acquire(req2).await.expect("cache hit must succeed");
        assert_eq!(backend.lookup_volume_calls.load(Ordering::SeqCst), 1);
        assert_eq!(backend.acquire_range_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_range_release_uses_cached_volume_id() {
        let (mgr, backend) = make_manager();
        let inode = 11u64;
        let range = Range::new(0, Some(4096));
        let req = LockRequest::new(inode, LockMode::Range(range), Duration::from_secs(30));
        let grant = mgr.acquire(req).await.expect("acquire must succeed");

        // Release via the unified trait (token only). The manager must
        // recover (volume_id, stripe_start) from the range cache.
        mgr.release(inode, &grant.token)
            .await
            .expect("release must succeed");
        assert_eq!(backend.release_range_calls.load(Ordering::SeqCst), 1);
        // No inode-level release should fire (the token was in range cache).
        assert_eq!(backend.release_inode_calls.load(Ordering::SeqCst), 0);
    }

    // ---------- error mapping ----------

    #[tokio::test]
    async fn test_acquire_conflict_maps_to_lock_error_conflict() {
        let (mgr, backend) = make_manager();
        // Override the inode acquire to return a conflict.
        *backend.acquire_inode_resp.lock().unwrap() =
            Err("conflict: held by other client".to_string());

        let req = LockRequest::new(1, LockMode::Exclusive, Duration::from_secs(30));
        let err = mgr.acquire(req).await.expect_err("must fail");
        assert!(matches!(err, LockError::Conflict(_)));

        // Conflict must bump both `acquire_conflict` and `errors_total`.
        let snap = mgr.metrics().snapshot();
        assert_eq!(snap.acquire_conflict, 1);
        assert_eq!(snap.errors_total, 1);
    }

    #[tokio::test]
    async fn test_acquire_network_error_maps_to_lock_error_network() {
        let (mgr, backend) = make_manager();
        *backend.acquire_inode_resp.lock().unwrap() =
            Err("network timeout contacting filer".to_string());

        let req = LockRequest::new(2, LockMode::Shared, Duration::from_secs(30));
        let err = mgr.acquire(req).await.expect_err("must fail");
        assert!(matches!(err, LockError::Network(_)));
    }

    // ---------- renew ----------

    #[tokio::test]
    async fn test_renew_extends_cache_expiry() {
        let (mgr, backend) = make_manager();
        let inode = 8u64;
        let req = LockRequest::new(inode, LockMode::Shared, Duration::from_secs(30));
        let grant = mgr.acquire(req).await.expect("acquire must succeed");

        // Renew.
        mgr.renew(inode, &grant.token, Duration::from_secs(60))
            .await
            .expect("renew must succeed");
        assert_eq!(backend.renew_inode_calls.load(Ordering::SeqCst), 1);

        // The cached entry must still be a hit (renewed).
        let req2 = LockRequest::new(inode, LockMode::Shared, Duration::from_secs(30));
        mgr.acquire(req2)
            .await
            .expect("cache hit must succeed after renew");
        assert_eq!(backend.acquire_inode_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_renew_unknown_token_returns_not_found() {
        let (mgr, _backend) = make_manager();
        let err = mgr
            .renew(999, "never-seen", Duration::from_secs(30))
            .await
            .expect_err("renew of unknown token must fail");
        assert!(matches!(err, LockError::NotFound));
    }

    // ---------- sweep ----------

    #[tokio::test]
    async fn test_sweep_expired_is_called_on_acquire() {
        let (mgr, backend) = make_manager();
        let inode = 33u64;

        // Override the mock to return a 1ms expiry so the cached entry
        // expires well before the second acquire (the default mock
        // returns 30s, which would keep the cache valid).
        *backend.acquire_inode_resp.lock().unwrap() = Ok(("tok-short".to_string(), 1));

        // Acquire once to populate cache.
        let req = LockRequest::new(inode, LockMode::Shared, Duration::from_millis(1));
        mgr.acquire(req).await.expect("acquire must succeed");
        assert_eq!(backend.acquire_inode_calls.load(Ordering::SeqCst), 1);

        // Wait for the lease to expire.
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Acquire again — the cached entry has expired, so the cache
        // lookup misses and we issue a fresh RPC. The lazy sweep should
        // also evict the expired entry.
        let req2 = LockRequest::new(inode, LockMode::Shared, Duration::from_secs(30));
        mgr.acquire(req2).await.expect("acquire must succeed");
        assert_eq!(
            backend.acquire_inode_calls.load(Ordering::SeqCst),
            2,
            "expired cached entry must miss and trigger fresh RPC"
        );

        // sweep_removed_total must have incremented.
        let snap = mgr.metrics().snapshot();
        assert!(
            snap.sweep_removed_total >= 1,
            "sweep must remove expired entry"
        );
    }

    // ---------- lazy sweep (phase 4 P1) ----------

    /// Helper: inject a stale (already-expired) inode lease directly
    /// into the cache state so we can observe whether `sweep_expired`
    /// actually ran. `get_inode` filters expired entries per-lookup,
    /// so a stale entry only disappears when the sweep runs.
    fn inject_stale_inode(mgr: &FuseLockManager, inode: u64) {
        mgr.state().put_inode(
            inode,
            InodeLeaseEntry {
                token: format!("stale-{inode}"),
                expire_at: Instant::now() - Duration::from_secs(60),
                mode: LockMode::Shared,
            },
        );
    }

    #[tokio::test]
    async fn test_lazy_sweep_skips_on_cache_hit_within_threshold() {
        // Default threshold is 100ms — a cache hit immediately after a
        // sweep must skip and bump `sweep_skipped_total`.
        let (mgr, backend) = make_manager();
        let inode = 42u64;

        // Prime the cache (cache miss → sweep runs, sets last_sweep).
        let req = LockRequest::new(inode, LockMode::Shared, Duration::from_secs(30));
        mgr.acquire(req).await.expect("prime acquire");
        assert_eq!(backend.acquire_inode_calls.load(Ordering::SeqCst), 1);

        // Inject a stale entry for a *different* inode so we can
        // detect whether the sweep ran on the next acquire.
        inject_stale_inode(&mgr, 999);

        // Cache hit on inode=42 — within the 100ms threshold, so the
        // sweep must be skipped and the stale inode=999 entry must
        // remain in the cache.
        let req2 = LockRequest::new(inode, LockMode::Shared, Duration::from_secs(30));
        mgr.acquire(req2).await.expect("cache hit must succeed");
        assert_eq!(
            backend.acquire_inode_calls.load(Ordering::SeqCst),
            1,
            "second acquire must be a cache hit"
        );

        let snap = mgr.metrics().snapshot();
        assert_eq!(
            snap.sweep_skipped_total, 1,
            "cache hit within threshold must skip sweep"
        );
        assert_eq!(
            snap.sweep_removed_total, 0,
            "sweep must not have run, so stale entry must still be cached"
        );
        // The stale entry is still in the cache (sweep didn't run).
        assert_eq!(
            mgr.state().inode_cache_size(),
            2,
            "stale entry must still be cached (sweep skipped)"
        );
    }

    #[tokio::test]
    async fn test_lazy_sweep_runs_on_cache_hit_when_threshold_exceeded() {
        // With `sweep_threshold = Duration::ZERO`, the lazy check
        // `elapsed < ZERO` is always false → sweep runs on every
        // cache hit. This validates the "force sweep" escape hatch
        // and proves the cache-hit path can still sweep when needed.
        let (mgr, backend) = make_manager();
        let mgr = mgr.with_sweep_threshold(Duration::ZERO);
        let inode = 42u64;

        // Prime the cache.
        let req = LockRequest::new(inode, LockMode::Shared, Duration::from_secs(30));
        mgr.acquire(req).await.expect("prime acquire");
        assert_eq!(backend.acquire_inode_calls.load(Ordering::SeqCst), 1);

        // Inject a stale entry for a different inode.
        inject_stale_inode(&mgr, 999);

        // Cache hit on inode=42 — threshold=0 forces the sweep to
        // run, which must evict the stale inode=999 entry.
        let req2 = LockRequest::new(inode, LockMode::Shared, Duration::from_secs(30));
        mgr.acquire(req2).await.expect("cache hit must succeed");
        assert_eq!(
            backend.acquire_inode_calls.load(Ordering::SeqCst),
            1,
            "second acquire must be a cache hit"
        );

        let snap = mgr.metrics().snapshot();
        assert_eq!(
            snap.sweep_skipped_total, 0,
            "zero threshold must never skip sweep"
        );
        assert!(
            snap.sweep_removed_total >= 1,
            "sweep must run and remove the stale entry"
        );
        assert_eq!(
            mgr.state().inode_cache_size(),
            1,
            "stale entry must be evicted; only the fresh entry remains"
        );
    }

    #[tokio::test]
    async fn test_cache_miss_sweeps_regardless_of_threshold() {
        // Even with a huge threshold (so lazy-sweep would always skip),
        // the cache-miss path must still sweep unconditionally — the
        // upcoming RPC hides the sweep cost and the cache stays clean
        // before inserting the new entry.
        let (mgr, _backend) = make_manager();
        let mgr = mgr.with_sweep_threshold(Duration::from_secs(60));

        // Inject a stale entry.
        inject_stale_inode(&mgr, 999);

        // Acquire a different inode → cache miss → sweep must run
        // (despite the 60s threshold) and evict the stale entry.
        let req = LockRequest::new(7u64, LockMode::Shared, Duration::from_secs(30));
        mgr.acquire(req).await.expect("acquire must succeed");

        let snap = mgr.metrics().snapshot();
        assert_eq!(
            snap.sweep_skipped_total, 0,
            "cache-miss path never goes through the lazy-skip fast path"
        );
        assert!(
            snap.sweep_removed_total >= 1,
            "cache-miss sweep must evict the stale entry"
        );
        assert_eq!(
            mgr.state().inode_cache_size(),
            1,
            "only the freshly-acquired entry should remain"
        );
    }

    #[tokio::test]
    async fn test_lazy_sweep_runs_on_cache_hit_after_threshold_elapses() {
        // Realistic scenario: cache hit, but enough time has elapsed
        // since the last sweep that the threshold is exceeded → the
        // sweep runs on the cache-hit path. Uses a 1ms threshold so
        // the test doesn't have to sleep for 100ms.
        let (mgr, backend) = make_manager();
        let mgr = mgr.with_sweep_threshold(Duration::from_millis(1));
        let inode = 42u64;

        // Prime the cache (sets last_sweep).
        let req = LockRequest::new(inode, LockMode::Shared, Duration::from_secs(30));
        mgr.acquire(req).await.expect("prime acquire");
        assert_eq!(backend.acquire_inode_calls.load(Ordering::SeqCst), 1);

        // Wait long enough for the threshold to elapse.
        tokio::time::sleep(Duration::from_millis(5)).await;

        // Inject a stale entry AFTER the sleep so the sweep has
        // something to remove.
        inject_stale_inode(&mgr, 999);

        // Cache hit — threshold exceeded → sweep must run and evict
        // the stale entry.
        let req2 = LockRequest::new(inode, LockMode::Shared, Duration::from_secs(30));
        mgr.acquire(req2).await.expect("cache hit must succeed");
        assert_eq!(
            backend.acquire_inode_calls.load(Ordering::SeqCst),
            1,
            "second acquire must be a cache hit"
        );

        let snap = mgr.metrics().snapshot();
        assert_eq!(
            snap.sweep_skipped_total, 0,
            "threshold exceeded → sweep must not be skipped"
        );
        assert!(
            snap.sweep_removed_total >= 1,
            "sweep must run and remove the stale entry"
        );
    }

    // ---------- register_handler ----------

    struct CapturingHandler {
        on_revoke_calls: AtomicU64,
        on_invalidate_calls: AtomicU64,
    }

    #[async_trait::async_trait]
    impl LockEventHandler for CapturingHandler {
        fn on_revoke(&self, _inode: u64, _token: &str) {
            self.on_revoke_calls.fetch_add(1, Ordering::SeqCst);
        }
        fn on_invalidate(&self, _inode: u64, _range: Option<Range>) {
            self.on_invalidate_calls.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn test_register_handler_stores_weak_reference() {
        let (mgr, _backend) = make_manager();
        let handler = Arc::new(CapturingHandler {
            on_revoke_calls: AtomicU64::new(0),
            on_invalidate_calls: AtomicU64::new(0),
        });
        mgr.register_handler(Arc::clone(&handler) as Arc<dyn LockEventHandler>);

        // The handler must still be alive (we hold a strong ref in the
        // test); the manager's Weak must upgrade successfully. The
        // upgraded Arc must be released (via the inner block) before we
        // drop the original `handler` ref, otherwise the upgraded Arc
        // itself would keep the inner value alive.
        {
            let guard = mgr.handler.lock().unwrap();
            let upgraded = guard.as_ref().and_then(|w| w.upgrade());
            assert!(
                upgraded.is_some(),
                "Weak handler must upgrade while strong ref exists"
            );
            // `upgraded` drops here, releasing its strong ref.
        }

        // Drop the strong ref — the Weak inside the manager must not
        // keep the handler alive.
        drop(handler);
        let guard = mgr.handler.lock().unwrap();
        let upgraded = guard.as_ref().and_then(|w| w.upgrade());
        assert!(upgraded.is_none(), "Weak must not prevent handler drop");
    }

    // ---------- Lockify fast path (phase 4 §5.1) ----------

    /// Build a manager with Lockify enabled, sharing the test's mock
    /// backend so `acquire_local` async-syncs to the same backend
    /// the regular `acquire` uses. Uses `with_lockify(None)` so the
    /// spawned sync tasks run on the `#[tokio::test]` runtime.
    fn make_manager_with_lockify(backend: Arc<MockBackend>) -> (FuseLockManager, Arc<MockBackend>) {
        let mgr = FuseLockManager::new(
            Arc::clone(&backend) as Arc<dyn FuseLockBackend>,
            "client-A".to_string(),
            30_000,
        )
        .with_lockify(None);
        (mgr, backend)
    }

    #[tokio::test]
    async fn test_acquire_local_returns_local_token_no_rpc() {
        let (mgr, backend) = make_manager_with_lockify(Arc::new(MockBackend::new()));
        let grant = mgr
            .acquire_local(42, LockMode::Exclusive, 30_000)
            .expect("acquire_local must succeed");

        // Token has the local prefix.
        assert!(grant.token.starts_with(crate::lockify::LOCAL_TOKEN_PREFIX));
        assert_eq!(grant.inode, 42);
        assert_eq!(grant.lease_ms, 30_000);
        assert_eq!(grant.sn, 0);
        assert!(grant.range.is_none());

        // Synchronous path: cache hit, zero RPC.
        assert_eq!(
            backend.acquire_inode_calls.load(Ordering::SeqCst),
            0,
            "no synchronous RPC on fast path"
        );
        let cached = mgr.state().get_inode(42).expect("must be cached");
        assert_eq!(cached.token, grant.token);

        let snap = mgr.metrics().snapshot();
        assert_eq!(snap.lockify_self_declare_total, 1);
    }

    #[tokio::test]
    async fn test_acquire_local_without_with_lockify_errors() {
        let (mgr, _backend) = make_manager();
        let err = mgr
            .acquire_local(1, LockMode::Exclusive, 30_000)
            .expect_err("must require with_lockify");
        assert!(matches!(err, LockError::Internal(_)));
    }

    #[tokio::test]
    async fn test_acquire_local_async_sync_cas_replaces_token() {
        // The mock returns "tok-inode" for `acquire_inode_lease`. The
        // async sync should CAS-replace the local token with that.
        let (mgr, backend) = make_manager_with_lockify(Arc::new(MockBackend::new()));
        let grant = mgr
            .acquire_local(7, LockMode::Exclusive, 30_000)
            .expect("acquire_local");

        // Wait for the spawned sync to call the backend.
        for _ in 0..100 {
            if backend.acquire_inode_calls.load(Ordering::SeqCst) >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        // Wait for the CAS to swap the cached token.
        for _ in 0..100 {
            if mgr
                .state()
                .get_inode(7)
                .map(|e| e.token == "tok-inode")
                .unwrap_or(false)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        let cached = mgr.state().get_inode(7).expect("must still be cached");
        assert_eq!(
            cached.token, "tok-inode",
            "local token must be CAS-replaced by server token"
        );

        let snap = mgr.metrics().snapshot();
        assert_eq!(snap.lockify_sync_ok_total, 1);

        // The original local token (from grant) is now orphaned —
        // releasing via the trait should hit the new cached token.
        // (We don't release here — the assertion is just that the
        // cache now holds the server token, not the local one.)
        let _ = grant;
    }

    #[tokio::test]
    async fn test_acquire_local_then_acquire_uses_cached_token() {
        // The Lockify fast path populates the cache; a subsequent
        // `LockManager::acquire` for the same inode must hit the
        // cache and skip the RPC.
        let (mgr, backend) = make_manager_with_lockify(Arc::new(MockBackend::new()));
        let _local_grant = mgr
            .acquire_local(11, LockMode::Exclusive, 30_000)
            .expect("acquire_local");

        // Wait briefly to allow the spawned sync to settle (it
        // replaces the local token with the server "tok-inode").
        for _ in 0..200 {
            if backend.acquire_inode_calls.load(Ordering::SeqCst) >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        for _ in 0..200 {
            if mgr
                .state()
                .get_inode(11)
                .map(|e| e.token == "tok-inode")
                .unwrap_or(false)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        let calls_before = backend.acquire_inode_calls.load(Ordering::SeqCst);

        // Now a regular `acquire` — must hit the cache (the
        // CAS-merged server token is still valid).
        let req = LockRequest::new(11, LockMode::Exclusive, Duration::from_secs(30));
        let grant = mgr.acquire(req).await.expect("acquire must succeed");
        assert_eq!(grant.token, "tok-inode", "must reuse cached server token");
        assert_eq!(
            backend.acquire_inode_calls.load(Ordering::SeqCst),
            calls_before,
            "regular acquire after Lockify CAS must hit cache"
        );

        let snap = mgr.metrics().snapshot();
        assert!(snap.acquire_cache_hit >= 1);
    }
}
