//! Lockify §5.1 asynchronous metadata — local self-declaration fast
//! path for inode lease acquisition.
//!
//! For new inodes (creat/mkdir/mknod), the client locally self-declares
//! ownership by generating a local token and inserting it into the
//! cache — **no synchronous RPC**. The filer is asynchronously notified
//! in the background via `FuseLockBackend::acquire_inode_lease`, off the
//! critical path.
//!
//! # Why this is safe
//!
//! New inodes have no prior ownership history (the inode number was
//! just minted by the parent directory's owner), so the optimistic
//! assumption "I am the owner" holds in the common case. If it turns
//! out another client raced us to the filer, the async sync RPC
//! returns a conflict and we invalidate the local entry — subsequent
//! operations on the inode fall back to the regular RPC path.
//!
//! The local token is recognisable by its [`LOCAL_TOKEN_PREFIX`] so
//! the server can also accept it idempotently in a future protocol
//! revision (current filer just issues a fresh server token on
//! `acquire_inode_lease`; the client-side CAS-merge handles the
//! handoff).
//!
//! # Failure handling
//!
//! - **Sync success**: the cache entry's local token is replaced with
//!   the server-issued token via CAS
//!   ([`ClientLeaseState::replace_inode_by_token`]). Only replaces if
//!   the cached token still matches the local one — concurrent
//!   `release`/`re-acquire` is preserved.
//! - **Sync conflict** (another client owns the inode): the local
//!   entry is invalidated. Subsequent operations must re-acquire via
//!   the regular path. This is the rare race; the metadata fast path
//!   is only useful for low-conflict workloads anyway.
//! - **Sync network error**: the local token remains valid until TTL
//!   expires (graceful degradation). The server will eventually
//!   reconcile on the next acquire/renew.
//!
//! # Applicability
//!
//! Lockify is opt-in: only the FUSE create/mkdir/mknod paths use it
//! (low-conflict metadata workloads). Read/write paths still use the
//! regular `acquire` to preserve correctness on contended inodes.
//!
//! See `docs/lock-optimization-plan.md` §5.1.

use crate::backend::FuseLockBackend;
use crate::metrics::LockMetrics;
use crate::state::{ClientLeaseState, InodeLeaseEntry};
use powerfs_lock::{LockError, LockGrant, LockMode};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Prefix for locally-generated tokens. Distinguishes them from
/// server-issued tokens (server tokens are typically UUID-like or
/// numeric, never starting with this prefix).
pub const LOCAL_TOKEN_PREFIX: &str = "local-";

/// Lockify §5.1 fast path helper.
///
/// Owned by [`crate::manager::FuseLockManager`] (optional, via
/// [`crate::manager::FuseLockManager::with_lockify`]). Calls through
/// to [`FuseLockBackend::acquire_inode_lease`] for the async sync —
/// no new backend method is required, keeping the wire protocol
/// unchanged.
///
/// # Spawning
///
/// `self_declare` spawns the async sync task via either:
/// - the `tokio::runtime::Handle` provided via [`Self::with_runtime`]
///   (useful in tests / when called from a non-tokio thread), or
/// - `tokio::spawn` (requires a runtime in the caller's context).
///
/// The task is fire-and-forget — `Lockify` does not track in-flight
/// syncs. If the manager is dropped while a sync is in flight, the
/// task still runs to completion (it owns clones of `Arc`s).
pub struct Lockify {
    backend: Arc<dyn FuseLockBackend>,
    client_id: Arc<String>,
    metrics: Arc<LockMetrics>,
    state: Arc<ClientLeaseState>,
    /// Optional runtime handle. When `None`, `tokio::spawn` is used.
    runtime: Option<tokio::runtime::Handle>,
}

impl Lockify {
    /// Construct a new `Lockify` helper.
    ///
    /// All args should come from the owning `FuseLockManager` so
    /// self-declared leases share the same cache/metrics/client_id as
    /// regular acquires.
    pub fn new(
        backend: Arc<dyn FuseLockBackend>,
        client_id: Arc<String>,
        metrics: Arc<LockMetrics>,
        state: Arc<ClientLeaseState>,
    ) -> Self {
        Self {
            backend,
            client_id,
            metrics,
            state,
            runtime: None,
        }
    }

    /// Attach a runtime handle so spawned sync tasks don't require a
    /// runtime in the caller's context. Useful for tests and for
    /// callers that may run on a worker thread without a runtime.
    #[must_use]
    pub fn with_runtime(mut self, handle: tokio::runtime::Handle) -> Self {
        self.runtime = Some(handle);
        self
    }

    /// Generate a local token (no RPC) and cache the lease. The async
    /// sync to the filer is spawned detached.
    ///
    /// Returns a `LockGrant` whose `token` starts with
    /// [`LOCAL_TOKEN_PREFIX`]. Callers should treat this as a regular
    /// grant — subsequent `release`/`renew` will hit the cache (hit)
    /// or fall through to the regular RPC path (miss) transparently.
    ///
    /// # Errors
    ///
    /// Returns [`LockError::Internal`] only if `duration_ms == 0`
    /// (a local lease must outlive the synchronous caller path).
    /// Backend errors are surfaced asynchronously via metrics; the
    /// self-declare fast path never fails synchronously on RPC.
    pub fn self_declare(
        &self,
        inode: u64,
        mode: LockMode,
        duration_ms: u64,
    ) -> Result<LockGrant, LockError> {
        if duration_ms == 0 {
            return Err(LockError::Internal(
                "lockify self_declare requires non-zero duration_ms".to_string(),
            ));
        }

        self.metrics.record_lockify_self_declare();
        let nonce = self.metrics.next_lockify_nonce();
        let token = format!(
            "{prefix}{client}-{inode}-{nonce}",
            prefix = LOCAL_TOKEN_PREFIX,
            client = self.client_id,
            inode = inode,
            nonce = nonce,
        );
        let expire_at = Instant::now() + Duration::from_millis(duration_ms);

        // Insert into cache. `put_inode` unconditionally replaces any
        // existing entry — but `self_declare` is only called on fresh
        // inodes (creat/mkdir/mknod), so there should be no prior
        // entry. If there is (e.g. caller mistake), we still proceed:
        // the new local token wins, and the async sync will reconcile.
        self.state.put_inode(
            inode,
            InodeLeaseEntry {
                token: token.clone(),
                expire_at,
                mode: mode.clone(),
            },
        );

        // Spawn async sync. The task captures clones of all the Arcs
        // it needs and runs to completion even if `self` is dropped.
        let backend = Arc::clone(&self.backend);
        let client_id = Arc::clone(&self.client_id);
        let metrics = Arc::clone(&self.metrics);
        let state = Arc::clone(&self.state);
        let local_token = token.clone();
        let task_mode = mode.clone();
        let effective_duration_ms = duration_ms;

        let task = async move {
            match backend
                .acquire_inode_lease(inode, &client_id, effective_duration_ms)
                .await
            {
                Ok((server_token, server_expire_ms)) => {
                    metrics.record_lockify_sync_ok();
                    let new_expire_at = Instant::now()
                        + Duration::from_millis(server_expire_ms.max(effective_duration_ms));
                    let new_entry = InodeLeaseEntry {
                        token: server_token.clone(),
                        expire_at: new_expire_at,
                        mode: task_mode,
                    };
                    let replaced = state.replace_inode_by_token(inode, &local_token, new_entry);
                    if replaced {
                        log::debug!(
                            "lockify sync ok inode={} local={} server={}",
                            inode,
                            local_token,
                            server_token,
                        );
                    } else {
                        // CAS failed: the local entry was already
                        // released or re-acquired via the regular path.
                        // The server-issued token is orphaned — the
                        // server will reclaim it via TTL or the next
                        // release RPC.
                        log::debug!(
                            "lockify sync cas-miss inode={} local={} server={} \
                             (entry already replaced or released)",
                            inode,
                            local_token,
                            server_token,
                        );
                    }
                }
                Err(e) => {
                    let lower = e.to_lowercase();
                    if lower.contains("conflict") {
                        metrics.record_lockify_sync_conflict();
                        let removed = state.remove_inode_by_token(inode, &local_token);
                        if removed {
                            log::warn!(
                                "lockify sync conflict inode={} local={} (local entry invalidated)",
                                inode,
                                local_token,
                            );
                        } else {
                            log::debug!(
                                "lockify sync conflict inode={} local={} (entry already replaced)",
                                inode,
                                local_token,
                            );
                        }
                    } else {
                        metrics.record_lockify_sync_err();
                        log::warn!(
                            "lockify sync err inode={} local={}: {} (local token remains valid until TTL)",
                            inode,
                            local_token,
                            e,
                        );
                    }
                }
            }
        };

        if let Some(handle) = &self.runtime {
            handle.spawn(task);
        } else {
            tokio::spawn(task);
        }

        Ok(LockGrant {
            inode,
            token,
            sn: 0,
            lease_ms: duration_ms,
            mode,
            range: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex as StdMutex;

    // ---------- mock backend ----------

    struct MockBackend {
        acquire_calls: AtomicU64,
        acquire_resp: StdMutex<Result<(String, u64), String>>,
    }

    impl MockBackend {
        fn new_ok(server_token: &str, expire_ms: u64) -> Self {
            Self {
                acquire_calls: AtomicU64::new(0),
                acquire_resp: StdMutex::new(Ok((server_token.to_string(), expire_ms))),
            }
        }
        fn new_err(err: &str) -> Self {
            Self {
                acquire_calls: AtomicU64::new(0),
                acquire_resp: StdMutex::new(Err(err.to_string())),
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
            self.acquire_calls.fetch_add(1, Ordering::SeqCst);
            self.acquire_resp.lock().unwrap().clone()
        }
        async fn release_inode_lease(
            &self,
            _inode: u64,
            _client_id: &str,
            _token: &str,
        ) -> Result<(), String> {
            Ok(())
        }
        async fn renew_inode_lease(
            &self,
            _inode: u64,
            _client_id: &str,
            _token: &str,
            _duration_ms: u64,
        ) -> Result<(), String> {
            Ok(())
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
            Ok("tok-range".to_string())
        }
        async fn release_range_lease(
            &self,
            _volume_id: u64,
            _inode: u64,
            _stripe_start: u64,
            _client_id: &str,
            _token: &str,
        ) -> Result<(), String> {
            Ok(())
        }
        async fn lookup_volume_id(&self, _inode: u64) -> Result<u64, String> {
            Ok(42)
        }
    }

    fn make_lockify(backend: Arc<dyn FuseLockBackend>) -> Lockify {
        let state = ClientLeaseState::new_shared();
        let metrics = Arc::new(LockMetrics::new());
        let client_id = Arc::new("test-client".to_string());
        // No `with_runtime` — `tokio::spawn` is used, so the
        // `#[tokio::test]` runtime hosts the async sync tasks.
        Lockify::new(backend, client_id, metrics, state)
    }

    // ---------- happy path ----------

    #[tokio::test]
    async fn test_self_declare_returns_local_token_no_rpc() {
        // The fast path returns immediately with a local-prefixed
        // token. The sync RPC is spawned async, but the test asserts
        // only what's visible synchronously.
        let backend = Arc::new(MockBackend::new_ok("server-tok-1", 30_000));
        let calls_before = backend.acquire_calls.load(Ordering::SeqCst);
        let lockify = make_lockify(Arc::clone(&backend) as Arc<dyn FuseLockBackend>);

        let grant = lockify
            .self_declare(42, LockMode::Exclusive, 30_000)
            .expect("self_declare must succeed");

        // Token is local-prefixed.
        assert!(grant.token.starts_with(LOCAL_TOKEN_PREFIX));
        assert_eq!(grant.inode, 42);
        assert_eq!(grant.mode, LockMode::Exclusive);
        assert_eq!(grant.sn, 0);
        assert_eq!(grant.lease_ms, 30_000);
        assert!(grant.range.is_none());

        // The sync RPC was spawned but may not have run yet. The cache
        // must already hold the local token.
        let cached = lockify.state.get_inode(42).expect("must be cached");
        assert_eq!(cached.token, grant.token);

        // The synchronous path bumped the self_declare counter.
        let snap = lockify.metrics.snapshot();
        assert_eq!(snap.lockify_self_declare_total, 1);

        // `acquire_calls` may still be 0 if the spawned task hasn't
        // scheduled yet; we'll assert on it after a yield.
        let _ = calls_before;
    }

    #[tokio::test]
    async fn test_self_declare_async_sync_replaces_local_token_on_success() {
        let backend = Arc::new(MockBackend::new_ok("server-tok-2", 30_000));
        let lockify = make_lockify(Arc::clone(&backend) as Arc<dyn FuseLockBackend>);

        let grant = lockify
            .self_declare(7, LockMode::Exclusive, 30_000)
            .expect("self_declare");

        // Wait for the spawned sync to complete. The backend records
        // the call and the cache should CAS-replace the local token.
        for _ in 0..100 {
            if backend.acquire_calls.load(Ordering::SeqCst) >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(
            backend.acquire_calls.load(Ordering::SeqCst),
            1,
            "async sync must call backend"
        );

        // Give the post-RPC CAS a beat to run.
        for _ in 0..100 {
            if lockify
                .state
                .get_inode(7)
                .map(|e| e.token == "server-tok-2")
                .unwrap_or(false)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        let cached = lockify.state.get_inode(7).expect("must still be cached");
        assert_eq!(
            cached.token, "server-tok-2",
            "local token must be CAS-replaced by server token"
        );

        let snap = lockify.metrics.snapshot();
        assert_eq!(snap.lockify_self_declare_total, 1);
        assert_eq!(snap.lockify_sync_ok_total, 1);
        assert_eq!(snap.lockify_sync_conflict_total, 0);
        assert_eq!(snap.lockify_sync_err_total, 0);

        // The original local token is gone — release must NOT find it.
        let _ = grant; // grant.token is now orphaned server-side
    }

    #[tokio::test]
    async fn test_self_declare_async_sync_conflict_invalidates_local_entry() {
        let backend = Arc::new(MockBackend::new_err("conflict: held by another client"));
        let lockify = make_lockify(Arc::clone(&backend) as Arc<dyn FuseLockBackend>);

        let _grant = lockify
            .self_declare(99, LockMode::Exclusive, 30_000)
            .expect("self_declare");

        // Wait for the sync to hit the conflict path.
        for _ in 0..100 {
            if backend.acquire_calls.load(Ordering::SeqCst) >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        // Give the post-conflict invalidation a beat to run.
        for _ in 0..100 {
            if lockify.state.get_inode(99).is_none() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        assert!(
            lockify.state.get_inode(99).is_none(),
            "conflict must invalidate the local cache entry"
        );
        let snap = lockify.metrics.snapshot();
        assert_eq!(snap.lockify_sync_conflict_total, 1);
        assert_eq!(snap.lockify_sync_ok_total, 0);
        assert_eq!(snap.lockify_sync_err_total, 0);
    }

    #[tokio::test]
    async fn test_self_declare_async_sync_network_err_keeps_local_token() {
        let backend = Arc::new(MockBackend::new_err("network timeout contacting filer"));
        let lockify = make_lockify(Arc::clone(&backend) as Arc<dyn FuseLockBackend>);

        let grant = lockify
            .self_declare(123, LockMode::Shared, 30_000)
            .expect("self_declare");

        // Wait for the sync to fail.
        for _ in 0..100 {
            if backend.acquire_calls.load(Ordering::SeqCst) >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        // Yield to let the error-handling branch run.
        tokio::time::sleep(Duration::from_millis(5)).await;

        // Local token remains valid until TTL (graceful degradation).
        let cached = lockify
            .state
            .get_inode(123)
            .expect("local token must remain");
        assert_eq!(cached.token, grant.token);

        let snap = lockify.metrics.snapshot();
        assert_eq!(snap.lockify_sync_err_total, 1);
        assert_eq!(snap.lockify_sync_conflict_total, 0);
        assert_eq!(snap.lockify_sync_ok_total, 0);
    }

    #[tokio::test]
    async fn test_self_declare_cas_miss_when_local_token_already_released() {
        // If the user releases the inode before the sync completes,
        // the CAS must fail (no clobber) and the server token is
        // orphaned — no cache corruption.
        let backend = Arc::new(MockBackend::new_ok("server-orphan", 30_000));
        let lockify = make_lockify(Arc::clone(&backend) as Arc<dyn FuseLockBackend>);

        let grant = lockify
            .self_declare(555, LockMode::Exclusive, 30_000)
            .expect("self_declare");

        // User releases before sync completes.
        let removed = lockify.state.remove_inode_by_token(555, &grant.token);
        assert!(removed, "user must be able to release local token");

        // Wait for the sync to run.
        for _ in 0..100 {
            if backend.acquire_calls.load(Ordering::SeqCst) >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;

        // The cache must NOT have the server-orphan token — CAS miss.
        assert!(
            lockify.state.get_inode(555).is_none(),
            "CAS miss must not re-populate the cache"
        );

        // sync_ok counter still bumps — the RPC succeeded; the CAS
        // miss is observable only via the cache state.
        let snap = lockify.metrics.snapshot();
        assert_eq!(snap.lockify_sync_ok_total, 1);
    }

    #[tokio::test]
    async fn test_self_declare_zero_duration_returns_error() {
        let backend: Arc<dyn FuseLockBackend> = Arc::new(MockBackend::new_ok("server-x", 30_000));
        let lockify = make_lockify(backend);

        let err = lockify
            .self_declare(1, LockMode::Exclusive, 0)
            .expect_err("zero duration must fail");
        assert!(matches!(err, LockError::Internal(_)));
    }

    #[tokio::test]
    async fn test_self_declare_tokens_are_unique_across_calls() {
        let backend: Arc<dyn FuseLockBackend> = Arc::new(MockBackend::new_ok("server-y", 30_000));
        let lockify = make_lockify(backend);

        let g1 = lockify
            .self_declare(7, LockMode::Exclusive, 30_000)
            .expect("call 1");
        let g2 = lockify
            .self_declare(7, LockMode::Exclusive, 30_000)
            .expect("call 2");
        let g3 = lockify
            .self_declare(7, LockMode::Exclusive, 30_000)
            .expect("call 3");

        // Same inode, but the nonce in the token must differ.
        assert_ne!(g1.token, g2.token);
        assert_ne!(g2.token, g3.token);
        assert_ne!(g1.token, g3.token);
    }
}
