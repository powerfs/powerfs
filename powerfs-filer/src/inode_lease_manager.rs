//! Inode Metadata Lease Manager (方案 A / Phase 2 — powerfs-lease backed)
//!
//! Manages per-inode exclusive leases in Filer memory. Used when the Volume
//! Server backend doesn't support range lease (e.g., NVMe-oF target).
//!
//! The lease is an admission-control mechanism: only the holder can write to
//! the inode. Strong consistency (content_size + chunks atomicity) is still
//! guaranteed by Raft (`UpdateInodeSizeChunks`); the lease prevents concurrent
//! writers from producing conflicting intermediate states.
//!
//! # Lifecycle
//! 1. FUSE client → Filer: `AcquireInodeLease(inode, exclusive)`
//! 2. Filer: if no active lease (or expired past grace), grant lease + token
//! 3. FUSE client → Volume Server: `write_needle` (no lease validation)
//! 4. FUSE client → Filer: `UpdateInodeSizeChunks` (Raft atomic commit)
//! 5. FUSE client → Filer: `ReleaseInodeLease(inode, token)`
//!
//! # Crash recovery
//! - Lease TTL expires automatically after `duration_ms + grace_period_ms`
//! - During grace period, new acquire requests from a different holder are
//!   rejected (safety margin for network delays)
//! - After grace period, the lease is evicted and a new client can acquire
//!
//! # Architecture (rewritten in 阶段一B)
//!
//! Previously this module used a hand-rolled `RwLock<HashMap<u64, InodeLeaseEntry>>`
//! — duplicating logic already present in `powerfs-lease`. It has been
//! rewritten to wrap [`MemoryLeaseStore<InodeKey>`], gaining:
//! - Conflict detection via the generic `LeaseKey::conflicts` contract
//! - Optional persistence through the `LeasePersistence` trait (Raft log
//!   integration is wired in the optimization phase, see
//!   `docs/lock-optimization-plan.md` §6.3 P1)
//! - Unified monitoring counters via `LeaseStats`
//!
//! The public API (`acquire`/`release`/`renew`/`validate`/...) is preserved
//! verbatim so `net_handler.rs` needs no changes.

use crate::adaptive_grace::AdaptiveGrace;
use crate::early_grant::{
    AcquireOutcome, LeaseRevoker, NoopPenalty, NoopRevoker, RevokeState, RevokeTimeoutPenalty,
    SnAllocator, WaitQueue, Waiter,
};
use powerfs_lease::{
    LeaseError, LeaseKey, LeaseMode, LeasePersistence, LeaseStats, LeaseStore, MemoryLeaseStore,
};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

/// Default grace period after lease expiry before allowing a new holder.
/// This prevents data corruption when a client is slow but still alive.
const DEFAULT_GRACE_PERIOD_MS: u64 = 5000;

/// §8.3.1: how long (ms) the server waits for a holder's `RevokeAck`
/// before force-reclaiming its lease and granting the next waiter.
/// 2 seconds per the plan — long enough for a well-behaved client to
/// flush dirty pages, short enough to bound waiter stall under an
/// unresponsive / crashed holder.
const DEFAULT_REVOKE_TIMEOUT_MS: u64 = 2000;

/// Resource key for inode-level leases.
///
/// `group_id = inode` so all leases for the same inode land in the same
/// conflict group; `conflicts` returns true iff two keys refer to the same
/// inode — i.e. inode leases are whole-inode exclusive (no sub-inode
/// granularity). For range granularity, the volume server uses `StripeKey`
/// instead.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct InodeKey {
    pub inode: u64,
}

impl InodeKey {
    pub fn new(inode: u64) -> Self {
        Self { inode }
    }
}

impl LeaseKey for InodeKey {
    fn group_id(&self) -> u64 {
        self.inode
    }

    fn conflicts(&self, other: &Self) -> bool {
        self.inode == other.inode
    }

    fn encode(&self) -> Vec<u8> {
        self.inode.to_le_bytes().to_vec()
    }

    fn decode(data: &[u8]) -> Result<Self, LeaseError> {
        if data.len() < 8 {
            return Err(LeaseError::Internal(format!(
                "InodeKey decode: expected 8 bytes, got {}",
                data.len()
            )));
        }
        let inode = u64::from_le_bytes(data[0..8].try_into().unwrap());
        Ok(Self { inode })
    }
}

/// Inode metadata lease manager — in-memory, per-Filer-leader.
///
/// Backed by [`MemoryLeaseStore<InodeKey>`]. The store holds lease state in
/// memory; persistence (Raft log integration) is optional and attached via
/// [`InodeLeaseManager::with_persistence`]. If the Filer leader changes,
/// lease state is lost unless persistence is configured — clients retry
/// acquire on the new leader. The actual data consistency is guaranteed by
/// Raft (`UpdateInodeSizeChunks`), not by the lease.
#[derive(Clone)]
pub struct InodeLeaseManager {
    store: Arc<MemoryLeaseStore<InodeKey>>,
    grace_period: Duration,
    /// Serializes the idempotent pre-check with the store mutation in
    /// [`acquire`], so concurrent same-holder acquires for the same inode
    /// always observe a consistent view and return the same token. Other
    /// operations (`release`/`renew`/`validate`/...) rely on the store's
    /// own internal locking for atomicity.
    acquire_lock: Arc<Mutex<()>>,
    /// Phase-4 §5.2/§5.3 SN allocator (leader-local, optimistic).
    /// `acquire`/`acquire_or_wait`/`handle_revoke_ack` allocate an SN
    /// on every grant. See `early_grant::SnAllocator`.
    sn: Arc<SnAllocator>,
    /// Phase-4 §5.2 wait queue for contended acquires. Populated by
    /// `acquire_or_wait` on conflict; drained by `handle_revoke_ack`
    /// (Early Grant) or `release` (normal handoff).
    waiters: Arc<WaitQueue>,
    /// Phase-4 §5.2 Early Revoke push transport. Defaults to a no-op
    /// (`NoopRevoker`) so the manager works without a push channel —
    /// in that mode `acquire_or_wait` queues waiters but never
    /// notifies the holder, so waiters only progress via the holder's
    /// voluntary release or TTL expiry.
    revoker: Arc<dyn LeaseRevoker>,
    /// Phase-4 P5: adaptive grace period tracker. Records how late
    /// each renew arrives relative to the lease expiry, and computes
    /// `max(DEFAULT_GRACE, 3 * p99_lateness)` as the effective grace
    /// period. Used in `acquire` to replace the fixed `grace_period`.
    adaptive_grace: Arc<AdaptiveGrace>,
    /// §8.3.1: how long (ms) to wait for a holder's `RevokeAck` before
    /// force-reclaiming its lease and granting the next waiter. Default
    /// 2000ms per the plan. The background sweep
    /// (`force_reclaim_expired_revokes`) measures each pending revoke
    /// against this.
    revoke_timeout_ms: u64,
    /// §8.3.1: hook for recording a health penalty when a holder fails
    /// to ACK a revoke within `revoke_timeout_ms`. Defaults to
    /// `NoopPenalty`; the net layer wires a real bridge into
    /// `powerfs-lock-health`'s `ClientHealth` (which feeds the §8.2
    /// three-layer defense — quarantine / blacklist after repeated
    /// violations).
    penalty: Arc<dyn RevokeTimeoutPenalty>,
}

/// Result of an acquire attempt.
#[derive(Debug, Clone)]
pub struct AcquireResult {
    pub token: String,
    pub expire_at_ms: u64,
    /// Global sequence number allocated by the filer leader (phase 4
    /// §5.2/§5.3 — `SnAllocator::next_sn`). SN 0 means "not allocated"
    /// (legacy path / before Early Grant wiring); SN > 0 orders IO
    /// across leader switches and Early Grant handoffs. The wire
    /// protocol carries this in `FIELD_SN` (see `powerfs-lock-net`).
    pub sn: u64,
}

impl Default for InodeLeaseManager {
    fn default() -> Self {
        Self::new()
    }
}

impl InodeLeaseManager {
    /// Create a new manager with the default 5s grace period.
    pub fn new() -> Self {
        Self::with_grace_period(DEFAULT_GRACE_PERIOD_MS)
    }

    /// Create a new manager with a custom grace period (milliseconds).
    ///
    /// The grace period is also propagated to the underlying store as its
    /// `cleanup_grace`, so expired-but-in-grace entries remain queryable
    /// via `get_all_entries_by_group` until they're truly evicted.
    pub fn with_grace_period(grace_ms: u64) -> Self {
        let grace = Duration::from_millis(grace_ms);
        let store = MemoryLeaseStore::<InodeKey>::new().with_cleanup_grace(grace);
        Self {
            store: Arc::new(store),
            grace_period: grace,
            acquire_lock: Arc::new(Mutex::new(())),
            sn: Arc::new(SnAllocator::default()),
            waiters: Arc::new(WaitQueue::new()),
            revoker: Arc::new(NoopRevoker),
            adaptive_grace: Arc::new(AdaptiveGrace::new()),
            // §8.3.1 defaults: 2s revoke timeout, no-op penalty (the net
            // layer wires a real ClientHealth bridge during startup).
            revoke_timeout_ms: DEFAULT_REVOKE_TIMEOUT_MS,
            penalty: Arc::new(NoopPenalty),
        }
    }

    /// Attach a persistence backend (e.g. a Raft-log-backed
    /// `LeasePersistence` impl). After this, acquire/renew/release are
    /// also persisted, and [`load_from_persistence`] can recover state on
    /// startup. The optimization phase wires the actual Raft integration
    /// (see `docs/lock-optimization-plan.md` §6.3 P1).
    pub fn with_persistence<P: LeasePersistence + 'static>(self, backend: P) -> Self {
        // Rebuild the store with persistence attached. We can't mutate the
        // existing Arc directly because the builder consumes self; create a
        // new store that mirrors the previous configuration.
        let new_store = MemoryLeaseStore::<InodeKey>::new()
            .with_cleanup_grace(self.grace_period)
            .with_persistence(backend);
        Self {
            store: Arc::new(new_store),
            grace_period: self.grace_period,
            acquire_lock: self.acquire_lock,
            sn: self.sn,
            waiters: self.waiters,
            revoker: self.revoker,
            adaptive_grace: self.adaptive_grace,
            revoke_timeout_ms: self.revoke_timeout_ms,
            penalty: self.penalty,
        }
    }

    /// Attach an Early Revoke push transport (phase 4 §5.2). When set,
    /// `acquire_or_wait` will push a `Revoke` notification to the
    /// current holder when a new waiter queues, enabling the holder to
    /// flush + release early instead of waiting for TTL + grace.
    ///
    /// Without this (the default `NoopRevoker`), `acquire_or_wait`
    /// still queues waiters but the holder is never notified — waiters
    /// only progress via the holder's voluntary release or TTL expiry.
    #[must_use]
    pub fn with_revoker<R: LeaseRevoker + 'static>(self, revoker: R) -> Self {
        Self {
            revoker: Arc::new(revoker),
            ..self
        }
    }

    /// §8.3.1: override the RevokeAck timeout (ms). The server waits
    /// this long after pushing a `Revoke` before force-reclaiming the
    /// lease and granting the next waiter. Default 2000ms; raise it on
    /// high-latency links where a well-behaved client needs more than
    /// 2s to flush, lower it for tight latency SLOs.
    #[must_use]
    pub fn with_revoke_timeout_ms(self, ms: u64) -> Self {
        Self {
            revoke_timeout_ms: ms,
            ..self
        }
    }

    /// §8.3.1: attach a health-penalty recorder. When the server
    /// force-reclaims a lease because the holder didn't ACK within the
    /// timeout, this is called with the unresponsive holder's client id.
    /// The net layer wires a bridge into `powerfs-lock-health`'s
    /// `ClientHealth` (feeding the §8.2 three-layer defense — repeated
    /// violations escalate to quarantine then blacklist). Without this
    /// (the default `NoopPenalty`), force-reclaim still happens but no
    /// health score is recorded.
    #[must_use]
    pub fn with_revoke_timeout_penalty<P: RevokeTimeoutPenalty + 'static>(
        self,
        penalty: P,
    ) -> Self {
        Self {
            penalty: Arc::new(penalty),
            ..self
        }
    }

    /// Override the SN allocator (phase 4 §5.3). Pass a pre-seeded
    /// `SnAllocator` on leader takeover so the new leader resumes
    /// allocating from the highest SN recovered from the Raft log —
    /// this avoids reusing SNs across leader switches.
    #[must_use]
    pub fn with_sn_allocator(self, sn: Arc<SnAllocator>) -> Self {
        Self { sn, ..self }
    }

    /// Allocate the next SN (phase 4 §5.2). Wraps `SnAllocator::next_sn`
    /// so callers don't need to import the `early_grant` module.
    pub fn next_sn(&self) -> u64 {
        self.sn.next_sn()
    }

    /// Current high-water SN. Wraps `SnAllocator::current_sn`.
    pub fn current_sn(&self) -> u64 {
        self.sn.current_sn()
    }

    /// Reset the SN allocator to a checkpoint value (phase 4 §5.3 —
    /// leader takeover after Raft log replay). Wraps
    /// `SnAllocator::reset_to`.
    pub fn reset_sn_to(&self, sn: u64) {
        self.sn.reset_to(sn);
    }

    /// Number of waiters queued for `inode` (phase 4 §5.2 — for
    /// monitoring / metrics). Returns 0 if no waiters.
    pub fn waiter_count(&self, inode: u64) -> usize {
        self.waiters.len(inode)
    }

    /// Whether any inode has queued waiters.
    pub fn has_queued_waiters(&self) -> bool {
        !self.waiters.is_empty()
    }

    /// Load non-expired leases from the persistence backend.
    /// Called on Filer leader takeover to recover lease state.
    pub fn load_from_persistence(&self) -> Result<usize, String> {
        self.store
            .load_from_persistence()
            .map_err(|e| format!("load_from_persistence failed: {}", e))
    }

    /// Persist the current epoch counter to the backend (best-effort,
    /// called periodically to fence ABA on token reuse).
    pub fn persist_epoch(&self) -> Result<(), String> {
        self.store
            .persist_epoch()
            .map_err(|e| format!("persist_epoch failed: {}", e))
    }

    /// Snapshot lease statistics (counters + active counts).
    pub fn stats(&self) -> LeaseStats {
        self.store.stats()
    }

    /// Acquire an exclusive inode metadata lease.
    ///
    /// Returns `Ok(AcquireResult)` on success, or `Err(reason)` if:
    /// - Another client holds a valid lease
    /// - The lease is in grace period (expired but not yet available)
    ///
    /// If the same client re-acquires (same holder), the existing lease is
    /// returned unchanged (idempotent) — matching the previous implementation's
    /// semantics.
    ///
    /// The entire check-then-grant sequence is serialized by
    /// [`acquire_lock`] so concurrent same-holder acquires for the same
    /// inode always return the same token (no duplicate tokens for the
    /// same `(inode, holder)` pair).
    pub fn acquire(
        &self,
        inode: u64,
        client_id: &str,
        duration_ms: u64,
    ) -> Result<AcquireResult, String> {
        let _guard = self.acquire_lock.lock().unwrap();
        let key = InodeKey::new(inode);
        let duration = Duration::from_millis(duration_ms);

        // Idempotent re-acquire: if an active (non-expired) lease exists for
        // this inode held by the same client, return its existing token.
        // We use get_entries_by_group (excludes expired) for this check.
        // SN is 0 on re-acquire — the caller already holds the SN from the
        // original grant and reuses it; SN 0 means "no new SN allocated"
        // (see `LockGrant::sn` doc in `powerfs-lock`). The store's `epoch`
        // field is the Fencer epoch (zombie-client fencing), NOT the SN.
        for entry in self.store.get_entries_by_group(inode) {
            if entry.holder == client_id {
                return Ok(AcquireResult {
                    token: entry.token,
                    expire_at_ms: duration_ms,
                    sn: 0,
                });
            }
        }

        // Grace-period protection: scan all entries (including expired ones
        // still in memory within the cleanup_grace window). If any is held by
        // a different client and is expired but NOT past grace, reject the
        // new acquire.
        //
        // Phase-4 P5: use the adaptive grace period (`max(configured,
        // 3 * p99_renew_lateness)`) instead of the fixed `grace_period`.
        // This expands the grace for slow networks where clients
        // consistently renew late, while keeping the configured floor for
        // fast networks.
        let effective_grace = self.adaptive_grace.effective_grace(self.grace_period);
        for entry in self.store.get_all_entries_by_group(inode) {
            if entry.holder == client_id {
                continue;
            }
            if entry.is_expired() && !entry.is_expired_beyond(effective_grace) {
                return Err(format!(
                    "inode {} lease in grace period (expired, holder={})",
                    inode, entry.holder
                ));
            }
        }

        // Delegate to the store — it handles conflict detection against
        // any active lease held by a different holder, token generation,
        // holder indexing, and optional persistence.
        match self
            .store
            .acquire(key, client_id, LeaseMode::Exclusive, duration)
        {
            Ok(entry) => {
                // Phase-4 §5.2/§5.3: allocate SN on every new grant.
                // Idempotent re-acquires (above) reuse the original SN.
                let sn = self.sn.next_sn();
                log::debug!(
                    "InodeLease: acquired inode={} holder={} duration_ms={} sn={}",
                    inode,
                    client_id,
                    duration_ms,
                    sn
                );
                Ok(AcquireResult {
                    token: entry.token,
                    expire_at_ms: duration_ms,
                    sn,
                })
            }
            Err(LeaseError::Conflict(msg)) => Err(format!(
                "inode {} lease held by another client: {}",
                inode, msg
            )),
            Err(e) => Err(format!("inode {} lease acquire failed: {}", inode, e)),
        }
    }

    /// Acquire leases for multiple inodes in sorted order (phase 4 P4).
    ///
    /// Sorts the inodes by number and acquires them in ascending order.
    /// If any acquire fails, all previously acquired leases in the batch
    /// are released (rollback). This prevents the classic A-B / B-A
    /// deadlock when two clients each need locks on the same set of
    /// inodes but acquire in different orders — by sorting, both clients
    /// acquire in the same global order, so there's no circular wait.
    ///
    /// # Deadlock prevention
    ///
    /// The sort is by raw `u64` inode number (not a hash). Since all
    /// clients use the same sort, the acquisition order is globally
    /// consistent. A client that needs inodes {3, 1, 2} acquires
    /// 1 → 2 → 3; another client needing {2, 3, 1} also acquires
    /// 1 → 2 → 3. No circular wait is possible.
    ///
    /// # Rollback
    ///
    /// On failure, all already-acquired leases in this batch are
    /// released via `release`. If a release fails (best-effort), the
    /// lease remains on the server until TTL or the next explicit
    /// release — the caller should log the orphaned tokens.
    ///
    /// # Returns
    ///
    /// `Ok(Vec<AcquireResult>)` — one result per input inode, in the
    /// **input** order (not the sorted order). The caller doesn't need
    /// to know the sort was applied.
    pub fn acquire_ordered(
        &self,
        inodes: &[u64],
        client_id: &str,
        duration_ms: u64,
    ) -> Result<Vec<AcquireResult>, String> {
        if inodes.is_empty() {
            return Ok(Vec::new());
        }

        // Sort by inode number for globally consistent ordering.
        // Use (original_index, inode) so we can restore input order
        // in the result.
        let mut sorted: Vec<(usize, u64)> = inodes.iter().copied().enumerate().collect();
        sorted.sort_by_key(|&(_, inode)| inode);

        let mut results_by_index: std::collections::HashMap<usize, AcquireResult> =
            std::collections::HashMap::with_capacity(inodes.len());
        let mut acquired: Vec<(u64, String)> = Vec::new();

        for (orig_idx, inode) in &sorted {
            match self.acquire(*inode, client_id, duration_ms) {
                Ok(result) => {
                    acquired.push((*inode, result.token.clone()));
                    results_by_index.insert(*orig_idx, result);
                }
                Err(e) => {
                    // Rollback: release all previously acquired leases.
                    for (rb_inode, rb_token) in &acquired {
                        if let Err(rb_err) = self.release(*rb_inode, client_id, rb_token) {
                            log::warn!(
                                "acquire_ordered rollback: failed to release \
                                 inode={} token={:.16}...: {} (orphaned, will TTL)",
                                rb_inode,
                                rb_token,
                                rb_err
                            );
                        }
                    }
                    return Err(format!(
                        "acquire_ordered failed at inode={} (index {}): {}; \
                         rolled back {} leases",
                        inode,
                        orig_idx,
                        e,
                        acquired.len()
                    ));
                }
            }
        }

        // Restore input order.
        let results = (0..inodes.len())
            .map(|i| {
                results_by_index
                    .remove(&i)
                    .expect("must have result for each index")
            })
            .collect();
        Ok(results)
    }

    /// Acquire-or-wait (phase 4 §5.2 Early Grant + Early Revoke).
    ///
    /// Like [`acquire`], but on conflict the request is queued and the
    /// caller is handed a `oneshot::Receiver` to await the grant
    /// notification. The server pushes an Early Revoke to the current
    /// holder (if a `LeaseRevoker` is configured); when the holder
    /// ACKs via [`handle_revoke_ack`], the next queued waiter is
    /// granted immediately (Early Grant) without waiting for the old
    /// holder's dirty-page flush — the SN on the grant preserves IO
    /// ordering.
    ///
    /// Returns:
    /// - `AcquireOutcome::Granted(r)` on immediate success (no
    ///   conflict, or idempotent re-acquire).
    /// - `AcquireOutcome::Queued(rx)` on conflict; await `rx` for the
    ///   grant. If the holder never ACKs and the TTL doesn't expire,
    ///   the waiter blocks indefinitely — callers should wrap the wait
    ///   in a `tokio::time::timeout`.
    /// - `AcquireOutcome::Error(s)` on grace-period rejection or other
    ///   failure (same conditions as `acquire`).
    pub fn acquire_or_wait(&self, inode: u64, client_id: &str, duration_ms: u64) -> AcquireOutcome {
        // Fast path: try a regular acquire first. The legacy path
        // handles idempotent re-acquire, grace-period rejection, and
        // the conflict-then-error case. On conflict we fall through
        // to the wait queue.
        let was_queued = self.waiters.has_waiters(inode);
        match self.acquire(inode, client_id, duration_ms) {
            Ok(result) => return AcquireOutcome::Granted(result),
            Err(e) if e.contains("held by another client") => {
                // Fall through to queue path.
            }
            Err(e) => return AcquireOutcome::Error(e),
        }

        // Conflict path: queue the waiter.
        let (tx, rx) = oneshot::channel();
        let waiter = Waiter::new(client_id.to_string(), duration_ms, tx);
        let queue_len = self.waiters.push(inode, waiter);

        // §5.2 Early Revoke: push a revoke notification to the current
        // holder ONLY if this is the first waiter — subsequent waiters
        // join a queue whose head has already been notified. Sending
        // a second revoke would be redundant and could confuse the
        // holder's state machine.
        if queue_len == 1 && !was_queued {
            // Look up the current holder to address the revoke.
            if let Some(holder_entry) = self.store.get_entries_by_group(inode).into_iter().next() {
                let _ = self
                    .revoker
                    .revoke(inode, &holder_entry.token, &holder_entry.holder);
                // §8.3.1: record the pending revoke so the background
                // sweep can force-reclaim the lease if the holder
                // doesn't ACK within `revoke_timeout_ms`. The snapshot
                // captures the holder + token at revoke-send time so
                // force-reclaim can release the correct entry even if
                // the holder later disconnects.
                self.waiters.record_revoke_sent(
                    inode,
                    RevokeState {
                        sent_at: Instant::now(),
                        holder: holder_entry.holder.clone(),
                        token: holder_entry.token.clone(),
                    },
                );
                log::debug!(
                    "InodeLease: early-revoke pushed inode={} holder={} waiter={}",
                    inode,
                    holder_entry.holder,
                    client_id
                );
            }
        }

        AcquireOutcome::Queued(rx)
    }

    /// Handle a RevokeAck from the current lease holder (phase 4 §5.2
    /// Early Grant).
    ///
    /// Called when the holder signals it has flushed its dirty data and
    /// is releasing the lease. The server:
    /// 1. Releases the holder's lease (verify holder + token match).
    /// 2. Pops the next queued waiter for this inode (FIFO).
    /// 3. Grants the lease to the waiter immediately — Early Grant:
    ///    the new holder gets the lease without waiting for the old
    ///    holder's dirty pages to be written back. The SN allocated
    ///    here preserves IO ordering (writes under the old SN are
    ///    sequenced before writes under the new SN).
    /// 4. Fulfills the waiter's `oneshot::Sender` with the grant.
    ///
    /// If no waiter is queued, the release is still performed (the
    /// holder's ACK means "I'm done"); the inode is just free for the
    /// next acquire (which will be a fast cache miss, not a queue pop).
    pub fn handle_revoke_ack(
        &self,
        inode: u64,
        token: &str,
        client_id: &str,
    ) -> Result<(), String> {
        // 1. Release the holder's lease. The store verifies holder +
        //    token; mismatches are errors (defensive — a misbehaving
        //    client might ACK with a stale token).
        match self.release(inode, client_id, token) {
            Ok(()) => {}
            Err(e) => {
                log::warn!(
                    "InodeLease: revoke-ack release failed inode={} holder={}: {}",
                    inode,
                    client_id,
                    e
                );
                return Err(e);
            }
        }

        // §8.3.1: the holder ACKed in time — clear the pending-revoke
        // entry so the background sweep doesn't force-reclaim a lease
        // the holder already voluntarily released.
        self.waiters.take_revoke(inode);

        // 2. Pop the next waiter (FIFO) and Early-Grant it. If none,
        //    the inode is free for the next acquire (no Early Grant).
        let Some(waiter) = self.waiters.pop(inode) else {
            log::debug!(
                "InodeLease: revoke-ack no waiter queued inode={} holder={}",
                inode,
                client_id
            );
            return Ok(());
        };

        self.grant_to_waiter(inode, waiter)
    }

    /// Grant the lease to a queued waiter (Early Grant). Shared by
    /// `handle_revoke_ack` (holder ACKed in time) and
    /// `force_reclaim_expired_revokes` (holder timed out → force-reclaim).
    ///
    /// Acquires the lease for the waiter, allocates a fresh SN (the
    /// waiter's IO must be sequenced behind the old holder's IO via the
    /// SN barrier), and fulfills the waiter's `oneshot::Sender`. If the
    /// acquire fails (e.g., a third client raced in), the sender is
    /// dropped so the waiter's `Receiver::await` returns an error and
    /// it retries.
    fn grant_to_waiter(&self, inode: u64, waiter: Waiter) -> Result<(), String> {
        match self.acquire(inode, &waiter.client_id, waiter.duration_ms) {
            Ok(result) => {
                let _ = waiter.sender.send(result);
                log::debug!(
                    "InodeLease: early-grant inode={} new_holder={}",
                    inode,
                    waiter.client_id
                );
                Ok(())
            }
            Err(e) => {
                log::warn!(
                    "InodeLease: early-grant failed inode={} waiter={}: {}",
                    inode,
                    waiter.client_id,
                    e
                );
                // Sender drops here → waiter's Receiver returns an error.
                Err(format!(
                    "early-grant failed for {}: {}",
                    waiter.client_id, e
                ))
            }
        }
    }

    /// Release an inode lease. The holder must match and the token must be
    /// valid (or already expired). Returns Ok(()) on success.
    ///
    /// Treating release of a non-existent / expired lease as success
    /// (idempotent), matching the Volume Server's range lease semantics.
    pub fn release(&self, inode: u64, client_id: &str, token: &str) -> Result<(), String> {
        // Look up by group to enforce inode-level token/holder checks
        // (the store keys by token alone; we need inode-scoped semantics).
        let entries = self.store.get_all_entries_by_group(inode);

        if let Some(matched) = entries.iter().find(|e| e.token == token) {
            // Token matches an entry for this inode — verify holder.
            if matched.holder != client_id {
                return Err(format!(
                    "inode {} lease holder mismatch: expected={}, got={}",
                    inode, matched.holder, client_id
                ));
            }
            match self.store.release(token, client_id) {
                Ok(()) => {
                    log::debug!("InodeLease: released inode={} holder={}", inode, client_id);
                    Ok(())
                }
                Err(LeaseError::NotFound) => {
                    // Race: entry was evicted between lookup and release —
                    // treat as idempotent success.
                    Ok(())
                }
                Err(e) => Err(format!("inode {} lease release failed: {}", inode, e)),
            }
        } else if let Some(first) = entries.first() {
            // An entry exists for the inode but with a different token.
            Err(format!(
                "inode {} lease token mismatch: expected={}, got={}",
                inode, first.token, token
            ))
        } else {
            // No entry — idempotent success.
            log::debug!(
                "InodeLease: released (idempotent, no entry) inode={} holder={}",
                inode,
                client_id
            );
            Ok(())
        }
    }

    /// Renew an existing inode lease. The holder and token must match.
    pub fn renew(
        &self,
        inode: u64,
        client_id: &str,
        token: &str,
        duration_ms: u64,
    ) -> Result<(), String> {
        let entries = self.store.get_all_entries_by_group(inode);
        let matched = entries
            .iter()
            .find(|e| e.token == token)
            .ok_or_else(|| format!("inode {} lease not found", inode))?;

        if matched.holder != client_id {
            return Err(format!("inode {} lease holder mismatch on renew", inode));
        }

        // Phase-4 P5: use the adaptive grace period for the past-grace
        // check, and record the renew lateness after success.
        let effective_grace = self.adaptive_grace.effective_grace(self.grace_period);
        // Reject renew if past grace — matches previous implementation's
        // `past_grace()` semantics.
        if matched.is_expired_beyond(effective_grace) {
            return Err(format!(
                "inode {} lease past grace period, cannot renew",
                inode
            ));
        }

        // Compute renew lateness BEFORE the store updates the expiry.
        // If the renew arrived after the old expiry, lateness is the gap;
        // otherwise zero (renewed early / on time).
        let lateness = Instant::now().saturating_duration_since(matched.expire_at);

        let duration = Duration::from_millis(duration_ms);
        match self.store.renew(token, client_id, duration) {
            Ok(()) => {
                // Phase-4 P5: record the lateness sample for adaptive
                // grace computation. The next `acquire` will use the
                // updated P99 to compute `max(grace, 3 * p99)`.
                self.adaptive_grace.record(lateness);
                log::debug!(
                    "InodeLease: renewed inode={} holder={} duration_ms={} lateness_ms={}",
                    inode,
                    client_id,
                    duration_ms,
                    lateness.as_millis()
                );
                Ok(())
            }
            Err(LeaseError::NotFound) => Err(format!("inode {} lease not found", inode)),
            Err(LeaseError::HolderMismatch { .. }) => {
                Err(format!("inode {} lease holder mismatch on renew", inode))
            }
            Err(e) => Err(format!("inode {} lease renew failed: {}", inode, e)),
        }
    }

    /// Validate that a client holds a valid lease for an inode.
    /// Used by Filer-side operations (e.g., close) to verify the caller
    /// is the lease holder before applying updates.
    pub fn validate(&self, inode: u64, client_id: &str, token: &str) -> Result<(), String> {
        let entries = self.store.get_all_entries_by_group(inode);

        // Find entry matching token; preserve original error semantics:
        // - no entry at all            → "not found"
        // - entry exists, token wrong  → "token mismatch"
        // - entry expired              → "expired"
        // - holder wrong               → "holder mismatch"
        let matched = entries.iter().find(|e| e.token == token);
        match matched {
            None => {
                if entries.is_empty() {
                    Err(format!("inode {} lease not found", inode))
                } else {
                    Err(format!("inode {} lease token mismatch", inode))
                }
            }
            Some(entry) => {
                if entry.is_expired() {
                    Err(format!("inode {} lease expired", inode))
                } else if entry.holder != client_id {
                    Err(format!(
                        "inode {} lease holder mismatch: expected={}, got={}",
                        inode, entry.holder, client_id
                    ))
                } else {
                    Ok(())
                }
            }
        }
    }

    /// Release all leases held by a client (used on client disconnect).
    pub fn disconnect_holder(&self, client_id: &str) -> usize {
        let count = self.store.disconnect_holder(client_id);
        if count > 0 {
            log::info!(
                "InodeLease: released {} leases for disconnected client={}",
                count,
                client_id
            );
        }
        count
    }

    /// Evict expired-and-past-grace entries (lazy cleanup).
    /// Called on acquire or periodically.
    pub fn cleanup_expired(&self) -> usize {
        self.store.cleanup_expired()
    }

    /// §8.3.1: force-reclaim leases whose holders didn't ACK a revoke
    /// within `revoke_timeout_ms`. For each expired revoke:
    /// 1. Release the stuck holder's lease (using the holder + token
    ///    snapshot captured at revoke-send time).
    /// 2. Pop the next queued waiter and Early-Grant it (the same
    ///    `grant_to_waiter` path used by `handle_revoke_ack`).
    /// 3. Record a health penalty for the unresponsive holder so the
    ///    §8.2 three-layer defense can quarantine / blacklist repeat
    ///    offenders.
    ///
    /// Returns the number of leases force-reclaimed. Intended to be
    /// called from a periodic background sweep (the filer spawns a
    /// 500ms `tokio::time::interval` task in `main.rs`). Safe to call
    /// when there are no pending revokes (returns 0).
    pub fn force_reclaim_expired_revokes(&self) -> usize {
        let expired = self.waiters.drain_expired_revokes(self.revoke_timeout_ms);
        if expired.is_empty() {
            return 0;
        }
        let count = expired.len();
        for (inode, state) in expired {
            // 1. Release the stuck holder's lease. If the release fails
            //    (e.g., the holder already reaped by TTL+grace or the
            //    token rotated), proceed to grant the waiter anyway —
            //    the acquire below will surface any real conflict.
            if let Err(e) = self.release(inode, &state.holder, &state.token) {
                log::warn!(
                    "InodeLease: §8.3.1 force-reclaim release failed inode={} holder={}: {} \
                     (granting waiter anyway)",
                    inode,
                    state.holder,
                    e
                );
            }
            // 2. Pop the next waiter and Early-Grant it. If the queue
            //    was drained (e.g., the waiter disconnected), skip.
            if let Some(waiter) = self.waiters.pop(inode) {
                if let Err(e) = self.grant_to_waiter(inode, waiter) {
                    log::warn!(
                        "InodeLease: §8.3.1 force-reclaim grant failed inode={}: {}",
                        inode,
                        e
                    );
                }
            }
            // 3. Penalize the unresponsive holder's health score. The
            //    net layer bridges this into `ClientHealth`; repeated
            //    violations escalate to quarantine then blacklist.
            self.penalty.on_revoke_ack_timeout(&state.holder);
            log::warn!(
                "InodeLease: §8.3.1 force-reclaimed inode={} holder={} \
                 (no RevokeAck within {}ms)",
                inode,
                state.holder,
                self.revoke_timeout_ms
            );
        }
        count
    }

    /// Number of active leases (for monitoring).
    pub fn active_count(&self) -> usize {
        self.store.active_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[test]
    fn test_acquire_and_release() {
        let mgr = InodeLeaseManager::new();
        let inode = 12345u64;

        // Acquire
        let result = mgr.acquire(inode, "client-A", 30000).unwrap();
        assert!(!result.token.is_empty());

        // Same client re-acquire (idempotent)
        let result2 = mgr.acquire(inode, "client-A", 30000).unwrap();
        assert_eq!(result.token, result2.token);

        // Different client acquire → fail
        let err = mgr.acquire(inode, "client-B", 30000).unwrap_err();
        assert!(err.contains("held by another client"));

        // Release
        mgr.release(inode, "client-A", &result.token).unwrap();

        // Now client-B can acquire
        let result3 = mgr.acquire(inode, "client-B", 30000).unwrap();
        assert_ne!(result3.token, result.token);
    }

    #[test]
    fn test_grace_period() {
        let mgr = InodeLeaseManager::with_grace_period(100);
        let inode = 999u64;

        // Acquire with 50ms duration, 100ms grace
        let result = mgr.acquire(inode, "client-A", 50).unwrap();

        // Wait for expiry but within grace
        std::thread::sleep(Duration::from_millis(60));

        // Expired but in grace → different client cannot acquire
        let err = mgr.acquire(inode, "client-B", 50).unwrap_err();
        assert!(err.contains("grace period"));

        // Same holder can still renew within grace
        mgr.renew(inode, "client-A", &result.token, 50).unwrap();

        // Wait past grace
        std::thread::sleep(Duration::from_millis(160));

        // Now different client can acquire (past grace, cleanup)
        mgr.acquire(inode, "client-B", 50).unwrap();
    }

    #[test]
    fn test_validate() {
        let mgr = InodeLeaseManager::new();
        let inode = 777u64;

        let result = mgr.acquire(inode, "client-A", 30000).unwrap();

        // Valid validation
        mgr.validate(inode, "client-A", &result.token).unwrap();

        // Wrong holder
        let err = mgr.validate(inode, "client-B", &result.token).unwrap_err();
        assert!(err.contains("holder mismatch"));

        // Wrong token
        let err = mgr.validate(inode, "client-A", "wrong-token").unwrap_err();
        assert!(err.contains("token mismatch"));

        // Non-existent inode
        let err = mgr.validate(888, "client-A", "any").unwrap_err();
        assert!(err.contains("not found"));
    }

    #[test]
    fn test_disconnect_holder() {
        let mgr = InodeLeaseManager::new();

        mgr.acquire(1, "client-A", 30000).unwrap();
        mgr.acquire(2, "client-A", 30000).unwrap();
        mgr.acquire(3, "client-B", 30000).unwrap();

        let removed = mgr.disconnect_holder("client-A");
        assert_eq!(removed, 2);

        // client-B's lease still active
        assert_eq!(mgr.active_count(), 1);
    }

    #[test]
    fn test_renew_wrong_holder() {
        let mgr = InodeLeaseManager::new();
        let inode = 555u64;

        let result = mgr.acquire(inode, "client-A", 30000).unwrap();

        // Wrong holder cannot renew
        let err = mgr
            .renew(inode, "client-B", &result.token, 30000)
            .unwrap_err();
        assert!(err.contains("holder mismatch"));
    }

    // =====================================================================
    // Concurrent tests — verify mutual exclusion under real thread contention
    // =====================================================================

    /// 多线程并发获取同一 inode 的 lease。
    /// 验证:只有一个线程成功获取,其余全部失败("held by another client")。
    #[test]
    fn test_concurrent_acquire_same_inode_mutual_exclusion() {
        let mgr = Arc::new(InodeLeaseManager::new());
        let inode = 42_000u64;
        let num_threads = 16;

        let barrier = Arc::new(std::sync::Barrier::new(num_threads));
        let success_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let fail_count = Arc::new(std::sync::atomic::AtomicU32::new(0));

        let mut handles = Vec::new();
        for i in 0..num_threads {
            let mgr = mgr.clone();
            let barrier = barrier.clone();
            let success_count = success_count.clone();
            let fail_count = fail_count.clone();
            handles.push(std::thread::spawn(move || {
                let client_id = format!("client-{}", i);
                barrier.wait(); // 所有线程同时开始竞争

                match mgr.acquire(inode, &client_id, 5_000) {
                    Ok(_) => {
                        success_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                    Err(e) => {
                        assert!(
                            e.contains("held by another client"),
                            "unexpected error: {}",
                            e
                        );
                        fail_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(
            success_count.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "exactly one thread should acquire the lease"
        );
        assert_eq!(
            fail_count.load(std::sync::atomic::Ordering::SeqCst),
            (num_threads - 1) as u32,
            "remaining threads should fail with 'held by another client'"
        );
    }

    /// 多线程并发获取不同 inode 的 lease。
    /// 验证:全部成功,互不干扰。
    #[test]
    fn test_concurrent_acquire_different_inodes_no_contention() {
        let mgr = Arc::new(InodeLeaseManager::new());
        let num_threads = 16;

        let barrier = Arc::new(std::sync::Barrier::new(num_threads));
        let success_count = Arc::new(std::sync::atomic::AtomicU32::new(0));

        let mut handles = Vec::new();
        for i in 0..num_threads {
            let mgr = mgr.clone();
            let barrier = barrier.clone();
            let success_count = success_count.clone();
            handles.push(std::thread::spawn(move || {
                let client_id = format!("client-{}", i);
                let inode = 100_000 + i as u64; // 每个线程不同 inode
                barrier.wait();

                match mgr.acquire(inode, &client_id, 5_000) {
                    Ok(result) => {
                        assert!(!result.token.is_empty());
                        success_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                    Err(e) => panic!("acquire for different inode should not fail: {}", e),
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(
            success_count.load(std::sync::atomic::Ordering::SeqCst),
            num_threads as u32,
            "all threads should succeed for different inodes"
        );
    }

    /// 持有者释放 lease 后,等待的线程可以获取。
    /// 验证:release → acquire 的交接在并发环境下正常工作。
    #[test]
    fn test_concurrent_release_then_acquire() {
        let mgr = Arc::new(InodeLeaseManager::new());
        let inode = 77_777u64;

        // client-A 先获取 lease
        let result_a = mgr.acquire(inode, "client-A", 10_000).unwrap();
        assert!(!result_a.token.is_empty());

        let mgr_clone = mgr.clone();
        let token = result_a.token.clone();
        let acquire_done = Arc::new(std::sync::Mutex::new(false));
        let acquire_done_clone = acquire_done.clone();

        // client-B 在后台尝试获取,不断重试
        let handle = std::thread::spawn(move || {
            for _ in 0..50 {
                if mgr_clone.acquire(inode, "client-B", 5_000).is_ok() {
                    *acquire_done_clone.lock().unwrap() = true;
                    return;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            panic!("client-B failed to acquire after 50 retries");
        });

        // 等一下让 client-B 开始重试
        std::thread::sleep(Duration::from_millis(50));

        // client-A 释放 lease
        mgr.release(inode, "client-A", &token).unwrap();

        // client-B 应该能获取到
        handle.join().unwrap();
        assert!(
            *acquire_done.lock().unwrap(),
            "client-B should have acquired the lease after release"
        );
    }

    /// 持有者持续续租,其他客户端无法获取。
    /// 验证:renew 延长 lease 有效期,阻止其他客户端 acquire。
    #[test]
    fn test_concurrent_renew_blocks_other_clients() {
        let mgr = Arc::new(InodeLeaseManager::new());
        let inode = 88_888u64;

        // client-A 获取短期 lease (200ms)
        let result_a = mgr.acquire(inode, "client-A", 200).unwrap();

        let mgr_clone = mgr.clone();
        let token = result_a.token.clone();
        let token_for_release = result_a.token.clone();
        let stop_renew = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_renew_clone = stop_renew.clone();

        // client-A 后台持续续租
        let renew_handle = std::thread::spawn(move || {
            while !stop_renew_clone.load(std::sync::atomic::Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(50));
                if let Err(e) = mgr_clone.renew(inode, "client-A", &token, 200) {
                    // 如果续租失败(可能已被清理),退出
                    eprintln!("renew failed: {}", e);
                    break;
                }
            }
        });

        // 等待 300ms(超过原始 lease 时效,但续租应该保持了有效性)
        std::thread::sleep(Duration::from_millis(300));

        // client-B 尝试获取 — 应该失败(client-A 仍在续租)
        let err = mgr.acquire(inode, "client-B", 200).unwrap_err();
        assert!(
            err.contains("held by another client") || err.contains("grace period"),
            "client-B should be blocked while client-A is renewing, got: {}",
            err
        );

        // 停止续租,等待 lease 过期 + grace period
        stop_renew.store(true, std::sync::atomic::Ordering::SeqCst);
        renew_handle.join().unwrap();

        // grace period = 5000ms (default),但我们等不了那么久
        // 直接释放,验证 release 后 client-B 能获取
        mgr.release(inode, "client-A", &token_for_release).unwrap();
        mgr.acquire(inode, "client-B", 200).unwrap();
    }

    /// 同一客户端并发获取同一 inode(幂等)。
    /// 验证:同一 holder 的并发 acquire 返回相同 token。
    #[test]
    fn test_concurrent_same_client_idempotent() {
        let mgr = Arc::new(InodeLeaseManager::new());
        let inode = 99_999u64;
        let num_threads = 8;

        let barrier = Arc::new(std::sync::Barrier::new(num_threads));
        let tokens = Arc::new(std::sync::Mutex::new(Vec::new()));

        let mut handles = Vec::new();
        for _ in 0..num_threads {
            let mgr = mgr.clone();
            let barrier = barrier.clone();
            let tokens = tokens.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                let result = mgr.acquire(inode, "client-same", 5_000).unwrap();
                tokens.lock().unwrap().push(result.token);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let tokens = tokens.lock().unwrap();
        assert_eq!(tokens.len(), num_threads);
        // 所有 token 应该相同(同一 holder 幂等)
        let first = &tokens[0];
        assert!(
            tokens.iter().all(|t| t == first),
            "same client should get the same token (idempotent), got: {:?}",
            *tokens
        );
    }

    /// 并发 disconnect_holder 在其他线程 acquire 时安全执行。
    /// 验证:disconnect 释放的 lease 可以被其他线程立即获取。
    #[test]
    fn test_concurrent_disconnect_and_acquire() {
        let mgr = Arc::new(InodeLeaseManager::new());
        let inode = 111_111u64;

        // client-A 持有多个 inode 的 lease
        mgr.acquire(inode, "client-A", 5_000).unwrap();
        mgr.acquire(inode + 1, "client-A", 5_000).unwrap();
        mgr.acquire(inode + 2, "client-A", 5_000).unwrap();

        let mgr_clone = mgr.clone();
        let disconnect_handle = std::thread::spawn(move || mgr_clone.disconnect_holder("client-A"));

        let mgr_clone2 = mgr.clone();
        let acquire_handle = std::thread::spawn(move || {
            // 在 disconnect 后尝试获取
            std::thread::sleep(Duration::from_millis(10));
            mgr_clone2.acquire(inode, "client-B", 5_000)
        });

        let removed = disconnect_handle.join().unwrap();
        let acquire_result = acquire_handle.join().unwrap();

        assert_eq!(removed, 3, "disconnect should release 3 leases");
        assert!(
            acquire_result.is_ok(),
            "client-B should acquire after disconnect"
        );
    }

    // =====================================================================
    // InodeKey unit tests
    // =====================================================================

    #[test]
    fn test_inode_key_encode_decode_roundtrip() {
        let key = InodeKey::new(42);
        let bytes = key.encode();
        assert_eq!(bytes.len(), 8);
        let decoded = InodeKey::decode(&bytes).unwrap();
        assert_eq!(decoded, key);
    }

    #[test]
    fn test_inode_key_decode_too_short() {
        let result = InodeKey::decode(&[1, 2, 3]);
        assert!(result.is_err());
    }

    #[test]
    fn test_inode_key_conflicts_same_inode() {
        let a = InodeKey::new(100);
        let b = InodeKey::new(100);
        let c = InodeKey::new(200);
        assert!(a.conflicts(&b));
        assert!(!a.conflicts(&c));
    }

    #[test]
    fn test_inode_key_group_id() {
        let key = InodeKey::new(123);
        assert_eq!(key.group_id(), 123);
    }

    // =====================================================================
    // Persistence integration tests (LeasePersistence trait)
    // =====================================================================

    #[test]
    fn test_with_persistence_roundtrip() {
        // Acquire a lease on manager A (with persistence), then create
        // manager B from the same backend and load — the lease should be
        // recovered.
        let backend = Arc::new(InMemoryPersistence::new());
        let mgr_a = InodeLeaseManager::new().with_persistence(PersistenceShim(backend.clone()));
        let inode = 42_042u64;

        let result = mgr_a.acquire(inode, "client-A", 30_000).unwrap();
        assert!(!result.token.is_empty());
        assert_eq!(mgr_a.active_count(), 1);
        assert_eq!(backend.count(), 1);

        // New manager sharing the same backend — should load the lease.
        let mgr_b = InodeLeaseManager::new().with_persistence(PersistenceShim(backend.clone()));
        let loaded = mgr_b.load_from_persistence().unwrap();
        assert_eq!(loaded, 1);
        assert_eq!(mgr_b.active_count(), 1);

        // The recovered lease should validate.
        mgr_b.validate(inode, "client-A", &result.token).unwrap();
    }

    #[test]
    fn test_persistence_release_deletes_backend_entry() {
        let backend = Arc::new(InMemoryPersistence::new());
        let mgr = InodeLeaseManager::new().with_persistence(PersistenceShim(backend.clone()));

        let inode = 7u64;
        let result = mgr.acquire(inode, "client-A", 30_000).unwrap();
        assert_eq!(backend.count(), 1);

        mgr.release(inode, "client-A", &result.token).unwrap();
        assert_eq!(backend.count(), 0);
    }

    #[test]
    fn test_persistence_renew_updates_backend() {
        let backend = Arc::new(InMemoryPersistence::new());
        let mgr = InodeLeaseManager::new().with_persistence(PersistenceShim(backend.clone()));

        let inode = 99u64;
        let result = mgr.acquire(inode, "client-A", 1_000).unwrap();
        assert_eq!(backend.count(), 1);

        mgr.renew(inode, "client-A", &result.token, 5_000).unwrap();
        // Same token, still one entry, but its serialized form has updated.
        assert_eq!(backend.count(), 1);
    }

    // =====================================================================
    // Phase 5 §5.3: Raft lease persistence — leader switch replay tests
    // =====================================================================
    //
    // These tests use the `InMemoryPersistence` mock to simulate a Raft-
    // replicated backend. The real `RaftLeasePersistence` round-trips
    // through Raft + RocksDB `CF_LEASES`; that path is exercised by the
    // filer integration tests. Here we verify the manager-level contract:
    //   1. Leases granted by the old leader survive a leader switch and
    //      are honored by the new leader (no client disruption).
    //   2. The Fencer epoch counter is strictly monotonic across leader
    //      switches (no ABA reuse — zombie old leader can't fool the new
    //      leader with a stale epoch).
    //   3. Expired leases are filtered out during recovery — only
    //      still-live leases are reloaded.

    /// Parse the Fencer epoch out of a lease token. The token format is
    /// `lease-{epoch}-{uuid}` (see `MemoryLeaseStore::generate_token`);
    /// `splitn(3, '-')` keeps the UUID's embedded dashes in the third part.
    fn epoch_from_token(token: &str) -> u64 {
        let parts: Vec<&str> = token.splitn(3, '-').collect();
        assert_eq!(parts.len(), 3, "token should be lease-{{epoch}}-{{uuid}}");
        parts[1].parse::<u64>().expect("epoch should be numeric")
    }

    /// Simulate a Filer leader switch: the old leader (`mgr_a`) grants
    /// several leases and persists them; the new leader (`mgr_b`) loads
    /// them from the shared backend on takeover and must honor every one
    /// — no client-visible disruption, no double-grant (the recovered
    /// lease blocks conflicting acquires from a different holder). This
    /// is the core correctness property of phase 5 §5.3.
    #[test]
    fn test_persistence_leader_switch_replay() {
        let backend = Arc::new(InMemoryPersistence::new());
        let mgr_a = InodeLeaseManager::new().with_persistence(PersistenceShim(backend.clone()));

        // Grant leases for several inodes to different clients.
        let inode1 = 1_000u64;
        let inode2 = 2_000u64;
        let inode3 = 3_000u64;

        let r1 = mgr_a.acquire(inode1, "client-A", 30_000).unwrap();
        let r2 = mgr_a.acquire(inode2, "client-B", 30_000).unwrap();
        let r3 = mgr_a.acquire(inode3, "client-C", 30_000).unwrap();
        assert_eq!(backend.count(), 3);
        assert_eq!(mgr_a.active_count(), 3);

        // --- Leader switch: discard mgr_a, build mgr_b from the same backend.
        // A real leader switch creates a fresh in-memory store on the new
        // leader and repopulates it from the Raft-replicated persistence.
        let mgr_b = InodeLeaseManager::new().with_persistence(PersistenceShim(backend.clone()));
        let loaded = mgr_b.load_from_persistence().unwrap();
        assert_eq!(loaded, 3, "all 3 leases should survive leader switch");
        assert_eq!(mgr_b.active_count(), 3);

        // The new leader honors every recovered lease — holders can still
        // validate against the same token they were granted.
        mgr_b.validate(inode1, "client-A", &r1.token).unwrap();
        mgr_b.validate(inode2, "client-B", &r2.token).unwrap();
        mgr_b.validate(inode3, "client-C", &r3.token).unwrap();

        // Double-write prevention: a different client cannot acquire an
        // inode whose lease was recovered from the old leader. This is
        // the "double-write consistency" hole phase 5 §5.3 closes.
        let err = mgr_b.acquire(inode1, "client-X", 30_000).unwrap_err();
        assert!(
            err.contains("lease held by another client"),
            "recovered lease should block conflicting acquire, got: {}",
            err
        );

        // The original holder can still re-acquire (idempotent path returns
        // the same token — no disruption to the active writer).
        let r1_again = mgr_b.acquire(inode1, "client-A", 30_000).unwrap();
        assert_eq!(
            r1_again.token, r1.token,
            "idempotent re-acquire returns same token"
        );
    }

    /// The Fencer epoch counter (powerfs-lock-health) must be strictly
    /// monotonic across leader switches: a zombie old leader must not be
    /// able to reuse a stale epoch to fool the new leader into accepting
    /// a stale token. `load_from_persistence` reseeds the epoch counter
    /// to `max(persisted, recovered-entry-epoch) + 1`, so the new leader's
    /// next grant uses a strictly higher epoch than any prior grant.
    #[test]
    fn test_persistence_epoch_survives_leader_switch() {
        let backend = Arc::new(InMemoryPersistence::new());
        let mgr_a = InodeLeaseManager::new().with_persistence(PersistenceShim(backend.clone()));

        // Grant several leases on the old leader — each grant bumps the
        // epoch counter (token format: "lease-{epoch}-{uuid}").
        let r1 = mgr_a.acquire(10, "client-A", 30_000).unwrap();
        let r2 = mgr_a.acquire(20, "client-B", 30_000).unwrap();
        let r3 = mgr_a.acquire(30, "client-C", 30_000).unwrap();

        let epoch1 = epoch_from_token(&r1.token);
        let epoch2 = epoch_from_token(&r2.token);
        let epoch3 = epoch_from_token(&r3.token);
        // Epochs are allocated sequentially starting at 0.
        let max_old_epoch = epoch1.max(epoch2).max(epoch3);
        assert_eq!(epoch1, 0);
        assert_eq!(epoch2, 1);
        assert_eq!(epoch3, 2);

        // Old leader persists its epoch counter to the backend. The
        // persisted value is the current counter (one past the last grant).
        mgr_a.persist_epoch().unwrap();
        let persisted_epoch = backend.load_epoch().unwrap();
        assert_eq!(
            persisted_epoch, 3,
            "persisted epoch counter should be one past the last granted epoch"
        );

        // --- Leader switch: new leader loads epoch + leases from backend ---
        let mgr_b = InodeLeaseManager::new().with_persistence(PersistenceShim(backend.clone()));
        mgr_b.load_from_persistence().unwrap();

        // New grant on the new leader must use an epoch strictly greater
        // than any epoch the old leader ever granted — no ABA reuse.
        let r4 = mgr_b.acquire(40, "client-D", 30_000).unwrap();
        let epoch4 = epoch_from_token(&r4.token);
        assert!(
            epoch4 > max_old_epoch,
            "new leader epoch {} must be > old max epoch {} (fence token ABA safety)",
            epoch4,
            max_old_epoch
        );
    }

    /// `decode_entry` skips entries whose `expire_at` is already in the
    /// past, so a leader takeover after some leases have naturally expired
    /// recovers only the still-live ones. Expired entries are also deleted
    /// from the backend during load (best-effort cleanup).
    #[test]
    fn test_persistence_expired_leases_filtered_on_load() {
        let backend = Arc::new(InMemoryPersistence::new());
        let mgr_a = InodeLeaseManager::new().with_persistence(PersistenceShim(backend.clone()));

        // One short-lived lease + one long-lived lease.
        let short = mgr_a.acquire(5, "client-A", 1).unwrap(); // 1ms TTL
        let long = mgr_a.acquire(6, "client-B", 30_000).unwrap(); // 30s TTL
        assert_eq!(backend.count(), 2);

        // Wait long enough for the short lease to be genuinely expired.
        // `decode_entry` only checks `Instant::now() > expire_at`; no
        // grace-period wait is needed.
        std::thread::sleep(Duration::from_millis(50));

        // --- Leader switch: load only non-expired leases ---
        let mgr_b = InodeLeaseManager::new().with_persistence(PersistenceShim(backend.clone()));
        let loaded = mgr_b.load_from_persistence().unwrap();
        assert_eq!(loaded, 1, "only the non-expired lease should be recovered");
        assert_eq!(mgr_b.active_count(), 1);

        // The long lease is honored; the short one is gone.
        mgr_b.validate(6, "client-B", &long.token).unwrap();
        let short_validate = mgr_b.validate(5, "client-A", &short.token);
        assert!(
            short_validate.is_err(),
            "expired lease should not be recoverable"
        );
    }

    #[test]
    fn test_stats_counters_visible() {
        let mgr = InodeLeaseManager::new();
        let s0 = mgr.stats();
        assert_eq!(s0.active_count, 0);
        assert_eq!(s0.acquire_total, 0);

        mgr.acquire(1, "client-A", 1_000).unwrap();
        let s1 = mgr.stats();
        assert_eq!(s1.active_count, 1);
        assert_eq!(s1.acquire_total, 1);
        assert_eq!(s1.active_holders, 1);

        // Conflict counts as an acquire_total too.
        let _ = mgr.acquire(1, "client-B", 1_000).unwrap_err();
        let s2 = mgr.stats();
        assert_eq!(s2.acquire_total, 2);
        assert_eq!(s2.acquire_conflict_total, 1);
    }

    /// A minimal in-memory `LeasePersistence` implementation for testing
    /// the manager's persistence wiring. The actual Raft-backed impl is
    /// added in the optimization phase.
    #[derive(Default)]
    struct InMemoryPersistence {
        entries: Mutex<HashMap<String, Vec<u8>>>,
        epoch: Mutex<u64>,
    }

    impl InMemoryPersistence {
        fn new() -> Self {
            Self::default()
        }

        fn count(&self) -> usize {
            self.entries.lock().unwrap().len()
        }
    }

    impl LeasePersistence for InMemoryPersistence {
        fn save(&self, token: &str, data: &[u8]) -> Result<(), LeaseError> {
            self.entries
                .lock()
                .unwrap()
                .insert(token.to_string(), data.to_vec());
            Ok(())
        }

        fn delete(&self, token: &str) -> Result<(), LeaseError> {
            self.entries.lock().unwrap().remove(token);
            Ok(())
        }

        fn load_all(&self) -> Result<Vec<(String, Vec<u8>)>, LeaseError> {
            Ok(self
                .entries
                .lock()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect())
        }

        fn save_epoch(&self, epoch: u64) -> Result<(), LeaseError> {
            *self.epoch.lock().unwrap() = epoch;
            Ok(())
        }

        fn load_epoch(&self) -> Result<u64, LeaseError> {
            Ok(*self.epoch.lock().unwrap())
        }
    }

    /// Newtype shim that lets `Arc<InMemoryPersistence>` satisfy
    /// `LeasePersistence` (since `Arc<T>` doesn't auto-impl user traits).
    struct PersistenceShim(Arc<InMemoryPersistence>);

    impl LeasePersistence for PersistenceShim {
        fn save(&self, token: &str, data: &[u8]) -> Result<(), LeaseError> {
            self.0.save(token, data)
        }
        fn delete(&self, token: &str) -> Result<(), LeaseError> {
            self.0.delete(token)
        }
        fn load_all(&self) -> Result<Vec<(String, Vec<u8>)>, LeaseError> {
            self.0.load_all()
        }
        fn save_epoch(&self, epoch: u64) -> Result<(), LeaseError> {
            self.0.save_epoch(epoch)
        }
        fn load_epoch(&self) -> Result<u64, LeaseError> {
            self.0.load_epoch()
        }
    }

    // =====================================================================
    // Early Grant + Early Revoke + SN integration tests (phase 4 §5.2)
    // =====================================================================

    /// A `LeaseRevoker` test double that captures every `revoke` call
    /// so tests can assert the Early Revoke was pushed (and how many
    /// times). Records the `(inode, token, holder)` triple per call.
    #[derive(Default)]
    struct CapturingRevoker {
        calls: Mutex<Vec<(u64, String, String)>>,
    }

    impl CapturingRevoker {
        fn call_count(&self) -> usize {
            self.calls.lock().unwrap().len()
        }

        fn calls(&self) -> Vec<(u64, String, String)> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl crate::early_grant::LeaseRevoker for CapturingRevoker {
        fn revoke(&self, inode: u64, token: &str, holder: &str) -> Result<(), String> {
            self.calls
                .lock()
                .unwrap()
                .push((inode, token.to_string(), holder.to_string()));
            Ok(())
        }
    }

    /// Newtype shim so `Arc<CapturingRevoker>` satisfies `LeaseRevoker`.
    /// (`Arc<T>` doesn't auto-impl user traits, so we wrap it.)
    struct RevokerShim(Arc<CapturingRevoker>);

    impl crate::early_grant::LeaseRevoker for RevokerShim {
        fn revoke(&self, inode: u64, token: &str, holder: &str) -> Result<(), String> {
            self.0.revoke(inode, token, holder)
        }
    }

    /// Helper: build a manager wired with a shared `CapturingRevoker`.
    fn make_early_grant_mgr() -> (InodeLeaseManager, Arc<CapturingRevoker>) {
        let revoker = Arc::new(CapturingRevoker::default());
        let mgr = InodeLeaseManager::new().with_revoker(RevokerShim(revoker.clone()));
        (mgr, revoker)
    }

    #[tokio::test]
    async fn test_acquire_or_wait_granted_on_no_conflict() {
        let mgr = InodeLeaseManager::new();
        let inode = 10_001u64;

        // No existing holder → immediate grant.
        let outcome = mgr.acquire_or_wait(inode, "client-A", 30_000);
        assert!(outcome.is_granted(), "no conflict → Granted");
        if let crate::early_grant::AcquireOutcome::Granted(r) = outcome {
            assert!(r.sn > 0, "fresh grant must allocate SN > 0");
            assert!(!r.token.is_empty());
        }
        assert!(!mgr.has_queued_waiters(), "no waiters queued on grant");
    }

    #[tokio::test]
    async fn test_acquire_or_wait_queued_on_conflict() {
        let mgr = InodeLeaseManager::new();
        let inode = 10_002u64;

        // client-A holds the lease.
        let first = mgr.acquire(inode, "client-A", 30_000).unwrap();

        // client-B conflicts → Queued.
        let outcome = mgr.acquire_or_wait(inode, "client-B", 30_000);
        assert!(outcome.is_queued(), "conflict → Queued");
        assert!(mgr.has_queued_waiters(), "waiter must be queued");
        assert_eq!(mgr.waiter_count(inode), 1);

        // NoopRevoker was used → the receiver should still be pending
        // (holder hasn't ACK'd).
        if let crate::early_grant::AcquireOutcome::Queued(rx) = outcome {
            assert!(rx.is_empty(), "receiver must be pending until Early Grant");
        }

        // Cleanup.
        mgr.release(inode, "client-A", &first.token).unwrap();
    }

    #[tokio::test]
    async fn test_early_grant_on_revoke_ack() {
        let (mgr, _revoker) = make_early_grant_mgr();
        let inode = 10_003u64;

        // client-A holds the lease.
        let first = mgr.acquire(inode, "client-A", 30_000).unwrap();
        assert!(first.sn > 0);

        // client-B conflicts → Queued.
        let outcome = mgr.acquire_or_wait(inode, "client-B", 30_000);
        assert!(outcome.is_queued());

        // Simulate client-A's RevokeAck (flushed + releasing).
        mgr.handle_revoke_ack(inode, &first.token, "client-A")
            .expect("revoke-ack must succeed");

        // client-B's receiver should now be fulfilled (Early Grant).
        if let crate::early_grant::AcquireOutcome::Queued(rx) = outcome {
            let grant = tokio::time::timeout(Duration::from_millis(500), rx)
                .await
                .expect("receiver must resolve after Early Grant")
                .expect("sender must not be dropped");

            // The new grant must carry a strictly higher SN (SN barrier
            // preserves IO ordering across the handoff).
            assert!(
                grant.sn > first.sn,
                "new grant SN={} must exceed old SN={}",
                grant.sn,
                first.sn
            );
            assert!(!grant.token.is_empty());
        }

        // Queue must be drained.
        assert!(!mgr.has_queued_waiters());
        assert_eq!(mgr.waiter_count(inode), 0);
    }

    #[tokio::test]
    async fn test_early_revoke_pushed_only_for_first_waiter() {
        let (mgr, revoker) = make_early_grant_mgr();
        let inode = 10_004u64;

        // client-A holds the lease.
        let first = mgr.acquire(inode, "client-A", 30_000).unwrap();

        // client-B queues (first waiter → Early Revoke pushed to A).
        let outcome_b = mgr.acquire_or_wait(inode, "client-B", 30_000);
        assert!(outcome_b.is_queued());
        assert_eq!(revoker.call_count(), 1, "first waiter triggers revoke");

        let pushed = revoker.calls();
        assert_eq!(pushed[0].0, inode, "revoke targets the right inode");
        assert_eq!(pushed[0].2, "client-A", "revoke addresses the holder");

        // client-C queues (second waiter → no second revoke).
        let outcome_c = mgr.acquire_or_wait(inode, "client-C", 30_000);
        assert!(outcome_c.is_queued());
        assert_eq!(
            revoker.call_count(),
            1,
            "second waiter must NOT trigger a redundant revoke"
        );

        assert_eq!(mgr.waiter_count(inode), 2);

        // Cleanup: ACK from client-A grants client-B (FIFO), leaving
        // client-C still queued.
        mgr.handle_revoke_ack(inode, &first.token, "client-A")
            .unwrap();
        assert_eq!(mgr.waiter_count(inode), 1, "FIFO grants the head waiter");
    }

    #[tokio::test]
    async fn test_revoke_ack_no_waiter_just_releases() {
        let mgr = InodeLeaseManager::new();
        let inode = 10_005u64;

        // client-A holds the lease; no one is queued.
        let first = mgr.acquire(inode, "client-A", 30_000).unwrap();
        assert!(!mgr.has_queued_waiters());

        // RevokeAck with no waiter → just release, no Early Grant.
        mgr.handle_revoke_ack(inode, &first.token, "client-A")
            .expect("revoke-ack with no waiter must succeed");

        // Inode is now free; client-B can acquire immediately.
        assert!(!mgr.has_queued_waiters());
        let next = mgr.acquire(inode, "client-B", 30_000).unwrap();
        assert!(next.sn > first.sn);
    }

    #[test]
    fn test_sn_zero_on_idempotent_reacquire() {
        let mgr = InodeLeaseManager::new();
        let inode = 10_006u64;

        // First acquire allocates SN.
        let first = mgr.acquire(inode, "client-A", 30_000).unwrap();
        assert!(first.sn > 0, "fresh grant allocates SN");

        // Idempotent re-acquire reuses the token and returns SN=0
        // (no new SN allocated — the caller reuses the original SN).
        let reacq = mgr.acquire(inode, "client-A", 30_000).unwrap();
        assert_eq!(
            first.token, reacq.token,
            "idempotent re-acquire returns the same token"
        );
        assert_eq!(
            reacq.sn, 0,
            "idempotent re-acquire must NOT allocate a new SN"
        );
    }

    #[tokio::test]
    async fn test_early_grant_fifo_ordering() {
        // Three clients queue for one inode; verify FIFO grant order.
        let (mgr, _revoker) = make_early_grant_mgr();
        let inode = 10_007u64;

        let first = mgr.acquire(inode, "client-A", 30_000).unwrap();

        let outcome_b = mgr.acquire_or_wait(inode, "client-B", 30_000);
        let outcome_c = mgr.acquire_or_wait(inode, "client-C", 30_000);
        let outcome_d = mgr.acquire_or_wait(inode, "client-D", 30_000);
        assert_eq!(mgr.waiter_count(inode), 3);

        // client-A ACKs → client-B gets the grant (FIFO head).
        mgr.handle_revoke_ack(inode, &first.token, "client-A")
            .unwrap();
        let grant_b = tokio::time::timeout(
            Duration::from_millis(500),
            match outcome_b {
                crate::early_grant::AcquireOutcome::Queued(rx) => rx,
                _ => panic!("B must be queued"),
            },
        )
        .await
        .expect("B must be granted")
        .expect("B sender alive");

        // client-B ACKs → client-C gets the grant.
        mgr.handle_revoke_ack(inode, &grant_b.token, "client-B")
            .unwrap();
        let grant_c = tokio::time::timeout(
            Duration::from_millis(500),
            match outcome_c {
                crate::early_grant::AcquireOutcome::Queued(rx) => rx,
                _ => panic!("C must be queued"),
            },
        )
        .await
        .expect("C must be granted")
        .expect("C sender alive");

        // client-C ACKs → client-D gets the grant.
        mgr.handle_revoke_ack(inode, &grant_c.token, "client-C")
            .unwrap();
        let grant_d = tokio::time::timeout(
            Duration::from_millis(500),
            match outcome_d {
                crate::early_grant::AcquireOutcome::Queued(rx) => rx,
                _ => panic!("D must be queued"),
            },
        )
        .await
        .expect("D must be granted")
        .expect("D sender alive");

        // SNs must be strictly increasing across the FIFO handoff.
        assert!(grant_b.sn > first.sn);
        assert!(grant_c.sn > grant_b.sn);
        assert!(grant_d.sn > grant_c.sn);

        assert!(!mgr.has_queued_waiters());
    }

    #[tokio::test]
    async fn test_acquire_or_wait_error_on_grace_period() {
        let mgr = InodeLeaseManager::with_grace_period(100);
        let inode = 10_008u64;

        // Acquire with short TTL, then wait into grace period.
        let first = mgr.acquire(inode, "client-A", 50).unwrap();
        std::thread::sleep(Duration::from_millis(60));

        // client-B conflicts AND the lease is in grace → Error (not
        // queued). The wait-queue path is for live conflicts only.
        let outcome = mgr.acquire_or_wait(inode, "client-B", 50);
        assert!(
            outcome.is_error(),
            "grace-period rejection must surface as Error, not Queued"
        );

        // Cleanup.
        mgr.release(inode, "client-A", &first.token).ok();
    }

    /// Phase-4 P4: `acquire_ordered` should return results in the input
    /// order regardless of how inodes were sorted internally. Verifies
    /// the index-preserving sort.
    #[test]
    fn test_acquire_ordered_preserves_input_order() {
        let mgr = InodeLeaseManager::new();
        // Input in descending order on purpose.
        let inodes = vec![30u64, 10, 20];

        let results = mgr.acquire_ordered(&inodes, "client-A", 30000).unwrap();
        assert_eq!(results.len(), 3);

        // Results map back to input positions: result[i] corresponds
        // to inodes[i]. We verify by re-acquiring each inode
        // individually and asserting the token matches the i-th
        // result (idempotent re-acquire hits the same lease entry).
        let mut tokens = std::collections::HashSet::new();
        for (i, r) in results.iter().enumerate() {
            assert!(!r.token.is_empty(), "token {} empty", i);
            assert!(tokens.insert(r.token.clone()), "token {} not unique", i);
            let again = mgr.acquire(inodes[i], "client-A", 30000).unwrap();
            assert_eq!(again.token, r.token, "result {} not idempotent", i);
        }

        // Cleanup: release each (inode, token) pair.
        for (i, inode) in inodes.iter().enumerate() {
            mgr.release(*inode, "client-A", &results[i].token).ok();
        }
    }

    /// Phase-4 P4: `acquire_ordered` must roll back all previously
    /// acquired leases when a later acquire in the batch fails, so the
    /// caller doesn't leak half-acquired state.
    #[test]
    fn test_acquire_ordered_rollback_on_conflict() {
        let mgr = InodeLeaseManager::new();
        // client-B pre-holds inode=20 so client-A's batch fails at
        // inode=20 (the last in sorted order).
        let held = mgr.acquire(20, "client-B", 30000).unwrap();

        // client-A tries to acquire [30, 10, 20] in one batch. Internally
        // sorted to [10, 20, 30]; acquires 10 (ok), then 20 (conflict)
        // → rollback releases 10.
        let inodes = vec![30u64, 10, 20];
        let err = mgr.acquire_ordered(&inodes, "client-A", 30000).unwrap_err();
        assert!(
            err.contains("rolled back"),
            "expected rollback message, got: {}",
            err
        );

        // inode=10 must be free (rolled back). client-B can acquire it.
        let b10 = mgr.acquire(10, "client-B", 30000).unwrap();
        assert!(!b10.token.is_empty());

        // inode=30 was never reached (acquire failed at 20). Verify
        // client-A doesn't hold it.
        let b30 = mgr.acquire(30, "client-B", 30000).unwrap();
        assert!(!b30.token.is_empty());

        // Cleanup.
        mgr.release(20, "client-B", &held.token).ok();
        mgr.release(10, "client-B", &b10.token).ok();
        mgr.release(30, "client-B", &b30.token).ok();
    }

    /// Phase-4 P4: `acquire_ordered` with empty input returns empty
    /// results (no sort, no acquire).
    #[test]
    fn test_acquire_ordered_empty_input() {
        let mgr = InodeLeaseManager::new();
        let results = mgr.acquire_ordered(&[], "client-A", 30000).unwrap();
        assert!(results.is_empty());
    }

    /// Phase-4 P4: `acquire_ordered` with a single inode returns a
    /// single result (no sort edge case).
    #[test]
    fn test_acquire_ordered_single_inode() {
        let mgr = InodeLeaseManager::new();
        let inode = 42u64;
        let results = mgr.acquire_ordered(&[inode], "client-A", 30000).unwrap();
        assert_eq!(results.len(), 1);
        assert!(!results[0].token.is_empty());
        // Cleanup.
        mgr.release(inode, "client-A", &results[0].token).ok();
    }
}
