//! Early Grant + Early Revoke + SN (phase 4 §5.2 — SeqDLM-style).
//!
//! See `docs/lock-optimization-plan.md` §5.2 "高冲突写吞吐优化":
//!
//! - **Early Revoke**: when a new waiter queues for a contended inode,
//!   the server proactively pushes a `Revoke` notification to the
//!   current holder so it can flush + release early, instead of
//!   waiting for the full TTL + grace.
//! - **Early Grant**: when the holder's `RevokeAck` arrives, the
//!   server immediately grants the next queued waiter — it does NOT
//!   wait for the old holder's dirty pages to finish writing back.
//!   The SN (sequence number) on the grant preserves global IO order;
//!   the old holder's writes are sequenced behind by the SN barrier.
//! - **SN**: leader-local `AtomicU64::fetch_add` (Decision 2, Option B
//!   "异步, 推荐"). The leader grants immediately and batches the
//!   Raft log append asynchronously (10ms window). On leader switch,
//!   uncommitted SN grants roll back and clients retry — the SN
//!   guarantees that any IO performed under a rolled-back grant is
//!   ordered behind the new grant, so there's no consistency violation.
//!
//! # Architecture
//!
//! These types live on the server (filer). The `LeaseRevoker` trait
//! abstracts the push transport so the core logic is testable without
//! a real network. `InodeLeaseManager` owns the wait queue and calls
//! into this module on `acquire_or_wait` / `handle_revoke_ack`.

use crate::inode_lease_manager::AcquireResult;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use tokio::sync::oneshot;

/// Monotonic SN (sequence number) allocator — leader-local, optimistic.
///
/// Phase 4 §5.3 ("Raft 强一致兜底"): the leader allocates SN via
/// `AtomicU64::fetch_add` and grants immediately; the Raft log append
/// is batched asynchronously. This trades a small rollback window on
/// leader switch for ~8-10x write throughput under contention (the
/// Early Grant benefit — no synchronous Raft round-trip per grant).
///
/// Thread-safe: a single `AtomicU64` is the entire allocator. The
/// counter persists across calls but is reset on process restart
/// (recovery from Raft log is the optimization-phase P1 task, not
/// this phase).
#[derive(Debug)]
pub struct SnAllocator {
    counter: AtomicU64,
}

impl SnAllocator {
    /// Construct with the initial SN. Pass the last-applied SN from
    /// the Raft log on leader takeover to avoid reusing SNs across
    /// leader switches (phase-4 §5.3 rollback story).
    pub fn new(initial: u64) -> Self {
        Self {
            counter: AtomicU64::new(initial),
        }
    }

    /// Allocate the next SN. Always returns a value strictly greater
    /// than any previously returned SN on this `SnAllocator` instance.
    pub fn next_sn(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Current high-water SN (the last allocated value, or the initial
    /// value if none has been allocated). Useful for checkpointing the
    /// allocator's state to the Raft log during the async batch window.
    pub fn current_sn(&self) -> u64 {
        self.counter.load(Ordering::Relaxed)
    }

    /// Reset the allocator to a checkpoint value. Called on leader
    /// takeover after replaying the Raft log: the new leader resumes
    /// allocating from the highest SN it recovered.
    pub fn reset_to(&self, sn: u64) {
        self.counter.store(sn, Ordering::Relaxed);
    }
}

impl Default for SnAllocator {
    fn default() -> Self {
        // Start at SN=0 so the first allocated SN is 1 (SN 0 is reserved
        // for "no SN" — see `LockGrant::sn` doc in `powerfs-lock`).
        Self::new(0)
    }
}

/// Server-side trait for pushing Early Revoke notifications to clients.
///
/// Implemented by the net layer (which has access to the transport —
/// ultimately the `CHANNEL_LOCK` logical channel from phase 2). The
/// filer's `InodeLeaseManager` calls `revoke(...)` when a new waiter
/// queues for a contended inode (§5.2 Early Revoke).
///
/// The default impl is a no-op (`NoopRevoker`) so the manager can be
/// constructed without a transport and still function — acquires that
/// conflict return an error (legacy behavior), and the wait queue is
/// never populated.
pub trait LeaseRevoker: Send + Sync {
    /// Push a `Revoke` notification to `holder` for the given
    /// `(inode, token)`. Returns `Ok(())` if the message was queued
    /// for delivery (not necessarily ACK'd). The server will wait for
    /// the holder's `RevokeAck` before granting the next waiter; if
    /// the ACK doesn't arrive within the revocation timeout (§8.3
    /// "Revoke after 2s no ACK"), the server force-reclaims the
    /// lease and penalizes the client's health score.
    fn revoke(&self, inode: u64, token: &str, holder: &str) -> Result<(), String>;
}

/// A `LeaseRevoker` that does nothing — the default when the manager
/// is constructed without a push transport. With this revoker, Early
/// Revoke is effectively disabled: `acquire_or_wait` will still queue
/// waiters but will never notify the holder, so the waiter only
/// succeeds if the holder releases voluntarily or its TTL expires.
#[derive(Debug, Default)]
pub struct NoopRevoker;

impl LeaseRevoker for NoopRevoker {
    fn revoke(&self, _inode: u64, _token: &str, _holder: &str) -> Result<(), String> {
        // Silently succeed — no transport, no notification. The caller
        // (the lease manager) still queues the waiter; it just won't be
        // woken until the holder releases or the TTL expires.
        Ok(())
    }
}

/// A queued acquire request waiting for Early Grant.
///
/// Created by `acquire_or_wait` when an acquire conflicts with an
/// active lease. The waiter holds a `oneshot::Sender` that the server
/// fulfills when it grants the lease (either via Early Grant on
/// RevokeAck, or via TTL expiry + retry).
pub(crate) struct Waiter {
    /// The client that requested the lease.
    pub client_id: String,
    /// Requested lease duration in milliseconds.
    pub duration_ms: u64,
    /// Fulfills the client's wait future with the grant result. If the
    /// sender is dropped (e.g., the client disconnected), `send` will
    /// fail and the queue pop is a no-op.
    pub sender: oneshot::Sender<AcquireResult>,
}

impl Waiter {
    pub fn new(
        client_id: String,
        duration_ms: u64,
        sender: oneshot::Sender<AcquireResult>,
    ) -> Self {
        Self {
            client_id,
            duration_ms,
            sender,
        }
    }
}

/// Per-inode FIFO wait queue for contended acquires.
///
/// `InodeLeaseManager` owns one `WaitQueue` and routes waiters by
/// `inode`. The queue is FIFO so waiters are granted in arrival order
/// (matches §5.2 "排队锁" semantics). Each `inode`'s queue is popped
/// when the holder releases or ACKs a revoke.
#[derive(Default)]
pub(crate) struct WaitQueue {
    queues: Mutex<HashMap<u64, VecDeque<Waiter>>>,
}

impl WaitQueue {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a waiter to the queue for `inode`. Returns the new
    /// queue length (useful for logging / deciding whether to send
    /// an Early Revoke — only the first waiter triggers the revoke).
    pub fn push(&self, inode: u64, waiter: Waiter) -> usize {
        let mut queues = self.queues.lock().unwrap();
        let queue = queues.entry(inode).or_default();
        queue.push_back(waiter);
        queue.len()
    }

    /// Pop the next waiter for `inode`, in FIFO order. Returns `None`
    /// if the queue is empty. The caller (the lease manager) is
    /// responsible for granting the lease and fulfilling the waiter's
    /// `oneshot::Sender`.
    pub fn pop(&self, inode: u64) -> Option<Waiter> {
        let mut queues = self.queues.lock().unwrap();
        let queue = queues.get_mut(&inode)?;
        let waiter = queue.pop_front();
        if queue.is_empty() {
            queues.remove(&inode);
        }
        waiter
    }

    /// Whether the queue for `inode` is non-empty. Used by the lease
    /// manager to decide whether to send an Early Revoke on a new push
    /// — if the queue was already non-empty, the holder has already
    /// been notified and a second revoke would be redundant.
    pub fn has_waiters(&self, inode: u64) -> bool {
        self.queues
            .lock()
            .unwrap()
            .get(&inode)
            .map(|q| !q.is_empty())
            .unwrap_or(false)
    }

    /// Current queue length for `inode`. Used by tests and metrics.
    pub fn len(&self, inode: u64) -> usize {
        self.queues
            .lock()
            .unwrap()
            .get(&inode)
            .map(|q| q.len())
            .unwrap_or(0)
    }

    /// Whether any inode has waiters queued. Used by tests.
    pub fn is_empty(&self) -> bool {
        self.queues.lock().unwrap().values().all(|q| q.is_empty())
    }

    /// Number of distinct inodes with at least one queued waiter.
    /// Used by monitoring/metrics. Currently exercised by tests; will
    /// be called from leader-demotion paths in phase 4 §5.3.
    #[allow(dead_code)]
    pub fn inode_count(&self) -> usize {
        self.queues.lock().unwrap().len()
    }

    /// Remove all waiters for an inode (e.g., on leader demotion /
    /// shutdown). Waiters' `oneshot::Sender`s are dropped, which
    /// causes the corresponding `Receiver::await` to return
    /// `Err(RecvError)` — clients retry on the new leader.
    #[allow(dead_code)]
    pub fn drain_inode(&self, inode: u64) -> usize {
        self.queues
            .lock()
            .unwrap()
            .remove(&inode)
            .map(|q| q.len())
            .unwrap_or(0)
    }

    /// Drain all waiters across all inodes. Used on leader demotion.
    #[allow(dead_code)]
    pub fn drain_all(&self) -> usize {
        let mut queues = self.queues.lock().unwrap();
        let total: usize = queues.values().map(|q| q.len()).sum();
        queues.clear();
        total
    }
}

/// The result of `InodeLeaseManager::acquire_or_wait`.
///
/// Replaces the legacy `Result<AcquireResult, String>` for callers
/// that want the Early Grant fast path. The three variants map to:
/// - `Granted` — no conflict (or idempotent re-acquire); the lease is
///   held immediately and the result carries the allocated SN.
/// - `Queued` — conflict with an active lease; the request has been
///   queued and an Early Revoke has been pushed to the current holder.
///   The caller awaits the receiver to be notified on Early Grant
///   (when the holder ACKs) or on TTL expiry.
/// - `Error` — rejection (grace period, quarantined, etc.).
pub enum AcquireOutcome {
    /// The acquire succeeded immediately.
    Granted(AcquireResult),
    /// The acquire conflicted; the request is queued and the caller
    /// should await the receiver for the grant notification.
    Queued(oneshot::Receiver<AcquireResult>),
    /// The acquire was rejected (grace period, quarantined, etc.).
    Error(String),
}

impl AcquireOutcome {
    /// Convenience: whether this is the `Granted` variant.
    pub fn is_granted(&self) -> bool {
        matches!(self, AcquireOutcome::Granted(_))
    }

    /// Convenience: whether this is the `Queued` variant.
    pub fn is_queued(&self) -> bool {
        matches!(self, AcquireOutcome::Queued(_))
    }

    /// Convenience: whether this is the `Error` variant.
    pub fn is_error(&self) -> bool {
        matches!(self, AcquireOutcome::Error(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_sn_allocator_monotonic() {
        let sn = SnAllocator::default();
        assert_eq!(sn.current_sn(), 0);
        assert_eq!(sn.next_sn(), 1);
        assert_eq!(sn.next_sn(), 2);
        assert_eq!(sn.next_sn(), 3);
        assert_eq!(sn.current_sn(), 3);
    }

    #[test]
    fn test_sn_allocator_reset_to() {
        let sn = SnAllocator::default();
        sn.next_sn();
        sn.next_sn();
        assert_eq!(sn.current_sn(), 2);
        // Simulate leader takeover after replaying Raft log up to SN=100.
        sn.reset_to(100);
        assert_eq!(sn.current_sn(), 100);
        assert_eq!(sn.next_sn(), 101);
    }

    #[test]
    fn test_sn_allocator_concurrent() {
        let sn = Arc::new(SnAllocator::default());
        let mut handles = Vec::new();
        for _ in 0..8 {
            let sn = Arc::clone(&sn);
            handles.push(std::thread::spawn(move || sn.next_sn()));
        }
        let mut values: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        values.sort_unstable();
        values.dedup();
        // All 8 threads must get distinct SNs in [1, 8].
        assert_eq!(values.len(), 8);
        assert_eq!(values[0], 1);
        assert_eq!(values[7], 8);
    }

    #[test]
    fn test_wait_queue_push_pop_fifo() {
        let q = WaitQueue::new();
        let inode = 42u64;

        let (tx1, _rx1) = oneshot::channel();
        let (tx2, _rx2) = oneshot::channel();
        let (tx3, _rx3) = oneshot::channel();

        assert_eq!(q.push(inode, Waiter::new("c1".into(), 1000, tx1)), 1);
        assert_eq!(q.push(inode, Waiter::new("c2".into(), 1000, tx2)), 2);
        assert_eq!(q.push(inode, Waiter::new("c3".into(), 1000, tx3)), 3);

        assert_eq!(q.len(inode), 3);

        let w1 = q.pop(inode).unwrap();
        assert_eq!(w1.client_id, "c1");
        let w2 = q.pop(inode).unwrap();
        assert_eq!(w2.client_id, "c2");
        let w3 = q.pop(inode).unwrap();
        assert_eq!(w3.client_id, "c3");
        assert!(q.pop(inode).is_none());
        assert!(q.is_empty());
    }

    #[test]
    fn test_wait_queue_per_inode_isolation() {
        let q = WaitQueue::new();
        let (tx1, _rx1) = oneshot::channel();
        let (tx2, _rx2) = oneshot::channel();

        q.push(1, Waiter::new("c1".into(), 1000, tx1));
        q.push(2, Waiter::new("c2".into(), 1000, tx2));

        assert_eq!(q.len(1), 1);
        assert_eq!(q.len(2), 1);
        assert!(!q.is_empty());

        // Popping inode 1 doesn't affect inode 2.
        let _ = q.pop(1).unwrap();
        assert_eq!(q.len(1), 0);
        assert_eq!(q.len(2), 1);

        assert_eq!(q.inode_count(), 1);
    }

    #[tokio::test]
    async fn test_wait_queue_drain_inode() {
        let q = WaitQueue::new();
        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();

        q.push(7, Waiter::new("c1".into(), 1000, tx1));
        q.push(7, Waiter::new("c2".into(), 1000, tx2));

        let drained = q.drain_inode(7);
        assert_eq!(drained, 2);
        assert!(q.is_empty());

        // Drained waiters' receivers should return an error (sender dropped).
        assert!(rx1.await.is_err());
        assert!(rx2.await.is_err());
    }

    #[test]
    fn test_wait_queue_drain_all() {
        let q = WaitQueue::new();
        for inode in 1..=3 {
            for i in 1..=2 {
                let (tx, _rx) = oneshot::channel();
                q.push(inode, Waiter::new(format!("c{i}"), 1000, tx));
            }
        }
        assert_eq!(q.inode_count(), 3);
        let drained = q.drain_all();
        assert_eq!(drained, 6);
        assert!(q.is_empty());
    }

    #[test]
    fn test_noop_revoker_succeeds() {
        let r = NoopRevoker;
        assert!(r.revoke(1, "tok", "client-A").is_ok());
    }

    #[test]
    fn test_acquire_outcome_predicates() {
        let granted = AcquireOutcome::Granted(AcquireResult {
            token: "t".into(),
            expire_at_ms: 1000,
            sn: 1,
        });
        assert!(granted.is_granted());
        assert!(!granted.is_queued());
        assert!(!granted.is_error());

        let (_tx, rx) = oneshot::channel();
        let queued = AcquireOutcome::Queued(rx);
        assert!(!queued.is_granted());
        assert!(queued.is_queued());
        assert!(!queued.is_error());

        let err = AcquireOutcome::Error("grace period".into());
        assert!(!err.is_granted());
        assert!(!err.is_queued());
        assert!(err.is_error());
    }
}
