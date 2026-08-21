//! Prometheus-style lock metrics — single source of truth for
//! `FuseLockManager` cache hit/miss and conflict counts.
//!
//! # Exposed counters (阶段一C step 4 — exposed via `metrics_export` in
//! §4.1)
//!
//! | counter                  | meaning                                |
//! |--------------------------|----------------------------------------|
//! | `acquire_total`          | total `LockManager::acquire` calls     |
//! | `acquire_cache_hit`      | calls served from `ClientLeaseState`   |
//! | `acquire_cache_miss`     | calls requiring a backend RPC         |
//! | `acquire_conflict`       | backend returned a conflict            |
//! | `release_total`          | total `LockManager::release` calls     |
//! | `renew_total`            | total `LockManager::renew` calls       |
//! | `errors_total`           | total errors (conflict + network + ...)|
//! | `sweep_removed_total`   | total entries removed by `sweep_expired`|
//! | `sweep_skipped_total`   | cache-hit acquires that skipped sweep (phase 4 P1 lazy sweep)|
//! | `lockify_self_declare_total`   | local self-declarations issued (phase 4 §5.1 Lockify)|
//! | `lockify_sync_ok_total`        | async sync RPCs that succeeded (CAS-merged server token)|
//! | `lockify_sync_conflict_total`  | async sync RPCs that hit a server-side conflict (local entry invalidated)|
//! | `lockify_sync_err_total`       | async sync RPCs that failed for non-conflict reasons (local token remains valid until TTL)|
//!
//! The counters are `AtomicU64` so they can be read from any thread
//! without locking. A future Prometheus exporter can scrape them via
//! `LockMetrics::snapshot()`.

use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Lock operation counters.
///
/// Stored as `AtomicU64` for lock-free reads; updates use `Relaxed`
/// ordering because counters are best-effort observability — we don't
/// need cross-field synchronization for the metrics use case.
#[derive(Default)]
pub struct LockMetrics {
    pub acquire_total: AtomicU64,
    pub acquire_cache_hit: AtomicU64,
    pub acquire_cache_miss: AtomicU64,
    pub acquire_conflict: AtomicU64,
    pub release_total: AtomicU64,
    pub renew_total: AtomicU64,
    pub errors_total: AtomicU64,
    pub sweep_removed_total: AtomicU64,
    /// Cache-hit acquires that skipped `sweep_expired` because the last
    /// sweep was within the lazy-sweep threshold (phase 4 P1). Lets
    /// operators verify the optimization is engaging in production.
    pub sweep_skipped_total: AtomicU64,
    /// Phase-4 §5.1 Lockify: total local self-declarations issued
    /// (one per `acquire_local` call).
    pub lockify_self_declare_total: AtomicU64,
    /// Phase-4 §5.1 Lockify: async sync RPCs that returned Ok. The
    /// local token was CAS-replaced by the server-issued token.
    pub lockify_sync_ok_total: AtomicU64,
    /// Phase-4 §5.1 Lockify: async sync RPCs that hit a server-side
    /// conflict (another client already owns the inode). The local
    /// entry is invalidated so subsequent operations re-acquire via
    /// the regular RPC path.
    pub lockify_sync_conflict_total: AtomicU64,
    /// Phase-4 §5.1 Lockify: async sync RPCs that failed for non-
    /// conflict reasons (network, server error). The local token
    /// remains valid until TTL — graceful degradation.
    pub lockify_sync_err_total: AtomicU64,
    /// Phase-4 §5.1 Lockify: monotonic nonce used to make each
    /// `local-...` token unique even if the same inode is self-declared
    /// twice (e.g. across client restarts within the same TTL window).
    /// `Relaxed` is sufficient — uniqueness only needs to hold per
    /// client lifetime, and `fetch_add` guarantees monotonic increment.
    lockify_nonce: AtomicU64,
}

/// A consistent point-in-time snapshot of all counters.
///
/// Counters may move between reads of individual fields (because we
/// use `Relaxed` ordering), but the snapshot is monotonic across calls.
///
/// Derives `Serialize` so the FUSE admin server can expose it as JSON
/// at `/lock-metrics` (see `powerfs-fuse/src/admin_server.rs`).
#[derive(Debug, Clone, Default, Serialize)]
pub struct LockMetricsSnapshot {
    pub acquire_total: u64,
    pub acquire_cache_hit: u64,
    pub acquire_cache_miss: u64,
    pub acquire_conflict: u64,
    pub release_total: u64,
    pub renew_total: u64,
    pub errors_total: u64,
    pub sweep_removed_total: u64,
    pub sweep_skipped_total: u64,
    pub lockify_self_declare_total: u64,
    pub lockify_sync_ok_total: u64,
    pub lockify_sync_conflict_total: u64,
    pub lockify_sync_err_total: u64,
}

impl LockMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    // ---------- acquire path ----------

    pub fn record_acquire(&self) {
        self.acquire_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_cache_hit(&self) {
        self.acquire_cache_hit.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_cache_miss(&self) {
        self.acquire_cache_miss.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_conflict(&self) {
        self.acquire_conflict.fetch_add(1, Ordering::Relaxed);
        // conflict is also an error
        self.errors_total.fetch_add(1, Ordering::Relaxed);
    }

    // ---------- release / renew path ----------

    pub fn record_release(&self) {
        self.release_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_renew(&self) {
        self.renew_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_error(&self) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_sweep_removed(&self, n: usize) {
        if n > 0 {
            self.sweep_removed_total
                .fetch_add(n as u64, Ordering::Relaxed);
        }
    }

    /// Record a cache-hit acquire that skipped `sweep_expired` because
    /// the last sweep was within the lazy-sweep threshold (phase 4 P1).
    pub fn record_sweep_skipped(&self) {
        self.sweep_skipped_total.fetch_add(1, Ordering::Relaxed);
    }

    // ---------- Lockify (phase 4 §5.1) ----------

    /// Record a local self-declaration. Called once per
    /// `FuseLockManager::acquire_local`. Also allocates the next
    /// monotonic nonce used to make the local token unique; callers
    /// must read it back via [`Self::next_lockify_nonce`].
    pub fn record_lockify_self_declare(&self) {
        self.lockify_self_declare_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record an async sync RPC that returned Ok.
    pub fn record_lockify_sync_ok(&self) {
        self.lockify_sync_ok_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an async sync RPC that hit a server-side conflict. The
    /// local entry is invalidated.
    pub fn record_lockify_sync_conflict(&self) {
        self.lockify_sync_conflict_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record an async sync RPC that failed for non-conflict reasons
    /// (network, server error). The local token remains valid until
    /// TTL — graceful degradation.
    pub fn record_lockify_sync_err(&self) {
        self.lockify_sync_err_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Allocate the next monotonic Lockify nonce. The returned value
    /// is strictly greater than any previously returned nonce on this
    /// `LockMetrics` instance, so the local token
    /// `local-{client}-{inode}-{nonce}` is unique across calls.
    pub fn next_lockify_nonce(&self) -> u64 {
        self.lockify_nonce.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Read a consistent-ish snapshot of all counters.
    pub fn snapshot(&self) -> LockMetricsSnapshot {
        LockMetricsSnapshot {
            acquire_total: self.acquire_total.load(Ordering::Relaxed),
            acquire_cache_hit: self.acquire_cache_hit.load(Ordering::Relaxed),
            acquire_cache_miss: self.acquire_cache_miss.load(Ordering::Relaxed),
            acquire_conflict: self.acquire_conflict.load(Ordering::Relaxed),
            release_total: self.release_total.load(Ordering::Relaxed),
            renew_total: self.renew_total.load(Ordering::Relaxed),
            errors_total: self.errors_total.load(Ordering::Relaxed),
            sweep_removed_total: self.sweep_removed_total.load(Ordering::Relaxed),
            sweep_skipped_total: self.sweep_skipped_total.load(Ordering::Relaxed),
            lockify_self_declare_total: self.lockify_self_declare_total.load(Ordering::Relaxed),
            lockify_sync_ok_total: self.lockify_sync_ok_total.load(Ordering::Relaxed),
            lockify_sync_conflict_total: self.lockify_sync_conflict_total.load(Ordering::Relaxed),
            lockify_sync_err_total: self.lockify_sync_err_total.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_acquire_and_cache_counters() {
        let m = LockMetrics::new();
        for _ in 0..3 {
            m.record_acquire();
            m.record_cache_hit();
        }
        for _ in 0..2 {
            m.record_acquire();
            m.record_cache_miss();
        }
        let snap = m.snapshot();
        assert_eq!(snap.acquire_total, 5);
        assert_eq!(snap.acquire_cache_hit, 3);
        assert_eq!(snap.acquire_cache_miss, 2);
    }

    #[test]
    fn test_record_conflict_increments_both_counters() {
        let m = LockMetrics::new();
        m.record_conflict();
        m.record_conflict();
        let snap = m.snapshot();
        assert_eq!(snap.acquire_conflict, 2);
        assert_eq!(snap.errors_total, 2, "conflict must also bump errors_total");
    }

    #[test]
    fn test_record_sweep_removed_no_op_when_zero() {
        let m = LockMetrics::new();
        m.record_sweep_removed(0);
        assert_eq!(m.snapshot().sweep_removed_total, 0);
        m.record_sweep_removed(5);
        assert_eq!(m.snapshot().sweep_removed_total, 5);
    }

    #[test]
    fn test_snapshot_is_monotonic_across_threads() {
        use std::sync::Arc;
        use std::thread;
        let m = Arc::new(LockMetrics::new());
        let mut handles = Vec::new();
        for _ in 0..4 {
            let m = Arc::clone(&m);
            handles.push(thread::spawn(move || {
                for _ in 0..1000 {
                    m.record_acquire();
                    m.record_cache_miss();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let snap = m.snapshot();
        assert_eq!(snap.acquire_total, 4000);
        assert_eq!(snap.acquire_cache_miss, 4000);
    }
}
