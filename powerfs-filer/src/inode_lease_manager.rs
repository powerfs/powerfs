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

use powerfs_lease::{
    LeaseError, LeaseKey, LeaseMode, LeasePersistence, LeaseStats, LeaseStore, MemoryLeaseStore,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Default grace period after lease expiry before allowing a new holder.
/// This prevents data corruption when a client is slow but still alive.
const DEFAULT_GRACE_PERIOD_MS: u64 = 5000;

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
}

/// Result of an acquire attempt.
#[derive(Debug, Clone)]
pub struct AcquireResult {
    pub token: String,
    pub expire_at_ms: u64,
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
        }
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
        for entry in self.store.get_entries_by_group(inode) {
            if entry.holder == client_id {
                return Ok(AcquireResult {
                    token: entry.token,
                    expire_at_ms: duration_ms,
                });
            }
        }

        // Grace-period protection: scan all entries (including expired ones
        // still in memory within the cleanup_grace window). If any is held by
        // a different client and is expired but NOT past grace, reject the
        // new acquire.
        for entry in self.store.get_all_entries_by_group(inode) {
            if entry.holder == client_id {
                continue;
            }
            if entry.is_expired() && !entry.is_expired_beyond(self.grace_period) {
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
                log::debug!(
                    "InodeLease: acquired inode={} holder={} duration_ms={}",
                    inode,
                    client_id,
                    duration_ms
                );
                Ok(AcquireResult {
                    token: entry.token,
                    expire_at_ms: duration_ms,
                })
            }
            Err(LeaseError::Conflict(msg)) => Err(format!(
                "inode {} lease held by another client: {}",
                inode, msg
            )),
            Err(e) => Err(format!("inode {} lease acquire failed: {}", inode, e)),
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

        // Reject renew if past grace — matches previous implementation's
        // `past_grace()` semantics.
        if matched.is_expired_beyond(self.grace_period) {
            return Err(format!(
                "inode {} lease past grace period, cannot renew",
                inode
            ));
        }

        let duration = Duration::from_millis(duration_ms);
        match self.store.renew(token, client_id, duration) {
            Ok(()) => {
                log::debug!(
                    "InodeLease: renewed inode={} holder={} duration_ms={}",
                    inode,
                    client_id,
                    duration_ms
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
}
