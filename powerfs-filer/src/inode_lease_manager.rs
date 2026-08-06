//! Inode Metadata Lease Manager (方案 A / Phase 2)
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
//! - During grace period, new acquire requests are rejected (safety margin
//!   for network delays)
//! - After grace period, the lease is evicted and a new client can acquire

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Default grace period after lease expiry before allowing a new holder.
/// This prevents data corruption when a client is slow but still alive.
const DEFAULT_GRACE_PERIOD_MS: u64 = 5000;

/// An inode metadata lease entry.
#[derive(Debug, Clone)]
struct InodeLeaseEntry {
    /// Unique token identifying this lease instance.
    token: String,
    /// Client ID of the lease holder.
    holder: String,
    /// When the lease was acquired (for monitoring).
    acquired_at: Instant,
    /// When the lease expires (TTL).
    expire_at: Instant,
    /// When the grace period ends (expire_at + grace_period).
    /// New acquires are rejected until this instant.
    grace_until: Instant,
}

impl InodeLeaseEntry {
    /// Whether the lease is still within its valid TTL.
    fn is_valid(&self) -> bool {
        Instant::now() < self.expire_at
    }

    /// Whether the lease is still within the grace period (expired but
    /// not yet available for a new holder).
    fn in_grace_period(&self) -> bool {
        let now = Instant::now();
        now >= self.expire_at && now < self.grace_until
    }

    /// Whether the grace period has fully elapsed; the entry can be evicted
    /// and a new holder can acquire.
    fn past_grace(&self) -> bool {
        Instant::now() >= self.grace_until
    }
}

/// Inode metadata lease manager — in-memory, per-Filer-leader.
///
/// Thread-safe via `RwLock<HashMap>`. Does NOT replicate across Raft; if the
/// Filer leader changes, lease state is lost. Clients retry acquire on the new
/// leader. The actual data consistency is guaranteed by Raft
/// (`UpdateInodeSizeChunks`), not by the lease.
#[derive(Clone)]
pub struct InodeLeaseManager {
    leases: Arc<RwLock<HashMap<u64, InodeLeaseEntry>>>,
    grace_period: Duration,
}

/// Result of an acquire attempt.
#[derive(Debug)]
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
    pub fn new() -> Self {
        Self {
            leases: Arc::new(RwLock::new(HashMap::new())),
            grace_period: Duration::from_millis(DEFAULT_GRACE_PERIOD_MS),
        }
    }

    pub fn with_grace_period(grace_ms: u64) -> Self {
        Self {
            leases: Arc::new(RwLock::new(HashMap::new())),
            grace_period: Duration::from_millis(grace_ms),
        }
    }

    /// Acquire an exclusive inode metadata lease.
    ///
    /// Returns `Ok(AcquireResult)` on success, or `Err(reason)` if:
    /// - Another client holds a valid lease
    /// - The lease is in grace period (expired but not yet available)
    ///
    /// If the same client re-acquires (same holder), the existing lease is
    /// renewed (idempotent).
    pub fn acquire(
        &self,
        inode: u64,
        client_id: &str,
        duration_ms: u64,
    ) -> Result<AcquireResult, String> {
        let now = Instant::now();
        let expire_at = now + Duration::from_millis(duration_ms);
        let grace_until = expire_at + self.grace_period;

        let mut leases = self.leases.write().unwrap();

        // Check existing lease
        if let Some(entry) = leases.get(&inode) {
            if entry.is_valid() {
                if entry.holder == client_id {
                    // Same holder re-acquire: treat as renewal (idempotent)
                    let token = entry.token.clone();
                    return Ok(AcquireResult {
                        token,
                        expire_at_ms: duration_ms,
                    });
                }
                // Different holder, lease still valid
                return Err(format!(
                    "inode {} lease held by another client: {}",
                    inode, entry.holder
                ));
            }
            if entry.in_grace_period() {
                return Err(format!(
                    "inode {} lease in grace period (expired, holder={})",
                    inode, entry.holder
                ));
            }
            // Past grace: fall through to acquire (entry will be replaced)
        }

        // Generate token
        let token = generate_token();
        leases.insert(
            inode,
            InodeLeaseEntry {
                token: token.clone(),
                holder: client_id.to_string(),
                acquired_at: now,
                expire_at,
                grace_until,
            },
        );

        log::debug!(
            "InodeLease: acquired inode={} holder={} duration_ms={}",
            inode,
            client_id,
            duration_ms
        );

        Ok(AcquireResult {
            token,
            expire_at_ms: duration_ms,
        })
    }

    /// Release an inode lease. The holder must match and the token must be
    /// valid (or already expired). Returns Ok(()) on success.
    ///
    /// Treating release of a non-existent / expired lease as success
    /// (idempotent), matching the Volume Server's range lease semantics.
    pub fn release(&self, inode: u64, client_id: &str, token: &str) -> Result<(), String> {
        let mut leases = self.leases.write().unwrap();
        if let Some(entry) = leases.get(&inode) {
            if entry.token != token {
                return Err(format!(
                    "inode {} lease token mismatch: expected={}, got={}",
                    inode, entry.token, token
                ));
            }
            if entry.holder != client_id {
                return Err(format!(
                    "inode {} lease holder mismatch: expected={}, got={}",
                    inode, entry.holder, client_id
                ));
            }
        }
        // Remove the lease (or clean up expired entry)
        leases.remove(&inode);
        log::debug!("InodeLease: released inode={} holder={}", inode, client_id);
        Ok(())
    }

    /// Renew an existing inode lease. The holder and token must match.
    pub fn renew(
        &self,
        inode: u64,
        client_id: &str,
        token: &str,
        duration_ms: u64,
    ) -> Result<(), String> {
        let mut leases = self.leases.write().unwrap();
        let entry = leases
            .get_mut(&inode)
            .ok_or_else(|| format!("inode {} lease not found", inode))?;

        if entry.token != token {
            return Err(format!("inode {} lease token mismatch on renew", inode));
        }
        if entry.holder != client_id {
            return Err(format!("inode {} lease holder mismatch on renew", inode));
        }
        if entry.past_grace() {
            return Err(format!(
                "inode {} lease past grace period, cannot renew",
                inode
            ));
        }

        let now = Instant::now();
        entry.expire_at = now + Duration::from_millis(duration_ms);
        entry.grace_until = entry.expire_at + self.grace_period;

        log::debug!(
            "InodeLease: renewed inode={} holder={} duration_ms={}",
            inode,
            client_id,
            duration_ms
        );
        Ok(())
    }

    /// Validate that a client holds a valid lease for an inode.
    /// Used by Filer-side operations (e.g., close) to verify the caller
    /// is the lease holder before applying updates.
    pub fn validate(&self, inode: u64, client_id: &str, token: &str) -> Result<(), String> {
        let leases = self.leases.read().unwrap();
        let entry = leases
            .get(&inode)
            .ok_or_else(|| format!("inode {} lease not found", inode))?;

        if !entry.is_valid() {
            return Err(format!("inode {} lease expired", inode));
        }
        if entry.holder != client_id {
            return Err(format!(
                "inode {} lease holder mismatch: expected={}, got={}",
                inode, entry.holder, client_id
            ));
        }
        if entry.token != token {
            return Err(format!("inode {} lease token mismatch", inode));
        }
        Ok(())
    }

    /// Release all leases held by a client (used on client disconnect).
    pub fn disconnect_holder(&self, client_id: &str) -> usize {
        let to_remove: Vec<u64> = {
            let leases = self.leases.read().unwrap();
            leases
                .iter()
                .filter(|(_, entry)| entry.holder == client_id)
                .map(|(inode, _)| *inode)
                .collect()
        };
        let count = to_remove.len();
        if count > 0 {
            let mut leases = self.leases.write().unwrap();
            for inode in &to_remove {
                leases.remove(inode);
            }
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
        let to_remove: Vec<u64> = {
            let leases = self.leases.read().unwrap();
            leases
                .iter()
                .filter(|(_, entry)| entry.past_grace())
                .map(|(inode, _)| *inode)
                .collect()
        };
        let count = to_remove.len();
        if count > 0 {
            let mut leases = self.leases.write().unwrap();
            for inode in &to_remove {
                leases.remove(inode);
            }
        }
        count
    }

    /// Number of active leases (for monitoring).
    pub fn active_count(&self) -> usize {
        let leases = self.leases.read().unwrap();
        leases.values().filter(|e| e.is_valid()).count()
    }
}

/// Generate a unique lease token (UUID v4 format).
fn generate_token() -> String {
    // Use a simple random token; the Filer is single-instance per shard
    // leader, so collisions are extremely unlikely.
    use std::time::SystemTime;
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    let nanos = now.as_nanos();
    format!("inode-lease-{:016x}{:08x}", nanos, std::process::id())
}

#[cfg(test)]
mod tests {
    use super::*;

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
    /// 验证：只有一个线程成功获取，其余全部失败（"held by another client"）。
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
    /// 验证：全部成功，互不干扰。
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

    /// 持有者释放 lease 后，等待的线程可以获取。
    /// 验证：release → acquire 的交接在并发环境下正常工作。
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

        // client-B 在后台尝试获取，不断重试
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

    /// 持有者持续续租，其他客户端无法获取。
    /// 验证：renew 延长 lease 有效期，阻止其他客户端 acquire。
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
                    // 如果续租失败（可能已被清理），退出
                    eprintln!("renew failed: {}", e);
                    break;
                }
            }
        });

        // 等待 300ms（超过原始 lease 时效，但续租应该保持了有效性）
        std::thread::sleep(Duration::from_millis(300));

        // client-B 尝试获取 — 应该失败（client-A 仍在续租）
        let err = mgr.acquire(inode, "client-B", 200).unwrap_err();
        assert!(
            err.contains("held by another client") || err.contains("grace period"),
            "client-B should be blocked while client-A is renewing, got: {}",
            err
        );

        // 停止续租，等待 lease 过期 + grace period
        stop_renew.store(true, std::sync::atomic::Ordering::SeqCst);
        renew_handle.join().unwrap();

        // grace period = 5000ms (default), 但我们等不了那么久
        // 直接释放，验证 release 后 client-B 能获取
        mgr.release(inode, "client-A", &token_for_release).unwrap();
        mgr.acquire(inode, "client-B", 200).unwrap();
    }

    /// 同一客户端并发获取同一 inode（幂等）。
    /// 验证：同一 holder 的并发 acquire 返回相同 token。
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
        // 所有 token 应该相同（同一 holder 幂等）
        let first = &tokens[0];
        assert!(
            tokens.iter().all(|t| t == first),
            "same client should get the same token (idempotent), got: {:?}",
            *tokens
        );
    }

    /// 并发 disconnect_holder 在其他线程 acquire 时安全执行。
    /// 验证：disconnect 释放的 lease 可以被其他线程立即获取。
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
}
