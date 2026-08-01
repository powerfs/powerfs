use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct RangeLease {
    pub inode: u64,
    pub stripe_start: u64,
    pub stripe_count: u64,
    pub holder: String,
    pub token: String,
    pub exclusive: bool,
    pub stripe_size: u64,
    pub acquired_at: Instant,
    pub expire_at: Instant,
    pub epoch: u64,
}

impl RangeLease {
    pub fn is_expired(&self) -> bool {
        Instant::now() > self.expire_at
    }

    pub fn covers_stripe(&self, stripe: u64) -> bool {
        stripe >= self.stripe_start && stripe < self.stripe_start + self.stripe_count
    }

    pub fn overlaps(&self, other: &RangeLease) -> bool {
        let self_end = self.stripe_start + self.stripe_count;
        let other_end = other.stripe_start + other.stripe_count;
        self.stripe_start < other_end && other.stripe_start < self_end
    }
}

#[derive(Clone)]
pub struct RangeLeaseManager {
    leases: Arc<RwLock<HashMap<String, RangeLease>>>,
    inode_index: Arc<RwLock<HashMap<u64, Vec<String>>>>,
    /// holder -> set of tokens held by this client (for fast disconnect cleanup)
    holder_index: Arc<RwLock<HashMap<String, HashSet<String>>>>,
    /// total active holders count
    holder_count: Arc<AtomicU64>,
    epoch_counter: Arc<AtomicU64>,
    default_stripe_size: u64,
    /// Background cleanup task shutdown flag
    shutdown_flag: Arc<AtomicBool>,
    /// Grace period (ms) for cleanup_expired: leases expired within this
    /// window are NOT removed, so validate_token_with_grace_period can still
    /// find them. Must be >= the grace period used by validation callers.
    cleanup_grace_ms: u64,
}

impl RangeLeaseManager {
    pub fn new(default_stripe_size: u64) -> Self {
        Self {
            leases: Arc::new(RwLock::new(HashMap::new())),
            inode_index: Arc::new(RwLock::new(HashMap::new())),
            holder_index: Arc::new(RwLock::new(HashMap::new())),
            holder_count: Arc::new(AtomicU64::new(0)),
            epoch_counter: Arc::new(AtomicU64::new(0)),
            default_stripe_size,
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            cleanup_grace_ms: 5000,
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(64 * 1024 * 1024)
    }

    /// Set the cleanup grace period (ms). Leases expired within this window
    /// are not removed by cleanup_expired, allowing validate_token_with_grace_period
    /// to still find them.
    pub fn with_cleanup_grace_ms(mut self, grace_ms: u64) -> Self {
        self.cleanup_grace_ms = grace_ms;
        self
    }

    /// Clone the shutdown flag for background tasks to monitor
    pub fn shutdown_flag(&self) -> Arc<AtomicBool> {
        self.shutdown_flag.clone()
    }

    /// Signal the background cleanup task to stop (used during graceful shutdown)
    pub fn request_shutdown(&self) {
        self.shutdown_flag.store(true, Ordering::Relaxed);
    }

    fn generate_token(&self) -> String {
        let epoch = self.epoch_counter.fetch_add(1, Ordering::Relaxed);
        let id = uuid::Uuid::new_v4();
        format!("lease-{}-{}", epoch, id)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn acquire(
        &self,
        inode: u64,
        stripe_start: u64,
        stripe_count: u64,
        client_id: &str,
        duration_ms: u64,
        exclusive: bool,
        stripe_size: u64,
    ) -> Result<RangeLease, String> {
        let stripe_size = if stripe_size > 0 {
            stripe_size
        } else {
            self.default_stripe_size
        };
        let duration = Duration::from_millis(duration_ms);
        let now = Instant::now();
        let expire_at = now + duration;

        let mut leases = self.leases.write().unwrap();
        let mut inode_index = self.inode_index.write().unwrap();
        let mut holder_index = self.holder_index.write().unwrap();

        // Check for conflicts with existing leases on the same inode
        if let Some(existing_tokens) = inode_index.get(&inode) {
            for token in existing_tokens {
                if let Some(existing_lease) = leases.get(token) {
                    if existing_lease.is_expired() {
                        continue;
                    }
                    if existing_lease.holder == client_id {
                        continue;
                    }
                    if existing_lease.exclusive || exclusive {
                        let new_lease = RangeLease {
                            inode,
                            stripe_start,
                            stripe_count,
                            holder: client_id.to_string(),
                            token: String::new(),
                            exclusive,
                            stripe_size,
                            acquired_at: now,
                            expire_at,
                            epoch: 0,
                        };
                        if existing_lease.overlaps(&new_lease) {
                            return Err(format!(
                                "Stripe lease conflict: inode={}, stripes [{}, {}) overlaps with existing lease held by {}",
                                inode, stripe_start, stripe_start + stripe_count, existing_lease.holder
                            ));
                        }
                    }
                }
            }
        }

        // Clean up expired leases for this inode
        if let Some(tokens) = inode_index.get_mut(&inode) {
            tokens.retain(|t| leases.get(t).map(|l| !l.is_expired()).unwrap_or(false));
        }

        let token = self.generate_token();
        let epoch = self.epoch_counter.fetch_add(1, Ordering::Relaxed);

        let lease = RangeLease {
            inode,
            stripe_start,
            stripe_count,
            holder: client_id.to_string(),
            token: token.clone(),
            exclusive,
            stripe_size,
            acquired_at: now,
            expire_at,
            epoch,
        };

        // Grant all requested stripes
        let granted_stripes: Vec<u64> = (stripe_start..stripe_start + stripe_count).collect();
        let _ = granted_stripes;

        leases.insert(token.clone(), lease.clone());
        inode_index.entry(inode).or_default().push(token.clone());

        // Update holder index for fast disconnect lookup
        // First-lease-for-this-holder: create the set and bump holder_count
        // Subsequent leases: just insert the token without bumping holder_count
        let holder_entry = holder_index.entry(client_id.to_string()).or_default();
        let is_new_holder = holder_entry.is_empty();
        holder_entry.insert(token.clone());
        if is_new_holder {
            self.holder_count.fetch_add(1, Ordering::Relaxed);
        }

        Ok(lease)
    }

    pub fn renew(&self, token: &str, holder: &str, duration_ms: u64) -> Result<(), String> {
        let mut leases = self.leases.write().unwrap();
        match leases.get_mut(token) {
            Some(lease) => {
                if lease.holder != holder {
                    return Err("Lease holder mismatch".to_string());
                }
                lease.expire_at = Instant::now() + Duration::from_millis(duration_ms);
                lease.epoch = self.epoch_counter.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            None => Err("Lease not found".to_string()),
        }
    }

    pub fn release(&self, token: &str, holder: &str) -> Result<(), String> {
        let inode = {
            let mut leases = self.leases.write().unwrap();
            let lease = leases
                .get(token)
                .ok_or_else(|| "Lease not found".to_string())?;

            if lease.holder != holder {
                return Err("Lease holder mismatch".to_string());
            }

            let inode = lease.inode;
            leases.remove(token);
            inode
        };

        let mut inode_index = self.inode_index.write().unwrap();
        if let Some(tokens) = inode_index.get_mut(&inode) {
            tokens.retain(|t| t != token);
            if tokens.is_empty() {
                inode_index.remove(&inode);
            }
        }

        drop(inode_index);

        // Also remove from holder_index
        let mut holder_index = self.holder_index.write().unwrap();
        if let Some(tokens) = holder_index.get_mut(holder) {
            tokens.remove(token);
            if tokens.is_empty() {
                holder_index.remove(holder);
                self.holder_count.fetch_sub(1, Ordering::Relaxed);
            }
        }

        Ok(())
    }

    pub fn validate_token(&self, token: &str, holder: &str, stripe: u64) -> Result<(), String> {
        let leases = self.leases.read().unwrap();
        let lease = leases
            .get(token)
            .ok_or_else(|| "Lease token not found".to_string())?;

        if lease.is_expired() {
            return Err("Lease expired".to_string());
        }
        if lease.holder != holder {
            return Err("Lease holder mismatch".to_string());
        }
        if !lease.covers_stripe(stripe) {
            return Err(format!(
                "Stripe {} not covered by lease [{}, {})",
                stripe,
                lease.stripe_start,
                lease.stripe_start + lease.stripe_count
            ));
        }
        Ok(())
    }

    pub fn validate_token_for_inode(
        &self,
        token: &str,
        holder: &str,
        inode: u64,
    ) -> Result<(), String> {
        let leases = self.leases.read().unwrap();
        let lease = leases
            .get(token)
            .ok_or_else(|| "Lease token not found".to_string())?;

        if lease.is_expired() {
            return Err("Lease expired".to_string());
        }
        if lease.holder != holder {
            return Err("Lease holder mismatch".to_string());
        }
        if lease.inode != inode {
            return Err(format!(
                "Lease inode mismatch: expected {}, got {}",
                lease.inode, inode
            ));
        }
        Ok(())
    }

    pub fn validate_token_with_grace_period(
        &self,
        token: &str,
        holder: &str,
        inode: u64,
        grace_ms: u64,
    ) -> Result<(), String> {
        let leases = self.leases.read().unwrap();
        let lease = leases
            .get(token)
            .ok_or_else(|| "Lease token not found".to_string())?;

        if lease.holder != holder {
            return Err("Lease holder mismatch".to_string());
        }
        if lease.inode != inode {
            return Err(format!(
                "Lease inode mismatch: expected {}, got {}",
                lease.inode, inode
            ));
        }

        let grace = Duration::from_millis(grace_ms);
        if Instant::now() > lease.expire_at + grace {
            return Err("Lease expired beyond grace period".to_string());
        }
        Ok(())
    }

    pub fn get_active_leases_count(&self) -> usize {
        let leases = self.leases.read().unwrap();
        leases.values().filter(|l| !l.is_expired()).count()
    }

    /// Number of distinct holders currently holding non-expired leases
    pub fn get_active_holders_count(&self) -> u64 {
        self.holder_count.load(Ordering::Relaxed)
    }

    /// Get all non-expired leases held by a specific client
    pub fn get_leases_by_holder(&self, holder: &str) -> Vec<RangeLease> {
        let leases = self.leases.read().unwrap();
        let holder_index = self.holder_index.read().unwrap();
        let mut result = Vec::new();

        if let Some(tokens) = holder_index.get(holder) {
            for token in tokens {
                if let Some(lease) = leases.get(token) {
                    if !lease.is_expired() {
                        result.push(lease.clone());
                    }
                }
            }
        }
        result
    }

    /// Disconnect a holder (client): forcibly remove all leases held by this client.
    /// Used for failover when a client connection is lost.
    /// Returns the number of leases released.
    pub fn disconnect_holder(&self, holder: &str) -> usize {
        let mut leases = self.leases.write().unwrap();
        let mut inode_index = self.inode_index.write().unwrap();
        let mut holder_index = self.holder_index.write().unwrap();

        let tokens_to_remove: Vec<String> = holder_index
            .get(holder)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default();

        let mut removed = 0usize;
        for token in tokens_to_remove {
            if let Some(lease) = leases.remove(&token) {
                removed += 1;
                if let Some(tokens) = inode_index.get_mut(&lease.inode) {
                    tokens.retain(|t| t != &token);
                    if tokens.is_empty() {
                        inode_index.remove(&lease.inode);
                    }
                }
            }
            if let Some(tokens) = holder_index.get_mut(holder) {
                tokens.remove(&token);
            }
        }

        // Clean up holder entry if empty
        if let Some(tokens) = holder_index.get_mut(holder) {
            if tokens.is_empty() {
                holder_index.remove(holder);
                self.holder_count.fetch_sub(1, Ordering::Relaxed);
            }
        }

        removed
    }

    pub fn get_leases_for_inode(&self, inode: u64) -> Vec<RangeLease> {
        let leases = self.leases.read().unwrap();
        let inode_index = self.inode_index.read().unwrap();
        let mut result = Vec::new();

        if let Some(tokens) = inode_index.get(&inode) {
            for token in tokens {
                if let Some(lease) = leases.get(token) {
                    if !lease.is_expired() {
                        result.push(lease.clone());
                    }
                }
            }
        }
        result
    }

    pub fn cleanup_expired(&self) -> usize {
        let mut leases = self.leases.write().unwrap();
        let mut inode_index = self.inode_index.write().unwrap();
        let mut holder_index = self.holder_index.write().unwrap();
        let mut removed = 0usize;

        // Only remove leases expired BEYOND the grace period.
        // Leases within the grace period are kept so that
        // validate_token_with_grace_period can still find them.
        let grace = Duration::from_millis(self.cleanup_grace_ms);
        let now = Instant::now();
        let expired_tokens: Vec<String> = leases
            .iter()
            .filter(|(_, l)| now > l.expire_at + grace)
            .map(|(t, _)| t.clone())
            .collect();

        for token in expired_tokens {
            if let Some(lease) = leases.remove(&token) {
                removed += 1;
                // Remove from inode index
                if let Some(tokens) = inode_index.get_mut(&lease.inode) {
                    tokens.retain(|t| t != &token);
                    if tokens.is_empty() {
                        inode_index.remove(&lease.inode);
                    }
                }
                // Remove from holder index
                if let Some(tokens) = holder_index.get_mut(&lease.holder) {
                    tokens.remove(&token);
                    if tokens.is_empty() {
                        holder_index.remove(&lease.holder);
                        self.holder_count.fetch_sub(1, Ordering::Relaxed);
                    }
                }
            }
        }
        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_acquire_and_release() {
        let mgr = RangeLeaseManager::with_defaults();
        let lease = mgr.acquire(1, 0, 4, "client-a", 30000, true, 0).unwrap();
        assert_eq!(lease.inode, 1);
        assert_eq!(lease.stripe_start, 0);
        assert_eq!(lease.stripe_count, 4);
        assert!(lease.exclusive);

        mgr.release(&lease.token, "client-a").unwrap();
        assert_eq!(mgr.get_active_leases_count(), 0);
    }

    #[test]
    fn test_conflict_detection() {
        let mgr = RangeLeaseManager::with_defaults();
        mgr.acquire(1, 0, 4, "client-a", 30000, true, 0).unwrap();
        let result = mgr.acquire(1, 2, 4, "client-b", 30000, true, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_same_holder_no_conflict() {
        let mgr = RangeLeaseManager::with_defaults();
        let l1 = mgr.acquire(1, 0, 4, "client-a", 30000, true, 0).unwrap();
        let l2 = mgr.acquire(1, 2, 4, "client-a", 30000, true, 0).unwrap();
        assert_ne!(l1.token, l2.token);
    }

    #[test]
    fn test_non_overlapping_no_conflict() {
        let mgr = RangeLeaseManager::with_defaults();
        mgr.acquire(1, 0, 4, "client-a", 30000, true, 0).unwrap();
        let result = mgr.acquire(1, 4, 4, "client-b", 30000, true, 0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validation() {
        let mgr = RangeLeaseManager::with_defaults();
        let lease = mgr.acquire(1, 0, 4, "client-a", 30000, true, 0).unwrap();

        assert!(mgr.validate_token(&lease.token, "client-a", 0).is_ok());
        assert!(mgr.validate_token(&lease.token, "client-a", 3).is_ok());
        assert!(mgr.validate_token(&lease.token, "client-a", 4).is_err());
        assert!(mgr.validate_token(&lease.token, "client-b", 0).is_err());
        assert!(mgr.validate_token("bad-token", "client-a", 0).is_err());
    }

    #[test]
    fn test_renew() {
        let mgr = RangeLeaseManager::with_defaults();
        let lease = mgr.acquire(1, 0, 4, "client-a", 1000, true, 0).unwrap();
        mgr.renew(&lease.token, "client-a", 30000).unwrap();
        assert_eq!(mgr.get_active_leases_count(), 1);
    }

    #[test]
    fn test_expired_cleanup() {
        let mgr = RangeLeaseManager::with_defaults().with_cleanup_grace_ms(0);
        let _lease = mgr.acquire(1, 0, 4, "client-a", 1, true, 0).unwrap();
        std::thread::sleep(Duration::from_millis(10));
        let removed = mgr.cleanup_expired();
        assert!(removed >= 1);
        assert_eq!(mgr.get_active_leases_count(), 0);
    }

    #[test]
    fn test_shared_lease_multiple_holders() {
        let mgr = RangeLeaseManager::with_defaults();
        // Shared (read) lease should allow multiple holders
        let l1 = mgr.acquire(1, 0, 4, "client-a", 30000, false, 0).unwrap();
        let l2 = mgr.acquire(1, 0, 4, "client-b", 30000, false, 0).unwrap();
        assert_ne!(l1.token, l2.token);
        assert_eq!(mgr.get_active_leases_count(), 2);
    }

    #[test]
    fn test_shared_vs_exclusive_conflict() {
        let mgr = RangeLeaseManager::with_defaults();
        // Shared lease first
        let _l1 = mgr.acquire(1, 0, 4, "client-a", 30000, false, 0).unwrap();
        // Exclusive lease on same stripe should fail
        let result = mgr.acquire(1, 0, 4, "client-b", 30000, true, 0);
        assert!(result.is_err());
        // Another shared lease should succeed
        let l3 = mgr.acquire(1, 0, 4, "client-c", 30000, false, 0).unwrap();
        assert_eq!(mgr.get_active_leases_count(), 2);
        let _ = l3;
    }

    #[test]
    fn test_different_inodes_no_conflict() {
        let mgr = RangeLeaseManager::with_defaults();
        let l1 = mgr.acquire(1, 0, 4, "client-a", 30000, true, 0).unwrap();
        let l2 = mgr.acquire(2, 0, 4, "client-a", 30000, true, 0).unwrap();
        assert_ne!(l1.inode, l2.inode);
        assert_eq!(mgr.get_active_leases_count(), 2);
    }

    #[test]
    fn test_validate_token_for_inode() {
        let mgr = RangeLeaseManager::with_defaults();
        let lease = mgr.acquire(1, 0, 4, "client-a", 30000, true, 0).unwrap();

        // Valid inode and holder
        assert!(mgr
            .validate_token_for_inode(&lease.token, "client-a", 1)
            .is_ok());
        // Wrong inode
        assert!(mgr
            .validate_token_for_inode(&lease.token, "client-a", 2)
            .is_err());
        // Wrong holder
        assert!(mgr
            .validate_token_for_inode(&lease.token, "client-b", 1)
            .is_err());
        // Bad token
        assert!(mgr
            .validate_token_for_inode("bad-token", "client-a", 1)
            .is_err());
    }

    #[test]
    fn test_renew_wrong_holder() {
        let mgr = RangeLeaseManager::with_defaults();
        let lease = mgr.acquire(1, 0, 4, "client-a", 1000, true, 0).unwrap();
        // Renew with wrong holder should fail
        let result = mgr.renew(&lease.token, "client-b", 30000);
        assert!(result.is_err());
        // Renew with correct holder should succeed
        let result = mgr.renew(&lease.token, "client-a", 30000);
        assert!(result.is_ok());
    }

    #[test]
    fn test_release_wrong_holder() {
        let mgr = RangeLeaseManager::with_defaults();
        let lease = mgr.acquire(1, 0, 4, "client-a", 30000, true, 0).unwrap();
        // Release with wrong holder should fail
        let result = mgr.release(&lease.token, "client-b");
        assert!(result.is_err());
        // Release with correct holder should succeed
        let result = mgr.release(&lease.token, "client-a");
        assert!(result.is_ok());
        assert_eq!(mgr.get_active_leases_count(), 0);
    }

    #[test]
    fn test_get_leases_for_inode() {
        let mgr = RangeLeaseManager::with_defaults();
        mgr.acquire(1, 0, 4, "client-a", 30000, true, 0).unwrap();
        mgr.acquire(1, 8, 4, "client-b", 30000, false, 0).unwrap();
        mgr.acquire(2, 0, 4, "client-c", 30000, true, 0).unwrap();

        let leases_inode1 = mgr.get_leases_for_inode(1);
        assert_eq!(leases_inode1.len(), 2);
        let leases_inode2 = mgr.get_leases_for_inode(2);
        assert_eq!(leases_inode2.len(), 1);
        let leases_inode3 = mgr.get_leases_for_inode(3);
        assert_eq!(leases_inode3.len(), 0);
    }

    #[test]
    fn test_cleanup_only_removes_expired() {
        let mgr = RangeLeaseManager::with_defaults().with_cleanup_grace_ms(0);
        // One expired lease
        let _expired = mgr.acquire(1, 0, 4, "client-a", 1, true, 0).unwrap();
        // One valid lease
        let _valid = mgr.acquire(2, 0, 4, "client-b", 30000, true, 0).unwrap();

        std::thread::sleep(Duration::from_millis(10));

        let removed = mgr.cleanup_expired();
        assert_eq!(removed, 1);
        // Valid lease should still exist
        assert_eq!(mgr.get_active_leases_count(), 1);
    }

    #[test]
    fn test_multiple_stripe_lease() {
        let mgr = RangeLeaseManager::with_defaults();
        // Acquire lease covering multiple stripes
        let lease = mgr.acquire(1, 0, 8, "client-a", 30000, true, 0).unwrap();
        assert_eq!(lease.stripe_start, 0);
        assert_eq!(lease.stripe_count, 8);

        // All covered stripes should be valid
        for stripe in 0..8 {
            assert!(mgr.validate_token(&lease.token, "client-a", stripe).is_ok());
        }

        // Uncovered stripe should be invalid
        assert!(mgr.validate_token(&lease.token, "client-a", 8).is_err());
    }

    #[test]
    fn test_disconnect_holder() {
        let mgr = RangeLeaseManager::with_defaults();
        let l1 = mgr.acquire(1, 0, 4, "client-a", 30000, true, 0).unwrap();
        let _l2 = mgr.acquire(2, 0, 4, "client-a", 30000, true, 0).unwrap();
        let l3 = mgr.acquire(3, 0, 4, "client-b", 30000, true, 0).unwrap();

        assert_eq!(mgr.get_active_leases_count(), 3);
        assert_eq!(mgr.get_active_holders_count(), 2);

        // Disconnect client-a - should release all their leases
        let removed = mgr.disconnect_holder("client-a");
        assert_eq!(removed, 2);
        assert_eq!(mgr.get_active_leases_count(), 1);
        assert_eq!(mgr.get_active_holders_count(), 1);

        // client-b's lease still valid
        assert!(mgr
            .validate_token_for_inode(&l3.token, "client-b", 3)
            .is_ok());

        // client-a's token no longer valid
        assert!(mgr
            .validate_token_for_inode(&l1.token, "client-a", 1)
            .is_err());

        // Can now acquire what client-a held
        assert!(mgr.acquire(1, 0, 4, "client-c", 30000, true, 0).is_ok());
    }

    #[test]
    fn test_get_leases_by_holder() {
        let mgr = RangeLeaseManager::with_defaults();
        let _l1 = mgr.acquire(1, 0, 4, "client-a", 30000, true, 0).unwrap();
        let _l2 = mgr.acquire(2, 0, 4, "client-a", 30000, true, 0).unwrap();
        let _l3 = mgr.acquire(3, 0, 4, "client-b", 30000, true, 0).unwrap();

        let a_leases = mgr.get_leases_by_holder("client-a");
        assert_eq!(a_leases.len(), 2);
        let b_leases = mgr.get_leases_by_holder("client-b");
        assert_eq!(b_leases.len(), 1);
        let c_leases = mgr.get_leases_by_holder("client-c");
        assert_eq!(c_leases.len(), 0);
    }

    #[test]
    fn test_disconnect_holder_empty() {
        let mgr = RangeLeaseManager::with_defaults();
        // Disconnect unknown holder - no-op
        let removed = mgr.disconnect_holder("nobody");
        assert_eq!(removed, 0);
        assert_eq!(mgr.get_active_holders_count(), 0);
    }

    #[test]
    fn test_holder_count_updates_on_release() {
        let mgr = RangeLeaseManager::with_defaults();
        let l1 = mgr.acquire(1, 0, 4, "client-a", 30000, true, 0).unwrap();
        let _l2 = mgr.acquire(2, 0, 4, "client-a", 30000, true, 0).unwrap();

        assert_eq!(mgr.get_active_holders_count(), 1);
        assert_eq!(mgr.get_active_leases_count(), 2);

        // Release one of client-a's leases - holder still active
        mgr.release(&l1.token, "client-a").unwrap();
        assert_eq!(mgr.get_active_holders_count(), 1);
        assert_eq!(mgr.get_active_leases_count(), 1);

        // Release the last - holder count should drop to 0
        let remaining = mgr.get_leases_by_holder("client-a");
        mgr.release(&remaining[0].token, "client-a").unwrap();
        assert_eq!(mgr.get_active_holders_count(), 0);
        assert_eq!(mgr.get_active_leases_count(), 0);
    }

    #[test]
    fn test_following_disconnect_new_client_can_acquire() {
        let mgr = RangeLeaseManager::with_defaults();
        let _l1 = mgr.acquire(1, 0, 4, "client-a", 30000, true, 0).unwrap();

        // client-b cannot acquire due to conflict
        let result = mgr.acquire(1, 0, 4, "client-b", 30000, true, 0);
        assert!(result.is_err());

        // Disconnect client-a
        mgr.disconnect_holder("client-a");

        // Now client-b can acquire
        let result = mgr.acquire(1, 0, 4, "client-b", 30000, true, 0);
        assert!(result.is_ok());
        assert_eq!(mgr.get_active_leases_count(), 1);
    }

    #[test]
    fn test_expired_leases_do_not_inflate_holder_count() {
        let mgr = RangeLeaseManager::with_defaults().with_cleanup_grace_ms(0);
        let _l1 = mgr.acquire(1, 0, 4, "client-a", 10, true, 0).unwrap();

        assert_eq!(mgr.get_active_holders_count(), 1);

        // Wait for lease to expire
        std::thread::sleep(Duration::from_millis(50));

        // After expiration, holder_count still 1 until cleanup runs
        // but active leases should be 0
        assert_eq!(mgr.get_active_leases_count(), 0);

        // Run cleanup
        let removed = mgr.cleanup_expired();
        assert_eq!(removed, 1);
        assert_eq!(mgr.get_active_holders_count(), 0);
    }

    #[test]
    fn test_cleanup_grace_period_preserves_recently_expired() {
        // With a 500ms grace period, leases expired within 500ms should
        // NOT be removed by cleanup_expired (so validate_token_with_grace_period
        // can still find them).
        let mgr = RangeLeaseManager::with_defaults().with_cleanup_grace_ms(500);
        let lease = mgr.acquire(1, 0, 4, "client-a", 50, true, 0).unwrap();

        // Wait for lease to expire but stay within grace period
        std::thread::sleep(Duration::from_millis(100));

        // cleanup_expired should NOT remove it (within 500ms grace)
        let removed = mgr.cleanup_expired();
        assert_eq!(
            removed, 0,
            "lease within grace period should not be removed"
        );

        // validate_token_with_grace_period should still find it
        let result = mgr.validate_token_with_grace_period(&lease.token, "client-a", 1, 3000);
        assert!(
            result.is_ok(),
            "validation should succeed within grace period"
        );

        // Wait beyond grace period
        std::thread::sleep(Duration::from_millis(500));

        // Now cleanup_expired should remove it
        let removed = mgr.cleanup_expired();
        assert_eq!(removed, 1, "lease beyond grace period should be removed");
    }
}
