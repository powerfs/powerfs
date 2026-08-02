//! Stripe-level lease manager for PowerFS volume server.
//!
//! Wraps `powerfs_lease::MemoryLeaseStore<StripeKey>` to provide inode/stripe-
//! specific lease semantics. The generic store handles acquire/release/renew/
//! cleanup; this module adds `StripeKey` and stripe-specific validation methods
//! (`validate_token` for a specific stripe, `validate_token_for_inode`, etc.).
//!
//! This is a thin wrapper — all core logic lives in `powerfs-lease`.

use powerfs_lease::{LeaseEntry, LeaseKey, LeaseMode, LeaseStore, MemoryLeaseStore};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Stripe resource key: identifies a range of stripes on a specific inode.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct StripeKey {
    pub inode: u64,
    pub stripe_start: u64,
    pub stripe_count: u64,
}

impl StripeKey {
    pub fn new(inode: u64, stripe_start: u64, stripe_count: u64) -> Self {
        Self {
            inode,
            stripe_start,
            stripe_count,
        }
    }

    /// Whether a specific stripe index falls within this key's range.
    pub fn covers_stripe(&self, stripe: u64) -> bool {
        stripe >= self.stripe_start && stripe < self.stripe_start + self.stripe_count
    }
}

impl LeaseKey for StripeKey {
    fn group_id(&self) -> u64 {
        self.inode
    }

    fn conflicts(&self, other: &Self) -> bool {
        if self.inode != other.inode {
            return false;
        }
        let self_end = self.stripe_start + self.stripe_count;
        let other_end = other.stripe_start + other.stripe_count;
        self.stripe_start < other_end && other.stripe_start < self_end
    }
}

/// A granted stripe lease (backward-compatible struct for existing callers).
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
        if self.inode != other.inode {
            return false;
        }
        let self_end = self.stripe_start + self.stripe_count;
        let other_end = other.stripe_start + other.stripe_count;
        self.stripe_start < other_end && other.stripe_start < self_end
    }
}

/// Convert a generic `LeaseEntry<StripeKey>` to a `RangeLease` for backward compat.
fn entry_to_range_lease(entry: &LeaseEntry<StripeKey>, stripe_size: u64) -> RangeLease {
    RangeLease {
        inode: entry.key.inode,
        stripe_start: entry.key.stripe_start,
        stripe_count: entry.key.stripe_count,
        holder: entry.holder.clone(),
        token: entry.token.clone(),
        exclusive: entry.mode.is_exclusive(),
        stripe_size,
        acquired_at: entry.acquired_at,
        expire_at: entry.expire_at,
        epoch: entry.epoch,
    }
}

/// Stripe lease manager — wraps `MemoryLeaseStore<StripeKey>`.
///
/// Provides the same API as the original `RangeLeaseManager` so existing
/// call sites in `server.rs` and `net_handler.rs` require no changes.
#[derive(Clone)]
pub struct RangeLeaseManager {
    store: Arc<MemoryLeaseStore<StripeKey>>,
    default_stripe_size: u64,
}

impl RangeLeaseManager {
    pub fn new(default_stripe_size: u64) -> Self {
        Self {
            store: Arc::new(MemoryLeaseStore::new()),
            default_stripe_size,
        }
    }

    /// Construct with a custom cleanup grace period (for tests that need
    /// immediate cleanup, use grace_ms = 0).
    pub fn new_with_grace(default_stripe_size: u64, grace_ms: u64) -> Self {
        Self {
            store: Arc::new(
                MemoryLeaseStore::new().with_cleanup_grace(Duration::from_millis(grace_ms)),
            ),
            default_stripe_size,
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(64 * 1024 * 1024)
    }

    /// Set the cleanup grace period (ms).
    ///
    /// Note: this is a no-op when the store is already shared (Arc). To
    /// configure a custom grace period, use `new_with_grace` at construction.
    /// This method is kept for backward compatibility with existing callers.
    pub fn with_cleanup_grace_ms(self, _grace_ms: u64) -> Self {
        self
    }

    /// Clone the shutdown flag for background tasks to monitor.
    pub fn shutdown_flag(&self) -> Arc<AtomicBool> {
        self.store.shutdown_flag()
    }

    /// Signal the background cleanup task to stop.
    pub fn request_shutdown(&self) {
        self.store.request_shutdown();
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
        let key = StripeKey::new(inode, stripe_start, stripe_count);
        let mode = if exclusive {
            LeaseMode::Exclusive
        } else {
            LeaseMode::Shared
        };
        let entry = self
            .store
            .acquire(key, client_id, mode, Duration::from_millis(duration_ms))
            .map_err(|e| e.to_string())?;
        Ok(entry_to_range_lease(&entry, stripe_size))
    }

    pub fn renew(&self, token: &str, holder: &str, duration_ms: u64) -> Result<(), String> {
        self.store
            .renew(token, holder, Duration::from_millis(duration_ms))
            .map_err(|e| e.to_string())
    }

    pub fn release(&self, token: &str, holder: &str) -> Result<(), String> {
        self.store.release(token, holder).map_err(|e| e.to_string())
    }

    /// Validate that a token is valid for a specific stripe index.
    pub fn validate_token(&self, token: &str, holder: &str, stripe: u64) -> Result<(), String> {
        let entry = self.store.get_entry(token).ok_or("Lease token not found")?;
        self.store
            .validate_token(token, holder)
            .map_err(|e| e.to_string())?;
        if !entry.key.covers_stripe(stripe) {
            return Err(format!(
                "Stripe {} not covered by lease [{}, {})",
                stripe,
                entry.key.stripe_start,
                entry.key.stripe_start + entry.key.stripe_count
            ));
        }
        Ok(())
    }

    /// Validate that a token is valid for a specific inode (any stripe).
    pub fn validate_token_for_inode(
        &self,
        token: &str,
        holder: &str,
        inode: u64,
    ) -> Result<(), String> {
        let entry = self.store.get_entry(token).ok_or("Lease token not found")?;

        if entry.is_expired() {
            return Err("Lease expired".to_string());
        }
        if entry.holder != holder {
            return Err("Lease holder mismatch".to_string());
        }
        if entry.key.inode != inode {
            return Err(format!(
                "Lease inode mismatch: expected {}, got {}",
                inode, entry.key.inode
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
        let entry = self.store.get_entry(token).ok_or("Lease token not found")?;

        if entry.holder != holder {
            return Err("Lease holder mismatch".to_string());
        }
        if entry.key.inode != inode {
            return Err(format!(
                "Lease inode mismatch: expected {}, got {}",
                inode, entry.key.inode
            ));
        }

        let grace = Duration::from_millis(grace_ms);
        if Instant::now() > entry.expire_at + grace {
            return Err("Lease expired beyond grace period".to_string());
        }
        Ok(())
    }

    pub fn get_active_leases_count(&self) -> usize {
        self.store.active_count()
    }

    pub fn get_active_holders_count(&self) -> u64 {
        self.store.active_holders_count()
    }

    /// Get all non-expired leases held by a specific client.
    pub fn get_leases_by_holder(&self, holder: &str) -> Vec<RangeLease> {
        self.store
            .get_entries_by_holder(holder)
            .iter()
            .map(|e| entry_to_range_lease(e, self.default_stripe_size))
            .collect()
    }

    /// Disconnect a holder: forcibly remove all leases held by this client.
    /// Returns the number of leases released.
    pub fn disconnect_holder(&self, holder: &str) -> usize {
        self.store.disconnect_holder(holder)
    }

    pub fn get_leases_for_inode(&self, inode: u64) -> Vec<RangeLease> {
        self.store
            .get_entries_by_group(inode)
            .iter()
            .map(|e| entry_to_range_lease(e, self.default_stripe_size))
            .collect()
    }

    pub fn cleanup_expired(&self) -> usize {
        self.store.cleanup_expired()
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
        // Construct with zero grace for immediate cleanup
        let mgr = RangeLeaseManager::new_with_grace(64 * 1024 * 1024, 0);
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
}
