use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
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

pub struct RangeLeaseManager {
    leases: Arc<RwLock<HashMap<String, RangeLease>>>,
    inode_index: Arc<RwLock<HashMap<u64, Vec<String>>>>,
    epoch_counter: AtomicU64,
    default_stripe_size: u64,
}

impl RangeLeaseManager {
    pub fn new(default_stripe_size: u64) -> Self {
        Self {
            leases: Arc::new(RwLock::new(HashMap::new())),
            inode_index: Arc::new(RwLock::new(HashMap::new())),
            epoch_counter: AtomicU64::new(0),
            default_stripe_size,
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(64 * 1024 * 1024)
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
        inode_index.entry(inode).or_default().push(token);

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

    pub fn get_active_leases_count(&self) -> usize {
        let leases = self.leases.read().unwrap();
        leases.values().filter(|l| !l.is_expired()).count()
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
        let mut removed = 0usize;

        let expired_tokens: Vec<String> = leases
            .iter()
            .filter(|(_, l)| l.is_expired())
            .map(|(t, _)| t.clone())
            .collect();

        for token in expired_tokens {
            if let Some(lease) = leases.remove(&token) {
                removed += 1;
                if let Some(tokens) = inode_index.get_mut(&lease.inode) {
                    tokens.retain(|t| t != &token);
                    if tokens.is_empty() {
                        inode_index.remove(&lease.inode);
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
        let mgr = RangeLeaseManager::with_defaults();
        let _lease = mgr.acquire(1, 0, 4, "client-a", 1, true, 0).unwrap();
        std::thread::sleep(Duration::from_millis(10));
        let removed = mgr.cleanup_expired();
        assert!(removed >= 1);
        assert_eq!(mgr.get_active_leases_count(), 0);
    }
}
