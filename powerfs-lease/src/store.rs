//! Server-side lease store: generic over a [`LeaseKey`] implementation.
//!
//! [`MemoryLeaseStore`] is the in-memory implementation, generalized from
//! PowerFS's `RangeLeaseManager`. It maintains three indexes for fast lookup:
//! - `leases`: token → entry
//! - `group_index`: group_id → tokens (for conflict checking within a group)
//! - `holder_index`: holder → tokens (for fast disconnect cleanup)

use crate::error::LeaseError;
use crate::token::LeaseMode;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Trait for resource keys managed by the lease store.
///
/// Implementors define:
/// - `group_id`: a coarse grouping (e.g., inode number) for indexing — keys
///   in different groups never conflict, so conflict checks only scan the
///   same group.
/// - `conflicts`: whether two keys in the same group conflict (e.g., overlapping
///   stripe ranges).
pub trait LeaseKey: Clone + Eq + Hash + Send + Sync + 'static {
    /// Coarse group identifier for indexing (e.g., inode number).
    fn group_id(&self) -> u64;

    /// Whether this key conflicts with another key.
    /// Only called for keys in the same group.
    fn conflicts(&self, other: &Self) -> bool;
}

/// A granted lease entry.
#[derive(Debug, Clone)]
pub struct LeaseEntry<K: LeaseKey> {
    pub key: K,
    pub holder: String,
    pub token: String,
    pub mode: LeaseMode,
    pub acquired_at: Instant,
    pub expire_at: Instant,
    pub epoch: u64,
}

impl<K: LeaseKey> LeaseEntry<K> {
    pub fn is_expired(&self) -> bool {
        Instant::now() > self.expire_at
    }

    pub fn is_expired_beyond(&self, grace: Duration) -> bool {
        Instant::now() > self.expire_at + grace
    }
}

/// Trait for server-side lease stores.
///
/// All methods are synchronous (designed to be called under a lock on the
/// volume server's request handler). The in-memory implementation is
/// [`MemoryLeaseStore`]; a persistent implementation can be added later.
pub trait LeaseStore<K: LeaseKey>: Send + Sync {
    fn acquire(
        &self,
        key: K,
        holder: &str,
        mode: LeaseMode,
        duration: Duration,
    ) -> Result<LeaseEntry<K>, LeaseError>;

    fn renew(&self, token: &str, holder: &str, duration: Duration) -> Result<(), LeaseError>;

    fn release(&self, token: &str, holder: &str) -> Result<(), LeaseError>;

    fn validate_token(&self, token: &str, holder: &str) -> Result<(), LeaseError>;

    fn validate_token_with_grace(
        &self,
        token: &str,
        holder: &str,
        grace: Duration,
    ) -> Result<(), LeaseError>;

    fn get_entry(&self, token: &str) -> Option<LeaseEntry<K>>;

    fn get_entries_by_group(&self, group_id: u64) -> Vec<LeaseEntry<K>>;

    fn get_entries_by_holder(&self, holder: &str) -> Vec<LeaseEntry<K>>;

    fn disconnect_holder(&self, holder: &str) -> usize;

    fn cleanup_expired(&self) -> usize;

    fn active_count(&self) -> usize;

    fn active_holders_count(&self) -> u64;

    fn shutdown_flag(&self) -> Arc<AtomicBool>;

    fn request_shutdown(&self);
}

/// In-memory lease store, generic over key type `K`.
///
/// Generalized from PowerFS's `RangeLeaseManager`. Maintains three indexes
/// for O(1) token lookup and O(leases_per_group) conflict checking.
pub struct MemoryLeaseStore<K: LeaseKey> {
    leases: Arc<RwLock<HashMap<String, LeaseEntry<K>>>>,
    group_index: Arc<RwLock<HashMap<u64, Vec<String>>>>,
    holder_index: Arc<RwLock<HashMap<String, HashSet<String>>>>,
    holder_count: Arc<AtomicU64>,
    epoch_counter: Arc<AtomicU64>,
    shutdown_flag: Arc<AtomicBool>,
    /// Grace period for cleanup_expired: leases expired within this window
    /// are NOT removed, so validate_token_with_grace can still find them.
    cleanup_grace: Duration,
}

impl<K: LeaseKey> MemoryLeaseStore<K> {
    pub fn new() -> Self {
        Self {
            leases: Arc::new(RwLock::new(HashMap::new())),
            group_index: Arc::new(RwLock::new(HashMap::new())),
            holder_index: Arc::new(RwLock::new(HashMap::new())),
            holder_count: Arc::new(AtomicU64::new(0)),
            epoch_counter: Arc::new(AtomicU64::new(0)),
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            cleanup_grace: Duration::from_millis(5000),
        }
    }

    pub fn with_cleanup_grace(mut self, grace: Duration) -> Self {
        self.cleanup_grace = grace;
        self
    }

    fn generate_token(&self) -> (String, u64) {
        let epoch = self.epoch_counter.fetch_add(1, Ordering::Relaxed);
        let id = uuid::Uuid::new_v4();
        (format!("lease-{}-{}", epoch, id), epoch)
    }
}

impl<K: LeaseKey> Default for MemoryLeaseStore<K> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: LeaseKey> LeaseStore<K> for MemoryLeaseStore<K> {
    fn acquire(
        &self,
        key: K,
        holder: &str,
        mode: LeaseMode,
        duration: Duration,
    ) -> Result<LeaseEntry<K>, LeaseError> {
        let now = Instant::now();
        let expire_at = now + duration;
        let group_id = key.group_id();

        let mut leases = self.leases.write().unwrap();
        let mut group_index = self.group_index.write().unwrap();
        let mut holder_index = self.holder_index.write().unwrap();

        // Check for conflicts with existing leases in the same group
        if let Some(existing_tokens) = group_index.get(&group_id) {
            for token in existing_tokens.iter() {
                if let Some(existing) = leases.get(token) {
                    if existing.is_expired() {
                        continue;
                    }
                    if existing.holder == holder {
                        continue;
                    }
                    // Conflict if either side is exclusive AND keys overlap
                    if (existing.mode.is_exclusive() || mode.is_exclusive())
                        && existing.key.conflicts(&key)
                    {
                        return Err(LeaseError::Conflict(format!(
                            "key group {} conflicts with existing lease held by {}",
                            group_id, existing.holder
                        )));
                    }
                }
            }
        }

        // Clean up expired leases for this group (inline housekeeping)
        if let Some(tokens) = group_index.get_mut(&group_id) {
            tokens.retain(|t| leases.get(t).map(|l| !l.is_expired()).unwrap_or(false));
        }

        let (token, epoch) = self.generate_token();
        let entry = LeaseEntry {
            key: key.clone(),
            holder: holder.to_string(),
            token: token.clone(),
            mode,
            acquired_at: now,
            expire_at,
            epoch,
        };

        leases.insert(token.clone(), entry.clone());
        group_index.entry(group_id).or_default().push(token.clone());

        // Update holder index
        let holder_entry = holder_index.entry(holder.to_string()).or_default();
        let is_new_holder = holder_entry.is_empty();
        holder_entry.insert(token.clone());
        if is_new_holder {
            self.holder_count.fetch_add(1, Ordering::Relaxed);
        }

        Ok(entry)
    }

    fn renew(&self, token: &str, holder: &str, duration: Duration) -> Result<(), LeaseError> {
        let mut leases = self.leases.write().unwrap();
        match leases.get_mut(token) {
            Some(entry) => {
                if entry.holder != holder {
                    return Err(LeaseError::HolderMismatch {
                        expected: entry.holder.clone(),
                        actual: holder.to_string(),
                    });
                }
                entry.expire_at = Instant::now() + duration;
                entry.epoch = self.epoch_counter.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            None => Err(LeaseError::NotFound),
        }
    }

    fn release(&self, token: &str, holder: &str) -> Result<(), LeaseError> {
        let group_id = {
            let mut leases = self.leases.write().unwrap();
            let entry = leases.get(token).ok_or(LeaseError::NotFound)?;

            if entry.holder != holder {
                return Err(LeaseError::HolderMismatch {
                    expected: entry.holder.clone(),
                    actual: holder.to_string(),
                });
            }

            let group_id = entry.key.group_id();
            leases.remove(token);
            group_id
        };

        // Update group index
        if let Some(tokens) = self.group_index.write().unwrap().get_mut(&group_id) {
            tokens.retain(|t| t != token);
        }

        // Update holder index
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

    fn validate_token(&self, token: &str, holder: &str) -> Result<(), LeaseError> {
        let leases = self.leases.read().unwrap();
        let entry = leases.get(token).ok_or(LeaseError::NotFound)?;

        if entry.is_expired() {
            return Err(LeaseError::Expired);
        }
        if entry.holder != holder {
            return Err(LeaseError::HolderMismatch {
                expected: entry.holder.clone(),
                actual: holder.to_string(),
            });
        }
        Ok(())
    }

    fn validate_token_with_grace(
        &self,
        token: &str,
        holder: &str,
        grace: Duration,
    ) -> Result<(), LeaseError> {
        let leases = self.leases.read().unwrap();
        let entry = leases.get(token).ok_or(LeaseError::NotFound)?;

        if entry.holder != holder {
            return Err(LeaseError::HolderMismatch {
                expected: entry.holder.clone(),
                actual: holder.to_string(),
            });
        }

        if Instant::now() > entry.expire_at + grace {
            return Err(LeaseError::ExpiredBeyondGrace);
        }
        Ok(())
    }

    fn get_entry(&self, token: &str) -> Option<LeaseEntry<K>> {
        self.leases.read().unwrap().get(token).cloned()
    }

    fn get_entries_by_group(&self, group_id: u64) -> Vec<LeaseEntry<K>> {
        let leases = self.leases.read().unwrap();
        let group_index = self.group_index.read().unwrap();
        let mut result = Vec::new();

        if let Some(tokens) = group_index.get(&group_id) {
            for token in tokens {
                if let Some(entry) = leases.get(token) {
                    if !entry.is_expired() {
                        result.push(entry.clone());
                    }
                }
            }
        }
        result
    }

    fn get_entries_by_holder(&self, holder: &str) -> Vec<LeaseEntry<K>> {
        let leases = self.leases.read().unwrap();
        let holder_index = self.holder_index.read().unwrap();
        let mut result = Vec::new();

        if let Some(tokens) = holder_index.get(holder) {
            for token in tokens {
                if let Some(entry) = leases.get(token) {
                    if !entry.is_expired() {
                        result.push(entry.clone());
                    }
                }
            }
        }
        result
    }

    fn disconnect_holder(&self, holder: &str) -> usize {
        let mut leases = self.leases.write().unwrap();
        let mut group_index = self.group_index.write().unwrap();
        let mut holder_index = self.holder_index.write().unwrap();

        let tokens_to_remove: Vec<String> = holder_index
            .get(holder)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default();

        let mut removed = 0usize;
        for token in tokens_to_remove {
            if let Some(entry) = leases.remove(&token) {
                removed += 1;
                let group_id = entry.key.group_id();
                if let Some(tokens) = group_index.get_mut(&group_id) {
                    tokens.retain(|t| t != &token);
                    if tokens.is_empty() {
                        group_index.remove(&group_id);
                    }
                }
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

    fn cleanup_expired(&self) -> usize {
        let mut leases = self.leases.write().unwrap();
        let mut group_index = self.group_index.write().unwrap();
        let mut holder_index = self.holder_index.write().unwrap();
        let mut removed = 0usize;

        // Only remove leases expired BEYOND the grace period.
        // Leases within the grace period are kept so that
        // validate_token_with_grace can still find them.
        let grace = self.cleanup_grace;
        let now = Instant::now();
        let expired_tokens: Vec<String> = leases
            .iter()
            .filter(|(_, e)| now > e.expire_at + grace)
            .map(|(t, _)| t.clone())
            .collect();

        for token in expired_tokens {
            if let Some(entry) = leases.remove(&token) {
                removed += 1;
                let group_id = entry.key.group_id();
                // Remove from group index
                if let Some(tokens) = group_index.get_mut(&group_id) {
                    tokens.retain(|t| t != &token);
                    if tokens.is_empty() {
                        group_index.remove(&group_id);
                    }
                }
                // Remove from holder index
                if let Some(tokens) = holder_index.get_mut(&entry.holder) {
                    tokens.remove(&token);
                    if tokens.is_empty() {
                        holder_index.remove(&entry.holder);
                        self.holder_count.fetch_sub(1, Ordering::Relaxed);
                    }
                }
            }
        }
        removed
    }

    fn active_count(&self) -> usize {
        self.leases
            .read()
            .unwrap()
            .values()
            .filter(|e| !e.is_expired())
            .count()
    }

    fn active_holders_count(&self) -> u64 {
        self.holder_count.load(Ordering::Relaxed)
    }

    fn shutdown_flag(&self) -> Arc<AtomicBool> {
        self.shutdown_flag.clone()
    }

    fn request_shutdown(&self) {
        self.shutdown_flag.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, PartialEq, Eq, Hash, Debug)]
    struct TestKey {
        id: u64,
        start: u64,
        count: u64,
    }

    impl LeaseKey for TestKey {
        fn group_id(&self) -> u64 {
            self.id
        }
        fn conflicts(&self, other: &Self) -> bool {
            if self.id != other.id {
                return false;
            }
            let self_end = self.start + self.count;
            let other_end = other.start + other.count;
            self.start < other_end && other.start < self_end
        }
    }

    #[test]
    fn test_acquire_and_release() {
        let store = MemoryLeaseStore::<TestKey>::new();
        let key = TestKey {
            id: 1,
            start: 0,
            count: 4,
        };
        let entry = store
            .acquire(
                key,
                "client-a",
                LeaseMode::Exclusive,
                Duration::from_secs(30),
            )
            .unwrap();
        assert_eq!(entry.holder, "client-a");

        store.release(&entry.token, "client-a").unwrap();
        assert_eq!(store.active_count(), 0);
    }

    #[test]
    fn test_conflict_detection() {
        let store = MemoryLeaseStore::<TestKey>::new();
        let key1 = TestKey {
            id: 1,
            start: 0,
            count: 4,
        };
        let key2 = TestKey {
            id: 1,
            start: 2,
            count: 4,
        };
        store
            .acquire(
                key1,
                "client-a",
                LeaseMode::Exclusive,
                Duration::from_secs(30),
            )
            .unwrap();
        let result = store.acquire(
            key2,
            "client-b",
            LeaseMode::Exclusive,
            Duration::from_secs(30),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_same_holder_no_conflict() {
        let store = MemoryLeaseStore::<TestKey>::new();
        let key1 = TestKey {
            id: 1,
            start: 0,
            count: 4,
        };
        let key2 = TestKey {
            id: 1,
            start: 2,
            count: 4,
        };
        let e1 = store
            .acquire(
                key1,
                "client-a",
                LeaseMode::Exclusive,
                Duration::from_secs(30),
            )
            .unwrap();
        let e2 = store
            .acquire(
                key2,
                "client-a",
                LeaseMode::Exclusive,
                Duration::from_secs(30),
            )
            .unwrap();
        assert_ne!(e1.token, e2.token);
    }

    #[test]
    fn test_non_overlapping_no_conflict() {
        let store = MemoryLeaseStore::<TestKey>::new();
        let key1 = TestKey {
            id: 1,
            start: 0,
            count: 4,
        };
        let key2 = TestKey {
            id: 1,
            start: 4,
            count: 4,
        };
        store
            .acquire(
                key1,
                "client-a",
                LeaseMode::Exclusive,
                Duration::from_secs(30),
            )
            .unwrap();
        let result = store.acquire(
            key2,
            "client-b",
            LeaseMode::Exclusive,
            Duration::from_secs(30),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_validation() {
        let store = MemoryLeaseStore::<TestKey>::new();
        let key = TestKey {
            id: 1,
            start: 0,
            count: 4,
        };
        let entry = store
            .acquire(
                key,
                "client-a",
                LeaseMode::Exclusive,
                Duration::from_secs(30),
            )
            .unwrap();

        assert!(store.validate_token(&entry.token, "client-a").is_ok());
        assert!(store.validate_token(&entry.token, "client-b").is_err());
        assert!(store.validate_token("bad-token", "client-a").is_err());
    }

    #[test]
    fn test_renew() {
        let store = MemoryLeaseStore::<TestKey>::new();
        let key = TestKey {
            id: 1,
            start: 0,
            count: 4,
        };
        let entry = store
            .acquire(
                key,
                "client-a",
                LeaseMode::Exclusive,
                Duration::from_millis(1000),
            )
            .unwrap();
        store
            .renew(&entry.token, "client-a", Duration::from_secs(30))
            .unwrap();
        assert_eq!(store.active_count(), 1);
    }

    #[test]
    fn test_expired_cleanup() {
        let store = MemoryLeaseStore::<TestKey>::new().with_cleanup_grace(Duration::from_millis(0));
        let key = TestKey {
            id: 1,
            start: 0,
            count: 4,
        };
        let _entry = store
            .acquire(
                key,
                "client-a",
                LeaseMode::Exclusive,
                Duration::from_millis(1),
            )
            .unwrap();
        std::thread::sleep(Duration::from_millis(10));
        let removed = store.cleanup_expired();
        assert!(removed >= 1);
        assert_eq!(store.active_count(), 0);
    }

    #[test]
    fn test_shared_lease_multiple_holders() {
        let store = MemoryLeaseStore::<TestKey>::new();
        let key1 = TestKey {
            id: 1,
            start: 0,
            count: 4,
        };
        let key2 = TestKey {
            id: 1,
            start: 0,
            count: 4,
        };
        // Shared (read) lease should allow multiple holders
        let _l1 = store
            .acquire(key1, "client-a", LeaseMode::Shared, Duration::from_secs(30))
            .unwrap();
        let _l2 = store
            .acquire(key2, "client-b", LeaseMode::Shared, Duration::from_secs(30))
            .unwrap();
        assert_eq!(store.active_count(), 2);
    }

    #[test]
    fn test_disconnect_holder() {
        let store = MemoryLeaseStore::<TestKey>::new();
        let key1 = TestKey {
            id: 1,
            start: 0,
            count: 4,
        };
        let key2 = TestKey {
            id: 2,
            start: 0,
            count: 4,
        };
        store
            .acquire(
                key1,
                "client-a",
                LeaseMode::Exclusive,
                Duration::from_secs(30),
            )
            .unwrap();
        store
            .acquire(
                key2,
                "client-a",
                LeaseMode::Exclusive,
                Duration::from_secs(30),
            )
            .unwrap();
        assert_eq!(store.active_count(), 2);

        let removed = store.disconnect_holder("client-a");
        assert_eq!(removed, 2);
        assert_eq!(store.active_count(), 0);
    }

    #[test]
    fn test_get_entries_by_group() {
        let store = MemoryLeaseStore::<TestKey>::new();
        let key1 = TestKey {
            id: 1,
            start: 0,
            count: 4,
        };
        let key2 = TestKey {
            id: 1,
            start: 4,
            count: 4,
        };
        let key3 = TestKey {
            id: 2,
            start: 0,
            count: 4,
        };
        store
            .acquire(
                key1,
                "client-a",
                LeaseMode::Exclusive,
                Duration::from_secs(30),
            )
            .unwrap();
        store
            .acquire(
                key2,
                "client-a",
                LeaseMode::Exclusive,
                Duration::from_secs(30),
            )
            .unwrap();
        store
            .acquire(
                key3,
                "client-a",
                LeaseMode::Exclusive,
                Duration::from_secs(30),
            )
            .unwrap();

        assert_eq!(store.get_entries_by_group(1).len(), 2);
        assert_eq!(store.get_entries_by_group(2).len(), 1);
        assert_eq!(store.get_entries_by_group(3).len(), 0);
    }
}
