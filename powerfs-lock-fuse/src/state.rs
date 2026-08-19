//! Client-side lease state cache — extracted from the FUSE client's
//! per-inode lease tracking.
//!
//! This is the "ClientLeaseState" mentioned in
//! `docs/lock-optimization-plan.md` §4.1 step 5. The conservative-adapter
//! strategy keeps `cache.rs::HoldState` (open_count refcount) in place;
//! this struct instead owns the **lease token + expiry** cache, which
//! previously lived inside `VolumeLeaseManager`'s private
//! `HashMap<LeaseKey, LeaseCacheEntry>`.
//!
//! # Why a separate struct?
//!
//! - Decouples lease state from the FUSE-specific `VolumeLeaseManager` so
//!   `FuseLockManager` can be unit-tested with a mock backend.
//! - Centralizes the "is my cached lease still valid?" fast path so both
//!   inode-mode and range-mode acquisitions share it.
//! - Gives Prometheus metrics a single source of truth for cache
//!   hit/miss counts.

use powerfs_lock::LockMode;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Cache entry for an inode-level lease (方案 A — Filer-managed).
#[derive(Clone, Debug)]
pub struct InodeLeaseEntry {
    pub token: String,
    pub expire_at: Instant,
    pub mode: LockMode,
}

/// Cache entry for a range-level lease (方案 D — Volume-managed).
#[derive(Clone, Debug)]
pub struct RangeLeaseEntry {
    pub token: String,
    pub expire_at: Instant,
    pub mode: LockMode,
    pub stripe_start: u64,
    pub stripe_count: u64,
    /// Volume hosting the inode. Stored at acquire time so that the
    /// `release(inode, token)` path can call
    /// `FuseLockBackend::release_range_lease` without an extra
    /// `lookup_volume_id` RPC.
    pub volume_id: u64,
}

/// Per-inode lease cache key for range-level leases.
/// (inode, stripe_start, stripe_count, exclusive)
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct RangeKey {
    inode: u64,
    stripe_start: u64,
    stripe_count: u64,
    exclusive: bool,
}

/// Client-side lease state — owns the per-inode and per-stripe lease
/// caches so `FuseLockManager` can answer cache hits without an RPC.
///
/// Thread-safe via `Mutex<HashMap>`; the critical sections are tiny
/// (lookup + insert), so a single mutex is sufficient. If contention
/// shows up in the baseline (阶段三), we can shard by inode.
#[derive(Default)]
pub struct ClientLeaseState {
    inode_leases: Mutex<HashMap<u64, InodeLeaseEntry>>,
    range_leases: Mutex<HashMap<RangeKey, RangeLeaseEntry>>,
}

impl ClientLeaseState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    // ---------- inode-level (方案 A) ----------

    /// Look up a cached inode lease. Returns `Some(entry)` only if the
    /// lease is still valid (non-expired).
    pub fn get_inode(&self, inode: u64) -> Option<InodeLeaseEntry> {
        let leases = self.inode_leases.lock().unwrap();
        leases.get(&inode).and_then(|e| {
            if Instant::now() < e.expire_at {
                Some(e.clone())
            } else {
                None
            }
        })
    }

    /// Insert / replace a cached inode lease.
    pub fn put_inode(&self, inode: u64, entry: InodeLeaseEntry) {
        let mut leases = self.inode_leases.lock().unwrap();
        leases.insert(inode, entry);
    }

    /// Remove a cached inode lease by token. Returns `true` if removed.
    pub fn remove_inode_by_token(&self, inode: u64, token: &str) -> bool {
        let mut leases = self.inode_leases.lock().unwrap();
        if let Some(entry) = leases.get(&inode) {
            if entry.token == token {
                leases.remove(&inode);
                return true;
            }
        }
        false
    }

    /// Compare-and-swap: replace the cached inode lease **only if the
    /// existing entry's token matches `old_token`**. Used by the
    /// Lockify async sync completion (phase 4 §5.1) to merge the
    /// server-issued token back into the cache without clobbering a
    /// concurrent acquire/release.
    ///
    /// Returns `true` if replaced. Returns `false` if:
    /// - the cache has no entry for `inode` (already released), or
    /// - the cached token differs from `old_token` (re-acquired by
    ///   another path, e.g. a regular `acquire` after invalidation).
    ///
    /// In both `false` cases the cache is left untouched — the caller
    /// should treat the server-issued token as orphaned (no client is
    /// tracking it; the server will eventually reclaim it via TTL or
    /// the next `release` RPC).
    pub fn replace_inode_by_token(
        &self,
        inode: u64,
        old_token: &str,
        new_entry: InodeLeaseEntry,
    ) -> bool {
        let mut leases = self.inode_leases.lock().unwrap();
        if let Some(existing) = leases.get(&inode) {
            if existing.token == old_token {
                leases.insert(inode, new_entry);
                return true;
            }
        }
        false
    }

    /// Invalidate (drop) the cached inode lease without notifying the
    /// server. Used when the server pushes an invalidate notification.
    pub fn invalidate_inode(&self, inode: u64) {
        self.inode_leases.lock().unwrap().remove(&inode);
    }

    /// Remaining duration of a cached inode lease, if any.
    pub fn remaining_inode(&self, inode: u64) -> Option<Duration> {
        let leases = self.inode_leases.lock().unwrap();
        leases
            .get(&inode)
            .map(|e| e.expire_at.saturating_duration_since(Instant::now()))
    }

    // ---------- range-level (方案 D) ----------

    /// Look up a cached range lease. `exclusive` should match the request
    /// mode (true for write, false for read).
    pub fn get_range(
        &self,
        inode: u64,
        stripe_start: u64,
        stripe_count: u64,
        exclusive: bool,
    ) -> Option<RangeLeaseEntry> {
        let key = RangeKey {
            inode,
            stripe_start,
            stripe_count,
            exclusive,
        };
        let leases = self.range_leases.lock().unwrap();
        leases.get(&key).and_then(|e| {
            if Instant::now() < e.expire_at {
                Some(e.clone())
            } else {
                None
            }
        })
    }

    /// Insert / replace a cached range lease with explicit inode.
    pub fn put_range_for_inode(
        &self,
        inode: u64,
        stripe_start: u64,
        stripe_count: u64,
        entry: RangeLeaseEntry,
    ) {
        let key = RangeKey {
            inode,
            stripe_start,
            stripe_count,
            exclusive: entry.mode.is_exclusive(),
        };
        let mut leases = self.range_leases.lock().unwrap();
        leases.insert(key, entry);
    }

    /// Remove all cached range leases for an inode (called on file
    /// close). Returns the list of `(stripe_start, token, volume_id)`
    /// triples so the caller can release them server-side without an
    /// extra `lookup_volume_id` RPC.
    pub fn drain_ranges_for_inode(&self, inode: u64) -> Vec<(u64, String, u64)> {
        let mut leases = self.range_leases.lock().unwrap();
        let keys_to_remove: Vec<RangeKey> = leases
            .keys()
            .filter(|k| k.inode == inode)
            .cloned()
            .collect();
        let mut result = Vec::with_capacity(keys_to_remove.len());
        for key in keys_to_remove {
            if let Some(entry) = leases.remove(&key) {
                result.push((key.stripe_start, entry.token, entry.volume_id));
            }
        }
        result
    }

    /// Find a cached range lease by token, returning the metadata
    /// needed for server-side release.
    ///
    /// Used by `FuseLockManager::release(inode, token)` when the token
    /// is not present in the inode cache (i.e. it's a range-level lease).
    /// Returns `(volume_id, stripe_start, stripe_count, exclusive)`.
    pub fn find_range_by_token(&self, token: &str) -> Option<(u64, u64, u64, bool)> {
        let leases = self.range_leases.lock().unwrap();
        for (key, entry) in leases.iter() {
            if entry.token == token {
                return Some((
                    entry.volume_id,
                    key.stripe_start,
                    key.stripe_count,
                    key.exclusive,
                ));
            }
        }
        None
    }

    /// Remove a single cached range lease by token. Returns the
    /// metadata needed for server-side release, or `None` if not found.
    pub fn remove_range_by_token(&self, token: &str) -> Option<(u64, u64, u64, bool)> {
        let mut leases = self.range_leases.lock().unwrap();
        let key_to_remove = leases
            .iter()
            .find(|(_, e)| e.token == token)
            .map(|(k, _)| k.clone());
        if let Some(key) = key_to_remove {
            if let Some(entry) = leases.remove(&key) {
                return Some((
                    entry.volume_id,
                    key.stripe_start,
                    key.stripe_count,
                    key.exclusive,
                ));
            }
        }
        None
    }

    // ---------- introspection (for metrics / tests) ----------

    /// Number of cached inode leases (including expired ones not yet
    /// swept).
    pub fn inode_cache_size(&self) -> usize {
        self.inode_leases.lock().unwrap().len()
    }

    /// Number of cached range leases (including expired ones not yet
    /// swept).
    pub fn range_cache_size(&self) -> usize {
        self.range_leases.lock().unwrap().len()
    }

    /// Sweep expired entries from both caches. Returns the number
    /// removed. Called lazily by `FuseLockManager` on acquire or
    /// periodically by a background task.
    pub fn sweep_expired(&self) -> usize {
        let now = Instant::now();
        let mut removed = 0;
        {
            let mut leases = self.inode_leases.lock().unwrap();
            let before = leases.len();
            leases.retain(|_, e| now < e.expire_at);
            removed += before - leases.len();
        }
        {
            let mut leases = self.range_leases.lock().unwrap();
            let before = leases.len();
            leases.retain(|_, e| now < e.expire_at);
            removed += before - leases.len();
        }
        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use powerfs_lock::Range;

    #[test]
    fn test_inode_cache_get_put_remove() {
        let state = ClientLeaseState::new();
        let inode = 42u64;
        assert!(state.get_inode(inode).is_none());

        state.put_inode(
            inode,
            InodeLeaseEntry {
                token: "tok-1".to_string(),
                expire_at: Instant::now() + Duration::from_secs(30),
                mode: LockMode::Exclusive,
            },
        );
        let got = state.get_inode(inode).unwrap();
        assert_eq!(got.token, "tok-1");

        assert!(!state.remove_inode_by_token(inode, "wrong"));
        // wrong token → not removed; entry still cached
        assert!(state.get_inode(inode).is_some());
        assert!(state.remove_inode_by_token(inode, "tok-1"));
        assert!(state.get_inode(inode).is_none());
    }

    #[test]
    fn test_inode_cache_expires() {
        let state = ClientLeaseState::new();
        let inode = 7u64;
        state.put_inode(
            inode,
            InodeLeaseEntry {
                token: "short".to_string(),
                expire_at: Instant::now() - Duration::from_secs(1), // already expired
                mode: LockMode::Shared,
            },
        );
        assert!(
            state.get_inode(inode).is_none(),
            "expired entry should not be returned"
        );
    }

    #[test]
    fn test_replace_inode_by_token_cas() {
        // Phase 4 §5.1 Lockify async sync CAS — see `lockify.rs`.
        let state = ClientLeaseState::new();
        let inode = 55u64;

        // No entry → CAS must fail (no clobber).
        assert!(!state.replace_inode_by_token(
            inode,
            "missing",
            InodeLeaseEntry {
                token: "new".to_string(),
                expire_at: Instant::now() + Duration::from_secs(30),
                mode: LockMode::Exclusive,
            },
        ));

        // Seed with a "local" token (Lockify fast path).
        state.put_inode(
            inode,
            InodeLeaseEntry {
                token: "local-1".to_string(),
                expire_at: Instant::now() + Duration::from_secs(30),
                mode: LockMode::Exclusive,
            },
        );

        // Wrong old_token → CAS must fail; cache unchanged.
        assert!(!state.replace_inode_by_token(
            inode,
            "wrong",
            InodeLeaseEntry {
                token: "server-1".to_string(),
                expire_at: Instant::now() + Duration::from_secs(30),
                mode: LockMode::Exclusive,
            },
        ));
        assert_eq!(state.get_inode(inode).unwrap().token, "local-1");

        // Correct old_token → CAS succeeds; cache now has the server token.
        assert!(state.replace_inode_by_token(
            inode,
            "local-1",
            InodeLeaseEntry {
                token: "server-1".to_string(),
                expire_at: Instant::now() + Duration::from_secs(60),
                mode: LockMode::Exclusive,
            },
        ));
        assert_eq!(state.get_inode(inode).unwrap().token, "server-1");
    }

    #[test]
    fn test_range_cache_drain_by_inode() {
        let state = ClientLeaseState::new();
        let inode = 100u64;
        let volume_id = 7u64;

        for stripe_start in [0, 64, 128] {
            state.put_range_for_inode(
                inode,
                stripe_start,
                64,
                RangeLeaseEntry {
                    token: format!("tok-{}", stripe_start),
                    expire_at: Instant::now() + Duration::from_secs(30),
                    mode: LockMode::Range(Range::new(stripe_start, Some(stripe_start + 64))),
                    stripe_start,
                    stripe_count: 64,
                    volume_id,
                },
            );
        }
        assert_eq!(state.range_cache_size(), 3);

        let drained = state.drain_ranges_for_inode(inode);
        assert_eq!(drained.len(), 3);
        // Each triple must echo the volume_id we stored at acquire time.
        for (_, _, vid) in &drained {
            assert_eq!(*vid, volume_id);
        }
        assert_eq!(state.range_cache_size(), 0);
    }

    #[test]
    fn test_find_range_by_token_returns_stored_metadata() {
        let state = ClientLeaseState::new();
        let inode = 42u64;
        let volume_id = 9u64;
        state.put_range_for_inode(
            inode,
            0,
            64,
            RangeLeaseEntry {
                token: "tok-x".to_string(),
                expire_at: Instant::now() + Duration::from_secs(30),
                mode: LockMode::Range(Range::new(0, Some(64))),
                stripe_start: 0,
                stripe_count: 64,
                volume_id,
            },
        );
        let found = state
            .find_range_by_token("tok-x")
            .expect("must find by token");
        assert_eq!(found, (volume_id, 0, 64, true));

        // Token not present
        assert!(state.find_range_by_token("nope").is_none());

        // remove_range_by_token must return the same metadata and clear
        let removed = state.remove_range_by_token("tok-x").expect("must remove");
        assert_eq!(removed, (volume_id, 0, 64, true));
        assert!(state.find_range_by_token("tok-x").is_none());
    }

    #[test]
    fn test_sweep_expired() {
        let state = ClientLeaseState::new();
        // One fresh, one expired
        state.put_inode(
            1,
            InodeLeaseEntry {
                token: "fresh".to_string(),
                expire_at: Instant::now() + Duration::from_secs(30),
                mode: LockMode::Shared,
            },
        );
        state.put_inode(
            2,
            InodeLeaseEntry {
                token: "stale".to_string(),
                expire_at: Instant::now() - Duration::from_secs(1),
                mode: LockMode::Shared,
            },
        );
        assert_eq!(state.inode_cache_size(), 2);
        let removed = state.sweep_expired();
        assert_eq!(removed, 1);
        assert_eq!(state.inode_cache_size(), 1);
    }
}
