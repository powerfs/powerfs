//! Phase-4 §5.2 (P3): Open-file lease binding.
//!
//! When a file is opened in inode-lease mode, the inode lease is
//! pre-acquired at `open()` time and bound to the open-file context
//! via this registry. Subsequent `flush_dirty_chunks` calls pass the
//! bound token to `write_blob_batch_with_lease`, bypassing
//! `ensure_lease`'s cache lookup + proactive-renew path on every
//! flush.
//!
//! On `release()` (close), the bound lease is invalidated from the
//! registry. The actual Filer-side release is handled by the existing
//! release path (`fuse.rs` release callback); the registry is just a
//! hint that mirrors the facade's `inode_lease_cache`.
//!
//! # Graceful degradation
//!
//! If the bound lease expires mid-session, `get_valid_token` returns
//! `None` and the caller falls through to `ensure_lease` (which
//! re-acquires or renews via the facade cache). The optimization is
//! opportunistic — correctness never depends on the registry.
//!
//! # Applicability
//!
//! Only inode-lease mode uses this registry. Range-lease mode (where
//! the lease depends on the write offset / stripe_start) cannot
//! pre-acquire at open time and is left untouched.
//!
//! See `docs/lock-optimization-plan.md` §6.2 (problem P3).

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Instant;

/// Registry of open-file lease bindings (phase 4 §5.2 / P3).
///
/// Thread-safe via `RwLock<HashMap>`. Reads (`get_valid_token`) take a
/// shared read lock; writes (`bind`/`take`/`invalidate`) take an
/// exclusive write lock. Contention is low because the registry is
/// per-FUSE-mount and the read path (flush_dirty_chunks) holds the
/// lock only for a HashMap lookup.
///
/// Stored as `Arc` so the FUSE admin server or background flusher can
/// hold a reference without borrowing from `PowerFsFs`.
#[derive(Clone)]
pub struct OpenFileLeaseRegistry {
    leases: Arc<RwLock<HashMap<u64, OpenFileLease>>>,
}

/// A single bound lease entry.
struct OpenFileLease {
    token: String,
    expire_at: Instant,
}

impl OpenFileLease {
    fn is_valid(&self) -> bool {
        Instant::now() < self.expire_at
    }
}

impl OpenFileLeaseRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            leases: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Bind a lease token to an open inode. Called at `open()` time
    /// after successfully acquiring the inode lease from the Filer.
    ///
    /// If an entry already exists for this inode (e.g. a concurrent
    /// open raced ahead), the new token replaces it — the latest
    /// acquire wins, and the old token is orphaned server-side
    /// (reclaimed via TTL or the next `release` RPC).
    pub fn bind(&self, inode: u64, token: String, expire_at: Instant) {
        let mut leases = self.leases.write().unwrap();
        leases.insert(inode, OpenFileLease { token, expire_at });
    }

    /// Get the bound lease token if it's still valid. Returns `None`
    /// if no lease is bound for this inode or the bound lease has
    /// expired (caller should fall through to `ensure_lease`).
    pub fn get_valid_token(&self, inode: u64) -> Option<String> {
        let leases = self.leases.read().unwrap();
        leases.get(&inode).and_then(|entry| {
            if entry.is_valid() {
                Some(entry.token.clone())
            } else {
                None
            }
        })
    }

    /// Remove and return the bound lease token. Called when the
    /// caller wants to explicitly release the lease on the Filer
    /// (e.g. last close). Returns `None` if no lease is bound.
    pub fn take(&self, inode: u64) -> Option<String> {
        let mut leases = self.leases.write().unwrap();
        leases.remove(&inode).map(|entry| entry.token)
    }

    /// Remove the bound lease without returning the token. Called
    /// when the registry hint should be cleared but the actual
    /// release is handled elsewhere (e.g. the release callback's
    /// existing inode-lease-release logic).
    pub fn invalidate(&self, inode: u64) {
        let mut leases = self.leases.write().unwrap();
        leases.remove(&inode);
    }

    /// Number of bound leases (for tests / diagnostics).
    pub fn len(&self) -> usize {
        let leases = self.leases.read().unwrap();
        leases.len()
    }

    /// Whether the registry is empty (for tests).
    pub fn is_empty(&self) -> bool {
        let leases = self.leases.read().unwrap();
        leases.is_empty()
    }
}

impl Default for OpenFileLeaseRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_bind_and_get_valid_token() {
        let reg = OpenFileLeaseRegistry::new();
        let inode = 42u64;
        let token = "server-tok-1".to_string();
        let expire_at = Instant::now() + Duration::from_secs(30);

        // No entry → None.
        assert!(reg.get_valid_token(inode).is_none());

        reg.bind(inode, token.clone(), expire_at);

        // Valid entry → returns the token.
        let got = reg.get_valid_token(inode).expect("must be valid");
        assert_eq!(got, token);
    }

    #[test]
    fn test_get_valid_token_returns_none_when_expired() {
        let reg = OpenFileLeaseRegistry::new();
        let inode = 7u64;
        let expire_at = Instant::now() - Duration::from_secs(1); // already expired

        reg.bind(inode, "expired-tok".to_string(), expire_at);

        assert!(
            reg.get_valid_token(inode).is_none(),
            "expired lease must not be returned"
        );
    }

    #[test]
    fn test_take_removes_and_returns_token() {
        let reg = OpenFileLeaseRegistry::new();
        let inode = 99u64;
        let token = "server-tok-99".to_string();
        let expire_at = Instant::now() + Duration::from_secs(30);

        reg.bind(inode, token.clone(), expire_at);

        let taken = reg.take(inode).expect("must take");
        assert_eq!(taken, token);

        // After take, get_valid_token returns None.
        assert!(reg.get_valid_token(inode).is_none());
    }

    #[test]
    fn test_take_returns_none_for_unbound_inode() {
        let reg = OpenFileLeaseRegistry::new();
        assert!(reg.take(123).is_none());
    }

    #[test]
    fn test_invalidate_removes_entry() {
        let reg = OpenFileLeaseRegistry::new();
        let inode = 55u64;
        reg.bind(
            inode,
            "tok-55".to_string(),
            Instant::now() + Duration::from_secs(30),
        );
        assert!(reg.get_valid_token(inode).is_some());

        reg.invalidate(inode);
        assert!(reg.get_valid_token(inode).is_none());
    }

    #[test]
    fn test_invalidate_is_noop_for_unbound_inode() {
        let reg = OpenFileLeaseRegistry::new();
        // Should not panic.
        reg.invalidate(999);
        assert!(reg.is_empty());
    }

    #[test]
    fn test_bind_replaces_existing_entry() {
        let reg = OpenFileLeaseRegistry::new();
        let inode = 1u64;

        reg.bind(
            inode,
            "old-tok".to_string(),
            Instant::now() + Duration::from_secs(30),
        );
        reg.bind(
            inode,
            "new-tok".to_string(),
            Instant::now() + Duration::from_secs(30),
        );

        let got = reg.get_valid_token(inode).expect("must be valid");
        assert_eq!(got, "new-tok");
    }

    #[test]
    fn test_len_and_is_empty() {
        let reg = OpenFileLeaseRegistry::new();
        assert!(reg.is_empty());
        assert_eq!(reg.len(), 0);

        reg.bind(
            1,
            "t1".to_string(),
            Instant::now() + Duration::from_secs(30),
        );
        reg.bind(
            2,
            "t2".to_string(),
            Instant::now() + Duration::from_secs(30),
        );
        assert_eq!(reg.len(), 2);
        assert!(!reg.is_empty());

        reg.invalidate(1);
        assert_eq!(reg.len(), 1);
        reg.invalidate(2);
        assert!(reg.is_empty());
    }

    #[test]
    fn test_clone_shares_state() {
        // `Arc<RwLock<HashMap>>` → clone shares the underlying map.
        let reg = OpenFileLeaseRegistry::new();
        let reg2 = reg.clone();

        reg.bind(
            42,
            "shared".to_string(),
            Instant::now() + Duration::from_secs(30),
        );
        assert_eq!(reg2.get_valid_token(42).unwrap(), "shared");
        assert_eq!(reg.len(), reg2.len());
    }
}
