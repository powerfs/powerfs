//! Fencer: epoch-based fencing token to prevent zombie-client writes after
//! split-brain (§8.3 point 2, §7.3 problem P6).
//!
//! # Model
//!
//! Every lease request carries the client's `epoch`. The filer's lease store
//! calls [`Fencer::validate`] on every acquire: a request whose epoch is
//! strictly less than the client's current registered epoch is rejected as a
//! zombie/replay, preventing a stale (presumed-dead) client from continuing
//! to write.
//!
//! ```text
//!  client startup ──► Fencer::register(client) ─► returns new epoch E
//!  lease acquire ──► carries (client, E) ─► Fencer::validate(client, E)
//!                                                ├─ Ok  → grant
//!                                                └─ Err(StaleEpoch) → reject (zombie)
//!                                                   └─ Err(NotRegistered) → reject (must register)
//!
//!  leader change  ──► Fencer::bump_all() ─► clears all client epochs
//!                     (forces every client to re-register; any in-flight
//!                      lease with the old epoch becomes NotRegistered)
//! ```
//!
//! Epochs are drawn from a process-global monotonic [`AtomicU64`], so each
//! registration yields a strictly larger number than any previous one — a
//! restarted client always outranks its presumed-dead predecessor.
//!
//! # Why not a global floor?
//!
//! A global "minimum valid epoch" would let a zombie legitimize itself by
//! simply calling `register`. Instead, `bump_all` clears the per-client map,
//! so a zombie that does not re-register keeps failing `validate` with
//! `NotRegistered`. A live client re-registers and gets a fresh epoch —
//! that is the intended recovery path (§8.3: "客户端宕机后重启必须先
//! 申请新 epoch").

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Errors returned by [`Fencer::validate`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FencerError {
    /// The client has no registered epoch (never registered, or evicted by a
    /// `bump_all` after a leader change). The client must call
    /// [`Fencer::register`] before issuing lease requests.
    #[error("client not registered (epoch required): {0}")]
    NotRegistered(String),

    /// The presented epoch is strictly less than the client's current
    /// registered epoch — a zombie/replayed request from a presumed-dead
    /// client incarnation (§8.3 point 2). Reject the lease.
    #[error("stale epoch for client {client}: presented {presented} < current {current}")]
    StaleEpoch {
        client: String,
        presented: u64,
        current: u64,
    },
}

/// Epoch-based fencing token manager (§8.3 point 2, §7.3 P6).
///
/// Held by the filer leader. The lease store consults [`Fencer::validate`]
/// on every `acquire` to reject stale-epoch (zombie) requests. On leader
/// change, the new leader calls [`Fencer::bump_all`] to invalidate every
/// outstanding epoch and force clients to re-register.
///
/// Thread-safe: the per-client map lives behind a `std::sync::Mutex`; the
/// global counter is an `AtomicU64`. Methods are synchronous because they
/// are fast and called inline from the lease acquire path.
pub struct Fencer {
    /// Monotonic source of new epochs. Every `register`/`bump_*` draws from
    /// this counter so a restarted client always outranks its predecessor.
    global_epoch: AtomicU64,
    /// Per-client currently-valid epoch. Absent entry ⇒ client must
    /// register before any lease request is honored.
    client_epochs: Mutex<HashMap<String, u64>>,
}

impl Fencer {
    /// Construct an empty fencer with the global counter starting at 0.
    /// The first `register` returns epoch 1.
    pub fn new() -> Self {
        Self {
            global_epoch: AtomicU64::new(0),
            client_epochs: Mutex::new(HashMap::new()),
        }
    }

    /// Register (or re-register) a client, returning its new epoch.
    ///
    /// Called on client startup and after a leader change (when the client
    /// discovers its prior epoch is no longer valid). The returned epoch is
    /// strictly greater than every previously-issued epoch, so a restarted
    /// client automatically supersedes its presumed-dead predecessor.
    pub fn register(&self, client_id: &str) -> u64 {
        let epoch = self.global_epoch.fetch_add(1, Ordering::Relaxed) + 1;
        let mut map = self.client_epochs.lock().unwrap();
        map.insert(client_id.to_string(), epoch);
        epoch
    }

    /// Validate a lease request's epoch.
    ///
    /// - `Ok(())` if `presented` equals the client's current registered epoch.
    /// - `Err(NotRegistered)` if the client has no current epoch (must
    ///   `register` first — e.g. after a `bump_all` leader change).
    /// - `Err(StaleEpoch)` if `presented` is strictly less than the current
    ///   epoch — a zombie/replayed request (§8.3 point 2).
    ///
    /// A `presented` value greater than the stored epoch is impossible under
    /// correct usage (the filer only ever stores epochs it issued), so it is
    /// also treated as stale for defense-in-depth.
    pub fn validate(&self, client_id: &str, presented: u64) -> Result<(), FencerError> {
        let map = self.client_epochs.lock().unwrap();
        let current = map
            .get(client_id)
            .copied()
            .ok_or_else(|| FencerError::NotRegistered(client_id.to_string()))?;
        if presented == current {
            Ok(())
        } else {
            Err(FencerError::StaleEpoch {
                client: client_id.to_string(),
                presented,
                current,
            })
        }
    }

    /// Force a single client to re-register (e.g. the filer detected a
    /// zombie incarnation still holding a lease). Returns the new epoch.
    /// The client's old epoch immediately fails `validate`.
    pub fn bump_client(&self, client_id: &str) -> u64 {
        let epoch = self.global_epoch.fetch_add(1, Ordering::Relaxed) + 1;
        let mut map = self.client_epochs.lock().unwrap();
        map.insert(client_id.to_string(), epoch);
        epoch
    }

    /// Invalidate every outstanding epoch (leader change / split-brain
    /// recovery, §7.3 P6). All subsequent `validate` calls return
    /// `NotRegistered` until clients re-register. Returns the new global
    /// epoch floor (informational; clients get their actual epoch from
    /// `register`).
    pub fn bump_all(&self) -> u64 {
        let epoch = self.global_epoch.fetch_add(1, Ordering::Relaxed) + 1;
        let mut map = self.client_epochs.lock().unwrap();
        map.clear();
        epoch
    }

    /// The client's currently-registered epoch, or `None` if not registered.
    pub fn current_epoch(&self, client_id: &str) -> Option<u64> {
        let map = self.client_epochs.lock().unwrap();
        map.get(client_id).copied()
    }

    /// `true` if the client has a registered epoch.
    pub fn is_registered(&self, client_id: &str) -> bool {
        self.current_epoch(client_id).is_some()
    }

    /// Number of clients with a currently-valid epoch (for metrics).
    pub fn registered_count(&self) -> usize {
        let map = self.client_epochs.lock().unwrap();
        map.len()
    }

    /// The global epoch counter's current value (for metrics / leader-change
    /// sanity checks). This is the next-to-be-issued epoch minus one.
    pub fn global_epoch(&self) -> u64 {
        self.global_epoch.load(Ordering::Relaxed)
    }
}

impl Default for Fencer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_returns_monotonic_epochs() {
        let f = Fencer::new();
        let e1 = f.register("c1");
        let e2 = f.register("c2");
        let e3 = f.register("c1");
        assert!(e1 > 0);
        assert!(e2 > e1, "each register must strictly increase");
        assert!(e3 > e2, "re-register must outrank all prior");
    }

    #[test]
    fn test_validate_accepts_current_epoch() {
        let f = Fencer::new();
        let e = f.register("c1");
        assert_eq!(f.validate("c1", e), Ok(()));
    }

    #[test]
    fn test_validate_rejects_unregistered() {
        let f = Fencer::new();
        assert_eq!(
            f.validate("ghost", 1),
            Err(FencerError::NotRegistered("ghost".to_string()))
        );
    }

    #[test]
    fn test_validate_rejects_stale_epoch_after_re_register() {
        let f = Fencer::new();
        let old = f.register("c1"); // client's "old incarnation"
                                    // Simulate restart: re-register supersedes the old epoch.
        let new = f.register("c1");
        assert!(new > old);
        // The old (zombie) epoch is now rejected.
        assert_eq!(
            f.validate("c1", old),
            Err(FencerError::StaleEpoch {
                client: "c1".to_string(),
                presented: old,
                current: new,
            })
        );
        // The new epoch is accepted.
        assert_eq!(f.validate("c1", new), Ok(()));
    }

    #[test]
    fn test_bump_client_invalidates_old_epoch() {
        let f = Fencer::new();
        let e = f.register("c1");
        assert_eq!(f.validate("c1", e), Ok(()));
        let new = f.bump_client("c1");
        assert!(new > e);
        // Old epoch now stale.
        assert!(matches!(
            f.validate("c1", e),
            Err(FencerError::StaleEpoch { .. })
        ));
        assert_eq!(f.validate("c1", new), Ok(()));
    }

    #[test]
    fn test_bump_all_clears_everyone() {
        let f = Fencer::new();
        let _ = f.register("c1");
        let _ = f.register("c2");
        assert!(f.is_registered("c1"));
        assert!(f.is_registered("c2"));
        let floor = f.bump_all();
        // Everyone is now NotRegistered — must re-register.
        assert!(!f.is_registered("c1"));
        assert!(!f.is_registered("c2"));
        assert!(matches!(
            f.validate("c1", floor),
            Err(FencerError::NotRegistered(_))
        ));
        // After re-register, the new epoch exceeds the floor.
        let e = f.register("c1");
        assert!(e > floor);
        assert_eq!(f.validate("c1", e), Ok(()));
    }

    #[test]
    fn test_presented_greater_than_current_is_rejected() {
        // Defense-in-depth: a presented epoch larger than the stored one is
        // impossible under correct usage and is treated as stale.
        let f = Fencer::new();
        let e = f.register("c1");
        assert!(matches!(
            f.validate("c1", e + 1),
            Err(FencerError::StaleEpoch { .. })
        ));
    }

    #[test]
    fn test_registered_count_and_global_epoch() {
        let f = Fencer::new();
        assert_eq!(f.registered_count(), 0);
        assert_eq!(f.global_epoch(), 0);
        let _ = f.register("c1");
        let _ = f.register("c2");
        assert_eq!(f.registered_count(), 2);
        assert_eq!(f.global_epoch(), 2);
        f.bump_all();
        assert_eq!(f.registered_count(), 0);
        assert_eq!(f.global_epoch(), 3);
    }

    #[test]
    fn test_cross_client_independence() {
        let f = Fencer::new();
        let a = f.register("a");
        let b = f.register("b");
        // Bumping A does not affect B.
        let a2 = f.bump_client("a");
        assert_eq!(f.validate("a", a2), Ok(()));
        assert_eq!(f.validate("b", b), Ok(()));
        assert!(matches!(
            f.validate("a", a),
            Err(FencerError::StaleEpoch { .. })
        ));
    }
}
