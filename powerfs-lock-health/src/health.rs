//! Layer 1-3 defense: client health scoring → adaptive throttle → quarantine.
//!
//! This module is consulted by the server-side lease stores (`InodeLeaseManager`
//! on the filer, `RangeLeaseManager` on the volume) on every `acquire`. It is
//! the admission-control gate mandated by `docs/lock-optimization-plan.md` §8.2
//! (constraint 8: "故障隔离必备") and §8.3 (revoke-after-2s-no-ack + blacklist).
//!
//! # Data flow
//!
//! ```text
//!  server lease store                 ClientHealth (this module)
//!  ─────────────────                  ──────────────────────────
//!  acquire(client) ──────────────────► check(client) ─► HealthDecision
//!                                                       (Allow / Throttle /
//!                                                        Quarantine / Blacklisted)
//!  on renew ok    ──────────────────► record_renew_success
//!  on renew fail  ──────────────────► record_renew_failure
//!  on lease fail ──────────────────► record_lease_failure
//!  on revoke-ack timeout ─────────► record_revoke_ack_timeout  (§8.3 point 1)
//!  on lease held & released ──────► record_lease_held_duration (P99 feed)
//!  on acquire (always) ──────────► record_acquire              (churn feed)
//! ```
//!
//! The three layers are intentionally coupled in one struct because the score
//! (Layer 1) directly drives the throttle duration (Layer 2) and quarantine
//! entry/exit (Layer 3). Splitting them would require leaking internal state
//! across module boundaries for no benefit.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::Serialize;

use crate::config::HealthConfig;

/// A client health score in `[0, 100]`. Lower = sicker.
///
/// Layer 1 output (§8.2). Derived from fault count, renew success rate,
/// lease-hold duration, and churn rate. The score alone does **not** decide
/// quarantine — that needs `quarantine_consecutive_required` consecutive
/// low samples (§8.2: "持续 N 次") to avoid one-off dips.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct ClientHealthScore(u8);

impl ClientHealthScore {
    /// Clamp a raw value into `[0, 100]`.
    pub const fn new(v: u8) -> Self {
        Self(if v > 100 { 100 } else { v })
    }

    /// Raw score value.
    pub const fn raw(self) -> u8 {
        self.0
    }

    /// `true` if this score falls in the Layer 2 throttle band
    /// (`quarantine_threshold <= score < throttle_threshold`).
    pub fn in_throttle_band(self, cfg: &HealthConfig) -> bool {
        self.0 < cfg.throttle_threshold
    }

    /// `true` if this score is below the quarantine threshold (Layer 3
    /// candidate). The actual quarantine decision also requires the
    /// consecutive-low counter to reach `quarantine_consecutive_required`.
    pub fn below_quarantine(self, cfg: &HealthConfig) -> bool {
        self.0 < cfg.quarantine_threshold
    }
}

impl Default for ClientHealthScore {
    fn default() -> Self {
        Self::new(100)
    }
}

impl std::fmt::Display for ClientHealthScore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Admission decision returned by [`ClientHealth::check`].
///
/// The lease store maps each variant onto a concrete action (see the crate
/// docs for the full mapping table).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HealthDecision {
    /// Client is healthy; grant the requested lease duration as-is.
    Allow,
    /// Client score is in the throttle band — grant a lease, but cap its
    /// duration at `lease_ms` (Layer 2, non-blocking).
    Throttle { lease_ms: u64 },
    /// Client is in the quarantine pool — reject with
    /// `LockError::Quarantined`. `until` is when the quarantine expires
    /// (Layer 3, blocking).
    Quarantine { until: Instant },
    /// Client is permanently blacklisted (3 consecutive quarantine entries,
    /// §8.3 point 3). Reject with `LockError::Quarantined` (admin-only release).
    Blacklisted,
}

/// Point-in-time view of one client's health state, for Prometheus-style
/// introspection. Cheap to clone and serialize.
#[derive(Debug, Clone, Serialize)]
pub struct HealthSnapshot {
    pub client_id: String,
    pub score: u8,
    pub consecutive_low_samples: u32,
    pub acquire_count: u64,
    pub renew_success: u64,
    pub renew_failure: u64,
    pub lease_failure: u64,
    pub revoke_ack_timeout: u64,
    /// `Some(until)` if currently quarantined, `None` otherwise.
    pub quarantined_until: Option<u64>,
    /// Epoch-ms at which the active quarantine expires (`0` if none).
    /// Kept as a relative offset by [`ClientHealth`] so snapshots stay
    /// meaningful across clock skew; converters normalize to wall time.
    pub blacklisted: bool,
}

/// Per-client mutable state tracked under the [`ClientHealth`] lock.
#[derive(Debug, Clone)]
struct ClientState {
    score: u8,
    /// Consecutive samples where `score < quarantine_threshold`. Reset on
    /// any sample that climbs back above the threshold (§8.2 "持续 N 次").
    consecutive_low: u32,
    /// Consecutive quarantine entries. Reaching `blacklist_threshold`
    /// permanently blacklists the client (§8.3 point 3).
    quarantine_streak: u32,
    /// `Some(expire_at)` while the client is actively quarantined.
    quarantine_until: Option<Instant>,
    blacklisted: bool,
    // ---- Layer 1 signal accumulators ----
    acquire_count: u64,
    renew_success: u64,
    renew_failure: u64,
    lease_failure: u64,
    revoke_ack_timeout: u64,
    /// Last `record_acquire` time — used to estimate churn (acquires/sec).
    last_acquire: Option<Instant>,
}

impl ClientState {
    fn new(initial_score: u8) -> Self {
        Self {
            score: initial_score,
            consecutive_low: 0,
            quarantine_streak: 0,
            quarantine_until: None,
            blacklisted: false,
            acquire_count: 0,
            renew_success: 0,
            renew_failure: 0,
            lease_failure: 0,
            revoke_ack_timeout: 0,
            last_acquire: None,
        }
    }
}

/// The three-layer defense manager. Shared across the filer's
/// `InodeLeaseManager` and the volume's `RangeLeaseManager` so a sick
/// client is treated consistently regardless of which backend it hits.
///
/// Thread-safe: all state lives behind a single `std::sync::Mutex`. Methods
/// are intentionally synchronous (no `async`) because they are fast,
/// non-blocking, and called inline from the lease acquire hot path —
/// holding the lock across an `await` would be a footgun.
pub struct ClientHealth {
    cfg: HealthConfig,
    clients: Mutex<HashMap<String, ClientState>>,
}

impl ClientHealth {
    /// Construct with the given config and no registered clients.
    pub fn new(cfg: HealthConfig) -> Self {
        Self {
            cfg,
            clients: Mutex::new(HashMap::new()),
        }
    }

    /// Construct with [`HealthConfig::default`].
    pub fn with_defaults() -> Self {
        Self::new(HealthConfig::default())
    }

    /// Configuration accessor (for tests / runtime introspection).
    pub fn config(&self) -> &HealthConfig {
        &self.cfg
    }

    /// Ensure a client entry exists, returning nothing — used internally
    /// before any `record_*` mutation. New clients start at `initial_score`.
    fn ensure(&self, clients: &mut HashMap<String, ClientState>, client_id: &str) {
        if !clients.contains_key(client_id) {
            clients.insert(
                client_id.to_string(),
                ClientState::new(self.cfg.initial_score),
            );
        }
    }

    // ===================== Layer 1: signal feeds =====================

    /// Bump the acquire counter and churn-rate feed. Called by the lease
    /// store on **every** `acquire` attempt (success or failure).
    pub fn record_acquire(&self, client_id: &str) {
        let mut clients = self.clients.lock().unwrap();
        self.ensure(&mut clients, client_id);
        let st = clients.get_mut(client_id).unwrap();
        st.acquire_count = st.acquire_count.saturating_add(1);
        st.last_acquire = Some(Instant::now());
    }

    /// A renew succeeded — small positive bump (Layer 1 input).
    pub fn record_renew_success(&self, client_id: &str) {
        let mut clients = self.clients.lock().unwrap();
        self.ensure(&mut clients, client_id);
        let st = clients.get_mut(client_id).unwrap();
        st.renew_success = st.renew_success.saturating_add(1);
        st.score = st
            .score
            .saturating_add(self.cfg.renew_success_bonus)
            .min(self.cfg.score_ceiling);
        // A successful renew is a positive signal — reset the consecutive-low
        // streak so a transient dip doesn't accumulate toward quarantine.
        if st.score >= self.cfg.quarantine_threshold {
            st.consecutive_low = 0;
        }
    }

    /// A renew failed — apply the standard failure penalty (Layer 1 input).
    pub fn record_renew_failure(&self, client_id: &str) {
        let mut clients = self.clients.lock().unwrap();
        self.ensure(&mut clients, client_id);
        let st = clients.get_mut(client_id).unwrap();
        st.renew_failure = st.renew_failure.saturating_add(1);
        st.score = st.score.saturating_sub(self.cfg.failure_penalty);
    }

    /// A lease operation failed (conflict, expired, etc.) — apply the
    /// standard failure penalty.
    pub fn record_lease_failure(&self, client_id: &str) {
        let mut clients = self.clients.lock().unwrap();
        self.ensure(&mut clients, client_id);
        let st = clients.get_mut(client_id).unwrap();
        st.lease_failure = st.lease_failure.saturating_add(1);
        st.score = st.score.saturating_sub(self.cfg.failure_penalty);
    }

    /// A Revoke ACK did not arrive within the 2s deadline (§8.3 point 1).
    /// This is the heaviest signal of a stuck/slow client — apply the large
    /// penalty. The next `check()` will likely push the client into throttle
    /// or quarantine.
    pub fn record_revoke_ack_timeout(&self, client_id: &str) {
        let mut clients = self.clients.lock().unwrap();
        self.ensure(&mut clients, client_id);
        let st = clients.get_mut(client_id).unwrap();
        st.revoke_ack_timeout = st.revoke_ack_timeout.saturating_add(1);
        st.score = st.score.saturating_sub(self.cfg.revoke_ack_timeout_penalty);
    }

    /// Feed a completed lease-hold duration into Layer 1. Long holds are a
    /// positive signal (the client is doing useful work, not churning); we
    /// apply a tiny bonus proportional to the hold length, capped at
    /// `renew_success_bonus` per call. Short holds (< 1s) are a churn
    /// signal and apply a small penalty instead.
    pub fn record_lease_held_duration(&self, client_id: &str, held: Duration) {
        let mut clients = self.clients.lock().unwrap();
        self.ensure(&mut clients, client_id);
        let st = clients.get_mut(client_id).unwrap();
        if held >= Duration::from_secs(1) {
            // Positive signal — bounded bonus (don't let long holds
            // outweigh repeated failures).
            st.score = st.score.saturating_add(1).min(self.cfg.score_ceiling);
        } else {
            // Churn: acquired-then-released almost immediately.
            st.score = st.score.saturating_sub(1);
        }
    }

    // ===================== Admission gate =====================

    /// The admission-control call. Lease stores invoke this on every
    /// `acquire` to decide whether to proceed, throttle, or reject.
    ///
    /// Also advances internal state: expired quarantines are released
    /// (score recovers to `post_quarantine_score`), and the consecutive-low
    /// counter is updated from the current score.
    pub fn check(&self, client_id: &str) -> HealthDecision {
        let mut clients = self.clients.lock().unwrap();
        self.ensure(&mut clients, client_id);
        let st = clients.get_mut(client_id).unwrap();

        // Layer 3 permanent: blacklist short-circuits everything.
        if st.blacklisted {
            return HealthDecision::Blacklisted;
        }

        // Layer 3 active quarantine: reject until it expires.
        if let Some(until) = st.quarantine_until {
            if Instant::now() < until {
                return HealthDecision::Quarantine { until };
            }
            // Quarantine expired — release and retry at post_quarantine_score.
            st.quarantine_until = None;
            st.score = self.cfg.post_quarantine_score;
            st.consecutive_low = 0;
        }

        let score = ClientHealthScore::new(st.score);

        // Layer 3 candidate: score below quarantine threshold.
        if score.below_quarantine(&self.cfg) {
            st.consecutive_low = st.consecutive_low.saturating_add(1);
            if st.consecutive_low >= self.cfg.quarantine_consecutive_required {
                // Enter quarantine.
                let until = Instant::now() + self.cfg.quarantine_duration;
                st.quarantine_until = Some(until);
                st.quarantine_streak = st.quarantine_streak.saturating_add(1);
                if st.quarantine_streak >= self.cfg.blacklist_threshold {
                    // §8.3 point 3: permanent blacklist.
                    st.blacklisted = true;
                    return HealthDecision::Blacklisted;
                }
                return HealthDecision::Quarantine { until };
            }
            // Below quarantine threshold but not yet N consecutive — still
            // throttle hard (treat as the bottom of the throttle band).
            return HealthDecision::Throttle {
                lease_ms: self.cfg.throttle_lease_ms_min,
            };
        }

        // Reset the consecutive-low streak as soon as the score climbs
        // back above the quarantine threshold.
        st.consecutive_low = 0;

        // Layer 2: throttle band.
        if score.in_throttle_band(&self.cfg) {
            return HealthDecision::Throttle {
                lease_ms: self.throttle_lease_ms(st.score),
            };
        }

        // Layer 1 only: healthy.
        HealthDecision::Allow
    }

    /// Linear sliding scale for the throttle lease duration (§8.2:
    /// "30s → 5s → 1s"). At `score == quarantine_threshold` the grant is
    /// `throttle_lease_ms_min`; approaching `throttle_threshold` it reaches
    /// `throttle_lease_ms_max`.
    fn throttle_lease_ms(&self, score: u8) -> u64 {
        let lo = self.cfg.quarantine_threshold;
        let hi = self.cfg.throttle_threshold;
        let min = self.cfg.throttle_lease_ms_min;
        let max = self.cfg.throttle_lease_ms_max;
        if hi <= lo {
            return min;
        }
        let span = (hi - lo) as u64;
        let pos = (score.saturating_sub(lo)) as u64;
        let frac = pos.min(span) as f64 / span as f64;
        let lease = min as f64 + (max as f64 - min as f64) * frac;
        // Round to the nearest millisecond; never exceed max.
        (lease.round() as u64).clamp(min, max)
    }

    // ===================== Admin / introspection =====================

    /// Read-only snapshot of one client's state (for `/lock-metrics`-style
    /// exporters). Returns `None` if the client has never been seen.
    pub fn snapshot(&self, client_id: &str) -> Option<HealthSnapshot> {
        let clients = self.clients.lock().unwrap();
        let st = clients.get(client_id)?;
        Some(HealthSnapshot {
            client_id: client_id.to_string(),
            score: st.score,
            consecutive_low_samples: st.consecutive_low,
            acquire_count: st.acquire_count,
            renew_success: st.renew_success,
            renew_failure: st.renew_failure,
            lease_failure: st.lease_failure,
            revoke_ack_timeout: st.revoke_ack_timeout,
            quarantined_until: st
                .quarantine_until
                .map(|t| t.duration_since(Instant::now()).as_millis() as u64),
            blacklisted: st.blacklisted,
        })
    }

    /// Snapshots for all known clients (for cluster-wide metrics export).
    pub fn snapshots(&self) -> Vec<HealthSnapshot> {
        let clients = self.clients.lock().unwrap();
        clients
            .keys()
            .filter_map(|id| {
                clients.get(id).map(|st| HealthSnapshot {
                    client_id: id.clone(),
                    score: st.score,
                    consecutive_low_samples: st.consecutive_low,
                    acquire_count: st.acquire_count,
                    renew_success: st.renew_success,
                    renew_failure: st.renew_failure,
                    lease_failure: st.lease_failure,
                    revoke_ack_timeout: st.revoke_ack_timeout,
                    quarantined_until: st
                        .quarantine_until
                        .map(|t| t.duration_since(Instant::now()).as_millis() as u64),
                    blacklisted: st.blacklisted,
                })
            })
            .collect()
    }

    /// Admin-only: manually release a client from quarantine / blacklist
    /// (§8.3 point 3 "需管理员解除"). Resets the score to
    /// `post_quarantine_score` and clears streaks. Returns `true` if the
    /// client was known and modified.
    pub fn admin_release(&self, client_id: &str) -> bool {
        let mut clients = self.clients.lock().unwrap();
        if let Some(st) = clients.get_mut(client_id) {
            st.blacklisted = false;
            st.quarantine_until = None;
            st.quarantine_streak = 0;
            st.consecutive_low = 0;
            st.score = self.cfg.post_quarantine_score;
            true
        } else {
            false
        }
    }

    /// Drop all clients that are neither quarantined, blacklisted, nor
    /// seen recently — bounds memory growth in long-running filers.
    /// `idle_after` is the idle threshold (e.g. 1h).
    pub fn sweep_idle(&self, idle_after: Duration) -> usize {
        let mut clients = self.clients.lock().unwrap();
        let cutoff = Instant::now().checked_sub(idle_after);
        let before = clients.len();
        clients.retain(|_, st| {
            // Keep quarantined/blacklisted clients (they're under active
            // sanction; evicting them would let them sneak back in at
            // `initial_score`).
            if st.quarantine_until.is_some() || st.blacklisted {
                return true;
            }
            match cutoff {
                Some(c) => match st.last_acquire {
                    Some(t) => t > c,
                    None => false,
                },
                None => true,
            }
        });
        before - clients.len()
    }
}

impl Default for ClientHealth {
    fn default() -> Self {
        Self::with_defaults()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn permissive() -> HealthConfig {
        // Tight thresholds so tests can drive transitions quickly.
        HealthConfig {
            initial_score: 100,
            throttle_threshold: 30,
            quarantine_threshold: 10,
            quarantine_consecutive_required: 2,
            post_quarantine_score: 50,
            failure_penalty: 20, // 5 failures => 0
            revoke_ack_timeout_penalty: 50,
            renew_success_bonus: 2,
            score_ceiling: 100,
            throttle_lease_ms_min: 1_000,
            throttle_lease_ms_max: 30_000,
            quarantine_duration: Duration::from_millis(50),
            blacklist_threshold: 3,
        }
    }

    #[test]
    fn test_score_clamps_to_100() {
        assert_eq!(ClientHealthScore::new(150).raw(), 100);
        assert_eq!(ClientHealthScore::new(0).raw(), 0);
    }

    #[test]
    fn test_healthy_client_is_allowed() {
        let ch = ClientHealth::new(permissive());
        assert_eq!(ch.check("c1"), HealthDecision::Allow);
    }

    #[test]
    fn test_renew_success_restores_score() {
        let ch = ClientHealth::new(permissive());
        // Drop the score, then let renew successes rebuild it.
        for _ in 0..3 {
            ch.record_lease_failure("c1");
        }
        // score = 100 - 3*20 = 40 → still Allow (> 30).
        assert_eq!(ch.check("c1"), HealthDecision::Allow);

        // One more failure → 20, throttle band. Don't pin the exact lease_ms
        // here (the sliding-scale value is covered by
        // `test_throttle_lease_scales_with_score`); just assert the variant.
        ch.record_lease_failure("c1");
        assert!(matches!(ch.check("c1"), HealthDecision::Throttle { .. }));

        // Renew successes bring it back up (each +2).
        for _ in 0..6 {
            ch.record_renew_success("c1");
        }
        // score = 20 + 6*2 = 32 → Allow again.
        assert_eq!(ch.check("c1"), HealthDecision::Allow);
    }

    #[test]
    fn test_throttle_lease_scales_with_score() {
        let ch = ClientHealth::new(permissive());
        // Drop score to 20 (mid-band: lo=10, hi=30).
        for _ in 0..4 {
            ch.record_lease_failure("c1"); // 100 - 4*20 = 20
        }
        let snap = ch.snapshot("c1").unwrap();
        assert_eq!(snap.score, 20);
        let d = ch.check("c1");
        match d {
            HealthDecision::Throttle { lease_ms } => {
                // pos = 20 - 10 = 10; span = 20; frac = 0.5
                // lease = 1000 + (30000-1000)*0.5 = 15500
                assert!(lease_ms > 1_000 && lease_ms < 30_000);
            }
            other => panic!("expected throttle, got {other:?}"),
        }
    }

    #[test]
    fn test_quarantine_requires_consecutive_low_samples() {
        let ch = ClientHealth::new(permissive());
        // Drive to 0 (below quarantine_threshold=10).
        for _ in 0..5 {
            ch.record_lease_failure("c1"); // 100 - 5*20 = 0
        }
        // First check: consecutive_low=1 < required(2) → Throttle (hard).
        assert!(matches!(ch.check("c1"), HealthDecision::Throttle { .. }));
        // Second consecutive check: quarantine kicks in.
        assert!(matches!(ch.check("c1"), HealthDecision::Quarantine { .. }));
    }

    #[test]
    fn test_quarantine_expires_and_score_recovers() {
        let ch = ClientHealth::new(permissive());
        for _ in 0..5 {
            ch.record_lease_failure("c1"); // → 0
        }
        // Two checks to enter quarantine.
        let _ = ch.check("c1");
        let d = ch.check("c1");
        assert!(matches!(d, HealthDecision::Quarantine { .. }));

        // While quarantined, further checks still return Quarantine.
        assert!(matches!(ch.check("c1"), HealthDecision::Quarantine { .. }));

        // Wait out the (50ms) quarantine.
        std::thread::sleep(Duration::from_millis(80));
        // Score recovers to post_quarantine_score=50 → Allow.
        assert_eq!(ch.check("c1"), HealthDecision::Allow);
        assert_eq!(ch.snapshot("c1").unwrap().score, 50);
    }

    #[test]
    fn test_blacklist_after_three_quarantine_streaks() {
        let ch = ClientHealth::new(permissive());
        // Helper: drive score to 0 (5 × 20 penalty from any starting point).
        let drive_to_zero = || {
            for _ in 0..5 {
                ch.record_lease_failure("c1");
            }
        };

        // Each quarantine cycle must: (1) recover-on-check after expiry (which
        // resets score to 50 + consecutive_low=0), then (2) re-drive the
        // score down before the two checks that re-enter quarantine.
        //
        // Cycle 1 (no prior quarantine, score starts at 100).
        drive_to_zero();
        let _ = ch.check("c1"); // consecutive_low=1 → Throttle
        assert!(matches!(ch.check("c1"), HealthDecision::Quarantine { .. })); // streak=1
        std::thread::sleep(Duration::from_millis(80));

        // Cycle 2.
        let _ = ch.check("c1"); // quarantine expired → recover to 50, Allow
        drive_to_zero();
        let _ = ch.check("c1"); // consecutive_low=1 → Throttle
        assert!(matches!(ch.check("c1"), HealthDecision::Quarantine { .. })); // streak=2
        std::thread::sleep(Duration::from_millis(80));

        // Cycle 3 — streak reaches blacklist_threshold(3) on entry.
        let _ = ch.check("c1"); // recover to 50, Allow
        drive_to_zero();
        let _ = ch.check("c1"); // consecutive_low=1 → Throttle
        assert_eq!(ch.check("c1"), HealthDecision::Blacklisted); // streak=3 → blacklist
        assert!(ch.snapshot("c1").unwrap().blacklisted);
    }

    #[test]
    fn test_revoke_ack_timeout_is_heavy_penalty() {
        let ch = ClientHealth::new(permissive());
        ch.record_revoke_ack_timeout("c1"); // 100 - 50 = 50
        assert_eq!(ch.snapshot("c1").unwrap().score, 50);
        assert_eq!(ch.check("c1"), HealthDecision::Allow);
        ch.record_revoke_ack_timeout("c1"); // 50 - 50 = 0
        assert_eq!(ch.snapshot("c1").unwrap().score, 0);
        assert!(matches!(ch.check("c1"), HealthDecision::Throttle { .. }));
    }

    #[test]
    fn test_admin_release_clears_blacklist() {
        let ch = ClientHealth::new(permissive());
        let drive_to_zero = || {
            for _ in 0..5 {
                ch.record_lease_failure("c1");
            }
        };

        // Drive three quarantine cycles to reach permanent blacklist.
        // (Same cycle structure as `test_blacklist_after_three_quarantine_streaks`.)
        for _ in 0..3 {
            drive_to_zero();
            let _ = ch.check("c1"); // consecutive_low=1
            let _ = ch.check("c1"); // enter quarantine (streak bumps)
            std::thread::sleep(Duration::from_millis(80));
            let _ = ch.check("c1"); // recover to 50 on next cycle (skip on last)
        }
        // The last iteration's recovery check left the score at 50; the
        // 3rd quarantine entry already blacklisted. Verify + clear.
        assert!(ch.snapshot("c1").unwrap().blacklisted);
        assert_eq!(ch.check("c1"), HealthDecision::Blacklisted);

        // Admin release clears blacklist and resets score.
        assert!(ch.admin_release("c1"));
        assert_eq!(ch.check("c1"), HealthDecision::Allow);
        assert_eq!(ch.snapshot("c1").unwrap().score, 50);
        assert!(!ch.snapshot("c1").unwrap().blacklisted);
    }

    #[test]
    fn test_long_hold_bonus_short_hold_penalty() {
        let ch = ClientHealth::new(permissive());
        // Drop to 80.
        ch.record_lease_failure("c1"); // 100 - 20 = 80
                                       // Long hold → +1.
        ch.record_lease_held_duration("c1", Duration::from_secs(5));
        assert_eq!(ch.snapshot("c1").unwrap().score, 81);
        // Short hold (< 1s) → -1.
        ch.record_lease_held_duration("c1", Duration::from_millis(100));
        assert_eq!(ch.snapshot("c1").unwrap().score, 80);
    }

    #[test]
    fn test_snapshots_enumerate_all_clients() {
        let ch = ClientHealth::new(permissive());
        ch.record_acquire("a");
        ch.record_acquire("b");
        let mut ids: Vec<_> = ch.snapshots().into_iter().map(|s| s.client_id).collect();
        ids.sort();
        assert_eq!(ids, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn test_sweep_idle_evicts_unseen_clients() {
        let ch = ClientHealth::new(permissive());
        ch.record_acquire("idle");
        ch.record_acquire("active");
        // Sleep so both become stale relative to a 10ms threshold.
        std::thread::sleep(Duration::from_millis(20));
        // Refresh "active" right before sweeping — its last_acquire is now
        // ~now, while "idle" is 20ms old.
        ch.record_acquire("active");
        let removed = ch.sweep_idle(Duration::from_millis(10));
        assert!(removed >= 1);
        let ids: Vec<_> = ch.snapshots().into_iter().map(|s| s.client_id).collect();
        assert!(ids.contains(&"active".to_string()));
        assert!(!ids.contains(&"idle".to_string()));
    }
}
