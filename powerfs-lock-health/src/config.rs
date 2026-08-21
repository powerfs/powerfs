//! Configuration for the three-layer defense (§8.2).
//!
//! All thresholds are tunable so operators can trade safety vs.
//! availability per cluster. Sensible defaults are calibrated for
//! PowerFS's expected workload (~100ms lease acquire RTT, ~30s
//! default lease TTL, 100-1000 active clients).

use std::time::Duration;

/// Configuration for [`crate::ClientHealth`].
#[derive(Debug, Clone)]
pub struct HealthConfig {
    // ---------- Layer 1: scoring thresholds ----------
    /// Initial score for a newly-registered client (§8.2 Layer 1
    /// "期满后分数恢复到 50 重试" implies new clients start at 100
    /// — fresh trust until proven unhealthy).
    pub initial_score: u8,

    /// Score below which Layer 2 (throttle) engages.
    /// §8.2: "< 30 触发 Layer 2/3".
    pub throttle_threshold: u8,

    /// Score below which Layer 3 (quarantine) engages.
    /// §8.2: "分数 < 10 且持续 N 次 → 加入隔离池".
    pub quarantine_threshold: u8,

    /// Number of consecutive low-score samples below
    /// `quarantine_threshold` required to trigger quarantine
    /// (§8.2: "持续 N 次"). Default 3 — avoids one-off dips.
    pub quarantine_consecutive_required: u32,

    /// Score restored to a quarantined client after the quarantine
    /// period expires (§8.2: "期满后分数恢复到 50 重试").
    pub post_quarantine_score: u8,

    // ---------- Layer 1: scoring weights ----------
    /// Penalty (score points deducted) per lease failure
    /// (e.g. release-then-acquire-again churn).
    pub failure_penalty: u8,

    /// Penalty for a Renew ACK timeout — the heaviest signal of a
    /// stuck/slow client (§8.3: "Revoke after 2s no ACK → 标记 unresponsive").
    /// Set higher than `failure_penalty` because an unresponsive
    /// client blocks the lock chain for everyone.
    pub revoke_ack_timeout_penalty: u8,

    /// Bonus per successful renew (positive signal). Capped to
    /// prevent runaway recovery; the cap is `score_ceiling`.
    pub renew_success_bonus: u8,

    /// Maximum score (can't exceed this even with many successes).
    pub score_ceiling: u8,

    // ---------- Layer 2: adaptive throttle ----------
    /// Lease duration granted when client score is in the throttle
    /// range (30 > score >= 10). §8.2: "30s → 5s → 1s".
    /// Default 5_000ms (mid-throttle); the actual grant is computed
    /// from a sliding scale in [`crate::ClientHealthScore`].
    pub throttle_lease_ms_min: u64,

    /// Upper bound of the throttle lease range (default 30_000ms).
    pub throttle_lease_ms_max: u64,

    // ---------- Layer 3: quarantine ----------
    /// How long a client stays in quarantine before being retried
    /// at `post_quarantine_score` (§8.2: "隔离期可配置 默认 60s").
    pub quarantine_duration: Duration,

    /// Number of consecutive quarantine entries that triggers
    /// permanent blacklist (§8.3: "连续 3 次进入隔离池 → 永久黑名单").
    pub blacklist_threshold: u32,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            initial_score: 100,
            throttle_threshold: 30,
            quarantine_threshold: 10,
            quarantine_consecutive_required: 3,
            post_quarantine_score: 50,
            failure_penalty: 5,
            revoke_ack_timeout_penalty: 15,
            renew_success_bonus: 2,
            score_ceiling: 100,
            throttle_lease_ms_min: 1_000,
            throttle_lease_ms_max: 30_000,
            quarantine_duration: Duration::from_secs(60),
            blacklist_threshold: 3,
        }
    }
}

impl HealthConfig {
    /// Conservative preset — wider throttle band, longer quarantine.
    /// Use for clusters with unstable network or many slow clients.
    pub fn conservative() -> Self {
        Self {
            throttle_threshold: 50,
            quarantine_threshold: 20,
            quarantine_consecutive_required: 2,
            quarantine_duration: Duration::from_secs(120),
            ..Self::default()
        }
    }

    /// Permissive preset — narrower throttle band, shorter quarantine.
    /// Use for trusted single-tenant clusters where availability >
    /// safety.
    pub fn permissive() -> Self {
        Self {
            throttle_threshold: 15,
            quarantine_threshold: 5,
            quarantine_consecutive_required: 5,
            quarantine_duration: Duration::from_secs(15),
            ..Self::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_thresholds() {
        let c = HealthConfig::default();
        assert_eq!(c.initial_score, 100);
        assert_eq!(c.throttle_threshold, 30);
        assert_eq!(c.quarantine_threshold, 10);
        assert_eq!(c.quarantine_duration, Duration::from_secs(60));
        assert_eq!(c.blacklist_threshold, 3);
    }

    #[test]
    fn test_presets_make_sense() {
        let cons = HealthConfig::conservative();
        assert!(cons.throttle_threshold > HealthConfig::default().throttle_threshold);
        assert!(cons.quarantine_duration > HealthConfig::default().quarantine_duration);

        let perm = HealthConfig::permissive();
        assert!(perm.throttle_threshold < HealthConfig::default().throttle_threshold);
        assert!(perm.quarantine_duration < HealthConfig::default().quarantine_duration);
    }
}
