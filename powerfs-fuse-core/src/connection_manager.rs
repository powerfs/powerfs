//! Master connection manager with per-node health tracking, circuit breaker,
//! and leader caching.
//!
//! This wraps (not replaces) the existing retry logic in
//! `powerfs_common::retry`. The genuinely new functionality is:
//!
//! 1. **Per-node backoff state**: each master address has its own failure
//!    counter and next-allowed-retry timestamp. Nodes that fail repeatedly
//!    are skipped for progressively longer durations.
//! 2. **Circuit breaker**: after `circuit_breaker_threshold` consecutive
//!    failures, a node is fully skipped for `circuit_breaker_duration`,
//!    regardless of subsequent retry attempts. This prevents fault
//!    amplification when one master is partitioned.
//! 3. **Leader caching**: when the client observes a "not leader" error, the
//!    reported leader address is cached so the next connection attempt
//!    targets the leader directly instead of round-robining.
//!
//! The manager is independent of gRPC types and can be unit-tested in
//! isolation. The client ([`crate::client::PowerFuseClient`]) integrates it
//! by calling [`MasterConnectionManager::select_masters`] in
//! `try_connect_to_master` and `record_success`/`record_failure` after each
//! gRPC call.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Configuration for [`MasterConnectionManager`].
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// Initial backoff after the first failure on a node.
    pub initial_backoff: Duration,
    /// Maximum backoff between retries of a single node.
    pub max_backoff: Duration,
    /// Multiplier applied to the backoff after each failure.
    pub backoff_multiplier: f64,
    /// Jitter fraction (0.0 = no jitter, 0.1 = ±10%).
    pub jitter_factor: f64,
    /// Number of consecutive failures that triggers the circuit breaker.
    pub circuit_breaker_threshold: u32,
    /// How long the circuit breaker stays open (node fully skipped).
    pub circuit_breaker_duration: Duration,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        ConnectionConfig {
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(30),
            backoff_multiplier: 2.0,
            jitter_factor: 0.1,
            circuit_breaker_threshold: 10,
            circuit_breaker_duration: Duration::from_secs(60),
        }
    }
}

/// Per-node health state.
#[derive(Debug, Clone)]
pub struct NodeHealth {
    pub consecutive_failures: u32,
    pub current_backoff: Duration,
    pub next_allowed_at: Instant,
    pub circuit_open: bool,
    pub last_success: Option<Instant>,
}

impl Default for NodeHealth {
    fn default() -> Self {
        NodeHealth {
            consecutive_failures: 0,
            current_backoff: Duration::ZERO,
            next_allowed_at: Instant::now(),
            circuit_open: false,
            last_success: None,
        }
    }
}

/// Master connection manager. Tracks per-node health and provides an ordered
/// list of master addresses to try, preferring healthy nodes and the cached
/// leader.
pub struct MasterConnectionManager {
    master_addresses: Vec<String>,
    config: ConnectionConfig,
    state: Arc<RwLock<HashMap<String, NodeHealth>>>,
    /// Cached leader address, if known. May be `None` or stale; callers
    /// should fall back to round-robin if it is unreachable.
    leader_hint: Arc<RwLock<Option<String>>>,
}

impl MasterConnectionManager {
    pub fn new(master_addresses: Vec<String>, config: ConnectionConfig) -> Self {
        let state: HashMap<String, NodeHealth> = master_addresses
            .iter()
            .map(|addr| (addr.clone(), NodeHealth::default()))
            .collect();
        MasterConnectionManager {
            master_addresses,
            config,
            state: Arc::new(RwLock::new(state)),
            leader_hint: Arc::new(RwLock::new(None)),
        }
    }

    pub fn addresses(&self) -> &[String] {
        &self.master_addresses
    }

    /// Return the cached leader address, if any.
    pub async fn leader_hint(&self) -> Option<String> {
        self.leader_hint.read().await.clone()
    }

    /// Update the cached leader. Called when the client observes a
    /// "not leader" error mentioning a specific leader address, or when a
    /// successful call confirms the current node is the leader.
    pub async fn set_leader_hint(&self, addr: Option<String>) {
        let mut hint = self.leader_hint.write().await;
        *hint = addr;
    }

    /// Select an ordered list of master addresses to try. The leader hint (if
    /// any and not in circuit-break) is first, followed by the remaining
    /// nodes sorted by health (fewest failures first). Nodes whose circuit
    /// breaker is open or whose backoff has not elapsed are excluded.
    pub async fn select_masters(&self) -> Vec<String> {
        let now = Instant::now();
        let state = self.state.read().await;

        // Filter out nodes that are not currently eligible.
        let eligible: Vec<&String> = self
            .master_addresses
            .iter()
            .filter(|addr| match state.get(*addr) {
                Some(h) => now >= h.next_allowed_at,
                None => true,
            })
            .collect();

        // If nothing is eligible, return all addresses anyway so the caller
        // can attempt a last-ditch connection (better than failing silently).
        if eligible.is_empty() {
            return self.master_addresses.clone();
        }

        let leader = self.leader_hint.read().await.clone();

        // Order: leader first (if eligible), then by consecutive_failures asc.
        let mut ordered: Vec<&String> = eligible;
        ordered.sort_by(|a, b| {
            let fa = state.get(*a).map(|h| h.consecutive_failures).unwrap_or(0);
            let fb = state.get(*b).map(|h| h.consecutive_failures).unwrap_or(0);
            fa.cmp(&fb)
        });

        if let Some(leader) = &leader {
            if let Some(pos) = ordered.iter().position(|a| *a == leader) {
                let ldr = ordered.remove(pos);
                ordered.insert(0, ldr);
            }
        }

        ordered.iter().map(|s| s.to_string()).collect()
    }

    /// Record a successful connection / gRPC call to `addr`. Resets the
    /// per-node failure counter and clears any circuit-breaker state.
    pub async fn record_success(&self, addr: &str) {
        let mut state = self.state.write().await;
        let health = state.entry(addr.to_string()).or_default();
        health.consecutive_failures = 0;
        health.current_backoff = Duration::ZERO;
        health.next_allowed_at = Instant::now();
        health.circuit_open = false;
        health.last_success = Some(Instant::now());
    }

    /// Record a failure on `addr`. Increases the backoff and may open the
    /// circuit breaker.
    pub async fn record_failure(&self, addr: &str) {
        let mut state = self.state.write().await;
        let health = state.entry(addr.to_string()).or_default();
        health.consecutive_failures = health.consecutive_failures.saturating_add(1);

        // Exponential backoff with jitter.
        let jitter = if self.config.jitter_factor > 0.0 {
            1.0 + (pseudo_random_f64() * 2.0 - 1.0) * self.config.jitter_factor
        } else {
            1.0
        };
        let base_secs = if health.current_backoff.is_zero() {
            self.config.initial_backoff.as_secs_f64()
        } else {
            health.current_backoff.as_secs_f64() * self.config.backoff_multiplier
        };
        let new_secs = (base_secs * jitter).min(self.config.max_backoff.as_secs_f64());
        health.current_backoff = Duration::from_secs_f64(new_secs.max(0.001));

        // Circuit breaker.
        if health.consecutive_failures >= self.config.circuit_breaker_threshold {
            health.circuit_open = true;
            health.next_allowed_at = Instant::now() + self.config.circuit_breaker_duration;
            log::warn!(
                "Master {} circuit breaker opened after {} consecutive failures (skipping for {:?})",
                addr,
                health.consecutive_failures,
                self.config.circuit_breaker_duration
            );
        } else {
            health.next_allowed_at = Instant::now() + health.current_backoff;
            log::debug!(
                "Master {} failure #{}: backing off for {:?}",
                addr,
                health.consecutive_failures,
                health.current_backoff
            );
        }
    }

    /// Snapshot of per-node health. Mainly useful for diagnostics and tests.
    pub async fn health_snapshot(&self) -> HashMap<String, NodeHealth> {
        self.state.read().await.clone()
    }
}

/// Deterministic-ish pseudo-random f64 in [0, 1). Uses the thread-local
/// state from `rand` if available; falls back to a nanosecond-based hash
/// otherwise. We avoid pulling `rand` as a hard dependency here because
/// `powerfs-fuse-core` may not have it as a direct dep — the caller already
/// links `powerfs-common` which does.
fn pseudo_random_f64() -> f64 {
    // Use a simple xorshift seeded from the system clock. Good enough for
    // jitter; we are not doing crypto here.
    use std::cell::Cell;
    thread_local! {
        static STATE: Cell<u64> = Cell::new({
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0xdead_beef_cafe_babe);
            now.wrapping_add(1)
        });
    }
    STATE.with(|s| {
        let mut x = s.get();
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        s.set(x);
        // Map to [0, 1).
        (x as f64) / (u64::MAX as f64)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    fn addrs() -> Vec<String> {
        vec![
            "master1:9333".to_string(),
            "master2:9333".to_string(),
            "master3:9333".to_string(),
        ]
    }

    #[tokio::test]
    async fn select_masters_returns_all_initially() {
        let mgr = MasterConnectionManager::new(addrs(), ConnectionConfig::default());
        let selected = mgr.select_masters().await;
        assert_eq!(selected.len(), 3);
    }

    #[tokio::test]
    async fn leader_hint_is_tried_first() {
        let mgr = MasterConnectionManager::new(addrs(), ConnectionConfig::default());
        mgr.set_leader_hint(Some("master3:9333".to_string())).await;
        let selected = mgr.select_masters().await;
        assert_eq!(selected.first().unwrap(), "master3:9333");
    }

    #[tokio::test]
    async fn failed_node_is_skipped_within_backoff() {
        let cfg = ConnectionConfig {
            initial_backoff: Duration::from_secs(60),
            max_backoff: Duration::from_secs(120),
            backoff_multiplier: 2.0,
            jitter_factor: 0.0,
            circuit_breaker_threshold: 10,
            circuit_breaker_duration: Duration::from_secs(120),
        };
        let mgr = MasterConnectionManager::new(addrs(), cfg);
        mgr.record_failure("master1:9333").await;
        let selected = mgr.select_masters().await;
        assert!(
            !selected.iter().any(|s| s == "master1:9333"),
            "failed node should be skipped within backoff window: {:?}",
            selected
        );
        // Other two still selectable.
        assert!(selected.iter().any(|s| s == "master2:9333"));
        assert!(selected.iter().any(|s| s == "master3:9333"));
    }

    #[tokio::test]
    async fn record_success_resets_failure_count() {
        let mgr = MasterConnectionManager::new(addrs(), ConnectionConfig::default());
        mgr.record_failure("master1:9333").await;
        mgr.record_failure("master1:9333").await;
        let snap = mgr.health_snapshot().await;
        assert_eq!(snap.get("master1:9333").unwrap().consecutive_failures, 2);
        mgr.record_success("master1:9333").await;
        let snap = mgr.health_snapshot().await;
        let h = snap.get("master1:9333").unwrap();
        assert_eq!(h.consecutive_failures, 0);
        assert!(!h.circuit_open);
        assert!(h.next_allowed_at <= Instant::now());
    }

    #[tokio::test]
    async fn circuit_breaker_opens_after_threshold() {
        let cfg = ConnectionConfig {
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
            backoff_multiplier: 1.0,
            jitter_factor: 0.0,
            circuit_breaker_threshold: 3,
            circuit_breaker_duration: Duration::from_secs(60),
        };
        let mgr = MasterConnectionManager::new(addrs(), cfg);
        for _ in 0..3 {
            mgr.record_failure("master1:9333").await;
        }
        let snap = mgr.health_snapshot().await;
        let h = snap.get("master1:9333").unwrap();
        assert!(h.circuit_open);
        // Next-allowed should be far in the future (60s breaker).
        assert!(h.next_allowed_at > Instant::now() + Duration::from_secs(30));
        // The tripped node should not appear in the candidate list.
        let selected = mgr.select_masters().await;
        assert!(!selected.iter().any(|s| s == "master1:9333"));
    }

    #[tokio::test]
    async fn all_nodes_failed_returns_all_anyway() {
        // If every node is in backoff, we still need to try *something*.
        let cfg = ConnectionConfig {
            initial_backoff: Duration::from_secs(60),
            max_backoff: Duration::from_secs(120),
            backoff_multiplier: 2.0,
            jitter_factor: 0.0,
            circuit_breaker_threshold: 10,
            circuit_breaker_duration: Duration::from_secs(120),
        };
        let mgr = MasterConnectionManager::new(addrs(), cfg);
        mgr.record_failure("master1:9333").await;
        mgr.record_failure("master2:9333").await;
        mgr.record_failure("master3:9333").await;
        let selected = mgr.select_masters().await;
        // All addresses returned as a last-ditch fallback.
        assert_eq!(selected.len(), 3);
    }

    #[tokio::test]
    async fn backoff_grows_exponentially() {
        let cfg = ConnectionConfig {
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            backoff_multiplier: 2.0,
            jitter_factor: 0.0,
            circuit_breaker_threshold: 100,
            circuit_breaker_duration: Duration::from_secs(60),
        };
        let mgr = MasterConnectionManager::new(addrs(), cfg);
        mgr.record_failure("m1:9333").await;
        let s1 = mgr.health_snapshot().await;
        let b1 = s1.get("m1:9333").unwrap().current_backoff;
        mgr.record_failure("m1:9333").await;
        let s2 = mgr.health_snapshot().await;
        let b2 = s2.get("m1:9333").unwrap().current_backoff;
        // Second backoff should be ~2x the first.
        assert!(b2 > b1, "backoff should grow: b1={:?}, b2={:?}", b1, b2);
    }
}
