//! Adaptive grace period (phase 4 P5).
//!
//! The fixed 5-second grace period doesn't adapt to network
//! conditions: too long for low-latency same-rack clusters (writes
//! blocked 5s after a lease expiry), too short for cross-datacenter
//! setups (a slow-but-alive client's renew may still be in-flight
//! when the 5s grace elapses).
//!
//! ## Approach
//!
//! Track how late each renew arrives relative to the lease's expiry
//! time. If a renew arrives before expiry, lateness is 0 (healthy).
//! If after expiry, lateness is the gap (the client was slow but
//! still alive). The P99 of lateness tells us how late the slowest
//! 1% of clients are. The grace period is then:
//!
//! ```text
//! grace = max(DEFAULT_GRACE, 3 * p99_lateness)
//! ```
//!
//! This keeps the 5s floor for safety (fast networks stay at 5s)
//! while expanding the grace for slow networks where clients
//! consistently renew late. See `docs/lock-optimization-plan.md`
//! §6.2 (problem P5) and §6.3.
//!
//! ## P99 computation
//!
//! Uses a fixed-size ring buffer (256 samples). P99 is approximated
//! by sorting the buffer and taking the value at the 99th percentile
//! index. This is not a streaming P99 (no t-digest or histogram) —
//! 256 samples is enough for a per-Filer-leader grace estimate, and
//! the simplicity avoids external dependencies.

use std::sync::Mutex;
use std::time::Duration;

/// Number of lateness samples to retain for P99 computation.
const SAMPLE_CAPACITY: usize = 256;

/// Default minimum grace period (5 seconds). The adaptive grace
/// never goes below this — see module docs.
pub const DEFAULT_GRACE: Duration = Duration::from_secs(5);

/// Multiplier applied to the P99 lateness to compute the adaptive
/// grace. 3x gives headroom for the worst-case client that's 3x
/// slower than the P99.
const P99_MULTIPLIER: u32 = 3;

/// Tracker for renew lateness samples, computing an adaptive grace
/// period.
///
/// Thread-safe via `Mutex`. Contention is low — only the `renew`
/// path writes samples, and the buffer is small (256 entries).
pub struct AdaptiveGrace {
    samples: Mutex<SampleBuffer>,
}

struct SampleBuffer {
    buf: Vec<Duration>,
    next: usize,
    filled: bool,
}

impl SampleBuffer {
    fn new() -> Self {
        Self {
            buf: Vec::with_capacity(SAMPLE_CAPACITY),
            next: 0,
            filled: false,
        }
    }

    fn push(&mut self, sample: Duration) {
        if self.buf.len() < SAMPLE_CAPACITY {
            self.buf.push(sample);
        } else {
            self.buf[self.next] = sample;
            self.filled = true;
        }
        self.next = (self.next + 1) % SAMPLE_CAPACITY;
    }

    /// Compute the P99 (99th percentile) of the recorded samples.
    /// Returns `Duration::ZERO` if fewer than 2 samples are recorded.
    ///
    /// Uses the "nearest-rank" method with rank = `ceil(0.99 * N)`,
    /// 0-indexed. For N=100, rank=99 → `sorted[99]` is the top sample,
    /// i.e. the worst 1% of renews. This is intentionally inclusive of
    /// the top outlier: the grace period must accommodate the slowest
    /// still-alive client, not the median slow client.
    fn p99(&self) -> Duration {
        if self.buf.is_empty() {
            return Duration::ZERO;
        }
        let mut sorted: Vec<Duration> = self.buf.clone();
        sorted.sort();
        let len = sorted.len();
        // ceil(0.99 * N), clamped to [0, len-1]. For N=100 → 99.
        // For N=256 → 254 (ceil of 253.44).
        let idx = (((len as f64) * 0.99).ceil() as usize).min(len - 1);
        sorted[idx]
    }
}

impl AdaptiveGrace {
    /// Create a new empty tracker.
    pub fn new() -> Self {
        Self {
            samples: Mutex::new(SampleBuffer::new()),
        }
    }

    /// Record a renew lateness sample. Called by
    /// `InodeLeaseManager::renew` after a successful renew.
    ///
    /// `lateness` is the gap between the lease's expiry time and the
    /// renew arrival time. If the renew arrived before expiry,
    /// `lateness` should be `Duration::ZERO` (or the caller can pass
    /// a zero for "renewed early").
    pub fn record(&self, lateness: Duration) {
        let mut buf = self.samples.lock().unwrap();
        buf.push(lateness);
    }

    /// Compute the adaptive grace period:
    /// `max(DEFAULT_GRACE, 3 * p99_lateness)`.
    ///
    /// Returns `DEFAULT_GRACE` if fewer than 2 samples are recorded
    /// (not enough data to trust the P99).
    pub fn grace_period(&self) -> Duration {
        self.effective_grace(DEFAULT_GRACE)
    }

    /// Compute the adaptive grace period with a caller-supplied floor:
    /// `max(floor, 3 * p99_lateness)`.
    ///
    /// This lets the `InodeLeaseManager` pass its configured
    /// `grace_period` as the floor (e.g., 100ms in tests, 5s in
    /// production) so the adaptive expansion only increases the
    /// grace, never shrinks it below the configured value.
    ///
    /// Returns `floor` if fewer than 2 samples are recorded.
    pub fn effective_grace(&self, floor: Duration) -> Duration {
        let buf = self.samples.lock().unwrap();
        if buf.buf.len() < 2 {
            return floor;
        }
        let p99 = buf.p99();
        let adaptive = p99 * P99_MULTIPLIER;
        if adaptive > floor {
            adaptive
        } else {
            floor
        }
    }

    /// Number of recorded samples (for tests / diagnostics).
    pub fn sample_count(&self) -> usize {
        let buf = self.samples.lock().unwrap();
        buf.buf.len()
    }
}

impl Default for AdaptiveGrace {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_tracker_returns_default_grace() {
        let tracker = AdaptiveGrace::new();
        assert_eq!(tracker.grace_period(), DEFAULT_GRACE);
        assert_eq!(tracker.sample_count(), 0);
    }

    #[test]
    fn test_single_sample_returns_default_grace() {
        // Not enough data (need >= 2 samples).
        let tracker = AdaptiveGrace::new();
        tracker.record(Duration::from_secs(10));
        assert_eq!(tracker.grace_period(), DEFAULT_GRACE);
        assert_eq!(tracker.sample_count(), 1);
    }

    #[test]
    fn test_all_early_renews_stay_at_default() {
        // All renews arrived before expiry → lateness = 0 → P99 = 0
        // → grace stays at DEFAULT_GRACE.
        let tracker = AdaptiveGrace::new();
        for _ in 0..10 {
            tracker.record(Duration::ZERO);
        }
        assert_eq!(tracker.grace_period(), DEFAULT_GRACE);
    }

    #[test]
    fn test_late_renews_expand_grace() {
        // Some renews arrive 2s after expiry.
        // P99 of [0, 0, ..., 2s] ≈ 2s → grace = max(5s, 3*2s) = 6s.
        let tracker = AdaptiveGrace::new();
        for _ in 0..99 {
            tracker.record(Duration::ZERO);
        }
        tracker.record(Duration::from_secs(2));
        let grace = tracker.grace_period();
        assert!(
            grace >= Duration::from_secs(6),
            "grace should be at least 6s (3 * 2s P99), got {:?}",
            grace
        );
    }

    #[test]
    fn test_very_late_renew_dominates_p99() {
        // 100 samples: 99 zeros + 1 at 10s.
        // P99 ≈ 10s → grace = max(5s, 3*10s) = 30s.
        let tracker = AdaptiveGrace::new();
        for _ in 0..99 {
            tracker.record(Duration::ZERO);
        }
        tracker.record(Duration::from_secs(10));
        let grace = tracker.grace_period();
        assert!(
            grace >= Duration::from_secs(30),
            "grace should be at least 30s (3 * 10s P99), got {:?}",
            grace
        );
    }

    #[test]
    fn test_ring_buffer_overwrite() {
        // Fill the buffer past capacity and verify the latest
        // samples dominate the P99.
        let tracker = AdaptiveGrace::new();
        // Fill with zeros.
        for _ in 0..SAMPLE_CAPACITY {
            tracker.record(Duration::ZERO);
        }
        assert_eq!(tracker.sample_count(), SAMPLE_CAPACITY);
        assert_eq!(tracker.grace_period(), DEFAULT_GRACE);

        // Overwrite with 5s lateness samples.
        for _ in 0..SAMPLE_CAPACITY {
            tracker.record(Duration::from_secs(5));
        }
        assert_eq!(tracker.sample_count(), SAMPLE_CAPACITY);
        let grace = tracker.grace_period();
        assert!(
            grace >= Duration::from_secs(15),
            "grace should be at least 15s (3 * 5s P99), got {:?}",
            grace
        );
    }

    #[test]
    fn test_p99_ignores_outliers_below_threshold() {
        // 100 samples: 100 zeros → P99 = 0 → grace = 5s.
        // Even though there might be a few non-zero samples, if
        // they're below the 99th percentile, grace stays at default.
        let tracker = AdaptiveGrace::new();
        for _ in 0..100 {
            tracker.record(Duration::from_millis(100));
        }
        // P99 of 100 samples all at 100ms ≈ 100ms.
        // grace = max(5s, 3*100ms) = max(5s, 300ms) = 5s.
        assert_eq!(tracker.grace_period(), DEFAULT_GRACE);
    }
}
