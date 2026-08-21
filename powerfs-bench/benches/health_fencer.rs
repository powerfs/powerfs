//! Benchmarks for the three-layer defense (§8.2) + Fencer token (§8.3).
//!
//! Bench harnesses intentionally call `validate` / `check` purely to time
//! them; the returned `Result` / decision is `debug_assert!`-ed then
//! dropped via `black_box`. The `unused_must_use` lint on `Result` would
//! fire for that pattern, so we allow it crate-wide here.

#![allow(unused_must_use)]
//!
//! These are called inline on every `acquire`, so their latency caps is the
//! floor of the server-side lease hot path. Phase-4 optimization (Early Grant
//! etc.) is gated by whether these gates stay sub-microsecond.
//!
//! Measured:
//! - `ClientHealth::check` Allow path (healthy client, score 100)
//! - `ClientHealth::check` Throttle path (score in 10..30 band)
//! - `ClientHealth::check` Quarantine path (active quarantine)
//! - `ClientHealth::record_acquire` (Layer 1 churn feed)
//! - `ClientHealth::record_renew_success` (Layer 1 score bump)
//! - `Fencer::register` (issue new epoch)
//! - `Fencer::validate` Ok path (current epoch)
//! - `Fencer::validate` StaleEpoch path (zombie detection)
//! - `Fencer::validate` NotRegistered path (post-bump_all)

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use powerfs_lock_health::{config::HealthConfig, ClientHealth, Fencer};
use std::time::Duration;

// ===========================================================================
// ClientHealth::check — three decision paths
// ===========================================================================

fn bench_check_allow(c: &mut Criterion) {
    let ch = ClientHealth::with_defaults();
    // Fresh client at score 100 → Allow.
    c.bench_function("health/check_allow", |b| {
        b.iter(|| {
            let d = ch.check(black_box("client-healthy"));
            debug_assert!(matches!(d, powerfs_lock_health::HealthDecision::Allow));
            black_box(d);
        });
    });
}

fn bench_check_throttle(c: &mut Criterion) {
    // Use a custom config so we can drive the score to a stable mid-band
    // value (20, between quarantine_threshold=10 and throttle_threshold=30)
    // without falling into quarantine.
    let cfg = HealthConfig {
        initial_score: 100,
        throttle_threshold: 30,
        quarantine_threshold: 10,
        quarantine_consecutive_required: 100, // never quarantine in this bench
        post_quarantine_score: 50,
        failure_penalty: 5,
        revoke_ack_timeout_penalty: 15,
        renew_success_bonus: 2,
        score_ceiling: 100,
        throttle_lease_ms_min: 1_000,
        throttle_lease_ms_max: 30_000,
        quarantine_duration: Duration::from_secs(60),
        blacklist_threshold: 3,
    };
    let ch = ClientHealth::new(cfg);
    // Drop score from 100 to 20 (16 × 5-point failures).
    for _ in 0..16 {
        ch.record_lease_failure("client-throttled");
    }
    // Sanity: first check returns Throttle.
    debug_assert!(matches!(
        ch.check("client-throttled"),
        powerfs_lock_health::HealthDecision::Throttle { .. }
    ));

    c.bench_function("health/check_throttle", |b| {
        b.iter(|| {
            let d = ch.check(black_box("client-throttled"));
            debug_assert!(matches!(
                d,
                powerfs_lock_health::HealthDecision::Throttle { .. }
            ));
            black_box(d);
        });
    });
}

fn bench_check_quarantine(c: &mut Criterion) {
    // Tight config: 2 consecutive low samples to enter quarantine, 60s
    // quarantine duration so it stays active for the whole bench run.
    let cfg = HealthConfig {
        initial_score: 100,
        throttle_threshold: 30,
        quarantine_threshold: 10,
        quarantine_consecutive_required: 2,
        post_quarantine_score: 50,
        failure_penalty: 50, // 2 failures → 0
        revoke_ack_timeout_penalty: 15,
        renew_success_bonus: 2,
        score_ceiling: 100,
        throttle_lease_ms_min: 1_000,
        throttle_lease_ms_max: 30_000,
        quarantine_duration: Duration::from_secs(60),
        blacklist_threshold: 100, // never blacklist in this bench
    };
    let ch = ClientHealth::new(cfg);
    // Drive score to 0 (2 × 50-point failures).
    ch.record_lease_failure("client-quant");
    ch.record_lease_failure("client-quant");
    // Two consecutive checks → enters quarantine.
    let _ = ch.check("client-quant");
    let _ = ch.check("client-quant");
    // Sanity: now in quarantine.
    debug_assert!(matches!(
        ch.check("client-quant"),
        powerfs_lock_health::HealthDecision::Quarantine { .. }
    ));

    c.bench_function("health/check_quarantine", |b| {
        b.iter(|| {
            let d = ch.check(black_box("client-quant"));
            debug_assert!(matches!(
                d,
                powerfs_lock_health::HealthDecision::Quarantine { .. }
            ));
            black_box(d);
        });
    });
}

// ===========================================================================
// ClientHealth::record_* — Layer 1 signal feeds
// ===========================================================================

fn bench_record_acquire(c: &mut Criterion) {
    let ch = ClientHealth::with_defaults();
    c.bench_function("health/record_acquire", |b| {
        b.iter(|| {
            ch.record_acquire(black_box("client-A"));
        });
    });
}

fn bench_record_renew_success(c: &mut Criterion) {
    let ch = ClientHealth::with_defaults();
    c.bench_function("health/record_renew_success", |b| {
        b.iter(|| {
            ch.record_renew_success(black_box("client-A"));
        });
    });
}

// ===========================================================================
// Fencer — epoch-based zombie defense (§8.3 point 2)
// ===========================================================================

fn bench_fencer_register(c: &mut Criterion) {
    let f = Fencer::new();
    let counter = std::sync::atomic::AtomicU64::new(0);
    c.bench_function("fencer/register", |b| {
        b.iter(|| {
            let i = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let id = format!("client-{i}");
            let epoch = f.register(black_box(&id));
            black_box(epoch);
        });
    });
}

fn bench_fencer_validate_ok(c: &mut Criterion) {
    let f = Fencer::new();
    let epoch = f.register("client-A");
    c.bench_function("fencer/validate_ok", |b| {
        b.iter(|| {
            let res = f.validate(black_box("client-A"), black_box(epoch));
            debug_assert!(res.is_ok());
            black_box(res);
        });
    });
}

fn bench_fencer_validate_stale(c: &mut Criterion) {
    let f = Fencer::new();
    let old = f.register("client-A");
    // Re-register → old epoch is now stale.
    let _new = f.register("client-A");
    c.bench_function("fencer/validate_stale", |b| {
        b.iter(|| {
            let res = f.validate(black_box("client-A"), black_box(old));
            debug_assert!(res.is_err());
            black_box(res);
        });
    });
}

fn bench_fencer_validate_not_registered(c: &mut Criterion) {
    let f = Fencer::new();
    // Register one client, then bump_all to clear everyone → client-A is
    // now NotRegistered.
    let _ = f.register("client-A");
    let _ = f.bump_all();
    c.bench_function("fencer/validate_not_registered", |b| {
        b.iter(|| {
            let res = f.validate(black_box("client-A"), black_box(1));
            debug_assert!(res.is_err());
            black_box(res);
        });
    });
}

// ===========================================================================
// Group
// ===========================================================================

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(200)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));
    targets =
        bench_check_allow,
        bench_check_throttle,
        bench_check_quarantine,
        bench_record_acquire,
        bench_record_renew_success,
        bench_fencer_register,
        bench_fencer_validate_ok,
        bench_fencer_validate_stale,
        bench_fencer_validate_not_registered,
}

criterion_main!(benches);
