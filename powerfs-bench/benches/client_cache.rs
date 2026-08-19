//! Benchmarks for the FUSE client-side lock hot paths.
//!
//! Covers:
//! - `ClientLeaseState` get/put/sweep (the cache that backs `FuseLockManager`)
//! - `LockMetrics` atomic-counter hot path
//! - `FuseLockManager::acquire` (cache hit + cache miss)
//! - `FuseLockManager::release` (cached inode lease)
//!
//! The `FuseLockManager` benches use a mock backend with near-zero overhead
//! (two atomic increments and a `Mutex` lock) so the numbers we collect
//! reflect manager / cache overhead, not RPC noise. Phase-4 optimization
//! priorities are driven by these numbers — see
//! `docs/lock-optimization-plan.md` §四 step 7 and §五.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use powerfs_lock::{LockManager, LockMode, LockRequest, Range};
use powerfs_lock_fuse::{
    backend::FuseLockBackend,
    manager::FuseLockManager,
    metrics::LockMetrics,
    state::{ClientLeaseState, InodeLeaseEntry},
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;

// ===========================================================================
// Mock backend (mirrors the one in `manager.rs` tests but kept minimal so the
// bench measures manager overhead, not mock work).
// ===========================================================================

struct NoopBackend {
    calls: AtomicU64,
}

impl NoopBackend {
    fn new() -> Self {
        Self {
            calls: AtomicU64::new(0),
        }
    }
}

#[async_trait::async_trait]
impl FuseLockBackend for NoopBackend {
    async fn acquire_inode_lease(
        &self,
        _inode: u64,
        _client_id: &str,
        duration_ms: u64,
    ) -> Result<(String, u64), String> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(("tok-inode".to_string(), duration_ms))
    }
    async fn release_inode_lease(
        &self,
        _inode: u64,
        _client_id: &str,
        _token: &str,
    ) -> Result<(), String> {
        Ok(())
    }
    async fn renew_inode_lease(
        &self,
        _inode: u64,
        _client_id: &str,
        _token: &str,
        _duration_ms: u64,
    ) -> Result<(), String> {
        Ok(())
    }
    async fn acquire_range_lease(
        &self,
        _volume_id: u64,
        _inode: u64,
        _stripe_start: u64,
        _stripe_count: u64,
        _client_id: &str,
        _exclusive: bool,
        duration_ms: u64,
    ) -> Result<String, String> {
        Ok(format!("tok-range-{}", duration_ms))
    }
    async fn release_range_lease(
        &self,
        _volume_id: u64,
        _inode: u64,
        _stripe_start: u64,
        _client_id: &str,
        _token: &str,
    ) -> Result<(), String> {
        Ok(())
    }
    async fn lookup_volume_id(&self, _inode: u64) -> Result<u64, String> {
        Ok(42)
    }
}

fn fresh_manager() -> (FuseLockManager, Arc<NoopBackend>) {
    let backend = Arc::new(NoopBackend::new());
    let manager = FuseLockManager::new(
        Arc::clone(&backend) as Arc<dyn FuseLockBackend>,
        "bench-client".to_string(),
        30_000,
    );
    (manager, backend)
}

// ===========================================================================
// ClientLeaseState benches
// ===========================================================================

fn bench_state_get_hit(c: &mut Criterion) {
    let state = ClientLeaseState::new_shared();
    // Pre-populate N entries.
    for i in 0..1024u64 {
        state.put_inode(
            i,
            InodeLeaseEntry {
                token: format!("tok-{i}"),
                expire_at: Instant::now() + Duration::from_secs(60),
                mode: LockMode::Shared,
            },
        );
    }
    c.bench_function("client_state/get_inode_hit", |b| {
        b.iter(|| {
            let e = state.get_inode(black_box(42));
            black_box(e);
        });
    });
}

fn bench_state_get_miss(c: &mut Criterion) {
    let state = ClientLeaseState::new_shared();
    // Populate some entries, but not the one we'll look up.
    for i in 0..1024u64 {
        state.put_inode(
            i,
            InodeLeaseEntry {
                token: format!("tok-{i}"),
                expire_at: Instant::now() + Duration::from_secs(60),
                mode: LockMode::Shared,
            },
        );
    }
    c.bench_function("client_state/get_inode_miss", |b| {
        b.iter(|| {
            let e = state.get_inode(black_box(u64::MAX));
            black_box(e);
        });
    });
}

fn bench_state_put(c: &mut Criterion) {
    let state = ClientLeaseState::new_shared();
    let counter = AtomicU64::new(0);
    c.bench_function("client_state/put_inode", |b| {
        b.iter(|| {
            let i = counter.fetch_add(1, Ordering::Relaxed);
            state.put_inode(
                i,
                InodeLeaseEntry {
                    token: format!("tok-{i}"),
                    expire_at: Instant::now() + Duration::from_secs(60),
                    mode: LockMode::Shared,
                },
            );
        });
    });
}

fn bench_state_sweep_empty(c: &mut Criterion) {
    let state = ClientLeaseState::new_shared();
    // 100 fresh entries, none expired — sweep is a no-op retain scan.
    for i in 0..100u64 {
        state.put_inode(
            i,
            InodeLeaseEntry {
                token: format!("tok-{i}"),
                expire_at: Instant::now() + Duration::from_secs(60),
                mode: LockMode::Shared,
            },
        );
    }
    c.bench_function("client_state/sweep_empty", |b| {
        b.iter(|| {
            let removed = state.sweep_expired();
            black_box(removed);
        });
    });
}

fn bench_state_sweep_with_expired(c: &mut Criterion) {
    c.bench_function("client_state/sweep_with_expired", |b| {
        b.iter_batched(
            || {
                // Each iter starts with 100 entries, 10 of which are expired.
                let state = ClientLeaseState::new_shared();
                for i in 0..90u64 {
                    state.put_inode(
                        i,
                        InodeLeaseEntry {
                            token: format!("tok-{i}"),
                            expire_at: Instant::now() + Duration::from_secs(60),
                            mode: LockMode::Shared,
                        },
                    );
                }
                for i in 90..100u64 {
                    state.put_inode(
                        i,
                        InodeLeaseEntry {
                            token: format!("stale-{i}"),
                            expire_at: Instant::now() - Duration::from_secs(1),
                            mode: LockMode::Shared,
                        },
                    );
                }
                state
            },
            |state| {
                let removed = state.sweep_expired();
                black_box(removed);
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

// ===========================================================================
// LockMetrics benches (atomic-counter hot path)
// ===========================================================================

fn bench_metrics_record(c: &mut Criterion) {
    let metrics = LockMetrics::new_shared();
    c.bench_function("metrics/record_acquire_hit", |b| {
        b.iter(|| {
            metrics.record_acquire();
            metrics.record_cache_hit();
        });
    });
}

fn bench_metrics_snapshot(c: &mut Criterion) {
    let metrics = LockMetrics::new_shared();
    // Pre-populate counters so snapshot isn't trivially all-zero.
    for _ in 0..1000 {
        metrics.record_acquire();
        metrics.record_cache_miss();
    }
    c.bench_function("metrics/snapshot", |b| {
        b.iter(|| {
            let snap = metrics.snapshot();
            black_box(snap);
        });
    });
}

// ===========================================================================
// FuseLockManager async benches (run on a current-thread runtime)
// ===========================================================================

fn bench_manager_acquire_cache_hit(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let (mgr, _backend) = fresh_manager();
    let inode = 42u64;
    // Prime the cache with one real acquire (cache miss → RPC → cache fill).
    rt.block_on(async {
        let req = LockRequest::new(inode, LockMode::Shared, Duration::from_secs(30));
        mgr.acquire(req).await.expect("prime acquire");
    });

    c.bench_function("manager/acquire_inode_cache_hit", |b| {
        b.iter(|| {
            rt.block_on(async {
                let req =
                    LockRequest::new(black_box(inode), LockMode::Shared, Duration::from_secs(30));
                let grant = mgr.acquire(req).await.expect("cache hit");
                black_box(grant);
            });
        });
    });
}

/// Legacy variant (phase 4 P1 baseline comparison): disables the
/// lazy-sweep fast path with `sweep_threshold = Duration::ZERO`, so
/// every cache hit runs `sweep_expired`. Criterion's regression
/// report (`target/criterion/manager/acquire_inode_cache_hit_lazy/`)
/// shows the latency delta vs. the lazy variant above.
fn bench_manager_acquire_cache_hit_lazy_sweep_disabled(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let backend: Arc<dyn FuseLockBackend> = Arc::new(NoopBackend::new());
    let mgr = FuseLockManager::new(Arc::clone(&backend), "bench-legacy".to_string(), 30_000)
        .with_sweep_threshold(Duration::ZERO);
    let inode = 42u64;
    // Prime the cache.
    rt.block_on(async {
        let req = LockRequest::new(inode, LockMode::Shared, Duration::from_secs(30));
        mgr.acquire(req).await.expect("prime acquire");
    });

    c.bench_function("manager/acquire_inode_cache_hit_lazy_sweep_disabled", |b| {
        b.iter(|| {
            rt.block_on(async {
                let req =
                    LockRequest::new(black_box(inode), LockMode::Shared, Duration::from_secs(30));
                let grant = mgr.acquire(req).await.expect("cache hit");
                black_box(grant);
            });
        });
    });
}

fn bench_manager_acquire_cache_miss(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let backend: Arc<dyn FuseLockBackend> = Arc::new(NoopBackend::new());
    let counter = AtomicU64::new(0);

    // Use `iter_batched_ref` so each batch starts with a fresh manager
    // (empty cache) and a fresh inode — every routine call is a true
    // cache miss. The setup (manager construction + inode pick) runs
    // OUTSIDE the timed routine, so the measurement reflects only the
    // acquire hot path: cache lookup miss → backend RPC → cache fill.
    c.bench_function("manager/acquire_inode_cache_miss", |b| {
        b.iter_batched_ref(
            || {
                let mgr =
                    FuseLockManager::new(Arc::clone(&backend), "bench-miss".to_string(), 30_000);
                let inode = counter.fetch_add(1, Ordering::Relaxed) + 1_000_000;
                (mgr, inode)
            },
            |(mgr, inode)| {
                rt.block_on(async {
                    let req = LockRequest::new(
                        black_box(*inode),
                        LockMode::Shared,
                        Duration::from_secs(30),
                    );
                    let grant = mgr.acquire(req).await.expect("acquire");
                    black_box(grant);
                });
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

fn bench_manager_release_cached(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let (mgr, _backend) = fresh_manager();
    let inode = 7u64;
    // Prime: acquire + release cycle in each iter is what we measure.
    c.bench_function("manager/acquire_then_release_inode", |b| {
        b.iter(|| {
            rt.block_on(async {
                let req =
                    LockRequest::new(black_box(inode), LockMode::Shared, Duration::from_secs(30));
                let grant = mgr.acquire(req).await.expect("acquire");
                mgr.release(black_box(inode), black_box(&grant.token))
                    .await
                    .expect("release");
            });
        });
    });
}

fn bench_manager_range_acquire(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let (mgr, _backend) = fresh_manager();
    let inode = 99u64;
    let range = Range::new(0, Some(4096));
    // Prime the range cache.
    rt.block_on(async {
        let req = LockRequest::new(inode, LockMode::Range(range), Duration::from_secs(30));
        mgr.acquire(req).await.expect("prime range acquire");
    });

    c.bench_function("manager/acquire_range_cache_hit", |b| {
        b.iter(|| {
            rt.block_on(async {
                let req = LockRequest::new(
                    black_box(inode),
                    LockMode::Range(range),
                    Duration::from_secs(30),
                );
                let grant = mgr.acquire(req).await.expect("range hit");
                black_box(grant);
            });
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
        bench_state_get_hit,
        bench_state_get_miss,
        bench_state_put,
        bench_state_sweep_empty,
        bench_state_sweep_with_expired,
        bench_metrics_record,
        bench_metrics_snapshot,
        bench_manager_acquire_cache_hit,
        bench_manager_acquire_cache_hit_lazy_sweep_disabled,
        bench_manager_acquire_cache_miss,
        bench_manager_release_cached,
        bench_manager_range_acquire,
}

criterion_main!(benches);
