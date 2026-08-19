//! Benchmarks for the metadata creation path (creat/mkdir/mknod).
//!
//! §5.1 Lockify targets the metadata path latency. The current
//! architecture requires a synchronous lease-acquire RPC for every
//! new inode (cache miss → backend → cache fill). Lockify replaces
//! this with a local self-declaration (generate a local token + cache
//! fill, no RPC), then async-syncs ownership to the filer.
//!
//! These benchmarks quantify the Lockify improvement ceiling by
//! comparing the two paths. The `LatencyBackend` simulates a 100µs
//! LAN RPC roundtrip — the dominant cost the Lockify fast path
//! eliminates. The NoopBackend variant (zero latency) isolates the
//! manager/cache overhead for comparison with `client_cache.rs`.
//!
//! See `docs/lock-optimization-plan.md` §5.1 and §7.3 ("Lockify 异步
//! 元数据 — 基线数据确认元数据是瓶颈").

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use powerfs_lock::{LockManager, LockMode, LockRequest};
use powerfs_lock_fuse::{
    backend::FuseLockBackend,
    manager::FuseLockManager,
    state::{ClientLeaseState, InodeLeaseEntry},
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;

// ===========================================================================
// Backends
// ===========================================================================

/// A backend that simulates a realistic LAN RPC roundtrip. The
/// `client_cache.rs` benchmarks use a `NoopBackend` with near-zero
/// overhead to isolate manager/cache costs; this backend adds a
/// configurable sleep to represent the network + server processing
/// latency that dominates the real metadata creation path.
struct LatencyBackend {
    calls: AtomicU64,
    rpc_latency: Duration,
}

impl LatencyBackend {
    fn new(rpc_latency: Duration) -> Self {
        Self {
            calls: AtomicU64::new(0),
            rpc_latency,
        }
    }
}

#[async_trait::async_trait]
impl FuseLockBackend for LatencyBackend {
    async fn acquire_inode_lease(
        &self,
        _inode: u64,
        _client_id: &str,
        duration_ms: u64,
    ) -> Result<(String, u64), String> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(self.rpc_latency).await;
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

// ===========================================================================
// Benchmark 1: current path (cache miss → sync RPC → cache fill)
// ===========================================================================

/// Simulates the current metadata creation path: every new inode is a
/// cache miss, so the client must call the backend (synchronous RPC).
/// The `LatencyBackend` sleeps for `RPC_LATENCY` to represent the
/// network + server processing roundtrip that dominates real-world
/// creat/mkdir/mknod latency.
///
/// This is the baseline against which the Lockify fast path is
/// compared. The delta shows the RPC cost that Lockify eliminates.
const RPC_LATENCY: Duration = Duration::from_micros(100);

fn bench_metadata_current_sync_rpc(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let backend: Arc<dyn FuseLockBackend> = Arc::new(LatencyBackend::new(RPC_LATENCY));
    let counter = AtomicU64::new(0);

    // `iter_batched_ref` so each routine call starts with a fresh
    // manager (empty cache) and a fresh inode — every call is a true
    // cache miss, mirroring the creat/mkdir workload where each new
    // file has no prior lease.
    c.bench_function("metadata/current_sync_rpc_acquire", |b| {
        b.iter_batched_ref(
            || {
                let mgr =
                    FuseLockManager::new(Arc::clone(&backend), "bench-meta".to_string(), 30_000);
                let inode = counter.fetch_add(1, Ordering::Relaxed) + 2_000_000;
                (mgr, inode)
            },
            |(mgr, inode)| {
                rt.block_on(async {
                    let req = LockRequest::new(
                        black_box(*inode),
                        LockMode::Exclusive,
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

// ===========================================================================
// Benchmark 2: Lockify fast path (local self-declare, no RPC)
// ===========================================================================

/// Simulates the Lockify §5.1 fast path: the client locally
/// self-declares ownership by generating a local token and inserting
/// it into the cache — **no backend RPC**. The ownership receipt is
/// async-synced to the filer in the background (not measured here;
/// it's off the critical path).
///
/// This represents the theoretical lower bound for the metadata
/// creation lease overhead. The delta vs. `current_sync_rpc_acquire`
/// is the Lockify improvement ceiling.
fn bench_metadata_lockify_local_self_declare(c: &mut Criterion) {
    let state = ClientLeaseState::new_shared();
    let counter = AtomicU64::new(0);
    let client_id = "bench-lockify";
    let lease_ttl = Duration::from_secs(30);

    c.bench_function("metadata/lockify_local_self_declare", |b| {
        b.iter(|| {
            let inode = counter.fetch_add(1, Ordering::Relaxed) + 3_000_000;
            // Lockify: generate a local token (no RPC), insert into
            // cache. The async ownership sync runs in the background
            // and is off the critical path.
            let token = format!("local-{}-{}", client_id, inode);
            let entry = InodeLeaseEntry {
                token,
                expire_at: Instant::now() + lease_ttl,
                mode: LockMode::Exclusive,
            };
            state.put_inode(black_box(inode), entry);
        });
    });
}

// ===========================================================================
// Benchmark 3: current path with zero-latency backend (control)
// ===========================================================================

/// Control variant: same as `current_sync_rpc_acquire` but with a
/// zero-latency backend (no sleep). This isolates the manager + cache
/// overhead from the RPC latency, enabling apples-to-apples
/// comparison with the existing `client_cache.rs::manager_acquire_cache_miss`
/// benchmark. The delta between this and `lockify_local_self_declare`
/// shows the manager overhead that even Lockify can't eliminate
/// (cache lookup + backend call machinery).
struct ZeroLatencyBackend {
    calls: AtomicU64,
}

impl ZeroLatencyBackend {
    fn new() -> Self {
        Self {
            calls: AtomicU64::new(0),
        }
    }
}

#[async_trait::async_trait]
impl FuseLockBackend for ZeroLatencyBackend {
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

fn bench_metadata_current_zero_latency(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let backend: Arc<dyn FuseLockBackend> = Arc::new(ZeroLatencyBackend::new());
    let counter = AtomicU64::new(0);

    c.bench_function("metadata/current_zero_latency_acquire", |b| {
        b.iter_batched_ref(
            || {
                let mgr =
                    FuseLockManager::new(Arc::clone(&backend), "bench-zero".to_string(), 30_000);
                let inode = counter.fetch_add(1, Ordering::Relaxed) + 4_000_000;
                (mgr, inode)
            },
            |(mgr, inode)| {
                rt.block_on(async {
                    let req = LockRequest::new(
                        black_box(*inode),
                        LockMode::Exclusive,
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

criterion_group! {
    name = metadata_benches;
    config = Criterion::default();
    targets =
        bench_metadata_current_sync_rpc,
        bench_metadata_lockify_local_self_declare,
        bench_metadata_current_zero_latency,
}
criterion_main!(metadata_benches);
