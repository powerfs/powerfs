//! Benchmarks for the server-side lease store hot paths.
//!
//! Covers `MemoryLeaseStore<K>` (the generic primitive reused by both the
//! filer's `InodeLeaseManager` and the volume's `RangeLeaseManager`). We
//! use a local `BenchKey` that mirrors `InodeKey`'s conflict semantics
//! (same inode → conflict) so we don't have to pull the heavy `powerfs-filer`
//! dependency (rocksdb/axum/openraft/tonic) into the bench build.
//!
//! Bench harnesses intentionally call `acquire` / `renew` / `release`
//! purely to time them; the returned `Result` is `debug_assert!`-ed then
//! dropped via `black_box`. The `unused_must_use` lint on `Result` would
//! fire for that pattern, so we allow it crate-wide here.

#![allow(unused_must_use)]
//!
//! The hot paths measured:
//! - `acquire` clean — first acquire on an inode nobody holds
//! - `acquire` conflict — acquire on an inode already held by another client
//! - `renew` — extend an existing lease
//! - `release` — release an existing lease
//! - `acquire + release` combined — the typical per-write cycle
//! - `validate_token` — the per-write-IO guard (called once per `write_needle`)
//! - `cleanup_expired` empty — the periodic sweep cost when nothing's expired

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use powerfs_lease::{LeaseError, LeaseKey, LeaseMode, LeaseStore, MemoryLeaseStore};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

// ===========================================================================
// BenchKey — mirror of `powerfs_filer::InodeKey` semantics
// ===========================================================================

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct BenchKey {
    inode: u64,
}

impl BenchKey {
    fn new(inode: u64) -> Self {
        Self { inode }
    }
}

impl LeaseKey for BenchKey {
    fn group_id(&self) -> u64 {
        self.inode
    }
    fn conflicts(&self, other: &Self) -> bool {
        self.inode == other.inode
    }
    fn encode(&self) -> Vec<u8> {
        self.inode.to_le_bytes().to_vec()
    }
    fn decode(data: &[u8]) -> Result<Self, LeaseError> {
        if data.len() < 8 {
            return Err(LeaseError::Internal(format!(
                "BenchKey decode: expected 8 bytes, got {}",
                data.len()
            )));
        }
        let inode = u64::from_le_bytes(data[0..8].try_into().unwrap());
        Ok(Self { inode })
    }
}

// ===========================================================================
// acquire: clean path (fresh inode per call)
// ===========================================================================

fn bench_acquire_clean(c: &mut Criterion) {
    let store = MemoryLeaseStore::<BenchKey>::new();
    let counter = AtomicU64::new(0);
    c.bench_function("store/acquire_clean", |b| {
        b.iter(|| {
            let i = counter.fetch_add(1, Ordering::Relaxed);
            let key = BenchKey::new(black_box(i));
            let entry = store
                .acquire(
                    key,
                    "client-A",
                    LeaseMode::Exclusive,
                    Duration::from_secs(30),
                )
                .expect("clean acquire");
            black_box(entry);
        });
    });
}

// ===========================================================================
// acquire: conflict path (pre-held by another client)
// ===========================================================================

fn bench_acquire_conflict(c: &mut Criterion) {
    let store = MemoryLeaseStore::<BenchKey>::new();
    // Pre-acquire the inode with holder "A".
    store
        .acquire(
            BenchKey::new(42),
            "client-A",
            LeaseMode::Exclusive,
            Duration::from_secs(60),
        )
        .expect("prime");

    c.bench_function("store/acquire_conflict", |b| {
        b.iter(|| {
            let res = store.acquire(
                black_box(BenchKey::new(42)),
                "client-B",
                LeaseMode::Exclusive,
                Duration::from_secs(30),
            );
            // Expect Conflict every time.
            debug_assert!(matches!(res, Err(LeaseError::Conflict(_))));
            black_box(res);
        });
    });
}

// ===========================================================================
// renew: extend an existing lease
// ===========================================================================

fn bench_renew(c: &mut Criterion) {
    let store = MemoryLeaseStore::<BenchKey>::new();
    let entry = store
        .acquire(
            BenchKey::new(7),
            "client-A",
            LeaseMode::Exclusive,
            Duration::from_secs(30),
        )
        .expect("prime");
    let token = entry.token.clone();

    c.bench_function("store/renew", |b| {
        b.iter(|| {
            let res = store.renew(black_box(&token), "client-A", Duration::from_secs(30));
            res.expect("renew ok");
        });
    });
}

// ===========================================================================
// validate_token: per-write-IO guard
// ===========================================================================

fn bench_validate_token(c: &mut Criterion) {
    let store = MemoryLeaseStore::<BenchKey>::new();
    let entry = store
        .acquire(
            BenchKey::new(7),
            "client-A",
            LeaseMode::Exclusive,
            Duration::from_secs(30),
        )
        .expect("prime");
    let token = entry.token.clone();

    c.bench_function("store/validate_token", |b| {
        b.iter(|| {
            let res = store.validate_token(black_box(&token), "client-A");
            res.expect("valid");
        });
    });
}

// ===========================================================================
// acquire + release combined: the typical per-write cycle
// ===========================================================================

fn bench_acquire_then_release(c: &mut Criterion) {
    let store = MemoryLeaseStore::<BenchKey>::new();
    let counter = AtomicU64::new(0);
    c.bench_function("store/acquire_then_release", |b| {
        b.iter(|| {
            let i = counter.fetch_add(1, Ordering::Relaxed);
            let key = BenchKey::new(i);
            let entry = store
                .acquire(
                    key,
                    "client-A",
                    LeaseMode::Exclusive,
                    Duration::from_secs(30),
                )
                .expect("acquire");
            store.release(&entry.token, "client-A").expect("release");
        });
    });
}

// ===========================================================================
// cleanup_expired: periodic sweep cost when nothing is expired
// ===========================================================================

fn bench_cleanup_expired_empty(c: &mut Criterion) {
    let store = MemoryLeaseStore::<BenchKey>::new();
    // Populate with 100 fresh entries.
    for i in 0..100u64 {
        store
            .acquire(
                BenchKey::new(i),
                "client-A",
                LeaseMode::Exclusive,
                Duration::from_secs(60),
            )
            .expect("prime");
    }
    c.bench_function("store/cleanup_expired_empty", |b| {
        b.iter(|| {
            let n = store.cleanup_expired();
            black_box(n);
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
        bench_acquire_clean,
        bench_acquire_conflict,
        bench_renew,
        bench_validate_token,
        bench_acquire_then_release,
        bench_cleanup_expired_empty,
}

criterion_main!(benches);
