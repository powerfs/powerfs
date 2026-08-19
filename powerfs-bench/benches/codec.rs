//! Benchmarks for the TLV wire codec (§3.1, `powerfs-lock-net`).
//!
//! The Rust FUSE client and the in-kernel C client both encode/decode every
//! lock message with this codec. The encode/decode cost is part of every
//! acquire/release/renew round-trip, so it's a hard floor on RPC latency.
//!
//! Measured:
//! - `encode_frame` for Acquire / Grant / Release / Revoke
//! - `decode_frame` for the same
//!
//! Grant-with-SN is benchmarked separately because the codec omits the
//! `FIELD_SN` when `sn == 0` (forward-compat path); the with-SN variant
//! exercises the extra `write_u64_field`.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use powerfs_lock::{LockMode, Range};
use powerfs_lock_net::{
    codec::{decode_frame, encode_frame},
    msg::{Message, ERR_OK},
};
use std::time::Duration;

// ===========================================================================
// Sample messages
// ===========================================================================

fn sample_acquire() -> Message {
    Message::Acquire {
        inode: 42,
        mode: LockMode::Exclusive,
        range: None,
        timeout: Duration::from_secs(30),
        client_id: "client-A".to_string(),
    }
}

fn sample_acquire_range() -> Message {
    Message::Acquire {
        inode: 42,
        mode: LockMode::Range(Range::new(0, Some(4096))),
        range: Some(Range::new(0, Some(4096))),
        timeout: Duration::from_secs(30),
        client_id: "client-A".to_string(),
    }
}

fn sample_grant_no_sn() -> Message {
    Message::Grant {
        inode: 7,
        token: "lease-0-abc".to_string(),
        sn: 0,
        lease_ms: 30_000,
        mode: LockMode::Exclusive,
        range: None,
        error_code: ERR_OK,
    }
}

fn sample_grant_with_sn() -> Message {
    Message::Grant {
        inode: 7,
        token: "lease-1-def-12345".to_string(),
        sn: 12345,
        lease_ms: 30_000,
        mode: LockMode::Range(Range::new(0, Some(4096))),
        range: Some(Range::new(0, Some(4096))),
        error_code: ERR_OK,
    }
}

fn sample_release() -> Message {
    Message::Release {
        inode: 1,
        token: "lease-0-abc".to_string(),
        client_id: "client-A".to_string(),
    }
}

fn sample_revoke() -> Message {
    Message::Revoke {
        inode: 1,
        token: "lease-0-abc".to_string(),
    }
}

// ===========================================================================
// Encode benches
// ===========================================================================

fn bench_encode_acquire(c: &mut Criterion) {
    let msg = sample_acquire();
    c.bench_function("codec/encode_acquire", |b| {
        b.iter(|| {
            let bytes = encode_frame(black_box(&msg)).expect("encode");
            black_box(bytes);
        });
    });
}

fn bench_encode_acquire_range(c: &mut Criterion) {
    let msg = sample_acquire_range();
    c.bench_function("codec/encode_acquire_range", |b| {
        b.iter(|| {
            let bytes = encode_frame(black_box(&msg)).expect("encode");
            black_box(bytes);
        });
    });
}

fn bench_encode_grant_no_sn(c: &mut Criterion) {
    let msg = sample_grant_no_sn();
    c.bench_function("codec/encode_grant_no_sn", |b| {
        b.iter(|| {
            let bytes = encode_frame(black_box(&msg)).expect("encode");
            black_box(bytes);
        });
    });
}

fn bench_encode_grant_with_sn(c: &mut Criterion) {
    let msg = sample_grant_with_sn();
    c.bench_function("codec/encode_grant_with_sn", |b| {
        b.iter(|| {
            let bytes = encode_frame(black_box(&msg)).expect("encode");
            black_box(bytes);
        });
    });
}

fn bench_encode_release(c: &mut Criterion) {
    let msg = sample_release();
    c.bench_function("codec/encode_release", |b| {
        b.iter(|| {
            let bytes = encode_frame(black_box(&msg)).expect("encode");
            black_box(bytes);
        });
    });
}

fn bench_encode_revoke(c: &mut Criterion) {
    let msg = sample_revoke();
    c.bench_function("codec/encode_revoke", |b| {
        b.iter(|| {
            let bytes = encode_frame(black_box(&msg)).expect("encode");
            black_box(bytes);
        });
    });
}

// ===========================================================================
// Decode benches
// ===========================================================================

fn bench_decode_acquire(c: &mut Criterion) {
    let msg = sample_acquire();
    let bytes = encode_frame(&msg).expect("encode");
    c.bench_function("codec/decode_acquire", |b| {
        b.iter(|| {
            let decoded = decode_frame(black_box(&bytes)).expect("decode");
            black_box(decoded);
        });
    });
}

fn bench_decode_acquire_range(c: &mut Criterion) {
    let msg = sample_acquire_range();
    let bytes = encode_frame(&msg).expect("encode");
    c.bench_function("codec/decode_acquire_range", |b| {
        b.iter(|| {
            let decoded = decode_frame(black_box(&bytes)).expect("decode");
            black_box(decoded);
        });
    });
}

fn bench_decode_grant_no_sn(c: &mut Criterion) {
    let msg = sample_grant_no_sn();
    let bytes = encode_frame(&msg).expect("encode");
    c.bench_function("codec/decode_grant_no_sn", |b| {
        b.iter(|| {
            let decoded = decode_frame(black_box(&bytes)).expect("decode");
            black_box(decoded);
        });
    });
}

fn bench_decode_grant_with_sn(c: &mut Criterion) {
    let msg = sample_grant_with_sn();
    let bytes = encode_frame(&msg).expect("encode");
    c.bench_function("codec/decode_grant_with_sn", |b| {
        b.iter(|| {
            let decoded = decode_frame(black_box(&bytes)).expect("decode");
            black_box(decoded);
        });
    });
}

fn bench_decode_release(c: &mut Criterion) {
    let msg = sample_release();
    let bytes = encode_frame(&msg).expect("encode");
    c.bench_function("codec/decode_release", |b| {
        b.iter(|| {
            let decoded = decode_frame(black_box(&bytes)).expect("decode");
            black_box(decoded);
        });
    });
}

fn bench_decode_revoke(c: &mut Criterion) {
    let msg = sample_revoke();
    let bytes = encode_frame(&msg).expect("encode");
    c.bench_function("codec/decode_revoke", |b| {
        b.iter(|| {
            let decoded = decode_frame(black_box(&bytes)).expect("decode");
            black_box(decoded);
        });
    });
}

// ===========================================================================
// Group
// ===========================================================================

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(300)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));
    targets =
        bench_encode_acquire,
        bench_encode_acquire_range,
        bench_encode_grant_no_sn,
        bench_encode_grant_with_sn,
        bench_encode_release,
        bench_encode_revoke,
        bench_decode_acquire,
        bench_decode_acquire_range,
        bench_decode_grant_no_sn,
        bench_decode_grant_with_sn,
        bench_decode_release,
        bench_decode_revoke,
}

criterion_main!(benches);
