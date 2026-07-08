use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use powerfs_core::kv_cache::KVCacheEngine;
use rand::Rng;
use std::sync::Arc;
use std::time::Duration;

fn make_data(size: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(size);
    let mut rng = rand::thread_rng();
    for _ in 0..size {
        data.push(rng.gen());
    }
    data
}

fn make_engine() -> KVCacheEngine {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();
    KVCacheEngine::new_with_db(10 * 1024 * 1024, 1024 * 1024, db_path).unwrap()
}

fn kv_put_benchmark(c: &mut Criterion) {
    let engine = Arc::new(make_engine());
    engine
        .create_namespace("ns_bench", "benchmark", "user1")
        .unwrap();

    let data_sizes = vec![1024, 4096, 16384, 65536];

    let mut group = c.benchmark_group("kv_put");
    group.measurement_time(Duration::from_secs(10));

    for size in &data_sizes {
        let data = make_data(*size);
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::new("kv_put", size), &data, |b, data| {
            let engine = engine.clone();
            b.iter(|| {
                let key = format!("key_{}", rand::thread_rng().gen::<u64>());
                engine.kv_put("ns_bench", &key, data, "user1").unwrap();
            });
        });
    }
    group.finish();
}

fn kv_get_benchmark(c: &mut Criterion) {
    let engine = Arc::new(make_engine());
    engine
        .create_namespace("ns_bench", "benchmark", "user1")
        .unwrap();

    let data_sizes = vec![1024, 4096, 16384, 65536];

    let mut group = c.benchmark_group("kv_get");
    group.measurement_time(Duration::from_secs(10));

    for size in &data_sizes {
        let data = make_data(*size);
        let key = format!("key_get_{}", size);
        engine.kv_put("ns_bench", &key, &data, "user1").unwrap();

        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::new("kv_get", size), &key, |b, key| {
            let engine = engine.clone();
            b.iter(|| {
                let result = engine.kv_get("ns_bench", key).unwrap();
                assert!(result.is_some());
            });
        });
    }
    group.finish();
}

fn kv_batch_put_benchmark(c: &mut Criterion) {
    let engine = Arc::new(make_engine());
    engine
        .create_namespace("ns_bench", "benchmark", "user1")
        .unwrap();

    let batch_sizes = vec![10, 100, 1000];
    let value_size = 4096;

    let mut group = c.benchmark_group("kv_batch_put");
    group.measurement_time(Duration::from_secs(10));

    for batch_size in &batch_sizes {
        group.bench_with_input(
            BenchmarkId::new("kv_batch_put", batch_size),
            batch_size,
            |b, &batch_size| {
                let engine = engine.clone();
                b.iter(|| {
                    for i in 0..batch_size {
                        let key = format!("batch_key_{}_{}", batch_size, i);
                        let value = make_data(value_size);
                        engine.kv_put("ns_bench", &key, &value, "user1").unwrap();
                    }
                });
            },
        );
    }
    group.finish();
}

fn kv_concurrent_benchmark(c: &mut Criterion) {
    let engine = Arc::new(make_engine());
    engine
        .create_namespace("ns_bench", "benchmark", "user1")
        .unwrap();

    c.bench_function("kv_concurrent_put", |b| {
        b.iter_custom(|iters| {
            let start = std::time::Instant::now();
            let mut handles = Vec::new();

            for _ in 0..8 {
                let engine = engine.clone();
                let thread_rng = rand::thread_rng().gen::<u64>();
                handles.push(std::thread::spawn(move || {
                    for i in 0..(iters / 8) {
                        let key = format!("concurrent_key_{}_{}", thread_rng, i);
                        let value = make_data(1024);
                        engine.kv_put("ns_bench", &key, &value, "user1").unwrap();
                    }
                }));
            }

            for handle in handles {
                handle.join().unwrap();
            }

            start.elapsed()
        });
    });
}

fn kv_delete_benchmark(c: &mut Criterion) {
    let engine = Arc::new(make_engine());
    engine
        .create_namespace("ns_bench", "benchmark", "user1")
        .unwrap();

    let key = "delete_key";

    let mut group = c.benchmark_group("kv_delete");
    group.measurement_time(Duration::from_secs(10));

    group.bench_function("kv_delete", |b| {
        b.iter(|| {
            let data = make_data(4096);
            engine.kv_put("ns_bench", key, &data, "user1").unwrap();
            engine.kv_delete("ns_bench", key).unwrap();
        });
    });
    group.finish();
}

fn kv_list_benchmark(c: &mut Criterion) {
    let engine = Arc::new(make_engine());
    engine
        .create_namespace("ns_bench", "benchmark", "user1")
        .unwrap();

    for i in 0..1000 {
        let key = format!("list_key_{:04}", i);
        let data = make_data(100);
        engine.kv_put("ns_bench", &key, &data, "user1").unwrap();
    }

    let mut group = c.benchmark_group("kv_list");
    group.measurement_time(Duration::from_secs(10));

    group.bench_function("kv_list", |b| {
        b.iter(|| {
            let keys = engine.kv_list("ns_bench", None).unwrap();
            assert_eq!(keys.len(), 1000);
        });
    });

    group.bench_function("kv_list_prefix", |b| {
        b.iter(|| {
            let keys = engine.kv_list("ns_bench", Some("list_key_01")).unwrap();
            assert_eq!(keys.len(), 100);
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    kv_put_benchmark,
    kv_get_benchmark,
    kv_batch_put_benchmark,
    kv_concurrent_benchmark,
    kv_delete_benchmark,
    kv_list_benchmark
);
criterion_main!(benches);
