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

fn storage_write_benchmark(c: &mut Criterion) {
    let engine = Arc::new(make_engine());
    engine
        .create_namespace("ns_storage_write", "storage_write", "user1")
        .unwrap();

    let data_sizes = vec![512, 1024, 2048, 4096, 8192, 16384, 32768, 65536];

    let mut group = c.benchmark_group("storage_write");
    group.measurement_time(Duration::from_secs(10));

    for size in &data_sizes {
        let data = make_data(*size);
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::new("storage_write", size), &data, |b, data| {
            let engine = engine.clone();
            b.iter(|| {
                let key = format!("write_key_{}", rand::thread_rng().gen::<u64>());
                engine.kv_put("ns_storage_write", &key, data, "user1").unwrap();
            });
        });
    }
    group.finish();
}

fn storage_read_benchmark(c: &mut Criterion) {
    let engine = Arc::new(make_engine());
    engine
        .create_namespace("ns_storage_read", "storage_read", "user1")
        .unwrap();

    let data_sizes = vec![512, 1024, 2048, 4096, 8192, 16384, 32768, 65536];
    let mut keys = Vec::new();

    for size in &data_sizes {
        let data = make_data(*size);
        let key = format!("read_key_{}", size);
        engine.kv_put("ns_storage_read", &key, &data, "user1").unwrap();
        keys.push((key, data));
    }

    let mut group = c.benchmark_group("storage_read");
    group.measurement_time(Duration::from_secs(10));

    for (key, expected_data) in keys {
        let size = expected_data.len();
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("storage_read", size), &key, |b, key| {
            let engine = engine.clone();
            b.iter(|| {
                let result = engine.kv_get("ns_storage_read", key).unwrap();
                assert!(result.is_some());
                assert_eq!(result.unwrap().data, expected_data);
            });
        });
    }
    group.finish();
}

fn storage_read_write_mixed_benchmark(c: &mut Criterion) {
    let engine = Arc::new(make_engine());
    engine
        .create_namespace("ns_mixed", "mixed", "user1")
        .unwrap();

    for i in 0..1000 {
        let key = format!("mixed_key_{:04}", i);
        let data = make_data(4096);
        engine.kv_put("ns_mixed", &key, &data, "user1").unwrap();
    }

    let mut group = c.benchmark_group("storage_read_write_mixed");
    group.measurement_time(Duration::from_secs(10));

    group.bench_function("read_write_50_50", |b| {
        let engine = engine.clone();
        b.iter(|| {
            let rng = rand::thread_rng().gen::<u64>() % 1000;
            let read_key = format!("mixed_key_{:04}", rng);
            let _ = engine.kv_get("ns_mixed", &read_key).unwrap();

            let write_key = format!("mixed_key_new_{}", rand::thread_rng().gen::<u64>());
            let data = make_data(4096);
            engine.kv_put("ns_mixed", &write_key, &data, "user1").unwrap();
        });
    });

    group.finish();
}

fn storage_concurrent_write_benchmark(c: &mut Criterion) {
    let engine = Arc::new(make_engine());
    engine
        .create_namespace("ns_concurrent_write", "concurrent_write", "user1")
        .unwrap();

    let thread_counts = vec![1, 2, 4, 8, 16];

    let mut group = c.benchmark_group("storage_concurrent_write");
    group.measurement_time(Duration::from_secs(15));

    for threads in &thread_counts {
        group.bench_with_input(BenchmarkId::new("concurrent_write", threads), threads, |b, &threads| {
            let engine = engine.clone();
            b.iter_custom(|iters| {
                let start = std::time::Instant::now();
                let mut handles = Vec::new();

                for thread_id in 0..threads {
                    let engine = engine.clone();
                    handles.push(std::thread::spawn(move || {
                        let requests_per_thread = iters / threads as u64;
                        for i in 0..requests_per_thread {
                            let key = format!("concurrent_write_{}_{}_{}", thread_id, i, rand::thread_rng().gen::<u64>());
                            let value = make_data(4096);
                            engine.kv_put("ns_concurrent_write", &key, &value, "user1").unwrap();
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
    group.finish();
}

fn storage_concurrent_read_benchmark(c: &mut Criterion) {
    let engine = Arc::new(make_engine());
    engine
        .create_namespace("ns_concurrent_read", "concurrent_read", "user1")
        .unwrap();

    for i in 0..10000 {
        let key = format!("concurrent_read_key_{:05}", i);
        let data = make_data(4096);
        engine.kv_put("ns_concurrent_read", &key, &data, "user1").unwrap();
    }

    let thread_counts = vec![1, 2, 4, 8, 16];

    let mut group = c.benchmark_group("storage_concurrent_read");
    group.measurement_time(Duration::from_secs(15));

    for threads in &thread_counts {
        group.bench_with_input(BenchmarkId::new("concurrent_read", threads), threads, |b, &threads| {
            let engine = engine.clone();
            b.iter_custom(|iters| {
                let start = std::time::Instant::now();
                let mut handles = Vec::new();

                for _ in 0..threads {
                    let engine = engine.clone();
                    handles.push(std::thread::spawn(move || {
                        let requests_per_thread = iters / threads as u64;
                        for _ in 0..requests_per_thread {
                            let rng = rand::thread_rng().gen::<u64>() % 10000;
                            let key = format!("concurrent_read_key_{:05}", rng);
                            let result = engine.kv_get("ns_concurrent_read", &key).unwrap();
                            assert!(result.is_some());
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
    group.finish();
}

fn storage_persistence_benchmark(c: &mut Criterion) {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();

    c.bench_function("storage_persistence_reload", |b| {
        b.iter_custom(|iters| {
            let start = std::time::Instant::now();

            for _ in 0..iters {
                let engine = KVCacheEngine::new_with_db(10 * 1024 * 1024, 1024 * 1024, db_path).unwrap();
                engine.create_namespace("ns_persist", "persist", "user1").unwrap();

                for i in 0..100 {
                    let key = format!("persist_key_{}", i);
                    let data = make_data(1024);
                    engine.kv_put("ns_persist", &key, &data, "user1").unwrap();
                }

                drop(engine);

                let engine2 = KVCacheEngine::new_with_db(10 * 1024 * 1024, 1024 * 1024, db_path).unwrap();
                assert!(engine2.get_namespace("ns_persist").is_some());

                for i in 0..100 {
                    let key = format!("persist_key_{}", i);
                    let result = engine2.kv_get("ns_persist", &key).unwrap();
                    assert!(result.is_some());
                }

                drop(engine2);
            }

            start.elapsed()
        });
    });
}

fn storage_batch_operations_benchmark(c: &mut Criterion) {
    let engine = Arc::new(make_engine());
    engine
        .create_namespace("ns_batch", "batch", "user1")
        .unwrap();

    let batch_sizes = vec![10, 50, 100, 500, 1000];

    let mut group = c.benchmark_group("storage_batch_operations");
    group.measurement_time(Duration::from_secs(10));

    for batch_size in &batch_sizes {
        group.bench_with_input(BenchmarkId::new("batch_put", batch_size), batch_size, |b, &batch_size| {
            let engine = engine.clone();
            b.iter(|| {
                let base_key = rand::thread_rng().gen::<u64>();
                for i in 0..batch_size {
                    let key = format!("batch_key_{}_{}", base_key, i);
                    let value = make_data(1024);
                    engine.kv_put("ns_batch", &key, &value, "user1").unwrap();
                }
            });
        });
    }

    for i in 0..10000 {
        let key = format!("list_batch_key_{:05}", i);
        let data = make_data(100);
        engine.kv_put("ns_batch", &key, &data, "user1").unwrap();
    }

    group.bench_function("list_10000_keys", |b| {
        let engine = engine.clone();
        b.iter(|| {
            let keys = engine.kv_list("ns_batch", None).unwrap();
            assert!(keys.len() >= 10000);
        });
    });

    group.finish();
}

fn storage_large_value_benchmark(c: &mut Criterion) {
    let engine = Arc::new(make_engine());
    engine
        .create_namespace("ns_large", "large", "user1")
        .unwrap();

    let large_sizes = vec![1024 * 1024, 2 * 1024 * 1024, 4 * 1024 * 1024];

    let mut group = c.benchmark_group("storage_large_value");
    group.measurement_time(Duration::from_secs(15));

    for size in &large_sizes {
        let data = make_data(*size);
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::new("large_value_write", size), &data, |b, data| {
            let engine = engine.clone();
            b.iter(|| {
                let key = format!("large_key_{}", rand::thread_rng().gen::<u64>());
                engine.kv_put("ns_large", &key, data, "user1").unwrap();
            });
        });
    }

    for size in &large_sizes {
        let data = make_data(*size);
        let key = format!("large_read_key_{}", size);
        engine.kv_put("ns_large", &key, &data, "user1").unwrap();

        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::new("large_value_read", size), &key, |b, key| {
            let engine = engine.clone();
            b.iter(|| {
                let result = engine.kv_get("ns_large", key).unwrap();
                assert!(result.is_some());
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    storage_write_benchmark,
    storage_read_benchmark,
    storage_read_write_mixed_benchmark,
    storage_concurrent_write_benchmark,
    storage_concurrent_read_benchmark,
    storage_persistence_benchmark,
    storage_batch_operations_benchmark,
    storage_large_value_benchmark
);
criterion_main!(benches);