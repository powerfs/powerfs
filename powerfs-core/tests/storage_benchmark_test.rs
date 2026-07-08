use powerfs_core::kv_cache::KVCacheEngine;
use rand::Rng;
use std::sync::Arc;
use std::time::{Duration, Instant};

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

fn benchmark_report(name: &str, ops: usize, duration: Duration) {
    let duration_ms = duration.as_secs_f64() * 1000.0;
    let ops_per_sec = ops as f64 / duration.as_secs_f64();
    println!("\n=== {} ===", name);
    println!("  Operations: {}", ops);
    println!("  Duration:   {:.3} ms", duration_ms);
    println!("  Ops/sec:    {:.2}", ops_per_sec);
}

#[test]
fn storage_write_benchmark() {
    let engine = make_engine();
    engine.create_namespace("ns_write", "write", "user1").unwrap();

    let data_sizes = vec![512, 1024, 2048, 4096];
    let ops_per_size = 100;

    for size in data_sizes {
        let data = make_data(size);
        let start = Instant::now();

        for i in 0..ops_per_size {
            let key = format!("write_key_{}_{}", size, i);
            engine.kv_put("ns_write", &key, &data, "user1").unwrap();
        }

        let duration = start.elapsed();
        benchmark_report(&format!("Write {} bytes", size), ops_per_size, duration);
    }
}

#[test]
fn storage_read_benchmark() {
    let engine = make_engine();
    engine.create_namespace("ns_read", "read", "user1").unwrap();

    let data_sizes = vec![512, 1024, 2048, 4096];
    let ops_per_size = 100;

    for size in &data_sizes {
        let data = make_data(*size);
        for i in 0..ops_per_size {
            let key = format!("read_key_{}_{}", size, i);
            engine.kv_put("ns_read", &key, &data, "user1").unwrap();
        }
    }

    for size in &data_sizes {
        let start = Instant::now();

        for i in 0..ops_per_size {
            let key = format!("read_key_{}_{}", size, i);
            let result = engine.kv_get("ns_read", &key).unwrap();
            assert!(result.is_some());
        }

        let duration = start.elapsed();
        benchmark_report(&format!("Read {} bytes", size), ops_per_size, duration);
    }
}

#[test]
fn storage_read_write_mixed_benchmark() {
    let engine = make_engine();
    engine.create_namespace("ns_mixed", "mixed", "user1").unwrap();

    for i in 0..100 {
        let key = format!("mixed_key_{:04}", i);
        let data = make_data(4096);
        engine.kv_put("ns_mixed", &key, &data, "user1").unwrap();
    }

    let total_ops = 1000;
    let start = Instant::now();

    for i in 0..total_ops {
        if i % 2 == 0 {
            let rng = rand::thread_rng().gen::<usize>() % 100;
            let key = format!("mixed_key_{:04}", rng);
            let _ = engine.kv_get("ns_mixed", &key).unwrap();
        } else {
            let key = format!("mixed_new_key_{}", i);
            let data = make_data(4096);
            engine.kv_put("ns_mixed", &key, &data, "user1").unwrap();
        }
    }

    let duration = start.elapsed();
    benchmark_report("Read/Write Mixed (50/50)", total_ops, duration);
}

#[test]
fn storage_concurrent_write_benchmark() {
    let engine = Arc::new(make_engine());
    engine.create_namespace("ns_concurrent_write", "concurrent_write", "user1").unwrap();

    let thread_counts = vec![1, 2, 4];
    let ops_per_thread = 100;

    for threads in thread_counts {
        let start = Instant::now();
        let mut handles = Vec::new();

        for thread_id in 0..threads {
            let engine = engine.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..ops_per_thread {
                    let key = format!("concurrent_write_{}_{}", thread_id, i);
                    let data = make_data(4096);
                    engine.kv_put("ns_concurrent_write", &key, &data, "user1").unwrap();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let duration = start.elapsed();
        benchmark_report(
            &format!("Concurrent Write ({} threads)", threads),
            threads * ops_per_thread,
            duration,
        );
    }
}

#[test]
fn storage_concurrent_read_benchmark() {
    let engine = Arc::new(make_engine());
    engine.create_namespace("ns_concurrent_read", "concurrent_read", "user1").unwrap();

    for i in 0..1000 {
        let key = format!("concurrent_read_key_{:05}", i);
        let data = make_data(4096);
        engine.kv_put("ns_concurrent_read", &key, &data, "user1").unwrap();
    }

    let thread_counts = vec![1, 2, 4];
    let ops_per_thread = 100;

    for threads in thread_counts {
        let start = Instant::now();
        let mut handles = Vec::new();

        for _ in 0..threads {
            let engine = engine.clone();
            handles.push(std::thread::spawn(move || {
                for _ in 0..ops_per_thread {
                    let rng = rand::thread_rng().gen::<usize>() % 1000;
                    let key = format!("concurrent_read_key_{:05}", rng);
                    let result = engine.kv_get("ns_concurrent_read", &key).unwrap();
                    assert!(result.is_some());
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let duration = start.elapsed();
        benchmark_report(
            &format!("Concurrent Read ({} threads)", threads),
            threads * ops_per_thread,
            duration,
        );
    }
}

#[test]
fn storage_persistence_benchmark() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();

    let iterations = 5;
    let start = Instant::now();

    for iteration in 0..iterations {
        let engine = KVCacheEngine::new_with_db(10 * 1024 * 1024, 1024 * 1024, db_path).unwrap();
        let ns_name = format!("ns_persist_{}", iteration);
        engine.create_namespace(&ns_name, "persist", "user1").unwrap();

        for i in 0..50 {
            let key = format!("persist_key_{}_{}", iteration, i);
            let data = make_data(1024);
            engine.kv_put(&ns_name, &key, &data, "user1").unwrap();
        }

        drop(engine);

        let engine2 = KVCacheEngine::new_with_db(10 * 1024 * 1024, 1024 * 1024, db_path).unwrap();
        assert!(engine2.get_namespace(&ns_name).is_some());

        for i in 0..50 {
            let key = format!("persist_key_{}_{}", iteration, i);
            let result = engine2.kv_get(&ns_name, &key).unwrap();
            assert!(result.is_some());
        }

        drop(engine2);
    }

    let duration = start.elapsed();
    benchmark_report(&format!("Persistence Reload ({} iterations)", iterations), iterations, duration);
}

#[test]
fn storage_large_value_benchmark() {
    let engine = make_engine();
    engine.create_namespace("ns_large", "large", "user1").unwrap();

    let large_sizes = vec![256 * 1024, 512 * 1024, 1024 * 1024];

    for size in &large_sizes {
        let data = make_data(*size);
        let start = Instant::now();

        for i in 0..5 {
            let key = format!("large_write_key_{}_{}", size, i);
            engine.kv_put("ns_large", &key, &data, "user1").unwrap();
        }

        let duration = start.elapsed();
        benchmark_report(&format!("Large Value Write {} KB", size / 1024), 5, duration);
    }

    for size in &large_sizes {
        let start = Instant::now();

        for i in 0..5 {
            let key = format!("large_write_key_{}_{}", size, i);
            let result = engine.kv_get("ns_large", &key).unwrap();
            assert!(result.is_some());
        }

        let duration = start.elapsed();
        benchmark_report(&format!("Large Value Read {} KB", size / 1024), 5, duration);
    }
}