use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use crate::metadata_manager::MetadataManager;

#[test]
fn benchmark_single_thread() {
    let mgr = MetadataManager::new_local(1);

    println!("\n=== Benchmark: Single-threaded mkdir/lookup/rmdir ===");
    let start = Instant::now();
    for i in 0..10000 {
        let _ = mgr.mkdir(1, &format!("dir{}", i), 0o755);
        let _ = mgr.lookup(1, &format!("dir{}", i));
        let _ = mgr.rmdir(1, &format!("dir{}", i));
    }
    let elapsed = start.elapsed();
    println!(
        "Time: {:?}, Throughput: {:.2} ops/s",
        elapsed,
        30000.0 / elapsed.as_secs_f64()
    );

    println!("\n=== Benchmark: Single-threaded create/unlink ===");
    let start = Instant::now();
    for i in 0..10000 {
        let _ = mgr.create(1, &format!("file{}", i), 0o644);
        let _ = mgr.lookup(1, &format!("file{}", i));
        let _ = mgr.unlink(1, &format!("file{}", i));
    }
    let elapsed = start.elapsed();
    println!(
        "Time: {:?}, Throughput: {:.2} ops/s",
        elapsed,
        30000.0 / elapsed.as_secs_f64()
    );
}

#[test]
fn benchmark_multi_thread() {
    let mgr = Arc::new(MetadataManager::new_local(1));
    let num_threads = 8;
    let ops_per_thread = 1000;

    println!("\n=== Benchmark: Multi-threaded (8 threads) mkdir/lookup/rmdir ===");
    let barrier = Arc::new(Barrier::new(num_threads));
    let start = Instant::now();

    let mut handles = Vec::with_capacity(num_threads);
    for t in 0..num_threads {
        let mgr_clone = mgr.clone();
        let barrier_clone = barrier.clone();
        handles.push(thread::spawn(move || {
            barrier_clone.wait();
            for i in 0..ops_per_thread {
                let name = format!("thread{}_dir{}", t, i);
                let _ = mgr_clone.mkdir(1, &name, 0o755);
                let _ = mgr_clone.lookup(1, &name);
                let _ = mgr_clone.rmdir(1, &name);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let elapsed = start.elapsed();
    let total_ops = num_threads * ops_per_thread * 3;
    println!(
        "Time: {:?}, Throughput: {:.2} ops/s",
        elapsed,
        total_ops as f64 / elapsed.as_secs_f64()
    );

    println!("\n=== Benchmark: Multi-threaded (8 threads) create/unlink ===");
    let barrier = Arc::new(Barrier::new(num_threads));
    let start = Instant::now();

    let mut handles = Vec::with_capacity(num_threads);
    for t in 0..num_threads {
        let mgr_clone = mgr.clone();
        let barrier_clone = barrier.clone();
        handles.push(thread::spawn(move || {
            barrier_clone.wait();
            for i in 0..ops_per_thread {
                let name = format!("thread{}_file{}", t, i);
                let _ = mgr_clone.create(1, &name, 0o644);
                let _ = mgr_clone.lookup(1, &name);
                let _ = mgr_clone.unlink(1, &name);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let elapsed = start.elapsed();
    let total_ops = num_threads * ops_per_thread * 3;
    println!(
        "Time: {:?}, Throughput: {:.2} ops/s",
        elapsed,
        total_ops as f64 / elapsed.as_secs_f64()
    );
}

#[test]
fn benchmark_list_dir() {
    let mgr = MetadataManager::new_local(1);

    println!("\n=== Benchmark: List dir with 10000 entries ===");
    for i in 0..10000 {
        let _ = mgr.create(1, &format!("file{}", i), 0o644);
    }

    let start = Instant::now();
    let _ = mgr.list_dir(1);
    let elapsed = start.elapsed();
    println!("Time to list 10000 entries: {:?}", elapsed);

    for i in 0..10000 {
        let _ = mgr.unlink(1, &format!("file{}", i));
    }
}

#[test]
fn benchmark_rename() {
    let mgr = MetadataManager::new_local(1);

    println!("\n=== Benchmark: Rename operations ===");
    for i in 0..500 {
        let _ = mgr.create(1, &format!("file{}", i), 0o644);
    }

    let start = Instant::now();
    for i in 0..500 {
        let _ = mgr.rename(1, &format!("file{}", i), 1, &format!("renamed{}", i));
    }

    let _ = mgr.mkdir(1, "subdir", 0o755);
    for i in 0..500 {
        let _ = mgr.rename(1, &format!("renamed{}", i), 2, &format!("file{}", i));
    }

    let elapsed = start.elapsed();
    println!(
        "Time for 1000 rename operations: {:?}, Throughput: {:.2} ops/s",
        elapsed,
        1000.0 / elapsed.as_secs_f64()
    );

    let _ = mgr.rmdir(1, "subdir");
}
