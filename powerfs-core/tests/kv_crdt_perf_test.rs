use powerfs_core::kv_cache::KVCacheEngine;
use std::sync::Arc;
use std::time::{Duration, Instant};

struct PerfResult {
    operation: String,
    count: usize,
    duration: Duration,
    ops_per_sec: f64,
    avg_latency_ms: f64,
}

impl PerfResult {
    fn new(operation: &str, count: usize, duration: Duration) -> Self {
        let ops_per_sec = count as f64 / duration.as_secs_f64();
        let avg_latency_ms = (duration.as_nanos() as f64 / count as f64) / 1_000_000.0;
        Self {
            operation: operation.to_string(),
            count,
            duration,
            ops_per_sec,
            avg_latency_ms,
        }
    }

    fn print(&self) {
        println!(
            "| {:<20} | {:>8} | {:>12.2} | {:>15.2} | {:>14.4} |",
            self.operation,
            self.count,
            self.duration.as_secs_f64(),
            self.ops_per_sec,
            self.avg_latency_ms
        );
    }
}

fn run_benchmark(
    engine: Arc<KVCacheEngine>,
    key_prefix: &str,
    iterations: usize,
) -> Vec<PerfResult> {
    let mut results = Vec::new();

    let start = Instant::now();
    for i in 0..iterations {
        let key = format!("{}-{}", key_prefix, i);
        let value = vec![0u8; 1024];
        engine.kv_put("test-ns", &key, &value, "test-user").unwrap();
    }
    let duration = start.elapsed();
    results.push(PerfResult::new("kv_put", iterations, duration));

    let start = Instant::now();
    for i in 0..iterations {
        let key = format!("{}-{}", key_prefix, i);
        let _ = engine.kv_get("test-ns", &key);
    }
    let duration = start.elapsed();
    results.push(PerfResult::new("kv_get", iterations, duration));

    let start = Instant::now();
    for i in 0..iterations {
        let key = format!("{}-{}", key_prefix, i);
        let _ = engine.kv_exists("test-ns", &key);
    }
    let duration = start.elapsed();
    results.push(PerfResult::new("kv_exists", iterations, duration));

    let start = Instant::now();
    for i in 0..iterations {
        let key = format!("{}-{}", key_prefix, i);
        engine.kv_delete("test-ns", &key).unwrap();
    }
    let duration = start.elapsed();
    results.push(PerfResult::new("kv_delete", iterations, duration));

    results
}

fn print_results(results: &[PerfResult], title: &str) {
    println!("\n{}", "=".repeat(90));
    println!("{}", title);
    println!("{}", "=".repeat(90));
    println!(
        "| {:<20} | {:>8} | {:>12} | {:>15} | {:>14} |",
        "Operation", "Count", "Duration(s)", "Ops/sec", "Avg Latency(ms)"
    );
    println!("{}", "-".repeat(90));
    for result in results {
        result.print();
    }
    println!("{}", "=".repeat(90));
}

fn main() {
    println!("\nKV CRDT 架构性能测试");
    println!("{}", "=".repeat(90));
    println!("测试配置:");
    println!("  - 单次测试迭代: 10,000 次操作");
    println!("  - 测试轮数: 3 轮");
    println!("  - 单条数据大小: 1KB");
    println!("  - 内存限制: 1GB");
    println!("  - 块大小: 64KB");

    let engine = Arc::new(KVCacheEngine::new(1024 * 1024 * 1024, 64 * 1024));

    engine
        .create_namespace("test-ns", "test-namespace", "test-user")
        .unwrap();

    let iterations = 10_000;
    let mut all_results: Vec<Vec<PerfResult>> = Vec::new();

    for round in 1..=3 {
        let key_prefix = format!("bench-{}", round);
        let results = run_benchmark(engine.clone(), &key_prefix, iterations);
        print_results(&results, &format!("第 {} 轮测试", round));
        all_results.push(results);
    }

    println!("\n{}", "=".repeat(90));
    println!("综合统计 (3轮平均)");
    println!("{}", "=".repeat(90));
    println!(
        "| {:<20} | {:>15} | {:>14} |",
        "Operation", "Avg Ops/sec", "Avg Latency(ms)"
    );
    println!("{}", "-".repeat(90));

    let operations = ["kv_put", "kv_get", "kv_exists", "kv_delete"];
    for op in &operations {
        let ops_per_sec: Vec<f64> = all_results
            .iter()
            .flat_map(|r| {
                r.iter()
                    .filter(|p| p.operation == *op)
                    .map(|p| p.ops_per_sec)
            })
            .collect();
        let avg_latency: Vec<f64> = all_results
            .iter()
            .flat_map(|r| {
                r.iter()
                    .filter(|p| p.operation == *op)
                    .map(|p| p.avg_latency_ms)
            })
            .collect();

        let avg_ops = ops_per_sec.iter().sum::<f64>() / ops_per_sec.len() as f64;
        let avg_lat = avg_latency.iter().sum::<f64>() / avg_latency.len() as f64;

        println!("| {:<20} | {:>15.2} | {:>14.4} |", op, avg_ops, avg_lat);
    }

    let stats = engine.kv_get_stats();
    println!("\n{}", "=".repeat(90));
    println!("CRDT 状态统计");
    println!("{}", "=".repeat(90));
    println!("  - Replica ID: {}", engine.kv_get_replica_id());
    println!("  - 键总数: {}", stats.key_count);
    println!("  - 操作计数器: {}", stats.counter);
    println!("  - 内存缓存键数: {}", engine.kv_snapshot().len());

    println!("\n测试完成!");
}
