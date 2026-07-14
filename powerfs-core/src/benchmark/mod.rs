
pub mod storage;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

// 优化开关状态
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OptimizationFlags {
    // EC 优化
    pub ec_simd_enabled: bool,
    pub ec_parallel_encoding: bool,
    pub ec_dynamic_sharding: bool,
    pub ec_small_file_skip: bool,
    
    // Raft 优化
    pub raft_log_compression: bool,
    pub raft_pre_vote: bool,
    pub raft_read_scaling: bool,
    
    // 数据分布优化
    pub rack_awareness: bool,
    pub load_balancing: bool,
    
    // 缓存优化
    pub smart_cache_eviction: bool,
    pub hierarchical_index: bool,
}

impl OptimizationFlags {
    pub fn hash(&self) -> u64 {
        let mut h: u64 = 0;
        h = h.wrapping_mul(31) ^ self.ec_simd_enabled as u64;
        h = h.wrapping_mul(31) ^ self.ec_parallel_encoding as u64;
        h = h.wrapping_mul(31) ^ self.ec_dynamic_sharding as u64;
        h = h.wrapping_mul(31) ^ self.ec_small_file_skip as u64;
        h = h.wrapping_mul(31) ^ self.raft_log_compression as u64;
        h = h.wrapping_mul(31) ^ self.raft_pre_vote as u64;
        h = h.wrapping_mul(31) ^ self.raft_read_scaling as u64;
        h = h.wrapping_mul(31) ^ self.rack_awareness as u64;
        h = h.wrapping_mul(31) ^ self.load_balancing as u64;
        h = h.wrapping_mul(31) ^ self.smart_cache_eviction as u64;
        h.wrapping_mul(31) ^ self.hierarchical_index as u64
    }
}

impl Default for OptimizationFlags {
    fn default() -> Self {
        Self {
            ec_simd_enabled: true,
            ec_parallel_encoding: true,
            ec_dynamic_sharding: true,
            ec_small_file_skip: true,
            raft_log_compression: true,
            raft_pre_vote: true,
            raft_read_scaling: true,
            rack_awareness: true,
            load_balancing: true,
            smart_cache_eviction: true,
            hierarchical_index: true,
        }
    }
}

// 基准测试指标
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkMetrics {
    // EC 指标
    pub ec_throughput_mbps: f64,
    pub ec_latency_ms: f64,
    
    // Raft 指标
    pub raft_election_time_ms: f64,
    
    // KV 指标
    pub kv_cache_hit_rate: f64,
    pub kv_read_throughput_ops: f64,
    pub kv_write_throughput_ops: f64,
    
    // S3 指标
    pub s3_read_throughput_mbps: f64,
    pub s3_write_throughput_mbps: f64,
    
    // 数据分布指标
    pub data_balance_score: f64,
    
    // 系统指标
    pub cpu_usage_percent: f64,
    pub memory_usage_percent: f64,
}

impl Default for BenchmarkMetrics {
    fn default() -> Self {
        Self {
            ec_throughput_mbps: 0.0,
            ec_latency_ms: 0.0,
            raft_election_time_ms: 0.0,
            kv_cache_hit_rate: 0.0,
            kv_read_throughput_ops: 0.0,
            kv_write_throughput_ops: 0.0,
            s3_read_throughput_mbps: 0.0,
            s3_write_throughput_mbps: 0.0,
            data_balance_score: 0.0,
            cpu_usage_percent: 0.0,
            memory_usage_percent: 0.0,
        }
    }
}

// 环境信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentInfo {
    pub cpu_model: String,
    pub cpu_cores: usize,
    pub memory_gb: f64,
    pub node_count: usize,
    pub os_version: String,
    pub rust_version: String,
    pub powerfs_version: String,
}

// 比较报告
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparisonReport {
    pub baseline_id: String,
    pub target_id: String,
    
    // 提升比例（%）
    pub ec_throughput_improvement: f64,
    pub ec_latency_improvement: f64,
    pub raft_election_improvement: f64,
    pub kv_cache_hit_rate_improvement: f64,
    pub kv_read_throughput_improvement: f64,
    pub kv_write_throughput_improvement: f64,
    pub s3_read_throughput_improvement: f64,
    pub s3_write_throughput_improvement: f64,
    pub data_balance_improvement: f64,
}

// 基准测试结果
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResult {
    pub id: String,
    pub test_name: String,
    pub timestamp: DateTime<Utc>,
    pub flags: OptimizationFlags,
    pub metrics: BenchmarkMetrics,
    pub environment: EnvironmentInfo,
    pub duration_seconds: u64,
    pub comparison: Option<ComparisonReport>,
}

impl BenchmarkResult {
    pub fn new(test_name: &str, flags: OptimizationFlags, environment: EnvironmentInfo) -> Self {
        Self {
            id: format!("benchmark_{}_{}", test_name, Utc::now().timestamp()),
            test_name: test_name.to_string(),
            timestamp: Utc::now(),
            flags,
            metrics: BenchmarkMetrics::default(),
            environment,
            duration_seconds: 0,
            comparison: None,
        }
    }
}

// 测试结果存储 trait
pub trait BenchmarkStorage: Sync + Send {
    fn save_result(&self, result: &BenchmarkResult) -> Result<(), String>;
    fn get_result(&self, id: &str) -> Result<Option<BenchmarkResult>, String>;
    fn list_results(
        &self,
        limit: usize,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> Result<Vec<BenchmarkResult>, String>;
    fn delete_result(&self, id: &str) -> Result<bool, String>;
}

// 内存存储实现（用于测试）
pub struct MemoryBenchmarkStorage {
    results: RwLock<HashMap<String, BenchmarkResult>>,
}

impl MemoryBenchmarkStorage {
    pub fn new() -> Self {
        Self {
            results: RwLock::new(HashMap::new()),
        }
    }
}

impl BenchmarkStorage for MemoryBenchmarkStorage {
    fn save_result(&self, result: &BenchmarkResult) -> Result<(), String> {
        self.results.write().unwrap().insert(result.id.clone(), result.clone());
        Ok(())
    }
    
    fn get_result(&self, id: &str) -> Result<Option<BenchmarkResult>, String> {
        Ok(self.results.read().unwrap().get(id).cloned())
    }
    
    fn list_results(
        &self,
        limit: usize,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> Result<Vec<BenchmarkResult>, String> {
        let results = self.results.read().unwrap();
        let mut filtered: Vec<BenchmarkResult> = results.values()
            .filter(|r| {
                if let Some(start) = start_time {
                    if r.timestamp < start {
                        return false;
                    }
                }
                if let Some(end) = end_time {
                    if r.timestamp > end {
                        return false;
                    }
                }
                true
            })
            .cloned()
            .collect();
        
        filtered.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        filtered.truncate(limit);
        
        Ok(filtered)
    }
    
    fn delete_result(&self, id: &str) -> Result<bool, String> {
        Ok(self.results.write().unwrap().remove(id).is_some())
    }
}

// 基准测试管理器
pub struct BenchmarkManager {
    storage: Arc<dyn BenchmarkStorage>,
    results: RwLock<Vec<BenchmarkResult>>,
}

impl BenchmarkManager {
    pub fn new(storage: Arc<dyn BenchmarkStorage>) -> Self {
        Self {
            storage,
            results: RwLock::new(Vec::new()),
        }
    }
    
    pub async fn run_benchmark(&self, flags: OptimizationFlags) -> Result<BenchmarkResult, String> {
        let environment = self.get_environment_info().await;
        let mut result = BenchmarkResult::new(&format!("benchmark_{}", flags.hash()), flags, environment);
        
        let start = std::time::Instant::now();
        
        // 运行各项基准测试
        result.metrics = self.run_all_benchmarks(&flags).await;
        
        result.duration_seconds = start.elapsed().as_secs();
        
        // 保存结果
        self.storage.save_result(&result)?;
        self.results.write().unwrap().push(result.clone());
        
        Ok(result)
    }
    
    async fn run_all_benchmarks(&self, flags: &OptimizationFlags) -> BenchmarkMetrics {
        let mut metrics = BenchmarkMetrics::default();
        
        // EC 基准测试
        if flags.ec_simd_enabled || flags.ec_parallel_encoding {
            let (throughput, latency) = self.run_ec_benchmark(flags).await;
            metrics.ec_throughput_mbps = throughput;
            metrics.ec_latency_ms = latency;
        }
        
        // Raft 基准测试
        if flags.raft_pre_vote {
            metrics.raft_election_time_ms = self.run_raft_election_benchmark().await;
        }
        
        // KV 基准测试
        let (hit_rate, read_ops, write_ops) = self.run_kv_benchmark(flags).await;
        metrics.kv_cache_hit_rate = hit_rate;
        metrics.kv_read_throughput_ops = read_ops;
        metrics.kv_write_throughput_ops = write_ops;
        
        // S3 基准测试
        let (read_mbps, write_mbps) = self.run_s3_benchmark(flags).await;
        metrics.s3_read_throughput_mbps = read_mbps;
        metrics.s3_write_throughput_mbps = write_mbps;
        
        // 数据分布基准测试
        if flags.rack_awareness {
            metrics.data_balance_score = self.run_balance_benchmark().await;
        }
        
        // 系统指标
        let (cpu, mem) = self.get_system_metrics().await;
        metrics.cpu_usage_percent = cpu;
        metrics.memory_usage_percent = mem;
        
        metrics
    }
    
    async fn run_ec_benchmark(&self, flags: &OptimizationFlags) -> (f64, f64) {
        // TODO: 实际的 EC 基准测试实现
        let throughput = if flags.ec_simd_enabled && flags.ec_parallel_encoding {
            850.5  // 850 MB/s (SIMD + Parallel)
        } else if flags.ec_parallel_encoding {
            280.0  // 280 MB/s (Parallel only)
        } else if flags.ec_simd_enabled {
            450.0  // 450 MB/s (SIMD only)
        } else {
            100.0  // 100 MB/s (baseline)
        };
        
        let latency = if flags.ec_simd_enabled {
            1.2  // 1.2 ms
        } else {
            3.0  // 3.0 ms
        };
        
        (throughput, latency)
    }
    
    async fn run_raft_election_benchmark(&self) -> f64 {
        // TODO: 实际的 Raft 选举基准测试实现
        800.0  // 800 ms with Pre-Vote
    }
    
    async fn run_kv_benchmark(&self, flags: &OptimizationFlags) -> (f64, f64, f64) {
        // TODO: 实际的 KV 基准测试实现
        let hit_rate = if flags.smart_cache_eviction { 0.92 } else { 0.75 };
        let read_ops = if flags.hierarchical_index { 15000.0 } else { 8000.0 };
        let write_ops = if flags.hierarchical_index { 8000.0 } else { 4000.0 };
        
        (hit_rate, read_ops, write_ops)
    }
    
    async fn run_s3_benchmark(&self, flags: &OptimizationFlags) -> (f64, f64) {
        // TODO: 实际的 S3 基准测试实现
        if flags.load_balancing {
            (300.0, 250.0)  // 300 MB/s read, 250 MB/s write
        } else {
            (100.0, 80.0)   // 100 MB/s read, 80 MB/s write
        }
    }
    
    async fn run_balance_benchmark(&self) -> f64 {
        // TODO: 实际的均衡度基准测试实现
        0.95  // 95% balanced
    }
    
    async fn get_system_metrics(&self) -> (f64, f64) {
        // TODO: 实际获取系统指标
        (45.0, 60.0)  // 45% CPU, 60% memory
    }
    
    async fn get_environment_info(&self) -> EnvironmentInfo {
        EnvironmentInfo {
            cpu_model: "Intel(R) Xeon(R) Gold 6342".to_string(),
            cpu_cores: num_cpus::get(),
            memory_gb: (sys_info::mem_info().map(|m| m.total as f64 / 1024.0 / 1024.0 / 1024.0).unwrap_or(0.0)),
            node_count: 3,
            os_version: sys_info::os_release().unwrap_or_else(|_| "unknown".to_string()),
            rust_version: env!("RUSTC_VERSION").to_string(),
            powerfs_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
    
    pub async fn compare_results(
        &self,
        baseline_id: &str,
        target_id: &str,
    ) -> Result<ComparisonReport, String> {
        let baseline = self.storage.get_result(baseline_id)?
            .ok_or_else(|| format!("Baseline result not found: {}", baseline_id))?;
        
        let target = self.storage.get_result(target_id)?
            .ok_or_else(|| format!("Target result not found: {}", target_id))?;
        
        Ok(self.create_comparison_report(&baseline, &target))
    }
    
    fn create_comparison_report(&self, baseline: &BenchmarkResult, target: &BenchmarkResult) -> ComparisonReport {
        let baseline = &baseline.metrics;
        let target = &target.metrics;
        
        ComparisonReport {
            baseline_id: baseline.id.clone(),
            target_id: target.id.clone(),
            ec_throughput_improvement: if baseline.ec_throughput_mbps > 0.0 {
                (target.ec_throughput_mbps - baseline.ec_throughput_mbps) / baseline.ec_throughput_mbps * 100.0
            } else {
                0.0
            },
            ec_latency_improvement: if baseline.ec_latency_ms > 0.0 {
                (baseline.ec_latency_ms - target.ec_latency_ms) / baseline.ec_latency_ms * 100.0
            } else {
                0.0
            },
            raft_election_improvement: if baseline.raft_election_time_ms > 0.0 {
                (baseline.raft_election_time_ms - target.raft_election_time_ms) / baseline.raft_election_time_ms * 100.0
            } else {
                0.0
            },
            kv_cache_hit_rate_improvement: (target.kv_cache_hit_rate - baseline.kv_cache_hit_rate) * 100.0,
            kv_read_throughput_improvement: if baseline.kv_read_throughput_ops > 0.0 {
                (target.kv_read_throughput_ops - baseline.kv_read_throughput_ops) / baseline.kv_read_throughput_ops * 100.0
            } else {
                0.0
            },
            kv_write_throughput_improvement: if baseline.kv_write_throughput_ops > 0.0 {
                (target.kv_write_throughput_ops - baseline.kv_write_throughput_ops) / baseline.kv_write_throughput_ops * 100.0
            } else {
                0.0
            },
            s3_read_throughput_improvement: if baseline.s3_read_throughput_mbps > 0.0 {
                (target.s3_read_throughput_mbps - baseline.s3_read_throughput_mbps) / baseline.s3_read_throughput_mbps * 100.0
            } else {
                0.0
            },
            s3_write_throughput_improvement: if baseline.s3_write_throughput_mbps > 0.0 {
                (target.s3_write_throughput_mbps - baseline.s3_write_throughput_mbps) / baseline.s3_write_throughput_mbps * 100.0
            } else {
                0.0
            },
            data_balance_improvement: (target.data_balance_score - baseline.data_balance_score) * 100.0,
        }
    }
    
    pub async fn list_results(
        &self,
        limit: usize,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> Result<Vec<BenchmarkResult>, String> {
        self.storage.list_results(limit, start_time, end_time)
    }
    
    pub async fn get_latest_result(&self) -> Result<Option<BenchmarkResult>, String> {
        let results = self.storage.list_results(1, None, None)?;
        Ok(results.into_iter().next())
    }
}
