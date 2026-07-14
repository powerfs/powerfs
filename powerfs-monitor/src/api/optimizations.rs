
use axum::{
    extract::{Json, Path, Query},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post, put},
    Router, Extension,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};

// 从 benchmark 模块导入
use powerfs_core::benchmark::{
    BenchmarkManager, BenchmarkResult, BenchmarkStorage, MemoryBenchmarkStorage,
    OptimizationFlags,
};

// 优化开关管理状态
pub struct OptimizationManager {
    flags: RwLock<OptimizationFlags>,
    benchmark_manager: Arc<BenchmarkManager>,
}

impl OptimizationManager {
    pub fn new(benchmark_manager: Arc<BenchmarkManager>) -> Self {
        Self {
            flags: RwLock::new(OptimizationFlags::default()),
            benchmark_manager,
        }
    }

    pub fn get_flags(&self) -> OptimizationFlags {
        self.flags.read().unwrap().clone()
    }

    pub fn set_flag(&self, flag_name: &str, value: bool) -> Result<(), String> {
        let mut flags = self.flags.write().unwrap();

        match flag_name {
            "ec_simd_enabled" => flags.ec_simd_enabled = value,
            "ec_parallel_encoding" => flags.ec_parallel_encoding = value,
            "ec_dynamic_sharding" => flags.ec_dynamic_sharding = value,
            "ec_small_file_skip" => flags.ec_small_file_skip = value,
            "raft_log_compression" => flags.raft_log_compression = value,
            "raft_pre_vote" => flags.raft_pre_vote = value,
            "raft_read_scaling" => flags.raft_read_scaling = value,
            "rack_awareness" => flags.rack_awareness = value,
            "load_balancing" => flags.load_balancing = value,
            "smart_cache_eviction" => flags.smart_cache_eviction = value,
            "hierarchical_index" => flags.hierarchical_index = value,
            _ => return Err(format!("Invalid flag name: {}", flag_name)),
        }

        Ok(())
    }

    pub fn reset_to_default(&self) {
        *self.flags.write().unwrap() = OptimizationFlags::default();
    }

    pub fn reset_to_baseline(&self) {
        *self.flags.write().unwrap() = OptimizationFlags {
            ec_simd_enabled: false,
            ec_parallel_encoding: false,
            ec_dynamic_sharding: false,
            ec_small_file_skip: false,
            raft_log_compression: false,
            raft_pre_vote: false,
            raft_read_scaling: false,
            rack_awareness: false,
            load_balancing: false,
            smart_cache_eviction: false,
            hierarchical_index: false,
        };
    }

    pub async fn run_benchmark(&self) -> Result<BenchmarkResult, String> {
        let flags = self.get_flags();
        self.benchmark_manager.run_benchmark(flags).await
    }

    pub fn get_benchmark_manager(&self) -> &Arc<BenchmarkManager> {
        &self.benchmark_manager
    }
}

// API 请求/响应结构
#[derive(Debug, Deserialize)]
pub struct SetFlagRequest {
    value: bool,
}

#[derive(Debug, Serialize)]
pub struct SetFlagResponse {
    success: bool,
    flag_name: String,
    value: bool,
}

#[derive(Debug, Serialize)]
pub struct GetFlagsResponse {
    flags: OptimizationFlags,
}

#[derive(Debug, Deserialize)]
pub struct RunBenchmarkRequest {
    test_duration_seconds: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct RunBenchmarkResponse {
    success: bool,
    benchmark_id: String,
    message: String,
}

#[derive(Debug, Deserialize)]
pub struct ListResultsQuery {
    limit: Option<usize>,
}

// API 处理器
pub async fn get_flags_handler(manager: Arc<OptimizationManager>) -> impl IntoResponse {
    let flags = manager.get_flags();
    Json(GetFlagsResponse { flags })
}

pub async fn set_flag_handler(
    manager: Arc<OptimizationManager>,
    Path(flag_name): Path<String>,
    Json(request): Json<SetFlagRequest>,
) -> impl IntoResponse {
    match manager.set_flag(&flag_name, request.value) {
        Ok(_) => (
            StatusCode::OK,
            Json(SetFlagResponse {
                success: true,
                flag_name,
                value: request.value,
            }),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(SetFlagResponse {
                success: false,
                flag_name,
                value: request.value,
            }),
        ),
    }
}

pub async fn reset_to_default_handler(manager: Arc<OptimizationManager>) -> impl IntoResponse {
    manager.reset_to_default();
    StatusCode::NO_CONTENT
}

pub async fn reset_to_baseline_handler(manager: Arc<OptimizationManager>) -> impl IntoResponse {
    manager.reset_to_baseline();
    StatusCode::NO_CONTENT
}

pub async fn run_benchmark_handler(
    manager: Arc<OptimizationManager>,
    _Json(request): Json<RunBenchmarkRequest>,
) -> impl IntoResponse {
    match manager.run_benchmark().await {
        Ok(result) => (
            StatusCode::OK,
            Json(RunBenchmarkResponse {
                success: true,
                benchmark_id: result.id,
                message: "Benchmark completed successfully".to_string(),
            }),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(RunBenchmarkResponse {
                success: false,
                benchmark_id: "".to_string(),
                message: e,
            }),
        ),
    }
}

pub async fn list_results_handler(
    manager: Arc<OptimizationManager>,
    Query(query): Query<ListResultsQuery>,
) -> impl IntoResponse {
    let limit = query.limit.unwrap_or(10);
    match manager.benchmark_manager.list_results(limit, None, None).await {
        Ok(results) => Json(results),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    }
}

pub async fn get_result_handler(
    manager: Arc<OptimizationManager>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match manager.benchmark_manager.get_result(&id).await {
        Ok(Some(result)) => Json(result).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    }
}

// 构建路由
pub fn build_routes(manager: Arc<OptimizationManager>) -> Router {
    Router::new()
        .route("/api/optimizations", get(get_flags_handler))
        .route("/api/optimizations/:flag_name", put(set_flag_handler))
        .route("/api/optimizations/reset", post(reset_to_default_handler))
        .route("/api/optimizations/baseline", post(reset_to_baseline_handler))
        .route("/api/benchmark/run", post(run_benchmark_handler))
        .route("/api/benchmark/results", get(list_results_handler))
        .route("/api/benchmark/results/:id", get(get_result_handler))
        .with_state(manager)
}

// 测试用例
#[cfg(test)]
mod tests {
    use super::*;
    use powerfs_core::benchmark::MemoryBenchmarkStorage;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_optimization_manager() {
        let storage = Arc::new(MemoryBenchmarkStorage::new());
        let benchmark_manager = Arc::new(BenchmarkManager::new(storage));
        let manager = Arc::new(OptimizationManager::new(benchmark_manager));

        // 测试获取初始标志
        let flags = manager.get_flags();
        assert!(flags.ec_simd_enabled);
        assert!(flags.raft_pre_vote);

        // 测试设置标志
        assert!(manager.set_flag("ec_simd_enabled", false).is_ok());
        let flags = manager.get_flags();
        assert!(!flags.ec_simd_enabled);

        // 测试设置无效标志
        assert!(manager.set_flag("invalid_flag", true).is_err());

        // 测试重置为默认值
        manager.reset_to_default();
        let flags = manager.get_flags();
        assert!(flags.ec_simd_enabled);

        // 测试重置为基线
        manager.reset_to_baseline();
        let flags = manager.get_flags();
        assert!(!flags.ec_simd_enabled);
        assert!(!flags.raft_pre_vote);
    }

    #[tokio::test]
    async fn test_benchmark_run() {
        let storage = Arc::new(MemoryBenchmarkStorage::new());
        let benchmark_manager = Arc::new(BenchmarkManager::new(storage));
        let manager = Arc::new(OptimizationManager::new(benchmark_manager));

        // 测试运行基准测试
        let result = manager.run_benchmark().await;
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(!result.id.is_empty());
        assert!(result.metrics.ec_throughput_mbps > 0.0);
    }
}
