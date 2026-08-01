//! Sharded RPC Pool — per-worker MPSC 队列 + 并发 spawn 派发。
//!
//! 替代 MetaShardClient 的串行 `process_available_requests`，消除全局
//! `response_waiters` Mutex 并实现元数据请求的并发处理。
//!
//! # 架构
//!
//! ```text
//! FUSE 线程(多) ──submit──► per-worker MPSC 队列 ──► worker 任务
//!                                                         │
//!                                                         ▼
//!                                                   tokio::spawn
//!                                                   (并发处理请求)
//!                                                         │
//!                                         process_request_internal
//!                                         (send_request + redirect 重试)
//!                                                         │
//!                                                   oneshot 返回结果
//! ```
//!
//! # worker 数动态公式
//! `clamp(ceil(shard_count / 16), 2, 64)`
//! - 256 shard → 16 worker × 16 shard
//! - 4 shard → 2 worker × 2 shard
//!
//! # shard → worker 路由
//! `worker_idx = shard_id % worker_count`（稳定路由，无 work-stealing）

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use log::info;
use tokio::sync::{mpsc, oneshot};

use crate::circuit_breaker::CircuitBreakerPool;
use crate::client_error::{ClientError, ClientResult};
use crate::meta_shard_client::{process_request_internal, PendingRequest, RequestResult};
use crate::topology::ShardInfo;
use powerfs_net::PowerFsNetClient;

/// 每个 worker 负责的分片数（可配，默认 16）
const SHARDS_PER_WORKER: usize = 16;
/// 最少 worker 数
const MIN_WORKERS: usize = 2;
/// 最多 worker 数
const MAX_WORKERS: usize = 64;

/// 根据 shard 总数计算 worker 数
pub fn calc_worker_count(shard_count: usize) -> usize {
    let calculated = shard_count.div_ceil(SHARDS_PER_WORKER);
    calculated.clamp(MIN_WORKERS, MAX_WORKERS)
}

/// Worker 请求条目：(请求, 回复通道)
type WorkerEntry = (PendingRequest, oneshot::Sender<ClientResult<RequestResult>>);

/// Sharded RPC Pool — 管理 per-worker MPSC 队列，并发派发元数据请求。
///
/// 每个 worker 从自己的 MPSC 队列消费请求，`tokio::spawn` 独立任务处理。
/// spawned 任务调用 `process_request_internal`（含 redirect 重试），结果
/// 通过 oneshot 直接返回调用者，**不经过全局 response_waiters**。
pub struct ShardedRpcPool {
    workers: Vec<mpsc::UnboundedSender<WorkerEntry>>,
}

impl ShardedRpcPool {
    /// 创建 pool 并启动 worker 任务
    pub fn new(
        worker_count: usize,
        filer_connections: Arc<DashMap<String, Arc<PowerFsNetClient>>>,
        default_filer_addr: Arc<std::sync::Mutex<String>>,
        breakers: Arc<CircuitBreakerPool>,
        shard_router: Arc<DashMap<u64, ShardInfo>>,
    ) -> Self {
        let worker_count = worker_count.clamp(MIN_WORKERS, MAX_WORKERS);
        let mut workers = Vec::with_capacity(worker_count);

        for i in 0..worker_count {
            let (tx, rx) = mpsc::unbounded_channel::<WorkerEntry>();
            let fc = filer_connections.clone();
            let dfa = default_filer_addr.clone();
            let br = breakers.clone();
            let sr = shard_router.clone();

            tokio::spawn(async move {
                info!("ShardedRpcPool: worker {} started", i);
                worker_loop(rx, fc, dfa, br, sr).await;
                info!("ShardedRpcPool: worker {} stopped", i);
            });

            workers.push(tx);
        }

        info!(
            "ShardedRpcPool: started {} workers (shards_per_worker={})",
            worker_count, SHARDS_PER_WORKER
        );

        Self { workers }
    }

    /// 提交请求并等待响应（带超时）
    ///
    /// 根据 shard_id 路由到对应 worker，通过 oneshot 等待结果。
    /// 超时只影响当前请求，不影响 worker 队列内其他请求。
    pub async fn submit(
        &self,
        req: PendingRequest,
        timeout: Duration,
    ) -> ClientResult<RequestResult> {
        let worker_idx = (req.shard_id as usize) % self.workers.len();
        let (tx, rx) = oneshot::channel();

        self.workers[worker_idx]
            .send((req, tx))
            .map_err(|_| ClientError::Internal("worker channel closed".to_string()))?;

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(ClientError::Cancelled),
            Err(_) => Err(ClientError::Timeout(timeout)),
        }
    }

    /// 当前 worker 数
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }
}

/// Worker 循环：从 MPSC 队列消费请求，spawn 并发处理任务。
///
/// 不串行 await — 每个 spawn 的任务独立完成网络往返，互不阻塞。
/// 这与 volume_client 阶段0 的 process_data_requests 模式一致。
async fn worker_loop(
    mut rx: mpsc::UnboundedReceiver<WorkerEntry>,
    filer_connections: Arc<DashMap<String, Arc<PowerFsNetClient>>>,
    default_filer_addr: Arc<std::sync::Mutex<String>>,
    breakers: Arc<CircuitBreakerPool>,
    shard_router: Arc<DashMap<u64, ShardInfo>>,
) {
    while let Some((req, reply_tx)) = rx.recv().await {
        let fc = filer_connections.clone();
        let dfa = default_filer_addr.clone();
        let br = breakers.clone();
        let sr = shard_router.clone();

        // 并发派发 — spawn 独立任务执行 process_request_internal。
        // 单个请求的网络超时/redirect 重试不影响队列内其他请求。
        tokio::spawn(async move {
            let result = process_request_internal(req, &fc, &dfa, &br, &sr).await;
            let _ = reply_tx.send(result);
        });
    }
}
