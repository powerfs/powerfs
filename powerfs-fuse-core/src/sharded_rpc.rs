//! Sharded RPC Pool — per-worker MPSC 队列 + 并发 spawn 派发。
//!
//! 替代 MetaShardClient 的串行 `process_available_requests`，消除全局
//! `response_waiters` Mutex 并实现元数据请求的并发处理。
//!
//! # 架构
//!
//! ```text
//! FUSE 线程(多) ──submit──► per-worker MPSC 有界队列 ──► worker 任务
//!                    │                                        │
//!                    │ CircuitBreaker 预检                     ▼
//!                    │ (拒绝入队，快速失败)              Semaphore permit
//!                    │                                        │
//!                    ▼                                        ▼
//!              try_send (非阻塞)                       tokio::spawn
//!              满则返回 QueueFull                      (并发处理请求)
//!                                                         │
//!                                         process_request_internal
//!                                         (send_request + redirect 重试)
//!                                                         │
//!                                                   oneshot 返回结果
//! ```
//!
//! # Flow Control (流控)
//!
//! 1. **Bounded channel** (`WORKER_QUEUE_CAPACITY=256`): 队列满时 `try_send`
//!    立即返回 `QueueFull`，给调用方即时背压，避免内存无限增长。
//! 2. **Semaphore** (`MAX_CONCURRENT_PER_WORKER=16`): 限制每 worker 并发 spawn
//!    数。permit 耗尽时 worker 阻塞等待，间接导致队列填满 → `QueueFull`。
//! 3. **CircuitBreaker 预检**: `submit` 入队前检查目标 Filer 的熔断状态，
//!    避免无效排队和 spawn 资源浪费。
//!
//! # worker 数动态公式
//! `clamp(ceil(shard_count / 16), 2, 64)`
//! - 256 shard → 16 worker × 16 shard
//! - 4 shard → 2 worker × 2 shard
//!
//! # shard → worker 路由
//! `worker_idx = shard_id % worker_count`（稳定路由，无 work-stealing）

use std::sync::{Arc, Mutex};
use std::time::Duration;

use dashmap::DashMap;
use log::{info, warn};
use tokio::sync::{mpsc, oneshot, Semaphore};

use crate::circuit_breaker::CircuitBreakerPool;
use crate::client_error::{ClientError, ClientResult};
use crate::meta_shard_client::{process_request_internal, PendingRequest, RequestResult};
use crate::request_stats::RequestStats;
use crate::topology::ShardInfo;

/// 每个 worker 负责的分片数（可配，默认 16）
const SHARDS_PER_WORKER: usize = 16;
/// 最少 worker 数
const MIN_WORKERS: usize = 2;
/// 最多 worker 数
const MAX_WORKERS: usize = 64;
/// 每个 worker 的有界队列容量。
///
/// 队列满时 `try_send` 返回 `QueueFull`，给调用方即时背压。
/// 256 足够吸收短时突发（FUSE 多线程并发提交），同时限制内存：
/// 64 workers × 256 entries × ~200B/entry ≈ 3MB。
const WORKER_QUEUE_CAPACITY: usize = 256;
/// 每个 worker 允许的最大并发 spawn 数。
///
/// Semaphore 限制同时在飞的请求数。超过则 worker 阻塞等待 permit，
/// 间接填满队列 → 调用方收到 `QueueFull`。
/// 16 与 MetaShardClientConfig.data_channel.max_concurrent 一致。
const MAX_CONCURRENT_PER_WORKER: usize = 16;

/// 根据 shard 总数计算 worker 数
pub fn calc_worker_count(shard_count: usize) -> usize {
    let calculated = shard_count.div_ceil(SHARDS_PER_WORKER);
    calculated.clamp(MIN_WORKERS, MAX_WORKERS)
}

/// Worker 请求条目：(请求, 回复通道)
type WorkerEntry = (PendingRequest, oneshot::Sender<ClientResult<RequestResult>>);

/// Sharded RPC Pool — 管理 per-worker 有界 MPSC 队列，并发派发元数据请求。
///
/// 每个 worker 从自己的有界 MPSC 队列消费请求，在 Semaphore 许可下
/// `tokio::spawn` 独立任务处理。spawned 任务调用 `process_request_internal`
/// （含 redirect 重试），结果通过 oneshot 直接返回调用者。
pub struct ShardedRpcPool {
    workers: Vec<mpsc::Sender<WorkerEntry>>,
    /// CircuitBreaker pool — for pre-check in submit() before enqueueing.
    breakers: Arc<CircuitBreakerPool>,
    /// Shard router — to resolve shard_id → filer addr for breaker check.
    shard_router: Arc<DashMap<u64, ShardInfo>>,
    /// Default filer addr — fallback when shard not in router.
    default_filer_addr: Arc<Mutex<String>>,
    /// Request statistics tracker — for debug switch / admin endpoint.
    stats: Arc<RequestStats>,
}

impl ShardedRpcPool {
    /// 创建 pool 并启动 worker 任务
    pub fn new(
        worker_count: usize,
        conn_pool: Arc<powerfs_net::ClientConnPool>,
        default_filer_addr: Arc<std::sync::Mutex<String>>,
        breakers: Arc<CircuitBreakerPool>,
        shard_router: Arc<DashMap<u64, ShardInfo>>,
        filer_addresses: Arc<Mutex<Vec<String>>>,
        stats: Arc<RequestStats>,
    ) -> Self {
        let worker_count = worker_count.clamp(MIN_WORKERS, MAX_WORKERS);
        let mut workers = Vec::with_capacity(worker_count);

        for i in 0..worker_count {
            // Bounded channel: try_send returns Full when capacity reached,
            // giving the caller immediate backpressure (QueueFull error).
            let (tx, rx) = mpsc::channel::<WorkerEntry>(WORKER_QUEUE_CAPACITY);
            // Semaphore: limits concurrent spawns per worker. Without this,
            // a slow Filer causes unbounded tokio::spawn, exhausting the
            // runtime's task queue and memory.
            let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_PER_WORKER));

            let cp = conn_pool.clone();
            let dfa = default_filer_addr.clone();
            let br = breakers.clone();
            let sr = shard_router.clone();
            let fa = filer_addresses.clone();

            tokio::spawn(async move {
                info!("ShardedRpcPool: worker {} started", i);
                worker_loop(rx, cp, dfa, br, sr, fa, semaphore).await;
                info!("ShardedRpcPool: worker {} stopped", i);
            });

            workers.push(tx);
        }

        info!(
            "ShardedRpcPool: started {} workers (shards_per_worker={}, queue_capacity={}, max_concurrent_per_worker={})",
            worker_count, SHARDS_PER_WORKER, WORKER_QUEUE_CAPACITY, MAX_CONCURRENT_PER_WORKER
        );

        Self {
            workers,
            breakers,
            shard_router,
            default_filer_addr,
            stats,
        }
    }

    /// 提交请求并等待响应（带超时）
    ///
    /// 根据 shard_id 路由到对应 worker，通过 oneshot 等待结果。
    /// 超时只影响当前请求，不影响 worker 队列内其他请求。
    ///
    /// # Flow Control
    ///
    /// 1. **CircuitBreaker 预检**: 入队前检查目标 Filer 的熔断状态。如果
    ///    熔断器已打开，立即返回 `CircuitOpen`，避免无效排队。
    /// 2. **有界队列**: 使用 `try_send` 非阻塞入队。队列满时返回
    ///    `QueueFull`，给调用方即时背压。
    pub async fn submit(
        &self,
        req: PendingRequest,
        timeout: Duration,
    ) -> ClientResult<RequestResult> {
        let worker_idx = (req.shard_id as usize) % self.workers.len();

        // Record request start for statistics / stuck-request tracking.
        let stats_id = self.stats.record_start(req.context.msg_type, req.shard_id);

        // 1) CircuitBreaker pre-check: resolve target filer addr and check
        //    before enqueueing. This avoids wasting queue space and spawn
        //    resources on a filer that is known to be down.
        let filer_addr = self
            .shard_router
            .get(&req.shard_id)
            .map(|s| s.leader_addr.clone())
            .unwrap_or_else(|| {
                let default = self.default_filer_addr.lock().unwrap();
                default.clone()
            });

        if !filer_addr.is_empty() && !self.breakers.check(&filer_addr) {
            warn!(
                "ShardedRpcPool: submit rejected by circuit breaker (shard={}, filer={})",
                req.shard_id, filer_addr
            );
            self.stats
                .record_complete(stats_id, Err(&ClientError::CircuitOpen));
            return Err(ClientError::CircuitOpen);
        }

        let (tx, rx) = oneshot::channel();

        // 2) Bounded enqueue: try_send is non-blocking. Returns Full
        //    immediately when the worker's queue is at capacity, giving
        //    the caller instant backpressure instead of unbounded memory
        //    growth.
        if let Err(e) = self.workers[worker_idx].try_send((req, tx)) {
            let err = match e {
                mpsc::error::TrySendError::Full(_) => ClientError::QueueFull(WORKER_QUEUE_CAPACITY),
                mpsc::error::TrySendError::Closed(_) => {
                    ClientError::Internal("worker channel closed".to_string())
                }
            };
            self.stats.record_complete(stats_id, Err(&err));
            return Err(err);
        }

        let result = match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(ClientError::Cancelled),
            Err(_) => Err(ClientError::Timeout(timeout)),
        };

        match &result {
            Ok(_) => self.stats.record_complete(stats_id, Ok(())),
            Err(e) => self.stats.record_complete(stats_id, Err(e)),
        }

        result
    }

    /// 当前 worker 数
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    /// Get a reference to the request stats tracker.
    pub fn stats(&self) -> &Arc<RequestStats> {
        &self.stats
    }
}

/// Worker 循环：从有界 MPSC 队列消费请求，在 Semaphore 许可下 spawn 并发处理任务。
///
/// 不串行 await — 每个 spawn 的任务独立完成网络往返，互不阻塞。
/// Semaphore 限制并发 spawn 数，防止慢 Filer 导致无限 spawn。
async fn worker_loop(
    mut rx: mpsc::Receiver<WorkerEntry>,
    conn_pool: Arc<powerfs_net::ClientConnPool>,
    default_filer_addr: Arc<std::sync::Mutex<String>>,
    breakers: Arc<CircuitBreakerPool>,
    shard_router: Arc<DashMap<u64, ShardInfo>>,
    filer_addresses: Arc<Mutex<Vec<String>>>,
    semaphore: Arc<Semaphore>,
) {
    while let Some((req, reply_tx)) = rx.recv().await {
        let cp = conn_pool.clone();
        let dfa = default_filer_addr.clone();
        let br = breakers.clone();
        let sr = shard_router.clone();
        let fa = filer_addresses.clone();
        let sem = semaphore.clone();

        // Acquire permit before spawning — limits concurrent in-flight
        // requests per worker. If all permits are in use, this await
        // blocks (backpressure to the queue), causing try_send() in
        // submit() to eventually return QueueFull to the caller.
        //
        // acquire_owned returns a Permit that is released on drop (when
        // the spawned task completes), so we move it into the task.
        let permit = match sem.acquire_owned().await {
            Ok(p) => p,
            Err(_) => {
                // Semaphore closed — pool is shutting down
                let _ = reply_tx.send(Err(ClientError::Cancelled));
                break;
            }
        };

        // 并发派发 — spawn 独立任务执行 process_request_internal。
        // 单个请求的网络超时/redirect 重试不影响队列内其他请求。
        // permit 在任务完成后自动释放（Drop）。
        tokio::spawn(async move {
            let _permit = permit; // hold permit for task lifetime
            let result = process_request_internal(req, &cp, &dfa, &br, &sr, &fa).await;
            let _ = reply_tx.send(result);
        });
    }
}
