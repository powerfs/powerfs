use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dashmap::DashMap;

use tokio::sync::oneshot;

use crate::circuit_breaker::CircuitBreakerPool;
use crate::client_error::{ClientError, ClientResult};
use crate::request_id::RequestId;
use crate::request_state::{RequestContext, RequestKind};
use crate::sharded_rpc::{calc_worker_count, ShardedRpcPool};
use crate::topology::{ClusterTopologyManager, ShardInfo};
use powerfs_net::client::NotificationHandler;
use powerfs_net::PowerFsNetClient;

/// 根据 RequestKind 获取默认 MsgType
pub(crate) fn default_msg_type_for_kind(kind: RequestKind) -> powerfs_net::MsgType {
    match kind {
        RequestKind::Metadata => powerfs_net::MsgType::Lookup,
        RequestKind::Control => powerfs_net::MsgType::GetTopology,
        RequestKind::Read => powerfs_net::MsgType::ReadNeedleBlob,
        RequestKind::Write => powerfs_net::MsgType::WriteNeedle,
        RequestKind::Lease => powerfs_net::MsgType::RangeLease,
        RequestKind::Management => powerfs_net::MsgType::StatFs,
    }
}

/// 请求结果 - 统一的请求响应类型
#[derive(Debug, Clone)]
pub struct RequestResult {
    pub request_id: RequestId,
    pub data: Option<Vec<u8>>,
    pub payload: Option<Vec<u8>>,
}

/// 请求等待者类型别名
type ResponseWaiters = HashMap<RequestId, oneshot::Sender<Result<RequestResult, ClientError>>>;

impl RequestResult {
    pub fn success(request_id: RequestId, data: Vec<u8>) -> Self {
        Self {
            request_id,
            data: Some(data),
            payload: None,
        }
    }

    pub fn success_with_payload(request_id: RequestId, data: Vec<u8>, payload: Vec<u8>) -> Self {
        Self {
            request_id,
            data: Some(data),
            payload: Some(payload),
        }
    }

    pub fn empty(request_id: RequestId) -> Self {
        Self {
            request_id,
            data: None,
            payload: None,
        }
    }
}

/// 请求完成监听器
pub trait RequestCompletionListener: Send + Sync {
    fn on_request_complete(&self, result: ClientResult<RequestResult>);
}

/// 待处理请求
///
/// Phase 1.6: `response_tx` 直接嵌入请求中，消除 `response_waiters` 中间层。
/// processor 完成后直接通过 `response_tx` 投递结果，无需 HashMap 查找。
pub struct PendingRequest {
    pub context: RequestContext,
    pub shard_id: u64,
    pub enqueued_at: Instant,
    /// Phase 1.6: 直接 response 通道，None 表示 fire-and-forget 请求。
    pub response_tx: Option<oneshot::Sender<ClientResult<RequestResult>>>,
}

impl std::fmt::Debug for PendingRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingRequest")
            .field("context", &self.context)
            .field("shard_id", &self.shard_id)
            .field("enqueued_at", &self.enqueued_at)
            .field("response_tx", &self.response_tx.is_some())
            .finish()
    }
}

/// 传输通道配置
#[derive(Debug, Clone)]
pub struct ChannelConfig {
    /// 通道 ID
    pub channel_id: u32,
    /// 通道名称
    pub name: String,
    /// 最大并发请求数
    pub max_concurrent: u32,
    /// 请求超时
    pub timeout: std::time::Duration,
}

impl Default for ChannelConfig {
    fn default() -> Self {
        Self {
            channel_id: 0,
            name: "data".to_string(),
            max_concurrent: 16,
            timeout: std::time::Duration::from_secs(5),
        }
    }
}

/// 传输通道状态
pub struct TransportChannel {
    pub config: ChannelConfig,
    pub active_requests: Mutex<Vec<RequestId>>,
}

impl TransportChannel {
    pub fn new(config: ChannelConfig) -> Self {
        Self {
            config,
            active_requests: Mutex::new(Vec::new()),
        }
    }

    pub fn can_accept(&self) -> bool {
        let active = self.active_requests.lock().unwrap();
        active.len() < self.config.max_concurrent as usize
    }

    pub fn add_request(&self, id: RequestId) {
        let mut active = self.active_requests.lock().unwrap();
        active.push(id);
    }

    pub fn remove_request(&self, id: &RequestId) {
        let mut active = self.active_requests.lock().unwrap();
        active.retain(|r| r != id);
    }
}

/// Lock-free request queue using crossbeam ArrayQueue (MPMC)
pub struct RequestQueue {
    queue: crossbeam_queue::ArrayQueue<PendingRequest>,
}

impl RequestQueue {
    pub fn new(max_size: usize) -> Self {
        Self {
            queue: crossbeam_queue::ArrayQueue::new(max_size),
        }
    }

    pub fn enqueue(&self, req: PendingRequest) -> Result<(), String> {
        self.queue
            .push(req)
            .map_err(|_| "Queue is full".to_string())
    }

    pub fn dequeue(&self) -> Option<PendingRequest> {
        self.queue.pop()
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.queue.capacity()
    }
}

/// MetaShardClient 配置
#[derive(Debug, Clone)]
pub struct MetaShardClientConfig {
    /// 数据通道配置 (用于元数据请求)
    pub data_channel: ChannelConfig,
    /// 控制通道配置 (用于通知、管理请求)
    pub control_channel: ChannelConfig,
    /// 队列最大大小
    pub queue_max_size: usize,
    /// 熔断器配置
    pub circuit_breaker_config: crate::circuit_breaker::CircuitBreakerConfig,
}

impl Default for MetaShardClientConfig {
    fn default() -> Self {
        Self {
            data_channel: ChannelConfig {
                channel_id: 1,
                name: "metadata".to_string(),
                max_concurrent: 16,
                timeout: std::time::Duration::from_secs(5),
            },
            control_channel: ChannelConfig {
                channel_id: 2,
                name: "control".to_string(),
                max_concurrent: 8,
                timeout: std::time::Duration::from_secs(3),
            },
            queue_max_size: 1000,
            circuit_breaker_config: crate::circuit_breaker::CircuitBreakerConfig::default(),
        }
    }
}

/// MetaShardClient 状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetaShardClientState {
    /// 初始状态
    Init,
    /// 已初始化，等待请求
    Ready,
    /// 处理请求中
    Processing,
    /// 暂停 (Leader 变更等)
    Suspended,
    /// 已关闭
    Closed,
}

/// MetaShardClient - 元数据分片客户端
#[allow(dead_code)]
pub struct MetaShardClient {
    config: MetaShardClientConfig,
    state: Arc<Mutex<MetaShardClientState>>,
    /// 数据请求队列 (lock-free)
    data_queue: Arc<RequestQueue>,
    /// 控制请求队列 (lock-free)
    control_queue: Arc<RequestQueue>,
    /// 数据传输通道
    data_channel: Arc<TransportChannel>,
    /// 控制传输通道
    control_channel: Arc<TransportChannel>,
    /// 分片路由表 (shard_id -> ShardInfo)
    shard_router: Arc<DashMap<u64, ShardInfo>>,
    /// Per-server circuit breaker pool (one breaker per Filer server address)
    breakers: Arc<CircuitBreakerPool>,
    /// 拓扑管理器引用
    topology_manager: Arc<ClusterTopologyManager>,
    /// 统一连接池 (由 powerfs_net::ClientConnPool 管理 addr -> PowerFsNetClient)
    conn_pool: Arc<powerfs_net::ClientConnPool>,
    /// 请求完成监听器
    listeners: Arc<Mutex<Vec<Arc<dyn RequestCompletionListener>>>>,
    /// 后台处理是否在运行
    background_running: Arc<Mutex<bool>>,
    /// 请求等待者映射 (request_id -> oneshot sender)
    response_waiters: Arc<Mutex<ResponseWaiters>>,
    /// 事件通知器（替代 10ms 轮询）
    notify: Arc<tokio::sync::Notify>,
    /// 默认 Filer 地址（当 shard_id 不在路由表中时回退使用，例如 inode 作为 shard_id 时）
    default_filer_addr: Arc<Mutex<String>>,
    /// Sharded RPC Pool — 并发派发元数据请求，消除全局 response_waiters 锁。
    /// 在 init() 中创建（需要 shard_router 已填充）。
    rpc_pool: Arc<Mutex<Option<Arc<ShardedRpcPool>>>>,
    /// Phase 2: Notification handler for server-pushed Invalidate messages.
    /// Applied to every new Filer connection so the client can receive
    /// cache invalidation callbacks.
    notification_handler:
        Arc<std::sync::RwLock<Option<Arc<dyn NotificationHandler + Send + Sync>>>>,
    /// Phase 2: Unique client ID used in Filer handshake so the Filer can
    /// route Invalidate notifications to the correct client. Without this,
    /// all FUSE clients share client_id=0 and notifications go to the last
    /// connected client instead of the subscriber.
    client_id: u64,
}

impl MetaShardClient {
    pub fn new(
        config: MetaShardClientConfig,
        topology_manager: Arc<ClusterTopologyManager>,
        client_id: u64,
        conn_pool: Arc<powerfs_net::ClientConnPool>,
    ) -> Self {
        Self {
            breakers: Arc::new(CircuitBreakerPool::new(
                config.circuit_breaker_config.clone(),
            )),
            data_channel: Arc::new(TransportChannel::new(config.data_channel.clone())),
            control_channel: Arc::new(TransportChannel::new(config.control_channel.clone())),
            data_queue: Arc::new(RequestQueue::new(config.queue_max_size)),
            control_queue: Arc::new(RequestQueue::new(config.queue_max_size)),
            shard_router: Arc::new(DashMap::new()),
            state: Arc::new(Mutex::new(MetaShardClientState::Init)),
            response_waiters: Arc::new(Mutex::new(HashMap::new())),
            config,
            topology_manager,
            conn_pool,
            listeners: Arc::new(Mutex::new(Vec::new())),
            background_running: Arc::new(Mutex::new(false)),
            notify: Arc::new(tokio::sync::Notify::new()),
            default_filer_addr: Arc::new(Mutex::new(String::new())),
            rpc_pool: Arc::new(Mutex::new(None)),
            notification_handler: Arc::new(std::sync::RwLock::new(None)),
            client_id,
        }
    }

    /// Phase 2: Install a notification handler to receive server-pushed
    /// `Invalidate` messages from the Filer.  The handler is applied to
    /// every new Filer connection so the client can evict stale metadata
    /// cache entries when another client modifies the same directory.
    ///
    /// Note: the actual installation on connections is handled by the
    /// `ClientConnPool` (configured at pool creation time). This method
    /// only stores the handler in the struct field for API compatibility.
    pub fn set_notification_handler(&self, handler: Arc<dyn NotificationHandler + Send + Sync>) {
        *self.notification_handler.write().unwrap() = Some(handler);
    }

    /// 获取或创建到指定 filer 地址的连接
    async fn get_or_create_filer_client(&self, addr: &str) -> ClientResult<Arc<PowerFsNetClient>> {
        // 连接复用、懒创建、通知处理器安装均由 ClientConnPool 统一管理
        self.conn_pool
            .get_or_connect_addr(addr)
            .await
            .map_err(ClientError::from_net_error)
    }

    /// 添加请求完成监听器
    pub fn add_listener(&self, listener: Arc<dyn RequestCompletionListener>) {
        let mut listeners = self.listeners.lock().unwrap();
        listeners.push(listener);
    }

    /// 移除请求完成监听器
    pub fn remove_listeners(&self) {
        let mut listeners = self.listeners.lock().unwrap();
        listeners.clear();
    }

    /// 通知所有监听器请求完成
    pub fn notify_listeners(&self, result: ClientResult<RequestResult>) {
        let listeners = self.listeners.lock().unwrap();
        for listener in listeners.iter() {
            listener.on_request_complete(result.clone());
        }
    }

    /// 注册请求等待者
    pub fn register_waiter(
        &self,
        request_id: RequestId,
        sender: oneshot::Sender<ClientResult<RequestResult>>,
    ) {
        let mut waiters = self.response_waiters.lock().unwrap();
        waiters.insert(request_id, sender);
    }

    /// 解析请求等待者（请求完成后调用）
    pub fn resolve_waiter(&self, request_id: &RequestId, result: ClientResult<RequestResult>) {
        let sender = {
            let mut waiters = self.response_waiters.lock().unwrap();
            waiters.remove(request_id)
        };
        if let Some(sender) = sender {
            let _ = sender.send(result);
        }
    }

    /// 提交元数据请求并等待响应
    ///
    /// 通过 ShardedRpcPool 并发派发（per-worker MPSC 队列 + tokio::spawn），
    /// 消除全局 response_waiters 锁。结果通过 oneshot 直接返回。
    pub async fn submit_metadata_request_and_wait(
        &self,
        context: RequestContext,
        shard_id: u64,
        timeout: Duration,
    ) -> ClientResult<RequestResult> {
        let req = PendingRequest {
            context,
            shard_id,
            enqueued_at: Instant::now(),
            response_tx: None,
        };

        // 快速路径：ShardedRpcPool（延迟初始化，首次调用时创建）
        let pool = self.ensure_rpc_pool();
        pool.submit(req, timeout).await
    }

    /// 提交控制请求并等待响应
    ///
    /// 同样通过 ShardedRpcPool 派发（控制请求与元数据请求共用 pool，
    /// 控制请求低频，无需独立优先级队列）。
    pub async fn submit_control_request_and_wait(
        &self,
        context: RequestContext,
        shard_id: u64,
        timeout: Duration,
    ) -> ClientResult<RequestResult> {
        let req = PendingRequest {
            context,
            shard_id,
            enqueued_at: Instant::now(),
            response_tx: None,
        };

        // 快速路径：ShardedRpcPool
        let pool = self.ensure_rpc_pool();
        pool.submit(req, timeout).await
    }

    /// 启动后台处理循环
    ///
    /// 串行处理循环已被 ShardedRpcPool 取代（submit_*_and_wait 直接走 pool）。
    /// 连接健康检查（periodic ping + reconnect）现由 `ClientConnPool::start_health_check`
    /// 统一管理，在 `init()` 中启动。此方法仅设置运行标志。
    pub fn start_background_processor(&self) {
        let mut running = self.background_running.lock().unwrap();
        if *running {
            return;
        }
        *running = true;

        log::info!(
            "MetaShardClient: Background processor started (dispatch via ShardedRpcPool, health-check via ClientConnPool)"
        );
    }

    /// 停止后台处理循环
    pub fn stop_background_processor(&self) {
        let mut running = self.background_running.lock().unwrap();
        *running = false;
        // 唤醒后台任务以立即检查停止标志
        self.notify.notify_one();
        log::info!("MetaShardClient: Stopping background processor...");
    }

    /// 设置默认 filer 地址（用于初始化连接池和路由）
    pub fn set_default_filer_addr(&self, addr: String) {
        *self.default_filer_addr.lock().unwrap() = addr;
    }

    /// 获取默认 filer 地址
    pub fn default_filer_addr(&self) -> String {
        self.default_filer_addr.lock().unwrap().clone()
    }

    /// 初始化客户端
    pub fn init(&self) {
        // 从拓扑管理器加载分片信息
        self.sync_shard_router();

        // 如果分片路由表为空（新集群或拓扑未就绪），设置默认路由
        // 这确保所有分片请求都能被路由到 filer 进行处理
        if self.shard_router.is_empty() {
            self.setup_default_routes();
        }

        // 启动统一连接池的健康检查（periodic ping + reconnect），
        // 取代旧的内嵌健康检查任务。必须在 tokio 运行时上下文中调用。
        self.conn_pool.start_health_check();

        // ShardedRpcPool 延迟到首次 submit_*_and_wait 时创建（需要 tokio 运行时上下文）
        *self.state.lock().unwrap() = MetaShardClientState::Ready;
        log::info!(
            "MetaShardClient: Initialized (shard_count={})",
            self.shard_router.len()
        );
    }

    /// 确保 ShardedRpcPool 已创建（延迟初始化，在 async 上下文中调用）
    fn ensure_rpc_pool(&self) -> Arc<ShardedRpcPool> {
        let mut guard = self.rpc_pool.lock().unwrap();
        if guard.is_none() {
            let shard_count = self.shard_router.len().max(1);
            let worker_count = calc_worker_count(shard_count);
            let pool = ShardedRpcPool::new(
                worker_count,
                self.conn_pool.clone(),
                self.default_filer_addr.clone(),
                self.breakers.clone(),
                self.shard_router.clone(),
            );
            *guard = Some(Arc::new(pool));
        }
        guard.as_ref().unwrap().clone()
    }

    /// 设置默认分片路由 - 将 filer leader 地址设为所有分片的默认目标
    fn setup_default_routes(&self) {
        // 使用默认 filer 地址
        let default_addr = self.default_filer_addr();

        if default_addr.is_empty() {
            log::warn!("MetaShardClient: no filer leader address available for default routes");
            return;
        }

        log::info!(
            "MetaShardClient: setting default shard routes to filer leader: {}",
            default_addr
        );

        // 预填 256 个分片的默认路由（覆盖常用分片范围）
        for shard_id in 0..256 {
            self.shard_router
                .insert(shard_id, ShardInfo::new(shard_id, default_addr.clone()));
        }
        // Store default address for fallback when shard_id > 255 (e.g. inode numbers)
        self.default_filer_addr
            .lock()
            .unwrap()
            .clone_from(&default_addr);
        log::info!(
            "MetaShardClient: default routes configured for {} shards, fallback={}",
            self.shard_router.len(),
            default_addr
        );
    }

    /// 同步分片路由表
    fn sync_shard_router(&self) {
        let topology = self.topology_manager.get_topology();
        self.shard_router.clear();
        for (k, v) in topology.shards {
            self.shard_router.insert(k, v);
        }
    }

    /// 直接设置分片 Leader（用于测试或动态路由更新）
    pub fn set_shard_leader(&self, shard_id: u64, leader_addr: String) {
        self.shard_router
            .insert(shard_id, ShardInfo::new(shard_id, leader_addr));
    }

    /// Calculate shard_id from an inode using the same formula as the filer's ShardStrategy.
    /// This ensures the FUSE client looks up the correct leader in shard_router,
    /// avoiding redirects on every request.
    /// Formula: (inode / inode_per_shard) % shard_count
    /// inode_per_shard = 1_000_000 (must match filer's ShardStrategy::calculate_inode_per_shard)
    pub fn calculate_shard_id(&self, inode: u64) -> u64 {
        let shard_count = self.shard_router.len().max(1) as u64;
        if shard_count <= 1 {
            return 0;
        }
        let inode_per_shard = 1_000_000u64;
        (inode / inode_per_shard) % shard_count
    }

    /// 获取当前状态
    pub fn state(&self) -> MetaShardClientState {
        *self.state.lock().unwrap()
    }

    /// 获取指定分片的 Leader
    /// 当 shard_id 不在路由表中时（例如 inode 作为 shard_id 超出预配置范围），
    /// 回退到 default_filer_addr 确保请求可达。
    pub fn get_shard_leader(&self, shard_id: u64) -> Option<String> {
        if let Some(addr) = self
            .shard_router
            .get(&shard_id)
            .map(|s| s.leader_addr.clone())
        {
            return Some(addr);
        }
        let default_addr = self.default_filer_addr.lock().unwrap();
        if !default_addr.is_empty() {
            Some(default_addr.clone())
        } else {
            None
        }
    }

    /// 提交元数据请求
    pub fn submit_metadata_request(
        &self,
        context: RequestContext,
        shard_id: u64,
    ) -> Result<(), String> {
        if self.state() != MetaShardClientState::Ready
            && self.state() != MetaShardClientState::Processing
        {
            return Err(format!("Client not ready: {:?}", self.state()));
        }

        if !self.breakers.check(&self.resolve_filer_addr(shard_id)) {
            return Err("Circuit breaker is open for this filer server".to_string());
        }

        let req = PendingRequest {
            context,
            shard_id,
            enqueued_at: Instant::now(),
            response_tx: None,
        };

        self.data_queue.enqueue(req)?;

        *self.state.lock().unwrap() = MetaShardClientState::Processing;
        self.notify.notify_one();

        Ok(())
    }

    /// 提交控制请求
    pub fn submit_control_request(
        &self,
        context: RequestContext,
        shard_id: u64,
    ) -> Result<(), String> {
        if self.state() == MetaShardClientState::Closed {
            return Err("Client is closed".to_string());
        }

        let req = PendingRequest {
            context,
            shard_id,
            enqueued_at: Instant::now(),
            response_tx: None,
        };

        self.control_queue.enqueue(req)?;
        self.notify.notify_one();

        Ok(())
    }

    /// 从数据队列获取下一个请求
    pub fn next_data_request(&self) -> Option<PendingRequest> {
        self.data_queue.dequeue()
    }

    /// 从控制队列获取下一个请求
    pub fn next_control_request(&self) -> Option<PendingRequest> {
        self.control_queue.dequeue()
    }

    /// Resolve shard_id to its Filer server address.
    fn resolve_filer_addr(&self, shard_id: u64) -> String {
        self.shard_router
            .get(&shard_id)
            .map(|s| s.leader_addr.clone())
            .unwrap_or_else(|| {
                let default = self.default_filer_addr.lock().unwrap();
                default.clone()
            })
    }

    /// 检查数据通道是否可用
    pub fn can_use_data_channel(&self) -> bool {
        self.data_channel.can_accept()
    }

    /// 检查控制通道是否可用
    pub fn can_use_control_channel(&self) -> bool {
        self.control_channel.can_accept()
    }

    /// 记录请求成功 (per-server breaker)
    pub fn record_success(&self, request_id: &RequestId, kind: RequestKind, filer_addr: &str) {
        match kind {
            RequestKind::Metadata | RequestKind::Control => {
                self.data_channel.remove_request(request_id);
                self.breakers.record_success(filer_addr);
            }
            _ => {}
        }
    }

    /// 记录请求失败 (per-server breaker)
    pub fn record_failure(&self, request_id: &RequestId, kind: RequestKind, filer_addr: &str) {
        match kind {
            RequestKind::Metadata | RequestKind::Control => {
                self.data_channel.remove_request(request_id);
                self.breakers.record_failure(filer_addr);
            }
            _ => {}
        }
    }

    /// 处理 Leader 变更 - 完整的请求重放逻辑
    pub fn handle_leader_change(&self, shard_id: u64, new_leader: String) {
        log::warn!(
            "MetaShardClient: Leader change for shard {} -> {}",
            shard_id,
            new_leader
        );

        // 步骤 1: 暂停客户端，停止队列消费
        *self.state.lock().unwrap() = MetaShardClientState::Suspended;
        log::info!("MetaShardClient: Suspended for leader change");

        // 步骤 2: 保存受影响分片的 pending 请求 (lock-free drain)
        let mut affected_data_requests = Vec::new();
        let mut unaffected_data_requests = Vec::new();
        let mut affected_control_requests = Vec::new();
        let mut unaffected_control_requests = Vec::new();

        // 分离数据队列中的请求
        while let Some(req) = self.data_queue.dequeue() {
            if req.shard_id == shard_id {
                affected_data_requests.push(req);
            } else {
                unaffected_data_requests.push(req);
            }
        }

        // 分离控制队列中的请求
        while let Some(req) = self.control_queue.dequeue() {
            if req.shard_id == shard_id {
                affected_control_requests.push(req);
            } else {
                unaffected_control_requests.push(req);
            }
        }

        log::info!(
            "MetaShardClient: Found {} affected data requests, {} affected control requests",
            affected_data_requests.len(),
            affected_control_requests.len()
        );

        // 步骤 3: 更新路由表
        if let Some(mut shard) = self.shard_router.get_mut(&shard_id) {
            let old_leader = shard.leader_addr.clone();
            shard.leader_addr = new_leader.clone();
            log::info!(
                "MetaShardClient: Updated shard {} leader: {} -> {}",
                shard_id,
                old_leader,
                new_leader
            );
        } else {
            // 如果分片不存在，添加它
            self.shard_router
                .insert(shard_id, ShardInfo::new(shard_id, new_leader.clone()));
            log::info!(
                "MetaShardClient: Added new shard {} with leader {}",
                shard_id,
                new_leader
            );
        }

        // 步骤 4: 将未受影响的请求重新入队
        for req in unaffected_data_requests {
            self.data_queue.enqueue(req).ok();
        }
        for req in unaffected_control_requests {
            self.control_queue.enqueue(req).ok();
        }

        // 步骤 5: 将受影响的请求重新入队（将由后台处理器自动重放）
        for mut req in affected_data_requests {
            // 重置请求状态，准备重试
            req.context.state = crate::request_state::RequestState::Init;
            self.data_queue.enqueue(req).ok();
        }
        for mut req in affected_control_requests {
            // 重置请求状态，准备重试
            req.context.state = crate::request_state::RequestState::Init;
            self.control_queue.enqueue(req).ok();
        }

        // 步骤 6: 恢复客户端，后台处理器将自动消费队列中的请求
        *self.state.lock().unwrap() = MetaShardClientState::Ready;
        self.notify.notify_one();
        log::info!(
            "MetaShardClient: Resumed with {} data requests and {} control requests in queue",
            self.data_queue.len(),
            self.control_queue.len()
        );
    }

    /// 异步处理数据队列中的请求 (真实网络发送)
    pub async fn process_data_request(&self, req: PendingRequest) -> ClientResult<RequestResult> {
        let request_id = req.context.request_id.clone();
        let kind = req.context.kind;
        let msg_type = req.context.msg_type;
        let body = req.context.payload.clone();
        let shard_id = req.shard_id;

        // 获取分片 Leader 地址，或使用默认地址
        let leader_addr = self
            .shard_router
            .get(&shard_id)
            .map(|s| s.leader_addr.clone())
            .unwrap_or_else(|| self.default_filer_addr());

        // Per-server circuit breaker check
        if !self.breakers.check(&leader_addr) {
            let result = Err(ClientError::CircuitOpen);
            self.resolve_waiter(&request_id, result.clone());
            return result;
        }

        if leader_addr.is_empty() {
            let err = ClientError::NoShardLeader(shard_id);
            self.resolve_waiter(&request_id, Err(err.clone()));
            return Err(err);
        }

        // 获取或创建到该 leader 的连接
        let filer_client = self
            .get_or_create_filer_client(&leader_addr)
            .await
            .inspect_err(|e| {
                self.resolve_waiter(&request_id, Err(e.clone()));
            })?;

        // 从 context 获取 MsgType，若无效则回退到默认值
        let resolved_msg_type = powerfs_net::MsgType::from_u16(msg_type)
            .unwrap_or_else(|| default_msg_type_for_kind(kind));

        // 发送请求
        let result = match kind {
            RequestKind::Metadata | RequestKind::Control => {
                let msg = filer_client
                    .send_request(resolved_msg_type, &body, &[])
                    .await;

                match msg {
                    Ok(resp) => {
                        log::debug!("MetaShardClient: response: is_ok={}, status={}, is_response={}, body_len={}, data_len={}",
                            resp.is_ok(), resp.header.status, resp.is_response(), resp.body.len(), resp.data.len());
                        if resp.is_ok() {
                            self.breakers.record_success(&leader_addr);
                            Ok(RequestResult::success_with_payload(
                                request_id.clone(),
                                resp.body,
                                resp.data,
                            ))
                        } else {
                            self.breakers.record_failure(&leader_addr);
                            Err(ClientError::Server(format!(
                                "Server error: {}",
                                resp.header.status
                            )))
                        }
                    }
                    Err(e) => {
                        self.breakers.record_failure(&leader_addr);
                        Err(ClientError::from_net_error(e))
                    }
                }
            }
            _ => Err(ClientError::UnsupportedRequest(format!("{:?}", kind))),
        };

        // 解析 waiter（通知等待方结果已就绪）
        self.resolve_waiter(&request_id, result.clone());

        result
    }

    /// 异步处理控制队列中的请求
    pub async fn process_control_request(
        &self,
        req: PendingRequest,
    ) -> ClientResult<RequestResult> {
        self.process_data_request(req).await
    }

    /// 从队列获取并处理下一个数据请求
    pub async fn process_next_data_request(&self) -> Option<ClientResult<RequestResult>> {
        let req = self.next_data_request()?;
        Some(self.process_data_request(req).await)
    }

    /// 从队列获取并处理下一个控制请求
    pub async fn process_next_control_request(&self) -> Option<ClientResult<RequestResult>> {
        let req = self.next_control_request()?;
        Some(self.process_control_request(req).await)
    }

    /// 获取队列状态
    pub fn queue_stats(&self) -> (usize, usize) {
        let data_len = self.data_queue.len();
        let control_len = self.control_queue.len();
        (data_len, control_len)
    }

    // -----------------------------------------------------------------------
    // Phase 2: CRDT delta sync 方法（fuse→filer 走 net 层）
    // -----------------------------------------------------------------------

    /// 通用 coherence 请求发送：处理 leader 解析、连接、redirect 重试。
    ///
    /// 成功返回 STATUS_OK 响应的 body 字节；失败返回错误字符串。
    /// redirect 重试最多 5 次（与 process_request_internal 一致）。
    async fn send_coherence_msg(
        &self,
        msg_type: powerfs_net::MsgType,
        shard_id: u64,
        body: Vec<u8>,
    ) -> Result<Vec<u8>, String> {
        const MAX_ATTEMPTS: u32 = 5;
        let mut attempt: u32 = 0;

        loop {
            attempt += 1;

            // 1) 获取当前分片 leader 地址（回退到 default_filer_addr）
            let leader_addr = self
                .shard_router
                .get(&shard_id)
                .map(|s| s.leader_addr.clone())
                .unwrap_or_else(|| self.default_filer_addr());

            if leader_addr.is_empty() {
                return Err(format!("no leader for shard {}", shard_id));
            }

            // 2) 获取或创建连接
            let filer_client = self
                .get_or_create_filer_client(&leader_addr)
                .await
                .map_err(|e| format!("connect filer {}: {:?}", leader_addr, e))?;

            // 3) circuit breaker 检查
            if !self.breakers.check(&leader_addr) {
                return Err(format!("circuit open for {}", leader_addr));
            }

            // 4) 发送请求
            let send_result = filer_client.send_request(msg_type, &body, &[]).await;

            match send_result {
                Ok(resp) => {
                    let status = resp.header.status;
                    log::debug!(
                        "send_coherence_msg: {:?} shard={} attempt={} leader={} status={} body_len={}",
                        msg_type,
                        shard_id,
                        attempt,
                        leader_addr,
                        status,
                        resp.body.len()
                    );

                    if status == powerfs_net::STATUS_OK {
                        self.breakers.record_success(&leader_addr);
                        return Ok(resp.body);
                    }

                    // redirect：解析新 leader 地址，更新路由，重试
                    if status == powerfs_net::STATUS_ERR_REDIRECT && attempt < MAX_ATTEMPTS {
                        let new_leader = {
                            use powerfs_net::serialize::TlvDecoder;
                            let mut dec = TlvDecoder::new(&resp.body);
                            match dec.next_string(powerfs_net::FieldId::Owner) {
                                Ok(addr) if !addr.is_empty() => Some(addr),
                                _ => None,
                            }
                        };

                        if let Some(new_addr) = new_leader {
                            log::info!(
                                "send_coherence_msg: shard={} redirect {} -> {} (attempt {}/{})",
                                shard_id,
                                leader_addr,
                                new_addr,
                                attempt,
                                MAX_ATTEMPTS
                            );
                            self.shard_router
                                .insert(shard_id, ShardInfo::new(shard_id, new_addr));
                            // Minimal backoff for local cluster: 5ms instead of 50ms.
                            let delay_ms = 5u64 << (attempt - 1).min(3);
                            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                            continue;
                        }
                    }

                    // 其他错误：尝试从 body 解析错误信息
                    // 优先尝试 TLV (FieldId::Name = error string, 用于 UpdateInodeSizeChunks),
                    // 回退到 JSON (用于 OpenCountInc/Dec, alloc_inode_batch 等仍用 JSON 的协议)
                    self.breakers.record_failure(&leader_addr);
                    let err_msg = {
                        use powerfs_net::serialize::TlvDecoder;
                        let mut dec = TlvDecoder::new(&resp.body);
                        dec.next_string(powerfs_net::FieldId::Name)
                            .ok()
                            .filter(|s| !s.is_empty())
                    }
                    .or_else(|| {
                        serde_json::from_slice::<serde_json::Value>(&resp.body)
                            .ok()
                            .and_then(|v| v.get("error").and_then(|e| e.as_str()).map(String::from))
                    })
                    .unwrap_or_else(|| format!("server status {}", status));
                    return Err(err_msg);
                }
                Err(e) => {
                    self.breakers.record_failure(&leader_addr);
                    return Err(format!("net error: {:?}", e));
                }
            }
        }
    }

    /// alloc_inode_batch：向 filer 申请 inode 预留段（leader only）。
    ///
    /// TLV 编码: Request = ShardId + Count + ClientId
    ///           Response = StartInode + EndInode (成功) / Name=error (失败)
    pub async fn alloc_inode_batch(
        &self,
        req: &powerfs_coherence::AllocInodeBatchRequest,
    ) -> Result<powerfs_coherence::AllocInodeBatchResponse, String> {
        use powerfs_net::serialize::{TlvDecoder, TlvEncoder};
        use powerfs_net::FieldId;

        let mut enc = TlvEncoder::new();
        enc.add_u64(FieldId::ShardId, req.shard_id);
        enc.add_u32(FieldId::Count, req.count);
        let _ = enc.add_string(FieldId::ClientId, &req.client_id);

        let body = enc.into_bytes();
        let resp_body = self
            .send_coherence_msg(powerfs_net::MsgType::AllocInodeBatch, req.shard_id, body)
            .await?;

        let mut dec = TlvDecoder::new(&resp_body);
        let start_inode = dec.next_u64(FieldId::StartInode).unwrap_or(0);
        let end_inode = dec.next_u64(FieldId::EndInode).unwrap_or(0);

        Ok(powerfs_coherence::AllocInodeBatchResponse {
            success: true,
            error: String::new(),
            start_inode,
            end_inode,
        })
    }

    /// update_inode_size_chunks：close 时强一致 sync 账本到 filer（leader only）。
    ///
    /// TLV 编码 (替代 JSON):
    /// - Request: ShardId + Ino + Size + ClientId + FileLayout (chunks 二进制 TLV)
    /// - Response: STATUS_OK + 空 body (成功) / STATUS_ERR + FieldId::Name=error (失败)
    pub async fn update_inode_size_chunks(
        &self,
        req: &powerfs_coherence::UpdateInodeSizeChunksRequest,
    ) -> Result<powerfs_coherence::UpdateInodeSizeChunksResponse, String> {
        use powerfs_layout::codec::{encode_file_layout, FEATURE_CHUNK_LAYOUT_V2};
        use powerfs_layout::encoding::{ChunkEncoding, ChunkRef};
        use powerfs_layout::layout::FileLayout;
        use powerfs_layout::placement::Placement;
        use powerfs_layout::reliability::{CompressionState, Reliability, ReliabilityState};
        use powerfs_net::serialize::TlvEncoder;
        use powerfs_net::FieldId;

        let mut enc = TlvEncoder::new();
        enc.add_u64(FieldId::ShardId, req.shard_id);
        enc.add_u64(FieldId::Ino, req.inode);
        enc.add_u64(FieldId::Size, req.size);
        let _ = enc.add_string(FieldId::ClientId, &req.client_id);

        // P2.5: Inline 模式 — 把 inline_data 编码为 FileLayout
        // (Placement::Inline + ChunkEncoding::InlineData), 与 Filer 端
        // handle_update_inode_size_chunks 的解码逻辑对称. chunks 应为空.
        // Flat 模式 — chunks 编码为 FileLayout (Placement::Flat + PerChunk).
        if let Some(data) = &req.inline_data {
            // 安全上限: 与 Filer 端 INLINE_HARD_LIMIT (8KB) 一致
            if data.len() > 8 * 1024 {
                return Err(format!(
                    "inline_data too large: {} bytes > 8KB",
                    data.len()
                ));
            }
            let max_size = (8 * 1024u32).max(data.len() as u32);
            let layout = FileLayout {
                placement: Placement::Inline { max_size },
                reliability: Reliability::SingleReplica,
                reliability_state: ReliabilityState::default(),
                compression: CompressionState::default(),
                encoding: ChunkEncoding::InlineData { data: data.clone() },
            };
            encode_file_layout(&mut enc, &layout, FEATURE_CHUNK_LAYOUT_V2)
                .map_err(|e| format!("encode_file_layout: {}", e))?;
        } else if !req.chunks.is_empty() {
            let chunks: Vec<ChunkRef> = req
                .chunks
                .iter()
                .map(|c| ChunkRef {
                    offset: c.offset,
                    size: c.size,
                    needle_id: c.needle_id,
                    volume_id: c.volume_id,
                    crc32: c.crc32,
                    mtime: c.mtime,
                })
                .collect();
            let layout = FileLayout {
                placement: Placement::Flat,
                reliability: Reliability::SingleReplica,
                reliability_state: ReliabilityState::default(),
                compression: CompressionState::default(),
                encoding: ChunkEncoding::PerChunk { chunks },
            };
            encode_file_layout(&mut enc, &layout, FEATURE_CHUNK_LAYOUT_V2)
                .map_err(|e| format!("encode_file_layout: {}", e))?;
        }

        let body = enc.into_bytes();
        // send_coherence_msg returns Err on non-OK status (with error message),
        // Ok(body) on success. Success body is empty for UpdateInodeSizeChunks.
        self.send_coherence_msg(
            powerfs_net::MsgType::UpdateInodeSizeChunks,
            req.shard_id,
            body,
        )
        .await?;
        Ok(powerfs_coherence::UpdateInodeSizeChunksResponse {
            success: true,
            error: String::new(),
        })
    }

    /// P2.5c: Inline → Flat 迁移分配. 客户端 write 超 max_size×1.5 时调用.
    /// Filer 仅分配 (volume_id, needle_id), 不修改 inode (crash safety).
    /// 返回 (volume_id, needle_id), 客户端把数据放入 chunk_cache, close 时
    /// flush + sync 原子完成切换.
    ///
    /// TLV 编码: Request = ShardId + Ino
    ///           Response = VolumeId + FileKey(needle_id)
    pub async fn migrate_inline_alloc(
        &self,
        shard_id: u64,
        inode: u64,
    ) -> Result<(u64, u64), String> {
        use powerfs_net::serialize::{TlvDecoder, TlvEncoder};
        use powerfs_net::FieldId;

        let mut enc = TlvEncoder::new();
        enc.add_u64(FieldId::ShardId, shard_id);
        enc.add_u64(FieldId::Ino, inode);

        let body = enc.into_bytes();
        let resp_body = self
            .send_coherence_msg(powerfs_net::MsgType::MigrateInlineAlloc, shard_id, body)
            .await?;

        let mut dec = TlvDecoder::new(&resp_body);
        let volume_id = dec
            .next_u64(FieldId::VolumeId)
            .map_err(|_| "migrate_inline_alloc: response missing VolumeId".to_string())?;
        let needle_id = dec
            .next_u64(FieldId::FileKey)
            .map_err(|_| "migrate_inline_alloc: response missing FileKey".to_string())?;
        Ok((volume_id, needle_id))
    }

    /// P3: Set an extended attribute on an inode (persisted via Raft on Filer).
    /// Used to set `powerfs.placement` xattr on directories for stripe policy.
    pub async fn set_xattr(
        &self,
        shard_id: u64,
        inode: u64,
        key: &str,
        value: &[u8],
    ) -> Result<(), String> {
        use powerfs_net::serialize::{TlvDecoder, TlvEncoder};
        use powerfs_net::FieldId;

        let mut enc = TlvEncoder::new();
        enc.add_u64(FieldId::ShardId, shard_id);
        enc.add_u64(FieldId::Ino, inode);
        enc.add_string(FieldId::XattrKey, key)
            .map_err(|e| format!("encode xattr key: {:?}", e))?;
        enc.add_bytes(FieldId::XattrValue, value)
            .map_err(|e| format!("encode xattr value: {:?}", e))?;

        let body = enc.into_bytes();
        self.send_coherence_msg(powerfs_net::MsgType::SetXattr, shard_id, body)
            .await?;
        Ok(())
    }

    /// P3: Get an extended attribute from an inode.
    pub async fn get_xattr(
        &self,
        shard_id: u64,
        inode: u64,
        key: &str,
    ) -> Result<Vec<u8>, String> {
        use powerfs_net::serialize::{TlvDecoder, TlvEncoder};
        use powerfs_net::FieldId;

        let mut enc = TlvEncoder::new();
        enc.add_u64(FieldId::ShardId, shard_id);
        enc.add_u64(FieldId::Ino, inode);
        enc.add_string(FieldId::XattrKey, key)
            .map_err(|e| format!("encode xattr key: {:?}", e))?;

        let body = enc.into_bytes();
        let resp_body = self
            .send_coherence_msg(powerfs_net::MsgType::GetXattr, shard_id, body)
            .await?;

        let mut dec = TlvDecoder::new(&resp_body);
        dec.next_bytes(FieldId::XattrValue)
            .map_err(|e| format!("get_xattr: response missing XattrValue: {:?}", e))
    }

    /// Phase 3.5.3: open_count 递增——fuse open 时通知 filer（leader only）。
    ///
    /// TLV 编码: Request = ShardId + Ino
    ///           Response = OpenCount (成功) / Name=error (失败)
    pub async fn open_count_inc(
        &self,
        req: &powerfs_coherence::OpenCountRequest,
    ) -> Result<powerfs_coherence::OpenCountResponse, String> {
        use powerfs_net::serialize::{TlvDecoder, TlvEncoder};
        use powerfs_net::FieldId;

        let mut enc = TlvEncoder::new();
        enc.add_u64(FieldId::ShardId, req.shard_id);
        enc.add_u64(FieldId::Ino, req.inode);

        let body = enc.into_bytes();
        let resp_body = self
            .send_coherence_msg(powerfs_net::MsgType::OpenCountInc, req.shard_id, body)
            .await?;

        let mut dec = TlvDecoder::new(&resp_body);
        let open_count = dec.next_u32(FieldId::OpenCount).unwrap_or(0);

        Ok(powerfs_coherence::OpenCountResponse {
            success: true,
            open_count,
            error: String::new(),
        })
    }

    /// Phase 3.5.3: open_count 递减——fuse release/close 时通知 filer（leader only）。
    ///
    /// TLV 编码: Request = ShardId + Ino
    ///           Response = OpenCount (成功) / Name=error (失败)
    pub async fn open_count_dec(
        &self,
        req: &powerfs_coherence::OpenCountRequest,
    ) -> Result<powerfs_coherence::OpenCountResponse, String> {
        use powerfs_net::serialize::{TlvDecoder, TlvEncoder};
        use powerfs_net::FieldId;

        let mut enc = TlvEncoder::new();
        enc.add_u64(FieldId::ShardId, req.shard_id);
        enc.add_u64(FieldId::Ino, req.inode);

        let body = enc.into_bytes();
        let resp_body = self
            .send_coherence_msg(powerfs_net::MsgType::OpenCountDec, req.shard_id, body)
            .await?;

        let mut dec = TlvDecoder::new(&resp_body);
        let open_count = dec.next_u32(FieldId::OpenCount).unwrap_or(0);

        Ok(powerfs_coherence::OpenCountResponse {
            success: true,
            open_count,
            error: String::new(),
        })
    }

    // =====================================================================
    // Inode Metadata Lease (方案 A, Phase 2)
    // =====================================================================
    //
    // Filer-managed per-inode exclusive lease. Used when the Volume Server
    // backend doesn't support range lease (e.g., NVMe-oF target). The lease
    // is an admission-control mechanism: only the holder can write to the
    // inode. Strong consistency (content_size + chunks atomicity) is still
    // guaranteed by Raft (`UpdateInodeSizeChunks`); the lease prevents
    // concurrent writers from producing conflicting intermediate states.
    //
    // Lifecycle:
    //   1. acquire_inode_lease(inode) → token
    //   2. write data to Volume Server (no lease validation)
    //   3. update_inode_size_chunks (Raft atomic commit)
    //   4. release_inode_lease(inode, token)
    //
    // The lease is in-memory on the Filer shard leader. If the leader
    // changes, lease state is lost; clients retry acquire on the new leader
    // (send_coherence_msg handles REDIRECT + retry transparently).

    /// Acquire an exclusive inode metadata lease from the Filer.
    ///
    /// Returns `(token, expire_at_ms)` on success. The token must be passed
    /// to `release_inode_lease` / `renew_inode_lease` later.
    pub async fn acquire_inode_lease(
        &self,
        inode: u64,
        client_id: &str,
        duration_ms: u64,
    ) -> Result<(String, u64), String> {
        let shard_id = self.calculate_shard_id(inode);

        let mut enc = serialize::TlvEncoder::new();
        enc.add_u64(powerfs_net::FieldId::Ino, inode);
        enc.add_string(powerfs_net::FieldId::ClientId, client_id)
            .map_err(|e| format!("encode ClientId: {:?}", e))?;
        enc.add_u64(powerfs_net::FieldId::LeaseDuration, duration_ms);

        let resp_body = self
            .send_coherence_msg(MsgType::AcquireInodeLease, shard_id, enc.into_bytes())
            .await?;

        let mut dec = serialize::TlvDecoder::new(&resp_body);
        let token = dec
            .next_string(powerfs_net::FieldId::LeaseId)
            .map_err(|e| format!("decode LeaseId: {:?}", e))?;
        let expire_at_ms = dec
            .next_u64(powerfs_net::FieldId::LeaseDuration)
            .unwrap_or(duration_ms);

        if token.is_empty() {
            return Err("AcquireInodeLease response has empty token".to_string());
        }

        log::debug!(
            "acquire_inode_lease: inode={} client={} duration_ms={} token={}... expire_at_ms={}",
            inode,
            client_id,
            duration_ms,
            &token[..token.len().min(16)],
            expire_at_ms
        );

        Ok((token, expire_at_ms))
    }

    /// Release an inode metadata lease. Idempotent: releasing a non-existent
    /// or expired lease returns Ok(()).
    pub async fn release_inode_lease(
        &self,
        inode: u64,
        client_id: &str,
        token: &str,
    ) -> Result<(), String> {
        let shard_id = self.calculate_shard_id(inode);

        let mut enc = serialize::TlvEncoder::new();
        enc.add_u64(powerfs_net::FieldId::Ino, inode);
        enc.add_string(powerfs_net::FieldId::ClientId, client_id)
            .map_err(|e| format!("encode ClientId: {:?}", e))?;
        enc.add_string(powerfs_net::FieldId::LeaseToken, token)
            .map_err(|e| format!("encode LeaseToken: {:?}", e))?;

        self.send_coherence_msg(MsgType::ReleaseInodeLease, shard_id, enc.into_bytes())
            .await?;

        log::debug!("release_inode_lease: inode={} client={}", inode, client_id);

        Ok(())
    }

    /// Renew an existing inode metadata lease. The holder and token must
    /// match the original acquire.
    pub async fn renew_inode_lease(
        &self,
        inode: u64,
        client_id: &str,
        token: &str,
        duration_ms: u64,
    ) -> Result<(), String> {
        let shard_id = self.calculate_shard_id(inode);

        let mut enc = serialize::TlvEncoder::new();
        enc.add_u64(powerfs_net::FieldId::Ino, inode);
        enc.add_string(powerfs_net::FieldId::ClientId, client_id)
            .map_err(|e| format!("encode ClientId: {:?}", e))?;
        enc.add_string(powerfs_net::FieldId::LeaseToken, token)
            .map_err(|e| format!("encode LeaseToken: {:?}", e))?;
        enc.add_u64(powerfs_net::FieldId::LeaseDuration, duration_ms);

        self.send_coherence_msg(MsgType::RenewInodeLease, shard_id, enc.into_bytes())
            .await?;

        log::debug!(
            "renew_inode_lease: inode={} client={} duration_ms={}",
            inode,
            client_id,
            duration_ms
        );

        Ok(())
    }

    /// 关闭客户端
    pub fn close(&self) {
        self.stop_background_processor();
        *self.state.lock().unwrap() = MetaShardClientState::Closed;
        log::info!("MetaShardClient: Closed");
    }
}

// ---------------------------------------------------------------------------
// DeltaSyncChannel trait 实现：强一致路径下封装 meta_shard_client 的 RPC 调用
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
impl powerfs_coherence::DeltaSyncChannel for MetaShardClient {
    async fn alloc_inode_batch(
        &self,
        req: &powerfs_coherence::AllocInodeBatchRequest,
    ) -> Result<powerfs_coherence::AllocInodeBatchResponse, String> {
        MetaShardClient::alloc_inode_batch(self, req).await
    }

    async fn update_inode_size_chunks(
        &self,
        req: &powerfs_coherence::UpdateInodeSizeChunksRequest,
    ) -> Result<powerfs_coherence::UpdateInodeSizeChunksResponse, String> {
        MetaShardClient::update_inode_size_chunks(self, req).await
    }

    async fn open_count_inc(
        &self,
        req: &powerfs_coherence::OpenCountRequest,
    ) -> Result<powerfs_coherence::OpenCountResponse, String> {
        MetaShardClient::open_count_inc(self, req).await
    }

    async fn open_count_dec(
        &self,
        req: &powerfs_coherence::OpenCountRequest,
    ) -> Result<powerfs_coherence::OpenCountResponse, String> {
        MetaShardClient::open_count_dec(self, req).await
    }
}

// ---------------------------------------------------------------------------
// MetadataClient trait implementation: strong-consistency metadata operations
// ---------------------------------------------------------------------------

use crate::metadata_client::{
    MetadataAttr, MetadataClient, MetadataDirEntry, MetadataStatfs, SetattrParams,
};
use powerfs_common::error::{PowerFsError, Result as FsResult};
use powerfs_net::serialize;
use powerfs_net::MsgType;
use std::future::Future;
use std::pin::Pin;

fn map_err<E: std::fmt::Display>(e: E) -> PowerFsError {
    PowerFsError::Internal(e.to_string())
}

fn file_type_from_mode(mode: u32) -> u8 {
    match mode & libc::S_IFMT {
        libc::S_IFDIR => libc::DT_DIR,
        libc::S_IFLNK => libc::DT_LNK,
        libc::S_IFIFO => libc::DT_FIFO,
        libc::S_IFCHR => libc::DT_CHR,
        libc::S_IFBLK => libc::DT_BLK,
        libc::S_IFSOCK => libc::DT_SOCK,
        _ => libc::DT_REG,
    }
}

fn attr_from_resp(resp: serialize::AttrResponse) -> MetadataAttr {
    MetadataAttr {
        inode: resp.ino,
        mode: resp.mode,
        uid: resp.uid,
        gid: resp.gid,
        size: resp.size,
        mtime: resp.mtime,
        atime: resp.atime,
        ctime: resp.ctime,
        nlink: resp.nlink,
        rdev: resp.rdev,
        file_type: file_type_from_mode(resp.mode),
        symlink_target: None,
        // create 响应携带 Filer 自分配的 volume_id/needle_id；
        // lookup/getattr 响应通常为 None（chunks 由单独字段编码）。
        volume_id: resp.volume_id,
        file_key: resp.file_key,
        // 默认无 FileLayout (mkdir/symlink 等简单响应). 需要布局信息的调用点
        // (lookup/getattr/create) 用 attr_from_resp_with_layout.
        placement: None,
        inline_data: None,
        inline_max_size: None,
        chunks: Vec::new(),
    }
}

/// P2.5: 从响应 body 解析 FileLayout (best-effort). 无布局字段时返回 None.
fn parse_layout_from_body(body: &[u8]) -> Option<powerfs_layout::layout::FileLayout> {
    use powerfs_net::serialize::TlvDecoder;
    let mut dec = TlvDecoder::new(body);
    powerfs_layout::codec::decode_file_layout(&mut dec).ok()
}

/// P2.5: 构造 MetadataAttr 并从响应 body 解析 FileLayout, 提取
/// Placement / InlineData / InlineMaxSize. 用于 lookup/getattr/create
/// (这些响应携带 FileLayout TLV).
fn attr_from_resp_with_layout(resp: serialize::AttrResponse, body: &[u8]) -> MetadataAttr {
    let mut attr = attr_from_resp(resp);
    if let Some(layout) = parse_layout_from_body(body) {
        attr.inline_max_size = match &layout.placement {
            powerfs_layout::placement::Placement::Inline { max_size } => Some(*max_size),
            _ => None,
        };
        attr.inline_data = match &layout.encoding {
            powerfs_layout::encoding::ChunkEncoding::InlineData { data } => Some(data.clone()),
            _ => None,
        };
        // P3: Extract chunks from PerChunk encoding (for Stripe/Flat with explicit chunks)
        attr.chunks = match &layout.encoding {
            powerfs_layout::encoding::ChunkEncoding::PerChunk { chunks } => chunks.clone(),
            _ => Vec::new(),
        };
        attr.placement = Some(layout.placement);
    }
    attr
}

impl MetadataClient for MetaShardClient {
    fn lookup(
        &self,
        parent_ino: u64,
        name: &str,
        shard_id: u64,
    ) -> Pin<Box<dyn Future<Output = FsResult<MetadataAttr>> + Send + '_>> {
        let name = name.to_string();
        Box::pin(async move {
            let body = serialize::encode_lookup_req(parent_ino, &name).map_err(map_err)?;
            let resp = self
                .send_coherence_msg(MsgType::Lookup, shard_id, body)
                .await
                .map_err(map_err)?;
            let attr_resp = serialize::decode_attr_resp(&resp).map_err(map_err)?;
            Ok(attr_from_resp_with_layout(attr_resp, &resp))
        })
    }

    fn mkdir(
        &self,
        parent_ino: u64,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
        shard_id: u64,
    ) -> Pin<Box<dyn Future<Output = FsResult<MetadataAttr>> + Send + '_>> {
        let name = name.to_string();
        Box::pin(async move {
            let body =
                serialize::encode_mkdir_req(parent_ino, &name, mode, uid, gid).map_err(map_err)?;
            let resp = self
                .send_coherence_msg(MsgType::Mkdir, shard_id, body)
                .await
                .map_err(map_err)?;
            let attr_resp = serialize::decode_attr_resp(&resp).map_err(map_err)?;
            Ok(attr_from_resp(attr_resp))
        })
    }

    fn create(
        &self,
        parent_ino: u64,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
        shard_id: u64,
        fid_info: Option<(u64, u64, u64)>,
    ) -> Pin<Box<dyn Future<Output = FsResult<MetadataAttr>> + Send + '_>> {
        let name = name.to_string();
        Box::pin(async move {
            let body = serialize::encode_create_req(parent_ino, &name, mode, uid, gid, fid_info)
                .map_err(map_err)?;
            let resp = self
                .send_coherence_msg(MsgType::Create, shard_id, body)
                .await
                .map_err(map_err)?;
            let attr_resp = serialize::decode_attr_resp(&resp).map_err(map_err)?;
            Ok(attr_from_resp_with_layout(attr_resp, &resp))
        })
    }

    fn unlink(
        &self,
        parent_ino: u64,
        name: &str,
        shard_id: u64,
    ) -> Pin<Box<dyn Future<Output = FsResult<()>> + Send + '_>> {
        let name = name.to_string();
        Box::pin(async move {
            let body = serialize::encode_unlink_req(parent_ino, &name).map_err(map_err)?;
            self.send_coherence_msg(MsgType::Unlink, shard_id, body)
                .await
                .map_err(map_err)?;
            Ok(())
        })
    }

    fn rmdir(
        &self,
        parent_ino: u64,
        name: &str,
        shard_id: u64,
    ) -> Pin<Box<dyn Future<Output = FsResult<()>> + Send + '_>> {
        let name = name.to_string();
        Box::pin(async move {
            let body = serialize::encode_rmdir_req(parent_ino, &name).map_err(map_err)?;
            self.send_coherence_msg(MsgType::Rmdir, shard_id, body)
                .await
                .map_err(map_err)?;
            Ok(())
        })
    }

    fn rename(
        &self,
        parent_ino: u64,
        name: &str,
        new_parent_ino: u64,
        new_name: &str,
        shard_id: u64,
    ) -> Pin<Box<dyn Future<Output = FsResult<MetadataAttr>> + Send + '_>> {
        let name = name.to_string();
        let new_name = new_name.to_string();
        Box::pin(async move {
            let body = serialize::encode_rename_req(parent_ino, &name, new_parent_ino, &new_name)
                .map_err(map_err)?;
            let resp = self
                .send_coherence_msg(MsgType::Rename, shard_id, body)
                .await
                .map_err(map_err)?;
            let attr_resp = serialize::decode_attr_resp(&resp).map_err(map_err)?;
            Ok(attr_from_resp(attr_resp))
        })
    }

    fn symlink(
        &self,
        parent_ino: u64,
        name: &str,
        target: &str,
        shard_id: u64,
    ) -> Pin<Box<dyn Future<Output = FsResult<MetadataAttr>> + Send + '_>> {
        let name = name.to_string();
        let target = target.to_string();
        Box::pin(async move {
            let body =
                serialize::encode_symlink_req(parent_ino, &name, &target).map_err(map_err)?;
            let resp = self
                .send_coherence_msg(MsgType::Symlink, shard_id, body)
                .await
                .map_err(map_err)?;
            let attr_resp = serialize::decode_attr_resp(&resp).map_err(map_err)?;
            Ok(attr_from_resp(attr_resp))
        })
    }

    fn readlink(
        &self,
        ino: u64,
        shard_id: u64,
    ) -> Pin<Box<dyn Future<Output = FsResult<String>> + Send + '_>> {
        Box::pin(async move {
            let body = serialize::encode_readlink_req(ino).map_err(map_err)?;
            let resp = self
                .send_coherence_msg(MsgType::Readlink, shard_id, body)
                .await
                .map_err(map_err)?;
            let target = serialize::decode_readlink_resp(&resp).map_err(map_err)?;
            Ok(target)
        })
    }

    fn link(
        &self,
        ino: u64,
        new_parent_ino: u64,
        new_name: &str,
        shard_id: u64,
    ) -> Pin<Box<dyn Future<Output = FsResult<MetadataAttr>> + Send + '_>> {
        let new_name = new_name.to_string();
        Box::pin(async move {
            let body =
                serialize::encode_link_req(ino, new_parent_ino, &new_name).map_err(map_err)?;
            let resp = self
                .send_coherence_msg(MsgType::Link, shard_id, body)
                .await
                .map_err(map_err)?;
            let attr_resp = serialize::decode_attr_resp(&resp).map_err(map_err)?;
            Ok(attr_from_resp(attr_resp))
        })
    }

    fn readdir(
        &self,
        ino: u64,
        offset: u64,
        count: u32,
        shard_id: u64,
    ) -> Pin<Box<dyn Future<Output = FsResult<Vec<MetadataDirEntry>>> + Send + '_>> {
        Box::pin(async move {
            let body = serialize::encode_readdir_req(ino, offset, count).map_err(map_err)?;
            let resp = self
                .send_coherence_msg(MsgType::ReadDir, shard_id, body)
                .await
                .map_err(map_err)?;
            let entries = serialize::decode_readdir_resp(&resp).map_err(map_err)?;
            let result = entries
                .into_iter()
                .map(|e| MetadataDirEntry {
                    inode: e.ino,
                    name: e.name,
                    file_type: file_type_from_mode(e.mode),
                    offset: e.offset,
                })
                .collect();
            Ok(result)
        })
    }

    fn getattr(
        &self,
        ino: u64,
        shard_id: u64,
    ) -> Pin<Box<dyn Future<Output = FsResult<MetadataAttr>> + Send + '_>> {
        Box::pin(async move {
            let body = serialize::encode_getattr_req(ino).map_err(map_err)?;
            let resp = self
                .send_coherence_msg(MsgType::GetAttr, shard_id, body)
                .await
                .map_err(map_err)?;
            let attr_resp = serialize::decode_attr_resp(&resp).map_err(map_err)?;
            Ok(attr_from_resp_with_layout(attr_resp, &resp))
        })
    }

    fn setattr(
        &self,
        ino: u64,
        params: &SetattrParams,
        shard_id: u64,
    ) -> Pin<Box<dyn Future<Output = FsResult<MetadataAttr>> + Send + '_>> {
        let params = params.clone();
        Box::pin(async move {
            let body = serialize::encode_setattr_req(
                ino,
                params.mode,
                params.uid,
                params.gid,
                params.size,
            )
            .map_err(map_err)?;
            let resp = self
                .send_coherence_msg(MsgType::SetAttr, shard_id, body)
                .await
                .map_err(map_err)?;
            let attr_resp = serialize::decode_attr_resp(&resp).map_err(map_err)?;
            Ok(attr_from_resp(attr_resp))
        })
    }

    fn statfs(
        &self,
        shard_id: u64,
    ) -> Pin<Box<dyn Future<Output = FsResult<MetadataStatfs>> + Send + '_>> {
        Box::pin(async move {
            let body = serialize::encode_statfs_req().map_err(map_err)?;
            let resp = self
                .send_coherence_msg(MsgType::StatFs, shard_id, body)
                .await
                .map_err(map_err)?;
            let (total, free, total_inodes, free_inodes, block_size) =
                serialize::decode_statfs_resp(&resp).map_err(map_err)?;
            Ok(MetadataStatfs {
                total_bytes: total,
                free_bytes: free,
                total_inodes,
                free_inodes,
                block_size,
            })
        })
    }
}

// ---- 自由函数版本（供后台处理器使用） ----

/// 处理队列中所有可用的请求，返回是否处理了至少一个
#[allow(clippy::too_many_arguments)]
/// 旧版串行请求处理（已被 ShardedRpcPool 取代，保留作为参考）
#[allow(dead_code)]
async fn process_available_requests(
    data_queue: &Arc<RequestQueue>,
    control_queue: &Arc<RequestQueue>,
    data_channel: &Arc<TransportChannel>,
    control_channel: &Arc<TransportChannel>,
    breakers: &Arc<CircuitBreakerPool>,
    conn_pool: &Arc<powerfs_net::ClientConnPool>,
    default_filer_addr: &Arc<Mutex<String>>,
    shard_router: &Arc<DashMap<u64, ShardInfo>>,
    _topology_manager: &Arc<ClusterTopologyManager>,
    listeners: &Arc<Mutex<Vec<Arc<dyn RequestCompletionListener>>>>,
    response_waiters: &Arc<Mutex<ResponseWaiters>>,
) -> bool {
    // 优先处理控制请求
    if control_channel.can_accept() {
        let next_req = control_queue.dequeue();

        if let Some(req) = next_req {
            log::debug!("MetaShardClient: Processing control request");
            let request_id = req.context.request_id.clone();
            let result = process_request_internal(
                req,
                conn_pool,
                default_filer_addr,
                breakers,
                shard_router,
            )
            .await;

            // 解析 waiter
            {
                let mut waiters = response_waiters.lock().unwrap();
                if let Some(sender) = waiters.remove(&request_id) {
                    let _ = sender.send(result.clone());
                }
            }

            // 通知监听器
            for listener in listeners.lock().unwrap().iter() {
                listener.on_request_complete(result.clone());
            }
            return true;
        }
    }

    // 处理数据请求 - per-server breaker check happens inside process_request_internal
    if data_channel.can_accept() {
        let next_req = data_queue.dequeue();

        if let Some(req) = next_req {
            log::debug!("MetaShardClient: Processing data request");
            let request_id = req.context.request_id.clone();
            let result = process_request_internal(
                req,
                conn_pool,
                default_filer_addr,
                breakers,
                shard_router,
            )
            .await;

            // 解析 waiter
            {
                let mut waiters = response_waiters.lock().unwrap();
                if let Some(sender) = waiters.remove(&request_id) {
                    let _ = sender.send(result.clone());
                }
            }

            // 通知监听器
            for listener in listeners.lock().unwrap().iter() {
                listener.on_request_complete(result.clone());
            }
            return true;
        }
    }

    false
}

/// 内部请求处理逻辑（供 ShardedRpcPool 和后台处理器使用）
///
/// 包含 redirect 重试逻辑（最多 5 次，指数退避）。
pub(crate) async fn process_request_internal(
    req: PendingRequest,
    conn_pool: &Arc<powerfs_net::ClientConnPool>,
    default_filer_addr: &Arc<Mutex<String>>,
    breakers: &Arc<CircuitBreakerPool>,
    shard_router: &Arc<DashMap<u64, ShardInfo>>,
) -> ClientResult<RequestResult> {
    let request_id = req.context.request_id.clone();
    let kind = req.context.kind;
    let msg_type = req.context.msg_type;
    let body = req.context.payload.clone();
    let shard_id = req.shard_id;

    // 从 context 获取 MsgType
    let resolved_msg_type =
        powerfs_net::MsgType::from_u16(msg_type).unwrap_or_else(|| default_msg_type_for_kind(kind));

    // 检查是否为需要路由到 filer 的请求类型
    let needs_filer_route = matches!(kind, RequestKind::Metadata | RequestKind::Control);
    if !needs_filer_route {
        return Err(ClientError::UnsupportedRequest(format!("{:?}", kind)));
    }

    // 尝试最多5次（重定向重试），避免 Leader 切换期间误报失败
    const MAX_ATTEMPTS: u32 = 5;
    let mut attempt: u32 = 0;

    // 获取默认 filer 地址作为回退
    let fallback_addr = default_filer_addr.lock().unwrap().clone();

    loop {
        attempt += 1;

        // 1) 获取当前分片的 leader 地址，或使用默认地址
        let leader_addr = shard_router
            .get(&shard_id)
            .map(|s| s.leader_addr.clone())
            .unwrap_or_else(|| fallback_addr.clone());

        if leader_addr.is_empty() {
            return Err(ClientError::NoShardLeader(shard_id));
        }

        // 2) 获取或创建到该 leader 的连接（client_id / 通知处理器由连接池统一安装）
        let filer_client = get_or_create_filer_client(conn_pool, &leader_addr).await?;

        // 3) Per-server circuit breaker check
        if !breakers.check(&leader_addr) {
            return Err(ClientError::CircuitOpen);
        }

        // 4) 发送请求
        let send_result = filer_client
            .send_request(resolved_msg_type, &body, &[])
            .await;

        match send_result {
            Ok(resp) => {
                log::debug!(
                    "process_request_internal: attempt={} shard={} leader={} kind={:?} status={} body_len={} data_len={}",
                    attempt, shard_id, leader_addr, kind, resp.header.status, resp.body.len(), resp.data.len()
                );

                if resp.is_ok() {
                    breakers.record_success(&leader_addr);
                    return Ok(RequestResult::success_with_payload(
                        request_id, resp.body, resp.data,
                    ));
                }

                // 非 200 响应
                let status = resp.header.status;

                // STATUS_ERR_REDIRECT = 11, 需要解析重定向地址并重试
                // 注意: 重定向不是服务故障，不记录 breaker failure
                const STATUS_ERR_REDIRECT: u16 = 11;
                if status == STATUS_ERR_REDIRECT && attempt < MAX_ATTEMPTS {
                    // 从 TLV body 中解析 Owner 字段获取新的 leader 地址
                    let new_leader = {
                        use powerfs_net::serialize::TlvDecoder;
                        let mut dec = TlvDecoder::new(&resp.body);
                        match dec.next_string(powerfs_net::FieldId::Owner) {
                            Ok(addr) if !addr.is_empty() => Some(addr),
                            _ => None,
                        }
                    };

                    if let Some(new_addr) = new_leader {
                        log::info!(
                            "process_request_internal: shard={} redirect from {} -> {}, updating route and retrying (attempt {}/{})",
                            shard_id, leader_addr, new_addr, attempt, MAX_ATTEMPTS
                        );

                        // 更新分片路由表
                        shard_router.insert(shard_id, ShardInfo::new(shard_id, new_addr.clone()));

                        // Minimal backoff for local cluster: 5ms instead of 50ms.
                        let delay_ms = (5u64) << (attempt - 1).min(3);
                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;

                        // 重试请求
                        continue;
                    } else {
                        log::warn!(
                            "process_request_internal: redirect response with empty owner for shard={}",
                            shard_id
                        );
                    }
                }

                // STATUS_ERR_NOT_FOUND is a valid response for lookup/getattr
                // operations.  Return an empty RequestResult so callers can
                // interpret it as `Ok(None)` instead of a hard error.
                const STATUS_ERR_NOT_FOUND: u16 = 1;
                if status == STATUS_ERR_NOT_FOUND {
                    breakers.record_success(&leader_addr);
                    return Ok(RequestResult::empty(request_id));
                }

                // 其他错误或超过重试次数
                breakers.record_failure(&leader_addr);
                return Err(ClientError::Server(format!("Server error: {}", status)));
            }
            Err(e) => {
                breakers.record_failure(&leader_addr);
                return Err(ClientError::from_net_error(e));
            }
        }
    }
}

/// 获取或创建到指定地址的 filer 连接（自由函数版本，供后台处理器使用）
///
/// 连接的创建、复用、client_id 握手以及通知处理器安装均由 `ClientConnPool`
/// 统一管理。这里先以 `get_if_connected` 走快速 peek 路径，未命中时再调用
/// `get_or_connect_addr` 完成懒创建。
async fn get_or_create_filer_client(
    conn_pool: &Arc<powerfs_net::ClientConnPool>,
    addr: &str,
) -> ClientResult<Arc<PowerFsNetClient>> {
    // 先检查是否已有连接 (peek-only, 不触发创建)
    if let Some(client) = conn_pool.get_if_connected(addr) {
        return Ok(client);
    }

    // 慢路径：由连接池创建新连接（通知处理器在池创建时已配置）
    conn_pool
        .get_or_connect_addr(addr)
        .await
        .map_err(ClientError::from_net_error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::circuit_breaker::CircuitBreakerConfig;
    use crate::client_identity::ClientIdentity;
    use crate::topology::ClusterTopology;

    fn create_test_client() -> (MetaShardClient, Arc<ClusterTopologyManager>) {
        let topology_manager = Arc::new(ClusterTopologyManager::new());

        // 设置初始拓扑
        let mut topology = ClusterTopology::new();
        topology
            .shards
            .insert(1, ShardInfo::new(1, "127.0.0.1:9334".to_string()));
        topology_manager.update_topology(topology);

        let conn_pool = Arc::new(powerfs_net::ClientConnPool::new(
            1,
            powerfs_net::ClientPoolConfig::default(),
            None,
        ));
        let config = MetaShardClientConfig::default();
        let client = MetaShardClient::new(config, topology_manager.clone(), 1, conn_pool);
        client.init();

        (client, topology_manager)
    }

    fn create_test_context(kind: RequestKind) -> RequestContext {
        let identity = ClientIdentity::new();
        RequestContext::new(identity, kind, 0x0001, vec![])
    }

    #[test]
    fn test_initialization() {
        let (client, _) = create_test_client();
        assert_eq!(client.state(), MetaShardClientState::Ready);
        assert!(client.get_shard_leader(1).is_some());
        assert_eq!(client.get_shard_leader(2), None);
    }

    #[test]
    fn test_submit_metadata_request() {
        let (client, _) = create_test_client();
        let ctx = create_test_context(RequestKind::Metadata);

        assert!(client.submit_metadata_request(ctx, 1).is_ok());

        let (data_len, _) = client.queue_stats();
        assert_eq!(data_len, 1);
    }

    #[test]
    fn test_submit_control_request() {
        let (client, _) = create_test_client();
        let ctx = create_test_context(RequestKind::Control);

        assert!(client.submit_control_request(ctx, 1).is_ok());

        let (_, control_len) = client.queue_stats();
        assert_eq!(control_len, 1);
    }

    #[test]
    fn test_queue_processing() {
        let (client, _) = create_test_client();

        // 提交两个请求
        let ctx1 = create_test_context(RequestKind::Metadata);
        client.submit_metadata_request(ctx1, 1).unwrap();

        let ctx2 = create_test_context(RequestKind::Metadata);
        client.submit_metadata_request(ctx2, 1).unwrap();

        let (data_len, _) = client.queue_stats();
        assert_eq!(data_len, 2);

        // 出队一个
        let req = client.next_data_request();
        assert!(req.is_some());

        let (data_len, _) = client.queue_stats();
        assert_eq!(data_len, 1);
    }

    #[test]
    fn test_circuit_breaker_integration() {
        let (client, _) = create_test_client();

        // 先填充队列
        for _ in 0..100 {
            let ctx = create_test_context(RequestKind::Metadata);
            client.submit_metadata_request(ctx, 1).unwrap();
        }

        // 记录失败触发熔断 (使用默认阈值)
        let threshold = CircuitBreakerConfig::default().failure_threshold as usize;
        let filer_addr = client.get_shard_leader(1).unwrap_or_default();
        for _ in 0..threshold {
            let id = RequestId::new();
            client.record_failure(&id, RequestKind::Metadata, &filer_addr);
        }

        // 熔断器打开后，新请求应该被拒绝
        let ctx = create_test_context(RequestKind::Metadata);
        let result = client.submit_metadata_request(ctx, 1);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Circuit breaker is open"));
    }

    #[test]
    fn test_circuit_breaker_per_server_isolation() {
        let (client, _) = create_test_client();

        // 设置两个不同的 shard 路由到不同的 filer 地址
        client.set_shard_leader(1, "127.0.0.1:9334".to_string());
        client.set_shard_leader(2, "127.0.0.1:9335".to_string());

        // 对第一个 filer 记录失败触发熔断 (使用默认阈值)
        let threshold = CircuitBreakerConfig::default().failure_threshold as usize;
        let addr1 = client.get_shard_leader(1).unwrap_or_default();
        for _ in 0..threshold {
            let id = RequestId::new();
            client.record_failure(&id, RequestKind::Metadata, &addr1);
        }

        // 第一个 filer 的熔断器应打开
        assert!(!client.breakers.check(&addr1));

        // 第二个 filer 的熔断器应仍然关闭（可用）
        let addr2 = client.get_shard_leader(2).unwrap_or_default();
        assert!(client.breakers.check(&addr2));

        // 对第一个 shard 的请求应该被拒绝
        let ctx1 = create_test_context(RequestKind::Metadata);
        let result1 = client.submit_metadata_request(ctx1, 1);
        assert!(result1.is_err());

        // 对第二个 shard 的请求应该仍然可以提交
        let ctx2 = create_test_context(RequestKind::Metadata);
        let result2 = client.submit_metadata_request(ctx2, 2);
        assert!(result2.is_ok());
    }

    #[test]
    fn test_leader_change() {
        let (client, _topology_mgr) = create_test_client();

        // 提交一个请求
        let ctx = create_test_context(RequestKind::Metadata);
        client.submit_metadata_request(ctx, 1).unwrap();

        // 验证初始 Leader
        assert_eq!(
            client.get_shard_leader(1),
            Some("127.0.0.1:9334".to_string())
        );

        // 处理 Leader 变更
        client.handle_leader_change(1, "10.0.0.1:9334".to_string());

        // 验证新 Leader
        assert_eq!(
            client.get_shard_leader(1),
            Some("10.0.0.1:9334".to_string())
        );
        assert_eq!(client.state(), MetaShardClientState::Ready);
    }

    #[test]
    fn test_closed_client() {
        let (client, _) = create_test_client();
        client.close();

        let ctx = create_test_context(RequestKind::Metadata);
        let result = client.submit_metadata_request(ctx, 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_channel_availability() {
        let (client, _) = create_test_client();

        assert!(client.can_use_data_channel());
        assert!(client.can_use_control_channel());
    }
}
