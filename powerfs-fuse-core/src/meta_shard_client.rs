use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

use crate::circuit_breaker::CircuitBreaker;
use crate::client_error::{ClientError, ClientResult};
use crate::net_client::PowerFuseNetClient;
use crate::request_id::RequestId;
use crate::request_state::{RequestContext, RequestKind};
use crate::topology::{ClusterTopologyManager, ShardInfo};

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
}

/// 请求等待者类型别名
type ResponseWaiters = HashMap<RequestId, oneshot::Sender<Result<RequestResult, ClientError>>>;

impl RequestResult {
    pub fn success(request_id: RequestId, data: Vec<u8>) -> Self {
        Self {
            request_id,
            data: Some(data),
        }
    }

    pub fn empty(request_id: RequestId) -> Self {
        Self {
            request_id,
            data: None,
        }
    }
}

/// 请求完成监听器
pub trait RequestCompletionListener: Send + Sync {
    fn on_request_complete(&self, result: ClientResult<RequestResult>);
}

/// 待处理请求
#[derive(Debug)]
pub struct PendingRequest {
    pub context: RequestContext,
    pub shard_id: u64,
    pub enqueued_at: Instant,
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

/// 请求队列
pub struct RequestQueue {
    pub queue: VecDeque<PendingRequest>,
    pub max_size: usize,
}

impl RequestQueue {
    pub fn new(max_size: usize) -> Self {
        Self {
            queue: VecDeque::new(),
            max_size,
        }
    }

    pub fn enqueue(&mut self, req: PendingRequest) -> Result<(), String> {
        if self.queue.len() >= self.max_size {
            return Err("Queue is full".to_string());
        }
        self.queue.push_back(req);
        Ok(())
    }

    pub fn dequeue(&mut self) -> Option<PendingRequest> {
        self.queue.pop_front()
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
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
    /// 数据请求队列
    data_queue: Arc<Mutex<RequestQueue>>,
    /// 控制请求队列
    control_queue: Arc<Mutex<RequestQueue>>,
    /// 数据传输通道
    data_channel: Arc<TransportChannel>,
    /// 控制传输通道
    control_channel: Arc<TransportChannel>,
    /// 分片路由表 (shard_id -> ShardInfo)
    shard_router: Arc<RwLock<HashMap<u64, ShardInfo>>>,
    /// 熔断器
    breaker: Arc<CircuitBreaker>,
    /// 拓扑管理器引用
    topology_manager: Arc<ClusterTopologyManager>,
    /// 网络客户端 (可选，用于真实网络发送)
    net_client: Option<Arc<PowerFuseNetClient>>,
    /// 请求完成监听器
    listeners: Arc<Mutex<Vec<Arc<dyn RequestCompletionListener>>>>,
    /// 后台处理是否在运行
    background_running: Arc<Mutex<bool>>,
    /// 请求等待者映射 (request_id -> oneshot sender)
    response_waiters: Arc<Mutex<ResponseWaiters>>,
    /// 事件通知器（替代 10ms 轮询）
    notify: Arc<tokio::sync::Notify>,
}

impl MetaShardClient {
    pub fn new(
        config: MetaShardClientConfig,
        topology_manager: Arc<ClusterTopologyManager>,
    ) -> Self {
        Self {
            breaker: Arc::new(CircuitBreaker::new(config.circuit_breaker_config.clone())),
            data_channel: Arc::new(TransportChannel::new(config.data_channel.clone())),
            control_channel: Arc::new(TransportChannel::new(config.control_channel.clone())),
            data_queue: Arc::new(Mutex::new(RequestQueue::new(config.queue_max_size))),
            control_queue: Arc::new(Mutex::new(RequestQueue::new(config.queue_max_size))),
            shard_router: Arc::new(RwLock::new(HashMap::new())),
            state: Arc::new(Mutex::new(MetaShardClientState::Init)),
            response_waiters: Arc::new(Mutex::new(HashMap::new())),
            config,
            topology_manager,
            net_client: None,
            listeners: Arc::new(Mutex::new(Vec::new())),
            background_running: Arc::new(Mutex::new(false)),
            notify: Arc::new(tokio::sync::Notify::new()),
        }
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
    pub async fn submit_metadata_request_and_wait(
        &self,
        context: RequestContext,
        shard_id: u64,
        timeout: Duration,
    ) -> ClientResult<RequestResult> {
        let request_id = context.request_id.clone();
        let (tx, rx) = oneshot::channel();

        self.register_waiter(request_id.clone(), tx);
        self.submit_metadata_request(context, shard_id)
            .map_err(ClientError::Internal)?;

        // 尝试直接处理队列中的请求（如果队列前面没有其他请求）
        // 注意：这只是优化，真正的处理由后台处理器或显式 process_* 完成
        // 这里我们只需要等待结果
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(ClientError::Cancelled),
            Err(_) => {
                // 超时，清理等待者
                let mut waiters = self.response_waiters.lock().unwrap();
                waiters.remove(&request_id);
                Err(ClientError::Timeout(timeout))
            }
        }
    }

    /// 提交控制请求并等待响应
    pub async fn submit_control_request_and_wait(
        &self,
        context: RequestContext,
        shard_id: u64,
        timeout: Duration,
    ) -> ClientResult<RequestResult> {
        let request_id = context.request_id.clone();
        let (tx, rx) = oneshot::channel();

        self.register_waiter(request_id.clone(), tx);
        self.submit_control_request(context, shard_id)
            .map_err(ClientError::Internal)?;

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(ClientError::Cancelled),
            Err(_) => {
                let mut waiters = self.response_waiters.lock().unwrap();
                waiters.remove(&request_id);
                Err(ClientError::Timeout(timeout))
            }
        }
    }

    /// 启动后台处理循环（事件驱动，无轮询延迟）
    pub fn start_background_processor(&self) {
        let mut running = self.background_running.lock().unwrap();
        if *running {
            return;
        }
        *running = true;

        let data_queue = self.data_queue.clone();
        let control_queue = self.control_queue.clone();
        let data_channel = self.data_channel.clone();
        let control_channel = self.control_channel.clone();
        let shard_router = self.shard_router.clone();
        let breaker = self.breaker.clone();
        let topology_manager = self.topology_manager.clone();
        let net_client = self.net_client.clone();
        let listeners = self.listeners.clone();
        let background_running = self.background_running.clone();
        let state = self.state.clone();
        let response_waiters = self.response_waiters.clone();
        let notify = self.notify.clone();

        tokio::spawn(async move {
            log::info!("MetaShardClient: Background processor started");

            loop {
                // 检查是否应该停止
                if !*background_running.lock().unwrap() {
                    break;
                }

                // 检查状态
                let current_state = *state.lock().unwrap();
                if current_state == MetaShardClientState::Closed
                    || current_state == MetaShardClientState::Suspended
                {
                    // 暂停状态下等待通知（Leader变更恢复时会notify）
                    notify.notified().await;
                    continue;
                }

                // 尝试处理队列中的请求
                let processed = process_available_requests(
                    &data_queue,
                    &control_queue,
                    &data_channel,
                    &control_channel,
                    &breaker,
                    &net_client,
                    &shard_router,
                    &topology_manager,
                    &listeners,
                    &response_waiters,
                )
                .await;

                if processed {
                    // 处理了请求，检查队列中是否还有更多
                    continue;
                }

                // 队列为空，等待事件通知（入队时触发）
                notify.notified().await;
            }

            log::info!("MetaShardClient: Background processor stopped");
        });
    }

    /// 停止后台处理循环
    pub fn stop_background_processor(&self) {
        let mut running = self.background_running.lock().unwrap();
        *running = false;
        // 唤醒后台任务以立即检查停止标志
        self.notify.notify_one();
        log::info!("MetaShardClient: Stopping background processor...");
    }

    /// 设置网络客户端
    pub fn set_net_client(&mut self, client: Arc<PowerFuseNetClient>) {
        self.net_client = Some(client);
    }

    /// 获取网络客户端引用
    pub fn net_client(&self) -> Option<&Arc<PowerFuseNetClient>> {
        self.net_client.as_ref()
    }

    /// 初始化客户端
    pub fn init(&self) {
        // 从拓扑管理器加载分片信息
        self.sync_shard_router();
        *self.state.lock().unwrap() = MetaShardClientState::Ready;
        log::info!("MetaShardClient: Initialized");
    }

    /// 同步分片路由表
    fn sync_shard_router(&self) {
        let topology = self.topology_manager.get_topology();
        let mut router = self.shard_router.write().unwrap();
        *router = topology.shards.clone();
    }

    /// 直接设置分片 Leader（用于测试或动态路由更新）
    pub fn set_shard_leader(&self, shard_id: u64, leader_addr: String) {
        let mut router = self.shard_router.write().unwrap();
        router.insert(shard_id, ShardInfo::new(shard_id, leader_addr));
    }

    /// 获取当前状态
    pub fn state(&self) -> MetaShardClientState {
        *self.state.lock().unwrap()
    }

    /// 获取指定分片的 Leader
    pub fn get_shard_leader(&self, shard_id: u64) -> Option<String> {
        let router = self.shard_router.read().unwrap();
        router.get(&shard_id).map(|s| s.leader_addr.clone())
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

        if !self.breaker.is_available() {
            return Err("Circuit breaker is open".to_string());
        }

        let req = PendingRequest {
            context,
            shard_id,
            enqueued_at: Instant::now(),
        };

        let mut queue = self.data_queue.lock().unwrap();
        queue.enqueue(req)?;

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
        };

        let mut queue = self.control_queue.lock().unwrap();
        queue.enqueue(req)?;
        self.notify.notify_one();

        Ok(())
    }

    /// 从数据队列获取下一个请求
    pub fn next_data_request(&self) -> Option<PendingRequest> {
        let mut queue = self.data_queue.lock().unwrap();
        queue.dequeue()
    }

    /// 从控制队列获取下一个请求
    pub fn next_control_request(&self) -> Option<PendingRequest> {
        let mut queue = self.control_queue.lock().unwrap();
        queue.dequeue()
    }

    /// 检查数据通道是否可用
    pub fn can_use_data_channel(&self) -> bool {
        self.data_channel.can_accept() && self.breaker.is_available()
    }

    /// 检查控制通道是否可用
    pub fn can_use_control_channel(&self) -> bool {
        self.control_channel.can_accept()
    }

    /// 记录请求成功
    pub fn record_success(&self, request_id: &RequestId, kind: RequestKind) {
        match kind {
            RequestKind::Metadata | RequestKind::Control => {
                self.data_channel.remove_request(request_id);
                self.breaker.record_success();
            }
            _ => {}
        }
    }

    /// 记录请求失败
    pub fn record_failure(&self, request_id: &RequestId, kind: RequestKind) {
        match kind {
            RequestKind::Metadata | RequestKind::Control => {
                self.data_channel.remove_request(request_id);
                self.breaker.record_failure();
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

        // 步骤 2: 保存受影响分片的 pending 请求
        let mut data_queue = self.data_queue.lock().unwrap();
        let mut control_queue = self.control_queue.lock().unwrap();

        let mut affected_data_requests = Vec::new();
        let mut unaffected_data_requests = Vec::new();
        let mut affected_control_requests = Vec::new();
        let mut unaffected_control_requests = Vec::new();

        // 分离数据队列中的请求
        while let Some(req) = data_queue.dequeue() {
            if req.shard_id == shard_id {
                affected_data_requests.push(req);
            } else {
                unaffected_data_requests.push(req);
            }
        }

        // 分离控制队列中的请求
        while let Some(req) = control_queue.dequeue() {
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
        {
            let mut router = self.shard_router.write().unwrap();
            if let Some(shard) = router.get_mut(&shard_id) {
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
                router.insert(shard_id, ShardInfo::new(shard_id, new_leader.clone()));
                log::info!(
                    "MetaShardClient: Added new shard {} with leader {}",
                    shard_id,
                    new_leader
                );
            }
        }

        // 步骤 4: 将未受影响的请求重新入队
        for req in unaffected_data_requests {
            data_queue.enqueue(req).ok();
        }
        for req in unaffected_control_requests {
            control_queue.enqueue(req).ok();
        }

        // 步骤 5: 将受影响的请求重新入队（将由后台处理器自动重放）
        for mut req in affected_data_requests {
            // 重置请求状态，准备重试
            req.context.state = crate::request_state::RequestState::Init;
            data_queue.enqueue(req).ok();
        }
        for mut req in affected_control_requests {
            // 重置请求状态，准备重试
            req.context.state = crate::request_state::RequestState::Init;
            control_queue.enqueue(req).ok();
        }

        // 步骤 6: 恢复客户端，后台处理器将自动消费队列中的请求
        *self.state.lock().unwrap() = MetaShardClientState::Ready;
        self.notify.notify_one();
        log::info!(
            "MetaShardClient: Resumed with {} data requests and {} control requests in queue",
            data_queue.len(),
            control_queue.len()
        );
    }

    /// 异步处理数据队列中的请求 (真实网络发送)
    pub async fn process_data_request(&self, req: PendingRequest) -> ClientResult<RequestResult> {
        let request_id = req.context.request_id.clone();
        let kind = req.context.kind;
        let msg_type = req.context.msg_type;
        let body = req.context.payload.clone();

        // 检查熔断器
        if !self.breaker.is_available() {
            let result = Err(ClientError::CircuitOpen);
            self.resolve_waiter(&request_id, result.clone());
            return result;
        }

        // 检查网络客户端是否可用
        let net_client = self.net_client.as_ref().ok_or_else(|| {
            let err = ClientError::NoNetworkClient;
            self.resolve_waiter(&request_id, Err(err.clone()));
            err
        })?;

        // 获取分片 Leader
        let _leader = self.get_shard_leader(req.shard_id).ok_or_else(|| {
            let err = ClientError::NoShardLeader(req.shard_id);
            self.resolve_waiter(&request_id, Err(err.clone()));
            err
        })?;

        // 从 context 获取 MsgType，若无效则回退到默认值
        let resolved_msg_type = powerfs_net::MsgType::from_u16(msg_type)
            .unwrap_or_else(|| default_msg_type_for_kind(kind));

        // 发送请求
        let result = match kind {
            RequestKind::Metadata => {
                let msg = net_client
                    .filer_client()
                    .send_request(resolved_msg_type, &body, &[])
                    .await;

                match msg {
                    Ok(resp) if resp.is_ok() => {
                        self.breaker.record_success();
                        Ok(RequestResult::success(request_id.clone(), resp.body))
                    }
                    Ok(resp) => {
                        self.breaker.record_failure();
                        Err(ClientError::Server(format!(
                            "Server error: {}",
                            resp.header.status
                        )))
                    }
                    Err(e) => {
                        self.breaker.record_failure();
                        Err(ClientError::from_net_error(e))
                    }
                }
            }
            RequestKind::Control => {
                let msg = net_client
                    .filer_client()
                    .send_request(resolved_msg_type, &body, &[])
                    .await;

                match msg {
                    Ok(resp) if resp.is_ok() => {
                        self.breaker.record_success();
                        Ok(RequestResult::success(request_id.clone(), resp.body))
                    }
                    Ok(resp) => {
                        self.breaker.record_failure();
                        Err(ClientError::Server(format!(
                            "Server error: {}",
                            resp.header.status
                        )))
                    }
                    Err(e) => {
                        self.breaker.record_failure();
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
        let data_len = self.data_queue.lock().unwrap().len();
        let control_len = self.control_queue.lock().unwrap().len();
        (data_len, control_len)
    }

    /// 关闭客户端
    pub fn close(&self) {
        self.stop_background_processor();
        *self.state.lock().unwrap() = MetaShardClientState::Closed;
        log::info!("MetaShardClient: Closed");
    }
}

// ---- 自由函数版本（供后台处理器使用） ----

/// 处理队列中所有可用的请求，返回是否处理了至少一个
#[allow(clippy::too_many_arguments)]
async fn process_available_requests(
    data_queue: &Arc<Mutex<RequestQueue>>,
    control_queue: &Arc<Mutex<RequestQueue>>,
    data_channel: &Arc<TransportChannel>,
    control_channel: &Arc<TransportChannel>,
    breaker: &Arc<CircuitBreaker>,
    net_client: &Option<Arc<PowerFuseNetClient>>,
    shard_router: &Arc<RwLock<HashMap<u64, ShardInfo>>>,
    topology_manager: &Arc<ClusterTopologyManager>,
    listeners: &Arc<Mutex<Vec<Arc<dyn RequestCompletionListener>>>>,
    response_waiters: &Arc<Mutex<ResponseWaiters>>,
) -> bool {
    // 优先处理控制请求
    if control_channel.can_accept() {
        let next_req = {
            let mut queue = control_queue.lock().unwrap();
            queue.dequeue()
        };

        if let Some(req) = next_req {
            log::debug!("MetaShardClient: Processing control request");
            let request_id = req.context.request_id.clone();
            let result = process_request_internal(
                req,
                net_client,
                breaker,
                data_channel,
                control_channel,
                shard_router,
                topology_manager,
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

    // 处理数据请求
    if data_channel.can_accept() && breaker.is_available() {
        let next_req = {
            let mut queue = data_queue.lock().unwrap();
            queue.dequeue()
        };

        if let Some(req) = next_req {
            log::debug!("MetaShardClient: Processing data request");
            let request_id = req.context.request_id.clone();
            let result = process_request_internal(
                req,
                net_client,
                breaker,
                data_channel,
                control_channel,
                shard_router,
                topology_manager,
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

/// 内部请求处理逻辑（供后台处理器使用）
async fn process_request_internal(
    req: PendingRequest,
    net_client: &Option<Arc<PowerFuseNetClient>>,
    breaker: &Arc<CircuitBreaker>,
    _data_channel: &Arc<TransportChannel>,
    _control_channel: &Arc<TransportChannel>,
    shard_router: &Arc<RwLock<HashMap<u64, ShardInfo>>>,
    _topology_manager: &Arc<ClusterTopologyManager>,
) -> ClientResult<RequestResult> {
    let request_id = req.context.request_id.clone();
    let kind = req.context.kind;
    let msg_type = req.context.msg_type;
    let body = req.context.payload.clone();

    // 检查熔断器
    if !breaker.is_available() {
        return Err(ClientError::CircuitOpen);
    }

    // 检查网络客户端是否可用
    let nc = net_client.as_ref().ok_or(ClientError::NoNetworkClient)?;

    // 获取分片 Leader
    let _leader = shard_router
        .read()
        .unwrap()
        .get(&req.shard_id)
        .ok_or(ClientError::NoShardLeader(req.shard_id))?
        .leader_addr
        .clone();

    // 从 context 获取 MsgType
    let resolved_msg_type =
        powerfs_net::MsgType::from_u16(msg_type).unwrap_or_else(|| default_msg_type_for_kind(kind));

    // 发送请求
    match kind {
        RequestKind::Metadata => {
            let msg = nc
                .filer_client()
                .send_request(resolved_msg_type, &body, &[])
                .await;

            match msg {
                Ok(resp) if resp.is_ok() => {
                    breaker.record_success();
                    Ok(RequestResult::success(request_id, resp.body))
                }
                Ok(resp) => {
                    breaker.record_failure();
                    Err(ClientError::Server(format!(
                        "Server error: {}",
                        resp.header.status
                    )))
                }
                Err(e) => {
                    breaker.record_failure();
                    Err(ClientError::from_net_error(e))
                }
            }
        }
        RequestKind::Control => {
            let msg = nc
                .filer_client()
                .send_request(resolved_msg_type, &body, &[])
                .await;

            match msg {
                Ok(resp) if resp.is_ok() => {
                    breaker.record_success();
                    Ok(RequestResult::success(request_id, resp.body))
                }
                Ok(resp) => {
                    breaker.record_failure();
                    Err(ClientError::Server(format!(
                        "Server error: {}",
                        resp.header.status
                    )))
                }
                Err(e) => {
                    breaker.record_failure();
                    Err(ClientError::from_net_error(e))
                }
            }
        }
        _ => Err(ClientError::UnsupportedRequest(format!("{:?}", kind))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

        let config = MetaShardClientConfig::default();
        let client = MetaShardClient::new(config, topology_manager.clone());
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

        // 记录失败触发熔断
        for _ in 0..5 {
            let id = RequestId::new();
            client.record_failure(&id, RequestKind::Metadata);
        }

        // 熔断器打开后，新请求应该被拒绝
        let ctx = create_test_context(RequestKind::Metadata);
        let result = client.submit_metadata_request(ctx, 1);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Circuit breaker is open"));
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
