use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

use crate::circuit_breaker::CircuitBreaker;
use crate::client_error::{ClientError, ClientResult};
use crate::meta_shard_client::RequestResult;
use crate::meta_shard_client::{ChannelConfig, PendingRequest, RequestQueue, TransportChannel};
use crate::net_client::PowerFuseNetClient;
use crate::request_id::RequestId;
use crate::request_state::{RequestContext, RequestKind};
use crate::topology::{ClusterTopologyManager, VolumeInfo};
use powerfs_net::serialize::{TlvDecoder, TlvEncoder};
use powerfs_net::FieldId;

/// 请求等待者类型别名
type VolumeResponseWaiters =
    HashMap<RequestId, oneshot::Sender<Result<RequestResult, ClientError>>>;

/// Volume 客户端配置
#[derive(Debug, Clone)]
pub struct VolumeClientConfig {
    /// 数据通道配置 (用于读写请求)
    pub data_channel: ChannelConfig,
    /// Lease 通道配置
    pub lease_channel: ChannelConfig,
    /// 管理通道配置
    pub mgmt_channel: ChannelConfig,
    /// 队列最大大小
    pub queue_max_size: usize,
    /// 熔断器配置
    pub circuit_breaker_config: crate::circuit_breaker::CircuitBreakerConfig,
}

impl Default for VolumeClientConfig {
    fn default() -> Self {
        Self {
            data_channel: ChannelConfig {
                channel_id: 1,
                name: "data".to_string(),
                max_concurrent: 32,
                timeout: std::time::Duration::from_secs(10),
            },
            lease_channel: ChannelConfig {
                channel_id: 2,
                name: "lease".to_string(),
                max_concurrent: 4,
                timeout: std::time::Duration::from_secs(3),
            },
            mgmt_channel: ChannelConfig {
                channel_id: 3,
                name: "management".to_string(),
                max_concurrent: 4,
                timeout: std::time::Duration::from_secs(5),
            },
            queue_max_size: 2000,
            circuit_breaker_config: crate::circuit_breaker::CircuitBreakerConfig::default(),
        }
    }
}

/// Lease 状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseState {
    /// 未获取
    None,
    /// 已获取
    Acquired,
    /// 已过期
    Expired,
    /// 已释放
    Released,
}

impl LeaseState {
    pub fn as_str(&self) -> &str {
        match self {
            LeaseState::None => "None",
            LeaseState::Acquired => "Acquired",
            LeaseState::Expired => "Expired",
            LeaseState::Released => "Released",
        }
    }
}

impl std::fmt::Display for LeaseState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Lease 信息
#[derive(Debug, Clone)]
pub struct LeaseInfo {
    /// Lease token
    pub token: String,
    /// Lease 开始时间
    pub acquired_at: Instant,
    /// Lease 过期时间
    pub expire_at: Instant,
    /// 当前状态
    pub state: LeaseState,
}

impl LeaseInfo {
    pub fn new(token: String, duration: std::time::Duration) -> Self {
        let now = Instant::now();
        Self {
            token,
            acquired_at: now,
            expire_at: now + duration,
            state: LeaseState::Acquired,
        }
    }

    pub fn is_valid(&self) -> bool {
        self.state == LeaseState::Acquired && Instant::now() < self.expire_at
    }

    pub fn is_expired(&self) -> bool {
        Instant::now() >= self.expire_at
    }

    pub fn renew(&mut self, duration: std::time::Duration) {
        let now = Instant::now();
        self.acquired_at = now;
        self.expire_at = now + duration;
        self.state = LeaseState::Acquired;
    }

    pub fn release(&mut self) {
        self.state = LeaseState::Released;
    }
}

/// Volume 客户端状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VolumeClientState {
    /// 初始状态
    Init,
    /// 已就绪
    Ready,
    /// 处理中
    Processing,
    /// 已暂停
    Suspended,
    /// 已关闭
    Closed,
}

impl VolumeClientState {
    pub fn as_str(&self) -> &str {
        match self {
            VolumeClientState::Init => "Init",
            VolumeClientState::Ready => "Ready",
            VolumeClientState::Processing => "Processing",
            VolumeClientState::Suspended => "Suspended",
            VolumeClientState::Closed => "Closed",
        }
    }
}

impl std::fmt::Display for VolumeClientState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// VolumeClient - 数据卷客户端
#[allow(dead_code)]
pub struct VolumeClient {
    config: VolumeClientConfig,
    state: Arc<Mutex<VolumeClientState>>,
    /// 数据请求队列 (共享读写)
    data_queue: Arc<Mutex<RequestQueue>>,
    /// Lease 请求队列
    lease_queue: Arc<Mutex<RequestQueue>>,
    /// 管理请求队列
    mgmt_queue: Arc<Mutex<RequestQueue>>,
    /// 数据通道池
    data_channels: Vec<Arc<TransportChannel>>,
    /// Lease 通道
    lease_channel: Arc<TransportChannel>,
    /// 管理通道
    mgmt_channel: Arc<TransportChannel>,
    /// Volume 路由表
    volume_router: Arc<RwLock<HashMap<u64, VolumeInfo>>>,
    /// Lease 表 ((volume_id, inode) -> LeaseInfo)
    leases: Arc<RwLock<HashMap<(u64, u64), LeaseInfo>>>,
    /// 熔断器
    breaker: Arc<CircuitBreaker>,
    /// 拓扑管理器
    topology_manager: Arc<ClusterTopologyManager>,
    /// 网络客户端 (可选，用于真实网络发送)
    net_client: Option<Arc<PowerFuseNetClient>>,
    /// 请求等待者映射 (request_id -> oneshot sender)
    response_waiters: Arc<Mutex<VolumeResponseWaiters>>,
    /// 后台处理器运行状态
    background_running: Arc<Mutex<bool>>,
    /// 事件通知器
    notify: Arc<tokio::sync::Notify>,
}

impl VolumeClient {
    pub fn new(config: VolumeClientConfig, topology_manager: Arc<ClusterTopologyManager>) -> Self {
        // 创建多个数据通道组成通道池
        let data_channels = vec![
            Arc::new(TransportChannel::new(config.data_channel.clone())),
            Arc::new(TransportChannel::new(ChannelConfig {
                channel_id: config.data_channel.channel_id + 1,
                ..config.data_channel.clone()
            })),
        ];

        Self {
            breaker: Arc::new(CircuitBreaker::new(config.circuit_breaker_config.clone())),
            data_queue: Arc::new(Mutex::new(RequestQueue::new(config.queue_max_size))),
            lease_queue: Arc::new(Mutex::new(RequestQueue::new(100))),
            mgmt_queue: Arc::new(Mutex::new(RequestQueue::new(100))),
            data_channels,
            lease_channel: Arc::new(TransportChannel::new(config.lease_channel.clone())),
            mgmt_channel: Arc::new(TransportChannel::new(config.mgmt_channel.clone())),
            volume_router: Arc::new(RwLock::new(HashMap::new())),
            leases: Arc::new(RwLock::new(HashMap::new())),
            state: Arc::new(Mutex::new(VolumeClientState::Init)),
            response_waiters: Arc::new(Mutex::new(HashMap::new())),
            background_running: Arc::new(Mutex::new(false)),
            notify: Arc::new(tokio::sync::Notify::new()),
            config,
            topology_manager,
            net_client: None,
        }
    }

    /// 设置网络客户端
    pub fn set_net_client(&mut self, client: Arc<PowerFuseNetClient>) {
        self.net_client = Some(client);
    }

    /// 获取网络客户端引用
    pub fn net_client(&self) -> Option<&Arc<PowerFuseNetClient>> {
        self.net_client.as_ref()
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

    /// 解析请求等待者
    pub fn resolve_waiter(&self, request_id: &RequestId, result: ClientResult<RequestResult>) {
        let sender = {
            let mut waiters = self.response_waiters.lock().unwrap();
            waiters.remove(request_id)
        };
        if let Some(sender) = sender {
            let _ = sender.send(result);
        }
    }

    /// 提交数据请求并等待响应
    pub async fn submit_data_request_and_wait(
        &self,
        context: RequestContext,
        volume_id: u64,
        timeout: Duration,
    ) -> ClientResult<RequestResult> {
        let request_id = context.request_id.clone();
        let (tx, rx) = oneshot::channel();

        self.register_waiter(request_id.clone(), tx);
        self.submit_data_request(context, volume_id)
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

    /// 提交 Lease 请求并等待响应
    pub async fn submit_lease_request_and_wait(
        &self,
        context: RequestContext,
        volume_id: u64,
        timeout: Duration,
    ) -> ClientResult<RequestResult> {
        let request_id = context.request_id.clone();
        let (tx, rx) = oneshot::channel();

        self.register_waiter(request_id.clone(), tx);
        self.submit_lease_request(context, volume_id)
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

    /// 提交管理请求并等待响应
    pub async fn submit_mgmt_request_and_wait(
        &self,
        context: RequestContext,
        volume_id: u64,
        timeout: Duration,
    ) -> ClientResult<RequestResult> {
        let request_id = context.request_id.clone();
        let (tx, rx) = oneshot::channel();

        self.register_waiter(request_id.clone(), tx);
        self.submit_management_request(context, volume_id)
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

    /// 初始化
    pub fn init(&self) {
        self.sync_volume_router();
        self.cleanup_expired_leases();
        *self.state.lock().unwrap() = VolumeClientState::Ready;
        log::info!("VolumeClient: Initialized");
    }

    fn sync_volume_router(&self) {
        let topology = self.topology_manager.get_topology();
        let mut router = self.volume_router.write().unwrap();
        *router = topology.volumes.clone();
    }

    /// 直接设置 Volume 信息（用于测试或动态路由更新）
    pub fn set_volume_info(&self, volume_id: u64, addr: String) {
        let mut router = self.volume_router.write().unwrap();
        router.insert(
            volume_id,
            VolumeInfo::new(volume_id, format!("vol-{}", volume_id), addr),
        );
    }

    fn cleanup_expired_leases(&self) {
        let mut leases = self.leases.write().unwrap();
        leases.retain(|_, lease| {
            if lease.is_expired() && lease.state == LeaseState::Acquired {
                lease.state = LeaseState::Expired;
                log::warn!("VolumeClient: Lease expired for volume");
                false
            } else {
                true
            }
        });
    }

    /// 获取状态
    pub fn state(&self) -> VolumeClientState {
        *self.state.lock().unwrap()
    }

    /// 获取 Volume 信息
    pub fn get_volume(&self, volume_id: u64) -> Option<VolumeInfo> {
        let router = self.volume_router.read().unwrap();
        router.get(&volume_id).cloned()
    }

    /// 获取指定 inode 的 Lease 状态
    pub fn get_lease_state(&self, volume_id: u64, inode: u64) -> LeaseState {
        let leases = self.leases.read().unwrap();
        leases
            .get(&(volume_id, inode))
            .map(|l| l.state)
            .unwrap_or(LeaseState::None)
    }

    /// 检查指定 inode 的 Lease 是否有效
    pub fn has_valid_lease(&self, volume_id: u64, inode: u64) -> bool {
        let leases = self.leases.read().unwrap();
        leases
            .get(&(volume_id, inode))
            .map(|l| l.is_valid())
            .unwrap_or(false)
    }

    /// 检查指定 volume 是否有任意有效 Lease（粗粒度预检查）
    pub fn has_valid_lease_for_volume(&self, volume_id: u64) -> bool {
        let leases = self.leases.read().unwrap();
        leases
            .iter()
            .any(|(&(vid, _), lease)| vid == volume_id && lease.is_valid())
    }

    /// 提交数据请求 (读/写共享队列)
    pub fn submit_data_request(
        &self,
        context: RequestContext,
        volume_id: u64,
    ) -> Result<(), String> {
        if self.state() != VolumeClientState::Ready && self.state() != VolumeClientState::Processing
        {
            return Err(format!("Client not ready: {:?}", self.state()));
        }

        if !self.breaker.is_available() {
            return Err("Circuit breaker is open".to_string());
        }

        // 检查写请求的 Lease（粗粒度 volume 级预检查）
        if matches!(context.kind, RequestKind::Write) && !self.has_valid_lease_for_volume(volume_id)
        {
            return Err("No valid lease for write request".to_string());
        }

        let req = crate::meta_shard_client::PendingRequest {
            context,
            shard_id: volume_id,
            enqueued_at: Instant::now(),
        };

        let mut queue = self.data_queue.lock().unwrap();
        queue.enqueue(req)?;

        *self.state.lock().unwrap() = VolumeClientState::Processing;
        self.notify.notify_one();
        Ok(())
    }

    /// 提交 Lease 请求
    pub fn submit_lease_request(
        &self,
        context: RequestContext,
        volume_id: u64,
    ) -> Result<(), String> {
        if self.state() == VolumeClientState::Closed {
            return Err("Client is closed".to_string());
        }

        let req = crate::meta_shard_client::PendingRequest {
            context,
            shard_id: volume_id,
            enqueued_at: Instant::now(),
        };

        let mut queue = self.lease_queue.lock().unwrap();
        queue.enqueue(req)?;
        self.notify.notify_one();
        Ok(())
    }

    /// 提交管理请求
    pub fn submit_management_request(
        &self,
        context: RequestContext,
        volume_id: u64,
    ) -> Result<(), String> {
        if self.state() == VolumeClientState::Closed {
            return Err("Client is closed".to_string());
        }

        let req = crate::meta_shard_client::PendingRequest {
            context,
            shard_id: volume_id,
            enqueued_at: Instant::now(),
        };

        let mut queue = self.mgmt_queue.lock().unwrap();
        queue.enqueue(req)?;
        self.notify.notify_one();
        Ok(())
    }

    /// 获取下一个数据请求
    pub fn next_data_request(&self) -> Option<crate::meta_shard_client::PendingRequest> {
        let mut queue = self.data_queue.lock().unwrap();
        queue.dequeue()
    }

    /// 获取下一个 Lease 请求
    pub fn next_lease_request(&self) -> Option<crate::meta_shard_client::PendingRequest> {
        let mut queue = self.lease_queue.lock().unwrap();
        queue.dequeue()
    }

    /// 获取下一个管理请求
    pub fn next_mgmt_request(&self) -> Option<crate::meta_shard_client::PendingRequest> {
        let mut queue = self.mgmt_queue.lock().unwrap();
        queue.dequeue()
    }

    /// 获取可用的数据通道
    pub fn get_available_data_channel(&self) -> Option<&TransportChannel> {
        self.data_channels
            .iter()
            .find(|c| c.can_accept())
            .map(|v| &**v)
    }

    /// 记录成功
    pub fn record_success(&self, request_id: &RequestId, kind: RequestKind) {
        match kind {
            RequestKind::Read | RequestKind::Write => {
                // 从通道移除
                for channel in &self.data_channels {
                    channel.remove_request(request_id);
                }
                self.breaker.record_success();
            }
            RequestKind::Lease => {
                self.lease_channel.remove_request(request_id);
            }
            RequestKind::Management => {
                self.mgmt_channel.remove_request(request_id);
            }
            _ => {}
        }
    }

    /// 记录失败
    pub fn record_failure(&self, request_id: &RequestId, kind: RequestKind) {
        match kind {
            RequestKind::Read | RequestKind::Write => {
                for channel in &self.data_channels {
                    channel.remove_request(request_id);
                }
                self.breaker.record_failure();
            }
            _ => {}
        }
    }

    /// 更新指定 inode 的 Lease
    pub fn update_lease(
        &self,
        volume_id: u64,
        inode: u64,
        token: String,
        duration: std::time::Duration,
    ) {
        let mut leases = self.leases.write().unwrap();
        let key = (volume_id, inode);
        let lease = leases
            .entry(key)
            .or_insert_with(|| LeaseInfo::new(token.clone(), duration));
        lease.renew(duration);
    }

    /// 释放指定 inode 的 Lease
    pub fn release_lease(&self, volume_id: u64, inode: u64) {
        let mut leases = self.leases.write().unwrap();
        let key = (volume_id, inode);
        if let Some(lease) = leases.get_mut(&key) {
            lease.release();
        }
    }

    /// 处理 Volume 状态变更 - 完整的请求重放逻辑
    pub fn handle_volume_change(&self, volume_id: u64, new_info: VolumeInfo) {
        log::warn!("VolumeClient: Volume {} changed", volume_id);

        // 步骤 1: 暂停客户端
        *self.state.lock().unwrap() = VolumeClientState::Suspended;
        log::info!("VolumeClient: Suspended for volume change");

        // 步骤 2: 保存受影响 volume 的 pending 请求
        let mut data_queue = self.data_queue.lock().unwrap();
        let mut lease_queue = self.lease_queue.lock().unwrap();
        let mut mgmt_queue = self.mgmt_queue.lock().unwrap();

        let mut affected_data_requests = Vec::new();
        let mut unaffected_data_requests = Vec::new();
        let mut affected_lease_requests = Vec::new();
        let mut unaffected_lease_requests = Vec::new();
        let mut affected_mgmt_requests = Vec::new();
        let mut unaffected_mgmt_requests = Vec::new();

        // 分离数据队列
        while let Some(req) = data_queue.dequeue() {
            if req.shard_id == volume_id {
                affected_data_requests.push(req);
            } else {
                unaffected_data_requests.push(req);
            }
        }

        // 分离 lease 队列
        while let Some(req) = lease_queue.dequeue() {
            if req.shard_id == volume_id {
                affected_lease_requests.push(req);
            } else {
                unaffected_lease_requests.push(req);
            }
        }

        // 分离管理队列
        while let Some(req) = mgmt_queue.dequeue() {
            if req.shard_id == volume_id {
                affected_mgmt_requests.push(req);
            } else {
                unaffected_mgmt_requests.push(req);
            }
        }

        log::info!(
            "VolumeClient: Found {} affected data, {} affected lease, {} affected mgmt requests",
            affected_data_requests.len(),
            affected_lease_requests.len(),
            affected_mgmt_requests.len()
        );

        // 步骤 3: 更新路由表
        {
            let mut router = self.volume_router.write().unwrap();
            let old_info = router.insert(volume_id, new_info.clone());
            log::info!(
                "VolumeClient: Updated volume {} (was: {:?})",
                volume_id,
                old_info.map(|i| i.addr)
            );
        }

        // 步骤 4: 将未受影响的请求重新入队
        for req in unaffected_data_requests {
            data_queue.enqueue(req).ok();
        }
        for req in unaffected_lease_requests {
            lease_queue.enqueue(req).ok();
        }
        for req in unaffected_mgmt_requests {
            mgmt_queue.enqueue(req).ok();
        }

        // 步骤 5: 将受影响的请求重新入队（准备重试）
        for mut req in affected_data_requests {
            req.context.state = crate::request_state::RequestState::Init;
            data_queue.enqueue(req).ok();
        }
        for mut req in affected_lease_requests {
            req.context.state = crate::request_state::RequestState::Init;
            lease_queue.enqueue(req).ok();
        }
        for mut req in affected_mgmt_requests {
            req.context.state = crate::request_state::RequestState::Init;
            mgmt_queue.enqueue(req).ok();
        }

        // 步骤 6: 恢复客户端
        *self.state.lock().unwrap() = VolumeClientState::Ready;
        self.notify.notify_one();
        log::info!(
            "VolumeClient: Resumed with queues: data={}, lease={}, mgmt={}",
            data_queue.len(),
            lease_queue.len(),
            mgmt_queue.len()
        );
    }

    /// 获取队列统计
    pub fn queue_stats(&self) -> (usize, usize, usize) {
        let data_len = self.data_queue.lock().unwrap().len();
        let lease_len = self.lease_queue.lock().unwrap().len();
        let mgmt_len = self.mgmt_queue.lock().unwrap().len();
        (data_len, lease_len, mgmt_len)
    }

    /// 关闭
    pub fn close(&self) {
        self.stop_background_processor();
        *self.state.lock().unwrap() = VolumeClientState::Closed;
        log::info!("VolumeClient: Closed");
    }

    /// 启动后台处理循环（事件驱动）
    pub fn start_background_processor(&self) {
        let mut running = self.background_running.lock().unwrap();
        if *running {
            return;
        }
        *running = true;

        let data_queue = self.data_queue.clone();
        let lease_queue = self.lease_queue.clone();
        let mgmt_queue = self.mgmt_queue.clone();
        let data_channels = self.data_channels.clone();
        let lease_channel = self.lease_channel.clone();
        let mgmt_channel = self.mgmt_channel.clone();
        let breaker = self.breaker.clone();
        let net_client = self.net_client.clone();
        let background_running = self.background_running.clone();
        let state = self.state.clone();
        let volume_router = self.volume_router.clone();
        let leases = self.leases.clone();
        let response_waiters = self.response_waiters.clone();
        let notify = self.notify.clone();

        tokio::spawn(async move {
            log::info!("VolumeClient: Background processor started");

            loop {
                if !*background_running.lock().unwrap() {
                    break;
                }

                let current_state = *state.lock().unwrap();
                if current_state == VolumeClientState::Closed
                    || current_state == VolumeClientState::Suspended
                {
                    notify.notified().await;
                    continue;
                }

                // 尝试处理请求
                let processed = process_volume_available_requests(
                    &data_queue,
                    &lease_queue,
                    &mgmt_queue,
                    &data_channels,
                    &lease_channel,
                    &mgmt_channel,
                    &breaker,
                    &net_client,
                    &volume_router,
                    &leases,
                    &response_waiters,
                )
                .await;

                if processed {
                    continue;
                }

                // 队列为空，等待通知
                notify.notified().await;
            }

            log::info!("VolumeClient: Background processor stopped");
        });
    }

    /// 停止后台处理循环
    pub fn stop_background_processor(&self) {
        let mut running = self.background_running.lock().unwrap();
        *running = false;
        self.notify.notify_one();
        log::info!("VolumeClient: Stopping background processor...");
    }

    /// 异步处理数据请求 (真实网络发送)
    pub async fn process_data_request(&self, req: PendingRequest) -> ClientResult<RequestResult> {
        process_data_request_internal(
            req,
            &self.net_client,
            &self.breaker,
            &self.data_channels,
            &self.volume_router,
            &self.leases,
            &self.response_waiters,
        )
        .await
    }

    /// 异步处理 Lease 请求
    pub async fn process_lease_request(&self, req: PendingRequest) -> ClientResult<RequestResult> {
        process_lease_request_internal(
            req,
            &self.net_client,
            &self.breaker,
            &self.lease_channel,
            &self.volume_router,
            &self.response_waiters,
        )
        .await
    }

    /// 异步处理管理请求
    pub async fn process_mgmt_request(&self, req: PendingRequest) -> ClientResult<RequestResult> {
        process_mgmt_request_internal(
            req,
            &self.net_client,
            &self.breaker,
            &self.mgmt_channel,
            &self.volume_router,
            &self.response_waiters,
        )
        .await
    }

    /// 处理下一个数据请求
    pub async fn process_next_data_request(&self) -> Option<ClientResult<RequestResult>> {
        let req = self.next_data_request()?;
        Some(self.process_data_request(req).await)
    }

    /// 处理下一个 Lease 请求
    pub async fn process_next_lease_request(&self) -> Option<ClientResult<RequestResult>> {
        let req = self.next_lease_request()?;
        Some(self.process_lease_request(req).await)
    }

    /// 处理下一个管理请求
    pub async fn process_next_mgmt_request(&self) -> Option<ClientResult<RequestResult>> {
        let req = self.next_mgmt_request()?;
        Some(self.process_mgmt_request(req).await)
    }
}

// ---- 自由函数版本（供后台处理器使用） ----

/// 内部解析 waiter
fn resolve_waiter_for(
    request_id: &RequestId,
    result: ClientResult<RequestResult>,
    response_waiters: &Arc<Mutex<VolumeResponseWaiters>>,
) {
    let sender = {
        let mut waiters = response_waiters.lock().unwrap();
        waiters.remove(request_id)
    };
    if let Some(sender) = sender {
        let _ = sender.send(result);
    }
}

/// 处理 Volume 队列中所有可用的请求，返回是否处理了至少一个
#[allow(clippy::too_many_arguments)]
async fn process_volume_available_requests(
    data_queue: &Arc<Mutex<RequestQueue>>,
    lease_queue: &Arc<Mutex<RequestQueue>>,
    mgmt_queue: &Arc<Mutex<RequestQueue>>,
    data_channels: &[Arc<TransportChannel>],
    lease_channel: &Arc<TransportChannel>,
    mgmt_channel: &Arc<TransportChannel>,
    breaker: &Arc<CircuitBreaker>,
    net_client: &Option<Arc<PowerFuseNetClient>>,
    volume_router: &Arc<RwLock<HashMap<u64, VolumeInfo>>>,
    leases: &Arc<RwLock<HashMap<(u64, u64), LeaseInfo>>>,
    response_waiters: &Arc<Mutex<VolumeResponseWaiters>>,
) -> bool {
    // Lease 通道优先
    if lease_channel.can_accept() {
        let next_req = {
            let mut queue = lease_queue.lock().unwrap();
            queue.dequeue()
        };
        if let Some(req) = next_req {
            let _ = process_lease_request_internal(
                req,
                net_client,
                breaker,
                lease_channel,
                volume_router,
                response_waiters,
            )
            .await;
            return true;
        }
    }

    // Mgmt 通道次优先
    if mgmt_channel.can_accept() {
        let next_req = {
            let mut queue = mgmt_queue.lock().unwrap();
            queue.dequeue()
        };
        if let Some(req) = next_req {
            let _ = process_mgmt_request_internal(
                req,
                net_client,
                breaker,
                mgmt_channel,
                volume_router,
                response_waiters,
            )
            .await;
            return true;
        }
    }

    // 数据通道池
    if breaker.is_available() && data_channels.iter().any(|c| c.can_accept()) {
        let next_req = {
            let mut queue = data_queue.lock().unwrap();
            queue.dequeue()
        };
        if let Some(req) = next_req {
            let _ = process_data_request_internal(
                req,
                net_client,
                breaker,
                data_channels,
                volume_router,
                leases,
                response_waiters,
            )
            .await;
            return true;
        }
    }

    false
}

/// 数据请求处理（自由函数版本）
async fn process_data_request_internal(
    req: PendingRequest,
    net_client: &Option<Arc<PowerFuseNetClient>>,
    breaker: &Arc<CircuitBreaker>,
    _data_channels: &[Arc<TransportChannel>],
    volume_router: &Arc<RwLock<HashMap<u64, VolumeInfo>>>,
    leases: &Arc<RwLock<HashMap<(u64, u64), LeaseInfo>>>,
    response_waiters: &Arc<Mutex<VolumeResponseWaiters>>,
) -> ClientResult<RequestResult> {
    let request_id = req.context.request_id.clone();
    let kind = req.context.kind;
    let msg_type = req.context.msg_type;
    let body = req.context.payload.clone();

    if !breaker.is_available() {
        let result = Err(ClientError::CircuitOpen);
        resolve_waiter_for(&request_id, result.clone(), response_waiters);
        return result;
    }

    let nc = net_client.as_ref().ok_or_else(|| {
        let err = ClientError::NoNetworkClient;
        resolve_waiter_for(&request_id, Err(err.clone()), response_waiters);
        err
    })?;

    let volume = {
        let router = volume_router.read().unwrap();
        router.get(&req.shard_id).cloned()
    }
    .ok_or_else(|| {
        let err = ClientError::VolumeNotFound(req.shard_id);
        resolve_waiter_for(&request_id, Err(err.clone()), response_waiters);
        err
    })?;

    let vol_client = nc
        .get_volume_client(&volume.addr, nc.volume_net_port())
        .await
        .map_err(|e| {
            let err = ClientError::Network(format!("Failed to get volume client: {}", e));
            resolve_waiter_for(&request_id, Err(err.clone()), response_waiters);
            err
        })?;

    let resolved_msg_type = powerfs_net::MsgType::from_u16(msg_type).unwrap_or(match kind {
        RequestKind::Read => powerfs_net::MsgType::ReadNeedleBlob,
        RequestKind::Write => powerfs_net::MsgType::WriteNeedle,
        _ => powerfs_net::MsgType::ReadNeedleBlob,
    });

    let result = match kind {
        RequestKind::Read => {
            let msg = vol_client.send_request(resolved_msg_type, &body, &[]).await;
            match msg {
                Ok(resp) if resp.is_ok() => {
                    breaker.record_success();
                    Ok(RequestResult::success_with_payload(
                        request_id.clone(),
                        resp.body,
                        resp.data,
                    ))
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
        RequestKind::Write => {
            // Decode TLV body to extract file_key (inode) for per-inode lease check
            let file_key = TlvDecoder::new(&body).next_u64(FieldId::Name).unwrap_or(0);
            let (has_lease, lease_token) = {
                let lease_map = leases.read().unwrap();
                match lease_map.get(&(req.shard_id, file_key)) {
                    Some(lease) if lease.is_valid() => (true, Some(lease.token.clone())),
                    _ => (false, None),
                }
            };
            if !has_lease {
                let err = ClientError::NoValidLease;
                resolve_waiter_for(&request_id, Err(err.clone()), response_waiters);
                return Err(err);
            }

            // Inject lease_token into TLV body for server-side validation
            let final_body = if let Some(token) = lease_token {
                // Parse existing TLV fields and rebuild with lease_token appended
                let mut dec = TlvDecoder::new(&body);
                let mut enc = TlvEncoder::new();

                // Read volume_id (Ino)
                if let Ok(ino) = dec.next_u64(FieldId::Ino) {
                    let _ = enc.add_u64(FieldId::Ino, ino);
                }
                // Read file_key (Name)
                let _ = dec.next_u64(FieldId::Name).map(|_| {
                    let _ = enc.add_u64(FieldId::Name, file_key);
                });
                // Read data (DataLen)
                if let Ok(data) = dec.next_bytes(FieldId::DataLen) {
                    let _ = enc.add_bytes(FieldId::DataLen, &data);
                }
                // Add lease token for server-side validation
                let _ = enc.add_string(FieldId::LeaseToken, &token);
                // Add client_id for lease holder validation
                let _ = enc.add_string(FieldId::ClientId, "fuse-client");
                enc.into_bytes()
            } else {
                body.clone()
            };

            let msg = vol_client
                .send_request(resolved_msg_type, &final_body, &[])
                .await;
            match msg {
                Ok(resp) if resp.is_ok() => {
                    breaker.record_success();
                    Ok(RequestResult::success_with_payload(
                        request_id.clone(),
                        resp.body,
                        resp.data,
                    ))
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
    };

    resolve_waiter_for(&request_id, result.clone(), response_waiters);
    result
}

/// Lease 请求处理（自由函数版本）
async fn process_lease_request_internal(
    req: PendingRequest,
    net_client: &Option<Arc<PowerFuseNetClient>>,
    breaker: &Arc<CircuitBreaker>,
    _lease_channel: &Arc<TransportChannel>,
    volume_router: &Arc<RwLock<HashMap<u64, VolumeInfo>>>,
    response_waiters: &Arc<Mutex<VolumeResponseWaiters>>,
) -> ClientResult<RequestResult> {
    let request_id = req.context.request_id.clone();
    let body = req.context.payload.clone();

    if !breaker.is_available() {
        let result = Err(ClientError::CircuitOpen);
        resolve_waiter_for(&request_id, result.clone(), response_waiters);
        return result;
    }

    let nc = net_client.as_ref().ok_or_else(|| {
        let err = ClientError::NoNetworkClient;
        resolve_waiter_for(&request_id, Err(err.clone()), response_waiters);
        err
    })?;

    let volume = {
        let router = volume_router.read().unwrap();
        router.get(&req.shard_id).cloned()
    }
    .ok_or_else(|| {
        let err = ClientError::VolumeNotFound(req.shard_id);
        resolve_waiter_for(&request_id, Err(err.clone()), response_waiters);
        err
    })?;

    let vol_client = nc
        .get_volume_client(&volume.addr, nc.volume_net_port())
        .await
        .map_err(|e| {
            let err = ClientError::Network(format!("Failed to get volume client: {}", e));
            resolve_waiter_for(&request_id, Err(err.clone()), response_waiters);
            err
        })?;

    let msg_type = powerfs_net::MsgType::from_u16(req.context.msg_type)
        .unwrap_or(powerfs_net::MsgType::ReadNeedleBlob);

    let result = vol_client.send_request(msg_type, &body, &[]).await;

    let final_result = match result {
        Ok(resp) if resp.is_ok() => Ok(RequestResult::success_with_payload(
            request_id.clone(),
            resp.body,
            resp.data,
        )),
        Ok(resp) => Err(ClientError::Server(format!(
            "Server error: {}",
            resp.header.status
        ))),
        Err(e) => Err(ClientError::from_net_error(e)),
    };

    resolve_waiter_for(&request_id, final_result.clone(), response_waiters);
    final_result
}

/// 管理请求处理（自由函数版本）
async fn process_mgmt_request_internal(
    req: PendingRequest,
    net_client: &Option<Arc<PowerFuseNetClient>>,
    breaker: &Arc<CircuitBreaker>,
    _mgmt_channel: &Arc<TransportChannel>,
    volume_router: &Arc<RwLock<HashMap<u64, VolumeInfo>>>,
    response_waiters: &Arc<Mutex<VolumeResponseWaiters>>,
) -> ClientResult<RequestResult> {
    let request_id = req.context.request_id.clone();
    let body = req.context.payload.clone();

    if !breaker.is_available() {
        let result = Err(ClientError::CircuitOpen);
        resolve_waiter_for(&request_id, result.clone(), response_waiters);
        return result;
    }

    let nc = net_client.as_ref().ok_or_else(|| {
        let err = ClientError::NoNetworkClient;
        resolve_waiter_for(&request_id, Err(err.clone()), response_waiters);
        err
    })?;

    let volume = {
        let router = volume_router.read().unwrap();
        router.get(&req.shard_id).cloned()
    }
    .ok_or_else(|| {
        let err = ClientError::VolumeNotFound(req.shard_id);
        resolve_waiter_for(&request_id, Err(err.clone()), response_waiters);
        err
    })?;

    let vol_client = nc
        .get_volume_client(&volume.addr, nc.volume_net_port())
        .await
        .map_err(|e| {
            let err = ClientError::Network(format!("Failed to get volume client: {}", e));
            resolve_waiter_for(&request_id, Err(err.clone()), response_waiters);
            err
        })?;

    let msg_type = powerfs_net::MsgType::from_u16(req.context.msg_type)
        .unwrap_or(powerfs_net::MsgType::StatFs);

    let result = vol_client.send_request(msg_type, &body, &[]).await;

    let final_result = match result {
        Ok(resp) if resp.is_ok() => Ok(RequestResult::success_with_payload(
            request_id.clone(),
            resp.body,
            resp.data,
        )),
        Ok(resp) => Err(ClientError::Server(format!(
            "Server error: {}",
            resp.header.status
        ))),
        Err(e) => Err(ClientError::from_net_error(e)),
    };

    resolve_waiter_for(&request_id, final_result.clone(), response_waiters);
    final_result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_identity::ClientIdentity;
    use crate::topology::ClusterTopology;

    fn create_test_volume_client() -> (VolumeClient, Arc<ClusterTopologyManager>) {
        let topology_manager = Arc::new(ClusterTopologyManager::new());

        let mut topology = ClusterTopology::new();
        topology.volumes.insert(
            1,
            VolumeInfo::new(1, "/vol1".to_string(), "127.0.0.1:9344".to_string()),
        );
        topology_manager.update_topology(topology);

        let config = VolumeClientConfig::default();
        let client = VolumeClient::new(config, topology_manager.clone());
        client.init();

        (client, topology_manager)
    }

    fn create_test_context(kind: RequestKind) -> RequestContext {
        let identity = ClientIdentity::new();
        RequestContext::new(identity, kind, 0x0001, vec![])
    }

    #[test]
    fn test_initialization() {
        let (client, _) = create_test_volume_client();
        assert_eq!(client.state(), VolumeClientState::Ready);
        assert!(client.get_volume(1).is_some());
        assert!(client.get_volume(2).is_none());
    }

    #[test]
    fn test_submit_read_request() {
        let (client, _) = create_test_volume_client();
        let ctx = create_test_context(RequestKind::Read);

        assert!(client.submit_data_request(ctx, 1).is_ok());

        let (data_len, _, _) = client.queue_stats();
        assert_eq!(data_len, 1);
    }

    #[test]
    fn test_submit_write_without_lease() {
        let (client, _) = create_test_volume_client();
        let ctx = create_test_context(RequestKind::Write);

        // 写请求需要 Lease
        let result = client.submit_data_request(ctx, 1);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("No valid lease"));
    }

    #[test]
    fn test_submit_write_with_lease() {
        let (client, _) = create_test_volume_client();

        // 先获取 Lease
        client.update_lease(
            1,
            100,
            "token-1".to_string(),
            std::time::Duration::from_secs(30),
        );
        assert!(client.has_valid_lease_for_volume(1));

        let ctx = create_test_context(RequestKind::Write);
        assert!(client.submit_data_request(ctx, 1).is_ok());
    }

    #[test]
    fn test_lease_management() {
        let (client, _) = create_test_volume_client();

        // 初始无 Lease
        assert_eq!(client.get_lease_state(1, 100), LeaseState::None);
        assert!(!client.has_valid_lease(1, 100));

        // 获取 Lease
        client.update_lease(
            1,
            100,
            "token-1".to_string(),
            std::time::Duration::from_secs(30),
        );
        assert_eq!(client.get_lease_state(1, 100), LeaseState::Acquired);
        assert!(client.has_valid_lease(1, 100));

        // 释放 Lease
        client.release_lease(1, 100);
        assert_eq!(client.get_lease_state(1, 100), LeaseState::Released);
        assert!(!client.has_valid_lease(1, 100));
    }

    #[test]
    fn test_queue_processing() {
        let (client, _) = create_test_volume_client();

        // 提交多个请求
        for _ in 0..3 {
            let ctx = create_test_context(RequestKind::Read);
            client.submit_data_request(ctx, 1).unwrap();
        }

        let (data_len, _, _) = client.queue_stats();
        assert_eq!(data_len, 3);

        // 出队
        let req = client.next_data_request();
        assert!(req.is_some());

        let (data_len, _, _) = client.queue_stats();
        assert_eq!(data_len, 2);
    }

    #[test]
    fn test_different_queue_types() {
        let (client, _) = create_test_volume_client();

        // 数据请求
        let ctx1 = create_test_context(RequestKind::Read);
        client.submit_data_request(ctx1, 1).unwrap();

        // Lease 请求
        let ctx2 = create_test_context(RequestKind::Lease);
        client.submit_lease_request(ctx2, 1).unwrap();

        // 管理请求
        let ctx3 = create_test_context(RequestKind::Management);
        client.submit_management_request(ctx3, 1).unwrap();

        let (data_len, lease_len, mgmt_len) = client.queue_stats();
        assert_eq!(data_len, 1);
        assert_eq!(lease_len, 1);
        assert_eq!(mgmt_len, 1);

        // 分别出队
        assert!(client.next_data_request().is_some());
        assert!(client.next_lease_request().is_some());
        assert!(client.next_mgmt_request().is_some());
    }

    #[test]
    fn test_volume_change() {
        let (client, _) = create_test_volume_client();

        assert_eq!(client.get_volume(1).unwrap().addr, "127.0.0.1:9344");

        // 处理 Volume 变更
        let new_info = VolumeInfo::new(1, "/vol1-new".to_string(), "10.0.0.1:9344".to_string());
        client.handle_volume_change(1, new_info);

        assert_eq!(client.get_volume(1).unwrap().addr, "10.0.0.1:9344");
        assert_eq!(client.state(), VolumeClientState::Ready);
    }

    #[test]
    fn test_circuit_breaker() {
        let (client, _) = create_test_volume_client();

        // 触发熔断
        for _ in 0..5 {
            let id = RequestId::new();
            client.record_failure(&id, RequestKind::Read);
        }

        let ctx = create_test_context(RequestKind::Read);
        let result = client.submit_data_request(ctx, 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_closed_client() {
        let (client, _) = create_test_volume_client();
        client.close();

        let ctx = create_test_context(RequestKind::Read);
        let result = client.submit_data_request(ctx, 1);
        assert!(result.is_err());
    }
}
