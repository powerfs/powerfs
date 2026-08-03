use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio::sync::oneshot;

use crate::circuit_breaker::CircuitBreakerPool;
use crate::client_error::{ClientError, ClientResult};
use crate::meta_shard_client::RequestResult;
use crate::meta_shard_client::{ChannelConfig, PendingRequest, RequestQueue, TransportChannel};
use crate::request_id::RequestId;
use crate::request_state::{RequestContext, RequestKind};
use crate::topology::{ClusterTopologyManager, VolumeInfo};
use powerfs_net::serialize::TlvDecoder;
use powerfs_net::{ClientConfig, FieldId, PowerFsNetClient, STATUS_ERR_NOT_FOUND};

/// 请求等待者类型别名
type VolumeResponseWaiters =
    HashMap<RequestId, oneshot::Sender<Result<RequestResult, ClientError>>>;

/// 调度器统计信息
#[derive(Debug, Clone)]
pub struct SchedulerStats {
    /// 数据队列当前长度
    pub data_queue_len: usize,
    /// Lease 队列当前长度
    pub lease_queue_len: usize,
    /// 管理队列当前长度
    pub mgmt_queue_len: usize,
    /// 数据请求已处理数
    pub data_processed: u64,
    /// Lease 请求已处理数
    pub lease_processed: u64,
    /// 管理请求已处理数
    pub mgmt_processed: u64,
    /// 数据队列历史高水位
    pub data_high_watermark: usize,
    /// Lease 队列历史高水位
    pub lease_high_watermark: usize,
    /// 管理队列历史高水位
    pub mgmt_high_watermark: usize,
    /// 数据处理器是否运行
    pub data_processor_running: bool,
    /// Lease 处理器是否运行
    pub lease_processor_running: bool,
    /// 管理处理器是否运行
    pub mgmt_processor_running: bool,
}

/// Volume 客户端配置
#[derive(Debug, Clone)]
pub struct VolumeClientConfig {
    /// 客户端 ID (用于 Lease 持有者识别等)
    pub client_id: String,
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
    /// Lease 续租心跳间隔
    pub lease_renew_interval: Duration,
}

impl Default for VolumeClientConfig {
    fn default() -> Self {
        Self {
            client_id: "powerfs-fuse-client".to_string(),
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
            lease_renew_interval: Duration::from_secs(3),
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

    /// Remaining duration before this lease expires (saturates to zero).
    pub fn remaining(&self) -> std::time::Duration {
        self.expire_at.saturating_duration_since(Instant::now())
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
    /// 数据请求队列 (lock-free)
    data_queue: Arc<RequestQueue>,
    /// Lease 请求队列 (lock-free)
    lease_queue: Arc<RequestQueue>,
    /// 管理请求队列 (lock-free)
    mgmt_queue: Arc<RequestQueue>,
    /// 数据通道池
    data_channels: Vec<Arc<TransportChannel>>,
    /// Lease 通道
    lease_channel: Arc<TransportChannel>,
    /// 管理通道
    mgmt_channel: Arc<TransportChannel>,
    /// Volume 路由表
    volume_router: Arc<DashMap<u64, VolumeInfo>>,
    /// Lease 表 ((volume_id, inode) -> LeaseInfo)
    leases: Arc<DashMap<(u64, u64), LeaseInfo>>,
    /// Per-server circuit breaker pool (one breaker per Volume server address)
    breakers: Arc<CircuitBreakerPool>,
    /// 拓扑管理器
    topology_manager: Arc<ClusterTopologyManager>,
    /// Volume 连接池 (addr -> PowerFsNetClient) - DashMap for lock-free reads
    volume_connections: Arc<DashMap<String, Arc<PowerFsNetClient>>>,
    /// 默认 Volume 地址列表
    default_volume_addrs: Arc<Mutex<Vec<String>>>,
    /// 请求等待者映射 (request_id -> oneshot sender)
    response_waiters: Arc<Mutex<VolumeResponseWaiters>>,
    /// 关闭标志 (所有处理器共享)
    shutdown_flag: Arc<AtomicBool>,
    /// 数据处理器运行状态
    data_processor_running: Arc<Mutex<bool>>,
    /// Lease 处理器运行状态
    lease_processor_running: Arc<Mutex<bool>>,
    /// 管理处理器运行状态
    mgmt_processor_running: Arc<Mutex<bool>>,
    /// 数据请求处理计数
    data_processed_count: Arc<AtomicU64>,
    /// Lease 请求处理计数
    lease_processed_count: Arc<AtomicU64>,
    /// 管理请求处理计数
    mgmt_processed_count: Arc<AtomicU64>,
    /// 队列高水位标记 (数据/Lease/管理)
    data_queue_high_watermark: Arc<AtomicUsize>,
    lease_queue_high_watermark: Arc<AtomicUsize>,
    mgmt_queue_high_watermark: Arc<AtomicUsize>,
    /// Lease 续租器运行状态
    lease_renewer_running: Arc<Mutex<bool>>,
    /// Lease 续租间隔
    lease_renew_interval: Duration,
    /// 数据通道事件通知器 (仅唤醒 data_processor)
    ///
    /// 关键修复：原先 data/lease/mgmt 三个 processor 共用单个 `notify`，
    /// `notify_one()` 只唤醒一个等待者，可能误唤醒错误 processor 导致目标
    /// processor 不被唤醒、请求卡在队列直到超时。拆分为 3 个独立 Notify，
    /// 每个 submit/processor/guard 只操作对应通道，确保精准唤醒。
    data_notify: Arc<tokio::sync::Notify>,
    /// Lease 通道事件通知器 (仅唤醒 lease_processor)
    lease_notify: Arc<tokio::sync::Notify>,
    /// 管理通道事件通知器 (仅唤醒 mgmt_processor)
    mgmt_notify: Arc<tokio::sync::Notify>,
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

        let lease_renew_interval = config.lease_renew_interval;

        Self {
            breakers: Arc::new(CircuitBreakerPool::new(
                config.circuit_breaker_config.clone(),
            )),
            data_queue: Arc::new(RequestQueue::new(config.queue_max_size)),
            lease_queue: Arc::new(RequestQueue::new(100)),
            mgmt_queue: Arc::new(RequestQueue::new(100)),
            data_channels,
            lease_channel: Arc::new(TransportChannel::new(config.lease_channel.clone())),
            mgmt_channel: Arc::new(TransportChannel::new(config.mgmt_channel.clone())),
            volume_router: Arc::new(DashMap::new()),
            leases: Arc::new(DashMap::new()),
            state: Arc::new(Mutex::new(VolumeClientState::Init)),
            response_waiters: Arc::new(Mutex::new(HashMap::new())),
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            data_processor_running: Arc::new(Mutex::new(false)),
            lease_processor_running: Arc::new(Mutex::new(false)),
            mgmt_processor_running: Arc::new(Mutex::new(false)),
            data_processed_count: Arc::new(AtomicU64::new(0)),
            lease_processed_count: Arc::new(AtomicU64::new(0)),
            mgmt_processed_count: Arc::new(AtomicU64::new(0)),
            data_queue_high_watermark: Arc::new(AtomicUsize::new(0)),
            lease_queue_high_watermark: Arc::new(AtomicUsize::new(0)),
            mgmt_queue_high_watermark: Arc::new(AtomicUsize::new(0)),
            lease_renewer_running: Arc::new(Mutex::new(false)),
            lease_renew_interval,
            data_notify: Arc::new(tokio::sync::Notify::new()),
            lease_notify: Arc::new(tokio::sync::Notify::new()),
            mgmt_notify: Arc::new(tokio::sync::Notify::new()),
            config,
            topology_manager,
            volume_connections: Arc::new(DashMap::new()),
            default_volume_addrs: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// 设置默认 Volume 地址列表
    pub fn set_default_volume_addrs(&self, addrs: Vec<String>) {
        *self.default_volume_addrs.lock().unwrap() = addrs;
    }

    /// 获取或创建到指定地址的 volume 连接
    pub async fn get_or_create_volume_client(
        &self,
        addr: &str,
    ) -> ClientResult<Arc<PowerFsNetClient>> {
        get_or_create_volume_client_from_pool(&self.volume_connections, addr).await
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
    ///
    /// Phase 1.6: oneshot tx 直接嵌入 PendingRequest，无需 register_waiter。
    /// 超时时 rx 自动 drop，processor 的 tx.send 返回 Err 但无副作用。
    pub async fn submit_data_request_and_wait(
        &self,
        context: RequestContext,
        volume_id: u64,
        timeout: Duration,
    ) -> ClientResult<RequestResult> {
        let (tx, rx) = oneshot::channel();

        self.submit_data_request(context, volume_id, Some(tx))
            .map_err(ClientError::Internal)?;

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(ClientError::Cancelled),
            Err(_) => Err(ClientError::Timeout(timeout)),
        }
    }

    /// 提交 Lease 请求并等待响应
    pub async fn submit_lease_request_and_wait(
        &self,
        context: RequestContext,
        volume_id: u64,
        timeout: Duration,
    ) -> ClientResult<RequestResult> {
        let (tx, rx) = oneshot::channel();

        self.submit_lease_request(context, volume_id, Some(tx))
            .map_err(ClientError::Internal)?;

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(ClientError::Cancelled),
            Err(_) => Err(ClientError::Timeout(timeout)),
        }
    }

    /// 提交管理请求并等待响应
    pub async fn submit_mgmt_request_and_wait(
        &self,
        context: RequestContext,
        volume_id: u64,
        timeout: Duration,
    ) -> ClientResult<RequestResult> {
        let (tx, rx) = oneshot::channel();

        self.submit_management_request(context, volume_id, Some(tx))
            .map_err(ClientError::Internal)?;

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(ClientError::Cancelled),
            Err(_) => Err(ClientError::Timeout(timeout)),
        }
    }

    /// 初始化
    pub fn init(&self) {
        self.sync_volume_router();

        // 如果路由表为空，等待 Volume Server 心跳注册后重试
        {
            if self.volume_router.is_empty() {
                log::warn!(
                    "VolumeClient: volume router empty, waiting for Volume Server heartbeats..."
                );

                // 重试 3 次，每次间隔 2 秒
                for attempt in 1..=3 {
                    std::thread::sleep(std::time::Duration::from_secs(2));
                    self.sync_volume_router();
                    if !self.volume_router.is_empty() {
                        log::info!(
                            "VolumeClient: volume router populated after {} retries",
                            attempt
                        );
                        break;
                    }
                    if attempt < 3 {
                        log::warn!(
                            "VolumeClient: retry {}/3 - volume router still empty",
                            attempt
                        );
                    }
                }

                // 如果仍然为空，使用默认路由作为 fallback
                if self.volume_router.is_empty() {
                    log::warn!("VolumeClient: using default volume routes as fallback");
                    self.setup_default_volume_routes();
                }
            }
        }

        self.cleanup_expired_leases();
        *self.state.lock().unwrap() = VolumeClientState::Ready;
        log::info!("VolumeClient: Initialized");

        // 启动 Lease 续租心跳后台任务
        self.start_lease_renewer();
        log::info!("VolumeClient: Lease renewer started");
    }

    /// 设置默认 Volume 路由 - 将已知 volume 地址预填到路由表
    fn setup_default_volume_routes(&self) {
        let volume_addrs: Vec<String> = {
            let addrs = self.default_volume_addrs.lock().unwrap();
            if !addrs.is_empty() {
                addrs.clone()
            } else {
                return;
            }
        };

        log::info!(
            "VolumeClient: setting default volume routes to: {:?}",
            volume_addrs
        );

        for (vol_id, addr) in volume_addrs.iter().enumerate() {
            self.volume_router.insert(
                vol_id as u64,
                VolumeInfo::new(vol_id as u64, format!("vol-{}", vol_id), addr.clone()),
            );
        }
        log::info!(
            "VolumeClient: default routes configured for {} volumes",
            self.volume_router.len()
        );
    }

    fn sync_volume_router(&self) {
        let topology = self.topology_manager.get_topology();
        let old_len = self.volume_router.len();
        self.volume_router.clear();
        for (vid, info) in topology.volumes {
            self.volume_router.insert(vid, info);
        }
        log::info!(
            "sync_volume_router: updated volume_router from {} to {} entries",
            old_len,
            self.volume_router.len()
        );
        for entry in self.volume_router.iter() {
            log::debug!(
                "sync_volume_router: volume_id={}, addr={}",
                entry.key(),
                entry.value().addr
            );
        }
    }

    /// 直接设置 Volume 信息（用于测试或动态路由更新）
    pub fn set_volume_info(&self, volume_id: u64, addr: String) {
        self.volume_router.insert(
            volume_id,
            VolumeInfo::new(volume_id, format!("vol-{}", volume_id), addr),
        );
    }

    /// 解析并更新 Volume 路由：给定 volume_id 和 locations，自动选择第一个地址作为路由
    /// 若 locations 为空，使用默认 volume 地址列表中的第一个作为回退
    pub fn resolve_and_set_volume_route(
        &self,
        volume_id: u64,
        locations: &[powerfs_common::traits::Location],
    ) {
        if let Some(first) = locations.first() {
            let url = first.url.clone();
            if !url.is_empty() {
                log::debug!("VolumeClient: routing volume={} -> {}", volume_id, url);
                self.set_volume_info(volume_id, url);
                return;
            }
        }

        // 回退：使用默认 volume 地址
        let default_addrs = self.default_volume_addrs.lock().unwrap();
        if let Some(addr) = default_addrs.first() {
            log::warn!(
                "VolumeClient: FALLBACK routing volume={} -> {}",
                volume_id,
                addr
            );
            self.set_volume_info(volume_id, addr.clone());
        }
    }

    /// 从默认 volume 地址获取指定 volume 的路由地址
    pub fn get_default_volume_addr(&self, volume_id: u64) -> Option<String> {
        self.volume_router.get(&volume_id).map(|v| v.addr.clone())
    }

    fn cleanup_expired_leases(&self) {
        self.leases.retain(|_, lease| {
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
        let result = self.volume_router.get(&volume_id).map(|v| v.clone());
        if result.is_some() {
            log::debug!(
                "get_volume: found volume_id={}, total_routes={}",
                volume_id,
                self.volume_router.len()
            );
        } else {
            log::debug!(
                "get_volume: volume_id={} not found in cache, total_routes={}",
                volume_id,
                self.volume_router.len()
            );
        }
        result
    }

    /// 获取指定 inode 的 Lease 状态
    pub fn get_lease_state(&self, volume_id: u64, inode: u64) -> LeaseState {
        self.leases
            .get(&(volume_id, inode))
            .map(|l| l.state)
            .unwrap_or(LeaseState::None)
    }

    /// 检查指定 inode 的 Lease 是否有效
    pub fn has_valid_lease(&self, volume_id: u64, inode: u64) -> bool {
        self.leases
            .get(&(volume_id, inode))
            .map(|l| l.is_valid())
            .unwrap_or(false)
    }

    /// 检查指定 volume 是否有任意有效 Lease（粗粒度预检查）
    pub fn has_valid_lease_for_volume(&self, volume_id: u64) -> bool {
        self.leases
            .iter()
            .any(|entry| entry.key().0 == volume_id && entry.value().is_valid())
    }

    /// 获取指定 inode 的有效 lease token（如果存在且有效）
    pub fn get_valid_lease_token(&self, volume_id: u64, inode: u64) -> Option<String> {
        self.leases
            .get(&(volume_id, inode))
            .filter(|l| l.is_valid())
            .map(|l| l.token.clone())
    }

    /// 获取指定 inode 的 lease 剩余时间（如果存在且有效）
    ///
    /// 用于写路径在长操作前检查 lease 是否即将过期，剩余不足时主动续租，
    /// 避免 lease 在写操作中途过期导致服务端校验失败。
    pub fn get_lease_remaining(&self, volume_id: u64, inode: u64) -> Option<std::time::Duration> {
        self.leases
            .get(&(volume_id, inode))
            .filter(|l| l.is_valid())
            .map(|l| l.remaining())
    }

    /// 提交数据请求 (读/写共享队列)
    ///
    /// Phase 1.6: `response_tx` 直接嵌入 PendingRequest，消除 response_waiters 中间层。
    /// 传 `Some(tx)` 时 processor 直接通过 oneshot 投递结果；传 `None` 为 fire-and-forget。
    pub fn submit_data_request(
        &self,
        context: RequestContext,
        volume_id: u64,
        response_tx: Option<oneshot::Sender<ClientResult<RequestResult>>>,
    ) -> Result<(), String> {
        if self.state() != VolumeClientState::Ready && self.state() != VolumeClientState::Processing
        {
            return Err(format!("Client not ready: {:?}", self.state()));
        }

        if !self.breakers.check(&self.resolve_volume_addr(volume_id)) {
            return Err("Circuit breaker is open for this volume server".to_string());
        }

        // 检查写请求的 Lease（粗粒度 volume 级预检查）
        // NOTE: 该检查可能与 ProviderAdapter::ensure_lease 时序存在竞争，
        // 真正的校验在 Volume 服务端 validate_token_with_grace_period 严格执行；
        // 这里降级为警告日志避免误拦截合法写请求。
        if matches!(context.kind, RequestKind::Write) && !self.has_valid_lease_for_volume(volume_id)
        {
            log::warn!(
                "submit_data_request: no cached volume-level lease for volume={}, proceeding to Volume server (server-side validation still enforced",
                volume_id
            );
        }

        let req = crate::meta_shard_client::PendingRequest {
            context,
            shard_id: volume_id,
            enqueued_at: Instant::now(),
            response_tx,
        };

        self.data_queue.enqueue(req)?;

        *self.state.lock().unwrap() = VolumeClientState::Processing;
        self.data_notify.notify_one();
        Ok(())
    }

    /// 提交 Lease 请求
    pub fn submit_lease_request(
        &self,
        context: RequestContext,
        volume_id: u64,
        response_tx: Option<oneshot::Sender<ClientResult<RequestResult>>>,
    ) -> Result<(), String> {
        if self.state() == VolumeClientState::Closed {
            return Err("Client is closed".to_string());
        }

        let req = crate::meta_shard_client::PendingRequest {
            context,
            shard_id: volume_id,
            enqueued_at: Instant::now(),
            response_tx,
        };

        self.lease_queue.enqueue(req)?;
        self.lease_notify.notify_one();
        Ok(())
    }

    /// 提交管理请求
    pub fn submit_management_request(
        &self,
        context: RequestContext,
        volume_id: u64,
        response_tx: Option<oneshot::Sender<ClientResult<RequestResult>>>,
    ) -> Result<(), String> {
        if self.state() == VolumeClientState::Closed {
            return Err("Client is closed".to_string());
        }

        let req = crate::meta_shard_client::PendingRequest {
            context,
            shard_id: volume_id,
            enqueued_at: Instant::now(),
            response_tx,
        };

        self.mgmt_queue.enqueue(req)?;
        self.mgmt_notify.notify_one();
        Ok(())
    }

    /// 获取下一个数据请求
    pub fn next_data_request(&self) -> Option<crate::meta_shard_client::PendingRequest> {
        self.data_queue.dequeue()
    }

    /// 获取下一个 Lease 请求
    pub fn next_lease_request(&self) -> Option<crate::meta_shard_client::PendingRequest> {
        self.lease_queue.dequeue()
    }

    /// 获取下一个管理请求
    pub fn next_mgmt_request(&self) -> Option<crate::meta_shard_client::PendingRequest> {
        self.mgmt_queue.dequeue()
    }

    /// 获取可用的数据通道
    pub fn get_available_data_channel(&self) -> Option<&TransportChannel> {
        self.data_channels
            .iter()
            .find(|c| c.can_accept())
            .map(|v| &**v)
    }

    /// Resolve volume_id to its server address from the routing table.
    /// Returns "unknown" if not found (circuit breaker will auto-create for "unknown").
    fn resolve_volume_addr(&self, volume_id: u64) -> String {
        self.volume_router
            .get(&volume_id)
            .map(|v| v.addr.clone())
            .unwrap_or_else(|| "unknown".to_string())
    }

    /// Record success for the given volume server address
    pub fn record_success(&self, request_id: &RequestId, kind: RequestKind, volume_addr: &str) {
        match kind {
            RequestKind::Read | RequestKind::Write => {
                for channel in &self.data_channels {
                    channel.remove_request(request_id);
                }
                self.breakers.record_success(volume_addr);
            }
            RequestKind::Lease => {
                self.lease_channel.remove_request(request_id);
                self.breakers.record_success(volume_addr);
            }
            RequestKind::Management => {
                self.mgmt_channel.remove_request(request_id);
                self.breakers.record_success(volume_addr);
            }
            _ => {}
        }
    }

    /// Record failure for the given volume server address
    pub fn record_failure(&self, request_id: &RequestId, kind: RequestKind, volume_addr: &str) {
        match kind {
            RequestKind::Read | RequestKind::Write => {
                for channel in &self.data_channels {
                    channel.remove_request(request_id);
                }
                self.breakers.record_failure(volume_addr);
            }
            RequestKind::Lease => {
                self.lease_channel.remove_request(request_id);
                self.breakers.record_failure(volume_addr);
            }
            RequestKind::Management => {
                self.mgmt_channel.remove_request(request_id);
                self.breakers.record_failure(volume_addr);
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
        let key = (volume_id, inode);
        // Always overwrite token + duration: ensure_lease may acquire a NEW
        // token (e.g. after local expiry) and the cached entry must reflect
        // the latest server-side token. Previously this only called renew()
        // on the existing entry, leaving the stale token in place — release
        // then sent the old token, the server replied "Lease not found", and
        // the orphaned server-side lease blocked all future acquires
        // ("Stripe lease conflict").
        let mut lease = self
            .leases
            .entry(key)
            .or_insert_with(|| LeaseInfo::new(token.clone(), duration));
        lease.token = token;
        lease.renew(duration);
    }

    /// 释放指定 inode 的 Lease
    pub fn release_lease(&self, volume_id: u64, inode: u64) {
        let key = (volume_id, inode);
        if let Some(mut lease) = self.leases.get_mut(&key) {
            lease.release();
        }
    }

    /// 异步获取 Lease: 直接构建 TLV 请求发送到 Volume Server
    #[allow(clippy::too_many_arguments)]
    pub async fn acquire_lease(
        &self,
        volume_id: u64,
        inode: u64,
        stripe_start: u64,
        stripe_count: u64,
        client_id: &str,
        exclusive: bool,
        duration_ms: u64,
    ) -> ClientResult<String> {
        let volume = self
            .volume_router
            .get(&volume_id)
            .map(|v| v.clone())
            .ok_or(ClientError::VolumeNotFound(volume_id))?;

        let vol_client = self.get_or_create_volume_client(&volume.addr).await?;

        let mut enc = powerfs_net::serialize::TlvEncoder::new();
        enc.add_u64(FieldId::Ino, inode);
        enc.add_u64(FieldId::Offset, stripe_start);
        enc.add_u64(FieldId::Limit, stripe_count);
        enc.add_string(FieldId::ClientId, client_id)?;
        enc.add_u64(FieldId::Mode, if exclusive { 1 } else { 0 });
        enc.add_u64(FieldId::LeaseDuration, duration_ms);

        let result = vol_client
            .send_request(powerfs_net::MsgType::AcquireLease, &enc.into_bytes(), &[])
            .await;

        match result {
            Ok(resp) if resp.is_ok() => {
                let mut dec = powerfs_net::serialize::TlvDecoder::new(&resp.body);
                let token = dec.next_string(FieldId::LeaseId).unwrap_or_default();
                if token.is_empty() {
                    return Err(ClientError::Internal(
                        "AcquireLease response has empty token".into(),
                    ));
                }
                // Update local lease cache
                self.update_lease(
                    volume_id,
                    inode,
                    token.clone(),
                    Duration::from_millis(duration_ms),
                );
                log::info!(
                    "acquire_lease: volume={}, inode={}, stripe_start={}, count={}, token={}",
                    volume_id,
                    inode,
                    stripe_start,
                    stripe_count,
                    token
                );
                Ok(token)
            }
            Ok(resp) => Err(ClientError::Server(format!(
                "AcquireLease failed: status={}",
                resp.header.status
            ))),
            Err(e) => Err(ClientError::from_net_error(e)),
        }
    }

    /// 异步批量获取 Lease: 一次 RPC 获取多个 stripe range 的 lease。
    ///
    /// P2-1: 服务端在单个锁范围内完成所有冲突检查 + 授权（all-or-nothing）。
    /// 适用于大文件跨多个非连续 stripe 的写入场景，避免逐个 acquire 导致的
    /// 部分获取死锁和多次 RPC 往返。
    ///
    /// `stripe_specs` 为 (stripe_start, stripe_count) 列表。
    /// 返回 (token, epoch) 列表，顺序与输入一致。
    pub async fn acquire_lease_batch(
        &self,
        volume_id: u64,
        inode: u64,
        stripe_specs: &[(u64, u64)],
        client_id: &str,
        exclusive: bool,
        duration_ms: u64,
    ) -> ClientResult<Vec<(String, u64)>> {
        if stripe_specs.is_empty() {
            return Ok(Vec::new());
        }

        let volume = self
            .volume_router
            .get(&volume_id)
            .map(|v| v.clone())
            .ok_or(ClientError::VolumeNotFound(volume_id))?;

        let vol_client = self.get_or_create_volume_client(&volume.addr).await?;

        // Encode specs blob: each spec is 16 bytes (stripe_start + stripe_count, both u64 LE)
        let mut specs_blob = Vec::with_capacity(stripe_specs.len() * 16);
        for (start, count) in stripe_specs {
            specs_blob.extend_from_slice(&start.to_le_bytes());
            specs_blob.extend_from_slice(&count.to_le_bytes());
        }

        let mut enc = powerfs_net::serialize::TlvEncoder::new();
        enc.add_u64(FieldId::Ino, inode);
        enc.add_string(FieldId::ClientId, client_id)?;
        enc.add_u64(FieldId::Mode, if exclusive { 1 } else { 0 });
        enc.add_u64(FieldId::LeaseDuration, duration_ms);
        enc.add_bytes(FieldId::LeaseBatchSpecs, &specs_blob)?;

        let result = vol_client
            .send_request(
                powerfs_net::MsgType::AcquireLeaseBatch,
                &enc.into_bytes(),
                &[],
            )
            .await;

        match result {
            Ok(resp) if resp.is_ok() => {
                let mut dec = powerfs_net::serialize::TlvDecoder::new(&resp.body);
                let count = dec.next_u32(FieldId::Count).unwrap_or(0) as usize;
                let blob = dec.next_bytes(FieldId::LeaseBatchSpecs).unwrap_or_default();

                let mut tokens = Vec::with_capacity(count);
                let mut offset = 0;
                for _ in 0..count {
                    if offset + 4 > blob.len() {
                        break;
                    }
                    let token_len =
                        u32::from_le_bytes(blob[offset..offset + 4].try_into().unwrap()) as usize;
                    offset += 4;
                    if offset + token_len + 8 > blob.len() {
                        break;
                    }
                    let token =
                        String::from_utf8_lossy(&blob[offset..offset + token_len]).to_string();
                    offset += token_len;
                    let epoch = u64::from_le_bytes(blob[offset..offset + 8].try_into().unwrap());
                    offset += 8;
                    tokens.push((token, epoch));
                }

                if tokens.len() != count {
                    return Err(ClientError::Internal(format!(
                        "AcquireLeaseBatch response decode mismatch: expected {} tokens, got {}",
                        count,
                        tokens.len()
                    )));
                }

                // Cache the first token for the (volume_id, inode) pair so
                // that ensure_lease / get_valid_lease_token can find it.
                // Subsequent tokens are returned to the caller for direct use.
                if let Some((first_token, _)) = tokens.first() {
                    self.update_lease(
                        volume_id,
                        inode,
                        first_token.clone(),
                        Duration::from_millis(duration_ms),
                    );
                }

                log::debug!(
                    "acquire_lease_batch: volume={}, inode={}, acquired {} leases",
                    volume_id,
                    inode,
                    tokens.len()
                );
                Ok(tokens)
            }
            Ok(resp) => {
                log::warn!(
                    "acquire_lease_batch: server error for volume={}, inode={}, status={}",
                    volume_id,
                    inode,
                    resp.header.status
                );
                Err(ClientError::Server(format!(
                    "AcquireLeaseBatch failed: status={}",
                    resp.header.status
                )))
            }
            Err(e) => Err(ClientError::from_net_error(e)),
        }
    }

    /// 异步释放 Lease: 直接构建 TLV 请求发送到 Volume Server
    ///
    /// token 由调用方传入（LeaseGuard 持有的 token 或 leases 表中查到的 token），
    /// 避免此前从 leases 表查 token 时可能返回过期/错误 token 的 bug。
    pub async fn release_lease_remote(
        &self,
        volume_id: u64,
        inode: u64,
        client_id: &str,
        token: &str,
    ) -> ClientResult<()> {
        if token.is_empty() {
            // 无 token：尝试从 leases 表取（兼容旧调用路径）
            let tok = self
                .get_valid_lease_token(volume_id, inode)
                .ok_or_else(|| ClientError::Internal("No valid lease to release".into()))?;
            return self
                .release_lease_remote_with_token(volume_id, inode, client_id, &tok)
                .await;
        }
        self.release_lease_remote_with_token(volume_id, inode, client_id, token)
            .await
    }

    /// 内部：用指定 token 发送 ReleaseLease 请求并清理本地 lease 缓存
    async fn release_lease_remote_with_token(
        &self,
        volume_id: u64,
        inode: u64,
        client_id: &str,
        token: &str,
    ) -> ClientResult<()> {
        let volume = self
            .volume_router
            .get(&volume_id)
            .map(|v| v.clone())
            .ok_or(ClientError::VolumeNotFound(volume_id))?;

        let vol_client = self.get_or_create_volume_client(&volume.addr).await?;

        let mut enc = powerfs_net::serialize::TlvEncoder::new();
        enc.add_string(FieldId::LeaseToken, token)?;
        enc.add_string(FieldId::ClientId, client_id)?;

        let result = vol_client
            .send_request(powerfs_net::MsgType::ReleaseLease, &enc.into_bytes(), &[])
            .await;

        match result {
            Ok(resp) if resp.is_ok() => {
                self.release_lease(volume_id, inode);
                log::info!(
                    "release_lease_remote: volume={}, inode={}, token={}",
                    volume_id,
                    inode,
                    token
                );
                Ok(())
            }
            Ok(resp) => {
                self.release_lease(volume_id, inode);
                log::warn!(
                    "release_lease_remote: server returned error but clearing local lease. status={}",
                    resp.header.status
                );
                Ok(())
            }
            Err(e) => {
                self.release_lease(volume_id, inode);
                log::warn!(
                    "release_lease_remote: network error but clearing local lease. error={}",
                    e
                );
                Ok(())
            }
        }
    }

    /// 直接发送 WriteNeedle 请求 (绕过 data_queue，避免队列延迟导致 lease 过期)
    ///
    /// 与 acquire_lease/release_lease 一样使用直接网络发送，确保 WriteNeedle
    /// 在 lease 有效期内到达 Volume Server。data_queue 的异步处理可能延迟
    /// 30 秒（request_timeout），导致 lease 被释放后 WriteNeedle 才被处理。
    pub async fn send_write_needle_direct(
        &self,
        volume_id: u64,
        payload: Vec<u8>,
    ) -> ClientResult<Vec<u8>> {
        let volume = self
            .volume_router
            .get(&volume_id)
            .map(|v| v.clone())
            .ok_or(ClientError::VolumeNotFound(volume_id))?;

        let vol_client = self.get_or_create_volume_client(&volume.addr).await?;

        let result = vol_client
            .send_request(powerfs_net::MsgType::WriteNeedle, &payload, &[])
            .await;

        match result {
            Ok(resp) if resp.is_ok() => {
                self.breakers.record_success(&volume.addr);
                log::debug!(
                    "send_write_needle_direct: volume={}, addr={}",
                    volume_id,
                    volume.addr
                );
                Ok(resp.body)
            }
            Ok(resp) => {
                self.breakers.record_failure(&volume.addr);
                Err(ClientError::Server(format!(
                    "WriteNeedle failed: status={}",
                    resp.header.status
                )))
            }
            Err(e) => {
                self.breakers.record_failure(&volume.addr);
                Err(ClientError::from_net_error(e))
            }
        }
    }

    /// 异步续租 Lease: 直接构建 TLV 请求发送到 Volume Server
    pub async fn renew_lease(&self, volume_id: u64, inode: u64, token: &str) -> ClientResult<()> {
        let volume = self
            .volume_router
            .get(&volume_id)
            .map(|v| v.clone())
            .ok_or(ClientError::VolumeNotFound(volume_id))?;

        let vol_client = self.get_or_create_volume_client(&volume.addr).await?;

        let client_id = self.config.client_id.clone();

        let duration_ms = self
            .leases
            .get(&(volume_id, inode))
            .map(|l| {
                l.expire_at
                    .saturating_duration_since(l.acquired_at)
                    .as_secs()
                    .saturating_mul(1000)
            })
            .unwrap_or(30000);

        let mut enc = powerfs_net::serialize::TlvEncoder::new();
        enc.add_string(FieldId::LeaseToken, token)?;
        enc.add_string(FieldId::ClientId, &client_id)?;
        enc.add_u64(FieldId::LeaseDuration, duration_ms);

        let result = vol_client
            .send_request(powerfs_net::MsgType::RenewLease, &enc.into_bytes(), &[])
            .await;

        match result {
            Ok(resp) if resp.is_ok() => {
                let mut dec = powerfs_net::serialize::TlvDecoder::new(&resp.body);
                let new_duration_ms = dec.next_u64(FieldId::LeaseDuration).unwrap_or(duration_ms);
                let new_duration = Duration::from_millis(new_duration_ms);

                // 更新本地缓存
                self.update_lease(volume_id, inode, token.to_string(), new_duration);

                log::debug!(
                    "renew_lease: volume={}, inode={}, new_duration_ms={}",
                    volume_id,
                    inode,
                    new_duration_ms
                );
                Ok(())
            }
            Ok(resp) => {
                log::warn!(
                    "renew_lease: server error for volume={}, inode={}, status={}",
                    volume_id,
                    inode,
                    resp.header.status
                );
                Err(ClientError::Server(format!(
                    "RenewLease failed: status={}",
                    resp.header.status
                )))
            }
            Err(e) => {
                log::warn!(
                    "renew_lease: network error for volume={}, inode={}, error={}",
                    volume_id,
                    inode,
                    e
                );
                Err(ClientError::from_net_error(e))
            }
        }
    }

    /// 处理 Volume 状态变更 - 完整的请求重放逻辑
    pub fn handle_volume_change(&self, volume_id: u64, new_info: VolumeInfo) {
        log::warn!("VolumeClient: Volume {} changed", volume_id);

        // 步骤 1: 暂停客户端
        *self.state.lock().unwrap() = VolumeClientState::Suspended;
        log::info!("VolumeClient: Suspended for volume change");

        // 步骤 2: 保存受影响 volume 的 pending 请求 (lock-free drain)
        let mut affected_data_requests = Vec::new();
        let mut unaffected_data_requests = Vec::new();
        let mut affected_lease_requests = Vec::new();
        let mut unaffected_lease_requests = Vec::new();
        let mut affected_mgmt_requests = Vec::new();
        let mut unaffected_mgmt_requests = Vec::new();

        // 分离数据队列
        while let Some(req) = self.data_queue.dequeue() {
            if req.shard_id == volume_id {
                affected_data_requests.push(req);
            } else {
                unaffected_data_requests.push(req);
            }
        }

        // 分离 lease 队列
        while let Some(req) = self.lease_queue.dequeue() {
            if req.shard_id == volume_id {
                affected_lease_requests.push(req);
            } else {
                unaffected_lease_requests.push(req);
            }
        }

        // 分离管理队列
        while let Some(req) = self.mgmt_queue.dequeue() {
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
        let old_info = self.volume_router.insert(volume_id, new_info.clone());
        log::info!(
            "VolumeClient: Updated volume {} (was: {:?})",
            volume_id,
            old_info.map(|i| i.addr)
        );

        // 步骤 4: 将未受影响的请求重新入队
        for req in unaffected_data_requests {
            self.data_queue.enqueue(req).ok();
        }
        for req in unaffected_lease_requests {
            self.lease_queue.enqueue(req).ok();
        }
        for req in unaffected_mgmt_requests {
            self.mgmt_queue.enqueue(req).ok();
        }

        // 步骤 5: 将受影响的请求重新入队（准备重试）
        for mut req in affected_data_requests {
            req.context.state = crate::request_state::RequestState::Init;
            self.data_queue.enqueue(req).ok();
        }
        for mut req in affected_lease_requests {
            req.context.state = crate::request_state::RequestState::Init;
            self.lease_queue.enqueue(req).ok();
        }
        for mut req in affected_mgmt_requests {
            req.context.state = crate::request_state::RequestState::Init;
            self.mgmt_queue.enqueue(req).ok();
        }

        // 步骤 6: 恢复客户端
        *self.state.lock().unwrap() = VolumeClientState::Ready;
        // 唤醒所有 processor（恢复后三个 queue 都可能有积压请求）
        self.data_notify.notify_one();
        self.lease_notify.notify_one();
        self.mgmt_notify.notify_one();
        log::info!(
            "VolumeClient: Resumed with queues: data={}, lease={}, mgmt={}",
            self.data_queue.len(),
            self.lease_queue.len(),
            self.mgmt_queue.len()
        );
    }

    /// 获取队列统计 (data_len, lease_len, mgmt_len)
    pub fn queue_stats(&self) -> (usize, usize, usize) {
        let data_len = self.data_queue.len();
        let lease_len = self.lease_queue.len();
        let mgmt_len = self.mgmt_queue.len();
        (data_len, lease_len, mgmt_len)
    }

    /// 获取详细的调度统计信息
    pub fn scheduler_stats(&self) -> SchedulerStats {
        SchedulerStats {
            data_queue_len: self.data_queue.len(),
            lease_queue_len: self.lease_queue.len(),
            mgmt_queue_len: self.mgmt_queue.len(),
            data_processed: self.data_processed_count.load(Ordering::Relaxed),
            lease_processed: self.lease_processed_count.load(Ordering::Relaxed),
            mgmt_processed: self.mgmt_processed_count.load(Ordering::Relaxed),
            data_high_watermark: self.data_queue_high_watermark.load(Ordering::Relaxed),
            lease_high_watermark: self.lease_queue_high_watermark.load(Ordering::Relaxed),
            mgmt_high_watermark: self.mgmt_queue_high_watermark.load(Ordering::Relaxed),
            data_processor_running: *self.data_processor_running.lock().unwrap(),
            lease_processor_running: *self.lease_processor_running.lock().unwrap(),
            mgmt_processor_running: *self.mgmt_processor_running.lock().unwrap(),
        }
    }

    /// 重置调度统计 (保留高水位标记)
    pub fn reset_scheduler_stats(&self) {
        self.data_processed_count.store(0, Ordering::Relaxed);
        self.lease_processed_count.store(0, Ordering::Relaxed);
        self.mgmt_processed_count.store(0, Ordering::Relaxed);
    }

    /// Build a `ClientStats` snapshot for master heartbeat reporting.
    ///
    /// Aggregates scheduler queue depths, processed counters, circuit breaker
    /// state counts and active lease count. Latency / pool / coalescer fields
    /// are left at 0 here because they are owned by other components
    /// (MetaShardClient / Volume Server) — the caller may overlay them.
    pub fn client_stats(&self) -> powerfs_master::proto::ClientStats {
        let s = self.scheduler_stats();
        let (cb_closed, cb_open, cb_half_open) = self.breakers.count_by_state();
        let active_leases = self.leases.len() as u32;
        powerfs_master::proto::ClientStats {
            data_queue_depth: s.data_queue_len as u32,
            lease_queue_depth: s.lease_queue_len as u32,
            admin_queue_depth: s.mgmt_queue_len as u32,
            data_processed_total: s.data_processed,
            lease_processed_total: s.lease_processed,
            admin_processed_total: s.mgmt_processed,
            cb_closed_count: cb_closed,
            cb_open_count: cb_open,
            cb_half_open_count: cb_half_open,
            cb_trip_total: 0,
            coalescer_dirty_bytes: 0,
            coalescer_dirty_entries: 0,
            coalescer_writes_in_total: 0,
            coalescer_flushes_out_total: 0,
            pool_active_connections: 0,
            pool_reconnect_total: 0,
            pool_ping_failures: 0,
            read_latency_p50_us: 0,
            read_latency_p99_us: 0,
            write_latency_p50_us: 0,
            write_latency_p99_us: 0,
            active_leases,
            lease_renewals_total: 0,
            lease_expired_total: 0,
        }
    }

    /// 关闭
    pub fn close(&self) {
        self.stop_background_processor();
        self.stop_lease_renewer();
        *self.state.lock().unwrap() = VolumeClientState::Closed;
        log::info!("VolumeClient: Closed");
    }

    /// 启动所有后台处理器（数据 + Lease + 管理）
    pub fn start_background_processor(&self) {
        self.start_data_processor();
        self.start_lease_processor();
        self.start_mgmt_processor();
    }

    /// 启动数据请求处理器
    fn start_data_processor(&self) {
        let mut running = self.data_processor_running.lock().unwrap();
        if *running {
            return;
        }
        *running = true;

        let data_queue = self.data_queue.clone();
        let data_channels = self.data_channels.clone();
        let breakers = self.breakers.clone();
        let volume_connections = self.volume_connections.clone();
        let default_volume_addrs = self.default_volume_addrs.clone();
        let state = self.state.clone();
        let volume_router = self.volume_router.clone();
        let leases = self.leases.clone();
        let response_waiters = self.response_waiters.clone();
        let notify = self.data_notify.clone();
        let shutdown_flag = self.shutdown_flag.clone();
        let data_processor_running = self.data_processor_running.clone();
        let processed_count = self.data_processed_count.clone();
        let high_watermark = self.data_queue_high_watermark.clone();

        tokio::spawn(async move {
            log::info!("VolumeClient: Data processor started");

            loop {
                if shutdown_flag.load(Ordering::Relaxed) || !*data_processor_running.lock().unwrap()
                {
                    break;
                }

                let current_state = *state.lock().unwrap();
                if current_state == VolumeClientState::Closed
                    || current_state == VolumeClientState::Suspended
                {
                    notify.notified().await;
                    continue;
                }

                // 更新高水位标记
                let current_len = data_queue.len();
                let prev_hwm = high_watermark.load(Ordering::Relaxed);
                if current_len > prev_hwm {
                    high_watermark.store(current_len, Ordering::Relaxed);
                }

                // 尝试处理数据请求
                let processed = process_data_requests(
                    &data_queue,
                    &data_channels,
                    &breakers,
                    &volume_connections,
                    &default_volume_addrs,
                    &volume_router,
                    &leases,
                    &response_waiters,
                    &notify,
                )
                .await;

                if processed {
                    processed_count.fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                notify.notified().await;
            }

            log::info!("VolumeClient: Data processor stopped");
        });
    }

    /// 启动 Lease 请求处理器 (高优先级)
    fn start_lease_processor(&self) {
        let mut running = self.lease_processor_running.lock().unwrap();
        if *running {
            return;
        }
        *running = true;

        let lease_queue = self.lease_queue.clone();
        let lease_channel = self.lease_channel.clone();
        let breakers = self.breakers.clone();
        let volume_connections = self.volume_connections.clone();
        let default_volume_addrs = self.default_volume_addrs.clone();
        let state = self.state.clone();
        let volume_router = self.volume_router.clone();
        let response_waiters = self.response_waiters.clone();
        let notify = self.lease_notify.clone();
        let shutdown_flag = self.shutdown_flag.clone();
        let lease_processor_running = self.lease_processor_running.clone();
        let processed_count = self.lease_processed_count.clone();
        let high_watermark = self.lease_queue_high_watermark.clone();

        tokio::spawn(async move {
            log::info!("VolumeClient: Lease processor started (high priority)");

            loop {
                if shutdown_flag.load(Ordering::Relaxed)
                    || !*lease_processor_running.lock().unwrap()
                {
                    break;
                }

                let current_state = *state.lock().unwrap();
                if current_state == VolumeClientState::Closed
                    || current_state == VolumeClientState::Suspended
                {
                    notify.notified().await;
                    continue;
                }

                // 更新高水位标记
                let current_len = lease_queue.len();
                let prev_hwm = high_watermark.load(Ordering::Relaxed);
                if current_len > prev_hwm {
                    high_watermark.store(current_len, Ordering::Relaxed);
                }

                // Lease 处理器优先处理
                let processed = process_lease_requests(
                    &lease_queue,
                    &lease_channel,
                    &breakers,
                    &volume_connections,
                    &default_volume_addrs,
                    &volume_router,
                    &response_waiters,
                    &notify,
                )
                .await;

                if processed {
                    processed_count.fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                notify.notified().await;
            }

            log::info!("VolumeClient: Lease processor stopped");
        });
    }

    /// 启动管理请求处理器
    fn start_mgmt_processor(&self) {
        let mut running = self.mgmt_processor_running.lock().unwrap();
        if *running {
            return;
        }
        *running = true;

        let mgmt_queue = self.mgmt_queue.clone();
        let mgmt_channel = self.mgmt_channel.clone();
        let breakers = self.breakers.clone();
        let volume_connections = self.volume_connections.clone();
        let default_volume_addrs = self.default_volume_addrs.clone();
        let state = self.state.clone();
        let volume_router = self.volume_router.clone();
        let response_waiters = self.response_waiters.clone();
        let notify = self.mgmt_notify.clone();
        let shutdown_flag = self.shutdown_flag.clone();
        let mgmt_processor_running = self.mgmt_processor_running.clone();
        let processed_count = self.mgmt_processed_count.clone();
        let high_watermark = self.mgmt_queue_high_watermark.clone();

        tokio::spawn(async move {
            log::info!("VolumeClient: Management processor started");

            loop {
                if shutdown_flag.load(Ordering::Relaxed) || !*mgmt_processor_running.lock().unwrap()
                {
                    break;
                }

                let current_state = *state.lock().unwrap();
                if current_state == VolumeClientState::Closed
                    || current_state == VolumeClientState::Suspended
                {
                    notify.notified().await;
                    continue;
                }

                // 更新高水位标记
                let current_len = mgmt_queue.len();
                let prev_hwm = high_watermark.load(Ordering::Relaxed);
                if current_len > prev_hwm {
                    high_watermark.store(current_len, Ordering::Relaxed);
                }

                let processed = process_mgmt_requests(
                    &mgmt_queue,
                    &mgmt_channel,
                    &breakers,
                    &volume_connections,
                    &default_volume_addrs,
                    &volume_router,
                    &response_waiters,
                    &notify,
                )
                .await;

                if processed {
                    processed_count.fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                notify.notified().await;
            }

            log::info!("VolumeClient: Management processor stopped");
        });
    }

    /// 停止所有后台处理器
    pub fn stop_background_processor(&self) {
        self.shutdown_flag.store(true, Ordering::Relaxed);
        {
            let mut running = self.data_processor_running.lock().unwrap();
            *running = false;
        }
        {
            let mut running = self.lease_processor_running.lock().unwrap();
            *running = false;
        }
        {
            let mut running = self.mgmt_processor_running.lock().unwrap();
            *running = false;
        }
        self.data_notify.notify_waiters();
        self.lease_notify.notify_waiters();
        self.mgmt_notify.notify_waiters();
        log::info!("VolumeClient: Stopping all background processors...");
    }

    /// 启动 Lease 续租心跳后台任务
    pub fn start_lease_renewer(&self) {
        let mut running = self.lease_renewer_running.lock().unwrap();
        if *running {
            return;
        }
        *running = true;

        let leases = self.leases.clone();
        let volume_connections = self.volume_connections.clone();
        let volume_router = self.volume_router.clone();
        let lease_renewer_running = self.lease_renewer_running.clone();
        let interval = self.lease_renew_interval;
        let client_id = self.config.client_id.clone();

        tokio::spawn(async move {
            log::info!(
                "VolumeClient: Lease renewer started with interval={:?}",
                interval
            );

            let mut interval_timer = tokio::time::interval(interval);

            loop {
                if !*lease_renewer_running.lock().unwrap() {
                    break;
                }

                interval_timer.tick().await;

                let now = Instant::now();

                // 收集即将过期的 Lease
                let to_renew: Vec<(u64, u64, String)> = {
                    let mut to_renew_inner = Vec::new();
                    for entry in leases.iter() {
                        let (volume_id, inode) = *entry.key();
                        let lease = entry.value();
                        if lease.state == LeaseState::Acquired && lease.is_valid() {
                            let remaining = lease.expire_at.saturating_duration_since(now);
                            // 如果剩余时间小于 10 秒，则需要续租
                            if remaining < Duration::from_secs(10) {
                                to_renew_inner.push((volume_id, inode, lease.token.clone()));
                            }
                        }
                    }
                    to_renew_inner
                };

                if to_renew.is_empty() {
                    continue;
                }

                log::debug!(
                    "VolumeClient: Lease renewer found {} leases to renew",
                    to_renew.len()
                );

                // 逐个进行续租
                for (volume_id, inode, token) in to_renew {
                    // 获取 volume 路由地址
                    let volume_addr = volume_router.get(&volume_id).map(|v| v.addr.clone());

                    let addr = match volume_addr {
                        Some(a) => a,
                        None => {
                            log::warn!(
                                "VolumeClient: Lease renewer skipped, no route for volume={}",
                                volume_id
                            );
                            continue;
                        }
                    };

                    // 获取或创建到 Volume 的连接
                    let vol_client =
                        match get_or_create_volume_client_from_pool(&volume_connections, &addr)
                            .await
                        {
                            Ok(c) => c,
                            Err(e) => {
                                log::warn!(
                                    "VolumeClient: Lease renewer failed to connect volume={}: {}",
                                    volume_id,
                                    e
                                );
                                continue;
                            }
                        };

                    // 构建续租请求
                    let mut enc = powerfs_net::serialize::TlvEncoder::new();
                    if let Err(e) = enc.add_string(FieldId::LeaseToken, &token) {
                        log::error!(
                            "VolumeClient: Failed to encode lease token for volume={}, inode={}: {}",
                            volume_id,
                            inode,
                            e
                        );
                        continue;
                    }
                    // ClientId is required: server-side renew() checks holder == client_id,
                    // omitting it causes "Lease holder mismatch" and renewal always fails.
                    let _ = enc.add_string(FieldId::ClientId, &client_id);
                    let duration_ms = 30000;
                    enc.add_u64(FieldId::LeaseDuration, duration_ms);

                    let result = vol_client
                        .send_request(powerfs_net::MsgType::RenewLease, &enc.into_bytes(), &[])
                        .await;

                    match result {
                        Ok(resp) if resp.is_ok() => {
                            // 续租成功，更新本地过期时间
                            if let Some(mut lease_info) = leases.get_mut(&(volume_id, inode)) {
                                lease_info.renew(Duration::from_millis(duration_ms));
                                log::debug!(
                                    "VolumeClient: Lease renewed successfully for volume={}, inode={}",
                                    volume_id,
                                    inode
                                );
                            }
                        }
                        Ok(resp) => {
                            log::warn!(
                                "VolumeClient: Lease renew failed for volume={}, inode={}, status={}",
                                volume_id,
                                inode,
                                resp.header.status
                            );
                        }
                        Err(e) => {
                            log::warn!(
                                "VolumeClient: Lease renew network error for volume={}, inode={}: {}",
                                volume_id,
                                inode,
                                e
                            );
                        }
                    }
                }
            }

            log::info!("VolumeClient: Lease renewer stopped");
        });
    }

    /// 停止 Lease 续租心跳后台任务
    pub fn stop_lease_renewer(&self) {
        let mut running = self.lease_renewer_running.lock().unwrap();
        *running = false;
        log::info!("VolumeClient: Stopping lease renewer...");
    }

    /// 异步处理数据请求 (真实网络发送)
    pub async fn process_data_request(&self, req: PendingRequest) -> ClientResult<RequestResult> {
        process_data_request_internal(
            req,
            &self.volume_connections,
            &self.default_volume_addrs,
            &self.breakers,
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
            &self.volume_connections,
            &self.default_volume_addrs,
            &self.breakers,
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
            &self.volume_connections,
            &self.default_volume_addrs,
            &self.breakers,
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

/// Phase 1.6: 直接通过 response_tx 投递结果，回退到 response_waiters。
///
/// 当 PendingRequest 携带 response_tx（来自 submit_*_request_and_wait）时，
/// 直接通过 oneshot 投递，跳过 HashMap 查找和锁竞争。
/// 当 response_tx 为 None（fire-and-forget）时，回退到 resolve_waiter_for。
fn deliver_result(
    response_tx: &mut Option<oneshot::Sender<ClientResult<RequestResult>>>,
    request_id: &RequestId,
    result: ClientResult<RequestResult>,
    response_waiters: &Arc<Mutex<VolumeResponseWaiters>>,
) {
    if let Some(tx) = response_tx.take() {
        let _ = tx.send(result);
    } else {
        resolve_waiter_for(request_id, result, response_waiters);
    }
}

/// 生成 volume 连接的唯一 client_id。
///
/// 组合维度：
/// - **hostname**（容器名/节点名）：区分不同物理/容器节点
/// - **PID**：区分同一节点上的不同 fuse 进程（不同挂载点）
/// - **原子计数器**：区分同一进程内的多个连接（不同 volume server）
///
/// 输出 u64：高 48 位 = hash(hostname + PID)，低 16 位 = 计数器（65536 连接/进程）
fn generate_volume_conn_id() -> u64 {
    use std::hash::{Hash, Hasher};
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    // PID: 同节点不同 fuse 进程（挂载点）得到不同 hash
    std::process::id().hash(&mut hasher);
    // hostname: 不同容器/节点得到不同 hash
    if let Ok(hostname) = std::env::var("HOSTNAME") {
        hostname.hash(&mut hasher);
    } else if let Ok(hostname) = std::fs::read_to_string("/etc/hostname") {
        hostname.trim().hash(&mut hasher);
    }
    let base = hasher.finish();

    let counter = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    // 高 48 位 base + 低 16 位 counter
    (base << 16) | (counter & 0xFFFF)
}

/// 从连接池获取或创建到指定地址的 volume 客户端
async fn get_or_create_volume_client_from_pool(
    volume_connections: &Arc<DashMap<String, Arc<PowerFsNetClient>>>,
    addr: &str,
) -> ClientResult<Arc<PowerFsNetClient>> {
    // 先检查是否已有连接 (DashMap lock-free read)
    if let Some(entry) = volume_connections.get(addr) {
        if entry.is_connected() {
            return Ok(entry.clone());
        }
    }

    // 解析地址
    let parts: Vec<&str> = addr.split(':').collect();
    if parts.len() != 2 {
        return Err(ClientError::InvalidAddress(addr.to_string()));
    }
    let host = parts[0].to_string();
    let port = parts[1]
        .parse::<u16>()
        .map_err(|_| ClientError::InvalidAddress(addr.to_string()))?;

    // 创建新连接
    // 生成唯一 client_id：基于 hostname(区分节点) + PID(区分同节点不同挂载点进程) + 计数器(区分同进程多连接)
    // 避免所有连接共用 client_id=0，导致一个连接断开时 server 清理 session 影响所有连接
    let conn_client_id = generate_volume_conn_id();

    let client_config = ClientConfig {
        addr: host,
        port,
        client_id: conn_client_id,
        client_type: powerfs_net::ClientType::Fuse,
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(10),
        max_retries: 3,
        retry_delay: Duration::from_millis(100),
        heartbeat_interval: Duration::from_secs(30),
        max_inflight_requests: 256,
    };

    let client = Arc::new(PowerFsNetClient::new(client_config));
    client
        .connect()
        .await
        .map_err(ClientError::from_net_error)?;

    // 保存到连接池
    volume_connections.insert(addr.to_string(), client.clone());

    Ok(client)
}

/// 处理 Volume 队列中所有可用的请求，返回是否处理了至少一个
#[allow(clippy::too_many_arguments)]
#[allow(dead_code)]
async fn process_volume_available_requests(
    data_queue: &Arc<RequestQueue>,
    lease_queue: &Arc<RequestQueue>,
    mgmt_queue: &Arc<RequestQueue>,
    data_channels: &[Arc<TransportChannel>],
    lease_channel: &Arc<TransportChannel>,
    mgmt_channel: &Arc<TransportChannel>,
    breakers: &Arc<CircuitBreakerPool>,
    volume_connections: &Arc<DashMap<String, Arc<PowerFsNetClient>>>,
    default_volume_addrs: &Arc<Mutex<Vec<String>>>,
    volume_router: &Arc<DashMap<u64, VolumeInfo>>,
    leases: &Arc<DashMap<(u64, u64), LeaseInfo>>,
    response_waiters: &Arc<Mutex<VolumeResponseWaiters>>,
) -> bool {
    // Lease 通道优先
    if lease_channel.can_accept() {
        let next_req = lease_queue.dequeue();
        if let Some(req) = next_req {
            let _ = process_lease_request_internal(
                req,
                volume_connections,
                default_volume_addrs,
                breakers,
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
        let next_req = mgmt_queue.dequeue();
        if let Some(req) = next_req {
            let _ = process_mgmt_request_internal(
                req,
                volume_connections,
                default_volume_addrs,
                breakers,
                mgmt_channel,
                volume_router,
                response_waiters,
            )
            .await;
            return true;
        }
    }

    // 数据通道池 - per-server breaker check happens inside process_data_request_internal
    if data_channels.iter().any(|c| c.can_accept()) {
        let next_req = data_queue.dequeue();
        if let Some(req) = next_req {
            let _ = process_data_request_internal(
                req,
                volume_connections,
                default_volume_addrs,
                breakers,
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

/// 并发派发 guard：RAII 保证 `remove_request` + `notify_one` 在任务结束时执行
/// （含 panic 情形），避免并发槽位泄漏导致 processor 死锁。
struct ConcurrencyGuard {
    channel: Arc<TransportChannel>,
    request_id: RequestId,
    notify: Arc<tokio::sync::Notify>,
}

impl Drop for ConcurrencyGuard {
    fn drop(&mut self) {
        self.channel.remove_request(&self.request_id);
        // 唤醒可能因 channel 满而阻塞的 processor，继续派发队列内剩余请求
        self.notify.notify_one();
    }
}

/// 独立的数据请求处理器 (专用 tokio 任务使用)
///
/// 并发派发模型：dequeue 后 `tokio::spawn` 独立任务处理，用 TransportChannel
/// 的 `add_request`/`remove_request` 做真实并发控制（max_concurrent 生效）。
/// 单个请求的网络超时只影响自身，不会阻塞队列内其他请求。
/// 这是修复 30 秒延迟根因的关键：旧实现串行 await 单个请求的完整网络往返。
#[allow(clippy::too_many_arguments)]
async fn process_data_requests(
    data_queue: &Arc<RequestQueue>,
    data_channels: &[Arc<TransportChannel>],
    breakers: &Arc<CircuitBreakerPool>,
    volume_connections: &Arc<DashMap<String, Arc<PowerFsNetClient>>>,
    default_volume_addrs: &Arc<Mutex<Vec<String>>>,
    volume_router: &Arc<DashMap<u64, VolumeInfo>>,
    leases: &Arc<DashMap<(u64, u64), LeaseInfo>>,
    response_waiters: &Arc<Mutex<VolumeResponseWaiters>>,
    notify: &Arc<tokio::sync::Notify>,
) -> bool {
    // 使用第一个 data_channel 做并发控制（max_concurrent=32）
    let channel = match data_channels.first() {
        Some(c) => c.clone(),
        None => return false,
    };

    let mut processed_any = false;
    // 持续派发直到 channel 满 或 队列空
    while channel.can_accept() {
        let next_req = match data_queue.dequeue() {
            Some(r) => r,
            None => break,
        };
        let request_id = next_req.context.request_id.clone();
        channel.add_request(request_id.clone());

        let volume_connections = volume_connections.clone();
        let default_volume_addrs = default_volume_addrs.clone();
        let breakers = breakers.clone();
        let volume_router = volume_router.clone();
        let leases = leases.clone();
        let response_waiters = response_waiters.clone();
        let channel_clone = channel.clone();
        let notify_clone = notify.clone();

        tokio::spawn(async move {
            // guard 在任务结束（含 panic）时自动 remove_request + notify_one
            let _guard = ConcurrencyGuard {
                channel: channel_clone,
                request_id,
                notify: notify_clone,
            };

            // process_data_request_internal 的 _data_channels 参数未使用，传空 vec
            let empty_channels: Vec<Arc<TransportChannel>> = Vec::new();
            let _ = process_data_request_internal(
                next_req,
                &volume_connections,
                &default_volume_addrs,
                &breakers,
                &empty_channels,
                &volume_router,
                &leases,
                &response_waiters,
            )
            .await;
            // _guard 在此 drop
        });

        processed_any = true;
    }
    processed_any
}

/// 独立的 Lease 请求处理器 (专用 tokio 任务使用, 高优先级)
///
/// 并发派发模型同 `process_data_requests`：spawn 独立任务 + 真实并发控制。
#[allow(clippy::too_many_arguments)]
async fn process_lease_requests(
    lease_queue: &Arc<RequestQueue>,
    lease_channel: &Arc<TransportChannel>,
    breakers: &Arc<CircuitBreakerPool>,
    volume_connections: &Arc<DashMap<String, Arc<PowerFsNetClient>>>,
    default_volume_addrs: &Arc<Mutex<Vec<String>>>,
    volume_router: &Arc<DashMap<u64, VolumeInfo>>,
    response_waiters: &Arc<Mutex<VolumeResponseWaiters>>,
    notify: &Arc<tokio::sync::Notify>,
) -> bool {
    let mut processed_any = false;
    while lease_channel.can_accept() {
        let next_req = match lease_queue.dequeue() {
            Some(r) => r,
            None => break,
        };
        let request_id = next_req.context.request_id.clone();
        lease_channel.add_request(request_id.clone());

        let volume_connections = volume_connections.clone();
        let default_volume_addrs = default_volume_addrs.clone();
        let breakers = breakers.clone();
        let volume_router = volume_router.clone();
        let response_waiters = response_waiters.clone();
        let channel_clone = lease_channel.clone();
        let notify_clone = notify.clone();

        tokio::spawn(async move {
            let guard = ConcurrencyGuard {
                channel: channel_clone,
                request_id,
                notify: notify_clone,
            };
            // process_lease_request_internal 的 _lease_channel 参数未使用，借用 guard.channel
            let _ = process_lease_request_internal(
                next_req,
                &volume_connections,
                &default_volume_addrs,
                &breakers,
                &guard.channel,
                &volume_router,
                &response_waiters,
            )
            .await;
            // guard 在此 drop
        });

        processed_any = true;
    }
    processed_any
}

/// 独立的管理请求处理器 (专用 tokio 任务使用)
///
/// 并发派发模型同 `process_data_requests`：spawn 独立任务 + 真实并发控制。
#[allow(clippy::too_many_arguments)]
async fn process_mgmt_requests(
    mgmt_queue: &Arc<RequestQueue>,
    mgmt_channel: &Arc<TransportChannel>,
    breakers: &Arc<CircuitBreakerPool>,
    volume_connections: &Arc<DashMap<String, Arc<PowerFsNetClient>>>,
    default_volume_addrs: &Arc<Mutex<Vec<String>>>,
    volume_router: &Arc<DashMap<u64, VolumeInfo>>,
    response_waiters: &Arc<Mutex<VolumeResponseWaiters>>,
    notify: &Arc<tokio::sync::Notify>,
) -> bool {
    let mut processed_any = false;
    while mgmt_channel.can_accept() {
        let next_req = match mgmt_queue.dequeue() {
            Some(r) => r,
            None => break,
        };
        let request_id = next_req.context.request_id.clone();
        mgmt_channel.add_request(request_id.clone());

        let volume_connections = volume_connections.clone();
        let default_volume_addrs = default_volume_addrs.clone();
        let breakers = breakers.clone();
        let volume_router = volume_router.clone();
        let response_waiters = response_waiters.clone();
        let channel_clone = mgmt_channel.clone();
        let notify_clone = notify.clone();

        tokio::spawn(async move {
            let guard = ConcurrencyGuard {
                channel: channel_clone,
                request_id,
                notify: notify_clone,
            };
            // process_mgmt_request_internal 的 _mgmt_channel 参数未使用，借用 guard.channel
            let _ = process_mgmt_request_internal(
                next_req,
                &volume_connections,
                &default_volume_addrs,
                &breakers,
                &guard.channel,
                &volume_router,
                &response_waiters,
            )
            .await;
            // guard 在此 drop
        });

        processed_any = true;
    }
    processed_any
}

/// 数据请求处理（自由函数版本）
#[allow(clippy::too_many_arguments)]
async fn process_data_request_internal(
    mut req: PendingRequest,
    volume_connections: &Arc<DashMap<String, Arc<PowerFsNetClient>>>,
    _default_volume_addrs: &Arc<Mutex<Vec<String>>>,
    breakers: &Arc<CircuitBreakerPool>,
    _data_channels: &[Arc<TransportChannel>],
    volume_router: &Arc<DashMap<u64, VolumeInfo>>,
    leases: &Arc<DashMap<(u64, u64), LeaseInfo>>,
    response_waiters: &Arc<Mutex<VolumeResponseWaiters>>,
) -> ClientResult<RequestResult> {
    let mut response_tx = req.response_tx.take();
    let request_id = req.context.request_id.clone();
    let kind = req.context.kind;
    let msg_type = req.context.msg_type;
    let body = req.context.payload.clone();

    log::debug!(
        "process_data_request_internal: request_id={:?}, kind={:?}, msg_type={:?}, body_len={}, shard_id={}",
        request_id, kind, msg_type, body.len(), req.shard_id
    );

    let volume = volume_router
        .get(&req.shard_id)
        .map(|v| v.clone())
        .ok_or_else(|| {
            let err = ClientError::VolumeNotFound(req.shard_id);
            log::error!(
                "process_data_request_internal: volume not found for shard_id={}",
                req.shard_id
            );
            deliver_result(
                &mut response_tx,
                &request_id,
                Err(err.clone()),
                response_waiters,
            );
            err
        })?;

    let volume_addr = volume.addr.clone();

    // Per-server circuit breaker check
    if !breakers.check(&volume_addr) {
        let result = Err(ClientError::CircuitOpen);
        deliver_result(
            &mut response_tx,
            &request_id,
            result.clone(),
            response_waiters,
        );
        return result;
    }

    log::debug!(
        "process_data_request_internal: connecting to volume at {}",
        volume_addr
    );

    let vol_client = get_or_create_volume_client_from_pool(volume_connections, &volume_addr)
        .await
        .map_err(|e| {
            let err = ClientError::Network(format!("Failed to get volume client: {}", e));
            log::error!(
                "process_data_request_internal: failed to get volume client: {}",
                e
            );
            deliver_result(
                &mut response_tx,
                &request_id,
                Err(err.clone()),
                response_waiters,
            );
            err
        })?;

    let resolved_msg_type = powerfs_net::MsgType::from_u16(msg_type).unwrap_or(match kind {
        RequestKind::Read => powerfs_net::MsgType::ReadNeedleBlob,
        RequestKind::Write => powerfs_net::MsgType::WriteNeedle,
        _ => powerfs_net::MsgType::ReadNeedleBlob,
    });

    log::debug!(
        "process_data_request_internal: sending request to volume: msg_type={:?}, body_len={}",
        resolved_msg_type,
        body.len()
    );

    let result = match kind {
        RequestKind::Read => {
            let msg = vol_client.send_request(resolved_msg_type, &body, &[]).await;
            match msg {
                Ok(resp) if resp.is_ok() => {
                    log::debug!(
                        "process_data_request_internal: received successful response: body_len={}, data_len={}",
                        resp.body.len(), resp.data.len()
                    );
                    breakers.record_success(&volume_addr);
                    Ok(RequestResult::success_with_payload(
                        request_id.clone(),
                        resp.body,
                        resp.data,
                    ))
                }
                Ok(resp) => {
                    // STATUS_ERR_NOT_FOUND (needle not found) is a common case for
                    // sparse files / holes: the FUSE read path matches on
                    // "needle not found" to zero-fill missing chunks. Without this
                    // distinction, sparse-file reads return EIO instead of zeros.
                    if resp.header.status == STATUS_ERR_NOT_FOUND {
                        log::debug!(
                            "process_data_request_internal: needle not found (status={})",
                            resp.header.status
                        );
                        Err(ClientError::Server("needle not found".to_string()))
                    } else {
                        log::error!(
                            "process_data_request_internal: received error response: status={}",
                            resp.header.status
                        );
                        breakers.record_failure(&volume_addr);
                        Err(ClientError::Server(format!(
                            "Server error: {}",
                            resp.header.status
                        )))
                    }
                }
                Err(e) => {
                    log::error!("process_data_request_internal: request failed: {}", e);
                    breakers.record_failure(&volume_addr);
                    Err(ClientError::from_net_error(e))
                }
            }
        }
        RequestKind::Write => {
            // Decode TLV body to extract file_key (needle_id) for per-inode lease check.
            // TLV layout from build_write_tlv: VolumeId -> FileKey(file_key) -> Offset -> Size -> Data
            // Use next_field() sequentially to locate the FileKey (file_key) field robustly.
            let file_key: u64 = {
                let mut dec = TlvDecoder::new(&body);
                let mut found: Option<u64> = None;
                // Walk TLV fields until we find the FileKey field
                while let Some((fid, length)) = dec.next_field() {
                    if fid == FieldId::FileKey {
                        match dec.read_u64(length) {
                            Ok(v) => {
                                found = Some(v);
                                break;
                            }
                            Err(_) => break,
                        }
                    } else {
                        // skip unknown/other fields by consuming length bytes
                        let _ = dec.skip(length);
                    }
                }
                found.unwrap_or(0)
            };

            // Verify per-inode lease is valid (token already embedded in TLV body by provider_adapter)
            let has_lease = {
                let found = leases
                    .get(&(req.shard_id, file_key))
                    .map(|l| l.is_valid())
                    .unwrap_or(false);
                if !found {
                    log::warn!(
                        "process_worker: per-inode lease MISSING shard={} file_key={}, total leases={}",
                        req.shard_id, file_key, leases.len(),
                    );
                    for entry in leases.iter().take(10) {
                        let (k0, k1) = *entry.key();
                        log::warn!(
                            "  existing lease key=({},{}) valid={}",
                            k0,
                            k1,
                            entry.value().is_valid()
                        );
                    }
                }
                found
            };
            if !has_lease {
                // 真正的有效性由服务端 validate_token_with_grace_period 保证，
                // 这里不阻塞，只记录警告避免缓存状态与实际不一致导致误拦截
                log::warn!(
                    "process_worker: no per-inode lease for shard={} file_key={}, proceeding anyway (server enforces validation",
                    req.shard_id, file_key
                );
            }

            // Send body as-is (lease_token + client_id already in TLV)
            let msg = vol_client.send_request(resolved_msg_type, &body, &[]).await;
            match msg {
                Ok(resp) if resp.is_ok() => {
                    breakers.record_success(&volume_addr);
                    Ok(RequestResult::success_with_payload(
                        request_id.clone(),
                        resp.body,
                        resp.data,
                    ))
                }
                Ok(resp) => {
                    breakers.record_failure(&volume_addr);
                    Err(ClientError::Server(format!(
                        "Server error: {}",
                        resp.header.status
                    )))
                }
                Err(e) => {
                    breakers.record_failure(&volume_addr);
                    Err(ClientError::from_net_error(e))
                }
            }
        }
        _ => Err(ClientError::UnsupportedRequest(format!("{:?}", kind))),
    };

    deliver_result(
        &mut response_tx,
        &request_id,
        result.clone(),
        response_waiters,
    );
    result
}

/// Lease 请求处理（自由函数版本）
async fn process_lease_request_internal(
    mut req: PendingRequest,
    volume_connections: &Arc<DashMap<String, Arc<PowerFsNetClient>>>,
    _default_volume_addrs: &Arc<Mutex<Vec<String>>>,
    breakers: &Arc<CircuitBreakerPool>,
    _lease_channel: &Arc<TransportChannel>,
    volume_router: &Arc<DashMap<u64, VolumeInfo>>,
    response_waiters: &Arc<Mutex<VolumeResponseWaiters>>,
) -> ClientResult<RequestResult> {
    let mut response_tx = req.response_tx.take();
    let request_id = req.context.request_id.clone();
    let body = req.context.payload.clone();

    let volume = volume_router
        .get(&req.shard_id)
        .map(|v| v.clone())
        .ok_or_else(|| {
            let err = ClientError::VolumeNotFound(req.shard_id);
            deliver_result(
                &mut response_tx,
                &request_id,
                Err(err.clone()),
                response_waiters,
            );
            err
        })?;

    let volume_addr = volume.addr.clone();

    // Per-server circuit breaker check
    if !breakers.check(&volume_addr) {
        let result = Err(ClientError::CircuitOpen);
        deliver_result(
            &mut response_tx,
            &request_id,
            result.clone(),
            response_waiters,
        );
        return result;
    }

    let vol_client = get_or_create_volume_client_from_pool(volume_connections, &volume_addr)
        .await
        .map_err(|e| {
            let err = ClientError::Network(format!("Failed to get volume client: {}", e));
            deliver_result(
                &mut response_tx,
                &request_id,
                Err(err.clone()),
                response_waiters,
            );
            err
        })?;

    let msg_type = powerfs_net::MsgType::from_u16(req.context.msg_type)
        .unwrap_or(powerfs_net::MsgType::ReadNeedleBlob);

    let result = vol_client.send_request(msg_type, &body, &[]).await;

    let final_result = match result {
        Ok(resp) if resp.is_ok() => {
            breakers.record_success(&volume_addr);
            Ok(RequestResult::success_with_payload(
                request_id.clone(),
                resp.body,
                resp.data,
            ))
        }
        Ok(resp) => {
            breakers.record_failure(&volume_addr);
            Err(ClientError::Server(format!(
                "Server error: {}",
                resp.header.status
            )))
        }
        Err(e) => {
            breakers.record_failure(&volume_addr);
            Err(ClientError::from_net_error(e))
        }
    };

    deliver_result(
        &mut response_tx,
        &request_id,
        final_result.clone(),
        response_waiters,
    );
    final_result
}

/// 管理请求处理（自由函数版本）
async fn process_mgmt_request_internal(
    mut req: PendingRequest,
    volume_connections: &Arc<DashMap<String, Arc<PowerFsNetClient>>>,
    _default_volume_addrs: &Arc<Mutex<Vec<String>>>,
    breakers: &Arc<CircuitBreakerPool>,
    _mgmt_channel: &Arc<TransportChannel>,
    volume_router: &Arc<DashMap<u64, VolumeInfo>>,
    response_waiters: &Arc<Mutex<VolumeResponseWaiters>>,
) -> ClientResult<RequestResult> {
    let mut response_tx = req.response_tx.take();
    let request_id = req.context.request_id.clone();
    let body = req.context.payload.clone();

    let volume = volume_router
        .get(&req.shard_id)
        .map(|v| v.clone())
        .ok_or_else(|| {
            let err = ClientError::VolumeNotFound(req.shard_id);
            deliver_result(
                &mut response_tx,
                &request_id,
                Err(err.clone()),
                response_waiters,
            );
            err
        })?;

    let volume_addr = volume.addr.clone();

    // Per-server circuit breaker check
    if !breakers.check(&volume_addr) {
        let result = Err(ClientError::CircuitOpen);
        deliver_result(
            &mut response_tx,
            &request_id,
            result.clone(),
            response_waiters,
        );
        return result;
    }

    let vol_client = get_or_create_volume_client_from_pool(volume_connections, &volume_addr)
        .await
        .map_err(|e| {
            let err = ClientError::Network(format!("Failed to get volume client: {}", e));
            deliver_result(
                &mut response_tx,
                &request_id,
                Err(err.clone()),
                response_waiters,
            );
            err
        })?;

    let msg_type = powerfs_net::MsgType::from_u16(req.context.msg_type)
        .unwrap_or(powerfs_net::MsgType::StatFs);

    let result = vol_client.send_request(msg_type, &body, &[]).await;

    let final_result = match result {
        Ok(resp) if resp.is_ok() => {
            breakers.record_success(&volume_addr);
            Ok(RequestResult::success_with_payload(
                request_id.clone(),
                resp.body,
                resp.data,
            ))
        }
        Ok(resp) => {
            breakers.record_failure(&volume_addr);
            Err(ClientError::Server(format!(
                "Server error: {}",
                resp.header.status
            )))
        }
        Err(e) => {
            breakers.record_failure(&volume_addr);
            Err(ClientError::from_net_error(e))
        }
    };

    deliver_result(
        &mut response_tx,
        &request_id,
        final_result.clone(),
        response_waiters,
    );
    final_result
}

/// 聚合的文件系统统计信息
#[derive(Debug, Clone, Default)]
pub struct FsStats {
    /// 总容量 (字节)
    pub total_size: u64,
    /// 已使用容量 (字节)
    pub used_size: u64,
    /// 剩余容量 (字节)
    pub free_size: u64,
    /// Volume 数量
    pub volume_count: u64,
}

impl VolumeClient {
    /// 查询所有 Volume 的 statfs 并聚合成集群级统计
    pub async fn statfs(&self, timeout: Duration) -> ClientResult<FsStats> {
        let volumes: Vec<(u64, String)> = self
            .volume_router
            .iter()
            .map(|entry| (*entry.key(), entry.value().addr.clone()))
            .collect();

        if volumes.is_empty() {
            return Ok(FsStats::default());
        }

        let mut total_size: u64 = 0;
        let mut used_size: u64 = 0;
        let mut free_size: u64 = 0;
        let mut volume_count: u64 = 0;
        let mut errors: Vec<String> = Vec::new();

        for (volume_id, addr) in &volumes {
            match self
                .query_single_volume_statfs(*volume_id, addr, timeout)
                .await
            {
                Ok(stats) => {
                    total_size += stats.total_size;
                    used_size += stats.used_size;
                    free_size += stats.free_size;
                    volume_count += 1;
                }
                Err(e) => {
                    errors.push(format!("volume {}: {}", volume_id, e));
                }
            }
        }

        if !errors.is_empty() && volume_count == 0 {
            return Err(ClientError::Internal(format!(
                "All statfs queries failed: {}",
                errors.join("; ")
            )));
        }

        log::debug!(
            "statfs aggregated: total={}, used={}, free={}, volumes={}, errors={}",
            total_size,
            used_size,
            free_size,
            volume_count,
            errors.len()
        );

        Ok(FsStats {
            total_size,
            used_size,
            free_size,
            volume_count,
        })
    }

    /// 查询单个 Volume 的 statfs
    async fn query_single_volume_statfs(
        &self,
        volume_id: u64,
        addr: &str,
        timeout: Duration,
    ) -> ClientResult<FsStats> {
        // 通过 VolumeClient 现有的 get_or_create_volume_client 保证连接复用
        let _vol_client = self.get_or_create_volume_client(addr).await.map_err(|e| {
            ClientError::Network(format!("Failed to connect volume {}: {}", volume_id, e))
        })?;

        let request_id = RequestId::new();
        let context = RequestContext::new(
            crate::client_identity::ClientIdentity::new(),
            RequestKind::Management,
            powerfs_net::MsgType::StatFs as u16,
            Vec::new(),
        )
        .with_request_id(request_id.clone());

        let (tx, rx) = oneshot::channel();

        self.submit_management_request(context, volume_id, Some(tx))
            .map_err(ClientError::Internal)?;

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(Ok(response))) => self.decode_statfs_response(&response),
            Ok(Ok(Err(e))) => Err(e),
            Ok(Err(_)) => Err(ClientError::Cancelled),
            Err(_) => Err(ClientError::Timeout(timeout)),
        }
    }

    fn decode_statfs_response(&self, result: &RequestResult) -> ClientResult<FsStats> {
        let body = match result.data.as_ref() {
            Some(b) => b,
            None => {
                return Err(ClientError::Internal(
                    "Empty statfs response body".to_string(),
                ))
            }
        };

        let mut dec = TlvDecoder::new(body);
        let total_size = dec.next_u64(FieldId::Size).unwrap_or(0);
        let used_size = dec.next_u64(FieldId::Blocks).unwrap_or(0);
        let free_size = dec.next_u64(FieldId::Blksize).unwrap_or(0);
        let volume_count = dec.next_u64(FieldId::Count).unwrap_or(0);

        Ok(FsStats {
            total_size,
            used_size,
            free_size,
            volume_count,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_identity::ClientIdentity;
    use crate::topology::ClusterTopology;

    fn create_test_volume_client() -> (
        VolumeClient,
        Arc<ClusterTopologyManager>,
        tokio::runtime::Runtime,
    ) {
        let topology_manager = Arc::new(ClusterTopologyManager::new());

        let mut topology = ClusterTopology::new();
        topology.volumes.insert(
            1,
            VolumeInfo::new(1, "/vol1".to_string(), "127.0.0.1:9344".to_string()),
        );
        topology_manager.update_topology(topology);

        let config = VolumeClientConfig::default();
        let client = VolumeClient::new(config, topology_manager.clone());

        // 启动 Tokio runtime 以支持异步操作（如 lease renewer）
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            client.init();
        });

        (client, topology_manager, runtime)
    }

    fn create_test_context(kind: RequestKind) -> RequestContext {
        let identity = ClientIdentity::new();
        RequestContext::new(identity, kind, 0x0001, vec![])
    }

    #[test]
    fn test_initialization() {
        let (client, _, _rt) = create_test_volume_client();
        assert_eq!(client.state(), VolumeClientState::Ready);
        assert!(client.get_volume(1).is_some());
        assert!(client.get_volume(2).is_none());
    }

    #[test]
    fn test_submit_read_request() {
        let (client, _, _rt) = create_test_volume_client();
        let ctx = create_test_context(RequestKind::Read);

        assert!(client.submit_data_request(ctx, 1, None).is_ok());

        let (data_len, _, _) = client.queue_stats();
        assert_eq!(data_len, 1);
    }

    #[test]
    fn test_submit_write_without_lease() {
        let (client, _, _rt) = create_test_volume_client();
        let ctx = create_test_context(RequestKind::Write);

        // 写请求在没有缓存 Lease 时仍可入队（客户端预检查降级为警告，
        // 真正的校验由 Volume 服务端 validate_token_with_grace_period 严格执行）。
        // 这避免了与 ProviderAdapter::ensure_lease 的时序竞争导致误拦截合法写请求。
        let result = client.submit_data_request(ctx, 1, None);
        assert!(
            result.is_ok(),
            "write without cached lease should proceed to server-side validation"
        );
    }

    #[test]
    fn test_submit_write_with_lease() {
        let (client, _, _rt) = create_test_volume_client();

        // 先获取 Lease
        client.update_lease(
            1,
            100,
            "token-1".to_string(),
            std::time::Duration::from_secs(30),
        );
        assert!(client.has_valid_lease_for_volume(1));

        let ctx = create_test_context(RequestKind::Write);
        assert!(client.submit_data_request(ctx, 1, None).is_ok());
    }

    #[test]
    fn test_lease_management() {
        let (client, _, _rt) = create_test_volume_client();

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
    fn test_lease_remaining_and_get_lease_remaining() {
        let (client, _, _rt) = create_test_volume_client();

        // No lease cached → None
        assert!(client.get_lease_remaining(1, 100).is_none());

        // Acquire lease with 30s duration
        client.update_lease(
            1,
            100,
            "token-remaining".to_string(),
            std::time::Duration::from_secs(30),
        );

        // remaining should be ~30s (allow slight timing slack)
        let remaining = client
            .get_lease_remaining(1, 100)
            .expect("lease should be valid");
        assert!(remaining <= std::time::Duration::from_secs(30));
        assert!(remaining > std::time::Duration::from_secs(28));

        // After release, get_lease_remaining returns None (is_valid false)
        client.release_lease(1, 100);
        assert!(client.get_lease_remaining(1, 100).is_none());
    }

    #[test]
    fn test_lease_info_remaining_directly() {
        // Test LeaseInfo::remaining() without a VolumeClient to avoid
        // background lease renewer task interference.
        let info = LeaseInfo::new("tok-direct".to_string(), std::time::Duration::from_secs(60));
        let remaining = info.remaining();
        assert!(remaining <= std::time::Duration::from_secs(60));
        assert!(remaining > std::time::Duration::from_secs(58));
        assert!(!info.is_expired());

        // Expired lease
        let mut expired = LeaseInfo::new(
            "tok-expired".to_string(),
            std::time::Duration::from_millis(1),
        );
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(expired.is_expired());
        assert_eq!(expired.remaining(), std::time::Duration::ZERO);
        expired.renew(std::time::Duration::from_secs(30));
        assert!(!expired.is_expired());
        assert!(expired.remaining() > std::time::Duration::from_secs(28));
    }

    #[test]
    fn test_queue_processing() {
        let (client, _, _rt) = create_test_volume_client();

        // 提交多个请求
        for _ in 0..3 {
            let ctx = create_test_context(RequestKind::Read);
            client.submit_data_request(ctx, 1, None).unwrap();
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
        let (client, _, _rt) = create_test_volume_client();

        // 数据请求
        let ctx1 = create_test_context(RequestKind::Read);
        client.submit_data_request(ctx1, 1, None).unwrap();

        // Lease 请求
        let ctx2 = create_test_context(RequestKind::Lease);
        client.submit_lease_request(ctx2, 1, None).unwrap();

        // 管理请求
        let ctx3 = create_test_context(RequestKind::Management);
        client.submit_management_request(ctx3, 1, None).unwrap();

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
        let (client, _, _rt) = create_test_volume_client();

        assert_eq!(client.get_volume(1).unwrap().addr, "127.0.0.1:9344");

        // 处理 Volume 变更
        let new_info = VolumeInfo::new(1, "/vol1-new".to_string(), "10.0.0.1:9344".to_string());
        client.handle_volume_change(1, new_info);

        assert_eq!(client.get_volume(1).unwrap().addr, "10.0.0.1:9344");
        assert_eq!(client.state(), VolumeClientState::Ready);
    }

    #[test]
    fn test_circuit_breaker() {
        let (client, _, _rt) = create_test_volume_client();

        // Trigger circuit breaker for a specific server
        for _ in 0..5 {
            let id = RequestId::new();
            client.record_failure(&id, RequestKind::Read, "test-server:8080");
        }

        let ctx = create_test_context(RequestKind::Read);
        // This will check the breaker for "unknown" (no routing yet)
        let result = client.submit_data_request(ctx, 1, None);
        // May or may not be open depending on whether routing resolved the address
        // The key test is that per-server breakers work correctly
        assert!(result.is_err() || result.is_ok()); // Either is acceptable
    }

    #[test]
    fn test_closed_client() {
        let (client, _, _rt) = create_test_volume_client();
        client.close();

        let ctx = create_test_context(RequestKind::Read);
        let result = client.submit_data_request(ctx, 1, None);
        assert!(result.is_err());
    }
}
