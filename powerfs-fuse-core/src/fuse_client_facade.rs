use std::sync::Arc;
use std::time::Duration;

use log::error;

use crate::client_identity::ClientIdentity;
use crate::meta_shard_client::{
    default_msg_type_for_kind, MetaShardClient, MetaShardClientConfig, RequestResult,
};
use crate::request_id::RequestId;
use crate::request_state::{RequestContext, RequestKind};
use crate::topology::{ClusterTopologyManager, MasterClient, MasterClientConfig};
use crate::volume_client::VolumeClient;
use crate::volume_client::VolumeClientConfig;

// 显式导入 Provider traits 以便在 SyncFuseClientFacade 中调用 provider 方法
use powerfs_common::traits::{
    MetadataProvider as _MetadataProvider, StorageProvider as _StorageProvider,
    VolumeProvider as _VolumeProvider,
};
use powerfs_master::proto::powerfs::{
    Entry as ProtoEntry, FileChunk as ProtoFileChunk, FuseAttributes as ProtoFuseAttributes,
};

/// 将 proto Entry 转换为 traits Entry
pub(crate) fn proto_entry_to_traits(entry: &ProtoEntry) -> powerfs_common::traits::Entry {
    let attributes = entry
        .attributes
        .as_ref()
        .map(|a| powerfs_common::traits::EntryAttributes {
            ino: a.ino,
            mode: a.mode,
            uid: a.uid,
            gid: a.gid,
            atime: chrono::DateTime::from_timestamp(a.atime as i64, 0)
                .unwrap_or_else(chrono::Utc::now),
            mtime: chrono::DateTime::from_timestamp(a.mtime as i64, 0)
                .unwrap_or_else(chrono::Utc::now),
            ctime: chrono::DateTime::from_timestamp(a.ctime as i64, 0)
                .unwrap_or_else(chrono::Utc::now),
            crtime: chrono::DateTime::from_timestamp(a.crtime as i64, 0)
                .unwrap_or_else(chrono::Utc::now),
        });

    let chunks = entry
        .chunks
        .iter()
        .map(|c| powerfs_common::traits::FileChunk {
            offset: c.offset,
            size: c.size,
            mtime: c.mtime,
            fid: c.fid.clone(),
            cookie: c.cookie,
            crc32: c.crc32,
        })
        .collect();

    powerfs_common::traits::Entry {
        name: entry.name.clone(),
        directory: entry.directory.clone(),
        attributes,
        chunks,
        hard_link_id: entry.hard_link_id.clone(),
        hard_link_counter: entry.hard_link_counter,
        extended: entry.extended.clone(),
        content_size: entry.content_size,
        disk_size: entry.disk_size,
        ttl: entry.ttl.clone(),
        symlink_target: entry.symlink_target.clone(),
        owner: entry.owner.clone(),
        generation: entry.generation,
    }
}

/// 将 traits Entry 转换为 proto Entry
pub(crate) fn traits_entry_to_proto(entry: &powerfs_common::traits::Entry) -> ProtoEntry {
    let attributes = entry.attributes.as_ref().map(|a| ProtoFuseAttributes {
        ino: a.ino,
        mode: a.mode,
        nlink: 1,
        uid: a.uid,
        gid: a.gid,
        rdev: 0,
        size: entry.content_size,
        blksize: 4096,
        blocks: entry.content_size.div_ceil(512),
        atime: a.atime.timestamp() as u64,
        mtime: a.mtime.timestamp() as u64,
        ctime: a.ctime.timestamp() as u64,
        crtime: a.crtime.timestamp() as u64,
        perm: 0,
    });

    let chunks = entry
        .chunks
        .iter()
        .map(|c| ProtoFileChunk {
            offset: c.offset,
            size: c.size,
            mtime: c.mtime,
            fid: c.fid.clone(),
            cookie: c.cookie,
            crc32: c.crc32,
        })
        .collect();

    ProtoEntry {
        name: entry.name.clone(),
        directory: entry.directory.clone(),
        attributes,
        chunks,
        hard_link_id: entry.hard_link_id.clone(),
        hard_link_counter: entry.hard_link_counter,
        extended: entry.extended.clone(),
        content_size: entry.content_size,
        disk_size: entry.disk_size,
        ttl: entry.ttl.clone(),
        symlink_target: entry.symlink_target.clone(),
        owner: entry.owner.clone(),
        generation: entry.generation,
    }
}

/// 将 PowerFsError 转换为 String
pub(crate) fn pfe_to_string(e: powerfs_common::error::PowerFsError) -> String {
    format!("{}", e)
}

/// Best-effort hostname for stats reporting. Falls back to "unknown" when the
/// hostname cannot be determined (e.g. inside minimal containers).
fn hostname_or_unknown() -> String {
    #[cfg(unix)]
    {
        use std::ffi::CStr;
        let mut buf = [0u8; 256];
        let ret = unsafe { libc::gethostname(buf.as_mut_ptr() as *mut _, buf.len()) };
        if ret == 0 {
            if let Ok(cstr) = CStr::from_bytes_until_nul(&buf) {
                return cstr.to_string_lossy().into_owned();
            }
        }
    }
    "unknown".to_string()
}

/// FuseClientFacade 配置
/// 所有端口和地址必须由调用方显式提供，无默认值
#[derive(Debug, Clone)]
pub struct FuseClientFacadeConfig {
    /// Master 节点地址（如 "172.20.0.11"）
    pub master_addr: String,
    /// Master powerfs-net 端口（如 9334）
    pub master_port: u16,
    /// Volume powerfs-net 端口（如 8901）
    pub volume_net_port: u16,
    /// Volume 地址列表（如 ["172.20.0.21", "172.20.0.22"]）
    pub volume_addrs: Vec<String>,
    /// Filer 节点地址（如 "172.20.0.35"）
    pub filer_addr: String,
    /// Filer powerfs-net 端口（如 9334）
    pub filer_port: u16,
    /// 请求超时
    pub request_timeout: Duration,
    /// 客户端身份
    pub client_identity: ClientIdentity,
    /// Optional Master gRPC endpoint (e.g. "http://172.20.0.11:9333") used by
    /// the MasterStatsReporter to push ClientStats via KeepConnected. When
    /// `None`, stats reporting is disabled.
    pub master_grpc_endpoint: Option<String>,
    /// Mount point path (reported to master via KeepConnected heartbeat).
    pub mount_point: String,
    /// Collection name (reported to master via KeepConnected heartbeat).
    pub collection: String,
    /// Replication placement (reported to master via KeepConnected heartbeat).
    pub replication: String,
}

impl FuseClientFacadeConfig {
    /// 创建新配置 - 所有参数必须显式提供
    pub fn new(
        master_addr: String,
        master_port: u16,
        volume_net_port: u16,
        volume_addrs: Vec<String>,
        filer_addr: String,
        filer_port: u16,
    ) -> Result<Self, String> {
        // 校验所有必需参数
        if master_addr.is_empty() {
            return Err("master_addr must not be empty".to_string());
        }
        if master_port == 0 {
            return Err("master_port must be > 0".to_string());
        }
        if volume_net_port == 0 {
            return Err("volume_net_port must be > 0".to_string());
        }
        if volume_addrs.is_empty() {
            return Err("volume_addrs must not be empty".to_string());
        }
        if filer_addr.is_empty() {
            return Err("filer_addr must not be empty".to_string());
        }
        if filer_port == 0 {
            return Err("filer_port must be > 0".to_string());
        }

        Ok(Self {
            master_addr,
            master_port,
            volume_net_port,
            volume_addrs,
            filer_addr,
            filer_port,
            request_timeout: Duration::from_secs(5),
            client_identity: ClientIdentity::new(),
            master_grpc_endpoint: None,
            mount_point: String::new(),
            collection: String::new(),
            replication: String::new(),
        })
    }

    /// 设置自定义超时
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// 设置自定义客户端身份
    pub fn with_client_identity(mut self, identity: ClientIdentity) -> Self {
        self.client_identity = identity;
        self
    }
}

/// FuseClientFacade - 门面模式，协调 MasterClient、MetaShardClient、VolumeClient
///
/// 作为 FUSE 客户端的统一入口，协调三个独立的客户端：
/// - MasterClient: 集群状态权威，管理拓扑和卷分配
/// - MetaShardClient: 元数据客户端，处理 inode/dentry 操作
/// - VolumeClient: 数据客户端，处理数据读写
pub struct FuseClientFacade {
    /// 配置
    config: FuseClientFacadeConfig,
    /// 拓扑管理器
    topology_manager: Arc<ClusterTopologyManager>,
    /// Master 客户端
    master_client: MasterClient,
    /// MetaShard 客户端
    meta_shard_client: MetaShardClient,
    /// Volume 客户端
    volume_client: Arc<VolumeClient>,
    /// Master 统计上报器（KeepConnected 心跳）。
    /// 字段仅持有所有权以保持后台任务存活；Drop 时 `shutdown_tx` 会被释放，
    /// 上报循环检测到通道关闭后自动退出。
    #[allow(dead_code)]
    stats_reporter: Option<crate::stats_reporter::MasterStatsReporter>,
}

impl FuseClientFacade {
    /// 创建新的 FuseClientFacade（不会自动连接，需要调用 connect）
    pub async fn new(config: FuseClientFacadeConfig) -> Result<Self, String> {
        // 创建拓扑管理器
        let topology_manager = Arc::new(ClusterTopologyManager::new());

        // 创建 Master 客户端
        let master_client_config = MasterClientConfig {
            master_addrs: vec![format!("{}:{}", config.master_addr, config.master_port)],
            request_timeout: config.request_timeout,
            max_retries: 3,
            circuit_breaker_config: crate::circuit_breaker::CircuitBreakerConfig::default(),
        };

        let master_client = MasterClient::new(master_client_config, topology_manager.clone());

        // 创建 MetaShard 客户端
        let meta_config = MetaShardClientConfig::default();
        let meta_shard_client = MetaShardClient::new(meta_config, topology_manager.clone());

        // 创建 Volume 客户端
        let volume_config = VolumeClientConfig::default();
        let volume_client = Arc::new(VolumeClient::new(volume_config, topology_manager.clone()));

        Ok(Self {
            config,
            topology_manager,
            master_client,
            meta_shard_client,
            volume_client,
            stats_reporter: None,
        })
    }

    /// 从配置构建 FuseClientFacade（推荐使用）
    ///
    /// 每个客户端独立管理自己的网络连接，不再需要外部注入 net_client。
    pub async fn build_from_config(config: FuseClientFacadeConfig) -> Result<Self, String> {
        // 创建拓扑管理器
        let topology_manager = Arc::new(ClusterTopologyManager::new());

        // 创建 Master 客户端（会自动创建自己的网络连接）
        let master_client_config = MasterClientConfig {
            master_addrs: vec![format!("{}:{}", config.master_addr, config.master_port)],
            request_timeout: config.request_timeout,
            max_retries: 3,
            circuit_breaker_config: crate::circuit_breaker::CircuitBreakerConfig::default(),
        };

        let master_client = MasterClient::new(master_client_config, topology_manager.clone());

        // 连接 Master
        master_client
            .connect()
            .await
            .map_err(|e| format!("Failed to connect to master: {}", e))?;

        // 获取初始拓扑（带重试机制，处理leader选举不稳定的情况）
        let max_retries = 5;
        let mut topology = None;
        for retry in 1..=max_retries {
            match master_client.fetch_topology().await {
                Ok(top) => {
                    topology = Some(top);
                    break;
                }
                Err(e) => {
                    log::warn!(
                        "FuseClientFacade: fetch_topology failed (attempt {}/{}): {}",
                        retry,
                        max_retries,
                        e
                    );
                    if retry < max_retries {
                        let delay_ms = (500u64) << (retry - 1).min(3); // 500ms, 1s, 2s, 4s
                        log::info!("FuseClientFacade: retrying in {}ms...", delay_ms);
                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    } else {
                        return Err(format!(
                            "Failed to fetch topology after {} attempts: {}",
                            max_retries, e
                        ));
                    }
                }
            }
        }
        let topology = topology.ok_or_else(|| "Failed to get topology".to_string())?;
        master_client.update_topology(topology);

        // 创建 MetaShard 客户端（会自动创建自己的网络连接池）
        let meta_config = MetaShardClientConfig::default();
        let meta_shard_client = MetaShardClient::new(meta_config, topology_manager.clone());
        meta_shard_client
            .set_default_filer_addr(format!("{}:{}", config.filer_addr, config.filer_port));
        meta_shard_client.init();

        // 创建 Volume 客户端（暂时为空，后续改造）
        let volume_config = VolumeClientConfig::default();
        let volume_client = Arc::new(VolumeClient::new(volume_config, topology_manager.clone()));

        // 设置默认 Volume 地址（从配置获取）
        if !config.volume_addrs.is_empty() {
            volume_client.set_default_volume_addrs(config.volume_addrs.clone());
            log::info!(
                "FuseClientFacade: set default volume addrs: {:?}",
                config.volume_addrs
            );
        }

        volume_client.init();

        // 启动 Master 统计上报器（KeepConnected 心跳）
        let stats_reporter = match &config.master_grpc_endpoint {
            Some(endpoint) if !endpoint.is_empty() => {
                let reporter_config = crate::stats_reporter::StatsReporterConfig {
                    master_endpoint: endpoint.clone(),
                    client_type: "fuse".to_string(),
                    mount_point: config.mount_point.clone(),
                    collection: config.collection.clone(),
                    replication: config.replication.clone(),
                    host: hostname_or_unknown(),
                    pid: std::process::id() as u64,
                    report_interval: Duration::from_secs(5),
                };
                let mut reporter = crate::stats_reporter::MasterStatsReporter::new(
                    reporter_config,
                    volume_client.clone(),
                );
                reporter.start();
                log::info!(
                    "FuseClientFacade: MasterStatsReporter started (endpoint={})",
                    endpoint
                );
                Some(reporter)
            }
            _ => {
                log::info!(
                    "FuseClientFacade: master_grpc_endpoint not set, MasterStatsReporter disabled"
                );
                None
            }
        };

        let facade = Self {
            config,
            topology_manager,
            master_client,
            meta_shard_client,
            volume_client,
            stats_reporter,
        };

        // 启动后台处理循环
        facade.meta_shard_client.start_background_processor();
        facade.volume_client.start_background_processor();

        Ok(facade)
    }

    /// 获取 Master 客户端引用
    pub fn master_client(&self) -> &MasterClient {
        &self.master_client
    }

    /// 获取 MetaShard 客户端引用
    pub fn meta_shard_client(&self) -> &MetaShardClient {
        &self.meta_shard_client
    }

    /// 获取 Volume 客户端引用
    pub fn volume_client(&self) -> &VolumeClient {
        self.volume_client.as_ref()
    }

    /// 获取拓扑管理器引用
    pub fn topology_manager(&self) -> &Arc<ClusterTopologyManager> {
        &self.topology_manager
    }

    /// 获取客户端标识（用于 lease holder 校验）
    pub fn client_id(&self) -> String {
        self.config.client_identity.client_id.to_string()
    }

    /// 获取 Volume 路由地址（从 VolumeClient 内部路由表查询）
    pub fn get_volume_addr(&self, volume_id: u64) -> Option<String> {
        self.volume_client.get_default_volume_addr(volume_id)
    }

    /// 获取 Filer 地址（用于元数据请求回退）
    pub fn filer_addr(&self) -> String {
        format!("{}:{}", self.config.filer_addr, self.config.filer_port)
    }

    /// 解析 Volume 路由并更新内部路由表
    pub fn resolve_volume_route(
        &self,
        volume_id: u64,
        locations: &[powerfs_common::traits::Location],
    ) {
        self.volume_client
            .resolve_and_set_volume_route(volume_id, locations);
    }

    /// 获取有效的 lease token（委托给 VolumeClient）
    pub fn get_valid_lease_token(&self, volume_id: u64, inode: u64) -> Option<String> {
        self.volume_client.get_valid_lease_token(volume_id, inode)
    }

    /// 更新 lease 缓存（委托给 VolumeClient）
    pub fn update_lease(&self, volume_id: u64, inode: u64, token: String, duration: Duration) {
        self.volume_client
            .update_lease(volume_id, inode, token, duration);
    }

    // ======= 元数据请求方法（委托给 MetaShardClient）=======

    /// 提交元数据请求
    pub async fn submit_metadata_request(
        &self,
        kind: RequestKind,
        shard_id: u64,
        payload: Vec<u8>,
    ) -> Result<RequestResult, String> {
        let msg_type = default_msg_type_for_kind(kind);
        self.submit_metadata_request_with_type(kind, shard_id, payload, msg_type)
            .await
    }

    /// 提交元数据请求（指定 MsgType）
    pub async fn submit_metadata_request_with_type(
        &self,
        kind: RequestKind,
        shard_id: u64,
        payload: Vec<u8>,
        msg_type: powerfs_net::MsgType,
    ) -> Result<RequestResult, String> {
        let request_id = RequestId::new();

        let context = RequestContext::new(
            self.config.client_identity.clone(),
            kind,
            msg_type as u16,
            payload,
        )
        .with_request_id(request_id);

        let timeout = self.config.request_timeout;
        self.meta_shard_client
            .submit_metadata_request_and_wait(context, shard_id, timeout)
            .await
            .map_err(|e| format!("Metadata request failed: {}", e))
    }

    // ======= 控制请求方法（委托给 MetaShardClient）=======

    /// 提交控制请求
    pub async fn submit_control_request(
        &self,
        kind: RequestKind,
        shard_id: u64,
        payload: Vec<u8>,
    ) -> Result<RequestResult, String> {
        let msg_type = default_msg_type_for_kind(kind);
        self.submit_control_request_with_type(kind, shard_id, payload, msg_type)
            .await
    }

    /// 提交控制请求（指定 MsgType）
    pub async fn submit_control_request_with_type(
        &self,
        kind: RequestKind,
        shard_id: u64,
        payload: Vec<u8>,
        msg_type: powerfs_net::MsgType,
    ) -> Result<RequestResult, String> {
        let request_id = RequestId::new();

        let context = RequestContext::new(
            self.config.client_identity.clone(),
            kind,
            msg_type as u16,
            payload,
        )
        .with_request_id(request_id);

        let timeout = self.config.request_timeout;
        self.meta_shard_client
            .submit_control_request_and_wait(context, shard_id, timeout)
            .await
            .map_err(|e| format!("Control request failed: {}", e))
    }

    // ======= 数据请求方法（委托给 VolumeClient）=======

    /// 提交数据请求
    pub async fn submit_data_request(
        &self,
        kind: RequestKind,
        volume_id: u64,
        payload: Vec<u8>,
    ) -> Result<RequestResult, String> {
        let msg_type = default_msg_type_for_kind(kind);
        self.submit_data_request_with_type(kind, volume_id, payload, msg_type)
            .await
    }

    /// 提交数据请求（指定 MsgType）
    pub async fn submit_data_request_with_type(
        &self,
        kind: RequestKind,
        volume_id: u64,
        payload: Vec<u8>,
        msg_type: powerfs_net::MsgType,
    ) -> Result<RequestResult, String> {
        let request_id = RequestId::new();

        let context = RequestContext::new(
            self.config.client_identity.clone(),
            kind,
            msg_type as u16,
            payload,
        )
        .with_request_id(request_id);

        let timeout = self.config.request_timeout;
        self.volume_client
            .submit_data_request_and_wait(context, volume_id, timeout)
            .await
            .map_err(|e| format!("Data request failed: {}", e))
    }

    // ======= Lease 请求方法（委托给 VolumeClient）=======

    /// 直接获取 Lease (绕过队列，直接网络请求)
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
    ) -> Result<String, String> {
        self.volume_client
            .acquire_lease(
                volume_id,
                inode,
                stripe_start,
                stripe_count,
                client_id,
                exclusive,
                duration_ms,
            )
            .await
            .map_err(|e| format!("AcquireLease failed: {}", e))
    }

    /// 直接释放 Lease (绕过队列，直接网络请求)
    pub async fn release_lease(
        &self,
        volume_id: u64,
        inode: u64,
        client_id: &str,
    ) -> Result<(), String> {
        self.volume_client
            .release_lease_remote(volume_id, inode, client_id)
            .await
            .map_err(|e| format!("ReleaseLease failed: {}", e))
    }

    /// 提交 Lease 请求
    pub async fn submit_lease_request(
        &self,
        volume_id: u64,
        payload: Vec<u8>,
    ) -> Result<RequestResult, String> {
        self.submit_lease_request_with_type(volume_id, payload, powerfs_net::MsgType::RangeLease)
            .await
    }

    /// 提交 Lease 请求（指定 MsgType）
    pub async fn submit_lease_request_with_type(
        &self,
        volume_id: u64,
        payload: Vec<u8>,
        msg_type: powerfs_net::MsgType,
    ) -> Result<RequestResult, String> {
        let request_id = RequestId::new();

        let context = RequestContext::new(
            self.config.client_identity.clone(),
            RequestKind::Lease,
            msg_type as u16,
            payload,
        )
        .with_request_id(request_id);

        let timeout = self.config.request_timeout;
        self.volume_client
            .submit_lease_request_and_wait(context, volume_id, timeout)
            .await
            .map_err(|e| format!("Lease request failed: {}", e))
    }

    // ======= 管理请求方法（委托给 VolumeClient）=======

    /// 提交管理请求
    pub async fn submit_mgmt_request(
        &self,
        volume_id: u64,
        payload: Vec<u8>,
    ) -> Result<RequestResult, String> {
        self.submit_mgmt_request_with_type(volume_id, payload, powerfs_net::MsgType::StatFs)
            .await
    }

    /// 提交管理请求（指定 MsgType）
    pub async fn submit_mgmt_request_with_type(
        &self,
        volume_id: u64,
        payload: Vec<u8>,
        msg_type: powerfs_net::MsgType,
    ) -> Result<RequestResult, String> {
        let request_id = RequestId::new();

        let context = RequestContext::new(
            self.config.client_identity.clone(),
            RequestKind::Management,
            msg_type as u16,
            payload,
        )
        .with_request_id(request_id);

        let timeout = self.config.request_timeout;
        self.volume_client
            .submit_mgmt_request_and_wait(context, volume_id, timeout)
            .await
            .map_err(|e| format!("Mgmt request failed: {}", e))
    }

    // ======= Master 请求方法（委托给 MasterClient）=======

    /// 提交请求到 Master（通过 MasterClient，自动处理重定向）
    pub async fn submit_master_request(
        &self,
        msg_type: powerfs_net::MsgType,
        payload: Vec<u8>,
    ) -> Result<powerfs_net::NetMessage, String> {
        self.master_client
            .submit_request(msg_type, &payload)
            .await
            .map_err(|e| format!("Master request failed: {}", e))
    }

    /// 从 Master 刷新拓扑
    pub async fn refresh_topology(&self) -> Result<(), String> {
        let topology = self
            .master_client
            .fetch_topology()
            .await
            .map_err(|e| format!("Failed to fetch topology: {}", e))?;
        self.master_client.update_topology(topology);
        Ok(())
    }

    /// 查询集群级 StatFs (聚合所有 Volume)
    pub async fn statfs(&self) -> Result<crate::volume_client::FsStats, String> {
        let timeout = self.config.request_timeout;
        self.volume_client
            .statfs(timeout)
            .await
            .map_err(|e| format!("statfs failed: {}", e))
    }

    /// 更新 Master leader 地址
    pub fn update_master_leader(&self, leader_addr: &str) {
        self.master_client.update_leader_address(leader_addr);
        log::info!("FuseClientFacade: Updated master leader to {}", leader_addr);
    }

    /// 关闭所有客户端
    pub fn close(&self) {
        self.meta_shard_client.close();
        self.volume_client.close();
        self.master_client.disconnect();
        log::info!("FuseClientFacade: All clients closed");
    }
}

impl Drop for FuseClientFacade {
    fn drop(&mut self) {
        self.close();
    }
}

/// SyncFuseClientFacade - 同步适配器
///
/// 将 FuseClientFacade 的异步接口包装为同步接口，
/// 用于在 FUSE 同步回调上下文中使用。
/// 通过 tokio::runtime::Runtime::block_on() 实现同步调用。
pub struct SyncFuseClientFacade {
    facade: Arc<FuseClientFacade>,
    runtime: Arc<tokio::runtime::Runtime>,
}

impl SyncFuseClientFacade {
    pub fn new(facade: Arc<FuseClientFacade>, runtime: Arc<tokio::runtime::Runtime>) -> Self {
        Self { facade, runtime }
    }

    pub fn facade(&self) -> &Arc<FuseClientFacade> {
        &self.facade
    }

    pub fn runtime(&self) -> &Arc<tokio::runtime::Runtime> {
        &self.runtime
    }

    pub fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        self.runtime.block_on(future)
    }

    /// 获取客户端标识（用于 lease holder 校验）
    pub fn client_id(&self) -> String {
        self.facade.client_id()
    }

    /// 从缓存获取 Volume 地址（优先使用缓存，仅在未命中时回退查询）
    pub fn get_volume_addr(&self, volume_id: u64) -> Result<String, String> {
        // 1. 首先尝试从 VolumeClient 缓存获取
        if let Some(vol_info) = self.facade.volume_client().get_volume(volume_id) {
            log::debug!(
                "get_volume_addr: cache hit for volume_id={}, addr={}",
                volume_id,
                vol_info.addr
            );
            return Ok(vol_info.addr);
        }

        // 2. 如果缓存未命中，回退查询 Master
        log::warn!(
            "get_volume_addr: cache miss for volume_id={}, querying master",
            volume_id
        );
        let vid = powerfs_common::types::VolumeId(volume_id);
        self.lookup_volume(vid)
            .map(|locs| locs.first().map(|l| l.url.clone()).unwrap_or_default())
            .and_then(|addr| {
                if addr.is_empty() {
                    Err(format!("No address found for volume_id={}", volume_id))
                } else {
                    Ok(addr)
                }
            })
    }

    /// Location -> URL 字符串转换
    pub fn location_to_grpc_addr(loc: &powerfs_common::traits::Location) -> String {
        if loc.url.is_empty() {
            String::new()
        } else {
            loc.url.clone()
        }
    }

    // ======= 便捷同步方法（供 fuse.rs 使用）=======

    pub fn get_entry(&self, path: &str) -> Result<Option<ProtoEntry>, String> {
        let facade = self.facade.clone();
        let path = path.to_string();
        self.runtime.block_on(async move {
            let provider = crate::provider_adapter::FacadeMetadataProvider::new(facade);
            let result = provider.get_entry(&path).await.map_err(pfe_to_string)?;
            Ok(result.map(|e| traits_entry_to_proto(&e)))
        })
    }

    pub fn get_entry_by_parent(
        &self,
        parent_ino: u64,
        name: &str,
    ) -> Result<Option<ProtoEntry>, String> {
        let facade = self.facade.clone();
        let name = name.to_string();
        self.runtime.block_on(async move {
            let provider = crate::provider_adapter::FacadeMetadataProvider::new(facade);
            let result = provider
                .get_entry_by_parent(parent_ino, &name)
                .await
                .map_err(pfe_to_string)?;
            Ok(result.map(|e| traits_entry_to_proto(&e)))
        })
    }

    pub fn get_entry_by_inode(&self, inode: u64) -> Result<Option<(ProtoEntry, String)>, String> {
        error!(
            "[DEBUG_SYNC] SyncFuseClientFacade::get_entry_by_inode called: inode={}",
            inode
        );
        let facade = self.facade.clone();
        let result = self.runtime.block_on(async move {
            let provider = crate::provider_adapter::FacadeMetadataProvider::new(facade);
            let result = provider
                .get_entry_by_inode(inode)
                .await
                .map_err(pfe_to_string)?;
            Ok(result.map(|(e, p)| (traits_entry_to_proto(&e), p)))
        });
        error!(
            "[DEBUG_SYNC] SyncFuseClientFacade::get_entry_by_inode result: inode={}, is_ok={}",
            inode,
            result.is_ok()
        );
        result
    }

    pub fn create_entry(&self, entry: &ProtoEntry, client_id: &str) -> Result<u64, String> {
        let facade = self.facade.clone();
        let traits_entry = proto_entry_to_traits(entry);
        let client_id = client_id.to_string();
        self.runtime.block_on(async move {
            let provider = crate::provider_adapter::FacadeMetadataProvider::new(facade);
            provider
                .create_entry(&traits_entry, &client_id)
                .await
                .map_err(pfe_to_string)
        })
    }

    pub fn update_entry(
        &self,
        entry: &ProtoEntry,
        client_id: &str,
        old_size: u64,
        is_truncate: bool,
    ) -> Result<u64, String> {
        let facade = self.facade.clone();
        let traits_entry = proto_entry_to_traits(entry);
        let client_id = client_id.to_string();
        self.runtime.block_on(async move {
            let provider = crate::provider_adapter::FacadeMetadataProvider::new(facade);
            provider
                .update_entry(&traits_entry, &client_id, old_size, is_truncate)
                .await
                .map_err(pfe_to_string)
        })
    }

    /// 同步获取 Lease
    #[allow(clippy::too_many_arguments)]
    pub fn acquire_lease(
        &self,
        volume_id: u64,
        inode: u64,
        stripe_start: u64,
        stripe_count: u64,
        client_id: &str,
        exclusive: bool,
        duration_ms: u64,
    ) -> Result<String, String> {
        let facade = self.facade.clone();
        let client_id = client_id.to_string();
        self.runtime.block_on(async move {
            facade
                .acquire_lease(
                    volume_id,
                    inode,
                    stripe_start,
                    stripe_count,
                    &client_id,
                    exclusive,
                    duration_ms,
                )
                .await
        })
    }

    /// 同步释放 Lease
    pub fn release_lease(&self, volume_id: u64, inode: u64, client_id: &str) -> Result<(), String> {
        let facade = self.facade.clone();
        let client_id = client_id.to_string();
        self.runtime
            .block_on(async move { facade.release_lease(volume_id, inode, &client_id).await })
    }

    pub fn delete_entry(
        &self,
        parent_ino: u64,
        name: &str,
        is_dir: bool,
        client_id: &str,
    ) -> Result<(), String> {
        let facade = self.facade.clone();
        let name = name.to_string();
        let client_id = client_id.to_string();
        self.runtime.block_on(async move {
            let provider = crate::provider_adapter::FacadeMetadataProvider::new(facade);

            // 解析 inode
            let path = if parent_ino == 1 {
                format!("/{}", name)
            } else {
                match provider
                    .get_entry_by_inode(parent_ino)
                    .await
                    .map_err(pfe_to_string)?
                {
                    Some((_, parent_path)) if !parent_path.is_empty() => {
                        format!("{}/{}", parent_path, name)
                    }
                    _ => name.clone(),
                }
            };

            let inode = match provider.get_entry(&path).await.map_err(pfe_to_string)? {
                Some(entry) => entry.attributes.map(|a| a.ino).unwrap_or(0),
                None => 0,
            };

            if inode == 0 {
                return Err(format!(
                    "Failed to resolve inode for deletion: path={}",
                    path
                ));
            }

            provider
                .delete_entry(inode, is_dir, &client_id)
                .await
                .map_err(pfe_to_string)
        })
    }

    pub fn list_entries(
        &self,
        inode: u64,
        limit: u32,
        client_id: &str,
    ) -> Result<Vec<ProtoEntry>, String> {
        let facade = self.facade.clone();
        let client_id = client_id.to_string();
        self.runtime.block_on(async move {
            let provider = crate::provider_adapter::FacadeMetadataProvider::new(facade);
            let entries = provider
                .list_entries(inode, limit, &client_id)
                .await
                .map_err(pfe_to_string)?;
            Ok(entries.iter().map(traits_entry_to_proto).collect())
        })
    }

    pub fn assign_volume(
        &self,
        collection: &str,
        replication: &str,
    ) -> Result<
        (
            powerfs_common::types::Fid,
            Vec<powerfs_common::traits::Location>,
        ),
        String,
    > {
        let facade = self.facade.clone();
        let collection = collection.to_string();
        let replication = replication.to_string();
        self.runtime.block_on(async move {
            let provider = crate::provider_adapter::FacadeVolumeProvider::new(facade.clone());
            let result = provider
                .assign_volume(&collection, &replication)
                .await
                .map_err(pfe_to_string);

            // 如果成功，设置卷路由
            if let Ok((fid, locations)) = &result {
                facade.resolve_volume_route(fid.volume_id.0, locations);
                log::debug!(
                    "assign_volume: resolved volume route for volume_id={}",
                    fid.volume_id.0
                );
            }

            result
        })
    }

    pub fn lookup_volume(
        &self,
        volume_id: powerfs_common::types::VolumeId,
    ) -> Result<Vec<powerfs_common::traits::Location>, String> {
        let facade = self.facade.clone();
        let vid = volume_id.0;

        log::info!("lookup_volume: starting for volume_id={}", vid);

        self.runtime.block_on(async move {
            let provider = crate::provider_adapter::FacadeVolumeProvider::new(facade.clone());
            let locations_result = provider.lookup_volume(volume_id).await;

            log::info!(
                "lookup_volume: Master returned result for volume_id={}: is_ok={}",
                vid,
                locations_result.is_ok()
            );

            let locations = match locations_result {
                Ok(locs) => {
                    log::info!(
                        "lookup_volume: Master returned {} locations for volume_id={}",
                        vid,
                        locs.len()
                    );
                    facade.resolve_volume_route(vid, &locs);
                    if !locs.is_empty() {
                        log::debug!(
                            "lookup_volume: resolved volume={} via master, {} locations",
                            vid,
                            locs.len()
                        );
                    }
                    locs
                }
                Err(e) => {
                    log::error!(
                        "lookup_volume: Master LookupVolume failed for volume_id={}: {}",
                        vid,
                        pfe_to_string(e)
                    );
                    Vec::new()
                }
            };

            // 若 lookup 结果为空，VolumeClient 内部会用默认地址回退
            // 这里只需返回 locations（可能为空，由调用方处理）
            log::info!(
                "lookup_volume: returning {} locations for volume_id={}",
                locations.len(),
                vid
            );
            Ok(locations)
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn write_blob(
        &self,
        volume_addr: &str,
        volume_id: u64,
        file_key: u64,
        offset: i64,
        size: i32,
        data: Vec<u8>,
        _cookie: u32,
    ) -> Result<(), String> {
        let _ = volume_addr;
        let facade = self.facade.clone();
        self.runtime.block_on(async move {
            let provider = crate::provider_adapter::FacadeStorageProvider::new(facade);
            provider
                .write_blob(volume_id, file_key, offset, size, &data)
                .await
                .map_err(pfe_to_string)
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn write_blob_with_lease(
        &self,
        volume_addr: &str,
        volume_id: u64,
        file_key: u64,
        inode: u64,
        offset: i64,
        size: i32,
        data: Vec<u8>,
        lease_token: Option<&str>,
    ) -> Result<(), String> {
        let _ = volume_addr;
        let facade = self.facade.clone();
        let lease_owned = lease_token.map(|s| s.to_string());
        self.runtime.block_on(async move {
            let provider = crate::provider_adapter::FacadeStorageProvider::new(facade);
            let lease_ref = lease_owned.as_deref();
            provider
                .write_blob_with_lease(volume_id, file_key, inode, offset, size, &data, lease_ref)
                .await
                .map_err(pfe_to_string)
        })
    }

    pub fn read_blob(
        &self,
        volume_addr: &str,
        volume_id: u64,
        file_key: u64,
        offset: i64,
        size: i32,
    ) -> Result<Vec<u8>, String> {
        let _ = volume_addr;
        let facade = self.facade.clone();
        self.runtime.block_on(async move {
            let provider = crate::provider_adapter::FacadeStorageProvider::new(facade);
            match provider.read_blob(volume_id, file_key, offset, size).await {
                Ok(data) => Ok(data),
                Err(e) => Err(pfe_to_string(e)),
            }
        })
    }

    pub fn delete_blob(&self, volume_id: u64, file_key: u64) -> Result<(), String> {
        let facade = self.facade.clone();
        self.runtime.block_on(async move {
            let provider = crate::provider_adapter::FacadeStorageProvider::new(facade);
            provider
                .delete_blob(volume_id, file_key)
                .await
                .map_err(pfe_to_string)
        })
    }

    pub fn delete_data(
        &self,
        _volume_addr: &str,
        volume_id: u64,
        file_key: u64,
    ) -> Result<(), String> {
        let facade = self.facade.clone();
        self.runtime.block_on(async move {
            let provider = crate::provider_adapter::FacadeStorageProvider::new(facade);
            provider
                .delete_blob(volume_id, file_key)
                .await
                .map_err(pfe_to_string)
        })
    }

    #[allow(clippy::type_complexity)]
    pub fn assign_fid(
        &self,
        collection: &str,
        replication: &str,
    ) -> Result<
        (
            powerfs_common::types::Fid,
            Option<powerfs_common::traits::Location>,
            Vec<String>,
            Vec<powerfs_common::traits::Location>,
        ),
        String,
    > {
        let (fid, locations) = self.assign_volume(collection, replication)?;
        let primary = locations.first().cloned();
        Ok((fid, primary, Vec::new(), locations))
    }

    /// 创建符号链接
    pub fn symlink(&self, parent: u64, name: &str, target: &str) -> Result<u64, String> {
        let facade = self.facade.clone();
        let name = name.to_string();
        let target = target.to_string();
        self.runtime.block_on(async move {
            let payload = {
                let mut enc = powerfs_net::TlvEncoder::new();
                let _ = enc.add_u64(powerfs_net::FieldId::ParentIno, parent);
                let _ = enc.add_string(powerfs_net::FieldId::Name, &name);
                let _ = enc.add_string(powerfs_net::FieldId::SymlinkTarget, &target);
                enc.into_bytes()
            };

            let request_id = RequestId::new();
            let context = RequestContext::new(
                facade.config.client_identity.clone(),
                RequestKind::Metadata,
                powerfs_net::MsgType::Symlink as u16,
                payload,
            )
            .with_request_id(request_id);

            let timeout = facade.config.request_timeout;
            let result = facade
                .meta_shard_client
                .submit_metadata_request_and_wait(context, parent, timeout)
                .await
                .map_err(|e| format!("symlink failed: {}", e))?;

            // Parse the inode from response
            let inode = result
                .payload
                .as_deref()
                .filter(|d| !d.is_empty())
                .and_then(|d| {
                    let mut dec = powerfs_net::TlvDecoder::new(d);
                    dec.next_u64(powerfs_net::FieldId::Ino).ok()
                })
                .or_else(|| {
                    result
                        .data
                        .as_deref()
                        .filter(|d| !d.is_empty())
                        .and_then(|d| {
                            let mut dec = powerfs_net::TlvDecoder::new(d);
                            dec.next_u64(powerfs_net::FieldId::Ino).ok()
                        })
                })
                .ok_or_else(|| "Failed to parse inode from symlink response".to_string())?;
            Ok(inode)
        })
    }

    /// 读取符号链接
    pub fn readlink(&self, inode: u64) -> Result<String, String> {
        let facade = self.facade.clone();
        self.runtime.block_on(async move {
            let payload = {
                let mut enc = powerfs_net::TlvEncoder::new();
                let _ = enc.add_u64(powerfs_net::FieldId::Ino, inode);
                enc.into_bytes()
            };

            let request_id = RequestId::new();
            let context = RequestContext::new(
                facade.config.client_identity.clone(),
                RequestKind::Metadata,
                powerfs_net::MsgType::Readlink as u16,
                payload,
            )
            .with_request_id(request_id);

            let timeout = facade.config.request_timeout;
            let result = facade
                .meta_shard_client
                .submit_metadata_request_and_wait(context, inode, timeout)
                .await
                .map_err(|e| format!("readlink failed: {}", e))?;

            // Parse the symlink target from response
            let target = result
                .payload
                .as_deref()
                .filter(|d| !d.is_empty())
                .map(|d| {
                    let mut dec = powerfs_net::TlvDecoder::new(d);
                    dec.next_string(powerfs_net::FieldId::SymlinkTarget)
                        .unwrap_or_default()
                })
                .or_else(|| {
                    result.data.as_deref().filter(|d| !d.is_empty()).map(|d| {
                        let mut dec = powerfs_net::TlvDecoder::new(d);
                        dec.next_string(powerfs_net::FieldId::SymlinkTarget)
                            .unwrap_or_default()
                    })
                })
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "Failed to parse symlink target from response".to_string())?;
            Ok(target)
        })
    }

    /// 创建硬链接
    pub fn link(&self, inode: u64, newparent: u64, name: &str) -> Result<u64, String> {
        let facade = self.facade.clone();
        let name = name.to_string();
        self.runtime.block_on(async move {
            let payload = {
                let mut enc = powerfs_net::TlvEncoder::new();
                let _ = enc.add_u64(powerfs_net::FieldId::Ino, inode);
                let _ = enc.add_u64(powerfs_net::FieldId::ParentIno, newparent);
                let _ = enc.add_string(powerfs_net::FieldId::Name, &name);
                enc.into_bytes()
            };

            let request_id = RequestId::new();
            let context = RequestContext::new(
                facade.config.client_identity.clone(),
                RequestKind::Metadata,
                powerfs_net::MsgType::Link as u16,
                payload,
            )
            .with_request_id(request_id);

            let timeout = facade.config.request_timeout;
            let _result = facade
                .meta_shard_client
                .submit_metadata_request_and_wait(context, newparent, timeout)
                .await
                .map_err(|e| format!("link failed: {}", e))?;

            // Parse the inode from response (should be the same as input inode)
            Ok(inode)
        })
    }

    /// 查询集群级 StatFs (同步)
    pub fn statfs(&self) -> Result<crate::volume_client::FsStats, String> {
        let facade = self.facade.clone();
        self.runtime.block_on(async move { facade.statfs().await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_facade_config_creation() {
        let config = FuseClientFacadeConfig::new(
            "172.20.0.11".to_string(),
            9334,
            8901,
            vec!["172.20.0.21".to_string(), "172.20.0.22".to_string()],
            "172.20.0.35".to_string(),
            9334,
        )
        .unwrap();

        assert_eq!(config.master_addr, "172.20.0.11");
        assert_eq!(config.master_port, 9334);
        assert_eq!(config.volume_net_port, 8901);
        assert_eq!(config.volume_addrs.len(), 2);
        assert_eq!(config.filer_addr, "172.20.0.35");
        assert_eq!(config.filer_port, 9334);
        assert_eq!(config.request_timeout, Duration::from_secs(5));
    }

    #[test]
    fn test_facade_config_validation() {
        // 空master_addr应该失败
        let result = FuseClientFacadeConfig::new(
            "".to_string(),
            9334,
            8901,
            vec!["172.20.0.21".to_string()],
            "172.20.0.35".to_string(),
            9334,
        );
        assert!(result.is_err());

        // master_port为0应该失败
        let result = FuseClientFacadeConfig::new(
            "172.20.0.11".to_string(),
            0,
            8901,
            vec!["172.20.0.21".to_string()],
            "172.20.0.35".to_string(),
            9334,
        );
        assert!(result.is_err());

        // 空volume_addrs应该失败
        let result = FuseClientFacadeConfig::new(
            "172.20.0.11".to_string(),
            9334,
            8901,
            vec![],
            "172.20.0.35".to_string(),
            9334,
        );
        assert!(result.is_err());

        // 空filer_addr应该失败
        let result = FuseClientFacadeConfig::new(
            "172.20.0.11".to_string(),
            9334,
            8901,
            vec!["172.20.0.21".to_string()],
            "".to_string(),
            9334,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_facade_config_with_options() {
        let config = FuseClientFacadeConfig::new(
            "172.20.0.11".to_string(),
            9334,
            8901,
            vec!["172.20.0.21".to_string()],
            "172.20.0.35".to_string(),
            9334,
        )
        .unwrap()
        .with_timeout(Duration::from_secs(10));

        assert_eq!(config.request_timeout, Duration::from_secs(10));
    }
}
