use std::sync::Arc;
use std::time::Duration;

use crate::client_identity::ClientIdentity;
use crate::meta_shard_client::{
    default_msg_type_for_kind, MetaShardClient, MetaShardClientConfig, RequestResult,
};
use crate::net_client::PowerFuseNetClient;
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
pub(crate) fn proto_entry_to_traits(
    entry: &ProtoEntry,
) -> powerfs_common::traits::Entry {
    let attributes = entry.attributes.as_ref().map(|a| {
        powerfs_common::traits::EntryAttributes {
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
        }
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
pub(crate) fn traits_entry_to_proto(
    entry: &powerfs_common::traits::Entry,
) -> ProtoEntry {
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

/// FuseClientFacade 配置
#[derive(Debug, Clone)]
pub struct FuseClientFacadeConfig {
    /// Master 节点地址
    pub master_addr: String,
    /// Master 端口
    pub master_port: u16,
    /// Volume 网络端口
    pub volume_net_port: u16,
    /// Volume 地址列表
    pub volume_addrs: Vec<String>,
    /// Filer 节点地址
    pub filer_addr: String,
    /// Filer 端口
    pub filer_port: u16,
    /// 请求超时
    pub request_timeout: Duration,
    /// 客户端身份
    pub client_identity: ClientIdentity,
}

impl Default for FuseClientFacadeConfig {
    fn default() -> Self {
        Self {
            master_addr: "127.0.0.1".to_string(),
            master_port: 9333,
            volume_net_port: 9344,
            volume_addrs: Vec::new(),
            filer_addr: "127.0.0.1".to_string(),
            filer_port: 9343,
            request_timeout: Duration::from_secs(5),
            client_identity: ClientIdentity::default(),
        }
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
    /// 网络客户端（可选，用于统一网络管理）
    net_client: Option<Arc<PowerFuseNetClient>>,
    /// 拓扑管理器
    topology_manager: Arc<ClusterTopologyManager>,
    /// Master 客户端
    master_client: MasterClient,
    /// MetaShard 客户端
    meta_shard_client: MetaShardClient,
    /// Volume 客户端
    volume_client: VolumeClient,
}

impl FuseClientFacade {
    /// 创建新的 FuseClientFacade
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
        let volume_client = VolumeClient::new(volume_config, topology_manager.clone());

        Ok(Self {
            config,
            net_client: None,
            topology_manager,
            master_client,
            meta_shard_client,
            volume_client,
        })
    }

    /// 从已有的 PowerFuseNetClient 构建 FuseClientFacade
    ///
    /// 用于已经建立 master/filer 连接的场景，复用现有 net_client。
    /// 每个客户端通过 set_net_client() 注入自己的网络连接。
    pub async fn build_from_net_client(
        config: FuseClientFacadeConfig,
        net_client: Arc<PowerFuseNetClient>,
    ) -> Result<Self, String> {
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
        master_client.set_net_client(net_client.clone());

        // 连接 Master
        master_client
            .connect()
            .await
            .map_err(|e| format!("Failed to connect to master: {}", e))?;

        // 获取初始拓扑
        let topology = master_client
            .fetch_topology()
            .await
            .map_err(|e| format!("Failed to fetch topology: {}", e))?;
        master_client.update_topology(topology);

        // 创建 MetaShard 客户端
        let meta_config = MetaShardClientConfig::default();
        let mut meta_shard_client = MetaShardClient::new(meta_config, topology_manager.clone());
        meta_shard_client.set_net_client(net_client.clone());
        meta_shard_client.init();

        // 创建 Volume 客户端
        let volume_config = VolumeClientConfig::default();
        let mut volume_client = VolumeClient::new(volume_config, topology_manager.clone());
        volume_client.set_net_client(net_client.clone());
        volume_client.init();

        let facade = Self {
            config,
            net_client: Some(net_client),
            topology_manager,
            master_client,
            meta_shard_client,
            volume_client,
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
        &self.volume_client
    }

    /// 获取拓扑管理器引用
    pub fn topology_manager(&self) -> &Arc<ClusterTopologyManager> {
        &self.topology_manager
    }

    /// 获取客户端标识（用于 lease holder 校验）
    pub fn client_id(&self) -> String {
        self.config.client_identity.client_id.to_string()
    }

    /// 获取默认 Volume 地址列表
    pub fn volume_addrs(&self) -> Vec<String> {
        self.config.volume_addrs.clone()
    }

    /// 获取 Filer Leader 地址（用于 Volume 路由回退）
    pub fn filer_leader_addr(&self) -> String {
        self.net_client
            .as_ref()
            .map(|nc| nc.filer_leader_addr())
            .unwrap_or_default()
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

    /// 提交请求直接到 Master
    pub async fn submit_master_request(
        &self,
        msg_type: powerfs_net::MsgType,
        payload: Vec<u8>,
    ) -> Result<powerfs_net::NetMessage, String> {
        // 尝试最多3次处理重定向
        for attempt in 0..3 {
            let net_client = self
                .master_client
                .net_client()
                .ok_or_else(|| "Master net client not available".to_string())?;

            let master_nc = net_client.master_client();
            let response = master_nc
                .send_request(msg_type, &payload, &[])
                .await
                .map_err(|e| format!("Master request failed: {}", e))?;

            // 检查重定向响应
            if response.header.status == powerfs_net::STATUS_ERR_REDIRECT {
                log::warn!(
                    "submit_master_request: attempt {}, received redirect response",
                    attempt + 1
                );
                let body = if !response.body.is_empty() {
                    &response.body
                } else {
                    &response.data
                };
                let mut dec = powerfs_net::TlvDecoder::new(body);
                let leader_addr = dec.next_string(powerfs_net::FieldId::Owner).unwrap_or_default();

                if leader_addr.is_empty() {
                    return Err("redirect response has empty leader address".to_string());
                }

                log::info!(
                    "submit_master_request: redirecting to leader at {} (attempt {})",
                    leader_addr,
                    attempt + 1
                );
                self.update_master_leader(&leader_addr);

                // 解析 leader 地址
                let parts: Vec<&str> = leader_addr.split(':').collect();
                let host = parts.first().unwrap_or(&"127.0.0.1").to_string();
                let port = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(9333);

                // 创建新的内部网络客户端连接到 leader
                let inner_config = powerfs_net::ClientConfig {
                    addr: host,
                    port,
                    client_type: powerfs_net::ClientType::Fuse,
                    client_id: self.config.client_identity.as_client_id(),
                    ..powerfs_net::ClientConfig::default()
                };
                let new_inner_client = Arc::new(powerfs_net::PowerFsNetClient::new(inner_config));

                match new_inner_client.connect().await {
                    Ok(_) => {
                        log::info!("submit_master_request: connected to leader at {}", leader_addr);

                        let net_client = self.net_client.as_ref().ok_or_else(|| {
                            "Net client not available for leader redirect".to_string()
                        })?;

                        // 创建新的包装客户端
                        let new_wrapper = crate::net_client::PowerFuseNetClient::new_with_master(
                            net_client.config().clone(),
                            new_inner_client,
                        );

                        self.master_client.set_net_client(Arc::new(new_wrapper));
                        continue;
                    }
                    Err(e) => {
                        log::error!(
                            "submit_master_request: failed to connect to leader at {}: {}",
                            leader_addr,
                            e
                        );
                        if attempt == 2 {
                            return Err(format!(
                                "Failed to connect to leader after 3 attempts: {}",
                                e
                            ));
                        }
                        continue;
                    }
                }
            }

            return Ok(response);
        }

        Err("Failed to complete request after 3 attempts".to_string())
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

    /// Location -> URL 字符串转换
    pub fn location_to_grpc_addr(
        loc: &powerfs_common::traits::Location,
    ) -> String {
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

    pub fn get_entry_by_inode(&self, inode: u64) -> Result<Option<(ProtoEntry, String)>, String> {
        let facade = self.facade.clone();
        self.runtime.block_on(async move {
            let provider = crate::provider_adapter::FacadeMetadataProvider::new(facade);
            let result = provider.get_entry_by_inode(inode).await.map_err(pfe_to_string)?;
            Ok(result.map(|(e, p)| (traits_entry_to_proto(&e), p)))
        })
    }

    pub fn create_entry(&self, entry: &ProtoEntry, client_id: &str) -> Result<u64, String> {
        let facade = self.facade.clone();
        let traits_entry = proto_entry_to_traits(entry);
        let client_id = client_id.to_string();
        self.runtime.block_on(async move {
            let provider = crate::provider_adapter::FacadeMetadataProvider::new(facade);
            provider.create_entry(&traits_entry, &client_id).await.map_err(pfe_to_string)
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
            provider.update_entry(&traits_entry, &client_id, old_size, is_truncate).await.map_err(pfe_to_string)
        })
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
                match provider.get_entry_by_inode(parent_ino).await.map_err(pfe_to_string)? {
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
                return Err(format!("Failed to resolve inode for deletion: path={}", path));
            }

            provider.delete_entry(inode, is_dir, &client_id).await.map_err(pfe_to_string)
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
            let entries = provider.list_entries(inode, limit, &client_id).await.map_err(pfe_to_string)?;
            Ok(entries.iter().map(traits_entry_to_proto).collect())
        })
    }

    pub fn assign_volume(
        &self,
        collection: &str,
        replication: &str,
    ) -> Result<(powerfs_common::types::Fid, Vec<powerfs_common::traits::Location>), String> {
        let facade = self.facade.clone();
        let collection = collection.to_string();
        let replication = replication.to_string();
        self.runtime.block_on(async move {
            let provider = crate::provider_adapter::FacadeVolumeProvider::new(facade);
            provider.assign_volume(&collection, &replication).await.map_err(pfe_to_string)
        })
    }

    pub fn lookup_volume(
        &self,
        volume_id: powerfs_common::types::VolumeId,
    ) -> Result<Vec<powerfs_common::traits::Location>, String> {
        let facade = self.facade.clone();
        let volume_addrs = facade.volume_addrs();

        self.runtime.block_on(async move {
            let provider = crate::provider_adapter::FacadeVolumeProvider::new(facade.clone());
            let locations_result = provider.lookup_volume(volume_id).await;

            let locations = match locations_result {
                Ok(locs) => {
                    if !locs.is_empty() {
                        if let Some(first) = locs.first() {
                            let url = first.url.clone();
                            log::debug!(
                                "lookup_volume: updating volume router volume={} -> {}",
                                volume_id.0, url
                            );
                            facade.volume_client().set_volume_info(volume_id.0 as u64, url);
                        }
                    }
                    locs
                }
                Err(e) => {
                    log::error!(
                        "lookup_volume: Master LookupVolume failed for volume_id={}: {}",
                        volume_id.0, pfe_to_string(e)
                    );
                    Vec::new()
                }
            };

            if locations.is_empty() && !volume_addrs.is_empty() {
                let addr = volume_addrs[0].clone();
                log::warn!(
                    "lookup_volume: FALLBACK to default volume address {} for volume_id={}",
                    addr, volume_id.0
                );
                facade.volume_client().set_volume_info(volume_id.0 as u64, addr.clone());
                Ok(vec![powerfs_common::traits::Location {
                    public_url: addr.clone(),
                    url: addr,
                    grpc_port: 0,
                    data_center: String::new(),
                }])
            } else {
                Ok(locations)
            }
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn write_blob(
        &self,
        volume_addr: &str,
        volume_id: u32,
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

    pub fn read_blob(
        &self,
        volume_addr: &str,
        volume_id: u32,
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

    pub fn delete_blob(&self, volume_id: u32, file_key: u64) -> Result<(), String> {
        let facade = self.facade.clone();
        self.runtime.block_on(async move {
            let provider = crate::provider_adapter::FacadeStorageProvider::new(facade);
            provider.delete_blob(volume_id, file_key).await.map_err(pfe_to_string)
        })
    }

    pub fn delete_data(
        &self,
        _volume_addr: &str,
        volume_id: u32,
        file_key: u64,
    ) -> Result<(), String> {
        let facade = self.facade.clone();
        self.runtime.block_on(async move {
            let provider = crate::provider_adapter::FacadeStorageProvider::new(facade);
            provider.delete_blob(volume_id, file_key).await.map_err(pfe_to_string)
        })
    }

    #[allow(clippy::type_complexity)]
    pub fn assign_fid(
        &self,
        collection: &str,
        replication: &str,
    ) -> Result<(powerfs_common::types::Fid, Option<powerfs_common::traits::Location>, Vec<String>, Vec<powerfs_common::traits::Location>), String> {
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
                .and_then(|d| {
                    let mut dec = powerfs_net::TlvDecoder::new(d);
                    Some(dec.next_string(powerfs_net::FieldId::SymlinkTarget).unwrap_or_default())
                })
                .or_else(|| {
                    result
                        .data
                        .as_deref()
                        .filter(|d| !d.is_empty())
                        .and_then(|d| {
                            let mut dec = powerfs_net::TlvDecoder::new(d);
                            Some(dec.next_string(powerfs_net::FieldId::SymlinkTarget).unwrap_or_default())
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_identity::ClientIdentity;

    #[test]
    fn test_facade_config_default() {
        let config = FuseClientFacadeConfig::default();
        assert_eq!(config.master_addr, "127.0.0.1");
        assert_eq!(config.master_port, 9333);
        assert_eq!(config.filer_addr, "127.0.0.1");
        assert_eq!(config.filer_port, 9343);
        assert_eq!(config.request_timeout, Duration::from_secs(5));
    }

    #[test]
    fn test_facade_config_custom() {
        let identity = ClientIdentity::new();
        let config = FuseClientFacadeConfig {
            master_addr: "192.168.1.100".to_string(),
            master_port: 9000,
            volume_net_port: 9002,
            volume_addrs: Vec::new(),
            filer_addr: "192.168.1.101".to_string(),
            filer_port: 9001,
            request_timeout: Duration::from_secs(10),
            client_identity: identity,
        };

        assert_eq!(config.master_addr, "192.168.1.100");
        assert_eq!(config.master_port, 9000);
        assert_eq!(config.request_timeout, Duration::from_secs(10));
    }
}
