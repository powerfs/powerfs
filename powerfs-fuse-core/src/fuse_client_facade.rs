use std::sync::Arc;
use std::time::Duration;

use crate::client_identity::ClientIdentity;
use crate::meta_shard_client::{
    default_msg_type_for_kind, MetaShardClient, MetaShardClientConfig, RequestResult,
};
use crate::net_client::{NetClientConfig, PowerFuseNetClient};
use crate::provider_adapter::parse_response_from_result;
use crate::request_id::RequestId;
use crate::request_state::{RequestContext, RequestKind};
use crate::topology::{ClusterTopologyManager, MasterClient, MasterClientConfig};
use crate::volume_client::{VolumeClient, VolumeClientConfig};

// 显式导入 Provider traits 以便在 SyncFuseClientFacade 中调用 provider 方法
use powerfs_common::traits::{
    Entry as TraitsEntry, EntryAttributes as TraitsEntryAttributes, FileChunk as TraitsFileChunk,
    MetadataProvider as _MetadataProvider, StorageProvider as _StorageProvider,
    VolumeProvider as _VolumeProvider,
};
use powerfs_master::proto::powerfs::{
    Entry as ProtoEntry, FileChunk as ProtoFileChunk, FuseAttributes as ProtoFuseAttributes,
};

/// 将 proto Entry 转换为 traits Entry（SyncFuseClientFacade 内部使用）
fn proto_entry_to_traits(entry: &ProtoEntry) -> TraitsEntry {
    let attributes = entry.attributes.as_ref().map(|a| TraitsEntryAttributes {
        ino: a.ino,
        mode: a.mode,
        uid: a.uid,
        gid: a.gid,
        atime: chrono::DateTime::from_timestamp(a.atime as i64, 0).unwrap_or_else(chrono::Utc::now),
        mtime: chrono::DateTime::from_timestamp(a.mtime as i64, 0).unwrap_or_else(chrono::Utc::now),
        ctime: chrono::DateTime::from_timestamp(a.ctime as i64, 0).unwrap_or_else(chrono::Utc::now),
        crtime: chrono::DateTime::from_timestamp(a.crtime as i64, 0)
            .unwrap_or_else(chrono::Utc::now),
    });

    let chunks = entry
        .chunks
        .iter()
        .map(|c| TraitsFileChunk {
            offset: c.offset,
            size: c.size,
            mtime: c.mtime,
            fid: c.fid.clone(),
            cookie: c.cookie,
            crc32: c.crc32,
        })
        .collect();

    TraitsEntry {
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
fn pfe_to_string(e: powerfs_common::error::PowerFsError) -> String {
    format!("{}", e)
}

/// 将 traits Entry 转换为 proto Entry（返回给 fuse.rs 使用）
fn traits_entry_to_proto(entry: &TraitsEntry) -> ProtoEntry {
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

/// FuseClientFacade 配置
#[derive(Debug, Clone)]
pub struct FuseClientFacadeConfig {
    /// Master 节点地址
    pub master_addr: String,
    /// Master 端口
    pub master_port: u16,
    /// Volume 网络端口（用于数据传输）
    pub volume_net_port: u16,
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
            filer_addr: "127.0.0.1".to_string(),
            filer_port: 9343,
            request_timeout: Duration::from_secs(5),
            client_identity: ClientIdentity::default(),
        }
    }
}

/// FuseClientFacade - 统一门面，协调 MasterClient、MetaShardClient、VolumeClient
pub struct FuseClientFacade {
    config: FuseClientFacadeConfig,
    /// 网络客户端
    net_client: Arc<PowerFuseNetClient>,
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
        // 创建网络客户端配置
        let net_config = NetClientConfig {
            master_addr: config.master_addr.clone(),
            master_net_port: config.master_port,
            volume_net_port: config.volume_net_port,
            filer_addr: config.filer_addr.clone(),
            filer_net_port: config.filer_port,
            client_id: config.client_identity.client_id,
            connect_timeout: Duration::from_secs(3),
            request_timeout: config.request_timeout,
        };

        // 创建网络客户端
        let net_client = Arc::new(
            PowerFuseNetClient::new(net_config)
                .await
                .map_err(|e| format!("Failed to create net client: {}", e))?,
        );

        Self::build_from_net_client(config, net_client).await
    }

    /// 从已有的 PowerFuseNetClient 构建 FuseClientFacade
    ///
    /// 用于已经建立 master/filer 连接的场景，复用现有 net_client。
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

        let mut master_client = MasterClient::new(master_client_config, topology_manager.clone());
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
            net_client,
            topology_manager,
            master_client,
            meta_shard_client,
            volume_client,
        };

        // 启动后台处理循环，确保 _and_wait 方法能实际处理请求
        facade.meta_shard_client.start_background_processor();
        facade.volume_client.start_background_processor();

        Ok(facade)
    }

    /// 获取网络客户端引用
    pub fn net_client(&self) -> &Arc<PowerFuseNetClient> {
        &self.net_client
    }

    /// 获取拓扑管理器引用
    pub fn topology_manager(&self) -> &Arc<ClusterTopologyManager> {
        &self.topology_manager
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

    // ======= 通用请求提交方法（支持指定 MsgType）=======

    /// 提交元数据请求（支持指定 MsgType）
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
            .map_err(|e| format!("Request failed: {}", e))
    }

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
            .map_err(|e| format!("Request failed: {}", e))
    }

    /// 提交数据请求 (读/写)
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
            .map_err(|e| format!("Request failed: {}", e))
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
            .map_err(|e| format!("Request failed: {}", e))
    }

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
            .map_err(|e| format!("Request failed: {}", e))
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

    /// 关闭所有客户端
    pub fn close(&self) {
        self.meta_shard_client.close();
        self.volume_client.close();
        self.master_client.disconnect();
        log::info!("FuseClientFacade: All clients closed");
    }
}

/// 同步门面适配器 - 将 FuseClientFacade 的异步接口包装为同步接口
///
/// 用于在 FUSE 同步回调上下文中使用异步的 FuseClientFacade。
/// 通过 tokio::runtime::Runtime::block_on() 将异步调用转为同步调用。
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
            let result = provider
                .get_entry_by_inode(inode)
                .await
                .map_err(pfe_to_string)?;
            Ok(result.map(|(e, p)| (traits_entry_to_proto(&e), p)))
        })
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

            // Resolve inode by path: try constructing full path
            let path = if parent_ino == 1 {
                format!("/{}", name)
            } else {
                // Try to get parent path via inode lookup
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

            // Look up the entry to get its inode
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
            let provider = crate::provider_adapter::FacadeVolumeProvider::new(facade);
            provider
                .assign_volume(&collection, &replication)
                .await
                .map_err(pfe_to_string)
        })
    }

    pub fn lookup_volume(
        &self,
        volume_id: powerfs_common::types::VolumeId,
    ) -> Result<Vec<powerfs_common::traits::Location>, String> {
        let facade = self.facade.clone();
        self.runtime.block_on(async move {
            let provider = crate::provider_adapter::FacadeVolumeProvider::new(facade);
            provider
                .lookup_volume(volume_id)
                .await
                .map_err(pfe_to_string)
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
            provider
                .read_blob(volume_id, file_key, offset, size)
                .await
                .map_err(pfe_to_string)
        })
    }

    pub fn delete_blob(&self, volume_id: u32, file_key: u64) -> Result<(), String> {
        let facade = self.facade.clone();
        self.runtime.block_on(async move {
            let provider = crate::provider_adapter::FacadeStorageProvider::new(facade);
            provider
                .delete_blob(volume_id, file_key)
                .await
                .map_err(pfe_to_string)
        })
    }

    /// 兼容 fuse.rs 的 delete_data 接口 - 直接调用 delete_blob
    pub fn delete_data(
        &self,
        _volume_addr: &str,
        volume_id: u32,
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

    /// 兼容 fuse.rs 的 assign_fid 接口
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

    /// 兼容 fuse.rs 的 symlink 接口
    pub fn symlink(&self, parent_ino: u64, name: &str, target: &str) -> Result<u64, String> {
        let facade = self.facade.clone();
        let name = name.to_string();
        let target = target.to_string();
        self.runtime.block_on(async move {
            let payload = serde_json::to_vec(&serde_json::json!({
                "parent_ino": parent_ino,
                "name": name,
                "target": target,
            }))
            .map_err(|e| format!("json encode failed: {}", e))?;

            let result = facade
                .submit_metadata_request_with_type(
                    crate::request_state::RequestKind::Metadata,
                    parent_ino,
                    payload,
                    powerfs_net::MsgType::Symlink,
                )
                .await
                .map_err(|e| format!("Facade symlink failed: {}", e))?;

            let response = parse_response_from_result(result)
                .map_err(|e| format!("parse response failed: {}", e))?;

            if !response.success {
                return Err(response
                    .error
                    .unwrap_or_else(|| "Symlink failed".to_string()));
            }

            response
                .data
                .as_ref()
                .and_then(|d| d.get("ino"))
                .and_then(|i| i.as_u64())
                .ok_or_else(|| "No inode in response".to_string())
        })
    }

    /// 兼容 fuse.rs 的 readlink 接口
    pub fn readlink(&self, inode: u64) -> Result<String, String> {
        let facade = self.facade.clone();
        self.runtime.block_on(async move {
            let payload = serde_json::to_vec(&serde_json::json!({
                "ino": inode,
            }))
            .map_err(|e| format!("json encode failed: {}", e))?;

            let result = facade
                .submit_metadata_request_with_type(
                    crate::request_state::RequestKind::Metadata,
                    inode,
                    payload,
                    powerfs_net::MsgType::Readlink,
                )
                .await
                .map_err(|e| format!("Facade readlink failed: {}", e))?;

            let response = parse_response_from_result(result)
                .map_err(|e| format!("parse response failed: {}", e))?;

            if !response.success {
                return Err(response
                    .error
                    .unwrap_or_else(|| "Readlink failed".to_string()));
            }

            let data = response
                .data
                .as_ref()
                .ok_or_else(|| "No data in response".to_string())?;
            let target = data
                .get("target")
                .and_then(|t| t.as_str())
                .ok_or_else(|| "No symlink target in response".to_string())?;
            Ok(target.to_string())
        })
    }

    /// 兼容 fuse.rs 的 link（硬链接）接口
    pub fn link(&self, inode: u64, newparent: u64, newname: &str) -> Result<bool, String> {
        let facade = self.facade.clone();
        let newname = newname.to_string();
        self.runtime.block_on(async move {
            let payload = serde_json::to_vec(&serde_json::json!({
                "ino": inode,
                "new_parent_ino": newparent,
                "new_name": newname,
            }))
            .map_err(|e| format!("json encode failed: {}", e))?;

            let result = facade
                .submit_metadata_request_with_type(
                    crate::request_state::RequestKind::Metadata,
                    newparent,
                    payload,
                    powerfs_net::MsgType::Link,
                )
                .await
                .map_err(|e| format!("Facade link failed: {}", e))?;

            let response = parse_response_from_result(result)
                .map_err(|e| format!("parse response failed: {}", e))?;

            Ok(response.success)
        })
    }

    /// 兼容 fuse.rs 的 rename 接口
    #[allow(clippy::too_many_arguments)]
    pub fn rename_entry(
        &self,
        old_parent_ino: u64,
        old_name: &str,
        new_parent_ino: u64,
        new_name: &str,
    ) -> Result<bool, String> {
        let facade = self.facade.clone();
        let old_name = old_name.to_string();
        let new_name = new_name.to_string();
        self.runtime.block_on(async move {
            let payload = serde_json::to_vec(&serde_json::json!({
                "old_parent_ino": old_parent_ino,
                "old_name": old_name,
                "new_parent_ino": new_parent_ino,
                "new_name": new_name,
            }))
            .map_err(|e| format!("json encode failed: {}", e))?;

            let result = facade
                .submit_metadata_request_with_type(
                    crate::request_state::RequestKind::Metadata,
                    new_parent_ino,
                    payload,
                    powerfs_net::MsgType::Rename,
                )
                .await
                .map_err(|e| format!("Facade rename failed: {}", e))?;

            let response = parse_response_from_result(result)
                .map_err(|e| format!("parse response failed: {}", e))?;

            Ok(response.success)
        })
    }

    /// Location -> URL 字符串 转换（兼容 fuse.rs）
    pub fn location_to_grpc_addr(loc: &powerfs_common::traits::Location) -> String {
        if loc.url.is_empty() {
            String::new()
        } else {
            loc.url.clone()
        }
    }
}

impl Drop for FuseClientFacade {
    fn drop(&mut self) {
        self.close();
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
