use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
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
            needle_id: c.needle_id,
            volume_id: c.volume_id,
            crc32: c.crc32,
            mtime: c.mtime,
        })
        .collect();

    let replica_chunks = entry
        .replica_chunks
        .iter()
        .map(|c| powerfs_common::traits::FileChunk {
            offset: c.offset,
            size: c.size,
            needle_id: c.needle_id,
            volume_id: c.volume_id,
            crc32: c.crc32,
            mtime: c.mtime,
        })
        .collect();

    powerfs_common::traits::Entry {
        name: entry.name.clone(),
        directory: entry.directory.clone(),
        attributes,
        chunks,
        replica_chunks,
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
            needle_id: c.needle_id,
            volume_id: c.volume_id,
            crc32: c.crc32,
            mtime: c.mtime,
        })
        .collect();

    let replica_chunks = entry
        .replica_chunks
        .iter()
        .map(|c| ProtoFileChunk {
            offset: c.offset,
            size: c.size,
            needle_id: c.needle_id,
            volume_id: c.volume_id,
            crc32: c.crc32,
            mtime: c.mtime,
        })
        .collect();

    ProtoEntry {
        name: entry.name.clone(),
        directory: entry.directory.clone(),
        attributes,
        chunks,
        replica_chunks,
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
    /// Mount point path (reported to master via TLV KeepConnected heartbeat).
    pub mount_point: String,
    /// Collection name (reported to master via KeepConnected heartbeat).
    pub collection: String,
    /// Replication placement (reported to master via KeepConnected heartbeat).
    pub replication: String,
    /// Lease mode: "range" (方案 D, default) or "inode" (方案 A).
    /// "range" — Volume Server manages per-stripe range lease.
    /// "inode" — Filer manages per-inode metadata lease (for backends
    ///          that don't support lease, e.g., NVMe-oF target).
    pub lease_mode: String,
    /// Lease duration in milliseconds (default 30000 = 30s).
    pub lease_duration_ms: u64,
    /// Lease background renew interval in milliseconds (default 10000 = 10s).
    pub lease_renew_interval_ms: u64,
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
            mount_point: String::new(),
            collection: String::new(),
            replication: String::new(),
            lease_mode: "range".to_string(),
            lease_duration_ms: 30_000,
            lease_renew_interval_ms: 10_000,
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

    /// 设置 Lease 模式 ("range" 或 "inode") 及相关参数。
    ///
    /// - "range" (方案 D, 默认): Volume Server 管理 per-stripe range lease
    /// - "inode" (方案 A):       Filer 管理 per-inode metadata lease
    pub fn with_lease_mode(mut self, mode: &str, duration_ms: u64, renew_interval_ms: u64) -> Self {
        self.lease_mode = mode.to_string();
        self.lease_duration_ms = duration_ms;
        self.lease_renew_interval_ms = renew_interval_ms;
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
    master_client: Arc<MasterClient>,
    /// MetaShard 客户端（Arc 包装：实现 DeltaSyncChannel trait）
    meta_shard_client: Arc<MetaShardClient>,
    /// Volume 客户端
    volume_client: Arc<VolumeClient>,
    /// 统一连接池 — Master/Filer/Volume 连接共享复用
    conn_pool: Arc<powerfs_net::ClientConnPool>,
    /// Master 统计上报器（KeepConnected 心跳）。
    /// 字段仅持有所有权以保持后台任务存活；Drop 时 `shutdown_tx` 会被释放，
    /// 上报循环检测到通道关闭后自动退出。
    #[allow(dead_code)]
    stats_reporter: Option<crate::stats_reporter::MasterStatsReporter>,
    /// Inode metadata lease cache (方案 A).
    ///
    /// Per-inode cache of Filer-managed leases. Keyed by inode number.
    /// Used only when `lease_mode == "inode"`. The cache avoids re-acquiring
    /// the lease on every write; `ensure_lease` checks validity and renews
    /// proactively when nearing expiry.
    inode_lease_cache: Arc<std::sync::Mutex<std::collections::HashMap<u64, InodeLeaseCacheEntry>>>,
}

/// Cached inode metadata lease entry (方案 A).
#[derive(Clone)]
struct InodeLeaseCacheEntry {
    token: String,
    expire_at: std::time::Instant,
}

impl InodeLeaseCacheEntry {
    fn is_valid(&self) -> bool {
        std::time::Instant::now() < self.expire_at
    }

    fn remaining(&self) -> Duration {
        self.expire_at
            .saturating_duration_since(std::time::Instant::now())
    }
}

impl FuseClientFacade {
    /// 创建新的 FuseClientFacade（不会自动连接，需要调用 connect）
    pub async fn new(config: FuseClientFacadeConfig) -> Result<Self, String> {
        // 创建拓扑管理器
        let topology_manager = Arc::new(ClusterTopologyManager::new());

        // 创建统一连接池 — Master/Filer/Volume 共享复用
        let pool_config = powerfs_net::ClientPoolConfig {
            request_timeout: config.request_timeout,
            ..Default::default()
        };
        let conn_pool = Arc::new(powerfs_net::ClientConnPool::new(
            config.client_identity.client_id,
            pool_config,
            None,
        ));

        // 创建 Master 客户端
        let master_client_config = MasterClientConfig {
            master_addrs: vec![format!("{}:{}", config.master_addr, config.master_port)],
            request_timeout: config.request_timeout,
            max_retries: 3,
            circuit_breaker_config: crate::circuit_breaker::CircuitBreakerConfig::default(),
        };

        let master_client = Arc::new(MasterClient::new(
            master_client_config,
            topology_manager.clone(),
        ));

        // 创建 MetaShard 客户端
        let meta_config = MetaShardClientConfig::default();
        let meta_shard_client = MetaShardClient::new(
            meta_config,
            topology_manager.clone(),
            config.client_identity.client_id,
            conn_pool.clone(),
        );

        // 创建 Volume 客户端
        // CRITICAL: sync volume_config.client_id with client_identity.client_id so
        // that background lease renewal uses the same client_id as acquire/release.
        // Otherwise the Volume Server rejects renewals with "Lease holder mismatch".
        let volume_config = VolumeClientConfig {
            client_id: config.client_identity.client_id.to_string(),
            ..Default::default()
        };
        let volume_client = Arc::new(VolumeClient::new(
            volume_config,
            topology_manager.clone(),
            conn_pool.clone(),
        ));

        Ok(Self {
            config,
            topology_manager,
            master_client,
            meta_shard_client: Arc::new(meta_shard_client),
            volume_client,
            conn_pool,
            stats_reporter: None,
            inode_lease_cache: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        })
    }

    /// 从配置构建 FuseClientFacade（推荐使用）
    ///
    /// 统一连接池由 FuseClientFacade 持有，MetaShardClient 与 VolumeClient
    /// 共享同一 `ClientConnPool`，避免连接管理重复实现。
    pub async fn build_from_config(config: FuseClientFacadeConfig) -> Result<Self, String> {
        // 创建拓扑管理器
        let topology_manager = Arc::new(ClusterTopologyManager::new());

        // 创建统一连接池 — Master/Filer/Volume 共享复用
        // 通知处理器在 fuse.rs 中创建 InvalidateHandler 后通过
        // `set_notification_handler` 安装（pool 已支持运行时替换）。
        let pool_config = powerfs_net::ClientPoolConfig {
            request_timeout: config.request_timeout,
            ..Default::default()
        };
        let conn_pool = Arc::new(powerfs_net::ClientConnPool::new(
            config.client_identity.client_id,
            pool_config,
            None,
        ));

        // 创建 Master 客户端（会自动创建自己的网络连接）
        let master_client_config = MasterClientConfig {
            master_addrs: vec![format!("{}:{}", config.master_addr, config.master_port)],
            request_timeout: config.request_timeout,
            max_retries: 3,
            circuit_breaker_config: crate::circuit_breaker::CircuitBreakerConfig::default(),
        };

        let master_client = Arc::new(MasterClient::new(
            master_client_config,
            topology_manager.clone(),
        ));

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

        // 创建 MetaShard 客户端（共享连接池）
        let meta_config = MetaShardClientConfig::default();
        let meta_shard_client = MetaShardClient::new(
            meta_config,
            topology_manager.clone(),
            config.client_identity.client_id,
            conn_pool.clone(),
        );
        meta_shard_client
            .set_default_filer_addr(format!("{}:{}", config.filer_addr, config.filer_port));
        meta_shard_client.init();

        // 创建 Volume 客户端（共享连接池）
        // CRITICAL: sync volume_config.client_id with client_identity.client_id so
        // that background lease renewal uses the same client_id as acquire/release.
        // Otherwise the Volume Server rejects renewals with "Lease holder mismatch".
        let volume_config = VolumeClientConfig {
            client_id: config.client_identity.client_id.to_string(),
            ..Default::default()
        };
        let volume_client = Arc::new(VolumeClient::new(
            volume_config,
            topology_manager.clone(),
            conn_pool.clone(),
        ));

        // 设置默认 Volume 地址（从配置获取）
        if !config.volume_addrs.is_empty() {
            volume_client.set_default_volume_addrs(config.volume_addrs.clone());
            log::info!(
                "FuseClientFacade: set default volume addrs: {:?}",
                config.volume_addrs
            );
        }

        volume_client.init();

        // 启动连接池后台健康检查（ping + 自动重连）
        conn_pool.start_health_check();

        // 启动 Master 统计上报器（TLV KeepConnected 心跳 + 拓扑变更通知）
        let reporter_config = crate::stats_reporter::StatsReporterConfig {
            client_id: config.client_identity.client_id.to_string(),
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
            master_client.clone(),
            topology_manager.clone(),
        );
        reporter.start();
        log::info!(
            "FuseClientFacade: MasterStatsReporter started (TLV KeepConnected, client_id={})",
            config.client_identity.client_id
        );
        let stats_reporter = Some(reporter);

        let facade = Self {
            config,
            topology_manager,
            master_client,
            meta_shard_client: Arc::new(meta_shard_client),
            volume_client,
            conn_pool,
            stats_reporter,
            inode_lease_cache: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        };

        // 启动后台处理循环
        facade.meta_shard_client.start_background_processor();
        facade.volume_client.start_background_processor();

        Ok(facade)
    }

    /// 获取共享连接池引用（用于安装通知处理器等）
    pub fn conn_pool(&self) -> &Arc<powerfs_net::ClientConnPool> {
        &self.conn_pool
    }

    /// 获取 Master 客户端引用
    pub fn master_client(&self) -> &Arc<MasterClient> {
        &self.master_client
    }

    /// 获取 MetaShard 客户端引用（Arc 包装，便于注入 DeltaSyncChannel）
    pub fn meta_shard_client(&self) -> &Arc<MetaShardClient> {
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

    /// 获取客户端 u64 ID（用于 lease 标识与元数据同步）
    pub fn client_id_u64(&self) -> u64 {
        self.config.client_identity.client_id
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
    pub fn get_valid_lease_token(
        &self,
        volume_id: u64,
        inode: u64,
        stripe_start: u64,
    ) -> Option<String> {
        self.volume_client
            .get_valid_lease_token(volume_id, inode, stripe_start)
    }

    /// 获取指定 inode 的指定 stripe 的 lease 剩余时间（委托给 VolumeClient）。
    pub fn get_lease_remaining(
        &self,
        volume_id: u64,
        inode: u64,
        stripe_start: u64,
    ) -> Option<Duration> {
        self.volume_client
            .get_lease_remaining(volume_id, inode, stripe_start)
    }

    /// 更新 lease 缓存（委托给 VolumeClient）
    pub fn update_lease(
        &self,
        volume_id: u64,
        inode: u64,
        stripe_start: u64,
        token: String,
        duration: Duration,
    ) {
        self.volume_client
            .update_lease(volume_id, inode, stripe_start, token, duration);
    }

    /// 异步续租 Lease（委托给 VolumeClient）。
    ///
    /// 在 lease 即将过期但仍在有效期内时调用，延长 lease 的过期时间，
    /// 避免写操作中途 lease 过期导致服务端校验失败。
    pub async fn renew_lease(
        &self,
        volume_id: u64,
        inode: u64,
        stripe_start: u64,
        token: &str,
    ) -> Result<(), String> {
        self.volume_client
            .renew_lease(volume_id, inode, stripe_start, token)
            .await
            .map_err(|e| format!("RenewLease failed: {}", e))
    }

    // ======= Inode Metadata Lease (方案 A, Phase 2) =======
    //
    // Filer-managed per-inode exclusive lease. Delegated to MetaShardClient
    // (which talks to the Filer shard leader via powerfs-net).
    // Used when lease_mode == "inode" (e.g., NVMe-oF target backend that
    // doesn't support Volume Server range lease).

    /// 当前 Lease 模式: "range" (方案 D) 或 "inode" (方案 A)。
    pub fn lease_mode(&self) -> &str {
        &self.config.lease_mode
    }

    /// Lease 有效期 (毫秒)。
    pub fn lease_duration_ms(&self) -> u64 {
        self.config.lease_duration_ms
    }

    /// 是否使用 inode metadata lease (方案 A)。
    pub fn is_inode_lease_mode(&self) -> bool {
        self.config.lease_mode == "inode"
    }

    /// 获取 inode metadata lease (方案 A)。委托给 MetaShardClient → Filer。
    ///
    /// 成功后自动缓存 token，后续 `get_valid_inode_lease_token` 可直接命中。
    /// 返回 `(token, expire_at_ms)`。token 用于后续 release/renew。
    pub async fn acquire_inode_lease(
        &self,
        inode: u64,
        client_id: &str,
        duration_ms: u64,
    ) -> Result<(String, u64), String> {
        let result = self
            .meta_shard_client
            .acquire_inode_lease(inode, client_id, duration_ms)
            .await?;
        // Auto-cache the lease
        self.update_inode_lease(inode, &result.0, Duration::from_millis(duration_ms));
        Ok(result)
    }

    /// 释放 inode metadata lease (方案 A)。委托给 MetaShardClient → Filer。
    /// 成功后自动清除缓存。
    pub async fn release_inode_lease(
        &self,
        inode: u64,
        client_id: &str,
        token: &str,
    ) -> Result<(), String> {
        self.meta_shard_client
            .release_inode_lease(inode, client_id, token)
            .await?;
        // Auto-invalidate cache
        self.invalidate_inode_lease(inode);
        Ok(())
    }

    /// 续租 inode metadata lease (方案 A)。委托给 MetaShardClient → Filer。
    /// 成功后自动更新缓存过期时间。
    pub async fn renew_inode_lease(
        &self,
        inode: u64,
        client_id: &str,
        token: &str,
        duration_ms: u64,
    ) -> Result<(), String> {
        self.meta_shard_client
            .renew_inode_lease(inode, client_id, token, duration_ms)
            .await?;
        // Auto-update cache expiry
        self.update_inode_lease(inode, token, Duration::from_millis(duration_ms));
        Ok(())
    }

    // ------- Inode lease cache management (方案 A) -------

    /// Get a valid cached inode lease token + remaining duration.
    /// Returns None if not cached or expired.
    pub fn get_valid_inode_lease_token(&self, inode: u64) -> Option<(String, Duration)> {
        let cache = self.inode_lease_cache.lock().unwrap();
        cache.get(&inode).and_then(|entry| {
            if entry.is_valid() {
                Some((entry.token.clone(), entry.remaining()))
            } else {
                None
            }
        })
    }

    /// Cache an inode lease token after successful acquire/renew.
    pub fn update_inode_lease(&self, inode: u64, token: &str, duration: Duration) {
        let mut cache = self.inode_lease_cache.lock().unwrap();
        cache.insert(
            inode,
            InodeLeaseCacheEntry {
                token: token.to_string(),
                expire_at: std::time::Instant::now() + duration,
            },
        );
    }

    /// Remove an inode lease from the cache (after release or on error).
    pub fn invalidate_inode_lease(&self, inode: u64) {
        let mut cache = self.inode_lease_cache.lock().unwrap();
        cache.remove(&inode);
    }

    /// 获取指定 inode 的所有有效 stripe lease token（委托给 VolumeClient）
    pub fn get_all_valid_lease_tokens_for_inode(
        &self,
        volume_id: u64,
        inode: u64,
    ) -> Vec<(u64, String)> {
        self.volume_client
            .get_all_valid_lease_tokens_for_inode(volume_id, inode)
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

    /// 直接发送 WriteNeedle 请求（绕过 data_queue，避免 block_on 死锁）
    ///
    /// FUSE write 回调在同步线程中通过 block_on 调用 write_blob_with_lease，
    /// 若走 data_queue（异步队列+spawn），响应需要 tokio worker 调度才能回到
    /// block_on 的 waiter。block_on 阻塞 worker 后形成死锁，10s 超时。
    /// 直接发送用 vol_client.send_request，recv_loop 是独立 task 不受影响。
    pub async fn send_write_needle_direct(
        &self,
        volume_id: u64,
        payload: Vec<u8>,
        data: &[u8],
    ) -> Result<Vec<u8>, String> {
        self.volume_client
            .send_write_needle_direct(volume_id, payload, data)
            .await
            .map_err(|e| format!("Direct WriteNeedle failed: {}", e))
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
    ///
    /// token 由调用方传入，避免从 leases 表查到错误 token。
    /// 传空字符串则内部从 leases 表取（兼容旧路径）。
    pub async fn release_lease(
        &self,
        volume_id: u64,
        inode: u64,
        stripe_start: u64,
        client_id: &str,
        token: &str,
    ) -> Result<(), String> {
        self.volume_client
            .release_lease_remote(volume_id, inode, stripe_start, client_id, token)
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
        // 停止连接池后台健康检查；连接本身由各 PowerFsNetClient Drop 时清理
        self.conn_pool.stop_health_check();
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

/// Parameters for a single chunk write in a batch flush.
#[derive(Clone)]
pub struct WriteBlobRequest {
    pub volume_id: u64,
    pub file_key: u64,
    pub inode: u64,
    pub offset: i64,
    pub size: i32,
    pub data: Bytes,
}

/// Parameters for a single chunk read in a batch fetch.
#[derive(Clone)]
pub struct ReadBlobRequest {
    pub volume_id: u64,
    pub file_key: u64,
    pub offset: i64,
    pub size: i32,
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

    /// 同步桥接异步 future（不占用 tokio worker 线程）
    ///
    /// 通过 handle.spawn 将 future 提交到 tokio runtime，当前线程在
    /// mpsc::channel 上阻塞等待结果。这样 tokio worker 可以自由调度
    /// data_queue processor、send_task、recv_loop 等 spawn task，
    /// 避免 block_on 占用 worker 导致的调度争用和超时。
    ///
    /// 详见 docs/communication-optimization-plan.md §12 阶段1.5
    pub fn block_on<F: std::future::Future + Send + 'static>(&self, future: F) -> F::Output
    where
        F::Output: Send + 'static,
    {
        let handle = self.runtime.handle().clone();
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        handle.spawn(async move {
            let result = future.await;
            let _ = tx.send(result);
        });
        rx.recv()
            .expect("block_on: future panicked or runtime dropped")
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

    /// Create an entry with a known parent inode, avoiding path resolution.
    pub fn create_entry_with_parent_ino(
        &self,
        entry: &ProtoEntry,
        parent_ino: u64,
        client_id: &str,
    ) -> Result<u64, String> {
        let facade = self.facade.clone();
        let traits_entry = proto_entry_to_traits(entry);
        let client_id = client_id.to_string();
        self.runtime.block_on(async move {
            let provider = crate::provider_adapter::FacadeMetadataProvider::new(facade);
            provider
                .create_entry_with_parent_ino(&traits_entry, parent_ino, &client_id)
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
    ///
    /// token 由调用方传入（LeaseGuard 持有的 token 或空字符串由内部查表）。
    pub fn release_lease(
        &self,
        volume_id: u64,
        inode: u64,
        stripe_start: u64,
        client_id: &str,
        token: &str,
    ) -> Result<(), String> {
        let facade = self.facade.clone();
        let client_id = client_id.to_string();
        let token = token.to_string();
        self.runtime.block_on(async move {
            facade
                .release_lease(volume_id, inode, stripe_start, &client_id, &token)
                .await
        })
    }

    /// 获取有效的 lease token（委托给 Facade → VolumeClient）
    pub fn get_valid_lease_token(
        &self,
        volume_id: u64,
        inode: u64,
        stripe_start: u64,
    ) -> Option<String> {
        self.facade
            .get_valid_lease_token(volume_id, inode, stripe_start)
    }

    /// 获取指定 inode 的所有有效 stripe lease token（委托给 Facade → VolumeClient）
    pub fn get_all_valid_lease_tokens_for_inode(
        &self,
        volume_id: u64,
        inode: u64,
    ) -> Vec<(u64, String)> {
        self.facade
            .get_all_valid_lease_tokens_for_inode(volume_id, inode)
    }

    // ======= Inode Metadata Lease sync wrappers (方案 A, Phase 2) =======

    /// 当前 Lease 模式: "range" (方案 D) 或 "inode" (方案 A)。
    pub fn lease_mode(&self) -> &str {
        self.facade.lease_mode()
    }

    /// Lease 有效期 (毫秒)。
    pub fn lease_duration_ms(&self) -> u64 {
        self.facade.lease_duration_ms()
    }

    /// 是否使用 inode metadata lease (方案 A)。
    pub fn is_inode_lease_mode(&self) -> bool {
        self.facade.is_inode_lease_mode()
    }

    /// Get a valid cached inode lease token + remaining duration (方案 A).
    /// Synchronous — just reads from the in-memory cache.
    pub fn get_valid_inode_lease_token(&self, inode: u64) -> Option<(String, std::time::Duration)> {
        self.facade.get_valid_inode_lease_token(inode)
    }

    /// Remove an inode lease from the cache (方案 A). Synchronous.
    pub fn invalidate_inode_lease(&self, inode: u64) {
        self.facade.invalidate_inode_lease(inode)
    }

    /// 同步获取 inode metadata lease (方案 A)。
    /// 返回 `(token, expire_at_ms)`。
    pub fn acquire_inode_lease(
        &self,
        inode: u64,
        client_id: &str,
        duration_ms: u64,
    ) -> Result<(String, u64), String> {
        let facade = self.facade.clone();
        let client_id = client_id.to_string();
        self.runtime.block_on(async move {
            facade
                .acquire_inode_lease(inode, &client_id, duration_ms)
                .await
        })
    }

    /// 同步释放 inode metadata lease (方案 A)。
    pub fn release_inode_lease(
        &self,
        inode: u64,
        client_id: &str,
        token: &str,
    ) -> Result<(), String> {
        let facade = self.facade.clone();
        let client_id = client_id.to_string();
        let token = token.to_string();
        self.runtime
            .block_on(async move { facade.release_inode_lease(inode, &client_id, &token).await })
    }

    /// 同步续租 inode metadata lease (方案 A)。
    pub fn renew_inode_lease(
        &self,
        inode: u64,
        client_id: &str,
        token: &str,
        duration_ms: u64,
    ) -> Result<(), String> {
        let facade = self.facade.clone();
        let client_id = client_id.to_string();
        let token = token.to_string();
        self.runtime.block_on(async move {
            facade
                .renew_inode_lease(inode, &client_id, &token, duration_ms)
                .await
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

    /// Batch write multiple chunks in parallel using tokio::join_all.
    /// Each chunk is sent concurrently (up to the runtime's thread pool),
    /// reducing total flush time from N×latency to ~1×latency.
    pub fn write_blob_batch_with_lease(
        &self,
        requests: Vec<WriteBlobRequest>,
        lease_token: Option<&str>,
    ) -> Vec<Result<(), String>> {
        let facade = self.facade.clone();
        let lease_owned = lease_token.map(|s| s.to_string());
        self.runtime.block_on(async move {
            let futures: Vec<_> = requests
                .into_iter()
                .map(|req| {
                    let facade = facade.clone();
                    let lease_ref = lease_owned.as_deref();
                    async move {
                        let provider = crate::provider_adapter::FacadeStorageProvider::new(facade);
                        provider
                            .write_blob_with_lease(
                                req.volume_id,
                                req.file_key,
                                req.inode,
                                req.offset,
                                req.size,
                                &req.data,
                                lease_ref,
                            )
                            .await
                            .map_err(pfe_to_string)
                    }
                })
                .collect();
            futures::future::join_all(futures).await
        })
    }

    /// Batch read multiple chunks in parallel using tokio::join_all.
    pub fn read_blob_batch(&self, requests: Vec<ReadBlobRequest>) -> Vec<Result<Vec<u8>, String>> {
        let facade = self.facade.clone();
        self.runtime.block_on(async move {
            let futures: Vec<_> = requests
                .into_iter()
                .map(|req| {
                    let facade = facade.clone();
                    async move {
                        let provider = crate::provider_adapter::FacadeStorageProvider::new(facade);
                        provider
                            .read_blob(req.volume_id, req.file_key, req.offset, req.size)
                            .await
                            .map_err(pfe_to_string)
                    }
                })
                .collect();
            futures::future::join_all(futures).await
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
