use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

/// 服务类型枚举 - 用于按服务类型校验配置
/// 每个服务只需配置自己的段落，其他段落可省略（使用默认空值）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceType {
    Master,
    Volume,
    Filer,
    S3,
    Fuse,
    Monitor,
}

/// PowerFS 主配置 - 每个服务只需提供自己段落的配置，其他段落可省略
/// （serde 会填充默认空值，validate_for 按 ServiceType 只校验本服务字段）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PowerFsConfig {
    #[serde(default)]
    pub global: GlobalConfig,
    #[serde(default)]
    pub master: MasterConfig,
    #[serde(default)]
    pub volume: VolumeConfig,
    #[serde(default)]
    pub filer: FilerConfig,
    #[serde(default)]
    pub s3: S3Config,
    #[serde(default)]
    pub fuse: FuseConfig,
    #[serde(default)]
    pub monitor: MonitorConfig,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GlobalConfig {
    pub log_level: String,
    pub log_file: Option<String>,
    pub redis_url: String,
}

/// Master 节点配置 - 所有端口和地址必须显式配置
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MasterConfig {
    /// HTTP/gRPC端口 - 必须配置
    pub port: u16,
    /// Raft gRPC端口 - 必须配置，必须与port不同
    /// RaftService (Vote/AppendEntries/Snapshot) 监听此端口
    pub raft_port: u16,
    /// Metrics/Admin HTTP 端口 - 必须配置，必须与 port/raft_port/net_port 都不同
    /// /metrics, /admin/log-level, /admin/debug 都监听此端口
    pub metrics_port: u16,
    /// 数据目录 - 必须配置
    pub dir: String,
    pub raft_dir: Option<String>,
    pub meta_dir: Option<String>,
    pub ip: Option<String>,
    pub advertise_addr: Option<String>,
    pub raft_id: u64,
    /// Raft 集群成员地址列表（ip:raft_port），包含自身
    pub raft_peers: Vec<String>,
    /// powerfs-net 二进制协议端口 - 必须配置，FUSE客户端通过此端口连接
    pub net_port: u16,
    /// Admin token for management API authentication (powerfs-cli, admin HTTP endpoints).
    /// If set, all admin API requests must carry `Authorization: Bearer <admin_token>`.
    /// If empty, admin APIs are open (insecure, for dev only).
    #[serde(default)]
    pub admin_token: Option<String>,
    /// Directory for CA certificate and key storage. Default: `{dir}/ca`.
    #[serde(default)]
    pub ca_dir: Option<String>,
    /// Registration token for authenticating Volume/Filer node registrations.
    /// Volume and Filer must send this token in their Heartbeat/RegisterFiler
    /// TLV requests. If empty/None, registrations are open (dev only).
    #[serde(default)]
    pub registration_token: Option<String>,
    /// Transport type for powerfs-net: "tcp" (default) or "rdma".
    /// When "rdma", the service's net_port listener uses RdmaTransport.
    /// Inter-service clients also use this transport for connections.
    #[serde(default)]
    pub transport: Option<String>,
    /// RDMA device name for powerfs-net (e.g. "rxe0", "mlx5_0").
    /// None = auto-select (picks the first ACTIVE port, preferring hardware
    /// RDMA; for Soft-RoCE (rxe) bridged setups, set this to "rxe0" to avoid
    /// choosing a hardware device whose subnet doesn't match the bind IP).
    #[serde(default)]
    pub rdma_device: Option<String>,
    /// #47 硬化: 为 true 时, 启动时检查二进制是否以 --features rdma 编译.
    /// 未编译 rdma feature 则 fatal error 拒绝启动, 防止 RDMA 部署中
    /// 误用 TCP-only 二进制导致 RDMA listener 缺失.
    #[serde(default)]
    pub require_rdma: bool,
}

/// Volume 节点配置 - 所有端口和地址必须显式配置
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VolumeConfig {
    /// gRPC端口 - 必须配置
    pub grpc_port: u16,
    /// HTTP管理端口 - 必须配置，必须与net_port不同
    pub http_port: u16,
    /// 数据目录 - 必须配置
    pub data_dir: String,
    /// Master地址列表 - 必须配置
    pub master_addresses: Vec<String>,
    pub node_id: String,
    pub max_volume_size: u64,
    /// 预创建卷数量
    #[serde(default = "default_initial_volume_count")]
    pub initial_volume_count: u32,
    /// 设备容量覆盖（可选，未设置时自动检测）
    pub device_capacity: Option<u64>,
    /// powerfs-net 二进制协议端口 - 必须配置，必须与http_port不同
    pub net_port: u16,
    /// Master的powerfs-net端口 (必填, 用于TLV心跳注册)
    pub master_net_port: u16,
    /// 广播地址 - Volume Server对外可达地址（如 "172.20.0.21"），用于Master注册volume路由
    /// 必须配置，不能使用0.0.0.0，否则FUSE客户端无法连接
    pub advertise_addr: Option<String>,
    /// Whether Volume Server supports range lease validation in write_needle.
    /// Set to `false` for backends that don't support lease (e.g., NVMe-oF target).
    /// When false, write_needle skips lease token validation; consistency is
    /// enforced by the Filer's lock_arbiter (§13 Cap model) instead.
    #[serde(default = "default_true")]
    pub lease_enabled: bool,
    /// Registration token for authenticating with the master on KeepConnected.
    /// Must match the master's expected token; empty = no auth (dev only).
    #[serde(default)]
    pub registration_token: Option<String>,
    /// CA 证书文件路径 (PEM)。生产模式下 master 有 CA 时必填。
    #[serde(default)]
    pub ca_crt: Option<String>,
    /// 客户端证书文件路径 (PEM)。生产模式下 master 有 CA 时必填。
    /// 内容读取后通过 TLV FieldId::ClientCert(0xD4) 嵌入 KeepConnected 请求。
    #[serde(default)]
    pub client_crt: Option<String>,
    /// 客户端证书私钥文件路径 (PEM)。保留用于 TLS 升级；
    /// 当前 TLV 0xD4 只发送公钥证书，私钥保存在本地即可。
    #[serde(default)]
    pub client_key: Option<String>,
    /// Transport type for powerfs-net: "tcp" (default) or "rdma".
    #[serde(default)]
    pub transport: Option<String>,
    /// RDMA device name override. See MasterConfig.rdma_device for details.
    #[serde(default)]
    pub rdma_device: Option<String>,
    /// #47 硬化: 为 true 时, 启动时检查二进制是否以 --features rdma 编译.
    #[serde(default)]
    pub require_rdma: bool,
}

/// Filer 节点配置 - 所有端口和地址必须显式配置
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FilerConfig {
    /// HTTP端口 - 必须配置
    pub port: u16,
    /// gRPC端口 - 必须配置
    pub grpc_port: u16,
    pub master_addresses: Vec<String>,
    pub ip: Option<String>,
    pub data_dir: String,
    pub shard_count: u32,
    pub raft_id: u64,
    pub raft_peers: Vec<String>,
    /// powerfs-net 二进制协议端口 - 必须配置
    pub net_port: u16,
    /// Master 的 powerfs-net 端口 (用于 Zone 注册等 TLV 通信) - 必须配置
    /// 注意: 与 master_addresses 中的端口 (HTTP/gRPC) 不同
    pub master_net_port: u16,
    /// 对外可达地址 (IP, 供 Master 注册和内核发现使用).
    /// 若未设置, 从 raft_peers[raft_id-1] 提取 IP.
    #[serde(default)]
    pub advertise_addr: Option<String>,
    /// CRDT 后台维护任务执行间隔（秒），默认 60 秒
    #[serde(default)]
    pub crdt_maintenance_interval_secs: Option<u64>,
    /// Phase 3.5: GC 后台任务执行间隔（秒），默认 300 秒
    #[serde(default)]
    pub gc_interval_secs: Option<u64>,
    /// Phase 3.5: GC grace period（秒），tombstone 标记后等待多久才可被物理删除，默认 86400 秒（24 小时）
    /// 所有 filer 节点必须配置相同的值，避免元数据不一致
    #[serde(default)]
    pub gc_grace_period_secs: Option<u64>,
    /// P2.5: Inline 小文件全局阈值 (字节). 0 = 禁用 (默认, 保持 Flat 行为).
    /// 大于 0 时, handle_create 对新文件返回 Placement::Inline, 跳过 Volume Server
    /// 分配, 数据直接存 Filer 元数据 (Raft 复制). 上限 8KB.
    /// 父目录 `powerfs.inline` xattr 可覆盖此值.
    #[serde(default)]
    pub inline_max_size: Option<u32>,
    /// 强制注册到 master，跳过 shard_count 一致性校验。
    ///
    /// 默认 false：master 拒绝 shard_count 与集群现有 filer 不一致的注册请求，
    /// filer 收到 BAD_REQUEST 后立即退出（exit 1），避免错误配置的节点进入集群
    /// 导致 inode 路由错位（% shard_count 模数不同 → inode not found / EIO）。
    ///
    /// 设为 true 仅用于运维场景：集群升级、临时调试 shard_count 不一致。
    /// 即使 force=true，master 仍会下发告警日志，便于事后审计。
    #[serde(default)]
    pub force_register: bool,
    /// Prometheus metrics HTTP server port. **Must be explicitly configured
    /// — no port-derivation shortcuts allowed.**  Exposes `/metrics`
    /// (Prometheus text format) for lease manager + MetaCache counters.
    /// Must be unique within the node (different from port / grpc_port /
    /// net_port).
    pub metrics_port: u16,
    /// Registration token for authenticating with the master on shard registration.
    /// Must match the master's expected token; empty = no auth (dev only).
    #[serde(default)]
    pub registration_token: Option<String>,
    /// CA 证书文件路径 (PEM)。生产模式下 master 有 CA 时必填。
    #[serde(default)]
    pub ca_crt: Option<String>,
    /// 客户端证书文件路径 (PEM)。生产模式下 master 有 CA 时必填。
    /// 内容读取后通过 TLV FieldId::ClientCert(0xD4) 嵌入 RegisterFiler 请求。
    #[serde(default)]
    pub client_crt: Option<String>,
    /// 客户端证书私钥文件路径 (PEM)。保留用于 TLS 升级；
    /// 当前 TLV 0xD4 只发送公钥证书，私钥保存在本地即可。
    #[serde(default)]
    pub client_key: Option<String>,
    /// Transport type for powerfs-net: "tcp" (default) or "rdma".
    #[serde(default)]
    pub transport: Option<String>,
    /// RDMA device name override. See MasterConfig.rdma_device for details.
    #[serde(default)]
    pub rdma_device: Option<String>,
    /// #47 硬化: 为 true 时, 启动时检查二进制是否以 --features rdma 编译.
    #[serde(default)]
    pub require_rdma: bool,
}

/// S3 服务配置 - 所有端口和地址必须显式配置
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct S3Config {
    /// 服务端口 - 必须配置
    pub port: u16,
    /// Master地址 - 必须配置（向后兼容；当 master_endpoints 为空时使用此地址）
    pub master_address: String,
    /// 所有 master gRPC 端点列表，用于 leader 发现和 failover。
    /// 为空时回退到 master_address 单点模式。
    #[serde(default)]
    pub master_endpoints: Vec<String>,
    pub ip: Option<String>,
    pub dir: String,
    pub access_key: String,
    pub secret_key: String,
}

/// FUSE 客户端配置 - 所有地址必须显式配置
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FuseConfig {
    pub mount_point: String,
    /// Master地址列表 - 必须配置
    pub master_addresses: Vec<String>,
    /// Filer地址列表 - 可选；为空时由 FUSE 客户端从 master 拓扑发现。
    ///
    /// 历史上必填；现在 master 通过 GetTopology 下发 filer 列表 + 全局
    /// shard_count，所以新部署只需 master_addresses。保留此字段作为：
    ///   1. 旧配置兼容；
    ///   2. force_mount=true 时的兜底地址；
    ///   3. 启动期到拓扑就绪之前的临时路由。
    #[serde(default)]
    pub filer_addresses: Vec<String>,
    /// Volume地址列表 - 可选；为空时由 FUSE 客户端从 master 拓扑发现。
    /// 新部署只需 master_addresses，volume 路由由 master GetTopology 下发。
    #[serde(default)]
    pub volume_addresses: Vec<String>,
    /// Master net端口 - 必须配置
    pub master_net_port: u16,
    /// Volume net端口 - 必须配置
    pub volume_net_port: u16,
    /// Filer net端口 - 必须配置
    pub filer_net_port: u16,
    pub collection: String,
    pub replication: String,
    pub threads: usize,
    pub verbose: bool,
    pub container: bool,
    pub log_file: Option<String>,
    /// Lease 模式配置 (可选，缺省为 range 模式)
    #[serde(default)]
    pub lease: LeaseConfig,
    /// 强制挂载：跳过拓扑健康检查（total_shards > 0 + 至少 1 个 healthy filer）。
    ///
    /// 默认 false：若 master 未下发 total_shards 或无 healthy filer，拒绝挂载
    /// 并退出。设为 true 时降级使用 filer_addresses 作为单分片兜底。
    /// 仅用于运维场景（master 临时不可达但需挂载）。
    #[serde(default)]
    pub force_mount: bool,
    /// 请求超时 (秒) — FUSE 对 master/filer/volume 的单次 RPC 超时。
    /// 默认 10s (生产)；测试环境建议设 3s 以快速暴露挂起问题。
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,
    /// Admin/debug HTTP server port (0 = disabled).
    ///
    /// When non-zero, FUSE starts a minimal HTTP server on this port exposing
    /// request statistics and in-flight request tracking for debugging hangs.
    /// Query via `powerfs-cli fuse-stats --addr <host>:<port>`.
    #[serde(default)]
    pub admin_port: u16,
    /// CA 证书文件路径 (PEM)。用于验证 master 返回的服务端证书；
    /// 当前保留用于未来 9334 TLS 升级。CLI `--ca-crt` 优先于此配置项。
    #[serde(default)]
    pub ca_crt: Option<String>,
    /// 客户端证书文件路径 (PEM)。生产模式下 master 有 CA 时必填。
    /// 内容读取后通过 TLV FieldId::ClientCert(0xD4) 嵌入 RegisterClient 请求。
    /// CLI `--client-crt` 优先于此配置项。
    #[serde(default)]
    pub client_crt: Option<String>,
    /// 客户端证书私钥文件路径 (PEM)。保留用于 9334 端口 TLS 升级；
    /// 当前 TLV 0xD4 只发送公钥证书，私钥保存在本地即可。
    /// CLI `--client-key` 优先于此配置项。
    #[serde(default)]
    pub client_key: Option<String>,
}

/// Lease 模式配置 — §13 Capability model 是唯一生产模式.
/// - "cap" (默认, §13 Capability): Filer 端 lock_arbiter 统一仲裁
///   FileLock/ScatterLock/SimpleLock/LocalLock 状态机, 提供
///   strong consistency (linearization) + GATHER 同步屏障.
///
/// 历史遗留 "range" 和 "inode" 模式已废弃: validate() 拒绝,
/// 相关代码已删除.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseConfig {
    /// Lease 模式: 仅允许 "cap"
    #[serde(default = "default_lease_mode")]
    pub mode: String,
    /// Lease 有效期 (毫秒) — 在 cap 模式下用作
    /// `DEFAULT_LEASE_DURATION_MS` (30s) 的 Filer 侧 cap holder
    /// TTL 参考值, 与 `DEFAULT_RECALL_TIMEOUT_MS` (2s) 独立.
    #[serde(default = "default_lease_duration_ms")]
    pub lease_duration_ms: u64,
    /// 续租间隔 (毫秒) — cap 模式下保留字段用于后续 "soft-cap"
    /// 主动续约模式; 当前 lock_arbiter 不发 renew, 仅靠 epoch
    /// fencing 驱动 recall, 因此此值仅作为配置兼容占位.
    #[serde(default = "default_renew_interval_ms")]
    pub renew_interval_ms: u64,
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self {
            mode: default_lease_mode(),
            lease_duration_ms: default_lease_duration_ms(),
            renew_interval_ms: default_renew_interval_ms(),
        }
    }
}

fn default_lease_mode() -> String {
    "cap".to_string()
}

fn default_lease_duration_ms() -> u64 {
    30000
}

fn default_renew_interval_ms() -> u64 {
    10000
}

fn default_request_timeout_secs() -> u64 {
    10
}

/// Monitor 服务配置 - 所有地址必须显式配置
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MonitorConfig {
    /// 服务监听地址 (如 "0.0.0.0:8081")
    pub addr: String,
    pub redis_url: String,
    pub s3_endpoint: String,
    pub s3_backend_endpoint: String,
    pub master_endpoint: String,
    /// 所有 master gRPC 端点列表，用于 leader 发现和 failover。
    /// 为空时回退到 master_endpoint 单点模式。
    #[serde(default)]
    pub master_endpoints: Vec<String>,
}

impl PowerFsConfig {
    /// 从配置文件加载 - 文件必须包含所有必需字段
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let content =
            fs::read_to_string(path).map_err(|e| ConfigError::ReadError(e.to_string()))?;
        let config: PowerFsConfig =
            toml::from_str(&content).map_err(|e| ConfigError::ParseError(e.to_string()))?;
        config.validate()?;
        Ok(config)
    }

    /// 从字符串加载配置
    pub fn load_from_string(content: &str) -> Result<Self, ConfigError> {
        let config: PowerFsConfig =
            toml::from_str(content).map_err(|e| ConfigError::ParseError(e.to_string()))?;
        config.validate()?;
        Ok(config)
    }

    /// 加载或报错（配置文件不存在或字段不全直接报错，不静默回退）
    pub fn load_or_error<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        if !path.exists() {
            return Err(ConfigError::ReadError(format!(
                "Configuration file not found: {}. \
                 You must provide a valid configuration file with all required ports and addresses.",
                path.display()
            )));
        }
        Self::load_from_file(path)
    }

    pub fn to_toml(&self) -> Result<String, ConfigError> {
        toml::to_string_pretty(self).map_err(|e| ConfigError::SerializeError(e.to_string()))
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), ConfigError> {
        let content = self.to_toml()?;
        fs::write(path, content).map_err(|e| ConfigError::WriteError(e.to_string()))
    }

    /// 严格校验 - 所有必需字段缺失时直接报错
    pub fn validate(&self) -> Result<(), ConfigError> {
        // === Master 校验 ===
        if self.master.port == 0 {
            return Err(ConfigError::ValidationError(
                "master.port must be set (> 0)".to_string(),
            ));
        }
        if self.master.raft_port == 0 {
            return Err(ConfigError::ValidationError(
                "master.raft_port must be set (> 0) for Raft inter-node gRPC".to_string(),
            ));
        }
        if self.master.net_port == 0 {
            return Err(ConfigError::ValidationError(
                "master.net_port must be set (> 0) for FUSE client connections".to_string(),
            ));
        }
        if self.master.port == self.master.net_port {
            return Err(ConfigError::ValidationError(
                "master.port and master.net_port must be different".to_string(),
            ));
        }
        if self.master.port == self.master.raft_port {
            return Err(ConfigError::ValidationError(
                "master.port and master.raft_port must be different".to_string(),
            ));
        }
        if self.master.net_port == self.master.raft_port {
            return Err(ConfigError::ValidationError(
                "master.net_port and master.raft_port must be different".to_string(),
            ));
        }
        if self.master.dir.is_empty() {
            return Err(ConfigError::ValidationError(
                "master.dir must be set".to_string(),
            ));
        }
        if self.master.raft_id == 0 {
            return Err(ConfigError::ValidationError(
                "master.raft_id must be set (> 0)".to_string(),
            ));
        }
        if self.master.raft_peers.is_empty() {
            return Err(ConfigError::ValidationError(
                "master.raft_peers must not be empty (at least one peer required for Raft cluster)"
                    .to_string(),
            ));
        }
        if self.master.ip.is_none() || self.master.ip.as_ref().unwrap().is_empty() {
            return Err(ConfigError::ValidationError(
                "master.ip must be set explicitly (e.g., '0.0.0.0' or specific bind IP)"
                    .to_string(),
            ));
        }
        if self.master.advertise_addr.is_none()
            || self.master.advertise_addr.as_ref().unwrap().is_empty()
        {
            return Err(ConfigError::ValidationError(
                "master.advertise_addr must be set explicitly (address used by other nodes to reach this master, e.g., '172.20.0.11:9333')".to_string(),
            ));
        }

        // === Volume 校验 ===
        if self.volume.grpc_port == 0 {
            return Err(ConfigError::ValidationError(
                "volume.grpc_port must be set (> 0)".to_string(),
            ));
        }
        if self.volume.http_port == 0 {
            return Err(ConfigError::ValidationError(
                "volume.http_port must be set (> 0)".to_string(),
            ));
        }
        if self.volume.net_port == 0 {
            return Err(ConfigError::ValidationError(
                "volume.net_port must be set (> 0) for FUSE client connections".to_string(),
            ));
        }
        if self.volume.http_port == self.volume.net_port {
            return Err(ConfigError::ValidationError(
                "volume.http_port and volume.net_port must be different (HTTP port conflicts with powerfs-net port)".to_string(),
            ));
        }
        if self.volume.master_net_port == 0 {
            return Err(ConfigError::ValidationError(
                "volume.master_net_port must be set (> 0) for TLV heartbeat".to_string(),
            ));
        }
        if self.volume.node_id.is_empty() {
            return Err(ConfigError::ValidationError(
                "volume.node_id must be set".to_string(),
            ));
        }
        if self.volume.master_addresses.is_empty() {
            return Err(ConfigError::ValidationError(
                "volume.master_addresses must not be empty".to_string(),
            ));
        }
        // 检查Master地址格式
        for addr in &self.volume.master_addresses {
            if !addr.contains(':') {
                return Err(ConfigError::ValidationError(format!(
                    "volume.master_addresses entry '{}' must be in host:port format",
                    addr
                )));
            }
        }

        // === Filer 校验 ===
        if self.filer.port == 0 {
            return Err(ConfigError::ValidationError(
                "filer.port must be set (> 0)".to_string(),
            ));
        }
        if self.filer.grpc_port == 0 {
            return Err(ConfigError::ValidationError(
                "filer.grpc_port must be set (> 0)".to_string(),
            ));
        }
        if self.filer.net_port == 0 {
            return Err(ConfigError::ValidationError(
                "filer.net_port must be set (> 0) for FUSE client connections".to_string(),
            ));
        }
        if self.filer.port == self.filer.net_port {
            return Err(ConfigError::ValidationError(
                "filer.port and filer.net_port must be different".to_string(),
            ));
        }
        if self.filer.master_addresses.is_empty() {
            return Err(ConfigError::ValidationError(
                "filer.master_addresses must not be empty".to_string(),
            ));
        }
        if self.filer.master_net_port == 0 {
            return Err(ConfigError::ValidationError(
                "filer.master_net_port must be set (> 0)".to_string(),
            ));
        }

        // === S3 校验 ===
        if self.s3.port == 0 {
            return Err(ConfigError::ValidationError(
                "s3.port must be set (> 0)".to_string(),
            ));
        }
        if self.s3.master_address.is_empty() {
            return Err(ConfigError::ValidationError(
                "s3.master_address must be set".to_string(),
            ));
        }

        // === FUSE 校验 ===
        if self.fuse.master_addresses.is_empty() {
            return Err(ConfigError::ValidationError(
                "fuse.master_addresses must not be empty".to_string(),
            ));
        }
        // filer_addresses 现在可选：为空时由 FUSE 客户端从 master 拓扑发现。
        // 但若 force_mount=true 且 filer_addresses 也为空，则无兜底地址，
        // 启动后会立即无法路由——此时仍需报错。
        if self.fuse.filer_addresses.is_empty() && self.fuse.force_mount {
            return Err(ConfigError::ValidationError(
                "fuse.filer_addresses must not be empty when force_mount=true \
                 (no fallback address available)"
                    .to_string(),
            ));
        }
        // volume_addresses 现在可选：为空时由 FUSE 客户端从 master 拓扑发现。
        // force_mount=true 时作为兜底地址必须提供。
        if self.fuse.volume_addresses.is_empty() && self.fuse.force_mount {
            return Err(ConfigError::ValidationError(
                "fuse.volume_addresses must not be empty when force_mount=true \
                 (no fallback address available)"
                    .to_string(),
            ));
        }
        if self.fuse.master_net_port == 0 {
            return Err(ConfigError::ValidationError(
                "fuse.master_net_port must be set (> 0)".to_string(),
            ));
        }
        if self.fuse.volume_net_port == 0 {
            return Err(ConfigError::ValidationError(
                "fuse.volume_net_port must be set (> 0)".to_string(),
            ));
        }
        if self.fuse.filer_net_port == 0 {
            return Err(ConfigError::ValidationError(
                "fuse.filer_net_port must be set (> 0)".to_string(),
            ));
        }
        if self.fuse.mount_point.is_empty() {
            return Err(ConfigError::ValidationError(
                "fuse.mount_point must be set".to_string(),
            ));
        }

        // === Lease 模式校验 ===
        let mode = &self.fuse.lease.mode;
        if mode != "cap" {
            return Err(ConfigError::ValidationError(format!(
                "fuse.lease.mode must be 'cap' (legacy 'range'/'inode' removed), got '{}'",
                mode
            )));
        }
        if self.fuse.lease.lease_duration_ms == 0 {
            return Err(ConfigError::ValidationError(
                "fuse.lease.lease_duration_ms must be > 0".to_string(),
            ));
        }
        if self.fuse.lease.renew_interval_ms == 0 {
            return Err(ConfigError::ValidationError(
                "fuse.lease.renew_interval_ms must be > 0".to_string(),
            ));
        }

        // === Monitor 校验 ===
        if self.monitor.addr.is_empty() {
            return Err(ConfigError::ValidationError(
                "monitor.addr must be set (e.g., '0.0.0.0:8081')".to_string(),
            ));
        }
        if self.monitor.redis_url.is_empty() {
            return Err(ConfigError::ValidationError(
                "monitor.redis_url must be set".to_string(),
            ));
        }
        if self.monitor.s3_endpoint.is_empty() {
            return Err(ConfigError::ValidationError(
                "monitor.s3_endpoint must be set".to_string(),
            ));
        }
        if self.monitor.master_endpoint.is_empty() {
            return Err(ConfigError::ValidationError(
                "monitor.master_endpoint must be set".to_string(),
            ));
        }

        Ok(())
    }

    /// 按服务类型校验 - 只校验指定服务的必需字段 + global 段
    /// 精简配置模式下，每个服务配置文件只需包含 [global] + 本服务段，
    /// 其他段使用默认空值，validate_for 只校验本服务字段。
    pub fn validate_for(&self, service: ServiceType) -> Result<(), ConfigError> {
        // global 段始终校验 redis_url (所有服务依赖 redis 事件)
        if self.global.redis_url.is_empty() {
            return Err(ConfigError::ValidationError(
                "global.redis_url must be set".to_string(),
            ));
        }

        match service {
            ServiceType::Master => self.validate_master(),
            ServiceType::Volume => self.validate_volume(),
            ServiceType::Filer => self.validate_filer(),
            ServiceType::S3 => self.validate_s3(),
            ServiceType::Fuse => self.validate_fuse(),
            ServiceType::Monitor => self.validate_monitor(),
        }
    }

    fn validate_master(&self) -> Result<(), ConfigError> {
        if self.master.port == 0 {
            return Err(ConfigError::ValidationError(
                "master.port must be set (> 0)".to_string(),
            ));
        }
        if self.master.raft_port == 0 {
            return Err(ConfigError::ValidationError(
                "master.raft_port must be set (> 0) for Raft inter-node gRPC".to_string(),
            ));
        }
        if self.master.net_port == 0 {
            return Err(ConfigError::ValidationError(
                "master.net_port must be set (> 0) for FUSE client connections".to_string(),
            ));
        }
        if self.master.port == self.master.net_port {
            return Err(ConfigError::ValidationError(
                "master.port and master.net_port must be different".to_string(),
            ));
        }
        if self.master.port == self.master.raft_port {
            return Err(ConfigError::ValidationError(
                "master.port and master.raft_port must be different".to_string(),
            ));
        }
        if self.master.net_port == self.master.raft_port {
            return Err(ConfigError::ValidationError(
                "master.net_port and master.raft_port must be different".to_string(),
            ));
        }
        if self.master.dir.is_empty() {
            return Err(ConfigError::ValidationError(
                "master.dir must be set".to_string(),
            ));
        }
        if self.master.raft_id == 0 {
            return Err(ConfigError::ValidationError(
                "master.raft_id must be set (> 0)".to_string(),
            ));
        }
        if self.master.raft_peers.is_empty() {
            return Err(ConfigError::ValidationError(
                "master.raft_peers must not be empty (at least one peer required for Raft cluster)"
                    .to_string(),
            ));
        }
        if self.master.ip.is_none() || self.master.ip.as_ref().unwrap().is_empty() {
            return Err(ConfigError::ValidationError(
                "master.ip must be set explicitly (e.g., '0.0.0.0' or specific bind IP)"
                    .to_string(),
            ));
        }
        if self.master.advertise_addr.is_none()
            || self.master.advertise_addr.as_ref().unwrap().is_empty()
        {
            return Err(ConfigError::ValidationError(
                "master.advertise_addr must be set explicitly (address used by other nodes to reach this master, e.g., '172.20.0.11:9333')".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_volume(&self) -> Result<(), ConfigError> {
        if self.volume.grpc_port == 0 {
            return Err(ConfigError::ValidationError(
                "volume.grpc_port must be set (> 0)".to_string(),
            ));
        }
        if self.volume.http_port == 0 {
            return Err(ConfigError::ValidationError(
                "volume.http_port must be set (> 0)".to_string(),
            ));
        }
        if self.volume.net_port == 0 {
            return Err(ConfigError::ValidationError(
                "volume.net_port must be set (> 0) for FUSE client connections".to_string(),
            ));
        }
        if self.volume.http_port == self.volume.net_port {
            return Err(ConfigError::ValidationError(
                "volume.http_port and volume.net_port must be different (HTTP port conflicts with powerfs-net port)".to_string(),
            ));
        }
        if self.volume.master_net_port == 0 {
            return Err(ConfigError::ValidationError(
                "volume.master_net_port must be set (> 0) for TLV heartbeat".to_string(),
            ));
        }
        if self.volume.node_id.is_empty() {
            return Err(ConfigError::ValidationError(
                "volume.node_id must be set".to_string(),
            ));
        }
        if self.volume.master_addresses.is_empty() {
            return Err(ConfigError::ValidationError(
                "volume.master_addresses must not be empty".to_string(),
            ));
        }
        for addr in &self.volume.master_addresses {
            if !addr.contains(':') {
                return Err(ConfigError::ValidationError(format!(
                    "volume.master_addresses entry '{}' must be in host:port format",
                    addr
                )));
            }
        }
        Ok(())
    }

    fn validate_filer(&self) -> Result<(), ConfigError> {
        if self.filer.port == 0 {
            return Err(ConfigError::ValidationError(
                "filer.port must be set (> 0)".to_string(),
            ));
        }
        if self.filer.grpc_port == 0 {
            return Err(ConfigError::ValidationError(
                "filer.grpc_port must be set (> 0)".to_string(),
            ));
        }
        if self.filer.net_port == 0 {
            return Err(ConfigError::ValidationError(
                "filer.net_port must be set (> 0) for FUSE client connections".to_string(),
            ));
        }
        if self.filer.port == self.filer.net_port {
            return Err(ConfigError::ValidationError(
                "filer.port and filer.net_port must be different".to_string(),
            ));
        }
        if self.filer.master_addresses.is_empty() {
            return Err(ConfigError::ValidationError(
                "filer.master_addresses must not be empty".to_string(),
            ));
        }
        if self.filer.master_net_port == 0 {
            return Err(ConfigError::ValidationError(
                "filer.master_net_port must be set (> 0)".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_s3(&self) -> Result<(), ConfigError> {
        if self.s3.port == 0 {
            return Err(ConfigError::ValidationError(
                "s3.port must be set (> 0)".to_string(),
            ));
        }
        if self.s3.master_address.is_empty() {
            return Err(ConfigError::ValidationError(
                "s3.master_address must be set".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_fuse(&self) -> Result<(), ConfigError> {
        if self.fuse.master_addresses.is_empty() {
            return Err(ConfigError::ValidationError(
                "fuse.master_addresses must not be empty".to_string(),
            ));
        }
        if self.fuse.filer_addresses.is_empty() && self.fuse.force_mount {
            return Err(ConfigError::ValidationError(
                "fuse.filer_addresses must not be empty when force_mount=true".to_string(),
            ));
        }
        if self.fuse.volume_addresses.is_empty() && self.fuse.force_mount {
            return Err(ConfigError::ValidationError(
                "fuse.volume_addresses must not be empty when force_mount=true".to_string(),
            ));
        }
        if self.fuse.master_net_port == 0 {
            return Err(ConfigError::ValidationError(
                "fuse.master_net_port must be set (> 0)".to_string(),
            ));
        }
        if self.fuse.volume_net_port == 0 {
            return Err(ConfigError::ValidationError(
                "fuse.volume_net_port must be set (> 0)".to_string(),
            ));
        }
        if self.fuse.filer_net_port == 0 {
            return Err(ConfigError::ValidationError(
                "fuse.filer_net_port must be set (> 0)".to_string(),
            ));
        }
        if self.fuse.mount_point.is_empty() {
            return Err(ConfigError::ValidationError(
                "fuse.mount_point must be set".to_string(),
            ));
        }
        let mode = &self.fuse.lease.mode;
        if mode != "cap" {
            return Err(ConfigError::ValidationError(format!(
                "fuse.lease.mode must be 'cap' (legacy 'range'/'inode' removed), got '{}'",
                mode
            )));
        }
        if self.fuse.lease.lease_duration_ms == 0 {
            return Err(ConfigError::ValidationError(
                "fuse.lease.lease_duration_ms must be > 0".to_string(),
            ));
        }
        if self.fuse.lease.renew_interval_ms == 0 {
            return Err(ConfigError::ValidationError(
                "fuse.lease.renew_interval_ms must be > 0".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_monitor(&self) -> Result<(), ConfigError> {
        if self.monitor.addr.is_empty() {
            return Err(ConfigError::ValidationError(
                "monitor.addr must be set (e.g., '0.0.0.0:8081')".to_string(),
            ));
        }
        if self.monitor.redis_url.is_empty() {
            return Err(ConfigError::ValidationError(
                "monitor.redis_url must be set".to_string(),
            ));
        }
        if self.monitor.s3_endpoint.is_empty() {
            return Err(ConfigError::ValidationError(
                "monitor.s3_endpoint must be set".to_string(),
            ));
        }
        if self.monitor.master_endpoint.is_empty() {
            return Err(ConfigError::ValidationError(
                "monitor.master_endpoint must be set".to_string(),
            ));
        }
        Ok(())
    }

    /// 为指定服务加载配置 - 只校验该服务的必需字段
    pub fn load_for_service<P: AsRef<Path>>(
        path: P,
        service: ServiceType,
    ) -> Result<Self, ConfigError> {
        let content =
            fs::read_to_string(path).map_err(|e| ConfigError::ReadError(e.to_string()))?;
        let config: PowerFsConfig =
            toml::from_str(&content).map_err(|e| ConfigError::ParseError(e.to_string()))?;
        config.validate_for(service)?;
        Ok(config)
    }

    /// 生成示例配置文件内容（用于参考，不会自动生效）
    pub fn generate_template() -> String {
        let template = r#"# PowerFS 配置文件模板
# 所有端口和地址必须显式设置，无默认值

[global]
log_level = "info"
redis_url = "redis://127.0.0.1:6379"

[master]
port = 9333              # HTTP/gRPC端口 (必填)
raft_port = 9335         # Raft gRPC端口 (必填，必须与port和net_port不同)
net_port = 9334          # powerfs-net端口 (必填，必须与port不同)
dir = "./data/master"    # 数据目录 (必填)
raft_id = 1
raft_peers = []

[volume]
grpc_port = 8080         # gRPC端口 (必填)
http_port = 8091         # HTTP管理端口 (必填，必须与net_port不同)
net_port = 8901          # powerfs-net端口 (必填，必须与http_port不同)
data_dir = "./data/volume"
master_addresses = ["172.20.0.11:9333", "172.20.0.12:9333", "172.20.0.13:9333"]
master_net_port = 9334   # Master的powerfs-net端口 (必填, 用于TLV心跳)
node_id = "volume-server-1"
max_volume_size = 107374182400   # 100GB (must be 100GB, not 10GB)
initial_volume_count = 4
lease_enabled = true          # Volume Server 是否支持 lease 验证 (NVMe-oF target 设为 false)

[filer]
port = 8888              # HTTP端口 (必填)
grpc_port = 8889         # gRPC端口 (必填)
net_port = 9334          # powerfs-net端口 (必填)
master_addresses = ["172.20.0.11:9333"]
master_net_port = 9334   # Master的powerfs-net端口 (必填, 用于Zone注册)
data_dir = "./data/filer"
shard_count = 2
raft_id = 1
raft_peers = []

[s3]
port = 9000              # 服务端口 (必填)
master_address = "172.20.0.11:9333"
# 所有 master gRPC 端点，用于 leader 发现和 failover（为空时回退到 master_address）
master_endpoints = ["172.20.0.11:9333", "172.20.0.12:9333", "172.20.0.13:9333"]
dir = "./data/s3"
access_key = "powerfs"
secret_key = "powerfs123"

[fuse]
mount_point = "/mnt/powerfs"
master_addresses = ["172.20.0.11", "172.20.0.12", "172.20.0.13"]  # (必填，3 个 master 用于 leader 发现和 failover)
filer_addresses = []                        # (可选，为空时由 master 拓扑发现)
volume_addresses = []                       # (可选，为空时由 master 拓扑发现)
master_net_port = 9334                       # (必填)
volume_net_port = 8901                       # (必填)
filer_net_port = 9334                        # (必填)
collection = "default"
replication = "000"
threads = 8
verbose = false
container = false
request_timeout_secs = 10          # 请求超时 (秒), 测试环境建议 3s

[fuse.lease]
mode = "cap"                 # §13 Capability 模型 (唯一允许值; 旧 "range"/"inode" 已废弃并被 validate 拒绝)
lease_duration_ms = 30000    # Filer 侧 cap holder TTL 参考值 (毫秒)
renew_interval_ms = 10000    # 保留字段: 后续 soft-cap 主动续约模式使用

[monitor]
addr = "0.0.0.0:8081"                      # (必填) 监听地址
redis_url = "redis://127.0.0.1:6379"
s3_endpoint = "http://127.0.0.1:9000"
s3_backend_endpoint = "http://127.0.0.1:9000"
master_endpoint = "http://127.0.0.1:9333"
"#;
        template.to_string()
    }
}

#[derive(Debug)]
pub enum ConfigError {
    ReadError(String),
    WriteError(String),
    ParseError(String),
    SerializeError(String),
    ValidationError(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::ReadError(e) => write!(f, "Failed to read config file: {}", e),
            ConfigError::WriteError(e) => write!(f, "Failed to write config file: {}", e),
            ConfigError::ParseError(e) => write!(f, "Failed to parse config file: {}", e),
            ConfigError::SerializeError(e) => write!(f, "Failed to serialize config: {}", e),
            ConfigError::ValidationError(e) => write!(f, "Config validation failed: {}", e),
        }
    }
}

impl std::error::Error for ConfigError {}

fn default_initial_volume_count() -> u32 {
    4
}

fn default_true() -> bool {
    true
}
