use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

/// PowerFS 主配置 - 必须通过配置文件提供所有必需值，无默认值
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PowerFsConfig {
    pub global: GlobalConfig,
    pub master: MasterConfig,
    pub volume: VolumeConfig,
    pub filer: FilerConfig,
    pub s3: S3Config,
    pub fuse: FuseConfig,
    pub monitor: MonitorConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalConfig {
    pub log_level: String,
    pub log_file: Option<String>,
    pub redis_url: String,
}

/// Master 节点配置 - 所有端口和地址必须显式配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MasterConfig {
    /// HTTP/gRPC端口 - 必须配置
    pub port: u16,
    /// 数据目录 - 必须配置
    pub dir: String,
    pub raft_dir: Option<String>,
    pub meta_dir: Option<String>,
    pub ip: Option<String>,
    pub advertise_addr: Option<String>,
    pub raft_id: u64,
    pub peers: Vec<String>,
    /// powerfs-net 二进制协议端口 - 必须配置，FUSE客户端通过此端口连接
    pub net_port: u16,
}

/// Volume 节点配置 - 所有端口和地址必须显式配置
#[derive(Debug, Clone, Serialize, Deserialize)]
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
}

/// Filer 节点配置 - 所有端口和地址必须显式配置
#[derive(Debug, Clone, Serialize, Deserialize)]
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
}

/// S3 服务配置 - 所有端口和地址必须显式配置
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FuseConfig {
    pub mount_point: String,
    /// Master地址列表 - 必须配置
    pub master_addresses: Vec<String>,
    /// Filer地址列表 - 必须配置
    pub filer_addresses: Vec<String>,
    /// Volume地址列表 - 必须配置
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
}

/// Monitor 服务配置 - 所有地址必须显式配置
#[derive(Debug, Clone, Serialize, Deserialize)]
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
        if self.master.peers.is_empty() {
            return Err(ConfigError::ValidationError(
                "master.peers must not be empty (at least one peer required for Raft cluster)"
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
        if self.fuse.filer_addresses.is_empty() {
            return Err(ConfigError::ValidationError(
                "fuse.filer_addresses must not be empty".to_string(),
            ));
        }
        if self.fuse.volume_addresses.is_empty() {
            return Err(ConfigError::ValidationError(
                "fuse.volume_addresses must not be empty".to_string(),
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

    /// 生成示例配置文件内容（用于参考，不会自动生效）
    pub fn generate_template() -> String {
        let template = r#"# PowerFS 配置文件模板
# 所有端口和地址必须显式设置，无默认值

[global]
log_level = "info"
redis_url = "redis://127.0.0.1:6379"

[master]
port = 9333              # HTTP/gRPC端口 (必填)
net_port = 9334          # powerfs-net端口 (必填，必须与port不同)
dir = "./data/master"    # 数据目录 (必填)
raft_id = 1
peers = []

[volume]
grpc_port = 8080         # gRPC端口 (必填)
http_port = 8091         # HTTP管理端口 (必填，必须与net_port不同)
net_port = 8901          # powerfs-net端口 (必填，必须与http_port不同)
data_dir = "./data/volume"
master_addresses = ["172.20.0.11:9333", "172.20.0.12:9333", "172.20.0.13:9333"]
master_net_port = 9334   # Master的powerfs-net端口 (必填, 用于TLV心跳)
node_id = "volume-server-1"
max_volume_size = 10737418240
initial_volume_count = 4

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
master_addresses = ["172.20.0.11"]          # (必填)
filer_addresses = ["172.20.0.35"]           # (必填)
volume_addresses = ["172.20.0.21", "172.20.0.22", "172.20.0.23"]  # (必填)
master_net_port = 9334                       # (必填)
volume_net_port = 8901                       # (必填)
filer_net_port = 9334                        # (必填)
collection = "default"
replication = "000"
threads = 8
verbose = false
container = false

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
