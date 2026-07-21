use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
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
#[serde(default)]
pub struct GlobalConfig {
    pub log_level: String,
    pub log_file: Option<String>,
    pub redis_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MasterConfig {
    pub port: u16,
    pub dir: String,
    pub raft_dir: Option<String>,
    pub meta_dir: Option<String>,
    pub ip: Option<String>,
    pub advertise_addr: Option<String>,
    pub raft_id: u64,
    pub peers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct VolumeConfig {
    pub grpc_port: u16,
    pub http_port: u16,
    pub data_dir: String,
    pub master_addresses: Vec<String>,
    pub node_id: String,
    pub max_volume_size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct FilerConfig {
    pub port: u16,
    pub grpc_port: u16,
    pub master_addresses: Vec<String>,
    pub ip: Option<String>,
    pub data_dir: String,
    pub shard_count: u32,
    pub raft_id: u64,
    pub raft_peers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct S3Config {
    pub port: u16,
    pub master_address: String,
    pub ip: Option<String>,
    pub dir: String,
    pub access_key: String,
    pub secret_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct FuseConfig {
    pub mount_point: String,
    pub master_addresses: Vec<String>,
    pub collection: String,
    pub replication: String,
    pub threads: usize,
    pub verbose: bool,
    pub container: bool,
    pub log_file: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MonitorConfig {
    pub redis_url: String,
    pub s3_endpoint: String,
    pub s3_backend_endpoint: String,
    pub master_endpoint: String,
}

impl Default for GlobalConfig {
    fn default() -> Self {
        Self {
            log_level: "info".to_string(),
            log_file: None,
            redis_url: "redis://127.0.0.1:6379".to_string(),
        }
    }
}

impl Default for MasterConfig {
    fn default() -> Self {
        Self {
            port: 9333,
            dir: "./data/master".to_string(),
            raft_dir: None,
            meta_dir: None,
            ip: None,
            advertise_addr: None,
            raft_id: 1,
            peers: Vec::new(),
        }
    }
}

impl Default for VolumeConfig {
    fn default() -> Self {
        Self {
            grpc_port: 8080,
            http_port: 8090,
            data_dir: "./data/volume".to_string(),
            master_addresses: vec!["http://localhost:9333".to_string()],
            node_id: "volume-server".to_string(),
            max_volume_size: 1073741824,
        }
    }
}

impl Default for FilerConfig {
    fn default() -> Self {
        Self {
            port: 8888,
            grpc_port: 8889,
            master_addresses: vec!["http://localhost:9333".to_string()],
            ip: None,
            data_dir: "./data/filer".to_string(),
            shard_count: 4,
            raft_id: 1,
            raft_peers: Vec::new(),
        }
    }
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            port: 9000,
            master_address: "http://localhost:9333".to_string(),
            ip: None,
            dir: "./data/s3".to_string(),
            access_key: "powerfs".to_string(),
            secret_key: "powerfs123".to_string(),
        }
    }
}

impl Default for FuseConfig {
    fn default() -> Self {
        Self {
            mount_point: "/mnt/powerfs".to_string(),
            master_addresses: vec!["http://localhost:9333".to_string()],
            collection: "default".to_string(),
            replication: "000".to_string(),
            threads: 8,
            verbose: false,
            container: false,
            log_file: None,
        }
    }
}

impl Default for MonitorConfig {
    fn default() -> Self {
        Self {
            redis_url: "redis://127.0.0.1:6379".to_string(),
            s3_endpoint: "http://localhost:9000".to_string(),
            s3_backend_endpoint: "http://localhost:9000".to_string(),
            master_endpoint: "http://localhost:9333".to_string(),
        }
    }
}

impl PowerFsConfig {
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let content =
            fs::read_to_string(path).map_err(|e| ConfigError::ReadError(e.to_string()))?;
        Self::load_from_string(&content)
    }

    pub fn load_from_string(content: &str) -> Result<Self, ConfigError> {
        toml::from_str(content).map_err(|e| ConfigError::ParseError(e.to_string()))
    }

    pub fn load_or_default<P: AsRef<Path>>(path: P) -> Self {
        match Self::load_from_file(path) {
            Ok(config) => config,
            Err(e) => {
                log::warn!("Failed to load config file: {}, using defaults", e);
                Self::default()
            }
        }
    }

    pub fn to_toml(&self) -> Result<String, ConfigError> {
        toml::to_string_pretty(self).map_err(|e| ConfigError::SerializeError(e.to_string()))
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), ConfigError> {
        let content = self.to_toml()?;
        fs::write(path, content).map_err(|e| ConfigError::WriteError(e.to_string()))
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.master.port == 0 {
            return Err(ConfigError::ValidationError(
                "master.port must be > 0".to_string(),
            ));
        }
        if self.master.dir.is_empty() {
            return Err(ConfigError::ValidationError(
                "master.dir is required".to_string(),
            ));
        }

        if self.volume.grpc_port == 0 {
            return Err(ConfigError::ValidationError(
                "volume.grpc_port must be > 0".to_string(),
            ));
        }
        if self.volume.node_id.is_empty() {
            return Err(ConfigError::ValidationError(
                "volume.node_id is required".to_string(),
            ));
        }
        if self.volume.master_addresses.is_empty() {
            return Err(ConfigError::ValidationError(
                "volume.master_addresses must not be empty".to_string(),
            ));
        }

        if self.filer.port == 0 {
            return Err(ConfigError::ValidationError(
                "filer.port must be > 0".to_string(),
            ));
        }
        if self.filer.grpc_port == 0 {
            return Err(ConfigError::ValidationError(
                "filer.grpc_port must be > 0".to_string(),
            ));
        }

        if self.s3.port == 0 {
            return Err(ConfigError::ValidationError(
                "s3.port must be > 0".to_string(),
            ));
        }

        Ok(())
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
