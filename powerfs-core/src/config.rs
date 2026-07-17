use crate::storage_backend::*;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub node: NodeConfig,
    pub storage: StorageConfig,
    pub network: NetworkConfig,
    pub reliability: ReliabilityConfig,
    pub performance: PerformanceConfig,
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct NodeConfig {
    pub node_id: String,
    pub node_type: NodeType,
    pub zone: String,
    pub rack: Option<String>,
    pub data_center: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum NodeType {
    #[default]
    Volume,
    Master,
    Monitor,
    Gateway,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    pub backend: BackendConfig,
    pub checksum_algorithm: ChecksumAlgorithm,
    pub volume_size_gib: u64,
    pub max_volumes_per_device: usize,
    pub compact_threshold_percent: f64,
    pub compact_interval_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct NetworkConfig {
    pub grpc_port: u16,
    pub grpc_threads: usize,
    pub grpc_conn_timeout_seconds: u64,
    pub grpc_enable_tcp_no_delay: bool,
    pub master_addresses: Vec<String>,
    pub raft_port: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ReliabilityConfig {
    pub bitrot_scan_interval_seconds: u64,
    pub bitrot_scan_enabled: bool,
    pub ec_data_shards: usize,
    pub ec_parity_shards: usize,
    pub recycle_bin_retention_seconds: u64,
    pub ha_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PerformanceConfig {
    pub metadata_cache_size_gib: u64,
    pub needle_index_cache_size_gib: u64,
    pub ec_thread_count: usize,
    pub io_thread_count: usize,
    pub write_buffer_size_mib: u64,
    pub read_ahead_size_kib: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
    pub level: String,
    pub format: LogFormat,
    pub file_path: Option<String>,
    pub max_file_size_mib: u64,
    pub max_file_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum LogFormat {
    #[default]
    Text,
    Json,
}

#[allow(clippy::derivable_impls)]
impl Default for Config {
    fn default() -> Self {
        Self {
            node: NodeConfig::default(),
            storage: StorageConfig::default(),
            network: NetworkConfig::default(),
            reliability: ReliabilityConfig::default(),
            performance: PerformanceConfig::default(),
            logging: LoggingConfig::default(),
        }
    }
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            node_id: "node-0".to_string(),
            node_type: NodeType::Volume,
            zone: "default".to_string(),
            rack: None,
            data_center: None,
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            backend: BackendConfig {
                backend_type: BackendType::LocalFile,
                node_id: "node-0".to_string(),
                config: BackendConfigDetails::LocalFile(LocalFileBackendConfig {
                    data_dir: "/data/powerfs".to_string(),
                    devices: vec![LocalFileDeviceConfig {
                        name: "default".to_string(),
                        total_capacity: 100 * 1024 * 1024 * 1024,
                    }],
                }),
            },
            checksum_algorithm: ChecksumAlgorithm::Crc32c,
            volume_size_gib: 100,
            max_volumes_per_device: 100,
            compact_threshold_percent: 30.0,
            compact_interval_seconds: 3600,
        }
    }
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            grpc_port: 8080,
            grpc_threads: 4,
            grpc_conn_timeout_seconds: 30,
            grpc_enable_tcp_no_delay: true,
            master_addresses: vec!["http://localhost:9333".to_string()],
            raft_port: None,
        }
    }
}

impl Default for ReliabilityConfig {
    fn default() -> Self {
        Self {
            bitrot_scan_interval_seconds: 3600,
            bitrot_scan_enabled: true,
            ec_data_shards: 8,
            ec_parity_shards: 4,
            recycle_bin_retention_seconds: 2592000,
            ha_enabled: false,
        }
    }
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            metadata_cache_size_gib: 10,
            needle_index_cache_size_gib: 5,
            ec_thread_count: 8,
            io_thread_count: 16,
            write_buffer_size_mib: 64,
            read_ahead_size_kib: 1024,
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            format: LogFormat::Text,
            file_path: None,
            max_file_size_mib: 100,
            max_file_count: 10,
        }
    }
}

impl Config {
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let content =
            fs::read_to_string(path).map_err(|e| ConfigError::ReadError(e.to_string()))?;
        serde_yaml::from_str(&content).map_err(|e| ConfigError::ParseError(e.to_string()))
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

    pub fn to_yaml(&self) -> Result<String, ConfigError> {
        serde_yaml::to_string(self).map_err(|e| ConfigError::SerializeError(e.to_string()))
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), ConfigError> {
        let content = self.to_yaml()?;
        fs::write(path, content).map_err(|e| ConfigError::WriteError(e.to_string()))
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.node.node_id.is_empty() {
            return Err(ConfigError::ValidationError(
                "node_id is required".to_string(),
            ));
        }

        match &self.storage.backend.config {
            BackendConfigDetails::LocalFile(cfg) => {
                if cfg.data_dir.is_empty() {
                    return Err(ConfigError::ValidationError(
                        "data_dir is required for local_file backend".to_string(),
                    ));
                }
                if cfg.devices.is_empty() {
                    return Err(ConfigError::ValidationError(
                        "at least one device is required for local_file backend".to_string(),
                    ));
                }
            }
            BackendConfigDetails::Spdk(cfg) => {
                if cfg.devices.is_empty() {
                    return Err(ConfigError::ValidationError(
                        "at least one device is required for spdk backend".to_string(),
                    ));
                }
            }
        }

        if self.reliability.ec_data_shards < 1 {
            return Err(ConfigError::ValidationError(
                "ec_data_shards must be at least 1".to_string(),
            ));
        }

        if self.reliability.ec_parity_shards < 1 {
            return Err(ConfigError::ValidationError(
                "ec_parity_shards must be at least 1".to_string(),
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
