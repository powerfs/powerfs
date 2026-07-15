use serde::Deserialize;
use std::default::Default;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub master: Option<MasterConfig>,
    pub volume: Option<VolumeConfig>,
    pub raft: Option<RaftConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MasterConfig {
    #[serde(default = "default_id")]
    pub id: String,
    #[serde(default = "default_http_address")]
    pub http_address: String,
    #[serde(default = "default_grpc_address")]
    pub grpc_address: String,
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    pub log_file: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct VolumeConfig {
    #[serde(default = "default_id")]
    pub id: String,
    #[serde(default = "default_http_address")]
    pub http_address: String,
    #[serde(default = "default_grpc_address")]
    pub grpc_address: String,
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    pub log_file: Option<String>,
    #[serde(default = "default_volume_size")]
    pub volume_size: u64,
    #[serde(default = "default_max_file_count")]
    pub max_file_count: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RaftConfig {
    #[serde(default = "default_raft_address")]
    pub address: String,
    #[serde(default = "default_election_tick")]
    pub election_tick: usize,
    #[serde(default = "default_heartbeat_tick")]
    pub heartbeat_tick: usize,
    pub peers: Option<Vec<PeerConfig>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PeerConfig {
    pub id: u64,
    pub address: String,
}

fn default_id() -> String {
    "1".to_string()
}

fn default_http_address() -> String {
    "0.0.0.0:9333".to_string()
}

fn default_grpc_address() -> String {
    "0.0.0.0:9334".to_string()
}

fn default_raft_address() -> String {
    "0.0.0.0:9335".to_string()
}

fn default_data_dir() -> String {
    "./data".to_string()
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_volume_size() -> u64 {
    1024 * 1024 * 1024
}

fn default_max_file_count() -> u64 {
    1_000_000
}

fn default_election_tick() -> usize {
    10
}

fn default_heartbeat_tick() -> usize {
    3
}

impl Config {
    pub fn from_file(path: &Path) -> Result<Self, String> {
        let content =
            fs::read_to_string(path).map_err(|e| format!("failed to read config file: {}", e))?;

        toml::from_str(&content).map_err(|e| format!("failed to parse config file: {}", e))
    }

    pub fn from_string(content: &str) -> Result<Self, String> {
        toml::from_str(content).map_err(|e| format!("failed to parse config: {}", e))
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            master: Some(MasterConfig::default()),
            volume: Some(VolumeConfig::default()),
            raft: Some(RaftConfig::default()),
        }
    }
}

impl Default for MasterConfig {
    fn default() -> Self {
        MasterConfig {
            id: default_id(),
            http_address: default_http_address(),
            grpc_address: default_grpc_address(),
            data_dir: default_data_dir(),
            log_level: default_log_level(),
            log_file: None,
        }
    }
}

impl Default for VolumeConfig {
    fn default() -> Self {
        VolumeConfig {
            id: default_id(),
            http_address: "0.0.0.0:8080".to_string(),
            grpc_address: "0.0.0.0:8081".to_string(),
            data_dir: default_data_dir(),
            log_level: default_log_level(),
            log_file: None,
            volume_size: default_volume_size(),
            max_file_count: default_max_file_count(),
        }
    }
}

impl Default for RaftConfig {
    fn default() -> Self {
        RaftConfig {
            address: default_raft_address(),
            election_tick: default_election_tick(),
            heartbeat_tick: default_heartbeat_tick(),
            peers: None,
        }
    }
}
