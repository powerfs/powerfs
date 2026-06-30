use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct VolumeId(pub Uuid);

impl fmt::Display for VolumeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct NeedleId(pub Uuid);

impl fmt::Display for NeedleId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct FileId(pub Uuid);

impl fmt::Display for FileId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct NodeId(pub String);

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeInfo {
    pub id: VolumeId,
    pub node_id: NodeId,
    pub size: u64,
    pub used: u64,
    pub replica_count: u32,
    pub state: VolumeState,
    pub created_at: DateTime<Utc>,
    pub modified_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum VolumeState {
    #[default]
    Creating,
    Available,
    Full,
    ReadOnly,
    Deleting,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NeedleInfo {
    pub id: NeedleId,
    pub volume_id: VolumeId,
    pub data_size: u32,
    pub offset: u64,
    pub checksum: u64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: NodeId,
    pub address: String,
    pub rack: String,
    pub data_center: String,
    pub total_space: u64,
    pub used_space: u64,
    pub volume_count: u32,
    pub state: NodeState,
    pub last_heartbeat: DateTime<Utc>,
    pub grpc_port: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum NodeState {
    #[default]
    Healthy,
    Degraded,
    Unavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MasterInfo {
    pub id: NodeId,
    pub address: String,
    pub is_leader: bool,
    pub term: u64,
    pub last_heartbeat: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadata {
    pub file_id: FileId,
    pub name: String,
    pub size: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub atime: DateTime<Utc>,
    pub mtime: DateTime<Utc>,
    pub ctime: DateTime<Utc>,
    pub volume_ids: Vec<VolumeId>,
    pub needle_ids: Vec<NeedleId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftConfig {
    pub heartbeat_interval: u64,
    pub election_timeout_min: u64,
    pub election_timeout_max: u64,
    pub snapshot_interval: u64,
    pub max_log_entries: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub replication_factor: u32,
    pub volume_size_limit: u64,
    pub max_volumes_per_node: u32,
    pub rack_awareness_enabled: bool,
    pub data_center_awareness_enabled: bool,
}

impl Default for RaftConfig {
    fn default() -> Self {
        RaftConfig {
            heartbeat_interval: 100,
            election_timeout_min: 300,
            election_timeout_max: 500,
            snapshot_interval: 60000,
            max_log_entries: 10000,
        }
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        ClusterConfig {
            replication_factor: 3,
            volume_size_limit: 1024 * 1024 * 1024 * 1024,
            max_volumes_per_node: 100,
            rack_awareness_enabled: true,
            data_center_awareness_enabled: false,
        }
    }
}
