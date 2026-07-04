use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct VolumeId(pub u32);

impl fmt::Display for VolumeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u32> for VolumeId {
    fn from(v: u32) -> Self {
        VolumeId(v)
    }
}

impl FromStr for VolumeId {
    type Err = std::num::ParseIntError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(VolumeId(u32::from_str(s)?))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct NeedleId(pub u64);

impl fmt::Display for NeedleId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct FileId(pub String);

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

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct DataCenterId(pub String);

impl fmt::Display for DataCenterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct RackId(pub String);

impl fmt::Display for RackId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct Collection(pub String);

impl fmt::Display for Collection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Default for Collection {
    fn default() -> Self {
        Collection("default".to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionConfig {
    pub name: Collection,
    pub replication: ReplicaPlacement,
    pub ttl: Ttl,
    pub disk_type: DiskType,
    pub max_volume_count: u64,
    pub volume_count: u64,
    pub created_at: DateTime<Utc>,
    pub modified_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct DiskType(pub String);

impl fmt::Display for DiskType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Default for DiskType {
    fn default() -> Self {
        DiskType("".to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Ttl(pub i32);

impl fmt::Display for Ttl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0 == 0 {
            write!(f, "")
        } else {
            write!(f, "{}", self.0)
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct ReplicaPlacement {
    pub copies: u32,
    pub same_rack: bool,
    pub same_data_center: bool,
}

impl Default for ReplicaPlacement {
    fn default() -> Self {
        ReplicaPlacement {
            copies: 1,
            same_rack: false,
            same_data_center: false,
        }
    }
}

impl ReplicaPlacement {
    /// Parse SeaweedFS replica placement string format.
    ///
    /// Format: Three-digit string like "001", "010", "100", "002"
    /// - First digit: copies in same data center (can be on different racks)
    /// - Second digit: copies in same rack but different data centers (if possible)
    /// - Third digit: copies in different data centers
    ///
    /// Examples:
    /// - "001": 1 copy, different rack, different data center
    /// - "010": 1 copy, same rack, different data center  
    /// - "100": 1 copy, same data center (any rack)
    /// - "011": 2 copies total (1 same rack + 1 different dc)
    /// - "111": 3 copies (1 same dc + 1 same rack + 1 different dc)
    /// - "002": 2 copies, both in different data centers
    pub fn from_string(s: &str) -> Result<Self, String> {
        if s.is_empty() {
            return Ok(Self::default());
        }

        // SeaweedFS three-digit format
        if s.len() == 3 {
            let same_dc: u32 = s[0..1]
                .parse()
                .map_err(|_| format!("invalid replica placement: {}", s))?;
            let same_rack_diff_dc: u32 = s[1..2]
                .parse()
                .map_err(|_| format!("invalid replica placement: {}", s))?;
            let diff_rack_dc: u32 = s[2..3]
                .parse()
                .map_err(|_| format!("invalid replica placement: {}", s))?;

            let total = same_dc + same_rack_diff_dc + diff_rack_dc;

            // "000" means no additional replicas = 1 copy (the original)
            let copies = total.max(1);

            // same_rack is true if we have copies that should stay in same rack
            // (either same_rack_diff_dc > 0 or same_dc > 0 with implicit same rack)
            let same_rack = same_rack_diff_dc > 0;

            // same_data_center is true if any copies should stay in same dc
            let same_data_center = same_dc > 0 || same_rack_diff_dc > 0;

            return Ok(ReplicaPlacement {
                copies,
                same_rack,
                same_data_center,
            });
        }

        // Fallback: simple number format (e.g., "3" means 3 copies)
        let copies: u32 = s
            .parse()
            .map_err(|_| format!("invalid replica placement: {}", s))?;
        Ok(ReplicaPlacement {
            copies,
            same_rack: false,
            same_data_center: false,
        })
    }

    pub fn get_copy_count(&self) -> u32 {
        self.copies
    }

    /// Convert to SeaweedFS three-digit format string
    pub fn to_string_format(&self) -> String {
        // Simple conversion back - not exact but representative
        if self.same_data_center && self.same_rack {
            format!("{}00", self.copies)
        } else if self.same_rack {
            format!("0{}0", self.copies)
        } else {
            format!("00{}", self.copies)
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fid {
    pub volume_id: VolumeId,
    pub cookie: u64,
    pub file_key: u64,
}

impl fmt::Display for Fid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{},{},{}", self.volume_id.0, self.cookie, self.file_key)
    }
}

impl Fid {
    pub fn from_string(s: &str) -> Result<Self, String> {
        let parts: Vec<&str> = s.split(',').collect();
        if parts.len() != 3 {
            return Err(format!("invalid fid format: {}", s));
        }
        let volume_id = VolumeId::from_str(parts[0]).map_err(|e| e.to_string())?;
        let cookie = parts[1].parse::<u64>().map_err(|e| e.to_string())?;
        let file_key = parts[2].parse::<u64>().map_err(|e| e.to_string())?;
        Ok(Fid {
            volume_id,
            cookie,
            file_key,
        })
    }

    pub fn new_kv_fid(session_id: &str, layer_id: u32, block_index: u32) -> Self {
        let volume_id = (session_id.len() as u32 % 1000) + 1;
        let cookie = ((layer_id as u64) << 32) | (block_index as u64);
        let file_key = session_id.len() as u64 * 1000000 + layer_id as u64 * 1000 + block_index as u64;
        Fid {
            volume_id: VolumeId(volume_id),
            cookie,
            file_key,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeInfo {
    pub id: VolumeId,
    pub node_id: NodeId,
    pub collection: Collection,
    pub size: u64,
    pub used: u64,
    pub replica_count: u32,
    pub ttl: Ttl,
    pub disk_type: DiskType,
    pub state: VolumeState,
    pub created_at: DateTime<Utc>,
    pub modified_at: DateTime<Utc>,
    /// Next file key to assign for this volume (per-volume counter)
    pub next_file_key: u64,
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
pub struct DataNodeInfo {
    pub id: NodeId,
    pub address: String,
    pub rack_id: RackId,
    pub data_center_id: DataCenterId,
    pub total_space: u64,
    pub used_space: u64,
    pub volume_count: u32,
    pub state: NodeState,
    pub last_heartbeat: DateTime<Utc>,
    pub grpc_port: u32,
    pub http_port: u32,
    pub public_url: String,
    pub maintenance_mode: bool,
}

impl DataNodeInfo {
    pub fn url(&self) -> String {
        format!("{}:{}", self.address, self.http_port)
    }

    pub fn new(
        id: NodeId,
        address: String,
        rack_id: RackId,
        data_center_id: DataCenterId,
        http_port: u32,
        grpc_port: u32,
        public_url: String,
    ) -> Self {
        DataNodeInfo {
            id,
            address,
            rack_id,
            data_center_id,
            total_space: 0,
            used_space: 0,
            volume_count: 0,
            state: NodeState::Healthy,
            last_heartbeat: Utc::now(),
            grpc_port,
            http_port,
            public_url,
            maintenance_mode: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RackInfo {
    pub id: RackId,
    pub data_center_id: DataCenterId,
    pub nodes: HashMap<NodeId, DataNodeInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataCenterInfo {
    pub id: DataCenterId,
    pub racks: HashMap<RackId, RackInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Topology {
    pub data_centers: HashMap<DataCenterId, DataCenterInfo>,
}

impl Topology {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_or_create_data_center(&mut self, id: DataCenterId) -> &mut DataCenterInfo {
        self.data_centers
            .entry(id.clone())
            .or_insert_with(|| DataCenterInfo {
                id,
                racks: HashMap::new(),
            })
    }

    pub fn get_or_create_rack(&mut self, dc_id: DataCenterId, rack_id: RackId) -> &mut RackInfo {
        let dc = self.get_or_create_data_center(dc_id);
        dc.racks.entry(rack_id.clone()).or_insert_with(|| RackInfo {
            id: rack_id,
            data_center_id: dc.id.clone(),
            nodes: HashMap::new(),
        })
    }

    pub fn get_or_create_node(&mut self, node: DataNodeInfo) -> &mut DataNodeInfo {
        let rack = self.get_or_create_rack(node.data_center_id.clone(), node.rack_id.clone());
        rack.nodes.entry(node.id.clone()).or_insert_with(|| node)
    }

    pub fn get_node(&self, node_id: &NodeId) -> Option<&DataNodeInfo> {
        for dc in self.data_centers.values() {
            for rack in dc.racks.values() {
                if let Some(node) = rack.nodes.get(node_id) {
                    return Some(node);
                }
            }
        }
        None
    }

    pub fn get_node_mut(&mut self, node_id: &NodeId) -> Option<&mut DataNodeInfo> {
        for dc in self.data_centers.values_mut() {
            for rack in dc.racks.values_mut() {
                if let Some(node) = rack.nodes.get_mut(node_id) {
                    return Some(node);
                }
            }
        }
        None
    }

    pub fn remove_node(&mut self, node_id: &NodeId) -> Option<DataNodeInfo> {
        for dc in self.data_centers.values_mut() {
            for rack in dc.racks.values_mut() {
                if let Some(node) = rack.nodes.remove(node_id) {
                    return Some(node);
                }
            }
        }
        None
    }

    pub fn list_all_nodes(&self) -> Vec<DataNodeInfo> {
        let mut nodes = Vec::new();
        for dc in self.data_centers.values() {
            for rack in dc.racks.values() {
                nodes.extend(rack.nodes.values().cloned());
            }
        }
        nodes
    }
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
