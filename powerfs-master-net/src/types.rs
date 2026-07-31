//! Result types returned by [`TlvMasterClient`](crate::TlvMasterClient).

/// A single volume route entry from the cluster topology.
#[derive(Debug, Clone)]
pub struct VolumeRoute {
    pub volume_id: u64,
    /// `host:port` of the volume server (net port).
    pub addr: String,
    pub size: u64,
}

/// Cluster topology returned by `get_topology()`.
#[derive(Debug, Clone, Default)]
pub struct TopologyInfo {
    /// Current Raft leader address (`host:port`).
    pub leader: String,
    /// All volume routes known to the master.
    pub volumes: Vec<VolumeRoute>,
}

/// Result of an `assign()` call.
#[derive(Debug, Clone)]
pub struct AssignResult {
    pub volume_id: u64,
    pub cookie: u64,
    pub file_key: u64,
    /// `host:port` of the volume server to write to (net port).
    pub route_addr: String,
    pub replica_count: usize,
}

/// Volume location returned by `lookup_volume()`.
#[derive(Debug, Clone)]
pub struct VolumeLocation {
    /// `http://host:port` URL of the volume server.
    pub url: String,
    pub data_center: String,
}
