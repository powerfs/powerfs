//! Result types returned by [`TlvMasterClient`](crate::TlvMasterClient).

/// A single volume route entry from the cluster topology.
#[derive(Debug, Clone)]
pub struct VolumeRoute {
    pub volume_id: u64,
    /// `host:port` of the volume server (net port).
    pub addr: String,
    pub size: u64,
}

/// A single filer route entry from the cluster topology.
///
/// Returned by `get_topology()` so that FUSE clients can build a shard router
/// from the master's authoritative view of the cluster instead of relying on
/// static config. `shard_ids` lists every shard this filer participates in;
/// whether the filer is the leader of a given shard is *not* exposed here —
/// the FUSE client learns shard→leader by issuing a metadata RPC and caching
/// the redirect, or by reading the filer's own topology endpoint.
#[derive(Debug, Clone, Default)]
pub struct FilerRoute {
    /// Advertised `host:port` (net port) at which the filer accepts TLV connections.
    pub address: String,
    /// Net port (raw u32 form of the port part of `address`).
    pub net_port: u32,
    /// Whether the master considers the filer healthy (heartbeat fresh).
    pub is_healthy: bool,
    /// Shard IDs this filer participates in.
    pub shard_ids: Vec<u64>,
}

/// Cluster topology returned by `get_topology()`.
#[derive(Debug, Clone, Default)]
pub struct TopologyInfo {
    /// Current Raft leader address (`host:port`).
    pub leader: String,
    /// All volume routes known to the master.
    pub volumes: Vec<VolumeRoute>,
    /// Filer nodes registered to the master.
    ///
    /// Empty when the master is an older build that does not ship the
    /// `FilerListEntries` extension; callers must fall back to config-supplied
    /// filer addresses in that case.
    pub filers: Vec<FilerRoute>,
    /// Global cluster-wide shard count (every healthy filer must agree).
    /// `0` means unknown (e.g. old master without the extension). FUSE clients
    /// use this as the modulus for `calculate_shard_id(inode)`; falling back to
    /// a hardcoded value (such as the legacy 256) is incorrect when the filer
    /// cluster uses a different shard_count.
    pub total_shards: u64,
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
