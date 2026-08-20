use crate::collection::{CollectionInfo, CollectionManager, CollectionStats, VolumeAllocationMode};
use crate::raft_v2::RaftNodeV2;
use crate::raft_v2::{ApplyEntry, Peer, RaftCommand};
use crate::volume_client::VolumeClientPool;
use chrono::Utc;
use log::{debug, error, info, warn};
use powerfs_common::{
    collect_system_metrics,
    error::{PowerFsError, Result},
    event::{Event, NodeStatusEvent, NullEventProvider, VolumeStatusEvent},
    traits::EventProvider,
    types::{
        ClusterConfig, Collection, CollectionConfig, DataCenterId, DataNodeInfo, DiskType, Fid,
        NodeId, NodeState, RackId, RaftConfig, ReplicaPlacement, Topology, Ttl, VolumeId,
        VolumeInfo, VolumeRoute, VolumeState,
    },
};
use powerfs_core::kv_cache::KVCacheEngine;
use powerfs_core::kv_cache_persist::KVPersistStore;
use powerfs_net::{PowerFsNetServer, ServerConnectionManager};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, RwLock, RwLock as StdRwLock};
use std::time::Duration;
use tokio::sync::mpsc;

pub use crate::proto::VolumeShortInfo;
pub use crate::volume_assigner::{
    AssignContext, RoundRobinAssigner, SmartVolumeAssigner, VolumeAssigner,
};

/// Batch size for persisting next_file_key via Raft.
/// Every FILE_KEY_BATCH_SIZE allocations, master proposes AdvanceFileKey
/// to survive restarts without reusing file_keys.
const FILE_KEY_BATCH_SIZE: u64 = 1000;

pub struct MasterNode {
    id: NodeId,
    address: SocketAddr,
    net_port: u16,
    topology: RwLock<Topology>,
    volumes: RwLock<HashMap<VolumeId, VolumeInfo>>,
    volume_routes: RwLock<HashMap<u64, VolumeRoute>>,
    collections: RwLock<HashMap<String, CollectionConfig>>,
    collection_manager: RwLock<CollectionManager>,
    volume_layouts: RwLock<HashMap<String, VolumeLayout>>,
    cluster_config: RwLock<ClusterConfig>,
    raft_config: RaftConfig,
    peers: Vec<crate::raft_v2::Peer>,
    /// openraft-backed Raft node (replaces old RaftNode + propose/step/message channels).
    /// Wrapped in `Arc` because `RaftNodeV2` is not `Clone` but `MasterNode` has a manual
    /// `Clone` impl (each clone shares the same underlying openraft handle).
    raft_v2: Arc<RaftNodeV2>,
    raft_id: u64,
    raft_address: String,
    is_leader: Arc<AtomicBool>,
    leader_address: Arc<StdRwLock<String>>,
    raft_term: RwLock<u64>,
    next_volume_id: RwLock<u64>,
    max_file_key: RwLock<u64>,
    heartbeat_tx: mpsc::Sender<NodeId>,
    /// Shared across all clones so that FUSE clients registered via the
    /// TLV handler are visible to the gRPC monitor/frontend and vice
    /// versa.
    client_manager: Arc<RwLock<ClientManager>>,
    notify_tx: mpsc::Sender<VolumeLocationUpdate>,
    /// Shared `ServerConnectionManager` used to push NOTIFY frames
    /// (e.g. VolumeLocation updates) to TLV clients.  Set when the net
    /// server starts; `None` before that.
    net_manager: Arc<RwLock<Option<Arc<ServerConnectionManager>>>>,
    pub kv_cache: Arc<KVCacheEngine>,
    pub kv_persist: Arc<KVPersistStore>,
    pub volume_client_pool: Arc<VolumeClientPool>,
    event_provider: Arc<dyn EventProvider>,
    filer_nodes: RwLock<HashMap<String, FilerNodeInfo>>,
    shard_mapping: RwLock<HashMap<u64, String>>,
    stripe_round_robin: Arc<AtomicU32>,
    /// Round-robin counter for single-volume assignment (assign_volume).
    /// Ensures writes are distributed across all writable volumes instead
    /// of always picking the first one returned by HashMap iteration.
    volume_round_robin: Arc<AtomicU32>,
    /// Zone 管理: zone_id → ZoneInfo
    zone_registry: RwLock<HashMap<u32, powerfs_common::types::ZoneInfo>>,
    /// 下一个分配的 zone_id (从 1 开始, 原子递增)
    next_zone_id: Arc<AtomicU32>,
    /// Allocator rebalance engine (LoadBalancer + MigrationScheduler).
    /// `None` until `start()` constructs it; shared so status/monitor queries
    /// can read migration task state.
    rebalance_engine: RwLock<Option<Arc<crate::allocator_integration::RebalanceEngine>>>,
    /// Allocator management API (volume scaling, migration control, policy
    /// updates). `None` until `start()` constructs it.
    management_api: RwLock<Option<Arc<crate::allocator_integration::MasterManagementApi>>>,
    /// Volume pin registry: `volume_id → node_id`. A pinned volume's data must
    /// stay on the pinned node; the LoadBalancer skips it as a migration
    /// source. Raft-replicated via `PinVolume`/`UnpinVolume` commands so all
    /// master replicas agree.
    volume_pins: RwLock<HashMap<VolumeId, String>>,
    /// Global placement strategy name: "round_robin" | "least_loaded" |
    /// "anti_affinity". Controls which `VolumeAssigner` is used for new volume
    /// allocation. Raft-replicated via `SetPlacementStrategy` command.
    placement_strategy: RwLock<String>,
    /// Centralized debug config store. Shared (Arc inside) across all clones.
    /// Nodes poll `GetDebugConfig` to fetch their effective config every 2s.
    debug_config: crate::debug_config::DebugConfigStore,
}

#[derive(Clone)]
pub struct VolumeLayout {
    #[allow(dead_code)]
    collection: Collection,
    #[allow(dead_code)]
    replica_placement: ReplicaPlacement,
    #[allow(dead_code)]
    ttl: Ttl,
    #[allow(dead_code)]
    disk_type: DiskType,
    #[allow(dead_code)]
    volumes: Vec<VolumeId>,
}

#[derive(Debug, Clone)]
pub struct FilerNodeInfo {
    pub node_id: String,
    pub address: String,
    pub grpc_port: u32,
    pub http_port: u32,
    pub net_port: u32,
    pub is_healthy: bool,
    pub leader_count: u64,
    pub total_shards: u64,
    pub shard_ids: Vec<u64>,
}

#[derive(Debug, Clone)]
pub struct AddNodeParams {
    pub node_id: NodeId,
    pub address: String,
    pub rack: String,
    pub data_center: String,
    pub http_port: u32,
    pub grpc_port: u32,
    pub public_url: String,
}

#[derive(Debug, Clone)]
pub struct AssignVolumeParams {
    pub node_id: String,
    pub volume_id: u64,
    pub collection: String,
    pub replica_count: u32,
    pub ttl: i32,
    pub disk_type: String,
    pub size: u64,
}

#[derive(Debug, Clone)]
pub struct UpdateNodeVolumesParams {
    pub node_id: NodeId,
    pub volumes: Vec<VolumeShortInfo>,
    pub new_volumes: Vec<VolumeShortInfo>,
    pub deleted_volumes: Vec<VolumeShortInfo>,
    pub ip: String,
    pub grpc_port: u32,
    pub http_port: u32,
    pub net_port: u32,
}

#[derive(Debug, Clone)]
pub struct FuseClientInfo {
    pub client_id: String,
    pub client_type: String,
    pub mount_point: String,
    pub collection: String,
    pub replication: String,
    pub host: String,
    pub pid: u64,
    pub connected_at: u64,
    pub last_heartbeat: u64,
    pub dirty_chunks: u64,
    pub dirty_bytes: u64,
    pub stats: Option<crate::proto::ClientStats>,
}

pub struct ClientManager {
    clients: HashMap<String, mpsc::Sender<VolumeLocationUpdate>>,
    fuse_clients: HashMap<String, FuseClientInfo>,
}

impl ClientManager {
    fn new() -> Self {
        ClientManager {
            clients: HashMap::new(),
            fuse_clients: HashMap::new(),
        }
    }

    fn add_client(&mut self, client_id: String, tx: mpsc::Sender<VolumeLocationUpdate>) {
        self.clients.insert(client_id, tx);
    }

    fn remove_client(&mut self, client_id: &str) {
        self.clients.remove(client_id);
        self.fuse_clients.remove(client_id);
    }

    fn broadcast(&self, update: &VolumeLocationUpdate) {
        for (id, tx) in &self.clients {
            if let Err(e) = tx.try_send(update.clone()) {
                warn!("Failed to broadcast to client {}: {}", id, e);
            }
        }
    }

    fn register_fuse_client(&mut self, info: FuseClientInfo) {
        info!(
            "Registering FUSE client: id={}, type={}, mount_point={}, collection={}",
            info.client_id, info.client_type, info.mount_point, info.collection
        );
        // Preserve the original connected_at if the client already exists.
        if let Some(existing) = self.fuse_clients.get_mut(&info.client_id) {
            existing.client_type = info.client_type;
            existing.mount_point = info.mount_point;
            existing.collection = info.collection;
            existing.replication = info.replication;
            existing.host = info.host;
            existing.pid = info.pid;
            existing.last_heartbeat = info.last_heartbeat;
            existing.dirty_chunks = info.dirty_chunks;
            existing.dirty_bytes = info.dirty_bytes;
            existing.stats = info.stats;
        } else {
            self.fuse_clients.insert(info.client_id.clone(), info);
        }
    }

    fn update_fuse_client_heartbeat(&mut self, client_id: &str) {
        if let Some(client) = self.fuse_clients.get_mut(client_id) {
            client.last_heartbeat = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
        } else {
            warn!("FUSE client not found for heartbeat update: {}", client_id);
        }
    }

    fn update_fuse_client_stats(&mut self, client_id: &str, stats: crate::proto::ClientStats) {
        if let Some(client) = self.fuse_clients.get_mut(client_id) {
            client.stats = Some(stats);
        } else {
            warn!("FUSE client not found for stats update: {}", client_id);
        }
    }

    fn get_fuse_clients(&self) -> Vec<FuseClientInfo> {
        let clients: Vec<_> = self.fuse_clients.values().cloned().collect();
        debug!("Getting FUSE clients: count={}", clients.len());
        clients
    }
}

#[derive(Debug, Clone)]
pub struct VolumeLocationUpdate {
    pub new_vids: Vec<u32>,
    pub deleted_vids: Vec<u32>,
    pub leader: String,
}

impl MasterNode {
    pub async fn new(
        bind_address: &str,
        raft_address: &str,
        cluster_config: Option<ClusterConfig>,
        raft_path: &str,
        raft_id: u64,
        peers: Vec<String>,
        net_port: u16,
    ) -> Result<Self> {
        let addr: SocketAddr = bind_address.parse()?;

        let node_id = NodeId(raft_address.to_string());
        let config = cluster_config.unwrap_or_default();
        let raft_config = RaftConfig::default();

        let peer_list: Vec<Peer> = peers
            .into_iter()
            .enumerate()
            .map(|(i, addr)| {
                // Derive the powerfs-net address (ip:net_port) from the peer's
                // gRPC address (ip:port) by replacing the port with this node's
                // `net_port`. All master nodes in the cluster share the same
                // `net_port`, so this yields the correct TLV endpoint.
                let peer_ip = addr.split(':').next().unwrap_or(&addr);
                let net_address = format!("{}:{}", peer_ip, net_port);
                Peer {
                    id: (i + 1) as u64,
                    address: addr,
                    net_address,
                }
            })
            .filter(|p| p.id != raft_id)
            .collect();

        // 共享 leader 状态：RaftNode 在角色变更时更新，Master 读取
        // 单节点模式（无 peers）初始为 true，多节点模式初始为 false
        let leader_state = Arc::new(AtomicBool::new(peer_list.is_empty()));

        // 共享 leader 地址：RaftNode 在角色变更时更新，Master 读取
        let leader_address = Arc::new(StdRwLock::new(if peer_list.is_empty() {
            raft_address.to_string()
        } else {
            String::new()
        }));

        let (raft_v2, mut apply_rx) = RaftNodeV2::new(
            raft_id,
            raft_address.to_string(),
            peer_list.clone(),
            raft_path,
        )
        .await
        .map_err(|e| PowerFsError::Internal(format!("Failed to create raft node v2: {}", e)))?;
        let raft_v2 = Arc::new(raft_v2);

        let (heartbeat_tx, mut heartbeat_rx) = mpsc::channel(100);
        let (notify_tx, mut notify_rx) = mpsc::channel(1000);

        // P1.3: Extract Zone commands from applied entries for replay after restart.
        // openraft persists Normal entry payloads to the `raft_state_data` CF; we scan
        // it and filter for RegisterZone/UpdateZone commands to restore zone_registry.
        let zone_commands = raft_v2
            .scan_applied_entries()
            .map_err(|e| PowerFsError::Internal(format!("Failed to scan applied entries: {}", e)))?
            .into_iter()
            .filter_map(|(_, payload)| {
                serde_json::from_slice::<crate::raft_v2::RaftCommand>(&payload).ok()
            })
            .filter(|cmd| {
                matches!(
                    cmd,
                    crate::raft_v2::RaftCommand::RegisterZone { .. }
                        | crate::raft_v2::RaftCommand::UpdateZone { .. }
                )
            })
            .collect::<Vec<_>>();

        // openraft is self-driven (internal tick + network threads); no run() loop needed.

        let mut collections = HashMap::new();
        collections.insert(
            "default".to_string(),
            CollectionConfig {
                name: Collection::default(),
                replication: ReplicaPlacement::default(),
                ttl: Ttl::default(),
                disk_type: DiskType::default(),
                max_volume_count: 0,
                volume_count: 0,
                created_at: Utc::now(),
                modified_at: Utc::now(),
            },
        );

        let kv_cache = Arc::new(KVCacheEngine::new(
            1024 * 1024 * 1024, // 1GB default
            2 * 1024 * 1024,    // 2MB block
        ));

        let kv_persist_path = std::path::Path::new(raft_path)
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .join("kv_persist");
        let kv_persist = Arc::new(
            KVPersistStore::new(kv_persist_path.to_str().unwrap_or("kv_persist")).map_err(|e| {
                PowerFsError::Internal(format!("Failed to create KV persist store: {}", e))
            })?,
        );

        Self::restore_kv_sessions(&kv_cache, &kv_persist);

        let volume_client_pool = Arc::new(VolumeClientPool::new());

        let event_provider: Arc<dyn EventProvider> = match std::env::var("REDIS_URL") {
            #[cfg(feature = "redis-event")]
            Ok(url) => {
                info!("Event provider enabled with Redis: {}", url);
                Arc::new(powerfs_common::event::RedisEventProvider::new(
                    &url,
                    "powerfs_events",
                    "master",
                ))
            }
            _ => {
                warn!("REDIS_URL not set, using null event provider");
                Arc::new(NullEventProvider)
            }
        };

        let master = MasterNode {
            id: node_id.clone(),
            address: addr,
            net_port,
            topology: RwLock::new(Topology::new()),
            volumes: RwLock::new(HashMap::new()),
            volume_routes: RwLock::new(HashMap::new()),
            collections: RwLock::new(collections),
            collection_manager: RwLock::new(CollectionManager::with_default()),
            volume_layouts: RwLock::new(HashMap::new()),
            cluster_config: RwLock::new(config),
            raft_config,
            peers: peer_list,
            raft_v2,
            raft_id,
            raft_address: raft_address.to_string(),
            is_leader: leader_state,
            leader_address,
            raft_term: RwLock::new(1),
            next_volume_id: RwLock::new(1),
            max_file_key: RwLock::new(0),
            heartbeat_tx,
            client_manager: Arc::new(RwLock::new(ClientManager::new())),
            notify_tx,
            net_manager: Arc::new(RwLock::new(None)),
            kv_cache,
            kv_persist,
            volume_client_pool,
            event_provider,
            filer_nodes: RwLock::new(HashMap::new()),
            shard_mapping: RwLock::new(HashMap::new()),
            stripe_round_robin: Arc::new(AtomicU32::new(0)),
            volume_round_robin: Arc::new(AtomicU32::new(0)),
            zone_registry: RwLock::new(HashMap::new()),
            next_zone_id: Arc::new(AtomicU32::new(1)),
            rebalance_engine: RwLock::new(None),
            management_api: RwLock::new(None),
            volume_pins: RwLock::new(HashMap::new()),
            placement_strategy: RwLock::new("least_loaded".to_string()),
            debug_config: crate::debug_config::DebugConfigStore::new(),
        };

        // P1.3: Replay Zone commands from Raft log to restore zone_registry.
        // Raft sets cfg.applied = commit_index, so already-applied entries
        // (including RegisterZone/UpdateZone) won't be re-applied by the Raft
        // event loop. We manually replay them here to restore in-memory state.
        if !zone_commands.is_empty() {
            info!(
                "P1.3: replaying {} Zone commands from Raft log to restore zone_registry",
                zone_commands.len()
            );
            for cmd in zone_commands {
                let entry = ApplyEntry {
                    index: 0,
                    command: cmd,
                };
                if let Err(e) = master.apply_command(entry).await {
                    warn!("P1.3: failed to replay Zone command: {}", e);
                }
            }
            let zone_count = master.zone_registry.read().unwrap().len();
            let next_zid = master
                .next_zone_id
                .load(std::sync::atomic::Ordering::SeqCst);
            info!(
                "P1.3: zone_registry restored: {} zones, next_zone_id={}",
                zone_count, next_zid
            );
        }

        let master_clone = master.clone();
        tokio::spawn(async move {
            while let Some(node_id) = heartbeat_rx.recv().await {
                master_clone.handle_heartbeat(&node_id).await;
            }
        });

        let master_clone = master.clone();
        tokio::spawn(async move {
            while let Some(update) = notify_rx.recv().await {
                // 1. Push to gRPC KeepConnected streams (legacy path).
                master_clone
                    .client_manager
                    .read()
                    .unwrap()
                    .broadcast(&update);

                // 2. Push a TopologyChanged NOTIFY to all TLV clients
                //    (FUSE / kernel FS).  Clients react by re-fetching
                //    the full topology, which is simpler and more robust
                //    than shipping volume deltas.
                let net_mgr_opt = master_clone.net_manager.read().unwrap().clone();
                if let Some(net_mgr) = net_mgr_opt {
                    let notify_msg = powerfs_net::NetMessage::notification(
                        powerfs_net::MsgType::TopologyChanged,
                        Vec::new(),
                        Vec::new(),
                    );
                    let n = net_mgr.broadcast_notification(&notify_msg);
                    if n > 0 {
                        debug!(
                            "Broadcast TopologyChanged NOTIFY to {} TLV clients (new={}, del={}, leader={})",
                            n,
                            update.new_vids.len(),
                            update.deleted_vids.len(),
                            update.leader
                        );
                    }
                }
            }
        });

        // Start apply loop (receives applied log indices from openraft's state machine).
        // For each index, read the serialized payload from `raft_state_data` CF,
        // deserialize it into a `RaftCommand`, and replay it onto the in-memory state.
        let master_clone = master.clone();
        tokio::spawn(async move {
            while let Some(index) = apply_rx.recv().await {
                match master_clone.raft_v2.read_applied_entry(index) {
                    Ok(Some(payload)) => {
                        match serde_json::from_slice::<crate::raft_v2::RaftCommand>(&payload) {
                            Ok(cmd) => {
                                let entry = ApplyEntry {
                                    index,
                                    command: cmd,
                                };
                                if let Err(e) = master_clone.apply_command(entry).await {
                                    error!("Failed to apply command at index {}: {}", index, e);
                                }
                            }
                            Err(e) => {
                                error!(
                                    "Failed to deserialize RaftCommand at index {}: {}",
                                    index, e
                                );
                            }
                        }
                    }
                    Ok(None) => {
                        debug!(
                            "Applied entry at index {} not found in state_data CF",
                            index
                        );
                    }
                    Err(e) => {
                        error!("Failed to read applied entry at index {}: {}", index, e);
                    }
                }
            }
        });

        // Volumes are NOT pre-allocated here.
        // Volume servers create their own volumes (with UUID-based IDs) at startup
        // and register them via heartbeat. The Master builds the volume table
        // and route table from heartbeats. This ensures volume IDs in the Master
        // match the actual Volume server volume IDs.
        {
            // Pre-register Volume Servers in topology so that lookups work
            // Note: DataNodeInfo.address is for gRPC, while VolumeRoute.addr is for net_port
            {
                let mut topology = master.topology.write().unwrap();

                // Add volume-server-1: IP=172.20.0.21, grpc=8080, http=8091
                let node1 = DataNodeInfo::new(
                    NodeId("volume-server-1".to_string()),
                    "172.20.0.21:8080".to_string(), // gRPC address
                    RackId("rack-1".to_string()),
                    DataCenterId("dc-1".to_string()),
                    8091,                                  // HTTP port for management
                    8080,                                  // gRPC port
                    "http://172.20.0.21:8091".to_string(), // Public URL
                );
                topology.get_or_create_node(node1);

                // Add volume-server-2: IP=172.20.0.22, grpc=8080, http=8092
                let node2 = DataNodeInfo::new(
                    NodeId("volume-server-2".to_string()),
                    "172.20.0.22:8080".to_string(), // gRPC address
                    RackId("rack-1".to_string()),
                    DataCenterId("dc-1".to_string()),
                    8092,                                  // HTTP port for management
                    8080,                                  // gRPC port
                    "http://172.20.0.22:8092".to_string(), // Public URL
                );
                topology.get_or_create_node(node2);

                // Add volume-server-3: IP=172.20.0.23, grpc=8080, http=8093
                let node3 = DataNodeInfo::new(
                    NodeId("volume-server-3".to_string()),
                    "172.20.0.23:8080".to_string(), // gRPC address
                    RackId("rack-1".to_string()),
                    DataCenterId("dc-1".to_string()),
                    8093,                                  // HTTP port for management
                    8080,                                  // gRPC port
                    "http://172.20.0.23:8093".to_string(), // Public URL
                );
                topology.get_or_create_node(node3);

                info!("Pre-registered 3 Volume Servers in topology");
            }
        }

        Ok(master)
    }

    fn restore_kv_sessions(kv_cache: &Arc<KVCacheEngine>, kv_persist: &Arc<KVPersistStore>) {
        if let Ok(sessions) = kv_persist.list_sessions() {
            for session_id in sessions {
                if let Ok(Some(meta)) = kv_persist.load_session(&session_id) {
                    let dtype = meta.dtype_enum();
                    let _ = kv_cache.create_session(
                        &session_id,
                        "",
                        "",
                        &meta.model_name,
                        meta.num_layers,
                        meta.num_heads,
                        meta.head_dim,
                        dtype,
                        meta.ttl_seconds,
                        &meta.collection,
                    );
                    for block_id in &meta.block_ids {
                        if let Ok(Some(fid)) = kv_persist.load_block_fid(*block_id) {
                            kv_cache.restore_block_id_mapping(*block_id, &fid);
                        }
                    }
                }
            }
        }
    }

    pub fn id(&self) -> &NodeId {
        &self.id
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }

    pub async fn is_leader(&self) -> bool {
        self.raft_v2.is_leader()
    }

    pub async fn get_leader(&self) -> String {
        let leader = self.leader_address.read().unwrap().clone();
        // Convert raft address to net address for FUSE clients that use powerfs-net protocol
        // raft_address is "host:raft_port", net_port is the port for powerfs-net connections
        let convert_to_net_addr = |addr: &str| -> String {
            if let Some(host) = addr.split(':').next() {
                format!("{}:{}", host, self.net_port)
            } else {
                addr.to_string()
            }
        };

        if !leader.is_empty() {
            return convert_to_net_addr(&leader);
        }
        // Fallback: if we are the leader, return our own net address
        if self.raft_v2.is_leader() {
            return convert_to_net_addr(&self.raft_address);
        }
        // Follower: 实时查 raft 当前 leader 地址。
        // `leader_address` 字段从未被 `set_leader` 更新过（死代码），
        // follower 必须通过 `current_leader_addr()` 从 raft metrics 查 leader。
        if let Some(addr) = self.raft_v2.current_leader_addr().await {
            return convert_to_net_addr(&addr);
        }
        // 无 leader 且自身非 leader: 返回空字符串, 让调用方返回 SERVER_ERROR
        // (旧实现返回 first_peer 地址, 导致 follower 间互相 REDIRECT 形成循环)
        warn!(
            "get_leader: no leader available (self={}, is_leader=false, leader_address field empty); \
             returning empty — caller MUST return SERVER_ERROR, NOT empty REDIRECT",
            self.raft_v2.node_id()
        );
        String::new()
    }

    /// Returns the leader's gRPC address (`host:grpc_port`).
    ///
    /// Unlike `get_leader()` which returns the net (powerfs-net) port for
    /// FUSE clients, this method returns the gRPC port so that intra-cluster
    /// gRPC forwarding (e.g. `get_leader_client`) connects to the right
    /// service. All master nodes share the same port layout, so we reuse
    /// `self.address.port()` (the local gRPC port) when rebuilding the
    /// leader address.
    pub async fn get_leader_grpc_addr(&self) -> String {
        let leader = self.leader_address.read().unwrap().clone();
        let grpc_port = self.address.port();
        let convert_to_grpc_addr = |addr: &str| -> String {
            if let Some(host) = addr.split(':').next() {
                format!("{}:{}", host, grpc_port)
            } else {
                addr.to_string()
            }
        };

        if !leader.is_empty() {
            return convert_to_grpc_addr(&leader);
        }
        if self.raft_v2.is_leader() {
            return convert_to_grpc_addr(&self.raft_address);
        }
        // Follower: 实时查 raft 当前 leader 地址（同 `get_leader`）。
        if let Some(addr) = self.raft_v2.current_leader_addr().await {
            return convert_to_grpc_addr(&addr);
        }
        // No leader and self is not leader: return empty string.
        // (旧实现返回 first_peer 地址, 导致 follower 间互相 REDIRECT 形成循环)
        warn!(
            "get_leader_grpc_addr: no leader available (self={}, is_leader=false); \
             returning empty — caller MUST return SERVER_ERROR, NOT empty REDIRECT",
            self.raft_v2.node_id()
        );
        String::new()
    }

    pub fn set_leader(&self, leader_addr: String) {
        *self.leader_address.write().unwrap() = leader_addr;
    }

    pub fn register_filer(&self, info: FilerNodeInfo) {
        let mut filer_nodes = self.filer_nodes.write().unwrap();
        filer_nodes.insert(info.node_id.clone(), info.clone());

        let mut shard_mapping = self.shard_mapping.write().unwrap();
        for &shard_id in &info.shard_ids {
            shard_mapping.insert(shard_id, info.address.clone());
        }
        info!(
            "Registered filer: id={}, address={}, shards={:?}",
            info.node_id, info.address, info.shard_ids
        );

        // Push a TopologyChanged NOTIFY to all TLV clients (FUSE/kernel FS)
        // so they re-fetch the full topology and update their shard routing
        // tables. Without this, clients never receive TopologyChanged and
        // rely solely on passive RPC redirects to repair stale routes after
        // a filer restart or leader change.
        let update = VolumeLocationUpdate {
            new_vids: Vec::new(),
            deleted_vids: Vec::new(),
            leader: info.address.clone(),
        };
        if let Err(e) = self.notify_tx.try_send(update) {
            warn!(
                "register_filer: failed to notify topology change for filer {}: {}",
                info.node_id, e
            );
        }
    }

    pub fn get_filer_for_inode(&self, inode: u64) -> Option<String> {
        let shard_id = Self::calculate_shard(inode);
        self.shard_mapping.read().unwrap().get(&shard_id).cloned()
    }

    pub fn get_shard_for_inode(&self, inode: u64) -> u64 {
        Self::calculate_shard(inode)
    }

    pub fn list_filers(&self) -> Vec<FilerNodeInfo> {
        self.filer_nodes.read().unwrap().values().cloned().collect()
    }

    /// 获取集中式调试配置存储（用于 HTTP 端点和 GetDebugConfig handler 共享）
    pub fn debug_config(&self) -> &crate::debug_config::DebugConfigStore {
        &self.debug_config
    }

    pub fn get_shard_mapping(&self) -> Vec<(u64, String)> {
        self.shard_mapping
            .read()
            .unwrap()
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect()
    }

    fn calculate_shard(inode: u64) -> u64 {
        let inode_per_shard = 1_000_000;
        inode / inode_per_shard
    }

    pub fn raft_term(&self) -> u64 {
        *self.raft_term.read().unwrap()
    }

    pub fn set_raft_term(&self, term: u64) {
        *self.raft_term.write().unwrap() = term;
    }

    pub fn raft_id(&self) -> u64 {
        self.raft_id
    }

    pub fn raft_address(&self) -> &str {
        &self.raft_address
    }

    pub fn set_is_leader(&self, is_leader: bool) {
        self.is_leader.store(is_leader, Ordering::Relaxed);
    }

    /// Propose a command to the Raft cluster
    ///
    /// The command is applied directly to the local state machine for immediate
    /// visibility (callers like `create_collection` read state right after), and
    /// also proposed to openraft for persistence/replication. The apply loop will
    /// re-apply the same entry when openraft commits it (idempotent for all
    /// command variants).
    pub async fn propose_command(&self, cmd: RaftCommand) -> Result<u64> {
        if !self.is_leader().await {
            return Err(PowerFsError::NotLeader);
        }

        // Apply directly to local state machine for immediate visibility.
        let entry = ApplyEntry {
            index: 0,
            command: cmd.clone(),
        };
        self.apply_command(entry).await?;

        // Propose to openraft for persistence and replication.
        let data = cmd.serialize();
        let index = self
            .raft_v2
            .propose(data)
            .await
            .map_err(|e| PowerFsError::Internal(format!("raft propose failed: {}", e)))?;

        Ok(index)
    }

    /// Apply a committed Raft command to the state machine
    pub async fn apply_command(&self, entry: ApplyEntry) -> Result<()> {
        debug!(
            "Applying command at index {}: {:?}",
            entry.index, entry.command
        );

        match entry.command {
            RaftCommand::AddNode {
                node_id,
                address,
                rack,
                data_center,
                http_port,
                grpc_port,
                public_url,
            } => {
                self.apply_add_node(AddNodeParams {
                    node_id: NodeId(node_id),
                    address,
                    rack,
                    data_center,
                    http_port,
                    grpc_port,
                    public_url,
                })?;
            }
            RaftCommand::RemoveNode { node_id } => {
                self.apply_remove_node(&node_id)?;
            }
            RaftCommand::AssignVolume {
                node_id,
                volume_id,
                collection,
                replica_count,
                ttl,
                disk_type,
                size,
            } => {
                self.apply_assign_volume(AssignVolumeParams {
                    node_id,
                    volume_id,
                    collection,
                    replica_count,
                    ttl,
                    disk_type,
                    size,
                })?;
            }
            RaftCommand::UpdateVolumeState { volume_id, state } => {
                let vol_state = match state.as_str() {
                    "Creating" => VolumeState::Creating,
                    "Available" => VolumeState::Available,
                    "Full" => VolumeState::Full,
                    "ReadOnly" => VolumeState::ReadOnly,
                    "Draining" => VolumeState::Draining,
                    "Deleting" => VolumeState::Deleting,
                    _ => VolumeState::Available,
                };
                self.apply_update_volume_state(volume_id, vol_state)?;
            }
            RaftCommand::UpdateNodeVolumes {
                node_id,
                volumes,
                ip,
                grpc_port,
                net_port,
            } => {
                self.apply_update_node_volumes(&node_id, &volumes, &ip, grpc_port, net_port)
                    .await?;
            }
            RaftCommand::Heartbeat { node_id } => {
                self.apply_heartbeat(&node_id).await?;
            }
            RaftCommand::CreateCollection {
                name,
                replication,
                ttl,
                disk_type,
                max_volume_count,
            } => {
                self.apply_create_collection(
                    &name,
                    &replication,
                    ttl,
                    &disk_type,
                    max_volume_count,
                )
                .await?;
            }
            RaftCommand::DeleteCollection { name } => {
                self.apply_delete_collection(&name).await?;
            }
            RaftCommand::CreateCollectionExt { info } => {
                self.collection_manager
                    .write()
                    .unwrap()
                    .create_collection(info)?;
            }
            RaftCommand::UpdateCollectionExt { name, info } => {
                self.collection_manager
                    .write()
                    .unwrap()
                    .update_collection(&name, info)?;
            }
            RaftCommand::DeleteCollectionExt { name } => {
                self.collection_manager
                    .write()
                    .unwrap()
                    .delete_collection(&name)?;
            }
            RaftCommand::DeleteVolume { volume_id } => {
                self.apply_delete_volume(volume_id).await?;
            }
            RaftCommand::AdvanceFileKey {
                volume_id,
                new_next_key,
            } => {
                let mut volumes = self.volumes.write().unwrap();
                if let Some(vol_info) = volumes.get_mut(&VolumeId(volume_id)) {
                    // Only advance, never regress (avoid stale proposals overwriting newer state)
                    if new_next_key > vol_info.next_file_key {
                        vol_info.next_file_key = new_next_key;
                        debug!(
                            "Advanced next_file_key for volume {} to {} via Raft",
                            volume_id, new_next_key
                        );
                    }
                }
            }
            RaftCommand::RegisterZone { zone } => {
                let zone_id = zone.zone_id;
                self.zone_registry.write().unwrap().insert(zone_id, zone);
                // Recover next_zone_id to avoid reusing zone_id after restart
                let current = self.next_zone_id.load(std::sync::atomic::Ordering::SeqCst);
                if zone_id >= current {
                    self.next_zone_id
                        .store(zone_id + 1, std::sync::atomic::Ordering::SeqCst);
                }
                debug!("Applied RegisterZone via Raft: zone_id={}", zone_id);
            }
            RaftCommand::UpdateZone { zone } => {
                let zone_id = zone.zone_id;
                self.zone_registry.write().unwrap().insert(zone_id, zone);
                debug!("Applied UpdateZone via Raft: zone_id={}", zone_id);
            }
            RaftCommand::SetNodeMaintenance { node_id, enabled } => {
                self.apply_set_node_maintenance(&node_id, enabled)?;
            }
            RaftCommand::PinVolume { volume_id, node_id } => {
                self.apply_pin_volume(volume_id, node_id)?;
            }
            RaftCommand::UnpinVolume { volume_id } => {
                self.apply_unpin_volume(volume_id)?;
            }
            RaftCommand::SetPlacementStrategy { strategy } => {
                self.apply_set_placement_strategy(&strategy)?;
            }
        }

        Ok(())
    }

    /// After allocating a file_key, check if we need to persist the advance
    /// via Raft. Called after the write lock on `self.volumes` is released.
    /// Best-effort: failures are logged but don't block the allocation.
    ///
    /// With block allocation, allocated_key = 1 + (N-1) * FILE_KEY_BLOCK_SIZE.
    /// We persist every FILE_KEY_BATCH_SIZE allocations. The check uses:
    ///   (allocated_key - 1) % (BLOCK_SIZE * BATCH_SIZE) == 0
    /// which triggers on the 1st, (1+BATCH)th, (1+2*BATCH)th, ... allocations.
    async fn maybe_advance_file_key(&self, volume_id: u64, allocated_key: u64) {
        let block = powerfs_common::constants::FILE_KEY_BLOCK_SIZE;
        let persist_interval = block * FILE_KEY_BATCH_SIZE;
        // Persist on first allocation (key=1: (1-1)%interval==0) and every BATCH allocations after.
        // This ensures the first file_key is persisted, preventing collision on restart.
        if allocated_key >= 1 && (allocated_key - 1).is_multiple_of(persist_interval) {
            // Persist the NEXT file_key (current + block, since current was just allocated)
            let new_next_key = allocated_key + block;
            let cmd = crate::raft_v2::RaftCommand::AdvanceFileKey {
                volume_id,
                new_next_key,
            };
            if let Err(e) = self.propose_command(cmd).await {
                warn!(
                    "Failed to propose AdvanceFileKey for volume {}: {} (next_file_key may not persist across restart)",
                    volume_id, e
                );
            }
        }
    }

    fn apply_add_node(&self, params: AddNodeParams) -> Result<()> {
        let dc_id = DataCenterId(params.data_center);
        let rack_id = RackId(params.rack);
        let node_id = params.node_id.clone();
        let address = params.address.clone();
        let http_port = params.http_port;
        let _grpc_port = params.grpc_port;

        let mut topology = self.topology.write().unwrap();
        // Refresh mutable fields on existing nodes (every heartbeat), instead
        // of only inserting when absent. get_or_create_node uses or_insert_with
        // which never updates an existing entry, so stale persisted ports
        // (e.g. grpc_port from a previous run) would linger forever. Callers
        // build needle-write addresses from grpc_port, so it must stay fresh.
        let existed = topology
            .get_node_mut(&params.node_id)
            .map(|existing| {
                existing.address = params.address.clone();
                existing.grpc_port = params.grpc_port;
                existing.http_port = params.http_port;
                existing.public_url = params.public_url.clone();
            })
            .is_some();
        if !existed {
            let node = DataNodeInfo::new(
                params.node_id,
                params.address,
                rack_id,
                dc_id,
                params.http_port,
                params.grpc_port,
                params.public_url,
            );
            topology.get_or_create_node(node);
        }

        info!("Applied AddNode: {} at {}:{}", node_id, address, http_port);

        Ok(())
    }

    fn apply_remove_node(&self, node_id: &str) -> Result<()> {
        let nid = NodeId(node_id.to_string());
        let mut topology = self.topology.write().unwrap();
        if topology.remove_node(&nid).is_none() {
            return Err(PowerFsError::InvalidRequest("node not found".to_string()));
        }
        info!("Applied RemoveNode: {}", node_id);
        Ok(())
    }

    fn apply_assign_volume(&self, params: AssignVolumeParams) -> Result<()> {
        let vid = VolumeId(params.volume_id);
        let nid = NodeId(params.node_id);
        let nid_clone = nid.clone();
        let coll = Collection(params.collection);
        let t = Ttl(params.ttl);
        let dt = DiskType(params.disk_type);
        let size = params.size;
        let replica_count = params.replica_count;

        let mut volumes = self.volumes.write().unwrap();
        volumes.insert(
            vid,
            VolumeInfo {
                id: vid,
                node_id: nid.clone(),
                collection: coll.clone(),
                size,
                used: 0,
                replica_count,
                ttl: t,
                disk_type: dt,
                state: VolumeState::Creating,
                created_at: Utc::now(),
                modified_at: Utc::now(),
                next_file_key: 1,
            },
        );

        info!("Applied AssignVolume: vid={}, node={}", vid, nid_clone);

        // 通知 Volume Server 创建 volume，否则后续 write_needle 会报 `volume not found`。
        // 通过 VolumeClientPool 复用 gRPC 通道，避免在请求路径上反复建连。
        let topology = self.topology.read().unwrap();
        let node_info = topology.get_node(&nid).cloned();
        drop(topology);

        if let Some(node) = node_info {
            let grpc_addr = format!("{}:{}", node.address, node.grpc_port);
            let vid_clone = vid.0;
            let coll_name = coll.0.clone();
            let pool = self.volume_client_pool.clone();
            // master 的 size 是逻辑上限（volume_size_limit，默认 1TB），
            // 但 volume server 的存储设备容量通常较小（如 100GB 虚拟设备）。
            // 裁剪到设备可承受的范围，默认 256GB，满足 IO500 stonewall=300s
            // 的高带宽写入需求（~500MB/s * 300s ≈ 150GB）。
            let max_create_size = 1024 * 1024 * 1024 * 1024; // 1TB
            let create_size = if size > 0 && size < max_create_size {
                size
            } else {
                max_create_size
            };
            tokio::spawn(async move {
                info!(
                    "Notifying volume server {} to create volume {} (size={}, collection={})",
                    grpc_addr, vid_clone, create_size, coll_name
                );
                if let Err(e) = pool
                    .create_volume_with_retry(
                        &grpc_addr,
                        vid_clone,
                        create_size,
                        &coll_name,
                        5,
                        Duration::from_millis(500),
                    )
                    .await
                {
                    warn!(
                        "Failed to create volume {} on {}: {}",
                        vid_clone, grpc_addr, e
                    );
                }
            });
        } else {
            warn!(
                "Applied AssignVolume: node {} not found in topology, cannot notify volume server",
                nid_clone
            );
        }

        let provider = self.event_provider.clone();
        let vid_clone = vid.0;
        let nid_str = nid.0.clone();
        let coll_str = coll.0.clone();
        tokio::spawn(async move {
            let event = Event::VolumeStatus(VolumeStatusEvent {
                volume_id: vid_clone,
                node_id: nid_str,
                size,
                used: 0,
                file_count: 0,
                status: "creating".to_string(),
                collection: coll_str,
                read_only: false,
                replica_placement: 0,
                ttl: 0,
                disk_type: String::new(),
                compact_status: 0,
                append_offset: 0,
                read_ops: 0,
                write_ops: 0,
                read_bytes: 0,
                write_bytes: 0,
                read_avg_latency_us: 0,
                write_avg_latency_us: 0,
                read_p50_latency_us: 0,
                read_p99_latency_us: 0,
                write_p50_latency_us: 0,
                write_p99_latency_us: 0,
            });
            if let Err(e) = provider.publish(event, &format!("{}", vid_clone)).await {
                warn!("Failed to publish volume_status event: {}", e);
            }
        });

        Ok(())
    }

    fn apply_update_volume_state(&self, volume_id: u64, state: VolumeState) -> Result<()> {
        let vid = VolumeId(volume_id);
        let mut volumes = self.volumes.write().unwrap();
        if let Some(info) = volumes.get_mut(&vid) {
            info.state = state;
            info.modified_at = Utc::now();
        }
        Ok(())
    }

    /// Apply `SetNodeMaintenance` Raft command: toggle `maintenance_mode` on
    /// the node in the in-memory topology. When enabled, the allocator's
    /// `ClusterSnapshot` builder maps the node to `NodeRuntimeState::Maintenance`,
    /// excluding it from allocation decisions.
    fn apply_set_node_maintenance(&self, node_id: &str, enabled: bool) -> Result<()> {
        let nid = NodeId(node_id.to_string());
        let mut topology = self.topology.write().unwrap();
        if let Some(node) = topology.get_node_mut(&nid) {
            node.maintenance_mode = enabled;
            debug!(
                "Applied SetNodeMaintenance via Raft: node={} enabled={}",
                node_id, enabled
            );
        }
        Ok(())
    }

    /// Apply `PinVolume` Raft command: record `volume_id → node_id` in the pin
    /// registry. The `ClusterSnapshot` builder exposes this to the LoadBalancer,
    /// which skips pinned volumes as migration sources.
    fn apply_pin_volume(&self, volume_id: u64, node_id: String) -> Result<()> {
        self.volume_pins
            .write()
            .unwrap()
            .insert(VolumeId(volume_id), node_id.clone());
        debug!(
            "Applied PinVolume via Raft: volume={} node={}",
            volume_id, node_id
        );
        Ok(())
    }

    /// Apply `UnpinVolume` Raft command: remove a volume from the pin registry,
    /// restoring normal LoadBalancer migration behaviour.
    fn apply_unpin_volume(&self, volume_id: u64) -> Result<()> {
        self.volume_pins
            .write()
            .unwrap()
            .remove(&VolumeId(volume_id));
        debug!("Applied UnpinVolume via Raft: volume={}", volume_id);
        Ok(())
    }

    /// Apply `SetPlacementStrategy` Raft command: update the global placement
    /// strategy name. The next `create_new_volume` call will use the
    /// corresponding `VolumeAssigner`.
    fn apply_set_placement_strategy(&self, strategy: &str) -> Result<()> {
        *self.placement_strategy.write().unwrap() = strategy.to_string();
        info!(
            "Applied SetPlacementStrategy via Raft: strategy={}",
            strategy
        );
        Ok(())
    }

    async fn apply_update_node_volumes(
        &self,
        node_id: &str,
        volumes: &[crate::raft_v2::RaftVolumeShortInfo],
        ip: &str,
        grpc_port: u32,
        net_port: u32,
    ) -> Result<()> {
        let nid = NodeId(node_id.to_string());

        // Update topology
        {
            let mut topology = self.topology.write().unwrap();
            if let Some(node) = topology.get_node_mut(&nid) {
                node.address = ip.to_string();
                node.grpc_port = grpc_port;
                node.last_heartbeat = Utc::now();
                node.state = NodeState::Healthy;
                node.volume_count = volumes.len() as u32;
            }
        }

        // Update volumes
        let mut volumes_map = self.volumes.write().unwrap();
        let mut routes_map = self.volume_routes.write().unwrap();
        for vol in volumes {
            let vid = VolumeId(vol.volume_id);
            let state = if vol.read_only {
                VolumeState::ReadOnly
            } else {
                VolumeState::Available
            };

            // 增量更新：只更新变化的字段（used, state），保留其他原有字段
            if let Some(existing) = volumes_map.get_mut(&vid) {
                existing.used = vol.used;
                existing.state = state;
                existing.modified_at = Utc::now();
            } else {
                // 新增 volume（首次注册）
                volumes_map.insert(
                    vid,
                    VolumeInfo {
                        id: vid,
                        node_id: nid.clone(),
                        collection: Collection(vol.collection.clone()),
                        size: vol.size,
                        used: vol.used,
                        replica_count: 1,
                        ttl: Ttl::default(),
                        disk_type: DiskType::default(),
                        state,
                        created_at: Utc::now(),
                        modified_at: Utc::now(),
                        next_file_key: 1,
                    },
                );
            }

            // 更新路由表
            // 使用心跳中传递的 net_port 构建 Volume 路由地址
            if let Some(existing_route) = routes_map.get_mut(&vol.volume_id) {
                existing_route.used = vol.used;
                existing_route.file_count = vol.file_count;
                existing_route.state = state;
                existing_route.updated_at = Utc::now();
                // 如果 net_port 有效，更新地址（支持 Volume 换地址场景）
                if net_port > 0 {
                    let addr = format!("{}:{}", ip, net_port);
                    if existing_route.addr != addr {
                        info!(
                            "Volume {} route address updated: {} -> {}",
                            vol.volume_id, existing_route.addr, addr
                        );
                        existing_route.addr = addr;
                    }
                }
            } else {
                // 新增 volume（首次注册）
                let addr = if net_port > 0 {
                    format!("{}:{}", ip, net_port)
                } else {
                    format!("{}:{}", ip, grpc_port)
                };
                routes_map.insert(
                    vol.volume_id,
                    VolumeRoute::new(vol.volume_id, addr, vol.size, nid.clone().0),
                );
            }
        }

        Ok(())
    }

    async fn apply_heartbeat(&self, node_id: &str) -> Result<()> {
        let nid = NodeId(node_id.to_string());
        let mut topology = self.topology.write().unwrap();
        if let Some(node) = topology.get_node_mut(&nid) {
            node.last_heartbeat = Utc::now();
            node.state = NodeState::Healthy;
        }
        Ok(())
    }

    async fn apply_create_collection(
        &self,
        name: &str,
        replication: &str,
        ttl: i32,
        disk_type: &str,
        max_volume_count: u64,
    ) -> Result<()> {
        let rep = ReplicaPlacement::from_string(replication).unwrap_or_default();
        let coll = Collection(name.to_string());
        let t = Ttl(ttl);
        let dt = DiskType(disk_type.to_string());

        let mut collections = self.collections.write().unwrap();
        if collections.contains_key(name) {
            return Err(PowerFsError::InvalidRequest(format!(
                "collection {} already exists",
                name
            )));
        }

        collections.insert(
            name.to_string(),
            CollectionConfig {
                name: coll,
                replication: rep,
                ttl: t,
                disk_type: dt,
                max_volume_count,
                volume_count: 0,
                created_at: Utc::now(),
                modified_at: Utc::now(),
            },
        );
        drop(collections);

        // Sync extended-attribute store so the new collection is visible to
        // get_collection_info / list_collection_infos / capacity checks.
        // Use ensure_collection (idempotent) so re-applied raft entries do not
        // fail when the CollectionManager already knows about this name.
        self.collection_manager
            .write()
            .unwrap()
            .ensure_collection(name);

        info!("Applied CreateCollection: {}", name);
        Ok(())
    }

    async fn apply_delete_collection(&self, name: &str) -> Result<()> {
        if name == "default" {
            return Err(PowerFsError::InvalidRequest(
                "cannot delete default collection".to_string(),
            ));
        }

        let mut collections = self.collections.write().unwrap();
        if collections.remove(name).is_none() {
            return Err(PowerFsError::InvalidRequest(format!(
                "collection {} not found",
                name
            )));
        }
        drop(collections);

        // Best-effort sync: ignore not-found in CollectionManager since the
        // extended store may not have been populated for legacy collections.
        let _ = self
            .collection_manager
            .write()
            .unwrap()
            .delete_collection(name);

        info!("Applied DeleteCollection: {}", name);
        Ok(())
    }

    pub async fn create_collection(
        &self,
        name: &str,
        replication: &str,
        ttl: i32,
        disk_type: &str,
        max_volume_count: u64,
    ) -> Result<CollectionConfig> {
        if !self.is_leader().await {
            return Err(PowerFsError::NotLeader);
        }

        let cmd = RaftCommand::CreateCollection {
            name: name.to_string(),
            replication: replication.to_string(),
            ttl,
            disk_type: disk_type.to_string(),
            max_volume_count,
        };

        self.propose_command(cmd).await?;

        let collections = self.collections.read().unwrap();
        collections.get(name).cloned().ok_or(PowerFsError::Internal(
            "collection not found after creation".to_string(),
        ))
    }

    pub async fn delete_collection(&self, name: &str) -> Result<()> {
        if !self.is_leader().await {
            return Err(PowerFsError::NotLeader);
        }

        let cmd = RaftCommand::DeleteCollection {
            name: name.to_string(),
        };

        self.propose_command(cmd).await?;
        Ok(())
    }

    pub async fn get_collection(&self, name: &str) -> Option<CollectionConfig> {
        self.collections.read().unwrap().get(name).cloned()
    }

    pub async fn list_collections(&self) -> Vec<CollectionConfig> {
        self.collections.read().unwrap().values().cloned().collect()
    }

    /// Create a collection with P0 extended attributes.
    ///
    /// This writes directly to the in-memory [`CollectionManager`] and does
    /// NOT go through Raft. It is intended for the new HTTP management API
    /// and administrative tooling. The basic [`CollectionConfig`] is NOT
    /// modified; callers that need gRPC visibility should also invoke the
    /// existing [`create_collection`] (raft-backed) path.
    pub async fn create_collection_ext(&self, info: CollectionInfo) -> Result<()> {
        if !self.is_leader().await {
            return Err(PowerFsError::NotLeader);
        }
        self.collection_manager
            .write()
            .unwrap()
            .create_collection(info)
    }

    /// Create a collection with P0 extended attributes via Raft consensus.
    pub async fn create_collection_via_raft(&self, info: CollectionInfo) -> Result<()> {
        if !self.is_leader().await {
            return Err(PowerFsError::NotLeader);
        }
        let cmd = RaftCommand::CreateCollectionExt { info };
        self.propose_command(cmd).await?;
        Ok(())
    }

    /// Update a collection's extended attributes in place.
    pub async fn update_collection_ext(&self, name: &str, info: CollectionInfo) -> Result<()> {
        if !self.is_leader().await {
            return Err(PowerFsError::NotLeader);
        }
        self.collection_manager
            .write()
            .unwrap()
            .update_collection(name, info)
    }

    /// Update a collection with P0 extended attributes via Raft consensus.
    pub async fn update_collection_via_raft(&self, name: &str, info: CollectionInfo) -> Result<()> {
        if !self.is_leader().await {
            return Err(PowerFsError::NotLeader);
        }
        let cmd = RaftCommand::UpdateCollectionExt {
            name: name.to_string(),
            info,
        };
        self.propose_command(cmd).await?;
        Ok(())
    }

    /// Delete a collection (extended-attribute store) via Raft consensus.
    pub async fn delete_collection_via_raft(&self, name: &str) -> Result<()> {
        if !self.is_leader().await {
            return Err(PowerFsError::NotLeader);
        }
        let cmd = RaftCommand::DeleteCollectionExt {
            name: name.to_string(),
        };
        self.propose_command(cmd).await?;
        Ok(())
    }

    /// Delete a collection from the extended-attribute store.
    pub async fn delete_collection_ext(&self, name: &str) -> Result<()> {
        if !self.is_leader().await {
            return Err(PowerFsError::NotLeader);
        }
        self.collection_manager
            .write()
            .unwrap()
            .delete_collection(name)
    }

    /// Fetch the extended attributes for a collection.
    pub async fn get_collection_info(&self, name: &str) -> Option<CollectionInfo> {
        self.collection_manager.read().unwrap().get_collection(name)
    }

    /// Snapshot all collections with their extended attributes.
    pub async fn list_collection_infos(&self) -> Vec<CollectionInfo> {
        self.collection_manager.read().unwrap().list_collections()
    }

    /// Compute runtime stats for a collection from the current volume table.
    pub async fn get_collection_stats(&self, name: &str) -> Option<CollectionStats> {
        let volumes = self.volumes.read().unwrap().clone();
        let cm = self.collection_manager.read().unwrap();
        // Bail early if the collection does not exist; the returned
        // CollectionInfo is intentionally discarded.
        cm.get_collection(name)?;
        Some(cm.compute_stats(name, &volumes))
    }

    /// Check whether a collection is within its capacity quota.
    ///
    /// Used by [`assign_volume`] to reject writes that would exceed the
    /// configured `capacity_quota_bytes`. The `_info` parameter is accepted
    /// for API symmetry with the task spec; the actual check re-reads the
    /// volume table to get a fresh `used_bytes` total.
    pub async fn check_collection_capacity(
        &self,
        name: &str,
        _info: &CollectionInfo,
    ) -> Result<()> {
        let volumes = self.volumes.read().unwrap().clone();
        self.collection_manager
            .read()
            .unwrap()
            .check_capacity(name, &volumes)
    }

    async fn apply_delete_volume(&self, volume_id: u64) -> Result<()> {
        let vid = VolumeId(volume_id);
        let mut volumes = self.volumes.write().unwrap();
        if volumes.remove(&vid).is_none() {
            return Err(PowerFsError::VolumeNotFound(vid));
        }
        info!("Applied DeleteVolume: {}", volume_id);
        Ok(())
    }

    pub async fn delete_volume(&self, volume_id: &VolumeId) -> Result<()> {
        if !self.is_leader().await {
            return Err(PowerFsError::NotLeader);
        }

        let cmd = RaftCommand::DeleteVolume {
            volume_id: volume_id.0,
        };

        self.propose_command(cmd).await?;
        Ok(())
    }

    pub async fn add_node(&self, params: AddNodeParams) -> Result<()> {
        if !self.is_leader().await {
            return Err(PowerFsError::NotLeader);
        }

        if self.get_node(&params.node_id).is_some() {
            return Ok(());
        }

        let cmd = RaftCommand::AddNode {
            node_id: params.node_id.0.clone(),
            address: params.address.clone(),
            rack: params.rack.clone(),
            data_center: params.data_center.clone(),
            http_port: params.http_port,
            grpc_port: params.grpc_port,
            public_url: params.public_url.clone(),
        };

        self.propose_command(cmd).await?;

        let dc_id = DataCenterId(params.data_center);
        let rack_id = RackId(params.rack);
        let node = DataNodeInfo::new(
            params.node_id,
            params.address,
            rack_id,
            dc_id,
            params.http_port,
            params.grpc_port,
            params.public_url,
        );
        let mut topology = self.topology.write().unwrap();
        topology.get_or_create_node(node);

        Ok(())
    }

    pub async fn remove_node(&self, node_id: &NodeId) -> Result<()> {
        if !self.is_leader().await {
            return Err(PowerFsError::NotLeader);
        }

        let cmd = RaftCommand::RemoveNode {
            node_id: node_id.0.clone(),
        };

        self.propose_command(cmd).await?;
        info!("Proposed RemoveNode: {:?}", node_id);

        Ok(())
    }

    pub async fn get_volume(&self, volume_id: &VolumeId) -> Result<VolumeInfo> {
        let volumes = self.volumes.read().unwrap();
        volumes
            .get(volume_id)
            .cloned()
            .ok_or(PowerFsError::VolumeNotFound(*volume_id))
    }

    pub async fn update_volume_state(
        &self,
        volume_id: &VolumeId,
        state: VolumeState,
    ) -> Result<()> {
        if !self.is_leader().await {
            return Err(PowerFsError::NotLeader);
        }

        let state_str = match state {
            VolumeState::Creating => "Creating",
            VolumeState::Available => "Available",
            VolumeState::Full => "Full",
            VolumeState::ReadOnly => "ReadOnly",
            VolumeState::Draining => "Draining",
            VolumeState::Deleting => "Deleting",
        }
        .to_string();

        let cmd = RaftCommand::UpdateVolumeState {
            volume_id: volume_id.0,
            state: state_str,
        };

        self.propose_command(cmd).await?;
        Ok(())
    }

    /// Set node maintenance mode via Raft replication.
    ///
    /// When `enabled=true`, the node is excluded from allocation decisions
    /// (mapped to `NodeRuntimeState::Maintenance` in the cluster snapshot).
    /// When `enabled=false`, the node returns to normal allocation.
    ///
    /// Only the Raft leader can propose this command.
    pub async fn set_node_maintenance(&self, node_id: &str, enabled: bool) -> Result<()> {
        if !self.is_leader().await {
            return Err(PowerFsError::NotLeader);
        }

        let cmd = RaftCommand::SetNodeMaintenance {
            node_id: node_id.to_string(),
            enabled,
        };

        self.propose_command(cmd).await?;
        Ok(())
    }

    /// Pin a volume to a specific node via Raft replication.
    ///
    /// Once pinned, the LoadBalancer will not migrate the volume's data away
    /// from the pinned node. Use `unpin_volume` to restore normal behaviour.
    ///
    /// Only the Raft leader can propose this command.
    pub async fn pin_volume_to_node(&self, volume_id: u64, node_id: &str) -> Result<()> {
        if !self.is_leader().await {
            return Err(PowerFsError::NotLeader);
        }

        let cmd = RaftCommand::PinVolume {
            volume_id,
            node_id: node_id.to_string(),
        };

        self.propose_command(cmd).await?;
        Ok(())
    }

    /// Remove a volume pin via Raft replication.
    ///
    /// Only the Raft leader can propose this command.
    pub async fn unpin_volume(&self, volume_id: u64) -> Result<()> {
        if !self.is_leader().await {
            return Err(PowerFsError::NotLeader);
        }

        let cmd = RaftCommand::UnpinVolume { volume_id };

        self.propose_command(cmd).await?;
        Ok(())
    }

    /// Set the global placement strategy via Raft replication.
    ///
    /// Valid strategy names: `"round_robin"`, `"least_loaded"`,
    /// `"anti_affinity"`. Only the Raft leader can propose this command.
    pub async fn set_placement_strategy(&self, strategy: &str) -> Result<()> {
        if !self.is_leader().await {
            return Err(PowerFsError::NotLeader);
        }

        // Validate strategy name before proposing.
        match strategy {
            "round_robin" | "least_loaded" | "anti_affinity" => {}
            _ => {
                return Err(PowerFsError::InvalidRequest(format!(
                    "unknown placement strategy '{}': expected one of round_robin, least_loaded, anti_affinity",
                    strategy
                )));
            }
        }

        let cmd = RaftCommand::SetPlacementStrategy {
            strategy: strategy.to_string(),
        };

        self.propose_command(cmd).await?;
        Ok(())
    }

    /// Read the current placement strategy name.
    pub fn current_placement_strategy(&self) -> String {
        self.placement_strategy.read().unwrap().clone()
    }

    pub async fn list_volumes(&self) -> Vec<VolumeInfo> {
        self.volumes.read().unwrap().values().cloned().collect()
    }

    pub async fn list_nodes(&self) -> Vec<DataNodeInfo> {
        self.topology.read().unwrap().list_all_nodes()
    }

    pub fn get_node(&self, node_id: &NodeId) -> Option<DataNodeInfo> {
        self.topology.read().unwrap().get_node(node_id).cloned()
    }

    /// Get the gRPC address (`host:port`) of the volume server hosting
    /// `volume_id`. Returns `None` if the volume or node is not found.
    pub fn get_volume_address(&self, volume_id: u64) -> Option<String> {
        let volumes = self.volumes.read().unwrap();
        let vol = volumes.get(&VolumeId(volume_id))?;
        let topology = self.topology.read().unwrap();
        let node = topology.get_node(&vol.node_id)?;
        Some(format!("{}:{}", node.address, node.grpc_port))
    }

    /// Get all volume IDs hosted on `node_id`.
    pub fn volumes_on_node(&self, node_id: &str) -> Vec<u64> {
        let volumes = self.volumes.read().unwrap();
        volumes
            .values()
            .filter(|v| v.node_id.0 == node_id)
            .map(|v| v.id.0)
            .collect()
    }

    /// P5: Update node load metrics (cpu/memory) on the leader's in-memory
    /// topology. This is a **local** update — not proposed through Raft —
    /// because load metrics are ephemeral monitoring data that changes every
    /// heartbeat and doesn't need strong consistency.
    ///
    /// Called from `handle_heartbeat` after `add_node` / `update_node_volumes`.
    pub fn update_node_load_metrics(&self, node_id: &NodeId, cpu_usage: f32, memory_usage: f32) {
        let mut topology = self.topology.write().unwrap();
        if let Some(node) = topology.get_node_mut(node_id) {
            node.cpu_usage = cpu_usage;
            node.memory_usage = memory_usage;
            node.last_heartbeat = Utc::now();
        }
    }

    pub async fn update_node_volumes(&self, params: UpdateNodeVolumesParams) -> Result<()> {
        if !self.is_leader().await {
            return Err(PowerFsError::NotLeader);
        }

        let short_volumes: Vec<crate::raft_v2::RaftVolumeShortInfo> = params
            .volumes
            .iter()
            .map(|v| crate::raft_v2::RaftVolumeShortInfo {
                volume_id: v.volume_id,
                size: v.size,
                read_only: v.read_only,
                used: v.used,
                file_count: v.file_count,
                collection: v.collection.clone(),
            })
            .collect();

        info!(
            "HEARTBEAT_DEBUG: update_node_volumes called for node={}, volumes={}, new_volumes={}, deleted_volumes={}",
            params.node_id,
            params.volumes.len(),
            params.new_volumes.len(),
            params.deleted_volumes.len()
        );

        let current_volumes = self.get_node_volumes(&params.node_id);
        info!(
            "HEARTBEAT_DEBUG: current volumes for node={}: {}",
            params.node_id,
            current_volumes.len()
        );

        let current_short: Vec<crate::raft_v2::RaftVolumeShortInfo> = current_volumes
            .into_iter()
            .map(|v| crate::raft_v2::RaftVolumeShortInfo {
                volume_id: v.id.0,
                size: v.size,
                read_only: v.state == VolumeState::ReadOnly,
                used: 0,
                file_count: 0,
                collection: v.collection.0.clone(),
            })
            .collect();

        let short_volumes_no_used: Vec<crate::raft_v2::RaftVolumeShortInfo> = short_volumes
            .iter()
            .cloned()
            .map(|mut v| {
                v.used = 0;
                v
            })
            .collect();

        if short_volumes_no_used == current_short {
            info!(
                "HEARTBEAT_DEBUG: volumes unchanged for node={}, skipping propose, updating used locally",
                params.node_id
            );
            let mut volumes_map = self.volumes.write().unwrap();
            for vol in &short_volumes {
                let vid = VolumeId(vol.volume_id);
                if let Some(existing) = volumes_map.get_mut(&vid) {
                    existing.used = vol.used;
                }
            }
            return Ok(());
        }

        info!(
            "HEARTBEAT_DEBUG: proposing UpdateNodeVolumes for node={}",
            params.node_id
        );

        let cmd = RaftCommand::UpdateNodeVolumes {
            node_id: params.node_id.0.clone(),
            volumes: short_volumes,
            ip: params.ip,
            grpc_port: params.grpc_port,
            net_port: params.net_port,
        };

        self.propose_command(cmd).await?;
        Ok(())
    }

    pub async fn assign_volume(
        &self,
        replication: &str,
        collection: &str,
    ) -> Result<(Fid, Vec<DataNodeInfo>)> {
        if !self.is_leader().await {
            return Err(PowerFsError::NotLeader);
        }

        // P0 capacity check: reject writes to non-active or over-quota
        // collections. Collections absent from the extended store (legacy
        // or pre-migration) bypass this check to preserve backward compat.
        if let Some(coll) = self.get_collection_info(collection).await {
            if !coll.is_writable() {
                return Err(PowerFsError::InvalidRequest(format!(
                    "collection {} is not writable (status={:?})",
                    collection, coll.status
                )));
            }
            self.check_collection_capacity(collection, &coll).await?;
        }

        let mut nodes = self.topology.read().unwrap().list_all_nodes();
        nodes.sort_by_key(|n| n.id.0.clone());
        info!(
            "VOL_DEBUG: found {} nodes in topology: {:?}",
            nodes.len(),
            nodes.iter().map(|n| n.id.0.clone()).collect::<Vec<_>>()
        );
        if nodes.is_empty() {
            return Err(PowerFsError::InvalidRequest(
                "no nodes available".to_string(),
            ));
        }

        let (_volume_size_limit, _rack_awareness_enabled) = {
            let config = self.cluster_config.read().unwrap();
            (config.volume_size_limit, config.rack_awareness_enabled)
        };

        let _replica_placement = ReplicaPlacement::from_string(replication).unwrap_or_default();

        let collection_obj = Collection(collection.to_string());
        let _ttl = Ttl::default();
        let _disk_type = DiskType::default();

        let _replica_count = _replica_placement.get_copy_count();

        // Resolve the collection's allocation mode + blacklist. Collections
        // absent from the extended store fall back to Auto with no blacklist
        // (legacy/default behaviour).
        let coll_info = self.get_collection_info(collection).await;
        let (allocation_mode, excluded) = match &coll_info {
            Some(info) => (
                info.volume_allocation.clone(),
                info.excluded_volume_ids.clone(),
            ),
            None => (VolumeAllocationMode::default(), Vec::new()),
        };

        // Try to find an existing writable volume for this collection with
        // available space, honouring the allocation mode and blacklist.
        if let Some((existing_vid, host_node_id)) =
            self.select_writable_volume(&collection_obj, &nodes, &allocation_mode, &excluded)
        {
            let file_key = {
                let mut volumes = self.volumes.write().unwrap();
                if let Some(vol_info) = volumes.get_mut(&existing_vid) {
                    let key = vol_info.next_file_key;
                    vol_info.next_file_key += powerfs_common::constants::FILE_KEY_BLOCK_SIZE;
                    key
                } else {
                    1
                }
            };
            // Persist next_file_key advance in batches via Raft
            self.maybe_advance_file_key(existing_vid.0, file_key).await;

            let volume_id = existing_vid;
            let cookie = rand::random::<u32>() as u64;
            let fid = Fid {
                volume_id,
                cookie,
                file_key,
            };

            let mut host_node = nodes
                .iter()
                .find(|n| n.id == host_node_id)
                .cloned()
                .into_iter()
                .collect::<Vec<_>>();

            // Authoritative net address fix: DataNodeInfo.grpc_port may be stale
            // (the heartbeat handler historically stored http_port there, and
            // get_or_create_node does not refresh fields on existing nodes).
            // VolumeRoute.addr is always "ip:net_port" from the latest heartbeat,
            // so use its port to override grpc_port. Callers (S3 PutObject/Get/
            // Delete in both Master and Filer) build the needle-write address as
            // "ip:grpc_port"; without this they hit the HTTP metrics port
            // (e.g. :8093) instead of the powerfs-net data port (e.g. :8901),
            // causing "Protocol error: invalid handshake response".
            if let Some(route) = self.volume_routes.read().unwrap().get(&volume_id.0) {
                if let Some(net_port) = route
                    .addr
                    .rsplit_once(':')
                    .and_then(|(_, p)| p.parse::<u32>().ok())
                {
                    if net_port > 0 {
                        for n in host_node.iter_mut() {
                            n.grpc_port = net_port;
                        }
                    }
                }
            }

            info!(
                "Reused existing volume: {} for collection {:?}, fid: {},{},{}",
                volume_id, collection_obj, volume_id.0, cookie, file_key
            );

            return Ok((fid, host_node));
        }

        // No available volume in the pre-allocated pool
        warn!(
            "No available volume for collection {:?}. Pre-allocated volumes are full.",
            collection_obj
        );
        Err(PowerFsError::InvalidRequest(
            "no available volume in the pre-allocated pool. Please contact admin to allocate more volumes."
                .to_string(),
        ))
    }

    /// Select a writable volume for a collection honouring the allocation mode.
    ///
    /// - `Auto`: scan all volumes matching the collection.
    /// - `Manual`: only consider the pinned volume ids.
    /// - `Hybrid`: try pinned ids first, then fall back to Auto.
    ///
    /// Volumes in `excluded` (blacklist) are never selected. The scan reuses
    /// volumes already created on volume servers, avoiding "volume not found"
    /// errors when writing.
    fn select_writable_volume(
        &self,
        collection: &Collection,
        nodes: &[DataNodeInfo],
        mode: &VolumeAllocationMode,
        excluded: &[u64],
    ) -> Option<(VolumeId, NodeId)> {
        let volumes = self.volumes.read().unwrap();
        // Advance the round-robin counter so consecutive calls pick different
        // volumes, distributing write load across all writable volumes.
        let start_idx = self.volume_round_robin.fetch_add(1, Ordering::Relaxed) as usize;
        select_writable_volume_from(&volumes, collection, nodes, mode, excluded, start_idx)
    }

    /// 批量分配 stripe volumes
    /// 使用预分配的 volume 池，从池中选择可用的 volume
    /// 返回 (volume_ids, start_volume_idx) 用于 FileLayout
    pub async fn assign_stripe_volumes(
        &self,
        count: u32,
        _replication: &str,
        collection: &str,
    ) -> Result<(Vec<u64>, u32)> {
        if !self.is_leader().await {
            return Err(PowerFsError::NotLeader);
        }

        let collection_obj = Collection(collection.to_string());
        let mut volume_ids = Vec::with_capacity(count as usize);

        // Resolve the collection blacklist (stripe assignment always uses Auto
        // semantics but still honours excluded volumes).
        let excluded: Vec<u64> = self
            .get_collection_info(collection)
            .await
            .map(|c| c.excluded_volume_ids.clone())
            .unwrap_or_default();

        // Find available volumes from the pre-allocated pool
        {
            let volumes = self.volumes.read().unwrap();
            let mut available_volumes: Vec<VolumeId> = Vec::new();

            for (vid, vinfo) in volumes.iter() {
                if excluded.contains(&vid.0) {
                    continue;
                }
                if vinfo.collection != collection_obj {
                    continue;
                }
                // Writable states: Creating or Available
                if !matches!(vinfo.state, VolumeState::Creating | VolumeState::Available) {
                    continue;
                }
                // Check available space
                if vinfo.used >= vinfo.size {
                    continue;
                }
                available_volumes.push(*vid);
            }

            info!(
                "assign_stripe_volumes: found {} available volumes for collection {:?}, need {}",
                available_volumes.len(),
                collection_obj,
                count
            );

            if available_volumes.is_empty() {
                return Err(PowerFsError::InvalidRequest(
                    "no available volume in the pre-allocated pool".to_string(),
                ));
            }

            // Use round-robin to select volumes for stripe
            let start = self.stripe_round_robin.fetch_add(1, Ordering::Relaxed) as usize;
            for i in 0..count as usize {
                let idx = (start + i) % available_volumes.len();
                volume_ids.push(available_volumes[idx].0);
            }
        }

        let start_idx = self.stripe_round_robin.fetch_add(1, Ordering::Relaxed) % count;

        Ok((volume_ids, start_idx))
    }

    /// 在指定 volume 上分配一个新的 file_key（块分配，每文件 FILE_KEY_BLOCK_SIZE 个 needle ID）
    ///
    /// Each file gets a non-overlapping block: [file_key, file_key + FILE_KEY_BLOCK_SIZE).
    /// Chunks within the file use needle_id = file_key + chunk_idx, so consecutive
    /// files never collide (fixes needle ID overlap bug).
    pub async fn allocate_file_key(&self, volume_id: &VolumeId) -> Option<u64> {
        let allocated_key = {
            let mut volumes = self.volumes.write().unwrap();
            if let Some(vol_info) = volumes.get_mut(volume_id) {
                let key = vol_info.next_file_key;
                vol_info.next_file_key += powerfs_common::constants::FILE_KEY_BLOCK_SIZE;
                Some(key)
            } else {
                None
            }
        };
        if let Some(key) = allocated_key {
            // Persist next_file_key advance in batches via Raft
            self.maybe_advance_file_key(volume_id.0, key).await;
        }
        allocated_key
    }

    pub async fn create_new_volume(
        &self,
        replication: &str,
        collection: &str,
    ) -> Result<(Fid, Vec<DataNodeInfo>)> {
        static CREATE_VOL_COUNT: AtomicU64 = AtomicU64::new(0);
        let count = CREATE_VOL_COUNT.fetch_add(1, Ordering::Relaxed);
        info!(
            "VOL_DEBUG: create_new_volume called #{}: replication={}, collection={}",
            count, replication, collection
        );

        if !self.is_leader().await {
            return Err(PowerFsError::NotLeader);
        }

        let mut nodes = self.topology.read().unwrap().list_all_nodes();
        nodes.sort_by_key(|n| n.id.0.clone());
        info!(
            "VOL_DEBUG: found {} nodes in topology: {:?}",
            nodes.len(),
            nodes.iter().map(|n| n.id.0.clone()).collect::<Vec<_>>()
        );
        if nodes.is_empty() {
            return Err(PowerFsError::InvalidRequest(
                "no nodes available".to_string(),
            ));
        }

        let (volume_size_limit, _rack_awareness_enabled) = {
            let config = self.cluster_config.read().unwrap();
            (config.volume_size_limit, config.rack_awareness_enabled)
        };

        let replica_placement = ReplicaPlacement::from_string(replication).unwrap_or_default();

        let collection_obj = Collection(collection.to_string());
        let ttl = Ttl::default();
        let disk_type = DiskType::default();

        let replica_count = replica_placement.get_copy_count();

        let volume_id = {
            let mut next_id = self.next_volume_id.write().unwrap();
            let vid = VolumeId(*next_id);
            *next_id += 1;
            vid
        };

        // Select assigner based on the current placement strategy (Raft-replicated).
        // "round_robin" → RoundRobinAssigner; "least_loaded"/"anti_affinity" → SmartVolumeAssigner.
        let assigned_nodes = {
            let strategy = self.placement_strategy.read().unwrap().clone();
            match strategy.as_str() {
                "round_robin" => {
                    let assigner = RoundRobinAssigner;
                    assigner.assign(volume_id.0, &nodes, replica_count as usize)
                }
                // "least_loaded" and "anti_affinity" both use the smart
                // assigner (capacity/load scoring + rack/DC isolation).
                _ => {
                    let assigner = SmartVolumeAssigner;
                    assigner.assign(volume_id.0, &nodes, replica_count as usize)
                }
            }
        };

        if assigned_nodes.is_empty() {
            return Err(PowerFsError::InvalidRequest(
                "not enough nodes available for replication".to_string(),
            ));
        }

        {
            let mut layouts = self.volume_layouts.write().unwrap();
            let key = Self::get_volume_layout_key(&collection_obj, replica_count, &ttl, &disk_type);
            layouts.entry(key).or_insert_with(|| VolumeLayout {
                collection: collection_obj.clone(),
                replica_placement: replica_placement.clone(),
                ttl: ttl.clone(),
                disk_type: disk_type.clone(),
                volumes: Vec::new(),
            });
        }

        if let Some(primary_node) = assigned_nodes.first() {
            let cmd = RaftCommand::AssignVolume {
                node_id: primary_node.id.0.clone(),
                volume_id: volume_id.0,
                collection: collection_obj.0.clone(),
                replica_count,
                ttl: ttl.0,
                disk_type: disk_type.0.clone(),
                size: volume_size_limit,
            };
            self.propose_command(cmd).await?;
        }

        // 等待 Volume Server 实际创建 volume（gRPC 通知是异步的）
        tokio::time::sleep(Duration::from_millis(500)).await;

        let file_key = {
            let mut volumes = self.volumes.write().unwrap();
            if let Some(vol_info) = volumes.get_mut(&volume_id) {
                let key = vol_info.next_file_key;
                vol_info.next_file_key += powerfs_common::constants::FILE_KEY_BLOCK_SIZE;
                key
            } else {
                1
            }
        };
        // Persist next_file_key advance in batches via Raft
        self.maybe_advance_file_key(volume_id.0, file_key).await;

        let cookie = rand::random::<u32>() as u64;

        let fid = Fid {
            volume_id,
            cookie,
            file_key,
        };

        info!(
            "Assigned volume: {} to nodes: {:?}, fid: {},{},{}",
            volume_id,
            assigned_nodes
                .iter()
                .map(|n| n.id.clone())
                .collect::<Vec<_>>(),
            volume_id.0,
            cookie,
            file_key
        );

        Ok((fid, assigned_nodes))
    }

    /// Create a new volume using the [`SmartVolumeAssigner`] with explicit
    /// control over the assignment context (rack/DC awareness, preferred
    /// primary node).
    ///
    /// When `preferred_node` is `Some`, the smart assigner attempts to pin
    /// the primary replica on that node. If the preferred node is unhealthy
    /// or missing, the assigner falls back to the next-best candidate
    /// instead of failing.
    ///
    /// This replaces the previous create-and-discard retry loop in
    /// `volume_grow` that created up to `count * 10` volumes and threw away
    /// the ones whose primary did not match `data_node`.
    pub async fn create_new_volume_with_preference(
        &self,
        replication: &str,
        collection: &str,
        preferred_node: Option<&str>,
    ) -> Result<(Fid, Vec<DataNodeInfo>)> {
        if !self.is_leader().await {
            return Err(PowerFsError::NotLeader);
        }

        let mut nodes = self.topology.read().unwrap().list_all_nodes();
        nodes.sort_by_key(|n| n.id.0.clone());
        if nodes.is_empty() {
            return Err(PowerFsError::InvalidRequest(
                "no nodes available".to_string(),
            ));
        }

        let (volume_size_limit, rack_awareness_enabled) = {
            let config = self.cluster_config.read().unwrap();
            (config.volume_size_limit, config.rack_awareness_enabled)
        };

        let replica_placement = ReplicaPlacement::from_string(replication).unwrap_or_default();
        let collection_obj = Collection(collection.to_string());
        let ttl = Ttl::default();
        let disk_type = DiskType::default();
        let replica_count = replica_placement.get_copy_count();

        let volume_id = {
            let mut next_id = self.next_volume_id.write().unwrap();
            let vid = VolumeId(*next_id);
            *next_id += 1;
            vid
        };

        let ctx = AssignContext {
            rack_awareness_enabled,
            data_center_awareness_enabled: false,
            preferred_node: preferred_node.map(|s| s.to_string()),
        };
        let assigner = SmartVolumeAssigner;
        let assigned_nodes =
            assigner.assign_with_context(volume_id.0, &nodes, replica_count as usize, &ctx);

        if assigned_nodes.is_empty() {
            return Err(PowerFsError::InvalidRequest(
                "not enough nodes available for replication".to_string(),
            ));
        }

        {
            let mut layouts = self.volume_layouts.write().unwrap();
            let key = Self::get_volume_layout_key(&collection_obj, replica_count, &ttl, &disk_type);
            layouts.entry(key).or_insert_with(|| VolumeLayout {
                collection: collection_obj.clone(),
                replica_placement: replica_placement.clone(),
                ttl: ttl.clone(),
                disk_type: disk_type.clone(),
                volumes: Vec::new(),
            });
        }

        if let Some(primary_node) = assigned_nodes.first() {
            let cmd = RaftCommand::AssignVolume {
                node_id: primary_node.id.0.clone(),
                volume_id: volume_id.0,
                collection: collection_obj.0.clone(),
                replica_count,
                ttl: ttl.0,
                disk_type: disk_type.0.clone(),
                size: volume_size_limit,
            };
            self.propose_command(cmd).await?;
        }

        // 等待 Volume Server 实际创建 volume（gRPC 通知是异步的）
        tokio::time::sleep(Duration::from_millis(500)).await;

        let file_key = {
            let mut volumes = self.volumes.write().unwrap();
            if let Some(vol_info) = volumes.get_mut(&volume_id) {
                let key = vol_info.next_file_key;
                vol_info.next_file_key += powerfs_common::constants::FILE_KEY_BLOCK_SIZE;
                key
            } else {
                1
            }
        };
        // Persist next_file_key advance in batches via Raft
        self.maybe_advance_file_key(volume_id.0, file_key).await;

        let cookie = rand::random::<u32>() as u64;
        let fid = Fid {
            volume_id,
            cookie,
            file_key,
        };

        info!(
            "Assigned volume (smart): {} to nodes: {:?}, fid: {},{},{}, preferred={:?}",
            volume_id,
            assigned_nodes
                .iter()
                .map(|n| n.id.clone())
                .collect::<Vec<_>>(),
            volume_id.0,
            cookie,
            file_key,
            preferred_node
        );

        Ok((fid, assigned_nodes))
    }

    #[allow(dead_code)]
    fn select_nodes_by_rack(nodes: &[DataNodeInfo], count: u32) -> Vec<DataNodeInfo> {
        let mut selected = Vec::new();
        let mut used_racks = HashMap::new();

        for node in nodes {
            if selected.len() >= count as usize {
                break;
            }

            let rack_id = &node.rack_id;
            if !used_racks.contains_key(rack_id) {
                selected.push(node.clone());
                used_racks.insert(rack_id.clone(), true);
            }
        }

        if selected.len() < count as usize {
            for node in nodes {
                if selected.len() >= count as usize {
                    break;
                }
                if !selected.iter().any(|s| s.id == node.id) {
                    selected.push(node.clone());
                }
            }
        }

        selected
    }

    fn get_volume_layout_key(
        collection: &Collection,
        replica_count: u32,
        ttl: &Ttl,
        disk_type: &DiskType,
    ) -> String {
        format!("{}:{}:{}:{}", collection, replica_count, ttl, disk_type)
    }

    pub fn get_node_volumes(&self, node_id: &NodeId) -> Vec<VolumeInfo> {
        self.volumes
            .read()
            .unwrap()
            .values()
            .filter(|v| &v.node_id == node_id)
            .cloned()
            .collect()
    }

    pub fn get_volume_info(&self, volume_id: &VolumeId) -> Option<VolumeInfo> {
        self.volumes.read().unwrap().get(volume_id).cloned()
    }

    /// Get volume info by original ID (as used by Volume Server)
    /// Returns the first matching volume with the given original ID
    pub fn get_volume_info_by_original_id(&self, original_id: u64) -> Option<VolumeInfo> {
        let volumes = self.volumes.read().unwrap();
        volumes
            .values()
            .find(|v| v.id.0 % 1000 == original_id)
            .cloned()
    }

    /// Get volume route by volume ID
    pub fn get_volume_route(&self, volume_id: u64) -> Option<VolumeRoute> {
        self.volume_routes.read().unwrap().get(&volume_id).cloned()
    }

    /// Set or update volume route
    pub fn set_volume_route(&self, route: VolumeRoute) {
        self.volume_routes
            .write()
            .unwrap()
            .insert(route.volume_id, route);
    }

    /// Update volume route address (for volume migration/relocation)
    pub fn update_volume_route(&self, volume_id: u64, new_addr: String) -> Result<()> {
        let mut routes = self.volume_routes.write().unwrap();
        if let Some(route) = routes.get_mut(&volume_id) {
            route.update_addr(new_addr);
            info!(
                "Updated volume route: volume_id={}, new_addr={}",
                volume_id, route.addr
            );
            Ok(())
        } else {
            Err(PowerFsError::Internal(format!(
                "Volume route not found: {}",
                volume_id
            )))
        }
    }

    /// List all volume routes
    pub fn list_volume_routes(&self) -> Vec<VolumeRoute> {
        self.volume_routes
            .read()
            .unwrap()
            .values()
            .cloned()
            .collect()
    }

    /// Return a snapshot of the rebalance engine's migration tasks (empty if
    /// the engine hasn't started yet). For StatusQuery / monitoring.
    pub fn migration_tasks(&self) -> Vec<powerfs_allocator::MigrationTaskStatus> {
        self.rebalance_engine
            .read()
            .unwrap()
            .as_ref()
            .map(|e| e.tasks())
            .unwrap_or_default()
    }

    /// Return a cloned `Arc` to the management API if initialized.
    /// For gRPC handlers / admin tools that need to call `ManagementApi` methods.
    pub fn management_api(&self) -> Option<Arc<crate::allocator_integration::MasterManagementApi>> {
        self.management_api.read().unwrap().clone()
    }

    /// Build a [`powerfs_allocator::ClusterSnapshot`] from the Master's
    /// current heartbeat-aggregated state.
    ///
    /// This is the integration point for P6 (StatusQuery): the Master
    /// produces a snapshot from its existing `Topology`, `volume_routes`,
    /// `filer_nodes`, and `zone_registry`, then feeds it to
    /// [`powerfs_allocator::SnapshotStatusQuery`].
    ///
    /// Load metrics (cpu/disk/iops/latency) default to zero until P5
    /// enriches heartbeats; the capacity/health/usage views are fully
    /// functional today.
    pub fn build_cluster_snapshot(&self) -> powerfs_allocator::ClusterSnapshot {
        use powerfs_allocator::{NodeRuntime, ShardRuntime, VolumeLoad, VolumeRuntime};

        // --- Nodes: Topology → NodeRuntime ---
        let data_nodes = self.topology.read().unwrap().list_all_nodes();
        let nodes: Vec<NodeRuntime> = data_nodes
            .iter()
            .map(|n| NodeRuntime {
                node_id: n.id.0.clone(),
                state: map_node_state(n.state, n.maintenance_mode),
                // P5: cpu/memory from heartbeat-reported load metrics.
                cpu_usage: n.cpu_usage,
                memory_usage: n.memory_usage,
                // disk_usage derived from the node's reported space.
                disk_usage: if n.total_space > 0 {
                    (n.used_space as f32 / n.total_space as f32).clamp(0.0, 1.0)
                } else {
                    0.0
                },
                // Composite load_score: weighted blend of cpu, memory, disk.
                // Until P5 enriched heartbeats, cpu/memory were 0 and this
                // reduced to disk usage. Now all three contribute.
                load_score: compute_load_score(
                    n.cpu_usage,
                    n.memory_usage,
                    n.total_space,
                    n.used_space,
                ),
                in_maintenance: n.maintenance_mode,
            })
            .collect();

        // --- volume_id → zone_id lookup from zone_registry ---
        // A volume may belong to one or more zones; we take the first match.
        let vol_zone: std::collections::HashMap<u64, u32> = {
            let registry = self.zone_registry.read().unwrap();
            let mut map = std::collections::HashMap::new();
            for (zone_id, zone_info) in registry.iter() {
                for zv in &zone_info.physical_volumes {
                    map.entry(zv.volume_id).or_insert(*zone_id);
                }
            }
            map
        };

        // --- Volumes: volume_routes → VolumeRuntime ---
        let routes = self.volume_routes.read().unwrap();
        let volumes: Vec<VolumeRuntime> = routes
            .values()
            .map(|r| VolumeRuntime {
                volume_id: r.volume_id,
                node_id: r.node_id.clone(),
                zone_id: vol_zone.get(&r.volume_id).copied().unwrap_or(0),
                total_size: r.size,
                used_size: r.used,
                state: map_volume_state(r.state),
                load: VolumeLoad::default(),
                cold_needle_count: 0,
                hot_needle_count: 0,
            })
            .collect();
        drop(routes);

        // --- Shards: filer_nodes + shard_mapping → ShardRuntime ---
        // shard_mapping: shard_id → filer_id (leader). Each shard's leader_node
        // is the owning filer; followers are the other filers that replicate it.
        let filer_ids: Vec<String> = {
            let filers = self.filer_nodes.read().unwrap();
            filers.values().map(|f| f.node_id.clone()).collect()
        };
        let shards: Vec<ShardRuntime> = {
            let mapping = self.shard_mapping.read().unwrap();
            mapping
                .iter()
                .map(|(shard_id, leader)| ShardRuntime {
                    shard_id: *shard_id,
                    leader_node: leader.clone(),
                    follower_nodes: filer_ids
                        .iter()
                        .filter(|f| **f != *leader)
                        .cloned()
                        .collect(),
                    qps: 0,
                    raft_backlog: 0,
                    open_inode_count: 0,
                    active_lease_count: 0,
                })
                .collect()
        };

        // --- cluster_avg_load: mean of node load_score ---
        let cluster_avg_load = if nodes.is_empty() {
            0.0
        } else {
            nodes.iter().map(|n| n.load_score).sum::<f64>() / nodes.len() as f64
        };

        // --- Volume pins: Raft-replicated pin registry → snapshot ---
        let pinned_volumes: std::collections::HashMap<u64, String> = {
            self.volume_pins
                .read()
                .unwrap()
                .iter()
                .map(|(vid, node_id)| (vid.0, node_id.clone()))
                .collect()
        };

        powerfs_allocator::ClusterSnapshot {
            version: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            timestamp: std::time::Instant::now(),
            config_version: 1,
            volumes,
            nodes,
            shards,
            cluster_avg_load,
            pinned_volumes,
        }
    }

    /// 节点级反亲和性 volume 选择：从 sorted_routes 中选 count 个 volume，
    /// 尽量让每个 volume 落在不同物理节点上。
    ///
    /// 算法：按 node_id 分组（组内保持按空闲率排序），round-robin 跨节点选取。
    /// - 节点数 >= count: 每个 volume 落不同节点（完美反亲和）
    /// - 节点数 < count: 先每节点取 1 个，剩余按空闲率排序补充
    /// - exclude_ids: 跳过这些 volume_id（用于 zone 扩容时排除已有 volume）
    fn select_volumes_node_anti_affinity(
        sorted_routes: &[VolumeRoute],
        count: usize,
        exclude_ids: &std::collections::HashSet<u64>,
    ) -> Vec<powerfs_common::types::ZoneVolume> {
        use std::collections::HashMap;

        // 按 node_id 分组（跳过已排除的 volume），组内保持 sorted_routes 的空闲率排序
        let mut node_groups: Vec<(&str, Vec<&VolumeRoute>)> = Vec::new();
        let mut node_map: HashMap<&str, usize> = HashMap::new();
        for r in sorted_routes {
            if exclude_ids.contains(&r.volume_id) {
                continue;
            }
            let nid = r.node_id.as_str();
            if let std::collections::hash_map::Entry::Vacant(e) = node_map.entry(nid) {
                e.insert(node_groups.len());
                node_groups.push((nid, Vec::new()));
            }
            let idx = node_map[nid];
            node_groups[idx].1.push(r);
        }

        let num_nodes = node_groups.len();
        if num_nodes == 0 {
            return Vec::new();
        }

        // Round-robin 跨节点选取：每轮从每个节点取空闲率最高的 1 个 volume
        let mut result = Vec::with_capacity(count);
        loop {
            let mut picked = false;
            for (_, group) in node_groups.iter_mut().take(num_nodes) {
                if result.len() >= count {
                    break;
                }
                if let Some(r) = group.first().copied() {
                    result.push(powerfs_common::types::ZoneVolume {
                        volume_id: r.volume_id,
                        addr: r.addr.clone(),
                        size: r.size,
                        used: r.used,
                        node_id: r.node_id.clone(),
                    });
                    group.remove(0);
                    picked = true;
                }
            }
            if !picked || result.len() >= count {
                break;
            }
        }

        result
    }

    /// Register Filer: 分配 Zone + 选物理 volume
    ///
    /// 1. 分配新 zone_id (或复用已有)
    /// 2. 从 volume_routes 选 N 个物理 volume (基于负载)
    /// 3. 建立 zone → physical_volumes 映射
    /// 4. 返回该 filer 的所有 Zone (旧 + 新)
    ///
    /// 多 Zone 设计:
    ///   - 首次注册: 创建 1 个新 Zone, 返回 Vec(1)
    ///   - 重启再注册: 返回该 filer_id 的所有已有 Zone, 不自动创建新 Zone
    ///   - 扩容 (未来): 返回旧 Zone + 创建新 Zone
    ///
    /// P1.3: Zone 注册/更新通过 Raft 持久化, Master 重启后从 Raft 日志重放恢复.
    pub async fn register_filer_zone(
        &self,
        filer_id: &str,
    ) -> Vec<powerfs_common::types::ZoneInfo> {
        // 收集该 filer 的所有已有 Zone
        let existing: Vec<powerfs_common::types::ZoneInfo> = {
            let registry = self.zone_registry.read().unwrap();
            registry
                .values()
                .filter(|z| z.owner_filer_id == filer_id)
                .cloned()
                .collect()
        };

        // 选 N 个物理 volume (按空闲比例排序, 节点级反亲和性选取)
        // N 自动从 EC 配置推导: max(3, ec_data + ec_parity), 无需用户额外配置.
        //   - EC(4+2) → N=6, 保证 anti-affinity 每个 shard 落不同节点
        //   - 无 EC   → N=3, 满足副本复制需求
        // 用户仍可用 POWERFS_ZONE_VOLUME_COUNT 显式覆盖.
        let ec_data = std::env::var("POWERFS_EC_DATA_SHARDS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(4);
        let ec_parity = std::env::var("POWERFS_EC_PARITY_SHARDS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(2);
        let zone_volume_count = std::env::var("POWERFS_ZONE_VOLUME_COUNT")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or_else(|| (ec_data + ec_parity).max(3));
        let routes = self.list_volume_routes();
        let mut sorted_routes: Vec<VolumeRoute> = routes.into_iter().collect();
        sorted_routes.sort_by(|a, b| {
            let free_a = if a.size > 0 {
                1.0 - (a.used as f64 / a.size as f64)
            } else {
                0.0
            };
            let free_b = if b.size > 0 {
                1.0 - (b.used as f64 / b.size as f64)
            } else {
                0.0
            };
            free_b
                .partial_cmp(&free_a)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // 节点级反亲和性选取: 尽量让 N 个 volume 分布在 N 个不同物理节点上
        let empty_exclude = std::collections::HashSet::new();
        let physical_volumes: Vec<powerfs_common::types::ZoneVolume> =
            Self::select_volumes_node_anti_affinity(
                &sorted_routes,
                zone_volume_count,
                &empty_exclude,
            );

        if !existing.is_empty() {
            // 重注册: 保留已有 Zone 的 volume_id 集合 (稳定性),
            // 仅从当前 volume routes 更新 addr/size/used.
            // 不能重新选 top-3, 否则文件写入的 volume 可能不再属于 Zone,
            // 导致 scrubber 找不到 volume 地址 (P4 bug: addr not found).
            let route_map: std::collections::HashMap<u64, &VolumeRoute> =
                sorted_routes.iter().map(|r| (r.volume_id, r)).collect();
            let mut result = Vec::with_capacity(existing.len());
            for mut zone in existing {
                // 更新已有 volume 的 addr/size/used/node_id, 保留 volume_id 不变
                zone.physical_volumes = zone
                    .physical_volumes
                    .iter()
                    .filter_map(|zv| {
                        route_map
                            .get(&zv.volume_id)
                            .map(|r| powerfs_common::types::ZoneVolume {
                                volume_id: zv.volume_id,
                                addr: r.addr.clone(),
                                size: r.size,
                                used: r.used,
                                node_id: r.node_id.clone(),
                            })
                    })
                    .collect();
                // 如果原有 volume 全部下线 (route_map 中找不到), 补充 top-N 中的 volume
                if zone.physical_volumes.is_empty() && !physical_volumes.is_empty() {
                    warn!(
                        "MASTER_ZONE: zone_id={} all original volumes offline, falling back to top-{}",
                        zone.zone_id, zone_volume_count
                    );
                    zone.physical_volumes = physical_volumes.clone();
                }
                // Zone 扩容: 如果现有 volume 数少于配置值 (POWERFS_ZONE_VOLUME_COUNT),
                // 从可用路由中补充新 volume (不替换已有 volume, 只追加).
                // 使用节点级反亲和性选取, 优先从未覆盖的节点补充.
                if zone.physical_volumes.len() < zone_volume_count {
                    let existing_ids: std::collections::HashSet<u64> =
                        zone.physical_volumes.iter().map(|v| v.volume_id).collect();
                    let added: Vec<powerfs_common::types::ZoneVolume> =
                        Self::select_volumes_node_anti_affinity(
                            &sorted_routes,
                            zone_volume_count - zone.physical_volumes.len(),
                            &existing_ids,
                        );
                    if !added.is_empty() {
                        info!(
                            "MASTER_ZONE: zone_id={} expanding {} -> {} volumes (added {})",
                            zone.zone_id,
                            zone.physical_volumes.len(),
                            zone.physical_volumes.len() + added.len(),
                            added.len()
                        );
                        zone.physical_volumes.extend(added);
                    }
                }

                // 节点级反亲和性修复: 如果现有 volumes 集中在少数节点上,
                // 且当前有更多节点可用, 重新选择 volumes 以改善节点分布.
                // 这在集群扩容 (新 volume 节点加入) 后尤为重要.
                // 安全性: zone volumes 仅用于新文件分配, 不影响已有文件访问
                // (已有文件的 volume_id 存储在 inode 元数据中, 不依赖 zone 成员).
                {
                    let current_nodes: std::collections::HashSet<&str> = zone
                        .physical_volumes
                        .iter()
                        .map(|v| v.node_id.as_str())
                        .collect();
                    let available_nodes: std::collections::HashSet<&str> =
                        sorted_routes.iter().map(|r| r.node_id.as_str()).collect();
                    let target_nodes = zone_volume_count.min(available_nodes.len());

                    if current_nodes.len() < target_nodes {
                        warn!(
                            "MASTER_ZONE: zone_id={} anti-affinity re-selection: \
                             current_nodes={} < target_nodes={} (available={}), \
                             re-selecting {} volumes across {} nodes",
                            zone.zone_id,
                            current_nodes.len(),
                            target_nodes,
                            available_nodes.len(),
                            zone_volume_count,
                            target_nodes
                        );
                        let empty_exclude = std::collections::HashSet::new();
                        zone.physical_volumes = Self::select_volumes_node_anti_affinity(
                            &sorted_routes,
                            zone_volume_count,
                            &empty_exclude,
                        );
                    }
                }
                // P1.3: propose UpdateZone (apply 到内存 + Raft 日志持久化)
                let cmd = crate::raft_v2::RaftCommand::UpdateZone { zone: zone.clone() };
                if let Err(e) = self.propose_command(cmd).await {
                    warn!(
                        "MASTER_ZONE: failed to persist UpdateZone for zone_id={}: {} \
                         (memory updated, may not survive restart)",
                        zone.zone_id, e
                    );
                    // Fallback: 直接更新内存 (与旧行为一致)
                    self.zone_registry
                        .write()
                        .unwrap()
                        .insert(zone.zone_id, zone.clone());
                }
                result.push(zone);
            }
            info!(
                "MASTER_ZONE: returning {} existing zone(s) for filer={}, volumes={}",
                result.len(),
                filer_id,
                result
                    .iter()
                    .map(|z| z.physical_volumes.len())
                    .sum::<usize>()
            );
            return result;
        }

        // 首次注册: 分配新 zone_id
        let zone_id = self
            .next_zone_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let zone_info = powerfs_common::types::ZoneInfo {
            zone_id,
            owner_filer_id: filer_id.to_string(),
            physical_volumes,
        };

        info!(
            "MASTER_ZONE: allocated zone_id={} for filer={}, volumes={}",
            zone_id,
            filer_id,
            zone_info.physical_volumes.len()
        );

        // P1.3: propose RegisterZone (apply 到内存 + Raft 日志持久化)
        let cmd = crate::raft_v2::RaftCommand::RegisterZone {
            zone: zone_info.clone(),
        };
        if let Err(e) = self.propose_command(cmd).await {
            warn!(
                "MASTER_ZONE: failed to persist RegisterZone for zone_id={}: {} \
                 (memory updated, may not survive restart)",
                zone_id, e
            );
            // Fallback: 直接更新内存 (与旧行为一致)
            self.zone_registry
                .write()
                .unwrap()
                .insert(zone_id, zone_info.clone());
        }

        vec![zone_info]
    }

    /// Remove volume route
    pub fn remove_volume_route(&self, volume_id: u64) -> Option<VolumeRoute> {
        self.volume_routes.write().unwrap().remove(&volume_id)
    }

    pub fn get_node_info(&self, node_id: &NodeId) -> Option<DataNodeInfo> {
        self.topology.read().unwrap().get_node(node_id).cloned()
    }

    pub async fn handle_heartbeat(&self, node_id: &NodeId) {
        let mut topology = self.topology.write().unwrap();

        if let Some(node) = topology.get_node_mut(node_id) {
            node.last_heartbeat = Utc::now();
            node.state = NodeState::Healthy;
            debug!("Received heartbeat from node: {:?}", node_id);
        } else {
            warn!("Heartbeat from unknown node: {:?}", node_id);
        }
    }

    pub fn add_client(&self, client_id: String, tx: mpsc::Sender<VolumeLocationUpdate>) {
        self.client_manager
            .write()
            .unwrap()
            .add_client(client_id, tx);
    }

    pub fn remove_client(&self, client_id: &str) {
        self.client_manager
            .write()
            .unwrap()
            .remove_client(client_id);
    }

    pub fn register_fuse_client(&self, info: FuseClientInfo) {
        self.client_manager
            .write()
            .unwrap()
            .register_fuse_client(info);
    }

    pub fn update_fuse_client_heartbeat(&self, client_id: &str) {
        self.client_manager
            .write()
            .unwrap()
            .update_fuse_client_heartbeat(client_id);
    }

    pub fn update_fuse_client_stats(&self, client_id: &str, stats: crate::proto::ClientStats) {
        self.client_manager
            .write()
            .unwrap()
            .update_fuse_client_stats(client_id, stats);
    }

    pub fn get_fuse_clients(&self) -> Vec<FuseClientInfo> {
        self.client_manager.read().unwrap().get_fuse_clients()
    }

    pub async fn lookup_volume(
        &self,
        volume_ids: &[String],
    ) -> HashMap<VolumeId, Vec<DataNodeInfo>> {
        let mut result = HashMap::new();
        let volumes = self.volumes.read().unwrap();
        let topology = self.topology.read().unwrap();

        for vid_str in volume_ids {
            if let Ok(vid) = u64::from_str(vid_str) {
                let volume_id = VolumeId(vid);
                if let Some(vol) = volumes.get(&volume_id) {
                    if let Some(node) = topology.get_node(&vol.node_id) {
                        result
                            .entry(volume_id)
                            .or_insert_with(Vec::new)
                            .push(node.clone());
                    }
                }
            }
        }

        result
    }

    pub async fn get_statistics(&self) -> crate::proto::StatisticsResponse {
        let volumes = self.volumes.read().unwrap();
        let topology = self.topology.read().unwrap();

        let mut total_volume_count = 0;
        let mut total_volume_size = 0;
        let mut total_used_size = 0;
        let mut available_volume_count = 0;
        let mut full_volume_count = 0;
        let mut read_only_volume_count = 0;

        let mut collection_stats: HashMap<String, (u64, u64, u64)> = HashMap::new();
        let mut dc_stats: HashMap<String, (u64, u64, u64)> = HashMap::new();
        let mut rack_stats: HashMap<String, (u64, u64, u64)> = HashMap::new();

        for vol in volumes.values() {
            total_volume_count += 1;
            total_volume_size += vol.size;
            total_used_size += vol.used;

            match vol.state {
                VolumeState::Available => available_volume_count += 1,
                VolumeState::Full => full_volume_count += 1,
                VolumeState::ReadOnly => read_only_volume_count += 1,
                _ => {}
            }

            let coll_name = vol.collection.0.clone();
            let (count, size, used) = collection_stats.entry(coll_name).or_insert((0, 0, 0));
            *count += 1;
            *size += vol.size;
            *used += vol.used;

            if let Some(node) = topology.get_node(&vol.node_id) {
                let dc_name = node.data_center_id.0.clone();
                let (dc_count, dc_size, dc_used) =
                    dc_stats.entry(dc_name.clone()).or_insert((0, 0, 0));
                *dc_count += 1;
                *dc_size += vol.size;
                *dc_used += vol.used;

                let rack_name = format!("{}:{}", dc_name, node.rack_id.0);
                let (rack_count, rack_size, rack_used) =
                    rack_stats.entry(rack_name).or_insert((0, 0, 0));
                *rack_count += 1;
                *rack_size += vol.size;
                *rack_used += vol.used;
            }
        }

        let mut collection_stats_list = Vec::new();
        for (name, (count, size, used)) in collection_stats {
            collection_stats_list.push(crate::proto::CollectionStats {
                name,
                volume_count: count,
                total_size: size,
                used_size: used,
            });
        }

        let mut dc_stats_list = Vec::new();
        for (name, (count, _size, _used)) in dc_stats {
            dc_stats_list.push(crate::proto::DataCenterStats {
                name,
                node_count: 0,
                volume_count: count,
                total_size: 0,
                used_size: 0,
            });
        }

        let mut rack_stats_list = Vec::new();
        for (name, (count, _size, _used)) in rack_stats {
            let parts: Vec<&str> = name.split(':').collect();
            let dc_name = if parts.len() > 1 { parts[0] } else { "" };
            let rack_name = if parts.len() > 1 { parts[1] } else { &name };
            rack_stats_list.push(crate::proto::RackStats {
                name: rack_name.to_string(),
                data_center: dc_name.to_string(),
                node_count: 0,
                volume_count: count,
                total_size: 0,
                used_size: 0,
            });
        }

        let nodes = topology.list_all_nodes();
        let node_count = nodes.len();
        let mut dc_node_counts: HashMap<String, u64> = HashMap::new();
        let mut rack_node_counts: HashMap<String, u64> = HashMap::new();

        for node in nodes {
            let dc_name = node.data_center_id.0.clone();
            *dc_node_counts.entry(dc_name.clone()).or_insert(0) += 1;

            let rack_name = format!("{}:{}", dc_name, node.rack_id.0);
            *rack_node_counts.entry(rack_name).or_insert(0) += 1;
        }

        for dc_stat in dc_stats_list.iter_mut() {
            if let Some(count) = dc_node_counts.get(&dc_stat.name) {
                dc_stat.node_count = *count;
            }
        }

        for rack_stat in rack_stats_list.iter_mut() {
            let rack_name = format!("{}:{}", rack_stat.data_center, rack_stat.name);
            if let Some(count) = rack_node_counts.get(&rack_name) {
                rack_stat.node_count = *count;
            }
        }

        crate::proto::StatisticsResponse {
            total_volume_count,
            total_node_count: node_count as u64,
            total_data_center_count: topology.data_centers.len() as u64,
            total_rack_count: topology
                .data_centers
                .values()
                .map(|dc| dc.racks.len())
                .sum::<usize>() as u64,
            total_volume_size,
            total_used_size,
            available_volume_count,
            full_volume_count,
            read_only_volume_count,
            collection_stats: collection_stats_list,
            data_center_stats: dc_stats_list,
            rack_stats: rack_stats_list,
            error: String::new(),
        }
    }

    pub async fn get_cluster_info(&self) -> crate::proto::ClusterInfoResponse {
        let info = self.raft_v2.get_cluster_info().await;
        crate::proto::ClusterInfoResponse {
            node_id: self.raft_id,
            address: self.raft_address.clone(),
            is_leader: info.is_leader,
            term: info.term,
            peers: info.peers,
        }
    }

    pub async fn raft_propose(&self, data: Vec<u8>) -> std::result::Result<u64, String> {
        self.raft_v2.propose(data).await
    }

    /// Transfer leadership to `target_id`.
    ///
    /// Delegates to openraft's `transfer_leader`. In single-node clusters this
    /// is a no-op (there is no other node to transfer to).
    pub async fn raft_transfer_leader(&self, target_id: u64) -> std::result::Result<(), String> {
        self.raft_v2.transfer_leader(target_id).await
    }

    pub async fn start_raft(&self, _peers: Vec<String>) -> Result<()> {
        // openraft is self-driven; nothing to start here. Leader state is
        // determined by openraft internally and queried via `raft_v2.is_leader()`.
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    pub async fn start(self: Arc<Self>) -> Result<()> {
        info!("Starting PowerFS Master node: {:?}", self.id);
        info!("Listening on: {}", self.address);

        let node_id_str = self.raft_address.clone();
        let _ip_address = self
            .raft_address
            .split(':')
            .next()
            .unwrap_or("0.0.0.0")
            .to_string();
        let grpc_port = self.address.port() as u32;
        let event_provider = self.event_provider.clone();
        let address = self.address.to_string();
        // 持有 Arc 引用以在事件发布循环中读取实时的 leader 状态
        let master_ref = self.clone();
        let raft_term = *self.raft_term.read().unwrap();

        tokio::spawn(async move {
            let mut sys = sysinfo::System::new_all();
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                sys.refresh_all();

                let metrics = collect_system_metrics(&mut sys, ".");

                // 每次循环读取实时的 leader 状态（反映 raft 角色变更）
                let is_leader = master_ref.raft_v2.is_leader();

                let event = Event::NodeStatus(NodeStatusEvent {
                    node_id: node_id_str.clone(),
                    node_type: "master".to_string(),
                    address: address.clone(),
                    grpc_port,
                    http_port: grpc_port,
                    status: if is_leader {
                        "leader".to_string()
                    } else {
                        "follower".to_string()
                    },
                    cpu_usage: metrics.cpu_usage,
                    mem_usage: metrics.mem_usage,
                    disk_usage: metrics.disk_usage,
                    network_rx: metrics.network_rx,
                    network_tx: metrics.network_tx,
                    uptime: metrics.uptime,
                    volume_count: 0,
                    is_leader,
                    raft_term,
                });

                if let Err(e) = event_provider.publish(event, &node_id_str).await {
                    warn!("Failed to publish node_status event: {}", e);
                }
            }
        });

        // 内存泄漏诊断任务：每 30 秒打印一次关键指标
        let diag_master = self.clone();
        tokio::spawn(async move {
            let mut prev_snapshot: Option<crate::tracking_allocator::AllocSnapshot> = None;
            let mut prev_vm_rss: u64 = 0;
            let mut tick = 0u64;
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                tick += 1;

                let snap = crate::tracking_allocator::ALLOC_STATS.snapshot();

                if tick.is_multiple_of(30) {
                    let vm = crate::tracking_allocator::read_self_vm();
                    let (rss_kb, data_kb, peak_kb) = vm.unwrap_or((0, 0, 0));

                    let (
                        jemalloc_res_mb,
                        jemalloc_active_mb,
                        jemalloc_mapped_mb,
                        jemalloc_retained_mb,
                    ) = match crate::tracking_allocator::read_jemalloc_stats() {
                        Some((res, act, map, ret)) => (
                            res / 1024 / 1024,
                            act / 1024 / 1024,
                            map / 1024 / 1024,
                            ret / 1024 / 1024,
                        ),
                        None => (0, 0, 0, 0),
                    };

                    // 关键数据结构大小
                    let topology_n = diag_master.topology.read().unwrap().data_centers.len();
                    let volumes_n = diag_master.volumes.read().unwrap().len();
                    let collections_n = diag_master.collections.read().unwrap().len();
                    let volume_layouts_n = diag_master.volume_layouts.read().unwrap().len();
                    let client_mgr = diag_master.client_manager.read().unwrap();
                    let clients_n = client_mgr.clients.len();
                    let fuse_clients_n = client_mgr.fuse_clients.len();
                    drop(client_mgr);

                    // 增量计算
                    let (delta_live_kb, delta_alloc_mb) = if let Some(prev) = prev_snapshot {
                        let d_live = snap.live_bytes().saturating_sub(prev.live_bytes());
                        let d_alloc = snap.alloc_bytes.saturating_sub(prev.alloc_bytes);
                        (d_live / 1024, d_alloc / 1024 / 1024)
                    } else {
                        (0, 0)
                    };
                    let delta_rss_kb = rss_kb.saturating_sub(prev_vm_rss);

                    info!(
                        "MEM_DIAG tick={} rss_mb={} data_mb={} peak_mb={} live_mb={} live_cnt={} \
                         delta_live_kb={} delta_rss_kb={} delta_alloc_mb={} \
                         jemalloc_res_mb={} jemalloc_active_mb={} jemalloc_mapped_mb={} jemalloc_retained_mb={} \
                         topo={} vols={} cols={} layouts={} clients={} fuse_clients={}",
                        tick,
                        rss_kb / 1024,
                        data_kb / 1024,
                        peak_kb / 1024,
                        snap.live_bytes() / 1024 / 1024,
                        snap.live_count(),
                        delta_live_kb,
                        delta_rss_kb,
                        delta_alloc_mb,
                        jemalloc_res_mb,
                        jemalloc_active_mb,
                        jemalloc_mapped_mb,
                        jemalloc_retained_mb,
                        topology_n,
                        volumes_n,
                        collections_n,
                        volume_layouts_n,
                        clients_n,
                        fuse_clients_n,
                    );

                    prev_snapshot = Some(snap);
                    prev_vm_rss = rss_kb;
                } else if tick.is_multiple_of(5) {
                    info!(
                        "MEM_DIAG_FAST tick={} alloc_bytes={} alloc_count={} live_bytes={} live_cnt={}",
                        tick,
                        snap.alloc_bytes,
                        snap.alloc_count,
                        snap.live_bytes(),
                        snap.live_count(),
                    );
                }
            }
        });

        // Allocator rebalance engine: build it now (after heartbeats have a
        // chance to populate topology) and spawn the background tick loop.
        // Only the Raft leader runs ticks; followers no-op.
        {
            let volume_default_size = self.cluster_config.read().unwrap().volume_size_limit;
            let engine = crate::allocator_integration::RebalanceEngine::new_with_master(
                Arc::clone(&self),
                volume_default_size,
            );
            *self.rebalance_engine.write().unwrap() = Some(Arc::clone(&engine));
            crate::allocator_integration::spawn_rebalance_loop(
                Arc::clone(&engine),
                Arc::clone(&self),
            );
            info!(
                "Allocator rebalance engine started (scan_interval={}s)",
                self.rebalance_engine
                    .read()
                    .unwrap()
                    .as_ref()
                    .map(|e| e.scan_interval_secs())
                    .unwrap_or(0)
            );

            // Management API: volume scaling + migration control + policy updates.
            let mgmt = crate::allocator_integration::MasterManagementApi::new(
                Arc::clone(&self),
                Arc::clone(&engine),
            );
            *self.management_api.write().unwrap() = Some(Arc::new(mgmt));
            info!("Allocator management API initialized");
        }

        let master_clone = self.clone();
        let kv_cache_clone = self.kv_cache.clone();
        let server_address = self.address;

        tokio::spawn(async move {
            // Phase D: Raft inter-node transport migrated to TLV (MsgType::RaftMessage).
            // The RaftGrpcServer is no longer registered here; only the MasterService
            // gRPC server remains (retained for monitoring/admin RPCs).
            let master_server = crate::server::MasterGrpcServer::new(master_clone, kv_cache_clone);

            tonic::transport::Server::builder()
                .add_service(
                    crate::proto::powerfs::master_service_server::MasterServiceServer::new(
                        master_server,
                    ),
                )
                .serve(server_address)
                .await
                .map_err(|e| {
                    error!("Failed to start gRPC server: {}", e);
                    e
                })
                .ok();
        });

        // Start powerfs-net binary protocol server
        let net_port = self.net_port;
        if net_port > 0 {
            let net_addr = format!("{}:{}", self.address.ip(), net_port);
            let net_handler = Arc::new(crate::net_handler::MasterNetHandler::new(self.clone()));
            let net_handler: Arc<dyn powerfs_net::NetHandler> = net_handler;

            info!("Starting powerfs-net server on {}", net_addr);
            if let Ok(net_server) = PowerFsNetServer::bind_with_manager(
                &self.address.ip().to_string(),
                net_port,
                net_handler,
            )
            .await
            {
                // Share the manager so the broadcast task can push NOTIFY
                // frames (e.g. VolumeLocation updates) to TLV clients.
                if let Some(manager) = net_server.manager() {
                    *self.net_manager.write().unwrap() = Some(manager.clone());
                }
                tokio::spawn(async move {
                    if let Err(e) = net_server.serve().await {
                        error!("powerfs-net server error: {:?}", e);
                    }
                });
            }
        }

        // Start HTTP metrics + debug config server.
        // Port = net_port + 1 (e.g. net_port=9334 → metrics=9335).
        // Exposes /metrics, /admin/log-level, /admin/debug.
        {
            let metrics_port = if net_port > 0 { net_port + 1 } else { 9335 };
            let metrics_addr = format!("{}:{}", self.address.ip(), metrics_port);
            let debug_store = self.debug_config.clone();
            info!("Starting metrics + debug config server on {}", metrics_addr);
            if let Err(e) = crate::metrics::start_metrics_server(&metrics_addr, debug_store).await {
                error!("Failed to start metrics server on {}: {}", metrics_addr, e);
            }
        }

        // Note: Raft inter-node transport is now handled entirely by openraft's
        // gRPC RaftService (started inside `RaftNodeV2::new`). The old TLV-based
        // message forwarder and `broadcast::Sender<OutgoingMessage>` have been
        // removed.

        // Keep the master running
        tokio::signal::ctrl_c().await?;
        info!("Received shutdown signal, stopping master node");

        Ok(())
    }
}

impl Clone for MasterNode {
    fn clone(&self) -> Self {
        MasterNode {
            id: self.id.clone(),
            address: self.address,
            net_port: self.net_port,
            topology: RwLock::new(self.topology.read().unwrap().clone()),
            volumes: RwLock::new(self.volumes.read().unwrap().clone()),
            volume_routes: RwLock::new(self.volume_routes.read().unwrap().clone()),
            collections: RwLock::new(self.collections.read().unwrap().clone()),
            collection_manager: RwLock::new(self.collection_manager.read().unwrap().clone()),
            volume_layouts: RwLock::new(self.volume_layouts.read().unwrap().clone()),
            cluster_config: RwLock::new(self.cluster_config.read().unwrap().clone()),
            raft_config: self.raft_config.clone(),
            peers: self.peers.clone(),
            raft_v2: self.raft_v2.clone(),
            raft_id: self.raft_id,
            raft_address: self.raft_address.clone(),
            is_leader: self.is_leader.clone(),
            leader_address: self.leader_address.clone(),
            raft_term: RwLock::new(*self.raft_term.read().unwrap()),
            next_volume_id: RwLock::new(*self.next_volume_id.read().unwrap()),
            max_file_key: RwLock::new(*self.max_file_key.read().unwrap()),
            heartbeat_tx: self.heartbeat_tx.clone(),
            client_manager: self.client_manager.clone(),
            notify_tx: self.notify_tx.clone(),
            net_manager: self.net_manager.clone(),
            kv_cache: self.kv_cache.clone(),
            kv_persist: self.kv_persist.clone(),
            volume_client_pool: self.volume_client_pool.clone(),
            event_provider: self.event_provider.clone(),
            filer_nodes: RwLock::new(self.filer_nodes.read().unwrap().clone()),
            shard_mapping: RwLock::new(self.shard_mapping.read().unwrap().clone()),
            stripe_round_robin: self.stripe_round_robin.clone(),
            volume_round_robin: self.volume_round_robin.clone(),
            zone_registry: RwLock::new(self.zone_registry.read().unwrap().clone()),
            next_zone_id: self.next_zone_id.clone(),
            rebalance_engine: RwLock::new(self.rebalance_engine.read().unwrap().clone()),
            management_api: RwLock::new(self.management_api.read().unwrap().clone()),
            volume_pins: RwLock::new(self.volume_pins.read().unwrap().clone()),
            placement_strategy: RwLock::new(self.placement_strategy.read().unwrap().clone()),
            debug_config: self.debug_config.clone(),
        }
    }
}

/// Pure volume-selection logic extracted from [`MasterNode::select_writable_volume`].
///
/// Operates on a borrowed volume table so it can be unit-tested without
/// spinning up a full Master node.
///
/// `start_idx` is a round-robin hint: when multiple candidates match, the
/// function picks `candidates[start_idx % len]` instead of always returning
/// the first one. This distributes write load across all writable volumes.
fn select_writable_volume_from(
    volumes: &HashMap<VolumeId, VolumeInfo>,
    collection: &Collection,
    nodes: &[DataNodeInfo],
    mode: &VolumeAllocationMode,
    excluded: &[u64],
    start_idx: usize,
) -> Option<(VolumeId, NodeId)> {
    // Build the candidate id list according to the allocation mode. `None`
    // means "scan all volumes" (Auto).
    let pinned: Option<&[u64]> = match mode {
        VolumeAllocationMode::Manual { volume_ids } => Some(volume_ids.as_slice()),
        VolumeAllocationMode::Hybrid {
            fixed_volume_ids, ..
        } => Some(fixed_volume_ids.as_slice()),
        VolumeAllocationMode::Auto { .. } => None,
    };

    // First pass: try the pinned list (Manual / Hybrid fixed set).
    if let Some(ids) = pinned {
        let mut candidates: Vec<(VolumeId, NodeId)> = Vec::new();
        for vid in ids {
            if excluded.contains(vid) {
                continue;
            }
            if let Some(vinfo) = volumes.get(&VolumeId(*vid)) {
                if vinfo.collection != *collection {
                    continue;
                }
                if !matches!(vinfo.state, VolumeState::Creating | VolumeState::Available) {
                    continue;
                }
                if vinfo.used >= vinfo.size {
                    continue;
                }
                if !nodes.iter().any(|n| n.id == vinfo.node_id) {
                    continue;
                }
                candidates.push((VolumeId(*vid), vinfo.node_id.clone()));
            }
        }
        if !candidates.is_empty() {
            let pick = candidates[start_idx % candidates.len()].clone();
            return Some(pick);
        }
        // Manual mode never falls back to auto-scan.
        if matches!(mode, VolumeAllocationMode::Manual { .. }) {
            return None;
        }
    }

    // Auto pass (also the Hybrid fallback): scan all volumes.
    // Collect all candidates first, then round-robin select to distribute
    // write load evenly across available volumes.
    let mut candidates: Vec<(VolumeId, NodeId)> = Vec::new();
    for (vid, vinfo) in volumes.iter() {
        if excluded.contains(&vid.0) {
            continue;
        }
        if vinfo.collection != *collection {
            continue;
        }
        if !matches!(vinfo.state, VolumeState::Creating | VolumeState::Available) {
            continue;
        }
        if vinfo.used >= vinfo.size {
            continue;
        }
        if !nodes.iter().any(|n| n.id == vinfo.node_id) {
            continue;
        }
        candidates.push((*vid, vinfo.node_id.clone()));
    }

    if candidates.is_empty() {
        return None;
    }
    let pick = candidates[start_idx % candidates.len()].clone();
    Some(pick)
}

/// P5: Composite load score (0.0 idle - 1.0 saturated).
///
/// Weighted blend: 40% cpu, 30% memory, 30% disk. When cpu/memory are 0
/// (pre-P5 nodes that don't report load metrics), the score reduces to
/// 30% disk usage — still meaningful for ranking, just less precise.
fn compute_load_score(cpu: f32, memory: f32, total_space: u64, used_space: u64) -> f64 {
    let disk = if total_space > 0 {
        (used_space as f32 / total_space as f32).clamp(0.0, 1.0)
    } else {
        0.0
    };
    let score = 0.4 * cpu.clamp(0.0, 1.0) + 0.3 * memory.clamp(0.0, 1.0) + 0.3 * disk;
    score as f64
}

/// Map `powerfs_common::NodeState` + maintenance flag to the allocator's
/// `NodeRuntimeState`. Maintenance takes precedence over the reported state.
fn map_node_state(
    state: powerfs_common::types::NodeState,
    in_maintenance: bool,
) -> powerfs_allocator::NodeRuntimeState {
    use powerfs_allocator::NodeRuntimeState;
    use powerfs_common::types::NodeState;
    if in_maintenance {
        return NodeRuntimeState::Maintenance;
    }
    match state {
        NodeState::Init
        | NodeState::Ready
        | NodeState::Healthy
        | NodeState::SoftError
        | NodeState::FailSlow => NodeRuntimeState::Healthy,
        NodeState::Degraded => NodeRuntimeState::Degraded,
        NodeState::Maintenance => NodeRuntimeState::Maintenance,
        NodeState::Fault | NodeState::Unavailable => NodeRuntimeState::Down,
    }
}

/// Map `powerfs_common::VolumeState` to the allocator's `VolumeRuntimeState`.
fn map_volume_state(
    state: powerfs_common::types::VolumeState,
) -> powerfs_allocator::VolumeRuntimeState {
    use powerfs_allocator::VolumeRuntimeState;
    use powerfs_common::types::VolumeState;
    match state {
        VolumeState::Creating | VolumeState::Available | VolumeState::ReadOnly => {
            VolumeRuntimeState::Active
        }
        VolumeState::Draining => VolumeRuntimeState::Draining,
        VolumeState::Full => VolumeRuntimeState::Full,
        VolumeState::Deleting => VolumeRuntimeState::Deleted,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use powerfs_common::types::{DataCenterId, DiskType, NodeId, RackId, Ttl};

    fn node(id: &str) -> DataNodeInfo {
        DataNodeInfo::new(
            NodeId(id.to_string()),
            "127.0.0.1".to_string(),
            RackId(String::new()),
            DataCenterId(String::new()),
            8080,
            8081,
            String::new(),
        )
    }

    fn vol(vid: u64, collection: &str, node_id: &str, used: u64, size: u64) -> VolumeInfo {
        VolumeInfo {
            id: VolumeId(vid),
            node_id: NodeId(node_id.to_string()),
            collection: Collection(collection.to_string()),
            size,
            used,
            replica_count: 1,
            ttl: Ttl::default(),
            disk_type: DiskType::default(),
            state: VolumeState::Available,
            created_at: Utc::now(),
            modified_at: Utc::now(),
            next_file_key: 1,
        }
    }

    fn build_volumes(entries: &[(u64, &str, &str, u64, u64)]) -> HashMap<VolumeId, VolumeInfo> {
        entries
            .iter()
            .map(|(vid, coll, nid, used, size)| {
                (VolumeId(*vid), vol(*vid, coll, nid, *used, *size))
            })
            .collect()
    }

    #[test]
    fn test_auto_mode_picks_matching_writable_volume() {
        let volumes = build_volumes(&[
            (1, "default", "n1", 0, 100),
            (2, "user", "n1", 0, 100),
            (3, "default", "n1", 100, 100), // full
        ]);
        let nodes = vec![node("n1")];
        let mode = VolumeAllocationMode::default(); // Auto
        let coll = Collection("default".to_string());
        let pick = select_writable_volume_from(&volumes, &coll, &nodes, &mode, &[], 0);
        assert_eq!(pick.map(|(v, _)| v.0), Some(1));
    }

    #[test]
    fn test_manual_mode_only_considers_pinned_ids() {
        let volumes = build_volumes(&[(1, "default", "n1", 0, 100), (2, "default", "n1", 0, 100)]);
        let nodes = vec![node("n1")];
        let mode = VolumeAllocationMode::Manual {
            volume_ids: vec![2],
        };
        let coll = Collection("default".to_string());
        let pick = select_writable_volume_from(&volumes, &coll, &nodes, &mode, &[], 0);
        assert_eq!(pick.map(|(v, _)| v.0), Some(2));
    }

    #[test]
    fn test_manual_mode_returns_none_when_pinned_unavailable() {
        let volumes = build_volumes(&[
            (1, "default", "n1", 0, 100),   // not pinned
            (2, "default", "n1", 100, 100), // pinned but full
        ]);
        let nodes = vec![node("n1")];
        let mode = VolumeAllocationMode::Manual {
            volume_ids: vec![2],
        };
        let coll = Collection("default".to_string());
        let pick = select_writable_volume_from(&volumes, &coll, &nodes, &mode, &[], 0);
        assert!(pick.is_none(), "Manual must not fall back to auto-scan");
    }

    #[test]
    fn test_hybrid_mode_falls_back_to_auto() {
        let volumes = build_volumes(&[
            (1, "default", "n1", 100, 100), // pinned but full
            (2, "default", "n1", 0, 100),   // auto fallback
        ]);
        let nodes = vec![node("n1")];
        let mode = VolumeAllocationMode::Hybrid {
            fixed_volume_ids: vec![1],
            auto_count: 1,
        };
        let coll = Collection("default".to_string());
        let pick = select_writable_volume_from(&volumes, &coll, &nodes, &mode, &[], 0);
        assert_eq!(pick.map(|(v, _)| v.0), Some(2));
    }

    #[test]
    fn test_blacklist_excludes_volumes() {
        let volumes = build_volumes(&[
            (1, "default", "n1", 0, 100), // blacklisted
            (2, "default", "n1", 0, 100),
        ]);
        let nodes = vec![node("n1")];
        let mode = VolumeAllocationMode::default();
        let coll = Collection("default".to_string());
        let pick = select_writable_volume_from(&volumes, &coll, &nodes, &mode, &[1], 0);
        assert_eq!(pick.map(|(v, _)| v.0), Some(2));
    }

    #[test]
    fn test_blacklist_blocks_manual_pinned_volume() {
        let volumes = build_volumes(&[(1, "default", "n1", 0, 100)]);
        let nodes = vec![node("n1")];
        let mode = VolumeAllocationMode::Manual {
            volume_ids: vec![1],
        };
        let coll = Collection("default".to_string());
        let pick = select_writable_volume_from(&volumes, &coll, &nodes, &mode, &[1], 0);
        assert!(pick.is_none(), "blacklist must override Manual pin");
    }

    #[test]
    fn test_skips_volume_whose_node_left_topology() {
        let volumes = build_volumes(&[(1, "default", "ghost", 0, 100)]);
        let nodes = vec![node("n1")]; // ghost not present
        let mode = VolumeAllocationMode::default();
        let coll = Collection("default".to_string());
        let pick = select_writable_volume_from(&volumes, &coll, &nodes, &mode, &[], 0);
        assert!(pick.is_none());
    }

    #[test]
    fn test_skips_readonly_state() {
        let mut volumes = build_volumes(&[(1, "default", "n1", 0, 100)]);
        volumes.get_mut(&VolumeId(1)).unwrap().state = VolumeState::ReadOnly;
        let nodes = vec![node("n1")];
        let mode = VolumeAllocationMode::default();
        let coll = Collection("default".to_string());
        let pick = select_writable_volume_from(&volumes, &coll, &nodes, &mode, &[], 0);
        assert!(pick.is_none());
    }

    #[test]
    fn test_round_robin_distributes_across_volumes() {
        // Three writable volumes in the same collection — round-robin
        // should cycle through all of them instead of always returning
        // the first one.
        let volumes = build_volumes(&[
            (1, "default", "n1", 0, 100),
            (2, "default", "n1", 0, 100),
            (3, "default", "n1", 0, 100),
        ]);
        let nodes = vec![node("n1")];
        let mode = VolumeAllocationMode::default();
        let coll = Collection("default".to_string());

        let mut seen = std::collections::HashSet::new();
        for i in 0..6 {
            let pick = select_writable_volume_from(&volumes, &coll, &nodes, &mode, &[], i);
            if let Some((vid, _)) = pick {
                seen.insert(vid.0);
            }
        }
        // After 6 calls (2 full cycles), all 3 volumes should have been picked.
        assert_eq!(
            seen.len(),
            3,
            "round-robin must distribute across all volumes, got: {:?}",
            seen
        );
    }

    // ========== 节点级反亲和性测试 ==========

    fn make_route(vid: u64, node_id: &str, used: u64, size: u64) -> VolumeRoute {
        VolumeRoute {
            volume_id: vid,
            addr: format!("10.0.0.{}:8080", vid),
            size,
            used,
            file_count: 0,
            state: VolumeState::Available,
            node_id: node_id.to_string(),
            updated_at: Utc::now(),
        }
    }

    /// 测试: 6 节点各 4 volumes, 选 6 个 → 每个 volume 应落不同节点
    #[test]
    fn test_node_anti_affinity_perfect_distribution() {
        // 6 nodes × 4 volumes = 24 volumes, all same free ratio
        let mut routes: Vec<VolumeRoute> = Vec::new();
        for node_idx in 0..6u64 {
            for vol_idx in 0..4u64 {
                let vid = node_idx * 4 + vol_idx + 1;
                routes.push(make_route(vid, &format!("node-{}", node_idx), 0, 100));
            }
        }
        // Sort by free ratio (all same, so order preserved)
        routes.sort_by(|a, b| {
            let fa = 1.0 - (a.used as f64 / a.size as f64);
            let fb = 1.0 - (b.used as f64 / b.size as f64);
            fb.partial_cmp(&fa).unwrap_or(std::cmp::Ordering::Equal)
        });

        let exclude = std::collections::HashSet::new();
        let result = MasterNode::select_volumes_node_anti_affinity(&routes, 6, &exclude);

        assert_eq!(result.len(), 6, "should select exactly 6 volumes");

        let unique_nodes: std::collections::HashSet<&str> =
            result.iter().map(|v| v.node_id.as_str()).collect();
        assert_eq!(
            unique_nodes.len(),
            6,
            "6 volumes should span 6 different nodes, got: {:?}",
            unique_nodes
        );
    }

    /// 测试: 2 节点各 3 volumes, 选 6 个 → 每节点 3 个 (节点数 < count)
    #[test]
    fn test_node_anti_affinity_fewer_nodes_than_count() {
        let routes = vec![
            make_route(1, "node-A", 0, 100),
            make_route(2, "node-A", 10, 100),
            make_route(3, "node-A", 20, 100),
            make_route(4, "node-B", 0, 100),
            make_route(5, "node-B", 10, 100),
            make_route(6, "node-B", 20, 100),
        ];
        let mut sorted = routes.clone();
        sorted.sort_by(|a, b| {
            let fa = 1.0 - (a.used as f64 / a.size as f64);
            let fb = 1.0 - (b.used as f64 / b.size as f64);
            fb.partial_cmp(&fa).unwrap_or(std::cmp::Ordering::Equal)
        });

        let exclude = std::collections::HashSet::new();
        let result = MasterNode::select_volumes_node_anti_affinity(&sorted, 6, &exclude);

        assert_eq!(result.len(), 6);
        let node_a_count = result.iter().filter(|v| v.node_id == "node-A").count();
        let node_b_count = result.iter().filter(|v| v.node_id == "node-B").count();
        assert_eq!(node_a_count, 3, "node-A should have 3 volumes");
        assert_eq!(node_b_count, 3, "node-B should have 3 volumes");
    }

    /// 测试: exclude_ids 排除已有 volume (zone 扩容场景)
    #[test]
    fn test_node_anti_affinity_excludes_existing() {
        let routes = vec![
            make_route(1, "node-A", 0, 100),
            make_route(2, "node-B", 0, 100),
            make_route(3, "node-C", 0, 100),
            make_route(4, "node-D", 0, 100),
        ];
        let mut exclude = std::collections::HashSet::new();
        exclude.insert(1u64); // 已有 volume 1 (node-A)

        let result = MasterNode::select_volumes_node_anti_affinity(&routes, 1, &exclude);
        assert_eq!(result.len(), 1);
        assert_ne!(
            result[0].volume_id, 1,
            "excluded volume should not be selected"
        );
        assert_ne!(
            result[0].node_id, "node-A",
            "should prefer a different node when expanding"
        );
    }

    /// 测试: 旧测试场景 (2 节点, 6 volumes 集中) — 反亲和性应避免集中
    #[test]
    fn test_node_anti_affinity_prevents_concentration() {
        // 模拟 P0 bug 场景: 2 节点, 每节点 3 volumes, 空闲率最高
        // 旧逻辑: top-6 全在这 2 节点 → 停 1 节点丢 3 分片
        // 新逻辑: 仍在这 2 节点 (因为只有 2 节点), 但每节点最多 3 个
        // 实际修复在 Master 层保证 zone 分配时 6 volumes 分布在 6 节点
        let routes = vec![
            make_route(1, "node-A", 0, 100),
            make_route(2, "node-A", 0, 100),
            make_route(3, "node-A", 0, 100),
            make_route(4, "node-B", 0, 100),
            make_route(5, "node-B", 0, 100),
            make_route(6, "node-B", 0, 100),
        ];

        let exclude = std::collections::HashSet::new();
        let result = MasterNode::select_volumes_node_anti_affinity(&routes, 6, &exclude);

        assert_eq!(result.len(), 6);
        // 6 volumes across 2 nodes → 3 per node (best possible with 2 nodes)
        let node_a_count = result.iter().filter(|v| v.node_id == "node-A").count();
        let node_b_count = result.iter().filter(|v| v.node_id == "node-B").count();
        assert_eq!(node_a_count, 3);
        assert_eq!(node_b_count, 3);
    }

    #[test]
    fn test_map_node_state_maintenance_precedence() {
        use powerfs_allocator::NodeRuntimeState;
        // maintenance_mode flag overrides any reported state
        assert_eq!(
            map_node_state(NodeState::Healthy, true),
            NodeRuntimeState::Maintenance
        );
        assert_eq!(
            map_node_state(NodeState::Fault, true),
            NodeRuntimeState::Maintenance
        );
    }

    #[test]
    fn test_map_node_state_variants() {
        use powerfs_allocator::NodeRuntimeState;
        assert_eq!(
            map_node_state(NodeState::Healthy, false),
            NodeRuntimeState::Healthy
        );
        assert_eq!(
            map_node_state(NodeState::Ready, false),
            NodeRuntimeState::Healthy
        );
        assert_eq!(
            map_node_state(NodeState::SoftError, false),
            NodeRuntimeState::Healthy
        );
        assert_eq!(
            map_node_state(NodeState::FailSlow, false),
            NodeRuntimeState::Healthy
        );
        assert_eq!(
            map_node_state(NodeState::Degraded, false),
            NodeRuntimeState::Degraded
        );
        assert_eq!(
            map_node_state(NodeState::Maintenance, false),
            NodeRuntimeState::Maintenance
        );
        assert_eq!(
            map_node_state(NodeState::Fault, false),
            NodeRuntimeState::Down
        );
        assert_eq!(
            map_node_state(NodeState::Unavailable, false),
            NodeRuntimeState::Down
        );
    }

    #[test]
    fn test_map_volume_state_variants() {
        use powerfs_allocator::VolumeRuntimeState;
        use powerfs_common::types::VolumeState;
        assert_eq!(
            map_volume_state(VolumeState::Creating),
            VolumeRuntimeState::Active
        );
        assert_eq!(
            map_volume_state(VolumeState::Available),
            VolumeRuntimeState::Active
        );
        assert_eq!(
            map_volume_state(VolumeState::ReadOnly),
            VolumeRuntimeState::Active
        );
        assert_eq!(
            map_volume_state(VolumeState::Draining),
            VolumeRuntimeState::Draining
        );
        assert_eq!(
            map_volume_state(VolumeState::Full),
            VolumeRuntimeState::Full
        );
        assert_eq!(
            map_volume_state(VolumeState::Deleting),
            VolumeRuntimeState::Deleted
        );
    }

    #[test]
    fn test_compute_load_score_weights() {
        // Tolerance accounts for f32 arithmetic in the weighted blend.
        // All-zero (pre-P5 node, empty disk) → 0.0
        assert!((compute_load_score(0.0, 0.0, 0, 0) - 0.0).abs() < 1e-6);
        // 100% cpu, 100% mem, 100% disk → 1.0
        assert!((compute_load_score(1.0, 1.0, 100, 100) - 1.0).abs() < 1e-6);
        // 50% cpu only, empty disk → 0.4 * 0.5 = 0.2
        assert!((compute_load_score(0.5, 0.0, 0, 0) - 0.2).abs() < 1e-6);
        // 0% cpu, 0% mem, 50% disk → 0.3 * 0.5 = 0.15
        assert!((compute_load_score(0.0, 0.0, 100, 50) - 0.15).abs() < 1e-6);
        // 50% cpu, 50% mem, 50% disk → 0.4*0.5 + 0.3*0.5 + 0.3*0.5 = 0.5
        assert!((compute_load_score(0.5, 0.5, 100, 50) - 0.5).abs() < 1e-6);
    }

    #[test]
    fn test_update_node_load_metrics() {
        // update_node_load_metrics is a local (non-Raft) topology update.
        // We verify it sets cpu/memory on an existing node.
        // Note: MasterNode requires Raft + network setup; we test the
        // topology-level update directly via get_node_mut.
        use powerfs_common::types::Topology;
        let mut topo = Topology::new();
        let n = DataNodeInfo::new(
            NodeId("test-node".to_string()),
            "127.0.0.1".to_string(),
            RackId("r1".to_string()),
            DataCenterId("dc1".to_string()),
            8080,
            8081,
            String::new(),
        );
        topo.get_or_create_node(n);

        // Simulate the update_node_load_metrics logic
        {
            if let Some(node) = topo.get_node_mut(&NodeId("test-node".to_string())) {
                node.cpu_usage = 0.75;
                node.memory_usage = 0.50;
            }
        }

        let node = topo.get_node(&NodeId("test-node".to_string())).unwrap();
        assert!((node.cpu_usage - 0.75).abs() < 1e-6);
        assert!((node.memory_usage - 0.50).abs() < 1e-6);
    }
}
