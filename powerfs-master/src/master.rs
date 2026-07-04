use crate::raft_node::{ApplyEntry, OutgoingMessage, ProposeRequest, RaftNode};
use crate::raft_storage::RaftCommand;
use crate::volume_client::VolumeClientPool;
use chrono::Utc;
use log::{debug, error, info, warn};
use powerfs_common::{
    error::{PowerFsError, Result},
    event::{Event, EventPublisher, NodeStatusEvent, VolumeStatusEvent},
    types::{
        ClusterConfig, Collection, CollectionConfig, DataCenterId, DataNodeInfo, DiskType, Fid,
        NodeId, NodeState, RackId, RaftConfig, ReplicaPlacement, Topology, Ttl, VolumeId,
        VolumeInfo, VolumeState,
    },
};
use powerfs_core::kv_cache::{KVCacheEngine, KVDtype};
use powerfs_core::kv_cache_persist::KVPersistStore;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use tokio::sync::{broadcast, mpsc};

pub use crate::proto::VolumeShortInfo;

pub struct MasterNode {
    id: NodeId,
    address: SocketAddr,
    topology: RwLock<Topology>,
    volumes: RwLock<HashMap<VolumeId, VolumeInfo>>,
    collections: RwLock<HashMap<String, CollectionConfig>>,
    volume_layouts: RwLock<HashMap<String, VolumeLayout>>,
    cluster_config: RwLock<ClusterConfig>,
    raft_config: RaftConfig,
    propose_tx: mpsc::Sender<ProposeRequest>,
    step_tx: mpsc::Sender<raft::eraftpb::Message>,
    message_tx: broadcast::Sender<OutgoingMessage>,
    raft_id: u64,
    raft_address: String,
    is_leader: RwLock<bool>,
    leader_address: RwLock<String>,
    next_volume_id: RwLock<u32>,
    max_file_key: RwLock<u64>,
    heartbeat_tx: mpsc::Sender<NodeId>,
    client_manager: RwLock<ClientManager>,
    notify_tx: mpsc::Sender<VolumeLocationUpdate>,
    pub kv_cache: Arc<KVCacheEngine>,
    pub kv_persist: Arc<KVPersistStore>,
    pub directory_tree: Arc<crate::directory_tree::DirectoryTree>,
    pub volume_client_pool: Arc<VolumeClientPool>,
    event_publisher: Option<EventPublisher>,
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
    pub volume_id: u32,
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
}

pub struct ClientManager {
    clients: HashMap<String, mpsc::Sender<VolumeLocationUpdate>>,
}

impl ClientManager {
    fn new() -> Self {
        ClientManager {
            clients: HashMap::new(),
        }
    }

    fn add_client(&mut self, client_id: String, tx: mpsc::Sender<VolumeLocationUpdate>) {
        self.clients.insert(client_id, tx);
    }

    fn remove_client(&mut self, client_id: &str) {
        self.clients.remove(client_id);
    }

    fn broadcast(&self, update: &VolumeLocationUpdate) {
        for (id, tx) in &self.clients {
            if let Err(e) = tx.try_send(update.clone()) {
                warn!("Failed to broadcast to client {}: {}", id, e);
            }
        }
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
        address: &str,
        cluster_config: Option<ClusterConfig>,
        raft_path: &str,
    ) -> Result<Self> {
        let addr: SocketAddr = address.parse()?;

        let node_id = NodeId(format!("{}", addr));
        let config = cluster_config.unwrap_or_default();
        let raft_config = RaftConfig::default();

        // Create Raft node (single node for now, will add peers later)
        let mut raft_node = RaftNode::new(1, address.to_string(), vec![], raft_path)
            .map_err(|e| PowerFsError::Internal(format!("Failed to create raft node: {}", e)))?;

        let (heartbeat_tx, mut heartbeat_rx) = mpsc::channel(100);
        let (notify_tx, mut notify_rx) = mpsc::channel(1000);

        // Extract channel senders before spawning the Raft event loop
        let propose_tx = raft_node.get_propose_tx();
        let step_tx = raft_node.get_step_tx();
        let message_tx = raft_node.get_message_tx();
        let mut apply_rx = raft_node.take_apply_rx();

        // Spawn the Raft event loop (processes ticks, proposals, and messages)
        tokio::spawn(async move {
            if let Err(e) = raft_node.run().await {
                error!("Raft event loop exited: {}", e);
            }
        });

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
            KVPersistStore::new(kv_persist_path.to_str().unwrap_or("kv_persist"))
                .map_err(|e| PowerFsError::Internal(format!("Failed to create KV persist store: {}", e)))?,
        );

        Self::restore_kv_sessions(&kv_cache, &kv_persist);

        let dir_tree_path = std::path::Path::new(raft_path)
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."));
        let dir_tree = Arc::new(
            crate::directory_tree::DirectoryTree::new(&dir_tree_path.join("directory_tree"))
                .map_err(|e| {
                    PowerFsError::Internal(format!("Failed to create directory tree: {}", e))
                })?,
        );
        dir_tree
            .init_root()
            .map_err(|e| PowerFsError::Internal(format!("Failed to init root: {}", e)))?;

        let volume_client_pool = Arc::new(VolumeClientPool::new());

        let event_publisher = match std::env::var("REDIS_URL") {
            Ok(url) => {
                info!("Event publisher enabled with Redis: {}", url);
                Some(EventPublisher::new(&url, "powerfs_events", "master"))
            }
            Err(_) => {
                warn!("REDIS_URL not set, event publishing disabled");
                None
            }
        };

        let master = MasterNode {
            id: node_id.clone(),
            address: addr,
            topology: RwLock::new(Topology::new()),
            volumes: RwLock::new(HashMap::new()),
            collections: RwLock::new(collections),
            volume_layouts: RwLock::new(HashMap::new()),
            cluster_config: RwLock::new(config),
            raft_config,
            propose_tx,
            step_tx,
            message_tx,
            raft_id: 1,
            raft_address: address.to_string(),
            is_leader: RwLock::new(true),
            leader_address: RwLock::new(address.to_string()),
            next_volume_id: RwLock::new(1),
            max_file_key: RwLock::new(0),
            heartbeat_tx,
            client_manager: RwLock::new(ClientManager::new()),
            notify_tx,
            kv_cache,
            kv_persist,
            directory_tree: dir_tree,
            volume_client_pool,
            event_publisher,
        };

        let master_clone = master.clone();
        tokio::spawn(async move {
            while let Some(node_id) = heartbeat_rx.recv().await {
                master_clone.handle_heartbeat(&node_id).await;
            }
        });

        let master_clone = master.clone();
        tokio::spawn(async move {
            while let Some(update) = notify_rx.recv().await {
                master_clone
                    .client_manager
                    .read()
                    .unwrap()
                    .broadcast(&update);
            }
        });

        // Start apply loop (receives committed entries from the Raft event loop)
        let master_clone = master.clone();
        tokio::spawn(async move {
            while let Some(entry) = apply_rx.recv().await {
                if let Err(e) = master_clone.apply_command(entry).await {
                    error!("Failed to apply command: {}", e);
                }
            }
        });

        Ok(master)
    }

    fn restore_kv_sessions(kv_cache: &Arc<KVCacheEngine>, kv_persist: &Arc<KVPersistStore>) {
        if let Ok(sessions) = kv_persist.list_sessions() {
            for session_id in sessions {
                if let Ok(Some(meta)) = kv_persist.load_session(&session_id) {
                    let dtype = meta.dtype_enum();
                    let _ = kv_cache.create_session(
                        &session_id,
                        &meta.model_name,
                        meta.num_layers,
                        meta.num_heads,
                        meta.head_dim,
                        dtype,
                        meta.ttl_seconds,
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
        *self.is_leader.read().unwrap()
    }

    pub async fn get_leader(&self) -> String {
        self.leader_address.read().unwrap().clone()
    }

    pub fn set_leader(&self, leader_addr: String) {
        *self.leader_address.write().unwrap() = leader_addr;
    }

    /// Propose a command to the Raft cluster
    ///
    /// In single-node mode, we apply the command directly to the state machine
    /// to avoid waiting for Raft commit (which never advances without peers).
    /// The command is also proposed to Raft for log persistence (best effort).
    pub async fn propose_command(&self, cmd: RaftCommand) -> Result<u64> {
        if !self.is_leader().await {
            return Err(PowerFsError::NotLeader);
        }

        // Single-node mode: apply directly to state machine (bypass Raft commit delay)
        let entry = ApplyEntry {
            index: 0,
            command: cmd.clone(),
        };
        self.apply_command(entry).await?;

        // Best-effort: also propose to Raft for log persistence (don't wait for response)
        let data = cmd.serialize();
        let (resp_tx, _resp_rx) = tokio::sync::oneshot::channel();
        let req = crate::raft_node::ProposeRequest {
            data,
            response_tx: resp_tx,
        };
        let _ = self.propose_tx.try_send(req);

        Ok(0)
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
            } => {
                self.apply_update_node_volumes(&node_id, &volumes, &ip, grpc_port)
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
            RaftCommand::DeleteVolume { volume_id } => {
                self.apply_delete_volume(volume_id).await?;
            }
        }

        Ok(())
    }

    fn apply_add_node(&self, params: AddNodeParams) -> Result<()> {
        let dc_id = DataCenterId(params.data_center);
        let rack_id = RackId(params.rack);
        let node_id = params.node_id.clone();
        let address = params.address.clone();
        let http_port = params.http_port;
        let grpc_port = params.grpc_port;

        let mut topology = self.topology.write().unwrap();
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

        info!("Applied AddNode: {} at {}:{}", node_id, address, http_port);

        if let Some(publisher) = self.event_publisher.clone() {
            let node_id_str = node_id.0.clone();
            let addr_clone = address.clone();
            tokio::spawn(async move {
                let event = Event::NodeStatus(NodeStatusEvent {
                    node_id: node_id_str.clone(),
                    address: addr_clone,
                    grpc_port,
                    http_port,
                    status: "healthy".to_string(),
                    cpu_usage: 0.0,
                    mem_usage: 0.0,
                    disk_usage: 0.0,
                    network_rx: 0,
                    network_tx: 0,
                    uptime: 0,
                    volume_count: 0,
                });
                if let Err(e) = publisher.publish(event, &node_id_str).await {
                    warn!("Failed to publish node_status event: {}", e);
                }
            });
        }

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

        if let Some(publisher) = self.event_publisher.clone() {
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
                });
                if let Err(e) = publisher.publish(event, &format!("{}", vid_clone)).await {
                    warn!("Failed to publish volume_status event: {}", e);
                }
            });
        }

        Ok(())
    }

    fn apply_update_volume_state(&self, volume_id: u32, state: VolumeState) -> Result<()> {
        let vid = VolumeId(volume_id);
        let mut volumes = self.volumes.write().unwrap();
        if let Some(info) = volumes.get_mut(&vid) {
            info.state = state;
            info.modified_at = Utc::now();
        }
        Ok(())
    }

    async fn apply_update_node_volumes(
        &self,
        node_id: &str,
        volumes: &[crate::raft_storage::RaftVolumeShortInfo],
        ip: &str,
        grpc_port: u32,
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
        for vol in volumes {
            let vid = VolumeId(vol.volume_id);
            let state = if vol.read_only {
                VolumeState::ReadOnly
            } else {
                VolumeState::Available
            };

            volumes_map.insert(
                vid,
                VolumeInfo {
                    id: vid,
                    node_id: nid.clone(),
                    collection: Collection::default(),
                    size: vol.size,
                    used: 0,
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

    async fn apply_delete_volume(&self, volume_id: u32) -> Result<()> {
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
        info!(
            "Proposed AddNode: {} at {}:{}",
            params.node_id, params.address, params.http_port
        );

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

    pub async fn list_volumes(&self) -> Vec<VolumeInfo> {
        self.volumes.read().unwrap().values().cloned().collect()
    }

    pub async fn list_nodes(&self) -> Vec<DataNodeInfo> {
        self.topology.read().unwrap().list_all_nodes()
    }

    pub fn get_node(&self, node_id: &NodeId) -> Option<DataNodeInfo> {
        self.topology.read().unwrap().get_node(node_id).cloned()
    }

    pub async fn update_node_volumes(&self, params: UpdateNodeVolumesParams) -> Result<()> {
        if !self.is_leader().await {
            return Err(PowerFsError::NotLeader);
        }

        let short_volumes: Vec<crate::raft_storage::RaftVolumeShortInfo> = params
            .volumes
            .iter()
            .map(|v| crate::raft_storage::RaftVolumeShortInfo {
                volume_id: v.volume_id,
                size: v.size,
                read_only: v.read_only,
            })
            .collect();

        let cmd = RaftCommand::UpdateNodeVolumes {
            node_id: params.node_id.0.clone(),
            volumes: short_volumes,
            ip: params.ip,
            grpc_port: params.grpc_port,
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

        let nodes = self.topology.read().unwrap().list_all_nodes();
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

        // Try to find an existing writable volume for this collection with available space.
        // This reuses volumes already created on volume servers (via grow), avoiding
        // "volume not found" errors when writing.
        {
            let volumes = self.volumes.read().unwrap();
            let mut best: Option<(VolumeId, NodeId)> = None;
            for (vid, vinfo) in volumes.iter() {
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
                // Check the hosting node is still in topology
                if !nodes.iter().any(|n| n.id == vinfo.node_id) {
                    continue;
                }
                best = Some((*vid, vinfo.node_id.clone()));
                break;
            }
            if let Some((existing_vid, host_node_id)) = best {
                // Found an existing volume - just allocate a new file_key on it
                drop(volumes);
                let file_key = {
                    let mut volumes = self.volumes.write().unwrap();
                    if let Some(vol_info) = volumes.get_mut(&existing_vid) {
                        let key = vol_info.next_file_key;
                        vol_info.next_file_key += 1;
                        key
                    } else {
                        1
                    }
                };

                let cookie = rand::random::<u32>() as u64;
                let fid = Fid {
                    volume_id: existing_vid,
                    cookie,
                    file_key,
                };

                let host_node = nodes
                    .iter()
                    .find(|n| n.id == host_node_id)
                    .cloned()
                    .into_iter()
                    .collect::<Vec<_>>();

                info!(
                    "Reused existing volume: {} for collection {:?}, fid: {},{},{}",
                    existing_vid, collection_obj, existing_vid.0, cookie, file_key
                );

                return Ok((fid, host_node));
            }
        }

        // No existing writable volume found - create a new one
        let selected_nodes = if rack_awareness_enabled && nodes.len() > 1 {
            Self::select_nodes_by_rack(&nodes, replica_count)
        } else {
            nodes.into_iter().take(replica_count as usize).collect()
        };

        if selected_nodes.len() < replica_count as usize {
            return Err(PowerFsError::InvalidRequest(
                "not enough nodes available for replication".to_string(),
            ));
        }

        let volume_id = {
            let mut next_id = self.next_volume_id.write().unwrap();
            let vid = VolumeId(*next_id);
            *next_id += 1;
            vid
        };

        for (i, node) in selected_nodes.iter().enumerate() {
            let state = if i == 0 {
                VolumeState::Creating
            } else {
                VolumeState::Available
            };

            let mut volumes = self.volumes.write().unwrap();
            volumes.insert(
                volume_id,
                VolumeInfo {
                    id: volume_id,
                    node_id: node.id.clone(),
                    collection: collection_obj.clone(),
                    size: volume_size_limit,
                    used: 0,
                    replica_count,
                    ttl: ttl.clone(),
                    disk_type: disk_type.clone(),
                    state,
                    created_at: Utc::now(),
                    modified_at: Utc::now(),
                    next_file_key: 1,
                },
            );
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

        // Get file_key from this volume's next_file_key counter
        let file_key = {
            let mut volumes = self.volumes.write().unwrap();
            if let Some(vol_info) = volumes.get_mut(&volume_id) {
                let key = vol_info.next_file_key;
                vol_info.next_file_key += 1;
                key
            } else {
                1
            }
        };

        // Generate random cookie to prevent FID collision
        let cookie = rand::random::<u32>() as u64;

        let fid = Fid {
            volume_id,
            cookie,
            file_key,
        };

        info!(
            "Assigned volume: {} to nodes: {:?}, fid: {},{},{}",
            volume_id,
            selected_nodes
                .iter()
                .map(|n| n.id.clone())
                .collect::<Vec<_>>(),
            volume_id.0,
            cookie,
            file_key
        );

        // Propose to Raft for replication
        if let Some(first_node) = selected_nodes.first() {
            let cmd = RaftCommand::AssignVolume {
                node_id: first_node.id.0.clone(),
                volume_id: volume_id.0,
                collection: collection_obj.0.clone(),
                replica_count,
                ttl: ttl.0,
                disk_type: disk_type.0.clone(),
                size: volume_size_limit,
            };
            // Best effort - don't fail the request if propose fails in single-node mode
            let _ = self.propose_command(cmd).await;
        }

        Ok((fid, selected_nodes))
    }

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

    pub async fn lookup_volume(
        &self,
        volume_ids: &[String],
    ) -> HashMap<VolumeId, Vec<DataNodeInfo>> {
        let mut result = HashMap::new();
        let volumes = self.volumes.read().unwrap();
        let topology = self.topology.read().unwrap();

        for vid_str in volume_ids {
            if let Ok(vid) = u32::from_str(vid_str) {
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
        crate::proto::ClusterInfoResponse {
            node_id: self.raft_id,
            address: self.raft_address.clone(),
            is_leader: *self.is_leader.read().unwrap(),
            term: 1,
            peers: Vec::new(),
        }
    }

    pub async fn raft_propose(&self, data: Vec<u8>) -> std::result::Result<u64, String> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        self.propose_tx
            .send(crate::raft_node::ProposeRequest { data, response_tx })
            .await
            .map_err(|e| format!("failed to send propose: {}", e))?;
        response_rx
            .await
            .map_err(|e| format!("propose response error: {}", e))?
    }

    pub fn raft_step_tx(&self) -> tokio::sync::mpsc::Sender<raft::eraftpb::Message> {
        self.step_tx.clone()
    }

    pub fn raft_transfer_leader(&self, _target_id: u64) -> std::result::Result<(), String> {
        Err("transfer_leader not supported in single-node mode".to_string())
    }

    pub fn raft_message_tx(
        &self,
    ) -> tokio::sync::broadcast::Sender<crate::raft_node::OutgoingMessage> {
        self.message_tx.clone()
    }

    pub async fn start_raft(&self, _peers: Vec<String>) -> Result<()> {
        info!("Starting Raft (single node mode, always leader)");
        *self.is_leader.write().unwrap() = true;
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    pub async fn start(self: Arc<Self>) -> Result<()> {
        info!("Starting PowerFS Master node: {:?}", self.id);
        info!("Listening on: {}", self.address);

        let server = crate::server::MasterGrpcServer::new(self.clone(), self.kv_cache.clone());
        server
            .start(self.address)
            .await
            .map_err(|e| PowerFsError::Internal(format!("Failed to start server: {}", e)))?;

        Ok(())
    }
}

impl Clone for MasterNode {
    fn clone(&self) -> Self {
        MasterNode {
            id: self.id.clone(),
            address: self.address,
            topology: RwLock::new(self.topology.read().unwrap().clone()),
            volumes: RwLock::new(self.volumes.read().unwrap().clone()),
            collections: RwLock::new(self.collections.read().unwrap().clone()),
            volume_layouts: RwLock::new(self.volume_layouts.read().unwrap().clone()),
            cluster_config: RwLock::new(self.cluster_config.read().unwrap().clone()),
            raft_config: self.raft_config.clone(),
            propose_tx: self.propose_tx.clone(),
            step_tx: self.step_tx.clone(),
            message_tx: self.message_tx.clone(),
            raft_id: self.raft_id,
            raft_address: self.raft_address.clone(),
            is_leader: RwLock::new(*self.is_leader.read().unwrap()),
            leader_address: RwLock::new(self.leader_address.read().unwrap().clone()),
            next_volume_id: RwLock::new(*self.next_volume_id.read().unwrap()),
            max_file_key: RwLock::new(*self.max_file_key.read().unwrap()),
            heartbeat_tx: self.heartbeat_tx.clone(),
            client_manager: RwLock::new(ClientManager::new()),
            notify_tx: self.notify_tx.clone(),
            kv_cache: self.kv_cache.clone(),
            kv_persist: self.kv_persist.clone(),
            directory_tree: self.directory_tree.clone(),
            volume_client_pool: self.volume_client_pool.clone(),
            event_publisher: self.event_publisher.clone(),
        }
    }
}
