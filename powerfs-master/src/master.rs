use crate::raft_node::{ApplyEntry, RaftNode};
use crate::raft_storage::RaftCommand;
use chrono::Utc;
use log::{debug, error, info, warn};
use powerfs_common::{
    error::{PowerFsError, Result},
    types::{
        ClusterConfig, Collection, DataCenterId, DataNodeInfo, DiskType, Fid, NodeId, NodeState,
        RackId, RaftConfig, ReplicaPlacement, Topology, Ttl, VolumeId, VolumeInfo, VolumeState,
    },
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;

pub use crate::proto::VolumeShortInfo;

pub struct MasterNode {
    id: NodeId,
    address: SocketAddr,
    topology: RwLock<Topology>,
    volumes: RwLock<HashMap<VolumeId, VolumeInfo>>,
    volume_layouts: RwLock<HashMap<String, VolumeLayout>>,
    cluster_config: RwLock<ClusterConfig>,
    raft_config: RaftConfig,
    raft_node: Arc<RwLock<RaftNode>>,
    is_leader: RwLock<bool>,
    leader_address: RwLock<String>,
    next_volume_id: RwLock<u32>,
    max_file_key: RwLock<u64>,
    heartbeat_tx: mpsc::Sender<NodeId>,
    client_manager: RwLock<ClientManager>,
    notify_tx: mpsc::Sender<VolumeLocationUpdate>,
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
        let raft_node = RaftNode::new(1, address.to_string(), vec![], raft_path)
            .map_err(|e| PowerFsError::Internal(format!("Failed to create raft node: {}", e)))?;

        let (heartbeat_tx, mut heartbeat_rx) = mpsc::channel(100);
        let (notify_tx, mut notify_rx) = mpsc::channel(1000);

        let raft_node_arc = Arc::new(RwLock::new(raft_node));

        let master = MasterNode {
            id: node_id.clone(),
            address: addr,
            topology: RwLock::new(Topology::new()),
            volumes: RwLock::new(HashMap::new()),
            volume_layouts: RwLock::new(HashMap::new()),
            cluster_config: RwLock::new(config),
            raft_config,
            raft_node: raft_node_arc.clone(),
            is_leader: RwLock::new(true),
            leader_address: RwLock::new(address.to_string()),
            next_volume_id: RwLock::new(1),
            max_file_key: RwLock::new(0),
            heartbeat_tx,
            client_manager: RwLock::new(ClientManager::new()),
            notify_tx,
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

        // Start apply loop
        let master_clone = master.clone();
        let mut apply_rx = {
            let mut node = raft_node_arc.write().unwrap();
            node.take_apply_rx()
        };
        tokio::spawn(async move {
            while let Some(entry) = apply_rx.recv().await {
                if let Err(e) = master_clone.apply_command(entry).await {
                    error!("Failed to apply command: {}", e);
                }
            }
        });

        Ok(master)
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
    pub async fn propose_command(&self, cmd: RaftCommand) -> Result<u64> {
        if !self.is_leader().await {
            return Err(PowerFsError::NotLeader);
        }

        let data = cmd.serialize();
        let propose_tx = {
            let node = self.raft_node.read().unwrap();
            node.get_propose_tx()
        };

        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        let req = crate::raft_node::ProposeRequest {
            data,
            response_tx: resp_tx,
        };

        propose_tx
            .send(req)
            .await
            .map_err(|e| PowerFsError::Internal(format!("propose send failed: {}", e)))?;

        resp_rx
            .await
            .map_err(|e| PowerFsError::Internal(format!("propose recv failed: {}", e)))?
            .map_err(PowerFsError::Internal)
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
        }

        Ok(())
    }

    fn apply_add_node(&self, params: AddNodeParams) -> Result<()> {
        let dc_id = DataCenterId(params.data_center);
        let rack_id = RackId(params.rack);
        let node_id = params.node_id.clone();
        let address = params.address.clone();
        let http_port = params.http_port;

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
                node_id: nid,
                collection: coll,
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

        let collection = Collection(collection.to_string());
        let ttl = Ttl::default();
        let disk_type = DiskType::default();

        let replica_count = replica_placement.get_copy_count();
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
                    collection: collection.clone(),
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
            let key = Self::get_volume_layout_key(&collection, replica_count, &ttl, &disk_type);
            layouts.entry(key).or_insert_with(|| VolumeLayout {
                collection: collection.clone(),
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
                collection: collection.0.clone(),
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

    pub async fn start_raft(&self, _peers: Vec<String>) -> Result<()> {
        info!("Starting Raft (single node mode, always leader)");
        *self.is_leader.write().unwrap() = true;
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    pub async fn start(self: Arc<Self>) -> Result<()> {
        info!("Starting PowerFS Master node: {:?}", self.id);
        info!("Listening on: {}", self.address);

        let server = crate::server::MasterGrpcServer::new(self.clone());
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
            volume_layouts: RwLock::new(self.volume_layouts.read().unwrap().clone()),
            cluster_config: RwLock::new(self.cluster_config.read().unwrap().clone()),
            raft_config: self.raft_config.clone(),
            raft_node: self.raft_node.clone(),
            is_leader: RwLock::new(*self.is_leader.read().unwrap()),
            leader_address: RwLock::new(self.leader_address.read().unwrap().clone()),
            next_volume_id: RwLock::new(*self.next_volume_id.read().unwrap()),
            max_file_key: RwLock::new(*self.max_file_key.read().unwrap()),
            heartbeat_tx: self.heartbeat_tx.clone(),
            client_manager: RwLock::new(ClientManager::new()),
            notify_tx: self.notify_tx.clone(),
        }
    }
}
