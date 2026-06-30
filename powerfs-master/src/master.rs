use crate::raft_storage::MasterRaftStorage;
use chrono::Utc;
use log::{debug, info, warn};
use powerfs_common::{
    error::{PowerFsError, Result},
    types::{
        ClusterConfig, Collection, DataCenterId, DataNodeInfo, DiskType, Fid, NodeId, NodeState,
        RackId, RaftConfig, ReplicaPlacement, Ttl, Topology, VolumeId, VolumeInfo, VolumeState,
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
    raft_storage: MasterRaftStorage,
    is_leader: RwLock<bool>,
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

        let raft_storage = MasterRaftStorage::new(raft_path)
            .map_err(|e| PowerFsError::Internal(format!("Failed to create raft storage: {}", e)))?;

        let (heartbeat_tx, mut heartbeat_rx) = mpsc::channel(100);
        let (notify_tx, mut notify_rx) = mpsc::channel(1000);

        let master = MasterNode {
            id: node_id.clone(),
            address: addr,
            topology: RwLock::new(Topology::new()),
            volumes: RwLock::new(HashMap::new()),
            volume_layouts: RwLock::new(HashMap::new()),
            cluster_config: RwLock::new(config),
            raft_config,
            raft_storage,
            is_leader: RwLock::new(true),
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
                master_clone.client_manager.read().unwrap().broadcast(&update);
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
        if *self.is_leader.read().unwrap() {
            format!("{}", self.address)
        } else {
            String::new()
        }
    }

    pub async fn add_node(
        &self,
        node_id: NodeId,
        address: String,
        rack: String,
        data_center: String,
        http_port: u32,
        grpc_port: u32,
        public_url: String,
    ) -> Result<()> {
        if !self.is_leader().await {
            return Err(PowerFsError::NotLeader);
        }

        let dc_id = DataCenterId(data_center);
        let rack_id = RackId(rack);

        let mut topology = self.topology.write().unwrap();
        topology.get_or_create_node(
            dc_id,
            rack_id,
            node_id.clone(),
            address.clone(),
            http_port,
            grpc_port,
            public_url,
        );

        info!("Added node: {} at {}:{}", node_id, address, http_port);

        Ok(())
    }

    pub async fn remove_node(&self, node_id: &NodeId) -> Result<()> {
        if !self.is_leader().await {
            return Err(PowerFsError::NotLeader);
        }

        let mut topology = self.topology.write().unwrap();
        if topology.remove_node(node_id).is_none() {
            return Err(PowerFsError::InvalidRequest("node not found".to_string()));
        }

        info!("Removed node: {:?}", node_id);

        Ok(())
    }

    pub async fn get_volume(&self, volume_id: &VolumeId) -> Result<VolumeInfo> {
        let volumes = self.volumes.read().unwrap();
        volumes
            .get(volume_id)
            .cloned()
            .ok_or(PowerFsError::VolumeNotFound(volume_id.clone()))
    }

    pub async fn update_volume_state(
        &self,
        volume_id: &VolumeId,
        state: VolumeState,
    ) -> Result<()> {
        if !self.is_leader().await {
            return Err(PowerFsError::NotLeader);
        }

        let mut volumes = self.volumes.write().unwrap();
        if let Some(info) = volumes.get_mut(volume_id) {
            info.state = state;
            info.modified_at = Utc::now();

            if state == VolumeState::Available {
                let mut layouts = self.volume_layouts.write().unwrap();
                let key =
                    Self::get_volume_layout_key(&info.collection, info.replica_count, &info.ttl, &info.disk_type);
                if let Some(layout) = layouts.get_mut(&key) {
                    layout.volumes.push(info.id);
                }
            }

            Ok(())
        } else {
            Err(PowerFsError::VolumeNotFound(volume_id.clone()))
        }
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

    pub async fn update_node_volumes(
        &self,
        node_id: &NodeId,
        volumes: &[VolumeShortInfo],
        new_volumes: &[VolumeShortInfo],
        deleted_volumes: &[VolumeShortInfo],
        ip: &str,
        grpc_port: u32,
        http_port: u32,
    ) -> Result<()> {
        {
            let mut topology = self.topology.write().unwrap();
            let node = topology.get_node_mut(node_id);

            if let Some(node) = node {
                node.address = ip.to_string();
                node.grpc_port = grpc_port;
                node.http_port = http_port;
                node.last_heartbeat = Utc::now();
                node.state = NodeState::Healthy;
                node.volume_count = volumes.len() as u32;
            }
        }

        let (new_vids, deleted_vids) = {
            let mut volumes_map = self.volumes.write().unwrap();
            let mut new_vids = Vec::new();
            let mut deleted_vids = Vec::new();

            for vol in new_volumes {
                let volume_id = VolumeId(vol.volume_id);
                let state = if vol.read_only {
                    VolumeState::ReadOnly
                } else {
                    VolumeState::Available
                };

                let is_new = !volumes_map.contains_key(&volume_id);

                volumes_map.insert(
                    volume_id,
                    VolumeInfo {
                        id: volume_id,
                        node_id: node_id.clone(),
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

                if is_new && state == VolumeState::Available {
                    new_vids.push(vol.volume_id);
                }
            }

            for vol in volumes {
                let volume_id = VolumeId(vol.volume_id);
                let state = if vol.read_only {
                    VolumeState::ReadOnly
                } else {
                    VolumeState::Available
                };

                volumes_map.insert(
                    volume_id,
                    VolumeInfo {
                        id: volume_id,
                        node_id: node_id.clone(),
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

            for vol in deleted_volumes {
                let volume_id = VolumeId(vol.volume_id);
                if volumes_map.remove(&volume_id).is_some() {
                    deleted_vids.push(vol.volume_id);
                }
            }

            (new_vids, deleted_vids)
        };

        if !new_vids.is_empty() || !deleted_vids.is_empty() {
            let leader = self.get_leader().await;
            let _ = self.notify_tx.try_send(VolumeLocationUpdate {
                new_vids,
                deleted_vids,
                leader,
            });
        }

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
            return Err(PowerFsError::InvalidRequest("no nodes available".to_string()));
        }

        let config = self.cluster_config.read().unwrap();
        let replica_placement = ReplicaPlacement::from_string(replication).unwrap_or_default();

        let collection = Collection(collection.to_string());
        let ttl = Ttl::default();
        let disk_type = DiskType::default();

        let replica_count = replica_placement.get_copy_count();
        let selected_nodes: Vec<DataNodeInfo>;

        if config.rack_awareness_enabled && nodes.len() > 1 {
            selected_nodes = Self::select_nodes_by_rack(&nodes, replica_count);
        } else {
            selected_nodes = nodes.into_iter().take(replica_count as usize).collect();
        }

        if selected_nodes.len() < replica_count as usize {
            return Err(PowerFsError::InvalidRequest(
                "not enough nodes available for replication".to_string(),
            ));
        }

        let mut next_id = self.next_volume_id.write().unwrap();
        let volume_id = VolumeId(*next_id);
        *next_id += 1;
        drop(next_id);

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
                    size: config.volume_size_limit,
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

        let mut layouts = self.volume_layouts.write().unwrap();
        let key = Self::get_volume_layout_key(&collection, replica_count, &ttl, &disk_type);
        layouts.entry(key).or_insert_with(|| VolumeLayout {
            collection,
            replica_placement,
            ttl,
            disk_type,
            volumes: Vec::new(),
        });

        // Get file_key from this volume's next_file_key counter
        let mut volumes = self.volumes.write().unwrap();
        let file_key = if let Some(vol_info) = volumes.get_mut(&volume_id) {
            let key = vol_info.next_file_key;
            vol_info.next_file_key += 1;
            key
        } else {
            // If volume not found, start from 1
            1
        };
        drop(volumes);

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
            selected_nodes.iter().map(|n| n.id.clone()).collect::<Vec<_>>(),
            volume_id.0, cookie, file_key
        );

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
        self.client_manager.write().unwrap().add_client(client_id, tx);
    }

    pub fn remove_client(&self, client_id: &str) {
        self.client_manager.write().unwrap().remove_client(client_id);
    }

    pub async fn lookup_volume(&self, volume_ids: &[String]) -> HashMap<VolumeId, Vec<DataNodeInfo>> {
        let mut result = HashMap::new();
        let volumes = self.volumes.read().unwrap();
        let topology = self.topology.read().unwrap();

        for vid_str in volume_ids {
            if let Ok(vid) = u32::from_str(vid_str) {
                let volume_id = VolumeId(vid);
                if let Some(vol) = volumes.get(&volume_id) {
                    if let Some(node) = topology.get_node(&vol.node_id) {
                        result.entry(volume_id).or_insert_with(Vec::new).push(node.clone());
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
            raft_storage: self.raft_storage.clone(),
            is_leader: RwLock::new(*self.is_leader.read().unwrap()),
            next_volume_id: RwLock::new(*self.next_volume_id.read().unwrap()),
            max_file_key: RwLock::new(*self.max_file_key.read().unwrap()),
            heartbeat_tx: self.heartbeat_tx.clone(),
            client_manager: RwLock::new(ClientManager::new()),
            notify_tx: self.notify_tx.clone(),
        }
    }
}
