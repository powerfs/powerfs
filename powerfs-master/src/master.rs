use powerfs_common::{
    types::{VolumeId, VolumeInfo, VolumeState, NodeId, NodeInfo, NodeState, ClusterConfig, RaftConfig},

    utils::{generate_volume_id, generate_node_id},
    error::{PowerFsError, Result},
};
use std::collections::HashMap;
use std::sync::RwLock;
use std::net::SocketAddr;
use tokio::sync::mpsc;
use chrono::Utc;
use log::{info, warn, debug};

pub struct MasterNode {
    id: NodeId,
    address: SocketAddr,
    nodes: RwLock<HashMap<NodeId, NodeInfo>>,
    volumes: RwLock<HashMap<VolumeId, VolumeInfo>>,
    cluster_config: RwLock<ClusterConfig>,
    raft_config: RaftConfig,
    heartbeat_tx: mpsc::Sender<NodeId>,
    is_leader: RwLock<bool>,
}

impl MasterNode {
    pub async fn new(address: &str, cluster_config: Option<ClusterConfig>) -> Result<Self> {
        let addr: SocketAddr = address.parse()?;
        
        let node_id = generate_node_id();
        let config = cluster_config.unwrap_or_default();
        let raft_config = RaftConfig::default();
        
        let (heartbeat_tx, mut heartbeat_rx) = mpsc::channel(100);
        
        let master = MasterNode {
            id: node_id.clone(),
            address: addr,
            nodes: RwLock::new(HashMap::new()),
            volumes: RwLock::new(HashMap::new()),
            cluster_config: RwLock::new(config),
            raft_config,
            heartbeat_tx,
            is_leader: RwLock::new(true),
        };
        
        let master_clone = master.clone();
        tokio::spawn(async move {
            while let Some(node_id) = heartbeat_rx.recv().await {
                master_clone.handle_heartbeat(&node_id).await;
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

    pub async fn add_node(&self, node_id: NodeId, address: String, rack: String, data_center: String) -> Result<()> {
        if !self.is_leader().await {
            return Err(PowerFsError::NotLeader);
        }
        
        let mut nodes = self.nodes.write().unwrap();
        
        if nodes.contains_key(&node_id) {
            return Err(PowerFsError::InvalidRequest("node already exists".to_string()));
        }
        
        let info = NodeInfo {
            id: node_id.clone(),
            address,
            rack,
            data_center,
            total_space: 0,
            used_space: 0,
            volume_count: 0,
            state: NodeState::Healthy,
            last_heartbeat: Utc::now(),
        };
        
        nodes.insert(node_id.clone(), info);
        info!("Added node: {:?}", node_id);
        
        Ok(())
    }

    pub async fn remove_node(&self, node_id: &NodeId) -> Result<()> {
        if !self.is_leader().await {
            return Err(PowerFsError::NotLeader);
        }
        
        let mut nodes = self.nodes.write().unwrap();
        
        if nodes.remove(node_id).is_none() {
            return Err(PowerFsError::InvalidRequest("node not found".to_string()));
        }
        
        info!("Removed node: {:?}", node_id);
        
        Ok(())
    }

    pub async fn allocate_volume(&self, node_id: &NodeId) -> Result<VolumeInfo> {
        if !self.is_leader().await {
            return Err(PowerFsError::NotLeader);
        }
        
        let nodes = self.nodes.read().unwrap();
        
        if !nodes.contains_key(node_id) {
            return Err(PowerFsError::InvalidRequest("node not found".to_string()));
        }
        
        let config = self.cluster_config.read().unwrap();
        
        let volumes = self.volumes.read().unwrap();
        let node_volume_count = volumes.values()
            .filter(|v| &v.node_id == node_id)
            .count();
        
        if node_volume_count >= config.max_volumes_per_node as usize {
            return Err(PowerFsError::OutOfSpace);
        }
        
        drop(volumes);
        
        let volume_id = generate_volume_id();
        
        let info = VolumeInfo {
            id: volume_id.clone(),
            node_id: node_id.clone(),
            size: config.volume_size_limit,
            used: 0,
            replica_count: config.replication_factor,
            state: VolumeState::Creating,
            created_at: Utc::now(),
            modified_at: Utc::now(),
        };
        
        let mut volumes = self.volumes.write().unwrap();
        volumes.insert(volume_id, info.clone());
        
        info!("Allocated volume: {:?} to node: {:?}", info.id, node_id);
        
        Ok(info)
    }

    pub async fn get_volume(&self, volume_id: &VolumeId) -> Result<VolumeInfo> {
        let volumes = self.volumes.read().unwrap();
        
        volumes.get(volume_id)
            .cloned()
            .ok_or(PowerFsError::VolumeNotFound(volume_id.clone()))
    }

    pub async fn update_volume_state(&self, volume_id: &VolumeId, state: VolumeState) -> Result<()> {
        if !self.is_leader().await {
            return Err(PowerFsError::NotLeader);
        }
        
        let mut volumes = self.volumes.write().unwrap();
        
        if let Some(info) = volumes.get_mut(volume_id) {
            info.state = state;
            info.modified_at = Utc::now();
            Ok(())
        } else {
            Err(PowerFsError::VolumeNotFound(volume_id.clone()))
        }
    }

    pub async fn list_volumes(&self) -> Vec<VolumeInfo> {
        self.volumes.read().unwrap()
            .values()
            .cloned()
            .collect()
    }

    pub async fn list_nodes(&self) -> Vec<NodeInfo> {
        self.nodes.read().unwrap()
            .values()
            .cloned()
            .collect()
    }

    pub async fn handle_heartbeat(&self, node_id: &NodeId) {
        let mut nodes = self.nodes.write().unwrap();
        
        if let Some(info) = nodes.get_mut(node_id) {
            info.last_heartbeat = Utc::now();
            info.state = NodeState::Healthy;
            debug!("Received heartbeat from node: {:?}", node_id);
        } else {
            warn!("Heartbeat from unknown node: {:?}", node_id);
        }
    }

    pub async fn start(&self) -> Result<()> {
        info!("Starting PowerFS Master node: {:?}", self.id);
        info!("Listening on: {}", self.address);
        
        Ok(())
    }

}

impl Clone for MasterNode {
    fn clone(&self) -> Self {
        MasterNode {
            id: self.id.clone(),
            address: self.address,
            nodes: RwLock::new(self.nodes.read().unwrap().clone()),
            volumes: RwLock::new(self.volumes.read().unwrap().clone()),
            cluster_config: RwLock::new(self.cluster_config.read().unwrap().clone()),
            raft_config: self.raft_config.clone(),
            heartbeat_tx: self.heartbeat_tx.clone(),
            is_leader: RwLock::new(*self.is_leader.read().unwrap()),
        }
    }
}
