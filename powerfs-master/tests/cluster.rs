//! Raft test cluster infrastructure

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use log::info;
use tempfile::TempDir;
use tokio::sync::Mutex;
use tokio::time::sleep;

use powerfs_master::raft_node::{OutgoingMessage, Peer, RaftNode};
use powerfs_master::raft_storage::RaftCommand;
use protobuf::Message;
use raft::eraftpb::Message as RaftMessage;

#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub id: u64,
    pub address: String,
}

#[derive(Clone)]
pub struct RaftTestCluster {
    pub nodes: Arc<Mutex<HashMap<u64, RaftTestNode>>>,
    node_infos: HashMap<u64, NodeInfo>,
    #[allow(dead_code)]
    temp_dirs: Vec<Arc<TempDir>>,
}

impl RaftTestCluster {
    pub async fn new(num_nodes: u32) -> Self {
        Self::builder().num_nodes(num_nodes).build().await
    }

    pub fn builder() -> ClusterBuilder {
        ClusterBuilder::default()
    }

    pub async fn start_all(&self) {
        let mut nodes: Vec<_> = self.nodes.lock().await.values().cloned().collect();

        let node_map: HashMap<u64, RaftTestNode> =
            nodes.iter().map(|n| (n.id, n.clone())).collect();

        for node in &mut nodes {
            node.register_message_handler(&node_map).await;
        }

        sleep(Duration::from_millis(50)).await;

        for node in nodes {
            node.start().await;
        }

        sleep(Duration::from_millis(100)).await;
    }

    pub async fn stop_node(&self, id: u64) {
        if let Some(node) = self.nodes.lock().await.get(&id) {
            node.stop().await;
            info!("Stopped node {}", id);
        }
    }

    pub async fn wait_for_leader(&self, timeout_dur: Duration) -> Option<NodeInfo> {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout_dur {
            let leaders = self.get_all_leaders().await;
            if !leaders.is_empty() {
                return Some(leaders[0].clone());
            }
            sleep(Duration::from_millis(100)).await;
        }
        None
    }

    pub async fn get_all_leaders(&self) -> Vec<NodeInfo> {
        let mut leaders = Vec::new();
        for (&id, node) in self.nodes.lock().await.iter() {
            if node.is_leader().await {
                if let Some(info) = self.node_infos.get(&id) {
                    leaders.push(info.clone());
                }
            }
        }
        leaders
    }

    pub async fn propose(&self, leader: &NodeInfo, cmd: RaftCommand) -> Result<u64, String> {
        if let Some(node) = self.nodes.lock().await.get(&leader.id) {
            node.propose(cmd).await
        } else {
            Err(format!("Node {} not found", leader.id))
        }
    }

    pub async fn propose_to(&self, address: &str, cmd: RaftCommand) -> Result<u64, String> {
        for node in self.nodes.lock().await.values() {
            if node.address() == address {
                return node.propose(cmd).await;
            }
        }
        Err("Node not found".to_string())
    }

    pub async fn get_all_last_indices(&self) -> HashMap<u64, u64> {
        let mut indices = HashMap::new();
        for (&id, node) in self.nodes.lock().await.iter() {
            indices.insert(id, node.last_index().await);
        }
        indices
    }

    pub async fn get_all_applied_indices(&self) -> HashMap<u64, u64> {
        let mut indices = HashMap::new();
        for (&id, node) in self.nodes.lock().await.iter() {
            indices.insert(id, node.applied_index().await);
        }
        indices
    }

    pub async fn get_snapshot_info(&self, leader: &NodeInfo) -> SnapshotInfo {
        if let Some(node) = self.nodes.lock().await.get(&leader.id) {
            node.get_snapshot().await
        } else {
            SnapshotInfo { index: 0, term: 0 }
        }
    }

    pub async fn shutdown(&self) {
        for (_, node) in self.nodes.lock().await.drain() {
            node.stop().await;
        }
    }
}

#[derive(Clone)]
pub struct RaftTestNode {
    id: u64,
    address: String,
    node: Arc<Mutex<Option<RaftNode>>>,
    started: Arc<tokio::sync::RwLock<bool>>,
    step_tx: Option<tokio::sync::mpsc::Sender<RaftMessage>>,
    peer_routes: Option<HashMap<u64, tokio::sync::mpsc::Sender<RaftMessage>>>,
}

impl RaftTestNode {
    pub fn new(id: u64, address: String, node: RaftNode) -> Self {
        Self {
            id,
            address,
            node: Arc::new(Mutex::new(Some(node))),
            started: Arc::new(tokio::sync::RwLock::new(false)),
            step_tx: None,
            peer_routes: None,
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub async fn register_message_handler(&mut self, nodes: &HashMap<u64, RaftTestNode>) {
        let step_tx = {
            let node_guard = self.node.lock().await;
            node_guard.as_ref().map(|node| node.get_step_tx())
        };

        if let Some(step_tx) = step_tx {
            self.step_tx = Some(step_tx.clone());

            let mut peer_routes = HashMap::new();
            for (&id, peer) in nodes.iter() {
                if id != self.id {
                    if let Some(peer_step_tx) = peer.step_tx.as_ref() {
                        peer_routes.insert(id, peer_step_tx.clone());
                    }
                }
            }
            self.peer_routes = Some(peer_routes);
        }
    }

    pub async fn start(&self) {
        if *self.started.read().await {
            return;
        }

        let msg_rx = {
            let mut node_guard = self.node.lock().await;
            node_guard.as_mut().map(|node| node.take_message_rx())
        };

        if let Some(mut msg_rx) = msg_rx {
            let peer_routes_clone = self.peer_routes.clone();
            tokio::spawn(async move {
                if let Some(peer_routes) = peer_routes_clone {
                    while let Ok(msg) = msg_rx.recv().await {
                        if let Some(sender) = peer_routes.get(&msg.to_id) {
                            if let Ok(raft_msg) = RaftMessage::parse_from_bytes(&msg.message) {
                                let _ = sender.send(raft_msg).await;
                            }
                        }
                    }
                }
            });
        }

        let node_clone = self.node.clone();
        *self.started.write().await = true;

        tokio::spawn(async move {
            let mut node_guard = node_clone.lock().await;
            if let Some(node) = node_guard.as_mut() {
                let _ = node.run().await;
            }
        });
    }

    #[allow(dead_code)]
    async fn message_router(
        mut msg_rx: tokio::sync::mpsc::Receiver<OutgoingMessage>,
        msg_routes: HashMap<u64, tokio::sync::mpsc::Sender<RaftMessage>>,
    ) {
        while let Some(msg) = msg_rx.recv().await {
            if let Some(sender) = msg_routes.get(&msg.to_id) {
                if let Ok(raft_msg) = RaftMessage::parse_from_bytes(&msg.message) {
                    let _ = sender.send(raft_msg).await;
                }
            }
        }
    }

    pub async fn stop(&self) {
        *self.started.write().await = false;
    }

    pub async fn is_leader(&self) -> bool {
        let node_guard = self.node.lock().await;
        if let Some(node) = node_guard.as_ref() {
            node.is_leader()
        } else {
            false
        }
    }

    pub async fn propose(&self, cmd: RaftCommand) -> Result<u64, String> {
        let node_guard = self.node.lock().await;
        if let Some(node) = node_guard.as_ref() {
            let data = cmd.serialize();
            node.propose(data).await
        } else {
            Err("Node not running".to_string())
        }
    }

    pub async fn last_index(&self) -> u64 {
        let node_guard = self.node.lock().await;
        if let Some(node) = node_guard.as_ref() {
            node.last_index()
        } else {
            0
        }
    }

    pub async fn applied_index(&self) -> u64 {
        let node_guard = self.node.lock().await;
        if let Some(node) = node_guard.as_ref() {
            node.applied_index()
        } else {
            0
        }
    }

    pub async fn get_snapshot(&self) -> SnapshotInfo {
        let node_guard = self.node.lock().await;
        if let Some(node) = node_guard.as_ref() {
            let info = node.get_cluster_info();
            SnapshotInfo {
                index: info.last_applied,
                term: info.term,
            }
        } else {
            SnapshotInfo { index: 0, term: 0 }
        }
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotInfo {
    pub index: u64,
    pub term: u64,
}

#[derive(Default)]
pub struct ClusterBuilder {
    num_nodes: u32,
}

impl ClusterBuilder {
    pub fn num_nodes(mut self, n: u32) -> Self {
        self.num_nodes = n;
        self
    }

    pub async fn build(self) -> RaftTestCluster {
        let num_nodes = if self.num_nodes == 0 {
            3
        } else {
            self.num_nodes
        };

        let mut nodes = HashMap::new();
        let mut node_infos = HashMap::new();
        let mut temp_dirs = Vec::new();

        for i in 1..=num_nodes {
            let id = i as u64;
            let address = format!("127.0.0.1:{}", 10000 + i);

            let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
            let db_path = temp_dir.path().join(format!("raft_{}", id));
            std::fs::create_dir_all(&db_path).expect("Failed to create db dir");

            let mut peers = Vec::new();
            for j in 1..=num_nodes {
                if j != i {
                    peers.push(Peer {
                        id: j as u64,
                        address: format!("127.0.0.1:{}", 10000 + j),
                    });
                }
            }

            let node = RaftNode::new(id, address.clone(), peers, db_path.to_str().unwrap())
                .expect("Failed to create Raft node");

            let test_node = RaftTestNode::new(id, address.clone(), node);

            nodes.insert(id, test_node);
            node_infos.insert(id, NodeInfo { id, address });
            temp_dirs.push(Arc::new(temp_dir));
        }

        RaftTestCluster {
            nodes: Arc::new(Mutex::new(nodes)),
            node_infos,
            temp_dirs,
        }
    }
}
