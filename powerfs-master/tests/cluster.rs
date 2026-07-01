//! Raft test cluster infrastructure
//!
//! Provides utilities for creating and managing a test Raft cluster
//! with multiple nodes for integration testing.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use log::{error, info};
use tempfile::TempDir;
use tokio::sync::{mpsc, RwLock};
use tokio::time::{sleep, timeout};

use powerfs_master::raft_node::{Peer, RaftNode};
use powerfs_master::raft_storage::{RaftCommand, RocksDbStorage};

/// Information about a node in the cluster
#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub id: u64,
    pub address: String,
}

/// A Raft test cluster
pub struct RaftTestCluster {
    /// All nodes in the cluster
    pub nodes: Arc<RwLock<HashMap<u64, RaftTestNode>>>,
    /// Node infos (addresses)
    node_infos: HashMap<u64, NodeInfo>,
    /// Temp directories for each node
    temp_dirs: Vec<Arc<TempDir>>,
    /// Base port for the cluster
    base_port: u32,
    /// Tick interval in milliseconds
    tick_ms: u64,
    /// Election timeout in milliseconds
    election_timeout_ms: u64,
}

impl RaftTestCluster {
    /// Create a new cluster with default settings
    pub async fn new(num_nodes: u32) -> Self {
        Self::builder()
            .num_nodes(num_nodes)
            .build()
            .await
    }

    /// Create a cluster builder
    pub fn builder() -> ClusterBuilder {
        ClusterBuilder::default()
    }

    /// Start all nodes in the cluster
    pub async fn start_all(&self) {
        let nodes: Vec<_> = self.nodes.read().await.values().cloned().collect();

        for node in nodes {
            node.start().await;
        }

        // Give nodes time to connect and elect leader
        sleep(Duration::from_millis(100)).await;
    }

    /// Stop a specific node
    pub async fn stop_node(&self, id: u64) {
        if let Some(node) = self.nodes.read().await.get(&id) {
            node.stop().await;
            info!("Stopped node {}", id);
        }
    }

    /// Wait for a leader to be elected
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

    /// Get all current leaders
    pub async fn get_all_leaders(&self) -> Vec<NodeInfo> {
        let mut leaders = Vec::new();

        for (&id, node) in self.nodes.read().await.iter() {
            let is_leader = node.is_leader().await;
            if is_leader {
                if let Some(info) = self.node_infos.get(&id) {
                    leaders.push(info.clone());
                }
            }
        }

        leaders
    }

    /// Propose a command to the cluster via the leader
    pub async fn propose(&self, leader: &NodeInfo, cmd: RaftCommand) -> Result<u64, String> {
        if let Some(node) = self.nodes.read().await.get(&leader.id) {
            node.propose(cmd).await
        } else {
            Err(format!("Node {} not found", leader.id))
        }
    }

    /// Propose a command to a specific node (for follower testing)
    pub async fn propose_to(&self, address: &str, cmd: RaftCommand) -> Result<u64, String> {
        // Find node by address
        for node in self.nodes.read().await.values() {
            if node.address() == address {
                return node.propose(cmd).await;
            }
        }
        Err("Node not found".to_string())
    }

    /// Get the last committed index for all nodes
    pub async fn get_all_last_indices(&self) -> HashMap<u64, u64> {
        let mut indices = HashMap::new();

        for (&id, node) in self.nodes.read().await.iter() {
            indices.insert(id, node.last_index().await);
        }

        indices
    }

    /// Get the applied index for all nodes
    pub async fn get_all_applied_indices(&self) -> HashMap<u64, u64> {
        let mut indices = HashMap::new();

        for (&id, node) in self.nodes.read().await.iter() {
            indices.insert(id, node.applied_index().await);
        }

        indices
    }

    /// Get snapshot info for a node
    pub async fn get_snapshot_info(&self, leader: &NodeInfo) -> SnapshotInfo {
        if let Some(node) = self.nodes.read().await.get(&leader.id) {
            node.get_snapshot().await
        } else {
            SnapshotInfo { index: 0, term: 0 }
        }
    }

    /// Shutdown the entire cluster
    pub async fn shutdown(&self) {
        for (_, node) in self.nodes.write().await.drain() {
            node.stop().await;
        }
        drop(self.temp_dirs.drain(..).collect::<Vec<_>>());
    }
}

impl Drop for RaftTestCluster {
    fn drop(&mut self) {
        // Ensure cleanup happens
    }
}

/// A single test node
#[derive(Clone)]
pub struct RaftTestNode {
    id: u64,
    address: String,
    node: Arc<RwLock<Option<RaftNode>>>,
    running: Arc<RwLock<bool>>,
}

impl RaftTestNode {
    pub fn new(id: u64, address: String, node: RaftNode) -> Self {
        Self {
            id,
            address,
            node: Arc::new(RwLock::new(Some(node))),
            running: Arc::new(RwLock::new(false)),
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub async fn start(&self) {
        *self.running.write().await = true;
        let node_guard = self.node.read().await;
        if let Some(mut node) = node_guard.as_ref() {
            let running = self.running.clone();
            tokio::spawn(async move {
                node.run().await.ok();
            });
        }
    }

    pub async fn stop(&self) {
        *self.running.write().await = false;
    }

    pub async fn is_leader(&self) -> bool {
        let node_guard = self.node.read().await;
        if let Some(node) = node_guard.as_ref() {
            node.is_leader()
        } else {
            false
        }
    }

    pub async fn propose(&self, cmd: RaftCommand) -> Result<u64, String> {
        let node_guard = self.node.read().await;
        if let Some(node) = node_guard.as_ref() {
            let data = cmd.serialize();
            node.propose(data).await
        } else {
            Err("Node not running".to_string())
        }
    }

    pub async fn last_index(&self) -> u64 {
        let node_guard = self.node.read().await;
        if let Some(node) = node_guard.as_ref() {
            node.last_index().unwrap_or(0)
        } else {
            0
        }
    }

    pub async fn applied_index(&self) -> u64 {
        let node_guard = self.node.read().await;
        if let Some(node) = node_guard.as_ref() {
            node.applied_index()
        } else {
            0
        }
    }

    pub async fn get_snapshot(&self) -> SnapshotInfo {
        let node_guard = self.node.read().await;
        if let Some(node) = node_guard.as_ref() {
            let info = node.get_cluster_info();
            SnapshotInfo {
                index: info.node_id, // Use node_id as placeholder
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

/// Builder for RaftTestCluster
#[derive(Default)]
pub struct ClusterBuilder {
    num_nodes: u32,
    base_port: u32,
    tick_ms: u64,
    election_timeout_ms: u64,
}

impl ClusterBuilder {
    pub fn num_nodes(mut self, n: u32) -> Self {
        self.num_nodes = n;
        self
    }

    pub fn base_port(mut self, port: u32) -> Self {
        self.base_port = port;
        self
    }

    pub fn tick_ms(mut self, ms: u64) -> Self {
        self.tick_ms = ms;
        self
    }

    pub fn election_timeout_ms(mut self, ms: u64) -> Self {
        self.election_timeout_ms = ms;
        self
    }

    pub async fn build(self) -> RaftTestCluster {
        let num_nodes = if self.num_nodes == 0 { 3 } else { self.num_nodes };
        let base_port = if self.base_port == 0 { 10000 } else { self.base_port };
        let tick_ms = if self.tick_ms == 0 { 100 } else { self.tick_ms };
        let election_timeout_ms = if self.election_timeout_ms == 0 { 300 } else { self.election_timeout_ms };

        let mut nodes = HashMap::new();
        let mut node_infos = HashMap::new();
        let mut temp_dirs = Vec::new();

        for i in 1..=num_nodes {
            let id = i as u64;
            let address = format!("127.0.0.1:{}", base_port + i);

            // Create temp directory for RocksDB
            let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
            let db_path = temp_dir.path().join(format!("raft_{}", id));
            std::fs::create_dir_all(&db_path).expect("Failed to create db dir");

            // Create peers (all nodes in cluster)
            let mut peers = Vec::new();
            for j in 1..=num_nodes {
                if j != i {
                    peers.push(Peer {
                        id: j as u64,
                        address: format!("127.0.0.1:{}", base_port + j),
                    });
                }
            }

            // Create Raft node
            let storage = RocksDbStorage::new(db_path.to_str().unwrap())
                .expect("Failed to create storage");

            let node = RaftNode::new(
                id,
                address.clone(),
                peers,
                db_path.to_str().unwrap(),
            ).expect("Failed to create Raft node");

            let test_node = RaftTestNode::new(id, address.clone(), node);

            nodes.insert(id, test_node.clone());
            node_infos.insert(id, NodeInfo { id, address });
            temp_dirs.push(Arc::new(temp_dir));
        }

        RaftTestCluster {
            nodes: Arc::new(RwLock::new(nodes)),
            node_infos,
            temp_dirs,
            base_port,
            tick_ms,
            election_timeout_ms,
        }
    }
}
