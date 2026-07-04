use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::sync::RwLock;

use crate::event::{ClusterMetrics, KVMetrics, NodeStatusEvent, VolumeStatusEvent};

#[derive(Debug, Clone, Serialize)]
pub struct NodeInfo {
    pub id: String,
    pub address: String,
    pub grpc_port: u32,
    pub http_port: u32,
    pub status: String,
    pub cpu_usage: f64,
    pub mem_usage: f64,
    pub disk_usage: f64,
    pub network_rx: u64,
    pub network_tx: u64,
    pub uptime: u64,
    pub volume_count: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct VolumeInfo {
    pub id: u32,
    pub node_id: String,
    pub size: u64,
    pub used: u64,
    pub file_count: u64,
    pub status: String,
    pub collection: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct KVSessionInfo {
    pub id: String,
    pub model_name: String,
    pub layer_count: u32,
    pub block_count: u64,
    pub memory_used: u64,
    pub hit_ratio: f64,
    pub eviction_count: u64,
    pub created_at: String,
}

pub struct MetricStore {
    nodes: RwLock<HashMap<String, NodeInfo>>,
    volumes: RwLock<HashMap<u32, VolumeInfo>>,
    kv_sessions: RwLock<HashMap<String, KVSessionInfo>>,
    cluster_metrics: RwLock<ClusterMetrics>,
    kv_metrics: RwLock<KVMetrics>,
    collection_names: RwLock<HashSet<String>>,
    start_time: Instant,
}

impl MetricStore {
    pub fn new() -> Self {
        Self {
            nodes: RwLock::new(HashMap::new()),
            volumes: RwLock::new(HashMap::new()),
            kv_sessions: RwLock::new(HashMap::new()),
            cluster_metrics: RwLock::new(ClusterMetrics {
                node_count: 0,
                volume_count: 0,
                collection_count: 0,
                is_leader: false,
                raft_term: 0,
                uptime: 0,
                total_storage: 0,
                used_storage: 0,
                file_count: 0,
            }),
            kv_metrics: RwLock::new(KVMetrics {
                session_count: 0,
                block_count: 0,
                memory_used: 0,
                hit_ratio: 0.0,
                eviction_count: 0,
                put_count: 0,
                get_count: 0,
                avg_latency: 0.0,
            }),
            collection_names: RwLock::new(HashSet::new()),
            start_time: Instant::now(),
        }
    }

    pub async fn update_node(&self, event: NodeStatusEvent) {
        let mut nodes = self.nodes.write().await;
        nodes.insert(
            event.node_id.clone(),
            NodeInfo {
                id: event.node_id,
                address: event.address,
                grpc_port: event.grpc_port,
                http_port: event.http_port,
                status: event.status,
                cpu_usage: event.cpu_usage,
                mem_usage: event.mem_usage,
                disk_usage: event.disk_usage,
                network_rx: event.network_rx,
                network_tx: event.network_tx,
                uptime: event.uptime,
                volume_count: event.volume_count,
            },
        );
        self.update_cluster_metrics().await;
    }

    pub async fn update_volume(&self, event: VolumeStatusEvent) {
        let mut volumes = self.volumes.write().await;
        volumes.insert(
            event.volume_id,
            VolumeInfo {
                id: event.volume_id,
                node_id: event.node_id,
                size: event.size,
                used: event.used,
                file_count: event.file_count,
                status: event.status,
                collection: event.collection.clone(),
                created_at: chrono::Utc::now().to_rfc3339(),
            },
        );

        let mut collections = self.collection_names.write().await;
        collections.insert(event.collection);

        self.update_cluster_metrics().await;
    }

    pub async fn update_kv_session(&self, event: crate::event::KVSessionEvent) {
        let mut sessions = self.kv_sessions.write().await;
        match event.event_type.as_str() {
            "create" | "update" => {
                sessions.insert(
                    event.session_id.clone(),
                    KVSessionInfo {
                        id: event.session_id,
                        model_name: event.model_name,
                        layer_count: event.layer_count,
                        block_count: event.block_count,
                        memory_used: event.memory_used,
                        hit_ratio: event.hit_ratio,
                        eviction_count: event.eviction_count,
                        created_at: chrono::Utc::now().to_rfc3339(),
                    },
                );
            }
            "delete" => {
                sessions.remove(&event.session_id);
            }
            _ => {}
        }
        self.update_kv_metrics().await;
    }

    pub async fn update_cluster_metrics(&self) {
        let nodes = self.nodes.read().await;
        let volumes = self.volumes.read().await;
        let collections = self.collection_names.read().await;

        let mut total_storage: u64 = 0;
        let mut used_storage: u64 = 0;
        let mut file_count: u64 = 0;

        for volume in volumes.values() {
            total_storage += volume.size;
            used_storage += volume.used;
            file_count += volume.file_count;
        }

        let mut metrics = self.cluster_metrics.write().await;
        *metrics = ClusterMetrics {
            node_count: nodes.len() as u32,
            volume_count: volumes.len() as u32,
            collection_count: collections.len() as u32,
            is_leader: metrics.is_leader,
            raft_term: metrics.raft_term,
            uptime: self.start_time.elapsed().as_secs(),
            total_storage,
            used_storage,
            file_count,
        };
    }

    pub async fn update_kv_metrics(&self) {
        let sessions = self.kv_sessions.read().await;

        let mut block_count: u64 = 0;
        let mut memory_used: u64 = 0;
        let mut total_hit_ratio: f64 = 0.0;
        let mut eviction_count: u64 = 0;

        for session in sessions.values() {
            block_count += session.block_count;
            memory_used += session.memory_used;
            total_hit_ratio += session.hit_ratio;
            eviction_count += session.eviction_count;
        }

        let avg_hit_ratio = if sessions.is_empty() { 0.0 } else { total_hit_ratio / sessions.len() as f64 };

        let mut metrics = self.kv_metrics.write().await;
        *metrics = KVMetrics {
            session_count: sessions.len() as u32,
            block_count,
            memory_used,
            hit_ratio: avg_hit_ratio,
            eviction_count,
            put_count: metrics.put_count,
            get_count: metrics.get_count,
            avg_latency: metrics.avg_latency,
        };
    }

    pub async fn get_nodes(&self) -> Vec<NodeInfo> {
        self.nodes.read().await.values().cloned().collect()
    }

    pub async fn get_node(&self, id: &str) -> Option<NodeInfo> {
        self.nodes.read().await.get(id).cloned()
    }

    pub async fn get_volumes(&self) -> Vec<VolumeInfo> {
        self.volumes.read().await.values().cloned().collect()
    }

    pub async fn get_volume(&self, id: u32) -> Option<VolumeInfo> {
        self.volumes.read().await.get(&id).cloned()
    }

    pub async fn get_kv_sessions(&self) -> Vec<KVSessionInfo> {
        self.kv_sessions.read().await.values().cloned().collect()
    }

    pub async fn get_kv_session(&self, id: &str) -> Option<KVSessionInfo> {
        self.kv_sessions.read().await.get(id).cloned()
    }

    pub async fn get_cluster_metrics(&self) -> ClusterMetrics {
        self.cluster_metrics.read().await.clone()
    }

    pub async fn get_kv_metrics(&self) -> KVMetrics {
        self.kv_metrics.read().await.clone()
    }

    pub async fn set_leader_info(&self, is_leader: bool, raft_term: u64) {
        let mut metrics = self.cluster_metrics.write().await;
        metrics.is_leader = is_leader;
        metrics.raft_term = raft_term;
    }

    pub async fn increment_kv_put(&self) {
        let mut metrics = self.kv_metrics.write().await;
        metrics.put_count += 1;
    }

    pub async fn increment_kv_get(&self) {
        let mut metrics = self.kv_metrics.write().await;
        metrics.get_count += 1;
    }
}

pub type MetricStoreRef = Arc<MetricStore>;