pub use powerfs_common::event::*;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterMetrics {
    pub node_count: u32,
    pub volume_count: u32,
    pub collection_count: u32,
    pub is_leader: bool,
    pub raft_term: u64,
    pub uptime: u64,
    pub total_storage: u64,
    pub used_storage: u64,
    pub file_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KVMetrics {
    pub session_count: u32,
    pub block_count: u64,
    pub memory_used: u64,
    pub hit_ratio: f64,
    pub eviction_count: u64,
    pub put_count: u64,
    pub get_count: u64,
    pub avg_latency: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertInfo {
    pub id: String,
    pub name: String,
    pub severity: String,
    pub status: String,
    pub source: String,
    pub message: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub resolved_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertRule {
    pub id: String,
    pub name: String,
    pub description: String,
    pub enabled: bool,
    pub severity: String,
    pub condition: AlertCondition,
    pub notifications: Vec<NotificationConfig>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertCondition {
    pub metric: String,
    pub operator: String,
    pub value: f64,
    pub duration: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationConfig {
    pub r#type: String,
    pub url: Option<String>,
    pub to: Option<Vec<String>>,
}