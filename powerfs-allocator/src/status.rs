//! Status query trait (Module 5 — read-only, for monitoring & AI Agent).
//!
//! Provides cluster overview, load distribution, allocation stats, and
//! migration task status. Does NOT touch the allocation path.

use crate::config::ClusterStaticConfig;

/// Read-only status query — for monitor dashboards, management UI, AI Agent.
pub trait StatusQuery: Send + Sync {
    /// Cluster-wide overview (top-level dashboard).
    fn cluster_overview(&self) -> ClusterOverview;

    /// Per-node load distribution (detect hot/cold imbalance).
    fn node_load_distribution(&self) -> Vec<NodeLoadReport>;

    /// Per-volume usage details (capacity planning).
    fn volume_details(&self) -> Vec<VolumeDetail>;

    /// Allocation statistics (strategy effectiveness).
    fn allocation_stats(&self) -> AllocationStats;

    /// Migration task status (progress tracking).
    fn migration_tasks(&self) -> Vec<MigrationTaskStatus>;

    /// Current effective configuration.
    fn current_config(&self) -> ClusterStaticConfig;
}

#[derive(Clone, Debug)]
pub struct ClusterOverview {
    pub total_capacity: u64,
    pub used_capacity: u64,
    pub free_capacity: u64,
    pub node_count: u32,
    pub healthy_nodes: u32,
    pub volume_count: u32,
    pub avg_load: f64,
    pub max_load: f64,
    pub min_load: f64,
    /// max/min load ratio; > 2.0 indicates imbalance.
    pub imbalance_ratio: f64,
}

#[derive(Clone, Debug)]
pub struct NodeLoadReport {
    pub node_id: String,
    pub zone_id: u32,
    pub load_score: f64,
    pub cpu: f32,
    pub disk: f32,
    pub volume_count: u32,
    pub active_migrations: u32,
}

#[derive(Clone, Debug)]
pub struct VolumeDetail {
    pub volume_id: u64,
    pub node_id: String,
    pub zone_id: u32,
    pub total_size: u64,
    pub used_size: u64,
    pub usage_ratio: f64,
    pub state: String,
    pub cold_needle_count: u64,
    pub hot_needle_count: u64,
}

#[derive(Clone, Debug, Default)]
pub struct AllocationStats {
    pub total_requests: u64,
    pub successful: u64,
    pub failed: u64,
    pub no_space_count: u64,
    pub avg_decision_latency_us: u64,
}

/// Migration task state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MigrationState {
    Pending,
    Running,
    /// Silently paused due to high cluster load.
    PausedByLoad,
    Completed,
    Failed,
}

/// Migration type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MigrationType {
    ColdData,
    HotData,
    VolumeGrow,
}

#[derive(Clone, Debug)]
pub struct MigrationTaskStatus {
    pub task_id: String,
    pub action_type: MigrationType,
    pub state: MigrationState,
    /// 0.0 - 1.0
    pub progress: f32,
    pub bytes_migrated: u64,
    pub bytes_total: u64,
    pub started_at: std::time::Instant,
    /// Only set when state == PausedByLoad.
    pub pause_reason: Option<String>,
}
