//! Static cluster configuration (Module 1).
//!
//! Loaded at startup, changes require the management API.

use serde::{Deserialize, Serialize};

/// Cluster static config — merged from config file + master registration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterStaticConfig {
    pub zones: Vec<ZoneConfig>,
    pub shard_count: usize,
    pub inode_per_shard: u64,
    pub placement: PlacementPolicyConfig,
    pub rebalance: RebalancePolicy,
    pub migration: MigrationPolicy,
    pub volume_default_size: u64,
}

impl Default for ClusterStaticConfig {
    fn default() -> Self {
        Self {
            zones: Vec::new(),
            shard_count: 3,
            inode_per_shard: 1_000_000,
            placement: PlacementPolicyConfig::default(),
            rebalance: RebalancePolicy::default(),
            migration: MigrationPolicy::default(),
            volume_default_size: 100 * 1024 * 1024 * 1024, // 100GB
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZoneConfig {
    pub zone_id: u32,
    pub name: String,
    pub node_ids: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlacementPolicyConfig {
    /// Strategy name: "round_robin" | "anti_affinity" | "least_loaded"
    pub strategy: String,
    pub rack_aware: bool,
    pub cross_zone_replication: bool,
}

impl Default for PlacementPolicyConfig {
    fn default() -> Self {
        Self {
            strategy: "least_loaded".to_string(),
            rack_aware: false,
            cross_zone_replication: true,
        }
    }
}

/// Migration throttling + load-adaptive policy.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MigrationPolicy {
    /// Max simultaneous migration tasks.
    pub max_concurrent_migrations: u32,
    /// Per-task bandwidth cap (Mbps).
    pub max_bandwidth_mbps: u64,
    /// Cluster avg load_score above this → silently pause all migrations.
    pub load_pause_threshold: f64,
    /// Cluster avg load_score below this → resume paused migrations.
    pub load_resume_threshold: f64,
    /// Min interval between rebalance scans (seconds).
    pub scan_interval_secs: u64,
}

impl Default for MigrationPolicy {
    fn default() -> Self {
        Self {
            max_concurrent_migrations: 4,
            max_bandwidth_mbps: 100,
            load_pause_threshold: 0.7,
            load_resume_threshold: 0.4,
            scan_interval_secs: 60,
        }
    }
}

/// Rebalance trigger thresholds.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RebalancePolicy {
    /// Volume usage above this → trigger cold-data migration.
    pub volume_full_threshold: f64,
    /// Volume usage above this → exclude from allocation (even if idle).
    pub near_full_exclude_ratio: f64,
    /// Node load imbalance (max/min) above this → trigger hot-data migration.
    pub load_imbalance_threshold: f64,
    /// Data not accessed for this many hours → considered cold.
    pub cold_data_threshold_hours: u64,
    /// Don't migrate fewer than this many chunks (avoid thrashing).
    pub min_migration_chunk_count: u32,
}

impl Default for RebalancePolicy {
    fn default() -> Self {
        Self {
            volume_full_threshold: 0.85,
            near_full_exclude_ratio: 0.90,
            load_imbalance_threshold: 2.0,
            cold_data_threshold_hours: 24,
            min_migration_chunk_count: 10,
        }
    }
}
