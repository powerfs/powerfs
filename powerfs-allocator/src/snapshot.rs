//! Cluster runtime snapshot (Module 2 — dynamic state).
//!
//! Aggregated by Master from heartbeats (10-30s lag). The allocator receives
//! a `&ClusterSnapshot` reference per allocation call — never queries live state.

use std::collections::HashMap;
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::error::ShardId;

/// Cluster runtime snapshot — Master aggregates all heartbeats into this.
#[derive(Clone, Debug)]
pub struct ClusterSnapshot {
    /// Monotonically increasing; allocators may cache scores keyed by version.
    pub version: u64,
    pub timestamp: Instant,
    /// Links to the static config version for cache invalidation.
    pub config_version: u64,

    pub volumes: Vec<VolumeRuntime>,
    pub nodes: Vec<NodeRuntime>,
    pub shards: Vec<ShardRuntime>,
    /// Cluster-wide average load (0.0-1.0). Used by migration scheduler.
    pub cluster_avg_load: f64,
    /// Volume pins: `volume_id → node_id`. A pinned volume's data must stay on
    /// the pinned node; the LoadBalancer skips pinned volumes as migration
    /// sources. Populated by the master's Raft-replicated pin registry.
    pub pinned_volumes: HashMap<u64, String>,
}

impl ClusterSnapshot {
    /// Find a volume by ID.
    pub fn get_volume(&self, volume_id: u64) -> Option<&VolumeRuntime> {
        self.volumes.iter().find(|v| v.volume_id == volume_id)
    }

    /// Find a node by ID.
    pub fn get_node(&self, node_id: &str) -> Option<&NodeRuntime> {
        self.nodes.iter().find(|n| n.node_id == node_id)
    }

    /// Find a shard by ID.
    pub fn get_shard(&self, shard_id: ShardId) -> Option<&ShardRuntime> {
        self.shards.iter().find(|s| s.shard_id == shard_id.0)
    }

    /// Volumes eligible for allocation (Active state, not in maintenance).
    pub fn allocatable_volumes(&self) -> impl Iterator<Item = &VolumeRuntime> {
        self.volumes
            .iter()
            .filter(|v| v.state == VolumeRuntimeState::Active)
    }
}

/// Extended volume state (superset of `powerfs_common::VolumeState`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum VolumeRuntimeState {
    Active,
    Draining,
    Full,
    Deleted,
}

#[derive(Clone, Debug)]
pub struct VolumeRuntime {
    pub volume_id: u64,
    pub node_id: String,
    pub zone_id: u32,
    pub total_size: u64,
    pub used_size: u64,
    pub state: VolumeRuntimeState,
    pub load: VolumeLoad,
    /// Cold needle count (reported by volume server).
    pub cold_needle_count: u64,
    /// Hot needle count (reported by volume server).
    pub hot_needle_count: u64,
}

impl VolumeRuntime {
    /// Usage ratio (0.0 - 1.0).
    pub fn usage_ratio(&self) -> f64 {
        if self.total_size == 0 {
            return 1.0;
        }
        self.used_size as f64 / self.total_size as f64
    }

    /// Free space ratio (0.0 - 1.0).
    pub fn space_ratio(&self) -> f64 {
        1.0 - self.usage_ratio()
    }
}

#[derive(Clone, Debug, Default)]
pub struct VolumeLoad {
    pub iops: u64,
    pub bandwidth_mbps: u64,
    pub write_latency_p99_us: u64,
    pub active_connections: u32,
}

/// Extended node state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum NodeRuntimeState {
    #[default]
    Healthy,
    Degraded,
    Maintenance,
    Down,
}

#[derive(Clone, Debug)]
pub struct NodeRuntime {
    pub node_id: String,
    pub state: NodeRuntimeState,
    pub cpu_usage: f32,
    pub memory_usage: f32,
    pub disk_usage: f32,
    /// Composite load score 0.0 (idle) - 1.0 (saturated).
    pub load_score: f64,
    pub in_maintenance: bool,
}

impl NodeRuntime {
    /// Is this node available for new allocations?
    pub fn is_allocatable(&self) -> bool {
        self.state == NodeRuntimeState::Healthy && !self.in_maintenance
    }
}

#[derive(Clone, Debug)]
pub struct ShardRuntime {
    pub shard_id: u64,
    pub leader_node: String,
    pub follower_nodes: Vec<String>,
    pub qps: u64,
    pub raft_backlog: u32,
    pub open_inode_count: u64,
    /// Active lease count (used for migration avoidance).
    pub active_lease_count: u64,
}
