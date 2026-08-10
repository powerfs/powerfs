//! Status query trait (Module 5 — read-only, for monitoring & AI Agent).
//!
//! Provides cluster overview, load distribution, allocation stats, and
//! migration task status. Does NOT touch the allocation path.
//!
//! The concrete [`SnapshotStatusQuery`] implementation derives all views from
//! a [`ClusterSnapshot`] + [`ClusterStaticConfig`] held behind shared state.
//! The Master builds the snapshot from existing heartbeat data (enriched by
//! P5 load metrics later); until then, load fields default to zero and the
//! space/capacity/health views are fully functional.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::config::ClusterStaticConfig;
use crate::error::AllocError;
use crate::snapshot::{ClusterSnapshot, NodeRuntimeState, VolumeRuntimeState};

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

// ============================================================================
// Concrete implementation: SnapshotStatusQuery
// ============================================================================

/// Atomic allocation statistics collector.
///
/// Owned by the service (Master/Filer) and fed into [`SnapshotStatusQuery`].
/// Counters are lock-free; the avg-latency is derived from a running sum +
/// count pair. Reset is not provided — stats are monotonic over process life.
#[derive(Debug, Default)]
pub struct AllocationStatsCollector {
    total_requests: AtomicU64,
    successful: AtomicU64,
    failed: AtomicU64,
    no_space_count: AtomicU64,
    latency_sum_us: AtomicU64,
    latency_count: AtomicU64,
}

impl AllocationStatsCollector {
    /// Create a zeroed collector.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a successful allocation with the given decision latency.
    pub fn record_success(&self, latency_us: u64) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.successful.fetch_add(1, Ordering::Relaxed);
        self.latency_sum_us.fetch_add(latency_us, Ordering::Relaxed);
        self.latency_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failed allocation. `NoSpace` failures are tracked separately.
    pub fn record_failure(&self, err: &AllocError, latency_us: u64) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.failed.fetch_add(1, Ordering::Relaxed);
        if matches!(err, AllocError::NoSpace) {
            self.no_space_count.fetch_add(1, Ordering::Relaxed);
        }
        self.latency_sum_us.fetch_add(latency_us, Ordering::Relaxed);
        self.latency_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Snapshot the current counters into an [`AllocationStats`].
    pub fn snapshot(&self) -> AllocationStats {
        let total = self.total_requests.load(Ordering::Relaxed);
        let sum = self.latency_sum_us.load(Ordering::Relaxed);
        let count = self.latency_count.load(Ordering::Relaxed);
        let avg = sum.checked_div(count).unwrap_or(0);
        AllocationStats {
            total_requests: total,
            successful: self.successful.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            no_space_count: self.no_space_count.load(Ordering::Relaxed),
            avg_decision_latency_us: avg,
        }
    }
}

/// Concrete [`StatusQuery`] backed by a shared snapshot + config.
///
/// All views are derived read-only from the shared state:
/// - `snapshot`: `Arc<RwLock<ClusterSnapshot>>` — updated by the Master's
///   heartbeat aggregator (P5 enriches the load fields).
/// - `config`: `Arc<RwLock<ClusterStaticConfig>>` — the effective policy.
/// - `stats`: `Arc<AllocationStatsCollector>` — lock-free allocation counters.
/// - `migration_tasks`: `Arc<RwLock<Vec<MigrationTaskStatus>>>` — empty until
///   P7 (LoadBalancer + MigrationScheduler) lands; the field exists so the
///   interface is complete and monitor-ready.
///
/// This struct is cheap to clone (all `Arc`); the typical pattern is one
/// shared instance per service, cloned into handler tasks.
#[derive(Clone)]
pub struct SnapshotStatusQuery {
    snapshot: Arc<RwLock<ClusterSnapshot>>,
    config: Arc<RwLock<ClusterStaticConfig>>,
    stats: Arc<AllocationStatsCollector>,
    migration_tasks: Arc<RwLock<Vec<MigrationTaskStatus>>>,
}

impl SnapshotStatusQuery {
    /// Create a new query handle over the given shared state.
    pub fn new(
        snapshot: Arc<RwLock<ClusterSnapshot>>,
        config: Arc<RwLock<ClusterStaticConfig>>,
        stats: Arc<AllocationStatsCollector>,
    ) -> Self {
        Self {
            snapshot,
            config,
            stats,
            migration_tasks: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Attach a shared migration-task list (used by P7 MigrationScheduler).
    /// Until P7, this stays empty and `migration_tasks()` returns `vec![]`.
    pub fn with_migration_tasks(mut self, tasks: Arc<RwLock<Vec<MigrationTaskStatus>>>) -> Self {
        self.migration_tasks = tasks;
        self
    }
}

impl StatusQuery for SnapshotStatusQuery {
    fn cluster_overview(&self) -> ClusterOverview {
        let snap = self.snapshot.read().unwrap();

        let total_capacity: u64 = snap.volumes.iter().map(|v| v.total_size).sum();
        let used_capacity: u64 = snap.volumes.iter().map(|v| v.used_size).sum();
        let free_capacity = total_capacity.saturating_sub(used_capacity);

        let node_count = snap.nodes.len() as u32;
        let healthy_nodes = snap
            .nodes
            .iter()
            .filter(|n| n.state == NodeRuntimeState::Healthy && !n.in_maintenance)
            .count() as u32;
        let volume_count = snap.volumes.len() as u32;

        // Load aggregation. Empty node lists are degenerate (avoid div-by-zero).
        let loads: Vec<f64> = snap.nodes.iter().map(|n| n.load_score).collect();
        let (avg_load, max_load, min_load, imbalance_ratio) = if loads.is_empty() {
            (0.0, 0.0, 0.0, 1.0)
        } else {
            let sum: f64 = loads.iter().sum();
            let avg = sum / loads.len() as f64;
            let max = loads.iter().cloned().fold(0.0f64, f64::max);
            let min = loads.iter().cloned().fold(f64::MAX, f64::min);
            // imbalance = max/min; guard against min==0
            let ratio = if min > 1e-9 { max / min } else { f64::INFINITY };
            (avg, max, min, ratio)
        };

        ClusterOverview {
            total_capacity,
            used_capacity,
            free_capacity,
            node_count,
            healthy_nodes,
            volume_count,
            avg_load,
            max_load,
            min_load,
            imbalance_ratio,
        }
    }

    fn node_load_distribution(&self) -> Vec<NodeLoadReport> {
        let snap = self.snapshot.read().unwrap();
        // Map node_id → volume_count by scanning volumes.
        let mut per_node_volumes: std::collections::HashMap<String, u32> =
            std::collections::HashMap::new();
        for v in &snap.volumes {
            *per_node_volumes.entry(v.node_id.clone()).or_insert(0) += 1;
        }

        let zone_lookup = build_node_zone_lookup(&snap);

        snap.nodes
            .iter()
            .map(|n| NodeLoadReport {
                node_id: n.node_id.clone(),
                zone_id: zone_lookup.get(&n.node_id).copied().unwrap_or(0),
                load_score: n.load_score,
                cpu: n.cpu_usage,
                disk: n.disk_usage,
                volume_count: per_node_volumes.get(&n.node_id).copied().unwrap_or(0),
                active_migrations: 0,
            })
            .collect()
    }

    fn volume_details(&self) -> Vec<VolumeDetail> {
        let snap = self.snapshot.read().unwrap();
        snap.volumes
            .iter()
            .map(|v| VolumeDetail {
                volume_id: v.volume_id,
                node_id: v.node_id.clone(),
                zone_id: v.zone_id,
                total_size: v.total_size,
                used_size: v.used_size,
                usage_ratio: v.usage_ratio(),
                state: volume_state_label(&v.state),
                cold_needle_count: v.cold_needle_count,
                hot_needle_count: v.hot_needle_count,
            })
            .collect()
    }

    fn allocation_stats(&self) -> AllocationStats {
        self.stats.snapshot()
    }

    fn migration_tasks(&self) -> Vec<MigrationTaskStatus> {
        self.migration_tasks.read().unwrap().clone()
    }

    fn current_config(&self) -> ClusterStaticConfig {
        self.config.read().unwrap().clone()
    }
}

/// Map node_id → zone_id by scanning volumes (a volume's node_id maps to
/// its zone_id). Nodes with no volumes default to zone 0.
fn build_node_zone_lookup(snap: &ClusterSnapshot) -> std::collections::HashMap<String, u32> {
    let mut map = std::collections::HashMap::new();
    for v in &snap.volumes {
        map.entry(v.node_id.clone()).or_insert(v.zone_id);
    }
    map
}

/// Human-readable label for a volume runtime state.
fn volume_state_label(state: &VolumeRuntimeState) -> String {
    match state {
        VolumeRuntimeState::Active => "Active".to_string(),
        VolumeRuntimeState::Draining => "Draining".to_string(),
        VolumeRuntimeState::Full => "Full".to_string(),
        VolumeRuntimeState::Deleted => "Deleted".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClusterStaticConfig;
    use crate::error::ShardId;
    use crate::snapshot::{
        ClusterSnapshot, NodeRuntime, NodeRuntimeState, ShardRuntime, VolumeLoad, VolumeRuntime,
        VolumeRuntimeState,
    };
    use std::time::Instant;

    fn make_node(id: &str, load: f64, state: NodeRuntimeState) -> NodeRuntime {
        NodeRuntime {
            node_id: id.to_string(),
            state,
            cpu_usage: 0.0,
            memory_usage: 0.0,
            disk_usage: 0.0,
            load_score: load,
            in_maintenance: false,
        }
    }

    fn make_volume(
        id: u64,
        node: &str,
        zone: u32,
        total: u64,
        used: u64,
        state: VolumeRuntimeState,
    ) -> VolumeRuntime {
        VolumeRuntime {
            volume_id: id,
            node_id: node.to_string(),
            zone_id: zone,
            total_size: total,
            used_size: used,
            state,
            load: VolumeLoad::default(),
            cold_needle_count: 0,
            hot_needle_count: 0,
        }
    }

    fn make_snapshot(nodes: Vec<NodeRuntime>, volumes: Vec<VolumeRuntime>) -> ClusterSnapshot {
        ClusterSnapshot {
            version: 1,
            timestamp: Instant::now(),
            config_version: 1,
            volumes,
            nodes,
            shards: vec![ShardRuntime {
                shard_id: 0,
                leader_node: "filer-1".to_string(),
                follower_nodes: vec!["filer-2".to_string()],
                qps: 0,
                raft_backlog: 0,
                open_inode_count: 0,
                active_lease_count: 0,
            }],
            cluster_avg_load: 0.0,
        }
    }

    fn build_query(snapshot: ClusterSnapshot, config: ClusterStaticConfig) -> SnapshotStatusQuery {
        SnapshotStatusQuery::new(
            Arc::new(RwLock::new(snapshot)),
            Arc::new(RwLock::new(config)),
            Arc::new(AllocationStatsCollector::new()),
        )
    }

    #[test]
    fn test_cluster_overview_capacity_aggregation() {
        let snap = make_snapshot(
            vec![
                make_node("n1", 0.2, NodeRuntimeState::Healthy),
                make_node("n2", 0.8, NodeRuntimeState::Healthy),
                make_node("n3", 0.5, NodeRuntimeState::Down),
            ],
            vec![
                make_volume(1, "n1", 1, 100, 30, VolumeRuntimeState::Active),
                make_volume(2, "n2", 2, 200, 150, VolumeRuntimeState::Active),
            ],
        );
        let q = build_query(snap, ClusterStaticConfig::default());

        let ov = q.cluster_overview();
        assert_eq!(ov.total_capacity, 300);
        assert_eq!(ov.used_capacity, 180);
        assert_eq!(ov.free_capacity, 120);
        assert_eq!(ov.node_count, 3);
        assert_eq!(ov.healthy_nodes, 2); // n3 is Down
        assert_eq!(ov.volume_count, 2);
    }

    #[test]
    fn test_cluster_overview_load_stats() {
        let snap = make_snapshot(
            vec![
                make_node("n1", 0.2, NodeRuntimeState::Healthy),
                make_node("n2", 0.8, NodeRuntimeState::Healthy),
            ],
            vec![],
        );
        let q = build_query(snap, ClusterStaticConfig::default());

        let ov = q.cluster_overview();
        // avg = (0.2 + 0.8) / 2 = 0.5
        assert!((ov.avg_load - 0.5).abs() < 1e-9);
        assert!((ov.max_load - 0.8).abs() < 1e-9);
        assert!((ov.min_load - 0.2).abs() < 1e-9);
        // imbalance = 0.8 / 0.2 = 4.0
        assert!((ov.imbalance_ratio - 4.0).abs() < 1e-9);
    }

    #[test]
    fn test_cluster_overview_empty_cluster() {
        let snap = make_snapshot(vec![], vec![]);
        let q = build_query(snap, ClusterStaticConfig::default());

        let ov = q.cluster_overview();
        assert_eq!(ov.total_capacity, 0);
        assert_eq!(ov.node_count, 0);
        assert_eq!(ov.healthy_nodes, 0);
        assert_eq!(ov.imbalance_ratio, 1.0); // degenerate guard
    }

    #[test]
    fn test_node_load_distribution_volume_count() {
        let snap = make_snapshot(
            vec![
                make_node("n1", 0.1, NodeRuntimeState::Healthy),
                make_node("n2", 0.9, NodeRuntimeState::Healthy),
            ],
            vec![
                make_volume(1, "n1", 1, 100, 10, VolumeRuntimeState::Active),
                make_volume(2, "n1", 1, 100, 20, VolumeRuntimeState::Active),
                make_volume(3, "n2", 2, 100, 30, VolumeRuntimeState::Active),
            ],
        );
        let q = build_query(snap, ClusterStaticConfig::default());

        let dist = q.node_load_distribution();
        assert_eq!(dist.len(), 2);

        let n1 = dist.iter().find(|d| d.node_id == "n1").unwrap();
        assert_eq!(n1.volume_count, 2);
        assert_eq!(n1.zone_id, 1);
        let n2 = dist.iter().find(|d| d.node_id == "n2").unwrap();
        assert_eq!(n2.volume_count, 1);
        assert_eq!(n2.zone_id, 2);
    }

    #[test]
    fn test_volume_details_usage_ratio_and_state() {
        let snap = make_snapshot(
            vec![make_node("n1", 0.0, NodeRuntimeState::Healthy)],
            vec![
                make_volume(1, "n1", 1, 100, 75, VolumeRuntimeState::Active),
                make_volume(2, "n1", 1, 100, 100, VolumeRuntimeState::Full),
            ],
        );
        let q = build_query(snap, ClusterStaticConfig::default());

        let details = q.volume_details();
        assert_eq!(details.len(), 2);
        let v1 = details.iter().find(|d| d.volume_id == 1).unwrap();
        assert!((v1.usage_ratio - 0.75).abs() < 1e-9);
        assert_eq!(v1.state, "Active");
        let v2 = details.iter().find(|d| d.volume_id == 2).unwrap();
        assert!((v2.usage_ratio - 1.0).abs() < 1e-9);
        assert_eq!(v2.state, "Full");
    }

    #[test]
    fn test_allocation_stats_collector() {
        let collector = AllocationStatsCollector::new();
        collector.record_success(100);
        collector.record_success(300);
        collector.record_failure(&AllocError::NoSpace, 50);
        collector.record_failure(&AllocError::NoHealthyNode, 70);

        let stats = collector.snapshot();
        assert_eq!(stats.total_requests, 4);
        assert_eq!(stats.successful, 2);
        assert_eq!(stats.failed, 2);
        assert_eq!(stats.no_space_count, 1);
        // avg latency = (100 + 300 + 50 + 70) / 4 = 130
        assert_eq!(stats.avg_decision_latency_us, 130);
    }

    #[test]
    fn test_migration_tasks_empty_until_p7() {
        let snap = make_snapshot(vec![], vec![]);
        let q = build_query(snap, ClusterStaticConfig::default());
        assert!(q.migration_tasks().is_empty());
    }

    #[test]
    fn test_current_config_returns_clone() {
        let cfg = ClusterStaticConfig {
            shard_count: 5,
            ..Default::default()
        };
        let snap = make_snapshot(vec![], vec![]);
        let q = build_query(snap, cfg.clone());

        let returned = q.current_config();
        assert_eq!(returned.shard_count, 5);
    }

    #[test]
    fn test_maintenance_node_excluded_from_healthy() {
        let mut n = make_node("n1", 0.1, NodeRuntimeState::Healthy);
        n.in_maintenance = true;
        let snap = make_snapshot(
            vec![n, make_node("n2", 0.2, NodeRuntimeState::Healthy)],
            vec![],
        );
        let q = build_query(snap, ClusterStaticConfig::default());

        let ov = q.cluster_overview();
        assert_eq!(ov.healthy_nodes, 1); // n1 in maintenance excluded
    }

    #[test]
    fn test_shared_snapshot_updates_are_visible() {
        // Verify the Arc<RwLock<>> pattern: external snapshot updates are
        // reflected without rebuilding the query handle.
        let snap = make_snapshot(
            vec![make_node("n1", 0.5, NodeRuntimeState::Healthy)],
            vec![make_volume(1, "n1", 1, 100, 10, VolumeRuntimeState::Active)],
        );
        let shared_snap = Arc::new(RwLock::new(snap));
        let q = SnapshotStatusQuery::new(
            shared_snap.clone(),
            Arc::new(RwLock::new(ClusterStaticConfig::default())),
            Arc::new(AllocationStatsCollector::new()),
        );

        assert_eq!(q.cluster_overview().volume_count, 1);

        // Master updates the snapshot (new heartbeat)
        {
            let mut s = shared_snap.write().unwrap();
            s.volumes
                .push(make_volume(2, "n1", 1, 100, 20, VolumeRuntimeState::Active));
            s.version += 1;
        }

        assert_eq!(q.cluster_overview().volume_count, 2);
    }

    #[test]
    fn test_shardid_display() {
        // Sanity: ShardId formats for monitor logs.
        let id = ShardId(7);
        assert_eq!(format!("{}", id), "shard-7");
    }
}
