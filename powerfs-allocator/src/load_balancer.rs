//! LoadBalancer — analyzes a [`ClusterSnapshot`] and produces
//! [`RebalanceAction`]s (Module 6 decision layer).
//!
//! The LoadBalancer is **pure decision logic**: it never copies data. It reads
//! the aggregate snapshot and emits actions describing *what* should move and
//! *where*; the [`MigrationExecutor`](crate::migration_scheduler::MigrationExecutor)
//! (service-side) is responsible for resolving the actual needle ids and
//! performing the copy.
//!
//! ## Action precedence
//!
//! 1. **Draining volumes** — highest priority; a Draining volume must be
//!    emptied before it can be removed, so its data is migrated to the
//!    least-used Active volume regardless of cold/hot classification.
//! 2. **Cold-data migration** — Active volumes above `volume_full_threshold`
//!    with enough cold needles (`>= min_migration_chunk_count`) push cold
//!    needles to a less-used Active volume.
//! 3. **Hot-data migration** — when node load imbalance (max/min load_score
//!    among healthy nodes) exceeds `load_imbalance_threshold`, hot volumes
//!    move from the busiest to the idlest node.
//! 4. **Volume grow** — when *every* Active volume is above
//!    `volume_full_threshold` (cluster near-full), request a new volume in
//!    the zone of the least-loaded healthy node.

use std::sync::{Arc, RwLock};

use crate::config::RebalancePolicy;
use crate::management::RebalanceAction;
use crate::snapshot::{ClusterSnapshot, NodeRuntimeState, VolumeRuntime, VolumeRuntimeState};

/// Decides rebalance actions from a cluster snapshot.
///
/// Holds the [`RebalancePolicy`] behind a shared `RwLock` so the
/// `ManagementApi::update_rebalance_policy` path can update thresholds at
/// runtime.
pub struct LoadBalancer {
    policy: Arc<RwLock<RebalancePolicy>>,
    /// Default size used when emitting [`RebalanceAction::RequestVolumeGrow`].
    volume_default_size: u64,
}

impl LoadBalancer {
    pub fn new(policy: Arc<RwLock<RebalancePolicy>>, volume_default_size: u64) -> Self {
        Self {
            policy,
            volume_default_size,
        }
    }

    /// Read the current policy (cloned to avoid holding the lock across analysis).
    fn policy(&self) -> RebalancePolicy {
        self.policy.read().unwrap().clone()
    }

    /// Analyze the snapshot and return the rebalance actions to consider.
    ///
    /// The returned list is ordered by precedence (drain → cold → hot → grow).
    /// Callers may apply a subset (e.g. only the first N) respecting the
    /// migration concurrency limit.
    pub fn analyze(&self, snapshot: &ClusterSnapshot) -> Vec<RebalanceAction> {
        let policy = self.policy();
        let maintenance_nodes = maintenance_node_set(snapshot);
        let mut actions = Vec::new();

        // 1. Draining volumes → migrate to least-used Active volume.
        for src in draining_volumes(snapshot) {
            if let Some(target) = pick_target_volume(snapshot, &policy, &maintenance_nodes, None) {
                if target.volume_id != src.volume_id {
                    actions.push(RebalanceAction::MigrateColdData {
                        from_volume: src.volume_id,
                        to_volume: target.volume_id,
                        // Needle ids resolved by the executor (can_migrate filter).
                        needle_ids: Vec::new(),
                    });
                }
            }
        }

        // 2. Cold-data migration from over-threshold Active volumes.
        for src in over_threshold_active_volumes(snapshot, &policy) {
            // Skip if not enough cold needles to bother (avoid thrashing).
            if src.cold_needle_count < policy.min_migration_chunk_count as u64 {
                continue;
            }
            if let Some(target) =
                pick_target_volume(snapshot, &policy, &maintenance_nodes, Some(src.volume_id))
            {
                actions.push(RebalanceAction::MigrateColdData {
                    from_volume: src.volume_id,
                    to_volume: target.volume_id,
                    needle_ids: Vec::new(),
                });
            }
        }

        // 3. Hot-data migration when node load is imbalanced.
        if let Some(action) = compute_hot_data_action(snapshot, &policy) {
            actions.push(action);
        }

        // 4. Cluster near-full → request a new volume.
        if cluster_near_full(snapshot, &policy) {
            if let Some(zone_id) = pick_grow_zone(snapshot) {
                actions.push(RebalanceAction::RequestVolumeGrow {
                    zone_id,
                    size: self.volume_default_size,
                });
            }
        }

        actions
    }
}

/// Collect node ids that are in Maintenance (excluded as migration targets).
fn maintenance_node_set(snapshot: &ClusterSnapshot) -> std::collections::HashSet<String> {
    snapshot
        .nodes
        .iter()
        .filter(|n| n.in_maintenance || n.state == NodeRuntimeState::Maintenance)
        .map(|n| n.node_id.clone())
        .collect()
}

/// All Draining volumes (must be emptied before removal).
fn draining_volumes(snapshot: &ClusterSnapshot) -> impl Iterator<Item = &VolumeRuntime> {
    snapshot
        .volumes
        .iter()
        .filter(|v| v.state == VolumeRuntimeState::Draining)
}

/// Active volumes whose usage exceeds `volume_full_threshold`.
///
/// Pinned volumes are excluded: their data must stay on the pinned node, so
/// they are not eligible as cold-data migration sources.
fn over_threshold_active_volumes<'a>(
    snapshot: &'a ClusterSnapshot,
    policy: &RebalancePolicy,
) -> Vec<&'a VolumeRuntime> {
    snapshot
        .volumes
        .iter()
        .filter(|v| {
            v.state == VolumeRuntimeState::Active && v.usage_ratio() > policy.volume_full_threshold
        })
        .filter(|v| !snapshot.pinned_volumes.contains_key(&v.volume_id))
        .collect()
}

/// Pick the least-used Active volume as a migration target.
///
/// Candidates must be `Active`, below `volume_full_threshold` (room for data),
/// on a non-maintenance node. `exclude_volume` skips the source volume.
/// Prefers a different node than... (left to the executor for anti-affinity
/// refinement); here we simply pick the lowest usage_ratio.
fn pick_target_volume<'a>(
    snapshot: &'a ClusterSnapshot,
    policy: &RebalancePolicy,
    maintenance_nodes: &std::collections::HashSet<String>,
    exclude_volume: Option<u64>,
) -> Option<&'a VolumeRuntime> {
    snapshot
        .volumes
        .iter()
        .filter(|v| v.state == VolumeRuntimeState::Active)
        .filter(|v| Some(v.volume_id) != exclude_volume)
        .filter(|v| v.usage_ratio() < policy.volume_full_threshold)
        .filter(|v| !maintenance_nodes.contains(&v.node_id))
        .min_by(|a, b| {
            a.usage_ratio()
                .partial_cmp(&b.usage_ratio())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
}

/// Compute a hot-data migration action if node load is imbalanced.
///
/// Imbalance = max load_score / min load_score among healthy, non-maintenance
/// nodes. If above `load_imbalance_threshold`, move volume ids from the
/// busiest node to the idlest. `volume_ids` is populated with the non-pinned
/// Active volumes on the busiest node — pinned volumes are protected from
/// outbound migration.
fn compute_hot_data_action(
    snapshot: &ClusterSnapshot,
    policy: &RebalancePolicy,
) -> Option<RebalanceAction> {
    let healthy: Vec<&crate::snapshot::NodeRuntime> = snapshot
        .nodes
        .iter()
        .filter(|n| n.state == NodeRuntimeState::Healthy && !n.in_maintenance)
        .collect();
    if healthy.len() < 2 {
        return None;
    }

    let busiest = healthy.iter().max_by(|a, b| {
        a.load_score
            .partial_cmp(&b.load_score)
            .unwrap_or(std::cmp::Ordering::Equal)
    })?;
    let idlest = healthy.iter().min_by(|a, b| {
        a.load_score
            .partial_cmp(&b.load_score)
            .unwrap_or(std::cmp::Ordering::Equal)
    })?;

    let min = idlest.load_score.max(0.01);
    let imbalance = busiest.load_score / min;
    if imbalance <= policy.load_imbalance_threshold {
        return None;
    }

    // Collect non-pinned Active volumes on the busiest node. The executor
    // iterates these to migrate needles to the idle node.
    let volume_ids: Vec<u64> = snapshot
        .volumes
        .iter()
        .filter(|v| {
            v.state == VolumeRuntimeState::Active
                && v.node_id == busiest.node_id
                && !snapshot.pinned_volumes.contains_key(&v.volume_id)
        })
        .map(|v| v.volume_id)
        .collect();

    Some(RebalanceAction::MigrateHotData {
        from_node: busiest.node_id.clone(),
        to_node: idlest.node_id.clone(),
        volume_ids,
    })
}

/// Is the cluster near-full? True when *every* Active volume is above the
/// full threshold (no room to migrate internally → must grow).
fn cluster_near_full(snapshot: &ClusterSnapshot, policy: &RebalancePolicy) -> bool {
    let mut active = 0;
    let mut over = 0;
    for v in &snapshot.volumes {
        if v.state == VolumeRuntimeState::Active {
            active += 1;
            if v.usage_ratio() > policy.volume_full_threshold {
                over += 1;
            }
        }
    }
    active > 0 && active == over
}

/// Pick the zone to grow a new volume in: the zone of the least-loaded
/// healthy node. Falls back to zone 0 if no healthy node reports a zone
/// (zones are inferred from volumes).
fn pick_grow_zone(snapshot: &ClusterSnapshot) -> Option<u32> {
    // Build node_id → zone_id from volumes (a node's zone = its volume's zone).
    let mut node_zone: std::collections::HashMap<&str, u32> = std::collections::HashMap::new();
    for v in &snapshot.volumes {
        node_zone.entry(v.node_id.as_str()).or_insert(v.zone_id);
    }
    let idlest = snapshot
        .nodes
        .iter()
        .filter(|n| n.state == NodeRuntimeState::Healthy && !n.in_maintenance)
        .min_by(|a, b| {
            a.load_score
                .partial_cmp(&b.load_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        })?;
    Some(*node_zone.get(idlest.node_id.as_str()).unwrap_or(&0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::{
        ClusterSnapshot, NodeRuntime, NodeRuntimeState, VolumeLoad, VolumeRuntime,
        VolumeRuntimeState,
    };
    use std::time::Instant;

    fn make_snapshot(volumes: Vec<VolumeRuntime>, nodes: Vec<NodeRuntime>) -> ClusterSnapshot {
        ClusterSnapshot {
            version: 1,
            timestamp: Instant::now(),
            config_version: 1,
            volumes,
            nodes,
            shards: Vec::new(),
            cluster_avg_load: 0.3,
            pinned_volumes: std::collections::HashMap::new(),
        }
    }

    fn vol(
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

    fn node(id: &str, load: f64, state: NodeRuntimeState) -> NodeRuntime {
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

    fn lb() -> LoadBalancer {
        LoadBalancer::new(Arc::new(RwLock::new(RebalancePolicy::default())), 100)
    }

    #[test]
    fn test_draining_volume_migrated_to_least_used() {
        let snap = make_snapshot(
            vec![
                vol(1, "n1", 1, 100, 90, VolumeRuntimeState::Draining), // must empty
                vol(2, "n2", 1, 100, 10, VolumeRuntimeState::Active),   // 10% used → target
                vol(3, "n2", 1, 100, 50, VolumeRuntimeState::Active),
            ],
            vec![
                node("n1", 0.2, NodeRuntimeState::Healthy),
                node("n2", 0.2, NodeRuntimeState::Healthy),
            ],
        );
        let actions = lb().analyze(&snap);
        let drain = actions
            .iter()
            .find(|a| matches!(a, RebalanceAction::MigrateColdData { from_volume: 1, .. }));
        assert!(drain.is_some(), "draining volume must produce a migration");
        if let Some(RebalanceAction::MigrateColdData { to_volume, .. }) = drain {
            assert_eq!(*to_volume, 2, "should target least-used volume");
        }
    }

    #[test]
    fn test_cold_data_migration_respects_threshold_and_min_chunks() {
        let mut src = vol(1, "n1", 1, 100, 90, VolumeRuntimeState::Active); // 90% > 0.85
        src.cold_needle_count = 20; // >= min_migration_chunk_count (10)
        let snap = make_snapshot(
            vec![src, vol(2, "n2", 1, 100, 10, VolumeRuntimeState::Active)],
            vec![
                node("n1", 0.2, NodeRuntimeState::Healthy),
                node("n2", 0.2, NodeRuntimeState::Healthy),
            ],
        );
        let actions = lb().analyze(&snap);
        assert!(actions.iter().any(|a| matches!(
            a,
            RebalanceAction::MigrateColdData {
                from_volume: 1,
                to_volume: 2,
                ..
            }
        )));
    }

    #[test]
    fn test_cold_data_skipped_when_too_few_chunks() {
        let mut src = vol(1, "n1", 1, 100, 90, VolumeRuntimeState::Active);
        src.cold_needle_count = 3; // < min_migration_chunk_count (10) → skip
        let snap = make_snapshot(
            vec![src, vol(2, "n2", 1, 100, 10, VolumeRuntimeState::Active)],
            vec![
                node("n1", 0.2, NodeRuntimeState::Healthy),
                node("n2", 0.2, NodeRuntimeState::Healthy),
            ],
        );
        let actions = lb().analyze(&snap);
        assert!(!actions
            .iter()
            .any(|a| matches!(a, RebalanceAction::MigrateColdData { from_volume: 1, .. })));
    }

    #[test]
    fn test_hot_data_migration_on_imbalance() {
        // n1 load 0.9, n2 load 0.2 → imbalance 4.5 > 2.0
        let snap = make_snapshot(
            vec![vol(1, "n1", 1, 100, 10, VolumeRuntimeState::Active)],
            vec![
                node("n1", 0.9, NodeRuntimeState::Healthy),
                node("n2", 0.2, NodeRuntimeState::Healthy),
            ],
        );
        let actions = lb().analyze(&snap);
        assert!(actions
            .iter()
            .any(|a| matches!(a, RebalanceAction::MigrateHotData { from_node, to_node, .. } if from_node == "n1" && to_node == "n2")));
    }

    #[test]
    fn test_hot_data_skipped_when_balanced() {
        let snap = make_snapshot(
            vec![vol(1, "n1", 1, 100, 10, VolumeRuntimeState::Active)],
            vec![
                node("n1", 0.3, NodeRuntimeState::Healthy),
                node("n2", 0.35, NodeRuntimeState::Healthy),
            ],
        );
        let actions = lb().analyze(&snap);
        assert!(!actions
            .iter()
            .any(|a| matches!(a, RebalanceAction::MigrateHotData { .. })));
    }

    #[test]
    fn test_volume_grow_when_all_active_near_full() {
        let snap = make_snapshot(
            vec![
                vol(1, "n1", 1, 100, 90, VolumeRuntimeState::Active),
                vol(2, "n2", 1, 100, 88, VolumeRuntimeState::Active),
            ],
            vec![
                node("n1", 0.9, NodeRuntimeState::Healthy),
                node("n2", 0.2, NodeRuntimeState::Healthy),
            ],
        );
        let actions = lb().analyze(&snap);
        assert!(actions.iter().any(|a| matches!(
            a,
            RebalanceAction::RequestVolumeGrow {
                zone_id: 1,
                size: 100
            }
        )));
    }

    #[test]
    fn test_no_actions_on_healthy_cluster() {
        let snap = make_snapshot(
            vec![vol(1, "n1", 1, 100, 30, VolumeRuntimeState::Active)],
            vec![node("n1", 0.3, NodeRuntimeState::Healthy)],
        );
        let actions = lb().analyze(&snap);
        assert!(actions.is_empty(), "no rebalance needed on healthy cluster");
    }

    #[test]
    fn test_maintenance_node_excluded_as_target() {
        let snap = make_snapshot(
            vec![
                vol(1, "n1", 1, 100, 90, VolumeRuntimeState::Draining),
                vol(2, "n2", 1, 100, 10, VolumeRuntimeState::Active), // on maintenance node
                vol(3, "n3", 1, 100, 20, VolumeRuntimeState::Active), // on healthy node
            ],
            vec![
                node("n1", 0.2, NodeRuntimeState::Healthy),
                NodeRuntime {
                    node_id: "n2".to_string(),
                    state: NodeRuntimeState::Maintenance,
                    cpu_usage: 0.0,
                    memory_usage: 0.0,
                    disk_usage: 0.0,
                    load_score: 0.2,
                    in_maintenance: true,
                },
                node("n3", 0.2, NodeRuntimeState::Healthy),
            ],
        );
        let actions = lb().analyze(&snap);
        if let Some(RebalanceAction::MigrateColdData { to_volume, .. }) = actions
            .iter()
            .find(|a| matches!(a, RebalanceAction::MigrateColdData { from_volume: 1, .. }))
        {
            assert_ne!(*to_volume, 2, "must not target volume on maintenance node");
            assert_eq!(*to_volume, 3);
        } else {
            panic!("expected a drain migration");
        }
    }

    #[test]
    fn test_pinned_volume_excluded_from_cold_migration() {
        // Volume 1 is over-threshold with enough cold needles, but pinned to
        // its current node → must NOT be a cold-data migration source.
        let mut src = vol(1, "n1", 1, 100, 90, VolumeRuntimeState::Active); // 90% > 0.85
        src.cold_needle_count = 20;
        let mut snap = make_snapshot(
            vec![src, vol(2, "n2", 1, 100, 10, VolumeRuntimeState::Active)],
            vec![
                node("n1", 0.2, NodeRuntimeState::Healthy),
                node("n2", 0.2, NodeRuntimeState::Healthy),
            ],
        );
        snap.pinned_volumes.insert(1, "n1".to_string());

        let actions = lb().analyze(&snap);
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, RebalanceAction::MigrateColdData { from_volume: 1, .. })),
            "pinned volume must not be a cold-data migration source"
        );
    }

    #[test]
    fn test_pinned_volume_excluded_from_hot_migration() {
        // n1 is overloaded (0.9 vs 0.2). Volume 1 on n1 is pinned → must NOT
        // appear in the hot-data migration's volume_ids. Volume 4 on n1 is
        // unpinned → SHOULD appear.
        let snap = make_snapshot(
            vec![
                vol(1, "n1", 1, 100, 10, VolumeRuntimeState::Active), // pinned
                vol(4, "n1", 1, 100, 10, VolumeRuntimeState::Active), // not pinned
            ],
            vec![
                node("n1", 0.9, NodeRuntimeState::Healthy),
                node("n2", 0.2, NodeRuntimeState::Healthy),
            ],
        );
        let mut snap = snap;
        snap.pinned_volumes.insert(1, "n1".to_string());

        let actions = lb().analyze(&snap);
        if let Some(RebalanceAction::MigrateHotData { volume_ids, .. }) = actions
            .iter()
            .find(|a| matches!(a, RebalanceAction::MigrateHotData { .. }))
        {
            assert!(
                !volume_ids.contains(&1),
                "pinned volume 1 must not be in hot-data volume_ids"
            );
            assert!(
                volume_ids.contains(&4),
                "unpinned volume 4 should be in hot-data volume_ids"
            );
        } else {
            panic!("expected a hot-data migration action");
        }
    }
}
