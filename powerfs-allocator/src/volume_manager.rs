//! VolumeManager — adapter wiring the volume-scaling portion of
//! [`ManagementApi`] to a service-side [`VolumeControl`] (Module 7.2, P9).
//!
//! Mirrors the [`ShardManager`](crate::shard_manager::ShardManager) pattern:
//! the allocator decides *where* a volume should go and *when* it is safe to
//! drain/remove, while the actual master-side state mutation is delegated to
//! the `VolumeControl` trait. Services (master) implement `VolumeControl`;
//! `NoopVolumeControl` is provided for allocator-internal testing.
//!
//! ## Lifecycle
//!
//! `create_volume` -> `Active` (allocates a volume id, serves I/O).
//! `drain_volume`  -> `Draining` (stops new allocations; the LoadBalancer and
//! MigrationScheduler detect Draining volumes on each tick and migrate their
//! data asynchronously).
//! `remove_volume` -> `Deleted` (synchronous; requires `used_size == 0`, i.e.
//! all data has been migrated out).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::error::ManageError;
use crate::snapshot::{ClusterSnapshot, NodeRuntimeState, VolumeRuntimeState};

/// Service-side hook for mutating volume lifecycle state on the master.
///
/// The allocator decision layer validates against the snapshot and computes
/// *which* node/zone to use, then calls into this trait to perform the actual
/// master-side mutation (allocate a volume id, flip state). Implementations
/// must be idempotent where noted.
pub trait VolumeControl: Send + Sync {
    /// Create a new volume on `node_id` in `zone_id` with the given `size`.
    /// Returns the newly allocated `volume_id`.
    fn create_volume(&self, node_id: &str, zone_id: u32, size: u64) -> Result<u64, ManageError>;

    /// Mark `volume_id` as Draining (stop new allocations, keep serving reads).
    /// Idempotent: draining an already-Draining volume succeeds.
    fn mark_draining(&self, volume_id: u64) -> Result<(), ManageError>;

    /// Remove `volume_id` (must be Draining and empty). The caller (VolumeManager)
    /// validates emptiness against the snapshot before calling this.
    fn mark_removed(&self, volume_id: u64) -> Result<(), ManageError>;
}

/// No-op `VolumeControl` for allocator-internal tests.
///
/// Hands out monotonically increasing volume ids and records state transitions
/// in an in-memory map so tests can assert lifecycle behavior without a real
/// master.
pub struct NoopVolumeControl {
    next_id: AtomicU64,
    states: RwLock<std::collections::HashMap<u64, VolumeRuntimeState>>,
}

impl NoopVolumeControl {
    pub fn new() -> Self {
        Self {
            // Start at 100 so test fixtures (which use small ids) don't collide.
            next_id: AtomicU64::new(100),
            states: RwLock::new(std::collections::HashMap::new()),
        }
    }

    /// Inject a pre-existing volume state (for test setup).
    pub fn seed(&self, volume_id: u64, state: VolumeRuntimeState) {
        self.states.write().unwrap().insert(volume_id, state);
    }

    /// Inspect the recorded state of a volume (for test assertions).
    pub fn state_of(&self, volume_id: u64) -> Option<VolumeRuntimeState> {
        self.states.read().unwrap().get(&volume_id).cloned()
    }
}

impl Default for NoopVolumeControl {
    fn default() -> Self {
        Self::new()
    }
}

impl VolumeControl for NoopVolumeControl {
    fn create_volume(&self, _node_id: &str, _zone_id: u32, _size: u64) -> Result<u64, ManageError> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        self.states
            .write()
            .unwrap()
            .insert(id, VolumeRuntimeState::Active);
        Ok(id)
    }

    fn mark_draining(&self, volume_id: u64) -> Result<(), ManageError> {
        self.states
            .write()
            .unwrap()
            .insert(volume_id, VolumeRuntimeState::Draining);
        Ok(())
    }

    fn mark_removed(&self, volume_id: u64) -> Result<(), ManageError> {
        self.states
            .write()
            .unwrap()
            .insert(volume_id, VolumeRuntimeState::Deleted);
        Ok(())
    }
}

/// Adapter exposing volume-scaling operations on top of a [`VolumeControl`].
///
/// All decisions (node selection, drain/remove validation) are made against a
/// [`ClusterSnapshot`] passed per call; the manager itself is stateless aside
/// from the shared `VolumeControl` handle.
pub struct VolumeManager {
    control: Arc<dyn VolumeControl>,
}

impl VolumeManager {
    pub fn new(control: Arc<dyn VolumeControl>) -> Self {
        Self { control }
    }

    /// Create a new volume. `node_id = None` auto-selects the least-loaded
    /// healthy node in `zone_id`.
    ///
    /// Backs `ManagementApi::create_volume`.
    pub fn create_volume(
        &self,
        zone_id: u32,
        node_id: Option<String>,
        size: u64,
        snapshot: &ClusterSnapshot,
    ) -> Result<u64, ManageError> {
        let chosen = match node_id {
            Some(id) => {
                // Explicit node: validate it exists, is healthy, and is in the zone.
                let node = snapshot
                    .get_node(&id)
                    .ok_or_else(|| ManageError::ResourceNotFound(format!("node {id} not found")))?;
                if node.in_maintenance || node.state == NodeRuntimeState::Maintenance {
                    return Err(ManageError::InvalidState(format!(
                        "node {id} is in maintenance"
                    )));
                }
                if !node_in_zone(snapshot, &id, zone_id) {
                    return Err(ManageError::InvalidState(format!(
                        "node {id} is not in zone {zone_id}"
                    )));
                }
                id
            }
            None => pick_node_in_zone(snapshot, zone_id).ok_or_else(|| {
                ManageError::InvalidState(format!(
                    "no healthy, non-maintenance node available in zone {zone_id}"
                ))
            })?,
        };

        if size == 0 {
            return Err(ManageError::InvalidState(
                "volume size must be > 0".to_string(),
            ));
        }

        self.control.create_volume(&chosen, zone_id, size)
    }

    /// Mark a volume as Draining. The volume must currently be Active.
    ///
    /// Migration of the volume's data is driven asynchronously by the
    /// LoadBalancer + MigrationScheduler on subsequent ticks.
    ///
    /// Backs `ManagementApi::drain_volume`.
    pub fn drain_volume(
        &self,
        volume_id: u64,
        snapshot: &ClusterSnapshot,
    ) -> Result<(), ManageError> {
        let vol = snapshot.get_volume(volume_id).ok_or_else(|| {
            ManageError::ResourceNotFound(format!("volume {volume_id} not found"))
        })?;
        if vol.state != VolumeRuntimeState::Active {
            return Err(ManageError::InvalidState(format!(
                "volume {volume_id} is {:?}, only Active volumes can be drained",
                vol.state
            )));
        }
        self.control.mark_draining(volume_id)
    }

    /// Remove a drained volume. The volume must be Draining and empty
    /// (`used_size == 0`) — i.e. all its data has been migrated out.
    ///
    /// Backs `ManagementApi::remove_volume`.
    pub fn remove_volume(
        &self,
        volume_id: u64,
        snapshot: &ClusterSnapshot,
    ) -> Result<(), ManageError> {
        let vol = snapshot.get_volume(volume_id).ok_or_else(|| {
            ManageError::ResourceNotFound(format!("volume {volume_id} not found"))
        })?;
        if vol.state != VolumeRuntimeState::Draining {
            return Err(ManageError::InvalidState(format!(
                "volume {volume_id} is {:?}, must be Draining before removal",
                vol.state
            )));
        }
        if vol.used_size > 0 {
            return Err(ManageError::InvalidState(format!(
                "volume {volume_id} still has {} bytes; drain migration incomplete",
                vol.used_size
            )));
        }
        self.control.mark_removed(volume_id)
    }
}

/// Is `node_id` in `zone_id`? A node's zone is inferred from its volumes.
fn node_in_zone(snapshot: &ClusterSnapshot, node_id: &str, zone_id: u32) -> bool {
    snapshot
        .volumes
        .iter()
        .any(|v| v.node_id == node_id && v.zone_id == zone_id)
}

/// Pick the least-loaded healthy, non-maintenance node in `zone_id`.
fn pick_node_in_zone(snapshot: &ClusterSnapshot, zone_id: u32) -> Option<String> {
    // Collect node ids that belong to this zone (via their volumes).
    let zone_nodes: std::collections::HashSet<&str> = snapshot
        .volumes
        .iter()
        .filter(|v| v.zone_id == zone_id)
        .map(|v| v.node_id.as_str())
        .collect();

    snapshot
        .nodes
        .iter()
        .filter(|n| zone_nodes.contains(n.node_id.as_str()))
        .filter(|n| n.state == NodeRuntimeState::Healthy && !n.in_maintenance)
        .min_by(|a, b| {
            a.load_score
                .partial_cmp(&b.load_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|n| n.node_id.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::{
        ClusterSnapshot, NodeRuntime, NodeRuntimeState, VolumeLoad, VolumeRuntime,
        VolumeRuntimeState,
    };
    use std::time::Instant;

    fn snap(volumes: Vec<VolumeRuntime>, nodes: Vec<NodeRuntime>) -> ClusterSnapshot {
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

    fn vol(id: u64, node: &str, zone: u32, state: VolumeRuntimeState, used: u64) -> VolumeRuntime {
        VolumeRuntime {
            volume_id: id,
            node_id: node.to_string(),
            zone_id: zone,
            total_size: 100,
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

    fn mgr() -> (VolumeManager, Arc<NoopVolumeControl>) {
        let control = Arc::new(NoopVolumeControl::new());
        let m = VolumeManager::new(Arc::clone(&control) as Arc<dyn VolumeControl>);
        (m, control)
    }

    #[test]
    fn test_create_volume_auto_selects_least_loaded_node() {
        let (m, control) = mgr();
        let snapshot = snap(
            // n1 and n2 both in zone 1 (via their volumes); n2 is less loaded.
            vec![
                vol(1, "n1", 1, VolumeRuntimeState::Active, 0),
                vol(2, "n2", 1, VolumeRuntimeState::Active, 0),
            ],
            vec![
                node("n1", 0.8, NodeRuntimeState::Healthy),
                node("n2", 0.2, NodeRuntimeState::Healthy),
            ],
        );
        let id = m.create_volume(1, None, 50, &snapshot).unwrap();
        assert!(id >= 100);
        assert_eq!(control.state_of(id), Some(VolumeRuntimeState::Active));
    }

    #[test]
    fn test_create_volume_explicit_node_validated() {
        let (m, _control) = mgr();
        let snapshot = snap(
            vec![vol(1, "n1", 1, VolumeRuntimeState::Active, 0)],
            vec![node("n1", 0.2, NodeRuntimeState::Healthy)],
        );
        // Explicit valid node.
        assert!(m
            .create_volume(1, Some("n1".to_string()), 50, &snapshot)
            .is_ok());
        // Unknown node rejected.
        assert!(m
            .create_volume(1, Some("nope".to_string()), 50, &snapshot)
            .is_err());
    }

    #[test]
    fn test_create_volume_rejects_maintenance_node() {
        let (m, _control) = mgr();
        let snapshot = snap(
            vec![vol(1, "n1", 1, VolumeRuntimeState::Active, 0)],
            vec![NodeRuntime {
                node_id: "n1".to_string(),
                state: NodeRuntimeState::Maintenance,
                cpu_usage: 0.0,
                memory_usage: 0.0,
                disk_usage: 0.0,
                load_score: 0.2,
                in_maintenance: true,
            }],
        );
        let err = m
            .create_volume(1, Some("n1".to_string()), 50, &snapshot)
            .unwrap_err();
        assert!(matches!(err, ManageError::InvalidState(_)));
    }

    #[test]
    fn test_create_volume_rejects_wrong_zone() {
        let (m, _control) = mgr();
        // n1 is in zone 1 (via its volume); request zone 2.
        let snapshot = snap(
            vec![vol(1, "n1", 1, VolumeRuntimeState::Active, 0)],
            vec![node("n1", 0.2, NodeRuntimeState::Healthy)],
        );
        assert!(m
            .create_volume(2, Some("n1".to_string()), 50, &snapshot)
            .is_err());
    }

    #[test]
    fn test_create_volume_rejects_zero_size() {
        let (m, _control) = mgr();
        let snapshot = snap(
            vec![vol(1, "n1", 1, VolumeRuntimeState::Active, 0)],
            vec![node("n1", 0.2, NodeRuntimeState::Healthy)],
        );
        let err = m.create_volume(1, None, 0, &snapshot).unwrap_err();
        assert!(matches!(err, ManageError::InvalidState(_)));
    }

    #[test]
    fn test_create_volume_no_healthy_node_in_zone() {
        let (m, _control) = mgr();
        let snapshot = snap(
            vec![vol(1, "n1", 1, VolumeRuntimeState::Active, 0)],
            vec![NodeRuntime {
                node_id: "n1".to_string(),
                state: NodeRuntimeState::Maintenance,
                cpu_usage: 0.0,
                memory_usage: 0.0,
                disk_usage: 0.0,
                load_score: 0.2,
                in_maintenance: true,
            }],
        );
        assert!(m.create_volume(1, None, 50, &snapshot).is_err());
    }

    #[test]
    fn test_drain_volume_marks_draining() {
        let (m, control) = mgr();
        let snapshot = snap(
            vec![vol(1, "n1", 1, VolumeRuntimeState::Active, 50)],
            vec![node("n1", 0.2, NodeRuntimeState::Healthy)],
        );
        m.drain_volume(1, &snapshot).unwrap();
        assert_eq!(control.state_of(1), Some(VolumeRuntimeState::Draining));
    }

    #[test]
    fn test_drain_volume_rejects_non_active() {
        let (m, _control) = mgr();
        let snapshot = snap(
            vec![vol(1, "n1", 1, VolumeRuntimeState::Draining, 50)],
            vec![node("n1", 0.2, NodeRuntimeState::Healthy)],
        );
        assert!(m.drain_volume(1, &snapshot).is_err());
    }

    #[test]
    fn test_drain_volume_unknown_rejected() {
        let (m, _control) = mgr();
        let snapshot = snap(vec![], vec![]);
        let err = m.drain_volume(99, &snapshot).unwrap_err();
        assert!(matches!(err, ManageError::ResourceNotFound(_)));
    }

    #[test]
    fn test_remove_volume_requires_draining_and_empty() {
        let (m, control) = mgr();
        // Draining but not empty → rejected.
        let snapshot = snap(
            vec![vol(1, "n1", 1, VolumeRuntimeState::Draining, 30)],
            vec![node("n1", 0.2, NodeRuntimeState::Healthy)],
        );
        assert!(m.remove_volume(1, &snapshot).is_err());

        // Draining and empty → removed.
        let snapshot_empty = snap(
            vec![vol(1, "n1", 1, VolumeRuntimeState::Draining, 0)],
            vec![node("n1", 0.2, NodeRuntimeState::Healthy)],
        );
        m.remove_volume(1, &snapshot_empty).unwrap();
        assert_eq!(control.state_of(1), Some(VolumeRuntimeState::Deleted));
    }

    #[test]
    fn test_remove_volume_rejects_active() {
        let (m, _control) = mgr();
        let snapshot = snap(
            vec![vol(1, "n1", 1, VolumeRuntimeState::Active, 0)],
            vec![node("n1", 0.2, NodeRuntimeState::Healthy)],
        );
        // Active (even if empty) cannot be removed directly — must drain first.
        assert!(m.remove_volume(1, &snapshot).is_err());
    }
}
