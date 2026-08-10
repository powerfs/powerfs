//! MigrationScheduler + MigrationExecutor (Module 6 — migration control).
//!
//! The scheduler owns the migration task state machine, rate limiting, and
//! load-adaptive hysteresis (pause on high load, resume when load recovers).
//! It does **not** copy data itself — actual needle copy and inode chunks
//! updates are delegated to a service-side [`MigrationExecutor`].
//!
//! ## Lifecycle
//!
//! ```text
//! Pending → Running → Completed
//!              ↕
//!          PausedByLoad   (cluster avg load > load_pause_threshold)
//! Running → Failed        (executor reports failure)
//! any → (canceled)        (cancel_migration)
//! ```
//!
//! ## Hysteresis
//!
//! - `cluster_avg_load > load_pause_threshold` (default 0.7): all Running
//!   tasks move to `PausedByLoad` and no new tasks start this tick.
//! - `cluster_avg_load < load_resume_threshold` (default 0.4): `PausedByLoad`
//!   tasks resume to `Running`.
//!
//! The gap (0.7 / 0.4) prevents flapping around the threshold.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::config::{MigrationPolicy, RebalancePolicy};
use crate::error::ManageError;
use crate::management::{
    MigrationExecutionResult, MigrationRejection, RebalanceAction, RejectionReason,
};
use crate::snapshot::{ClusterSnapshot, NodeRuntimeState, VolumeRuntimeState};
use crate::status::{MigrationState, MigrationTaskStatus, MigrationType};

/// Service-side hook for executing migrations and resolving needle-level info.
///
/// The allocator decision layer only sees aggregate snapshot data; it cannot
/// enumerate needles or check per-needle leases. Services (volume server /
/// filer) implement this trait so the scheduler can:
/// 1. resolve `MigrateColdData.needle_ids` (cold + can_migrate filter), and
/// 2. actually dispatch the data copy.
///
/// `NoopExecutor` is provided for allocator-internal testing.
pub trait MigrationExecutor: Send + Sync {
    /// Cold needle ids on `volume_id` (up to `limit`).
    fn cold_needles(&self, volume_id: u64, limit: usize) -> Vec<u64>;

    /// Can `needle_id` migrate right now? (no active lease, `open_count == 0`,
    /// past lease grace period — per plan §4.2).
    fn can_migrate_needle(&self, volume_id: u64, needle_id: u64) -> bool;

    /// Volume ids hosted on `node_id` (for `MigrateHotData` source selection).
    fn volumes_on_node(&self, node_id: &str) -> Vec<u64>;

    /// Dispatch a migration. The `action` carries resolved `needle_ids` /
    /// `volume_ids` (filled by the scheduler during validation). Returns
    /// `Ok(())` once the task is queued on the service side.
    fn start_migration(&self, action: &RebalanceAction, task_id: &str) -> Result<(), ManageError>;

    /// Cancel a running migration identified by `task_id`.
    fn cancel_migration(&self, task_id: &str) -> Result<(), ManageError>;
}

/// No-op executor for allocator-internal tests.
///
/// Pretends every requested needle is cold and migratable, and accepts all
/// start/cancel calls. Does not copy any data.
pub struct NoopExecutor;

impl MigrationExecutor for NoopExecutor {
    fn cold_needles(&self, _volume_id: u64, limit: usize) -> Vec<u64> {
        (0..limit as u64).collect()
    }
    fn can_migrate_needle(&self, _volume_id: u64, _needle_id: u64) -> bool {
        true
    }
    fn volumes_on_node(&self, _node_id: &str) -> Vec<u64> {
        Vec::new()
    }
    fn start_migration(
        &self,
        _action: &RebalanceAction,
        _task_id: &str,
    ) -> Result<(), ManageError> {
        Ok(())
    }
    fn cancel_migration(&self, _task_id: &str) -> Result<(), ManageError> {
        Ok(())
    }
}

/// Cap on needles resolved per cold-data action (keeps actions bounded).
const NEEDLE_RESOLVE_LIMIT: usize = 1024;

/// Identifies the source of a migration so the scheduler can avoid starting
/// two migrations for the same source volume/node at once.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum TaskSource {
    Volume(u64),
    Node(String),
}

/// Migration scheduler: task state machine + rate limiting + load hysteresis.
pub struct MigrationScheduler {
    tasks: Arc<RwLock<Vec<MigrationTaskStatus>>>,
    /// task_id → source, kept in sync with `tasks` so we can de-duplicate.
    task_sources: RwLock<HashMap<String, TaskSource>>,
    policy: Arc<RwLock<MigrationPolicy>>,
    rebalance_policy: Arc<RwLock<RebalancePolicy>>,
    executor: Arc<dyn MigrationExecutor>,
    next_task_id: AtomicU64,
}

impl MigrationScheduler {
    pub fn new(
        policy: Arc<RwLock<MigrationPolicy>>,
        rebalance_policy: Arc<RwLock<RebalancePolicy>>,
        executor: Arc<dyn MigrationExecutor>,
    ) -> Self {
        Self {
            tasks: Arc::new(RwLock::new(Vec::new())),
            task_sources: RwLock::new(HashMap::new()),
            policy,
            rebalance_policy,
            executor,
            next_task_id: AtomicU64::new(1),
        }
    }

    /// Shared task list (also handed to `SnapshotStatusQuery::with_migration_tasks`).
    pub fn tasks_handle(&self) -> Arc<RwLock<Vec<MigrationTaskStatus>>> {
        Arc::clone(&self.tasks)
    }

    fn policy(&self) -> MigrationPolicy {
        self.policy.read().unwrap().clone()
    }

    fn alloc_task_id(&self) -> String {
        format!("mig-{}", self.next_task_id.fetch_add(1, Ordering::SeqCst))
    }

    /// Periodic tick: load-adaptive pause/resume + start new tasks up to the
    /// concurrency limit. Called by the master's heartbeat loop.
    ///
    /// `load_balancer` produces candidate actions from `snapshot`; the
    /// scheduler validates each against the snapshot + executor and starts
    /// as many as the concurrency limit allows.
    pub fn tick(
        &self,
        snapshot: &ClusterSnapshot,
        load_balancer: &crate::load_balancer::LoadBalancer,
    ) {
        let policy = self.policy();
        let load = snapshot.cluster_avg_load;

        // 1. High load → pause everything and don't start new tasks.
        if load > policy.load_pause_threshold {
            self.pause_all_running(&policy);
            return;
        }

        // 2. Load recovered → resume paused tasks.
        if load < policy.load_resume_threshold {
            self.resume_all_paused();
        }

        // 3. Fill free slots up to max_concurrent. Paused tasks still occupy a
        //    slot (they will resume), so count Running + PausedByLoad.
        let active = self.count_active();
        let slots = policy.max_concurrent_migrations.saturating_sub(active) as usize;
        if slots == 0 {
            return;
        }

        let actions = load_balancer.analyze(snapshot);
        let mut started = 0usize;
        for action in actions {
            if started >= slots {
                break;
            }
            // RequestVolumeGrow is not a tracked data migration; skip task creation.
            if matches!(action, RebalanceAction::RequestVolumeGrow { .. }) {
                continue;
            }
            // Don't start a second migration for a source that already has an
            // active (Running/PausedByLoad) task.
            if let Some(src) = source_of(&action) {
                if self.has_active_task_for_source(&src) {
                    continue;
                }
            }
            if self.try_start(&action, snapshot).is_some() {
                started += 1;
            }
        }
    }

    /// Validate `actions` against `snapshot` + executor and (unless `dry_run`)
    /// start them. Returns accepted task ids and rejections.
    ///
    /// This backs `ManagementApi::execute_migrations`.
    pub fn execute_migrations(
        &self,
        actions: Vec<RebalanceAction>,
        snapshot: &ClusterSnapshot,
        dry_run: bool,
    ) -> Result<MigrationExecutionResult, ManageError> {
        let mut accepted_task_ids = Vec::new();
        let mut rejected = Vec::new();

        for action in actions {
            match self.validate_action(&action, snapshot) {
                Ok(resolved) => {
                    let task_id = if dry_run {
                        self.alloc_task_id()
                    } else {
                        self.start_task(&resolved, &action)?
                    };
                    accepted_task_ids.push(task_id);
                }
                Err(reason) => rejected.push(MigrationRejection { action, reason }),
            }
        }

        Ok(MigrationExecutionResult {
            accepted_task_ids,
            rejected,
        })
    }

    /// Pause all Running tasks (emergency). Backs `ManagementApi::pause_all_migrations`.
    pub fn pause_all(&self) -> Result<(), ManageError> {
        let policy = self.policy();
        self.pause_all_running(&policy);
        Ok(())
    }

    /// Resume all PausedByLoad tasks. Backs `ManagementApi::resume_migrations`.
    pub fn resume_all(&self) -> Result<(), ManageError> {
        self.resume_all_paused();
        Ok(())
    }

    /// Cancel a specific task. Backs `ManagementApi::cancel_migration`.
    pub fn cancel(&self, task_id: &str) -> Result<(), ManageError> {
        let mut tasks = self.tasks.write().unwrap();
        let task = tasks
            .iter_mut()
            .find(|t| t.task_id == task_id)
            .ok_or_else(|| ManageError::ResourceNotFound(format!("migration task {task_id}")))?;
        if task.state == MigrationState::Completed || task.state == MigrationState::Failed {
            return Err(ManageError::InvalidState(format!(
                "task {task_id} already in terminal state {:?}",
                task.state
            )));
        }
        // Drop the write lock before calling the executor (avoid holding the
        // tasks lock across a potentially blocking service call).
        let prev_state = task.state.clone();
        task.state = MigrationState::Failed;
        task.pause_reason = Some(format!("canceled (was {prev_state:?})"));
        drop(tasks);

        // Release the source so a fresh migration may start for it.
        self.task_sources.write().unwrap().remove(task_id);

        // Best-effort executor cancel; ignore errors (task already marked Failed).
        let _ = self.executor.cancel_migration(task_id);
        Ok(())
    }

    /// Snapshot of all migration tasks (for StatusQuery).
    pub fn tasks(&self) -> Vec<MigrationTaskStatus> {
        self.tasks.read().unwrap().clone()
    }

    // ===== Internal helpers =====

    fn pause_all_running(&self, policy: &MigrationPolicy) {
        let mut tasks = self.tasks.write().unwrap();
        for t in tasks.iter_mut() {
            if t.state == MigrationState::Running {
                t.state = MigrationState::PausedByLoad;
                t.pause_reason = Some(format!("cluster load > {:.2}", policy.load_pause_threshold));
            }
        }
    }

    fn resume_all_paused(&self) {
        let mut tasks = self.tasks.write().unwrap();
        for t in tasks.iter_mut() {
            if t.state == MigrationState::PausedByLoad {
                t.state = MigrationState::Running;
                t.pause_reason = None;
            }
        }
    }

    #[cfg(test)]
    fn count_running(&self) -> u32 {
        self.tasks
            .read()
            .unwrap()
            .iter()
            .filter(|t| t.state == MigrationState::Running)
            .count() as u32
    }

    /// Count tasks occupying a slot: Running + PausedByLoad (paused tasks will
    /// resume and still need their slot).
    fn count_active(&self) -> u32 {
        self.tasks
            .read()
            .unwrap()
            .iter()
            .filter(|t| {
                t.state == MigrationState::Running || t.state == MigrationState::PausedByLoad
            })
            .count() as u32
    }

    /// Is there an active (Running/PausedByLoad) task for `source`?
    fn has_active_task_for_source(&self, source: &TaskSource) -> bool {
        let tasks = self.tasks.read().unwrap();
        let sources = self.task_sources.read().unwrap();
        tasks.iter().any(|t| {
            (t.state == MigrationState::Running || t.state == MigrationState::PausedByLoad)
                && sources.get(&t.task_id).is_some_and(|s| s == source)
        })
    }

    /// Validate one action against the snapshot + executor.
    ///
    /// Returns the action with resolved `needle_ids` / `volume_ids` on success,
    /// or a rejection reason on failure.
    fn validate_action(
        &self,
        action: &RebalanceAction,
        snapshot: &ClusterSnapshot,
    ) -> Result<RebalanceAction, RejectionReason> {
        match action {
            RebalanceAction::MigrateColdData {
                from_volume,
                to_volume,
                needle_ids,
            } => {
                // Target volume must have room and not be on a maintenance node.
                let target = snapshot
                    .get_volume(*to_volume)
                    .ok_or(RejectionReason::VolumeFull)?;
                if target.state != VolumeRuntimeState::Active {
                    return Err(RejectionReason::VolumeFull);
                }
                let rebalance = self.rebalance_policy.read().unwrap().clone();
                if target.usage_ratio() >= rebalance.near_full_exclude_ratio {
                    return Err(RejectionReason::VolumeFull);
                }
                let target_node = snapshot.get_node(&target.node_id);
                if target_node
                    .is_none_or(|n| n.in_maintenance || n.state == NodeRuntimeState::Maintenance)
                {
                    return Err(RejectionReason::NodeInMaintenance);
                }

                // Resolve needle ids: use provided, or query executor + filter.
                let resolved: Vec<u64> = if !needle_ids.is_empty() {
                    needle_ids
                        .iter()
                        .filter(|nid| self.executor.can_migrate_needle(*from_volume, **nid))
                        .copied()
                        .collect()
                } else {
                    self.executor
                        .cold_needles(*from_volume, NEEDLE_RESOLVE_LIMIT)
                        .into_iter()
                        .filter(|nid| self.executor.can_migrate_needle(*from_volume, *nid))
                        .collect()
                };
                if resolved.is_empty() {
                    // No needle is migratable right now (all have active leases).
                    return Err(RejectionReason::HasActiveLease);
                }

                Ok(RebalanceAction::MigrateColdData {
                    from_volume: *from_volume,
                    to_volume: *to_volume,
                    needle_ids: resolved,
                })
            }
            RebalanceAction::MigrateHotData {
                from_node,
                to_node,
                volume_ids,
            } => {
                let target_node = snapshot
                    .get_node(to_node)
                    .ok_or(RejectionReason::NodeInMaintenance)?;
                if target_node.in_maintenance || target_node.state == NodeRuntimeState::Maintenance
                {
                    return Err(RejectionReason::NodeInMaintenance);
                }
                // Resolve volume ids from the executor if not provided.
                let resolved: Vec<u64> = if !volume_ids.is_empty() {
                    volume_ids.clone()
                } else {
                    self.executor.volumes_on_node(from_node)
                };
                if resolved.is_empty() {
                    return Err(RejectionReason::HasActiveLease);
                }
                Ok(RebalanceAction::MigrateHotData {
                    from_node: from_node.clone(),
                    to_node: to_node.clone(),
                    volume_ids: resolved,
                })
            }
            RebalanceAction::RequestVolumeGrow { .. } => Ok(action.clone()),
        }
    }

    /// Attempt to start a single action (used by `tick`). Returns the task id
    /// on success, or `None` if the action was rejected.
    fn try_start(&self, action: &RebalanceAction, snapshot: &ClusterSnapshot) -> Option<String> {
        let resolved = self.validate_action(action, snapshot).ok()?;
        // start_task only fails on executor dispatch error; in tick we skip those.
        self.start_task(&resolved, action).ok()
    }

    /// Create a Running task and dispatch it via the executor.
    fn start_task(
        &self,
        resolved: &RebalanceAction,
        original: &RebalanceAction,
    ) -> Result<String, ManageError> {
        let task_id = self.alloc_task_id();
        // Dispatch first; if it fails, no task is recorded.
        self.executor.start_migration(resolved, &task_id)?;

        let (action_type, bytes_total) = match original {
            RebalanceAction::MigrateColdData { needle_ids, .. } => {
                (MigrationType::ColdData, needle_ids.len() as u64)
            }
            RebalanceAction::MigrateHotData { volume_ids, .. } => {
                (MigrationType::HotData, volume_ids.len() as u64)
            }
            RebalanceAction::RequestVolumeGrow { .. } => (MigrationType::VolumeGrow, 0),
        };

        let task = MigrationTaskStatus {
            task_id: task_id.clone(),
            action_type,
            state: MigrationState::Running,
            progress: 0.0,
            bytes_migrated: 0,
            bytes_total,
            started_at: std::time::Instant::now(),
            pause_reason: None,
        };
        self.tasks.write().unwrap().push(task);

        // Record the source so future ticks de-duplicate against it.
        if let Some(src) = source_of(original) {
            self.task_sources
                .write()
                .unwrap()
                .insert(task_id.clone(), src);
        }
        Ok(task_id)
    }
}

/// Extract the migration source from an action (for de-duplication).
/// `RequestVolumeGrow` has no source and returns `None`.
fn source_of(action: &RebalanceAction) -> Option<TaskSource> {
    match action {
        RebalanceAction::MigrateColdData { from_volume, .. } => {
            Some(TaskSource::Volume(*from_volume))
        }
        RebalanceAction::MigrateHotData { from_node, .. } => {
            Some(TaskSource::Node(from_node.clone()))
        }
        RebalanceAction::RequestVolumeGrow { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{MigrationPolicy, RebalancePolicy};
    use crate::load_balancer::LoadBalancer;
    use crate::snapshot::{
        ClusterSnapshot, NodeRuntime, NodeRuntimeState, VolumeLoad, VolumeRuntime,
        VolumeRuntimeState,
    };
    use std::time::Instant;

    fn policy_pause_at(
        pause: f64,
        resume: f64,
        max_concurrent: u32,
    ) -> Arc<RwLock<MigrationPolicy>> {
        Arc::new(RwLock::new(MigrationPolicy {
            max_concurrent_migrations: max_concurrent,
            max_bandwidth_mbps: 100,
            load_pause_threshold: pause,
            load_resume_threshold: resume,
            scan_interval_secs: 60,
        }))
    }

    fn snap(
        volumes: Vec<VolumeRuntime>,
        nodes: Vec<NodeRuntime>,
        avg_load: f64,
    ) -> ClusterSnapshot {
        ClusterSnapshot {
            version: 1,
            timestamp: Instant::now(),
            config_version: 1,
            volumes,
            nodes,
            shards: Vec::new(),
            cluster_avg_load: avg_load,
        }
    }

    fn vol(id: u64, node: &str, total: u64, used: u64, state: VolumeRuntimeState) -> VolumeRuntime {
        VolumeRuntime {
            volume_id: id,
            node_id: node.to_string(),
            zone_id: 1,
            total_size: total,
            used_size: used,
            state,
            load: VolumeLoad::default(),
            cold_needle_count: 50,
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

    fn scheduler_with(policy: Arc<RwLock<MigrationPolicy>>) -> (MigrationScheduler, LoadBalancer) {
        let rebalance = Arc::new(RwLock::new(RebalancePolicy::default()));
        let sched = MigrationScheduler::new(policy, Arc::clone(&rebalance), Arc::new(NoopExecutor));
        let lb = LoadBalancer::new(rebalance, 100);
        (sched, lb)
    }

    #[test]
    fn test_tick_starts_migration_for_draining_volume() {
        let policy = policy_pause_at(0.7, 0.4, 4);
        let (sched, lb) = scheduler_with(policy);
        let snapshot = snap(
            vec![
                vol(1, "n1", 100, 90, VolumeRuntimeState::Draining),
                vol(2, "n2", 100, 10, VolumeRuntimeState::Active),
            ],
            vec![
                node("n1", 0.2, NodeRuntimeState::Healthy),
                node("n2", 0.2, NodeRuntimeState::Healthy),
            ],
            0.3,
        );
        sched.tick(&snapshot, &lb);
        let tasks = sched.tasks();
        assert_eq!(tasks.len(), 1, "one drain migration should start");
        assert_eq!(tasks[0].state, MigrationState::Running);
        assert_eq!(tasks[0].action_type, MigrationType::ColdData);
    }

    #[test]
    fn test_tick_pauses_on_high_load() {
        let policy = policy_pause_at(0.7, 0.4, 4);
        let (sched, lb) = scheduler_with(policy);
        let snapshot = snap(
            vec![
                vol(1, "n1", 100, 90, VolumeRuntimeState::Draining),
                vol(2, "n2", 100, 10, VolumeRuntimeState::Active),
            ],
            vec![
                node("n1", 0.2, NodeRuntimeState::Healthy),
                node("n2", 0.2, NodeRuntimeState::Healthy),
            ],
            0.3,
        );
        // Start a task at low load.
        sched.tick(&snapshot, &lb);
        assert_eq!(sched.count_running(), 1);

        // High load → pause.
        let high_load = snap(snapshot.volumes.clone(), snapshot.nodes.clone(), 0.8);
        sched.tick(&high_load, &lb);
        assert_eq!(sched.count_running(), 0);
        assert!(sched
            .tasks()
            .iter()
            .all(|t| t.state == MigrationState::PausedByLoad));
    }

    #[test]
    fn test_tick_resumes_when_load_recovers() {
        let policy = policy_pause_at(0.7, 0.4, 4);
        let (sched, lb) = scheduler_with(policy);
        let snapshot = snap(
            vec![
                vol(1, "n1", 100, 90, VolumeRuntimeState::Draining),
                vol(2, "n2", 100, 10, VolumeRuntimeState::Active),
            ],
            vec![
                node("n1", 0.2, NodeRuntimeState::Healthy),
                node("n2", 0.2, NodeRuntimeState::Healthy),
            ],
            0.3,
        );
        sched.tick(&snapshot, &lb);
        let high = snap(snapshot.volumes.clone(), snapshot.nodes.clone(), 0.8);
        sched.tick(&high, &lb);
        assert_eq!(sched.count_running(), 0);

        // Below resume threshold → resume.
        let low = snap(snapshot.volumes.clone(), snapshot.nodes.clone(), 0.2);
        sched.tick(&low, &lb);
        assert_eq!(sched.count_running(), 1, "paused task should resume");
    }

    #[test]
    fn test_tick_respects_concurrency_limit() {
        let policy = policy_pause_at(0.7, 0.4, 1); // only 1 concurrent
        let (sched, lb) = scheduler_with(policy);
        // Two draining volumes → two candidate actions, but limit is 1.
        let snapshot = snap(
            vec![
                vol(1, "n1", 100, 90, VolumeRuntimeState::Draining),
                vol(2, "n1", 100, 90, VolumeRuntimeState::Draining),
                vol(3, "n2", 100, 10, VolumeRuntimeState::Active),
            ],
            vec![
                node("n1", 0.2, NodeRuntimeState::Healthy),
                node("n2", 0.2, NodeRuntimeState::Healthy),
            ],
            0.3,
        );
        sched.tick(&snapshot, &lb);
        assert_eq!(
            sched.count_running(),
            1,
            "concurrency limit must be respected"
        );
    }

    #[test]
    fn test_execute_migrations_dry_run_validates_without_starting() {
        let policy = policy_pause_at(0.7, 0.4, 4);
        let (sched, _lb) = scheduler_with(policy);
        let snapshot = snap(
            vec![
                vol(1, "n1", 100, 90, VolumeRuntimeState::Draining),
                vol(2, "n2", 100, 10, VolumeRuntimeState::Active),
            ],
            vec![
                node("n1", 0.2, NodeRuntimeState::Healthy),
                node("n2", 0.2, NodeRuntimeState::Healthy),
            ],
            0.3,
        );
        let action = RebalanceAction::MigrateColdData {
            from_volume: 1,
            to_volume: 2,
            needle_ids: Vec::new(),
        };
        let result = sched
            .execute_migrations(vec![action], &snapshot, true)
            .unwrap();
        assert_eq!(result.accepted_task_ids.len(), 1);
        assert!(result.rejected.is_empty());
        // Dry-run must not create a tracked task.
        assert!(sched.tasks().is_empty());
    }

    #[test]
    fn test_execute_migrations_rejects_full_target() {
        let policy = policy_pause_at(0.7, 0.4, 4);
        let (sched, _lb) = scheduler_with(policy);
        // RebalancePolicy.near_full_exclude_ratio defaults to 0.90.
        let snapshot = snap(
            vec![
                vol(1, "n1", 100, 10, VolumeRuntimeState::Draining),
                vol(2, "n2", 100, 95, VolumeRuntimeState::Active), // 95% >= 0.90 → full
            ],
            vec![
                node("n1", 0.2, NodeRuntimeState::Healthy),
                node("n2", 0.2, NodeRuntimeState::Healthy),
            ],
            0.3,
        );
        let action = RebalanceAction::MigrateColdData {
            from_volume: 1,
            to_volume: 2,
            needle_ids: Vec::new(),
        };
        let result = sched
            .execute_migrations(vec![action], &snapshot, true)
            .unwrap();
        assert!(result.accepted_task_ids.is_empty());
        assert_eq!(result.rejected.len(), 1);
        assert_eq!(result.rejected[0].reason, RejectionReason::VolumeFull);
    }

    #[test]
    fn test_execute_migrations_rejects_maintenance_target_node() {
        let policy = policy_pause_at(0.7, 0.4, 4);
        let (sched, _lb) = scheduler_with(policy);
        let snapshot = snap(
            vec![
                vol(1, "n1", 100, 10, VolumeRuntimeState::Draining),
                vol(2, "n2", 100, 10, VolumeRuntimeState::Active),
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
            ],
            0.3,
        );
        let action = RebalanceAction::MigrateColdData {
            from_volume: 1,
            to_volume: 2,
            needle_ids: Vec::new(),
        };
        let result = sched
            .execute_migrations(vec![action], &snapshot, true)
            .unwrap();
        assert_eq!(result.rejected.len(), 1);
        assert_eq!(
            result.rejected[0].reason,
            RejectionReason::NodeInMaintenance
        );
    }

    #[test]
    fn test_cancel_task() {
        let policy = policy_pause_at(0.7, 0.4, 4);
        let (sched, lb) = scheduler_with(policy);
        let snapshot = snap(
            vec![
                vol(1, "n1", 100, 90, VolumeRuntimeState::Draining),
                vol(2, "n2", 100, 10, VolumeRuntimeState::Active),
            ],
            vec![
                node("n1", 0.2, NodeRuntimeState::Healthy),
                node("n2", 0.2, NodeRuntimeState::Healthy),
            ],
            0.3,
        );
        sched.tick(&snapshot, &lb);
        let task_id = sched.tasks()[0].task_id.clone();
        sched.cancel(&task_id).unwrap();
        assert_eq!(sched.tasks()[0].state, MigrationState::Failed);
    }

    #[test]
    fn test_cancel_unknown_task_rejected() {
        let policy = policy_pause_at(0.7, 0.4, 4);
        let (sched, _lb) = scheduler_with(policy);
        let err = sched.cancel("nope").unwrap_err();
        assert!(matches!(err, ManageError::ResourceNotFound(_)));
    }

    #[test]
    fn test_tick_does_not_duplicate_migration_for_same_source() {
        // Two consecutive ticks at low load must not start a second migration
        // for the same draining volume (source de-duplication).
        let policy = policy_pause_at(0.7, 0.4, 4);
        let (sched, lb) = scheduler_with(policy);
        let snapshot = snap(
            vec![
                vol(1, "n1", 100, 90, VolumeRuntimeState::Draining),
                vol(2, "n2", 100, 10, VolumeRuntimeState::Active),
            ],
            vec![
                node("n1", 0.2, NodeRuntimeState::Healthy),
                node("n2", 0.2, NodeRuntimeState::Healthy),
            ],
            0.3,
        );
        sched.tick(&snapshot, &lb);
        assert_eq!(sched.count_active(), 1);
        // Second tick: the source still has an active task → no duplicate.
        sched.tick(&snapshot, &lb);
        assert_eq!(
            sched.count_active(),
            1,
            "must not start a duplicate migration for the same source"
        );
    }
}
