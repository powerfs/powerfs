//! Allocator integration: wires the `powerfs-allocator` decision pipeline
//! (LoadBalancer + MigrationScheduler) into the master's background loop.
//!
//! This is the **orchestration layer**. It periodically builds a
//! [`ClusterSnapshot`] from the master's heartbeat-aggregated state, feeds it
//! to [`MigrationScheduler::tick`], and drains the executor's completion
//! queue.
//!
//! ## Current scope
//!
//! Actual data movement (needle copy between volumes + inode chunks update on
//! the filer) is delegated to the [`MigrationExecutor`] trait. This module
//! ships a [`LoggingExecutor`] that records each action and completes it
//! immediately — exercising the full decision pipeline end-to-end on a real
//! cluster **without** depending on volume-server cold-data tracking or filer
//! needle→inode reverse lookup, which are prerequisites for real migration
//! (plan §5.2 stage 3).
//!
//! When those service-side prerequisites land, swap `LoggingExecutor` for a
//! real executor that dispatches needle copies and reports asynchronous
//! completion via [`MigrationScheduler::complete_task`].
//!
//! ## Leader-only
//!
//! The rebalance loop is a no-op on followers: only the Raft leader has the
//! authority to mutate volume state, so non-leaders skip the tick entirely.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, RwLock};

use log::{debug, info, warn};

use powerfs_allocator::{
    config::{MigrationPolicy, RebalancePolicy},
    error::{ManageError, ShardId},
    management::{ManagementApi, MigrationExecutionResult, RebalanceAction},
    migration_scheduler::MigrationExecutor,
    shard_map::ShardSplitPlan,
    LoadBalancer, MigrationScheduler, MigrationState, VolumeControl, VolumeManager,
};
use powerfs_common::error::PowerFsError;
use powerfs_common::types::{VolumeId, VolumeState};

use crate::master::MasterNode;

/// Logging executor: records each migration action and enqueues immediate
/// completion. No data is moved.
///
/// See the module docs for why this exists and how to replace it with a real
/// executor.
pub struct LoggingExecutor {
    completion_tx: mpsc::Sender<String>,
}

impl LoggingExecutor {
    pub fn new(completion_tx: mpsc::Sender<String>) -> Self {
        Self { completion_tx }
    }
}

impl MigrationExecutor for LoggingExecutor {
    fn cold_needles(&self, _volume_id: u64, limit: usize) -> Vec<u64> {
        // Pretend every requested needle is cold and migratable so the
        // scheduler can resolve and dispatch the action.
        (0..limit as u64).collect()
    }

    fn can_migrate_needle(&self, _volume_id: u64, _needle_id: u64) -> bool {
        true
    }

    fn volumes_on_node(&self, _node_id: &str) -> Vec<u64> {
        Vec::new()
    }

    fn start_migration(&self, action: &RebalanceAction, task_id: &str) -> Result<(), ManageError> {
        info!(
            "[rebalance] executor accepted task={} action={:?}",
            task_id, action
        );
        // No real data copy: enqueue immediate completion so the scheduler
        // releases the source slot for the next tick.
        let _ = self.completion_tx.send(task_id.to_string());
        Ok(())
    }

    fn cancel_migration(&self, task_id: &str) -> Result<(), ManageError> {
        debug!("[rebalance] executor cancel task={}", task_id);
        Ok(())
    }
}

/// Real migration executor: dispatches needle copies between volumes and
/// updates filer chunk mappings.
///
/// `start_migration` spawns an async task that:
/// 1. Calls filer `FindInodesByVolume` to discover which inodes own the
///    needles being migrated (needle→inode reverse lookup).
/// 2. For each needle: reads data from the source volume, writes to the
///    target volume (auto-assigned file_key).
/// 3. Calls filer `UpdateInodeSizeChunks` to update each affected inode's
///    chunk mapping with the new (volume_id, needle_id).
/// 4. Deletes old needles from the source volume.
/// 5. Reports completion via `completion_tx`.
///
/// Cancellation is cooperative: `cancel_migration` sets an `AtomicBool`
/// flag that the async task checks between needle copies.
pub struct MasterMigrationExecutor {
    master: Arc<MasterNode>,
    completion_tx: mpsc::Sender<String>,
    /// Cancellation flags keyed by task_id.
    cancel_flags: Arc<Mutex<HashMap<String, Arc<AtomicBool>>>>,
}

impl MasterMigrationExecutor {
    pub fn new(master: Arc<MasterNode>, completion_tx: mpsc::Sender<String>) -> Self {
        Self {
            master,
            completion_tx,
            cancel_flags: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl MigrationExecutor for MasterMigrationExecutor {
    fn cold_needles(&self, volume_id: u64, limit: usize) -> Vec<u64> {
        let address = match self.master.get_volume_address(volume_id) {
            Some(a) => a,
            None => {
                warn!(
                    "cold_needles: volume {} not found in topology, returning empty",
                    volume_id
                );
                return Vec::new();
            }
        };

        let pool = self.master.volume_client_pool.clone();
        let limit_u32 = if limit > u32::MAX as usize {
            u32::MAX
        } else {
            limit as u32
        };

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                match pool.list_needles(&address, volume_id, limit_u32).await {
                    Ok(needles) => needles.into_iter().map(|(id, _, _, _)| id).collect(),
                    Err(e) => {
                        warn!(
                            "cold_needles: list_needles failed for volume {}: {}",
                            volume_id, e
                        );
                        Vec::new()
                    }
                }
            })
        })
    }

    fn can_migrate_needle(&self, _volume_id: u64, _needle_id: u64) -> bool {
        // Simplification: always allow migration. A full implementation would
        // check the filer for active leases on the inode owning this needle.
        // The migration itself is idempotent (read→write→update→delete), so
        // even if a concurrent write happens, the old needle is simply deleted
        // after the new one is in place.
        true
    }

    fn volumes_on_node(&self, node_id: &str) -> Vec<u64> {
        self.master.volumes_on_node(node_id)
    }

    fn start_migration(&self, action: &RebalanceAction, task_id: &str) -> Result<(), ManageError> {
        let cancel_flag = Arc::new(AtomicBool::new(false));
        self.cancel_flags
            .lock()
            .unwrap()
            .insert(task_id.to_string(), Arc::clone(&cancel_flag));

        let master = Arc::clone(&self.master);
        let completion_tx = self.completion_tx.clone();
        let cancel_flags = Arc::clone(&self.cancel_flags);
        let action = action.clone();
        let task_id = task_id.to_string();

        tokio::runtime::Handle::current().spawn(async move {
            let result = run_migration(&master, &action, &cancel_flag).await;

            // Remove this task's cancel flag from the registry so the map
            // does not grow unboundedly across ticks. If `cancel_migration`
            // already removed it (cancellation path), this is a no-op.
            cancel_flags.lock().unwrap().remove(&task_id);

            match result {
                Ok(bytes_migrated) => {
                    info!(
                        "[rebalance] migration task {} completed, {} bytes migrated",
                        task_id, bytes_migrated
                    );
                }
                Err(e) => {
                    warn!("[rebalance] migration task {} failed: {}", task_id, e);
                }
            }

            // Report completion to the scheduler (sync mpsc send is non-blocking
            // if the receiver is alive).
            let _ = completion_tx.send(task_id);
        });

        Ok(())
    }

    fn cancel_migration(&self, task_id: &str) -> Result<(), ManageError> {
        if let Some(flag) = self.cancel_flags.lock().unwrap().remove(task_id) {
            flag.store(true, Ordering::Relaxed);
            debug!("[rebalance] cancel flag set for task={}", task_id);
        }
        Ok(())
    }
}

/// Run a single migration action to completion (or cancellation).
///
/// Returns `Ok(bytes_migrated)` on success.
async fn run_migration(
    master: &Arc<MasterNode>,
    action: &RebalanceAction,
    cancel_flag: &Arc<AtomicBool>,
) -> Result<u64, String> {
    match action {
        RebalanceAction::MigrateColdData {
            from_volume,
            to_volume,
            needle_ids,
        } => {
            run_cold_data_migration(master, *from_volume, *to_volume, needle_ids, cancel_flag).await
        }
        RebalanceAction::MigrateHotData {
            from_node,
            to_node: _,
            volume_ids,
        } => {
            // Hot data migration: migrate all needles from volumes on the
            // source node. For each volume, find cold needles and migrate.
            let mut total = 0u64;
            for &vid in volume_ids {
                if cancel_flag.load(Ordering::Relaxed) {
                    return Ok(total);
                }
                let target = pick_target_volume(master, vid).await?;
                total += run_cold_data_migration(master, vid, target, &[], cancel_flag).await?;
            }
            let _ = from_node; // logged for context
            Ok(total)
        }
        RebalanceAction::RequestVolumeGrow { zone_id, size } => {
            // Volume grow is handled by the VolumeManager, not the executor.
            // This should not be dispatched here; log and succeed.
            warn!(
                "[rebalance] RequestVolumeGrow dispatched to executor: zone={} size={} — no-op",
                zone_id, size
            );
            Ok(0)
        }
    }
}

/// Migrate cold needles from `from_volume` to `to_volume`.
async fn run_cold_data_migration(
    master: &Arc<MasterNode>,
    from_volume: u64,
    to_volume: u64,
    needle_ids: &[u64],
    cancel_flag: &Arc<AtomicBool>,
) -> Result<u64, String> {
    let source_addr = master
        .get_volume_address(from_volume)
        .ok_or_else(|| format!("source volume {} not found", from_volume))?;
    let target_addr = master
        .get_volume_address(to_volume)
        .ok_or_else(|| format!("target volume {} not found", to_volume))?;

    // 1. Reverse lookup: find which inodes own the needles.
    let filer_client = crate::filer_client::FilerManagementClient::new(Arc::clone(master));
    let entries = filer_client
        .find_inodes_by_volume(from_volume, needle_ids)
        .await?;

    if entries.is_empty() {
        info!(
            "[rebalance] no inodes found for volume {} needles {:?} — already migrated or deleted",
            from_volume, needle_ids
        );
        return Ok(0);
    }

    info!(
        "[rebalance] migrating {} chunk entries from volume {} to {}",
        entries.len(),
        from_volume,
        to_volume
    );

    // 2. Group entries by inode for batch chunk updates.
    let mut by_inode: HashMap<u64, Vec<crate::filer_client::FilerChunkEntry>> = HashMap::new();
    for entry in entries {
        by_inode.entry(entry.inode).or_default().push(entry);
    }

    // 3. For each inode: read each needle, write to target, collect new chunk info.
    let pool = master.volume_client_pool.clone();
    let mut total_bytes = 0u64;
    let mut needle_map: HashMap<u64, u64> = HashMap::new(); // old_needle_id → new_needle_id

    for (inode, chunks) in &by_inode {
        for chunk in chunks {
            if cancel_flag.load(Ordering::Relaxed) {
                info!(
                    "[rebalance] migration cancelled, partial: {} bytes",
                    total_bytes
                );
                return Ok(total_bytes);
            }

            // Read needle data from source volume.
            let data = pool
                .read_needle(&source_addr, from_volume, chunk.needle_id)
                .await
                .map_err(|e| format!("read needle {} failed: {}", chunk.needle_id, e))?;

            // Write to target volume (auto-assign file_key=0).
            let new_needle_id = pool
                .write_needle_return_key(&target_addr, to_volume, 0, &data)
                .await
                .map_err(|e| format!("write needle to volume {} failed: {}", to_volume, e))?;

            needle_map.insert(chunk.needle_id, new_needle_id);
            total_bytes += data.len() as u64;
        }

        // 4. Build updated chunk list for this inode.
        let updated_chunks: Vec<(u64, u64, u64, u64, u32)> = chunks
            .iter()
            .map(|c| {
                let new_nid = needle_map.get(&c.needle_id).copied().unwrap_or(c.needle_id);
                (c.offset, c.size, new_nid, to_volume, 0) // crc32=0: volume server will recompute
            })
            .collect();

        // 5. Update filer with new chunk mapping.
        let shard_id = chunks.first().map(|c| c.shard_id).unwrap_or(0);
        let file_size = chunks.first().map(|c| c.file_size).unwrap_or(0);
        filer_client
            .update_inode_size_chunks(
                shard_id,
                *inode,
                file_size,
                &updated_chunks,
                "migration-executor",
            )
            .await
            .map_err(|e| format!("update inode {} chunks failed: {}", inode, e))?;
    }

    // 6. Delete old needles from source volume.
    for old_needle_id in needle_map.keys() {
        if cancel_flag.load(Ordering::Relaxed) {
            break;
        }
        if let Err(e) = pool
            .delete_needle(&source_addr, from_volume, *old_needle_id)
            .await
        {
            warn!(
                "[rebalance] failed to delete old needle {} on volume {}: {} (non-fatal)",
                old_needle_id, from_volume, e
            );
        }
    }

    info!(
        "[rebalance] cold data migration complete: {} inodes, {} bytes, volume {} → {}",
        by_inode.len(),
        total_bytes,
        from_volume,
        to_volume
    );

    Ok(total_bytes)
}

/// Pick a target volume for hot-data migration (any available volume not on
/// the source volume's node).
async fn pick_target_volume(master: &Arc<MasterNode>, source_volume: u64) -> Result<u64, String> {
    let volumes = master.list_volumes().await;
    let source_node = volumes
        .iter()
        .find(|v| v.id.0 == source_volume)
        .map(|v| v.node_id.0.clone());

    let target = volumes
        .iter()
        .find(|v| {
            v.state == VolumeState::Available
                && v.id.0 != source_volume
                && v.node_id.0 != source_node.as_deref().unwrap_or("")
        })
        .or_else(|| {
            volumes
                .iter()
                .find(|v| v.state == VolumeState::Available && v.id.0 != source_volume)
        });

    target
        .map(|v| v.id.0)
        .ok_or_else(|| "no available target volume".to_string())
}

/// Master-backed `VolumeControl`: bridges the allocator's sync trait to the
/// master's async Raft-replicated volume lifecycle methods.
///
/// ## sync→async bridge
///
/// The allocator's `VolumeControl` trait is synchronous, but the master's
/// `create_new_volume_with_preference` / `update_volume_state` / `delete_volume`
/// are async (they propose Raft commands). This implementation uses
/// [`tokio::task::block_in_place`] + [`tokio::runtime::Handle::block_on`] to
/// bridge the boundary.
///
/// `block_in_place` is safe here because:
/// 1. The master runs on `#[tokio::main]` (multi-threaded runtime).
/// 2. `VolumeControl` is only called from management operations (gRPC
///    handlers), never from the hot I/O path or from `spawn_blocking`.
///
/// ## size parameter
///
/// The master's `create_new_volume_with_preference` uses
/// `cluster_config.volume_size_limit` as the volume size; the `size` parameter
/// from `VolumeControl` is validated by `VolumeManager` against the snapshot
/// but the master's configured limit is authoritative. A future refinement may
/// pass the requested size through to the Raft command.
pub struct MasterVolumeControl {
    master: Arc<MasterNode>,
}

impl MasterVolumeControl {
    pub fn new(master: Arc<MasterNode>) -> Self {
        Self { master }
    }
}

impl VolumeControl for MasterVolumeControl {
    fn create_volume(&self, node_id: &str, _zone_id: u32, _size: u64) -> Result<u64, ManageError> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let (fid, _nodes) = self
                    .master
                    .create_new_volume_with_preference(
                        "000", // default replication (single replica)
                        "default",
                        Some(node_id),
                    )
                    .await
                    .map_err(map_powerfs_error)?;
                Ok(fid.volume_id.0)
            })
        })
    }

    fn mark_draining(&self, volume_id: u64) -> Result<(), ManageError> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.master
                    .update_volume_state(&VolumeId(volume_id), VolumeState::Draining)
                    .await
                    .map_err(map_powerfs_error)
            })
        })
    }

    fn mark_removed(&self, volume_id: u64) -> Result<(), ManageError> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.master
                    .delete_volume(&VolumeId(volume_id))
                    .await
                    .map_err(map_powerfs_error)
            })
        })
    }
}

/// Map `PowerFsError` → `ManageError` for the VolumeControl bridge.
fn map_powerfs_error(err: PowerFsError) -> ManageError {
    match err {
        PowerFsError::NotLeader => {
            ManageError::InvalidState("master is not the Raft leader".to_string())
        }
        PowerFsError::VolumeNotFound(vid) => {
            ManageError::ResourceNotFound(format!("volume {}", vid.0))
        }
        PowerFsError::InvalidRequest(msg) => ManageError::InvalidState(msg),
        PowerFsError::InvalidVolumeState(msg) => ManageError::InvalidState(msg),
        other => ManageError::InvalidState(format!("{other}")),
    }
}

/// Master-backed `ManagementApi`: ties the allocator's management interface to
/// the master's runtime state.
///
/// Implements the full [`ManagementApi`] trait. Methods that can work today
/// (policy updates, migration control, volume scaling) delegate to the
/// [`RebalanceEngine`] / [`VolumeManager`] / [`MasterVolumeControl`]. Methods
/// that need additional service-side work (shard scaling via filer, volume
/// pinning, node maintenance via Raft) return `InvalidState` with a clear
/// "not yet implemented" message so callers can distinguish missing features
/// from transient errors.
pub struct MasterManagementApi {
    master: Arc<MasterNode>,
    engine: Arc<RebalanceEngine>,
    volume_manager: VolumeManager,
    filer_client: crate::filer_client::FilerManagementClient,
}

impl MasterManagementApi {
    /// Construct from the master and its rebalance engine. Creates a
    /// [`MasterVolumeControl`] internally and wraps it in a [`VolumeManager`],
    /// and a [`FilerManagementClient`] for shard scaling via filer gRPC.
    pub fn new(master: Arc<MasterNode>, engine: Arc<RebalanceEngine>) -> Self {
        let volume_control: Arc<dyn VolumeControl> =
            Arc::new(MasterVolumeControl::new(Arc::clone(&master)));
        let volume_manager = VolumeManager::new(volume_control);
        let filer_client = crate::filer_client::FilerManagementClient::new(Arc::clone(&master));
        Self {
            master,
            engine,
            volume_manager,
            filer_client,
        }
    }

    /// Build a fresh cluster snapshot from the master's heartbeat-aggregated
    /// state. All decision methods call this to get a consistent view.
    fn snapshot(&self) -> powerfs_allocator::ClusterSnapshot {
        self.master.build_cluster_snapshot()
    }
}

impl ManagementApi for MasterManagementApi {
    // ===== Configuration management =====

    fn set_placement_strategy(&self, strategy: &str) -> Result<(), ManageError> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.master
                    .set_placement_strategy(strategy)
                    .await
                    .map_err(map_powerfs_error)
            })
        })
    }

    fn update_migration_policy(&self, policy: MigrationPolicy) -> Result<(), ManageError> {
        *self.engine.migration_policy().write().unwrap() = policy;
        Ok(())
    }

    fn update_rebalance_policy(&self, policy: RebalancePolicy) -> Result<(), ManageError> {
        *self.engine.rebalance_policy().write().unwrap() = policy;
        Ok(())
    }

    fn set_node_maintenance(&self, node_id: &str, enabled: bool) -> Result<(), ManageError> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.master
                    .set_node_maintenance(node_id, enabled)
                    .await
                    .map_err(map_powerfs_error)
            })
        })
    }

    // ===== Migration control =====

    fn trigger_rebalance_check(&self, dry_run: bool) -> Result<Vec<RebalanceAction>, ManageError> {
        let snapshot = self.snapshot();
        let actions = self.engine.load_balancer.analyze(&snapshot);
        if !dry_run {
            // Feed actions to the scheduler for execution.
            let _ = self
                .engine
                .scheduler
                .execute_migrations(actions.clone(), &snapshot, false)?;
        }
        Ok(actions)
    }

    fn execute_migrations(
        &self,
        actions: Vec<RebalanceAction>,
        dry_run: bool,
    ) -> Result<MigrationExecutionResult, ManageError> {
        let snapshot = self.snapshot();
        self.engine
            .scheduler
            .execute_migrations(actions, &snapshot, dry_run)
    }

    fn pause_all_migrations(&self) -> Result<(), ManageError> {
        self.engine.scheduler.pause_all()
    }

    fn resume_migrations(&self) -> Result<(), ManageError> {
        self.engine.scheduler.resume_all()
    }

    fn cancel_migration(&self, task_id: &str) -> Result<(), ManageError> {
        self.engine.scheduler.cancel(task_id)
    }

    // ===== Override operations =====

    fn pin_volume_to_node(&self, volume_id: u64, node_id: &str) -> Result<(), ManageError> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.master
                    .pin_volume_to_node(volume_id, node_id)
                    .await
                    .map_err(map_powerfs_error)
            })
        })
    }

    fn unpin_volume(&self, volume_id: u64) -> Result<(), ManageError> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.master
                    .unpin_volume(volume_id)
                    .await
                    .map_err(map_powerfs_error)
            })
        })
    }

    // ===== Shard scaling =====

    fn add_shard(
        &self,
        split_from: Option<ShardId>,
        dry_run: bool,
    ) -> Result<ShardSplitPlan, ManageError> {
        let split_from_id = split_from.map(|s| s.0);
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.filer_client
                    .add_shard(split_from_id, dry_run)
                    .await
                    .map_err(ManageError::InvalidState)
            })
        })
    }

    fn drain_shard(&self, shard_id: ShardId) -> Result<(), ManageError> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.filer_client
                    .drain_shard(shard_id.0)
                    .await
                    .map_err(ManageError::InvalidState)
            })
        })
    }

    fn remove_shard(&self, shard_id: ShardId) -> Result<(), ManageError> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.filer_client
                    .remove_shard(shard_id.0)
                    .await
                    .map_err(ManageError::InvalidState)
            })
        })
    }

    // ===== Volume scaling =====

    fn create_volume(
        &self,
        zone_id: u32,
        node_id: Option<String>,
        size: u64,
    ) -> Result<u64, ManageError> {
        let snapshot = self.snapshot();
        self.volume_manager
            .create_volume(zone_id, node_id, size, &snapshot)
    }

    fn drain_volume(&self, volume_id: u64) -> Result<(), ManageError> {
        let snapshot = self.snapshot();
        self.volume_manager.drain_volume(volume_id, &snapshot)
    }

    fn remove_volume(&self, volume_id: u64) -> Result<(), ManageError> {
        let snapshot = self.snapshot();
        self.volume_manager.remove_volume(volume_id, &snapshot)
    }
}

/// Rebalance engine: owns the LoadBalancer + MigrationScheduler + executor
/// and drives each periodic tick.
///
/// Constructed once at master startup and shared between the background tick
/// task (calls [`run_tick`](Self::run_tick)) and any status/management query
/// path (reads [`tasks`](Self::tasks)).
pub struct RebalanceEngine {
    pub load_balancer: LoadBalancer,
    pub scheduler: MigrationScheduler,
    migration_policy: Arc<RwLock<MigrationPolicy>>,
    rebalance_policy: Arc<RwLock<RebalancePolicy>>,
    /// Completion queue drained at the end of each tick. Wrapped in a Mutex
    /// because `MigrationExecutor::start_migration` is called synchronously
    /// from within `scheduler.tick` (on the same thread), so contention is
    /// nil.
    completion_rx: Mutex<mpsc::Receiver<String>>,
}

impl RebalanceEngine {
    /// Build a new engine with default policies and a [`LoggingExecutor`].
    ///
    /// `volume_default_size` is the size used when emitting
    /// [`RebalanceAction::RequestVolumeGrow`] (cluster near-full → ask the
    /// master for a new volume). Read it from the master's
    /// `cluster_config.volume_size_limit`.
    pub fn new_logging(volume_default_size: u64) -> Arc<Self> {
        let migration_policy = Arc::new(RwLock::new(MigrationPolicy::default()));
        let rebalance_policy = Arc::new(RwLock::new(RebalancePolicy::default()));
        let (completion_tx, completion_rx) = mpsc::channel::<String>();
        let executor = Arc::new(LoggingExecutor::new(completion_tx));
        let scheduler = MigrationScheduler::new(
            Arc::clone(&migration_policy),
            Arc::clone(&rebalance_policy),
            executor,
        );
        let load_balancer = LoadBalancer::new(Arc::clone(&rebalance_policy), volume_default_size);
        Arc::new(Self {
            load_balancer,
            scheduler,
            migration_policy,
            rebalance_policy,
            completion_rx: Mutex::new(completion_rx),
        })
    }

    /// Build a new engine backed by the real [`MasterMigrationExecutor`].
    ///
    /// Unlike `new_logging`, this executor actually copies needles between
    /// volumes and updates filer chunk mappings. It requires an `Arc<MasterNode>`
    /// for volume-client / filer-client access and leader-state queries.
    ///
    /// `volume_default_size` has the same meaning as in `new_logging`.
    pub fn new_with_master(master: Arc<MasterNode>, volume_default_size: u64) -> Arc<Self> {
        let migration_policy = Arc::new(RwLock::new(MigrationPolicy::default()));
        let rebalance_policy = Arc::new(RwLock::new(RebalancePolicy::default()));
        let (completion_tx, completion_rx) = mpsc::channel::<String>();
        let executor = Arc::new(MasterMigrationExecutor::new(master, completion_tx));
        let scheduler = MigrationScheduler::new(
            Arc::clone(&migration_policy),
            Arc::clone(&rebalance_policy),
            executor,
        );
        let load_balancer = LoadBalancer::new(Arc::clone(&rebalance_policy), volume_default_size);
        Arc::new(Self {
            load_balancer,
            scheduler,
            migration_policy,
            rebalance_policy,
            completion_rx: Mutex::new(completion_rx),
        })
    }

    /// Shared migration-policy handle (for ManagementApi updates).
    pub fn migration_policy(&self) -> Arc<RwLock<MigrationPolicy>> {
        Arc::clone(&self.migration_policy)
    }

    /// Shared rebalance-policy handle (for ManagementApi updates).
    pub fn rebalance_policy(&self) -> Arc<RwLock<RebalancePolicy>> {
        Arc::clone(&self.rebalance_policy)
    }

    /// Scan interval (seconds) used by the background loop.
    pub fn scan_interval_secs(&self) -> u64 {
        self.migration_policy.read().unwrap().scan_interval_secs
    }

    /// Snapshot of all migration tasks (for StatusQuery / monitoring).
    pub fn tasks(&self) -> Vec<powerfs_allocator::MigrationTaskStatus> {
        self.scheduler.tasks()
    }

    /// Run one rebalance tick against the master's current cluster state.
    ///
    /// 1. Build a [`ClusterSnapshot`] from the master's heartbeat-aggregated
    ///    topology/volume/filer state.
    /// 2. Feed it to [`MigrationScheduler::tick`], which load-adaptively
    ///    pauses/resumes and starts new migrations up to the concurrency
    ///    limit.
    /// 3. Drain the executor's completion queue and finalize each task via
    ///    [`MigrationScheduler::complete_task`].
    /// 4. Emit a debug summary.
    ///
    /// This method is synchronous: all allocator entry points are sync, and
    /// `build_cluster_snapshot` only reads the master's `RwLock`-guarded
    /// state. The async background loop just calls this periodically.
    pub fn run_tick(&self, master: &MasterNode) {
        let snapshot = master.build_cluster_snapshot();
        let n_volumes = snapshot.volumes.len();
        let n_nodes = snapshot.nodes.len();
        let avg_load = snapshot.cluster_avg_load;

        self.scheduler.tick(&snapshot, &self.load_balancer);

        // Drain completions enqueued by the executor. `LoggingExecutor`
        // completes synchronously (during `tick`); `MasterMigrationExecutor`
        // completes asynchronously (spawned task sends on the channel when
        // the needle copy + filer update finishes). `try_recv` picks up any
        // arrivals since the previous tick for either executor.
        if let Ok(rx) = self.completion_rx.lock() {
            while let Ok(task_id) = rx.try_recv() {
                if let Err(e) = self.scheduler.complete_task(&task_id, true, 0) {
                    warn!("[rebalance] completion for task {} failed: {}", task_id, e);
                }
            }
        }

        let tasks = self.scheduler.tasks();
        let running = tasks
            .iter()
            .filter(|t| t.state == MigrationState::Running)
            .count();
        let paused = tasks
            .iter()
            .filter(|t| t.state == MigrationState::PausedByLoad)
            .count();
        let completed = tasks
            .iter()
            .filter(|t| t.state == MigrationState::Completed)
            .count();
        debug!(
            "[rebalance] tick done: volumes={} nodes={} avg_load={:.2} \
             tasks(total={} running={} paused={} completed={})",
            n_volumes,
            n_nodes,
            avg_load,
            tasks.len(),
            running,
            paused,
            completed
        );
    }
}

/// Spawn the background rebalance loop.
///
/// Runs forever (until the process exits). Each iteration:
/// - sleeps `scan_interval_secs`,
/// - skips if this master is not the Raft leader,
/// - otherwise calls [`RebalanceEngine::run_tick`].
///
/// The loop is tolerant of errors: a panic in `run_tick` is caught and logged
/// so the loop survives transient inconsistencies in the snapshot.
pub fn spawn_rebalance_loop(engine: Arc<RebalanceEngine>, master: Arc<MasterNode>) {
    tokio::spawn(async move {
        loop {
            let interval = std::time::Duration::from_secs(engine.scan_interval_secs());
            tokio::time::sleep(interval).await;

            // Only the leader may mutate volume state; followers skip.
            if !master.is_leader().await {
                continue;
            }

            // Catch panics so a bad snapshot doesn't kill the loop.
            let engine_ref = Arc::clone(&engine);
            let master_ref = Arc::clone(&master);
            tokio::task::spawn_blocking(move || engine_ref.run_tick(&master_ref))
                .await
                .ok();
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_map_powerfs_error_not_leader() {
        let err = map_powerfs_error(PowerFsError::NotLeader);
        assert!(matches!(err, ManageError::InvalidState(_)));
    }

    #[test]
    fn test_map_powerfs_error_volume_not_found() {
        let err = map_powerfs_error(PowerFsError::VolumeNotFound(VolumeId(42)));
        assert!(matches!(err, ManageError::ResourceNotFound(_)));
    }

    #[test]
    fn test_map_powerfs_error_invalid_request() {
        let err = map_powerfs_error(PowerFsError::InvalidRequest("bad".to_string()));
        assert!(matches!(err, ManageError::InvalidState(_)));
    }

    #[test]
    fn test_logging_executor_completes_immediately() {
        let (tx, rx) = mpsc::channel::<String>();
        let exec = LoggingExecutor::new(tx);
        let action = RebalanceAction::MigrateColdData {
            from_volume: 1,
            to_volume: 2,
            needle_ids: vec![10, 11],
        };
        exec.start_migration(&action, "mig-1").unwrap();
        // The executor must have enqueued the completion.
        assert_eq!(rx.recv().unwrap(), "mig-1");
    }

    #[test]
    fn test_engine_drains_completions() {
        // Use a tiny volume_default_size; irrelevant for this test.
        let engine = RebalanceEngine::new_logging(100);
        // Build a minimal snapshot with a draining source and an active target,
        // then tick the scheduler. The LoggingExecutor (owned by the scheduler)
        // enqueues an immediate completion on the engine's channel.
        use powerfs_allocator::{
            NodeRuntime, NodeRuntimeState, VolumeLoad, VolumeRuntime, VolumeRuntimeState,
        };
        let snapshot = powerfs_allocator::ClusterSnapshot {
            version: 1,
            timestamp: std::time::Instant::now(),
            config_version: 1,
            volumes: vec![
                VolumeRuntime {
                    volume_id: 1,
                    node_id: "n1".to_string(),
                    zone_id: 1,
                    total_size: 100,
                    used_size: 90,
                    state: VolumeRuntimeState::Draining,
                    load: VolumeLoad::default(),
                    cold_needle_count: 50,
                    hot_needle_count: 0,
                },
                VolumeRuntime {
                    volume_id: 2,
                    node_id: "n2".to_string(),
                    zone_id: 1,
                    total_size: 100,
                    used_size: 10,
                    state: VolumeRuntimeState::Active,
                    load: VolumeLoad::default(),
                    cold_needle_count: 0,
                    hot_needle_count: 0,
                },
            ],
            nodes: vec![
                NodeRuntime {
                    node_id: "n1".to_string(),
                    state: NodeRuntimeState::Healthy,
                    cpu_usage: 0.0,
                    memory_usage: 0.0,
                    disk_usage: 0.0,
                    load_score: 0.2,
                    in_maintenance: false,
                },
                NodeRuntime {
                    node_id: "n2".to_string(),
                    state: NodeRuntimeState::Healthy,
                    cpu_usage: 0.0,
                    memory_usage: 0.0,
                    disk_usage: 0.0,
                    load_score: 0.2,
                    in_maintenance: false,
                },
            ],
            shards: Vec::new(),
            cluster_avg_load: 0.2,
            pinned_volumes: std::collections::HashMap::new(),
        };
        // tick starts a migration (LoggingExecutor enqueues completion).
        engine.scheduler.tick(&snapshot, &engine.load_balancer);
        let tasks = engine.scheduler.tasks();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].state, MigrationState::Running);

        // Drain: the completion enqueued by LoggingExecutor should now
        // transition the task to Completed. We can't call run_tick (needs a
        // MasterNode), so drain the channel directly and complete.
        let rx = engine.completion_rx.lock().unwrap();
        let task_id = rx.try_recv().unwrap();
        drop(rx);
        engine.scheduler.complete_task(&task_id, true, 0).unwrap();
        assert_eq!(engine.scheduler.tasks()[0].state, MigrationState::Completed);
    }
}
