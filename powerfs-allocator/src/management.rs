//! Management API trait (Module 6 — write operations, for admin & AI Agent).
//!
//! Supports dry-run mode for all destructive/expensive operations.

use crate::config::{MigrationPolicy, RebalancePolicy};
use crate::error::{ManageError, ShardId};

/// Rebalance action — output of LoadBalancer analysis.
#[derive(Clone, Debug)]
pub enum RebalanceAction {
    /// Migrate cold data from a near-full volume to a free one.
    MigrateColdData {
        from_volume: u64,
        to_volume: u64,
        needle_ids: Vec<u64>,
    },
    /// Migrate hot data from a busy node to an idle one.
    MigrateHotData {
        from_node: String,
        to_node: String,
        volume_ids: Vec<u64>,
    },
    /// Request Master to create a new volume.
    RequestVolumeGrow {
        zone_id: u32,
        size: u64,
    },
}

/// Result of a migration execution attempt.
#[derive(Clone, Debug)]
pub struct MigrationExecutionResult {
    pub accepted_task_ids: Vec<String>,
    pub rejected: Vec<MigrationRejection>,
}

/// Why a migration action was rejected.
#[derive(Clone, Debug)]
pub struct MigrationRejection {
    pub action: RebalanceAction,
    pub reason: RejectionReason,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RejectionReason {
    /// Needle has an active lease (client is writing).
    HasActiveLease,
    /// Target volume is full.
    VolumeFull,
    /// Target node is in maintenance.
    NodeInMaintenance,
}

/// Shard split plan (returned by dry-run `add_shard`).
#[derive(Clone, Debug)]
pub struct ShardSplitPlan {
    pub split_from: ShardId,
    pub split_point: u64,
    pub new_shard_id: ShardId,
    pub new_range: (u64, u64),
    /// Estimated future inodes affected.
    pub affected_future_allocations: u64,
}

/// Management API — write operations. Requires auth (TBD, may reuse RBAC).
pub trait ManagementApi: Send + Sync {
    // ===== Configuration management =====

    /// Switch placement strategy by name.
    fn set_placement_strategy(&self, strategy: &str) -> Result<(), ManageError>;

    /// Update migration throttling policy.
    fn update_migration_policy(&self, policy: MigrationPolicy) -> Result<(), ManageError>;

    /// Update rebalance trigger thresholds.
    fn update_rebalance_policy(&self, policy: RebalancePolicy) -> Result<(), ManageError>;

    /// Put a node into maintenance mode (excluded from allocation + drained).
    fn set_node_maintenance(&self, node_id: &str, enabled: bool) -> Result<(), ManageError>;

    // ===== Migration control =====

    /// Trigger a rebalance check.
    /// `dry_run=true` returns suggestions without executing.
    fn trigger_rebalance_check(
        &self,
        dry_run: bool,
    ) -> Result<Vec<RebalanceAction>, ManageError>;

    /// Execute migration actions.
    /// `dry_run=true` validates without executing, returning accept/reject.
    fn execute_migrations(
        &self,
        actions: Vec<RebalanceAction>,
        dry_run: bool,
    ) -> Result<MigrationExecutionResult, ManageError>;

    /// Pause all running migrations (emergency).
    fn pause_all_migrations(&self) -> Result<(), ManageError>;

    /// Resume paused migrations.
    fn resume_migrations(&self) -> Result<(), ManageError>;

    /// Cancel a specific migration task.
    fn cancel_migration(&self, task_id: &str) -> Result<(), ManageError>;

    // ===== Override operations (ops) =====

    /// Pin a volume to a specific node (overrides strategy).
    fn pin_volume_to_node(&self, volume_id: u64, node_id: &str) -> Result<(), ManageError>;

    /// Remove a pin.
    fn unpin_volume(&self, volume_id: u64) -> Result<(), ManageError>;

    // ===== Shard scaling =====

    /// Add a shard by splitting an existing shard's range.
    /// `split_from=None` auto-selects the busiest Active shard.
    /// `dry_run=true` returns the split plan without executing.
    fn add_shard(
        &self,
        split_from: Option<ShardId>,
        dry_run: bool,
    ) -> Result<ShardSplitPlan, ManageError>;

    /// Mark a shard as Draining (stop new allocations).
    fn drain_shard(&self, shard_id: ShardId) -> Result<(), ManageError>;

    /// Remove a drained shard (all inodes must be migrated first).
    fn remove_shard(&self, shard_id: ShardId) -> Result<(), ManageError>;

    // ===== Volume scaling =====

    /// Manually create a new volume.
    /// `node_id=None` auto-selects the least-loaded node.
    fn create_volume(
        &self,
        zone_id: u32,
        node_id: Option<String>,
        size: u64,
    ) -> Result<u64, ManageError>;

    /// Mark a volume as Draining (triggers data migration).
    fn drain_volume(&self, volume_id: u64) -> Result<(), ManageError>;

    /// Remove a drained volume (all data must be migrated first).
    fn remove_volume(&self, volume_id: u64) -> Result<(), ManageError>;
}
