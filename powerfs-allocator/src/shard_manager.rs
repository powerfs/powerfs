//! ShardManager — adapter wiring the shard-scaling portion of
//! [`ManagementApi`] to [`ShardMap`].
//!
//! This is the seam between the management API (which speaks `ManageError`)
//! and the low-level `ShardMap` (which speaks `ShardError`). Services that
//! implement the full `ManagementApi` trait can delegate the shard-scaling
//! methods to a `ShardManager` instance.
//!
//! ## Why a separate adapter?
//!
//! `ShardMap` is a pure routing data structure and should not depend on the
//! management error taxonomy. `ShardManager` owns the `ShardError →
//! ManageError` mapping, keeps the shard-scaling operations testable in
//! isolation, and gives the full `ManagementApi` implementation a single
//! composeable component.

use std::sync::Arc;

use crate::error::{ManageError, ShardError, ShardId};
use crate::shard_map::{ShardMap, ShardSplitPlan};

/// Adapter that exposes shard-scaling operations on top of a [`ShardMap`].
///
/// Holds the map behind an `Arc` so it can be shared with the routing path
/// (which reads the same map on every `calculate_shard` call). All write
/// operations go through `ShardMap`'s internal `RwLock`.
pub struct ShardManager {
    map: Arc<ShardMap>,
}

impl ShardManager {
    /// Wrap an existing, shared `ShardMap`.
    ///
    /// The same `Arc<ShardMap>` should be handed to the routing layer so that
    /// scaling decisions and routing see a single source of truth.
    pub fn new(map: Arc<ShardMap>) -> Self {
        Self { map }
    }

    /// Access the underlying map (e.g. for routing or inspection).
    pub fn map(&self) -> &ShardMap {
        &self.map
    }

    /// Add a shard by splitting an existing shard's range.
    ///
    /// - `split_from = None`: auto-selects the largest Active shard.
    /// - `dry_run = true`: returns the split plan without modifying the map.
    /// - `dry_run = false`: plans and executes atomically.
    ///
    /// Existing inodes keep their routing; only future allocations in the
    /// split range land on the new shard. No metadata migration is required.
    pub fn add_shard(
        &self,
        split_from: Option<ShardId>,
        dry_run: bool,
    ) -> Result<ShardSplitPlan, ManageError> {
        self.map
            .add_shard_auto(split_from, dry_run)
            .map_err(shard_err_to_manage)
    }

    /// Mark a shard as Draining: stop new allocations, keep routing for
    /// existing inodes.
    pub fn drain_shard(&self, shard_id: ShardId) -> Result<(), ManageError> {
        self.map.drain_shard(shard_id).map_err(shard_err_to_manage)
    }

    /// Remove a drained shard.
    ///
    /// The shard must be Draining first and all of its inodes must have been
    /// migrated to other shards; otherwise the shard's range is merged into
    /// the previous Active entry and existing inodes would mis-route.
    pub fn remove_shard(&self, shard_id: ShardId) -> Result<(), ManageError> {
        self.map.remove_shard(shard_id).map_err(shard_err_to_manage)
    }

    /// Convenience: return a dry-run split plan without executing.
    pub fn plan_add_shard(
        &self,
        split_from: Option<ShardId>,
    ) -> Result<ShardSplitPlan, ManageError> {
        self.map
            .plan_add_shard(split_from)
            .map_err(shard_err_to_manage)
    }
}

/// Map low-level [`ShardError`] to management-level [`ManageError`].
///
/// `ShardError::ShardNotFound` / `ShardNotDraining` map to `ResourceNotFound`
/// (caller referenced a non-existent or wrong-state shard). `ShardAlreadyExists`
/// and `InvalidSplitPoint` / `NoActiveShardToSplit` map to `InvalidState`
/// (the cluster isn't in a state that admits the requested operation).
fn shard_err_to_manage(err: ShardError) -> ManageError {
    match err {
        ShardError::ShardNotFound(id) => {
            ManageError::ResourceNotFound(format!("shard {id} not found"))
        }
        ShardError::ShardNotDraining(id) => {
            ManageError::InvalidState(format!("shard {id} is not draining, cannot remove"))
        }
        ShardError::ShardAlreadyExists(id) => {
            ManageError::InvalidState(format!("shard {id} already exists"))
        }
        ShardError::InvalidSplitPoint(p) => {
            ManageError::InvalidState(format!("invalid split point {p}"))
        }
        ShardError::NoActiveShardToSplit => {
            ManageError::InvalidState("no active shard available to split".to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manager_with(n: u64) -> ShardManager {
        ShardManager::new(Arc::new(ShardMap::from_shard_count(n)))
    }

    #[test]
    fn test_add_shard_dry_run_does_not_mutate() {
        let mgr = manager_with(3);

        let plan = mgr.add_shard(None, true).unwrap();
        // Dry-run: no new entry should exist.
        assert_eq!(mgr.map().active_shards().len(), 3);
        assert_eq!(plan.new_shard_id, ShardId(3));
        // Plan points at the largest active shard (shard 2: [2M, u64::MAX)).
        assert_eq!(plan.split_from, ShardId(2));
    }

    #[test]
    fn test_add_shard_execute_grows_active_count() {
        let mgr = manager_with(3);

        let plan = mgr.add_shard(None, false).unwrap();
        assert_eq!(mgr.map().active_shards().len(), 4);
        assert!(mgr.map().active_shards().contains(&plan.new_shard_id));

        // Existing inodes in the split source's lower range keep routing.
        let source_range = mgr.map().shard_range(plan.split_from).unwrap();
        assert_eq!(mgr.map().route(source_range.0), plan.split_from);
        // New range routes to the new shard.
        assert_eq!(mgr.map().route(plan.split_point), plan.new_shard_id);
    }

    #[test]
    fn test_add_shard_explicit_source() {
        let mgr = manager_with(3);

        let plan = mgr.add_shard(Some(ShardId(0)), false).unwrap();
        assert_eq!(plan.split_from, ShardId(0));
        assert_eq!(mgr.map().active_shards().len(), 4);
    }

    #[test]
    fn test_add_shard_unknown_source_rejected() {
        let mgr = manager_with(3);

        let err = mgr.add_shard(Some(ShardId(99)), false).unwrap_err();
        assert!(matches!(err, ManageError::ResourceNotFound(_)));
        // Map untouched.
        assert_eq!(mgr.map().active_shards().len(), 3);
    }

    #[test]
    fn test_drain_then_remove() {
        let mgr = manager_with(3);

        mgr.drain_shard(ShardId(1)).unwrap();
        let active = mgr.map().active_shards();
        assert_eq!(active.len(), 2);
        assert!(!active.contains(&ShardId(1)));

        mgr.remove_shard(ShardId(1)).unwrap();
        // After removal, shard 1 is gone entirely.
        assert!(mgr.map().shard_range(ShardId(1)).is_none());
    }

    #[test]
    fn test_remove_active_shard_rejected() {
        let mgr = manager_with(3);

        let err = mgr.remove_shard(ShardId(1)).unwrap_err();
        assert!(matches!(err, ManageError::InvalidState(_)));
        assert_eq!(mgr.map().active_shards().len(), 3);
    }

    #[test]
    fn test_plan_add_shard_matches_execute() {
        let mgr = manager_with(3);

        let planned = mgr.plan_add_shard(None).unwrap();
        // Executing the same plan should succeed and produce identical output.
        let executed = mgr.add_shard(None, false).unwrap();
        assert_eq!(planned.split_from, executed.split_from);
        assert_eq!(planned.new_shard_id, executed.new_shard_id);
    }

    #[test]
    fn test_shared_arc_visibility() {
        // The routing path holds the same Arc<ShardMap>; scaling must be
        // visible to it immediately.
        let map = Arc::new(ShardMap::from_shard_count(3));
        let mgr = ShardManager::new(map.clone());

        assert_eq!(map.route(2_500_000), ShardId(2));
        let plan = mgr.add_shard(Some(ShardId(2)), false).unwrap();
        // After the split, the upper half of shard 2's range now routes to
        // the new shard, visible through the shared Arc.
        assert_eq!(map.route(plan.split_point), plan.new_shard_id);
    }
}
