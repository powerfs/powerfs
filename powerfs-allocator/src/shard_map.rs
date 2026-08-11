//! Range-based shard mapping (replaces modulo-based `calculate_shard`).
//!
//! The ShardMap uses a sorted range table instead of `inode % shard_count`.
//! This allows adding shards without migrating existing inodes:
//!   - Adding a shard = split an existing range, new range goes to new shard.
//!   - Existing inodes keep their routing (their range entry is unchanged).
//!   - Only new inode allocations may land in the new shard.
//!
//! On startup, `ShardMap::from_shard_count(n)` generates an initial mapping
//! that is behaviorally equivalent to the old modulo-based routing.

use std::sync::RwLock;

use crate::error::{ShardError, ShardId};

/// Shard state within the map.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ShardState {
    /// Available for new allocations.
    Active,
    /// Draining: no new allocations, existing inodes still routable.
    /// Waiting for metadata migration before removal.
    Draining,
}

/// Plan returned by a (dry-run) `add_shard` operation.
///
/// Describes which existing shard is split, at which inode boundary, and the
/// new shard id + range that will receive future allocations. Existing inodes
/// keep their original routing (no metadata migration required).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardSplitPlan {
    /// Shard whose range is being split.
    pub split_from: ShardId,
    /// Inclusive lower bound of the new shard's range (split boundary).
    pub split_point: u64,
    /// Id assigned to the new shard.
    pub new_shard_id: ShardId,
    /// `[range_start, range_end)` of the new shard.
    pub new_range: (u64, u64),
    /// Estimated count of future inode allocations that will land in the new
    /// shard (half of the split source's range size). Used for capacity
    /// planning / dry-run previews.
    pub affected_future_allocations: u64,
}

/// A single range entry in the shard map.
#[derive(Clone, Debug)]
struct ShardMapEntry {
    /// Inclusive lower bound.
    range_start: u64,
    /// Exclusive upper bound.
    range_end: u64,
    shard_id: ShardId,
    state: ShardState,
}

/// Range-based shard routing table.
///
/// Thread-safe via `RwLock`. Lookups use binary search on the sorted entries.
pub struct ShardMap {
    /// Sorted by `range_start`. Gaps are not allowed (ranges are contiguous).
    entries: RwLock<Vec<ShardMapEntry>>,
}

impl ShardMap {
    /// Create an initial ShardMap equivalent to the old modulo-based routing.
    ///
    /// Divides the inode space into `shard_count` equal ranges of 1M each,
    /// one per shard. The last shard does NOT extend to u64::MAX — instead,
    /// it also gets a 1M range. This keeps inode numbers small and
    /// predictable, which is important for:
    /// - Debugging (human-readable inode numbers)
    /// - Tool compatibility (some tools don't handle 18-digit inode numbers)
    /// - Per-node allocator offsets (large ranges produce huge offsets)
    ///
    /// Inodes outside [0, shard_count * 1M) are never allocated and will
    /// route to the last shard (binary search falls through).
    pub fn from_shard_count(shard_count: u64) -> Self {
        let inode_per_shard = u64::MAX
            .checked_div(shard_count)
            .map(|v| v.min(1_000_000))
            .unwrap_or(1_000_000);

        let mut entries = Vec::with_capacity(shard_count as usize);
        let mut start = 0u64;
        for i in 0..shard_count {
            // All shards get equal 1M ranges. The last shard does NOT extend
            // to u64::MAX — this prevents absurdly large inode numbers from
            // per-node offsets in the last shard's enormous range.
            let end = start.saturating_add(inode_per_shard);
            entries.push(ShardMapEntry {
                range_start: start,
                range_end: end,
                shard_id: ShardId(i),
                state: ShardState::Active,
            });
            start = end;
        }

        Self {
            entries: RwLock::new(entries),
        }
    }

    /// Create an empty map (for testing or manual construction).
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(Vec::new()),
        }
    }

    /// Reconstruct a ShardMap from an `entries_snapshot()` produced by another
    /// ShardMap (e.g. Master → client topology sync via S3).
    ///
    /// Each tuple = `(range_start, range_end, shard_id, state)`.
    /// Entries are sorted by `range_start` to ensure binary-search correctness.
    /// Malformed entries (state > 1) are treated as Active.
    pub fn from_entries(entries: Vec<(u64, u64, ShardId, ShardState)>) -> Self {
        let mut entries: Vec<ShardMapEntry> = entries
            .into_iter()
            .map(|(range_start, range_end, shard_id, state)| ShardMapEntry {
                range_start,
                range_end,
                shard_id,
                state,
            })
            .collect();
        entries.sort_by_key(|e| e.range_start);
        Self {
            entries: RwLock::new(entries),
        }
    }

    /// Route an inode to its shard via binary search.
    ///
    /// Returns `ShardId(0)` if the map is empty (should not happen in production).
    pub fn route(&self, inode: u64) -> ShardId {
        let entries = self.entries.read().unwrap();
        if entries.is_empty() {
            return ShardId(0);
        }
        // Binary search: find the last entry whose range_start <= inode
        let idx = entries
            .partition_point(|e| e.range_start <= inode)
            .saturating_sub(1);
        entries[idx].shard_id
    }

    /// Get all Active shard IDs (available for new allocations).
    pub fn active_shards(&self) -> Vec<ShardId> {
        let entries = self.entries.read().unwrap();
        let mut result: Vec<ShardId> = entries
            .iter()
            .filter(|e| e.state == ShardState::Active)
            .map(|e| e.shard_id)
            .collect();
        result.sort();
        result.dedup();
        result
    }

    /// Get all Draining shard IDs.
    pub fn draining_shards(&self) -> Vec<ShardId> {
        let entries = self.entries.read().unwrap();
        let mut result: Vec<ShardId> = entries
            .iter()
            .filter(|e| e.state == ShardState::Draining)
            .map(|e| e.shard_id)
            .collect();
        result.sort();
        result.dedup();
        result
    }

    /// Add a new shard by splitting an existing shard's range.
    ///
    /// The existing shard keeps `[range_start, split_point)` — existing inodes
    /// are unaffected. The new shard gets `[split_point, range_end)` — only
    /// newly allocated inodes in this range will route to the new shard.
    ///
    /// No metadata migration is required.
    pub fn add_shard(
        &self,
        new_shard_id: ShardId,
        split_from: ShardId,
        split_point: u64,
    ) -> Result<(), ShardError> {
        let mut entries = self.entries.write().unwrap();

        // Find the entry to split
        let entry_idx = entries
            .iter()
            .position(|e| e.shard_id == split_from && e.state == ShardState::Active)
            .ok_or(ShardError::ShardNotFound(split_from))?;

        let entry = &entries[entry_idx];

        // Validate split point
        if split_point <= entry.range_start || split_point >= entry.range_end {
            return Err(ShardError::InvalidSplitPoint(split_point));
        }

        // Check new shard doesn't already exist
        if entries.iter().any(|e| e.shard_id == new_shard_id) {
            return Err(ShardError::ShardAlreadyExists(new_shard_id));
        }

        // Save the original range end
        let original_end = entry.range_end;

        // Shrink the existing entry
        entries[entry_idx].range_end = split_point;

        // Insert the new entry
        let new_entry = ShardMapEntry {
            range_start: split_point,
            range_end: original_end,
            shard_id: new_shard_id,
            state: ShardState::Active,
        };
        entries.insert(entry_idx + 1, new_entry);

        Ok(())
    }

    /// Mark a shard as Draining (stop new allocations, keep routing for existing inodes).
    pub fn drain_shard(&self, shard_id: ShardId) -> Result<(), ShardError> {
        let mut entries = self.entries.write().unwrap();
        let found = entries
            .iter_mut()
            .any(|e| e.shard_id == shard_id && e.state == ShardState::Active);

        if !found {
            return Err(ShardError::ShardNotFound(shard_id));
        }

        for entry in entries.iter_mut() {
            if entry.shard_id == shard_id {
                entry.state = ShardState::Draining;
            }
        }
        Ok(())
    }

    /// Remove a shard (must be Draining first, and all inodes must be migrated).
    ///
    /// **Warning**: This removes the range entry. The caller must ensure all
    /// inodes in this range have been migrated to other shards, otherwise
    /// they will route to the wrong shard.
    pub fn remove_shard(&self, shard_id: ShardId) -> Result<(), ShardError> {
        let mut entries = self.entries.write().unwrap();

        // Check the shard exists and is Draining
        let any_draining = entries
            .iter()
            .any(|e| e.shard_id == shard_id && e.state == ShardState::Draining);
        if !any_draining {
            // Check if it exists but is Active
            let any_exists = entries.iter().any(|e| e.shard_id == shard_id);
            if any_exists {
                return Err(ShardError::ShardNotDraining(shard_id));
            }
            return Err(ShardError::ShardNotFound(shard_id));
        }

        // Merge the removed shard's range into the previous Active entry
        let remove_idx = entries.iter().position(|e| e.shard_id == shard_id).unwrap();

        if remove_idx > 0 {
            // Extend the previous entry's range_end to cover the removed entry
            let removed_end = entries[remove_idx].range_end;
            entries[remove_idx - 1].range_end = removed_end;
        }
        // If remove_idx == 0, the range is simply dropped (edge case: should not
        // happen in production as shard 0 is never drained first).

        entries.remove(remove_idx);
        Ok(())
    }

    /// Get the inode range for a shard.
    pub fn shard_range(&self, shard_id: ShardId) -> Option<(u64, u64)> {
        let entries = self.entries.read().unwrap();
        entries
            .iter()
            .find(|e| e.shard_id == shard_id)
            .map(|e| (e.range_start, e.range_end))
    }

    /// Number of range entries (not necessarily unique shards).
    pub fn entry_count(&self) -> usize {
        self.entries.read().unwrap().len()
    }

    /// Get all entries (for debugging/monitoring).
    pub fn entries_snapshot(&self) -> Vec<(u64, u64, ShardId, ShardState)> {
        self.entries
            .read()
            .unwrap()
            .iter()
            .map(|e| (e.range_start, e.range_end, e.shard_id, e.state.clone()))
            .collect()
    }

    // ===== P8: High-level shard scaling (auto-selection + dry-run) =====

    /// Auto-select the best shard to split.
    ///
    /// Strategy: pick the Active shard with the largest inode range (most
    /// room for future allocations). Returns `None` if no Active shards.
    pub fn select_split_candidate(&self) -> Option<ShardId> {
        let entries = self.entries.read().unwrap();
        entries
            .iter()
            .filter(|e| e.state == ShardState::Active)
            .max_by_key(|e| e.range_end.saturating_sub(e.range_start))
            .map(|e| e.shard_id)
    }

    /// Compute the next available shard ID (max existing + 1).
    pub fn next_shard_id(&self) -> ShardId {
        let entries = self.entries.read().unwrap();
        let max_id = entries.iter().map(|e| e.shard_id.0).max().unwrap_or(0);
        ShardId(max_id + 1)
    }

    /// Plan an `add_shard` operation without executing.
    ///
    /// - `split_from=None`: auto-selects the largest Active shard.
    /// - Split point: midpoint of the selected shard's range.
    /// - New shard ID: `next_shard_id()` (max existing + 1).
    ///
    /// Returns a `ShardSplitPlan` that can be inspected (dry-run) or
    /// passed to `execute_add_shard` for execution.
    ///
    /// **Note**: this takes a short-lived read lock only. To avoid a TOCTOU
    /// race (plan computed, then map mutated by another thread before
    /// execution), prefer [`add_shard_auto`] which plans and executes under
    /// a single write lock.
    pub fn plan_add_shard(
        &self,
        split_from: Option<ShardId>,
    ) -> Result<ShardSplitPlan, ShardError> {
        let entries = self.entries.read().unwrap();
        plan_add_shard_locked(&entries, split_from)
    }

    /// Execute a pre-computed `ShardSplitPlan`.
    ///
    /// Re-validates the plan against the current map state (the split source
    /// must still be Active, the split point must still be in range, and the
    /// new shard id must not collide). Returns an error if the map has changed
    /// incompatibly since the plan was computed.
    pub fn execute_add_shard(&self, plan: &ShardSplitPlan) -> Result<(), ShardError> {
        let mut entries = self.entries.write().unwrap();
        execute_add_shard_locked(&mut entries, plan)
    }

    /// High-level add_shard: plan + optionally execute, atomically.
    ///
    /// This is the primary entry point for the ManagementApi's `add_shard`.
    /// - `split_from=None`: auto-selects the largest Active shard.
    /// - `dry_run=true`: returns the plan without modifying the map.
    /// - `dry_run=false`: plans and executes under a single write lock, so
    ///   no concurrent scaling/routing mutation can interleave between plan
    ///   and execute.
    pub fn add_shard_auto(
        &self,
        split_from: Option<ShardId>,
        dry_run: bool,
    ) -> Result<ShardSplitPlan, ShardError> {
        let mut entries = self.entries.write().unwrap();
        let plan = plan_add_shard_locked(&entries, split_from)?;
        if !dry_run {
            execute_add_shard_locked(&mut entries, &plan)?;
        }
        Ok(plan)
    }
}

// ===== Locked helpers (operate on a pre-held guard) =====
//
// These exist so that [`ShardMap::add_shard_auto`] can plan and execute
// under a single write lock, avoiding a TOCTOU race where another thread
// mutates the map between the (read-locked) plan and the (write-locked)
// execute.

/// Plan a split against a snapshot of entries held under an external lock.
fn plan_add_shard_locked(
    entries: &[ShardMapEntry],
    split_from: Option<ShardId>,
) -> Result<ShardSplitPlan, ShardError> {
    let target = match split_from {
        Some(id) => entries
            .iter()
            .find(|e| e.shard_id == id && e.state == ShardState::Active)
            .ok_or(ShardError::ShardNotFound(id))?,
        None => {
            let candidate = entries
                .iter()
                .filter(|e| e.state == ShardState::Active)
                .max_by_key(|e| e.range_end.saturating_sub(e.range_start))
                .ok_or(ShardError::NoActiveShardToSplit)?;
            candidate
        }
    };

    let range_size = target.range_end.saturating_sub(target.range_start);
    let split_point = target.range_start + range_size / 2;
    let new_id = ShardId(entries.iter().map(|e| e.shard_id.0).max().unwrap_or(0) + 1);

    // Estimate: half the range's future allocations go to the new shard.
    let affected = range_size / 2;

    Ok(ShardSplitPlan {
        split_from: target.shard_id,
        split_point,
        new_shard_id: new_id,
        new_range: (split_point, target.range_end),
        affected_future_allocations: affected,
    })
}

/// Execute a split against entries held under an external write lock.
///
/// Mirrors the validation in [`ShardMap::add_shard`] so that a stale plan
/// (e.g. computed against an older map state) is rejected rather than
/// corrupting the map.
fn execute_add_shard_locked(
    entries: &mut Vec<ShardMapEntry>,
    plan: &ShardSplitPlan,
) -> Result<(), ShardError> {
    let entry_idx = entries
        .iter()
        .position(|e| e.shard_id == plan.split_from && e.state == ShardState::Active)
        .ok_or(ShardError::ShardNotFound(plan.split_from))?;

    let entry = &entries[entry_idx];

    if plan.split_point <= entry.range_start || plan.split_point >= entry.range_end {
        return Err(ShardError::InvalidSplitPoint(plan.split_point));
    }

    if entries.iter().any(|e| e.shard_id == plan.new_shard_id) {
        return Err(ShardError::ShardAlreadyExists(plan.new_shard_id));
    }

    let original_end = entry.range_end;
    entries[entry_idx].range_end = plan.split_point;

    let new_entry = ShardMapEntry {
        range_start: plan.split_point,
        range_end: original_end,
        shard_id: plan.new_shard_id,
        state: ShardState::Active,
    };
    entries.insert(entry_idx + 1, new_entry);

    Ok(())
}

impl Default for ShardMap {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_shard_count_routing() {
        let map = ShardMap::from_shard_count(3);

        // Inode 0 should route to shard 0
        assert_eq!(map.route(0), ShardId(0));
        // Inode 1_000_000 should route to shard 1
        assert_eq!(map.route(1_000_000), ShardId(1));
        // Inode 2_000_000 should route to shard 2
        assert_eq!(map.route(2_000_000), ShardId(2));
        // Inode u64::MAX should route to shard 2 (last shard)
        assert_eq!(map.route(u64::MAX), ShardId(2));
    }

    #[test]
    fn test_add_shard_no_migration() {
        let map = ShardMap::from_shard_count(3);

        // Split shard 1 at 1_500_000
        map.add_shard(ShardId(3), ShardId(1), 1_500_000).unwrap();

        // Existing inodes keep their routing
        assert_eq!(map.route(1_000_000), ShardId(1)); // still shard 1
        assert_eq!(map.route(1_499_999), ShardId(1)); // still shard 1

        // New inodes in the split range route to shard 3
        assert_eq!(map.route(1_500_000), ShardId(3));
        assert_eq!(map.route(1_999_999), ShardId(3));

        // Shard 2 is unaffected
        assert_eq!(map.route(2_000_000), ShardId(2));

        // 4 entries now
        assert_eq!(map.entry_count(), 4);
        assert_eq!(map.active_shards().len(), 4);
    }

    #[test]
    fn test_drain_shard() {
        let map = ShardMap::from_shard_count(3);

        map.drain_shard(ShardId(1)).unwrap();

        // Routing still works (Draining shards are still in the map)
        assert_eq!(map.route(1_000_000), ShardId(1));

        // But not in active_shards
        let active = map.active_shards();
        assert!(!active.contains(&ShardId(1)));
        assert_eq!(active.len(), 2);

        // In draining_shards
        let draining = map.draining_shards();
        assert!(draining.contains(&ShardId(1)));
    }

    #[test]
    fn test_remove_shard_requires_draining() {
        let map = ShardMap::from_shard_count(3);

        // Can't remove Active shard
        let err = map.remove_shard(ShardId(1)).unwrap_err();
        assert!(matches!(err, ShardError::ShardNotDraining(_)));

        // Drain then remove
        map.drain_shard(ShardId(1)).unwrap();
        map.remove_shard(ShardId(1)).unwrap();

        // Shard 0's range now extends to cover shard 1's old range
        let (start, end) = map.shard_range(ShardId(0)).unwrap();
        assert_eq!(start, 0);
        assert_eq!(end, 2_000_000); // shard 0 + shard 1 merged
    }

    #[test]
    fn test_shard_range() {
        let map = ShardMap::from_shard_count(3);

        let (start, end) = map.shard_range(ShardId(0)).unwrap();
        assert_eq!(start, 0);
        assert_eq!(end, 1_000_000);

        let (start, end) = map.shard_range(ShardId(2)).unwrap();
        assert_eq!(start, 2_000_000);
        assert_eq!(end, 3_000_000); // capped at 1M, not u64::MAX
    }

    #[test]
    fn test_invalid_split_point() {
        let map = ShardMap::from_shard_count(3);

        // Split at range_start (invalid)
        let err = map
            .add_shard(ShardId(3), ShardId(1), 1_000_000)
            .unwrap_err();
        assert!(matches!(err, ShardError::InvalidSplitPoint(_)));

        // Split at range_end (invalid)
        let err = map
            .add_shard(ShardId(3), ShardId(1), 2_000_000)
            .unwrap_err();
        assert!(matches!(err, ShardError::InvalidSplitPoint(_)));
    }

    #[test]
    fn test_duplicate_shard_id() {
        let map = ShardMap::from_shard_count(3);

        let err = map.add_shard(ShardId(1), ShardId(0), 500_000).unwrap_err();
        assert!(matches!(err, ShardError::ShardAlreadyExists(_)));
    }

    #[test]
    fn test_plan_add_shard_no_active_returns_no_active_error() {
        let map = ShardMap::from_shard_count(3);
        // Drain all shards → no Active shard to split.
        map.drain_shard(ShardId(0)).unwrap();
        map.drain_shard(ShardId(1)).unwrap();
        map.drain_shard(ShardId(2)).unwrap();

        let err = map.plan_add_shard(None).unwrap_err();
        assert!(matches!(err, ShardError::NoActiveShardToSplit));
    }

    #[test]
    fn test_add_shard_auto_concurrent_no_collisions() {
        // Many threads add_shard_auto concurrently. Because each call plans
        // and executes under a single write lock, every new shard id must be
        // unique and the final active count must equal initial + num_adds.
        let map = std::sync::Arc::new(ShardMap::from_shard_count(2));
        let n = 16;

        let handles: Vec<_> = (0..n)
            .map(|_| {
                let m = map.clone();
                std::thread::spawn(move || m.add_shard_auto(None, false).map(|p| p.new_shard_id))
            })
            .collect();

        let mut new_ids: Vec<ShardId> = Vec::new();
        for h in handles {
            new_ids.push(h.join().unwrap().unwrap());
        }

        // All new shard ids must be distinct (no TOCTOU collisions).
        let mut sorted = new_ids.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(
            sorted.len(),
            n as usize,
            "duplicate new shard ids: {new_ids:?}"
        );

        // Final active count = initial (2) + successful adds (n).
        assert_eq!(map.active_shards().len(), 2 + n as usize);
    }

    #[test]
    fn test_from_entries_roundtrip() {
        // from_entries(entries_snapshot()) should produce an equivalent map.
        let original = ShardMap::from_shard_count(4);
        let snapshot = original.entries_snapshot();
        let reconstructed = ShardMap::from_entries(snapshot);

        // Routing must match for all inodes in [0, 4M).
        for inode in [0u64, 1, 999_999, 1_000_000, 2_000_001, 3_500_000] {
            assert_eq!(
                original.route(inode),
                reconstructed.route(inode),
                "inode {} routed differently after roundtrip",
                inode
            );
        }
        assert_eq!(original.entry_count(), reconstructed.entry_count());
        assert_eq!(original.active_shards(), reconstructed.active_shards());
    }

    #[test]
    fn test_from_entries_unsorted_input() {
        // from_entries should sort by range_start internally.
        let entries = vec![
            (2_000_000, 3_000_000, ShardId(2), ShardState::Active),
            (0, 1_000_000, ShardId(0), ShardState::Active),
            (1_000_000, 2_000_000, ShardId(1), ShardState::Active),
        ];
        let map = ShardMap::from_entries(entries);
        // Binary search requires sorted entries; if unsorted, route() would
        // return wrong results.
        assert_eq!(map.route(500_000), ShardId(0));
        assert_eq!(map.route(1_500_000), ShardId(1));
        assert_eq!(map.route(2_500_000), ShardId(2));
    }

    #[test]
    fn test_from_entries_with_draining() {
        // Reconstruct a map that includes a Draining shard.
        let entries = vec![
            (0, 1_000_000, ShardId(0), ShardState::Active),
            (1_000_000, 2_000_000, ShardId(1), ShardState::Draining),
        ];
        let map = ShardMap::from_entries(entries);
        assert_eq!(map.active_shards(), vec![ShardId(0)]);
        assert_eq!(map.draining_shards(), vec![ShardId(1)]);
        // Draining shards still route correctly.
        assert_eq!(map.route(1_500_000), ShardId(1));
    }
}
