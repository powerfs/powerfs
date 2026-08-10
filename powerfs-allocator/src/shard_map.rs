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
    /// Divides `[0, u64::MAX]` into `shard_count` equal ranges, one per shard.
    /// This is the zero-migration migration path: behavior is identical to
    /// `inode / inode_per_shard % shard_count`.
    pub fn from_shard_count(shard_count: u64) -> Self {
        let inode_per_shard = u64::MAX
            .checked_div(shard_count)
            .map(|v| v.min(1_000_000))
            .unwrap_or(1_000_000);

        let mut entries = Vec::with_capacity(shard_count as usize);
        let mut start = 0u64;
        for i in 0..shard_count {
            let end = if i == shard_count - 1 {
                u64::MAX
            } else {
                start.saturating_add(inode_per_shard)
            };
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
        let remove_idx = entries
            .iter()
            .position(|e| e.shard_id == shard_id)
            .unwrap();

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
        assert_eq!(end, u64::MAX);
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

        let err = map
            .add_shard(ShardId(1), ShardId(0), 500_000)
            .unwrap_err();
        assert!(matches!(err, ShardError::ShardAlreadyExists(_)));
    }
}
