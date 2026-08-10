use std::sync::RwLock;

use crate::raft_group_manager::ShardId;
use powerfs_allocator::ShardMap;

/// Shard routing strategy backed by [`ShardMap`] (range-based routing).
///
/// Replaces the old modulo-based `inode / inode_per_shard % shard_count`
/// with a range table lookup. Range-based routing is simpler, more
/// efficient (binary search on a small sorted array), and supports
/// shard scaling (add/drain/remove) without migrating existing inodes.
pub struct ShardStrategy {
    shard_count: RwLock<u64>,
    inode_per_shard: u64,
    shard_map: RwLock<ShardMap>,
}

impl ShardStrategy {
    pub fn new(shard_count: u64) -> Self {
        let inode_per_shard = Self::calculate_inode_per_shard(shard_count);
        let shard_map = ShardMap::from_shard_count(shard_count);

        Self {
            shard_count: RwLock::new(shard_count),
            inode_per_shard,
            shard_map: RwLock::new(shard_map),
        }
    }

    fn calculate_inode_per_shard(shard_count: u64) -> u64 {
        u64::MAX
            .checked_div(shard_count)
            .map(|v| v.min(1_000_000))
            .unwrap_or(1_000_000)
    }

    /// Route an inode to its shard via range-based lookup.
    pub fn calculate_shard(&self, inode: u64) -> ShardId {
        let map = self.shard_map.read().unwrap();
        ShardId(map.route(inode).0)
    }

    /// Get the inode range [start, end) for a shard.
    pub fn get_shard_range(&self, shard_id: ShardId) -> (u64, u64) {
        let map = self.shard_map.read().unwrap();
        let alloc_id = powerfs_allocator::ShardId(shard_id.0);
        map.shard_range(alloc_id).unwrap_or((0, u64::MAX))
    }

    pub fn get_shard_count(&self) -> u64 {
        *self.shard_count.read().unwrap()
    }

    /// Rebuild the shard map with a new shard count.
    ///
    /// **Warning**: This changes routing for all inodes. Use only during
    /// initial setup or controlled migration. For online shard scaling,
    /// use `add_shard` / `drain_shard` instead.
    pub fn set_shard_count(&self, new_count: u64) {
        *self.shard_count.write().unwrap() = new_count;
        let new_map = ShardMap::from_shard_count(new_count);
        *self.shard_map.write().unwrap() = new_map;
    }

    pub fn get_inode_per_shard(&self) -> u64 {
        self.inode_per_shard
    }

    pub fn find_best_split_point(&self, shard_id: ShardId, directories: &[u64]) -> u64 {
        if directories.is_empty() {
            let (start, end) = self.get_shard_range(shard_id);
            return start + (end - start) / 2;
        }

        let mid_index = directories.len() / 2;
        directories[mid_index]
    }

    /// Add a new shard by splitting an existing shard's range.
    /// Existing inodes keep their routing; only new inodes in the split
    /// range route to the new shard. No metadata migration required.
    pub fn add_shard(
        &self,
        new_shard_id: ShardId,
        split_from: ShardId,
        split_point: u64,
    ) -> Result<(), String> {
        let map = self.shard_map.read().unwrap();
        map.add_shard(
            powerfs_allocator::ShardId(new_shard_id.0),
            powerfs_allocator::ShardId(split_from.0),
            split_point,
        )
        .map_err(|e| e.to_string())?;
        drop(map);

        // Increment shard count
        let mut count = self.shard_count.write().unwrap();
        *count += 1;
        Ok(())
    }

    /// Add a shard via the auto-scaling path (plan + optionally execute).
    ///
    /// - `split_from = None`: auto-selects the largest Active shard.
    /// - `dry_run = true`: returns the split plan without modifying the map.
    /// - `dry_run = false`: plans and executes atomically, then bumps the
    ///   shard count.
    ///
    /// This is the filer-side entry point for the ManagementApi's
    /// `add_shard` operation. Existing inodes keep their routing; only future
    /// allocations in the split range land on the new shard.
    pub fn add_shard_auto(
        &self,
        split_from: Option<ShardId>,
        dry_run: bool,
    ) -> Result<powerfs_allocator::ShardSplitPlan, String> {
        let map = self.shard_map.read().unwrap();
        let plan = map
            .add_shard_auto(split_from.map(|s| powerfs_allocator::ShardId(s.0)), dry_run)
            .map_err(|e| e.to_string())?;
        drop(map);

        if !dry_run {
            let mut count = self.shard_count.write().unwrap();
            *count += 1;
        }
        Ok(plan)
    }

    /// Mark a shard as Draining (stop new allocations).
    pub fn drain_shard(&self, shard_id: ShardId) -> Result<(), String> {
        let map = self.shard_map.read().unwrap();
        map.drain_shard(powerfs_allocator::ShardId(shard_id.0))
            .map_err(|e| e.to_string())
    }

    /// Remove a Draining shard and merge its range back into the previous
    /// Active entry. The shard must be Draining and have no active inodes.
    pub fn remove_shard(&self, shard_id: ShardId) -> Result<(), String> {
        let map = self.shard_map.read().unwrap();
        map.remove_shard(powerfs_allocator::ShardId(shard_id.0))
            .map_err(|e| e.to_string())?;
        drop(map);

        let mut count = self.shard_count.write().unwrap();
        if *count > 0 {
            *count -= 1;
        }
        Ok(())
    }

    /// Get all Active shard IDs (available for new allocations).
    pub fn active_shards(&self) -> Vec<ShardId> {
        let map = self.shard_map.read().unwrap();
        map.active_shards()
            .into_iter()
            .map(|s| ShardId(s.0))
            .collect()
    }

    /// Access the underlying ShardMap (for advanced operations).
    pub fn shard_map(&self) -> std::sync::RwLockReadGuard<'_, ShardMap> {
        self.shard_map.read().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_shard() {
        let strategy = ShardStrategy::new(3);

        // Range-based routing: [0, 1M) → shard 0, [1M, 2M) → shard 1, [2M, MAX) → shard 2
        assert_eq!(strategy.calculate_shard(0).0, 0);
        assert_eq!(strategy.calculate_shard(999_999).0, 0);
        assert_eq!(strategy.calculate_shard(1_000_000).0, 1);
        assert_eq!(strategy.calculate_shard(1_500_000).0, 1);
        assert_eq!(strategy.calculate_shard(2_000_000).0, 2);
        assert_eq!(strategy.calculate_shard(u64::MAX).0, 2);
    }

    #[test]
    fn test_get_shard_range() {
        let strategy = ShardStrategy::new(3);

        let (start, end) = strategy.get_shard_range(ShardId(0));
        assert_eq!(start, 0);
        assert_eq!(end, 1_000_000);

        let (start, end) = strategy.get_shard_range(ShardId(1));
        assert_eq!(start, 1_000_000);
        assert_eq!(end, 2_000_000);

        let (start, end) = strategy.get_shard_range(ShardId(2));
        assert_eq!(start, 2_000_000);
        assert_eq!(end, u64::MAX);
    }

    #[test]
    fn test_add_shard_no_migration() {
        let strategy = ShardStrategy::new(3);

        // Split shard 1 at 1_500_000
        strategy
            .add_shard(ShardId(3), ShardId(1), 1_500_000)
            .unwrap();

        // Existing inodes keep routing
        assert_eq!(strategy.calculate_shard(1_000_000).0, 1);
        assert_eq!(strategy.calculate_shard(1_499_999).0, 1);

        // New inodes in split range route to shard 3
        assert_eq!(strategy.calculate_shard(1_500_000).0, 3);
        assert_eq!(strategy.calculate_shard(1_999_999).0, 3);

        // Shard 2 unaffected
        assert_eq!(strategy.calculate_shard(2_000_000).0, 2);
    }

    #[test]
    fn test_find_best_split_point() {
        let strategy = ShardStrategy::new(3);

        let directories = vec![100_000, 200_000, 300_000, 400_000, 500_000];
        let split_point = strategy.find_best_split_point(ShardId(0), &directories);
        assert_eq!(split_point, 300_000);
    }

    #[test]
    fn test_active_shards() {
        let strategy = ShardStrategy::new(3);
        let active = strategy.active_shards();
        assert_eq!(active.len(), 3);

        strategy.drain_shard(ShardId(1)).unwrap();
        let active = strategy.active_shards();
        assert_eq!(active.len(), 2);
        assert!(!active.contains(&ShardId(1)));
    }

    #[test]
    fn test_add_shard_auto_dry_run_no_count_change() {
        let strategy = ShardStrategy::new(3);
        let start_count = strategy.get_shard_count();

        let plan = strategy.add_shard_auto(None, true).unwrap();
        // Dry-run: shard count and routing must be unchanged.
        assert_eq!(strategy.get_shard_count(), start_count);
        assert_eq!(strategy.active_shards().len(), 3);
        // Plan selects the largest active shard (shard 2: [2M, u64::MAX)).
        assert_eq!(plan.split_from.0, 2);
        assert_eq!(plan.new_shard_id.0, 3);
    }

    #[test]
    fn test_add_shard_auto_execute_bumps_count_and_routes() {
        let strategy = ShardStrategy::new(3);

        let plan = strategy.add_shard_auto(None, false).unwrap();
        // Shard count bumped, new shard is active.
        assert_eq!(strategy.get_shard_count(), 4);
        assert!(strategy
            .active_shards()
            .iter()
            .any(|s| s.0 == plan.new_shard_id.0));

        // Inodes at/after the split point route to the new shard.
        assert_eq!(
            strategy.calculate_shard(plan.split_point).0,
            plan.new_shard_id.0
        );
        // The split source's lower range keeps routing to the source shard.
        let (src_start, _) = strategy.get_shard_range(ShardId(plan.split_from.0));
        assert_eq!(strategy.calculate_shard(src_start).0, plan.split_from.0);
    }
}
