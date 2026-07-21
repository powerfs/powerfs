use std::sync::RwLock;

use crate::raft_group_manager::ShardId;

pub struct ShardStrategy {
    shard_count: RwLock<u64>,
    inode_per_shard: u64,
}

impl ShardStrategy {
    pub fn new(shard_count: u64) -> Self {
        let inode_per_shard = Self::calculate_inode_per_shard(shard_count);

        Self {
            shard_count: RwLock::new(shard_count),
            inode_per_shard,
        }
    }

    fn calculate_inode_per_shard(shard_count: u64) -> u64 {
        u64::MAX
            .checked_div(shard_count)
            .map(|v| v.min(1_000_000))
            .unwrap_or(1_000_000)
    }

    pub fn calculate_shard(&self, inode: u64) -> ShardId {
        let shard_count = *self.shard_count.read().unwrap();

        if shard_count == 0 {
            return ShardId(0);
        }

        let shard_key = inode / self.inode_per_shard;
        ShardId(shard_key % shard_count)
    }

    pub fn get_shard_range(&self, shard_id: ShardId) -> (u64, u64) {
        let shard_count = *self.shard_count.read().unwrap();

        if shard_count == 0 {
            return (0, u64::MAX);
        }

        let start = shard_id.0 * self.inode_per_shard;
        let end = if shard_id.0 == shard_count - 1 {
            u64::MAX
        } else {
            (shard_id.0 + 1) * self.inode_per_shard
        };

        (start, end)
    }

    pub fn get_shard_count(&self) -> u64 {
        *self.shard_count.read().unwrap()
    }

    pub fn set_shard_count(&self, new_count: u64) {
        *self.shard_count.write().unwrap() = new_count;
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_shard() {
        let strategy = ShardStrategy::new(100);

        assert_eq!(strategy.calculate_shard(0).0, 0);
        assert_eq!(strategy.calculate_shard(999_999).0, 0);
        assert_eq!(strategy.calculate_shard(1_000_000).0, 1);
        assert_eq!(strategy.calculate_shard(1_500_000).0, 1);
        assert_eq!(strategy.calculate_shard(99_000_000).0, 99);
    }

    #[test]
    fn test_get_shard_range() {
        let strategy = ShardStrategy::new(100);

        let (start, end) = strategy.get_shard_range(ShardId(0));
        assert_eq!(start, 0);
        assert_eq!(end, 1_000_000);

        let (start, end) = strategy.get_shard_range(ShardId(50));
        assert_eq!(start, 50_000_000);
        assert_eq!(end, 51_000_000);

        let (start, end) = strategy.get_shard_range(ShardId(99));
        assert_eq!(start, 99_000_000);
        assert_eq!(end, u64::MAX);
    }

    #[test]
    fn test_find_best_split_point() {
        let strategy = ShardStrategy::new(100);

        let directories = vec![100_000, 200_000, 300_000, 400_000, 500_000];
        let split_point = strategy.find_best_split_point(ShardId(0), &directories);
        assert_eq!(split_point, 300_000);
    }
}
