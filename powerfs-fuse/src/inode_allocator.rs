//! Inode 分配器
//!
//! Phase 1A：本地递增分配（从客户端专属范围开始，避免多客户端冲突）
//! Phase 1B：升级为 Master 预分配范围

use std::sync::atomic::{AtomicU64, Ordering};

/// Inode 分配起点（避开根 inode=1 和系统保留段）
const INODE_START: u64 = 100;

/// 每个客户端的 inode 范围大小
const INODE_RANGE_SIZE: u64 = 1_000_000;

pub struct InodeAllocator {
    next_inode: AtomicU64,
}

impl Default for InodeAllocator {
    fn default() -> Self {
        Self::new(0)
    }
}

impl InodeAllocator {
    pub fn new(client_id: u64) -> Self {
        let start = INODE_START + (client_id % 100) * INODE_RANGE_SIZE;
        Self {
            next_inode: AtomicU64::new(start),
        }
    }

    /// 分配一个新的 inode 号
    pub fn allocate(&self) -> u64 {
        self.next_inode.fetch_add(1, Ordering::SeqCst)
    }

    /// 获取下一个将分配的 inode 号（不消耗）
    pub fn peek(&self) -> u64 {
        self.next_inode.load(Ordering::SeqCst)
    }

    /// 重置分配器（仅用于测试）
    #[cfg(test)]
    pub fn reset(&self, start: u64) {
        self.next_inode.store(start, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_allocate_sequential() {
        let alloc = InodeAllocator::new(0);
        assert_eq!(alloc.allocate(), 100);
        assert_eq!(alloc.allocate(), 101);
        assert_eq!(alloc.allocate(), 102);
    }

    #[test]
    fn test_peek_does_not_consume() {
        let alloc = InodeAllocator::new(0);
        assert_eq!(alloc.peek(), 100);
        assert_eq!(alloc.peek(), 100);
        assert_eq!(alloc.allocate(), 100);
        assert_eq!(alloc.peek(), 101);
    }

    #[test]
    fn test_allocate_does_not_use_inode_1() {
        let alloc = InodeAllocator::new(0);
        let ino = alloc.allocate();
        assert_ne!(ino, 1); // 不与根 inode 冲突
        assert!(ino >= INODE_START);
    }

    #[test]
    fn test_allocate_many() {
        let alloc = InodeAllocator::new(0);
        for i in 0..1000 {
            let ino = alloc.allocate();
            assert_eq!(ino, INODE_START + i);
        }
    }

    #[test]
    fn test_client_inode_range_isolation() {
        let alloc1 = InodeAllocator::new(1);
        let alloc2 = InodeAllocator::new(2);
        
        let ino1 = alloc1.allocate();
        let ino2 = alloc2.allocate();
        
        assert_ne!(ino1, ino2);
        assert!(ino1 >= 100 + INODE_RANGE_SIZE);
        assert!(ino2 >= 100 + 2 * INODE_RANGE_SIZE);
    }
}
