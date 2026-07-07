use chrono::Utc;
use powerfs_common::types::{NeedleId, NeedleInfo, VolumeId};
use powerfs_core::index::{MemoryIndex, NeedleIndex};

// ============================================================================
// Helper to create test NeedleInfo
// ============================================================================

fn make_info(id: u64, vid: u32, size: u32, offset: u64) -> NeedleInfo {
    NeedleInfo {
        id: NeedleId(id),
        volume_id: VolumeId(vid),
        data_size: size,
        offset,
        checksum: 0,
        checksum_algorithm: powerfs_common::types::ChecksumAlgorithm::default(),
        last_verified_at: None,
        verification_count: 0,
        deleted_at: None,
        delete_retention_until: None,
        worm_retention_until: None,
        created_at: Utc::now(),
        ec_enabled: false,
        ec_k: None,
        ec_m: None,
        ec_shards: Vec::new(),
    }
}

// ============================================================================
// MemoryIndex tests
// ============================================================================

#[test]
fn test_memory_index_new() {
    let index = MemoryIndex::new(100);
    assert!(index.is_empty());
    assert_eq!(index.len(), 0);
}

#[test]
fn test_memory_index_insert_and_get() {
    let index = MemoryIndex::new(100);
    let info = make_info(1, 1, 100, 0);
    index.insert(NeedleId(1), info.clone());

    assert_eq!(index.len(), 1);
    assert!(!index.is_empty());

    let retrieved = index.get(&NeedleId(1));
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.id, NeedleId(1));
    assert_eq!(retrieved.data_size, 100);
}

#[test]
fn test_memory_index_get_nonexistent() {
    let index = MemoryIndex::new(100);
    let result = index.get(&NeedleId(999));
    assert!(result.is_none());
}

#[test]
fn test_memory_index_insert_multiple() {
    let index = MemoryIndex::new(100);
    for i in 1..=50 {
        index.insert(NeedleId(i), make_info(i, 1, 10, i * 100));
    }
    assert_eq!(index.len(), 50);
    assert!(index.contains(&NeedleId(25)));
    assert!(index.contains(&NeedleId(50)));
}

#[test]
fn test_memory_index_contains() {
    let index = MemoryIndex::new(100);
    index.insert(NeedleId(1), make_info(1, 1, 10, 0));

    assert!(index.contains(&NeedleId(1)));
    assert!(!index.contains(&NeedleId(2)));
}

#[test]
fn test_memory_index_remove_existing() {
    let index = MemoryIndex::new(100);
    let info = make_info(1, 1, 100, 0);
    index.insert(NeedleId(1), info.clone());

    let removed = index.remove(&NeedleId(1));
    assert!(removed.is_some());
    assert_eq!(removed.unwrap().id, NeedleId(1));
    assert_eq!(index.len(), 0);
    assert!(index.is_empty());
    assert!(!index.contains(&NeedleId(1)));
}

#[test]
fn test_memory_index_remove_nonexistent() {
    let index = MemoryIndex::new(100);
    let removed = index.remove(&NeedleId(999));
    assert!(removed.is_none());
}

#[test]
fn test_memory_index_remove_then_reinsert() {
    let index = MemoryIndex::new(100);
    index.insert(NeedleId(1), make_info(1, 1, 10, 0));
    index.remove(&NeedleId(1));
    index.insert(NeedleId(1), make_info(1, 1, 20, 100));

    let retrieved = index.get(&NeedleId(1));
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.data_size, 20);
    assert_eq!(retrieved.offset, 100);
}

#[test]
fn test_memory_index_insert_overwrite() {
    let index = MemoryIndex::new(100);
    index.insert(NeedleId(1), make_info(1, 1, 10, 0));
    index.insert(NeedleId(1), make_info(1, 1, 50, 500));

    let retrieved = index.get(&NeedleId(1));
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.data_size, 50);
    assert_eq!(retrieved.offset, 500);
    assert_eq!(index.len(), 1);
}

#[test]
fn test_memory_index_len_and_is_empty() {
    let index = MemoryIndex::new(100);
    assert!(index.is_empty());
    assert_eq!(index.len(), 0);

    index.insert(NeedleId(1), make_info(1, 1, 10, 0));
    assert!(!index.is_empty());
    assert_eq!(index.len(), 1);

    index.insert(NeedleId(2), make_info(2, 1, 20, 0));
    assert_eq!(index.len(), 2);

    index.remove(&NeedleId(1));
    assert_eq!(index.len(), 1);
    assert!(!index.is_empty());

    index.remove(&NeedleId(2));
    assert_eq!(index.len(), 0);
    assert!(index.is_empty());
}

#[test]
fn test_memory_index_large_capacity() {
    let index = MemoryIndex::new(10000);
    for i in 0..5000 {
        index.insert(NeedleId(i), make_info(i, 1, 10, i * 100));
    }
    assert_eq!(index.len(), 5000);
    for i in 0..5000 {
        assert!(index.contains(&NeedleId(i)));
    }
}

#[test]
fn test_memory_index_different_volume_ids() {
    let index = MemoryIndex::new(100);
    index.insert(NeedleId(1), make_info(1, 10, 100, 0));
    index.insert(NeedleId(2), make_info(2, 20, 200, 500));

    let info1 = index.get(&NeedleId(1)).unwrap();
    assert_eq!(info1.volume_id, VolumeId(10));

    let info2 = index.get(&NeedleId(2)).unwrap();
    assert_eq!(info2.volume_id, VolumeId(20));
}

#[test]
fn test_memory_index_get_updates_lru() {
    let index = MemoryIndex::new(100);
    index.insert(NeedleId(1), make_info(1, 1, 10, 0));
    // Multiple gets should not panic
    for _ in 0..10 {
        let result = index.get(&NeedleId(1));
        assert!(result.is_some());
    }
}

#[test]
fn test_memory_index_thread_safe() {
    use std::sync::Arc;
    use std::thread;

    let index = Arc::new(MemoryIndex::new(1000));
    let mut handles = vec![];

    for t in 0..4 {
        let idx = index.clone();
        let handle = thread::spawn(move || {
            for i in 0..100 {
                let id = t * 100 + i;
                idx.insert(
                    NeedleId(id as u64),
                    make_info(id as u64, 1, 10, id as u64 * 10),
                );
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    assert_eq!(index.len(), 400);
}
