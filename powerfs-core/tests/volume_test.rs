use bytes::Bytes;
use powerfs_common::types::{NeedleId, VolumeId, VolumeState};
use powerfs_core::storage_backend::LocalFsBackend;
use powerfs_core::volume::Volume;
use std::sync::Arc;

fn create_test_volume(vol_id: u32, size: u64) -> (tempfile::TempDir, Volume) {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap();
    let backend = Arc::new(
        LocalFsBackend::new(path, "test-node", "default", Some(100 * 1024 * 1024 * 1024)).unwrap(),
    );
    let volume = Volume::new(VolumeId(vol_id), "test-node", path, size, backend).unwrap();
    (dir, volume)
}

// ============================================================================
// Volume creation tests
// ============================================================================

#[test]
fn test_volume_new() {
    let (_dir, volume) = create_test_volume(1, 10 * 1024 * 1024); // 10MB

    assert_eq!(volume.id(), VolumeId(1));
    assert_eq!(volume.size(), 10 * 1024 * 1024);
    assert_eq!(volume.used(), 0);
    assert_eq!(volume.free_space(), 10 * 1024 * 1024);
    assert_eq!(volume.state(), VolumeState::Available);
    assert!(volume.is_available());
    assert!(!volume.is_full());
    assert!(!volume.is_read_only());
    assert!(!volume.is_deleting());
    assert_eq!(volume.count(), 0);
}

#[test]
fn test_volume_info() {
    let (_dir, volume) = create_test_volume(42, 1024 * 1024);
    let info = volume.info();

    assert_eq!(info.id, VolumeId(42));
    assert_eq!(info.size, 1024 * 1024);
    assert_eq!(info.used, 0);
    assert_eq!(info.state, VolumeState::Available);
}

#[test]
fn test_volume_multiple_volumes_different_ids() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap();
    let backend = Arc::new(
        LocalFsBackend::new(path, "node", "default", Some(100 * 1024 * 1024 * 1024)).unwrap(),
    );

    let v1 = Volume::new(VolumeId(1), "node", path, 1024 * 1024, backend.clone()).unwrap();
    let v2 = Volume::new(VolumeId(2), "node", path, 1024 * 1024, backend).unwrap();

    assert_eq!(v1.id(), VolumeId(1));
    assert_eq!(v2.id(), VolumeId(2));
    assert_ne!(v1.id(), v2.id());
}

// ============================================================================
// Volume write tests
// ============================================================================

#[test]
fn test_volume_write_needle() {
    let (_dir, volume) = create_test_volume(1, 10 * 1024 * 1024);

    let data = Bytes::from("hello powerfs");
    let info = volume.write_needle(100, data.clone()).unwrap();

    assert_eq!(info.id, NeedleId(100));
    assert_eq!(info.volume_id, VolumeId(1));
    assert_eq!(info.data_size, data.len() as u32);
    assert_eq!(volume.count(), 1);
    assert!(volume.used() > 0);
}

#[test]
fn test_volume_write_multiple_needles() {
    let (_dir, volume) = create_test_volume(1, 10 * 1024 * 1024);

    volume.write_needle(1, Bytes::from("first")).unwrap();
    volume.write_needle(2, Bytes::from("second")).unwrap();
    volume.write_needle(3, Bytes::from("third")).unwrap();

    assert_eq!(volume.count(), 3);
    assert!(volume.used() > 0);
}

#[test]
fn test_volume_write_same_file_key() {
    let (_dir, volume) = create_test_volume(1, 10 * 1024 * 1024);

    volume.write_needle(1, Bytes::from("original")).unwrap();
    volume.write_needle(1, Bytes::from("updated")).unwrap();

    assert_eq!(volume.count(), 1);
}

// ============================================================================
// Volume read tests
// ============================================================================

#[test]
fn test_volume_read_needle() {
    let (_dir, volume) = create_test_volume(1, 10 * 1024 * 1024);

    let data = Bytes::from("read test data");
    volume.write_needle(200, data.clone()).unwrap();

    let read_data = volume.read_needle(&NeedleId(200)).unwrap();
    assert_eq!(read_data, data);
}

#[test]
fn test_volume_read_nonexistent() {
    let (_dir, volume) = create_test_volume(1, 10 * 1024 * 1024);
    let result = volume.read_needle(&NeedleId(999));
    assert!(result.is_err());
}

#[test]
fn test_volume_read_after_multiple_writes() {
    let (_dir, volume) = create_test_volume(1, 10 * 1024 * 1024);

    let d1 = Bytes::from("data1");
    let d2 = Bytes::from("data2");
    let d3 = Bytes::from("data3");

    volume.write_needle(1, d1.clone()).unwrap();
    volume.write_needle(2, d2.clone()).unwrap();
    volume.write_needle(3, d3.clone()).unwrap();

    assert_eq!(volume.read_needle(&NeedleId(1)).unwrap(), d1);
    assert_eq!(volume.read_needle(&NeedleId(2)).unwrap(), d2);
    assert_eq!(volume.read_needle(&NeedleId(3)).unwrap(), d3);
}

// ============================================================================
// Volume delete tests
// ============================================================================

#[test]
fn test_volume_delete_needle() {
    let (_dir, volume) = create_test_volume(1, 10 * 1024 * 1024);

    volume.write_needle(1, Bytes::from("to delete")).unwrap();
    assert_eq!(volume.count(), 1);

    volume.delete_needle(&NeedleId(1)).unwrap();

    assert!(volume.read_needle(&NeedleId(1)).is_err());

    volume.restore_needle(&NeedleId(1)).unwrap();
    assert_eq!(
        volume.read_needle(&NeedleId(1)).unwrap(),
        Bytes::from("to delete")
    );
}

#[test]
fn test_volume_delete_nonexistent() {
    let (_dir, volume) = create_test_volume(1, 10 * 1024 * 1024);
    let result = volume.delete_needle(&NeedleId(999));
    assert!(result.is_err());
}

// ============================================================================
// Volume get_needle_info tests
// ============================================================================

#[test]
fn test_volume_get_needle_info() {
    let (_dir, volume) = create_test_volume(1, 10 * 1024 * 1024);

    let data = Bytes::from("info test");
    let written = volume.write_needle(50, data).unwrap();

    let info = volume.get_needle_info(&NeedleId(50)).unwrap();
    assert_eq!(info.id, written.id);
    assert_eq!(info.volume_id, written.volume_id);
    assert_eq!(info.data_size, written.data_size);
}

#[test]
fn test_volume_get_needle_info_nonexistent() {
    let (_dir, volume) = create_test_volume(1, 10 * 1024 * 1024);
    assert!(volume.get_needle_info(&NeedleId(999)).is_none());
}

// ============================================================================
// Volume state management tests
// ============================================================================

#[test]
fn test_volume_set_read_only() {
    let (_dir, volume) = create_test_volume(1, 10 * 1024 * 1024);

    volume.set_read_only();
    assert!(volume.is_read_only());
    assert!(!volume.is_available());
    assert!(!volume.is_full());
    assert!(!volume.is_deleting());
}

#[test]
fn test_volume_set_deleting() {
    let (_dir, volume) = create_test_volume(1, 10 * 1024 * 1024);

    volume.set_deleting();
    assert!(volume.is_deleting());
    assert!(!volume.is_available());
    assert!(!volume.is_read_only());
    assert!(!volume.is_full());
}

#[test]
fn test_volume_write_to_read_only_fails() {
    let (_dir, volume) = create_test_volume(1, 10 * 1024 * 1024);

    volume.set_read_only();
    let result = volume.write_needle(1, Bytes::from("data"));
    assert!(result.is_err());
}

// ============================================================================
// Volume full test
// ============================================================================

#[test]
fn test_volume_out_of_space() {
    // Create a tiny volume
    let (_dir, volume) = create_test_volume(1, 1024); // Only 1KB

    // Write a relatively large needle
    let large_data = Bytes::from(vec![0u8; 900]);
    let result = volume.write_needle(1, large_data);

    // Should either succeed or fail with OutOfSpace
    if result.is_err() {
        // If it failed, volume should be full
        assert!(volume.is_full());
    }
}

// ============================================================================
// Volume large data round-trip
// ============================================================================

#[test]
fn test_volume_large_data_round_trip() {
    let (_dir, volume) = create_test_volume(1, 100 * 1024 * 1024); // 100MB

    let data = Bytes::from(vec![0xABu8; 1024 * 64]); // 64KB
    volume.write_needle(1, data.clone()).unwrap();

    let read_data = volume.read_needle(&NeedleId(1)).unwrap();
    assert_eq!(read_data, data);
    assert_eq!(read_data.len(), 1024 * 64);
}

#[test]
fn test_volume_binary_data_round_trip() {
    let (_dir, volume) = create_test_volume(1, 10 * 1024 * 1024);

    let data = Bytes::from(vec![0u8, 1, 2, 3, 255, 254, 253, 128]);
    volume.write_needle(1, data.clone()).unwrap();

    let read_data = volume.read_needle(&NeedleId(1)).unwrap();
    assert_eq!(read_data, data);
}

#[test]
fn test_volume_empty_data_round_trip() {
    let (_dir, volume) = create_test_volume(1, 10 * 1024 * 1024);

    let data = Bytes::new();
    volume.write_needle(1, data.clone()).unwrap();

    let read_data = volume.read_needle(&NeedleId(1)).unwrap();
    assert_eq!(read_data.len(), 0);
}

// ============================================================================
// Volume persistence test (reopen)
// ============================================================================

#[test]
fn test_volume_reopen_persists_data() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap();
    let backend = Arc::new(
        LocalFsBackend::new(path, "node", "default", Some(100 * 1024 * 1024 * 1024)).unwrap(),
    );

    {
        let volume =
            Volume::new(VolumeId(1), "node", path, 10 * 1024 * 1024, backend.clone()).unwrap();
        volume
            .write_needle(1, Bytes::from("persistent data"))
            .unwrap();
    }

    let volume2 = Volume::new(VolumeId(1), "node", path, 10 * 1024 * 1024, backend).unwrap();

    let read_data = volume2.read_needle(&NeedleId(1)).unwrap();
    assert_eq!(read_data, Bytes::from("persistent data"));
}

// ============================================================================
// Bitrot scrub tests
// ============================================================================

#[test]
fn test_volume_verify_needle_valid() {
    let (_dir, volume) = create_test_volume(1, 10 * 1024 * 1024);

    volume
        .write_needle(1, Bytes::from("test data for verification"))
        .unwrap();

    let result = volume.verify_needle(&NeedleId(1)).unwrap();
    assert!(result);

    let info = volume.read_needle_meta(1).unwrap();
    assert!(info.last_verified_at.is_some());
    assert_eq!(info.verification_count, 1);
}

#[test]
fn test_volume_verify_nonexistent_needle() {
    let (_dir, volume) = create_test_volume(1, 10 * 1024 * 1024);

    let result = volume.verify_needle(&NeedleId(999));
    assert!(result.is_err());
}

#[test]
fn test_volume_scrub_empty() {
    let (_dir, volume) = create_test_volume(1, 10 * 1024 * 1024);

    let result = volume.scrub_volume();
    assert_eq!(result.total, 0);
    assert_eq!(result.verified, 0);
    assert_eq!(result.corrupted, 0);
    assert_eq!(result.skipped, 0);
    assert_eq!(result.errors, 0);
    assert!(result.corrupted_needles.is_empty());
}

#[test]
fn test_volume_scrub_all_valid() {
    let (_dir, volume) = create_test_volume(1, 10 * 1024 * 1024);

    volume.write_needle(1, Bytes::from("first needle")).unwrap();
    volume
        .write_needle(2, Bytes::from("second needle"))
        .unwrap();
    volume.write_needle(3, Bytes::from("third needle")).unwrap();

    let result = volume.scrub_volume();
    assert_eq!(result.total, 3);
    assert_eq!(result.verified, 3);
    assert_eq!(result.corrupted, 0);
    assert_eq!(result.errors, 0);
    assert!(result.corrupted_needles.is_empty());
}

#[test]
fn test_volume_scrub_skips_deleted() {
    let (_dir, volume) = create_test_volume(1, 10 * 1024 * 1024);

    volume.write_needle(1, Bytes::from("keep this")).unwrap();
    volume.write_needle(2, Bytes::from("delete this")).unwrap();
    volume.delete_needle(&NeedleId(2)).unwrap();

    let result = volume.scrub_volume();
    assert_eq!(result.total, 1);
    assert_eq!(result.verified, 1);
    assert_eq!(result.skipped, 1);
    assert_eq!(result.corrupted, 0);
}

#[test]
fn test_storage_manager_scrub_volume() {
    use powerfs_common::types::NodeId;
    use powerfs_core::storage::StorageManager;

    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap();
    let manager =
        StorageManager::new(NodeId("node-1".to_string()), path.to_string(), None).unwrap();

    manager
        .create_volume(VolumeId(1), 10 * 1024 * 1024)
        .unwrap();
    let volume = manager.get_volume(&VolumeId(1)).unwrap();
    volume.write_needle(1, Bytes::from("sm test data")).unwrap();

    let result = manager.scrub_volume(&VolumeId(1)).unwrap();
    assert_eq!(result.total, 1);
    assert_eq!(result.verified, 1);
    assert_eq!(result.corrupted, 0);
}

#[test]
fn test_storage_manager_verify_needle() {
    use powerfs_common::types::NodeId;
    use powerfs_core::storage::StorageManager;

    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap();
    let manager =
        StorageManager::new(NodeId("node-1".to_string()), path.to_string(), None).unwrap();

    manager
        .create_volume(VolumeId(1), 10 * 1024 * 1024)
        .unwrap();
    let volume = manager.get_volume(&VolumeId(1)).unwrap();
    volume.write_needle(42, Bytes::from("verify me")).unwrap();

    let valid = manager.verify_needle(&VolumeId(1), &NeedleId(42)).unwrap();
    assert!(valid);
}

#[test]
fn test_storage_manager_scrub_all_volumes() {
    use powerfs_common::types::NodeId;
    use powerfs_core::storage::StorageManager;

    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap();
    let manager =
        StorageManager::new(NodeId("node-1".to_string()), path.to_string(), None).unwrap();

    manager
        .create_volume(VolumeId(1), 10 * 1024 * 1024)
        .unwrap();
    manager
        .create_volume(VolumeId(2), 10 * 1024 * 1024)
        .unwrap();

    let v1 = manager.get_volume(&VolumeId(1)).unwrap();
    v1.write_needle(1, Bytes::from("v1 needle")).unwrap();

    let v2 = manager.get_volume(&VolumeId(2)).unwrap();
    v2.write_needle(10, Bytes::from("v2 needle a")).unwrap();
    v2.write_needle(11, Bytes::from("v2 needle b")).unwrap();

    let results = manager.scrub_all_volumes();
    assert_eq!(results.len(), 2);

    for (vid, result) in &results {
        if vid.0 == 1 {
            assert_eq!(result.total, 1);
            assert_eq!(result.verified, 1);
        } else if vid.0 == 2 {
            assert_eq!(result.total, 2);
            assert_eq!(result.verified, 2);
        }
    }
}

// ============================================================================
// Append-only & compact tests
// ============================================================================

#[test]
fn test_volume_reopen_restores_used() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap();
    let backend = Arc::new(
        LocalFsBackend::new(path, "node", "default", Some(100 * 1024 * 1024 * 1024)).unwrap(),
    );

    let used_before;
    {
        let volume =
            Volume::new(VolumeId(1), "node", path, 10 * 1024 * 1024, backend.clone()).unwrap();
        volume.write_needle(1, Bytes::from("hello world")).unwrap();
        volume
            .write_needle(2, Bytes::from("second needle"))
            .unwrap();
        used_before = volume.used();
        assert!(used_before > 0);
    }

    let volume2 = Volume::new(VolumeId(1), "node", path, 10 * 1024 * 1024, backend).unwrap();
    assert_eq!(volume2.used(), used_before);
    assert_eq!(volume2.free_space(), 10 * 1024 * 1024 - used_before);
}

#[test]
fn test_write_needle_blob_growth_updates_used() {
    let (_dir, volume) = create_test_volume(1, 10 * 1024 * 1024);

    volume
        .write_needle_blob(1, 0, 1024, Bytes::from(vec![0u8; 1024]), 0)
        .unwrap();
    let used_after_first = volume.used();
    assert!(used_after_first > 0);

    volume
        .write_needle_blob(1, 1024, 1024, Bytes::from(vec![1u8; 1024]), 0)
        .unwrap();
    let used_after_second = volume.used();
    assert!(used_after_second > used_after_first);
}

#[test]
fn test_write_needle_blob_append_only_offset() {
    let (_dir, volume) = create_test_volume(1, 10 * 1024 * 1024);

    volume
        .write_needle_blob(1, 0, 1024, Bytes::from(vec![0u8; 1024]), 0)
        .unwrap();

    let info1 = volume.get_needle_info(&NeedleId(1)).unwrap();
    let offset1 = info1.offset;

    volume
        .write_needle_blob(1, 1024, 1024, Bytes::from(vec![1u8; 1024]), 0)
        .unwrap();

    let info2 = volume.get_needle_info(&NeedleId(1)).unwrap();
    let offset2 = info2.offset;

    assert!(offset2 > offset1);
}

#[test]
fn test_write_needle_blob_data_integrity_after_growth() {
    let (_dir, volume) = create_test_volume(1, 10 * 1024 * 1024);

    volume
        .write_needle_blob(1, 0, 4, Bytes::from(b"abcd".to_vec()), 0)
        .unwrap();

    volume
        .write_needle_blob(1, 4, 4, Bytes::from(b"efgh".to_vec()), 0)
        .unwrap();

    let data = volume.read_needle(&NeedleId(1)).unwrap();
    assert_eq!(&data[..8], b"abcdefgh");
}

/// 回归测试：read_needle_blob 在请求 size 超出实际数据时应返回短读，而不是错误。
///
/// 场景：cp /usr/bin/bash testfile（bash 约 1.2MB，chunk_size = 1MB）
/// - chunk 0: write_needle_blob(offset=0, size=1MB)
/// - chunk 1: write_needle_blob(offset=1MB, size=200KB) → needle.data 扩容到 1.2MB
/// - 读取 chunk 1: read_needle_blob(offset=1MB, size=1MB)  ← 请求 chunk_size 字节
///
/// 修复前：1MB + 1MB = 2MB > 1.2MB → 返回 InvalidRequest 错误 → FUSE 返回 EIO
/// 修复后：返回短读（200KB 可用数据）
#[test]
fn test_read_needle_blob_short_read_beyond_data_end() {
    let (_dir, volume) = create_test_volume(1, 10 * 1024 * 1024);

    // 写入 chunk 0：1MB 数据
    let chunk0_data = vec![0xAAu8; 1024 * 1024];
    volume
        .write_needle_blob(1, 0, chunk0_data.len() as i32, Bytes::from(chunk0_data), 0)
        .unwrap();

    // 写入 chunk 1：200KB 数据（offset = 1MB）
    let chunk1_data = vec![0xBBu8; 200 * 1024];
    volume
        .write_needle_blob(
            1,
            1024 * 1024,
            chunk1_data.len() as i32,
            Bytes::from(chunk1_data),
            0,
        )
        .unwrap();

    // 读取 chunk 0：请求 1MB，应完整返回 1MB
    let data0 = volume
        .read_needle_blob(1, 0, 1024 * 1024)
        .expect("read chunk 0 should succeed");
    assert_eq!(data0.len(), 1024 * 1024);
    assert!(data0.iter().all(|&b| b == 0xAA));

    // 读取 chunk 1：请求 1MB（chunk_size），但实际只有 200KB
    // 修复前会返回错误，修复后应返回短读（200KB）
    let data1 = volume
        .read_needle_blob(1, 1024 * 1024, 1024 * 1024)
        .expect("read chunk 1 should succeed with short read");
    assert_eq!(
        data1.len(),
        200 * 1024,
        "short read should return only available data"
    );
    assert!(data1.iter().all(|&b| b == 0xBB));
}

/// 回归测试：read_needle_blob 在 offset 超出数据范围时应返回空数据，而不是错误。
#[test]
fn test_read_needle_blob_offset_beyond_data_end() {
    let (_dir, volume) = create_test_volume(1, 10 * 1024 * 1024);

    // 只写入 100 字节
    volume
        .write_needle_blob(1, 0, 100, Bytes::from(vec![0u8; 100]), 0)
        .unwrap();

    // offset 超出数据范围，应返回空数据（短读）
    let data = volume
        .read_needle_blob(1, 200, 100)
        .expect("offset beyond data end should return empty data");
    assert!(
        data.is_empty(),
        "offset beyond data end should return empty data, not error"
    );
}

/// 回归测试：模拟 cp /usr/bin/bash testfile 的完整流程
///
/// 验证多 chunk 写入后，按 chunk_size 读取每个 chunk 都能成功，
/// 且拼起来的数据与原始数据一致。
#[test]
fn test_blob_multi_chunk_round_trip_like_cp_bash() {
    let (_dir, volume) = create_test_volume(1, 10 * 1024 * 1024);

    // 模拟 bash 文件：1.2MB（跨 2 个 chunk，chunk_size = 1MB）
    let chunk_size = 1024 * 1024;
    let file_size = chunk_size + 200 * 1024; // 1.2MB
    let original_data: Vec<u8> = (0..file_size).map(|i| (i % 251) as u8).collect();

    // 按 chunk 边界切分写入
    let chunk0_data = original_data[0..chunk_size].to_vec();
    let chunk1_data = original_data[chunk_size..file_size].to_vec();

    volume
        .write_needle_blob(1, 0, chunk0_data.len() as i32, Bytes::from(chunk0_data), 0)
        .unwrap();
    volume
        .write_needle_blob(
            1,
            chunk_size as i64,
            chunk1_data.len() as i32,
            Bytes::from(chunk1_data),
            0,
        )
        .unwrap();

    // 按 chunk_size 读取每个 chunk（FUSE 客户端的读取方式）
    let data0 = volume
        .read_needle_blob(1, 0, chunk_size as i32)
        .expect("read chunk 0");
    let data1 = volume
        .read_needle_blob(1, chunk_size as i64, chunk_size as i32)
        .expect("read chunk 1 should short-read");

    // 拼接读取的数据
    let mut reconstructed = Vec::with_capacity(file_size);
    reconstructed.extend_from_slice(&data0);
    reconstructed.extend_from_slice(&data1);

    assert_eq!(
        reconstructed, original_data,
        "reconstructed data must match original after multi-chunk round trip"
    );
}

#[test]
fn test_compact_reclaims_space_after_deletes() {
    let (_dir, volume) = create_test_volume(1, 10 * 1024 * 1024);

    volume.write_needle(1, Bytes::from("needle one")).unwrap();
    volume.write_needle(2, Bytes::from("needle two")).unwrap();
    volume.write_needle(3, Bytes::from("needle three")).unwrap();

    let used_before = volume.used();
    volume.delete_needle(&NeedleId(2)).unwrap();
    let used_after_delete = volume.used();
    assert_eq!(used_after_delete, used_before);

    let (reclaimed, moved) = volume.compact().unwrap();
    assert!(reclaimed > 0);
    assert!(moved > 0);

    let used_after_compact = volume.used();
    assert!(used_after_compact < used_before);
    assert_eq!(used_after_compact, used_before - reclaimed);

    let data1 = volume.read_needle(&NeedleId(1)).unwrap();
    assert_eq!(data1, Bytes::from("needle one"));
    let data3 = volume.read_needle(&NeedleId(3)).unwrap();
    assert_eq!(data3, Bytes::from("needle three"));
}

#[test]
fn test_compact_after_append_only_growth() {
    let (_dir, volume) = create_test_volume(1, 10 * 1024 * 1024);

    volume
        .write_needle_blob(1, 0, 1024, Bytes::from(vec![0u8; 1024]), 0)
        .unwrap();
    volume
        .write_needle_blob(1, 1024, 1024, Bytes::from(vec![1u8; 1024]), 0)
        .unwrap();

    let used_before = volume.used();
    let (reclaimed, moved) = volume.compact().unwrap();
    assert!(reclaimed > 0);
    assert!(moved > 0);

    let used_after = volume.used();
    assert!(used_after < used_before);

    let data = volume.read_needle(&NeedleId(1)).unwrap();
    assert_eq!(data.len(), 2048);
    assert_eq!(data[0], 0);
    assert_eq!(data[1024], 1);
}

#[test]
fn test_compact_empty_volume() {
    let (_dir, volume) = create_test_volume(1, 10 * 1024 * 1024);

    let (reclaimed, moved) = volume.compact().unwrap();
    assert_eq!(reclaimed, 0);
    assert_eq!(moved, 0);
    assert_eq!(volume.used(), 0);
}

#[test]
fn test_storage_manager_compact_volume() {
    use powerfs_common::types::NodeId;
    use powerfs_core::storage::StorageManager;

    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap();
    let manager =
        StorageManager::new(NodeId("node-1".to_string()), path.to_string(), None).unwrap();

    manager
        .create_volume(VolumeId(1), 10 * 1024 * 1024)
        .unwrap();
    let v1 = manager.get_volume(&VolumeId(1)).unwrap();
    v1.write_needle(1, Bytes::from("test")).unwrap();
    v1.write_needle(2, Bytes::from("delete me")).unwrap();
    v1.delete_needle(&NeedleId(2)).unwrap();

    let (reclaimed, _moved) = manager.compact_volume(&VolumeId(1)).unwrap();
    assert!(reclaimed > 0);
}
