use bytes::Bytes;
use powerfs_common::types::{NeedleId, VolumeId, VolumeState};
use powerfs_core::volume::Volume;

// Helper: create a temporary directory and volume
fn create_test_volume(vol_id: u32, size: u64) -> (tempfile::TempDir, Volume) {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap();
    let volume = Volume::new(VolumeId(vol_id), "test-node", path, size).unwrap();
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

    let v1 = Volume::new(VolumeId(1), "node", path, 1024 * 1024).unwrap();
    let v2 = Volume::new(VolumeId(2), "node", path, 1024 * 1024).unwrap();

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

    // Write data
    {
        let volume = Volume::new(VolumeId(1), "node", path, 10 * 1024 * 1024).unwrap();
        volume
            .write_needle(1, Bytes::from("persistent data"))
            .unwrap();
    }

    // Reopen
    let volume2 = Volume::new(VolumeId(1), "node", path, 10 * 1024 * 1024).unwrap();

    // Data should be readable from persistent index
    let read_data = volume2.read_needle(&NeedleId(1)).unwrap();
    assert_eq!(read_data, Bytes::from("persistent data"));
}
