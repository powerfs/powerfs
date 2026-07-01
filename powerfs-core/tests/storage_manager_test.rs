use powerfs_common::types::{NodeId, VolumeId, VolumeState};
use powerfs_core::storage::StorageManager;

// Helper: create a StorageManager with a temp directory
fn create_storage_manager() -> (tempfile::TempDir, StorageManager) {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let mgr = StorageManager::new(NodeId("test-node".to_string()), path);
    (dir, mgr)
}

// ============================================================================
// StorageManager creation tests
// ============================================================================

#[test]
fn test_storage_manager_new() {
    let (_dir, mgr) = create_storage_manager();
    assert_eq!(mgr.node_id(), &NodeId("test-node".to_string()));
    assert_eq!(mgr.volume_count(), 0);
    assert_eq!(mgr.total_space(), 0);
    assert_eq!(mgr.used_space(), 0);
    assert_eq!(mgr.free_space(), 0);
    assert!(mgr.list_volumes().is_empty());
}

#[test]
fn test_storage_manager_find_available_volume_empty() {
    let (_dir, mgr) = create_storage_manager();
    assert!(mgr.find_available_volume().is_none());
}

// ============================================================================
// StorageManager volume CRUD tests
// ============================================================================

#[test]
fn test_create_volume() {
    let (_dir, mgr) = create_storage_manager();

    let info = mgr.create_volume(VolumeId(1), 1024 * 1024).unwrap();
    assert_eq!(info.id, VolumeId(1));
    assert_eq!(info.size, 1024 * 1024);
    assert_eq!(info.state, VolumeState::Available);
    assert_eq!(mgr.volume_count(), 1);
}

#[test]
fn test_create_volume_already_exists() {
    let (_dir, mgr) = create_storage_manager();

    mgr.create_volume(VolumeId(1), 1024 * 1024).unwrap();
    let result = mgr.create_volume(VolumeId(1), 2048 * 1024);
    assert!(result.is_err());
    assert_eq!(mgr.volume_count(), 1);
}

#[test]
fn test_create_multiple_volumes() {
    let (_dir, mgr) = create_storage_manager();

    mgr.create_volume(VolumeId(1), 1024 * 1024).unwrap();
    mgr.create_volume(VolumeId(2), 2048 * 1024).unwrap();
    mgr.create_volume(VolumeId(3), 4096 * 1024).unwrap();

    assert_eq!(mgr.volume_count(), 3);
}

#[test]
fn test_get_volume_exists() {
    let (_dir, mgr) = create_storage_manager();
    mgr.create_volume(VolumeId(42), 1024 * 1024).unwrap();

    let vol = mgr.get_volume(&VolumeId(42));
    assert!(vol.is_some());
    assert_eq!(vol.unwrap().id(), VolumeId(42));
}

#[test]
fn test_get_volume_not_found() {
    let (_dir, mgr) = create_storage_manager();
    assert!(mgr.get_volume(&VolumeId(999)).is_none());
}

#[test]
fn test_list_volumes() {
    let (_dir, mgr) = create_storage_manager();

    mgr.create_volume(VolumeId(1), 1024 * 1024).unwrap();
    mgr.create_volume(VolumeId(2), 2048 * 1024).unwrap();

    let volumes = mgr.list_volumes();
    assert_eq!(volumes.len(), 2);
    let ids: Vec<u32> = volumes.iter().map(|v| v.id.0).collect();
    assert!(ids.contains(&1));
    assert!(ids.contains(&2));
}

#[test]
fn test_delete_volume() {
    let (_dir, mgr) = create_storage_manager();

    mgr.create_volume(VolumeId(1), 1024 * 1024).unwrap();
    assert_eq!(mgr.volume_count(), 1);

    mgr.delete_volume(&VolumeId(1)).unwrap();
    assert_eq!(mgr.volume_count(), 0);
    assert!(mgr.get_volume(&VolumeId(1)).is_none());
}

#[test]
fn test_delete_volume_not_found() {
    let (_dir, mgr) = create_storage_manager();
    let result = mgr.delete_volume(&VolumeId(999));
    assert!(result.is_err());
}

// ============================================================================
// StorageManager space accounting tests
// ============================================================================

#[test]
fn test_total_space() {
    let (_dir, mgr) = create_storage_manager();

    mgr.create_volume(VolumeId(1), 1024).unwrap();
    mgr.create_volume(VolumeId(2), 2048).unwrap();
    mgr.create_volume(VolumeId(3), 4096).unwrap();

    assert_eq!(mgr.total_space(), 1024 + 2048 + 4096);
}

#[test]
fn test_used_space_initially_zero() {
    let (_dir, mgr) = create_storage_manager();
    mgr.create_volume(VolumeId(1), 1024 * 1024).unwrap();
    assert_eq!(mgr.used_space(), 0);
}

#[test]
fn test_free_space_equals_total_initially() {
    let (_dir, mgr) = create_storage_manager();
    mgr.create_volume(VolumeId(1), 1024 * 1024).unwrap();
    assert_eq!(mgr.free_space(), mgr.total_space());
}

#[test]
fn test_used_space_after_write() {
    let (_dir, mgr) = create_storage_manager();
    mgr.create_volume(VolumeId(1), 10 * 1024 * 1024).unwrap();

    let vol = mgr.get_volume(&VolumeId(1)).unwrap();
    vol.write_needle(1, bytes::Bytes::from("data")).unwrap();

    assert!(mgr.used_space() > 0);
}

// ============================================================================
// StorageManager find_available_volume tests
// ============================================================================

#[test]
fn test_find_available_volume_success() {
    let (_dir, mgr) = create_storage_manager();
    mgr.create_volume(VolumeId(1), 10 * 1024 * 1024).unwrap();

    let found = mgr.find_available_volume();
    assert!(found.is_some());
    assert_eq!(found.unwrap(), VolumeId(1));
}

#[test]
fn test_find_available_volume_skips_read_only() {
    let (_dir, mgr) = create_storage_manager();

    mgr.create_volume(VolumeId(1), 10 * 1024 * 1024).unwrap();
    mgr.create_volume(VolumeId(2), 10 * 1024 * 1024).unwrap();

    // Set first volume to read-only
    let vol1 = mgr.get_volume(&VolumeId(1)).unwrap();
    vol1.set_read_only();

    // Should find the second (available) volume
    let found = mgr.find_available_volume();
    assert!(found.is_some());
    assert_eq!(found.unwrap(), VolumeId(2));
}

// ============================================================================
// StorageManager load_volumes test
// ============================================================================

#[test]
fn test_load_volumes_empty_dir() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let mgr = StorageManager::new(NodeId("node".to_string()), path);
    assert!(mgr.load_volumes().is_ok());
    assert_eq!(mgr.volume_count(), 0);
}

#[test]
fn test_load_volumes_with_existing() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    // Create a volume manually
    {
        let mgr1 = StorageManager::new(NodeId("node".to_string()), path.clone());
        mgr1.create_volume(VolumeId(5), 1024 * 1024).unwrap();
    }

    // Load with a new manager
    let mgr2 = StorageManager::new(NodeId("node".to_string()), path);
    mgr2.load_volumes().unwrap();

    // Should find the existing volume
    assert_eq!(mgr2.volume_count(), 1);
    let vol = mgr2.get_volume(&VolumeId(5));
    assert!(vol.is_some());
}
