use powerfs_master::directory_tree::DirectoryTree;
use tempfile::TempDir;

fn create_test_dir_tree() -> (DirectoryTree, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let tree = DirectoryTree::new(temp_dir.path()).expect("Failed to create DirectoryTree");
    (tree, temp_dir)
}

// ============================================================
// Phase: Master Failover - Epoch Mechanism
// ============================================================

#[test]
fn test_epoch_starts_from_1_on_first_start() {
    let (tree, _temp) = create_test_dir_tree();
    let epoch = tree.get_epoch();
    assert!(
        epoch >= 1,
        "Epoch should start from 1 on first start, got {}",
        epoch
    );
}

#[test]
fn test_epoch_increments_on_restart() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    let tree1 = DirectoryTree::new(temp_dir.path()).expect("Failed to create first DirectoryTree");
    let epoch1 = tree1.get_epoch();

    drop(tree1);

    let tree2 = DirectoryTree::new(temp_dir.path()).expect("Failed to create second DirectoryTree");
    let epoch2 = tree2.get_epoch();

    assert_eq!(
        epoch2,
        epoch1 + 1,
        "Epoch should increment by 1 on restart: {} -> {}",
        epoch1,
        epoch2
    );
}

#[test]
fn test_epoch_increments_multiple_restarts() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    let mut last_epoch = 0;
    for i in 0..5 {
        let tree =
            DirectoryTree::new(temp_dir.path()).expect("Failed to create DirectoryTree iteration");
        let epoch = tree.get_epoch();
        assert!(
            epoch > last_epoch,
            "Epoch should increase on restart #{}: {} -> {}",
            i,
            last_epoch,
            epoch
        );
        last_epoch = epoch;
    }
    assert_eq!(last_epoch, 5, "After 5 restarts epoch should be 5");
}

#[test]
fn test_lease_records_epoch() {
    let (tree, _temp) = create_test_dir_tree();
    let current_epoch = tree.get_epoch();

    let lease_id = tree.acquire_lease("/test/file.txt", "client-1", 60000);

    assert!(!lease_id.is_empty(), "Lease ID should not be empty");

    let leases = tree.leases.read().unwrap();
    let lease = leases.get(&lease_id).expect("Lease should exist");
    assert_eq!(
        lease.epoch, current_epoch,
        "Lease epoch should match current epoch"
    );
}

#[test]
fn test_has_active_lease_after_acquire() {
    let (tree, _temp) = create_test_dir_tree();

    tree.acquire_lease("/test/file.txt", "client-1", 60000);

    assert!(
        tree.has_active_lease("/test/file.txt"),
        "Should have active lease after acquire"
    );
}

#[test]
fn test_has_active_lease_false_after_epoch_change() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    let tree1 = DirectoryTree::new(temp_dir.path()).expect("Failed to create first DirectoryTree");
    let _lease_id = tree1.acquire_lease("/test/file.txt", "client-1", 60000);
    assert!(
        tree1.has_active_lease("/test/file.txt"),
        "Should have active lease before restart"
    );

    drop(tree1);

    let tree2 = DirectoryTree::new(temp_dir.path()).expect("Failed to create second DirectoryTree");

    assert!(
        !tree2.has_active_lease("/test/file.txt"),
        "Should NOT have active lease after Master restart (epoch changed)"
    );
}

#[test]
fn test_release_old_lease_after_restart_returns_false() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    let tree1 = DirectoryTree::new(temp_dir.path()).expect("Failed to create first DirectoryTree");
    let lease_id = tree1.acquire_lease("/test/file.txt", "client-1", 60000);
    drop(tree1);

    let tree2 = DirectoryTree::new(temp_dir.path()).expect("Failed to create second DirectoryTree");

    let result = tree2.release_lease(&lease_id);
    assert!(
        !result,
        "Releasing old lease after Master restart should return false"
    );
}

#[test]
fn test_new_lease_acquired_after_restart() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    let tree1 = DirectoryTree::new(temp_dir.path()).expect("Failed to create first DirectoryTree");
    let epoch1 = tree1.get_epoch();
    let _old_lease = tree1.acquire_lease("/test/file.txt", "client-1", 60000);
    drop(tree1);

    let tree2 = DirectoryTree::new(temp_dir.path()).expect("Failed to create second DirectoryTree");
    let epoch2 = tree2.get_epoch();
    assert!(epoch2 > epoch1, "Epoch should have incremented");

    let new_lease = tree2.acquire_lease("/test/file.txt", "client-1", 60000);
    assert!(
        !new_lease.is_empty(),
        "Should acquire new lease after restart"
    );

    assert!(
        tree2.has_active_lease("/test/file.txt"),
        "New lease should be active"
    );

    let leases = tree2.leases.read().unwrap();
    let lease = leases.get(&new_lease).expect("New lease should exist");
    assert_eq!(lease.epoch, epoch2, "New lease should have current epoch");
}

#[test]
fn test_notification_contains_epoch() {
    use powerfs_master::proto::powerfs::metadata_notification::EventType;

    let (tree, _temp) = create_test_dir_tree();
    let current_epoch = tree.get_epoch();

    let mut rx = tree.subscribe();

    tree.create_directory("/test_notif_epoch")
        .expect("mkdir failed");

    let notif = rx.try_recv().expect("Should receive notification");
    assert_eq!(
        notif.event_type,
        EventType::Create as i32,
        "Should be CREATE event"
    );
    assert_eq!(
        notif.epoch, current_epoch,
        "Notification should contain current epoch"
    );
}

#[test]
fn test_notification_epoch_increments_after_restart() {
    use powerfs_master::proto::powerfs::metadata_notification::EventType;

    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    let tree1 = DirectoryTree::new(temp_dir.path()).expect("Failed to create first DirectoryTree");
    let epoch1 = tree1.get_epoch();
    let mut rx1 = tree1.subscribe();
    tree1.create_directory("/dir1").expect("mkdir failed");
    let notif1 = rx1.try_recv().expect("Should receive notification 1");
    assert_eq!(notif1.epoch, epoch1);
    drop(tree1);

    let tree2 = DirectoryTree::new(temp_dir.path()).expect("Failed to create second DirectoryTree");
    let epoch2 = tree2.get_epoch();
    assert!(epoch2 > epoch1, "Epoch should have incremented");
    let mut rx2 = tree2.subscribe();
    tree2.create_directory("/dir2").expect("mkdir failed");
    let notif2 = rx2.try_recv().expect("Should receive notification 2");
    assert_eq!(
        notif2.epoch, epoch2,
        "Notification after restart should have new epoch"
    );
    assert_eq!(notif2.event_type, EventType::Create as i32);
}

#[test]
fn test_multiple_leases_all_invalidated_on_restart() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    let tree1 = DirectoryTree::new(temp_dir.path()).expect("Failed to create first DirectoryTree");

    let _l1 = tree1.acquire_lease("/file1.txt", "client-1", 60000);
    let _l2 = tree1.acquire_lease("/file2.txt", "client-1", 60000);
    let _l3 = tree1.acquire_lease("/file3.txt", "client-2", 60000);

    assert!(tree1.has_active_lease("/file1.txt"));
    assert!(tree1.has_active_lease("/file2.txt"));
    assert!(tree1.has_active_lease("/file3.txt"));

    drop(tree1);

    let tree2 = DirectoryTree::new(temp_dir.path()).expect("Failed to create second DirectoryTree");

    assert!(
        !tree2.has_active_lease("/file1.txt"),
        "All leases should be invalid"
    );
    assert!(
        !tree2.has_active_lease("/file2.txt"),
        "All leases should be invalid"
    );
    assert!(
        !tree2.has_active_lease("/file3.txt"),
        "All leases should be invalid"
    );
}

#[test]
fn test_epoch_persisted_to_db() {
    use rocksdb::DB;

    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    let tree1 = DirectoryTree::new(temp_dir.path()).expect("Failed to create first DirectoryTree");
    let epoch1 = tree1.get_epoch();
    drop(tree1);

    let db = DB::open_default(temp_dir.path()).expect("Failed to open DB");
    if let Ok(Some(val)) = db.get(b"epoch") {
        if let Ok(s) = String::from_utf8(val) {
            assert_eq!(
                s,
                epoch1.to_string(),
                "Epoch should be persisted to RocksDB"
            );
        } else {
            panic!("Epoch value in DB is not valid UTF-8");
        }
    } else {
        panic!("Epoch key not found in RocksDB");
    }
}

// ============================================================
// Extended Epoch Mechanism Tests
// ============================================================

#[test]
fn test_update_notification_contains_epoch() {
    use powerfs_master::proto::powerfs::metadata_notification::EventType;
    use powerfs_master::proto::powerfs::{Entry, FuseAttributes};

    let (tree, _temp) = create_test_dir_tree();
    let current_epoch = tree.get_epoch();
    let mut rx = tree.subscribe();

    let dir = "/".to_string();
    let name = "epoch_update_file".to_string();
    let entry = Entry {
        directory: dir.clone(),
        name: name.clone(),
        attributes: Some(FuseAttributes {
            ino: 100,
            mode: 0o644,
            ..FuseAttributes::default()
        }),
        ..Entry::default()
    };

    tree.create_entry(entry, "test_client")
        .expect("create_entry failed");
    let _ = rx.try_recv();

    let updated_entry = Entry {
        directory: dir,
        name,
        attributes: Some(FuseAttributes {
            ino: 100,
            mode: 0o644,
            size: 100,
            ..FuseAttributes::default()
        }),
        ..Entry::default()
    };
    tree.update_entry(updated_entry, "test_client", 0, false)
        .expect("update_entry failed");

    let notif = rx.try_recv().expect("Should receive UPDATE notification");
    assert_eq!(
        notif.event_type,
        EventType::Update as i32,
        "Should be UPDATE event"
    );
    assert_eq!(
        notif.epoch, current_epoch,
        "UPDATE notification should contain current epoch"
    );
}

#[test]
fn test_delete_notification_contains_epoch() {
    use powerfs_master::proto::powerfs::metadata_notification::EventType;
    use powerfs_master::proto::powerfs::{Entry, FuseAttributes};

    let (tree, _temp) = create_test_dir_tree();
    let current_epoch = tree.get_epoch();
    let mut rx = tree.subscribe();

    let entry = Entry {
        directory: "/".to_string(),
        name: "epoch_delete_file".to_string(),
        attributes: Some(FuseAttributes {
            ino: 200,
            mode: 0o644,
            ..FuseAttributes::default()
        }),
        ..Entry::default()
    };
    tree.create_entry(entry, "test_client")
        .expect("create_entry failed");
    let _ = rx.try_recv();

    let ino = tree
        .get_entry("/epoch_delete_file")
        .unwrap()
        .attributes
        .unwrap()
        .ino;
    tree.delete_entry(ino, "test_client")
        .expect("delete failed");

    let notif = rx.try_recv().expect("Should receive DELETE notification");
    assert_eq!(
        notif.event_type,
        EventType::Delete as i32,
        "Should be DELETE event"
    );
    assert_eq!(
        notif.epoch, current_epoch,
        "DELETE notification should contain current epoch"
    );
}

#[test]
fn test_same_path_multiple_clients_leases_invalidated_on_restart() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    let tree1 = DirectoryTree::new(temp_dir.path()).expect("Failed to create first DirectoryTree");

    let _l1 = tree1.acquire_lease("/shared.txt", "client-A", 60000);
    let _l2 = tree1.acquire_lease("/shared.txt", "client-B", 60000);
    let _l3 = tree1.acquire_lease("/shared.txt", "client-C", 60000);

    assert!(tree1.has_active_lease("/shared.txt"));

    drop(tree1);

    let tree2 = DirectoryTree::new(temp_dir.path()).expect("Failed to create second DirectoryTree");

    assert!(
        !tree2.has_active_lease("/shared.txt"),
        "All leases on same path should be invalid after restart"
    );
}

#[test]
fn test_expired_lease_before_restart_still_absent_after() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    let tree1 = DirectoryTree::new(temp_dir.path()).expect("Failed to create first DirectoryTree");

    let _short_lease = tree1.acquire_lease("/expire.txt", "client-1", 100);

    std::thread::sleep(std::time::Duration::from_millis(200));

    assert!(
        !tree1.has_active_lease("/expire.txt"),
        "Lease should have expired before restart"
    );

    drop(tree1);

    let tree2 = DirectoryTree::new(temp_dir.path()).expect("Failed to create second DirectoryTree");

    assert!(
        !tree2.has_active_lease("/expire.txt"),
        "Expired lease should remain absent after restart"
    );
}

#[test]
fn test_cleanup_expired_leases_after_restart_preserves_new_leases() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    let tree1 = DirectoryTree::new(temp_dir.path()).expect("Failed to create first DirectoryTree");
    let _old_lease = tree1.acquire_lease("/old.txt", "client-1", 60000);
    drop(tree1);

    let tree2 = DirectoryTree::new(temp_dir.path()).expect("Failed to create second DirectoryTree");

    let new_lease = tree2.acquire_lease("/new.txt", "client-1", 60000);
    assert!(!new_lease.is_empty());

    tree2.cleanup_expired_leases();

    assert!(
        tree2.has_active_lease("/new.txt"),
        "New lease should survive cleanup after restart"
    );
}

#[test]
fn test_different_durations_all_invalidated_on_restart() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    let tree1 = DirectoryTree::new(temp_dir.path()).expect("Failed to create first DirectoryTree");

    let _l1 = tree1.acquire_lease("/short.txt", "client-1", 10000);
    let _l2 = tree1.acquire_lease("/medium.txt", "client-1", 300000);
    let _l3 = tree1.acquire_lease("/long.txt", "client-1", 3600000);

    assert!(tree1.has_active_lease("/short.txt"));
    assert!(tree1.has_active_lease("/medium.txt"));
    assert!(tree1.has_active_lease("/long.txt"));

    drop(tree1);

    let tree2 = DirectoryTree::new(temp_dir.path()).expect("Failed to create second DirectoryTree");

    assert!(
        !tree2.has_active_lease("/short.txt"),
        "Short lease invalidated"
    );
    assert!(
        !tree2.has_active_lease("/medium.txt"),
        "Medium lease invalidated"
    );
    assert!(
        !tree2.has_active_lease("/long.txt"),
        "Long lease invalidated"
    );
}

#[test]
fn test_concurrent_acquire_same_epoch() {
    use std::sync::Arc;
    use std::thread;

    let (tree, _temp) = create_test_dir_tree();
    let tree = Arc::new(tree);
    let current_epoch = tree.get_epoch();

    let mut handles = vec![];
    for i in 0..4 {
        let tree = tree.clone();
        handles.push(thread::spawn(move || {
            let lease_id = tree.acquire_lease(
                &format!("/concurrent_{}.txt", i),
                &format!("client-{}", i),
                60000,
            );
            lease_id
        }));
    }

    let lease_ids: Vec<String> = handles
        .into_iter()
        .map(|h| h.join().expect("thread panicked"))
        .collect();

    assert_eq!(lease_ids.len(), 4);

    let leases = tree.leases.read().unwrap();
    for lease_id in &lease_ids {
        let lease = leases.get(lease_id).expect("Lease should exist");
        assert_eq!(
            lease.epoch, current_epoch,
            "All concurrently acquired leases should have the same epoch"
        );
    }
}

#[test]
fn test_double_restart_epoch_increments_twice() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    let tree1 = DirectoryTree::new(temp_dir.path()).expect("Failed to create first DirectoryTree");
    let epoch1 = tree1.get_epoch();
    drop(tree1);

    let tree2 = DirectoryTree::new(temp_dir.path()).expect("Failed to create second DirectoryTree");
    let epoch2 = tree2.get_epoch();
    assert_eq!(epoch2, epoch1 + 1);
    drop(tree2);

    let tree3 = DirectoryTree::new(temp_dir.path()).expect("Failed to create third DirectoryTree");
    let epoch3 = tree3.get_epoch();
    assert_eq!(
        epoch3,
        epoch1 + 2,
        "Epoch should increment twice after two restarts"
    );
}

#[test]
fn test_lease_acquired_after_double_restart_has_latest_epoch() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    let tree1 = DirectoryTree::new(temp_dir.path()).expect("Failed to create first DirectoryTree");
    let _ = tree1.acquire_lease("/file.txt", "client-1", 60000);
    drop(tree1);

    let tree2 = DirectoryTree::new(temp_dir.path()).expect("Failed to create second DirectoryTree");
    let _ = tree2.acquire_lease("/file.txt", "client-1", 60000);
    drop(tree2);

    let tree3 = DirectoryTree::new(temp_dir.path()).expect("Failed to create third DirectoryTree");
    let latest_epoch = tree3.get_epoch();

    let lease_id = tree3.acquire_lease("/file.txt", "client-1", 60000);
    assert!(!lease_id.is_empty());

    let leases = tree3.leases.read().unwrap();
    let lease = leases.get(&lease_id).expect("Lease should exist");
    assert_eq!(
        lease.epoch, latest_epoch,
        "Lease after double restart should have latest epoch"
    );
}

#[test]
fn test_many_leases_all_invalidated_on_restart() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    let tree1 = DirectoryTree::new(temp_dir.path()).expect("Failed to create first DirectoryTree");

    for i in 0..50 {
        let _ = tree1.acquire_lease(&format!("/file_{}.txt", i), "client-1", 60000);
    }

    for i in 0..50 {
        assert!(
            tree1.has_active_lease(&format!("/file_{}.txt", i)),
            "Lease {} should be active before restart",
            i
        );
    }

    drop(tree1);

    let tree2 = DirectoryTree::new(temp_dir.path()).expect("Failed to create second DirectoryTree");

    for i in 0..50 {
        assert!(
            !tree2.has_active_lease(&format!("/file_{}.txt", i)),
            "Lease {} should be invalid after restart",
            i
        );
    }
}

#[test]
fn test_new_lease_expiry_time_correct_after_restart() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    let tree1 = DirectoryTree::new(temp_dir.path()).expect("Failed to create first DirectoryTree");
    let _ = tree1.acquire_lease("/file.txt", "client-1", 60000);
    drop(tree1);

    let tree2 = DirectoryTree::new(temp_dir.path()).expect("Failed to create second DirectoryTree");
    let before_acquire = std::time::Instant::now();
    let lease_id = tree2.acquire_lease("/file.txt", "client-1", 60000);
    let after_acquire = std::time::Instant::now();

    let leases = tree2.leases.read().unwrap();
    let lease = leases.get(&lease_id).expect("Lease should exist");

    let acquire_duration = after_acquire.duration_since(before_acquire);
    let expected_min = after_acquire + std::time::Duration::from_millis(60000) - acquire_duration;
    assert!(
        lease.expires_at >= expected_min,
        "New lease expiry should be ~60s from now"
    );
}

#[test]
fn test_release_old_lease_does_not_affect_new_lease_after_restart() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    let tree1 = DirectoryTree::new(temp_dir.path()).expect("Failed to create first DirectoryTree");
    let old_lease_id = tree1.acquire_lease("/file.txt", "client-1", 60000);
    drop(tree1);

    let tree2 = DirectoryTree::new(temp_dir.path()).expect("Failed to create second DirectoryTree");
    let new_lease_id = tree2.acquire_lease("/file.txt", "client-1", 60000);

    assert!(tree2.has_active_lease("/file.txt"));

    let result = tree2.release_lease(&old_lease_id);
    assert!(!result, "Releasing old lease should return false");

    assert!(
        tree2.has_active_lease("/file.txt"),
        "New lease should still be active after releasing old lease"
    );

    let result = tree2.release_lease(&new_lease_id);
    assert!(result, "Releasing new lease should return true");
    assert!(
        !tree2.has_active_lease("/file.txt"),
        "Path should have no active lease after releasing new lease"
    );
}

#[test]
fn test_epoch_does_not_change_without_restart() {
    let (tree, _temp) = create_test_dir_tree();
    let epoch_before = tree.get_epoch();

    let _ = tree.acquire_lease("/file1.txt", "client-1", 60000);
    let _ = tree.acquire_lease("/file2.txt", "client-2", 60000);
    tree.cleanup_expired_leases();

    let epoch_after = tree.get_epoch();
    assert_eq!(
        epoch_before, epoch_after,
        "Epoch should not change during normal operation (no restart)"
    );
}

#[test]
fn test_all_event_types_carry_epoch() {
    use powerfs_master::proto::powerfs::metadata_notification::EventType;
    use powerfs_master::proto::powerfs::{Entry, FuseAttributes};

    let (tree, _temp) = create_test_dir_tree();
    let current_epoch = tree.get_epoch();
    let mut rx = tree.subscribe();

    let entry = Entry {
        directory: "/".to_string(),
        name: "all_events_file".to_string(),
        attributes: Some(FuseAttributes {
            ino: 300,
            mode: 0o644,
            ..FuseAttributes::default()
        }),
        ..Entry::default()
    };

    // CREATE
    tree.create_entry(entry.clone(), "test_client")
        .expect("create failed");
    let notif_create = rx.try_recv().expect("Should receive CREATE");
    assert_eq!(notif_create.event_type, EventType::Create as i32);
    assert_eq!(notif_create.epoch, current_epoch);

    // UPDATE
    let mut updated_entry = entry.clone();
    if let Some(ref mut attrs) = updated_entry.attributes {
        attrs.size = 42;
    }
    tree.update_entry(updated_entry, "test_client", 0, false)
        .expect("update failed");
    let notif_update = rx.try_recv().expect("Should receive UPDATE");
    assert_eq!(notif_update.event_type, EventType::Update as i32);
    assert_eq!(notif_update.epoch, current_epoch);

    // DELETE
    let ino = tree
        .get_entry("/all_events_file")
        .unwrap()
        .attributes
        .unwrap()
        .ino;
    tree.delete_entry(ino, "test_client")
        .expect("delete failed");
    let notif_delete = rx.try_recv().expect("Should receive DELETE");
    assert_eq!(notif_delete.event_type, EventType::Delete as i32);
    assert_eq!(notif_delete.epoch, current_epoch);
}

#[test]
fn test_epoch_gap_detection_across_restarts() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    let epochs: Vec<u64> = (0..4)
        .map(|_| {
            let tree = DirectoryTree::new(temp_dir.path()).expect("Failed to create DirectoryTree");
            let e = tree.get_epoch();
            drop(tree);
            e
        })
        .collect();

    for i in 1..4 {
        assert_eq!(
            epochs[i],
            epochs[i - 1] + 1,
            "Epoch should increment by exactly 1 each restart: {} -> {}",
            epochs[i - 1],
            epochs[i]
        );
    }
}

// ============================================================
// Lease Renewal Tests (Feature 1)
// ============================================================

#[test]
fn test_renew_lease_updates_expiry() {
    let (tree, _temp) = create_test_dir_tree();
    let lease_id = tree.acquire_lease("/renew/test.txt", "client-1", 60000);
    assert!(!lease_id.is_empty());
    assert!(tree.has_active_lease("/renew/test.txt"));

    let result = tree.renew_lease(&lease_id, 60000);
    assert!(result.is_some());
    assert!(tree.has_active_lease("/renew/test.txt"));
}

#[test]
fn test_renew_nonexistent_lease() {
    let (tree, _temp) = create_test_dir_tree();
    let result = tree.renew_lease("nonexistent-lease-id", 60000);
    assert!(result.is_none());
}

#[test]
fn test_renew_lease_preserves_epoch() {
    let (tree, _temp) = create_test_dir_tree();
    let current_epoch = tree.get_epoch();
    let lease_id = tree.acquire_lease("/renew/epoch.txt", "client-1", 60000);

    let renewed_epoch = tree.renew_lease(&lease_id, 60000).unwrap();
    assert_eq!(renewed_epoch, current_epoch);
}

#[test]
fn test_renew_lease_extends_expiry_beyond_original() {
    let (tree, _temp) = create_test_dir_tree();
    let lease_id = tree.acquire_lease("/renew/extend.txt", "client-1", 100);
    assert!(tree.has_active_lease("/renew/extend.txt"));

    std::thread::sleep(std::time::Duration::from_millis(50));

    tree.renew_lease(&lease_id, 60000);

    std::thread::sleep(std::time::Duration::from_millis(60));

    assert!(
        tree.has_active_lease("/renew/extend.txt"),
        "Lease should still be active after renewal despite original expiry"
    );
}

#[test]
fn test_renew_after_release_returns_none() {
    let (tree, _temp) = create_test_dir_tree();
    let lease_id = tree.acquire_lease("/renew/release.txt", "client-1", 60000);

    tree.release_lease(&lease_id);

    let result = tree.renew_lease(&lease_id, 60000);
    assert!(result.is_none(), "Should not renew released lease");
}

// ============================================================
// Job Complete Notification Tests (Feature 2)
// ============================================================

#[test]
fn test_complete_job_publishes_notification() {
    use powerfs_master::proto::powerfs::metadata_notification::EventType;

    let (tree, _temp) = create_test_dir_tree();

    let mut rx = tree.subscribe();
    tree.register_job_client("job-complete-1", "test-job", "client-a");
    tree.complete_job("job-complete-1");

    let notif = rx
        .try_recv()
        .expect("Should receive JOB_COMPLETE notification");
    assert_eq!(
        notif.event_type,
        EventType::JobComplete as i32,
        "Should be JOB_COMPLETE event"
    );
}

#[test]
fn test_complete_nonexistent_job_no_notification() {
    let (tree, _temp) = create_test_dir_tree();

    let mut rx = tree.subscribe();
    let result = tree.complete_job("nonexistent-job");
    assert!(result.is_none());
    assert!(
        rx.try_recv().is_err(),
        "Should not receive notification for nonexistent job"
    );
}

// ============================================================
// Job ID in Notification Tests (Feature 4)
// ============================================================

#[test]
fn test_notification_includes_job_id() {
    use powerfs_master::proto::powerfs::{Entry, FuseAttributes};

    let (tree, _temp) = create_test_dir_tree();

    tree.register_job_client("job-id-test", "test-job", "client-a");
    let mut rx = tree.subscribe();

    let entry = Entry {
        directory: "/".to_string(),
        name: "job_test_file".to_string(),
        attributes: Some(FuseAttributes {
            ino: 400,
            mode: 0o644,
            ..FuseAttributes::default()
        }),
        ..Entry::default()
    };
    tree.create_entry(entry, "test_client")
        .expect("create_entry failed");

    let notif = rx.try_recv().expect("Should receive notification");
    assert_eq!(
        notif.job_id, "job-id-test",
        "Notification should include job_id from registered job"
    );
}

#[test]
fn test_notification_without_job_has_empty_job_id() {
    use powerfs_master::proto::powerfs::{Entry, FuseAttributes};

    let (tree, _temp) = create_test_dir_tree();

    let mut rx = tree.subscribe();

    let entry = Entry {
        directory: "/".to_string(),
        name: "no_job_file".to_string(),
        attributes: Some(FuseAttributes {
            ino: 401,
            mode: 0o644,
            ..FuseAttributes::default()
        }),
        ..Entry::default()
    };
    tree.create_entry(entry, "test_client")
        .expect("create_entry failed");

    let notif = rx.try_recv().expect("Should receive notification");
    assert_eq!(
        notif.job_id, "",
        "Notification without registered job should have empty job_id"
    );
}
