use powerfs_master::metadata_manager::MetadataManager;
use powerfs_master::proto::powerfs::Entry;
use powerfs_orset::MergePolicy;
use rocksdb::DB;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn test_create_create_conflict_detection() {
    let dir = tempdir().unwrap();
    let db = Arc::new(DB::open_default(dir.path()).unwrap());

    let mgr = MetadataManager::new(db.clone());

    let entry1 = Entry {
        name: "test.txt".to_string(),
        directory: "/".to_string(),
        attributes: Some(powerfs_master::proto::powerfs::FuseAttributes {
            mode: 0o644,
            size: 100,
            ino: 101,
            ..Default::default()
        }),
        ..Default::default()
    };

    mgr.send_event(powerfs_master::metadata_manager::MetadataEvent::Create {
        client_id: "client1".to_string(),
        client_id_num: 1,
        entry: entry1,
        parent_ino: 1,
        inode: 101,
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let conflicts1 = mgr.get_conflicts(1, false);
    println!("After client 1: {} conflicts", conflicts1.len());

    let entry2 = Entry {
        name: "test.txt".to_string(),
        directory: "/".to_string(),
        attributes: Some(powerfs_master::proto::powerfs::FuseAttributes {
            mode: 0o644,
            size: 200,
            ino: 102,
            ..Default::default()
        }),
        ..Default::default()
    };

    mgr.send_event(powerfs_master::metadata_manager::MetadataEvent::Create {
        client_id: "client2".to_string(),
        client_id_num: 2,
        entry: entry2,
        parent_ino: 1,
        inode: 102,
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let conflicts2 = mgr.get_conflicts(1, false);
    println!("After client 2: {} conflicts", conflicts2.len());

    for c in &conflicts2 {
        println!("  Conflict: {:?}", c.conflict_type);
    }

    assert!(
        !conflicts2.is_empty(),
        "Should detect CreateCreate conflict"
    );
}

#[tokio::test]
async fn test_merge_policy_setting() {
    let dir = tempdir().unwrap();
    let db = Arc::new(DB::open_default(dir.path()).unwrap());

    let mgr = MetadataManager::new(db.clone());

    mgr.set_merge_policy(100, MergePolicy::WritePriority);

    let entry1 = Entry {
        name: "policy_test.txt".to_string(),
        directory: "/dir".to_string(),
        attributes: Some(powerfs_master::proto::powerfs::FuseAttributes {
            mode: 0o644,
            size: 100,
            ino: 201,
            ..Default::default()
        }),
        ..Default::default()
    };

    mgr.send_event(powerfs_master::metadata_manager::MetadataEvent::Create {
        client_id: "client1".to_string(),
        client_id_num: 1,
        entry: entry1,
        parent_ino: 100,
        inode: 201,
    });

    let entry2 = Entry {
        name: "policy_test.txt".to_string(),
        directory: "/dir".to_string(),
        attributes: Some(powerfs_master::proto::powerfs::FuseAttributes {
            mode: 0o644,
            size: 200,
            ino: 202,
            ..Default::default()
        }),
        ..Default::default()
    };

    mgr.send_event(powerfs_master::metadata_manager::MetadataEvent::Create {
        client_id: "client2".to_string(),
        client_id_num: 2,
        entry: entry2,
        parent_ino: 100,
        inode: 202,
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let conflicts = mgr.get_conflicts(100, false);
    println!("Conflicts with WritePriority: {}", conflicts.len());

    let resolved = mgr.auto_resolve_conflicts(100, MergePolicy::Aggressive);
    println!("Auto-resolved: {} conflicts", resolved);

    let remaining = mgr.get_conflicts(100, true);
    println!("Remaining unresolved after resolve: {} conflicts", remaining.len());

    assert_eq!(remaining.len(), 0, "All conflicts should be resolved");
}
