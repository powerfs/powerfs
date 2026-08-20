use std::sync::Arc;

use tempfile::tempdir;
use tokio::runtime::Runtime;

use crate::meta_shard_manager::MetaShardManager;
use crate::raft_group_manager_v2::{Peer, ShardId};
use crate::raft_group_manager_v2::RaftGroupManagerV2;
use crate::shard_store::ShardStore;
use crate::shard_strategy::ShardStrategy;

/// Wait until the local node becomes leader of `shard_id`, or panic.
async fn wait_for_leadership(mgr: &RaftGroupManagerV2, shard_id: ShardId) {
    for _ in 0..100 {
        if mgr.is_shard_leader(shard_id).await {
            return;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }
    panic!("node did not become leader of shard {}", shard_id.0);
}

#[test]
fn test_shard_creation_and_file_operations() {
    let rt = Runtime::new().unwrap();
    let _guard = rt.enter();

    let tmp_dir = tempdir().unwrap();
    let data_path = tmp_dir.path().to_str().unwrap().to_string();

    let shard_strategy = Arc::new(ShardStrategy::new(4));

    rt.block_on(async {
        let raft_group_manager = RaftGroupManagerV2::new(
            1,
            "127.0.0.1:50051".to_string(),
            format!("{}/raft", data_path),
        )
        .await
        .expect("failed to create RaftGroupManagerV2");

        let meta_shard_manager = Arc::new(MetaShardManager::new(
            raft_group_manager.clone(),
            shard_strategy.clone(),
            data_path,
            1, // node_id for testing
        ));

        let peers = vec![Peer {
            id: 1,
            address: "127.0.0.1:50051".to_string(),
            net_address: "127.0.0.1:50052".to_string(),
        }];

        let result = meta_shard_manager.create_shard(ShardId(0), peers).await;
        assert!(result.is_ok(), "Failed to create shard: {:?}", result);

        wait_for_leadership(&raft_group_manager, ShardId(0)).await;

        meta_shard_manager.register_root_inode("test-bucket", 1);

        let result = meta_shard_manager
            .create_directory(1, "test-dir", 0o040755, 0, 0)
            .await;
        assert!(result.is_ok(), "Failed to create directory: {:?}", result);

        let dir_info = result.unwrap();
        let dir_inode = dir_info.inode;

        let result = meta_shard_manager.create_file(dir_inode, "test-file").await;
        assert!(result.is_ok(), "Failed to create file: {:?}", result);

        let file_info = result.unwrap();
        let file_inode = file_info.inode;

        let inode_info = meta_shard_manager.get_inode(file_inode);
        assert!(inode_info.is_some(), "File inode not found");
        assert_eq!(inode_info.unwrap().name, "test-file");

        let entries = meta_shard_manager.list_directory(dir_inode);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "test-file");

        let result = meta_shard_manager.delete_file(dir_inode, "test-file").await;
        assert!(result.is_ok(), "Failed to delete file: {:?}", result);

        let entries = meta_shard_manager.list_directory(dir_inode);
        assert_eq!(entries.len(), 0);
    });
}

#[test]
fn test_shard_strategy() {
    let strategy = ShardStrategy::new(4);

    let shard0 = strategy.calculate_shard(0);
    let shard1 = strategy.calculate_shard(1000);
    let shard2 = strategy.calculate_shard(2000);
    let shard3 = strategy.calculate_shard(3000);

    assert_eq!(shard0.0, 0);
    assert_eq!(shard1.0, 0);
    assert_eq!(shard2.0, 1);
    assert_eq!(shard3.0, 1);

    let range0 = strategy.get_shard_range(ShardId(0));
    assert_eq!(range0, (0, 16384));

    let range1 = strategy.get_shard_range(ShardId(1));
    assert_eq!(range1, (16384, 32768));
}

#[test]
fn test_shard_store_persistence() {
    let tmp_dir = tempdir().unwrap();
    let db_path = tmp_dir.path().to_str().unwrap().to_string();

    let store = ShardStore::new(ShardId(0), (0, 16384), &db_path).unwrap();

    store.apply_command(crate::raft_group_manager_v2::ShardCommand::CreateFile {
        parent_inode: 1,
        name: "persisted-file".to_string(),
        inode: 100,
    });

    let info = store.get_inode(100);
    assert!(info.is_some());
    assert_eq!(info.unwrap().name, "persisted-file");

    drop(store);

    let store2 = ShardStore::new(ShardId(0), (0, 16384), &db_path).unwrap();

    let info2 = store2.get_inode(100);
    assert!(info2.is_some());
    assert_eq!(info2.unwrap().name, "persisted-file");
}

#[test]
fn test_path_resolution() {
    let rt = Runtime::new().unwrap();
    let _guard = rt.enter();

    let tmp_dir = tempdir().unwrap();
    let data_path = tmp_dir.path().to_str().unwrap().to_string();

    let shard_strategy = Arc::new(ShardStrategy::new(4));

    rt.block_on(async {
        let raft_group_manager = RaftGroupManagerV2::new(
            1,
            "127.0.0.1:50053".to_string(),
            format!("{}/raft", data_path),
        )
        .await
        .expect("failed to create RaftGroupManagerV2");

        let meta_shard_manager = Arc::new(MetaShardManager::new(
            raft_group_manager.clone(),
            shard_strategy,
            data_path,
            1, // node_id for testing
        ));

        let peers = vec![Peer {
            id: 1,
            address: "127.0.0.1:50053".to_string(),
            net_address: "127.0.0.1:50054".to_string(),
        }];

        meta_shard_manager
            .create_shard(ShardId(0), peers)
            .await
            .unwrap();
        wait_for_leadership(&raft_group_manager, ShardId(0)).await;

        meta_shard_manager.register_root_inode("my-bucket", 1);

        meta_shard_manager
            .create_directory(1, "level1", 0o040755, 0, 0)
            .await
            .unwrap();
        let level1_info = meta_shard_manager.lookup(1, "level1").unwrap();

        meta_shard_manager
            .create_directory(level1_info.inode, "level2", 0o040755, 0, 0)
            .await
            .unwrap();
        let level2_info = meta_shard_manager
            .lookup(level1_info.inode, "level2")
            .unwrap();

        meta_shard_manager
            .create_file(level2_info.inode, "test.txt")
            .await
            .unwrap();

        let resolved_inode = meta_shard_manager
            .resolve_path("my-bucket/level1/level2/test.txt")
            .await;
        assert!(resolved_inode.is_ok());
    });
}
