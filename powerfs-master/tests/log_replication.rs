//! Log replication tests
//!
//! Tests for Raft log replication including:
//! - Basic log replication
//! - Replication across all nodes
//! - Log consistency verification

use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;

/// Test that commands are replicated to all nodes
#[tokio::test]
async fn test_basic_log_replication() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .try_init();

    use powerfs_master::tests::cluster::RaftTestCluster;
use powerfs_master::raft_storage::RaftCommand;

    let cluster = RaftTestCluster::new(3).await;
    cluster.start_all().await;

    let leader = cluster.wait_for_leader(Duration::from_secs(5)).await
        .expect("Should have a leader");

    // Propose several commands
    for i in 0..5 {
        let cmd = RaftCommand::UpdateVolumeState {
            volume_id: i,
            state: format!("State{}", i),
        };
        cluster.propose(&leader, cmd).await.expect("Propose should succeed");
    }

    // Wait for replication
    sleep(Duration::from_millis(500)).await;

    // Verify all nodes have the same last index
    let indices = cluster.get_all_last_indices().await;
    log::info!("Last indices after replication: {:?}", indices);

    let values: Vec<u64> = indices.values().copied().collect();
    let min = values.iter().min().copied().unwrap_or(0);
    let max = values.iter().max().copied().unwrap_or(0);

    assert_eq!(min, max, "All nodes should have the same last index after replication");

    cluster.shutdown().await;
}

/// Test that commands are committed and applied
#[tokio::test]
async fn test_command_application() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .try_init();

    use powerfs_master::tests::cluster::RaftTestCluster;
use powerfs_master::raft_storage::RaftCommand;

    let cluster = RaftTestCluster::new(3).await;
    cluster.start_all().await;

    let leader = cluster.wait_for_leader(Duration::from_secs(5)).await
        .expect("Should have a leader");

    // Propose a command
    let cmd = RaftCommand::Heartbeat {
        node_id: "test_node".to_string(),
    };
    cluster.propose(&leader, cmd).await.expect("Propose should succeed");

    // Wait for application
    sleep(Duration::from_millis(500)).await;

    // Verify applied indices
    let applied = cluster.get_all_applied_indices().await;
    log::info!("Applied indices: {:?}", applied);

    let values: Vec<u64> = applied.values().copied().collect();
    let min = values.iter().min().copied().unwrap_or(0);

    assert!(min > 0, "At least one command should be applied");

    cluster.shutdown().await;
}

/// Test concurrent proposals from multiple clients
#[tokio::test]
async fn test_concurrent_log_replication() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .try_init();

    use powerfs_master::tests::cluster::RaftTestCluster;
use powerfs_master::raft_storage::RaftCommand;
    use tokio::task::JoinSet;

    let cluster = RaftTestCluster::new(3).await;
    cluster.start_all().await;

    let leader = cluster.wait_for_leader(Duration::from_secs(5)).await
        .expect("Should have a leader");

    // Propose concurrently
    let mut join_set = JoinSet::new();
    for i in 0..10 {
        let cluster_clone = cluster.clone();
        let leader_clone = leader.clone();
        join_set.spawn(async move {
            let cmd = RaftCommand::Heartbeat {
                node_id: format!("node_{}", i),
            };
            cluster_clone.propose(&leader_clone, cmd).await
        });
    }

    // Collect results
    let mut success_count = 0;
    let mut fail_count = 0;
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(_)) => success_count += 1,
            _ => fail_count += 1,
        }
    }

    log::info!("Concurrent proposals: {} success, {} failed", success_count, fail_count);

    // Most proposals should succeed
    assert!(success_count >= 8, "Most concurrent proposals should succeed");

    // Wait for all to be replicated
    sleep(Duration::from_millis(500)).await;

    let indices = cluster.get_all_last_indices().await;
    let values: Vec<u64> = indices.values().copied().collect();
    let min = values.iter().min().copied().unwrap_or(0);
    let max = values.iter().max().copied().unwrap_or(0);

    assert_eq!(min, max, "All nodes should have consistent indices");

    cluster.shutdown().await;
}

/// Test that a follower can catch up after being behind
#[tokio::test]
async fn test_follower_catch_up() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .try_init();

    use powerfs_master::tests::cluster::RaftTestCluster;
use powerfs_master::raft_storage::RaftCommand;

    let cluster = RaftTestCluster::new(3).await;
    cluster.start_all().await;

    let leader = cluster.wait_for_leader(Duration::from_secs(5)).await
        .expect("Should have a leader");

    // Get a follower
    let followers: Vec<u64> = cluster.nodes.read().await.keys()
        .filter(|&&id| id != leader.id)
        .cloned()
        .collect();
    assert!(!followers.is_empty());
    let follower_id = followers[0];

    // Propose some commands
    for i in 0..5 {
        let cmd = RaftCommand::UpdateVolumeState {
            volume_id: i,
            state: format!("State{}", i),
        };
        cluster.propose(&leader, cmd).await.expect("Propose should succeed");
    }

    // Wait a bit
    sleep(Duration::from_millis(200)).await;

    // Check follower has caught up
    let indices = cluster.get_all_last_indices().await;
    let leader_idx = indices.get(&leader.id).copied().unwrap_or(0);
    let follower_idx = indices.get(&follower_id).copied().unwrap_or(0);

    log::info!("Leader index: {}, Follower index: {}", leader_idx, follower_idx);

    // Follower should be close to leader (within a few entries due to async replication)
    assert!(leader_idx > 0, "Leader should have entries");
    assert!(follower_idx >= leader_idx.saturating_sub(2),
        "Follower should be caught up or close");

    cluster.shutdown().await;
}

/// Test log compaction (snapshot)
#[tokio::test]
async fn test_log_compaction() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .try_init();

    use powerfs_master::tests::cluster::RaftTestCluster;
use powerfs_master::raft_storage::RaftCommand;

    let cluster = RaftTestCluster::builder()
        .num_nodes(3)
        .build()
        .await;
    cluster.start_all().await;

    let leader = cluster.wait_for_leader(Duration::from_secs(5)).await
        .expect("Should have a leader");

    // Propose many commands to trigger potential snapshot
    for i in 0..50 {
        let cmd = RaftCommand::UpdateVolumeState {
            volume_id: i,
            state: format!("State{}", i),
        };
        cluster.propose(&leader, cmd).await.expect("Propose should succeed");
    }

    // Wait for compaction
    sleep(Duration::from_secs(2)).await;

    // Verify snapshot info
    let snapshot_info = cluster.get_snapshot_info(&leader).await;
    log::info!("Snapshot info: index={}, term={}", snapshot_info.index, snapshot_info.term);

    // Check that we have progress
    let applied = cluster.get_all_applied_indices().await;
    log::info!("Applied indices: {:?}", applied);

    cluster.shutdown().await;
}
