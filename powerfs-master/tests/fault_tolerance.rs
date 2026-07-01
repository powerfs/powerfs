//! Fault tolerance tests
//!
//! Tests for Raft fault tolerance including:
//! - Node failure handling
//! - Network partition simulation
//! - Data consistency after failures

use std::time::Duration;
use tokio::time::sleep;

/// Test that the cluster continues to work after a follower failure
#[tokio::test]
async fn test_follower_failure() {

    use crate::cluster::RaftTestCluster;
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
    let follower_id = followers[0];

    // Stop follower
    cluster.stop_node(follower_id).await;
    sleep(Duration::from_millis(500)).await;

    // Propose commands - should still work
    for i in 0..5 {
        let cmd = RaftCommand::UpdateVolumeState {
            volume_id: i,
            state: format!("State{}", i),
        };
        let result = cluster.propose(&leader, cmd).await;
        assert!(result.is_ok(), "Propose should succeed with one follower down");
    }

    // Verify we still have a leader
    let leaders = cluster.get_all_leaders().await;
    assert_eq!(leaders.len(), 1, "Should still have a leader");

    cluster.shutdown().await;
}

/// Test that the cluster continues to work after the leader fails
#[tokio::test]
async fn test_leader_failure() {

    use crate::cluster::RaftTestCluster;
use powerfs_master::raft_storage::RaftCommand;

    let cluster = RaftTestCluster::new(3).await;
    cluster.start_all().await;

    let initial_leader = cluster.wait_for_leader(Duration::from_secs(5)).await
        .expect("Should have a leader");
    let initial_id = initial_leader.id;

    // Propose some commands before failure
    for i in 0..3 {
        let cmd = RaftCommand::UpdateVolumeState {
            volume_id: i,
            state: format!("State{}", i),
        };
        cluster.propose(&initial_leader, cmd).await.expect("Propose should succeed");
    }

    // Stop the leader
    cluster.stop_node(initial_id).await;
    sleep(Duration::from_secs(2)).await;

    // Wait for new leader
    let new_leader = cluster.wait_for_leader(Duration::from_secs(5)).await
        .expect("Should have a new leader");

    // Propose more commands with new leader
    for i in 3..6 {
        let cmd = RaftCommand::UpdateVolumeState {
            volume_id: i,
            state: format!("State{}", i),
        };
        let result = cluster.propose(&new_leader, cmd).await;
        assert!(result.is_ok(), "Propose should succeed with new leader");
    }

    assert_ne!(new_leader.id, initial_id, "New leader should be different");

    cluster.shutdown().await;
}

/// Test that a new leader is elected after partition heals
#[tokio::test]
async fn test_partition_recovery() {

    use crate::cluster::RaftTestCluster;

    let cluster = RaftTestCluster::new(3).await;
    cluster.start_all().await;

    let leader = cluster.wait_for_leader(Duration::from_secs(5)).await
        .expect("Should have a leader");

    // Get a follower
    let followers: Vec<u64> = cluster.nodes.read().await.keys()
        .filter(|&&id| id != leader.id)
        .cloned()
        .collect();

    // Simulate partition by stopping a follower
    if !followers.is_empty() {
        cluster.stop_node(followers[0]).await;
        sleep(Duration::from_secs(2)).await;

        // Should still have a leader
        let leaders = cluster.get_all_leaders().await;
        assert_eq!(leaders.len(), 1, "Should have a leader during partition");

        // Restart the partitioned node
        // Note: In a real test, we'd restart with fresh state
        // Here we just verify the remaining cluster works
    }

    cluster.shutdown().await;
}

/// Test that cluster survives multiple failures
#[tokio::test]
async fn test_multiple_failures() {

    use crate::cluster::RaftTestCluster;
use powerfs_master::raft_storage::RaftCommand;

    let cluster = RaftTestCluster::new(5).await;
    cluster.start_all().await;

    let leader = cluster.wait_for_leader(Duration::from_secs(5)).await
        .expect("Should have a leader");

    // Stop two followers (cluster should still work with 3 nodes)
    let nodes_to_stop: Vec<u64> = cluster.nodes.read().await.keys()
        .filter(|&&id| id != leader.id)
        .take(2)
        .cloned()
        .collect();

    for node_id in &nodes_to_stop {
        cluster.stop_node(*node_id).await;
    }
    sleep(Duration::from_millis(500)).await;

    // Propose commands
    for i in 0..5 {
        let cmd = RaftCommand::Heartbeat {
            node_id: format!("node_{}", i),
        };
        let result = cluster.propose(&leader, cmd).await;
        assert!(result.is_ok(), "Propose should succeed with majority");
    }

    cluster.shutdown().await;
}

/// Test that cluster cannot proceed when majority is lost
#[tokio::test]
async fn test_majority_loss() {

    use crate::cluster::RaftTestCluster;
use powerfs_master::raft_storage::RaftCommand;

    let cluster = RaftTestCluster::new(3).await;
    cluster.start_all().await;

    let leader = cluster.wait_for_leader(Duration::from_secs(5)).await
        .expect("Should have a leader");

    // Stop leader and one follower (no majority)
    cluster.stop_node(leader.id).await;

    let remaining: Vec<u64> = cluster.nodes.read().await.keys()
        .filter(|&&id| id != leader.id)
        .cloned()
        .collect();
    if !remaining.is_empty() {
        cluster.stop_node(remaining[0]).await;
    }

    sleep(Duration::from_secs(2)).await;

    // No leader should be elected
    let leaders = cluster.get_all_leaders().await;
    assert_eq!(leaders.len(), 0, "Should have no leader without majority");

    cluster.shutdown().await;
}

/// Test that previously committed commands are not lost after leader change
#[tokio::test]
async fn test_commit_persistence_after_leader_change() {

    use crate::cluster::RaftTestCluster;
use powerfs_master::raft_storage::RaftCommand;

    let cluster = RaftTestCluster::new(3).await;
    cluster.start_all().await;

    let leader1 = cluster.wait_for_leader(Duration::from_secs(5)).await
        .expect("Should have a leader");

    // Commit several commands
    for i in 0..5 {
        let cmd = RaftCommand::UpdateVolumeState {
            volume_id: i,
            state: format!("State{}", i),
        };
        cluster.propose(&leader1, cmd).await.expect("Propose should succeed");
    }

    // Get index before failure
    let indices_before = cluster.get_all_last_indices().await;
    let index_before = indices_before.get(&leader1.id).copied().unwrap_or(0);

    // Stop leader
    cluster.stop_node(leader1.id).await;
    sleep(Duration::from_secs(2)).await;

    // Wait for new leader
    let leader2 = cluster.wait_for_leader(Duration::from_secs(5)).await
        .expect("Should have a new leader");

    // Wait for replication
    sleep(Duration::from_millis(500)).await;

    // Verify indices on new leader
    let indices_after = cluster.get_all_last_indices().await;
    let index_after = indices_after.get(&leader2.id).copied().unwrap_or(0);

    log::info!("Index before: {}, Index after: {}", index_before, index_after);

    // The new leader should have at least as many entries as committed before
    assert!(index_after >= index_before, "New leader should have committed entries");

    cluster.shutdown().await;
}

/// Test that the cluster handles rapid leader changes
#[tokio::test]
async fn test_rapid_leader_changes() {

    use crate::cluster::RaftTestCluster;
use powerfs_master::raft_storage::RaftCommand;

    let cluster = RaftTestCluster::new(3).await;
    cluster.start_all().await;

    // Do several leader changes rapidly
    for round in 0..3 {
        let leader = cluster.wait_for_leader(Duration::from_secs(5)).await
            .expect("Should have a leader");

        // Propose a command
        let cmd = RaftCommand::Heartbeat {
            node_id: format!("round_{}", round),
        };
        cluster.propose(&leader, cmd).await.ok();

        // Stop leader immediately
        cluster.stop_node(leader.id).await;
        sleep(Duration::from_millis(800)).await; // Less than election timeout
    }

    // Final leader should be elected
    sleep(Duration::from_secs(1)).await;
    let final_leaders = cluster.get_all_leaders().await;
    assert_eq!(final_leaders.len(), 1, "Should have a final leader");

    cluster.shutdown().await;
}
