//! Leader election tests
//!
//! Tests for Raft leader election behavior including:
//! - Initial leader election
//! - Leader re-election after failure
//! - Election timeout handling

use std::time::Duration;
use tokio::time::sleep;

/// Test that a single leader is elected in a new cluster
#[tokio::test]
async fn test_single_leader_election() {

    use crate::cluster::RaftTestCluster;

    let cluster = RaftTestCluster::new(3).await;
    cluster.start_all().await;

    // Wait for leader
    let leader = cluster.wait_for_leader(Duration::from_secs(5)).await;

    assert!(leader.is_some(), "A leader should be elected");
    assert_eq!(cluster.get_all_leaders().await.len(), 1, "Should have exactly one leader");

    cluster.shutdown().await;
}

/// Test that a new leader is elected when the current leader fails
#[tokio::test]
async fn test_leader_reelection_after_failure() {

    use crate::cluster::RaftTestCluster;

    let cluster = RaftTestCluster::new(3).await;
    cluster.start_all().await;

    // Wait for initial leader
    let initial_leader = cluster.wait_for_leader(Duration::from_secs(5)).await
        .expect("Should have a leader");
    let initial_id = initial_leader.id;

    // Stop the leader
    cluster.stop_node(initial_id).await;

    // Wait for new leader
    sleep(Duration::from_secs(2)).await;
    let new_leader = cluster.wait_for_leader(Duration::from_secs(5)).await
        .expect("Should have a new leader");

    assert_ne!(new_leader.id, initial_id, "New leader should be different from the failed one");

    cluster.shutdown().await;
}

/// Test that cluster can still elect a leader with minority nodes down
#[tokio::test]
async fn test_election_with_quorum() {

    use crate::cluster::RaftTestCluster;

    let cluster = RaftTestCluster::new(5).await;
    cluster.start_all().await;

    // Wait for leader
    let leader = cluster.wait_for_leader(Duration::from_secs(5)).await
        .expect("Should have a leader");

    // Stop 2 nodes (less than quorum)
    cluster.stop_node(1).await;
    cluster.stop_node(2).await;

    // Should still have a leader
    sleep(Duration::from_secs(2)).await;
    let remaining_leaders = cluster.get_all_leaders().await;

    assert!(!remaining_leaders.is_empty() || remaining_leaders.len() == 1,
        "Should have 0 or 1 leader with 3 nodes remaining");

    cluster.shutdown().await;
}

/// Test that no leader is elected when majority is unavailable
#[tokio::test]
async fn test_no_leader_without_quorum() {

    use crate::cluster::RaftTestCluster;

    let cluster = RaftTestCluster::new(3).await;
    cluster.start_all().await;

    // Wait for leader
    let leader = cluster.wait_for_leader(Duration::from_secs(5)).await
        .expect("Should have a leader");

    // Stop 2 nodes (no quorum for 3-node cluster)
    cluster.stop_node(leader.id).await;
    let other_nodes: Vec<u64> = cluster.nodes.read().await.keys()
        .filter(|&&id| id != leader.id)
        .cloned()
        .collect();
    if other_nodes.len() >= 2 {
        cluster.stop_node(other_nodes[0]).await;
        cluster.stop_node(other_nodes[1]).await;
    }

    // Wait and check no leader
    sleep(Duration::from_secs(2)).await;
    let leaders = cluster.get_all_leaders().await;

    assert_eq!(leaders.len(), 0, "Should have no leader without quorum");

    cluster.shutdown().await;
}

/// Test that the same leader is maintained when the cluster is stable
#[tokio::test]
async fn test_leader_stability() {

    use crate::cluster::RaftTestCluster;

    let cluster = RaftTestCluster::new(3).await;
    cluster.start_all().await;

    // Wait for leader
    let leader = cluster.wait_for_leader(Duration::from_secs(5)).await
        .expect("Should have a leader");
    let leader_id = leader.id;

    // Wait for several seconds and check leader doesn't change
    for _ in 0..5 {
        sleep(Duration::from_secs(1)).await;
        let current_leaders = cluster.get_all_leaders().await;
        if current_leaders.len() == 1 {
            assert_eq!(current_leaders[0].id, leader_id, "Leader should be stable");
        }
    }

    cluster.shutdown().await;
}
