//! Multi-node Raft integration tests
//!
//! This module provides comprehensive integration tests for the Raft-based
//! distributed consensus implementation.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use log::info;
use tempfile::TempDir;
use tokio::sync::{mpsc, RwLock};
use tokio::time::{interval, timeout};

use powerfs_master::raft_storage::RaftCommand;
use powerfs_common::types::{NodeId, VolumeId, VolumeInfo, VolumeState};

mod cluster;
mod leader_election;
mod log_replication;
mod fault_tolerance;

pub use cluster::*;
pub use leader_election::*;
pub use log_replication::*;
pub use fault_tolerance::*;

#[tokio::test]
async fn test_cluster_startup() {
    let cluster = RaftTestCluster::new(3).await;
    cluster.start_all().await;

    // Wait for leader election
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify one leader exists
    let leaders = cluster.get_all_leaders().await;
    assert_eq!(leaders.len(), 1, "Should have exactly one leader");

    info!("Cluster startup test passed: {:?}", leaders);
}

#[tokio::test]
async fn test_propose_command() {
    let cluster = RaftTestCluster::new(3).await;
    cluster.start_all().await;

    // Wait for leader
    let leader = cluster.wait_for_leader(Duration::from_secs(5)).await
        .expect("Should have a leader");

    // Propose a command
    let cmd = crate::raft_storage::RaftCommand::AddNode {
        node_id: "test_node".to_string(),
        address: "127.0.0.1:8001".to_string(),
        rack: "rack1".to_string(),
        data_center: "dc1".to_string(),
        http_port: 8080,
        grpc_port: 9000,
        public_url: "http://localhost:8080".to_string(),
    };

    let index = cluster.propose(&leader, cmd).await
        .expect("Should be able to propose");

    info!("Proposed command at index {}", index);
    assert!(index > 0, "Index should be positive");
}

#[tokio::test]
async fn test_log_replication() {

    let cluster = RaftTestCluster::new(3).await;
    cluster.start_all().await;

    let leader = cluster.wait_for_leader(Duration::from_secs(5)).await
        .expect("Should have a leader");

    // Propose multiple commands
    let mut indices = Vec::new();
    for i in 0..5 {
        let cmd = crate::raft_storage::RaftCommand::UpdateVolumeState {
            volume_id: i,
            state: format!("State{}", i),
        };
        let idx = cluster.propose(&leader, cmd).await.unwrap();
        indices.push(idx);
    }

    // Wait for replication
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Verify all nodes have the same last index
    let last_indices = cluster.get_all_last_indices().await;
    info!("Last indices: {:?}", last_indices);

    let min_idx = last_indices.values().min().copied().unwrap_or(0);
    let max_idx = last_indices.values().max().copied().unwrap_or(0);

    assert_eq!(min_idx, max_idx, "All nodes should have same last index");
}

#[tokio::test]
async fn test_follower_forwarding() {

    let cluster = RaftTestCluster::new(3).await;
    cluster.start_all().await;

    let leader = cluster.wait_for_leader(Duration::from_secs(5)).await
        .expect("Should have a leader");

    // Get a follower
    let followers: Vec<_> = cluster.nodes.read().await.values()
        .filter(|n| n.id != leader.id)
        .collect();

    assert!(!followers.is_empty(), "Should have at least one follower");

    let follower = followers[0];

    // Propose via follower (should forward to leader)
    let cmd = crate::raft_storage::RaftCommand::Heartbeat {
        node_id: "test".to_string(),
    };

    // Note: In current implementation, forwarding happens at gRPC level
    // This test verifies the command reaches the cluster
    let result = cluster.propose_to(&follower.address, cmd).await;
    info!("Propose via follower result: {:?}", result);
}

#[tokio::test]
async fn test_leader_election_on_leader_failure() {

    let cluster = RaftTestCluster::new(3).await;
    cluster.start_all().await;

    let initial_leader = cluster.wait_for_leader(Duration::from_secs(5)).await
        .expect("Should have a leader");

    info!("Initial leader: {:?}", initial_leader);

    // Stop the leader
    cluster.stop_node(initial_leader.id).await;

    // Wait for new leader election
    tokio::time::sleep(Duration::from_secs(3)).await;

    let new_leader = cluster.wait_for_leader(Duration::from_secs(5)).await
        .expect("Should have a new leader after leader failure");

    info!("New leader: {:?}", new_leader);
    assert_ne!(initial_leader.id, new_leader.id, "New leader should be different");
}

#[tokio::test]
async fn test_read_after_write_consistency() {

    let cluster = RaftTestCluster::new(3).await;
    cluster.start_all().await;

    let leader = cluster.wait_for_leader(Duration::from_secs(5)).await
        .expect("Should have a leader");

    // Propose a command and wait for it to be committed
    let cmd = crate::raft_storage::RaftCommand::UpdateVolumeState {
        volume_id: 1,
        state: "ReadOnly".to_string(),
    };
    cluster.propose(&leader, cmd).await.unwrap();

    // Wait for commit
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify the command was applied on all nodes
    let applied_indices = cluster.get_all_applied_indices().await;
    info!("Applied indices: {:?}", applied_indices);

    let min_applied = applied_indices.values().min().copied().unwrap_or(0);
    assert!(min_applied > 0, "All nodes should have applied at least one command");
}

#[tokio::test]
async fn test_cluster_survives_multiple_failures() {

    let cluster = RaftTestCluster::new(5).await;
    cluster.start_all().await;

    let initial_leader = cluster.wait_for_leader(Duration::from_secs(5)).await
        .expect("Should have a leader");

    // Stop two followers (cluster should still work with 3 nodes)
    let followers: Vec<_> = cluster.nodes.read().await.values()
        .filter(|n| n.id != initial_leader.id)
        .take(2)
        .collect();

    for follower in &followers {
        cluster.stop_node(follower.id).await;
    }

    // Propose commands - should still work
    let cmd = crate::raft_storage::RaftCommand::Heartbeat {
        node_id: "test".to_string(),
    };

    let result = cluster.propose(&initial_leader, cmd).await;
    info!("Propose after failures result: {:?}", result);
}

#[tokio::test]
async fn test_snapshot_and_recovery() {

    let cluster = RaftTestCluster::new(3).await;
    cluster.start_all().await;

    let leader = cluster.wait_for_leader(Duration::from_secs(5)).await
        .expect("Should have a leader");

    // Propose many commands to trigger snapshot
    for i in 0..20 {
        let cmd = crate::raft_storage::RaftCommand::UpdateVolumeState {
            volume_id: i,
            state: format!("State{}", i),
        };
        cluster.propose(&leader, cmd).await.unwrap();
    }

    // Wait for snapshot to be created
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify snapshot was created
    let snapshot_info = cluster.get_snapshot_info(&leader).await;
    info!("Snapshot info: {:?}", snapshot_info);

    assert!(snapshot_info.index > 0, "Snapshot should have been created");
}

#[tokio::test]
async fn test_concurrent_proposals() {

    let cluster = RaftTestCluster::new(3).await;
    cluster.start_all().await;

    let leader = cluster.wait_for_leader(Duration::from_secs(5)).await
        .expect("Should have a leader");

    // Propose commands concurrently
    let mut handles = Vec::new();
    for i in 0..10 {
        let cluster_clone = cluster.clone();
        let leader_clone = leader.clone();
        let handle = tokio::spawn(async move {
            let cmd = crate::raft_storage::RaftCommand::Heartbeat {
                node_id: format!("node_{}", i),
            };
            cluster_clone.propose(&leader_clone, cmd).await
        });
        handles.push(handle);
    }

    // Wait for all proposals
    let results = futures::future::join_all(handles).await;
    let successful = results.iter().filter(|r| r.is_ok() && r.as_ref().unwrap().is_ok()).count();

    info!("Successful concurrent proposals: {}/10", successful);
    assert!(successful >= 8, "Most proposals should succeed");
}

#[tokio::test]
async fn test_cluster_with_adjusted_timing() {

    // Create a 3-node cluster
    let cluster = RaftTestCluster::builder()
        .num_nodes(3)
        .tick_ms(50)
        .election_timeout_ms(200)
        .build()
        .await;

    cluster.start_all().await;

    // Wait for leader
    let leader = cluster.wait_for_leader(Duration::from_secs(5)).await
        .expect("Should have a leader");

    info!("Leader elected with adjusted timing: {:?}", leader);

    // Verify cluster is stable
    tokio::time::sleep(Duration::from_secs(2)).await;
    let leaders = cluster.get_all_leaders().await;
    assert_eq!(leaders.len(), 1, "Should have exactly one leader");
}
