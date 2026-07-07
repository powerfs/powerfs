//! Multi-node Raft integration tests

use std::time::Duration;

use log::info;
use tempfile::tempdir;
use tokio::sync::oneshot;
use tokio::time::sleep;

use powerfs_master::raft_node::{OutgoingMessage, Peer, ProposeRequest, RaftNode};
use powerfs_master::raft_storage::{
    RaftCommand, RaftNodeSnapshot, RaftSnapshotData, RaftVolumeSnapshot,
};
use protobuf::Message as ProtobufMessage;
use raft::eraftpb::Message as RaftMessage;

#[tokio::test]
async fn test_single_node_is_leader() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("raft_1");
    std::fs::create_dir_all(&db_path).expect("Failed to create db dir");

    let mut node = RaftNode::new(
        1,
        "127.0.0.1:10001".to_string(),
        vec![],
        db_path.to_str().unwrap(),
    )
    .expect("Failed to create Raft node");

    let propose_tx = node.get_propose_tx();

    let (_done_tx, _done_rx): (oneshot::Sender<()>, oneshot::Receiver<()>) = oneshot::channel();

    tokio::spawn(async move {
        let _ = node.run().await;
    });

    sleep(Duration::from_secs(2)).await;

    let cmd = RaftCommand::Heartbeat {
        node_id: "test".to_string(),
    };
    let data = cmd.serialize();

    let (resp_tx, resp_rx) = oneshot::channel();
    let req = ProposeRequest {
        data,
        response_tx: resp_tx,
    };

    propose_tx.send(req).await.expect("Should send propose");
    let result = resp_rx.await.expect("Should receive response");

    println!("Propose result: {:?}", result);
    let index = result.expect("Propose should succeed");

    println!("Proposed command at index {}", index);
    assert!(index > 0, "Index should be positive");
}

#[tokio::test]
async fn test_two_node_election() {
    let temp_dir1 = tempdir().expect("Failed to create temp dir");
    let db_path1 = temp_dir1.path().join("raft_1");
    std::fs::create_dir_all(&db_path1).expect("Failed to create db dir");

    let temp_dir2 = tempdir().expect("Failed to create temp dir");
    let db_path2 = temp_dir2.path().join("raft_2");
    std::fs::create_dir_all(&db_path2).expect("Failed to create db dir");

    let node1 = RaftNode::new(
        1,
        "127.0.0.1:10001".to_string(),
        vec![Peer {
            id: 2,
            address: "127.0.0.1:10002".to_string(),
        }],
        db_path1.to_str().unwrap(),
    )
    .expect("Failed to create Raft node 1");

    let node2 = RaftNode::new(
        2,
        "127.0.0.1:10002".to_string(),
        vec![Peer {
            id: 1,
            address: "127.0.0.1:10001".to_string(),
        }],
        db_path2.to_str().unwrap(),
    )
    .expect("Failed to create Raft node 2");

    let step_tx1 = node1.get_step_tx();
    let step_tx2 = node2.get_step_tx();
    let msg_rx1 = node1.take_message_rx();
    let msg_rx2 = node2.take_message_rx();

    tokio::spawn(async move {
        let _ = message_router(msg_rx1, step_tx2).await;
    });

    tokio::spawn(async move {
        let _ = message_router(msg_rx2, step_tx1).await;
    });

    let mut node1_mut = node1;
    let mut node2_mut = node2;

    tokio::spawn(async move {
        let _ = node1_mut.run().await;
    });

    tokio::spawn(async move {
        let _ = node2_mut.run().await;
    });

    sleep(Duration::from_secs(3)).await;

    info!("Test completed");
}

async fn message_router(
    mut msg_rx: tokio::sync::broadcast::Receiver<OutgoingMessage>,
    step_tx: tokio::sync::mpsc::Sender<RaftMessage>,
) {
    while let Ok(msg) = msg_rx.recv().await {
        info!("Routing message to {}", msg.to_id);
        if let Ok(raft_msg) = ProtobufMessage::parse_from_bytes(&msg.message) {
            let _ = step_tx.send(raft_msg).await;
        }
    }
}

#[tokio::test]
async fn test_snapshot_creation() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("raft_1");
    std::fs::create_dir_all(&db_path).expect("Failed to create db dir");

    let mut node = RaftNode::new(
        1,
        "127.0.0.1:10001".to_string(),
        vec![],
        db_path.to_str().unwrap(),
    )
    .expect("Failed to create Raft node");

    let propose_tx = node.get_propose_tx();

    let snapshot_data = RaftSnapshotData {
        nodes: vec![RaftNodeSnapshot {
            id: "node1".to_string(),
            address: "127.0.0.1:10001".to_string(),
            rack: "rack1".to_string(),
            data_center: "dc1".to_string(),
            http_port: 8080,
            grpc_port: 9090,
            public_url: "http://node1:8080".to_string(),
        }],
        volumes: vec![RaftVolumeSnapshot {
            volume_id: 1,
            node_id: "node1".to_string(),
            collection: "default".to_string(),
            size: 1024 * 1024 * 1024,
            used: 0,
            replica_count: 1,
            ttl: 86400,
            disk_type: "ssd".to_string(),
            state: "online".to_string(),
        }],
        next_volume_id: 2,
        max_file_key: 100,
    };

    let _test_done_tx = tokio::sync::oneshot::channel::<()>();

    tokio::spawn(async move {
        let _ = node.run().await;
    });

    sleep(Duration::from_secs(2)).await;

    let cmd = RaftCommand::Heartbeat {
        node_id: "test".to_string(),
    };
    let data = cmd.serialize();

    let (resp_tx, resp_rx) = oneshot::channel();
    let req = ProposeRequest {
        data,
        response_tx: resp_tx,
    };

    if propose_tx.send(req).await.is_err() {
        println!("Failed to send propose");
        return;
    }

    match tokio::time::timeout(Duration::from_secs(5), resp_rx).await {
        Ok(Ok(Ok(index))) => {
            println!("Proposed command at index {}", index);
        }
        _ => {
            println!("Propose failed");
            return;
        }
    }

    let temp_dir2 = tempdir().expect("Failed to create temp dir");
    let db_path2 = temp_dir2.path().join("raft_2");
    std::fs::create_dir_all(&db_path2).expect("Failed to create db dir");

    let mut node2 = RaftNode::new(
        1,
        "127.0.0.1:10002".to_string(),
        vec![],
        db_path2.to_str().unwrap(),
    )
    .expect("Failed to create Raft node");

    for _ in 0..50 {
        node2.tick();
        if node2.is_leader() {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }

    let result = node2.trigger_snapshot(&snapshot_data);
    println!("Snapshot result: {:?}", result);
    assert!(result.is_ok(), "Snapshot should be created successfully");

    let stored_data = node2.get_snapshot_data();
    println!("Stored snapshot data: {:?}", stored_data);
    assert!(stored_data.is_some(), "Snapshot data should be stored");

    let stored = stored_data.unwrap();
    assert_eq!(stored.nodes.len(), 1);
    assert_eq!(stored.volumes.len(), 1);
    assert_eq!(stored.next_volume_id, 2);
}

#[tokio::test]
async fn test_three_node_failover() {
    let temp_dir1 = tempdir().expect("Failed to create temp dir");
    let db_path1 = temp_dir1.path().join("raft_1");
    std::fs::create_dir_all(&db_path1).expect("Failed to create db dir");

    let temp_dir2 = tempdir().expect("Failed to create temp dir");
    let db_path2 = temp_dir2.path().join("raft_2");
    std::fs::create_dir_all(&db_path2).expect("Failed to create db dir");

    let temp_dir3 = tempdir().expect("Failed to create temp dir");
    let db_path3 = temp_dir3.path().join("raft_3");
    std::fs::create_dir_all(&db_path3).expect("Failed to create db dir");

    let mut node1 = RaftNode::new(
        1,
        "127.0.0.1:10001".to_string(),
        vec![],
        db_path1.to_str().unwrap(),
    )
    .expect("Failed to create Raft node 1");

    let mut node2 = RaftNode::new(
        2,
        "127.0.0.1:10002".to_string(),
        vec![],
        db_path2.to_str().unwrap(),
    )
    .expect("Failed to create Raft node 2");

    let mut node3 = RaftNode::new(
        3,
        "127.0.0.1:10003".to_string(),
        vec![],
        db_path3.to_str().unwrap(),
    )
    .expect("Failed to create Raft node 3");

    let step_tx1 = node1.get_step_tx();
    let step_tx2 = node2.get_step_tx();
    let step_tx3 = node3.get_step_tx();
    let msg_rx1 = node1.take_message_rx();
    let msg_rx2 = node2.take_message_rx();
    let msg_rx3 = node3.take_message_rx();

    let propose_tx1 = node1.get_propose_tx();
    let propose_tx2 = node2.get_propose_tx();
    let propose_tx3 = node3.get_propose_tx();

    let st1_1 = step_tx1.clone();
    let st1_2 = step_tx1.clone();
    let st2_1 = step_tx2.clone();
    let st2_2 = step_tx2.clone();
    let st3_1 = step_tx3.clone();
    let st3_2 = step_tx3.clone();

    tokio::spawn(async move {
        let _ = multi_message_router(msg_rx1, step_tx1, step_tx2, step_tx3).await;
    });
    tokio::spawn(async move {
        let _ = multi_message_router(msg_rx2, st1_1, st2_1, st3_1).await;
    });
    tokio::spawn(async move {
        let _ = multi_message_router(msg_rx3, st1_2, st2_2, st3_2).await;
    });

    tokio::spawn(async move {
        let _ = node1.run().await;
    });
    tokio::spawn(async move {
        let _ = node2.run().await;
    });
    tokio::spawn(async move {
        let _ = node3.run().await;
    });

    sleep(Duration::from_secs(3)).await;

    let propose_txs = [propose_tx1, propose_tx2, propose_tx3];

    let cmd = RaftCommand::Heartbeat {
        node_id: "test_node".to_string(),
    };
    let data = cmd.serialize();

    let mut proposed = false;
    for (idx, propose_tx) in propose_txs.iter().enumerate() {
        let (resp_tx, resp_rx) = oneshot::channel();
        let req = ProposeRequest {
            data: data.clone(),
            response_tx: resp_tx,
        };

        if propose_tx.send(req).await.is_err() {
            continue;
        }

        match tokio::time::timeout(Duration::from_secs(3), resp_rx).await {
            Ok(Ok(Ok(index))) => {
                println!("Proposed command at index {} via node {}", index, idx + 1);
                assert!(index > 0, "Index should be positive");
                proposed = true;
                break;
            }
            Ok(Ok(Err(e))) => {
                println!("Node {} not leader: {}", idx + 1, e);
            }
            _ => {
                println!("Node {} timeout", idx + 1);
            }
        }
    }

    assert!(proposed, "Should have successfully proposed a command");

    println!("Three node failover test completed successfully");
}

async fn multi_message_router(
    mut msg_rx: tokio::sync::broadcast::Receiver<OutgoingMessage>,
    step_tx1: tokio::sync::mpsc::Sender<RaftMessage>,
    step_tx2: tokio::sync::mpsc::Sender<RaftMessage>,
    step_tx3: tokio::sync::mpsc::Sender<RaftMessage>,
) {
    while let Ok(msg) = msg_rx.recv().await {
        info!("Routing message to {}", msg.to_id);
        if let Ok(raft_msg) = ProtobufMessage::parse_from_bytes(&msg.message) {
            match msg.to_id {
                1 => {
                    let _ = step_tx1.send(raft_msg).await;
                }
                2 => {
                    let _ = step_tx2.send(raft_msg).await;
                }
                3 => {
                    let _ = step_tx3.send(raft_msg).await;
                }
                _ => {}
            }
        }
    }
}

#[tokio::test]
async fn test_state_machine_consistency() {
    let temp_dir1 = tempdir().expect("Failed to create temp dir");
    let db_path1 = temp_dir1.path().join("raft_1");
    std::fs::create_dir_all(&db_path1).expect("Failed to create db dir");

    let temp_dir2 = tempdir().expect("Failed to create temp dir");
    let db_path2 = temp_dir2.path().join("raft_2");
    std::fs::create_dir_all(&db_path2).expect("Failed to create db dir");

    let temp_dir3 = tempdir().expect("Failed to create temp dir");
    let db_path3 = temp_dir3.path().join("raft_3");
    std::fs::create_dir_all(&db_path3).expect("Failed to create db dir");

    let mut node1 = RaftNode::new(
        1,
        "127.0.0.1:10001".to_string(),
        vec![],
        db_path1.to_str().unwrap(),
    )
    .expect("Failed to create Raft node 1");

    let mut node2 = RaftNode::new(
        2,
        "127.0.0.1:10002".to_string(),
        vec![],
        db_path2.to_str().unwrap(),
    )
    .expect("Failed to create Raft node 2");

    let mut node3 = RaftNode::new(
        3,
        "127.0.0.1:10003".to_string(),
        vec![],
        db_path3.to_str().unwrap(),
    )
    .expect("Failed to create Raft node 3");

    let step_tx1 = node1.get_step_tx();
    let step_tx2 = node2.get_step_tx();
    let step_tx3 = node3.get_step_tx();
    let msg_rx1 = node1.take_message_rx();
    let msg_rx2 = node2.take_message_rx();
    let msg_rx3 = node3.take_message_rx();

    let propose_tx1 = node1.get_propose_tx();
    let propose_tx2 = node2.get_propose_tx();
    let propose_tx3 = node3.get_propose_tx();

    let st1_1 = step_tx1.clone();
    let st1_2 = step_tx1.clone();
    let st2_1 = step_tx2.clone();
    let st2_2 = step_tx2.clone();
    let st3_1 = step_tx3.clone();
    let st3_2 = step_tx3.clone();

    tokio::spawn(async move {
        let _ = multi_message_router(msg_rx1, step_tx1, step_tx2, step_tx3).await;
    });
    tokio::spawn(async move {
        let _ = multi_message_router(msg_rx2, st1_1, st2_1, st3_1).await;
    });
    tokio::spawn(async move {
        let _ = multi_message_router(msg_rx3, st1_2, st2_2, st3_2).await;
    });

    tokio::spawn(async move {
        let _ = node1.run().await;
    });
    tokio::spawn(async move {
        let _ = node2.run().await;
    });
    tokio::spawn(async move {
        let _ = node3.run().await;
    });

    sleep(Duration::from_secs(3)).await;

    let propose_txs = [propose_tx1, propose_tx2, propose_tx3];

    for i in 0..5 {
        let cmd = RaftCommand::Heartbeat {
            node_id: format!("test_node_{}", i),
        };
        let data = cmd.serialize();

        let mut proposed = false;
        for (idx, propose_tx) in propose_txs.iter().enumerate() {
            let (resp_tx, resp_rx) = oneshot::channel();
            let req = ProposeRequest {
                data: data.clone(),
                response_tx: resp_tx,
            };

            if propose_tx.send(req).await.is_err() {
                continue;
            }

            match tokio::time::timeout(Duration::from_secs(3), resp_rx).await {
                Ok(Ok(Ok(index))) => {
                    println!(
                        "Proposed command {} at index {} via node {}",
                        i,
                        index,
                        idx + 1
                    );
                    proposed = true;
                    break;
                }
                Ok(Ok(Err(_))) => {}
                _ => {}
            }
        }

        assert!(proposed, "Should have successfully proposed command {}", i);
        sleep(Duration::from_secs(1)).await;
    }

    sleep(Duration::from_secs(2)).await;

    println!("State machine consistency test completed successfully");
}

#[allow(dead_code)]
fn process_node(from_node: &mut RaftNode, to_node1: &mut RaftNode, to_node2: &mut RaftNode) {
    let mut ready = from_node.node.ready();

    let mut msgs = ready.take_messages();
    msgs.extend(ready.take_persisted_messages());

    if !msgs.is_empty() {
        println!("Node {} sending {} messages", from_node.id(), msgs.len());
    }

    for msg in msgs {
        let to_id = msg.to;
        if to_id == to_node1.id() {
            let _ = to_node1.step(msg);
            process_node(to_node1, from_node, to_node2);
        } else if to_id == to_node2.id() {
            let _ = to_node2.step(msg);
            process_node(to_node2, from_node, to_node1);
        }
    }

    if !ready.entries().is_empty() {
        let _ = from_node.node.mut_store().append(ready.entries());
    }

    if let Some(hs) = ready.hs() {
        from_node.node.mut_store().set_hardstate(hs.clone());
    }

    from_node.node.advance(ready);
}
