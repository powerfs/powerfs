use tempfile::TempDir;

#[tokio::test]
async fn test_raft_grpc_basic() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("raft_e2e");
    std::fs::create_dir_all(&db_path).unwrap();

    let node = powerfs_master::raft_node::RaftNode::new(
        1,
        "127.0.0.1:9335".to_string(),
        vec![],
        db_path.to_str().unwrap(),
    )
    .unwrap();

    assert_eq!(node.id(), 1);
    assert_eq!(node.address(), "127.0.0.1:9335");
}

#[tokio::test]
async fn test_cluster_info_endpoint() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("cluster_info");
    std::fs::create_dir_all(&db_path).unwrap();

    let node = powerfs_master::raft_node::RaftNode::new(
        1,
        "127.0.0.1:9335".to_string(),
        vec![],
        db_path.to_str().unwrap(),
    )
    .unwrap();

    let info = node.get_cluster_info();
    assert_eq!(info.node_id, 1);
    assert_eq!(info.address, "127.0.0.1:9335");
    assert_eq!(info.term, 1);
    assert!(info.peers.is_empty());
}

#[tokio::test]
async fn test_raft_client_basic() {
    let client = powerfs_master::raft_client::RaftGrpcClient::new(3, 100);

    let result = client.get_cluster_info("127.0.0.1:9335").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_config_parsing() {
    let config_content = r#"
[master]
id = "1"
http_address = "0.0.0.0:9333"
grpc_address = "0.0.0.0:9334"
data_dir = "./data"
log_level = "info"

[volume]
id = "1"
http_address = "0.0.0.0:8080"
grpc_address = "0.0.0.0:8081"
data_dir = "./data"
volume_size = 1073741824
max_file_count = 1000000

[raft]
address = "0.0.0.0:9335"
election_tick = 10
heartbeat_tick = 3
"#;

    let config = powerfs_common::config::Config::from_string(config_content).unwrap();

    assert!(config.master.is_some());
    assert!(config.volume.is_some());
    assert!(config.raft.is_some());

    let master = config.master.unwrap();
    assert_eq!(master.id, "1");
    assert_eq!(master.http_address, "0.0.0.0:9333");
    assert_eq!(master.grpc_address, "0.0.0.0:9334");
    assert_eq!(master.data_dir, "./data");
    assert_eq!(master.log_level, "info");

    let volume = config.volume.unwrap();
    assert_eq!(volume.volume_size, 1073741824);
    assert_eq!(volume.max_file_count, 1000000);

    let raft = config.raft.unwrap();
    assert_eq!(raft.election_tick, 10);
    assert_eq!(raft.heartbeat_tick, 3);
}

#[tokio::test]
async fn test_config_with_peers() {
    let config_content = r#"
[raft]
address = "0.0.0.0:9335"
election_tick = 10
heartbeat_tick = 3

[[raft.peers]]
id = 2
address = "127.0.0.1:9336"

[[raft.peers]]
id = 3
address = "127.0.0.1:9337"
"#;

    let config = powerfs_common::config::Config::from_string(config_content).unwrap();

    let raft = config.raft.unwrap();
    assert!(raft.peers.is_some());

    let peers = raft.peers.unwrap();
    assert_eq!(peers.len(), 2);
    assert_eq!(peers[0].id, 2);
    assert_eq!(peers[0].address, "127.0.0.1:9336");
    assert_eq!(peers[1].id, 3);
    assert_eq!(peers[1].address, "127.0.0.1:9337");
}

#[tokio::test]
async fn test_raft_node_lifecycle() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("lifecycle");
    std::fs::create_dir_all(&db_path).unwrap();

    let node = powerfs_master::raft_node::RaftNode::new(
        1,
        "127.0.0.1:9335".to_string(),
        vec![],
        db_path.to_str().unwrap(),
    )
    .unwrap();

    assert_eq!(node.term(), 1);
    assert_eq!(node.id(), 1);
}
