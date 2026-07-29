use std::sync::atomic::AtomicBool;
use std::sync::{Arc, RwLock};
use tempfile::TempDir;

#[tokio::test]
async fn test_raft_grpc_basic() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("raft_e2e");
    std::fs::create_dir_all(&db_path).unwrap();

    let leader_state = Arc::new(AtomicBool::new(true));
    let leader_address = Arc::new(RwLock::new(String::new()));
    let node = powerfs_master::raft_node::RaftNode::new(
        1,
        "127.0.0.1:9335".to_string(),
        vec![],
        db_path.to_str().unwrap(),
        leader_state,
        leader_address,
    )
    .unwrap();

    assert_eq!(node.id(), 1);
    assert_eq!(node.address(), "127.0.0.1:9335");
}

#[tokio::test]
async fn test_raft_grpc_with_peer() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("cluster_info");
    std::fs::create_dir_all(&db_path).unwrap();

    let leader_state = Arc::new(AtomicBool::new(true));
    let leader_address = Arc::new(RwLock::new(String::new()));
    let node = powerfs_master::raft_node::RaftNode::new(
        1,
        "127.0.0.1:9335".to_string(),
        vec![],
        db_path.to_str().unwrap(),
        leader_state,
        leader_address,
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

    // Use a port that is guaranteed to be free (port 1 is a reserved system
    // port with no real service), so this test does not depend on the host
    // environment. Previously 9335 was used, but that collides with the
    // docker-compose master-3 port mapping.
    let result = client.get_cluster_info("127.0.0.1:1").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_config_parsing() {
    let config_content = r#"
[global]
log_level = "info"
redis_url = "redis://127.0.0.1:6379"

[master]
port = 9333
net_port = 9334
dir = "./data"
ip = "0.0.0.0"
advertise_addr = "127.0.0.1:9333"
raft_id = 1
peers = ["127.0.0.1:9333"]

[volume]
grpc_port = 8080
http_port = 8081
net_port = 8901
data_dir = "./data"
master_addresses = ["127.0.0.1:9333"]
node_id = "volume-1"
max_volume_size = 1073741824

[filer]
port = 8888
grpc_port = 8889
net_port = 9335
master_addresses = ["127.0.0.1:9333"]
data_dir = "./data"
shard_count = 1
raft_id = 1
raft_peers = ["127.0.0.1:8889"]

[s3]
port = 9000
master_address = "127.0.0.1:9333"
dir = "./data"
access_key = "test_key"
secret_key = "test_secret"

[fuse]
mount_point = "/mnt/powerfs"
master_addresses = ["127.0.0.1"]
filer_addresses = ["127.0.0.1"]
volume_addresses = ["127.0.0.1"]
master_net_port = 9334
volume_net_port = 8901
filer_net_port = 9335
collection = "default"
replication = "000"
threads = 4
verbose = false
container = false

[monitor]
addr = "0.0.0.0:8081"
redis_url = "redis://127.0.0.1:6379"
s3_endpoint = "http://127.0.0.1:9000"
s3_backend_endpoint = "http://127.0.0.1:9000"
master_endpoint = "127.0.0.1:9333"
"#;

    let config = powerfs_common::config::PowerFsConfig::load_from_string(config_content).unwrap();

    assert_eq!(config.master.port, 9333);
    assert_eq!(config.master.dir, "./data");
    assert_eq!(config.master.raft_id, 1);

    assert_eq!(config.volume.grpc_port, 8080);
    assert_eq!(config.volume.http_port, 8081);
    assert_eq!(config.volume.data_dir, "./data");
    assert_eq!(config.volume.node_id, "volume-1");
    assert_eq!(config.volume.max_volume_size, 1073741824);
}

#[tokio::test]
async fn test_config_with_peers() {
    let config_content = r#"
[global]
log_level = "info"
redis_url = "redis://127.0.0.1:6379"

[master]
port = 9335
net_port = 9336
dir = "./data"
ip = "0.0.0.0"
advertise_addr = "127.0.0.1:9335"
raft_id = 1
peers = ["127.0.0.1:9336", "127.0.0.1:9337"]

[volume]
grpc_port = 8080
http_port = 8081
net_port = 8901
data_dir = "./data"
master_addresses = ["127.0.0.1:9335"]
node_id = "volume-1"
max_volume_size = 1073741824

[filer]
port = 8888
grpc_port = 8889
net_port = 9337
master_addresses = ["127.0.0.1:9335"]
data_dir = "./data"
shard_count = 1
raft_id = 1
raft_peers = ["127.0.0.1:8889"]

[s3]
port = 9000
master_address = "127.0.0.1:9335"
dir = "./data"
access_key = "test_key"
secret_key = "test_secret"

[fuse]
mount_point = "/mnt/powerfs"
master_addresses = ["127.0.0.1"]
filer_addresses = ["127.0.0.1"]
volume_addresses = ["127.0.0.1"]
master_net_port = 9336
volume_net_port = 8901
filer_net_port = 9337
collection = "default"
replication = "000"
threads = 4
verbose = false
container = false

[monitor]
addr = "0.0.0.0:8081"
redis_url = "redis://127.0.0.1:6379"
s3_endpoint = "http://127.0.0.1:9000"
s3_backend_endpoint = "http://127.0.0.1:9000"
master_endpoint = "127.0.0.1:9335"
"#;

    let config = powerfs_common::config::PowerFsConfig::load_from_string(config_content).unwrap();

    assert_eq!(config.master.peers.len(), 2);
    assert_eq!(config.master.peers[0], "127.0.0.1:9336");
    assert_eq!(config.master.peers[1], "127.0.0.1:9337");
}

#[tokio::test]
async fn test_raft_node_lifecycle() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("lifecycle");
    std::fs::create_dir_all(&db_path).unwrap();

    let leader_state = Arc::new(AtomicBool::new(true));
    let leader_address = Arc::new(RwLock::new(String::new()));
    let node = powerfs_master::raft_node::RaftNode::new(
        1,
        "127.0.0.1:9335".to_string(),
        vec![],
        db_path.to_str().unwrap(),
        leader_state,
        leader_address,
    )
    .unwrap();

    assert_eq!(node.term(), 1);
    assert_eq!(node.id(), 1);
}
