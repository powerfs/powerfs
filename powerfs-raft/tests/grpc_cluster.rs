//! gRPC 传输层端到端集成测试。
//!
//! 启动 3 节点集群，验证 Vote / AppendEntries / StreamAppend RPC 正常工作：
//! - 领导者选举（Vote RPC）
//! - 日志复制（AppendEntries + StreamAppend RPC）
//! - 成员变更（AddLearner + ChangeMembership）
//!
//! 参考 `openraft/examples/raft-kv-memstore-grpc/tests/test_cluster.rs`，
//! 适配 PowerFS：使用 `MasterTypeConfig`（NodeId=String）+ `RocksLogStore`/`RocksStateMachine`。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::type_config::TypeConfigExt;
use openraft::ServerState;
use powerfs_raft::grpc::RaftServiceImpl;
use powerfs_raft::network::Network;
use powerfs_raft::protobuf::raft_service_server::RaftServiceServer;
use powerfs_raft::store;
use powerfs_raft::store::RocksStateMachine;
use powerfs_raft::BasicNode;
use powerfs_raft::MasterRequest;
use powerfs_raft::MasterTypeConfig;
use powerfs_raft::Raft;
use tempfile::TempDir;
use tonic::transport::Server;

type NodeId = <MasterTypeConfig as openraft::RaftTypeConfig>::NodeId;
type RaftNode = Raft<MasterTypeConfig, RocksStateMachine>;

/// 节点 n 的 gRPC 监听地址。
fn addr(n: u32) -> String {
    format!("127.0.0.1:{}", 22000 + n)
}

/// 节点 n 的 NodeId。
fn node_id(n: u32) -> String {
    format!("n{}", n)
}

/// 启动一个 Raft 节点 + gRPC 服务。
///
/// 返回 `(TempDir, Raft)` —— 调用方必须保留 `TempDir` 直到测试结束，
/// 否则 RocksDB 会被清理。
async fn start_node(n: u32) -> std::io::Result<(TempDir, RaftNode)> {
    let td = TempDir::new()?;

    let config = Arc::new(
        powerfs_raft::default_config()
            .validate()
            .map_err(|e| std::io::Error::other(e.to_string()))?,
    );

    let (log_store, sm) = store::new::<MasterTypeConfig, _>(td.path()).await?;
    let network = Network::<MasterTypeConfig>::new();

    let raft: RaftNode = Raft::new(node_id(n), config, network, log_store, sm)
        .await
        .map_err(|e| std::io::Error::other(format!("Raft::new failed: {e}")))?;

    // 启动 gRPC 服务（RaftService）作为后台任务。
    let socket_addr = addr(n).parse().map_err(std::io::Error::other)?;
    let service = RaftServiceImpl::new(raft.clone());
    MasterTypeConfig::spawn(async move {
        let _ = Server::builder()
            .add_service(RaftServiceServer::new(service))
            .serve(socket_addr)
            .await;
    });

    Ok((td, raft))
}

/// 等待节点的 `state` 到达目标值，超时 10 秒。
async fn wait_for_state(raft: &RaftNode, target: ServerState, msg: &str) {
    raft.wait(Some(Duration::from_secs(10)))
        .metrics(
            |m| m.state == target,
            format!("wait for {msg}: state={target:?}"),
        )
        .await
        .unwrap_or_else(|e| panic!("wait_for_state failed: {msg}: {e:?}"));
}

/// 等待节点的状态机已 apply 至少一个 entry（即初始 membership 已提交）。
///
/// `initialize` 返回后 leader 可能尚未提交初始 membership entry，
/// 立即调 `add_learner` 会触发 `InProgress` 错误。等待 `last_applied.is_some()`
/// 确保初始 membership 已 commit + apply。
async fn wait_for_membership_committed(raft: &RaftNode, msg: &str) {
    raft.wait(Some(Duration::from_secs(10)))
        .metrics(
            |m| m.last_applied.is_some(),
            format!("wait for {msg}: membership committed"),
        )
        .await
        .unwrap_or_else(|e| panic!("wait_for_membership_committed failed: {msg}: {e:?}"));
}

/// 等待节点的 `last_log_index` 达到 `>= target`，超时 10 秒。
async fn wait_for_log_index(raft: &RaftNode, target: u64, msg: &str) {
    raft.wait(Some(Duration::from_secs(10)))
        .metrics(
            |m| m.last_log_index.unwrap_or(0) >= target,
            format!("wait for {msg}: last_log_index>={target}"),
        )
        .await
        .unwrap_or_else(|e| panic!("wait_for_log_index failed: {msg}: {e:?}"));
}

/// 等待节点的 `current_leader` 不为 `None`。
async fn wait_for_leader(raft: &RaftNode, msg: &str) -> NodeId {
    let m = raft
        .wait(Some(Duration::from_secs(10)))
        .metrics(
            |m| m.current_leader.is_some(),
            format!("wait for leader: {msg}"),
        )
        .await
        .unwrap_or_else(|e| panic!("wait_for_leader failed: {msg}: {e:?}"));
    m.current_leader.unwrap()
}

#[test]
fn test_grpc_cluster_e2e() {
    // 初始化 tracing（可选；通过 RUST_LOG=debug 启用）。
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    MasterTypeConfig::run(async {
        // --- 启动 3 节点 ---
        let (_td1, raft1) = start_node(1).await.expect("start node 1");
        let (_td2, raft2) = start_node(2).await.expect("start node 2");
        let (_td3, raft3) = start_node(3).await.expect("start node 3");

        // 等待 gRPC 服务监听就绪。
        MasterTypeConfig::sleep(Duration::from_millis(300)).await;

        // --- 初始化单节点集群 ---
        println!("=== init single node cluster on n1");
        let mut members: BTreeMap<NodeId, BasicNode> = BTreeMap::new();
        members.insert(node_id(1), BasicNode { addr: addr(1) });
        raft1.initialize(members).await.expect("initialize failed");

        // --- 等待 n1 当选 leader（验证 Vote RPC） ---
        wait_for_state(&raft1, ServerState::Leader, "n1 leader").await;
        // 等待初始 membership 提交，避免 add_learner 撞上 InProgress。
        wait_for_membership_committed(&raft1, "n1 init membership").await;
        println!("=== n1 elected leader");

        // --- 添加 n2 / n3 为 learner（验证 AddLearner + 心跳 AppendEntries） ---
        println!("=== add learner n2");
        raft1
            .add_learner(node_id(2), BasicNode { addr: addr(2) }, true)
            .await
            .expect("add_learner n2");
        println!("=== add learner n3");
        raft1
            .add_learner(node_id(3), BasicNode { addr: addr(3) }, true)
            .await
            .expect("add_learner n3");

        // --- 切换成员到 {n1, n2, n3}（验证成员变更 AppendEntries） ---
        // `change_membership` 接收 `impl Into<ChangeMembers>`，即 NodeId 迭代器；
        // 节点信息（BasicNode.addr）已通过 add_learner 注册到集群。
        println!("=== change membership to {{n1, n2, n3}}");
        let new_members: Vec<NodeId> = vec![node_id(1), node_id(2), node_id(3)];
        raft1
            .change_membership(new_members, false)
            .await
            .expect("change_membership");

        // --- 写入若干 entries（验证 StreamAppend 日志复制） ---
        let n_writes = 10u64;
        println!("=== write {n_writes} entries via leader n1");
        for i in 0..n_writes {
            let req = MasterRequest {
                payload: format!("entry-{i}").into_bytes(),
            };
            raft1.client_write(req).await.expect("client_write");
        }

        // --- 验证 n2 / n3 复制了 entries ---
        // 初始 2 个 entry（init membership + add_learner 隐含的 membership 变更）
        // + 1 个 change_membership entry + 10 个 Normal entries ≈ 13+。
        // 直接等 last_log_index >= n_writes（更宽松）。
        wait_for_log_index(&raft2, n_writes, "n2 replication").await;
        wait_for_log_index(&raft3, n_writes, "n3 replication").await;
        println!("=== replication verified on n2 and n3");

        // --- 验证 n2 / n3 已观察到 leader ---
        let leader2 = wait_for_leader(&raft2, "n2 sees leader").await;
        let leader3 = wait_for_leader(&raft3, "n3 sees leader").await;
        assert_eq!(leader2, leader3, "n2 and n3 must agree on leader");
        assert_eq!(leader2, node_id(1), "leader must be n1");
        println!("=== leader consistency verified: {leader2}");

        println!("=== test passed");
    });
}
