//! openraft v2 适配层：用 `Raft<MasterTypeConfig, RocksStateMachine>` 替换旧 `RaftNode`。
//!
//! 本模块提供 `RaftNodeV2`，封装 openraft 的 `Raft` 句柄，对外暴露与旧 `RaftNode`
//! 相似的 API（`propose` / `add_learner` / `change_membership` / `transfer_leader`
//! / `is_leader` / `current_leader` / `get_cluster_info` 等），供 `MasterNode` 使用。
//!
//! 与旧 `RaftNode` 的关键区别：
//! - **无 `run()` 事件循环**：openraft 内部自驱动，不需要外部 tick。
//! - **无 `propose_tx` / `step_tx` / `message_tx`**：openraft 通过 `RaftNetworkFactory`
//!   自行管理出站通信，通过 `RaftService` gRPC 服务端处理入站通信。
//! - **apply 通知**：通过 `RocksStateMachine::apply_notifier` channel 发送 applied log index，
//!   `MasterNode` 收到后从 `raft_state_data` CF 读取序列化的 `MasterRequest` 并 replay。
//! - **节点 ID 类型**：`u64` → `String`（`MasterTypeConfig::NodeId = String`），内部自动转换。

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use log::info;
use log::warn;
use openraft::async_runtime::WatchReceiver;
use openraft::type_config::TypeConfigExt;
use openraft::Raft;
use openraft::ServerState;
use powerfs_raft::grpc::RaftServiceImpl;
use powerfs_raft::network::Network;
use powerfs_raft::protobuf::raft_service_server::RaftServiceServer;
use powerfs_raft::store;
use powerfs_raft::store::RocksStateMachine;
use powerfs_raft::BasicNode;
use powerfs_raft::MasterRequest;
use powerfs_raft::MasterTypeConfig;
use rocksdb::DB;
use tokio::sync::mpsc;

// ===== 共享数据类型（原 raft_node.rs / raft_storage.rs，已迁移至此） =====

/// Peer information for cluster communication
#[derive(Debug, Clone)]
pub struct Peer {
    pub id: u64,
    pub address: String,
    /// powerfs-net address (ip:net_port) for TLV transport.
    pub net_address: String,
}

/// Committed entry ready to apply to state machine
#[derive(Debug, Clone)]
pub struct ApplyEntry {
    pub index: u64,
    pub command: RaftCommand,
}

/// Raft commands that can be proposed to the cluster
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum RaftCommand {
    AddNode {
        node_id: String,
        address: String,
        rack: String,
        data_center: String,
        http_port: u32,
        grpc_port: u32,
        public_url: String,
    },
    RemoveNode {
        node_id: String,
    },
    AssignVolume {
        node_id: String,
        volume_id: u64,
        collection: String,
        replica_count: u32,
        ttl: i32,
        disk_type: String,
        size: u64,
    },
    UpdateVolumeState {
        volume_id: u64,
        state: String,
    },
    UpdateNodeVolumes {
        node_id: String,
        volumes: Vec<RaftVolumeShortInfo>,
        ip: String,
        grpc_port: u32,
        net_port: u32,
    },
    Heartbeat {
        node_id: String,
    },
    CreateCollection {
        name: String,
        replication: String,
        ttl: i32,
        disk_type: String,
        max_volume_count: u64,
    },
    DeleteCollection {
        name: String,
    },
    CreateCollectionExt {
        info: crate::collection::CollectionInfo,
    },
    UpdateCollectionExt {
        name: String,
        info: crate::collection::CollectionInfo,
    },
    DeleteCollectionExt {
        name: String,
    },
    DeleteVolume {
        volume_id: u64,
    },
    /// Persist `next_file_key` advance for a volume.
    AdvanceFileKey {
        volume_id: u64,
        new_next_key: u64,
    },
    /// P1.3: Persist Zone registration (zone_id → physical volumes mapping).
    RegisterZone {
        zone: powerfs_common::types::ZoneInfo,
    },
    /// P1.3: Persist Zone update (re-registration with updated physical volumes).
    UpdateZone {
        zone: powerfs_common::types::ZoneInfo,
    },
    /// Set node maintenance mode (excluded from allocation + drained).
    SetNodeMaintenance {
        node_id: String,
        enabled: bool,
    },
    /// Pin a volume to a specific node (ops override).
    PinVolume {
        volume_id: u64,
        node_id: String,
    },
    /// Remove a volume pin.
    UnpinVolume {
        volume_id: u64,
    },
    /// Set the global placement strategy.
    SetPlacementStrategy {
        strategy: String,
    },
}

/// Volume info for Raft serialization (serde-compatible)
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RaftVolumeShortInfo {
    pub volume_id: u64,
    pub size: u64,
    pub read_only: bool,
    pub used: u64,
    pub file_count: u64,
    pub collection: String,
}

impl RaftCommand {
    pub fn serialize(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    pub fn deserialize(data: &[u8]) -> Result<Self, String> {
        serde_json::from_slice(data).map_err(|e| e.to_string())
    }
}

impl From<&crate::proto::VolumeShortInfo> for RaftVolumeShortInfo {
    fn from(v: &crate::proto::VolumeShortInfo) -> Self {
        RaftVolumeShortInfo {
            volume_id: v.volume_id,
            size: v.size,
            read_only: v.read_only,
            used: v.used,
            file_count: v.file_count,
            collection: v.collection.clone(),
        }
    }
}

/// openraft 版本的集群信息。
#[derive(Debug, Clone)]
pub struct ClusterInfoV2 {
    pub node_id: String,
    pub address: String,
    pub is_leader: bool,
    pub term: u64,
    pub peers: Vec<String>,
    pub commit_index: u64,
    pub last_applied: u64,
}

/// openraft v2 封装的 Raft 节点，替换旧 `RaftNode`。
///
/// 持有 `Raft<MasterTypeConfig, RocksStateMachine>` 句柄（内部 `Arc`，可廉价克隆），
/// 以及 RocksDB 句柄（供 `MasterNode` 读取 `raft_state_data` CF 中的 applied entries）。
pub struct RaftNodeV2 {
    raft: Raft<MasterTypeConfig, RocksStateMachine>,
    node_id: String,
    address: String,
    /// RocksDB 句柄，供 `read_applied_entry()` 读取 `raft_state_data` CF。
    db: Arc<DB>,
}

/// 等待所有 peer 的 Raft gRPC 端口可达，确保 bootstrap 时 quorum 可用。
///
/// 多节点集群启动时，bootstrap 节点（id=1）必须等待所有 peer 的 Raft
/// 服务端口 TCP 可达后才能调用 `raft.initialize()`。否则单节点成为 leader
/// 后因 quorum 不足在 election_timeout 后下台，引发 "forward None" 死循环
/// 和 CPU 飙升，干扰 filer 正常处理请求（SLOW_REQ 根因）。
async fn wait_for_peers_ready(peers: &[Peer], timeout_secs: u64) {
    if peers.is_empty() {
        return;
    }
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(timeout_secs);
    info!(
        "RaftNodeV2: waiting for {} peers ready (timeout={}s)",
        peers.len(),
        timeout_secs
    );
    for peer in peers {
        let addr: SocketAddr = match peer.address.parse() {
            Ok(a) => a,
            Err(e) => {
                warn!(
                    "RaftNodeV2: invalid peer {} address '{}': {}; skipping readiness check",
                    peer.id, peer.address, e
                );
                continue;
            }
        };
        loop {
            if tokio::time::Instant::now() >= deadline {
                warn!(
                    "RaftNodeV2: timed out waiting for peer {} at {}; proceeding anyway",
                    peer.id, peer.address
                );
                break;
            }
            match tokio::time::timeout(
                tokio::time::Duration::from_secs(2),
                tokio::net::TcpStream::connect(addr),
            )
            .await
            {
                Ok(Ok(_)) => {
                    info!("RaftNodeV2: peer {} ready at {}", peer.id, peer.address);
                    break;
                }
                _ => {
                    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                }
            }
        }
    }
    info!("RaftNodeV2: all peers ready, proceeding with initialize");
}

impl RaftNodeV2 {
    /// 创建新的 Raft 节点并启动 gRPC 服务。
    ///
    /// - `id`：节点 ID（u64，内部转为 String）。
    /// - `address`：gRPC 监听地址（如 `"127.0.0.1:50051"`）。
    /// - `peers`：对端节点列表（不含 self）。
    /// - `storage_path`：RocksDB 数据目录。
    ///
    /// 返回 `(RaftNodeV2, apply_rx)`，其中 `apply_rx` 接收 applied log index 通知。
    pub async fn new(
        id: u64,
        address: String,
        peers: Vec<Peer>,
        storage_path: &str,
    ) -> Result<(Self, mpsc::Receiver<u64>), String> {
        let node_id = id.to_string();

        // 1) 创建 RocksDB 存储
        let (log_store, sm) = store::new::<MasterTypeConfig, _>(storage_path)
            .await
            .map_err(|e| format!("failed to create storage: {}", e))?;

        // 2) 设置 apply 通知 channel
        let (apply_tx, apply_rx) = mpsc::channel(1000);
        let sm = sm.with_apply_notifier(apply_tx);

        // 3) 获取 DB 句柄（供后续读取 applied entries）
        let db = sm.db().clone();

        // 4) 创建 openraft 配置
        let config = Arc::new(
            powerfs_raft::default_config()
                .validate()
                .map_err(|e| format!("invalid raft config: {}", e))?,
        );

        // 5) 创建 Network 工厂（gRPC 客户端）
        let network = Network::<MasterTypeConfig>::new();

        // 6) 创建 Raft 实例
        let raft = Raft::new(node_id.clone(), config, network, log_store, sm)
            .await
            .map_err(|e| format!("failed to create raft: {}", e))?;

        // 7) 启动 gRPC 服务（RaftService：Vote/AppendEntries/StreamAppend/Snapshot）
        let socket_addr: SocketAddr = address
            .parse()
            .map_err(|e| format!("invalid address '{}': {}", address, e))?;
        let service = RaftServiceImpl::new(raft.clone());
        MasterTypeConfig::spawn(async move {
            info!("RaftService gRPC server listening on {}", socket_addr);
            if let Err(e) = tonic::transport::Server::builder()
                .add_service(RaftServiceServer::new(service))
                .serve(socket_addr)
                .await
            {
                log::error!("RaftService gRPC server error: {}", e);
            }
        });

        // 8) 初始化集群
        // - 单节点模式（无 peers）：立即初始化为单节点集群
        // - 多节点模式：仅 id=1 的节点 bootstrap 整个集群（含所有 peers），
        //   其他节点不调用 initialize，等待 leader 推送初始配置
        //
        // 关键: bootstrap 节点必须等待所有 peer 的 Raft 端口可达后再
        // initialize, 否则单节点 leader 因 quorum 不足下台 → forward None
        // 死循环 → CPU 飙升 → filer 受干扰 → SLOW_REQ (根因修复)
        if id == 1 && !peers.is_empty() {
            wait_for_peers_ready(&peers, 120).await;
        }
        let should_bootstrap = peers.is_empty() || id == 1;
        if should_bootstrap {
            let mut members: BTreeMap<String, BasicNode> = BTreeMap::new();
            members.insert(
                node_id.clone(),
                BasicNode {
                    addr: address.clone(),
                },
            );
            for peer in &peers {
                members.insert(
                    peer.id.to_string(),
                    BasicNode {
                        addr: peer.address.clone(),
                    },
                );
            }
            match raft.initialize(members).await {
                Ok(()) => {
                    info!(
                        "RaftNodeV2: cluster initialized (id={}, members={})",
                        node_id,
                        peers.len() + 1
                    );
                }
                Err(e) => {
                    info!(
                        "RaftNodeV2: initialize returned (likely already initialized): {}",
                        e
                    );

                    // RocksDB 中已有 membership，检查地址是否与当前配置不同。
                    // 如果不同，打印警告，并启动后台任务在成为 leader 后通过
                    // add_learner 更新 RocksDB 中的地址。
                    let metrics_recv = raft.metrics();
                    let membership = metrics_recv
                        .borrow_watched()
                        .membership_config
                        .membership()
                        .clone();
                    drop(metrics_recv);

                    let mut addr_mismatches: Vec<(String, String, String)> = Vec::new();

                    // 检查本节点地址
                    if let Some((_, node)) = membership
                        .nodes()
                        .find(|(k, _)| k.as_str() == node_id.as_str())
                    {
                        if node.addr != address {
                            warn!(
                                "RaftNodeV2: node {} address mismatch: RocksDB='{}', current='{}'. \
                                 Will use new address and update via Raft when leader is elected.",
                                node_id, node.addr, address
                            );
                            addr_mismatches.push((
                                node_id.clone(),
                                node.addr.clone(),
                                address.clone(),
                            ));
                        }
                    }

                    // 检查 peer 地址
                    for peer in &peers {
                        let peer_id = peer.id.to_string();
                        if let Some((_, node)) = membership
                            .nodes()
                            .find(|(k, _)| k.as_str() == peer_id.as_str())
                        {
                            if node.addr != peer.address {
                                warn!(
                                    "RaftNodeV2: peer {} address mismatch: RocksDB='{}', config='{}'. \
                                     Will update to new address.",
                                    peer_id, node.addr, peer.address
                                );
                                addr_mismatches.push((
                                    peer_id,
                                    node.addr.clone(),
                                    peer.address.clone(),
                                ));
                            }
                        }
                    }

                    // 启动后台任务更新地址（等待成为 leader 后执行）
                    if !addr_mismatches.is_empty() {
                        let raft_clone = raft.clone();
                        MasterTypeConfig::spawn(async move {
                            // 最多等待 60 秒（30 次 × 2 秒）
                            for _ in 0..30 {
                                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                                let state = raft_clone.metrics().borrow_watched().state;
                                if state == ServerState::Leader {
                                    for (nid, old_addr, new_addr) in &addr_mismatches {
                                        match raft_clone
                                            .add_learner(
                                                nid.clone(),
                                                BasicNode {
                                                    addr: new_addr.clone(),
                                                },
                                                true,
                                            )
                                            .await
                                        {
                                            Ok(_) => {
                                                info!(
                                                    "RaftNodeV2: updated node {} address: '{}' -> '{}'",
                                                    nid, old_addr, new_addr
                                                );
                                            }
                                            Err(e) => {
                                                warn!(
                                                    "RaftNodeV2: failed to update node {} address: {}",
                                                    nid, e
                                                );
                                            }
                                        }
                                    }
                                    return;
                                }
                            }
                            warn!(
                                "RaftNodeV2: timed out waiting for leader to update addresses: {:?}",
                                addr_mismatches
                            );
                        });
                    }
                }
            }
        }

        info!(
            "Created RaftNodeV2: id={}, address={}, peers={:?}",
            node_id,
            address,
            peers.iter().map(|p| p.id).collect::<Vec<_>>()
        );

        Ok((
            Self {
                raft,
                node_id,
                address,
                db,
            },
            apply_rx,
        ))
    }

    /// 提议一个命令到 Raft 日志（等价于旧 `RaftNode::propose`）。
    ///
    /// `data` 是序列化后的 `RaftCommand`（`serde_json::to_vec`）。
    /// 返回 committed log index。
    pub async fn propose(&self, data: Vec<u8>) -> Result<u64, String> {
        let req = MasterRequest { payload: data };
        let resp = self
            .raft
            .client_write(req)
            .await
            .map_err(|e| format!("client_write failed: {}", e))?;
        // openraft 的 ClientWriteResponse 包含 log_id，取 index。
        let log_id = resp.log_id;
        Ok(log_id.index())
    }

    /// 添加 learner 节点（等价于旧 `RaftNode::add_peer`）。
    pub async fn add_learner(&self, peer_id: u64, addr: String) -> Result<(), String> {
        let node_id = peer_id.to_string();
        let node = BasicNode { addr };
        self.raft
            .add_learner(node_id, node, true)
            .await
            .map_err(|e| format!("add_learner failed: {}", e))?;
        Ok(())
    }

    /// 移除节点（等价于旧 `RaftNode::remove_peer`）。
    ///
    /// openraft 通过 `change_membership` 移除 voter。
    /// 调用方需提供当前所有 voter 的 ID（不含被移除的节点）。
    pub async fn remove_peer(&self, _peer_id: u64) -> Result<(), String> {
        // openraft 不提供直接的 remove_peer API；
        // 需要通过 change_membership 重新设置 voter 集合。
        // MasterNode 应调用 change_membership 而非此方法。
        Err(
            "remove_peer is not directly supported in openraft; use change_membership instead"
                .to_string(),
        )
    }

    /// 变更集群成员（等价于旧 `add_peer` + `remove_peer` 的组合）。
    ///
    /// `members`：voter 的 `(node_id, address)` 列表。
    pub async fn change_membership(&self, members: Vec<(u64, String)>) -> Result<(), String> {
        let voter_ids: Vec<String> = members.iter().map(|(id, _)| id.to_string()).collect();
        self.raft
            .change_membership(voter_ids, false)
            .await
            .map_err(|e| format!("change_membership failed: {}", e))?;
        Ok(())
    }

    /// 转移领导者（等价于旧 `RaftNode::transfer_leader`）。
    pub async fn transfer_leader(&self, target_id: u64) -> Result<(), String> {
        let target = target_id.to_string();
        self.raft
            .trigger()
            .transfer_leader(target)
            .await
            .map_err(|e| format!("transfer_leader failed: {}", e))?;
        Ok(())
    }

    /// 检查当前节点是否为 leader。
    pub fn is_leader(&self) -> bool {
        // 通过 metrics 获取状态（同步快照）。
        self.raft.metrics().borrow_watched().state == ServerState::Leader
    }

    /// 获取当前 leader 的 NodeId（`None` 如果无 leader）。
    pub async fn current_leader(&self) -> Option<String> {
        self.raft.current_leader().await
    }

    /// 获取当前 leader 的 Raft 地址（`host:raft_port`），通过 `current_leader()` +
    /// membership 节点表查询。无 leader 或 leader 不在 membership 中时返回 `None`。
    ///
    /// 用于 follower 在收到客户端请求时构造重定向响应：`leader_address` 字段从未被
    /// `set_leader` 更新过（死代码），follower 必须实时查 raft 获取 leader。
    ///
    /// **防御性**：任何异常（无 leader / leader 不在 membership / 地址为空）都打 warn
    /// 日志，便于排查路由死循环。调用方应检查返回值并返回 SERVER_ERROR 而非空 REDIRECT。
    pub async fn current_leader_addr(&self) -> Option<String> {
        let leader_id = match self.raft.current_leader().await {
            Some(id) => id,
            None => {
                warn!(
                    "current_leader_addr: raft reports no leader (node={}, state={:?}); \
                     follower cannot redirect — caller should return SERVER_ERROR",
                    self.node_id,
                    self.raft.metrics().borrow_watched().state
                );
                return None;
            }
        };

        let metrics_recv = self.raft.metrics();
        let metrics = metrics_recv.borrow_watched();
        let membership = metrics.membership_config.membership();
        let addr = membership
            .nodes()
            .find(|(k, _)| *k.as_str() == leader_id)
            .map(|(_, n)| n.addr.clone());
        // Collect voter ids now (within the guard's borrow scope) for diagnostics.
        let voters: Vec<String> = membership.voter_ids().map(|id| id.to_string()).collect();
        // addr is owned Option<String>; voters is owned Vec<String> — neither borrows metrics.
        drop(metrics);
        drop(metrics_recv);

        match addr {
            Some(a) if !a.is_empty() => Some(a),
            Some(a) => {
                warn!(
                    "current_leader_addr: leader node={} found in membership but address is \
                     empty '{}'; membership may be inconsistent (node={})",
                    leader_id, a, self.node_id
                );
                None
            }
            None => {
                warn!(
                    "current_leader_addr: leader node={} NOT found in membership (voters={:?}); \
                     membership/stale or leader left cluster (node={})",
                    leader_id, voters, self.node_id
                );
                None
            }
        }
    }

    /// 获取本节点 ID。
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// 获取本节点地址。
    pub fn address(&self) -> &str {
        &self.address
    }

    /// 获取集群信息。
    pub async fn get_cluster_info(&self) -> ClusterInfoV2 {
        let metrics = self.raft.metrics().borrow_watched().clone();

        ClusterInfoV2 {
            node_id: self.node_id.clone(),
            address: self.address.clone(),
            is_leader: metrics.state == ServerState::Leader,
            term: metrics.current_term,
            peers: metrics
                .membership_config
                .membership()
                .nodes()
                .map(|(k, _)| k.to_string())
                .collect(),
            commit_index: metrics.local_committed.map(|l| l.index()).unwrap_or(0),
            last_applied: metrics.last_applied.map(|l| l.index()).unwrap_or(0),
        }
    }

    /// 从 `raft_state_data` CF 读取指定 index 的 applied entry payload。
    ///
    /// 返回 `MasterRequest` 的序列化 bytes（即 `serde_json::to_vec(&MasterRequest)` 的结果）。
    /// 调用方可进一步 `serde_json::from_slice::<RaftCommand>(&master_request.payload)` 还原业务命令。
    pub fn read_applied_entry(&self, index: u64) -> Result<Option<Vec<u8>>, String> {
        let cf = self
            .db
            .cf_handle(store::CF_STATE_DATA)
            .ok_or("raft_state_data CF not found")?;

        let bytes = self
            .db
            .get_cf(cf, index.to_be_bytes())
            .map_err(|e| format!("rocksdb get failed: {}", e))?;

        Ok(bytes.map(|b| {
            // 反序列化 MasterRequest，返回其中的 payload（= RaftCommand 序列化 bytes）
            match serde_json::from_slice::<MasterRequest>(&b) {
                Ok(req) => req.payload,
                Err(_) => b, // fallback：直接返回原始 bytes
            }
        }))
    }

    /// 扫描 `raft_state_data` CF，返回所有 `(index, payload_bytes)` 对（按 index 升序）。
    ///
    /// 供启动时 replay 所有已 apply 的 Normal entries（如 P1.3 zone 注册命令）。
    pub fn scan_applied_entries(&self) -> Result<Vec<(u64, Vec<u8>)>, String> {
        let cf = self
            .db
            .cf_handle(store::CF_STATE_DATA)
            .ok_or("raft_state_data CF not found")?;

        let mut result = Vec::new();
        let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::Start);

        for item in iter {
            let (key, value) = item.map_err(|e| format!("rocksdb iterator error: {}", e))?;
            if key.len() == 8 {
                let index = u64::from_be_bytes([
                    key[0], key[1], key[2], key[3], key[4], key[5], key[6], key[7],
                ]);
                // 反序列化 MasterRequest，提取 payload
                let payload = match serde_json::from_slice::<MasterRequest>(&value) {
                    Ok(req) => req.payload,
                    Err(_) => value.to_vec(),
                };
                result.push((index, payload));
            }
        }

        result.sort_by_key(|(idx, _)| *idx);
        Ok(result)
    }

    /// 获取内部 Raft 句柄（供高级用法）。
    pub fn raft(&self) -> &Raft<MasterTypeConfig, RocksStateMachine> {
        &self.raft
    }
}
