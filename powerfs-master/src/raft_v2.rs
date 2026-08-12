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

        // 8) 单节点模式：立即初始化集群
        if peers.is_empty() {
            let mut members: BTreeMap<String, BasicNode> = BTreeMap::new();
            members.insert(
                node_id.clone(),
                BasicNode {
                    addr: address.clone(),
                },
            );
            raft.initialize(members)
                .await
                .map_err(|e| format!("initialize failed: {}", e))?;
            info!(
                "RaftNodeV2: single-node cluster initialized (id={})",
                node_id
            );
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
