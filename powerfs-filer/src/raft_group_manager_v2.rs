//! openraft v2 多组管理器（Filer 多分片）。
//!
//! 替代旧 `RaftGroupManager`（基于 raft-rs `RawNode` + `Ready` 循环）。
//!
//! 核心变化：
//! - **无 `process_ready` 循环**：openraft 自驱动，无需外部 tick。
//! - **无 TLV 消息转发**：openraft 通过 `RaftNetwork` (gRPC) 直接通信。
//! - **共享 gRPC 端口**：一个 `MultiRaftServiceImpl` 服务所有 shard，按 `group_id` 路由。
//! - **共享 gRPC 连接**：`MultiGroupRouter` 缓存对端 Channel，所有 shard 共用。
//! - **apply 通知**：`RocksStateMachine::apply` 存储 payload 后通过 channel 通知 index，
//!   `meta_shard_manager` 的 apply 循环读取 payload 并应用到 `ShardStore`。
//!
//! 每个 shard 拥有独立的 RocksDB 存储（`{storage_path}/shard_{id}`）和独立的 `Raft` 实例，
//! 但共享 gRPC 服务端和客户端连接池。

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use log::{debug, info, warn};
use openraft::alias::LogIdOf;
use openraft::async_runtime::WatchReceiver;
use openraft::Raft;
use openraft::ReadPolicy;
use openraft::ServerState;
use powerfs_raft::multi::MultiRaftRouter;
use powerfs_raft::multi::MultiRaftServiceImpl;
use powerfs_raft::multi_network::MultiGroupRouter;
use powerfs_raft::multi_network::MultiNetworkFactory;
use powerfs_raft::protobuf::raft_service_server::RaftServiceServer;
use powerfs_raft::store;
use powerfs_raft::store::RocksStateMachine;
use powerfs_raft::BasicNode;
use powerfs_raft::FilerRequest;
use powerfs_raft::FilerTypeConfig;
use rocksdb::DB;
use tokio::sync::mpsc;
use tokio::sync::RwLock;

// ===== 共享数据类型（原 raft_group_manager.rs，已迁移至此） =====

#[derive(Debug, Clone)]
pub struct Peer {
    pub id: u64,
    /// gRPC address for Raft communication (e.g., "172.21.0.31:8889")
    pub address: String,
    /// powerfs-net address for client connections (e.g., "172.21.0.31:8890")
    pub net_address: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub struct ShardId(pub u64);

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ShardCommand {
    CreateFile {
        parent_inode: u64,
        name: String,
        inode: u64,
    },
    UpdateFile {
        inode: u64,
        size: u64,
        mtime: u64,
    },
    DeleteFile {
        parent_inode: u64,
        name: String,
    },
    CreateDirectory {
        parent_inode: u64,
        name: String,
        inode: u64,
    },
    DeleteDirectory {
        parent_inode: u64,
        name: String,
    },
    Rename {
        old_parent_inode: u64,
        old_name: String,
        new_parent_inode: u64,
        new_name: String,
    },
    /// Create an S3 object file inode with data-location metadata in one step.
    PutObject {
        parent_inode: u64,
        name: String,
        inode: u64,
        size: u64,
        fid: String,
        volume_id: u64,
        etag: String,
    },
    /// Set inode attributes (size, mode, uid, gid, mtime, atime) - legacy unified command
    SetAttr {
        inode: u64,
        size: Option<u64>,
        mode: Option<u64>,
        uid: Option<u64>,
        gid: Option<u64>,
        mtime: Option<u64>,
        atime: Option<u64>,
    },
    /// Set data-related inode attributes (size, chunks) - strong consistency via Lease
    SetAttrData {
        inode: u64,
        size: Option<u64>,
    },
    /// Set metadata-related inode attributes (mode, uid, gid, timestamps) - eventual consistency via CRDT
    SetAttrMeta {
        inode: u64,
        mode: Option<u64>,
        uid: Option<u64>,
        gid: Option<u64>,
        mtime: Option<u64>,
        atime: Option<u64>,
        client_id: String,
        timestamp: u64,
    },
    /// Create a symbolic link
    CreateSymlink {
        parent_inode: u64,
        name: String,
        inode: u64,
        target: String,
    },
    /// Create a hard link
    CreateHardLink {
        inode: u64,
        new_parent_inode: u64,
        new_name: String,
    },
    /// Set chunk/fid info for an existing inode (for data location persistence)
    SetChunks {
        inode: u64,
        fid: String,
        volume_id: u64,
        cookie: u32,
        offset: u64,
        size: u64,
    },
    /// Atomically update inode size + full chunks list via Raft (strong
    /// consistency). Used by close/sync_size_chunks_on_close to persist
    /// content_size and chunks. MUST go through Raft so all filer nodes
    /// replicate the update; otherwise followers serve stale size/chunks
    /// to subsequent getattr/read, causing cross-client data corruption
    /// (e.g., IO500 ior-easy-read EOF after first chunk).
    UpdateInodeSizeChunks {
        inode: u64,
        size: u64,
        chunks: Vec<crate::shard_store::StoredFileChunk>,
        #[serde(default)]
        inline_data: Option<Vec<u8>>,
        #[serde(default)]
        is_append: bool,
    },
    /// P3: Set an extended attribute on an inode (persisted via Raft).
    /// Used for `powerfs.placement` xattr on directories.
    SetXattr {
        inode: u64,
        key: String,
        value: Vec<u8>,
    },
    /// Remove an extended attribute from an inode (persisted via Raft).
    RemoveXattr {
        inode: u64,
        key: String,
    },
    /// P4: Update reliability state after scrubber completes replication.
    /// Sets reliability = Replicated{count}, state = Replicated, and stores
    /// replica_chunks (the secondary copy locations).
    UpdateReliability {
        inode: u64,
        reliability: powerfs_layout::reliability::Reliability,
        reliability_state: powerfs_layout::reliability::ReliabilityState,
        replica_chunks: Vec<crate::shard_store::StoredFileChunk>,
    },
    /// P6: EC 转换 — 替换 chunks 为 data+parity shards, 更新可靠性状态
    UpdateToEC {
        inode: u64,
        reliability: powerfs_layout::reliability::Reliability,
        reliability_state: powerfs_layout::reliability::ReliabilityState,
        ec_chunks: Vec<crate::shard_store::StoredFileChunk>,
    },

    // ----- Decomposed inode + dir-entry commands (P? inode-sharding fix) -----
    //
    // The legacy commands above (CreateFile, CreateDirectory, PutObject,
    // CreateSymlink, CreateHardLink, DeleteFile, DeleteDirectory, Rename)
    // store the inode record (`CF_INODES`) and the parent's dir entry
    // (`CF_DIR_ENTRIES`) atomically on the *parent's* shard. That forces
    // inode-level ops (getattr/setattr/update_size_chunks/...) to scan every
    // shard because `calculate_shard(inode)` != `calculate_shard(parent_inode)`
    // for most files. The decomposed commands below split the two writes so
    // each lands on its correct hash-derived shard; inode-level ops then
    // become O(1) `calculate_shard(inode)` lookups. The legacy variants are
    // kept so old Raft log entries still apply on restart.
    //
    // Failure model: writes are no longer atomic across the two shards. If
    // `CreateInode` succeeds but `AddDirEntry` fails (or vice versa on
    // delete), a tombstone-style orphan is left; the GC scan
    // (`collect_orphan_inodes`) reclaims it.
    /// Write an inode record to `CF_INODES` on `calculate_shard(info.inode)`.
    /// Idempotent: re-apply overwrites with same content. Pairs with
    /// `AddDirEntry` to form a complete create.
    CreateInode {
        info: Box<crate::shard_store::InodeInfo>,
    },
    /// Write a dir entry `parent_inode:name -> inode` to `CF_DIR_ENTRIES`
    /// on `calculate_shard(parent_inode)`. Pairs with `CreateInode`.
    AddDirEntry {
        parent_inode: u64,
        name: String,
        inode: u64,
    },
    /// Remove the inode record from `CF_INODES` on `calculate_shard(inode)`.
    /// Pairs with `RemoveDirEntry`. Order on delete: `RemoveDirEntry` first
    /// (so subsequent lookups fail fast), then `DeleteInode`.
    DeleteInode {
        inode: u64,
    },
    /// Remove a dir entry from `CF_DIR_ENTRIES` on
    /// `calculate_shard(parent_inode)`.
    RemoveDirEntry {
        parent_inode: u64,
        name: String,
    },
    /// Bump `nlink` on an existing inode (hard link creation). Routed via
    /// `calculate_shard(inode)`. Pairs with `AddDirEntry` on the new parent.
    IncrementNlink {
        inode: u64,
    },
    /// Decrement `nlink` on an inode (hard link removal). When `nlink`
    /// reaches 0 the inode should also be removed via `DeleteInode`.
    /// Routed via `calculate_shard(inode)`. Pairs with `RemoveDirEntry`.
    DecrementNlink {
        inode: u64,
    },
    /// Update an inode record's `name` and `parent_inode` fields in-place.
    /// Used by cross-shard rename to update the inode record on its own shard
    /// after the dir_entry has been moved to a different shard.
    /// Routed via `calculate_shard(inode)`.
    RenameInode {
        inode: u64,
        new_name: String,
        new_parent_inode: u64,
    },

    // ----- Phase 5 §5.3: Lease state persistence via Raft -----
    //
    // The lease manager (`InodeLeaseManager`) keeps lease state in memory.
    // Without persistence, a leader switch loses every active lease, forcing
    // clients to retry acquire on the new leader. Worse, during the grace
    // period that protects against double-writes, the new leader rejects
    // re-acquires from a different holder, breaking forward progress until
    // the grace expires (~5s). Persisting lease entries through Raft means
    // the new leader observes the same lease state on takeover and continues
    // honoring valid leases — no client-visible disruption, no grace-period
    // stall, no risk of double-write (the lease is authoritative across
    // leader switches, see docs/lock-optimization-plan.md §5.3 and the
    // "Lessons Learned" entry in project_memory).
    //
    // The persistence backend (`RaftLeasePersistence`) is byte-keyed (token
    // → serialized LeaseEntry), matching the `LeasePersistence` trait in
    // powerfs-lease/src/persistence.rs. The three variants below are the
    // Raft-side surface; apply handlers write/delete in `CF_LEASES`.
    //
    // Routing: a lease token's first 8 hex chars are derived from the
    // inode (see `RaftLeasePersistence::token_shard_id`), so all
    // mutations for a given inode land on the same shard — followers of
    // that shard observe the same lease state and can serve acquire/release
    // queries consistently (the existing in-memory store on followers is
    // still authoritative for the local replica; the persisted copy is
    // the source of truth on leader takeover).
    /// Persist (or overwrite) a lease entry. `value` is the serialized
    /// `LeaseEntry<InodeKey>` (see powerfs-lease/src/persistence.rs).
    /// Idempotent: re-apply overwrites with the same content.
    LeasePut {
        token: String,
        value: Vec<u8>,
    },

    /// Delete a persisted lease entry by token. Idempotent: deleting an
    /// absent key is a no-op.
    LeaseDelete {
        token: String,
    },

    /// Persist the lease epoch counter (Fencer epoch, powerfs-lock-health).
    /// Stored under a reserved key (`b"\x00epoch"`) in `CF_LEASES` so
    /// `load_epoch` can locate it without scanning all entries.
    LeaseSaveEpoch {
        epoch: u64,
    },

    // ----- P4 cross-shard DirStatSummary on parent shard -----
    //
    // Persist (or overwrite) a lightweight DirStatSummary for a single
    // directory child on the *parent's* shard. The update is always
    // client-routed directly to `calculate_shard(parent_inode)` — there
    // is NEVER a filer → filer forward (see
    // docs/shard-routing-no-forward-principle.md §3). The
    // `summary.version_ts` guard drops out-of-order old updates.
    //
    // Typical producers:
    //   * MkdirPhaseB on parent_shard — first write of the child summary
    //     after AddDirEntry succeeds.
    //   * SetAttr handlers on the child shard — they re-issue a second
    //     client-routed UpdateChildSummary to the parent shard leader.
    //   * Unlink/rmdir/Rename on parent_shard — issue DeleteChildSummary
    //     to keep the cache consistent with the removed dir entry.
    UpdateChildSummary {
        parent_inode: u64,
        name: String,
        summary: crate::shard_store::DirStatSummary,
    },
    /// Drop a DirStatSummary from the parent shard when the child entry
    /// is removed or renamed-away. Idempotent: deleting a missing key is a
    /// no-op. Routed via `calculate_shard(parent_inode)`.
    DeleteChildSummary {
        parent_inode: u64,
        name: String,
    },
}

impl ShardCommand {
    pub fn serialize(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    pub fn deserialize(data: &[u8]) -> Result<Self, String> {
        serde_json::from_slice(data).map_err(|e| e.to_string())
    }
}

/// 单个 shard 的 Raft 组状态。
struct ShardGroup {
    raft: Raft<FilerTypeConfig, RocksStateMachine>,
    db: Arc<DB>,
    group_id: String,
    peers: Vec<Peer>,
}

/// openraft v2 多组管理器，替代旧 `RaftGroupManager`。
pub struct RaftGroupManagerV2 {
    /// 服务端路由：`group_id -> Raft` 映射（入站 RPC 路由）。
    router: Arc<MultiRaftRouter<FilerTypeConfig>>,
    /// 客户端路由：共享 gRPC 连接池 + 节点地址表（出站 RPC 路由）。
    network_router: MultiGroupRouter,
    /// 每 shard 的 Raft 句柄 + RocksDB 句柄。
    groups: RwLock<HashMap<ShardId, Arc<ShardGroup>>>,
    /// 本节点 ID。
    node_id: u64,
    /// 本节点 gRPC 地址（Raft 通信）。
    node_address: String,
    /// Raft 数据根目录（每 shard 子目录为 `shard_{id}`）。
    storage_path: String,
    /// 对端注册表（id -> Peer）。
    peers: RwLock<HashMap<u64, Peer>>,
}

impl RaftGroupManagerV2 {
    /// 创建多组管理器（不启动 gRPC server — 由调用者统一启动）。
    ///
    /// - `node_id`：本节点 ID（u64，内部转 String 用于 openraft NodeId）。
    /// - `node_address`：gRPC 监听地址（如 `"172.30.0.31:8889"`），用于
    ///   openraft 集群成员发现，不在此方法内 bind。
    /// - `storage_path`：Raft 数据根目录。
    ///
    /// gRPC server 的启动由 main.rs 统一负责，将 RaftService 和
    /// FilerMetaService 合并到同一个 tonic Server 上，避免端口冲突。
    pub async fn new(
        node_id: u64,
        node_address: String,
        storage_path: String,
    ) -> Result<Arc<Self>, String> {
        let router = Arc::new(MultiRaftRouter::<FilerTypeConfig>::new());
        let network_router = MultiGroupRouter::new();

        // 注册自身地址到 network_router（用于出站连接路由）
        network_router
            .register_node(node_id.to_string(), node_address.clone())
            .await;

        Ok(Arc::new(Self {
            router,
            network_router,
            groups: RwLock::new(HashMap::new()),
            node_id,
            node_address,
            storage_path,
            peers: RwLock::new(HashMap::new()),
        }))
    }

    /// 返回 Raft gRPC service，供调用者合并到共享 gRPC server。
    pub fn raft_service(&self) -> RaftServiceServer<MultiRaftServiceImpl<FilerTypeConfig>> {
        RaftServiceServer::new(MultiRaftServiceImpl::new(self.router.clone()))
    }

    /// 获取本节点 gRPC 地址。
    pub fn get_node_address(&self) -> &str {
        &self.node_address
    }

    /// 获取本节点 ID。
    pub fn get_node_id(&self) -> u64 {
        self.node_id
    }

    /// 注册对端节点（用于 Raft 通信路由）。
    pub async fn register_peer(&self, peer: Peer) {
        let peer_id = peer.id;
        self.network_router
            .register_node(peer_id.to_string(), peer.address.clone())
            .await;
        self.peers.write().await.insert(peer_id, peer);
        info!("RaftGroupManagerV2: registered peer id={}", peer_id);
    }

    /// 获取对端的 net_address（兼容旧 API，用于 TLV 客户端连接）。
    pub async fn get_peer_net_address(&self, peer_id: u64) -> Option<String> {
        let peers = self.peers.read().await;
        peers.get(&peer_id).map(|p| p.net_address.clone())
    }

    /// 创建一个 shard 的 Raft 组。
    ///
    /// 返回 apply_rx（接收 applied log index）。
    /// `meta_shard_manager` 的 apply 循环从此 channel 读取 index，
    /// 调用 `read_applied_entry` 获取 payload，反序列化为 `ShardCommand`，
    /// 再调用 `ShardStore::apply_command`。
    pub async fn create_group(
        self: &Arc<Self>,
        shard_id: ShardId,
        peers: Vec<Peer>,
    ) -> Result<mpsc::Receiver<u64>, String> {
        let group_id = shard_id.0.to_string();

        // 1) 创建 RocksDB 存储（每 shard 独立目录）
        let db_path = format!("{}/shard_{}", self.storage_path, shard_id.0);
        let (log_store, sm) = store::new::<FilerTypeConfig, _>(&db_path)
            .await
            .map_err(|e| format!("failed to create storage for shard {}: {}", shard_id.0, e))?;

        // 2) 设置 apply 通知 channel
        let (apply_tx, apply_rx) = mpsc::channel(1000);
        let sm = sm.with_apply_notifier(apply_tx);
        let db = sm.db().clone();

        // 3) 创建 openraft 配置
        let config = Arc::new(
            powerfs_raft::default_config()
                .validate()
                .map_err(|e| format!("invalid raft config: {}", e))?,
        );

        // 4) 创建 Network 工厂（绑定 group_id）
        let factory = MultiNetworkFactory::new(self.network_router.clone(), group_id.clone());

        // 5) 创建 Raft 实例
        let node_id_str = self.node_id.to_string();
        let raft = Raft::new(node_id_str, config, factory, log_store, sm)
            .await
            .map_err(|e| format!("failed to create raft for shard {}: {}", shard_id.0, e))?;

        // 6) 注册到服务端路由（入站 RPC 按 group_id 路由到此 Raft）
        self.router.register_group(&group_id, raft.clone()).await;

        // 7) 初始化集群成员
        //    - 单节点模式（peers 为空）：立即初始化
        //    - 多节点模式：ID 最小的节点调用 initialize，其余节点等待
        let should_initialize = peers.is_empty()
            || self.node_id <= peers.iter().map(|p| p.id).min().unwrap_or(self.node_id);

        if should_initialize {
            let mut members: BTreeMap<String, BasicNode> = BTreeMap::new();
            members.insert(
                self.node_id.to_string(),
                BasicNode {
                    addr: self.node_address.clone(),
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
                        "RaftGroupManagerV2: shard {} initialized (node={}, members={})",
                        shard_id.0,
                        self.node_id,
                        peers.len() + 1
                    );
                }
                Err(e) => {
                    // 多节点同时启动时，非首个 initialize 会失败 — 这是正常的
                    warn!(
                        "RaftGroupManagerV2: shard {} initialize returned: {} (may already be initialized by another node)",
                        shard_id.0, e
                    );
                }
            }
        }

        // 8) 保存 ShardGroup
        let group = Arc::new(ShardGroup {
            raft: raft.clone(),
            db,
            group_id: group_id.clone(),
            peers: peers.clone(),
        });
        self.groups.write().await.insert(shard_id, group);

        info!(
            "RaftGroupManagerV2: created shard {} (group_id={})",
            shard_id.0, group_id
        );

        Ok(apply_rx)
    }

    /// 获取 shard 的 Raft 句柄（内部使用）。
    async fn get_group(&self, shard_id: ShardId) -> Option<Arc<ShardGroup>> {
        self.groups.read().await.get(&shard_id).cloned()
    }

    /// 提议命令到指定 shard 的 Raft 日志。
    ///
    /// `data` 是序列化后的 `ShardCommand`（`serde_json::to_vec`）。
    /// 返回 committed log index。
    ///
    /// 如果本地节点不是 leader，自动通过 gRPC `Propose` RPC 转发到 leader 节点。
    /// 转发最多重试 5 次以应对 leader 切换。
    pub async fn propose(&self, shard_id: ShardId, data: Vec<u8>) -> Result<u64, String> {
        let group = self
            .get_group(shard_id)
            .await
            .ok_or_else(|| format!("shard {} not found", shard_id.0))?;

        let req = FilerRequest { payload: data };

        // **Design principle: NO server-to-server forwarding.**
        // Submit to local RaftCore. If not leader, openraft returns
        // ForwardToLeader error. We return it immediately so the caller
        // (net_handler) can return STATUS_ERR_REDIRECT to the client.
        // The client updates its shard_router and resends to the correct
        // leader. This is the容错 principle: a non-leader may be down,
        // forwarding to it would worsen the failure.
        //
        // Pre-check leadership via metrics to avoid the RaftCore round-trip
        // when we know we're not the leader. This also avoids spurious log
        // entries that openraft may silently drop on non-leader nodes.
        let metrics = group.raft.metrics().borrow_watched().clone();
        if metrics.state != ServerState::Leader {
            return Err(format!(
                "not_leader: shard {} requires client redirect (current state: {:?})",
                shard_id.0, metrics.state
            ));
        }

        match group.raft.client_write(req).await {
            Ok(resp) => Ok(resp.log_id.index()),
            Err(e) => {
                let err_str = format!("{}", e);
                let is_forward = err_str.contains("has to forward request to")
                    || err_str.contains("not the leader")
                    || err_str.contains("ForwardToLeader");
                if is_forward {
                    Err(format!(
                        "not_leader: shard {} requires client redirect",
                        shard_id.0
                    ))
                } else {
                    Err(format!(
                        "client_write failed for shard {}: {}",
                        shard_id.0, e
                    ))
                }
            }
        }
    }

    /// 检查本节点是否为指定 shard 的 leader。
    pub async fn is_shard_leader(&self, shard_id: ShardId) -> bool {
        if let Some(group) = self.get_group(shard_id).await {
            group.raft.metrics().borrow_watched().state == ServerState::Leader
        } else {
            false
        }
    }

    /// Ensure linearizable read on the local node for `shard_id`.
    ///
    /// When the local node is a **follower** of this shard, its RocksDB state
    /// machine may lag behind the leader's committed log. A read issued
    /// immediately after a leader-side propose (e.g. `unlink` on the leader
    /// followed by `rmdir` on a different shard leader that reads this shard
    /// as a follower) can observe a stale dir entry and wrongly return
    /// ENOTEMPTY.
    ///
    /// `ensure_linearizable()` blocks until the local Raft state machine has
    /// applied up to the leader's current commit index, guaranteeing the
    /// subsequent local read reflects all committed writes. On the leader it
    /// is effectively a no-op (leader apply is already up-to-date).
    ///
    /// Returns `Ok(())` if the local state is linearizable, or `Err` if the
    /// shard is unknown / Raft unavailable (caller should treat as "cannot
    /// confirm empty" and let the strict check proceed with its own logic).
    pub async fn ensure_linearizable(&self, shard_id: ShardId) -> Result<(), String> {
        let group = self
            .get_group(shard_id)
            .await
            .ok_or_else(|| format!("shard {} not found", shard_id.0))?;
        group
            .raft
            .ensure_linearizable(ReadPolicy::ReadIndex)
            .await
            .map(|_| ())
            .map_err(|e| format!("ensure_linearizable shard {}: {}", shard_id.0, e))
    }

    /// Spawn a background task that watches for leadership changes on the
    /// given shard. When the node loses leadership (Leader → non-Leader),
    /// the provided callback `on_leadership_lost` is invoked.
    ///
    /// This is used by `MetaShardManager::create_shard` to clear the
    /// `MetaCache` staging area when leadership is lost, preventing reads
    /// from returning uncommitted data that the new leader may not have.
    pub async fn spawn_leader_watcher<F>(&self, shard_id: ShardId, on_leadership_lost: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        let group = match self.get_group(shard_id).await {
            Some(g) => g,
            None => return,
        };

        let mut rx = group.raft.metrics();
        let sid = shard_id.0;
        let callback = std::sync::Arc::new(on_leadership_lost);

        tokio::spawn(async move {
            let mut was_leader = rx.borrow_watched().state == ServerState::Leader;
            debug!(
                "leader_watcher: shard {} initial state: was_leader={}",
                sid, was_leader
            );
            loop {
                match rx.changed().await {
                    Ok(_) => {
                        let current_state = rx.borrow_watched().state;
                        let is_leader = current_state == ServerState::Leader;
                        if was_leader && !is_leader {
                            warn!(
                                "leader_watcher: shard {} LOST leadership (now {:?}) — \
                                 clearing MetaCache staging to prevent stale reads",
                                sid, current_state
                            );
                            callback();
                        }
                        was_leader = is_leader;
                    }
                    Err(_) => {
                        // Sender dropped — Raft is shutting down
                        debug!(
                            "leader_watcher: shard {} metrics receiver closed (raft shutting down)",
                            sid
                        );
                        break;
                    }
                }
            }
        });
    }

    /// Spawn a background task that watches for leadership changes on the
    /// given shard and notifies the Master via `ShardLeaderUpdate` RPC.
    ///
    /// When this filer GAINS leadership (non-Leader → Leader), it sends
    /// `is_leader=1` + its own advertise_addr so the Master can update the
    /// `shard_id → leader_addr` table. When it LOSES leadership
    /// (Leader → non-Leader), it sends `is_leader=0` to clear the entry.
    ///
    /// This enables the zero-redirect fast path: fuse clients fetch the
    /// per-shard leader address from the Master's GetTopology response and
    /// route cap RPCs directly to the leader on the very first request.
    ///
    /// Best-effort: if the notification is lost, the fuse client's
    /// `check_leader_strict` redirect fallback still works.
    pub async fn spawn_shard_leader_notifier(
        &self,
        shard_id: ShardId,
        master_addr: String,
        filer_id: String,
        advertise_addr: String,
    ) {
        let group = match self.get_group(shard_id).await {
            Some(g) => g,
            None => return,
        };

        let mut rx = group.raft.metrics();
        let sid = shard_id.0;

        tokio::spawn(async move {
            let mut was_leader = rx.borrow_watched().state == ServerState::Leader;
            debug!(
                "shard_leader_notifier: shard {} initial state: was_leader={}",
                sid, was_leader
            );

            // If we start as leader, notify the Master immediately so the
            // shard_leaders table is populated on filer startup (not just
            // on leadership changes during runtime).
            if was_leader {
                info!(
                    "shard_leader_notifier: shard {} GAINED leadership at startup, notifying master",
                    sid
                );
                crate::zone_client::notify_shard_leader_change(
                    &master_addr,
                    sid,
                    true,
                    &filer_id,
                    &advertise_addr,
                )
                .await;
            }

            loop {
                match rx.changed().await {
                    Ok(_) => {
                        let current_state = rx.borrow_watched().state;
                        let is_leader = current_state == ServerState::Leader;
                        if !was_leader && is_leader {
                            // GAINED leadership: notify master with our address
                            info!(
                                "shard_leader_notifier: shard {} GAINED leadership, notifying master",
                                sid
                            );
                            crate::zone_client::notify_shard_leader_change(
                                &master_addr,
                                sid,
                                true,
                                &filer_id,
                                &advertise_addr,
                            )
                            .await;
                        } else if was_leader && !is_leader {
                            // LOST leadership: notify master to clear entry
                            warn!(
                                "shard_leader_notifier: shard {} LOST leadership (now {:?}), notifying master",
                                sid, current_state
                            );
                            crate::zone_client::notify_shard_leader_change(
                                &master_addr,
                                sid,
                                false,
                                &filer_id,
                                "",
                            )
                            .await;
                        }
                        was_leader = is_leader;
                    }
                    Err(_) => {
                        // Sender dropped — Raft is shutting down
                        debug!(
                            "shard_leader_notifier: shard {} metrics stream closed, exiting",
                            sid
                        );
                        return;
                    }
                }
            }
        });
    }

    /// Fire-and-forget propose: submit to Raft core and return immediately.
    ///
    /// Uses openraft's `client_write_ff` (fire-and-forget) API, which sends
    /// the `ClientWrite` message to RaftCore and returns without waiting for
    /// commit or apply. This is the fastest propose path — the log entry is
    /// appended to the leader's log and will be replicated/applied
    /// asynchronously by the Raft background task.
    ///
    /// Trade-off: if the leader crashes before the entry is committed, the
    /// data is lost. Acceptable for performance mode (default).
    ///
    /// **Design principle: NO server-to-server forwarding.**
    /// If the local node is not the leader for this shard, return an error
    /// immediately. The caller (net_handler) is responsible for returning
    /// STATUS_ERR_REDIRECT to the client, and the client updates its
    /// shard_router and resends to the correct leader. This is the容错
    /// principle: a non-leader node may be down, forwarding to it would
    /// worsen the failure. The client owns the shard→leader routing table
    /// (synced from Master topology + updated on redirect), so the server
    /// never needs to forward.
    pub async fn propose_ff(&self, shard_id: ShardId, data: Vec<u8>) -> Result<(), String> {
        let group = self
            .get_group(shard_id)
            .await
            .ok_or_else(|| format!("shard {} not found", shard_id.0))?;

        // **Critical: pre-check leadership before fire-and-forget.**
        //
        // openraft's `client_write_ff()` does NOT return ForwardToLeader when
        // the node is not the leader — it just sends a message to RaftCore and
        // returns Ok(()). RaftCore then silently drops the entry (no responder
        // to notify). Without this pre-check, propose_ff on a non-leader node
        // returns Ok but the entry is lost forever.
        //
        // This caused 98% data loss in testing (100 creates → only 2 visible)
        // because requests landed on non-leader nodes.
        //
        // The pre-check reads metrics to detect non-leader state. There's a
        // small TOCTOU window (leader change between check and submit), but
        // that's acceptable for fire-and-forget ops — the entry just gets
        // dropped by RaftCore, and the caller's retry (or client retry)
        // recovers. The important thing is catching the common case where the
        // node is clearly NOT the leader.
        let metrics = group.raft.metrics().borrow_watched().clone();
        if metrics.state != ServerState::Leader {
            debug!(
                "shard {}: propose_ff pre-check: not leader (state={:?}), returning redirect error",
                shard_id.0, metrics.state
            );
            return Err(format!(
                "not_leader: shard {} requires client redirect (current state: {:?})",
                shard_id.0, metrics.state
            ));
        }

        let req = FilerRequest { payload: data };

        // Fire-and-forget: submit to local RaftCore, return immediately.
        // No responder (None) — caller does not wait for commit/apply.
        match group.raft.client_write_ff(req, None).await {
            Ok(()) => Ok(()),
            Err(e) => {
                // Fatal errors only — client_write_ff does not return
                // ForwardToLeader (that's handled by the pre-check above).
                Err(format!(
                    "client_write_ff failed for shard {}: {}",
                    shard_id.0, e
                ))
            }
        }
    }

    /// Batch propose: submit multiple entries to the same shard's Raft log in
    /// a single `ClientWrite` message to RaftCore.
    ///
    /// This is significantly faster than calling `propose()` multiple times
    /// because all entries share one RaftCore round-trip, one `leader_append_entries`
    /// call, and one replication cycle (entries are batched into a single
    /// AppendEntries RPC to followers). Two entries committed via `propose_many`
    /// cost roughly the same latency as one `propose`.
    ///
    /// Each entry gets its own `ProgressResponder`, so `ForwardToLeader` is
    /// reliably returned (unlike `propose_ff` where `None` responder silently
    /// drops the entry on non-leader nodes).
    ///
    /// Returns the committed log indices for all entries, in submission order.
    ///
    /// **Design principle: NO server-to-server forwarding.** If the local node
    /// is not the leader, returns an error immediately so the caller can return
    /// `STATUS_ERR_REDIRECT` to the client.
    pub async fn propose_many(
        &self,
        shard_id: ShardId,
        data: Vec<Vec<u8>>,
    ) -> Result<Vec<u64>, String> {
        let group = self
            .get_group(shard_id)
            .await
            .ok_or_else(|| format!("shard {} not found", shard_id.0))?;

        if data.is_empty() {
            return Ok(Vec::new());
        }

        // Pre-check leadership (same rationale as propose/propose_ff).
        let metrics = group.raft.metrics().borrow_watched().clone();
        if metrics.state != ServerState::Leader {
            return Err(format!(
                "not_leader: shard {} requires client redirect (current state: {:?})",
                shard_id.0, metrics.state
            ));
        }

        let payloads: Vec<FilerRequest> = data
            .into_iter()
            .map(|d| FilerRequest { payload: d })
            .collect();

        let mut stream = group
            .raft
            .client_write_many(payloads)
            .await
            .map_err(|e| format!("client_write_many failed for shard {}: {}", shard_id.0, e))?;

        use futures::TryStreamExt;
        let mut indices = Vec::new();
        while let Some(write_result) = stream
            .try_next()
            .await
            .map_err(|e| format!("stream fatal for shard {}: {}", shard_id.0, e))?
        {
            match write_result {
                Ok(resp) => {
                    indices.push(resp.log_id.index());
                }
                Err(forward_err) => {
                    debug!(
                        "shard {}: propose_many ForwardToLeader: {:?}",
                        shard_id.0, forward_err
                    );
                    return Err(format!(
                        "not_leader: shard {} requires client redirect",
                        shard_id.0
                    ));
                }
            }
        }

        debug!(
            "propose_many: shard {} committed {} entries at indices {:?}",
            shard_id.0,
            indices.len(),
            indices
        );
        Ok(indices)
    }

    /// 获取指定 shard 的 leader 地址。
    ///
    /// 返回 `None` 如果无 leader 或本节点不知 leader 是谁。
    pub async fn get_shard_leader(&self, shard_id: ShardId) -> Option<String> {
        let group = self.get_group(shard_id).await?;
        let leader_id = group.raft.current_leader().await?;

        // leader_id 是 String（如 "1"），转 u64 查 peers 表获取 address
        let leader_u64: u64 = leader_id.parse().ok()?;
        let peers = self.peers.read().await;
        peers.get(&leader_u64).map(|p| p.address.clone())
    }

    /// 获取指定 shard 的 leader 状态。
    ///
    /// 返回 `(is_leader, leader_address)`。
    pub async fn get_shard_leader_status(&self, shard_id: ShardId) -> Option<(bool, String)> {
        let group = self.get_group(shard_id).await?;
        let metrics = group.raft.metrics().borrow_watched().clone();
        let is_leader = metrics.state == ServerState::Leader;

        let leader_addr = if is_leader {
            self.node_address.clone()
        } else {
            self.get_shard_leader(shard_id).await.unwrap_or_default()
        };

        Some((is_leader, leader_addr))
    }

    /// 获取指定 shard 的状态。
    ///
    /// 返回 `(is_leader, term, commit_index, last_applied)`。
    pub async fn get_shard_status(&self, shard_id: ShardId) -> Option<(bool, u64, u64, u64)> {
        let group = self.get_group(shard_id).await?;
        let metrics = group.raft.metrics().borrow_watched().clone();
        let is_leader = metrics.state == ServerState::Leader;
        let term = metrics.current_term;
        let commit_index = metrics.local_committed.map(|l| l.index()).unwrap_or(0);
        let last_applied = metrics.last_applied.map(|l| l.index()).unwrap_or(0);
        Some((is_leader, term, commit_index, last_applied))
    }

    /// 获取指定 shard 的对端列表（创建时的快照）。
    pub async fn get_shard_peers(&self, shard_id: ShardId) -> Option<Vec<Peer>> {
        let group = self.get_group(shard_id).await?;
        Some(group.peers.clone())
    }

    /// 列出所有 shard ID。
    pub async fn list_shards(&self) -> Vec<ShardId> {
        self.groups.read().await.keys().cloned().collect()
    }

    /// 获取 shard 数量。
    pub async fn get_shard_count(&self) -> usize {
        self.groups.read().await.len()
    }

    /// 转移指定 shard 的 leader 到目标节点。
    pub async fn transfer_shard_leader(
        &self,
        shard_id: ShardId,
        target_id: u64,
    ) -> Result<(), String> {
        let group = self
            .get_group(shard_id)
            .await
            .ok_or_else(|| format!("shard {} not found", shard_id.0))?;

        group
            .raft
            .trigger()
            .transfer_leader(target_id.to_string())
            .await
            .map_err(|e| {
                format!(
                    "transfer_leader failed for shard {} to {}: {}",
                    shard_id.0, target_id, e
                )
            })?;
        Ok(())
    }

    /// 移除一个 shard 的 Raft 组。
    pub async fn remove_group(&self, shard_id: ShardId) -> Result<(), String> {
        let group = self
            .groups
            .write()
            .await
            .remove(&shard_id)
            .ok_or_else(|| format!("shard {} not found", shard_id.0))?;

        self.router.unregister_group(&group.group_id).await;
        info!("RaftGroupManagerV2: removed shard {}", shard_id.0);
        Ok(())
    }

    /// 从 `raft_state_data` CF 读取指定 index 的 applied entry payload。
    ///
    /// 返回 `ShardCommand` 的序列化 bytes（即 `serde_json::to_vec(&ShardCommand)` 的结果）。
    /// 调用方进一步 `ShardCommand::deserialize(&payload)` 还原业务命令。
    pub async fn read_applied_entry(
        &self,
        shard_id: ShardId,
        index: u64,
    ) -> Result<Option<Vec<u8>>, String> {
        // 异步读取 — 避免 blocking_read 在 async 上下文中 panic
        let groups = self.groups.read().await;
        let group = groups
            .get(&shard_id)
            .ok_or_else(|| format!("shard {} not found", shard_id.0))?;

        let cf = group
            .db
            .cf_handle(store::CF_STATE_DATA)
            .ok_or("raft_state_data CF not found")?;

        let bytes = group
            .db
            .get_cf(cf, index.to_be_bytes())
            .map_err(|e| format!("rocksdb get failed: {}", e))?;

        Ok(bytes.map(|b| {
            // 反序列化 FilerRequest，提取 payload
            match serde_json::from_slice::<FilerRequest>(&b) {
                Ok(req) => req.payload,
                Err(_) => b.to_vec(),
            }
        }))
    }

    /// 读取指定 shard 的 Raft 状态机中已 apply 的最后 log index。
    ///
    /// 从 `raft_state_meta` CF 的 `"last_applied_log"` 键反序列化 `LogId` 获取 index。
    /// 用于 apply 循环启动时初始化 `last_applied`，避免首次通知只处理最后一个 index
    /// 而跳过同批提交的前序条目（如 CreateInode@N 被 AddDirEntry@N+1 的首次通知跳过）。
    pub async fn get_last_applied_index(&self, shard_id: ShardId) -> Option<u64> {
        let groups = self.groups.read().await;
        let group = groups.get(&shard_id)?;

        let cf = group.db.cf_handle(store::CF_STATE_META)?;
        let bytes = group.db.get_cf(cf, "last_applied_log").ok()??;

        // LogIdOf<FilerTypeConfig> = openraft::LogId<FilerTypeConfig::CommittedLeaderId>
        let log_id: LogIdOf<FilerTypeConfig> = serde_json::from_slice(&bytes).ok()?;
        Some(log_id.index())
    }

    /// 扫描指定 shard 的所有 applied entries（按 index 升序）。
    ///
    /// 供启动时 replay 所有已 apply 的 Normal entries（确保 ShardStore 与 Raft 状态一致）。
    pub fn scan_applied_entries(&self, shard_id: ShardId) -> Result<Vec<(u64, Vec<u8>)>, String> {
        let groups = self.groups.blocking_read();
        let group = groups
            .get(&shard_id)
            .ok_or_else(|| format!("shard {} not found", shard_id.0))?;

        let cf = group
            .db
            .cf_handle(store::CF_STATE_DATA)
            .ok_or("raft_state_data CF not found")?;

        let mut result = Vec::new();
        let iter = group.db.iterator_cf(cf, rocksdb::IteratorMode::Start);

        for item in iter {
            let (key, value) = item.map_err(|e| format!("rocksdb iterator error: {}", e))?;
            if key.len() == 8 {
                let index = u64::from_be_bytes([
                    key[0], key[1], key[2], key[3], key[4], key[5], key[6], key[7],
                ]);
                let payload = match serde_json::from_slice::<FilerRequest>(&value) {
                    Ok(req) => req.payload,
                    Err(_) => value.to_vec(),
                };
                result.push((index, payload));
            }
        }

        result.sort_by_key(|(idx, _)| *idx);
        Ok(result)
    }
}
