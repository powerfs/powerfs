use bytes;
use log::{debug, error, info, warn};
use powerfs_common::raft::RocksDbRaftStorage;
use powerfs_net::serialize::TlvEncoder;
use powerfs_net::FieldId;
use protobuf::Message;
use raft::eraftpb::{ConfChange, ConfChangeType, Message as RaftMessage};
use raft::storage::Storage;
use raft::{Config, RawNode, StateRole};
use slog::{Discard, Logger};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock as StdRwLock};
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tokio::time::interval;

const SNAPSHOT_THRESHOLD: u64 = 10000;

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

#[derive(Debug, Clone)]
pub struct OutgoingMessage {
    pub shard_id: ShardId,
    pub to_id: u64,
    pub message: bytes::Bytes,
}

#[derive(Debug)]
pub struct ProposeRequest {
    pub shard_id: ShardId,
    pub data: Vec<u8>,
    pub response_tx: tokio::sync::oneshot::Sender<Result<u64, String>>,
}

#[derive(Debug, Clone)]
pub struct ApplyEntry {
    pub shard_id: ShardId,
    pub index: u64,
    pub command: ShardCommand,
}

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
    /// Set inode attributes (size, mode, uid, gid) - legacy unified command
    SetAttr {
        inode: u64,
        size: Option<u64>,
        mode: Option<u64>,
        uid: Option<u64>,
        gid: Option<u64>,
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
    },
    /// P3: Set an extended attribute on an inode (persisted via Raft).
    /// Used for `powerfs.placement` xattr on directories.
    SetXattr {
        inode: u64,
        key: String,
        value: Vec<u8>,
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
}

impl ShardCommand {
    pub fn serialize(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    pub fn deserialize(data: &[u8]) -> Result<Self, String> {
        serde_json::from_slice(data).map_err(|e| e.to_string())
    }
}

pub struct RaftGroup {
    shard_id: ShardId,
    node: RawNode<RocksDbRaftStorage>,
    id: u64,
    address: String,
    peers: HashMap<u64, Peer>,
    propose_tx: mpsc::Sender<ProposeRequest>,
    propose_rx: mpsc::Receiver<ProposeRequest>,
    message_tx: tokio::sync::broadcast::Sender<OutgoingMessage>,
    step_tx: mpsc::Sender<RaftMessage>,
    step_rx: mpsc::Receiver<RaftMessage>,
    apply_tx: mpsc::Sender<ApplyEntry>,
    _apply_rx: mpsc::Receiver<ApplyEntry>,
    running: Arc<RwLock<bool>>,
    applied_index: Arc<StdRwLock<u64>>,
    leader_state: Arc<AtomicBool>,
    leader_address: Arc<StdRwLock<String>>,
}

impl RaftGroup {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        shard_id: ShardId,
        id: u64,
        address: String,
        peers: Vec<Peer>,
        storage_path: &str,
        leader_state: Arc<AtomicBool>,
        leader_address: Arc<StdRwLock<String>>,
        message_tx: tokio::sync::broadcast::Sender<OutgoingMessage>,
        apply_tx: mpsc::Sender<ApplyEntry>,
    ) -> Result<Self, String> {
        let storage_path = format!("{}/shard_{}", storage_path, shard_id.0);

        let storage = if peers.is_empty() {
            RocksDbRaftStorage::new_with_single_node(&storage_path, id)
                .map_err(|e| format!("failed to create storage: {}", e))?
        } else {
            let mut peer_ids = peers.iter().map(|p| p.id).collect::<Vec<_>>();
            if !peer_ids.contains(&id) {
                peer_ids.push(id);
            }
            peer_ids.sort_unstable();
            RocksDbRaftStorage::new_with_peers(&storage_path, &peer_ids)
                .map_err(|e| format!("failed to create storage: {}", e))?
        };

        let initial_state = storage
            .initial_state()
            .map_err(|e| format!("failed to get initial state: {}", e))?;

        let mut cfg = Config {
            id,
            // election_tick 决定选举超时范围 [election_tick, 2*election_tick) * tick_interval.
            // tick_interval=50ms, election_tick=30 => 超时范围 [1.5s, 3s), 随机跨度 1.5s
            // 足以避免 2 节点同时发起选举 (split vote). 之前 election_tick=20 跨度仅 1s,
            // 停 1 个节点后剩余 2 节点频繁同时选举, 无法选出 leader.
            election_tick: 30,
            heartbeat_tick: 5,
            max_size_per_msg: 1 << 20,
            max_inflight_msgs: 256,
            check_quorum: !peers.is_empty(),
            // 启用 PreVote: Candidate 先发 PreVote (不增加 term) 探测是否可能胜出,
            // 收到 quorum 同意后才正式选举. 避免多节点同时选举导致的 split vote
            // 和 term 无限增长. 这是 etcd/cockroachdb 等生产 Raft 实现的标准配置.
            pre_vote: true,
            ..Default::default()
        };
        cfg.validate()
            .map_err(|e| format!("invalid raft config: {}", e))?;

        if let Ok(last_idx) = storage.last_index() {
            let commit_index = initial_state.hard_state.commit;
            cfg.applied = last_idx.min(commit_index);
            if cfg.applied < last_idx {
                warn!(
                    "Clamped applied index from {} to {} (commit={})",
                    last_idx, cfg.applied, commit_index
                );
            }
        }

        let logger = Logger::root(Discard, slog::o!());

        let node = RawNode::new(&cfg, storage.clone(), &logger)
            .map_err(|e| format!("failed to create raft node: {}", e))?;

        let (propose_tx, propose_rx) = mpsc::channel(1000);
        let (step_tx, step_rx) = mpsc::channel(1000);

        let mut peer_map = HashMap::new();
        for peer in &peers {
            peer_map.insert(peer.id, peer.clone());
        }

        // Don't pre-set leader_state - let Raft election happen naturally
        // This ensures that the actual Raft state is consistent with leader_state

        info!(
            "Created RaftGroup: shard_id={}, id={}, address={}, peers={:?}",
            shard_id.0,
            id,
            address,
            peers.iter().map(|p| p.id).collect::<Vec<_>>()
        );

        Ok(Self {
            shard_id,
            node,
            id,
            address,
            peers: peer_map,
            propose_tx,
            propose_rx,
            message_tx,
            step_tx,
            step_rx,
            apply_tx,
            _apply_rx: mpsc::channel(1).1, // Dummy, not used
            running: Arc::new(RwLock::new(true)),
            applied_index: Arc::new(StdRwLock::new(0)),
            leader_state,
            leader_address,
        })
    }

    pub async fn run(&mut self) -> Result<(), String> {
        info!("Starting Raft event loop for shard {}", self.shard_id.0);

        let mut tick_interval = interval(Duration::from_millis(50));
        let mut tick_count = 0u64;

        while *self.running.read().await {
            tokio::select! {
                _ = tick_interval.tick() => {
                    self.node.tick();
                    tick_count += 1;

                    // Log state periodically
                    if tick_count.is_multiple_of(50) {
                        let state = format!("{:?}", self.node.raft.state);
                        let leader_id = self.node.raft.leader_id;
                        debug!("Shard {} tick #{}, state={}, leader_id={}", self.shard_id.0, tick_count, state, leader_id);
                    }

                    self.update_leader_address();
                    while self.node.has_ready() {
                        self.process_ready();
                    }
                }

                req = self.propose_rx.recv() => {
                    if let Some(req) = req {
                        self.handle_propose(req).await;
                    }
                }

                msg = self.step_rx.recv() => {
                    if let Some(msg) = msg {
                        self.handle_step(msg);
                    }
                }
            }
        }

        info!("Raft event loop stopped for shard {}", self.shard_id.0);
        Ok(())
    }

    fn process_ready(&mut self) {
        if !self.node.has_ready() {
            return;
        }

        let mut ready = self.node.ready();
        let mut messages_to_send = Vec::new();

        if !ready.messages().is_empty() {
            messages_to_send.extend(ready.take_messages());
        }

        if let Some(ss) = ready.ss() {
            let is_leader_now = ss.raft_state == StateRole::Leader;
            let prev = self.leader_state.swap(is_leader_now, Ordering::Relaxed);
            info!(
                "Shard {} state change: is_leader={}, prev={}, leader_id={:?}",
                self.shard_id.0, is_leader_now, prev, self.node.raft.leader_id
            );

            let new_leader_addr = if is_leader_now {
                self.address.clone()
            } else {
                let leader_id = self.node.raft.leader_id;
                if leader_id == 0 {
                    String::new()
                } else if leader_id == self.id {
                    self.address.clone()
                } else {
                    match self.peers.get(&leader_id) {
                        Some(peer) => peer.address.clone(),
                        None => String::new(),
                    }
                }
            };
            *self.leader_address.write().unwrap() = new_leader_addr;

            if prev != is_leader_now {
                info!(
                    "Shard {} role changed: node {} is now {:?}",
                    self.shard_id.0, self.id, ss.raft_state
                );
            }
        }

        if !ready.snapshot().is_empty() {
            let snap = ready.snapshot().clone();
            if let Err(e) = self.node.mut_store().apply_snapshot(snap) {
                error!("Shard {} failed to apply snapshot: {}", self.shard_id.0, e);
            }
        }

        if !ready.entries().is_empty() {
            if let Err(e) = self.node.mut_store().append(ready.entries()) {
                error!("Shard {} failed to append entries: {}", self.shard_id.0, e);
            }
        }

        if let Some(hs) = ready.hs() {
            self.node.mut_store().set_hardstate(hs.clone());
        }

        let committed = ready.take_committed_entries();
        if !committed.is_empty() {
            for entry in committed {
                if entry.data.is_empty() {
                    let mut applied = self.applied_index.write().unwrap();
                    *applied = entry.index;
                    continue;
                }

                match ShardCommand::deserialize(&entry.data) {
                    Ok(cmd) => {
                        let apply_entry = ApplyEntry {
                            shard_id: self.shard_id,
                            index: entry.index,
                            command: cmd,
                        };
                        // Use try_send (synchronous) instead of spawn+await
                        // so the apply item is in the channel buffer BEFORE
                        // we set applied_index.  This prevents the polling
                        // loop in create_directory/lookup from timing out
                        // while waiting for the spawned task to be scheduled.
                        if let Err(e) = self.apply_tx.try_send(apply_entry) {
                            error!(
                                "Shard {} failed to send apply item at index {}: {}",
                                self.shard_id.0, entry.index, e
                            );
                        }
                    }
                    Err(e) => {
                        error!(
                            "Shard {} failed to deserialize command at index {}: {}",
                            self.shard_id.0, entry.index, e
                        );
                    }
                }

                let mut applied = self.applied_index.write().unwrap();
                *applied = entry.index;
            }
        }

        if !ready.persisted_messages().is_empty() {
            messages_to_send.extend(ready.take_persisted_messages());
        }

        if self.is_leader() {
            self.try_create_snapshot();
        }

        let mut light_rd = self.node.advance(ready);

        if !light_rd.messages().is_empty() {
            messages_to_send.extend(light_rd.take_messages());
        }

        let committed = light_rd.take_committed_entries();
        if !committed.is_empty() {
            for entry in committed {
                if entry.data.is_empty() {
                    let mut applied = self.applied_index.write().unwrap();
                    *applied = entry.index;
                    continue;
                }

                match ShardCommand::deserialize(&entry.data) {
                    Ok(cmd) => {
                        let apply_entry = ApplyEntry {
                            shard_id: self.shard_id,
                            index: entry.index,
                            command: cmd,
                        };
                        // Use try_send (synchronous) so the apply item is in
                        // the channel buffer BEFORE applied_index is updated.
                        // This prevents polling loops in create_directory/lookup
                        // from timing out while waiting for a spawned task to be
                        // scheduled (same fix as the primary committed_entries
                        // path above).
                        if let Err(e) = self.apply_tx.try_send(apply_entry) {
                            error!(
                                "Shard {} failed to send light_rd apply item at index {}: {}",
                                self.shard_id.0, entry.index, e
                            );
                        }
                    }
                    Err(e) => {
                        error!(
                            "Shard {} failed to deserialize command at index {}: {}",
                            self.shard_id.0, entry.index, e
                        );
                    }
                }

                let mut applied = self.applied_index.write().unwrap();
                *applied = entry.index;
            }
        }

        self.node.advance_apply();

        // Update leader_address from raft.leader_id (covers heartbeat-only updates
        // where ss is None but leader_id has changed)
        if !self.is_leader() {
            let leader_id = self.node.raft.leader_id;
            let new_addr = if leader_id == 0 {
                String::new()
            } else if leader_id == self.id {
                self.address.clone()
            } else {
                self.peers
                    .get(&leader_id)
                    .map_or(String::new(), |p| p.address.clone())
            };
            *self.leader_address.write().unwrap() = new_addr;
        }

        if !messages_to_send.is_empty() {
            for msg in messages_to_send {
                self.send_message(&msg);
            }
        }
    }

    fn send_message(&self, msg: &RaftMessage) {
        let to_id = msg.to;
        if to_id == self.id {
            return;
        }

        if !self.peers.contains_key(&to_id) {
            return;
        }

        let mut buf = Vec::new();
        if let Err(e) = msg.write_to_vec(&mut buf) {
            error!(
                "Shard {} failed to serialize message: {}",
                self.shard_id.0, e
            );
            return;
        }

        let outgoing = OutgoingMessage {
            shard_id: self.shard_id,
            to_id,
            message: bytes::Bytes::from(buf),
        };

        if let Err(e) = self.message_tx.send(outgoing) {
            warn!(
                "Shard {} failed to send message to {}: {}",
                self.shard_id.0, to_id, e
            );
        }
    }

    async fn handle_propose(&mut self, req: ProposeRequest) {
        if !self.is_leader() {
            let _ = req.response_tx.send(Err("not the leader".to_string()));
            return;
        }

        let data = req.data;

        if let Err(e) = self.node.propose(vec![], data) {
            let _ = req.response_tx.send(Err(format!("propose failed: {}", e)));
            return;
        }

        self.process_ready();

        let index = self.node.raft.raft_log.committed;
        let _ = req.response_tx.send(Ok(index));
    }

    pub fn get_propose_tx(&self) -> mpsc::Sender<ProposeRequest> {
        self.propose_tx.clone()
    }

    pub async fn propose(&self, data: Vec<u8>) -> Result<u64, String> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        self.propose_tx
            .send(ProposeRequest {
                shard_id: self.shard_id,
                data,
                response_tx,
            })
            .await
            .map_err(|e| format!("failed to send propose: {}", e))?;

        response_rx
            .await
            .map_err(|e| format!("propose response error: {}", e))?
    }

    pub fn get_step_tx(&self) -> mpsc::Sender<RaftMessage> {
        self.step_tx.clone()
    }

    fn handle_step(&mut self, msg: RaftMessage) {
        if let Err(e) = self.node.step(msg) {
            error!("Shard {} step failed: {}", self.shard_id.0, e);
        }
        // Update leader_address from raft.leader_id (heartbeats update leader_id
        // without producing a Ready event)
        self.update_leader_address();
        self.process_ready();
    }

    /// Update leader_address from raft.leader_id
    fn update_leader_address(&self) {
        if self.is_leader() {
            *self.leader_address.write().unwrap() = self.address.clone();
            return;
        }
        let leader_id = self.node.raft.leader_id;
        let new_addr = if leader_id == 0 {
            String::new()
        } else if leader_id == self.id {
            self.address.clone()
        } else {
            self.peers
                .get(&leader_id)
                .map_or(String::new(), |p| p.address.clone())
        };
        *self.leader_address.write().unwrap() = new_addr;
    }

    pub fn is_leader(&self) -> bool {
        let result = self.node.raft.state == StateRole::Leader;
        if result {
            debug!(
                "Shard {}: is_leader()=true, state={:?}",
                self.shard_id.0, self.node.raft.state
            );
        }
        result
    }

    pub fn is_follower(&self) -> bool {
        self.node.raft.state == StateRole::Follower
    }

    pub fn get_status(&self) -> (bool, u64, u64, u64) {
        // 返回 (is_leader, term, commit_index, applied_index)
        let is_leader = self.leader_state.load(std::sync::atomic::Ordering::SeqCst);
        let applied = *self.applied_index.read().unwrap();
        // 获取raft的commit_index和term，通过RawNode获取
        // 如果无法获取，返回默认值
        (is_leader, 0, 0, applied)
    }

    /// Returns a clone of the Arc<StdRwLock<u64>> tracking the applied index,
    /// so external callers (e.g. RaftGroupManager) can read it without
    /// acquiring the group RwLock.
    pub fn applied_index_handle(&self) -> Arc<StdRwLock<u64>> {
        self.applied_index.clone()
    }

    pub fn term(&self) -> u64 {
        self.node.raft.term
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    pub fn leader_id(&self) -> u64 {
        self.node.raft.leader_id
    }

    pub fn leader_address(&self) -> String {
        let leader_id = self.node.raft.leader_id;
        if leader_id == 0 {
            return String::new();
        }
        if leader_id == self.id {
            return self.address.clone();
        }
        for peer in &self.peers {
            if *peer.0 == leader_id {
                return peer.1.address.clone();
            }
        }
        String::new()
    }

    pub fn commit_index(&self) -> u64 {
        self.node.raft.raft_log.committed
    }

    pub fn transfer_leader(&mut self, target_id: u64) -> Result<(), String> {
        info!(
            "Shard {} transferring leadership to node: {}",
            self.shard_id.0, target_id
        );

        if !self.is_leader() {
            return Err("not the leader".to_string());
        }

        if target_id == self.id {
            return Err("cannot transfer leadership to self".to_string());
        }

        if !self.peers.contains_key(&target_id) {
            return Err(format!("target node {} is not a peer", target_id));
        }

        self.node.transfer_leader(target_id);

        info!(
            "Shard {} leadership transfer initiated to node: {}",
            self.shard_id.0, target_id
        );
        Ok(())
    }

    pub fn last_index(&self) -> u64 {
        self.node.raft.raft_log.last_index()
    }

    pub fn get_peers(&self) -> Vec<Peer> {
        self.peers.values().cloned().collect()
    }

    pub fn get_message_tx(&self) -> tokio::sync::broadcast::Sender<OutgoingMessage> {
        self.message_tx.clone()
    }

    pub async fn stop(&mut self) {
        *self.running.write().await = false;
        info!("RaftGroup {} stopped", self.shard_id.0);
    }

    fn try_create_snapshot(&mut self) {
        let last_index = self.last_index();
        let last_applied = self.node.raft.raft_log.applied;

        if last_index - last_applied >= SNAPSHOT_THRESHOLD {
            info!(
                "Shard {} log entries exceed threshold ({}), triggering automatic snapshot",
                self.shard_id.0, SNAPSHOT_THRESHOLD
            );
        }
    }

    pub fn add_peer(&mut self, peer: Peer) -> Result<(), String> {
        info!(
            "Shard {} adding peer: id={}, address={}",
            self.shard_id.0, peer.id, peer.address
        );

        let cc = ConfChange {
            node_id: peer.id,
            change_type: ConfChangeType::AddNode,
            ..Default::default()
        };

        self.node
            .propose_conf_change(vec![], cc)
            .map_err(|e| format!("failed to add peer: {}", e))?;

        self.peers.insert(peer.id, peer);
        self.process_ready();

        Ok(())
    }

    pub fn remove_peer(&mut self, peer_id: u64) -> Result<(), String> {
        info!("Shard {} removing peer: id={}", self.shard_id.0, peer_id);

        let cc = ConfChange {
            node_id: peer_id,
            change_type: ConfChangeType::RemoveNode,
            ..Default::default()
        };

        self.node
            .propose_conf_change(vec![], cc)
            .map_err(|e| format!("failed to remove peer: {}", e))?;

        self.peers.remove(&peer_id);
        self.process_ready();

        Ok(())
    }
}

/// Snapshot of Arc handles to per-shard status, kept outside the group lock
/// so that status queries never block on the Raft event loop (which holds
/// the group's write lock for its entire lifetime).
pub struct ShardStatusArcs {
    pub leader_state: Arc<AtomicBool>,
    pub applied_index: Arc<StdRwLock<u64>>,
    pub leader_address: Arc<StdRwLock<String>>,
}

pub struct RaftGroupManager {
    groups: RwLock<HashMap<ShardId, Arc<RwLock<RaftGroup>>>>,
    // Per-shard Arc handles for status queries. Filled in create_group so
    // that get_shard_status can read leader_state/applied_index without
    // acquiring the group RwLock (which is permanently held by run()).
    shard_status_arcs: RwLock<HashMap<ShardId, ShardStatusArcs>>,
    // Per-shard propose senders, cached at group creation to avoid needing
    // the group RwLock (which is permanently held by run()).
    shard_propose_txs: RwLock<HashMap<ShardId, mpsc::Sender<ProposeRequest>>>,
    // Per-shard step senders, cached at group creation for the same reason.
    shard_step_txs: RwLock<HashMap<ShardId, mpsc::Sender<RaftMessage>>>,
    // Per-shard apply receivers, stored before event loop starts to avoid
    // needing the group RwLock (which is permanently held by run()).
    shard_apply_rxs: RwLock<HashMap<ShardId, mpsc::Receiver<ApplyEntry>>>,
    node_id: u64,
    node_address: String,
    storage_path: String,
    message_tx: tokio::sync::broadcast::Sender<OutgoingMessage>,
    apply_tx: mpsc::Sender<ApplyEntry>,
    peers: RwLock<HashMap<u64, Peer>>,
    // Persistent TLV connections for Raft message transport (per peer addr).
    // Replaces call_once (which created a new TCP connection per message).
    // Key = peer net_address ("ip:net_port"). Each peer has its own Mutex
    // so messages to different peers can be sent concurrently.
    peer_conns: tokio::sync::Mutex<
        HashMap<String, std::sync::Arc<tokio::sync::Mutex<powerfs_net::NetRpcClient>>>,
    >,
}

impl RaftGroupManager {
    pub fn new(node_id: u64, node_address: String, storage_path: String) -> Self {
        let (message_tx, _) = tokio::sync::broadcast::channel(1000);
        let (apply_tx, _) = mpsc::channel(1000);

        Self {
            groups: RwLock::new(HashMap::new()),
            shard_status_arcs: RwLock::new(HashMap::new()),
            shard_propose_txs: RwLock::new(HashMap::new()),
            shard_step_txs: RwLock::new(HashMap::new()),
            shard_apply_rxs: RwLock::new(HashMap::new()),
            node_id,
            node_address,
            storage_path,
            message_tx,
            apply_tx,
            peers: RwLock::new(HashMap::new()),
            peer_conns: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Get this node's gRPC address (used for Raft communication).
    /// The caller can convert it to a net address by replacing the port.
    pub fn get_node_address(&self) -> &str {
        &self.node_address
    }

    /// Register a peer node for Raft communication
    pub async fn register_peer(&self, peer: Peer) {
        let peer_id = peer.id;
        let mut peers = self.peers.write().await;
        peers.insert(peer_id, peer);
        info!("Registered peer: id={}", peer_id);
    }

    /// Get peer's powerfs-net address (ip:net_port) by ID.
    /// Used for TLV Raft message transport (replaces gRPC address).
    pub async fn get_peer_net_address(&self, peer_id: u64) -> Option<String> {
        let peers = self.peers.read().await;
        peers.get(&peer_id).map(|p| p.net_address.clone())
    }

    /// Send a Raft message to a peer via TLV (MsgType::RaftMessage).
    ///
    /// Replaces the gRPC FilerMetaService::send_raft_message RPC.
    /// Uses persistent `NetRpcClient` connections (one per peer addr) with
    /// auto-reconnect, replacing the former `call_once` which created a new
    /// TCP connection per message.
    async fn send_raft_message_to_peer(
        &self,
        peer_net_addr: &str,
        shard_id: ShardId,
        message: &RaftMessage,
    ) {
        // Serialize the Raft message (protobuf eraftpb::Message)
        let mut payload = Vec::new();
        if let Err(e) = message.write_to_vec(&mut payload) {
            error!("Failed to serialize Raft message: {}", e);
            return;
        }

        // Build TLV body: ShardId + RaftPayload
        let mut enc = TlvEncoder::new();
        let _ = enc.add_u64(FieldId::ShardId, shard_id.0);
        let _ = enc.add_bytes(FieldId::RaftPayload, &payload);
        let body = enc.into_bytes();

        // Use stable client_id (node_id) so the server can track the
        // connection across reconnects. channel=DATA (Raft is inter-service
        // control, not lease/meta).
        let client_id = self.node_id;

        // Get or create persistent connection for this peer addr.
        // The global lock is only held briefly to look up / insert the
        // per-peer Arc<Mutex<NetRpcClient>>. The actual send happens under
        // the per-peer mutex, so different peers can send concurrently.
        let conn_arc = {
            let conns = self.peer_conns.lock().await;
            conns.get(peer_net_addr).cloned()
        };

        let conn_arc = if let Some(arc) = conn_arc {
            arc
        } else {
            // No existing connection — create one (NOT holding the global lock
            // to avoid blocking other peers during this peer's connect).
            match powerfs_net::NetRpcClient::connect(
                peer_net_addr,
                powerfs_net::ClientType::Filer,
                client_id,
                powerfs_net::CHANNEL_DATA,
            )
            .await
            {
                Ok(client) => {
                    let arc = std::sync::Arc::new(tokio::sync::Mutex::new(client));
                    // Re-check: another task may have inserted a connection
                    // while we were connecting. If so, use the existing one
                    // and drop ours (prevents duplicate connections with the
                    // same client_id, which causes server-side registry races).
                    let mut conns = self.peer_conns.lock().await;
                    if let Some(existing) = conns.get(peer_net_addr) {
                        log::debug!(
                            "FILER_RAFT: duplicate connection to {} discarded (race won by another task)",
                            peer_net_addr
                        );
                        existing.clone()
                    } else {
                        info!(
                            "FILER_RAFT: established persistent connection to {}",
                            peer_net_addr
                        );
                        conns.insert(peer_net_addr.to_string(), arc.clone());
                        arc
                    }
                }
                Err(e) => {
                    warn!("FILER_RAFT: failed to connect to {}: {}", peer_net_addr, e);
                    return;
                }
            }
        };

        // Send via persistent connection with auto-reconnect.
        let mut conn = conn_arc.lock().await;
        match conn
            .call_with_retry(powerfs_net::MsgType::RaftMessage, &body)
            .await
        {
            Ok(reply) if reply.is_ok() => {
                debug!(
                    "Raft message sent to peer {} for shard {}",
                    peer_net_addr, shard_id.0
                );
            }
            Ok(reply) => {
                warn!(
                    "Failed to send Raft message to peer {}: status={:#06x}: {}",
                    peer_net_addr,
                    reply.status,
                    reply.body_str()
                );
            }
            Err(e) => {
                warn!(
                    "Failed to send Raft message to peer {}: {}",
                    peer_net_addr, e
                );
            }
        }
    }

    /// Start the Raft message transmission loop
    pub async fn start_message_transmitter(self: Arc<Self>) {
        eprintln!("[DEBUG] start_message_transmitter called, creating subscriber");
        info!("start_message_transmitter called, creating subscriber");
        let mut message_rx = self.message_tx.subscribe();
        eprintln!("[DEBUG] Subscriber created, spawning transmitter task");
        info!("Subscriber created, spawning transmitter task");

        tokio::spawn(async move {
            eprintln!("[DEBUG] Raft message transmitter started");
            info!("Raft message transmitter started");

            while let Ok(outgoing) = message_rx.recv().await {
                let peer_net_addr = match self.get_peer_net_address(outgoing.to_id).await {
                    Some(addr) => addr,
                    None => {
                        warn!("Peer {} not found for Raft message", outgoing.to_id);
                        continue;
                    }
                };

                // Deserialize the message
                let mut msg = RaftMessage::new();
                if let Err(e) = msg.merge_from_bytes(&outgoing.message) {
                    error!("Failed to deserialize outgoing Raft message: {}", e);
                    continue;
                }

                // 并发发送: 每条消息独立 spawn task.
                // 之前串行 await 导致 filer-3 不可达时 gRPC 5s 超时阻塞了
                // 发给 filer-2 的心跳, leader 的 check_quorum 因收不到 filer-2
                // 的 Ack 而失败, 降级为 Follower. 并发后各 peer 互不阻塞.
                let self_clone = self.clone();
                tokio::spawn(async move {
                    self_clone
                        .send_raft_message_to_peer(&peer_net_addr, outgoing.shard_id, &msg)
                        .await;
                });
            }

            info!("Raft message transmitter stopped");
        });
    }

    pub async fn create_group(
        &self,
        shard_id: ShardId,
        peers: Vec<Peer>,
    ) -> Result<Arc<RwLock<RaftGroup>>, String> {
        let mut groups = self.groups.write().await;
        if groups.contains_key(&shard_id) {
            return Err(format!("shard {} already exists", shard_id.0));
        }

        let leader_state = Arc::new(AtomicBool::new(false));
        let leader_address = Arc::new(StdRwLock::new(String::new()));

        // Create apply channel pair before creating the group
        let (apply_tx, apply_rx) = mpsc::channel(1000);

        let group = RaftGroup::new(
            shard_id,
            self.node_id,
            self.node_address.clone(),
            peers,
            &self.storage_path,
            leader_state.clone(),
            leader_address.clone(),
            self.message_tx.clone(),
            apply_tx,
        )?;

        // Save Arc clones of the status handles so get_shard_status can read
        // leader_state/applied_index/leader_address without acquiring the group RwLock.
        let status_arcs = ShardStatusArcs {
            leader_state,
            applied_index: group.applied_index_handle(),
            leader_address,
        };

        // Cache propose_tx and step_tx so we can use them without acquiring
        // the group RwLock (which is permanently held by run()).
        let propose_tx = group.get_propose_tx();
        let step_tx = group.get_step_tx();

        let group_ref = Arc::new(RwLock::new(group));
        let group_clone = group_ref.clone();

        tokio::spawn(async move {
            let mut group = group_clone.write().await;
            if let Err(e) = group.run().await {
                error!("RaftGroup {} run failed: {}", shard_id.0, e);
            }
        });

        groups.insert(shard_id, group_ref.clone());
        // Also record the status Arcs in the parallel map. The groups lock is
        // already held, so we use a separate write lock on shard_status_arcs.
        self.shard_status_arcs
            .write()
            .await
            .insert(shard_id, status_arcs);

        // Cache the senders and apply receiver
        self.shard_propose_txs
            .write()
            .await
            .insert(shard_id, propose_tx);
        self.shard_step_txs.write().await.insert(shard_id, step_tx);
        self.shard_apply_rxs
            .write()
            .await
            .insert(shard_id, apply_rx);

        Ok(group_ref)
    }

    pub async fn get_group(&self, shard_id: ShardId) -> Option<Arc<RwLock<RaftGroup>>> {
        self.groups.read().await.get(&shard_id).cloned()
    }

    pub async fn get_apply_rx(&self, shard_id: ShardId) -> Option<mpsc::Receiver<ApplyEntry>> {
        let mut rxs = self.shard_apply_rxs.write().await;
        rxs.remove(&shard_id)
    }

    pub async fn remove_group(&self, shard_id: ShardId) -> Result<(), String> {
        let mut groups = self.groups.write().await;
        let group = groups
            .remove(&shard_id)
            .ok_or_else(|| format!("shard {} not found", shard_id.0))?;

        // Drop the parallel status Arcs as well.
        self.shard_status_arcs.write().await.remove(&shard_id);
        self.shard_propose_txs.write().await.remove(&shard_id);
        self.shard_step_txs.write().await.remove(&shard_id);
        self.shard_apply_rxs.write().await.remove(&shard_id);

        let mut group = group.write().await;
        group.stop().await;

        Ok(())
    }

    pub async fn propose(&self, shard_id: ShardId, data: Vec<u8>) -> Result<u64, String> {
        let propose_tx = {
            let txs = self.shard_propose_txs.read().await;
            txs.get(&shard_id)
                .ok_or_else(|| format!("shard {} not found", shard_id.0))?
                .clone()
        };

        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        propose_tx
            .send(ProposeRequest {
                shard_id,
                data,
                response_tx,
            })
            .await
            .map_err(|e| format!("failed to send propose: {}", e))?;

        response_rx
            .await
            .map_err(|e| format!("propose response error: {}", e))?
    }

    pub async fn step(&self, shard_id: ShardId, msg: RaftMessage) -> Result<(), String> {
        let step_tx = {
            let txs = self.shard_step_txs.read().await;
            txs.get(&shard_id)
                .ok_or_else(|| format!("shard {} not found", shard_id.0))?
                .clone()
        };

        step_tx
            .send(msg)
            .await
            .map_err(|e| format!("failed to send step message: {}", e))?;

        Ok(())
    }

    pub async fn get_shard_leader(&self, shard_id: ShardId) -> Option<String> {
        let arcs = self.shard_status_arcs.read().await;
        let handle = arcs.get(&shard_id)?;
        let addr = handle.leader_address.read().unwrap().clone();
        if addr.is_empty() {
            None
        } else {
            Some(addr)
        }
    }

    pub async fn is_shard_leader(&self, shard_id: ShardId) -> bool {
        // Read from the parallel shard_status_arcs map so we never block on
        // the Raft event loop (which permanently holds the group write lock).
        if let Some(arcs) = self.shard_status_arcs.read().await.get(&shard_id) {
            arcs.leader_state.load(std::sync::atomic::Ordering::SeqCst)
        } else {
            false
        }
    }

    pub async fn get_shard_status(&self, shard_id: ShardId) -> Option<(bool, u64, u64, u64)> {
        // Read from the parallel shard_status_arcs map so we never block on
        // the Raft event loop (which permanently holds the group write lock).
        let arcs = self.shard_status_arcs.read().await;
        let handle = arcs.get(&shard_id)?;
        let is_leader = handle
            .leader_state
            .load(std::sync::atomic::Ordering::SeqCst);
        let applied = *handle.applied_index.read().unwrap();
        // term and commit_index are not exposed without the group lock; return
        // 0 for both (consistent with RaftGroup::get_status).
        Some((is_leader, 0, 0, applied))
    }

    /// Get the leader status and address for a given shard.
    /// Returns (is_leader, leader_address).
    pub async fn get_shard_leader_status(&self, shard_id: ShardId) -> Option<(bool, String)> {
        let arcs = self.shard_status_arcs.read().await;
        let handle = arcs.get(&shard_id)?;
        let is_leader = handle
            .leader_state
            .load(std::sync::atomic::Ordering::SeqCst);
        let leader_addr = handle.leader_address.read().unwrap().clone();
        Some((is_leader, leader_addr))
    }

    pub async fn list_shards(&self) -> Vec<ShardId> {
        self.groups.read().await.keys().cloned().collect()
    }

    pub async fn get_shard_count(&self) -> usize {
        self.groups.read().await.len()
    }

    pub fn get_message_tx(&self) -> tokio::sync::broadcast::Sender<OutgoingMessage> {
        self.message_tx.clone()
    }

    pub fn get_apply_tx(&self) -> mpsc::Sender<ApplyEntry> {
        self.apply_tx.clone()
    }

    pub async fn transfer_shard_leader(
        &self,
        shard_id: ShardId,
        target_id: u64,
    ) -> Result<(), String> {
        let group_arc = {
            let groups = self.groups.read().await;
            groups
                .get(&shard_id)
                .ok_or_else(|| format!("shard {} not found", shard_id.0))?
                .clone()
        };

        let mut group = group_arc.write().await;
        group.transfer_leader(target_id)
    }

    pub async fn broadcast_message(&self, msg: OutgoingMessage) {
        let _ = self.message_tx.send(msg);
    }
}
