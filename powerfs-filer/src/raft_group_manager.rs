use bytes;
use log::{error, info, warn};
use powerfs_common::raft::RocksDbRaftStorage;
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
    pub address: String,
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
        volume_id: u32,
        etag: String,
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
    pub fn new(
        shard_id: ShardId,
        id: u64,
        address: String,
        peers: Vec<Peer>,
        storage_path: &str,
        leader_state: Arc<AtomicBool>,
        leader_address: Arc<StdRwLock<String>>,
    ) -> Result<Self, String> {
        let storage_path = format!("{}/shard_{}", storage_path, shard_id.0);

        let storage = if peers.is_empty() {
            RocksDbRaftStorage::new_with_single_node(&storage_path, id)
                .map_err(|e| format!("failed to create storage: {}", e))?
        } else {
            let mut peer_ids = vec![id];
            for peer in &peers {
                peer_ids.push(peer.id);
            }
            RocksDbRaftStorage::new_with_peers(&storage_path, &peer_ids)
                .map_err(|e| format!("failed to create storage: {}", e))?
        };

        let _initial_state = storage
            .initial_state()
            .map_err(|e| format!("failed to get initial state: {}", e))?;

        let mut cfg = Config {
            id,
            election_tick: 10,
            heartbeat_tick: 3,
            max_size_per_msg: 1 << 20,
            max_inflight_msgs: 256,
            check_quorum: !peers.is_empty(),
            pre_vote: false,
            ..Default::default()
        };
        cfg.validate()
            .map_err(|e| format!("invalid raft config: {}", e))?;

        if let Ok(last_idx) = storage.last_index() {
            cfg.applied = last_idx;
        }

        let logger = Logger::root(Discard, slog::o!());

        let node = RawNode::new(&cfg, storage.clone(), &logger)
            .map_err(|e| format!("failed to create raft node: {}", e))?;

        let (propose_tx, propose_rx) = mpsc::channel(1000);
        let (message_tx, _message_rx) = tokio::sync::broadcast::channel(1000);
        let (step_tx, step_rx) = mpsc::channel(1000);
        let (apply_tx, apply_rx) = mpsc::channel(1000);

        let mut peer_map = HashMap::new();
        for peer in &peers {
            peer_map.insert(peer.id, peer.clone());
        }

        let is_single_node = peers.is_empty();
        leader_state.store(is_single_node, Ordering::Relaxed);

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
            _apply_rx: apply_rx,
            running: Arc::new(RwLock::new(true)),
            applied_index: Arc::new(StdRwLock::new(0)),
            leader_state,
            leader_address,
        })
    }

    pub async fn run(&mut self) -> Result<(), String> {
        info!("Starting Raft event loop for shard {}", self.shard_id.0);

        let mut tick_interval = interval(Duration::from_millis(100));

        while *self.running.read().await {
            tokio::select! {
                _ = tick_interval.tick() => {
                    self.node.tick();
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
                        let tx = self.apply_tx.clone();
                        tokio::spawn(async move {
                            let _ = tx.send(apply_entry).await;
                        });
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
                if !entry.data.is_empty() {
                    match ShardCommand::deserialize(&entry.data) {
                        Ok(cmd) => {
                            let apply_entry = ApplyEntry {
                                shard_id: self.shard_id,
                                index: entry.index,
                                command: cmd,
                            };
                            let tx = self.apply_tx.clone();
                            tokio::spawn(async move {
                                let _ = tx.send(apply_entry).await;
                            });
                        }
                        Err(e) => {
                            error!(
                                "Shard {} failed to deserialize command at index {}: {}",
                                self.shard_id.0, entry.index, e
                            );
                        }
                    }
                }
                let mut applied = self.applied_index.write().unwrap();
                *applied = entry.index;
            }
        }

        self.node.advance_apply();

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

        if self.message_tx.send(outgoing).is_err() {
            warn!(
                "Shard {} failed to send message to {}",
                self.shard_id.0, to_id
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
        self.process_ready();
    }

    pub fn is_leader(&self) -> bool {
        self.node.raft.state == StateRole::Leader
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

    pub fn take_apply_rx(&mut self) -> mpsc::Receiver<ApplyEntry> {
        let (tx, rx) = mpsc::channel(1000);
        let old_rx = std::mem::replace(&mut self._apply_rx, rx);
        self.apply_tx = tx;
        old_rx
    }

    pub fn get_apply_tx(&self) -> mpsc::Sender<ApplyEntry> {
        self.apply_tx.clone()
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
}

pub struct RaftGroupManager {
    groups: RwLock<HashMap<ShardId, Arc<RwLock<RaftGroup>>>>,
    // Per-shard Arc handles for status queries. Filled in create_group so
    // that get_shard_status can read leader_state/applied_index without
    // acquiring the group RwLock (which is permanently held by run()).
    shard_status_arcs: RwLock<HashMap<ShardId, ShardStatusArcs>>,
    node_id: u64,
    node_address: String,
    storage_path: String,
    message_tx: tokio::sync::broadcast::Sender<OutgoingMessage>,
    apply_tx: mpsc::Sender<ApplyEntry>,
}

impl RaftGroupManager {
    pub fn new(node_id: u64, node_address: String, storage_path: String) -> Self {
        let (message_tx, _) = tokio::sync::broadcast::channel(1000);
        let (apply_tx, _) = mpsc::channel(1000);

        Self {
            groups: RwLock::new(HashMap::new()),
            shard_status_arcs: RwLock::new(HashMap::new()),
            node_id,
            node_address,
            storage_path,
            message_tx,
            apply_tx,
        }
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

        let group = RaftGroup::new(
            shard_id,
            self.node_id,
            self.node_address.clone(),
            peers,
            &self.storage_path,
            leader_state.clone(),
            leader_address,
        )?;

        // Save Arc clones of the status handles so get_shard_status can read
        // leader_state/applied_index without acquiring the group RwLock.
        let status_arcs = ShardStatusArcs {
            leader_state,
            applied_index: group.applied_index_handle(),
        };

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
        Ok(group_ref)
    }

    pub async fn get_group(&self, shard_id: ShardId) -> Option<Arc<RwLock<RaftGroup>>> {
        self.groups.read().await.get(&shard_id).cloned()
    }

    pub async fn remove_group(&self, shard_id: ShardId) -> Result<(), String> {
        let mut groups = self.groups.write().await;
        let group = groups
            .remove(&shard_id)
            .ok_or_else(|| format!("shard {} not found", shard_id.0))?;

        // Drop the parallel status Arcs as well.
        self.shard_status_arcs.write().await.remove(&shard_id);

        let mut group = group.write().await;
        group.stop().await;

        Ok(())
    }

    pub async fn propose(&self, shard_id: ShardId, data: Vec<u8>) -> Result<u64, String> {
        // Clone the Arc<RwLock<RaftGroup>> and drop the groups read guard
        // before awaiting on the group's own read lock, so the Future stays Send.
        let group_arc = {
            let groups = self.groups.read().await;
            groups
                .get(&shard_id)
                .ok_or_else(|| format!("shard {} not found", shard_id.0))?
                .clone()
        };

        let propose_tx = {
            let group_guard = group_arc.read().await;
            group_guard.get_propose_tx()
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
        let group_arc = {
            let groups = self.groups.read().await;
            groups
                .get(&shard_id)
                .ok_or_else(|| format!("shard {} not found", shard_id.0))?
                .clone()
        };

        let step_tx = {
            let group_guard = group_arc.read().await;
            group_guard.get_step_tx()
        };

        step_tx
            .send(msg)
            .await
            .map_err(|e| format!("failed to send step message: {}", e))?;

        Ok(())
    }

    pub async fn get_shard_leader(&self, shard_id: ShardId) -> Option<String> {
        let groups = self.groups.read().await;
        let group = groups.get(&shard_id)?;
        let group = group.read().await;
        Some(group.leader_address())
    }

    pub async fn is_shard_leader(&self, shard_id: ShardId) -> bool {
        let groups = self.groups.read().await;
        if let Some(group) = groups.get(&shard_id) {
            group.read().await.is_leader()
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
