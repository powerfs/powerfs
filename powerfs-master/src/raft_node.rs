//! Raft consensus implementation for PowerFS Master
//!
//! This module provides the RaftNode structure for managing distributed consensus
//! using the tikv/raft-rs library.

use crate::raft_storage::{RaftCommand, RaftSnapshotData, RocksDbStorage};
use log::{error, info, warn};
use protobuf::Message;
use raft::eraftpb::{ConfChange, ConfChangeType, Message as RaftMessage};
use raft::storage::Storage;
use raft::{Config, RawNode, StateRole};
use slog::{Discard, Logger};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock as StdRwLock};
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tokio::time::interval;

const SNAPSHOT_THRESHOLD: u64 = 10000;

/// Peer information for cluster communication
#[derive(Debug, Clone)]
pub struct Peer {
    pub id: u64,
    pub address: String,
}

/// RaftNode manages the Raft state machine for a single master node
pub struct RaftNode {
    /// The underlying Raft RawNode
    pub node: RawNode<RocksDbStorage>,
    /// This node's ID
    id: u64,
    /// This node's address
    address: String,
    /// All peers in the cluster (including self)
    peers: HashMap<u64, Peer>,
    /// Channel to send proposed commands
    propose_tx: mpsc::Sender<ProposeRequest>,
    /// Receiver for propose requests
    propose_rx: mpsc::Receiver<ProposeRequest>,
    /// Broadcast channel for sending raft messages to peers
    message_tx: tokio::sync::broadcast::Sender<OutgoingMessage>,
    /// Receiver for outgoing messages (unused, broadcast subscribers are created via subscribe())
    #[allow(dead_code)]
    message_rx: tokio::sync::broadcast::Receiver<OutgoingMessage>,
    /// Channel for receiving incoming Raft messages from peers
    step_tx: mpsc::Sender<RaftMessage>,
    /// Receiver for incoming Raft messages
    step_rx: mpsc::Receiver<RaftMessage>,
    /// Sender for committed entries to apply
    apply_tx: mpsc::Sender<ApplyEntry>,
    /// Receiver for committed entries to apply
    _apply_rx: mpsc::Receiver<ApplyEntry>,
    /// Whether this node is running
    running: Arc<RwLock<bool>>,
    /// Applied index tracker (std::sync::RwLock for blocking read)
    applied_index: Arc<StdRwLock<u64>>,
    /// Shared leader state with Master (updated on raft role change)
    leader_state: Arc<AtomicBool>,
    /// Shared leader address (updated on raft role change)
    leader_address: Arc<StdRwLock<String>>,
}

/// Outgoing Raft message to a peer
#[derive(Debug, Clone)]
pub struct OutgoingMessage {
    pub to_id: u64,
    pub message: bytes::Bytes,
}

/// Request to propose a command
#[derive(Debug)]
pub struct ProposeRequest {
    pub data: Vec<u8>,
    pub response_tx: tokio::sync::oneshot::Sender<Result<u64, String>>,
}

/// Committed entry ready to apply to state machine
#[derive(Debug, Clone)]
pub struct ApplyEntry {
    pub index: u64,
    pub command: RaftCommand,
}

impl RaftNode {
    /// Create a new RaftNode with the given configuration
    pub fn new(
        id: u64,
        address: String,
        peers: Vec<Peer>,
        storage_path: &str,
        leader_state: Arc<AtomicBool>,
        leader_address: Arc<StdRwLock<String>>,
    ) -> Result<Self, String> {
        Self::new_with_config(
            id,
            address,
            peers,
            storage_path,
            10,
            3,
            leader_state,
            leader_address,
        )
    }

    /// Create a new RaftNode with custom election/heartbeat ticks
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_config(
        id: u64,
        address: String,
        peers: Vec<Peer>,
        storage_path: &str,
        election_tick: usize,
        heartbeat_tick: usize,
        leader_state: Arc<AtomicBool>,
        leader_address: Arc<StdRwLock<String>>,
    ) -> Result<Self, String> {
        let storage = if peers.is_empty() {
            RocksDbStorage::new_with_single_node(storage_path, id)
                .map_err(|e| format!("failed to create storage: {}", e))?
        } else {
            let mut peer_ids = vec![id];
            for peer in &peers {
                peer_ids.push(peer.id);
            }
            RocksDbStorage::new_with_peers(storage_path, &peer_ids)
                .map_err(|e| format!("failed to create storage: {}", e))?
        };

        let _initial_state = storage
            .initial_state()
            .map_err(|e| format!("failed to get initial state: {}", e))?;

        let mut cfg = Config {
            id,
            election_tick,
            heartbeat_tick,
            max_size_per_msg: 1 << 20, // 1MB
            max_inflight_msgs: 256,
            check_quorum: !peers.is_empty(),
            pre_vote: false,
            ..Default::default()
        };
        cfg.validate()
            .map_err(|e| format!("invalid raft config: {}", e))?;

        // Set applied to last index from storage
        if let Ok(last_idx) = storage.last_index() {
            cfg.applied = last_idx;
        }

        let logger = Logger::root(Discard, slog::o!());

        let node = RawNode::new(&cfg, storage.clone(), &logger)
            .map_err(|e| format!("failed to create raft node: {}", e))?;

        let (propose_tx, propose_rx) = mpsc::channel(1000);
        let (message_tx, message_rx) = tokio::sync::broadcast::channel(1000);
        let (step_tx, step_rx) = mpsc::channel(1000);
        let (apply_tx, apply_rx) = mpsc::channel(1000);

        let mut peer_map = HashMap::new();
        for peer in &peers {
            peer_map.insert(peer.id, peer.clone());
        }

        // 单节点模式（无 peers）立即成为 leader；多节点模式初始为 follower
        let is_single_node = peers.is_empty();
        leader_state.store(is_single_node, Ordering::Relaxed);

        info!(
            "Created RaftNode: id={}, address={}, peers={:?}, single_node={}",
            id,
            address,
            peers.iter().map(|p| p.id).collect::<Vec<_>>(),
            is_single_node
        );

        Ok(Self {
            node,
            id,
            address,
            peers: peer_map,
            propose_tx,
            propose_rx,
            message_tx,
            message_rx,
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

    /// Start the Raft event loop
    pub async fn run(&mut self) -> Result<(), String> {
        info!("Starting Raft event loop for node {}", self.id);

        let mut tick_interval = interval(Duration::from_millis(100));

        while *self.running.read().await {
            tokio::select! {
                _ = tick_interval.tick() => {
                    self.node.tick();
                    let mut ready_count = 0;
                    while self.node.has_ready() {
                        ready_count += 1;
                        self.process_ready();
                    }
                    if ready_count > 1 {
                        info!("RAFT_DEBUG: tick triggered {} process_ready calls", ready_count);
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

        info!("Raft event loop stopped for node {}", self.id);
        Ok(())
    }

    /// Process ready state from Raft
    pub fn process_ready(&mut self) {
        static PROCESS_READY_COUNT: AtomicU64 = AtomicU64::new(0);
        let count = PROCESS_READY_COUNT.fetch_add(1, Ordering::Relaxed);

        if !self.node.has_ready() {
            return;
        }

        let mut ready = self.node.ready();

        let msg_count = ready.messages().len();
        let committed_entries = ready.committed_entries().len();
        let has_ss = ready.ss().is_some();
        let has_hs = ready.hs().is_some();
        let has_snapshot = !ready.snapshot().is_empty();

        info!(
            "RAFT_DEBUG: process_ready #{} - msgs={}, committed={}, has_ss={}, has_hs={}, has_snapshot={}",
            count, msg_count, committed_entries, has_ss, has_hs, has_snapshot
        );

        let mut messages_to_send = Vec::new();

        if !ready.messages().is_empty() {
            info!(
                "RAFT_DEBUG: collecting {} messages from ready.messages()",
                ready.messages().len()
            );
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
                    "Raft role changed: node {} is now {:?} (was {})",
                    self.id,
                    ss.raft_state,
                    if prev { "Leader" } else { "Non-Leader" }
                );
            }
        }

        // Handle snapshot
        if !ready.snapshot().is_empty() {
            let snap = ready.snapshot().clone();
            if let Err(e) = self.node.mut_store().apply_snapshot(snap) {
                error!("Failed to apply snapshot: {}", e);
            }
        }

        // Append entries to storage
        if !ready.entries().is_empty() {
            if let Err(e) = self.node.mut_store().append(ready.entries()) {
                error!("Failed to append entries: {}", e);
            }
        }

        // Persist hard state
        if let Some(hs) = ready.hs() {
            self.node.mut_store().set_hardstate(hs.clone());
        }

        // Apply committed entries to state machine
        let committed = ready.take_committed_entries();
        if !committed.is_empty() {
            for entry in committed {
                if entry.data.is_empty() {
                    let mut applied = self.applied_index.write().unwrap();
                    *applied = entry.index;
                    continue;
                }

                match RaftCommand::deserialize(&entry.data) {
                    Ok(cmd) => {
                        let apply_entry = ApplyEntry {
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
                            "Failed to deserialize command at index {}: {}",
                            entry.index, e
                        );
                    }
                }

                let mut applied = self.applied_index.write().unwrap();
                *applied = entry.index;
            }
        }

        // Handle persisted messages
        if !ready.persisted_messages().is_empty() {
            info!(
                "RAFT_DEBUG: collecting {} messages from ready.persisted_messages()",
                ready.persisted_messages().len()
            );
            messages_to_send.extend(ready.take_persisted_messages());
        }

        // Check if we need to create a snapshot (leader only)
        if self.is_leader() {
            self.try_create_snapshot();
        }

        // Advance the state machine
        let mut light_rd = self.node.advance(ready);

        // Handle messages from light_rd
        if !light_rd.messages().is_empty() {
            info!(
                "RAFT_DEBUG: collecting {} messages from light_rd.messages()",
                light_rd.messages().len()
            );
            messages_to_send.extend(light_rd.take_messages());
        }

        // Apply committed entries from light_rd
        let committed = light_rd.take_committed_entries();
        if !committed.is_empty() {
            for entry in committed {
                if !entry.data.is_empty() {
                    match RaftCommand::deserialize(&entry.data) {
                        Ok(cmd) => {
                            let apply_entry = ApplyEntry {
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
                                "Failed to deserialize command at index {}: {}",
                                entry.index, e
                            );
                        }
                    }
                }
                let mut applied = self.applied_index.write().unwrap();
                *applied = entry.index;
            }
        }

        // Advance apply index
        self.node.advance_apply();

        // Send all collected messages
        if !messages_to_send.is_empty() {
            info!("RAFT_DEBUG: sending {} messages", messages_to_send.len());
            for msg in messages_to_send {
                self.send_message(&msg);
            }
        }
    }

    /// Send a Raft message to a peer
    fn send_message(&self, msg: &RaftMessage) {
        let to_id = msg.to;

        // Don't send to self
        if to_id == self.id {
            info!("RAFT_DEBUG: skip sending message to self (id={})", self.id);
            return;
        }

        // Check if peer exists
        if !self.peers.contains_key(&to_id) {
            info!(
                "RAFT_DEBUG: peer {} not found in peers map (peers: {:?})",
                to_id,
                self.peers.keys()
            );
            return;
        }

        // Serialize the message using protobuf
        let mut buf = Vec::new();
        if let Err(e) = msg.write_to_vec(&mut buf) {
            error!("Failed to serialize message: {}", e);
            return;
        }

        let outgoing = OutgoingMessage {
            to_id,
            message: bytes::Bytes::from(buf),
        };

        static SEND_MSG_COUNT: AtomicU64 = AtomicU64::new(0);
        let count = SEND_MSG_COUNT.fetch_add(1, Ordering::Relaxed);
        info!(
            "RAFT_DEBUG: sending message #{} to peer {}: type={:?}, from={}",
            count, to_id, msg.msg_type, self.id
        );

        if self.message_tx.send(outgoing).is_err() {
            warn!("Failed to send message to {}", to_id);
        }
    }

    /// Handle a propose request
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

        // Process any ready state from the propose
        self.process_ready();

        // Get the index of the proposed entry
        let index = self.node.raft.raft_log.committed;
        let _ = req.response_tx.send(Ok(index));
    }

    /// Get a clone of the propose sender
    pub fn get_propose_tx(&self) -> mpsc::Sender<ProposeRequest> {
        self.propose_tx.clone()
    }

    /// Propose a command to be replicated via Raft
    pub async fn propose(&self, data: Vec<u8>) -> Result<u64, String> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        self.propose_tx
            .send(ProposeRequest { data, response_tx })
            .await
            .map_err(|e| format!("failed to send propose: {}", e))?;

        response_rx
            .await
            .map_err(|e| format!("propose response error: {}", e))?
    }

    /// Tick is called periodically to drive the Raft state machine
    pub fn tick(&mut self) {
        self.node.tick();
    }

    /// Get a clone of the peers
    pub fn get_peers(&self) -> Vec<Peer> {
        self.peers.values().cloned().collect()
    }

    /// Get the current leader address
    pub fn get_leader_address(&self) -> String {
        self.leader_address.read().unwrap().clone()
    }

    /// Get a clone of the message broadcast sender
    pub fn get_message_tx(&self) -> tokio::sync::broadcast::Sender<OutgoingMessage> {
        self.message_tx.clone()
    }

    /// Get a new receiver for outgoing messages (for testing)
    pub fn take_message_rx(&self) -> tokio::sync::broadcast::Receiver<OutgoingMessage> {
        self.message_tx.subscribe()
    }

    /// Take the apply receiver (only call once)
    pub fn take_apply_rx(&mut self) -> mpsc::Receiver<ApplyEntry> {
        let (tx, rx) = mpsc::channel(1000);
        let old_rx = std::mem::replace(&mut self._apply_rx, rx);
        self.apply_tx = tx;
        old_rx
    }

    /// Get a clone of the apply sender
    pub fn get_apply_tx(&self) -> mpsc::Sender<ApplyEntry> {
        self.apply_tx.clone()
    }

    /// Check if this node is the leader
    pub fn is_leader(&self) -> bool {
        self.node.raft.state == StateRole::Leader
    }

    /// Check if this node is a follower
    pub fn is_follower(&self) -> bool {
        self.node.raft.state == StateRole::Follower
    }

    /// Check if this node is a candidate
    pub fn is_candidate(&self) -> bool {
        self.node.raft.state == StateRole::Candidate
    }

    /// Get the current term
    pub fn term(&self) -> u64 {
        self.node.raft.term
    }

    /// Get the node id
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Get the node address
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Get the leader id (0 if no leader)
    pub fn leader_id(&self) -> u64 {
        self.node.raft.leader_id
    }

    /// Get the leader address (empty string if no leader)
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

    /// Get the current commit index
    pub fn commit_index(&self) -> u64 {
        self.node.raft.raft_log.committed
    }

    /// Get the last index in the log
    pub fn last_index(&self) -> u64 {
        self.node.raft.raft_log.last_index()
    }

    /// Get the current last applied index
    pub fn last_applied_index(&self) -> u64 {
        self.node.raft.raft_log.applied
    }

    /// Get the applied index from storage
    pub fn applied_index(&self) -> u64 {
        *self.applied_index.read().unwrap()
    }

    /// Get the peer address by id
    pub fn get_peer_address(&self, id: u64) -> Option<&str> {
        self.peers.get(&id).map(|p| p.address.as_str())
    }

    /// Add a new peer to the cluster
    pub fn add_peer(&mut self, peer: Peer) -> Result<(), String> {
        info!("Adding peer: id={}, address={}", peer.id, peer.address);

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

    /// Remove a peer from the cluster
    pub fn remove_peer(&mut self, peer_id: u64) -> Result<(), String> {
        info!("Removing peer: id={}", peer_id);

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

    /// Transfer leadership to another node
    pub fn transfer_leader(&mut self, target_id: u64) -> Result<(), String> {
        info!("Transferring leadership to node: {}", target_id);

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

        info!("Leadership transfer initiated to node: {}", target_id);
        Ok(())
    }

    /// Handle an incoming Raft message from another node (internal)
    fn handle_step(&mut self, msg: RaftMessage) {
        if let Err(e) = self.node.step(msg) {
            error!("step failed: {}", e);
        }
        self.process_ready();
    }

    /// Get a clone of the step sender for sending incoming messages
    pub fn get_step_tx(&self) -> mpsc::Sender<RaftMessage> {
        self.step_tx.clone()
    }

    /// Handle an incoming Raft message from another node (deprecated - use get_step_tx)
    pub fn step(&mut self, msg: RaftMessage) -> Result<(), String> {
        self.node
            .step(msg)
            .map_err(|e| format!("step failed: {}", e))?;
        self.process_ready();
        Ok(())
    }

    /// Trigger snapshot creation
    pub fn trigger_snapshot(&mut self, data: &RaftSnapshotData) -> Result<(), String> {
        if !self.is_leader() {
            return Err("only leader can create snapshot".to_string());
        }

        let index = self.commit_index();
        let term = self.term();

        info!("Creating snapshot at index {}, term {}", index, term);

        if let Err(e) = self.node.mut_store().create_snapshot(index, term, data) {
            return Err(format!("failed to create snapshot: {}", e));
        }

        if let Err(e) = self.node.mut_store().compact_log(index) {
            warn!("Failed to compact log: {}", e);
        }

        info!("Snapshot created successfully");
        Ok(())
    }

    /// Try to create a snapshot automatically if needed
    fn try_create_snapshot(&mut self) {
        let last_index = self.last_index();
        let last_applied = self.last_applied_index();

        // Check if we have enough uncompacted entries
        if last_index - last_applied >= SNAPSHOT_THRESHOLD {
            info!(
                "Log entries exceed threshold ({}), triggering automatic snapshot",
                SNAPSHOT_THRESHOLD
            );
            warn!("Automatic snapshot creation is disabled - snapshot data should be provided by the master state machine");
        }
    }

    /// Compact log entries up to the given index
    pub fn compact_log(&mut self, index: u64) -> Result<(), String> {
        info!("Compacting log entries up to index {}", index);

        if let Err(e) = self.node.mut_store().compact_log(index) {
            return Err(format!("failed to compact log: {}", e));
        }

        info!("Log compacted successfully up to index {}", index);
        Ok(())
    }

    /// Get snapshot data from storage
    pub fn get_snapshot_data(&self) -> Option<RaftSnapshotData> {
        self.node.store().get_snapshot_data()
    }

    /// Get cluster information
    pub fn get_cluster_info(&self) -> ClusterInfo {
        ClusterInfo {
            node_id: self.id,
            address: self.address.clone(),
            is_leader: self.is_leader(),
            term: self.term(),
            peers: self.peers.values().map(|p| p.address.clone()).collect(),
            commit_index: self.commit_index(),
            last_applied: self.last_applied_index(),
        }
    }

    /// Stop the Raft node
    pub async fn stop(&mut self) {
        *self.running.write().await = false;
        info!("RaftNode {} stopped", self.id);
    }
}

/// Cluster information
#[derive(Debug, Clone)]
pub struct ClusterInfo {
    pub node_id: u64,
    pub address: String,
    pub is_leader: bool,
    pub term: u64,
    pub peers: Vec<String>,
    pub commit_index: u64,
    pub last_applied: u64,
}
