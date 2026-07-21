//! Raft storage implementation using RocksDB
//!
//! This module provides RocksDbStorage which implements the raft::Storage trait
//! for persistent storage of Raft state and logs.

use log::{info, warn};
use protobuf::Message;
use raft::eraftpb::{ConfState, Entry, HardState, Snapshot};
use raft::storage::{RaftState, Storage};
use rocksdb::{ColumnFamilyDescriptor, WriteBatch, DB};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::{Arc, RwLock};

const CF_RAFT_LOG: &str = "raft_log";
const CF_RAFT_STATE: &str = "raft_state";
const CF_SNAPSHOT: &str = "raft_snapshot";

/// Raft commands that can be proposed to the cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
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
        volume_id: u32,
        collection: String,
        replica_count: u32,
        ttl: i32,
        disk_type: String,
        size: u64,
    },
    UpdateVolumeState {
        volume_id: u32,
        state: String,
    },
    UpdateNodeVolumes {
        node_id: String,
        volumes: Vec<RaftVolumeShortInfo>,
        ip: String,
        grpc_port: u32,
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
    DeleteVolume {
        volume_id: u32,
    },
}

/// Volume info for Raft serialization (serde-compatible)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RaftVolumeShortInfo {
    pub volume_id: u32,
    pub size: u64,
    pub read_only: bool,
    pub used: u64,
    pub collection: String,
}

impl From<&crate::proto::VolumeShortInfo> for RaftVolumeShortInfo {
    fn from(v: &crate::proto::VolumeShortInfo) -> Self {
        RaftVolumeShortInfo {
            volume_id: v.volume_id,
            size: v.size,
            read_only: v.read_only,
            used: v.used,
            collection: v.collection.clone(),
        }
    }
}

/// Snapshot data stored in RocksDB
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftSnapshotData {
    pub nodes: Vec<RaftNodeSnapshot>,
    pub volumes: Vec<RaftVolumeSnapshot>,
    pub next_volume_id: u32,
    pub max_file_key: u64,
}

/// Node snapshot for serialization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftNodeSnapshot {
    pub id: String,
    pub address: String,
    pub rack: String,
    pub data_center: String,
    pub http_port: u32,
    pub grpc_port: u32,
    pub public_url: String,
}

/// Volume snapshot for serialization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftVolumeSnapshot {
    pub volume_id: u32,
    pub node_id: String,
    pub collection: String,
    pub size: u64,
    pub used: u64,
    pub replica_count: u32,
    pub ttl: i32,
    pub disk_type: String,
    pub state: String,
}

/// RocksDB-based storage for Raft
pub struct RocksDbStorage {
    db: Arc<DB>,
    /// In-memory cache of log entries
    entries: RwLock<VecDeque<Entry>>,
    /// Last applied index
    applied_index: RwLock<u64>,
    /// Hard state cache
    hard_state: RwLock<HardState>,
    /// Conf state cache
    conf_state: RwLock<ConfState>,
}

impl Clone for RocksDbStorage {
    fn clone(&self) -> Self {
        RocksDbStorage {
            db: self.db.clone(),
            entries: RwLock::new(self.entries.read().unwrap().clone()),
            applied_index: RwLock::new(*self.applied_index.read().unwrap()),
            hard_state: RwLock::new(self.hard_state.read().unwrap().clone()),
            conf_state: RwLock::new(self.conf_state.read().unwrap().clone()),
        }
    }
}

impl RocksDbStorage {
    /// Create a new RocksDbStorage
    pub fn new(path: &str) -> Result<Self, String> {
        Self::new_with_peers(path, &[])
    }

    /// Create a new RocksDbStorage with initial peers
    pub fn new_with_peers(path: &str, peers: &[u64]) -> Result<Self, String> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cf_descriptors = vec![
            ColumnFamilyDescriptor::new(CF_RAFT_LOG, rocksdb::Options::default()),
            ColumnFamilyDescriptor::new(CF_RAFT_STATE, rocksdb::Options::default()),
            ColumnFamilyDescriptor::new(CF_SNAPSHOT, rocksdb::Options::default()),
        ];

        let db = DB::open_cf_descriptors(&opts, path, cf_descriptors)
            .map_err(|e| format!("failed to open rocksdb: {}", e))?;

        let mut conf_state = ConfState::default();
        if !peers.is_empty() {
            conf_state.voters.extend_from_slice(peers);
        }

        let hard_state = HardState {
            term: 1,
            commit: 0,
            vote: 0,
            ..Default::default()
        };

        let storage = Self {
            db: Arc::new(db),
            entries: RwLock::new(VecDeque::new()),
            applied_index: RwLock::new(0),
            hard_state: RwLock::new(hard_state),
            conf_state: RwLock::new(conf_state),
        };

        storage.load_state()?;

        info!(
            "RocksDbStorage initialized at {} with peers {:?}",
            path, peers
        );
        Ok(storage)
    }

    /// Create a new RocksDbStorage for single node mode
    pub fn new_with_single_node(path: &str, node_id: u64) -> Result<Self, String> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cf_descriptors = vec![
            ColumnFamilyDescriptor::new(CF_RAFT_LOG, rocksdb::Options::default()),
            ColumnFamilyDescriptor::new(CF_RAFT_STATE, rocksdb::Options::default()),
            ColumnFamilyDescriptor::new(CF_SNAPSHOT, rocksdb::Options::default()),
        ];

        let db = DB::open_cf_descriptors(&opts, path, cf_descriptors)
            .map_err(|e| format!("failed to open rocksdb: {}", e))?;

        let mut conf_state = ConfState::default();
        conf_state.voters.push(node_id);

        let hard_state = HardState {
            term: 1,
            commit: 0,
            vote: node_id,
            ..Default::default()
        };

        let storage = Self {
            db: Arc::new(db),
            entries: RwLock::new(VecDeque::new()),
            applied_index: RwLock::new(0),
            hard_state: RwLock::new(hard_state),
            conf_state: RwLock::new(conf_state),
        };

        storage.save_state()?;

        info!("RocksDbStorage initialized at {} (single node mode)", path);
        Ok(storage)
    }

    fn save_state(&self) -> Result<(), String> {
        if let Some(cf) = self.db.cf_handle(CF_RAFT_STATE) {
            let mut batch = WriteBatch::default();

            let hs = self.hard_state.read().unwrap();
            let mut buf = Vec::new();
            if hs.write_to_vec(&mut buf).is_ok() {
                batch.put_cf(cf, b"hard_state", &buf);
            }

            let cs = self.conf_state.read().unwrap();
            let mut buf = Vec::new();
            if cs.write_to_vec(&mut buf).is_ok() {
                batch.put_cf(cf, b"conf_state", &buf);
            }

            let applied = *self.applied_index.read().unwrap();
            batch.put_cf(cf, b"applied_index", applied.to_string().as_bytes());

            if let Err(e) = self.db.write(batch) {
                warn!("Failed to write state batch: {}", e);
            }
        }
        Ok(())
    }

    /// Load hard state and conf state from RocksDB.
    ///
    /// This is best-effort: corrupt or missing state is logged and skipped
    /// rather than propagated as an error, so a partially-damaged Raft DB can
    /// still boot (and then be repaired via the `Raft` CLI subcommand) instead
    /// of bricking the master on startup.
    fn load_state(&self) -> Result<(), String> {
        // Load hard state. If the state CF is missing (freshly created or
        // corrupted DB), fall back to in-memory defaults instead of aborting.
        let cf = match self.db.cf_handle(CF_RAFT_STATE) {
            Some(cf) => cf,
            None => {
                warn!("raft_state column family not found; using default hard/conf state");
                return Ok(());
            }
        };
        if let Ok(Some(data)) = self.db.get_cf(cf, b"hard_state") {
            if let Ok(hs) = <HardState as Message>::parse_from_bytes(&data) {
                *self.hard_state.write().unwrap() = hs.clone();
                info!("Loaded hard state: term={}, commit={}", hs.term, hs.commit);
            } else {
                warn!("Failed to parse hard_state; keeping default");
            }
        }

        // Load conf state
        if let Ok(Some(data)) = self.db.get_cf(cf, b"conf_state") {
            if let Ok(cs) = <ConfState as Message>::parse_from_bytes(&data) {
                *self.conf_state.write().unwrap() = cs.clone();
                info!("Loaded conf state: voters={:?}", cs.voters);
            } else {
                warn!("Failed to parse conf_state; keeping default");
            }
        }

        // Load applied index
        if let Ok(Some(data)) = self.db.get_cf(cf, b"applied_index") {
            if let Ok(s) = String::from_utf8(data) {
                if let Ok(idx) = s.parse::<u64>() {
                    *self.applied_index.write().unwrap() = idx;
                }
            }
        }

        // Load log entries. Missing CF → no entries to load; skip silently.
        let log_cf = match self.db.cf_handle(CF_RAFT_LOG) {
            Some(cf) => cf,
            None => {
                warn!("raft_log column family not found; starting with empty log");
                return Ok(());
            }
        };
        let mut entries = self.entries.write().unwrap();

        // Use iterator to load all entries. Skip any entry whose key/value
        // fails to parse (corrupt UTF-8 key, bad protobuf, or index mismatch)
        // instead of failing the whole load.
        let mut it = self.db.raw_iterator_cf(log_cf);
        it.seek_to_first();

        let mut skipped = 0u64;
        while it.valid() {
            if let (Some(key), Some(value)) = (it.key(), it.value()) {
                let parsed = <Entry as Message>::parse_from_bytes(value)
                    .ok()
                    .and_then(|entry| {
                        let key_str = String::from_utf8(key.to_vec()).ok()?;
                        let idx = key_str.parse::<u64>().ok()?;
                        if idx == entry.index {
                            Some(entry)
                        } else {
                            None
                        }
                    });
                match parsed {
                    Some(entry) => entries.push_back(entry),
                    None => skipped += 1,
                }
            }
            it.next();
        }

        if skipped > 0 {
            warn!(
                "Skipped {} corrupt/unreadable raft log entries during load_state \
                 (use `powerfs raft verify` to inspect, `powerfs raft repair` to clean)",
                skipped
            );
        }

        let last_log_index = entries.back().map_or(0, |e| e.index);

        let mut hs = self.hard_state.write().unwrap();
        if hs.commit > last_log_index {
            warn!(
                "Hard state commit {} exceeds last log index {}; clamping to {}",
                hs.commit, last_log_index, last_log_index
            );
            hs.commit = last_log_index;
        }

        let mut applied = self.applied_index.write().unwrap();
        let max_valid_applied = hs.commit.min(last_log_index);
        if *applied > max_valid_applied {
            warn!(
                "Applied index {} exceeds valid range [0, min(commit={}, last_log={})]; clamping to {}",
                *applied, hs.commit, last_log_index, max_valid_applied
            );
            *applied = max_valid_applied;
        }

        info!("Loaded {} log entries", entries.len());
        Ok(())
    }

    /// Append entries to the storage
    pub fn append(&mut self, ents: &[Entry]) -> Result<(), raft::Error> {
        if ents.is_empty() {
            return Ok(());
        }

        let first = self.first_index().unwrap_or(1);
        if first > ents[0].index {
            return Err(raft::Error::Store(raft::StorageError::Compacted));
        }

        let mut entries = self.entries.write().unwrap();

        // Remove overlapping entries
        let diff = (ents[0].index - first) as usize;
        if diff <= entries.len() {
            entries.truncate(diff);
        }

        // Append new entries and persist to RocksDB
        let log_cf = self.db.cf_handle(CF_RAFT_LOG).ok_or_else(|| {
            raft::Error::Store(raft::StorageError::Other("column family not found".into()))
        })?;

        for entry in ents {
            // Persist to RocksDB
            let key = entry.index.to_string();
            let mut buf = Vec::new();
            if let Ok(()) = entry.write_to_vec(&mut buf) {
                let _ = self.db.put_cf(log_cf, key.as_bytes(), &buf);
            }
            entries.push_back(entry.clone());
        }

        Ok(())
    }

    /// Set hard state
    pub fn set_hardstate(&mut self, hs: HardState) {
        *self.hard_state.write().unwrap() = hs.clone();

        // Persist to RocksDB
        if let Some(cf) = self.db.cf_handle(CF_RAFT_STATE) {
            let mut buf = Vec::new();
            if let Ok(()) = hs.write_to_vec(&mut buf) {
                let _ = self.db.put_cf(cf, b"hard_state", &buf);
            }
        }
    }

    /// Apply snapshot
    pub fn apply_snapshot(&mut self, snapshot: Snapshot) -> Result<(), raft::Error> {
        let meta = snapshot.get_metadata();
        let index = meta.index;

        let first = self.first_index().unwrap_or(1);
        if first > index {
            return Err(raft::Error::Store(raft::StorageError::SnapshotOutOfDate));
        }

        let mut entries = self.entries.write().unwrap();
        entries.clear();

        let mut hs = self.hard_state.write().unwrap();
        hs.term = meta.term;
        hs.commit = index;

        *self.conf_state.write().unwrap() = meta.get_conf_state().clone();

        if let Some(cf) = self.db.cf_handle(CF_SNAPSHOT) {
            let mut buf = Vec::new();
            if let Ok(()) = snapshot.write_to_vec(&mut buf) {
                let _ = self.db.put_cf(cf, b"latest_snapshot", &buf);
            }
        }

        Ok(())
    }

    /// Create a snapshot with the given data
    pub fn create_snapshot(
        &mut self,
        index: u64,
        term: u64,
        data: &RaftSnapshotData,
    ) -> Result<Snapshot, raft::Error> {
        let cs = self.conf_state.read().unwrap().clone();

        let data_bytes = serde_json::to_vec(data).map_err(|e| {
            raft::Error::Store(raft::StorageError::Other(
                format!("failed to serialize snapshot data: {}", e).into(),
            ))
        })?;

        let mut snapshot = Snapshot::new();
        let meta = snapshot.mut_metadata();
        meta.set_index(index);
        meta.set_term(term);
        meta.set_conf_state(cs);

        snapshot.set_data(data_bytes.into());

        if let Some(cf) = self.db.cf_handle(CF_SNAPSHOT) {
            let mut buf = Vec::new();
            if snapshot.write_to_vec(&mut buf).is_ok() {
                let _ = self.db.put_cf(cf, b"latest_snapshot", &buf);
            }
        }

        Ok(snapshot)
    }

    /// Compact log entries up to the given index
    pub fn compact_log(&mut self, index: u64) -> Result<(), raft::Error> {
        let mut entries = self.entries.write().unwrap();

        while let Some(entry) = entries.front() {
            if entry.index <= index {
                entries.pop_front();
            } else {
                break;
            }
        }

        let log_cf = self.db.cf_handle(CF_RAFT_LOG).ok_or_else(|| {
            raft::Error::Store(raft::StorageError::Other("column family not found".into()))
        })?;

        let mut it = self.db.raw_iterator_cf(log_cf);
        it.seek_to_first();

        while it.valid() {
            if let Some(key) = it.key() {
                if let Ok(key_str) = String::from_utf8(key.to_vec()) {
                    if let Ok(idx) = key_str.parse::<u64>() {
                        if idx <= index {
                            let _ = self.db.delete_cf(log_cf, key);
                        }
                    }
                }
            }
            it.next();
        }

        Ok(())
    }

    /// Get snapshot data from stored snapshot
    pub fn get_snapshot_data(&self) -> Option<RaftSnapshotData> {
        if let Some(cf) = self.db.cf_handle(CF_SNAPSHOT) {
            if let Ok(Some(data)) = self.db.get_cf(cf, b"latest_snapshot") {
                if let Ok(snapshot) = <Snapshot as Message>::parse_from_bytes(&data) {
                    if let Ok(data) = serde_json::from_slice(snapshot.get_data()) {
                        return Some(data);
                    }
                }
            }
        }
        None
    }

    // ───────────────────────────────────────────────────────────────────
    // Offline maintenance: verify / repair / reset.
    //
    // These associated functions open the Raft DB directly (bypassing
    // `new_with_peers`, which runs the best-effort `load_state` and would
    // mask corruption). They back the `powerfs raft` CLI subcommand for
    // recovering a master that cannot boot because of a damaged Raft log.
    // ───────────────────────────────────────────────────────────────────

    /// Open an existing Raft DB without creating it. Missing column families
    /// are created empty so a partially-initialized DB can still be inspected.
    fn open_existing(path: &str) -> Result<DB, String> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(false);
        opts.create_missing_column_families(true);
        let cf_descriptors = vec![
            ColumnFamilyDescriptor::new(CF_RAFT_LOG, rocksdb::Options::default()),
            ColumnFamilyDescriptor::new(CF_RAFT_STATE, rocksdb::Options::default()),
            ColumnFamilyDescriptor::new(CF_SNAPSHOT, rocksdb::Options::default()),
        ];
        DB::open_cf_descriptors(&opts, path, cf_descriptors)
            .map_err(|e| format!("failed to open rocksdb at {}: {}", path, e))
    }

    /// Read-only integrity report. Does not modify the DB.
    pub fn verify(path: &str) -> RaftVerifyReport {
        let mut report = RaftVerifyReport {
            path: path.to_string(),
            exists: false,
            hard_state: None,
            conf_state_voters: Vec::new(),
            applied_index: None,
            total_log_entries: 0,
            valid_log_entries: 0,
            corrupt_log_entries: 0,
            corrupt_keys: Vec::new(),
            last_valid_index: None,
            snapshot_index: None,
            snapshot_term: None,
            ok: false,
            error: None,
        };

        let db = match Self::open_existing(path) {
            Ok(db) => db,
            Err(e) => {
                warn!("verify: {}", e);
                report.error = Some(e);
                return report;
            }
        };
        report.exists = true;

        if let Some(cf) = db.cf_handle(CF_RAFT_STATE) {
            if let Ok(Some(data)) = db.get_cf(cf, b"hard_state") {
                if let Ok(hs) = <HardState as Message>::parse_from_bytes(&data) {
                    report.hard_state = Some((hs.term, hs.vote, hs.commit));
                }
            }
            if let Ok(Some(data)) = db.get_cf(cf, b"conf_state") {
                if let Ok(cs) = <ConfState as Message>::parse_from_bytes(&data) {
                    report.conf_state_voters = cs.voters.clone();
                }
            }
            if let Ok(Some(data)) = db.get_cf(cf, b"applied_index") {
                if let Ok(s) = String::from_utf8(data) {
                    if let Ok(idx) = s.parse::<u64>() {
                        report.applied_index = Some(idx);
                    }
                }
            }
        }

        if let Some(cf) = db.cf_handle(CF_SNAPSHOT) {
            if let Ok(Some(data)) = db.get_cf(cf, b"latest_snapshot") {
                if let Ok(snap) = <Snapshot as Message>::parse_from_bytes(&data) {
                    report.snapshot_index = Some(snap.get_metadata().index);
                    report.snapshot_term = Some(snap.get_metadata().term);
                }
            }
        }

        if let Some(log_cf) = db.cf_handle(CF_RAFT_LOG) {
            let mut it = db.raw_iterator_cf(log_cf);
            it.seek_to_first();
            while it.valid() {
                if let (Some(key), Some(value)) = (it.key(), it.value()) {
                    report.total_log_entries += 1;
                    let parsed =
                        <Entry as Message>::parse_from_bytes(value)
                            .ok()
                            .and_then(|entry| {
                                let key_str = String::from_utf8(key.to_vec()).ok()?;
                                let idx = key_str.parse::<u64>().ok()?;
                                if idx == entry.index {
                                    Some(entry)
                                } else {
                                    None
                                }
                            });
                    match parsed {
                        Some(entry) => {
                            report.valid_log_entries += 1;
                            report.last_valid_index = Some(entry.index);
                        }
                        None => {
                            report.corrupt_log_entries += 1;
                            if report.corrupt_keys.len() < 50 {
                                let key_disp = String::from_utf8(key.to_vec())
                                    .unwrap_or_else(|_| format!("{:?}", key));
                                report.corrupt_keys.push(key_disp);
                            }
                        }
                    }
                }
                it.next();
            }
        }

        report.ok = report.corrupt_log_entries == 0 && report.hard_state.is_some();
        report
    }

    /// Delete corrupt/unreadable log entries and re-normalize `applied_index`
    /// so the master can boot. Returns the post-repair verify report.
    pub fn repair(path: &str) -> Result<RaftVerifyReport, String> {
        let db = Self::open_existing(path)?;
        let log_cf = db
            .cf_handle(CF_RAFT_LOG)
            .ok_or_else(|| "raft_log CF not found after open".to_string())?;

        // First pass: collect keys of corrupt entries (iterator borrows the
        // DB, so we cannot delete while iterating).
        let mut to_delete: Vec<Vec<u8>> = Vec::new();
        let mut last_valid: Option<u64> = None;
        {
            let mut it = db.raw_iterator_cf(log_cf);
            it.seek_to_first();
            while it.valid() {
                if let (Some(key), Some(value)) = (it.key(), it.value()) {
                    let valid =
                        <Entry as Message>::parse_from_bytes(value)
                            .ok()
                            .and_then(|entry| {
                                let key_str = String::from_utf8(key.to_vec()).ok()?;
                                let idx = key_str.parse::<u64>().ok()?;
                                if idx == entry.index {
                                    Some(idx)
                                } else {
                                    None
                                }
                            });
                    match valid {
                        Some(idx) => last_valid = Some(idx),
                        None => to_delete.push(key.to_vec()),
                    }
                }
                it.next();
            }
        }

        let removed = to_delete.len();
        for key in &to_delete {
            let _ = db.delete_cf(log_cf, key);
        }

        // Re-normalize applied_index: cap it at the last valid entry index so
        // the raft node does not try to apply entries that no longer exist.
        if let Some(cf) = db.cf_handle(CF_RAFT_STATE) {
            if let Ok(Some(data)) = db.get_cf(cf, b"applied_index") {
                if let Ok(s) = String::from_utf8(data) {
                    if let Ok(idx) = s.parse::<u64>() {
                        let capped = last_valid.map_or(0, |last| idx.min(last));
                        if capped != idx {
                            let _ = db.put_cf(cf, b"applied_index", capped.to_string().as_bytes());
                            info!(
                                "repair: capped applied_index {} -> {} (last valid entry)",
                                idx, capped
                            );
                        }
                    }
                }
            }
        }

        info!("repair: removed {} corrupt entries from {}", removed, path);
        drop(db);

        Ok(Self::verify(path))
    }

    /// Wipe the entire Raft directory and re-initialize it as a fresh
    /// single-node store. Use only when `repair` cannot recover the DB —
    /// all Raft log/snapshot state is lost. The meta DB is untouched.
    pub fn reset(path: &str, node_id: u64) -> Result<(), String> {
        let p = std::path::Path::new(path);
        if p.exists() {
            std::fs::remove_dir_all(p)
                .map_err(|e| format!("failed to remove raft dir {}: {}", path, e))?;
            warn!("reset: removed existing raft dir {}", path);
        }
        // Re-initialize as a fresh single-node store (writes initial state).
        let _ = Self::new_with_single_node(path, node_id)?;
        info!(
            "reset: re-initialized raft dir {} as single node {}",
            path, node_id
        );
        Ok(())
    }
}

/// Integrity report for a Raft DB, produced by [`RocksDbStorage::verify`] and
/// [`RocksDbStorage::repair`]. Serialized by the CLI for human-readable output.
#[derive(Debug, Clone, Serialize)]
pub struct RaftVerifyReport {
    pub path: String,
    /// Whether the DB directory could be opened at all.
    pub exists: bool,
    /// (term, vote, commit) of the persisted hard state, if parseable.
    pub hard_state: Option<(u64, u64, u64)>,
    pub conf_state_voters: Vec<u64>,
    pub applied_index: Option<u64>,
    pub total_log_entries: u64,
    pub valid_log_entries: u64,
    pub corrupt_log_entries: u64,
    /// Up to 50 keys of corrupt entries (for diagnostics).
    pub corrupt_keys: Vec<String>,
    pub last_valid_index: Option<u64>,
    pub snapshot_index: Option<u64>,
    pub snapshot_term: Option<u64>,
    /// True when no corruption was found and hard_state is readable.
    pub ok: bool,
    /// Open error message, if the DB could not be opened.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl Storage for RocksDbStorage {
    fn initial_state(&self) -> Result<RaftState, raft::Error> {
        Ok(RaftState {
            hard_state: self.hard_state.read().unwrap().clone(),
            conf_state: self.conf_state.read().unwrap().clone(),
        })
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _context: raft::storage::GetEntriesContext,
    ) -> Result<Vec<Entry>, raft::Error> {
        let max_size = max_size.into().unwrap_or(u64::MAX);
        let entries = self.entries.read().unwrap();
        let mut result = Vec::new();
        let mut size = 0u64;

        for entry in entries.iter() {
            if entry.index < low {
                continue;
            }
            if entry.index >= high {
                break;
            }
            // Calculate size by serializing
            let entry_size = {
                let mut buf = Vec::new();
                if entry.write_to_vec(&mut buf).is_ok() {
                    buf.len() as u64
                } else {
                    0
                }
            };
            size += entry_size;
            if size > max_size {
                break;
            }
            result.push(entry.clone());
        }

        Ok(result)
    }

    fn term(&self, idx: u64) -> Result<u64, raft::Error> {
        let hs = self.hard_state.read().unwrap();

        // Committed index has the term
        if idx == hs.commit {
            return Ok(hs.term);
        }

        let entries = self.entries.read().unwrap();
        for entry in entries.iter() {
            if entry.index == idx {
                return Ok(entry.term);
            }
        }

        Err(raft::Error::Store(raft::StorageError::Unavailable))
    }

    fn first_index(&self) -> Result<u64, raft::Error> {
        let entries = self.entries.read().unwrap();
        Ok(entries.front().map_or(1, |e| e.index))
    }

    fn last_index(&self) -> Result<u64, raft::Error> {
        let entries = self.entries.read().unwrap();
        let hs = self.hard_state.read().unwrap();
        Ok(entries.back().map_or(hs.commit, |e| e.index))
    }

    fn snapshot(
        &self,
        request_index: u64,
        _request_from_log_id: u64,
    ) -> Result<Snapshot, raft::Error> {
        // Try to load from RocksDB first
        let snapshot_cf = self.db.cf_handle(CF_SNAPSHOT);
        if let Some(cf) = snapshot_cf {
            if let Ok(Some(data)) = self.db.get_cf(cf, b"latest_snapshot") {
                if let Ok(snap) = <Snapshot as Message>::parse_from_bytes(&data) {
                    if snap.get_metadata().get_index() >= request_index {
                        return Ok(snap);
                    }
                }
            }
        }

        // Return empty snapshot
        let hs = self.hard_state.read().unwrap();
        let cs = self.conf_state.read().unwrap();
        let mut snapshot = Snapshot::new();
        let meta = snapshot.mut_metadata();
        meta.set_index(request_index);
        meta.set_term(hs.term);
        meta.set_conf_state(cs.clone());
        Ok(snapshot)
    }
}

impl RaftCommand {
    pub fn serialize(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    pub fn deserialize(data: &[u8]) -> Result<Self, String> {
        serde_json::from_slice(data).map_err(|e| e.to_string())
    }
}
