use log::{info, warn};
use protobuf::Message;
use raft::eraftpb::{ConfState, Entry, HardState, Snapshot};
use raft::storage::{RaftState, Storage};
use rocksdb::{ColumnFamilyDescriptor, WriteBatch, DB};
use std::collections::VecDeque;
use std::sync::{Arc, RwLock};

const CF_RAFT_LOG: &str = "raft_log";
const CF_RAFT_STATE: &str = "raft_state";
const CF_SNAPSHOT: &str = "raft_snapshot";

pub trait RaftStorageExt: Storage + Send + Sync {
    fn append_entries(&mut self, ents: &[Entry]) -> Result<(), raft::Error>;
    fn set_hard_state(&mut self, hs: HardState);
    fn apply_snapshot_entry(&mut self, snapshot: Snapshot) -> Result<(), raft::Error>;
    fn compact_log_entries(&mut self, index: u64) -> Result<(), raft::Error>;
    fn load_from_db(&self) -> Result<(), String>;
    fn save_to_db(&self) -> Result<(), String>;
}

pub struct RocksDbRaftStorage {
    db: Arc<DB>,
    entries: RwLock<VecDeque<Entry>>,
    applied_index: RwLock<u64>,
    hard_state: RwLock<HardState>,
    conf_state: RwLock<ConfState>,
}

impl Clone for RocksDbRaftStorage {
    fn clone(&self) -> Self {
        RocksDbRaftStorage {
            db: self.db.clone(),
            entries: RwLock::new(self.entries.read().unwrap().clone()),
            applied_index: RwLock::new(*self.applied_index.read().unwrap()),
            hard_state: RwLock::new(self.hard_state.read().unwrap().clone()),
            conf_state: RwLock::new(self.conf_state.read().unwrap().clone()),
        }
    }
}

impl RocksDbRaftStorage {
    pub fn new(path: &str) -> Result<Self, String> {
        Self::new_with_peers(path, &[])
    }

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
            "RocksDbRaftStorage initialized at {} with peers {:?}",
            path, peers
        );
        Ok(storage)
    }

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

        info!(
            "RocksDbRaftStorage initialized at {} (single node mode)",
            path
        );
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

    fn load_state(&self) -> Result<(), String> {
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

        if let Ok(Some(data)) = self.db.get_cf(cf, b"conf_state") {
            if let Ok(cs) = <ConfState as Message>::parse_from_bytes(&data) {
                *self.conf_state.write().unwrap() = cs.clone();
                info!("Loaded conf state: voters={:?}", cs.voters);
            } else {
                warn!("Failed to parse conf_state; keeping default");
            }
        }

        if let Ok(Some(data)) = self.db.get_cf(cf, b"applied_index") {
            if let Ok(s) = String::from_utf8(data) {
                if let Ok(idx) = s.parse::<u64>() {
                    *self.applied_index.write().unwrap() = idx;
                }
            }
        }

        let log_cf = match self.db.cf_handle(CF_RAFT_LOG) {
            Some(cf) => cf,
            None => {
                warn!("raft_log column family not found; starting with empty log");
                return Ok(());
            }
        };
        let mut entries = self.entries.write().unwrap();

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
                "Skipped {} corrupt/unreadable raft log entries during load_state",
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

    pub fn append(&mut self, ents: &[Entry]) -> Result<(), raft::Error> {
        if ents.is_empty() {
            return Ok(());
        }

        let first = self.first_index().unwrap_or(1);
        if first > ents[0].index {
            return Err(raft::Error::Store(raft::StorageError::Compacted));
        }

        let mut entries = self.entries.write().unwrap();

        let diff = (ents[0].index - first) as usize;
        if diff <= entries.len() {
            entries.truncate(diff);
        }

        let log_cf = self.db.cf_handle(CF_RAFT_LOG).ok_or_else(|| {
            raft::Error::Store(raft::StorageError::Other("column family not found".into()))
        })?;

        for entry in ents {
            let key = entry.index.to_string();
            let mut buf = Vec::new();
            if let Ok(()) = entry.write_to_vec(&mut buf) {
                let _ = self.db.put_cf(log_cf, key.as_bytes(), &buf);
            }
            entries.push_back(entry.clone());
        }

        Ok(())
    }

    pub fn set_hardstate(&mut self, hs: HardState) {
        *self.hard_state.write().unwrap() = hs.clone();

        if let Some(cf) = self.db.cf_handle(CF_RAFT_STATE) {
            let mut buf = Vec::new();
            if let Ok(()) = hs.write_to_vec(&mut buf) {
                let _ = self.db.put_cf(cf, b"hard_state", &buf);
            }
        }
    }

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

    pub fn get_snapshot_data(&self) -> Option<Vec<u8>> {
        if let Some(cf) = self.db.cf_handle(CF_SNAPSHOT) {
            if let Ok(Some(data)) = self.db.get_cf(cf, b"latest_snapshot") {
                if let Ok(snapshot) = <Snapshot as Message>::parse_from_bytes(&data) {
                    return Some(snapshot.get_data().to_vec());
                }
            }
        }
        None
    }

    pub fn create_snapshot(
        &mut self,
        index: u64,
        term: u64,
        data: Vec<u8>,
    ) -> Result<Snapshot, raft::Error> {
        let cs = self.conf_state.read().unwrap().clone();

        let mut snapshot = Snapshot::new();
        let meta = snapshot.mut_metadata();
        meta.set_index(index);
        meta.set_term(term);
        meta.set_conf_state(cs);

        snapshot.set_data(data.into());

        if let Some(cf) = self.db.cf_handle(CF_SNAPSHOT) {
            let mut buf = Vec::new();
            if snapshot.write_to_vec(&mut buf).is_ok() {
                let _ = self.db.put_cf(cf, b"latest_snapshot", &buf);
            }
        }

        Ok(snapshot)
    }
}

impl Storage for RocksDbRaftStorage {
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

        // When the log is empty, the only valid term is the committed term
        // for the committed index
        if idx <= hs.commit {
            return Ok(hs.term);
        }

        let entries = self.entries.read().unwrap();
        for entry in entries.iter() {
            if entry.index == idx {
                return Ok(entry.term);
            }
        }
        drop(entries);

        // Check snapshot for term (when log is compacted)
        let snapshot_cf = self.db.cf_handle(CF_SNAPSHOT);
        if let Some(cf) = snapshot_cf {
            if let Ok(Some(data)) = self.db.get_cf(cf, b"latest_snapshot") {
                if let Ok(snap) = <Snapshot as Message>::parse_from_bytes(&data) {
                    let meta = snap.get_metadata();
                    if idx <= meta.index {
                        return Ok(meta.term);
                    }
                }
            }
        }

        Err(raft::Error::Store(raft::StorageError::Unavailable))
    }

    fn first_index(&self) -> Result<u64, raft::Error> {
        let entries = self.entries.read().unwrap();
        if let Some(entry) = entries.front() {
            return Ok(entry.index);
        }
        drop(entries);

        // If no entries, check snapshot
        let snapshot_cf = self.db.cf_handle(CF_SNAPSHOT);
        if let Some(cf) = snapshot_cf {
            if let Ok(Some(data)) = self.db.get_cf(cf, b"latest_snapshot") {
                if let Ok(snap) = <Snapshot as Message>::parse_from_bytes(&data) {
                    return Ok(snap.get_metadata().index + 1);
                }
            }
        }

        // No entries, no snapshot - return hs.commit + 1 or 1
        let hs = self.hard_state.read().unwrap();
        if hs.commit > 0 {
            Ok(hs.commit + 1)
        } else {
            Ok(1)
        }
    }

    fn last_index(&self) -> Result<u64, raft::Error> {
        let entries = self.entries.read().unwrap();
        if let Some(entry) = entries.back() {
            return Ok(entry.index);
        }
        drop(entries);

        // If no entries, check snapshot for last index
        let snapshot_cf = self.db.cf_handle(CF_SNAPSHOT);
        if let Some(cf) = snapshot_cf {
            if let Ok(Some(data)) = self.db.get_cf(cf, b"latest_snapshot") {
                if let Ok(snap) = <Snapshot as Message>::parse_from_bytes(&data) {
                    return Ok(snap.get_metadata().index);
                }
            }
        }

        // No entries, no snapshot - return hard state commit
        let hs = self.hard_state.read().unwrap();
        Ok(hs.commit)
    }

    fn snapshot(
        &self,
        request_index: u64,
        _request_from_log_id: u64,
    ) -> Result<Snapshot, raft::Error> {
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

impl RaftStorageExt for RocksDbRaftStorage {
    fn append_entries(&mut self, ents: &[Entry]) -> Result<(), raft::Error> {
        self.append(ents)
    }

    fn set_hard_state(&mut self, hs: HardState) {
        self.set_hardstate(hs)
    }

    fn apply_snapshot_entry(&mut self, snapshot: Snapshot) -> Result<(), raft::Error> {
        self.apply_snapshot(snapshot)
    }

    fn compact_log_entries(&mut self, index: u64) -> Result<(), raft::Error> {
        self.compact_log(index)
    }

    fn load_from_db(&self) -> Result<(), String> {
        self.load_state()
    }

    fn save_to_db(&self) -> Result<(), String> {
        self.save_state()
    }
}
