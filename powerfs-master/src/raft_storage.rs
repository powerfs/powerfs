use log::info;
use powerfs_common::types::{VolumeId, VolumeInfo};
use rocksdb::{ColumnFamilyDescriptor, Options, DB};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};

const CF_LOG: &str = "raft_log";
const CF_STATE: &str = "raft_state";
const CF_META: &str = "raft_meta";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftSnapshotData {
    pub topology: HashMap<String, String>,
    pub volumes: HashMap<VolumeId, VolumeInfo>,
    pub next_volume_id: u32,
    pub max_file_key: u64,
}

pub struct MasterRaftStorage {
    db: Arc<DB>,
    entries: RwLock<VecDeque<Vec<u8>>>,
    applied_index: RwLock<u64>,
    last_index: RwLock<u64>,
    last_term: RwLock<u64>,
}

impl Clone for MasterRaftStorage {
    fn clone(&self) -> Self {
        MasterRaftStorage {
            db: self.db.clone(),
            entries: RwLock::new(self.entries.read().unwrap().clone()),
            applied_index: RwLock::new(*self.applied_index.read().unwrap()),
            last_index: RwLock::new(*self.last_index.read().unwrap()),
            last_term: RwLock::new(*self.last_term.read().unwrap()),
        }
    }
}

impl MasterRaftStorage {
    pub fn new(path: &str) -> Result<Self, String> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cf_descriptors = vec![
            ColumnFamilyDescriptor::new(CF_LOG, Options::default()),
            ColumnFamilyDescriptor::new(CF_STATE, Options::default()),
            ColumnFamilyDescriptor::new(CF_META, Options::default()),
        ];

        let db = DB::open_cf_descriptors(&opts, path, cf_descriptors)
            .map_err(|e| format!("failed to open rocksdb: {}", e))?;

        Ok(MasterRaftStorage {
            db: Arc::new(db),
            entries: RwLock::new(VecDeque::new()),
            applied_index: RwLock::new(0),
            last_index: RwLock::new(0),
            last_term: RwLock::new(0),
        })
    }

    pub fn append_entry(&self, entry_data: Vec<u8>) {
        let mut entries = self.entries.write().unwrap();
        entries.push_back(entry_data);
        let idx = entries.len() as u64;
        drop(entries);

        let mut last_index = self.last_index.write().unwrap();
        *last_index = idx;
    }

    pub fn set_applied_index(&self, index: u64) {
        *self.applied_index.write().unwrap() = index;
    }

    pub fn get_applied_index(&self) -> u64 {
        *self.applied_index.read().unwrap()
    }

    pub fn last_index(&self) -> u64 {
        *self.last_index.read().unwrap()
    }

    pub fn last_term(&self) -> u64 {
        *self.last_term.read().unwrap()
    }

    pub fn get_entry(&self, index: u64) -> Option<Vec<u8>> {
        let entries = self.entries.read().unwrap();
        if index == 0 || index > entries.len() as u64 {
            None
        } else {
            entries.get(index as usize - 1).cloned()
        }
    }

    pub fn entries(&self, low: u64, high: u64) -> Vec<Vec<u8>> {
        let entries = self.entries.read().unwrap();
        let start = if low == 0 { 0 } else { low as usize - 1 };
        let end = high as usize;

        if start >= entries.len() {
            return Vec::new();
        }

        let end = std::cmp::min(end, entries.len());
        entries.range(start..end).cloned().collect()
    }

    pub fn create_snapshot(
        &self,
        index: u64,
        _term: u64,
        topology: &HashMap<String, String>,
        volumes: &HashMap<VolumeId, VolumeInfo>,
        next_volume_id: u32,
        max_file_key: u64,
    ) -> Result<(), String> {
        let data = RaftSnapshotData {
            topology: topology.clone(),
            volumes: volumes.clone(),
            next_volume_id,
            max_file_key,
        };

        let data_bytes = serde_json::to_vec(&data)
            .map_err(|e| format!("failed to serialize snapshot: {}", e))?;

        let cf = self.db.cf_handle(CF_META).unwrap();
        let key = format!("snapshot_{}", index);
        self.db
            .put_cf(cf, key, &data_bytes)
            .map_err(|e| format!("Failed to write snapshot: {}", e))?;

        info!("Created snapshot at index {}", index);
        Ok(())
    }
}

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
        volumes: Vec<VolumeShortInfo>,
        ip: String,
        grpc_port: u32,
    },
    Heartbeat {
        node_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeShortInfo {
    pub volume_id: u32,
    pub size: u64,
    pub read_only: bool,
}

impl RaftCommand {
    pub fn serialize(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap()
    }

    pub fn deserialize(data: &[u8]) -> Result<Self, String> {
        serde_json::from_slice(data).map_err(|e| e.to_string())
    }
}
