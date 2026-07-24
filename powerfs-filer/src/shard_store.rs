use log::info;
use rocksdb::{ColumnFamilyDescriptor, DB};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::RwLock;

use crate::crdt_orset::{ServerDirORSet, Tombstone};
use crate::raft_group_manager::{ShardCommand, ShardId};

const CF_INODES: &str = "inodes";
const CF_DIR_ENTRIES: &str = "dir_entries";
const CF_STATS: &str = "stats";
const CF_METADATA: &str = "metadata"; // For storing root_inodes and other persistent metadata
const CF_ORSET_STATE: &str = "orset_state"; // For storing CRDT OR-Set state
const CF_TOMBSTONES: &str = "tombstones"; // For storing CRDT tombstones

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InodeInfo {
    pub inode: u64,
    pub name: String,
    pub parent_inode: u64,
    pub file_type: FileType,
    pub size: u64,
    pub mtime: u64,
    pub atime: u64,
    pub ctime: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub blocks: u64,
    // S3 object metadata (populated for file-type inodes serving S3 objects)
    #[serde(default)]
    pub fid: Option<String>,
    #[serde(default)]
    pub volume_id: Option<u32>,
    #[serde(default)]
    pub etag: Option<String>,
    // File chunks for data layout (stored in Filer, not Master)
    #[serde(default)]
    pub chunks: Vec<StoredFileChunk>,
    // Extended attributes (e.g. file layout: stripe/flat)
    #[serde(default)]
    pub extended: HashMap<String, Vec<u8>>,
}

/// Stored file chunk (persisted in Filer InodeInfo)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StoredFileChunk {
    pub offset: u64,
    pub size: u64,
    pub mtime: u64,
    pub fid: String,
    pub cookie: u32,
    pub crc32: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FileType {
    File,
    Directory,
    Symlink,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardStats {
    pub inode_count: u64,
    pub file_count: u64,
    pub dir_count: u64,
    pub write_qps: u64,
    pub read_qps: u64,
}

pub struct ShardStore {
    shard_id: ShardId,
    inode_range: (u64, u64),
    db: DB,
    inodes: RwLock<HashMap<u64, InodeInfo>>,
    directory_entries: RwLock<HashMap<u64, HashMap<String, u64>>>,
    stats: RwLock<ShardStats>,
    root_inodes: RwLock<HashMap<String, u64>>, // Persistent bucket->root_inode mapping
}

impl ShardStore {
    pub fn new(shard_id: ShardId, inode_range: (u64, u64), db_path: &str) -> Result<Self, String> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        // RocksDB performance tuning for metadata workload
        opts.set_max_open_files(10000);
        opts.set_write_buffer_size(64 * 1024 * 1024); // 64MB write buffer
        opts.set_max_write_buffer_number(4);
        opts.set_min_write_buffer_number_to_merge(2);
        opts.set_level_zero_file_num_compaction_trigger(4);
        opts.set_level_zero_slowdown_writes_trigger(16);
        opts.set_level_zero_stop_writes_trigger(32);
        opts.set_target_file_size_base(64 * 1024 * 1024); // 64MB
        opts.set_max_bytes_for_level_base(256 * 1024 * 1024); // 256MB
        opts.enable_statistics();
        opts.set_stats_dump_period_sec(60);

        // Optimized CF options for different workloads
        let make_cf_opts = || {
            let mut cf_opts = rocksdb::Options::default();
            cf_opts.set_write_buffer_size(32 * 1024 * 1024); // 32MB per CF
            cf_opts.set_max_write_buffer_number(3);
            cf_opts.set_level_zero_file_num_compaction_trigger(4);
            cf_opts.set_target_file_size_base(32 * 1024 * 1024);
            cf_opts
        };

        let cf_descriptors = vec![
            ColumnFamilyDescriptor::new(CF_INODES, make_cf_opts()),
            ColumnFamilyDescriptor::new(CF_DIR_ENTRIES, make_cf_opts()),
            ColumnFamilyDescriptor::new(CF_STATS, make_cf_opts()),
            ColumnFamilyDescriptor::new(CF_METADATA, make_cf_opts()),
            ColumnFamilyDescriptor::new(CF_ORSET_STATE, make_cf_opts()),
            ColumnFamilyDescriptor::new(CF_TOMBSTONES, make_cf_opts()),
        ];

        let db = DB::open_cf_descriptors(&opts, db_path, cf_descriptors)
            .map_err(|e| format!("failed to open rocksdb: {}", e))?;

        let mut store = Self {
            shard_id,
            inode_range,
            db,
            inodes: RwLock::new(HashMap::new()),
            directory_entries: RwLock::new(HashMap::new()),
            stats: RwLock::new(ShardStats {
                inode_count: 0,
                file_count: 0,
                dir_count: 0,
                write_qps: 0,
                read_qps: 0,
            }),
            root_inodes: RwLock::new(HashMap::new()),
        };

        store.load_data()?;
        Ok(store)
    }

    fn load_data(&mut self) -> Result<(), String> {
        self.load_inodes()?;
        self.load_dir_entries()?;
        self.load_stats()?;
        self.load_root_inodes()?;
        info!("Shard {} loaded data from rocksdb", self.shard_id.0);
        Ok(())
    }

    fn load_root_inodes(&mut self) -> Result<(), String> {
        let cf = match self.db.cf_handle(CF_METADATA) {
            Some(cf) => cf,
            None => return Ok(()),
        };

        if let Ok(Some(data)) = self.db.get_cf(cf, b"root_inodes") {
            if let Ok(map) = serde_json::from_slice::<HashMap<String, u64>>(&data) {
                *self.root_inodes.write().unwrap() = map;
            }
        }

        Ok(())
    }

    pub fn save_root_inodes(&self) {
        if let Some(cf) = self.db.cf_handle(CF_METADATA) {
            let root_inodes = self.root_inodes.read().unwrap().clone();
            if let Ok(data) = serde_json::to_vec(&root_inodes) {
                let _ = self.db.put_cf(cf, b"root_inodes", &data);
            }
        }
    }

    pub fn get_root_inode(&self, bucket: &str) -> Option<u64> {
        let root_inodes = self.root_inodes.read().unwrap();
        root_inodes.get(bucket).cloned()
    }

    pub fn set_root_inode(&self, bucket: &str, inode: u64) {
        self.root_inodes
            .write()
            .unwrap()
            .insert(bucket.to_string(), inode);
        self.save_root_inodes();
    }

    pub fn list_root_inodes(&self) -> Vec<(String, u64)> {
        let root_inodes = self.root_inodes.read().unwrap();
        root_inodes.iter().map(|(k, v)| (k.clone(), *v)).collect()
    }

    // ========================================================================
    // CRDT OR-Set State Persistence
    // ========================================================================

    /// 保存 OR-Set 状态到 RocksDB
    pub fn save_orset_state(&self, dir_ino: u64, state: &ServerDirORSet) {
        if let Some(cf) = self.db.cf_handle(CF_ORSET_STATE) {
            if let Ok(data) = serde_json::to_vec(state) {
                let key = format!("dir_orset:{}", dir_ino);
                let _ = self.db.put_cf(cf, key.as_bytes(), &data);
            }
        }
    }

    /// 加载 OR-Set 状态从 RocksDB
    pub fn load_orset_state(&self, dir_ino: u64) -> Option<ServerDirORSet> {
        let cf = self.db.cf_handle(CF_ORSET_STATE)?;
        let key = format!("dir_orset:{}", dir_ino);
        if let Ok(Some(data)) = self.db.get_cf(cf, key.as_bytes()) {
            if let Ok(state) = serde_json::from_slice::<ServerDirORSet>(&data) {
                return Some(state);
            }
        }
        None
    }

    /// 加载所有 OR-Set 状态
    pub fn load_all_orset_states(&self) -> Vec<(u64, ServerDirORSet)> {
        let mut states = Vec::new();
        if let Some(cf) = self.db.cf_handle(CF_ORSET_STATE) {
            let mut it = self.db.raw_iterator_cf(cf);
            it.seek_to_first();
            while it.valid() {
                if let (Some(key), Some(value)) = (it.key(), it.value()) {
                    if let Ok(key_str) = std::str::from_utf8(key) {
                        if let Some(dir_ino_str) = key_str.strip_prefix("dir_orset:") {
                            if let Ok(dir_ino) = dir_ino_str.parse::<u64>() {
                                if let Ok(state) = serde_json::from_slice::<ServerDirORSet>(value) {
                                    states.push((dir_ino, state));
                                }
                            }
                        }
                    }
                }
                it.next();
            }
        }
        states
    }

    // ========================================================================
    // CRDT Tombstone Persistence
    // ========================================================================

    /// 保存 Tombstone 列表到 RocksDB
    pub fn save_tombstones(&self, entry_key: &str, tombstones: &[Tombstone]) {
        if let Some(cf) = self.db.cf_handle(CF_TOMBSTONES) {
            if let Ok(data) = serde_json::to_vec(tombstones) {
                let key = format!("tombstone:{}", entry_key);
                let _ = self.db.put_cf(cf, key.as_bytes(), &data);
            }
        }
    }

    /// 加载 Tombstone 列表从 RocksDB
    pub fn load_tombstones(&self, entry_key: &str) -> Vec<Tombstone> {
        if let Some(cf) = self.db.cf_handle(CF_TOMBSTONES) {
            let key = format!("tombstone:{}", entry_key);
            if let Ok(Some(data)) = self.db.get_cf(cf, key.as_bytes()) {
                if let Ok(list) = serde_json::from_slice::<Vec<Tombstone>>(&data) {
                    return list;
                }
            }
        }
        Vec::new()
    }

    /// 清理过期的 Tombstone
    pub fn cleanup_expired_tombstones(&self, ttl_hours: u64) -> usize {
        let ttl = std::time::Duration::from_secs(ttl_hours * 3600);
        let mut cleaned_count = 0;

        if let Some(cf) = self.db.cf_handle(CF_TOMBSTONES) {
            let mut keys_to_delete = Vec::new();
            let mut it = self.db.raw_iterator_cf(cf);
            it.seek_to_first();

            while it.valid() {
                if let (Some(key), Some(value)) = (it.key(), it.value()) {
                    if let Ok(list) = serde_json::from_slice::<Vec<Tombstone>>(value) {
                        let remaining: Vec<Tombstone> = list
                            .iter()
                            .filter(|t| !t.is_expired(ttl))
                            .cloned()
                            .collect();

                        if remaining.len() < list.len() {
                            if remaining.is_empty() {
                                keys_to_delete.push(key.to_vec());
                            } else if let Ok(new_data) = serde_json::to_vec(&remaining) {
                                let _ = self.db.put_cf(cf, key, &new_data);
                            }
                            cleaned_count += list.len() - remaining.len();
                        }
                    }
                }
                it.next();
            }

            // 删除空 tombstone 列表
            for key in keys_to_delete {
                let _ = self.db.delete_cf(cf, &key);
            }
        }

        cleaned_count
    }

    fn load_inodes(&mut self) -> Result<(), String> {
        let cf = match self.db.cf_handle(CF_INODES) {
            Some(cf) => cf,
            None => return Ok(()),
        };

        let mut it = self.db.raw_iterator_cf(cf);
        it.seek_to_first();

        let mut inodes = self.inodes.write().unwrap();
        while it.valid() {
            if let (Some(key), Some(value)) = (it.key(), it.value()) {
                let mut key_bytes = [0u8; 8];
                key_bytes.copy_from_slice(&key.to_vec()[..8.min(key.len())]);
                let inode = u64::from_be_bytes(key_bytes);
                if let Ok(info) = serde_json::from_slice::<InodeInfo>(value) {
                    inodes.insert(inode, info);
                }
            }
            it.next();
        }

        Ok(())
    }

    fn load_dir_entries(&mut self) -> Result<(), String> {
        let cf = match self.db.cf_handle(CF_DIR_ENTRIES) {
            Some(cf) => cf,
            None => return Ok(()),
        };

        let mut it = self.db.raw_iterator_cf(cf);
        it.seek_to_first();

        let mut dir_entries = self.directory_entries.write().unwrap();
        while it.valid() {
            if let (Some(key), Some(value)) = (it.key(), it.value()) {
                let key_str = String::from_utf8_lossy(key);
                let parts: Vec<&str> = key_str.split(':').collect();
                if parts.len() == 2 {
                    if let Ok(parent_inode) = parts[0].parse::<u64>() {
                        let name = parts[1].to_string();
                        let mut value_bytes = [0u8; 8];
                        value_bytes.copy_from_slice(&value.to_vec()[..8.min(value.len())]);
                        let inode = u64::from_be_bytes(value_bytes);
                        dir_entries.entry(parent_inode).or_default();
                        if let Some(dir) = dir_entries.get_mut(&parent_inode) {
                            dir.insert(name, inode);
                        }
                    }
                }
            }
            it.next();
        }

        Ok(())
    }

    fn load_stats(&mut self) -> Result<(), String> {
        let cf = match self.db.cf_handle(CF_STATS) {
            Some(cf) => cf,
            None => return Ok(()),
        };

        if let Ok(Some(data)) = self.db.get_cf(cf, b"stats") {
            if let Ok(stats) = serde_json::from_slice::<ShardStats>(&data) {
                *self.stats.write().unwrap() = stats;
            }
        }

        Ok(())
    }

    fn save_stats(&self) {
        if let Some(cf) = self.db.cf_handle(CF_STATS) {
            let stats = self.stats.read().unwrap().clone();
            if let Ok(data) = serde_json::to_vec(&stats) {
                let _ = self.db.put_cf(cf, b"stats", &data);
            }
        }
    }

    pub fn apply_command(&self, cmd: ShardCommand) {
        match cmd {
            ShardCommand::CreateFile {
                parent_inode,
                name,
                inode,
            } => {
                self.create_file(parent_inode, name, inode);
            }
            ShardCommand::UpdateFile { inode, size, mtime } => {
                self.update_file(inode, size, mtime);
            }
            ShardCommand::DeleteFile { parent_inode, name } => {
                self.delete_file(parent_inode, name);
            }
            ShardCommand::CreateDirectory {
                parent_inode,
                name,
                inode,
            } => {
                self.create_directory(parent_inode, name, inode);
            }
            ShardCommand::DeleteDirectory { parent_inode, name } => {
                self.delete_directory(parent_inode, name);
            }
            ShardCommand::Rename {
                old_parent_inode,
                old_name,
                new_parent_inode,
                new_name,
            } => {
                self.rename(old_parent_inode, old_name, new_parent_inode, new_name);
            }
            ShardCommand::PutObject {
                parent_inode,
                name,
                inode,
                size,
                fid,
                volume_id,
                etag,
            } => {
                self.put_object(parent_inode, name, inode, size, fid, volume_id, etag);
            }
        }
    }

    fn create_file(&self, parent_inode: u64, name: String, inode: u64) {
        let now = chrono::Utc::now().timestamp() as u64;

        let inode_info = InodeInfo {
            inode,
            name: name.clone(),
            parent_inode,
            file_type: FileType::File,
            size: 0,
            mtime: now,
            atime: now,
            ctime: now,
            mode: 0o644,
            uid: 0,
            gid: 0,
            blocks: 0,
            fid: None,
            volume_id: None,
            etag: None,
            chunks: vec![],
            extended: HashMap::new(),
        };

        let cf_inodes = self.db.cf_handle(CF_INODES).unwrap();
        let cf_dir_entries = self.db.cf_handle(CF_DIR_ENTRIES).unwrap();

        let inode_key = inode.to_be_bytes();
        if let Ok(data) = serde_json::to_vec(&inode_info) {
            let _ = self.db.put_cf(cf_inodes, inode_key, &data);
        }

        let dir_entry_key = format!("{}:{}", parent_inode, name);
        let inode_value = inode.to_be_bytes();
        let _ = self
            .db
            .put_cf(cf_dir_entries, dir_entry_key.as_bytes(), inode_value);

        {
            let mut inodes = self.inodes.write().unwrap();
            let mut dir_entries = self.directory_entries.write().unwrap();

            inodes.insert(inode, inode_info);

            dir_entries.entry(parent_inode).or_default();
            if let Some(dir) = dir_entries.get_mut(&parent_inode) {
                dir.insert(name, inode);
            }
        }
        {
            let mut stats = self.stats.write().unwrap();
            stats.inode_count += 1;
            stats.file_count += 1;
        }
        self.save_stats();

        info!(
            "Shard {} created file: inode={}, parent_inode={}",
            self.shard_id.0, inode, parent_inode
        );
    }

    /// Create an S3 object inode with data-location metadata (fid/volume_id/etag) in one step.
    #[allow(clippy::too_many_arguments)]
    fn put_object(
        &self,
        parent_inode: u64,
        name: String,
        inode: u64,
        size: u64,
        fid: String,
        volume_id: u32,
        etag: String,
    ) {
        let now = chrono::Utc::now().timestamp() as u64;

        let inode_info = InodeInfo {
            inode,
            name: name.clone(),
            parent_inode,
            file_type: FileType::File,
            size,
            mtime: now,
            atime: now,
            ctime: now,
            mode: 0o644,
            uid: 0,
            gid: 0,
            blocks: size.div_ceil(4096),
            fid: Some(fid),
            volume_id: Some(volume_id),
            etag: Some(etag),
            chunks: vec![],
            extended: HashMap::new(),
        };

        let cf_inodes = self.db.cf_handle(CF_INODES).unwrap();
        let cf_dir_entries = self.db.cf_handle(CF_DIR_ENTRIES).unwrap();

        let inode_key = inode.to_be_bytes();
        if let Ok(data) = serde_json::to_vec(&inode_info) {
            let _ = self.db.put_cf(cf_inodes, inode_key, &data);
        }

        let dir_entry_key = format!("{}:{}", parent_inode, name);
        let inode_value = inode.to_be_bytes();
        let _ = self
            .db
            .put_cf(cf_dir_entries, dir_entry_key.as_bytes(), inode_value);

        {
            let mut inodes = self.inodes.write().unwrap();
            let mut dir_entries = self.directory_entries.write().unwrap();

            inodes.insert(inode, inode_info);

            dir_entries.entry(parent_inode).or_default();
            if let Some(dir) = dir_entries.get_mut(&parent_inode) {
                dir.insert(name, inode);
            }
        }
        {
            let mut stats = self.stats.write().unwrap();
            stats.inode_count += 1;
            stats.file_count += 1;
        }
        self.save_stats();

        info!(
            "Shard {} put object: inode={}, parent_inode={}, size={}, volume_id={}",
            self.shard_id.0, inode, parent_inode, size, volume_id
        );
    }

    fn update_file(&self, inode: u64, size: u64, mtime: u64) {
        let cf_inodes = self.db.cf_handle(CF_INODES).unwrap();

        let mut inodes = self.inodes.write().unwrap();

        if let Some(info) = inodes.get_mut(&inode) {
            info.size = size;
            info.mtime = mtime;
            info.atime = chrono::Utc::now().timestamp() as u64;

            if let Ok(data) = serde_json::to_vec(info) {
                let inode_key = inode.to_be_bytes();
                let _ = self.db.put_cf(cf_inodes, inode_key, &data);
            }
        }

        info!(
            "Shard {} updated file: inode={}, size={}",
            self.shard_id.0, inode, size
        );
    }

    fn delete_file(&self, parent_inode: u64, name: String) {
        let cf_inodes = self.db.cf_handle(CF_INODES).unwrap();
        let cf_dir_entries = self.db.cf_handle(CF_DIR_ENTRIES).unwrap();

        let removed = {
            let mut inodes = self.inodes.write().unwrap();
            let mut dir_entries = self.directory_entries.write().unwrap();

            let mut removed = None;
            if let Some(dir) = dir_entries.get_mut(&parent_inode) {
                if let Some(&inode) = dir.get(&name) {
                    let dir_entry_key = format!("{}:{}", parent_inode, name);
                    let _ = self.db.delete_cf(cf_dir_entries, dir_entry_key.as_bytes());

                    let inode_key = inode.to_be_bytes();
                    let _ = self.db.delete_cf(cf_inodes, inode_key);

                    dir.remove(&name);
                    if let Some(info) = inodes.remove(&inode) {
                        let is_file = matches!(info.file_type, FileType::File);
                        removed = Some(is_file);
                    }
                }
            }
            removed
        };
        {
            let mut stats = self.stats.write().unwrap();
            if let Some(is_file) = removed {
                stats.inode_count -= 1;
                if is_file {
                    stats.file_count -= 1;
                }
            }
        }
        self.save_stats();
        info!(
            "Shard {} deleted file: parent_inode={}, name={}",
            self.shard_id.0, parent_inode, name
        );
    }

    fn create_directory(&self, parent_inode: u64, name: String, inode: u64) {
        let now = chrono::Utc::now().timestamp() as u64;

        let inode_info = InodeInfo {
            inode,
            name: name.clone(),
            parent_inode,
            file_type: FileType::Directory,
            size: 0,
            mtime: now,
            atime: now,
            ctime: now,
            mode: 0o755,
            uid: 0,
            gid: 0,
            blocks: 0,
            fid: None,
            volume_id: None,
            etag: None,
            chunks: vec![],
            extended: HashMap::new(),
        };

        let cf_inodes = self.db.cf_handle(CF_INODES).unwrap();
        let cf_dir_entries = self.db.cf_handle(CF_DIR_ENTRIES).unwrap();

        let inode_key = inode.to_be_bytes();
        if let Ok(data) = serde_json::to_vec(&inode_info) {
            let _ = self.db.put_cf(cf_inodes, inode_key, &data);
        }

        let dir_entry_key = format!("{}:{}", parent_inode, name);
        let inode_value = inode.to_be_bytes();
        let _ = self
            .db
            .put_cf(cf_dir_entries, dir_entry_key.as_bytes(), inode_value);

        {
            let mut inodes = self.inodes.write().unwrap();
            let mut dir_entries = self.directory_entries.write().unwrap();

            inodes.insert(inode, inode_info);

            dir_entries.entry(parent_inode).or_default();
            if let Some(dir) = dir_entries.get_mut(&parent_inode) {
                dir.insert(name, inode);
            }

            dir_entries.entry(inode).or_default();
        }
        {
            let mut stats = self.stats.write().unwrap();
            stats.inode_count += 1;
            stats.dir_count += 1;
        }
        self.save_stats();

        info!(
            "Shard {} created directory: inode={}, parent_inode={}",
            self.shard_id.0, inode, parent_inode
        );
    }

    fn delete_directory(&self, parent_inode: u64, name: String) {
        let cf_inodes = self.db.cf_handle(CF_INODES).unwrap();
        let cf_dir_entries = self.db.cf_handle(CF_DIR_ENTRIES).unwrap();

        let removed = {
            let mut inodes = self.inodes.write().unwrap();
            let mut dir_entries = self.directory_entries.write().unwrap();

            let mut removed = None;
            if let Some(dir) = dir_entries.get_mut(&parent_inode) {
                if let Some(&inode) = dir.get(&name) {
                    let dir_entry_key = format!("{}:{}", parent_inode, name);
                    let _ = self.db.delete_cf(cf_dir_entries, dir_entry_key.as_bytes());

                    let inode_key = inode.to_be_bytes();
                    let _ = self.db.delete_cf(cf_inodes, inode_key);

                    let prefix = format!("{}:", inode);
                    let mut it = self.db.raw_iterator_cf(cf_dir_entries);
                    it.seek(prefix.as_bytes());
                    while it.valid() {
                        if let Some(key) = it.key() {
                            let key_str = String::from_utf8_lossy(key);
                            if key_str.starts_with(&prefix) {
                                let _ = self.db.delete_cf(cf_dir_entries, key);
                            } else {
                                break;
                            }
                        }
                        it.next();
                    }

                    dir.remove(&name);
                    if let Some(info) = inodes.remove(&inode) {
                        dir_entries.remove(&inode);
                        let is_dir = matches!(info.file_type, FileType::Directory);
                        removed = Some(is_dir);
                    }
                }
            }
            removed
        };
        {
            let mut stats = self.stats.write().unwrap();
            if let Some(is_dir) = removed {
                stats.inode_count -= 1;
                if is_dir {
                    stats.dir_count -= 1;
                }
            }
        }
        self.save_stats();
        info!(
            "Shard {} deleted directory: parent_inode={}, name={}",
            self.shard_id.0, parent_inode, name
        );
    }

    fn rename(
        &self,
        old_parent_inode: u64,
        old_name: String,
        new_parent_inode: u64,
        new_name: String,
    ) {
        let cf_inodes = self.db.cf_handle(CF_INODES).unwrap();
        let cf_dir_entries = self.db.cf_handle(CF_DIR_ENTRIES).unwrap();

        let mut inodes = self.inodes.write().unwrap();
        let mut dir_entries = self.directory_entries.write().unwrap();

        if let Some(old_dir) = dir_entries.get_mut(&old_parent_inode) {
            if let Some(&inode) = old_dir.get(&old_name) {
                let old_key = format!("{}:{}", old_parent_inode, old_name);
                let _ = self.db.delete_cf(cf_dir_entries, old_key.as_bytes());

                let new_key = format!("{}:{}", new_parent_inode, new_name);
                let inode_value = inode.to_be_bytes();
                let _ = self
                    .db
                    .put_cf(cf_dir_entries, new_key.as_bytes(), inode_value);

                dir_entries.entry(new_parent_inode).or_default();
                if let Some(new_dir) = dir_entries.get_mut(&new_parent_inode) {
                    new_dir.insert(new_name.clone(), inode);
                }

                if let Some(info) = inodes.get_mut(&inode) {
                    info.name = new_name.clone();
                    info.parent_inode = new_parent_inode;

                    if let Ok(data) = serde_json::to_vec(info) {
                        let inode_key = inode.to_be_bytes();
                        let _ = self.db.put_cf(cf_inodes, inode_key, &data);
                    }
                }
            }
        }

        info!(
            "Shard {} renamed: {} -> {}",
            self.shard_id.0, old_name, new_name
        );
    }

    pub fn get_inode(&self, inode: u64) -> Option<InodeInfo> {
        self.inodes.read().unwrap().get(&inode).cloned()
    }

    pub fn lookup(&self, parent_inode: u64, name: &str) -> Option<InodeInfo> {
        let dir_entries = self.directory_entries.read().unwrap();
        let inodes = self.inodes.read().unwrap();

        if let Some(dir) = dir_entries.get(&parent_inode) {
            if let Some(&inode) = dir.get(name) {
                return inodes.get(&inode).cloned();
            }
        }

        None
    }

    pub fn list_directory(&self, parent_inode: u64) -> Vec<InodeInfo> {
        let dir_entries = self.directory_entries.read().unwrap();
        let inodes = self.inodes.read().unwrap();

        let mut result = Vec::new();

        if let Some(dir) = dir_entries.get(&parent_inode) {
            for &inode in dir.values() {
                if let Some(info) = inodes.get(&inode) {
                    result.push(info.clone());
                }
            }
        }

        result
    }

    pub fn get_stats(&self) -> ShardStats {
        self.stats.read().unwrap().clone()
    }

    pub fn get_inode_range(&self) -> (u64, u64) {
        self.inode_range
    }

    pub fn get_shard_id(&self) -> ShardId {
        self.shard_id
    }

    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    pub fn inode_range(&self) -> (u64, u64) {
        self.inode_range
    }

    pub fn contains_inode(&self, inode: u64) -> bool {
        let (start, end) = self.inode_range;
        inode >= start && inode < end
    }

    // CRDT Delta Operations: Public API for applying deltas

    pub fn current_time() -> u64 {
        chrono::Utc::now().timestamp() as u64
    }

    pub fn create_inode(&self, info: InodeInfo) -> Result<(), String> {
        let cf_inodes = self
            .db
            .cf_handle(CF_INODES)
            .ok_or_else(|| "CF_INODES not found".to_string())?;

        let inode_key = info.inode.to_be_bytes();
        let data = serde_json::to_vec(&info).map_err(|e| format!("serialize inode: {}", e))?;
        self.db
            .put_cf(cf_inodes, inode_key, &data)
            .map_err(|e| format!("put inode to rocksdb: {}", e))?;

        let is_file = matches!(info.file_type, FileType::File);
        let is_dir = matches!(info.file_type, FileType::Directory);

        {
            let mut inodes = self.inodes.write().unwrap();
            inodes.insert(info.inode, info);
        }
        {
            let mut stats = self.stats.write().unwrap();
            stats.inode_count += 1;
            if is_file {
                stats.file_count += 1;
            }
            if is_dir {
                stats.dir_count += 1;
            }
        }
        self.save_stats();

        Ok(())
    }

    pub fn add_dir_entry(&self, parent_inode: u64, name: &str, inode: u64) -> Result<(), String> {
        let cf_dir_entries = self
            .db
            .cf_handle(CF_DIR_ENTRIES)
            .ok_or_else(|| "CF_DIR_ENTRIES not found".to_string())?;

        let key = format!("{}:{}", parent_inode, name);
        let value = inode.to_be_bytes();
        self.db
            .put_cf(cf_dir_entries, key.as_bytes(), value)
            .map_err(|e| format!("put dir entry to rocksdb: {}", e))?;

        let mut dir_entries = self.directory_entries.write().unwrap();
        dir_entries.entry(parent_inode).or_default();
        if let Some(dir) = dir_entries.get_mut(&parent_inode) {
            dir.insert(name.to_string(), inode);
        }

        Ok(())
    }

    pub fn remove_dir_entry(&self, parent_inode: u64, name: &str) -> Result<(), String> {
        let cf_dir_entries = self
            .db
            .cf_handle(CF_DIR_ENTRIES)
            .ok_or_else(|| "CF_DIR_ENTRIES not found".to_string())?;

        let key = format!("{}:{}", parent_inode, name);
        self.db
            .delete_cf(cf_dir_entries, key.as_bytes())
            .map_err(|e| format!("delete dir entry from rocksdb: {}", e))?;

        let mut dir_entries = self.directory_entries.write().unwrap();
        if let Some(dir) = dir_entries.get_mut(&parent_inode) {
            dir.remove(name);
        }

        Ok(())
    }

    pub fn delete_inode(&self, inode: u64) -> Result<(), String> {
        let cf_inodes = self
            .db
            .cf_handle(CF_INODES)
            .ok_or_else(|| "CF_INODES not found".to_string())?;

        self.db
            .delete_cf(cf_inodes, inode.to_be_bytes())
            .map_err(|e| format!("delete inode from rocksdb: {}", e))?;

        let removed = {
            let mut inodes = self.inodes.write().unwrap();
            inodes.remove(&inode).map(|info| {
                let is_file = matches!(info.file_type, FileType::File);
                let is_dir = matches!(info.file_type, FileType::Directory);
                (is_file, is_dir)
            })
        };
        if let Some((is_file, is_dir)) = removed {
            {
                let mut stats = self.stats.write().unwrap();
                stats.inode_count = stats.inode_count.saturating_sub(1);
                if is_file {
                    stats.file_count = stats.file_count.saturating_sub(1);
                }
                if is_dir {
                    stats.dir_count = stats.dir_count.saturating_sub(1);
                }
            }
            self.save_stats();
        }

        Ok(())
    }

    pub fn update_inode(&self, info: InodeInfo) -> Result<(), String> {
        let cf_inodes = self
            .db
            .cf_handle(CF_INODES)
            .ok_or_else(|| "CF_INODES not found".to_string())?;

        let data = serde_json::to_vec(&info).map_err(|e| format!("serialize inode: {}", e))?;
        self.db
            .put_cf(cf_inodes, info.inode.to_be_bytes(), &data)
            .map_err(|e| format!("put updated inode to rocksdb: {}", e))?;

        let mut inodes = self.inodes.write().unwrap();
        inodes.insert(info.inode, info);

        Ok(())
    }

    /// Batch update multiple inodes in a single RocksDB WriteBatch for better throughput
    pub fn batch_update_inodes(&self, inodes: Vec<InodeInfo>) -> Result<(), String> {
        if inodes.is_empty() {
            return Ok(());
        }

        let cf_inodes = self
            .db
            .cf_handle(CF_INODES)
            .ok_or_else(|| "CF_INODES not found".to_string())?;

        let mut batch = rocksdb::WriteBatch::default();
        let mut mem_updates = Vec::with_capacity(inodes.len());

        for info in inodes {
            let data = serde_json::to_vec(&info).map_err(|e| format!("serialize inode: {}", e))?;
            batch.put_cf(cf_inodes, info.inode.to_be_bytes(), &data);
            mem_updates.push(info);
        }

        let write_opts = rocksdb::WriteOptions::default();
        self.db
            .write_opt(batch, &write_opts)
            .map_err(|e| format!("batch write inodes to rocksdb: {}", e))?;

        // Update in-memory cache
        let mut cache = self.inodes.write().unwrap();
        for info in mem_updates {
            cache.insert(info.inode, info);
        }

        Ok(())
    }
}
