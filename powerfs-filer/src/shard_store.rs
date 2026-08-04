use log::{debug, info, warn};
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
const CF_PENDING_RECLAIMS: &str = "pending_reclaims"; // Phase 5: WAL for GC data chunk reclamation
const CF_FILE_DATA: &str = "file_data"; // Inline file data (replicated via Raft)

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
    pub volume_id: Option<u64>,
    #[serde(default)]
    pub etag: Option<String>,
    // File chunks for data layout (stored in Filer, not Master)
    #[serde(default)]
    pub chunks: Vec<StoredFileChunk>,
    // Extended attributes (e.g. file layout: stripe/flat)
    #[serde(default)]
    pub extended: HashMap<String, Vec<u8>>,
    // Symlink target (for symlink type)
    #[serde(default)]
    pub symlink_target: Option<String>,
    // Hard link count (for hard links)
    #[serde(default)]
    pub nlink: u32,
    // Version counter for cache coherence. Incremented on every modification.
    #[serde(default)]
    pub version: u64,
    // Phase 3.5: 延迟删除标记（0 = 未删除，>0 = 删除时间戳 unix seconds）
    // GC 任务扫描 delete_time > 0 且超过 grace_period 的条目进行物理删除
    #[serde(default)]
    pub delete_time: u64,
}

/// Stored file chunk (persisted in Filer InodeInfo)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StoredFileChunk {
    pub offset: u64,
    pub size: u64,
    /// Chunk-level storage key (needle_id on volume server).
    pub needle_id: u64,
    /// Volume this chunk resides on (per-chunk to support stripe mode).
    pub volume_id: u64,
    pub crc32: u32,
    pub mtime: u64,
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
    next_inode: std::sync::Mutex<u64>, // 下一个可分配 inode（leader 单点分配 + CF_METADATA 持久化，§4 1.4）
    // Phase 3.5.3: per-inode open 计数（内存追踪，filer 重启重置为 0；
    // fuse 端重新 open 时上报，grace_period 兜底重启窗口）
    open_counts: RwLock<HashMap<u64, u32>>,
    // Inline file data (replicated via Raft, persisted to RocksDB).
    // Keyed by inode. Replaces the per-Filer inline_data in net_handler,
    // which was lost on leader switch.
    file_data: RwLock<HashMap<u64, Vec<u8>>>,
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
            ColumnFamilyDescriptor::new(CF_PENDING_RECLAIMS, make_cf_opts()),
            ColumnFamilyDescriptor::new(CF_FILE_DATA, make_cf_opts()),
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
            next_inode: std::sync::Mutex::new(inode_range.0),
            open_counts: RwLock::new(HashMap::new()),
            file_data: RwLock::new(HashMap::new()),
        };

        store.load_data()?;
        store.init_next_inode();
        Ok(store)
    }

    fn load_data(&mut self) -> Result<(), String> {
        self.load_inodes()?;
        self.load_dir_entries()?;
        self.load_stats()?;
        self.load_root_inodes()?;
        self.load_file_data()?;
        info!("Shard {} loaded data from rocksdb", self.shard_id.0);
        Ok(())
    }

    fn load_file_data(&mut self) -> Result<(), String> {
        let cf = match self.db.cf_handle(CF_FILE_DATA) {
            Some(cf) => cf,
            None => return Ok(()),
        };

        let mut file_data = self.file_data.write().unwrap();
        let mut count = 0;
        let mut it = self.db.raw_iterator_cf(cf);
        it.seek_to_first();
        while it.valid() {
            if let (Some(key), Some(value)) = (it.key(), it.value()) {
                let mut key_bytes = [0u8; 8];
                key_bytes.copy_from_slice(&key.to_vec()[..8.min(key.len())]);
                let inode = u64::from_be_bytes(key_bytes);
                file_data.insert(inode, value.to_vec());
                count += 1;
            }
            it.next();
        }

        info!(
            "Shard {} loaded {} inline file data entries from rocksdb",
            self.shard_id.0, count
        );
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
        let mut count = 0;
        while it.valid() {
            if let (Some(key), Some(value)) = (it.key(), it.value()) {
                let mut key_bytes = [0u8; 8];
                key_bytes.copy_from_slice(&key.to_vec()[..8.min(key.len())]);
                let inode = u64::from_be_bytes(key_bytes);
                if let Ok(info) = serde_json::from_slice::<InodeInfo>(value) {
                    inodes.insert(inode, info);
                    count += 1;
                }
            }
            it.next();
        }

        info!(
            "Shard {} loaded {} inodes from rocksdb",
            self.shard_id.0, count
        );
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

        info!(
            "Shard {} loaded {} directory entries from rocksdb",
            self.shard_id.0,
            dir_entries.len()
        );
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
            ShardCommand::SetAttr {
                inode,
                size,
                mode,
                uid,
                gid,
            } => {
                self.setattr(inode, size, mode, uid, gid);
            }
            ShardCommand::SetAttrData { inode, size } => {
                self.setattr_data(inode, size);
            }
            ShardCommand::SetAttrMeta {
                inode,
                mode,
                uid,
                gid,
                mtime,
                atime,
                client_id: _,
                timestamp: _,
            } => {
                self.setattr_meta(inode, mode, uid, gid, mtime, atime);
            }
            ShardCommand::CreateSymlink {
                parent_inode,
                name,
                inode,
                target,
            } => {
                self.create_symlink(parent_inode, name, inode, target);
            }
            ShardCommand::CreateHardLink {
                inode,
                new_parent_inode,
                new_name,
            } => {
                self.create_hard_link(inode, new_parent_inode, new_name);
            }
            ShardCommand::SetChunks {
                inode,
                fid,
                volume_id,
                cookie,
                offset,
                size,
            } => {
                self.set_chunks(inode, fid, volume_id, cookie, offset, size);
            }
            ShardCommand::UpdateInodeSizeChunks {
                inode,
                size,
                chunks,
            } => {
                if let Err(e) = self.update_inode_size_chunks_atomic(inode, size, chunks) {
                    log::error!(
                        "Shard {} apply UpdateInodeSizeChunks failed for inode {}: {}",
                        self.shard_id.0,
                        inode,
                        e
                    );
                }
            }
            ShardCommand::WriteFileData {
                inode,
                offset,
                data,
            } => {
                self.write_file_data(inode, offset, data);
            }
            ShardCommand::DeleteFileData { inode } => {
                self.delete_file_data(inode);
            }
        }
    }

    /// Write inline file data at given offset (Raft-replicated).
    /// Extends the buffer with zeros if writing beyond current end.
    fn write_file_data(&self, inode: u64, offset: u64, data: Vec<u8>) {
        if data.is_empty() {
            return;
        }

        let cf = match self.db.cf_handle(CF_FILE_DATA) {
            Some(cf) => cf,
            None => {
                log::error!("Shard {} CF_FILE_DATA not found", self.shard_id.0);
                return;
            }
        };

        let new_end = offset as usize + data.len();

        {
            let mut file_data = self.file_data.write().unwrap();
            let buf = file_data.entry(inode).or_insert_with(Vec::new);
            if buf.len() < new_end {
                buf.resize(new_end, 0u8);
            }
            buf[offset as usize..new_end].copy_from_slice(&data);
        }

        // Persist to RocksDB
        let key = inode.to_be_bytes();
        let buf = self.file_data.read().unwrap();
        if let Some(data) = buf.get(&inode) {
            let _ = self.db.put_cf(cf, key, data);
        }

        debug!(
            "Shard {} wrote file data: inode={}, offset={}, len={}",
            self.shard_id.0,
            inode,
            offset,
            data.len()
        );
    }

    /// Read inline file data (Raft-replicated). Returns None if no data.
    pub fn read_file_data(&self, inode: u64, offset: usize, length: usize) -> Option<Vec<u8>> {
        let file_data = self.file_data.read().unwrap();
        let buf = file_data.get(&inode)?;

        if length == 0 {
            return Some(Vec::new());
        }

        let start = offset.min(buf.len());
        let end = (start + length).min(buf.len());
        let mut out = buf[start..end].to_vec();
        // Zero-fill trailing portion
        if out.len() < length {
            out.resize(length, 0u8);
        }
        Some(out)
    }

    /// Delete inline file data for an inode.
    fn delete_file_data(&self, inode: u64) {
        let cf = match self.db.cf_handle(CF_FILE_DATA) {
            Some(cf) => cf,
            None => return,
        };

        {
            let mut file_data = self.file_data.write().unwrap();
            file_data.remove(&inode);
        }

        let key = inode.to_be_bytes();
        let _ = self.db.delete_cf(cf, key);

        debug!(
            "Shard {} deleted file data: inode={}",
            self.shard_id.0, inode
        );
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
            mode: 0o100644,
            uid: 0,
            gid: 0,
            blocks: 0,
            fid: None,
            volume_id: None,
            etag: None,
            chunks: vec![],
            extended: HashMap::new(),
            symlink_target: None,
            nlink: 1,
            version: 0,
            delete_time: 0,
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
        volume_id: u64,
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
            mode: 0o100644,
            uid: 0,
            gid: 0,
            blocks: size.div_ceil(4096),
            fid: Some(fid),
            volume_id: Some(volume_id),
            etag: Some(etag),
            chunks: vec![],
            extended: HashMap::new(),
            symlink_target: None,
            nlink: 1,
            version: 0,
            delete_time: 0,
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
            info.version += 1;

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

        // Phase 1: Under inodes+dir_entries locks, remove dir entry and
        // determine the post-lock action (decrement nlink vs delete inode).
        // Action enum: None = not found; Decrement(info) = hardlink, update
        // nlink; DeleteData(inode, is_file) = nlink==0, delete inode+data.
        enum PostAction {
            Decrement(InodeInfo),
            DeleteData(u64, bool),
        }
        let action = {
            let mut inodes = self.inodes.write().unwrap();
            let mut dir_entries = self.directory_entries.write().unwrap();

            if let Some(dir) = dir_entries.get_mut(&parent_inode) {
                if let Some(&inode) = dir.get(&name) {
                    let dir_entry_key = format!("{}:{}", parent_inode, name);
                    let _ = self.db.delete_cf(cf_dir_entries, dir_entry_key.as_bytes());
                    dir.remove(&name);

                    let nlink = inodes.get(&inode).map(|i| i.nlink).unwrap_or(0);
                    if nlink > 1 {
                        // Hardlink: decrement nlink in memory, persist outside lock
                        if let Some(info) = inodes.get_mut(&inode) {
                            info.nlink -= 1;
                            PostAction::Decrement(info.clone())
                        } else {
                            // Inode not in memory (shouldn't happen) - dir entry
                            // already removed, skip nlink persistence
                            warn!(
                                "Shard {} unlink: inode {} not in memory for hardlink decrement",
                                self.shard_id.0, inode
                            );
                            PostAction::DeleteData(0, false)
                        }
                    } else {
                        // nlink <= 1: remove inode from RocksDB + memory
                        let inode_key = inode.to_be_bytes();
                        let _ = self.db.delete_cf(cf_inodes, inode_key);
                        let is_file = inodes
                            .remove(&inode)
                            .map(|info| matches!(info.file_type, FileType::File))
                            .unwrap_or(false);
                        PostAction::DeleteData(inode, is_file)
                    }
                } else {
                    PostAction::DeleteData(0, false) // not found, no-op
                }
            } else {
                PostAction::DeleteData(0, false) // not found, no-op
            }
        };

        // Phase 2: Outside locks, perform persistence (update_inode /
        // delete_file_data acquire their own locks).
        let mut inode_deleted = false;
        let mut is_file_deleted = false;
        let mut inode_val = 0;
        match action {
            PostAction::Decrement(info) => {
                inode_val = info.inode;
                let new_nlink = info.nlink;
                let _ = self.update_inode(info.clone());
                info!(
                    "Shard {} unlinked hardlink: parent={}, name={}, inode={}, nlink -> {}",
                    self.shard_id.0, parent_inode, name, inode_val, new_nlink
                );
            }
            PostAction::DeleteData(inode, is_file) => {
                if inode != 0 {
                    inode_val = inode;
                    inode_deleted = true;
                    is_file_deleted = is_file;
                    self.delete_file_data(inode);
                }
            }
        }

        // Stats only updated when inode is actually deleted (nlink reached 0)
        if inode_deleted {
            let mut stats = self.stats.write().unwrap();
            stats.inode_count -= 1;
            if is_file_deleted {
                stats.file_count -= 1;
            }
        }
        self.save_stats();
        info!(
            "Shard {} deleted file: parent_inode={}, name={}, inode={}, inode_deleted={}",
            self.shard_id.0, parent_inode, name, inode_val, inode_deleted
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
            mode: 0o040755,
            uid: 0,
            gid: 0,
            blocks: 0,
            fid: None,
            volume_id: None,
            etag: None,
            chunks: vec![],
            extended: HashMap::new(),
            symlink_target: None,
            nlink: 2,
            version: 0,
            delete_time: 0,
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

                /* 从内存 HashMap 移除旧名称 (之前只删了 DB 没删内存,
                 * 导致 Filer 服务运行期间 lookup/list_directory 仍能
                 * 找到旧名称, remount 后 old_name 仍然存在). */
                old_dir.remove(&old_name);

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

    /// Set chunk/fid info for an existing inode (for data location persistence)
    fn set_chunks(
        &self,
        inode: u64,
        fid: String,
        volume_id: u64,
        cookie: u32,
        offset: u64,
        size: u64,
    ) {
        let cf_inodes = self.db.cf_handle(CF_INODES).unwrap();

        {
            let mut inodes = self.inodes.write().unwrap();
            if let Some(info) = inodes.get_mut(&inode) {
                info.fid = Some(fid.clone());
                info.volume_id = Some(volume_id);
                // Extract file_key from fid string "volume_id,cookie,file_key"
                // to use as the chunk-level needle_id.
                let needle_id: u64 = fid
                    .split(',')
                    .nth(2)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                info.chunks.push(StoredFileChunk {
                    offset,
                    size,
                    mtime: chrono::Utc::now().timestamp() as u64,
                    needle_id,
                    volume_id,
                    crc32: 0,
                });

                // Persist to RocksDB
                if let Ok(data) = serde_json::to_vec(info) {
                    let inode_key = inode.to_be_bytes();
                    let _ = self.db.put_cf(cf_inodes, inode_key, &data);
                }

                info!(
                    "Shard {} set_chunks: inode={}, fid={}, volume_id={}, cookie={}",
                    self.shard_id.0, inode, fid, volume_id, cookie
                );
            } else {
                warn!(
                    "Shard {} set_chunks: inode {} not found",
                    self.shard_id.0, inode
                );
            }
        }
    }

    pub fn get_inode(&self, inode: u64) -> Option<InodeInfo> {
        self.inodes.read().unwrap().get(&inode).cloned()
    }

    pub fn lookup(&self, parent_inode: u64, name: &str) -> Option<InodeInfo> {
        let dir_entries = self.directory_entries.read().unwrap();
        let inodes = self.inodes.read().unwrap();

        if let Some(dir) = dir_entries.get(&parent_inode) {
            if let Some(&inode) = dir.get(name) {
                // Phase 3.5: 跳过 tombstoned 条目（延迟删除期间不可见）
                if let Some(info) = inodes.get(&inode) {
                    if info.delete_time > 0 {
                        return None;
                    }
                    return Some(info.clone());
                }
            }
        }

        None
    }

    pub fn list_directory(&self, parent_inode: u64) -> Vec<InodeInfo> {
        let dir_entries = self.directory_entries.read().unwrap();
        let inodes = self.inodes.read().unwrap();

        let mut result = Vec::new();

        if let Some(dir) = dir_entries.get(&parent_inode) {
            // Iterate dir_entries keys (not just values) to return the
            // authoritative name for each entry. Using InodeInfo.name is
            // wrong because:
            //  - Hard links: multiple names -> same inode, InodeInfo.name
            //    only stores one name, causing duplicate entries in readdir
            //  - Rename: if InodeInfo.name diverges from the dir_entries key
            //    (e.g., stale cache, Raft replication gap), readdir returns
            //    a name that lookup() cannot find -> ENOENT in userspace
            for (name, &inode) in dir.iter() {
                if let Some(mut info) = inodes.get(&inode).cloned() {
                    // Phase 3.5: 跳过 tombstoned 条目
                    if info.delete_time > 0 {
                        continue;
                    }
                    info.name = name.clone();
                    info.parent_inode = parent_inode;
                    result.push(info);
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

    /// 原子地创建 inode 并添加目录条目（WriteBatch 保证 CF_INODES + CF_DIR_ENTRIES 一致）。
    /// 用于 push_delta 的 Add 操作，避免 create_inode + add_dir_entry 两步非原子导致的不一致。
    pub fn create_inode_atomic(
        &self,
        info: InodeInfo,
        parent_inode: u64,
        name: &str,
    ) -> Result<(), String> {
        let cf_inodes = self
            .db
            .cf_handle(CF_INODES)
            .ok_or_else(|| "CF_INODES not found".to_string())?;
        let cf_dir_entries = self
            .db
            .cf_handle(CF_DIR_ENTRIES)
            .ok_or_else(|| "CF_DIR_ENTRIES not found".to_string())?;

        let inode_key = info.inode.to_be_bytes();
        let data = serde_json::to_vec(&info).map_err(|e| format!("serialize inode: {}", e))?;
        let dir_key = format!("{}:{}", parent_inode, name);
        let dir_value = info.inode.to_be_bytes();

        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(cf_inodes, inode_key, &data);
        batch.put_cf(cf_dir_entries, dir_key.as_bytes(), dir_value);

        let write_opts = rocksdb::WriteOptions::default();
        self.db
            .write_opt(batch, &write_opts)
            .map_err(|e| format!("batch create inode: {}", e))?;

        let is_file = matches!(info.file_type, FileType::File);
        let is_dir = matches!(info.file_type, FileType::Directory);
        let inode_val = info.inode;
        {
            let mut inodes = self.inodes.write().unwrap();
            inodes.insert(inode_val, info);
        }
        {
            let mut dir_entries = self.directory_entries.write().unwrap();
            dir_entries.entry(parent_inode).or_default();
            if let Some(dir) = dir_entries.get_mut(&parent_inode) {
                dir.insert(name.to_string(), inode_val);
            }
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

    /// 原子地删除 inode 并移除目录条目（WriteBatch 保证 CF_INODES + CF_DIR_ENTRIES 一致）。
    /// 用于 push_delta 的 Remove 操作，避免 remove_dir_entry + delete_inode 两步非原子。
    pub fn remove_inode_atomic(
        &self,
        inode: u64,
        parent_inode: u64,
        name: &str,
    ) -> Result<(), String> {
        let cf_inodes = self
            .db
            .cf_handle(CF_INODES)
            .ok_or_else(|| "CF_INODES not found".to_string())?;
        let cf_dir_entries = self
            .db
            .cf_handle(CF_DIR_ENTRIES)
            .ok_or_else(|| "CF_DIR_ENTRIES not found".to_string())?;

        let dir_key = format!("{}:{}", parent_inode, name);
        let mut batch = rocksdb::WriteBatch::default();
        batch.delete_cf(cf_inodes, inode.to_be_bytes());
        batch.delete_cf(cf_dir_entries, dir_key.as_bytes());

        let write_opts = rocksdb::WriteOptions::default();
        self.db
            .write_opt(batch, &write_opts)
            .map_err(|e| format!("batch remove inode: {}", e))?;

        let removed = {
            let mut inodes = self.inodes.write().unwrap();
            inodes.remove(&inode).map(|info| {
                let is_file = matches!(info.file_type, FileType::File);
                let is_dir = matches!(info.file_type, FileType::Directory);
                (is_file, is_dir)
            })
        };
        {
            let mut dir_entries = self.directory_entries.write().unwrap();
            if let Some(dir) = dir_entries.get_mut(&parent_inode) {
                dir.remove(name);
            }
        }
        if let Some((is_file, is_dir)) = removed {
            let mut stats = self.stats.write().unwrap();
            stats.inode_count = stats.inode_count.saturating_sub(1);
            if is_file {
                stats.file_count = stats.file_count.saturating_sub(1);
            }
            if is_dir {
                stats.dir_count = stats.dir_count.saturating_sub(1);
            }
            drop(stats);
            self.save_stats();
        }

        Ok(())
    }

    // ========================================================================
    // Phase 3.5: 延迟删除（tombstone + GC）
    // ========================================================================

    /// Phase 3.5: 标记 inode 为 tombstone（延迟删除）。
    ///
    /// 不物理删除 CF_INODES / CF_DIR_ENTRIES，仅设置 delete_time。
    /// lookup/list_directory 跳过 delete_time > 0 的条目。
    /// GC 任务在 grace_period 后物理删除。
    pub fn mark_tombstone(&self, inode: u64) -> Result<(), String> {
        let now = Self::current_time();
        let mut inodes = self.inodes.write().unwrap();
        if let Some(info) = inodes.get_mut(&inode) {
            info.delete_time = now;
            let updated = info.clone();
            drop(inodes);
            self.update_inode(updated)?;
            debug!("Phase 3.5: marked tombstone for inode {} at {}", inode, now);
        }
        Ok(())
    }

    /// Phase 3.5: 扫描所有 tombstoned 且超过 grace_period 的条目。
    ///
    /// 返回 (inode, parent_inode, name, chunks) 列表供 GC 任务物理删除。
    /// TODO: 检查 open_count == 0 + 无活跃 lease（当前仅检查 grace_period）
    pub fn scan_tombstones_for_gc(
        &self,
        grace_period_secs: u64,
    ) -> Vec<(u64, u64, String, Vec<StoredFileChunk>)> {
        let now = Self::current_time();
        let inodes = self.inodes.read().unwrap();
        let dir_entries = self.directory_entries.read().unwrap();

        let mut result = Vec::new();

        for (inode, info) in inodes.iter() {
            if info.delete_time == 0 {
                continue;
            }
            // 检查 grace_period
            if now < info.delete_time + grace_period_secs {
                continue; // 还在 grace period 内
            }
            // 查找 parent + name（从 directory_entries 反查）
            let mut found = false;
            for (parent, entries) in dir_entries.iter() {
                if let Some((name, _)) = entries.iter().find(|(_, &i)| i == *inode) {
                    result.push((*inode, *parent, name.clone(), info.chunks.clone()));
                    found = true;
                    break;
                }
            }
            if !found {
                // orphan inode（无 dir_entry 指向），直接加入 GC 列表
                result.push((*inode, 0, String::new(), info.chunks.clone()));
            }
        }

        result
    }

    // ========================================================================
    // Phase 3.5.3: open_count 追踪（GC 第三条件）
    // ========================================================================

    /// Phase 3.5.3: 递增 inode 的 open 计数。
    /// fuse 端 open 时通过 net 层通知 filer 调用此方法。
    pub fn increment_open_count(&self, inode: u64) -> u32 {
        let mut counts = self.open_counts.write().unwrap();
        let count = counts.entry(inode).or_insert(0);
        *count += 1;
        *count
    }

    /// Phase 3.5.3: 递减 inode 的 open 计数，不低于 0。
    /// fuse 端 release/close 时通过 net 层通知 filer 调用此方法。
    pub fn decrement_open_count(&self, inode: u64) -> u32 {
        let mut counts = self.open_counts.write().unwrap();
        let count = counts.entry(inode).or_insert(0);
        if *count > 0 {
            *count -= 1;
        }
        let result = *count;
        if result == 0 {
            counts.remove(&inode);
        }
        result
    }

    /// Phase 3.5.3: 查询 inode 的 open 计数。GC 物理删除前检查 == 0。
    pub fn get_open_count(&self, inode: u64) -> u32 {
        self.open_counts
            .read()
            .unwrap()
            .get(&inode)
            .copied()
            .unwrap_or(0)
    }

    // ========================================================================
    // Phase 5: pending_reclaims WAL（GC 数据块回收持久化）
    // ========================================================================

    /// Phase 5: 持久化待回收 chunk（WAL 模式：先持久化再删元数据）。
    /// key = "volume_id,needle_id", value = (inode, StoredFileChunk) JSON。
    pub fn add_pending_reclaim(&self, inode: u64, chunk: &StoredFileChunk) {
        if let Some(cf) = self.db.cf_handle(CF_PENDING_RECLAIMS) {
            let entry = serde_json::json!({"inode": inode, "chunk": chunk});
            if let Ok(data) = serde_json::to_vec(&entry) {
                let key = format!("{},{}", chunk.volume_id, chunk.needle_id);
                let _ = self.db.put_cf(cf, key.as_bytes(), &data);
            }
        }
    }

    /// Phase 5: 删除待回收 chunk（delete_needle 成功后调用）。
    pub fn remove_pending_reclaim(&self, volume_id: u64, needle_id: u64) {
        if let Some(cf) = self.db.cf_handle(CF_PENDING_RECLAIMS) {
            let key = format!("{},{}", volume_id, needle_id);
            let _ = self.db.delete_cf(cf, key.as_bytes());
        }
    }

    /// Phase 5: 列出所有待回收 chunks（GC 重试 + 崩溃恢复用）。
    pub fn list_pending_reclaims(&self) -> Vec<(u64, StoredFileChunk)> {
        let mut result = Vec::new();
        if let Some(cf) = self.db.cf_handle(CF_PENDING_RECLAIMS) {
            let mut it = self.db.raw_iterator_cf(cf);
            it.seek_to_first();
            while it.valid() {
                if let (Some(_key), Some(value)) = (it.key(), it.value()) {
                    if let Ok(entry) = serde_json::from_slice::<serde_json::Value>(value) {
                        let inode = entry.get("inode").and_then(|v| v.as_u64()).unwrap_or(0);
                        if let Some(chunk) = entry.get("chunk") {
                            if let Ok(c) = serde_json::from_value::<StoredFileChunk>(chunk.clone())
                            {
                                result.push((inode, c));
                            }
                        }
                    }
                }
                it.next();
            }
        }
        result
    }

    /// 原子地重命名目录条目（WriteBatch delete old + put new）。
    /// inode 本身不变，仅迁移 dir_entry。用于 push_delta 的 Rename 操作。
    pub fn rename_dir_entry_atomic(
        &self,
        old_parent: u64,
        old_name: &str,
        new_parent: u64,
        new_name: &str,
        inode: u64,
    ) -> Result<(), String> {
        let cf_dir_entries = self
            .db
            .cf_handle(CF_DIR_ENTRIES)
            .ok_or_else(|| "CF_DIR_ENTRIES not found".to_string())?;

        let old_key = format!("{}:{}", old_parent, old_name);
        let new_key = format!("{}:{}", new_parent, new_name);
        let new_value = inode.to_be_bytes();

        let mut batch = rocksdb::WriteBatch::default();
        batch.delete_cf(cf_dir_entries, old_key.as_bytes());
        batch.put_cf(cf_dir_entries, new_key.as_bytes(), new_value);

        let write_opts = rocksdb::WriteOptions::default();
        self.db
            .write_opt(batch, &write_opts)
            .map_err(|e| format!("batch rename dir entry: {}", e))?;

        {
            let mut dir_entries = self.directory_entries.write().unwrap();
            if let Some(dir) = dir_entries.get_mut(&old_parent) {
                dir.remove(old_name);
            }
            dir_entries.entry(new_parent).or_default();
            if let Some(dir) = dir_entries.get_mut(&new_parent) {
                dir.insert(new_name.to_string(), inode);
            }
        }

        Ok(())
    }

    /// 初始化 next_inode：从 CF_METADATA 读，若无用 inode_range.start（§4 1.4）
    /// 保留 inode 0（无效）和 inode 1（POSIX root），shard 0 起始至少为 2
    fn init_next_inode(&self) {
        let start = match self.db.cf_handle(CF_METADATA) {
            Some(cf) => match self.db.get_cf(cf, b"next_inode") {
                Ok(Some(v)) if v.len() == 8 => {
                    let mut arr = [0u8; 8];
                    arr.copy_from_slice(&v);
                    u64::from_be_bytes(arr)
                }
                _ => self.inode_range.0,
            },
            None => self.inode_range.0,
        };
        // 跳过保留 inode：0（无效）和 1（POSIX root）
        let start = start.max(2);
        *self.next_inode.lock().unwrap() = start;
        info!(
            "Shard {}: init_next_inode = {} (range={:?})",
            self.shard_id.0, start, self.inode_range
        );
    }

    /// Scan CF_INODES for the maximum inode in [range_start, range_end).
    /// Returns range_start if no inodes found in range.
    /// Used by `MetaShardManager::recover_inode_generator` to advance the
    /// in-memory counter past existing inodes after a restart.
    ///
    /// Keys in CF_INODES are `u64::to_be_bytes()` (big-endian), so they sort
    /// numerically. We iterate backwards from the end and return the first
    /// inode found within the range.
    pub fn get_max_inode_in_range(&self, range_start: u64, range_end: u64) -> u64 {
        let cf = match self.db.cf_handle(CF_INODES) {
            Some(cf) => cf,
            None => return range_start,
        };

        let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::End);
        for item in iter {
            if let Ok((key, _)) = item {
                if key.len() != 8 {
                    continue;
                }
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&key);
                let inode = u64::from_be_bytes(arr);
                if inode < range_start {
                    // Gone below our range; no more candidates
                    break;
                }
                if inode < range_end {
                    // Found the max inode in our range
                    return inode + 1;
                }
                // inode >= range_end: from a higher-numbered filer node,
                // keep scanning backwards
            }
        }
        range_start
    }

    /// 批量分配 inode 区间 [start, end)（leader 单点 + CF_METADATA sync 持久化，§4 1.4）。
    /// 返回 (start, end)，fuse 在区间内本地分配，写路径零等待。
    pub fn alloc_inode_batch(&self, count: u32) -> Result<(u64, u64), String> {
        if count == 0 {
            return Err("alloc_inode_batch: count must > 0".to_string());
        }
        let cf = self
            .db
            .cf_handle(CF_METADATA)
            .ok_or_else(|| "CF_METADATA not found".to_string())?;
        let mut guard = self.next_inode.lock().unwrap();
        let start = *guard;
        let end = start
            .checked_add(count as u64)
            .ok_or_else(|| "inode range overflow".to_string())?;
        if end > self.inode_range.1 {
            return Err(format!(
                "inode range exhausted: start={} end={} max={}",
                start, end, self.inode_range.1
            ));
        }
        // sync 持久化 next_inode=end，防 leader 崩溃丢分配
        let mut write_opts = rocksdb::WriteOptions::default();
        write_opts.set_sync(true);
        self.db
            .put_cf_opt(cf, b"next_inode", end.to_be_bytes(), &write_opts)
            .map_err(|e| format!("persist next_inode: {}", e))?;
        *guard = end;
        info!(
            "Shard {}: alloc_inode_batch count={} -> [{}, {})",
            self.shard_id.0, count, start, end
        );
        Ok((start, end))
    }

    /// 原子更新 inode 的 size + chunks（强一致，sync 写，§4 1.5 / §5.1 lease 协调）。
    /// close 时 sync 账本调用，保证数据账本全局强一致。
    pub fn update_inode_size_chunks_atomic(
        &self,
        inode: u64,
        size: u64,
        chunks: Vec<StoredFileChunk>,
    ) -> Result<(), String> {
        let cf_inodes = self
            .db
            .cf_handle(CF_INODES)
            .ok_or_else(|| "CF_INODES not found".to_string())?;
        let mut info = self
            .get_inode(inode)
            .ok_or_else(|| format!("update_inode_size_chunks: inode {} not found", inode))?;
        info.size = size;
        info.blocks = size.div_ceil(512);
        info.chunks = chunks;
        info.mtime = Self::current_time();
        let data = serde_json::to_vec(&info).map_err(|e| format!("serialize inode: {}", e))?;
        // sync 写保证 close sync 账本强持久化
        let mut write_opts = rocksdb::WriteOptions::default();
        write_opts.set_sync(true);
        self.db
            .put_cf_opt(cf_inodes, inode.to_be_bytes(), &data, &write_opts)
            .map_err(|e| format!("put inode size/chunks: {}", e))?;
        let mut inodes = self.inodes.write().unwrap();
        inodes.insert(inode, info);
        Ok(())
    }

    /// Set inode attributes
    fn setattr(
        &self,
        inode: u64,
        size: Option<u64>,
        mode: Option<u64>,
        uid: Option<u64>,
        gid: Option<u64>,
    ) {
        let info = match self.get_inode(inode) {
            Some(mut info) => {
                if let Some(s) = size {
                    info.size = s;
                    // Truncate inline file data to match new size
                    self.truncate_file_data(inode, s);
                }
                if let Some(m) = mode {
                    info.mode = m as u32;
                }
                if let Some(u) = uid {
                    info.uid = u as u32;
                }
                if let Some(g) = gid {
                    info.gid = g as u32;
                }
                let now = chrono::Utc::now().timestamp() as u64;
                info.ctime = now;
                info.mtime = now;
                info
            }
            None => return,
        };

        let _ = self.update_inode(info);
    }

    /// Truncate inline file data to new size. Removes data if size == 0.
    fn truncate_file_data(&self, inode: u64, new_size: u64) {
        let cf = match self.db.cf_handle(CF_FILE_DATA) {
            Some(cf) => cf,
            None => return,
        };

        let mut file_data = self.file_data.write().unwrap();
        if new_size == 0 {
            if file_data.remove(&inode).is_some() {
                let key = inode.to_be_bytes();
                let _ = self.db.delete_cf(cf, key);
            }
            return;
        }

        if let Some(buf) = file_data.get_mut(&inode) {
            if (new_size as usize) < buf.len() {
                buf.truncate(new_size as usize);
                let key = inode.to_be_bytes();
                let _ = self.db.put_cf(cf, key, buf);
            }
        }
    }

    /// Set data-related inode attributes (size) - strong consistency path via Raft
    fn setattr_data(&self, inode: u64, size: Option<u64>) {
        let info = match self.get_inode(inode) {
            Some(mut info) => {
                if let Some(s) = size {
                    info.size = s;
                    info.blocks = s.div_ceil(512);
                    // Truncate inline file data to match new size
                    self.truncate_file_data(inode, s);
                }
                let now = chrono::Utc::now().timestamp() as u64;
                info.ctime = now;
                info.mtime = now;
                info
            }
            None => return,
        };

        let _ = self.update_inode(info);
    }

    /// Set metadata-related inode attributes (mode, uid, gid, mtime, atime) - eventual consistency path
    fn setattr_meta(
        &self,
        inode: u64,
        mode: Option<u64>,
        uid: Option<u64>,
        gid: Option<u64>,
        mtime: Option<u64>,
        atime: Option<u64>,
    ) {
        let info = match self.get_inode(inode) {
            Some(mut info) => {
                if let Some(m) = mode {
                    info.mode = m as u32;
                }
                if let Some(u) = uid {
                    info.uid = u as u32;
                }
                if let Some(g) = gid {
                    info.gid = g as u32;
                }
                if let Some(mt) = mtime {
                    if mt > info.mtime {
                        info.mtime = mt;
                    }
                }
                if let Some(at) = atime {
                    if at > info.atime {
                        info.atime = at;
                    }
                }
                info
            }
            None => return,
        };

        let _ = self.update_inode(info);
    }

    /// Create a symbolic link
    fn create_symlink(&self, parent_inode: u64, name: String, inode: u64, target: String) {
        let now = chrono::Utc::now().timestamp() as u64;
        let target_len = target.len() as u64;
        let target_for_log = target.clone();

        let inode_info = InodeInfo {
            inode,
            name: name.clone(),
            parent_inode,
            file_type: FileType::Symlink,
            size: target_len,
            mtime: now,
            atime: now,
            ctime: now,
            mode: 0o120777,
            uid: 0,
            gid: 0,
            blocks: 0,
            fid: None,
            volume_id: None,
            etag: None,
            chunks: vec![],
            extended: HashMap::new(),
            symlink_target: Some(target),
            nlink: 1,
            version: 0,
            delete_time: 0,
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
        }
        self.save_stats();

        info!(
            "Shard {} created symlink: inode={}, target={}",
            self.shard_id.0, inode, target_for_log
        );
    }

    /// Create a hard link
    fn create_hard_link(&self, inode: u64, new_parent_inode: u64, new_name: String) {
        let cf_dir_entries = self.db.cf_handle(CF_DIR_ENTRIES).unwrap();
        let new_name_for_log = new_name.clone();

        let dir_entry_key = format!("{}:{}", new_parent_inode, new_name);
        let inode_value = inode.to_be_bytes();
        let _ = self
            .db
            .put_cf(cf_dir_entries, dir_entry_key.as_bytes(), inode_value);

        // Update directory entries
        {
            let mut dir_entries = self.directory_entries.write().unwrap();
            dir_entries.entry(new_parent_inode).or_default();
            if let Some(dir) = dir_entries.get_mut(&new_parent_inode) {
                dir.insert(new_name, inode);
            }
        }

        // Update nlink count - clone info while holding the lock,
        // then release before calling update_inode to avoid deadlock
        // (update_inode also tries to acquire inodes.write())
        let info_to_update = {
            let mut inodes = self.inodes.write().unwrap();
            if let Some(info) = inodes.get_mut(&inode) {
                info.nlink += 1;
                Some(info.clone())
            } else {
                None
            }
        };

        if let Some(info) = info_to_update {
            let _ = self.update_inode(info);
        }

        info!(
            "Shard {} created hard link: inode={}, new_parent={}, new_name={}",
            self.shard_id.0, inode, new_parent_inode, new_name_for_log
        );
    }

    /// Force flush all data to disk (for initialization tool to ensure persistence)
    pub fn flush(&self) -> Result<(), String> {
        self.db.flush().map_err(|e| format!("flush rocksdb: {}", e))
    }

    /// Create inode with sync to ensure immediate disk persistence (for init tool)
    pub fn create_inode_sync(&self, info: InodeInfo) -> Result<(), String> {
        let cf_inodes = self
            .db
            .cf_handle(CF_INODES)
            .ok_or_else(|| "CF_INODES not found".to_string())?;

        let inode_key = info.inode.to_be_bytes();
        let data = serde_json::to_vec(&info).map_err(|e| format!("serialize inode: {}", e))?;

        let mut write_opts = rocksdb::WriteOptions::default();
        write_opts.set_sync(true);
        self.db
            .put_cf_opt(cf_inodes, inode_key, &data, &write_opts)
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

    /// Save root inodes mapping with sync
    pub fn set_root_inode_sync(&self, bucket: &str, inode: u64) {
        let cf = match self.db.cf_handle(CF_METADATA) {
            Some(cf) => cf,
            None => return,
        };
        let key = format!("root_inode:{}", bucket);
        let value = inode.to_be_bytes();
        let mut write_opts = rocksdb::WriteOptions::default();
        write_opts.set_sync(true);
        let _ = self.db.put_cf_opt(cf, key.as_bytes(), value, &write_opts);
        self.root_inodes
            .write()
            .unwrap()
            .insert(bucket.to_string(), inode);
    }

    /// Add directory entry with sync (for init tool)
    pub fn add_dir_entry_sync(
        &self,
        parent_inode: u64,
        name: &str,
        inode: u64,
    ) -> Result<(), String> {
        let cf_dir_entries = self
            .db
            .cf_handle(CF_DIR_ENTRIES)
            .ok_or_else(|| "CF_DIR_ENTRIES not found".to_string())?;

        let key = format!("{}:{}", parent_inode, name);
        let value = inode.to_be_bytes();
        let mut write_opts = rocksdb::WriteOptions::default();
        write_opts.set_sync(true);
        self.db
            .put_cf_opt(cf_dir_entries, key.as_bytes(), value, &write_opts)
            .map_err(|e| format!("put dir entry to rocksdb: {}", e))?;

        let mut dir_entries = self.directory_entries.write().unwrap();
        dir_entries.entry(parent_inode).or_default();
        if let Some(dir) = dir_entries.get_mut(&parent_inode) {
            dir.insert(name.to_string(), inode);
        }

        Ok(())
    }

    /// Verify that an inode exists directly in RocksDB (not just in memory cache)
    pub fn verify_inode_in_db(&self, inode: u64) -> bool {
        if let Some(cf) = self.db.cf_handle(CF_INODES) {
            let key = inode.to_be_bytes();
            matches!(self.db.get_cf(cf, key), Ok(Some(_)))
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft_group_manager::ShardId;

    fn make_store() -> ShardStore {
        let tmp_dir = std::env::temp_dir().join(format!(
            "powerfs-shard-store-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&tmp_dir).unwrap();
        ShardStore::new(ShardId(1), (1000, 2000), tmp_dir.to_str().unwrap()).unwrap()
    }

    fn make_inode(inode: u64, parent: u64, name: &str) -> InodeInfo {
        InodeInfo {
            inode,
            name: name.to_string(),
            parent_inode: parent,
            file_type: FileType::File,
            size: 0,
            mtime: ShardStore::current_time(),
            atime: ShardStore::current_time(),
            ctime: ShardStore::current_time(),
            mode: 0o644,
            uid: 0,
            gid: 0,
            blocks: 0,
            fid: None,
            volume_id: None,
            etag: None,
            chunks: vec![],
            extended: HashMap::new(),
            symlink_target: None,
            nlink: 1,
            version: 0,
            delete_time: 0,
        }
    }

    #[test]
    fn test_mark_tombstone_hides_entry_from_lookup() {
        let store = make_store();
        let info = make_inode(1500, 1, "foo");
        store.create_inode_atomic(info, 1, "foo").unwrap();

        // 删除前 lookup 可见
        assert!(store.lookup(1, "foo").is_some());

        // 标记 tombstone
        store.mark_tombstone(1500).unwrap();

        // 删除后 lookup 不可见
        assert!(store.lookup(1, "foo").is_none());
        // 但 inode 仍存在于 store（仅标记 delete_time）
        assert!(store.get_inode(1500).is_some());
        assert!(store.get_inode(1500).unwrap().delete_time > 0);
    }

    #[test]
    fn test_scan_tombstones_respects_grace_period() {
        let store = make_store();
        let info = make_inode(1500, 1, "foo");
        store.create_inode_atomic(info, 1, "foo").unwrap();
        store.mark_tombstone(1500).unwrap();

        // grace_period 很大时，不应被扫描到
        let candidates = store.scan_tombstones_for_gc(999999);
        assert!(candidates.is_empty());

        // grace_period 为 0 时，应被扫描到
        let candidates = store.scan_tombstones_for_gc(0);
        assert_eq!(candidates.len(), 1);
        let (inode, parent, name, chunks) = &candidates[0];
        assert_eq!(*inode, 1500);
        assert_eq!(*parent, 1);
        assert_eq!(name, "foo");
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_gc_physical_delete_via_remove_inode_atomic() {
        let store = make_store();
        let info = make_inode(1500, 1, "foo");
        store.create_inode_atomic(info, 1, "foo").unwrap();
        store.mark_tombstone(1500).unwrap();

        // 模拟 GC：扫描 + 物理删除
        let candidates = store.scan_tombstones_for_gc(0);
        assert_eq!(candidates.len(), 1);
        let (inode, parent, name, _) = &candidates[0];
        store.remove_inode_atomic(*inode, *parent, name).unwrap();

        // 物理删除后 inode 与 dir_entry 均不存在
        assert!(store.get_inode(1500).is_none());
        assert!(store.lookup(1, "foo").is_none());
        assert!(!store.verify_inode_in_db(1500));
    }

    #[test]
    fn test_non_tombstoned_inodes_not_scanned() {
        let store = make_store();
        let info = make_inode(1500, 1, "foo");
        store.create_inode_atomic(info, 1, "foo").unwrap();

        // 未标记 tombstone 的 inode 不应被扫描到
        let candidates = store.scan_tombstones_for_gc(0);
        assert!(candidates.is_empty());
    }

    #[test]
    fn test_open_count_increment_decrement() {
        let store = make_store();

        // 初始 open_count 为 0
        assert_eq!(store.get_open_count(1500), 0);

        // open 两次 → open_count = 2
        assert_eq!(store.increment_open_count(1500), 1);
        assert_eq!(store.increment_open_count(1500), 2);
        assert_eq!(store.get_open_count(1500), 2);

        // close 一次 → open_count = 1
        assert_eq!(store.decrement_open_count(1500), 1);
        assert_eq!(store.get_open_count(1500), 1);

        // close 再次 → open_count = 0（条目从 map 移除）
        assert_eq!(store.decrement_open_count(1500), 0);
        assert_eq!(store.get_open_count(1500), 0);
    }

    #[test]
    fn test_open_count_decrement_below_zero_clamped() {
        let store = make_store();
        // 未 open 直接 close → 不会变负，返回 0
        assert_eq!(store.decrement_open_count(1500), 0);
        assert_eq!(store.get_open_count(1500), 0);
    }

    #[test]
    fn test_pending_reclaims_wal() {
        let store = make_store();
        let chunk = StoredFileChunk {
            offset: 0,
            size: 1024,
            mtime: 100,
            needle_id: 200,
            volume_id: 1,
            crc32: 0xDEADBEEF,
        };

        // 初始为空
        assert!(store.list_pending_reclaims().is_empty());

        // 添加 pending reclaim
        store.add_pending_reclaim(5000, &chunk);
        let pending = store.list_pending_reclaims();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].0, 5000);
        assert_eq!(pending[0].1.needle_id, 200);
        assert_eq!(pending[0].1.volume_id, 1);

        // 删除 pending reclaim
        store.remove_pending_reclaim(1, 200);
        assert!(store.list_pending_reclaims().is_empty());
    }
}
