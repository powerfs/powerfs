use log::info;
use rocksdb::{ColumnFamilyDescriptor, DB};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::RwLock;

use crate::raft_group_manager::{ShardCommand, ShardId};

const CF_INODES: &str = "inodes";
const CF_DIR_ENTRIES: &str = "dir_entries";
const CF_STATS: &str = "stats";

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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
}

impl ShardStore {
    pub fn new(shard_id: ShardId, inode_range: (u64, u64), db_path: &str) -> Result<Self, String> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cf_descriptors = vec![
            ColumnFamilyDescriptor::new(CF_INODES, rocksdb::Options::default()),
            ColumnFamilyDescriptor::new(CF_DIR_ENTRIES, rocksdb::Options::default()),
            ColumnFamilyDescriptor::new(CF_STATS, rocksdb::Options::default()),
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
        };

        store.load_data()?;
        Ok(store)
    }

    fn load_data(&mut self) -> Result<(), String> {
        self.load_inodes()?;
        self.load_dir_entries()?;
        self.load_stats()?;
        info!("Shard {} loaded data from rocksdb", self.shard_id.0);
        Ok(())
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

        let mut inodes = self.inodes.write().unwrap();
        let mut dir_entries = self.directory_entries.write().unwrap();
        let mut stats = self.stats.write().unwrap();

        inodes.insert(inode, inode_info);

        dir_entries.entry(parent_inode).or_default();
        if let Some(dir) = dir_entries.get_mut(&parent_inode) {
            dir.insert(name, inode);
        }

        stats.inode_count += 1;
        stats.file_count += 1;
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

        let mut inodes = self.inodes.write().unwrap();
        let mut dir_entries = self.directory_entries.write().unwrap();
        let mut stats = self.stats.write().unwrap();

        inodes.insert(inode, inode_info);

        dir_entries.entry(parent_inode).or_default();
        if let Some(dir) = dir_entries.get_mut(&parent_inode) {
            dir.insert(name, inode);
        }

        stats.inode_count += 1;
        stats.file_count += 1;
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

        let mut inodes = self.inodes.write().unwrap();
        let mut dir_entries = self.directory_entries.write().unwrap();
        let mut stats = self.stats.write().unwrap();

        if let Some(dir) = dir_entries.get_mut(&parent_inode) {
            if let Some(&inode) = dir.get(&name) {
                let dir_entry_key = format!("{}:{}", parent_inode, name);
                let _ = self.db.delete_cf(cf_dir_entries, dir_entry_key.as_bytes());

                let inode_key = inode.to_be_bytes();
                let _ = self.db.delete_cf(cf_inodes, inode_key);

                dir.remove(&name);
                if let Some(info) = inodes.remove(&inode) {
                    stats.inode_count -= 1;
                    if matches!(info.file_type, FileType::File) {
                        stats.file_count -= 1;
                    }
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

        let mut inodes = self.inodes.write().unwrap();
        let mut dir_entries = self.directory_entries.write().unwrap();
        let mut stats = self.stats.write().unwrap();

        inodes.insert(inode, inode_info);

        dir_entries.entry(parent_inode).or_default();
        if let Some(dir) = dir_entries.get_mut(&parent_inode) {
            dir.insert(name, inode);
        }

        dir_entries.entry(inode).or_default();

        stats.inode_count += 1;
        stats.dir_count += 1;
        self.save_stats();

        info!(
            "Shard {} created directory: inode={}, parent_inode={}",
            self.shard_id.0, inode, parent_inode
        );
    }

    fn delete_directory(&self, parent_inode: u64, name: String) {
        let cf_inodes = self.db.cf_handle(CF_INODES).unwrap();
        let cf_dir_entries = self.db.cf_handle(CF_DIR_ENTRIES).unwrap();

        let mut inodes = self.inodes.write().unwrap();
        let mut dir_entries = self.directory_entries.write().unwrap();
        let mut stats = self.stats.write().unwrap();

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
                    stats.inode_count -= 1;
                    if matches!(info.file_type, FileType::Directory) {
                        stats.dir_count -= 1;
                    }
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
}
