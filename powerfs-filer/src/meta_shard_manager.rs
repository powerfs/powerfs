use log::info;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::raft_group_manager::{Peer, RaftGroupManager, ShardCommand, ShardId};
use crate::shard_store::{InodeInfo, ShardStats, ShardStore};
use crate::shard_strategy::ShardStrategy;

#[derive(Debug, Clone, Serialize)]
pub struct ShardDetail {
    pub shard_id: u64,
    pub inode_range_start: u64,
    pub inode_range_end: u64,
    pub is_leader: bool,
    pub term: u64,
    pub commit_index: u64,
    pub applied_index: u64,
    pub inode_count: u64,
    pub file_count: u64,
    pub dir_count: u64,
    pub write_qps: u64,
    pub read_qps: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct FilerStatus {
    pub shard_count: u64,
    pub leader_count: u64,
    pub total_inodes: u64,
    pub total_files: u64,
    pub total_dirs: u64,
    pub buckets: Vec<String>,
}

pub struct MetaShardManager {
    raft_group_manager: Arc<RaftGroupManager>,
    shard_stores: RwLock<HashMap<ShardId, Arc<ShardStore>>>,
    shard_strategy: Arc<ShardStrategy>,
    inode_generator: Arc<RwLock<u64>>,
    data_path: String,
    root_inodes: RwLock<HashMap<String, u64>>,
}

impl MetaShardManager {
    pub fn new(
        raft_group_manager: Arc<RaftGroupManager>,
        shard_strategy: Arc<ShardStrategy>,
        data_path: String,
    ) -> Self {
        Self {
            raft_group_manager,
            shard_stores: RwLock::new(HashMap::new()),
            shard_strategy,
            inode_generator: Arc::new(RwLock::new(1)),
            data_path,
            root_inodes: RwLock::new(HashMap::new()),
        }
    }

    pub async fn create_shard(&self, shard_id: ShardId, peers: Vec<Peer>) -> Result<(), String> {
        let inode_range = self.shard_strategy.get_shard_range(shard_id);

        let group = self
            .raft_group_manager
            .create_group(shard_id, peers)
            .await?;

        let mut group_lock = group.write().await;
        let mut apply_rx = group_lock.take_apply_rx();

        let db_path = format!("{}/shard_{}_data", self.data_path, shard_id.0);
        let shard_store = Arc::new(
            ShardStore::new(shard_id, inode_range, &db_path)
                .map_err(|e| format!("failed to create shard store: {}", e))?,
        );

        {
            let mut stores = self.shard_stores.write().unwrap();
            stores.insert(shard_id, shard_store.clone());
        }

        tokio::spawn(async move {
            while let Some(entry) = apply_rx.recv().await {
                shard_store.apply_command(entry.command);
            }
        });

        info!("Created shard {} with range {:?}", shard_id.0, inode_range);
        Ok(())
    }

    pub async fn create_file(&self, parent_inode: u64, name: &str) -> Result<InodeInfo, String> {
        let shard_id = self.shard_strategy.calculate_shard(parent_inode);

        let shard_store = {
            let stores = self.shard_stores.read().unwrap();
            stores
                .get(&shard_id)
                .ok_or_else(|| format!("shard {} not found", shard_id.0))?
                .clone()
        };

        let inode = self.generate_inode();

        let cmd = ShardCommand::CreateFile {
            parent_inode,
            name: name.to_string(),
            inode,
        };

        self.raft_group_manager
            .propose(shard_id, cmd.serialize())
            .await?;

        let mut retries = 0;
        while retries < 10 {
            if let Some(info) = shard_store.get_inode(inode) {
                return Ok(info);
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            retries += 1;
        }

        Err("failed to create file: timeout waiting for apply".to_string())
    }

    pub async fn update_file(&self, inode: u64, size: u64, mtime: u64) -> Result<(), String> {
        let shard_id = self.shard_strategy.calculate_shard(inode);

        let cmd = ShardCommand::UpdateFile { inode, size, mtime };
        self.raft_group_manager
            .propose(shard_id, cmd.serialize())
            .await?;

        Ok(())
    }

    pub async fn delete_file(&self, parent_inode: u64, name: &str) -> Result<(), String> {
        let shard_id = self.shard_strategy.calculate_shard(parent_inode);

        {
            let stores = self.shard_stores.read().unwrap();
            let shard_store = stores
                .get(&shard_id)
                .ok_or_else(|| format!("shard {} not found", shard_id.0))?;

            if shard_store.lookup(parent_inode, name).is_none() {
                return Err("file not found".to_string());
            }
        }

        let cmd = ShardCommand::DeleteFile {
            parent_inode,
            name: name.to_string(),
        };

        self.raft_group_manager
            .propose(shard_id, cmd.serialize())
            .await?;

        Ok(())
    }

    pub async fn create_directory(
        &self,
        parent_inode: u64,
        name: &str,
    ) -> Result<InodeInfo, String> {
        let shard_id = self.shard_strategy.calculate_shard(parent_inode);

        let shard_store = {
            let stores = self.shard_stores.read().unwrap();
            stores
                .get(&shard_id)
                .ok_or_else(|| format!("shard {} not found", shard_id.0))?
                .clone()
        };

        let inode = self.generate_inode();

        let cmd = ShardCommand::CreateDirectory {
            parent_inode,
            name: name.to_string(),
            inode,
        };

        self.raft_group_manager
            .propose(shard_id, cmd.serialize())
            .await?;

        let mut retries = 0;
        while retries < 10 {
            if let Some(info) = shard_store.get_inode(inode) {
                return Ok(info);
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            retries += 1;
        }

        Err("failed to create directory: timeout waiting for apply".to_string())
    }

    pub async fn delete_directory(&self, parent_inode: u64, name: &str) -> Result<(), String> {
        let shard_id = self.shard_strategy.calculate_shard(parent_inode);

        {
            let stores = self.shard_stores.read().unwrap();
            let shard_store = stores
                .get(&shard_id)
                .ok_or_else(|| format!("shard {} not found", shard_id.0))?;

            if shard_store.lookup(parent_inode, name).is_none() {
                return Err("directory not found".to_string());
            }
        }

        let cmd = ShardCommand::DeleteDirectory {
            parent_inode,
            name: name.to_string(),
        };

        self.raft_group_manager
            .propose(shard_id, cmd.serialize())
            .await?;

        Ok(())
    }

    pub async fn rename(
        &self,
        old_parent_inode: u64,
        old_name: &str,
        new_parent_inode: u64,
        new_name: &str,
    ) -> Result<(), String> {
        let old_shard = self.shard_strategy.calculate_shard(old_parent_inode);
        let new_shard = self.shard_strategy.calculate_shard(new_parent_inode);

        if old_shard == new_shard {
            let cmd = ShardCommand::Rename {
                old_parent_inode,
                old_name: old_name.to_string(),
                new_parent_inode,
                new_name: new_name.to_string(),
            };

            self.raft_group_manager
                .propose(old_shard, cmd.serialize())
                .await?;
            Ok(())
        } else {
            Err("cross-shard rename not supported yet".to_string())
        }
    }

    pub fn lookup(&self, parent_inode: u64, name: &str) -> Option<InodeInfo> {
        let shard_id = self.shard_strategy.calculate_shard(parent_inode);

        let stores = self.shard_stores.read().unwrap();
        let shard_store = stores.get(&shard_id)?;

        shard_store.lookup(parent_inode, name)
    }

    pub fn get_inode(&self, inode: u64) -> Option<InodeInfo> {
        let shard_id = self.shard_strategy.calculate_shard(inode);

        let stores = self.shard_stores.read().unwrap();
        let shard_store = stores.get(&shard_id)?;

        shard_store.get_inode(inode)
    }

    pub fn list_directory(&self, parent_inode: u64) -> Vec<InodeInfo> {
        let shard_id = self.shard_strategy.calculate_shard(parent_inode);

        let stores = self.shard_stores.read().unwrap();

        if let Some(shard_store) = stores.get(&shard_id) {
            shard_store.list_directory(parent_inode)
        } else {
            Vec::new()
        }
    }

    pub fn get_shard_stats(&self, shard_id: ShardId) -> Option<ShardStats> {
        let stores = self.shard_stores.read().unwrap();
        stores.get(&shard_id).map(|s| s.get_stats())
    }

    pub fn list_shards(&self) -> Vec<ShardId> {
        self.shard_stores.read().unwrap().keys().cloned().collect()
    }

    fn generate_inode(&self) -> u64 {
        let mut gen = self.inode_generator.write().unwrap();
        let inode = *gen;
        *gen += 1;
        inode
    }

    pub fn get_shard_strategy(&self) -> Arc<ShardStrategy> {
        self.shard_strategy.clone()
    }

    pub async fn create_file_with_shard(
        &self,
        parent_inode: u64,
        name: &str,
        shard_id: ShardId,
    ) -> Result<u64, String> {
        {
            let stores = self.shard_stores.read().unwrap();
            if stores.get(&shard_id).is_none() {
                return Err(format!("shard {} not found", shard_id.0));
            }
        }

        let inode = self.generate_inode();

        let cmd = ShardCommand::CreateFile {
            parent_inode,
            name: name.to_string(),
            inode,
        };

        self.raft_group_manager
            .propose(shard_id, cmd.serialize())
            .await?;

        let mut retries = 0;
        while retries < 10 {
            if let Ok(stores) = self.shard_stores.read() {
                if let Some(shard_store) = stores.get(&shard_id) {
                    if shard_store.get_inode(inode).is_some() {
                        return Ok(inode);
                    }
                }
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            retries += 1;
        }

        Err("failed to create file: timeout waiting for apply".to_string())
    }

    pub async fn delete_file_by_inode(&self, inode: u64, shard_id: ShardId) -> Result<(), String> {
        let (parent_inode, name) = {
            let stores = self.shard_stores.read().unwrap();
            let shard_store = stores
                .get(&shard_id)
                .ok_or_else(|| format!("shard {} not found", shard_id.0))?;

            let inode_info = shard_store
                .get_inode(inode)
                .ok_or_else(|| "file not found".to_string())?;

            (inode_info.parent_inode, inode_info.name.clone())
        };

        let cmd = ShardCommand::DeleteFile { parent_inode, name };

        self.raft_group_manager
            .propose(shard_id, cmd.serialize())
            .await?;
        Ok(())
    }

    pub async fn delete_directory_by_inode(
        &self,
        inode: u64,
        shard_id: ShardId,
    ) -> Result<(), String> {
        let (parent_inode, name) = {
            let stores = self.shard_stores.read().unwrap();
            let shard_store = stores
                .get(&shard_id)
                .ok_or_else(|| format!("shard {} not found", shard_id.0))?;

            let inode_info = shard_store
                .get_inode(inode)
                .ok_or_else(|| "directory not found".to_string())?;

            (inode_info.parent_inode, inode_info.name.clone())
        };

        let cmd = ShardCommand::DeleteDirectory { parent_inode, name };

        self.raft_group_manager
            .propose(shard_id, cmd.serialize())
            .await?;
        Ok(())
    }

    pub async fn update_entry(
        &self,
        inode: u64,
        shard_id: ShardId,
        size: u64,
    ) -> Result<(), String> {
        let cmd = ShardCommand::UpdateFile {
            inode,
            size,
            mtime: chrono::Utc::now().timestamp_millis() as u64 * 1_000_000,
        };
        self.raft_group_manager
            .propose(shard_id, cmd.serialize())
            .await?;
        Ok(())
    }

    pub async fn rename_entry(
        &self,
        old_parent_ino: u64,
        old_name: &str,
        new_parent_ino: u64,
        new_name: &str,
        old_shard_id: ShardId,
        new_shard_id: ShardId,
    ) -> Result<(), String> {
        if old_shard_id == new_shard_id {
            let cmd = ShardCommand::Rename {
                old_parent_inode: old_parent_ino,
                old_name: old_name.to_string(),
                new_parent_inode: new_parent_ino,
                new_name: new_name.to_string(),
            };

            self.raft_group_manager
                .propose(old_shard_id, cmd.serialize())
                .await?;
            Ok(())
        } else {
            Err("cross-shard rename not supported yet".to_string())
        }
    }

    pub async fn list_entries(
        &self,
        parent_inode: u64,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<InodeInfo>, String> {
        let stores = self.shard_stores.read().unwrap();
        let shard_store = stores
            .get(&shard_id)
            .ok_or_else(|| format!("shard {} not found", shard_id.0))?;

        let entries = shard_store.list_directory(parent_inode);
        Ok(entries.into_iter().take(limit).collect())
    }

    pub async fn lookup_entry(
        &self,
        parent_inode: u64,
        name: &str,
        shard_id: ShardId,
    ) -> Result<u64, String> {
        let stores = self.shard_stores.read().unwrap();
        let shard_store = stores
            .get(&shard_id)
            .ok_or_else(|| format!("shard {} not found", shard_id.0))?;

        let inode_info = shard_store
            .lookup(parent_inode, name)
            .ok_or_else(|| "entry not found".to_string())?;

        Ok(inode_info.inode)
    }

    pub async fn get_entry(&self, inode: u64, shard_id: ShardId) -> Result<InodeInfo, String> {
        let stores = self.shard_stores.read().unwrap();
        let shard_store = stores
            .get(&shard_id)
            .ok_or_else(|| format!("shard {} not found", shard_id.0))?;

        shard_store
            .get_inode(inode)
            .ok_or_else(|| "entry not found".to_string())
    }

    pub async fn get_shard_store(&self, shard_id: ShardId) -> Result<Arc<ShardStore>, String> {
        let stores = self.shard_stores.read().unwrap();
        stores
            .get(&shard_id)
            .cloned()
            .ok_or_else(|| format!("shard {} not found", shard_id.0))
    }

    pub async fn resolve_path(&self, path: &str) -> Result<u64, String> {
        let parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();
        if parts.is_empty() {
            return Err("empty path".to_string());
        }

        let bucket = parts[0];
        let root_inodes = self.root_inodes.read().unwrap();
        let mut current_inode = *root_inodes
            .get(bucket)
            .ok_or_else(|| format!("bucket {} not found", bucket))?;

        for part in parts[1..].iter() {
            let shard_id = self.shard_strategy.calculate_shard(current_inode);
            let stores = self.shard_stores.read().unwrap();
            let shard_store = stores
                .get(&shard_id)
                .ok_or_else(|| format!("shard {} not found", shard_id.0))?;

            let inode_info = shard_store
                .lookup(current_inode, part)
                .ok_or_else(|| format!("path component {} not found", part))?;

            current_inode = inode_info.inode;
        }

        Ok(current_inode)
    }

    pub fn register_root_inode(&self, bucket: &str, inode: u64) {
        let mut root_inodes = self.root_inodes.write().unwrap();
        root_inodes.insert(bucket.to_string(), inode);
    }

    // ===== S3 object metadata operations (backed by sharded Raft + RocksDB) =====

    /// Get the root inode for a bucket, creating it if it does not exist.
    pub async fn ensure_bucket_root(&self, bucket: &str) -> Result<u64, String> {
        {
            let roots = self.root_inodes.read().unwrap();
            if let Some(&inode) = roots.get(bucket) {
                return Ok(inode);
            }
        }
        // Create a root directory inode for the bucket at parent inode 0.
        let inode = self.generate_inode();
        let shard_id = self.shard_strategy.calculate_shard(0);
        let cmd = ShardCommand::CreateDirectory {
            parent_inode: 0,
            name: bucket.to_string(),
            inode,
        };
        self.raft_group_manager
            .propose(shard_id, cmd.serialize())
            .await?;

        // Wait for apply
        let mut retries = 0;
        while retries < 10 {
            let applied = {
                let stores = self.shard_stores.read().unwrap();
                stores
                    .get(&shard_id)
                    .map(|s| s.get_inode(inode).is_some())
                    .unwrap_or(false)
            };
            if applied {
                self.register_root_inode(bucket, inode);
                return Ok(inode);
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            retries += 1;
        }
        Err("failed to create bucket root: timeout waiting for apply".to_string())
    }

    /// Create an S3 object entry (file inode with fid/volume_id/etag) under a bucket root.
    pub async fn put_object_entry(
        &self,
        bucket_root_inode: u64,
        key: &str,
        size: u64,
        fid: &str,
        volume_id: u32,
        etag: &str,
    ) -> Result<u64, String> {
        let shard_id = self.shard_strategy.calculate_shard(bucket_root_inode);

        {
            let stores = self.shard_stores.read().unwrap();
            if stores.get(&shard_id).is_none() {
                return Err(format!("shard {} not found", shard_id.0));
            }
        }

        let inode = self.generate_inode();
        let cmd = ShardCommand::PutObject {
            parent_inode: bucket_root_inode,
            name: key.to_string(),
            inode,
            size,
            fid: fid.to_string(),
            volume_id,
            etag: etag.to_string(),
        };

        self.raft_group_manager
            .propose(shard_id, cmd.serialize())
            .await?;

        let mut retries = 0;
        while retries < 10 {
            let applied = {
                let stores = self.shard_stores.read().unwrap();
                stores
                    .get(&shard_id)
                    .map(|s| s.get_inode(inode).is_some())
                    .unwrap_or(false)
            };
            if applied {
                return Ok(inode);
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            retries += 1;
        }

        Err("failed to put object: timeout waiting for apply".to_string())
    }

    /// Look up an S3 object entry by bucket root inode and key.
    pub fn get_object_entry(&self, bucket_root_inode: u64, key: &str) -> Option<InodeInfo> {
        let shard_id = self.shard_strategy.calculate_shard(bucket_root_inode);
        let stores = self.shard_stores.read().unwrap();
        let store = stores.get(&shard_id)?;
        store.lookup(bucket_root_inode, key)
    }

    /// Delete an S3 object entry by bucket root inode and key.
    pub async fn delete_object_entry(
        &self,
        bucket_root_inode: u64,
        key: &str,
    ) -> Result<(), String> {
        let shard_id = self.shard_strategy.calculate_shard(bucket_root_inode);

        let exists = {
            let stores = self.shard_stores.read().unwrap();
            let store = stores
                .get(&shard_id)
                .ok_or_else(|| format!("shard {} not found", shard_id.0))?;
            store.lookup(bucket_root_inode, key).is_some()
        };

        if !exists {
            return Err("object not found".to_string());
        }

        let cmd = ShardCommand::DeleteFile {
            parent_inode: bucket_root_inode,
            name: key.to_string(),
        };
        self.raft_group_manager
            .propose(shard_id, cmd.serialize())
            .await?;
        Ok(())
    }

    /// List S3 object entries under a bucket root inode.
    pub fn list_object_entries(&self, bucket_root_inode: u64) -> Vec<InodeInfo> {
        let shard_id = self.shard_strategy.calculate_shard(bucket_root_inode);
        let stores = self.shard_stores.read().unwrap();
        match stores.get(&shard_id) {
            Some(store) => store
                .list_directory(bucket_root_inode)
                .into_iter()
                .filter(|info| matches!(info.file_type, crate::shard_store::FileType::File))
                .collect(),
            None => Vec::new(),
        }
    }

    pub async fn list_shards_detail(&self) -> Vec<ShardDetail> {
        // Collect shard data under the read lock, then drop the guard before
        // awaiting on raft_group_manager (std::sync::RwLock is not Send).
        let shard_data: Vec<(ShardId, ShardStats, (u64, u64))> = {
            let stores = self.shard_stores.read().unwrap();
            stores
                .iter()
                .map(|(shard_id, store)| (*shard_id, store.get_stats(), store.get_inode_range()))
                .collect()
        };

        let mut details = Vec::new();
        for (shard_id, stats, range) in shard_data {
            let (is_leader, term, commit_index, applied_index) = self
                .raft_group_manager
                .get_shard_status(shard_id)
                .await
                .unwrap_or((false, 0, 0, 0));
            details.push(ShardDetail {
                shard_id: shard_id.0,
                inode_range_start: range.0,
                inode_range_end: range.1,
                is_leader,
                term,
                commit_index,
                applied_index,
                inode_count: stats.inode_count,
                file_count: stats.file_count,
                dir_count: stats.dir_count,
                write_qps: stats.write_qps,
                read_qps: stats.read_qps,
            });
        }
        details
    }

    pub async fn get_shard_detail(&self, shard_id: ShardId) -> Option<ShardDetail> {
        // Collect data under the read lock, then drop the guard before awaiting.
        let (stats, range) = {
            let stores = self.shard_stores.read().unwrap();
            let store = stores.get(&shard_id)?;
            (store.get_stats(), store.get_inode_range())
        };

        let (is_leader, term, commit_index, applied_index) = self
            .raft_group_manager
            .get_shard_status(shard_id)
            .await
            .unwrap_or((false, 0, 0, 0));
        Some(ShardDetail {
            shard_id: shard_id.0,
            inode_range_start: range.0,
            inode_range_end: range.1,
            is_leader,
            term,
            commit_index,
            applied_index,
            inode_count: stats.inode_count,
            file_count: stats.file_count,
            dir_count: stats.dir_count,
            write_qps: stats.write_qps,
            read_qps: stats.read_qps,
        })
    }

    pub async fn get_filer_status(&self) -> FilerStatus {
        let details = self.list_shards_detail().await;
        let leader_count = details.iter().filter(|d| d.is_leader).count() as u64;
        let total_inodes = details.iter().map(|d| d.inode_count).sum();
        let total_files = details.iter().map(|d| d.file_count).sum();
        let total_dirs = details.iter().map(|d| d.dir_count).sum();
        let buckets: Vec<String> = self.root_inodes.read().unwrap().keys().cloned().collect();
        FilerStatus {
            shard_count: details.len() as u64,
            leader_count,
            total_inodes,
            total_files,
            total_dirs,
            buckets,
        }
    }
}
