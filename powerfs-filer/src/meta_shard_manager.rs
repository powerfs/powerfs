use log::{debug, info, warn};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use crate::crdt_orset::{
    DirEntryOrset, EntryTag, MergeResult, ServerDirORSet, ServerVectorClock, Tombstone,
};
use crate::raft_group_manager::{Peer, RaftGroupManager, ShardCommand, ShardId};
use crate::shard_store::{InodeInfo, ShardStats, ShardStore};
use crate::shard_strategy::ShardStrategy;

// POSIX 根 inode (固定为 1，inode 0 保留给虚拟根)
pub const POSIX_ROOT_INODE: u64 = 1;

#[derive(Debug, Clone)]
struct LeaseInfo {
    inode: u64,
    client_id: String,
    expires_at: Instant,
    epoch: u64,
}

// Delta Log: stores applied delta operations for incremental sync
#[derive(Debug, Clone)]
struct DeltaLogEntry {
    client_id: String,
    seq: u64,
    delta: crate::powerfs::DeltaOp,
}

struct DeltaLog {
    entries: RwLock<Vec<DeltaLogEntry>>,
    max_size: usize,
}

impl DeltaLog {
    fn new(max_size: usize) -> Self {
        Self {
            entries: RwLock::new(Vec::new()),
            max_size,
        }
    }

    fn append(&self, client_id: &str, seq: u64, delta: crate::powerfs::DeltaOp) {
        let mut entries = self.entries.write().unwrap();
        entries.push(DeltaLogEntry {
            client_id: client_id.to_string(),
            seq,
            delta,
        });
        // Trim old entries if exceeding max_size
        if entries.len() > self.max_size {
            let excess = entries.len() - self.max_size;
            entries.drain(0..excess);
        }
    }

    fn get_since(&self, client_vclock: &HashMap<String, u64>) -> Vec<crate::powerfs::DeltaOp> {
        let entries = self.entries.read().unwrap();
        entries
            .iter()
            .filter(|e| {
                let client_seq = client_vclock.get(&e.client_id).copied().unwrap_or(0);
                e.seq > client_seq
            })
            .map(|e| e.delta.clone())
            .collect()
    }
}

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

// ========================================================================
// CRDT 管理接口类型
// ========================================================================

#[derive(Debug, Clone, Serialize)]
pub struct CrdtOverview {
    pub total_orset_states: usize,
    pub shard_states: HashMap<u64, Vec<OrsetStateInfo>>,
    pub shard_vclocks: HashMap<u64, HashMap<String, u64>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OrsetStateInfo {
    pub dir_ino: u64,
    pub entry_count: usize,
    pub tombstone_count: usize,
    pub vclock_entries: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct OrsetStateDetail {
    pub dir_ino: u64,
    pub entries: HashMap<String, DirEntryOrset>,
    pub entry_tags: HashMap<String, HashSet<EntryTag>>,
    pub tombstones: HashMap<String, Vec<Tombstone>>,
    pub vclock: ServerVectorClock,
}

pub struct MetaShardManager {
    raft_group_manager: Arc<RaftGroupManager>,
    shard_stores: RwLock<HashMap<ShardId, Arc<ShardStore>>>,
    shard_strategy: Arc<ShardStrategy>,
    inode_generator: Arc<RwLock<u64>>,
    data_path: String,
    root_inodes: RwLock<HashMap<String, u64>>,
    leases: RwLock<HashMap<String, LeaseInfo>>,
    lease_epoch: std::sync::atomic::AtomicU64,
    // CRDT: Per-shard vector clocks for tracking all client operations
    shard_vclocks: RwLock<HashMap<ShardId, ServerVectorClock>>,
    // CRDT: Delta log for incremental sync (backward compatibility)
    delta_logs: RwLock<HashMap<ShardId, Arc<DeltaLog>>>,
    // CRDT: Per-shard per-directory OR-Set state
    orset_states: RwLock<HashMap<(ShardId, u64), ServerDirORSet>>,
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
            leases: RwLock::new(HashMap::new()),
            lease_epoch: std::sync::atomic::AtomicU64::new(1),
            shard_vclocks: RwLock::new(HashMap::new()),
            delta_logs: RwLock::new(HashMap::new()),
            orset_states: RwLock::new(HashMap::new()),
        }
    }

    fn get_or_create_delta_log(&self, shard_id: ShardId) -> Arc<DeltaLog> {
        let mut logs = self.delta_logs.write().unwrap();
        if let Some(log) = logs.get(&shard_id) {
            return log.clone();
        }
        // Create new delta log with max 10000 entries
        let log = Arc::new(DeltaLog::new(10000));
        logs.insert(shard_id, log.clone());
        log
    }

    // ========================================================================
    // CRDT OR-Set 辅助方法
    // ========================================================================

    /// 获取或创建 per-shard per-directory 的 OR-Set 状态
    #[allow(dead_code)]
    fn get_or_create_orset(&self, shard_id: ShardId, dir_ino: u64) -> ServerDirORSet {
        let key = (shard_id, dir_ino);
        let mut states = self.orset_states.write().unwrap();
        if let Some(state) = states.get(&key) {
            return state.clone();
        }
        let state = ServerDirORSet::new(dir_ino);
        states.insert(key, state.clone());
        state
    }

    /// Atomically get-or-create, modify, and update OR-Set state
    /// This prevents race conditions between read and write operations
    fn modify_orset<F>(
        &self,
        shard_id: ShardId,
        dir_ino: u64,
        f: F,
    ) -> (ServerDirORSet, MergeResult)
    where
        F: FnOnce(&mut ServerDirORSet) -> MergeResult,
    {
        let key = (shard_id, dir_ino);
        let mut states = self.orset_states.write().unwrap();
        let mut orset = if let Some(state) = states.get(&key) {
            state.clone()
        } else {
            ServerDirORSet::new(dir_ino)
        };

        let merge_result = f(&mut orset);

        // Update the state in the map
        states.insert(key, orset.clone());
        drop(states); // Release lock before doing IO

        // Persist to RocksDB (after releasing lock to avoid blocking)
        if let Some(store) = self.shard_stores.read().unwrap().get(&shard_id).cloned() {
            store.save_orset_state(dir_ino, &orset);
            for (entry_key, tombstones) in &orset.tombstones {
                store.save_tombstones(entry_key, tombstones);
            }
        }

        (orset, merge_result)
    }

    /// 获取或创建 per-shard 的 VectorClock
    fn get_or_create_shard_vclock(&self, shard_id: ShardId) -> ServerVectorClock {
        let mut vclocks = self.shard_vclocks.write().unwrap();
        if let Some(vclock) = vclocks.get(&shard_id) {
            return vclock.clone();
        }
        let vclock = ServerVectorClock::new();
        vclocks.insert(shard_id, vclock.clone());
        vclock
    }

    /// 更新 per-shard 的 VectorClock
    fn update_shard_vclock(&self, shard_id: ShardId, vclock: ServerVectorClock) {
        let mut vclocks = self.shard_vclocks.write().unwrap();
        vclocks.insert(shard_id, vclock);
    }

    /// 更新 per-shard per-directory 的 OR-Set 状态，并持久化到 RocksDB
    #[allow(dead_code)]
    fn update_orset(&self, shard_id: ShardId, dir_ino: u64, state: ServerDirORSet) {
        let key = (shard_id, dir_ino);
        let mut states = self.orset_states.write().unwrap();
        states.insert(key, state.clone());
        drop(states);

        // 持久化到 RocksDB
        if let Some(store) = self.shard_stores.read().unwrap().get(&shard_id).cloned() {
            store.save_orset_state(dir_ino, &state);

            // 同时持久化 tombstones
            for (entry_key, tombstones) in &state.tombstones {
                store.save_tombstones(entry_key, tombstones);
            }
        }
    }

    /// 从 DeltaOp 中提取父目录 inode
    fn get_dir_ino_from_delta(&self, delta: &crate::powerfs::DeltaOp) -> Option<u64> {
        match &delta.op {
            Some(crate::powerfs::delta_op::Op::Add(entry)) => Some(entry.parent_ino),
            Some(crate::powerfs::delta_op::Op::Remove(entry_id)) => Some(entry_id.parent_ino),
            Some(crate::powerfs::delta_op::Op::Rename(rename_op)) => {
                // 返回旧位置的父目录 inode
                Some(rename_op.old_parent_ino)
            }
            Some(crate::powerfs::delta_op::Op::SetAttr(setattr_op)) => {
                // SetAttr 只有 inode，需要查找父目录
                let ino = setattr_op.inode;
                let stores = self.shard_stores.read().unwrap();
                for store in stores.values() {
                    if let Some(info) = store.get_inode(ino) {
                        return Some(info.parent_inode);
                    }
                }
                None
            }
            None => None,
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

        // 从 RocksDB 加载 OR-Set 状态
        let orset_states = shard_store.load_all_orset_states();
        {
            let mut states = self.orset_states.write().unwrap();
            for (dir_ino, state) in orset_states {
                states.insert((shard_id, dir_ino), state);
            }
        }
        info!(
            "Loaded {} OR-Set states for shard {}",
            shard_store.load_all_orset_states().len(),
            shard_id.0
        );

        // 启动 tombstone 清理任务 (每小时执行一次)
        let shard_store_clone = shard_store.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(3600));
            loop {
                interval.tick().await;
                let cleaned = shard_store_clone.cleanup_expired_tombstones(24); // 24 hours TTL
                if cleaned > 0 {
                    info!(
                        "Cleaned {} expired tombstones for shard {}",
                        cleaned, shard_id.0
                    );
                }
            }
        });

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

    /// Create directory for a given path, auto-creating parent directories (mkdir -p behavior)
    pub async fn create_directory_for_path(&self, path: &str) -> Result<u64, String> {
        let parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();
        if parts.is_empty() {
            return Ok(POSIX_ROOT_INODE);
        }

        let mut current_inode = POSIX_ROOT_INODE;
        for part in &parts {
            // Check if this component already exists
            let lookup_shard = self.shard_strategy.calculate_shard(current_inode);
            let exists = {
                let stores = self.shard_stores.read().unwrap();
                if let Some(store) = stores.get(&lookup_shard) {
                    store.lookup(current_inode, part).is_some()
                } else {
                    false
                }
            };

            if !exists {
                // Create this directory component
                let info = self.create_directory(current_inode, part).await?;
                current_inode = info.inode;
            } else {
                // Look up the inode for existing directory
                let lookup_shard = self.shard_strategy.calculate_shard(current_inode);
                let ino = {
                    let stores = self.shard_stores.read().unwrap();
                    if let Some(store) = stores.get(&lookup_shard) {
                        store
                            .lookup(current_inode, part)
                            .map(|e| e.inode)
                            .unwrap_or(0)
                    } else {
                        0
                    }
                };
                if ino == 0 {
                    return Err(format!("failed to find existing directory: {}", part));
                }
                current_inode = ino;
            }
        }
        Ok(current_inode)
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

    /// Resolve a POSIX flat path (e.g., "/dir1/file1") starting from POSIX_ROOT_INODE.
    /// This is used by FUSE clients.
    pub async fn resolve_flat_path(&self, path: &str) -> Result<u64, String> {
        let parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();

        // Root path "/" returns the POSIX root inode
        if parts.is_empty() {
            return Ok(POSIX_ROOT_INODE);
        }

        let mut current_inode = POSIX_ROOT_INODE;

        for part in parts.iter() {
            let shard_id = self.shard_strategy.calculate_shard(current_inode);
            let stores = self.shard_stores.read().unwrap();
            let shard_store = stores
                .get(&shard_id)
                .ok_or_else(|| format!("shard {} not found", shard_id.0))?;

            let inode_info = shard_store.lookup(current_inode, part).ok_or_else(|| {
                format!(
                    "path component '{}' not found in directory {}",
                    part, current_inode
                )
            })?;

            current_inode = inode_info.inode;
        }

        Ok(current_inode)
    }

    /// Check if POSIX root inode exists in the store
    pub fn has_posix_root(&self) -> bool {
        let shard_id = self.shard_strategy.calculate_shard(POSIX_ROOT_INODE);
        let stores = self.shard_stores.read().unwrap();
        stores
            .get(&shard_id)
            .map(|s| s.get_inode(POSIX_ROOT_INODE).is_some())
            .unwrap_or(false)
    }

    /// Format POSIX root inode (inode 1, directory "/")
    pub async fn format_posix_root(&self) -> Result<u64, String> {
        // Check if already exists
        if self.has_posix_root() {
            info!("POSIX root inode {} already exists", POSIX_ROOT_INODE);
            return Ok(POSIX_ROOT_INODE);
        }

        let shard_id = self.shard_strategy.calculate_shard(POSIX_ROOT_INODE);
        let cmd = ShardCommand::CreateDirectory {
            parent_inode: 0,
            name: "/".to_string(),
            inode: POSIX_ROOT_INODE,
        };
        self.raft_group_manager
            .propose(shard_id, cmd.serialize())
            .await?;

        // Wait for apply
        let mut retries = 0;
        while retries < 20 {
            let applied = {
                let stores = self.shard_stores.read().unwrap();
                stores
                    .get(&shard_id)
                    .map(|s| s.get_inode(POSIX_ROOT_INODE).is_some())
                    .unwrap_or(false)
            };
            if applied {
                info!(
                    "POSIX root inode {} initialized successfully",
                    POSIX_ROOT_INODE
                );
                return Ok(POSIX_ROOT_INODE);
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            retries += 1;
        }
        Err("failed to create POSIX root: timeout waiting for apply".to_string())
    }

    pub fn register_root_inode(&self, bucket: &str, inode: u64) {
        let mut root_inodes = self.root_inodes.write().unwrap();
        root_inodes.insert(bucket.to_string(), inode);

        // Persist to the shard store that owns inode 0
        let shard_id = self.shard_strategy.calculate_shard(0);
        let stores = self.shard_stores.read().unwrap();
        if let Some(store) = stores.get(&shard_id) {
            store.set_root_inode(bucket, inode);
        }
    }

    /// Load root_inodes from all shard stores (called during startup)
    pub fn load_root_inodes_from_shards(&self) {
        let mut root_inodes_map = std::collections::HashMap::new();
        let stores = self.shard_stores.read().unwrap();
        for store in stores.values() {
            for (bucket, inode) in store.list_root_inodes() {
                root_inodes_map.insert(bucket, inode);
            }
        }
        drop(stores);

        let mut root_inodes = self.root_inodes.write().unwrap();
        *root_inodes = root_inodes_map;
        info!("Loaded {} root inodes from shard stores", root_inodes.len());
    }

    /// Get all bucket names
    pub fn list_buckets(&self) -> Vec<String> {
        let root_inodes = self.root_inodes.read().unwrap();
        root_inodes.keys().cloned().collect()
    }

    // ===== S3 object metadata operations (backed by sharded Raft + RocksDB) =====

    /// Get the root inode for a bucket from in-memory cache only (no creation).
    pub fn get_bucket_root(&self, bucket: &str) -> Option<u64> {
        let roots = self.root_inodes.read().unwrap();
        roots.get(bucket).cloned()
    }

    /// Format: Create a root directory inode for the bucket at parent inode 0 and persist it.
    /// This is the "mkfs" operation - should be called once during initial setup.
    pub async fn format_bucket_root(&self, bucket: &str) -> Result<u64, String> {
        // Check if already exists
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
        while retries < 20 {
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
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            retries += 1;
        }
        Err("failed to create bucket root: timeout waiting for apply".to_string())
    }

    /// Ensure the root inode for a bucket, creating it if it does not exist.
    /// This is the legacy method - prefer format_bucket_root for initial setup.
    pub async fn ensure_bucket_root(&self, bucket: &str) -> Result<u64, String> {
        self.format_bucket_root(bucket).await
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

    pub async fn push_delta(
        &self,
        shard_id: ShardId,
        client_id: &str,
        deltas: &[crate::powerfs::DeltaOp],
        client_vclock: &Option<crate::powerfs::VectorClock>,
    ) -> Result<crate::powerfs::VectorClock, String> {
        // CRDT Merge: Apply client deltas to server OR-Set state
        let delta_log = self.get_or_create_delta_log(shard_id);
        let shard_store = {
            let stores = self.shard_stores.read().unwrap();
            stores.get(&shard_id).cloned()
        };

        let mut max_seq = 0u64;

        for delta in deltas {
            let seq = extract_seq_from_delta(delta).unwrap_or(0);
            if seq > max_seq {
                max_seq = seq;
            }

            // Record to delta log for backward compatibility
            delta_log.append(client_id, seq, delta.clone());

            // CRDT Merge: Apply to OR-Set first (atomic operation)
            if let Some(dir_ino) = self.get_dir_ino_from_delta(delta) {
                let tag = EntryTag::new(client_id, seq);

                // Use atomic modify_orset to prevent race conditions
                let (_orset, merge_result) = self.modify_orset(shard_id, dir_ino, |orset| {
                    match &delta.op {
                        Some(crate::powerfs::delta_op::Op::Add(entry)) => {
                            let orset_entry = DirEntryOrset {
                                tag: tag.clone(),
                                inode: entry.inode,
                                name: entry.name.clone(),
                                parent_ino: entry.parent_ino,
                                mode: entry.mode,
                                file_type: crate::shard_store::FileType::File,
                                size: 0,
                                mtime: 0,
                                etag: None,
                            };
                            orset.merge_add(orset_entry)
                        }
                        Some(crate::powerfs::delta_op::Op::Remove(entry_id)) => {
                            orset.merge_remove(entry_id.parent_ino, &entry_id.name, &tag)
                        }
                        Some(crate::powerfs::delta_op::Op::Rename(rename_op)) => orset
                            .merge_rename(
                                rename_op.old_parent_ino,
                                &rename_op.old_name,
                                rename_op.new_parent_ino,
                                &rename_op.new_name,
                                &tag,
                            ),
                        Some(crate::powerfs::delta_op::Op::SetAttr(setattr_op)) => {
                            // SetAttr needs to look up entry info first
                            let ino = setattr_op.inode;
                            let (parent_ino, name) = {
                                let stores = self.shard_stores.read().unwrap();
                                let mut found = None;
                                for store in stores.values() {
                                    if let Some(info) = store.get_inode(ino) {
                                        found = Some((info.parent_inode, info.name.clone()));
                                        break;
                                    }
                                }
                                match found {
                                    Some(v) => v,
                                    None => {
                                        return MergeResult::Applied; // Skip if inode not found
                                    }
                                }
                            };

                            orset.merge_setattr(
                                parent_ino,
                                &name,
                                &tag,
                                setattr_op.size,
                                setattr_op.mtime,
                            )
                        }
                        None => MergeResult::Applied,
                    }
                });

                // Apply to shard store (物理存储)
                if let Some(store) = &shard_store {
                    match merge_result {
                        MergeResult::Applied | MergeResult::Idempotent => {
                            self.apply_delta_to_store(store, delta).await?;
                        }
                        MergeResult::ConcurrentlyAdded => {
                            debug!(
                                "Concurrent Add detected for dir {}: {:?}",
                                dir_ino, merge_result
                            );
                            self.apply_delta_to_store(store, delta).await?;
                        }
                        MergeResult::ConcurrentlyRemoved => {
                            // 并发 Remove: 不物理删除，仅记录 tombstone
                            debug!(
                                "Concurrent Remove detected for dir {}: {:?}",
                                dir_ino, merge_result
                            );
                        }
                        MergeResult::Conflict => {
                            warn!("Conflict detected for dir {}: {:?}", dir_ino, merge_result);
                        }
                    }
                }
            } else if let Some(store) = &shard_store {
                // 无法确定目录的操作，直接应用
                self.apply_delta_to_store(store, delta).await?;
            }
        }

        // Merge client's VectorClock into per-shard VectorClock
        let mut shard_vclock = self.get_or_create_shard_vclock(shard_id);
        if let Some(vclock) = client_vclock {
            for entry in &vclock.entries {
                shard_vclock.observe(&entry.client_id.to_string(), entry.seq);
            }
        } else if max_seq > 0 {
            shard_vclock.observe(client_id, max_seq);
        }
        self.update_shard_vclock(shard_id, shard_vclock.clone());

        Ok(shard_vclock.to_proto())
    }

    pub async fn pull_delta(
        &self,
        shard_id: ShardId,
        _client_id: &str,
        client_vclock: &Option<crate::powerfs::VectorClock>,
    ) -> Result<(Vec<crate::powerfs::DeltaOp>, crate::powerfs::VectorClock), String> {
        // Get delta log for this shard (backward compatibility)
        let delta_log = self.get_or_create_delta_log(shard_id);

        // Convert client's VectorClock to HashMap for comparison
        let client_vclock_map: HashMap<String, u64> = match client_vclock {
            Some(vclock) => {
                let mut map = HashMap::new();
                for entry in &vclock.entries {
                    map.insert(entry.client_id.to_string(), entry.seq);
                }
                map
            }
            None => HashMap::new(),
        };

        // Get deltas that the client hasn't seen yet from delta log
        let mut deltas = delta_log.get_since(&client_vclock_map);

        // Also compute deltas from OR-Set state for more accurate sync
        let orset_deltas = self.compute_orset_deltas(shard_id, &client_vclock_map);
        deltas.extend(orset_deltas);

        // Return per-shard VectorClock
        let shard_vclock = self.get_or_create_shard_vclock(shard_id);
        Ok((deltas, shard_vclock.to_proto()))
    }

    /// 从 OR-Set 状态计算增量变更
    fn compute_orset_deltas(
        &self,
        shard_id: ShardId,
        client_vclock_map: &HashMap<String, u64>,
    ) -> Vec<crate::powerfs::DeltaOp> {
        let mut deltas = Vec::new();
        let states = self.orset_states.read().unwrap();

        for ((sid, _dir_ino), orset) in states.iter() {
            if *sid != shard_id {
                continue;
            }

            // 检查 OR-Set 的 vclock 是否有新的变更
            let orset_vclock = orset.vclock();
            let diff = orset_vclock.diff_against(&ServerVectorClock::from_map(client_vclock_map));

            for (client_id, seq) in diff {
                // 获取该客户端在这个目录下的所有变更
                for entry in orset.entries.values() {
                    if entry.tag.client_id == client_id && entry.tag.seq <= seq {
                        // 添加 Add 操作
                        deltas.push(crate::powerfs::DeltaOp {
                            op: Some(crate::powerfs::delta_op::Op::Add(
                                crate::powerfs::DirEntryOrset {
                                    parent_ino: entry.parent_ino,
                                    name: entry.name.clone(),
                                    inode: entry.inode,
                                    mode: entry.mode,
                                    seq: entry.tag.seq,
                                    client_id: entry.tag.client_id.parse().unwrap_or(0),
                                },
                            )),
                        });
                    }
                }
            }
        }

        deltas
    }

    async fn apply_delta_to_store(
        &self,
        store: &Arc<ShardStore>,
        delta: &crate::powerfs::DeltaOp,
    ) -> Result<(), String> {
        match &delta.op {
            Some(crate::powerfs::delta_op::Op::Add(entry_orset)) => {
                // Create inode and directory entry
                let inode_info = InodeInfo {
                    inode: entry_orset.inode,
                    name: entry_orset.name.clone(),
                    parent_inode: entry_orset.parent_ino,
                    file_type: crate::shard_store::FileType::File, // Simplified, should check mode
                    size: 0,
                    mtime: crate::shard_store::ShardStore::current_time(),
                    atime: crate::shard_store::ShardStore::current_time(),
                    ctime: crate::shard_store::ShardStore::current_time(),
                    mode: entry_orset.mode,
                    uid: 0,
                    gid: 0,
                    blocks: 0,
                    fid: None,
                    volume_id: None,
                    etag: None,
                };
                store.create_inode(inode_info)?;
                store.add_dir_entry(
                    entry_orset.parent_ino,
                    &entry_orset.name,
                    entry_orset.inode,
                )?;
                debug!(
                    "Applied Add delta: {}/{} inode={}",
                    entry_orset.parent_ino, entry_orset.name, entry_orset.inode
                );
            }
            Some(crate::powerfs::delta_op::Op::Remove(entry_id)) => {
                if let Some(inode_info) = store.lookup(entry_id.parent_ino, &entry_id.name) {
                    let inode = inode_info.inode;
                    store.remove_dir_entry(entry_id.parent_ino, &entry_id.name)?;
                    store.delete_inode(inode)?;
                    debug!(
                        "Applied Remove delta: {}/{} inode={}",
                        entry_id.parent_ino, entry_id.name, inode
                    );
                }
            }
            Some(crate::powerfs::delta_op::Op::Rename(rename_op)) => {
                if let Some(inode_info) =
                    store.lookup(rename_op.old_parent_ino, &rename_op.old_name)
                {
                    let inode = inode_info.inode;
                    store.remove_dir_entry(rename_op.old_parent_ino, &rename_op.old_name)?;
                    store.add_dir_entry(rename_op.new_parent_ino, &rename_op.new_name, inode)?;
                    debug!(
                        "Applied Rename delta: {}/{} -> {}/{}",
                        rename_op.old_parent_ino,
                        rename_op.old_name,
                        rename_op.new_parent_ino,
                        rename_op.new_name
                    );
                }
            }
            Some(crate::powerfs::delta_op::Op::SetAttr(setattr_op)) => {
                if let Some(mut inode_info) = store.get_inode(setattr_op.inode) {
                    if setattr_op.size > 0 {
                        inode_info.size = setattr_op.size;
                    }
                    inode_info.mtime = setattr_op.mtime;
                    store.update_inode(inode_info)?;
                    debug!(
                        "Applied SetAttr delta: inode={} size={} mtime={}",
                        setattr_op.inode, setattr_op.size, setattr_op.mtime
                    );
                }
            }
            None => {}
        }
        Ok(())
    }

    pub async fn acquire_lease(
        &self,
        inode: u64,
        _shard_id: ShardId,
        client_id: &str,
        duration_ms: u64,
    ) -> Result<(String, u64), String> {
        let mut leases = self.leases.write().unwrap();

        for (lease_id, info) in leases.iter() {
            if info.inode == inode && info.expires_at > Instant::now() {
                if info.client_id == client_id {
                    return Ok((lease_id.clone(), info.epoch));
                }
                return Err("lease already held by another client".to_string());
            }
        }

        let lease_id = format!("lease_{}_{}", inode, std::process::id());
        let epoch = self
            .lease_epoch
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let expires_at = Instant::now() + Duration::from_millis(duration_ms);

        leases.insert(
            lease_id.clone(),
            LeaseInfo {
                inode,
                client_id: client_id.to_string(),
                expires_at,
                epoch,
            },
        );

        Ok((lease_id, epoch))
    }

    pub async fn release_lease(&self, lease_id: &str) -> Result<(), String> {
        let mut leases = self.leases.write().unwrap();
        if leases.remove(lease_id).is_none() {
            return Err("lease not found".to_string());
        }
        Ok(())
    }

    pub async fn renew_lease(&self, lease_id: &str, duration_ms: u64) -> Result<u64, String> {
        let mut leases = self.leases.write().unwrap();
        let info = leases
            .get_mut(lease_id)
            .ok_or_else(|| "lease not found".to_string())?;

        info.expires_at = Instant::now() + Duration::from_millis(duration_ms);
        Ok(info.epoch)
    }

    /// Step a Raft message to the appropriate shard's Raft group
    pub async fn step_raft_message(
        &self,
        shard_id: ShardId,
        msg: raft::eraftpb::Message,
    ) -> Result<(), String> {
        self.raft_group_manager.step(shard_id, msg).await
    }

    // ========================================================================
    // CRDT 管理接口
    // ========================================================================

    /// 获取所有 OR-Set 状态概览
    pub fn get_crdt_overview(&self) -> CrdtOverview {
        let states = self.orset_states.read().unwrap();
        let vclocks = self.shard_vclocks.read().unwrap();

        let mut shard_states = HashMap::new();
        for ((shard_id, dir_ino), state) in states.iter() {
            let entry = shard_states.entry(shard_id.0).or_insert_with(Vec::new);
            entry.push(OrsetStateInfo {
                dir_ino: *dir_ino,
                entry_count: state.entries.len(),
                tombstone_count: state.tombstones.values().map(|t| t.len()).sum(),
                vclock_entries: state.vclock.entries().len(),
            });
        }

        let mut shard_vclocks_info = HashMap::new();
        for (shard_id, vclock) in vclocks.iter() {
            shard_vclocks_info.insert(shard_id.0, vclock.entries().clone());
        }

        CrdtOverview {
            total_orset_states: states.len(),
            shard_states,
            shard_vclocks: shard_vclocks_info,
        }
    }

    /// 获取指定分片的 OR-Set 状态
    pub fn get_shard_orset_states(&self, shard_id: ShardId) -> Vec<OrsetStateDetail> {
        let states = self.orset_states.read().unwrap();
        states
            .iter()
            .filter(|((sid, _), _)| *sid == shard_id)
            .map(|((_, dir_ino), state)| OrsetStateDetail {
                dir_ino: *dir_ino,
                entries: state.entries.clone(),
                entry_tags: state.entry_tags.clone(),
                tombstones: state.tombstones.clone(),
                vclock: state.vclock.clone(),
            })
            .collect()
    }

    /// 获取指定目录的 OR-Set 状态
    pub fn get_dir_orset_state(&self, shard_id: ShardId, dir_ino: u64) -> Option<ServerDirORSet> {
        let states = self.orset_states.read().unwrap();
        states.get(&(shard_id, dir_ino)).cloned()
    }

    /// 清理过期 Tombstone
    pub fn cleanup_tombstones(&self, ttl_hours: u64) -> usize {
        let mut total_cleaned = 0;
        let stores = self.shard_stores.read().unwrap();
        for store in stores.values() {
            total_cleaned += store.cleanup_expired_tombstones(ttl_hours);
        }
        total_cleaned
    }
}

// Helper function to extract sequence number from DeltaOp
fn extract_seq_from_delta(delta: &crate::powerfs::DeltaOp) -> Option<u64> {
    match &delta.op {
        Some(crate::powerfs::delta_op::Op::Add(entry_orset)) => Some(entry_orset.seq),
        _ => None,
    }
}
