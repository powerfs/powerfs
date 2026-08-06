use log::{debug, info, warn};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use crate::crdt_orset::{
    DirEntryOrset, EntryTag, MergeResult, ServerDirORSet, ServerVectorClock, Tombstone,
};
use crate::raft_group_manager::{Peer, RaftGroupManager, ShardCommand, ShardId};
use crate::shard_store::{FileType, InodeInfo, ShardStats, ShardStore, StoredFileChunk};
use crate::shard_strategy::ShardStrategy;
use crate::volume_router::VolumeRouter;
use powerfs_master::volume_client::VolumeClientPool;

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
    inode_generator: Arc<AtomicU64>,
    /// Filer node id, used to partition the inode space
    /// (each node owns [node_id*1B+1000, (node_id+1)*1B)).
    node_id: u64,
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
        node_id: u64,
    ) -> Self {
        // Use node_id to offset inode generator so that different Filer nodes
        // generate unique inodes. Each node gets a range of 1 billion inodes.
        let inode_base = node_id * 1_000_000_000 + 1000;
        Self {
            raft_group_manager,
            shard_stores: RwLock::new(HashMap::new()),
            shard_strategy,
            inode_generator: Arc::new(AtomicU64::new(inode_base)),
            node_id,
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

        self.raft_group_manager
            .create_group(shard_id, peers)
            .await?;

        let mut apply_rx = self
            .raft_group_manager
            .get_apply_rx(shard_id)
            .await
            .ok_or_else(|| format!("shard {} apply_rx not found", shard_id.0))?;

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
        let t0 = std::time::Instant::now();
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

        let t_propose = std::time::Instant::now();
        self.raft_group_manager
            .propose(shard_id, cmd.serialize())
            .await?;
        let propose_ms = t_propose.elapsed().as_millis();

        let t_poll = std::time::Instant::now();
        let mut retries = 0;
        while retries < 50 {
            if let Some(info) = shard_store.get_inode(inode) {
                let poll_ms = t_poll.elapsed().as_millis();
                let total_ms = t0.elapsed().as_millis();
                log::info!(
                    "create_file latency: propose={}ms, poll={}ms (retries={}), total={}ms, inode={}",
                    propose_ms, poll_ms, retries, total_ms, inode
                );
                return Ok(info);
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
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
        while retries < 50 {
            if let Some(info) = shard_store.get_inode(inode) {
                return Ok(info);
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
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

    /// 遍历所有 shard 的所有 inode, 收集 chunk 映射 (needle_id, volume_id).
    /// 用于 Filer 重启时恢复 Zone counter (P2.5).
    pub fn list_all_chunks(&self) -> Vec<(u64, u64)> {
        let stores = self.shard_stores.read().unwrap();
        let mut result = Vec::new();
        for shard_store in stores.values() {
            result.extend(shard_store.list_all_chunks());
        }
        result
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

    /// Resolve a full path to an inode (component by component lookup)
    pub fn path_to_inode(&self, path: &str) -> Option<InodeInfo> {
        let trimmed = path.trim_start_matches('/');
        if trimmed.is_empty() {
            return Some(InodeInfo {
                inode: 1,
                name: String::new(),
                parent_inode: 0,
                file_type: FileType::Directory,
                size: 0,
                mtime: 0,
                atime: 0,
                ctime: 0,
                mode: 0o755,
                uid: 0,
                gid: 0,
                blocks: 0,
                fid: None,
                volume_id: None,
                etag: None,
                chunks: Vec::new(),
                extended: std::collections::HashMap::new(),
                symlink_target: None,
                nlink: 2,
                version: 0,
                delete_time: 0,
            });
        }

        let mut current_ino: u64 = 1;
        let mut last_info: Option<InodeInfo> = None;

        for component in trimmed.split('/') {
            if component.is_empty() {
                continue;
            }
            let info = self.lookup(current_ino, component)?;
            current_ino = info.inode;
            last_info = Some(info);
        }

        last_info
    }

    pub fn generate_inode(&self) -> u64 {
        self.inode_generator.fetch_add(1, Ordering::SeqCst)
    }

    /// Recover inode_generator by scanning existing inodes in RocksDB.
    ///
    /// Problem: `inode_generator` is an in-memory AtomicU64 initialized to
    /// `node_id * 1B + 1000` on every restart. If the filer allocated inodes
    /// before restart, the counter resets and reuses existing inode numbers,
    /// causing `-ENOSPC` in the kernel (`new_inode called for existing inode`).
    ///
    /// Fix: After all shard stores are loaded, scan `CF_INODES` for the max
    /// inode in this node's range and advance `inode_generator` past it.
    /// This is O(N) in inode count but runs once at startup.
    pub fn recover_inode_generator(&self) {
        let range_start = self.node_id * 1_000_000_000 + 1000;
        let range_end = (self.node_id + 1) * 1_000_000_000;

        let stores = self.shard_stores.read().unwrap();
        let mut max_existing = range_start;
        for store in stores.values() {
            let max = store.get_max_inode_in_range(range_start, range_end);
            if max > max_existing {
                max_existing = max;
            }
        }

        let current = self.inode_generator.load(Ordering::SeqCst);
        if max_existing > current {
            self.inode_generator.store(max_existing, Ordering::SeqCst);
            info!(
                "Recovered inode_generator: {} -> {} (node_id={}, scanned max={})",
                current,
                max_existing,
                self.node_id,
                max_existing - 1
            );
        } else {
            info!(
                "inode_generator base {} is already >= scanned max {} (node_id={})",
                current, max_existing, self.node_id
            );
        }
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
        let t0 = std::time::Instant::now();
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

        let t_propose = std::time::Instant::now();
        self.raft_group_manager
            .propose(shard_id, cmd.serialize())
            .await?;
        let propose_ms = t_propose.elapsed().as_millis();

        let t_poll = std::time::Instant::now();
        let mut retries = 0;
        while retries < 50 {
            if let Ok(stores) = self.shard_stores.read() {
                if let Some(shard_store) = stores.get(&shard_id) {
                    if shard_store.get_inode(inode).is_some() {
                        let poll_ms = t_poll.elapsed().as_millis();
                        let total_ms = t0.elapsed().as_millis();
                        log::info!(
                            "create_file_with_shard latency: propose={}ms, poll={}ms (retries={}), total={}ms, inode={}",
                            propose_ms, poll_ms, retries, total_ms, inode
                        );
                        return Ok(inode);
                    }
                }
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
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

    /// Set inode attributes via Raft consensus
    pub async fn setattr(
        &self,
        inode: u64,
        shard_id: ShardId,
        size: Option<u64>,
        mode: Option<u64>,
        uid: Option<u64>,
        gid: Option<u64>,
    ) -> Result<(), String> {
        let cmd = ShardCommand::SetAttr {
            inode,
            size,
            mode,
            uid,
            gid,
        };

        self.raft_group_manager
            .propose(shard_id, cmd.serialize())
            .await?;

        // Wait for the command to be applied to the state machine.
        // Must check ALL changed fields (mode, uid, gid, size), not just mode,
        // otherwise chown (UID|GID only) returns before Raft applies the change,
        // causing subsequent GetAttr to read stale data.
        if mode.is_some() || uid.is_some() || gid.is_some() || size.is_some() {
            let store = {
                let stores = self.shard_stores.read().unwrap();
                stores.get(&shard_id).cloned()
            };
            if let Some(store) = store {
                let mut retries = 0;
                while retries < 50 {
                    if let Some(info) = store.get_inode(inode) {
                        let mode_ok =
                            mode.map_or(true, |m| (info.mode & 0o7777) == (m as u32 & 0o7777));
                        let uid_ok = uid.map_or(true, |u| info.uid == u as u32);
                        let gid_ok = gid.map_or(true, |g| info.gid == g as u32);
                        let size_ok = size.map_or(true, |s| info.size == s);
                        if mode_ok && uid_ok && gid_ok && size_ok {
                            return Ok(());
                        }
                    }
                    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                    retries += 1;
                }
                warn!(
                    "setattr timeout waiting for apply: inode={}, shard_id={:?}",
                    inode, shard_id
                );
                return Err("setattr timeout waiting for apply".to_string());
            }
        }

        Ok(())
    }

    /// Set data-related inode attributes (size, chunks) via Raft consensus (strong consistency)
    pub async fn setattr_data(
        &self,
        inode: u64,
        shard_id: ShardId,
        size: Option<u64>,
    ) -> Result<(), String> {
        let cmd = ShardCommand::SetAttrData { inode, size };

        self.raft_group_manager
            .propose(shard_id, cmd.serialize())
            .await?;

        // Wait for the command to be applied
        if let Some(expected_size) = size {
            let store = {
                let stores = self.shard_stores.read().unwrap();
                stores.get(&shard_id).cloned()
            };
            if let Some(store) = store {
                let mut retries = 0;
                while retries < 20 {
                    if let Some(info) = store.get_inode(inode) {
                        if info.size == expected_size {
                            return Ok(());
                        }
                    }
                    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
                    retries += 1;
                }
                return Err("setattr_data timeout waiting for apply".to_string());
            }
        }

        Ok(())
    }

    /// Set metadata-related inode attributes (mode, uid, gid, timestamps) via CRDT Delta merge (eventual consistency)
    ///
    /// This uses MetaDelta CRDT operations (LWW for mode/uid/gid, Max for timestamps, Counter for nlink)
    /// to ensure safe concurrent modification without locking.
    #[allow(clippy::too_many_arguments)]
    pub async fn setattr_meta(
        &self,
        inode: u64,
        shard_id: ShardId,
        mode: Option<u64>,
        uid: Option<u64>,
        gid: Option<u64>,
        mtime: Option<u64>,
        atime: Option<u64>,
        client_id: &str,
        timestamp: u64,
    ) -> Result<(), String> {
        let store = {
            let stores = self.shard_stores.read().unwrap();
            stores.get(&shard_id).cloned()
        };

        let store = store.ok_or_else(|| format!("shard {} not found", shard_id.0))?;

        let mut inode_info = store
            .get_inode(inode)
            .ok_or_else(|| "inode not found".to_string())?;

        // Build CRDT MetaState from current inode
        let mut state = crate::crdt_meta::MetaState {
            mode: Some(inode_info.mode),
            uid: Some(inode_info.uid),
            gid: Some(inode_info.gid),
            mtime: Some(inode_info.mtime),
            atime: Some(inode_info.atime),
            ctime: Some(inode_info.ctime),
            nlink: Some(inode_info.nlink as i32),
            mode_timestamp: inode_info.version,
            uid_timestamp: inode_info.version,
            gid_timestamp: inode_info.version,
            mtime_timestamp: inode_info.version,
            atime_timestamp: inode_info.version,
            ctime_timestamp: inode_info.version,
            nlink_delta: 0,
        };

        // Apply CRDT deltas
        if let Some(m) = mode {
            let delta = crate::crdt_meta::MetaDelta::SetMode {
                inode,
                mode: m as u32,
                timestamp,
                client_id: client_id.to_string(),
            };
            state.apply_delta(&delta);
        }
        if let Some(u) = uid {
            let delta = crate::crdt_meta::MetaDelta::SetUid {
                inode,
                uid: u as u32,
                timestamp,
                client_id: client_id.to_string(),
            };
            state.apply_delta(&delta);
        }
        if let Some(g) = gid {
            let delta = crate::crdt_meta::MetaDelta::SetGid {
                inode,
                gid: g as u32,
                timestamp,
                client_id: client_id.to_string(),
            };
            state.apply_delta(&delta);
        }
        if let Some(mt) = mtime {
            let delta = crate::crdt_meta::MetaDelta::SetMtime {
                inode,
                mtime: mt,
                timestamp,
                client_id: client_id.to_string(),
            };
            state.apply_delta(&delta);
        }
        if let Some(at) = atime {
            let delta = crate::crdt_meta::MetaDelta::SetAtime {
                inode,
                atime: at,
                timestamp,
                client_id: client_id.to_string(),
            };
            state.apply_delta(&delta);
        }

        // Write CRDT-merged state back to inode
        if let Some(m) = state.mode {
            inode_info.mode = m;
        }
        if let Some(u) = state.uid {
            inode_info.uid = u;
        }
        if let Some(g) = state.gid {
            inode_info.gid = g;
        }
        if let Some(mt) = state.mtime {
            inode_info.mtime = mt;
        }
        if let Some(at) = state.atime {
            inode_info.atime = at;
        }
        inode_info.version = timestamp;

        store.update_inode(inode_info)?;

        debug!(
            "setattr_meta CRDT merged: inode={}, mode={:?}, uid={:?}, gid={:?}, client={}, ts={}",
            inode, mode, uid, gid, client_id, timestamp
        );

        Ok(())
    }

    /// Set chunk/fid info for an existing inode via Raft consensus
    #[allow(clippy::too_many_arguments)]
    pub async fn set_chunks(
        &self,
        inode: u64,
        shard_id: ShardId,
        fid: String,
        volume_id: u64,
        cookie: u32,
        offset: u64,
        size: u64,
    ) -> Result<(), String> {
        let cmd = ShardCommand::SetChunks {
            inode,
            fid,
            volume_id,
            cookie,
            offset,
            size,
        };

        self.raft_group_manager
            .propose(shard_id, cmd.serialize())
            .await?;

        // Wait for the command to be applied
        let store = {
            let stores = self.shard_stores.read().unwrap();
            stores.get(&shard_id).cloned()
        };
        if let Some(store) = store {
            let mut retries = 0;
            while retries < 20 {
                if let Some(info) = store.get_inode(inode) {
                    if info.fid.is_some() && !info.chunks.is_empty() {
                        return Ok(());
                    }
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
                retries += 1;
            }
            return Err("set_chunks timeout waiting for apply".to_string());
        }

        Ok(())
    }

    /// Create a symbolic link via Raft consensus
    pub async fn create_symlink(
        &self,
        parent_inode: u64,
        name: &str,
        target: &str,
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

        let cmd = ShardCommand::CreateSymlink {
            parent_inode,
            name: name.to_string(),
            inode,
            target: target.to_string(),
        };

        self.raft_group_manager
            .propose(shard_id, cmd.serialize())
            .await?;

        let mut retries = 0;
        while retries < 50 {
            if let Some(info) = shard_store.get_inode(inode) {
                return Ok(info);
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            retries += 1;
        }

        Err("failed to create symlink: timeout waiting for apply".to_string())
    }

    /// Create a hard link via Raft consensus
    pub async fn create_hard_link(
        &self,
        inode: u64,
        new_parent_inode: u64,
        new_name: &str,
    ) -> Result<(), String> {
        let shard_id = self.shard_strategy.calculate_shard(new_parent_inode);

        let cmd = ShardCommand::CreateHardLink {
            inode,
            new_parent_inode,
            new_name: new_name.to_string(),
        };

        self.raft_group_manager
            .propose(shard_id, cmd.serialize())
            .await?;
        Ok(())
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
        let data = cmd.serialize();

        // Retry propose with backoff to handle leader election
        let mut propose_retries = 0;
        loop {
            match self
                .raft_group_manager
                .propose(shard_id, data.clone())
                .await
            {
                Ok(_) => break,
                Err(e) => {
                    if e.contains("not the leader") {
                        propose_retries += 1;
                        if propose_retries >= 30 {
                            return Err(format!(
                                "failed to propose POSIX root: leader election timeout after {} retries",
                                propose_retries
                            ));
                        }
                        debug!(
                            "Waiting for Raft leader election (retry {}/30)...",
                            propose_retries
                        );
                        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                    } else {
                        return Err(format!("failed to propose POSIX root: {}", e));
                    }
                }
            }
        }

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
        volume_id: u64,
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
        while retries < 50 {
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
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
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

    /// Check if current node is the leader for a given shard.
    /// Returns (is_leader, leader_address).
    pub async fn get_shard_leader_status(&self, shard_id: ShardId) -> Option<(bool, String)> {
        self.raft_group_manager
            .get_shard_leader_status(shard_id)
            .await
    }

    /// Get this node's gRPC address (for constructing redirect responses
    /// when leader status is unknown during Raft election).
    pub fn get_node_grpc_address(&self) -> String {
        self.raft_group_manager.get_node_address().to_string()
    }

    /// 批量分配 inode 区间（leader 单点 + CF_METADATA 持久化，§4 1.4）。
    /// fuse 在区间内本地分配，写路径零等待。
    pub async fn alloc_inode_batch(
        &self,
        shard_id: ShardId,
        count: u32,
    ) -> Result<(u64, u64), String> {
        let stores = self.shard_stores.read().unwrap();
        let store = stores
            .get(&shard_id)
            .cloned()
            .ok_or_else(|| format!("shard {} not found", shard_id.0))?;
        store.alloc_inode_batch(count)
    }

    /// 强一致更新 inode 的 size + chunks（close sync 账本，§4 1.5 / §5.1 lease 协调）。
    ///
    /// MUST go through Raft propose so all filer nodes replicate the update.
    /// The previous implementation wrote directly to the leader's local
    /// RocksDB, bypassing Raft — followers never saw the update and served
    /// stale size/chunks to subsequent getattr/read, causing cross-client
    /// data corruption (e.g., IO500 ior-easy-read returned EOF after the
    /// first chunk because a follower's size was stuck at 2MB while the
    /// leader had the correct 1GB).
    ///
    /// After propose we poll the local (leader) store until the apply
    /// completes, so that notify_inode_change (called by the net handler
    /// on return) and subsequent reads see the updated data.
    pub async fn update_inode_size_chunks_atomic(
        &self,
        shard_id: ShardId,
        inode: u64,
        size: u64,
        chunks: Vec<crate::shard_store::StoredFileChunk>,
    ) -> Result<(), String> {
        let target_chunk_count = chunks.len();
        let cmd = ShardCommand::UpdateInodeSizeChunks {
            inode,
            size,
            chunks,
        };
        self.raft_group_manager
            .propose(shard_id, cmd.serialize())
            .await?;

        // Wait for the apply to complete on this (leader) node so that
        // notify_inode_change and subsequent reads see the updated data.
        // Without this poll, a subscriber re-fetching the entry right
        // after notify could see the pre-apply (stale) state.
        let shard_store = {
            let stores = self.shard_stores.read().unwrap();
            stores
                .get(&shard_id)
                .cloned()
                .ok_or_else(|| format!("shard {} not found", shard_id.0))?
        };
        let mut retries = 0;
        while retries < 50 {
            if let Some(info) = shard_store.get_inode(inode) {
                if info.size == size && info.chunks.len() == target_chunk_count {
                    return Ok(());
                }
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            retries += 1;
        }
        log::warn!(
            "update_inode_size_chunks: timed out waiting for apply (inode={}, size={}, chunks={}), \
             propose committed; apply will catch up via Raft replication",
            inode,
            size,
            target_chunk_count
        );
        Ok(())
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
        // 注：每个 delta 自带 parent_ino，按 delta 维度路由到对应 (shard_id, dir_ino) OR-Set
        let delta_log = self.get_or_create_delta_log(shard_id);
        let shard_store = {
            let stores = self.shard_stores.read().unwrap();
            stores.get(&shard_id).cloned()
        };

        let mut max_seq = 0u64;
        // Collect SetAttr operations for batch processing at the end
        let mut pending_setattr_batch: Vec<InodeInfo> = Vec::new();

        // 从 client_vclock 中提取该 client 的最新 seq，用于 Remove/Rename delta
        // （proto DeltaOp 的 Remove/Rename 不携带 seq，extract_seq_from_delta 返回 None）
        let client_id_u64: u64 = client_id.parse().unwrap_or(0);
        let client_vc_seq: u64 = client_vclock
            .as_ref()
            .and_then(|vc| {
                vc.entries
                    .iter()
                    .find(|e| e.client_id == client_id_u64)
                    .map(|e| e.seq)
            })
            .unwrap_or(0);

        for delta in deltas {
            let seq = extract_seq_from_delta(delta).unwrap_or({
                // Remove/Rename delta 没有 seq 字段，用 client_vclock 中的 seq
                // 确保 delta_log.get_since 能正确返回这些 delta（seq > client_vclock[client_id]）
                if client_vc_seq > 0 {
                    client_vc_seq
                } else {
                    0
                }
            });
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
                            // 通过 mode & S_IFMT 判定 FileType（与 apply_delta_to_store 一致）
                            let ft = match entry.mode & 0o170000 {
                                0o040000 => crate::shard_store::FileType::Directory,
                                0o120000 => crate::shard_store::FileType::Symlink,
                                _ => crate::shard_store::FileType::File,
                            };
                            let orset_entry = DirEntryOrset {
                                tag: tag.clone(),
                                inode: entry.inode,
                                name: entry.name.clone(),
                                parent_ino: entry.parent_ino,
                                mode: entry.mode,
                                file_type: ft,
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
                            // For SetAttr, defer to batch processing
                            if let Some(crate::powerfs::delta_op::Op::SetAttr(setattr_op)) =
                                &delta.op
                            {
                                if let Some(mut inode_info) = store.get_inode(setattr_op.inode) {
                                    if setattr_op.size > 0 {
                                        inode_info.size = setattr_op.size;
                                        inode_info.blocks = setattr_op.size.div_ceil(512);
                                    }
                                    inode_info.mtime = setattr_op.mtime;
                                    if !setattr_op.chunks.is_empty() {
                                        inode_info.chunks = setattr_op
                                            .chunks
                                            .iter()
                                            .map(|c| crate::shard_store::StoredFileChunk {
                                                offset: c.offset,
                                                size: c.size,
                                                mtime: c.mtime,
                                                needle_id: c.needle_id,
                                                volume_id: c.volume_id,
                                                crc32: c.crc32,
                                            })
                                            .collect();
                                    }
                                    if !setattr_op.extended.is_empty() {
                                        inode_info.extended = setattr_op.extended.clone();
                                    }
                                    pending_setattr_batch.push(inode_info);
                                }
                            } else {
                                // Non-SetAttr operations: apply immediately
                                info!(
                                    "push_delta: dir {} merge_result={:?}, applying to store",
                                    dir_ino, merge_result
                                );
                                self.apply_delta_to_store(store, delta).await?;
                            }
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
                            info!(
                                "Concurrent Remove detected for dir {} (Add-Wins, apply_delta_to_store SKIPPED): {:?}",
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

        // Batch commit all pending SetAttr operations in a single RocksDB WriteBatch
        if !pending_setattr_batch.is_empty() {
            if let Some(store) = &shard_store {
                let count = pending_setattr_batch.len();
                store.batch_update_inodes(pending_setattr_batch)?;
                debug!("Batch committed {} SetAttr operations", count);
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
        dir_ino: u64,
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

        // Get deltas that the client hasn't seen yet from delta log.
        // 过滤 dir_ino：delta_log 是 per-shard 共享的，需按 parent_ino 过滤
        // 仅返回该目录的 delta，避免跨目录污染。
        let mut deltas: Vec<crate::powerfs::DeltaOp> = delta_log
            .get_since(&client_vclock_map)
            .into_iter()
            .filter(|d| self.get_dir_ino_from_delta(d) == Some(dir_ino))
            .collect();

        // Also compute deltas from OR-Set state for more accurate sync
        let orset_deltas = self.compute_orset_deltas(shard_id, dir_ino, &client_vclock_map);
        deltas.extend(orset_deltas);

        // Return per-shard VectorClock
        let shard_vclock = self.get_or_create_shard_vclock(shard_id);
        Ok((deltas, shard_vclock.to_proto()))
    }

    /// 从 OR-Set 状态计算增量变更
    /// 过滤 (shard_id, dir_ino) 双重维度，避免跨目录污染。
    fn compute_orset_deltas(
        &self,
        shard_id: ShardId,
        dir_ino: u64,
        client_vclock_map: &HashMap<String, u64>,
    ) -> Vec<crate::powerfs::DeltaOp> {
        let mut deltas = Vec::new();
        let states = self.orset_states.read().unwrap();

        for ((sid, d), orset) in states.iter() {
            if *sid != shard_id || *d != dir_ino {
                continue;
            }

            // 检查 OR-Set 的 vclock 是否有新的变更
            let orset_vclock = orset.vclock();
            let diff = orset_vclock.diff_against(&ServerVectorClock::from_map(client_vclock_map));

            for (client_id, seq) in diff {
                // 仅返回客户端尚未看到的 entry（tag.seq > client_seq）。
                // 修复：原逻辑 `tag.seq <= seq` 会重复返回已 apply 的 entry，
                // 导致 puller 每轮都收到相同 delta 并反复失效缓存。
                let client_seq = client_vclock_map.get(&client_id).copied().unwrap_or(0);
                for entry in orset.entries.values() {
                    if entry.tag.client_id == client_id
                        && entry.tag.seq > client_seq
                        && entry.tag.seq <= seq
                    {
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
                // 通过 mode & S_IFMT 判定 FileType —— fuse 端 mkdir/create 保留
                // S_IFDIR/S_IFREG 类型位（POSIX 语义），filer 端解析以保持一致。
                // 兼容历史调用：mode 缺失类型位时默认按 File 处理。
                let file_type = match entry_orset.mode & 0o170000 {
                    0o040000 => crate::shard_store::FileType::Directory,
                    0o120000 => crate::shard_store::FileType::Symlink,
                    _ => crate::shard_store::FileType::File,
                };
                let nlink = if matches!(file_type, crate::shard_store::FileType::Directory) {
                    2
                } else {
                    1
                };
                let inode_info = InodeInfo {
                    inode: entry_orset.inode,
                    name: entry_orset.name.clone(),
                    parent_inode: entry_orset.parent_ino,
                    file_type,
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
                    chunks: vec![],
                    extended: std::collections::HashMap::new(),
                    symlink_target: None,
                    nlink,
                    version: 0,
                    delete_time: 0,
                };
                store.create_inode_atomic(inode_info, entry_orset.parent_ino, &entry_orset.name)?;
                debug!(
                    "Applied Add delta: {}/{} inode={}",
                    entry_orset.parent_ino, entry_orset.name, entry_orset.inode
                );
            }
            Some(crate::powerfs::delta_op::Op::Remove(entry_id)) => {
                match store.lookup(entry_id.parent_ino, &entry_id.name) {
                    Some(inode_info) => {
                        let inode = inode_info.inode;
                        // Phase 3.5: 延迟删除——仅标记 tombstone，不物理删除。
                        // 物理删除由 GC 任务在 grace_period 后执行（需满足 nlink==0、
                        // 无活跃 lease、open_count==0）。
                        store.mark_tombstone(inode)?;
                        info!(
                            "Applied Remove delta (tombstone marked): {}/{} inode={}",
                            entry_id.parent_ino, entry_id.name, inode
                        );
                    }
                    None => {
                        warn!(
                            "Applied Remove delta: lookup FAILED {}/{} (parent_ino={}, name={})",
                            entry_id.parent_ino, entry_id.name, entry_id.parent_ino, entry_id.name
                        );
                    }
                }
            }
            Some(crate::powerfs::delta_op::Op::Rename(rename_op)) => {
                if let Some(inode_info) =
                    store.lookup(rename_op.old_parent_ino, &rename_op.old_name)
                {
                    let inode = inode_info.inode;
                    store.rename_dir_entry_atomic(
                        rename_op.old_parent_ino,
                        &rename_op.old_name,
                        rename_op.new_parent_ino,
                        &rename_op.new_name,
                        inode,
                    )?;
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
                        inode_info.blocks = setattr_op.size.div_ceil(512);
                    }
                    inode_info.mtime = setattr_op.mtime;
                    // Update chunks if provided
                    if !setattr_op.chunks.is_empty() {
                        inode_info.chunks = setattr_op
                            .chunks
                            .iter()
                            .map(|c| crate::shard_store::StoredFileChunk {
                                offset: c.offset,
                                size: c.size,
                                mtime: c.mtime,
                                needle_id: c.needle_id,
                                volume_id: c.volume_id,
                                crc32: c.crc32,
                            })
                            .collect();
                    }
                    // Update extended if provided
                    if !setattr_op.extended.is_empty() {
                        inode_info.extended = setattr_op.extended.clone();
                    }
                    store.update_inode(inode_info)?;
                    debug!(
                        "Applied SetAttr delta: inode={} size={} mtime={} chunks={} extended_keys={}",
                        setattr_op.inode,
                        setattr_op.size,
                        setattr_op.mtime,
                        setattr_op.chunks.len(),
                        setattr_op.extended.len(),
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

    /// 执行 CRDT 维护任务：清理过期 Tombstone、压缩 per-shard Delta Log、
    /// 统计 OR-Set 状态，用于后台定时调用。
    ///
    /// - `tombstone_ttl_hours`: tombstone 过期时间（小时）
    /// - `compact_delta_logs`: 是否压缩 Delta Log（当单 shard 条目超过 50% 容量时触发）
    ///
    /// 返回清理统计 (tombstone_cleaned, orset_state_count, delta_log_trimmed_total)
    pub fn crdt_maintenance(
        &self,
        tombstone_ttl_hours: u64,
        compact_delta_logs: bool,
    ) -> (usize, usize, usize) {
        let tombstone_cleaned = self.cleanup_tombstones(tombstone_ttl_hours);

        let orset_count = self.orset_states.read().unwrap().len();

        let mut delta_trimmed = 0usize;
        if compact_delta_logs {
            let logs = self.delta_logs.read().unwrap();
            for log in logs.values() {
                let mut entries = log.entries.write().unwrap();
                if entries.len() > log.max_size / 2 {
                    let target = log.max_size / 2;
                    let excess = entries.len() - target;
                    entries.drain(0..excess);
                    delta_trimmed += excess;
                }
            }
        }

        (tombstone_cleaned, orset_count, delta_trimmed)
    }

    // ========================================================================
    // Phase 3.5: 延迟删除 GC
    // ========================================================================

    /// Phase 3.5: 检查指定 inode 是否存在活跃 lease。
    ///
    /// GC 物理删除前必须确认无活跃 lease，避免删除正在被写入的数据。
    pub fn has_active_lease(&self, inode: u64) -> bool {
        let leases = self.leases.read().unwrap();
        let now = Instant::now();
        leases
            .values()
            .any(|info| info.inode == inode && info.expires_at > now)
    }

    /// Phase 3.5.3: 递增 inode 的 open 计数（fuse open 时上报）。
    /// 返回递增后的 open_count。
    pub fn increment_open_count(&self, inode: u64) -> Result<u32, String> {
        let stores = self.shard_stores.read().unwrap();
        for store in stores.values() {
            // inode 可能落在任意 shard，逐个尝试
            if store.get_inode(inode).is_some() {
                return Ok(store.increment_open_count(inode));
            }
        }
        // inode 不在任意 shard 中，仍记录（可能在创建途中）
        // 取第一个 store 记录
        if let Some(store) = stores.values().next() {
            Ok(store.increment_open_count(inode))
        } else {
            Err("no shard store available".to_string())
        }
    }

    /// Phase 3.5.3: 递减 inode 的 open 计数（fuse release/close 时上报）。
    /// 返回递减后的 open_count。
    pub fn decrement_open_count(&self, inode: u64) -> Result<u32, String> {
        let stores = self.shard_stores.read().unwrap();
        for store in stores.values() {
            let count = store.get_open_count(inode);
            if count > 0 {
                return Ok(store.decrement_open_count(inode));
            }
        }
        // open_count 已为 0 或未记录，幂等返回 0
        Ok(0)
    }

    /// Phase 3.5: 执行一次 GC 扫描与物理删除。
    ///
    /// 对每个 shard 扫描超过 `grace_period_secs` 的 tombstone 条目，逐一检查：
    /// 1. `nlink == 0`（无硬链接残留）
    /// 2. 无活跃 lease（`has_active_lease` 返回 false）
    /// 3. `open_count == 0`（Phase 3.5.3: fuse 端 open/release 上报计数）
    ///
    /// 满足条件后调用 `remove_inode_atomic` 物理删除元数据，并收集待回收的数据块。
    /// 返回 (扫描候选数, 物理删除数, 跳过数, 待回收 chunks 列表)。
    /// 待回收 chunks 由 GC 任务异步调用 `reclaim_data_chunks` 回收 volume server 数据。
    pub fn run_gc_pass(
        &self,
        grace_period_secs: u64,
    ) -> (usize, usize, usize, Vec<(u64, Vec<StoredFileChunk>)>) {
        let stores = self.shard_stores.read().unwrap();
        let mut scanned = 0usize;
        let mut deleted = 0usize;
        let mut skipped = 0usize;
        let mut chunks_to_reclaim: Vec<(u64, Vec<StoredFileChunk>)> = Vec::new();

        for (shard_id, store) in stores.iter() {
            let candidates = store.scan_tombstones_for_gc(grace_period_secs);
            for (inode, parent, name, chunks) in candidates {
                scanned += 1;

                // 条件 1: nlink == 0
                let nlink = store.get_inode(inode).map(|i| i.nlink).unwrap_or(0);
                if nlink > 0 {
                    debug!(
                        "GC skip inode {}: nlink={} > 0 (hard links remain)",
                        inode, nlink
                    );
                    skipped += 1;
                    continue;
                }

                // 条件 2: 无活跃 lease
                if self.has_active_lease(inode) {
                    debug!(
                        "GC skip inode {}: active lease exists (data still being written)",
                        inode
                    );
                    skipped += 1;
                    continue;
                }

                // 条件 3: open_count == 0
                let open_count = store.get_open_count(inode);
                if open_count > 0 {
                    debug!(
                        "GC skip inode {}: open_count={} > 0 (file still open)",
                        inode, open_count
                    );
                    skipped += 1;
                    continue;
                }

                // Phase 5: WAL — 先持久化待回收 chunks，再删元数据
                // 确保即使 reclaim_data_chunks 失败或 filer 崩溃，chunks 不丢失
                for chunk in &chunks {
                    store.add_pending_reclaim(inode, chunk);
                }

                // 物理删除元数据（CF_INODES + CF_DIR_ENTRIES）
                match store.remove_inode_atomic(inode, parent, &name) {
                    Ok(()) => {
                        deleted += 1;
                        if chunks.is_empty() {
                            debug!("GC deleted inode {} (no data chunks)", inode);
                        } else {
                            info!(
                                "GC deleted inode {} metadata, {} data chunks queued for reclamation (WAL persisted)",
                                inode,
                                chunks.len()
                            );
                            chunks_to_reclaim.push((inode, chunks));
                        }
                    }
                    Err(e) => {
                        warn!(
                            "GC failed to physically delete inode {} in shard {}: {}",
                            inode, shard_id.0, e
                        );
                        skipped += 1;
                    }
                }
            }
        }

        if scanned > 0 {
            info!(
                "GC pass: scanned={}, deleted={}, skipped={}, chunks_to_reclaim={}, grace_period={}s",
                scanned,
                deleted,
                skipped,
                chunks_to_reclaim.len(),
                grace_period_secs
            );
        }
        (scanned, deleted, skipped, chunks_to_reclaim)
    }

    /// Phase 3.5.2: 异步回收 volume server 上的数据块。
    ///
    /// 对每个 chunk，使用 chunk.volume_id 和 chunk.needle_id 通过 VolumeRouter
    /// 查询 volume server 地址，调用 delete_needle 回收数据。
    /// 回收失败时记录警告，不阻塞后续 chunk 回收（best-effort）。
    pub async fn reclaim_data_chunks(
        &self,
        chunks_to_reclaim: Vec<(u64, Vec<StoredFileChunk>)>,
        volume_router: &VolumeRouter,
        volume_client_pool: &VolumeClientPool,
    ) {
        for (inode, chunks) in chunks_to_reclaim {
            for chunk in chunks {
                let volume_id = chunk.volume_id;
                let needle_id = chunk.needle_id;

                // 查询 volume server 地址
                let server_addr = match volume_router.get_server_addr(volume_id).await {
                    Some(addr) => addr,
                    None => {
                        warn!(
                            "GC reclaim: volume server not found for volume_id={} (inode {}), skipping",
                            volume_id, inode
                        );
                        continue;
                    }
                };

                // 回收数据块
                match volume_client_pool
                    .delete_needle(&server_addr, volume_id, needle_id)
                    .await
                {
                    Ok(()) => {
                        debug!(
                            "GC reclaim: deleted needle volume_id={}, needle_id={} for inode {}",
                            volume_id, needle_id, inode
                        );
                        // Phase 5: WAL — 回收成功，从 pending_reclaims 移除
                        self.remove_pending_reclaim_all_stores(volume_id, needle_id);
                    }
                    Err(e) => {
                        warn!(
                            "GC reclaim: delete_needle failed for inode {} (volume_id={}, needle_id={}): {} — will retry next GC cycle",
                            inode, volume_id, needle_id, e
                        );
                        // Phase 5: WAL — 回收失败，保留在 pending_reclaims，下次 GC 重试
                    }
                }
            }
        }
    }

    /// Phase 5: 从所有 shard stores 中移除指定 chunk 的 pending_reclaim。
    /// RocksDB delete 对不存在的 key 是 no-op，所以遍历所有 store 是安全的。
    fn remove_pending_reclaim_all_stores(&self, volume_id: u64, needle_id: u64) {
        let stores = self.shard_stores.read().unwrap();
        for store in stores.values() {
            store.remove_pending_reclaim(volume_id, needle_id);
        }
    }

    /// Phase 5: 重试 pending_reclaims 中未回收的数据块。
    ///
    /// 在每轮 GC 正常扫描前调用，处理上一轮回收失败的 chunks。
    /// 崩溃恢复：filer 重启后从 RocksDB 加载 pending_reclaims，首次 GC 即重试。
    pub async fn retry_pending_reclaims(
        &self,
        volume_router: &VolumeRouter,
        volume_client_pool: &VolumeClientPool,
    ) -> usize {
        // 先收集所有 pending reclaims，然后释放锁（避免跨 await 持有 RwLockReadGuard）
        let all_pending: Vec<(ShardId, Vec<(u64, StoredFileChunk)>)> = {
            let stores = self.shard_stores.read().unwrap();
            let mut pending_per_shard = Vec::new();
            for (shard_id, store) in stores.iter() {
                let pending = store.list_pending_reclaims();
                if !pending.is_empty() {
                    pending_per_shard.push((*shard_id, pending));
                }
            }
            pending_per_shard
        }; // RwLockReadGuard 在此释放

        if all_pending.is_empty() {
            return 0;
        }

        let mut retried = 0usize;
        let mut succeeded = 0usize;
        let mut failed = 0usize;

        for (shard_id, pending) in &all_pending {
            for (inode, chunk) in pending {
                retried += 1;
                let volume_id = chunk.volume_id;
                let needle_id = chunk.needle_id;

                let server_addr = match volume_router.get_server_addr(volume_id).await {
                    Some(addr) => addr,
                    None => {
                        warn!(
                            "GC retry: volume server not found for volume_id={} (inode {}), will retry next cycle",
                            volume_id, inode
                        );
                        failed += 1;
                        continue;
                    }
                };

                match volume_client_pool
                    .delete_needle(&server_addr, volume_id, needle_id)
                    .await
                {
                    Ok(()) => {
                        succeeded += 1;
                        self.remove_pending_reclaim_all_stores(volume_id, needle_id);
                        debug!(
                            "GC retry: deleted needle volume_id={}, needle_id={} for inode {}",
                            volume_id, needle_id, inode
                        );
                    }
                    Err(e) => {
                        failed += 1;
                        warn!(
                            "GC retry: delete_needle failed for inode {} (volume_id={}, needle_id={}): {}",
                            inode, volume_id, needle_id, e
                        );
                    }
                }
            }
            debug!(
                "GC retry: shard {} retried={}, succeeded={}, failed={}",
                shard_id.0, retried, succeeded, failed
            );
        }

        if retried > 0 {
            info!(
                "GC retry_pending_reclaims: retried={}, succeeded={}, failed={}",
                retried, succeeded, failed
            );
        }
        retried
    }

    /// Phase 3.5: 启动后台 GC 任务。
    ///
    /// 每 `interval_secs` 秒执行一次 `run_gc_pass`：
    /// - `grace_period_secs`: tombstone 标记后等待多久才可被 GC 物理删除（默认与
    ///   `gc_grace_period` 配置一致，所有 filer 节点必须相同）
    /// - GC 采用批量扫描 + 限速（单次 pass 内串行删除，避免打满 RocksDB IO）
    /// - 物理删除元数据后，异步调用 `reclaim_data_chunks` 回收 volume server 数据块
    /// - Phase 5: 每轮先 `retry_pending_reclaims`（重试上次失败的回收），再 `run_gc_pass`
    pub fn spawn_gc_task(
        self: &Arc<Self>,
        interval_secs: u64,
        grace_period_secs: u64,
        volume_router: Arc<VolumeRouter>,
        volume_client_pool: Arc<VolumeClientPool>,
    ) -> tokio::task::JoinHandle<()> {
        let mgr = self.clone();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                // Phase 5: 先重试上次失败的 pending_reclaims（WAL 崩溃恢复）
                let retried = mgr
                    .retry_pending_reclaims(&volume_router, &volume_client_pool)
                    .await;
                // 正常 GC 扫描
                let (scanned, deleted, skipped, chunks_to_reclaim) =
                    mgr.run_gc_pass(grace_period_secs);
                if scanned > 0 || retried > 0 {
                    debug!(
                        "GC task heartbeat: retried={}, scanned={}, deleted={}, skipped={}, chunks_to_reclaim={}",
                        retried, scanned, deleted, skipped, chunks_to_reclaim.len()
                    );
                }
                // 异步回收 volume server 数据块（best-effort）
                if !chunks_to_reclaim.is_empty() {
                    mgr.reclaim_data_chunks(chunks_to_reclaim, &volume_router, &volume_client_pool)
                        .await;
                }
            }
        })
    }

    /// 启动后台 CRDT 维护任务。
    ///
    /// 每 `interval_secs` 秒执行一次 `crdt_maintenance`：
    /// - Tombstone TTL 默认 24 小时
    /// - 每个 shard 的 Delta Log 在超过 50% 容量时压缩
    pub fn spawn_crdt_maintenance(
        self: &Arc<Self>,
        interval_secs: u64,
    ) -> tokio::task::JoinHandle<()> {
        let mgr = self.clone();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                let (tombstone_cleaned, orset_count, delta_trimmed) =
                    mgr.crdt_maintenance(24, true);
                if tombstone_cleaned > 0 || delta_trimmed > 0 {
                    info!(
                        "CRDT maintenance: tombstones cleaned={}, orset_states={}, delta_log_trimmed={}",
                        tombstone_cleaned, orset_count, delta_trimmed
                    );
                } else {
                    debug!("CRDT maintenance heartbeat: orset_states={}", orset_count);
                }
            }
        })
    }
}

// Helper function to extract sequence number from DeltaOp
fn extract_seq_from_delta(delta: &crate::powerfs::DeltaOp) -> Option<u64> {
    match &delta.op {
        Some(crate::powerfs::delta_op::Op::Add(entry_orset)) => Some(entry_orset.seq),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft_group_manager::RaftGroupManager;
    use crate::shard_strategy::ShardStrategy;

    fn make_manager() -> Arc<MetaShardManager> {
        let tmp_dir =
            std::env::temp_dir().join(format!("powerfs-filer-test-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&tmp_dir);
        let data_path = tmp_dir.to_string_lossy().to_string();

        let raft_addr = "127.0.0.1:19999".to_string();
        let raft_mgr = Arc::new(RaftGroupManager::new(1, raft_addr, data_path.clone()));
        let strategy = Arc::new(ShardStrategy::new(4));
        Arc::new(MetaShardManager::new(raft_mgr, strategy, data_path, 1))
    }

    #[test]
    fn test_crdt_maintenance_empty_state() {
        let mgr = make_manager();
        let (tombstone_cleaned, orset_count, delta_trimmed) = mgr.crdt_maintenance(24, true);
        assert_eq!(tombstone_cleaned, 0, "no shard stores yet");
        assert_eq!(orset_count, 0, "no orset states yet");
        assert_eq!(delta_trimmed, 0, "no delta logs yet");
    }

    #[test]
    fn test_crdt_maintenance_disable_delta_compaction() {
        let mgr = make_manager();
        // Populate a delta log manually
        let log = mgr.get_or_create_delta_log(ShardId(0));
        for i in 0..200 {
            log.append(
                "client-a",
                i,
                crate::powerfs::DeltaOp {
                    op: Some(crate::powerfs::delta_op::Op::Add(
                        crate::powerfs::DirEntryOrset::default(),
                    )),
                },
            );
        }
        let before = log.entries.read().unwrap().len();
        assert_eq!(before, 200);

        // Disable compaction
        let (_, _, delta_trimmed) = mgr.crdt_maintenance(24, false);
        assert_eq!(delta_trimmed, 0, "compaction disabled, nothing trimmed");
        let after = log.entries.read().unwrap().len();
        assert_eq!(after, before, "entries untouched");
    }

    #[test]
    fn test_crdt_maintenance_delta_log_compaction() {
        let mgr = make_manager();
        let log = mgr.get_or_create_delta_log(ShardId(0));
        // Fill log beyond 50% of capacity (max_size=10000, trigger at >5000)
        for i in 0..6000 {
            log.append(
                "client-a",
                i,
                crate::powerfs::DeltaOp {
                    op: Some(crate::powerfs::delta_op::Op::Add(
                        crate::powerfs::DirEntryOrset::default(),
                    )),
                },
            );
        }
        let before = log.entries.read().unwrap().len();
        assert_eq!(before, 6000);

        let (_, _, delta_trimmed) = mgr.crdt_maintenance(24, true);
        assert!(delta_trimmed > 0, "entries should have been trimmed");

        let after = log.entries.read().unwrap().len();
        // Target is max_size/2 = 5000
        assert!(after <= 5000, "entries should be <= 5000 after compaction");
        assert_eq!(
            delta_trimmed,
            before - after,
            "trimmed count should match the reduction"
        );
    }

    #[test]
    fn test_crdt_maintenance_small_log_no_compaction() {
        let mgr = make_manager();
        let log = mgr.get_or_create_delta_log(ShardId(0));
        for i in 0..100 {
            log.append(
                "client-a",
                i,
                crate::powerfs::DeltaOp {
                    op: Some(crate::powerfs::delta_op::Op::Add(
                        crate::powerfs::DirEntryOrset::default(),
                    )),
                },
            );
        }
        let (_, _, delta_trimmed) = mgr.crdt_maintenance(24, true);
        assert_eq!(
            delta_trimmed, 0,
            "small delta log should not trigger compaction"
        );
    }
}
