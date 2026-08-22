use log::{debug, error, info, warn};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use crate::crdt_orset::{
    DirEntryOrset, EntryTag, MergeResult, ServerDirORSet, ServerVectorClock, Tombstone,
};
use crate::raft_group_manager_v2::RaftGroupManagerV2;
use crate::raft_group_manager_v2::{Peer, ShardCommand, ShardId};
use crate::shard_store::{DirEntry, FileType, InodeInfo, ShardStats, ShardStore, StoredFileChunk};
use crate::shard_strategy::ShardStrategy;
use crate::tlv_volume_client::TlvVolumeClient;
use crate::volume_router::VolumeRouter;

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
    raft_group_manager: Arc<RaftGroupManagerV2>,
    shard_stores: RwLock<HashMap<ShardId, Arc<ShardStore>>>,
    shard_strategy: Arc<ShardStrategy>,
    /// Per-shard inode allocators. Each shard has its own counter within
    /// the shard's inode range, with a per-node offset to avoid collisions
    /// across filer nodes.
    ///
    /// Replaces the old single `inode_generator: AtomicU64` which allocated
    /// all inodes from `node_id * 1B + 1000`, causing severe shard imbalance
    /// (nodes 1+ crammed all inodes into the last shard because 1B >> 1M
    /// shard range size).
    ///
    /// With per-shard allocators:
    /// - Files get inodes in the parent directory's shard range (locality)
    /// - Directories get inodes in a different shard's range (distribution)
    /// - Each node has a non-overlapping slot within each shard range
    shard_allocators: RwLock<Vec<ShardAllocator>>,
    /// Filer node id, used to partition the inode space within each shard.
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
    /// Async metadata persistence flag (default: true).
    ///
    /// When true (default, performance mode): create/mkdir/rename stage
    /// to MetaCache and `propose_ff` to Raft (fire-and-forget). The entry
    /// is immediately visible via MetaCache. Only dirty (setattr) and
    /// deleted (unlink) entries use synchronous `propose` (wait for
    /// quorum commit). This trades a small window of inconsistency (if
    /// leader crashes before apply) for ~80x IOPS improvement on
    /// small-file create.
    ///
    /// When false (strict mode): wait for Raft apply before returning,
    /// guaranteeing the metadata is visible on the authoritative shard
    /// before the client proceeds. Use this for苛刻环境 requiring强一致.
    ///
    /// Toggle via:
    /// - Config: `async_meta_persist = true|false` in [filer] section
    /// - Env: `POWERFS_ASYNC_META_PERSIST=0` to disable
    async_meta_persist: std::sync::atomic::AtomicBool,

    /// Staging cache for newly created metadata entries.
    ///
    /// When async_meta_persist is true, `create_inode()` stages the new
    /// inode + dir entry here BEFORE `propose_ff`, making them immediately
    /// visible to reads. Once Raft applies the entry to ShardStore, the
    /// staging copy is swept.
    ///
    /// On leader change (cache epoch mismatch), `invalidate_all()` clears
    /// all staging entries — the client retries on the new leader.
    meta_cache: std::sync::Arc<crate::meta_cache::MetaCache>,
    /// Phase 3 Lease Recall: optional reference to the InodeNotifier
    /// for pushing Invalidate notifications to lease holders when the
    /// GC loop's trim_pass identifies recall_candidates. Set via
    /// `set_recall_components` after both the notifier and lease
    /// manager are constructed in main.rs.
    inode_notifier: std::sync::RwLock<Option<std::sync::Arc<crate::inode_notifier::InodeNotifier>>>,
    /// Phase 3 Lease Recall: optional reference to the InodeLeaseManager
    /// for querying lease holders (`get_holder`) and running
    /// `sweep_leaked_refcounts` in the GC loop.
    lease_mgr:
        std::sync::RwLock<Option<std::sync::Arc<crate::inode_lease_manager::InodeLeaseManager>>>,
}

/// Per-shard inode allocator.
///
/// Each filer node owns a non-overlapping slot within each shard's inode
/// range. The slot is calculated as:
///   node_offset = node_id * (range_size / MAX_NODES)
///   actual_inode = shard_range_start + node_offset + counter
///
/// This ensures:
/// 1. No collisions between nodes (each node has a unique offset)
/// 2. Balanced distribution across shards (each shard gets allocations)
/// 3. Files can be placed on the parent's shard (locality for readdir)
struct ShardAllocator {
    /// Next counter value within this shard's slot.
    counter: AtomicU64,
    /// Start of this shard's inode range.
    shard_start: u64,
    /// Per-node offset within the shard range.
    node_offset: u64,
}

/// Maximum number of filer nodes supported. Each shard range is divided
/// into this many equal slots, one per node.
const MAX_FILER_NODES: u64 = 64;

impl MetaShardManager {
    pub fn new(
        raft_group_manager: Arc<RaftGroupManagerV2>,
        shard_strategy: Arc<ShardStrategy>,
        data_path: String,
        node_id: u64,
    ) -> Self {
        // Build per-shard allocators. Each shard gets its own counter within
        // the shard's inode range, with a per-node offset to avoid collisions.
        let shard_count = shard_strategy.get_shard_count();
        let allocators = Self::build_shard_allocators(&shard_strategy, shard_count, node_id);
        // Async meta persist: default true (performance mode).
        // Set POWERFS_ASYNC_META_PERSIST=0 to force strict mode.
        let async_default = match std::env::var("POWERFS_ASYNC_META_PERSIST") {
            Ok(v) => v != "0" && v.to_lowercase() != "false" && v != "off",
            Err(_) => true,
        };
        Self {
            raft_group_manager,
            shard_stores: RwLock::new(HashMap::new()),
            shard_strategy,
            shard_allocators: RwLock::new(allocators),
            node_id,
            data_path,
            root_inodes: RwLock::new(HashMap::new()),
            leases: RwLock::new(HashMap::new()),
            lease_epoch: std::sync::atomic::AtomicU64::new(1),
            shard_vclocks: RwLock::new(HashMap::new()),
            delta_logs: RwLock::new(HashMap::new()),
            orset_states: RwLock::new(HashMap::new()),
            async_meta_persist: std::sync::atomic::AtomicBool::new(async_default),
            meta_cache: std::sync::Arc::new(crate::meta_cache::MetaCache::new()),
            inode_notifier: std::sync::RwLock::new(None),
            lease_mgr: std::sync::RwLock::new(None),
        }
    }

    /// Set async_meta_persist mode at runtime.
    /// true = performance mode (default), false = strict mode.
    pub fn set_async_meta_persist(&self, enabled: bool) {
        self.async_meta_persist
            .store(enabled, std::sync::atomic::Ordering::Relaxed);
    }

    /// Accessor for the shared MetaCache — used by the filer metrics HTTP
    /// server to expose hit/miss/dirty counters on `/metrics` and
    /// `/admin/meta-cache-stats`. The Arc handle is cheap to clone.
    pub fn meta_cache(&self) -> std::sync::Arc<crate::meta_cache::MetaCache> {
        self.meta_cache.clone()
    }

    /// Phase 3 Lease Recall: wire the InodeNotifier and InodeLeaseManager
    /// into the MetaShardManager so the GC loop can (1) look up lease
    /// holders for recall candidates, (2) push Invalidate notifications
    /// to them, and (3) periodically sweep leaked refcounts.
    ///
    /// Called from main.rs after both the FilerNetHandler (which owns
    /// the lease manager) and the InodeNotifier are constructed.
    pub fn set_recall_components(
        &self,
        notifier: std::sync::Arc<crate::inode_notifier::InodeNotifier>,
        lease_mgr: std::sync::Arc<crate::inode_lease_manager::InodeLeaseManager>,
    ) {
        *self.inode_notifier.write().unwrap() = Some(notifier);
        *self.lease_mgr.write().unwrap() = Some(lease_mgr);
    }

    /// Check if async_meta_persist is enabled.
    pub fn is_async_meta_persist(&self) -> bool {
        self.async_meta_persist
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Unified propose method: auto-selects fire-and-forget or synchronous
    /// based on `async_meta_persist` flag.
    ///
    /// - Async mode (default): `propose_ff` — submit to RaftCore, return
    ///   immediately without waiting for commit/apply. ~10x faster.
    /// - Strict mode: `propose` — wait for quorum commit.
    ///
    /// Like `propose_meta` but always uses `propose` (wait for Raft quorum
    /// commit), regardless of async_meta_persist setting. Use for CRITICAL
    /// metadata operations where the entry MUST be in the committed log:
    /// CreateInode, AddDirEntry. Non-critical ops (UpdateInodeSizeChunks,
    /// setattr, etc.) can use `propose_meta` which may use propose_ff.
    ///
    /// Does NOT wait for apply — apply happens asynchronously (~1-5ms after
    /// commit). The lookup retry in net_handler handles the brief gap.
    async fn propose_meta_commit(&self, shard_id: ShardId, data: Vec<u8>) -> Result<(), String> {
        self.raft_group_manager
            .propose(shard_id, data)
            .await
            .map(|_| ())
    }

    /// Submit a metadata operation respecting the async_meta_persist setting.
    ///
    /// - Async mode (default): `propose_ff` — fire-and-forget, returns
    ///   immediately after submitting to RaftCore. No waiting for commit or
    ///   apply. Fastest path (~0ms). CRITICAL: if the node is not the leader
    ///   or lease expired, the entry is silently dropped (None responder).
    ///   Only use for non-critical ops where a dropped entry is recoverable
    ///   (e.g., UpdateInodeSizeChunks — client can retry the write).
    ///
    /// - Strict mode: `propose` — wait for quorum commit.
    ///
    /// All metadata operations (create/unlink/rename/setattr/update_file)
    /// should use this method instead of calling `raft_group_manager.propose`
    /// directly, to respect the async_meta_persist setting.
    async fn propose_meta(&self, shard_id: ShardId, data: Vec<u8>) -> Result<(), String> {
        if self.is_async_meta_persist() {
            self.raft_group_manager.propose_ff(shard_id, data).await
        } else {
            self.raft_group_manager
                .propose(shard_id, data)
                .await
                .map(|_| ())
        }
    }

    /// Build per-shard allocators for the given shard count and node_id.
    /// Each allocator owns a non-overlapping slot within its shard's range.
    fn build_shard_allocators(
        shard_strategy: &ShardStrategy,
        shard_count: u64,
        node_id: u64,
    ) -> Vec<ShardAllocator> {
        let mut allocators = Vec::with_capacity(shard_count as usize);
        for sid in 0..shard_count {
            let (start, end) = shard_strategy.get_shard_range(ShardId(sid));
            let range_size = end.saturating_sub(start);
            // Per-node slot: divide the shard range into MAX_FILER_NODES slots.
            // node_id < MAX_FILER_NODES gets a unique slot.
            let slot = range_size / MAX_FILER_NODES;
            let node_offset = node_id * slot;
            // Reserve the first 1000 inodes in each shard for special inodes
            // (root=1, bucket roots, etc.)
            let reserved = 1000u64;
            allocators.push(ShardAllocator {
                counter: AtomicU64::new(reserved),
                shard_start: start,
                node_offset,
            });
            info!(
                "ShardAllocator init: shard={} range=[{}, {}) slot={} node_offset={} (node_id={})",
                sid, start, end, slot, node_offset, node_id
            );
        }
        allocators
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

        // create_group 返回 apply_rx（接收 applied log index）
        let mut apply_rx = self
            .raft_group_manager
            .create_group(shard_id, peers)
            .await?;

        let db_path = format!("{}/shard_{}_data", self.data_path, shard_id.0);
        let shard_store = Arc::new(
            ShardStore::new(shard_id, inode_range, &db_path)
                .map_err(|e| format!("failed to create shard store: {}", e))?,
        );

        // P3: 注入 MetaCache 引用，Raft apply 后回调 confirm_*() 清除 staging
        shard_store.set_meta_cache(self.meta_cache.clone());

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

        // apply 循环：接收 applied log index → 从 Raft 状态机读取 payload → 反序列化为 ShardCommand → 应用到 ShardStore
        //
        // CRITICAL: RocksStateMachine::apply() processes a BATCH of entries in
        // one call, but only notifies the LAST index via apply_notifier
        // (try_send). If we only process the notified index, we SKIP all
        // intermediate entries (e.g., CreateInode at N, AddDirEntry at N+1,
        // but only N+1 is notified → CreateInode.apply_command never called →
        // "inode not found" on subsequent UpdateInodeSizeChunks apply).
        //
        // Fix: track last_applied_index and process ALL entries from
        // last_applied+1 to the notified index. This ensures no entry is
        // skipped even when apply() batches multiple entries.
        let mgr = self.raft_group_manager.clone();
        tokio::spawn(async move {
            let mut last_applied: u64 = 0;
            while let Some(index) = apply_rx.recv().await {
                if index <= last_applied {
                    // Already processed (duplicate notification or out-of-order)
                    continue;
                }
                let start = if last_applied == 0 || index - last_applied > 100 {
                    // First notification or large gap (e.g., after restart with
                    // thousands of pre-existing entries): only process the
                    // notified index. Old entries were already applied before
                    // the shard store was created (loaded from RocksDB snapshot).
                    index
                } else {
                    last_applied + 1
                };
                for idx in start..=index {
                    match mgr.read_applied_entry(shard_id, idx).await {
                        Ok(Some(payload)) => match ShardCommand::deserialize(&payload) {
                            Ok(cmd) => shard_store.apply_command(cmd),
                            Err(e) => error!(
                                "shard {}: failed to deserialize ShardCommand at index {}: {}",
                                shard_id.0, idx, e
                            ),
                        },
                        Ok(None) => {
                            // Blank/Membership entries have no payload —
                            // this is normal, not an error.
                            debug!(
                                "shard {}: no applied entry at index {} (Blank/Membership)",
                                shard_id.0, idx
                            );
                        }
                        Err(e) => {
                            error!(
                                "shard {}: failed to read applied entry at index {}: {}",
                                shard_id.0, idx, e
                            );
                        }
                    }
                }
                last_applied = index;
            }
        });

        // P3: Leader change watcher — clear MetaCache staging on leadership loss.
        // When the node loses leadership for this shard, pending staging entries
        // may not have been committed by the new leader. Clear them to prevent
        // reads from returning uncommitted data.
        let meta_cache_for_watcher = self.meta_cache.clone();
        self.raft_group_manager
            .spawn_leader_watcher(shard_id, move || {
                meta_cache_for_watcher.invalidate_all();
            })
            .await;

        // P3: Periodic sweep of expired deletion markers (every 60s, TTL 5min).
        // Prevents unbounded growth of deleted_inodes/deleted_direntries maps
        // if Raft apply is delayed or a node is isolated.
        let meta_cache_for_sweep = self.meta_cache.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
            loop {
                interval.tick().await;
                meta_cache_for_sweep.sweep_expired_deletions(std::time::Duration::from_secs(300));
            }
        });

        info!("Created shard {} with range {:?}", shard_id.0, inode_range);
        Ok(())
    }

    pub async fn create_file(&self, parent_inode: u64, name: &str) -> Result<InodeInfo, String> {
        let t0 = std::time::Instant::now();

        // Phase 3: Allocate inode within the parent directory's shard range.
        // This ensures calculate_shard(inode) == calculate_shard(parent_inode),
        // so readdir can fetch file inode records from the same shard as the
        // dir entries (no cross-shard lookup for files).
        let parent_shard = self.shard_strategy.calculate_shard(parent_inode);
        let inode = self.alloc_inode_in_shard(parent_shard);
        let now = chrono::Utc::now().timestamp() as u64;
        let info = InodeInfo {
            inode,
            name: name.to_string(),
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
            inline_data: None,
            extended: HashMap::new(),
            symlink_target: None,
            nlink: 1,
            version: 0,
            delete_time: 0,
            reliability: powerfs_layout::reliability::Reliability::default(),
            reliability_state: powerfs_layout::reliability::ReliabilityState::default(),
            compression_state: powerfs_layout::reliability::CompressionState::default(),
            replica_chunks: Vec::new(),
        };

        self.propose_create_inode_and_direntry(info.clone(), parent_inode, name, inode)
            .await?;

        log::info!(
            "create_file latency: total={}ms, inode={}",
            t0.elapsed().as_millis(),
            inode
        );
        Ok(info)
    }

    /// Two-phase create used by `create_file`, `create_directory`,
    /// `create_file_with_shard`, `put_object_entry`, and `create_symlink`.
    ///
    /// Phase A: propose `CreateInode { info }` to
    /// `shard_ino = calculate_shard(info.inode)`. On success the inode
    /// record exists but is unreachable from any directory (orphan until B).
    ///
    /// Phase B: propose `AddDirEntry { parent_inode, name, info.inode }` to
    /// `shard_dir = calculate_shard(parent_inode)`. On success the file is
    /// fully visible.
    ///
    /// If B fails after A succeeded, the inode is orphaned and the GC scan
    /// (`collect_orphan_inodes`) reclaims it. The caller surfaces the error.
    ///
    /// We poll `shard_ino` for the inode's appearance because subsequent
    /// getattr (route by `calculate_shard(inode)`) needs the record there;
    /// waiting on the dir entry's shard would not help.
    async fn propose_create_inode_and_direntry(
        &self,
        info: InodeInfo,
        parent_inode: u64,
        name: &str,
        inode: u64,
    ) -> Result<(), String> {
        let shard_ino = self.shard_strategy.calculate_shard(inode);
        let shard_dir = self.shard_strategy.calculate_shard(parent_inode);

        {
            let stores = self.shard_stores.read().unwrap();
            if stores.get(&shard_ino).is_none() {
                return Err(format!("shard {} not found", shard_ino.0));
            }
            if stores.get(&shard_dir).is_none() {
                return Err(format!("shard {} not found", shard_dir.0));
            }
        }

        // PERF: async_meta_persist mode (default: true)
        //
        // When async mode is enabled (default, performance mode):
        //   CreateInode + AddDirEntry use `propose` (waits for Raft quorum
        //   commit, ~10ms each) but do NOT wait for apply. This is CRITICAL:
        //   `propose_ff` (fire-and-forget) with None responder silently drops
        //   the entry if the node loses leadership or lease expires between
        //   check_leader and RaftCore processing. A dropped CreateInode means
        //   the inode never exists, but subsequent UpdateInodeSizeChunks
        //   (which may succeed on a different leader term) will fail with
        //   "inode not found" → EIO to the client.
        //
        //   By using `propose` (with responder) for CreateInode + AddDirEntry:
        //   - The entry is guaranteed to be in the committed Raft log
        //   - If not leader, `propose` returns ForwardToLeader error →
        //     caller returns STATUS_ERR_REDIRECT → client redirects
        //   - Apply happens asynchronously (~1-5ms after commit)
        //   - Lookup retry (20ms in net_handler) handles apply delay
        //
        //   UpdateInodeSizeChunks and other non-critical ops still use
        //   `propose_ff` for maximum throughput. If dropped, the file exists
        //   but with stale data — client can retry.
        //
        // When async mode is disabled (strict mode, POWERFS_ASYNC_META_PERSIST=0):
        //   Uses `propose` (waits for quorum commit) + apply polling.
        //   Guarantees the metadata is fully committed and applied before
        //   returning. Use for苛刻环境 requiring强一致.
        if self.is_async_meta_persist() {
            // P3: Async mode with MetaCache staging (方案B).
            //
            // Stage the new inode + dir entry in MetaCache BEFORE proposing
            // to Raft. This makes them immediately visible to reads
            // (get_inode / lookup_entry check MetaCache first), eliminating
            // the "file not found" window between create and Raft apply.
            //
            // Then use `propose` (wait for quorum COMMIT, NOT apply) for
            // CreateInode + AddDirEntry. This is critical for correctness:
            //
            // - `propose` (wait commit): guarantees data is replicated to
            //   majority of followers. If leader crashes after commit,
            //   new leader replays the Raft log → data survives.
            //   ~10ms latency, acceptable.
            //
            // - `propose_ff` (fire-and-forget, NOT waiting commit):
            //   DANGEROUS for CreateInode/AddDirEntry. If leader crashes
            //   before commit, the entry is lost. Client received "success"
            //   but data doesn't exist → "fake success" → "inode not found"
            //   on subsequent access. This was the root cause of the
            //   "UpdateInodeSizeChunks failed for inode not found" bug.
            //
            // The staging cache bridges the commit→apply gap:
            //   1. stage_create() → immediately visible to reads
            //   2. propose() → wait commit (~10ms), data replicated
            //   3. return success → client reads from staging
            //   4. (background) Raft apply → ShardStore has data
            //   5. confirm_create() → staging cleared, ShardStore serves reads
            //
            // dirty (setattr) and deleted (unlink) also use `propose`
            // (synchronous commit) — see their respective methods.

            self.meta_cache
                .stage_create(info.clone(), parent_inode, name);

            let cmd_ino = ShardCommand::CreateInode {
                info: Box::new(info),
            };
            let cmd_dir = ShardCommand::AddDirEntry {
                parent_inode,
                name: name.to_string(),
                inode,
            };

            // Use propose (wait commit) for both commands. On error
            // (e.g., lost leadership), remove staging and return error
            // so client redirects to new leader.
            let result: Result<(), String> = if shard_ino == shard_dir {
                // Same shard: batch both entries into one Raft message.
                self.raft_group_manager
                    .propose_many(shard_ino, vec![cmd_ino.serialize(), cmd_dir.serialize()])
                    .await
                    .map(|_| ())
            } else {
                // Cross-shard: different Raft groups, cannot batch.
                // Sequential propose: first CreateInode, then AddDirEntry.
                match self
                    .raft_group_manager
                    .propose(shard_ino, cmd_ino.serialize())
                    .await
                {
                    Ok(_) => self
                        .raft_group_manager
                        .propose(shard_dir, cmd_dir.serialize())
                        .await
                        .map(|_| ()),
                    Err(e) => Err(e),
                }
            };

            if let Err(e) = result {
                // Propose failed (lost leadership, network error, etc.)
                // Remove staging entries — they are NOT committed.
                // Client will receive error and redirect to new leader.
                warn!(
                    "create async(staged): propose FAILED for inode={} parent={} name={}: {} — removing staging, client will retry",
                    inode, parent_inode, name, e
                );
                self.meta_cache
                    .invalidate_staging(inode, parent_inode, name);
                return Err(e);
            }

            info!(
                "create async(staged): inode={} parent={} name={} staged + committed to shard(s) {}{} (apply pending)",
                inode, parent_inode, name, shard_ino.0,
                if shard_ino != shard_dir { format!("+{}", shard_dir.0) } else { String::new() }
            );
            return Ok(());
        }

        // Strict mode: synchronous propose (waits for quorum commit) + apply wait
        //
        // Optimization: when shard_ino == shard_dir, use `propose_many` to batch
        // both entries, then poll only one store for apply (both entries land in
        // the same shard store).

        let cmd_ino = ShardCommand::CreateInode {
            info: Box::new(info.clone()),
        };
        let cmd_dir = ShardCommand::AddDirEntry {
            parent_inode,
            name: name.to_string(),
            inode,
        };

        if shard_ino == shard_dir {
            // Same shard: batch both entries into one Raft message.
            self.raft_group_manager
                .propose_many(shard_ino, vec![cmd_ino.serialize(), cmd_dir.serialize()])
                .await?;
        } else {
            // Cross-shard: sequential propose to two Raft groups.
            self.raft_group_manager
                .propose(shard_ino, cmd_ino.serialize())
                .await?;
            self.raft_group_manager
                .propose(shard_dir, cmd_dir.serialize())
                .await?;
        }

        // Strict mode: Wait for the inode record to be visible on shard_ino
        // (its authoritative location). Subsequent getattr/setattr/update_size
        // route by calculate_shard(inode), so this is the only store we
        // need to poll. Poll up to 5s to absorb propose-forwarding latency.
        let shard_store = {
            let stores = self.shard_stores.read().unwrap();
            stores
                .get(&shard_ino)
                .ok_or_else(|| format!("shard {} not found", shard_ino.0))?
                .clone()
        };
        let mut retries = 0;
        while retries < 2500 {
            if shard_store.get_inode(inode).is_some() {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(2)).await;
            retries += 1;
        }
        if retries >= 2500 {
            return Err("create: timeout waiting for inode apply".to_string());
        }

        // Also wait for the dir_entry to be visible on shard_dir (the parent's
        // shard). Without this, a subsequent lookup(parent, name) may race
        // with the apply loop and return NOT_FOUND, causing failures in
        // cp -prf (which creates dirs then immediately does lookup/chmod).
        let dir_store = {
            let stores = self.shard_stores.read().unwrap();
            stores
                .get(&shard_dir)
                .ok_or_else(|| format!("shard {} not found", shard_dir.0))?
                .clone()
        };
        let mut retries = 0;
        while retries < 2500 {
            if dir_store.get_dir_entry_inode(parent_inode, name).is_some() {
                return Ok(());
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(2)).await;
            retries += 1;
        }
        Err("create: timeout waiting for dir_entry apply".to_string())
    }

    pub async fn update_file(&self, inode: u64, size: u64, mtime: u64) -> Result<(), String> {
        let shard_id = self.shard_strategy.calculate_shard(inode);

        let cmd = ShardCommand::UpdateFile { inode, size, mtime };
        self.propose_meta(shard_id, cmd.serialize()).await?;

        Ok(())
    }

    /// Poll the shard store until the named entry under `parent_inode` is
    /// gone, or timeout (500 ms).  Raft `propose()` returns after the entry
    /// is committed and `applied_index` is advanced, but the spawned apply
    /// task that actually mutates `ShardStore` runs asynchronously.  Without
    /// this wait a subsequent operation (e.g. `rmdir` after `unlink`) can
    /// read stale state and fail with a spurious POSIX ENOTEMPTY.
    async fn wait_for_entry_removed(&self, shard_id: ShardId, parent_inode: u64, name: &str) {
        for _ in 0..100 {
            let still_exists = {
                let stores = self.shard_stores.read().unwrap();
                stores
                    .get(&shard_id)
                    .map(|s| s.get_dir_entry_inode(parent_inode, name).is_some())
                    .unwrap_or(false)
            };
            if !still_exists {
                return;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }
    }

    /// Poll the shard store until the named entry under `parent_inode`
    /// appears, or timeout (5 s).  Same rationale as
    /// `wait_for_entry_removed`: the spawned apply task runs asynchronously
    /// so the entry may not be visible immediately after `propose()` returns.
    /// Increased from 500 ms to 5 s to accommodate propose forwarding latency
    /// when the current node is not the shard leader.
    async fn wait_for_entry_appeared(&self, shard_id: ShardId, parent_inode: u64, name: &str) {
        for _ in 0..100 {
            let exists = {
                let stores = self.shard_stores.read().unwrap();
                stores
                    .get(&shard_id)
                    .map(|s| s.get_dir_entry_inode(parent_inode, name).is_some())
                    .unwrap_or(false)
            };
            if exists {
                return;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }
    }

    pub async fn delete_file(&self, parent_inode: u64, name: &str) -> Result<(), String> {
        let shard_dir = self.shard_strategy.calculate_shard(parent_inode);

        // Resolve inode via dir entry on parent's shard. Use get_dir_entry_inode
        // (not lookup) because the inode record may be on a different shard.
        let inode = {
            let stores = self.shard_stores.read().unwrap();
            let shard_store = stores
                .get(&shard_dir)
                .ok_or_else(|| format!("shard {} not found", shard_dir.0))?;

            shard_store
                .get_dir_entry_inode(parent_inode, name)
                .ok_or_else(|| "file not found".to_string())?
        };

        self.propose_remove_direntry_and_inode(parent_inode, name, inode)
            .await
    }

    /// Two-phase delete used by `delete_file`, `delete_directory`,
    /// `delete_file_by_inode`, `delete_directory_by_inode`, and
    /// `delete_object_entry`.
    ///
    /// Phase A: propose `RemoveDirEntry { parent_inode, name }` to
    /// `shard_dir = calculate_shard(parent_inode)`. The file becomes
    /// invisible to subsequent lookups immediately.
    ///
    /// Phase B: propose `DeleteInode { inode }` to
    /// `shard_ino = calculate_shard(inode)`. The inode record is removed
    /// from its authoritative location.
    ///
    /// If B fails after A succeeded, the inode is orphaned (no dir entry
    /// pointing to it); the GC scan reclaims it. The caller still surfaces
    /// success because the user-visible effect (file gone) is achieved.
    async fn propose_remove_direntry_and_inode(
        &self,
        parent_inode: u64,
        name: &str,
        inode: u64,
    ) -> Result<(), String> {
        // Stage the deletion BEFORE proposing Raft so the entry becomes
        // immediately invisible to subsequent reads (POSIX semantics on the
        // same client). MetaCache Deleted markers are swept after Raft
        // apply confirms RemoveDirEntry/DeleteInode; if propose fails, we
        // fall back to ShardStore (stale Deleted markers never win over
        // live entries on a propose failure because lookup/getattr call
        // `get_dir_entry_inode`/`get_inode` after MetaCache misses).
        self.meta_cache.stage_delete(parent_inode, name, inode);

        let shard_dir = self.shard_strategy.calculate_shard(parent_inode);
        let shard_ino = self.shard_strategy.calculate_shard(inode);

        // Phase A: remove dir entry on the parent's shard.
        let cmd_dir = ShardCommand::RemoveDirEntry {
            parent_inode,
            name: name.to_string(),
        };
        self.propose_meta(shard_dir, cmd_dir.serialize()).await?;

        // Also drop the cross-shard DirStatSummary (if any) from the parent
        // shard. Unlink/rmdir means ls on parent must not show a stale cached
        // summary for a name that no longer exists. Routed via the same
        // parent_shard — still no service-to-service forwarding.
        let cmd_summary = ShardCommand::DeleteChildSummary {
            parent_inode,
            name: name.to_string(),
        };
        if let Err(e) = self.propose_meta(shard_dir, cmd_summary.serialize()).await {
            warn!(
                "propose_remove_direntry: DeleteChildSummary propose failed (non-fatal) \
                 parent={} name={}: {}",
                parent_inode, name, e
            );
        }

        // MetaCache.stage_delete was called before propose, so the dir
        // entry is already marked Deleted in the Filer's in-memory cache.
        // Any client that receives an Invalidate and re-queries the Filer
        // will get ENOENT from MetaCache without waiting for Raft apply.
        // This is sufficient for cross-client visibility: notify_inode_change
        // broadcasts Invalidate immediately after this function returns.
        //
        // Do NOT wait_for_entry_removed here — that would add Raft apply
        // latency (1-3ms) to every unlink. The MetaCache deleted marker
        // is the authoritative fast-path, matching Ceph's projected state
        // (MDS projects the unlink before journaling).
        if !self.is_async_meta_persist() {
            self.wait_for_entry_removed(shard_dir, parent_inode, name)
                .await;
        }

        // Phase B: Check nlink to decide between DecrementNlink and DeleteInode.
        // For hardlinks (nlink > 1), only decrement nlink so other links survive.
        // For the last link (nlink == 1), delete the inode entirely.
        let nlink = {
            let stores = self.shard_stores.read().unwrap();
            if let Some(store) = stores.get(&shard_ino) {
                store.get_inode(inode).map(|info| info.nlink).unwrap_or(0)
            } else {
                0
            }
        };

        if nlink > 1 {
            // Hardlink: decrement nlink, keep inode alive for remaining links.
            let expected_nlink = nlink - 1;
            let cmd = ShardCommand::DecrementNlink { inode };
            if let Err(e) = self.propose_meta(shard_ino, cmd.serialize()).await {
                log::warn!(
                    "DecrementNlink propose failed for inode {} on shard {}: {}. \
                     Dir entry already removed; nlink may be stale.",
                    inode,
                    shard_ino.0,
                    e
                );
            } else {
                // Wait for DecrementNlink to be applied to the state machine.
                // Without this wait, a subsequent getattr may read the stale
                // nlink value (before decrement), causing T5.04 intermittent
                // failures (expected nlink=1, actual nlink=2).
                if !self.is_async_meta_persist() {
                    self.wait_for_nlink(shard_ino, inode, expected_nlink).await;
                }
            }
        } else {
            // Last link: delete the inode record. Best-effort: if this fails,
            // GC will collect the orphan inode later.
            let cmd_ino = ShardCommand::DeleteInode { inode };
            if let Err(e) = self.propose_meta(shard_ino, cmd_ino.serialize()).await {
                log::warn!(
                    "DeleteInode propose failed for inode {} on shard {}: {}. \
                     Dir entry already removed; inode will be GC'd.",
                    inode,
                    shard_ino.0,
                    e
                );
            }
        }

        Ok(())
    }

    /// Batch delete multiple files in one RPC, using `propose_many` to merge
    /// all Raft commits into minimal replication cycles.
    ///
    /// All entries must share the same `parent_inode` shard (caller groups by
    /// shard). The optimization:
    /// - Phase A: all `RemoveDirEntry` commands go to the same `shard_dir`
    ///   (same parent → same shard) → **one** `propose_many` call.
    /// - Phase B: `DeleteInode`/`DecrementNlink` commands are grouped by
    ///   `shard_ino` → **one** `propose_many` per distinct inode shard.
    ///
    /// For N entries on the same shard, this reduces Raft commits from 2N
    /// (sequential `delete_file`) to 2 (one for dir entries, one for inodes).
    ///
    /// Returns per-entry `Result<(), String>`. Entry indices match input order.
    pub async fn batch_delete_files(&self, entries: &[(u64, String)]) -> Vec<Result<(), String>> {
        let n = entries.len();
        let mut results = vec![Ok(()); n];

        // Step 1: Resolve all inodes from dir entries (read-only, no Raft).
        // Track which entries resolved successfully.
        let mut resolved: Vec<(usize, u64, u64, String)> = Vec::with_capacity(n);
        // (entry_idx, parent_inode, inode, name)

        let shard_dir = self.shard_strategy.calculate_shard(entries[0].0);
        {
            let stores = self.shard_stores.read().unwrap();
            let shard_store = match stores.get(&shard_dir) {
                Some(s) => s,
                None => {
                    // Entire batch fails — shard not found.
                    let err = format!("shard {} not found", shard_dir.0);
                    return vec![Err(err); n];
                }
            };
            for (idx, (parent_inode, name)) in entries.iter().enumerate() {
                match shard_store.get_dir_entry_inode(*parent_inode, name) {
                    Some(inode) => {
                        resolved.push((idx, *parent_inode, inode, name.clone()));
                    }
                    None => {
                        results[idx] = Err("file not found".to_string());
                    }
                }
            }
        }

        if resolved.is_empty() {
            return results;
        }

        // Stage all deletions BEFORE the Raft propose so subsequent reads
        // on this filer immediately return ENOENT (matches POSIX semantics
        // for a single client thread that unlinks then stats).
        for (_, parent_inode, inode, name) in &resolved {
            self.meta_cache.stage_delete(*parent_inode, name, *inode);
        }

        // Step 2: Batch all RemoveDirEntry commands into one propose_many on
        // shard_dir. All entries share the same parent → same shard_dir.
        // Pair each RemoveDirEntry with a DeleteChildSummary so stale
        // cross-shard stat summaries are cleared with the dentry.
        let mut dir_cmds: Vec<Vec<u8>> = Vec::with_capacity(resolved.len() * 2);
        for (_, parent_inode, _, name) in &resolved {
            dir_cmds.push(
                ShardCommand::RemoveDirEntry {
                    parent_inode: *parent_inode,
                    name: name.clone(),
                }
                .serialize(),
            );
            dir_cmds.push(
                ShardCommand::DeleteChildSummary {
                    parent_inode: *parent_inode,
                    name: name.clone(),
                }
                .serialize(),
            );
        }

        match self
            .raft_group_manager
            .propose_many(shard_dir, dir_cmds)
            .await
        {
            Ok(_) => {
                debug!(
                    "batch_delete: Phase A committed {} RemoveDirEntry + {} DeleteChildSummary cmds to shard {}",
                    resolved.len(),
                    resolved.len(),
                    shard_dir.0
                );
                // MetaCache.stage_delete was called for all entries before
                // propose_many, so all dir entries are already marked Deleted
                // in the Filer's in-memory cache. Clients that receive
                // Invalidate and re-query the Filer will get ENOENT from
                // MetaCache without waiting for Raft apply.
                // notify_inode_change broadcasts Invalidate after this function
                // returns — no need to wait_for_entry_removed here.
            }
            Err(e) => {
                // If not leader, fail entire batch — caller should redirect.
                warn!(
                    "batch_delete: Phase A propose_many failed for shard {}: {}",
                    shard_dir.0, e
                );
                for (idx, _, _, _) in &resolved {
                    results[*idx] = Err(e.clone());
                }
                return results;
            }
        }

        // Step 3: Check nlink for each resolved inode, group by shard_ino.
        // Same-shard inodes go into one propose_many.
        let mut inode_groups: HashMap<ShardId, Vec<(usize, u64, u32)>> = HashMap::new();
        // (entry_idx, inode, nlink)

        for (idx, _, inode, _) in &resolved {
            let shard_ino = self.shard_strategy.calculate_shard(*inode);
            let nlink = {
                let stores = self.shard_stores.read().unwrap();
                stores
                    .get(&shard_ino)
                    .and_then(|s| s.get_inode(*inode).map(|info| info.nlink))
                    .unwrap_or(0)
            };
            inode_groups
                .entry(shard_ino)
                .or_default()
                .push((*idx, *inode, nlink));
        }

        // Step 4: For each inode shard, batch DeleteInode/DecrementNlink.
        for (shard_ino, group) in &inode_groups {
            let cmds: Vec<Vec<u8>> = group
                .iter()
                .map(|(_, inode, nlink)| {
                    if *nlink > 1 {
                        ShardCommand::DecrementNlink { inode: *inode }.serialize()
                    } else {
                        ShardCommand::DeleteInode { inode: *inode }.serialize()
                    }
                })
                .collect();

            match self.raft_group_manager.propose_many(*shard_ino, cmds).await {
                Ok(_) => {
                    debug!(
                        "batch_delete: Phase B committed {} inode ops to shard {}",
                        group.len(),
                        shard_ino.0
                    );
                }
                Err(e) => {
                    // Phase A succeeded (dir entries removed) but Phase B
                    // failed. Inodes are orphaned — GC will clean them up.
                    // Surface success to caller (file is invisible to users).
                    warn!(
                        "batch_delete: Phase B propose_many failed for shard {}: {} \
                         — {} inodes orphaned, GC will reclaim",
                        shard_ino.0,
                        e,
                        group.len()
                    );
                }
            }
        }

        results
    }

    /// Poll the shard store until the inode's nlink reaches `expected`, or
    /// timeout (5 s). Same rationale as `wait_for_entry_removed`: the spawned
    /// apply task runs asynchronously so the nlink change may not be visible
    /// immediately after `propose()` returns.
    async fn wait_for_nlink(&self, shard_id: ShardId, inode: u64, expected: u32) {
        for _ in 0..100 {
            let current = {
                let stores = self.shard_stores.read().unwrap();
                stores
                    .get(&shard_id)
                    .and_then(|s| s.get_inode(inode))
                    .map(|info| info.nlink)
                    .unwrap_or(0)
            };
            if current == expected {
                return;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }
        log::warn!(
            "wait_for_nlink: timed out waiting for inode {} nlink to reach {} on shard {}",
            inode,
            expected,
            shard_id.0
        );
    }

    pub async fn create_directory(
        &self,
        parent_inode: u64,
        name: &str,
        // P3.1: Real mode/uid/gid from the client, embedded directly in the
        // CreateInode command. This eliminates the follow-up SetAttr propose
        // in handle_mkdir (handle_mkdir_phase_a already had its own params so
        // it does not need updating).
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<InodeInfo, String> {
        // Phase 3: Allocate the directory's inode on a different shard from
        // the parent. The dir_entry goes on the parent's shard (so lookup
        // can find it), but the inode record goes on the target shard.
        // This spreads the directory tree across shards for load balancing,
        // while keeping files on the parent's shard for readdir locality.
        let parent_shard = self.shard_strategy.calculate_shard(parent_inode);
        let target_shard = self.pick_child_dir_shard(parent_shard);
        let inode = self.alloc_inode_in_shard(target_shard);
        let now = chrono::Utc::now().timestamp() as u64;
        // Ensure S_IFDIR bit is set even if caller only provided perms.
        let dir_mode = if mode & 0o170000 != 0 {
            mode
        } else {
            mode | 0o040000
        };
        let info = InodeInfo {
            inode,
            name: name.to_string(),
            parent_inode,
            file_type: FileType::Directory,
            size: 0,
            mtime: now,
            atime: now,
            ctime: now,
            mode: dir_mode,
            uid,
            gid,
            blocks: 0,
            fid: None,
            volume_id: None,
            etag: None,
            chunks: vec![],
            inline_data: None,
            extended: HashMap::new(),
            symlink_target: None,
            nlink: 2,
            version: 0,
            delete_time: 0,
            reliability: powerfs_layout::reliability::Reliability::default(),
            reliability_state: powerfs_layout::reliability::ReliabilityState::default(),
            compression_state: powerfs_layout::reliability::CompressionState::default(),
            replica_chunks: Vec::new(),
        };

        self.propose_create_inode_and_direntry(info.clone(), parent_inode, name, inode)
            .await?;

        Ok(info)
    }

    /// Phase A of two-phase mkdir: create ONLY the inode record on target_shard.
    ///
    /// Called by `handle_mkdir_phase_a` when the client routes the request to
    /// target_shard's leader. This creates the directory inode record but does
    /// NOT add the dir entry (that's Phase B on parent_shard).
    ///
    /// The inode is pre-allocated by the client via `alloc_inode_batch`, so we
    /// just propose the CreateInode command to Raft.
    ///
    /// Returns the InodeInfo on success.
    /// See docs/shard-routing-no-forward-principle.md §3
    pub async fn create_directory_phase_a(
        &self,
        ino: u64,
        parent_inode: u64,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<InodeInfo, String> {
        let target_shard = self.shard_strategy.calculate_shard(ino);

        // Verify shard store exists
        {
            let stores = self.shard_stores.read().unwrap();
            if stores.get(&target_shard).is_none() {
                return Err(format!("shard {} not found", target_shard.0));
            }
        }

        let now = chrono::Utc::now().timestamp() as u64;
        let dir_mode = (mode & 0o7777) | 0o040000;
        let info = InodeInfo {
            inode: ino,
            name: name.to_string(),
            parent_inode,
            file_type: FileType::Directory,
            size: 0,
            mtime: now,
            atime: now,
            ctime: now,
            mode: dir_mode,
            uid,
            gid,
            blocks: 0,
            fid: None,
            volume_id: None,
            etag: None,
            chunks: vec![],
            inline_data: None,
            extended: HashMap::new(),
            symlink_target: None,
            nlink: 2,
            version: 0,
            delete_time: 0,
            reliability: powerfs_layout::reliability::Reliability::default(),
            reliability_state: powerfs_layout::reliability::ReliabilityState::default(),
            compression_state: powerfs_layout::reliability::CompressionState::default(),
            replica_chunks: Vec::new(),
        };

        // Propose ONLY CreateInode (no AddDirEntry — that's Phase B).
        // Use propose_meta_commit (wait for Raft commit) — propose_ff would
        // silently drop the entry if leadership changes, leaving an orphan
        // inode that Phase B's AddDirEntry points to → EIO on lookup.
        let cmd = ShardCommand::CreateInode {
            info: Box::new(info.clone()),
        };
        self.propose_meta_commit(target_shard, cmd.serialize())
            .await?;

        debug!(
            "mkdir phase A: inode={} parent={} name={} shard={} (CreateInode only)",
            ino, parent_inode, name, target_shard.0
        );

        Ok(info)
    }

    /// Phase B of two-phase mkdir: add ONLY the dir entry on parent_shard.
    ///
    /// Called by `handle_mkdir_phase_b` when the client routes the request to
    /// parent_shard's leader. This adds the directory entry pointing to the
    /// inode created in Phase A. It does NOT create the inode record (that was
    /// Phase A on target_shard).
    ///
    /// Along with AddDirEntry, we atomically (in Raft log order) persist a
    /// DirStatSummary for the new child on the PARENT shard. This enables
    /// `ls -l` on a cross-shard directory to return mode/uid/gid/size/mtime
    /// locally without an extra RPC to the child shard (see docs/metacache.md §3.2).
    ///
    /// Returns Ok(()) on success.
    /// See docs/shard-routing-no-forward-principle.md §3
    pub async fn create_directory_phase_b(
        &self,
        parent_inode: u64,
        name: &str,
        ino: u64,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<(), String> {
        let parent_shard = self.shard_strategy.calculate_shard(parent_inode);
        let child_shard = self.shard_strategy.calculate_shard(ino);

        // Verify shard store exists
        {
            let stores = self.shard_stores.read().unwrap();
            if stores.get(&parent_shard).is_none() {
                return Err(format!("shard {} not found", parent_shard.0));
            }
        }

        // Propose AddDirEntry first — the canonical dentry is required for
        // the lookup path even if the summary goes missing (a reader will
        // just fall back to the child shard and backfill).
        let cmd = ShardCommand::AddDirEntry {
            parent_inode,
            name: name.to_string(),
            inode: ino,
        };
        self.propose_meta_commit(parent_shard, cmd.serialize())
            .await?;

        // Now drop the DirStatSummary on the same parent shard leader.
        // `version_ts` uses millisecond wall clock; we only need monotonic
        // ordering per-child and in practice the same filer never proposes
        // two summaries for a single mkdir in reverse order.
        let now_ms = chrono::Utc::now().timestamp_millis() as u64;
        let summary = crate::shard_store::DirStatSummary {
            child_inode: ino,
            mode_and_type: (mode & 0o7777) | 0o040000,
            uid,
            gid,
            size: 0,
            mtime: now_ms / 1000,
            ctime: now_ms / 1000,
            atime: now_ms / 1000,
            nlink: 2,
            child_shard_id: child_shard.0,
            version_ts: now_ms,
        };
        let summary_cmd = ShardCommand::UpdateChildSummary {
            parent_inode,
            name: name.to_string(),
            summary,
        };
        // Fire-and-forget commit: even if this secondary Raft fails, the
        // dentry is already committed; reader backfill will re-populate the
        // summary lazily on first miss.
        if let Err(e) = self
            .propose_meta_commit(parent_shard, summary_cmd.serialize())
            .await
        {
            warn!(
                "mkdir phase B: UpdateChildSummary parent={} name={} ino={} propose failed (non-fatal): {}",
                parent_inode, name, ino, e
            );
        }

        debug!(
            "mkdir phase B: inode={} parent={} name={} shard={} (AddDirEntry + UpdateChildSummary child_shard={})",
            ino, parent_inode, name, parent_shard.0, child_shard.0
        );

        Ok(())
    }

    /// Create directory for a given path, auto-creating parent directories (mkdir -p behavior)
    pub async fn create_directory_for_path(&self, path: &str) -> Result<u64, String> {
        let parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();
        if parts.is_empty() {
            return Ok(POSIX_ROOT_INODE);
        }

        let mut current_inode = POSIX_ROOT_INODE;
        for part in &parts {
            // Check if this component already exists. Use get_dir_entry_inode
            // (not lookup) because the inode record may be on a different shard.
            let lookup_shard = self.shard_strategy.calculate_shard(current_inode);
            let existing_inode = {
                let stores = self.shard_stores.read().unwrap();
                if let Some(store) = stores.get(&lookup_shard) {
                    store.get_dir_entry_inode(current_inode, part)
                } else {
                    None
                }
            };

            if let Some(ino) = existing_inode {
                current_inode = ino;
            } else {
                // Create this directory component
                let info = self
                    .create_directory(current_inode, part, 0o040755, 0, 0)
                    .await?;
                current_inode = info.inode;
            }
        }
        Ok(current_inode)
    }

    pub async fn delete_directory(&self, parent_inode: u64, name: &str) -> Result<(), String> {
        let shard_dir = self.shard_strategy.calculate_shard(parent_inode);

        // Resolve the target directory's inode so we can check emptiness.
        // Use get_dir_entry_inode (not lookup) because the inode record may
        // be on a different shard than the dir entry.
        let child_inode = {
            let stores = self.shard_stores.read().unwrap();
            let shard_store = stores
                .get(&shard_dir)
                .ok_or_else(|| format!("shard {} not found", shard_dir.0))?;

            shard_store
                .get_dir_entry_inode(parent_inode, name)
                .ok_or_else(|| "directory not found".to_string())?
        };

        // POSIX: rmdir on a non-empty directory must fail with ENOTEMPTY.
        // Reject before proposing the Raft command so the client gets a
        // clear error (the FUSE layer maps "not empty" to libc::ENOTEMPTY).
        //
        // Use `is_directory_empty_strict` (reads from RocksDB) instead of
        // `list_directory` (reads from in-memory cache) to avoid stale
        // entries from OR-Set sync re-adding deleted dir entries via
        // `create_inode_atomic`, which would cause spurious ENOTEMPTY.
        if !self.is_directory_empty_strict(child_inode) {
            return Err("directory not empty".to_string());
        }

        self.propose_remove_direntry_and_inode(parent_inode, name, child_inode)
            .await
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

            self.propose_meta(old_shard, cmd.serialize()).await?;

            if !self.is_async_meta_persist() {
                self.wait_for_entry_appeared(old_shard, new_parent_inode, new_name)
                    .await;
            }
            Ok(())
        } else {
            // Cross-shard rename: decompose into AddDirEntry + RemoveDirEntry
            // + RenameInode. Each propose is forwarded to the respective
            // shard leader (see RaftGroup::handle_propose), so this node
            // does not need to be the leader of all involved shards.
            //
            // Phase A: resolve inode from old dir_entry (old_parent's shard)
            let inode = {
                let stores = self.shard_stores.read().unwrap();
                let shard_store = stores
                    .get(&old_shard)
                    .ok_or_else(|| format!("shard {} not found", old_shard.0))?;
                shard_store
                    .get_dir_entry_inode(old_parent_inode, old_name)
                    .ok_or_else(|| "rename: source not found".to_string())?
            };

            // Phase B: AddDirEntry on new_parent's shard
            let add_cmd = ShardCommand::AddDirEntry {
                parent_inode: new_parent_inode,
                name: new_name.to_string(),
                inode,
            };
            self.propose_meta(new_shard, add_cmd.serialize()).await?;
            if !self.is_async_meta_persist() {
                self.wait_for_entry_appeared(new_shard, new_parent_inode, new_name)
                    .await;
            }

            // Phase C: RemoveDirEntry on old_parent's shard
            let rem_cmd = ShardCommand::RemoveDirEntry {
                parent_inode: old_parent_inode,
                name: old_name.to_string(),
            };
            self.propose_meta(old_shard, rem_cmd.serialize()).await?;
            if !self.is_async_meta_persist() {
                self.wait_for_entry_removed(old_shard, old_parent_inode, old_name)
                    .await;
            }

            // Phase D: RenameInode on inode's shard (update name + parent)
            let inode_shard = self.shard_strategy.calculate_shard(inode);
            let rename_cmd = ShardCommand::RenameInode {
                inode,
                new_name: new_name.to_string(),
                new_parent_inode,
            };
            if let Err(e) = self.propose_meta(inode_shard, rename_cmd.serialize()).await {
                log::warn!(
                    "cross-shard rename: RenameInode proposal failed (dir entries already moved): {}",
                    e
                );
            }

            // POSIX: update mtime/ctime of both parent directories
            let now = chrono::Utc::now().timestamp() as u64;
            for parent_ino in [old_parent_inode, new_parent_inode] {
                let p_shard = self.shard_strategy.calculate_shard(parent_ino);
                let attr_cmd = ShardCommand::SetAttr {
                    inode: parent_ino,
                    size: None,
                    mode: None,
                    uid: None,
                    gid: None,
                    mtime: Some(now),
                    atime: Some(now),
                };
                let _ = self.propose_meta(p_shard, attr_cmd.serialize()).await;
            }

            Ok(())
        }
    }

    pub fn lookup(&self, parent_inode: u64, name: &str) -> Option<InodeInfo> {
        // With split-create, the dir entry lives on `calculate_shard(parent_inode)`
        // but the inode record lives on `calculate_shard(inode)`. These may be
        // different shards, so we:
        //   1. Find the inode number from the dir entry on the parent's shard.
        //   2. Fetch the inode record from the inode's own shard.
        let parent_shard = self.shard_strategy.calculate_shard(parent_inode);

        let inode = {
            let stores = self.shard_stores.read().unwrap();
            let shard_store = stores.get(&parent_shard)?;
            shard_store.get_dir_entry_inode(parent_inode, name)
        };

        // If dir_entry not found, return None immediately.
        let inode = match inode {
            Some(i) => i,
            None => {
                log::debug!(
                    "lookup: dir_entry not found parent_ino={} name='{}' shard={}",
                    parent_inode,
                    name,
                    parent_shard.0
                );
                return None;
            }
        };

        // Fetch the inode record from its own shard (may differ from parent_shard).
        let inode_shard = self.shard_strategy.calculate_shard(inode);
        //
        // === CORRECTNESS: cross-shard split-create apply lag ===================
        // `create_file` proposes the inode record on `inode_shard` first, then
        // proposes the dir entry on `parent_shard`. Each shard's Raft applies
        // its own log entries asynchronously, so a client that observes the
        // dir entry (e.g. via readdir / a prior lookup that returned it) can
        // race with the inode_shard apply and observe:
        //
        //   dir_entry exists (parent_shard applied)
        //   inode record MISSING (inode_shard not yet applied)
        //
        // Prior behavior: return None → open() fails with EIO from user POV.
        // For workloads where tar create and open are back-to-back this is
        // catastrophic; see the Linux kernel E2E test where this caused ~1%
        // of file opens to spuriously fail.
        //
        // Fix: short spin-wait bounded by `CROSS_SHARD_APPLY_MAX_ATTEMPTS`.
        // Typical apply lag is microseconds (same filer process, shared
        // apply thread pool), so a handful of retries with 1 ms sleep is
        // more than sufficient. If after that the inode is still missing,
        // log a WARN and return None (the old behavior) so the failure is
        // not silent.
        const CROSS_SHARD_APPLY_MAX_ATTEMPTS: u32 = 50;
        let mut info: Option<InodeInfo> = None;
        for attempt in 0..CROSS_SHARD_APPLY_MAX_ATTEMPTS {
            {
                let stores = self.shard_stores.read().unwrap();
                let shard_store = stores.get(&inode_shard);
                info = match &shard_store {
                    Some(s) => s.get_inode(inode),
                    None => None,
                };
            }
            if info.is_some() {
                break;
            }
            if attempt == 0 {
                log::debug!(
                    "lookup: inode record not yet visible for inode={} shard={} \
                     (parent={}, name='{}'), dir_entry exists — waiting for cross-shard apply...",
                    inode,
                    inode_shard.0,
                    parent_inode,
                    name
                );
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }

        let info = match info {
            Some(i) => i,
            None => {
                log::warn!(
                    "lookup: inode record not found after {} retries inode={} shard={} \
                     (parent={}, name='{}') — dir_entry exists but inode record missing \
                     (cross-shard apply lag exceeded retry budget)",
                    CROSS_SHARD_APPLY_MAX_ATTEMPTS,
                    inode,
                    inode_shard.0,
                    parent_inode,
                    name
                );
                return None;
            }
        };

        // Phase 3.5: 跳过 tombstoned 条目（延迟删除期间不可见）
        if info.delete_time > 0 {
            return None;
        }
        Some(info)
    }

    pub fn get_inode(&self, inode: u64) -> Option<InodeInfo> {
        // P3: Check MetaCache first (Staging/Dirty/Clean entries, plus Deleted
        // marker for pending unlinks). If the entry is already there, return
        // directly — skips a RocksDB round-trip + deserialization.
        if let Some(cached_result) = self.meta_cache.get_inode(inode) {
            return cached_result;
        }

        // Cache miss → fetch from ShardStore and write back as Clean so the
        // next lookup hits the fast path.
        let shard_id = self.shard_strategy.calculate_shard(inode);

        let stores = self.shard_stores.read().unwrap();
        let shard_store = stores.get(&shard_id)?;

        let info = shard_store.get_inode(inode)?;
        if info.delete_time == 0 {
            self.meta_cache.cache_put_clean(info.clone());
        }
        Some(info)
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

    /// Find inodes with chunks on `volume_id` across all shards (for migration
    /// reverse lookup). Returns (inode, shard_id, needle_id, volume_id, offset,
    /// size, file_size) tuples.
    pub fn find_inodes_by_volume(
        &self,
        volume_id: u64,
        needle_ids: &[u64],
    ) -> Vec<(u64, u64, u64, u64, u64, u64, u64)> {
        let stores = self.shard_stores.read().unwrap();
        let mut result = Vec::new();
        for shard_store in stores.values() {
            result.extend(shard_store.find_inodes_by_volume(volume_id, needle_ids));
        }
        result
    }

    /// P4: 扫描所有 shard 中 reliability_state == PendingReplicated 的文件.
    /// 返回 (inode, chunks) 对, 供 scrubber worker 进行副本复制.
    pub fn list_pending_replicated(&self) -> Vec<(u64, Vec<crate::shard_store::StoredFileChunk>)> {
        let stores = self.shard_stores.read().unwrap();
        let mut result = Vec::new();
        for shard_store in stores.values() {
            result.extend(shard_store.list_pending_replicated());
        }
        result
    }

    /// P6: 列出可进行 EC 转换的文件 (state == Replicated)
    pub fn list_pending_ec(
        &self,
        min_file_size: u64,
    ) -> Vec<(u64, Vec<crate::shard_store::StoredFileChunk>)> {
        let stores = self.shard_stores.read().unwrap();
        let mut result = Vec::new();
        for shard_store in stores.values() {
            result.extend(shard_store.list_pending_ec(min_file_size));
        }
        result
    }

    /// P4: 计算 inode 所属的 shard ID (供 scrubber 使用)
    pub fn calculate_shard_id(&self, inode: u64) -> ShardId {
        self.shard_strategy.calculate_shard(inode)
    }

    pub fn list_directory(&self, parent_inode: u64) -> Vec<InodeInfo> {
        // With split-create, dir entries live on `calculate_shard(parent_inode)`
        // but each inode record lives on `calculate_shard(inode)`. We fetch
        // the (name, inode) pairs from the parent's shard, then resolve each
        // inode record from its own shard.
        let parent_shard = self.shard_strategy.calculate_shard(parent_inode);

        let pairs = {
            let stores = self.shard_stores.read().unwrap();
            match stores.get(&parent_shard) {
                Some(shard_store) => shard_store.list_dir_entry_inodes(parent_inode),
                None => Vec::new(),
            }
        };

        let stores = self.shard_stores.read().unwrap();
        let mut result = Vec::new();
        for (name, inode) in pairs {
            let inode_shard = self.shard_strategy.calculate_shard(inode);
            if let Some(shard_store) = stores.get(&inode_shard) {
                if let Some(mut info) = shard_store.get_inode(inode) {
                    // Phase 3.5: 跳过 tombstoned 条目
                    if info.delete_time > 0 {
                        continue;
                    }
                    info.name = name;
                    info.parent_inode = parent_inode;
                    result.push(info);
                }
            }
        }
        result
    }

    /// Paginated directory listing with lightweight DirEntry.
    ///
    /// Optimization over `list_directory`:
    ///   1. Pushes pagination to ShardStore (BTreeMap seek → O(log n + limit))
    ///      instead of fetching all entries → O(n).
    ///   2. Returns lightweight DirEntry (no chunks/inline_data clone).
    ///   3. Single lock acquisition per shard for inode resolution.
    ///
    /// Cross-shard resolution: dir entries live on the parent's shard, but
    /// each inode record lives on `calculate_shard(inode)`. We fetch paginated
    /// (name, inode) pairs from the parent's shard, then resolve each inode
    /// via `get_inode_metadata` (lightweight, no chunks/inline_data).
    pub fn list_directory_paginated(
        &self,
        parent_inode: u64,
        last_name: &str,
        limit: usize,
    ) -> (Vec<DirEntry>, bool) {
        let parent_shard = self.shard_strategy.calculate_shard(parent_inode);

        // Fetch paginated (name, inode) pairs from parent's shard.
        // Request limit+1 pairs to determine has_more.
        let (pairs, has_more_pairs) = {
            let stores = self.shard_stores.read().unwrap();
            match stores.get(&parent_shard) {
                Some(shard_store) => {
                    shard_store.list_dir_entry_inodes_paginated(parent_inode, last_name, limit)
                }
                None => return (Vec::new(), false),
            }
        };

        // Fast path: if all inodes are on the same shard as the parent
        // (common case — sequential inode allocation), use the single-shard
        // paginated method which acquires both locks once.
        let all_same_shard = pairs
            .iter()
            .all(|(_, ino)| self.shard_strategy.calculate_shard(*ino) == parent_shard);

        if all_same_shard {
            let stores = self.shard_stores.read().unwrap();
            if let Some(shard_store) = stores.get(&parent_shard) {
                return shard_store.list_directory_paginated(parent_inode, last_name, limit);
            }
            return (Vec::new(), false);
        }

        // Cross-shard path: resolve each inode from its own shard.
        //
        // Fallback #1 (remote / offline shard): when stores.get(inode_shard)
        // returns None (this filer doesn't own the child shard) OR
        // get_inode_metadata returns None (e.g. inode was created by Phase A
        // but Phase B hasn't replicated to every local follower yet, or the
        // shard is currently on a different filer), we use the
        // DirStatSummary cached on the parent shard via child_summaries —
        // this is exactly the fast-path the cross-shard summary design was
        // built for: we can still return mode/uid/gid/size/mtime back to
        // FUSE without a round-trip to the child shard's leader. The
        // child_shard_id still comes from calculate_shard so callers can
        // route SetAttr / other ops correctly.
        //
        // Fallback #2 (no summary either): drop the entry from the returned
        // list. The FUSE client will later re-lookup that specific name if
        // the user calls stat(), which triggers a proper per-child getattr
        // that routes to the correct shard (no forwarding).
        let stores = self.shard_stores.read().unwrap();
        let parent_store = stores.get(&parent_shard);
        let mut result = Vec::with_capacity(limit + 1);

        for (name, inode) in pairs {
            if result.len() >= limit {
                break;
            }
            let inode_shard = self.shard_strategy.calculate_shard(inode);
            // Try local child shard store first (covers same-filer
            // multi-shard ownership, or a follower that has applied the
            // CreateInode locally).
            let meta = stores
                .get(&inode_shard)
                .and_then(|ss| ss.get_inode_metadata(inode));
            if let Some(meta) = meta {
                result.push(DirEntry {
                    inode,
                    name,
                    mode: meta.mode,
                    uid: meta.uid,
                    gid: meta.gid,
                    size: meta.size,
                    atime: meta.atime,
                    mtime: meta.mtime,
                    ctime: meta.ctime,
                    nlink: meta.nlink,
                });
                continue;
            }
            // Fallback: parent shard cached DirStatSummary (written during
            // mkdir Phase B's UpdateChildSummary apply on this leader).
            let summary = parent_store.and_then(|ps| ps.get_child_summary(parent_inode, &name));
            if let Some(s) = summary {
                result.push(DirEntry {
                    inode,
                    name,
                    mode: s.mode_and_type,
                    uid: s.uid,
                    gid: s.gid,
                    size: s.size,
                    atime: s.atime,
                    mtime: s.mtime,
                    ctime: s.ctime,
                    nlink: s.nlink,
                });
                continue;
            }
            // No local info available — drop from this page. Callers will
            // trigger a per-name getattr if a user actually queries that
            // entry (e.g. `ls -l` does a per-entry stat after readdir, so
            // each missing attr still gets routed to the owning shard).
        }

        // has_more if we fetched limit+1 pairs (more entries in BTreeMap)
        // or if we still have limit entries after cross-shard resolution.
        let has_more = has_more_pairs || result.len() > limit;
        if result.len() > limit {
            result.truncate(limit);
        }

        (result, has_more)
    }

    /// Strict empty-directory check that reads dir entries from RocksDB
    /// (bypassing the in-memory cache) to avoid stale entries from OR-Set
    /// sync. Used by `delete_directory` (rmdir) for the POSIX ENOTEMPTY
    /// check. Cross-shard inode resolution is performed using the in-memory
    /// cache; tombstoned inodes (delete_time > 0) are filtered out.
    pub fn is_directory_empty_strict(&self, parent_inode: u64) -> bool {
        let parent_shard = self.shard_strategy.calculate_shard(parent_inode);

        // Read dir entries from RocksDB (source of truth)
        let pairs = {
            let stores = self.shard_stores.read().unwrap();
            match stores.get(&parent_shard) {
                Some(shard_store) => shard_store.list_dir_entry_inodes_rocksdb(parent_inode),
                None => return true, // no shard → empty
            }
        };

        if pairs.is_empty() {
            return true;
        }

        // Resolve each inode and check if any are live (not tombstoned)
        let stores = self.shard_stores.read().unwrap();
        for (_name, inode) in pairs {
            let inode_shard = self.shard_strategy.calculate_shard(inode);
            if let Some(shard_store) = stores.get(&inode_shard) {
                if let Some(info) = shard_store.get_inode(inode) {
                    if info.delete_time == 0 {
                        return false; // Found a live entry → not empty
                    }
                }
            }
        }

        // All entries are either missing or tombstoned → empty
        true
    }

    pub fn get_shard_stats(&self, shard_id: ShardId) -> Option<ShardStats> {
        let stores = self.shard_stores.read().unwrap();
        stores.get(&shard_id).map(|s| s.get_stats())
    }

    /// Phase 5 §5.3: borrow a shard's `ShardStore` for direct
    /// RocksDB access (used by `RaftLeasePersistence` for local
    /// reads of `CF_LEASES`). Returns `None` if the shard isn't
    /// initialized on this node.
    ///
    /// Sync variant of [`get_shard_store`]: that one is async and
    /// creates the shard on-demand; this one is for callers that
    /// already know the shard exists (the filer at startup) and
    /// want a non-async path.
    pub fn try_get_shard_store(&self, shard_id: ShardId) -> Option<Arc<ShardStore>> {
        self.shard_stores.read().unwrap().get(&shard_id).cloned()
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
                inline_data: None,
                extended: std::collections::HashMap::new(),
                symlink_target: None,
                nlink: 2,
                version: 0,
                delete_time: 0,
                reliability: powerfs_layout::reliability::Reliability::default(),
                reliability_state: powerfs_layout::reliability::ReliabilityState::default(),
                compression_state: powerfs_layout::reliability::CompressionState::default(),
                replica_chunks: Vec::new(),
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

    /// Allocate an inode within a specific shard's range.
    ///
    /// This is the shard-aware replacement for the old `generate_inode()`.
    /// It ensures the allocated inode routes to the specified shard via
    /// `calculate_shard(inode)`, enabling:
    /// - Files to be placed on the parent directory's shard (readdir locality)
    /// - Directories to be placed on a different shard (tree distribution)
    ///
    /// The inode is allocated from this node's non-overlapping slot within
    /// the shard range, so multiple filer nodes can allocate concurrently
    /// without collisions.
    pub fn alloc_inode_in_shard(&self, shard_id: ShardId) -> u64 {
        let allocators = self.shard_allocators.read().unwrap();
        let alloc = &allocators[shard_id.0 as usize];
        let n = alloc.counter.fetch_add(1, Ordering::SeqCst);
        let inode = alloc.shard_start + alloc.node_offset + n;
        debug!(
            "alloc_inode_in_shard: shard={} inode={} (start={} offset={} counter={})",
            shard_id.0, inode, alloc.shard_start, alloc.node_offset, n
        );
        inode
    }

    /// Pick a target shard for a new child directory.
    ///
    /// Strategy: round-robin to the next shard after the parent's shard.
    /// This spreads the directory tree across shards while keeping files
    /// on the parent's shard for readdir locality.
    ///
    /// For single-shard clusters, returns the same shard.
    pub fn pick_child_dir_shard(&self, parent_shard: ShardId) -> ShardId {
        let count = self.shard_strategy.get_shard_count();
        if count <= 1 {
            return parent_shard;
        }
        ShardId((parent_shard.0 + 1) % count)
    }

    /// Legacy inode allocation — allocates from shard 0's range.
    ///
    /// Kept for backward compatibility with callers that haven't been
    /// migrated to `alloc_inode_in_shard` yet. New code should use
    /// `alloc_inode_in_shard` with an explicit shard.
    pub fn generate_inode(&self) -> u64 {
        self.alloc_inode_in_shard(ShardId(0))
    }

    /// Recover shard allocators by scanning existing inodes in RocksDB.
    ///
    /// After all shard stores are loaded, scan `CF_INODES` for the max
    /// inode in each shard's range (for this node's slot) and advance
    /// each shard's counter past it. This prevents inode reuse after
    /// filer restart.
    pub fn recover_inode_generator(&self) {
        let allocators = self.shard_allocators.read().unwrap();
        let stores = self.shard_stores.read().unwrap();

        for (idx, alloc) in allocators.iter().enumerate() {
            let shard_id = ShardId(idx as u64);
            let slot_start = alloc.shard_start + alloc.node_offset;
            let slot_end = alloc.shard_start
                + alloc.node_offset
                + (self.shard_strategy.get_shard_range(shard_id).1
                    - self.shard_strategy.get_shard_range(shard_id).0)
                    / MAX_FILER_NODES;

            let mut max_existing = slot_start;
            for store in stores.values() {
                let max = store.get_max_inode_in_range(slot_start, slot_end);
                if max > max_existing {
                    max_existing = max;
                }
            }

            // Convert max existing inode back to counter value
            let max_counter = max_existing.saturating_sub(alloc.shard_start + alloc.node_offset);
            let current = alloc.counter.load(Ordering::SeqCst);
            if max_counter > current {
                alloc.counter.store(max_counter + 1, Ordering::SeqCst);
                info!(
                    "Recovered shard_allocator[{}]: counter {} -> {} (node_id={}, slot=[{}, {}), max_inode={})",
                    idx, current, max_counter + 1, self.node_id,
                    slot_start, slot_end, max_existing
                );
            } else {
                debug!(
                    "shard_allocator[{}] counter {} is already >= scanned max {} (node_id={})",
                    idx, current, max_counter, self.node_id
                );
            }
        }
    }

    pub fn get_shard_strategy(&self) -> Arc<ShardStrategy> {
        self.shard_strategy.clone()
    }

    pub async fn create_file_with_shard(
        &self,
        parent_inode: u64,
        name: &str,
        // Legacy client-supplied shard hint. Ignored — the inode is now
        // written to `calculate_shard(inode)` and the dir entry to
        // `calculate_shard(parent_inode)`. Kept in the signature so FUSE
        // clients that still pass `ShardId(parent)` do not break.
        _shard_id: ShardId,
        // P3.1: Real mode/uid/gid from the client, embedded directly into
        // the CreateInode command so we eliminate the follow-up SetAttr
        // propose. Eliminates one Raft round-trip per create().
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<u64, String> {
        let t0 = std::time::Instant::now();

        // Phase 3: Allocate inode within the parent directory's shard range
        // (same as create_file). The _shard_id parameter is ignored.
        let parent_shard = self.shard_strategy.calculate_shard(parent_inode);
        let inode = self.alloc_inode_in_shard(parent_shard);
        let now = chrono::Utc::now().timestamp() as u64;
        let info = InodeInfo {
            inode,
            name: name.to_string(),
            parent_inode,
            file_type: FileType::File,
            size: 0,
            mtime: now,
            atime: now,
            ctime: now,
            // Ensure S_IFREG is set even if the caller only passed permission bits
            mode: if mode & 0o170000 != 0 {
                mode
            } else {
                mode | 0o100000
            },
            uid,
            gid,
            blocks: 0,
            fid: None,
            volume_id: None,
            etag: None,
            chunks: vec![],
            inline_data: None,
            extended: HashMap::new(),
            symlink_target: None,
            nlink: 1,
            version: 0,
            delete_time: 0,
            reliability: powerfs_layout::reliability::Reliability::default(),
            reliability_state: powerfs_layout::reliability::ReliabilityState::default(),
            compression_state: powerfs_layout::reliability::CompressionState::default(),
            replica_chunks: Vec::new(),
        };

        self.propose_create_inode_and_direntry(info.clone(), parent_inode, name, inode)
            .await?;

        log::info!(
            "create_file_with_shard latency: total={}ms, inode={}, mode={:o}, uid={}, gid={}",
            t0.elapsed().as_millis(),
            inode,
            mode,
            uid,
            gid
        );
        Ok(inode)
    }

    pub async fn delete_file_by_inode(
        &self,
        inode: u64,
        // Legacy client-supplied shard hint. Ignored: we route dir-entry
        // removal by parent_inode and inode deletion by inode.
        _shard_id: ShardId,
    ) -> Result<(), String> {
        // Look up the inode record on its own hash-derived shard to recover
        // (parent_inode, name) so we can also remove the dir entry.
        let shard_ino = self.shard_strategy.calculate_shard(inode);
        let (parent_inode, name) = {
            let stores = self.shard_stores.read().unwrap();
            let shard_store = stores
                .get(&shard_ino)
                .ok_or_else(|| format!("shard {} not found", shard_ino.0))?;

            let inode_info = shard_store
                .get_inode(inode)
                .ok_or_else(|| "file not found".to_string())?;

            (inode_info.parent_inode, inode_info.name.clone())
        };

        self.propose_remove_direntry_and_inode(parent_inode, &name, inode)
            .await
    }

    pub async fn delete_directory_by_inode(
        &self,
        inode: u64,
        _shard_id: ShardId,
    ) -> Result<(), String> {
        let shard_ino = self.shard_strategy.calculate_shard(inode);
        let (parent_inode, name) = {
            let stores = self.shard_stores.read().unwrap();
            let shard_store = stores
                .get(&shard_ino)
                .ok_or_else(|| format!("shard {} not found", shard_ino.0))?;

            let inode_info = shard_store
                .get_inode(inode)
                .ok_or_else(|| "directory not found".to_string())?;

            (inode_info.parent_inode, inode_info.name.clone())
        };

        self.propose_remove_direntry_and_inode(parent_inode, &name, inode)
            .await
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
        self.propose_meta(shard_id, cmd.serialize()).await?;
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

            self.propose_meta(old_shard_id, cmd.serialize()).await?;

            if !self.is_async_meta_persist() {
                self.wait_for_entry_appeared(old_shard_id, new_parent_ino, new_name)
                    .await;
            }
            Ok(())
        } else {
            Err("cross-shard rename not supported yet".to_string())
        }
    }

    /// Set inode attributes via Raft consensus
    #[allow(clippy::too_many_arguments)]
    pub async fn setattr(
        &self,
        inode: u64,
        shard_id: ShardId,
        size: Option<u64>,
        mode: Option<u64>,
        uid: Option<u64>,
        gid: Option<u64>,
        mtime: Option<u64>,
        atime: Option<u64>,
    ) -> Result<(), String> {
        // MetaCache Dirty write-through: apply the caller's intended values to
        // the cached copy BEFORE the Raft propose. Subsequent getattrs on this
        // filer see the new mode/uid/... immediately (no stale reads between
        // propose and apply). The Dirty flag is demoted to Clean once ShardStore
        // apply confirms via confirm_dirty().
        let current_for_dirty = {
            let stores = self.shard_stores.read().unwrap();
            stores.get(&shard_id).and_then(|s| s.get_inode(inode))
        };
        let meta_cache = self.meta_cache.clone();
        meta_cache.mark_dirty(inode, current_for_dirty, |info| {
            if let Some(sz) = size {
                info.size = sz;
            }
            if let Some(m) = mode {
                info.mode = (m as u32) & 0o7777;
            }
            if let Some(u) = uid {
                info.uid = u as u32;
            }
            if let Some(g) = gid {
                info.gid = g as u32;
            }
            if let Some(mt) = mtime {
                info.mtime = mt;
            }
            if let Some(at) = atime {
                info.atime = at;
            }
        });

        let cmd = ShardCommand::SetAttr {
            inode,
            size,
            mode,
            uid,
            gid,
            mtime,
            atime,
        };

        self.propose_meta(shard_id, cmd.serialize()).await?;

        // Strict mode only: Wait for the command to be applied to the state
        // machine. Must check ALL changed fields (mode, uid, gid, size), not
        // just mode, otherwise chown (UID|GID only) returns before Raft
        // applies the change, causing subsequent GetAttr to read stale data.
        //
        // In async mode (default), skip the wait — propose_meta already
        // returned after submitting to RaftCore (fire-and-forget). Apply
        // happens asynchronously; readers use retry logic in net_handler
        // (handle_lookup/handle_getattr) to absorb the visibility gap.
        if !self.is_async_meta_persist()
            && (mode.is_some()
                || uid.is_some()
                || gid.is_some()
                || size.is_some()
                || mtime.is_some()
                || atime.is_some())
        {
            let store = {
                let stores = self.shard_stores.read().unwrap();
                stores.get(&shard_id).cloned()
            };
            if let Some(store) = store {
                // Increased from 50 (500ms) to 200 (2s) to handle concurrent
                // Raft commands. Under load (e.g. rsync -a syncing many files
                // while release paths sync size/chunks), the Raft group may
                // take longer to apply the SETATTR command. The previous 500ms
                // timeout caused spurious EIO when UPDATE_SIZE_CHUNKS (direct
                // write, bypassing Raft) was running concurrently.
                let mut retries = 0;
                while retries < 200 {
                    if let Some(info) = store.get_inode(inode) {
                        let mode_ok =
                            mode.is_none_or(|m| (info.mode & 0o7777) == (m as u32 & 0o7777));
                        let uid_ok = uid.is_none_or(|u| info.uid == u as u32);
                        let gid_ok = gid.is_none_or(|g| info.gid == g as u32);
                        let size_ok = size.is_none_or(|s| info.size == s);
                        let mtime_ok = mtime.is_none_or(|mt| info.mtime == mt);
                        let atime_ok = atime.is_none_or(|at| info.atime == at);
                        if mode_ok && uid_ok && gid_ok && size_ok && mtime_ok && atime_ok {
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
        // MetaCache Dirty write-through for size (same rationale as setattr()).
        let current_for_dirty = {
            let stores = self.shard_stores.read().unwrap();
            stores.get(&shard_id).and_then(|s| s.get_inode(inode))
        };
        self.meta_cache
            .mark_dirty(inode, current_for_dirty, |info| {
                if let Some(sz) = size {
                    info.size = sz;
                }
            });

        let cmd = ShardCommand::SetAttrData { inode, size };

        self.propose_meta(shard_id, cmd.serialize()).await?;

        // Strict mode only: Wait for the command to be applied.
        //
        // In async mode (default), propose_meta fire-and-forget returns
        // immediately after submitting to RaftCore. The size update will
        // commit and apply asynchronously. Readers (handle_getattr) retry
        // briefly to absorb the visibility gap.
        //
        // Trade-off: if leader crashes before commit, the size update is
        // lost. For performance mode this is acceptable — the FUSE client's
        // write path already persists data chunks to volume servers; only
        // the inode size metadata may be stale until Raft catches up.
        if !self.is_async_meta_persist() {
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

        // MetaCache projected state for setattr_meta: the CRDT merged
        // value is what we intend; Raft apply confirms it via SetAttrMeta
        // handler.
        //
        // CRITICAL: Must update MetaCache with the merged values, not just
        // mark Dirty. Otherwise cross-client getattr hits MetaCache and
        // returns stale mode/mtime/atime. This was the root cause of T1.3
        // "utimes cross-client not synced" and "directory chmod not visible".
        let merged_copy = inode_info.clone();
        store.update_inode(inode_info)?;
        self.meta_cache.project_setattr_meta(inode, merged_copy);

        // Propagate SetAttrMeta to all filers via Raft (fire-and-forget in
        // async mode, propose_meta_commit in strict mode). Without this,
        // leader-only update leaves followers with stale mode/uid/gid,
        // causing cross-client getattr to return old values.
        //
        // CRITICAL: This was the root cause of T1.3 "chmod cross-client not
        // synced": the leader's shard_store was updated, but followers never
        // received the change because no Raft propose was made.
        //
        // Design principle: MetaCache is updated first (immediate local
        // visibility), then propose_ff propagates to followers asynchronously.
        // If propose_ff is dropped (e.g. leader lost lease), the local update
        // remains — followers will catch up via future proposals or remain
        // stale until anti-entropy reconciliation (future work).
        let cmd = ShardCommand::SetAttrMeta {
            inode,
            mode,
            uid,
            gid,
            mtime,
            atime,
            client_id: client_id.to_string(),
            timestamp,
        };
        let async_mode = self.is_async_meta_persist();
        info!(
            "setattr_meta: proposing SetAttrMeta for inode={} shard={} async={}",
            inode, shard_id.0, async_mode
        );
        if let Err(e) = self.propose_meta(shard_id, cmd.serialize()).await {
            warn!(
                "setattr_meta: propose failed (may be dropped in async mode): {}",
                e
            );
            // Don't fail the request: leader already updated local store +
            // MetaCache. propose_ff is best-effort; lost entries are
            // acceptable per design principle.
        } else {
            info!(
                "setattr_meta: propose succeeded for inode={} shard={}",
                inode, shard_id.0
            );
        }

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

        self.propose_meta(shard_id, cmd.serialize()).await?;

        // Strict mode only: Wait for the command to be applied.
        // In async mode, propose_meta returns immediately; readers retry.
        if !self.is_async_meta_persist() {
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
        }

        Ok(())
    }

    /// P3: Set an extended attribute on an inode via Raft consensus.
    /// Used for `powerfs.placement` xattr on directories.
    pub async fn set_xattr(
        &self,
        inode: u64,
        shard_id: ShardId,
        key: &str,
        value: Vec<u8>,
    ) -> Result<(), String> {
        let cmd = ShardCommand::SetXattr {
            inode,
            key: key.to_string(),
            value,
        };

        self.propose_meta(shard_id, cmd.serialize()).await?;

        // Strict mode only: Wait for the command to be applied.
        // In async mode, propose_meta returns immediately; readers retry.
        if !self.is_async_meta_persist() {
            let store = {
                let stores = self.shard_stores.read().unwrap();
                stores.get(&shard_id).cloned()
            };
            if let Some(store) = store {
                let mut retries = 0;
                while retries < 20 {
                    if let Some(info) = store.get_inode(inode) {
                        if info.extended.contains_key(key) {
                            return Ok(());
                        }
                    }
                    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
                    retries += 1;
                }
                return Err("set_xattr timeout waiting for apply".to_string());
            }
        }

        Ok(())
    }

    /// Remove an extended attribute from an inode via Raft consensus.
    /// Waits for the command to be applied before returning.
    pub async fn remove_xattr(
        &self,
        inode: u64,
        shard_id: ShardId,
        key: &str,
    ) -> Result<(), String> {
        let cmd = ShardCommand::RemoveXattr {
            inode,
            key: key.to_string(),
        };

        self.propose_meta(shard_id, cmd.serialize()).await?;

        // Strict mode only: Wait for the command to be applied.
        // In async mode, propose_meta returns immediately; readers retry.
        if !self.is_async_meta_persist() {
            let store = {
                let stores = self.shard_stores.read().unwrap();
                stores.get(&shard_id).cloned()
            };
            if let Some(store) = store {
                let mut retries = 0;
                while retries < 20 {
                    if let Some(info) = store.get_inode(inode) {
                        if !info.extended.contains_key(key) {
                            return Ok(());
                        }
                    }
                    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
                    retries += 1;
                }
                return Err("remove_xattr timeout waiting for apply".to_string());
            }
        }

        Ok(())
    }

    /// P4: Update reliability state via Raft consensus.
    /// Called by scrubber worker after completing replica replication.
    pub async fn update_reliability(
        &self,
        inode: u64,
        shard_id: ShardId,
        reliability: powerfs_layout::reliability::Reliability,
        reliability_state: powerfs_layout::reliability::ReliabilityState,
        replica_chunks: Vec<crate::shard_store::StoredFileChunk>,
    ) -> Result<(), String> {
        let target_state = reliability_state.clone();
        let cmd = ShardCommand::UpdateReliability {
            inode,
            reliability,
            reliability_state,
            replica_chunks,
        };

        self.propose_meta(shard_id, cmd.serialize()).await?;

        // Strict mode only: Wait for the command to be applied.
        // In async mode, propose_meta returns immediately. This is a
        // background scrubber operation, not on the client critical path,
        // but we still respect the global async flag for consistency.
        if !self.is_async_meta_persist() {
            let store = {
                let stores = self.shard_stores.read().unwrap();
                stores.get(&shard_id).cloned()
            };
            if let Some(store) = store {
                let mut retries = 0;
                while retries < 20 {
                    if let Some(info) = store.get_inode(inode) {
                        if info.reliability_state == target_state {
                            return Ok(());
                        }
                    }
                    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
                    retries += 1;
                }
                return Err("update_reliability timeout waiting for apply".to_string());
            }
        }

        Ok(())
    }

    /// P6: Update inode to EC state via Raft consensus.
    /// Replaces chunks with data+parity shards, clears replica_chunks.
    pub async fn update_to_ec(
        &self,
        inode: u64,
        shard_id: ShardId,
        reliability: powerfs_layout::reliability::Reliability,
        reliability_state: powerfs_layout::reliability::ReliabilityState,
        ec_chunks: Vec<crate::shard_store::StoredFileChunk>,
    ) -> Result<(), String> {
        let target_state = reliability_state.clone();
        let cmd = ShardCommand::UpdateToEC {
            inode,
            reliability,
            reliability_state,
            ec_chunks,
        };

        self.propose_meta(shard_id, cmd.serialize()).await?;

        // Strict mode only: Wait for the command to be applied.
        if !self.is_async_meta_persist() {
            let store = {
                let stores = self.shard_stores.read().unwrap();
                stores.get(&shard_id).cloned()
            };
            if let Some(store) = store {
                let mut retries = 0;
                while retries < 20 {
                    if let Some(info) = store.get_inode(inode) {
                        if info.reliability_state == target_state {
                            return Ok(());
                        }
                    }
                    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
                    retries += 1;
                }
                return Err("update_to_ec timeout waiting for apply".to_string());
            }
        }

        Ok(())
    }
    pub async fn create_symlink(
        &self,
        parent_inode: u64,
        name: &str,
        target: &str,
    ) -> Result<InodeInfo, String> {
        // Phase 3: Symlinks follow the parent's shard (like files).
        let parent_shard = self.shard_strategy.calculate_shard(parent_inode);
        let inode = self.alloc_inode_in_shard(parent_shard);
        let now = chrono::Utc::now().timestamp() as u64;
        let info = InodeInfo {
            inode,
            name: name.to_string(),
            parent_inode,
            file_type: FileType::Symlink,
            size: target.len() as u64,
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
            inline_data: None,
            extended: HashMap::new(),
            symlink_target: Some(target.to_string()),
            nlink: 1,
            version: 0,
            delete_time: 0,
            reliability: powerfs_layout::reliability::Reliability::default(),
            reliability_state: powerfs_layout::reliability::ReliabilityState::default(),
            compression_state: powerfs_layout::reliability::CompressionState::default(),
            replica_chunks: Vec::new(),
        };

        self.propose_create_inode_and_direntry(info.clone(), parent_inode, name, inode)
            .await?;

        Ok(info)
    }

    /// Create a hard link via Raft consensus
    pub async fn create_hard_link(
        &self,
        inode: u64,
        new_parent_inode: u64,
        new_name: &str,
    ) -> Result<(), String> {
        // Hard link = bump nlink on the existing inode (on its own shard) +
        // add a new dir entry pointing to it (on the new parent's shard).
        let shard_ino = self.shard_strategy.calculate_shard(inode);
        let shard_dir = self.shard_strategy.calculate_shard(new_parent_inode);

        info!(
            "create_hard_link: inode={}, shard_ino={}, new_parent={}, shard_dir={}, new_name={}",
            inode, shard_ino.0, new_parent_inode, shard_dir.0, new_name
        );

        // Phase A: bump nlink on the inode's own shard.
        let cmd_nlink = ShardCommand::IncrementNlink { inode };
        match self
            .raft_group_manager
            .propose(shard_ino, cmd_nlink.serialize())
            .await
        {
            Ok(idx) => info!(
                "create_hard_link: Phase A IncrementNlink proposed on shard {} at index {}",
                shard_ino.0, idx
            ),
            Err(e) => {
                error!(
                    "create_hard_link: Phase A IncrementNlink FAILED on shard {}: {}",
                    shard_ino.0, e
                );
                return Err(e);
            }
        }

        // Phase B: add dir entry on the new parent's shard. If this fails
        // after A succeeded, nlink is over-counted by 1 — the next unlink
        // will decrement it back. Acceptable; no orphan inode is created.
        let cmd_dir = ShardCommand::AddDirEntry {
            parent_inode: new_parent_inode,
            name: new_name.to_string(),
            inode,
        };
        match self
            .raft_group_manager
            .propose(shard_dir, cmd_dir.serialize())
            .await
        {
            Ok(idx) => info!(
                "create_hard_link: Phase B AddDirEntry proposed on shard {} at index {}",
                shard_dir.0, idx
            ),
            Err(e) => {
                error!(
                    "create_hard_link: Phase B AddDirEntry FAILED on shard {}: {}",
                    shard_dir.0, e
                );
                return Err(e);
            }
        }

        self.wait_for_entry_appeared(shard_dir, new_parent_inode, new_name)
            .await;

        // Verify nlink was actually incremented
        match self.get_inode(inode) {
            Some(info) => info!(
                "create_hard_link: verified nlink={} for inode={}",
                info.nlink, inode
            ),
            None => warn!(
                "create_hard_link: inode {} not found after link creation",
                inode
            ),
        }

        Ok(())
    }

    pub async fn list_entries(
        &self,
        parent_inode: u64,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<InodeInfo>, String> {
        // Cross-shard aware: fetch dir entry pairs from the given shard, then
        // resolve each inode from its own shard.
        let pairs = {
            let stores = self.shard_stores.read().unwrap();
            let shard_store = stores
                .get(&shard_id)
                .ok_or_else(|| format!("shard {} not found", shard_id.0))?;
            shard_store.list_dir_entry_inodes(parent_inode)
        };

        let stores = self.shard_stores.read().unwrap();
        let mut result = Vec::new();
        for (name, inode) in pairs {
            let inode_shard = self.shard_strategy.calculate_shard(inode);
            if let Some(shard_store) = stores.get(&inode_shard) {
                if let Some(mut info) = shard_store.get_inode(inode) {
                    if info.delete_time > 0 {
                        continue;
                    }
                    info.name = name;
                    info.parent_inode = parent_inode;
                    result.push(info);
                }
            }
        }
        Ok(result.into_iter().take(limit).collect())
    }

    pub async fn lookup_entry(
        &self,
        parent_inode: u64,
        name: &str,
        shard_id: ShardId,
    ) -> Result<u64, String> {
        // P3: Check MetaCache first (Staging/Dirty/Clean + Deleted markers for
        // pending unlinks). Hit skips the ShardStore dir-index lookup.
        if let Some(cached_result) = self.meta_cache.get_direntry(parent_inode, name) {
            return cached_result.ok_or_else(|| "entry not found".to_string());
        }

        // Miss → read from ShardStore and backfill a Clean direntry so the
        // next lookup on this (parent, name) pair is served from MetaCache.
        let stores = self.shard_stores.read().unwrap();
        let shard_store = stores
            .get(&shard_id)
            .ok_or_else(|| format!("shard {} not found", shard_id.0))?;

        let child = shard_store
            .get_dir_entry_inode(parent_inode, name)
            .ok_or_else(|| "entry not found".to_string())?;
        self.meta_cache
            .cache_put_clean_direntry(parent_inode, name, child);
        Ok(child)
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
            // Use get_dir_entry_inode (not lookup) for cross-shard safety:
            // the inode record may be on a different shard than the dir entry.
            let shard_id = self.shard_strategy.calculate_shard(current_inode);
            let inode = {
                let stores = self.shard_stores.read().unwrap();
                let shard_store = stores
                    .get(&shard_id)
                    .ok_or_else(|| format!("shard {} not found", shard_id.0))?;
                shard_store
                    .get_dir_entry_inode(current_inode, part)
                    .ok_or_else(|| {
                        format!(
                            "path component '{}' not found in directory {}",
                            part, current_inode
                        )
                    })?
            };

            current_inode = inode;
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

        // Retry propose with backoff to handle leader election and forwarding.
        // openraft returns one of:
        //   - "not the leader"
        //   - "has to forward request to: Some(\"<id>\"), Some(BasicNode { addr: ... })"
        // when the local node is not the leader. We retry until leader election completes.
        let mut propose_retries = 0;
        loop {
            match self
                .raft_group_manager
                .propose(shard_id, data.clone())
                .await
            {
                Ok(idx) => {
                    info!(
                        "POSIX root proposed at log index {} on shard {}",
                        idx, shard_id.0
                    );
                    break;
                }
                Err(e) => {
                    let is_forward =
                        e.contains("not the leader") || e.contains("has to forward request to");
                    if is_forward {
                        propose_retries += 1;
                        if propose_retries >= 60 {
                            return Err(format!(
                                "failed to propose POSIX root: leader election timeout after {} retries: {}",
                                propose_retries, e
                            ));
                        }
                        debug!(
                            "Waiting for Raft leader election (retry {}/60): {}",
                            propose_retries, e
                        );
                        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                    } else {
                        return Err(format!("failed to propose POSIX root: {}", e));
                    }
                }
            }
        }

        // Wait for apply (up to 30 seconds; leader election can take 5+ seconds).
        let mut retries = 0;
        while retries < 300 {
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
        // 1. Check in-memory cache
        {
            let roots = self.root_inodes.read().unwrap();
            if let Some(&inode) = roots.get(bucket) {
                return Ok(inode);
            }
        }

        // 2. Check ShardStore — the bucket root may have been created by the
        //    shard-0 leader and replicated to us via Raft AppendEntries, even
        //    though our local root_inodes cache hasn't been populated.
        //    Use get_dir_entry_inode (not lookup) for cross-shard safety.
        let shard_id = self.shard_strategy.calculate_shard(0);
        {
            let stores = self.shard_stores.read().unwrap();
            if let Some(store) = stores.get(&shard_id) {
                if let Some(inode) = store.get_dir_entry_inode(0, bucket) {
                    // Found in store — cache and return
                    drop(stores);
                    self.register_root_inode(bucket, inode);
                    return Ok(inode);
                }
            }
        }

        // 3. Not found anywhere — need to propose CreateDirectory. The propose
        //    will be forwarded to the shard-0 leader via MsgProp if this node
        //    is not the leader (see handle_propose).
        let inode = self.generate_inode();
        let cmd = ShardCommand::CreateDirectory {
            parent_inode: 0,
            name: bucket.to_string(),
            inode,
        };
        self.propose_meta(shard_id, cmd.serialize()).await?;

        // Strict mode only: Wait for apply (increased timeout to accommodate
        // propose forwarding latency: follower → leader → commit →
        // AppendEntries → follower apply).
        //
        // In async mode (default), skip the wait — propose_meta fire-and-
        // forget returns immediately. We optimistically register the root
        // inode in memory and return it. If a subsequent operation on this
        // bucket races with apply, the reader retries (handle_lookup etc.).
        if !self.is_async_meta_persist() {
            let mut retries = 0;
            while retries < 100 {
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
            return Err("failed to create bucket root: timeout waiting for apply".to_string());
        }

        // Async mode: optimistically register and return.
        self.register_root_inode(bucket, inode);
        Ok(inode)
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
        // Phase 3: S3 objects follow the bucket root's shard (like files).
        let parent_shard = self.shard_strategy.calculate_shard(bucket_root_inode);
        let inode = self.alloc_inode_in_shard(parent_shard);
        let now = chrono::Utc::now().timestamp() as u64;
        let info = InodeInfo {
            inode,
            name: key.to_string(),
            parent_inode: bucket_root_inode,
            file_type: FileType::File,
            size,
            mtime: now,
            atime: now,
            ctime: now,
            mode: 0o100644,
            uid: 0,
            gid: 0,
            blocks: 0,
            fid: Some(fid.to_string()),
            volume_id: Some(volume_id),
            etag: Some(etag.to_string()),
            chunks: vec![],
            inline_data: None,
            extended: HashMap::new(),
            symlink_target: None,
            nlink: 1,
            version: 0,
            delete_time: 0,
            reliability: powerfs_layout::reliability::Reliability::default(),
            reliability_state: powerfs_layout::reliability::ReliabilityState::default(),
            compression_state: powerfs_layout::reliability::CompressionState::default(),
            replica_chunks: Vec::new(),
        };

        self.propose_create_inode_and_direntry(info, bucket_root_inode, key, inode)
            .await?;

        Ok(inode)
    }

    /// Look up an S3 object entry by bucket root inode and key.
    pub fn get_object_entry(&self, bucket_root_inode: u64, key: &str) -> Option<InodeInfo> {
        // Cross-shard: dir entry on calculate_shard(bucket_root_inode),
        // inode record on calculate_shard(inode).
        let parent_shard = self.shard_strategy.calculate_shard(bucket_root_inode);
        let inode = {
            let stores = self.shard_stores.read().unwrap();
            let store = stores.get(&parent_shard)?;
            store.get_dir_entry_inode(bucket_root_inode, key)
        }?;

        let inode_shard = self.shard_strategy.calculate_shard(inode);
        let stores = self.shard_stores.read().unwrap();
        let store = stores.get(&inode_shard)?;
        store.get_inode(inode)
    }

    /// Delete an S3 object entry by bucket root inode and key.
    pub async fn delete_object_entry(
        &self,
        bucket_root_inode: u64,
        key: &str,
    ) -> Result<(), String> {
        let shard_dir = self.shard_strategy.calculate_shard(bucket_root_inode);

        // Use get_dir_entry_inode (not lookup) because the inode record may
        // be on a different shard than the dir entry.
        let inode = {
            let stores = self.shard_stores.read().unwrap();
            let store = stores
                .get(&shard_dir)
                .ok_or_else(|| format!("shard {} not found", shard_dir.0))?;
            store
                .get_dir_entry_inode(bucket_root_inode, key)
                .ok_or_else(|| "object not found".to_string())?
        };

        self.propose_remove_direntry_and_inode(bucket_root_inode, key, inode)
            .await
    }

    /// List S3 object entries under a bucket root inode.
    pub fn list_object_entries(&self, bucket_root_inode: u64) -> Vec<InodeInfo> {
        // Cross-shard: dir entries on calculate_shard(bucket_root_inode),
        // inode records on their respective shards.
        self.list_directory(bucket_root_inode)
            .into_iter()
            .filter(|info| matches!(info.file_type, crate::shard_store::FileType::File))
            .collect()
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
        inline_data: Option<Vec<u8>>,
        is_append: bool,
    ) -> Result<(), String> {
        let target_chunk_count = chunks.len();

        // T1.3 fix (2026-08-22): Clone chunks/inline_data BEFORE moving into
        // ShardCommand so we can project them into MetaCache after propose.
        // Without this, async mode returns immediately after propose_meta,
        // but MetaCache still holds the create-time staged value (size=0,
        // chunks=[]). Cross-client getattr → get_inode → MetaCache hit →
        // returns stale size=0 → reader EIO after READ_LAG timeout.
        // The projection mirrors Ceph MDS projected state: memory updates
        // precede journal apply.
        let chunks_for_projection = chunks.clone();
        let inline_for_projection = inline_data.clone();

        let cmd = ShardCommand::UpdateInodeSizeChunks {
            inode,
            size,
            chunks,
            inline_data,
            is_append,
        };
        self.propose_meta(shard_id, cmd.serialize()).await?;

        // Project the update into MetaCache so subsequent get_inode calls
        // (e.g., from cross-client getattr) return the new size/chunks
        // immediately, without waiting for Raft apply.
        self.meta_cache.project_update_size_chunks(
            inode,
            size,
            chunks_for_projection,
            inline_for_projection,
            is_append,
        );

        // Strict mode only: Wait for the apply to complete on this (leader)
        // node so that notify_inode_change and subsequent reads see the
        // updated data. Without this poll, a subscriber re-fetching the
        // entry right after notify could see the pre-apply (stale) state.
        //
        // In async mode (default), skip the wait entirely — propose_meta
        // fire-and-forget returns immediately after submitting to RaftCore.
        // This is the CRITICAL performance path for small-file inline writes:
        //   FUSE write → update_inode_size_chunks → propose_ff → return
        // Skipping the ~10-50ms apply wait is what gives us 10-50x IOPS.
        //
        // Trade-off: if leader crashes before commit, the inline data is
        // lost. Acceptable for performance mode (default). The FUSE client's
        // read path retries briefly to absorb the visibility gap.
        //
        // Readers that need to see the just-written data (handle_read for
        // inline, handle_getattr for size) use retry logic in net_handler.rs.
        if !self.is_async_meta_persist() {
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
                    if is_append {
                        // Append mode: the Filer computes the new size, so we
                        // can't check info.size == size. Instead, check that
                        // inline_data is non-empty (the append succeeded).
                        if info
                            .inline_data
                            .as_ref()
                            .map(|d| !d.is_empty())
                            .unwrap_or(false)
                        {
                            return Ok(());
                        }
                    } else if info.size == size && info.chunks.len() == target_chunk_count {
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
        }
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
                    inline_data: None,
                    extended: std::collections::HashMap::new(),
                    symlink_target: None,
                    nlink,
                    version: 0,
                    delete_time: 0,
                    reliability: powerfs_layout::reliability::Reliability::default(),
                    reliability_state: powerfs_layout::reliability::ReliabilityState::default(),
                    compression_state: powerfs_layout::reliability::CompressionState::default(),
                    replica_chunks: Vec::new(),
                };
                store.create_inode_atomic(inode_info, entry_orset.parent_ino, &entry_orset.name)?;
                debug!(
                    "Applied Add delta: {}/{} inode={}",
                    entry_orset.parent_ino, entry_orset.name, entry_orset.inode
                );
            }
            Some(crate::powerfs::delta_op::Op::Remove(entry_id)) => {
                // Use get_dir_entry_inode (not lookup) because the inode
                // record may be on a different shard than the dir entry.
                match store.get_dir_entry_inode(entry_id.parent_ino, &entry_id.name) {
                    Some(inode) => {
                        // Phase 3.5: 延迟删除——仅标记 tombstone，不物理删除。
                        // 物理删除由 GC 任务在 grace_period 后执行（需满足 nlink==0、
                        // 无活跃 lease、open_count==0）。
                        // mark_tombstone only works if the inode record is on
                        // this shard; if not, the GC will eventually reclaim
                        // it via the orphan scan.
                        let _ = store.mark_tombstone(inode);
                        info!(
                            "Applied Remove delta (tombstone marked): {}/{} inode={}",
                            entry_id.parent_ino, entry_id.name, inode
                        );
                    }
                    None => {
                        warn!(
                            "Applied Remove delta: dir entry not found {}/{}",
                            entry_id.parent_ino, entry_id.name
                        );
                    }
                }
            }
            Some(crate::powerfs::delta_op::Op::Rename(rename_op)) => {
                // Use get_dir_entry_inode (not lookup) for cross-shard safety.
                if let Some(inode) =
                    store.get_dir_entry_inode(rename_op.old_parent_ino, &rename_op.old_name)
                {
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

    /// Deprecated: Raft inter-node messaging is now handled by openraft's gRPC
    /// RaftService (MultiRaftServiceImpl). TLV MsgType::RaftMessage is no longer used.
    pub async fn step_raft_message(&self, _shard_id: ShardId) -> Result<(), String> {
        Err("step_raft_message is deprecated; openraft uses gRPC RaftService".to_string())
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
        // open_count is an in-memory per-ShardStore counter keyed by inode.
        // The inode record lives on calculate_shard(inode); we route the
        // counter to the same shard so the increment is co-located with the
        // inode (and survives the in-memory scan shortcut below if other
        // shards happen to have cached lookups).
        let shard_id = self.shard_strategy.calculate_shard(inode);
        let stores = self.shard_stores.read().unwrap();
        if let Some(store) = stores.get(&shard_id) {
            return Ok(store.increment_open_count(inode));
        }
        // Fallback: if the nominal shard store is missing (shouldn't happen
        // in normal operation), scan all stores for a recorded count.
        for store in stores.values() {
            if store.get_open_count(inode) > 0 || store.get_inode(inode).is_some() {
                return Ok(store.increment_open_count(inode));
            }
        }
        if let Some(store) = stores.values().next() {
            Ok(store.increment_open_count(inode))
        } else {
            Err("no shard store available".to_string())
        }
    }

    /// Phase 3.5.3: 递减 inode 的 open 计数（fuse release/close 时上报）。
    /// 返回递减后的 open_count。
    pub fn decrement_open_count(&self, inode: u64) -> Result<u32, String> {
        // Mirror of increment_open_count: route to calculate_shard(inode)
        // first; fall back to scanning if the inode is not yet visible there.
        let shard_id = self.shard_strategy.calculate_shard(inode);
        let stores = self.shard_stores.read().unwrap();
        if let Some(store) = stores.get(&shard_id) {
            let count = store.get_open_count(inode);
            if count > 0 {
                return Ok(store.decrement_open_count(inode));
            }
        }
        for store in stores.values() {
            let count = store.get_open_count(inode);
            if count > 0 {
                return Ok(store.decrement_open_count(inode));
            }
        }
        // open_count 已为 0 或未记录，幂等返回 0
        Ok(0)
    }

    /// Scan all shards for orphan inode records and remove them.
    ///
    /// An orphan inode is an inode record (in `CF_INODES` on
    /// `calculate_shard(inode)`) whose `(parent_inode, name)` dir entry
    /// does not exist on the parent's shard (`calculate_shard(parent_inode)`).
    /// Orphans arise when a split-create's Phase B (`AddDirEntry`) fails
    /// after Phase A (`CreateInode`) succeeded, or when a split-delete's
    /// Phase B (`DeleteInode`) is skipped after Phase A (`RemoveDirEntry`)
    /// succeeded.
    ///
    /// This is a best-effort sweep with multiple safety layers:
    ///   1. **Grace age guard**: skip inodes younger than
    ///      `MIN_ORPHAN_AGE_SECS` (default 5 min) measured from `ctime`.
    ///      Phase A + Phase B for a healthy create finishes in << 1 s, so
    ///      this absorbs Phase B in-flight proposals, Raft replication lag,
    ///      and the 1-2 s window when `stores.get(parent_shard)` returns
    ///      `None` because the parent shard is owned by another filer node
    ///      (see guards #2 and #3).
    ///   2. **Parent shard unknown (multi-filer)**: if this filer does not
    ///      own the parent's shard locally we **cannot prove** the dentry
    ///      is missing — bail out and defer the decision to the filer that
    ///      actually owns the parent shard (its next GC pass will have the
    ///      authoritative CF_DIRENTRIES view). Otherwise we'd falsely
    ///      reclaim a live inode simply because the parent's shard is on a
    ///      different box.
    ///   3. **Parent shard known → dentry + child_summaries**: if either
    ///      the real dentry OR the cached DirStatSummary on the parent
    ///      shard claims this name → keep the inode. The summary is the
    ///      authoritative hint when multi-filer makes the child's inode
    ///      record unavailable locally (see §3.2 of metacache design).
    /// - directories with `nlink >= 2` (real directories have nlink=2+;
    ///   an orphaned directory would still have nlink=2 from creation,
    ///   so we can't distinguish from a live one — but live dirs always
    ///   have a dir entry, so the lookup below filters them correctly)
    /// - inodes with active leases or open_count > 0 (still in use)
    ///
    /// Returns the count of orphan inodes removed.
    pub fn collect_orphan_inodes(&self) -> usize {
        /// Inodes newer than this many seconds old are never GC'd as
        /// orphans. Absorbs Phase A → Phase B Raft latency plus multi-filer
        /// shard-migration skew. Tunable via `POWERFS_ORPHAN_MIN_AGE_SECS`.
        const DEFAULT_MIN_AGE_SECS: u64 = 5 * 60;
        fn min_age_secs() -> u64 {
            std::env::var("POWERFS_ORPHAN_MIN_AGE_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_MIN_AGE_SECS)
        }
        fn now_secs() -> u64 {
            use std::time::SystemTime;
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0)
        }
        // `ctime` and `mtime` are stored as **ms** in InodeInfo (see the
        // `atime/mtime/ctime` fields + the net protocol ms-based encoding).
        // To compare with wall-clock seconds above, convert ctime_ms to
        // seconds before comparing.
        const MS_PER_SEC: u64 = 1000;

        let stores = self.shard_stores.read().unwrap();
        let mut removed = 0usize;
        let age_threshold_ms = min_age_secs().saturating_mul(MS_PER_SEC);
        let now_s = now_secs();
        let now_ms = now_s.saturating_mul(MS_PER_SEC);

        for (shard_id, store) in stores.iter() {
            let inodes = store.list_all_inodes();
            for info in inodes {
                // Skip inodes that are clearly in use.
                if self.has_active_lease(info.inode) {
                    continue;
                }
                if store.get_open_count(info.inode) > 0 {
                    continue;
                }

                // ---- Grace age guard (#1) ----
                // Saturating subtraction: ctime==0 (old migrations) is treated
                // as "definitely older than threshold" to avoid holding
                // pre-existing garbage forever.
                let age_ms = now_ms.saturating_sub(info.ctime);
                if info.ctime > 0 && age_ms < age_threshold_ms {
                    continue;
                }

                // ---- Parent ownership guard (#2) ----
                // If this filer node doesn't own the parent shard, we have
                // NO authoritative dentry map for it. Refuse to make the
                // orphan call; the filer that actually owns the parent
                // shard will re-run collect_orphan_inodes and decide.
                let parent_shard = self.shard_strategy.calculate_shard(info.parent_inode);
                let Some(parent_store) = stores.get(&parent_shard) else {
                    continue;
                };

                // ---- Dentry + summary guard (#3) ----
                // Presence in EITHER the real dentry table OR the cross-shard
                // DirStatSummary cache is sufficient proof that some parent
                // directory still points at this inode.
                let has_dentry = parent_store
                    .get_dir_entry_inode(info.parent_inode, &info.name)
                    .is_some();
                let has_summary = parent_store
                    .get_child_summary(info.parent_inode, &info.name)
                    .map(|s| s.child_inode == info.inode)
                    .unwrap_or(false);
                if has_dentry || has_summary {
                    continue;
                }

                // Orphan: dir entry does not exist on parent's shard.
                // Remove the inode record from its own shard.
                log::info!(
                    "GC orphan: inode={} (name={:?}, parent={}) on shard {} has no dir entry \
                     on parent shard {} (age_ms={}) — removing",
                    info.inode,
                    info.name,
                    info.parent_inode,
                    shard_id.0,
                    parent_shard.0,
                    age_ms,
                );
                if let Err(e) = store.delete_inode(info.inode) {
                    log::warn!(
                        "GC orphan: failed to delete inode {} from shard {}: {}",
                        info.inode,
                        shard_id.0,
                        e
                    );
                } else {
                    removed += 1;
                }
            }
        }

        if removed > 0 {
            log::info!("GC orphan pass: removed {} orphan inodes", removed);
        }
        removed
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
        volume_client_pool: &TlvVolumeClient,
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
        volume_client_pool: &TlvVolumeClient,
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
        volume_client_pool: Arc<TlvVolumeClient>,
    ) -> tokio::task::JoinHandle<()> {
        let mgr = self.clone();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;

                // ===== Phase 2: MetaCache periodic maintenance =====
                //
                // Runs before orphan/GC scans so that (1) Staging entries
                // lost during leader-change windows are dropped fast, and
                // (2) Deleted tombstones older than deleted_timeout_ms get
                // swept out before the next orphan scan considers the
                // inode "still referenced by a dentry cache".
                let mc = mgr.meta_cache();
                mc.sweep_expired_staging();
                mc.sweep_expired_deletions(std::time::Duration::from_millis(
                    mc.trim.deleted_timeout_ms,
                ));
                // trim_pass only does work when usage > high_watermark; when
                // under watermark it's a no-op (just an atomic read).
                let trim_result = mc.trim_pass();

                // ===== Phase 3: Lease Recall =====
                //
                // If trim_pass returned recall_candidates (inodes that are
                // Clean but pinned by refcount > 0), push Invalidate
                // notifications to the lease holders so they release their
                // leases. The next trim_pass can then evict them.
                //
                // Also periodically sweep leaked refcounts (inodes where
                // refcount > 0 but no active lease exists — e.g. client
                // crashed before sending ReleaseInodeLease).
                if !trim_result.recall_candidates.is_empty() {
                    let notifier_opt = mgr.inode_notifier.read().unwrap().clone();
                    let lease_mgr_opt = mgr.lease_mgr.read().unwrap().clone();
                    if let (Some(notifier), Some(lease_mgr)) = (notifier_opt, lease_mgr_opt) {
                        for inode in &trim_result.recall_candidates {
                            // Look up the lease holder and push Invalidate.
                            // The holder string is the client's numeric id
                            // (e.g. "12345"); parse it to u64 for the
                            // notifier's connection lookup.
                            if let Some(holder) = lease_mgr.get_holder(*inode) {
                                if let Ok(client_id) = holder.parse::<u64>() {
                                    // Version 0 signals "recall" (not a
                                    // content change); the client releases
                                    // the lease and drops its cache.
                                    notifier.notify_client(client_id, *inode, 0);
                                    mc.mark_recalled(*inode);
                                    debug!(
                                        "Phase 3 recall: pushed Invalidate to \
                                         client {} for inode {} (memory pressure)",
                                        client_id, inode
                                    );
                                }
                            } else {
                                // No active lease but refcount > 0 — this
                                // is a leak; sweep_leaked_refcounts below
                                // will fix it.
                                debug!(
                                    "Phase 3 recall: inode {} has refcount > 0 \
                                     but no active lease (leak, will sweep)",
                                    inode
                                );
                            }
                        }
                    }
                }

                // Periodically sweep leaked refcounts (every GC cycle).
                // Uses get_holder as the "is lease active" predicate:
                // if get_holder returns None, the refcount should be 0.
                {
                    let lease_mgr_opt = mgr.lease_mgr.read().unwrap().clone();
                    if let Some(lease_mgr) = lease_mgr_opt {
                        mc.sweep_leaked_refcounts(|inode| lease_mgr.get_holder(inode).is_some());
                    }
                }

                // Phase 5: 先重试上次失败的 pending_reclaims（WAL 崩溃恢复）
                let retried = mgr
                    .retry_pending_reclaims(&volume_router, &volume_client_pool)
                    .await;
                // Reclaim orphan inodes left by split-create/delete failures
                // (CreateInode without AddDirEntry, or RemoveDirEntry without
                // DeleteInode).
                let orphans_removed = mgr.collect_orphan_inodes();
                // 正常 GC 扫描
                let (scanned, deleted, skipped, chunks_to_reclaim) =
                    mgr.run_gc_pass(grace_period_secs);
                if scanned > 0 || retried > 0 || orphans_removed > 0 {
                    debug!(
                        "GC task heartbeat: retried={}, orphans_removed={}, scanned={}, deleted={}, skipped={}, chunks_to_reclaim={}",
                        retried, orphans_removed, scanned, deleted, skipped, chunks_to_reclaim.len()
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
    use crate::raft_group_manager_v2::RaftGroupManagerV2;
    use crate::shard_strategy::ShardStrategy;
    use std::sync::atomic::{AtomicU16, Ordering};

    static TEST_PORT: AtomicU16 = AtomicU16::new(30000);

    fn next_test_port() -> u16 {
        TEST_PORT.fetch_add(1, Ordering::SeqCst)
    }

    async fn make_manager() -> Arc<MetaShardManager> {
        let tmp_dir =
            std::env::temp_dir().join(format!("powerfs-filer-test-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&tmp_dir);
        let data_path = tmp_dir.to_string_lossy().to_string();

        let port = next_test_port();
        let raft_addr = format!("127.0.0.1:{}", port);
        let raft_mgr = RaftGroupManagerV2::new(1, raft_addr, format!("{}/raft", data_path))
            .await
            .unwrap();
        let strategy = Arc::new(ShardStrategy::new(4));
        Arc::new(MetaShardManager::new(raft_mgr, strategy, data_path, 1))
    }

    #[tokio::test]
    async fn test_crdt_maintenance_empty_state() {
        let mgr = make_manager().await;
        let (tombstone_cleaned, orset_count, delta_trimmed) = mgr.crdt_maintenance(24, true);
        assert_eq!(tombstone_cleaned, 0, "no shard stores yet");
        assert_eq!(orset_count, 0, "no orset states yet");
        assert_eq!(delta_trimmed, 0, "no delta logs yet");
    }

    #[tokio::test]
    async fn test_crdt_maintenance_disable_delta_compaction() {
        let mgr = make_manager().await;
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

    #[tokio::test]
    async fn test_crdt_maintenance_delta_log_compaction() {
        let mgr = make_manager().await;
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

    #[tokio::test]
    async fn test_crdt_maintenance_small_log_no_compaction() {
        let mgr = make_manager().await;
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
