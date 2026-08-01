//! fuse 端 CRDT 副本一致性实现：
//! - [`ShardedDirCache`]：dir_ino → DirORSet 本地副本容器（DashMap + LRU 淘汰）
//! - [`CrdtReplicaCoherence`]：实现 [`CacheCoherence`]，协调本地副本 + delta sync + 联动失效
//! - [`InodeAllocator`]：filer 批量授权的预留段分配器（Phase 3 接入写路径）
//!
//! 核心流程：
//! - 写：本地 apply DirORSet → ChangeCache 缓冲 → change_cache_flusher push_delta（Phase 3）
//! - 读：读本地 DirORSet（零开销）；无副本时 ensure_replica 触发 pull
//! - delta sync：后台 puller 定时 pull_delta + merge；on_remote_delta 处理广播 delta

use std::sync::Arc;

use dashmap::DashMap;
use lru::LruCache;
use powerfs_orset::{DeltaOp, DirORSet};
use std::sync::Mutex;
use std::time::Duration;

use crate::{
    AllocInodeBatchRequest, CacheCoherence, CacheKind, DeltaOpType, DeltaSyncChannel, DeltaWire,
    MetadataCacheInvalidator, PullDeltaRequest, ValidationResult, WriteOp,
};

// ===========================================================================
// ShardedDirCache: dir_ino → Arc<RwLock<DirORSet>> 容器
// ===========================================================================

/// 目录 OR-Set 本地副本容器。
///
/// 使用 DashMap 提供并发读，配合 LRU 跟踪做容量淘汰。
/// 淘汰时仅移除最久未访问的目录副本（正在被读的 Arc 不会立即释放）。
pub struct ShardedDirCache {
    /// dir_ino → 本地副本
    replicas: DashMap<u64, Arc<std::sync::RwLock<DirORSet>>>,
    /// LRU 访问跟踪（dir_ino 顺序）
    lru: Mutex<LruCache<u64, ()>>,
    /// 最大缓存目录数
    max_dirs: usize,
}

impl ShardedDirCache {
    pub fn new(max_dirs: usize) -> Self {
        Self {
            replicas: DashMap::new(),
            lru: Mutex::new(LruCache::new(
                std::num::NonZeroUsize::new(max_dirs).unwrap(),
            )),
            max_dirs,
        }
    }

    /// 获取已有副本（不创建），更新 LRU
    pub fn get(&self, dir_ino: u64) -> Option<Arc<std::sync::RwLock<DirORSet>>> {
        if let Some(entry) = self.replicas.get(&dir_ino) {
            let arc = entry.clone();
            drop(entry);
            self.touch_lru(dir_ino);
            Some(arc)
        } else {
            None
        }
    }

    /// 插入新副本（若已存在则覆盖），更新 LRU 并可能淘汰
    pub fn insert(&self, dir_ino: u64, orset: Arc<std::sync::RwLock<DirORSet>>) {
        self.replicas.insert(dir_ino, orset);
        self.touch_lru_evict(dir_ino);
    }

    /// 确保副本存在：有则返回，无则用 initializer 创建并插入
    pub fn ensure_replica<F>(
        &self,
        dir_ino: u64,
        initializer: F,
    ) -> Arc<std::sync::RwLock<DirORSet>>
    where
        F: FnOnce() -> DirORSet,
    {
        // 快速路径：已存在
        if let Some(arc) = self.get(dir_ino) {
            return arc;
        }
        // 慢速路径：创建
        let orset = initializer();
        let arc = Arc::new(std::sync::RwLock::new(orset));
        self.insert(dir_ino, arc.clone());
        arc
    }

    /// 移除指定目录副本
    pub fn remove(&self, dir_ino: u64) -> Option<Arc<std::sync::RwLock<DirORSet>>> {
        let v = self.replicas.remove(&dir_ino).map(|(_, v)| v);
        if v.is_some() {
            let mut lru = self.lru.lock().unwrap();
            lru.pop(&dir_ino);
        }
        v
    }

    /// 当前缓存目录数
    pub fn len(&self) -> usize {
        self.replicas.len()
    }

    /// 是否为空
    pub fn is_empty(&self) -> bool {
        self.replicas.is_empty()
    }

    /// 列出所有缓存的 dir_ino（puller 用）
    pub fn cached_dirs(&self) -> Vec<u64> {
        self.replicas.iter().map(|e| *e.key()).collect()
    }

    fn touch_lru(&self, dir_ino: u64) {
        let mut lru = self.lru.lock().unwrap();
        lru.get(&dir_ino); // 访问即更新 LRU 顺序
    }

    fn touch_lru_evict(&self, dir_ino: u64) {
        {
            let mut lru = self.lru.lock().unwrap();
            lru.put(dir_ino, ());
        }
        // lru crate 的 put 不会返回被淘汰项，需要手动检查 replicas 数量
        // 简单策略：当 replicas 数量超过 max_dirs 时，扫描 LRU 找最老的淘汰
        if self.replicas.len() > self.max_dirs {
            self.evict_oldest();
        }
    }

    fn evict_oldest(&self) {
        let victim = {
            let lru = self.lru.lock().unwrap();
            // LruCache 没有 peek_lru 的稳定 API，用 iter 倒序取最后一个
            lru.iter().next_back().map(|(k, _)| *k)
        };
        if let Some(victim) = victim {
            // 从 LRU 移除
            {
                let mut lru = self.lru.lock().unwrap();
                lru.pop(&victim);
            }
            // 从 replicas 移除（如果没人在用，Arc 会释放）
            self.replicas.remove(&victim);
            log::debug!("ShardedDirCache evicted dir {} (LRU)", victim);
        }
    }
}

// ===========================================================================
// ChangeCache: 本地写 delta 缓冲区（change_cache_flusher 异步 push）
// ===========================================================================

/// 本地写 delta 缓冲区。
///
/// 按 dir_ino 分组缓存待 push 的 delta，change_cache_flusher 定时 drain + push。
/// 同一 dir_ino 的 delta 顺序追加（保序）；不同 dir_ino 可并发 push。
/// 全局总量达上限时返回 Err（背压：写路径应等待，不应丢弃）。
pub struct ChangeCache {
    /// dir_ino -> Vec<DeltaOp>（待 push 的 delta，按追加顺序）
    pending: DashMap<u64, Vec<DeltaOp>>,
    /// 全局 delta 总数（背压水位判断）
    total_count: std::sync::atomic::AtomicUsize,
    /// 全局上限
    max_global: usize,
    /// 高水位线（超过则减慢 apply，如 0.8）
    high_watermark: f64,
}

impl ChangeCache {
    pub fn new(max_global: usize, high_watermark: f64) -> Self {
        Self {
            pending: DashMap::new(),
            total_count: std::sync::atomic::AtomicUsize::new(0),
            max_global,
            high_watermark,
        }
    }

    /// push delta 到缓冲区（同一 dir_ino 的 delta 顺序追加）。
    ///
    /// 全局达上限时返回 Err —— 调用方应阻塞等待（背压），不应丢弃 delta。
    pub fn push(&self, dir_ino: u64, delta: DeltaOp) -> Result<(), String> {
        let current = self.total_count.load(std::sync::atomic::Ordering::Relaxed);
        if current >= self.max_global {
            return Err(format!(
                "ChangeCache full ({}/{}), apply backpressure",
                current, self.max_global
            ));
        }

        // 追加到 dir_ino 对应的 Vec（保序）
        let mut entry = self.pending.entry(dir_ino).or_default();
        entry.push(delta);
        self.total_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    /// drain 指定 dir_ino 的所有 delta（保序），返回 Vec。
    /// drain 后该 dir_ino 的缓冲区清空。
    pub fn drain(&self, dir_ino: u64) -> Vec<DeltaOp> {
        if let Some(mut entry) = self.pending.get_mut(&dir_ino) {
            let deltas = std::mem::take(&mut *entry);
            self.total_count
                .fetch_sub(deltas.len(), std::sync::atomic::Ordering::Relaxed);
            deltas
        } else {
            Vec::new()
        }
    }

    /// drain 所有 dir_ino 的 delta，返回 (dir_ino, Vec<DeltaOp>) 列表。
    /// flusher 定时调用此方法批量 push。
    pub fn drain_all(&self) -> Vec<(u64, Vec<DeltaOp>)> {
        let keys: Vec<u64> = self.pending.iter().map(|e| *e.key()).collect();
        let mut result = Vec::with_capacity(keys.len());
        for dir_ino in keys {
            let deltas = self.drain(dir_ino);
            if !deltas.is_empty() {
                result.push((dir_ino, deltas));
            }
        }
        result
    }

    /// 当前全局 delta 总数
    pub fn total_count(&self) -> usize {
        self.total_count.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// 是否超过高水位线（写路径可据此减慢 apply 速率）
    pub fn is_high_water(&self) -> bool {
        let current = self.total_count() as f64;
        current >= self.max_global as f64 * self.high_watermark
    }

    /// 是否已达上限（写路径应阻塞等待）
    pub fn is_full(&self) -> bool {
        self.total_count() >= self.max_global
    }
}

// ===========================================================================
// CrdtReplicaCoherence: fuse 端 CacheCoherence 实现
// ===========================================================================

/// CRDT 副本一致性配置
#[derive(Clone, Debug)]
pub struct CrdtConfig {
    /// 后台 puller 基础间隔
    pub pull_interval: Duration,
    /// 后台 puller 最大间隔（退避上限）
    pub pull_max_interval: Duration,
    /// 连续空响应多少次后开始退避
    pub pull_backoff_threshold: u32,
    /// 最大缓存目录数
    pub max_cached_dirs: usize,
    /// inode 预留段大小
    pub inode_batch_size: u32,
    /// change_cache_flusher 间隔（定时 push 本地 delta）
    pub sync_interval: Duration,
    /// 批量 flush 阈值（单个 dir_ino delta 数达此值立即 flush）
    pub sync_batch: usize,
    /// ChangeCache 全局上限（背压）
    pub change_cache_max_global: usize,
    /// ChangeCache 高水位线（0.0-1.0，超过则减慢 apply）
    pub change_cache_high_watermark: f64,
}

impl Default for CrdtConfig {
    fn default() -> Self {
        Self {
            pull_interval: Duration::from_millis(1000),
            pull_max_interval: Duration::from_secs(30),
            pull_backoff_threshold: 3,
            max_cached_dirs: 1024,
            inode_batch_size: 1024,
            sync_interval: Duration::from_millis(100),
            sync_batch: 64,
            change_cache_max_global: 100_000,
            change_cache_high_watermark: 0.8,
        }
    }
}

/// fuse 端 CRDT 副本一致性实现。
///
/// 持有：
/// - ShardedDirCache（目录条目本地副本）
/// - ChangeCache（本地写 delta 缓冲，change_cache_flusher 异步 push）
/// - DeltaSyncChannel（push/pull delta 到 filer）
/// - MetadataCacheInvalidator（联动失效 inode attr 缓存）
/// - InodeAllocator（inode 预留段，Phase 3 接入）
pub struct CrdtReplicaCoherence {
    dir_cache: ShardedDirCache,
    change_cache: ChangeCache,
    channel: Arc<dyn DeltaSyncChannel>,
    invalidator: Arc<dyn MetadataCacheInvalidator>,
    inode_allocator: Mutex<InodeAllocator>,
    config: CrdtConfig,
    /// fuse 客户端 ID（u64，用于 EntryId / VectorClock / push_delta 的 client_id 字段）
    client_id: u64,
}

impl CrdtReplicaCoherence {
    pub fn new(
        channel: Arc<dyn DeltaSyncChannel>,
        invalidator: Arc<dyn MetadataCacheInvalidator>,
        client_id: u64,
        config: CrdtConfig,
    ) -> Self {
        let max_dirs = config.max_cached_dirs;
        let batch_size = config.inode_batch_size;
        let change_cache = ChangeCache::new(
            config.change_cache_max_global,
            config.change_cache_high_watermark,
        );
        Self {
            dir_cache: ShardedDirCache::new(max_dirs),
            change_cache,
            channel,
            invalidator,
            inode_allocator: Mutex::new(InodeAllocator::new(batch_size)),
            config,
            client_id,
        }
    }

    /// 返回客户端 u64 ID（供 fuse 端构造 EntryId 等使用）
    pub fn client_id(&self) -> u64 {
        self.client_id
    }

    /// 获取目录副本（读路径用）：本地有则返回，无则触发 pull
    pub async fn ensure_replica(&self, dir_ino: u64) -> Arc<std::sync::RwLock<DirORSet>> {
        // 快速路径
        if let Some(arc) = self.dir_cache.get(dir_ino) {
            return arc;
        }
        // 慢速路径：pull 并创建副本
        self.do_pull_and_apply_deltas(dir_ino).await;
        self.dir_cache
            .ensure_replica(dir_ino, || DirORSet::new(dir_ino))
    }

    /// 查询目录条目（lookup 路径用）
    pub fn lookup(&self, dir_ino: u64, name: &str) -> Option<u64> {
        let arc = self.dir_cache.get(dir_ino)?;
        let orset = arc.read().unwrap();
        orset
            .entries
            .values()
            .find(|e| e.id.name == name)
            .map(|e| e.inode)
    }

    /// Lookup with file type — returns (inode, is_dir) from the local DirORSet.
    /// Used by lookup_attr_from_filer to correctly set is_dir when the filer
    /// doesn't yet have the entry (delta not synced).
    pub fn lookup_with_type(&self, dir_ino: u64, name: &str) -> Option<(u64, bool)> {
        let arc = self.dir_cache.get(dir_ino)?;
        let orset = arc.read().unwrap();
        orset
            .entries
            .values()
            .find(|e| e.id.name == name)
            .map(|e| (e.inode, e.file_type.is_dir()))
    }

    /// 列出目录所有条目（readdir 路径用）。
    ///
    /// DirORSet 是 OR-Set，同一 name 可能有多个 EntryId（不同 client_id/seq），
    /// 例如跨客户端重复 Add 或删除后重建。文件系统语义要求每个 name 只出现一次，
    /// 因此按 name 去重，保留第一个匹配条目。
    pub fn list_entries(&self, dir_ino: u64) -> Vec<(u64, String, bool)> {
        let arc = match self.dir_cache.get(dir_ino) {
            Some(a) => a,
            None => return vec![],
        };
        let orset = arc.read().unwrap();
        let mut seen: std::collections::HashSet<String> =
            std::collections::HashSet::with_capacity(orset.entries.len());
        orset
            .entries
            .values()
            .filter(|e| seen.insert(e.id.name.clone()))
            .map(|e| (e.inode, e.id.name.clone(), e.file_type.is_dir()))
            .collect()
    }

    /// 本地 apply 写操作 + 缓冲 delta 到 ChangeCache（写路径用）。
    ///
    /// 流程：
    /// 1. apply delta 到本地 DirORSet（立即可见，vclock 递增，delta_log 追加）
    /// 2. 从 delta_log 弹出刚生成的 delta（含正确 vclock）
    /// 3. push 到 ChangeCache（change_cache_flusher 异步 push 到 filer）
    ///
    /// ChangeCache 满时返回 Err —— 调用方应阻塞等待（背压）。
    /// delta 已 apply 到本地副本，仅 sync 被延迟。
    pub fn apply_local_write(&self, dir_ino: u64, delta: DeltaOp) -> Result<(), String> {
        let arc = self
            .dir_cache
            .ensure_replica(dir_ino, || DirORSet::new(dir_ino));
        let delta_to_push = {
            let mut orset = arc.write().unwrap();
            let len_before = orset.delta_log.len();
            apply_local_delta(&mut orset, &delta);
            // orset.add/remove/rename 会向 delta_log 追加一个 delta（含正确 vclock）
            if orset.delta_log.len() > len_before {
                orset.delta_log.pop()
            } else {
                None // SetAttr 不影响目录条目集合，不产生 delta
            }
        };
        if let Some(d) = delta_to_push {
            self.change_cache.push(dir_ino, d).map_err(|e| {
                log::warn!("ChangeCache backpressure for dir {}: {}", dir_ino, e);
                e
            })?;
        }
        Ok(())
    }

    /// 本地创建条目（Phase 3.3 写路径用）。
    ///
    /// 自动构造 EntryId（基于 vclock 预测 seq）→ orset.add → 缓冲 delta。
    /// 返回构造好的 DirEntry 供调用方填入 MetadataCache。
    #[allow(clippy::too_many_arguments)]
    pub fn local_create_entry(
        &self,
        dir_ino: u64,
        name: &str,
        inode: u64,
        is_dir: bool,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<powerfs_orset::DirEntry, String> {
        let arc = self
            .dir_cache
            .ensure_replica(dir_ino, || DirORSet::new(dir_ino));
        let (entry, delta) = {
            let mut orset = arc.write().unwrap();
            // 预测 vclock 递增后的值作为 EntryId.seq
            let seq = orset.vclock.get(self.client_id) + 1;
            let id = powerfs_orset::EntryId::new(name, self.client_id, seq);
            let entry = if is_dir {
                powerfs_orset::DirEntry::new_dir(id, inode, dir_ino, mode, uid, gid)
            } else {
                powerfs_orset::DirEntry::new_file(id, inode, dir_ino, mode, uid, gid)
            };
            orset.add(entry.clone()); // 递增 vclock + 追加 delta 到 delta_log
            let delta = orset
                .delta_log
                .pop()
                .ok_or_else(|| "delta_log empty after add".to_string())?;
            (entry, delta)
        };
        self.change_cache.push(dir_ino, delta).map_err(|e| {
            log::warn!("ChangeCache backpressure for dir {}: {}", dir_ino, e);
            e
        })?;
        self.invalidator.invalidate_dir(dir_ino);
        Ok(entry)
    }

    /// 本地 setattr（Phase 3.3 写路径用）。
    ///
    /// 对目录条目属性变更（mode/uid/gid/mtime）走 CRDT 弱一致异步同步。
    /// size 变更不在此处理，由 close 时 `sync_size_chunks_on_close` 强一致同步。
    /// 即使本地 OR-Set 未缓存该条目，delta 仍会生成并推送到 filer。
    pub fn local_setattr_entry(
        &self,
        dir_ino: u64,
        inode: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        mtime: Option<u64>,
    ) -> Result<(), String> {
        let arc = self
            .dir_cache
            .ensure_replica(dir_ino, || DirORSet::new(dir_ino));
        let delta = {
            let mut orset = arc.write().unwrap();
            orset.update_attr(inode, mode, uid, gid, None, mtime, None, self.client_id);
            orset.delta_log.pop()
        };
        if let Some(d) = delta {
            self.change_cache.push(dir_ino, d).map_err(|e| {
                log::warn!("ChangeCache backpressure for dir {}: {}", dir_ino, e);
                e
            })?;
        }
        Ok(())
    }

    /// 本地删除条目（Phase 3.3 写路径用）。
    ///
    /// 按 name 查找本地副本中所有同名 EntryId → 逐个 orset.remove → 缓冲 delta。
    /// 文件系统语义要求删除一个名字时清除所有同名条目（OR-Set 可能有多个同名
    /// EntryId，例如跨客户端重复 Add 或删除后重建）。仅删除一个会导致
    /// list_entries 仍返回该名字，rmdir 报 ENOTEMPTY、cp 报 EEXIST。
    /// 返回被删除条目的 inode（同名条目共享同一 inode）。
    pub fn local_remove_entry(&self, dir_ino: u64, name: &str) -> Result<u64, String> {
        let arc = self
            .dir_cache
            .get(dir_ino)
            .ok_or_else(|| format!("no local replica for dir {}", dir_ino))?;
        let (inode, deltas) = {
            let orset_read = arc.read().unwrap();
            let entries = orset_read.get_by_name(name);
            if entries.is_empty() {
                return Err(format!("entry '{}' not found in dir {}", name, dir_ino));
            }
            let ids: Vec<_> = entries.iter().map(|e| e.id.clone()).collect();
            let inode = entries[0].inode;
            drop(orset_read);

            let mut orset = arc.write().unwrap();
            let mut deltas = Vec::with_capacity(ids.len());
            for id in &ids {
                orset.remove(id); // 递增 vclock + 追加 delta 到 delta_log
                if let Some(d) = orset.delta_log.pop() {
                    deltas.push(d);
                }
            }
            (inode, deltas)
        };
        for delta in deltas {
            self.change_cache.push(dir_ino, delta).map_err(|e| {
                log::warn!("ChangeCache backpressure for dir {}: {}", dir_ino, e);
                e
            })?;
        }
        log::info!(
            "local_remove_entry: dir {} name '{}' inode {} delta buffered (change_cache total={})",
            dir_ino,
            name,
            inode,
            self.change_cache.total_count()
        );
        self.invalidator.invalidate_inode(inode);
        self.invalidator.invalidate_dir(dir_ino);
        Ok(inode)
    }

    /// 本地重命名条目（同目录，Phase 3.3 写路径用）。
    ///
    /// 按 old_name 查找 → orset.rename_entry → 缓冲 delta。
    /// 返回被重命名条目的 inode。
    /// 跨目录重命名由调用方分拆为 local_remove_entry + local_create_entry。
    pub fn local_rename_entry(
        &self,
        dir_ino: u64,
        old_name: &str,
        new_name: &str,
    ) -> Result<u64, String> {
        let arc = self
            .dir_cache
            .get(dir_ino)
            .ok_or_else(|| format!("no local replica for dir {}", dir_ino))?;
        let (inode, delta) = {
            let orset_read = arc.read().unwrap();
            let entry = orset_read
                .get_by_name(old_name)
                .into_iter()
                .next()
                .ok_or_else(|| format!("entry '{}' not found in dir {}", old_name, dir_ino))?;
            let old_id = entry.id.clone();
            let inode = entry.inode;
            drop(orset_read);

            let mut orset = arc.write().unwrap();
            orset.rename_entry(&old_id, new_name, self.client_id);
            let delta = orset
                .delta_log
                .pop()
                .ok_or_else(|| "delta_log empty after rename".to_string())?;
            (inode, delta)
        };
        self.change_cache.push(dir_ino, delta).map_err(|e| {
            log::warn!("ChangeCache backpressure for dir {}: {}", dir_ino, e);
            e
        })?;
        self.invalidator.invalidate_inode(inode);
        self.invalidator.invalidate_dir(dir_ino);
        Ok(inode)
    }

    /// force_sync：同步 push 指定 dir_ino 的所有待 sync delta + 等 filer 确认。
    ///
    /// 用于 close 等需要确保 delta 已 sync 的场景。
    /// 同步等 filer 返回 server_vclock，失败返回 Err。
    pub async fn force_sync(&self, dir_ino: u64) -> Result<(), String> {
        let deltas = self.change_cache.drain(dir_ino);
        if deltas.is_empty() {
            return Ok(());
        }
        self.push_deltas_to_filer(dir_ino, deltas).await
    }

    /// Phase 3.4: 同步将 size/chunks 推送到 filer（Raft 强一致）。
    ///
    /// 用于 close 时确保 filer 的 inode 账本（content_size + chunks）已持久化。
    /// 调用 DeltaSyncChannel::update_inode_size_chunks 并等待响应。
    pub async fn sync_size_chunks(
        &self,
        req: &crate::UpdateInodeSizeChunksRequest,
    ) -> Result<crate::UpdateInodeSizeChunksResponse, String> {
        self.channel.update_inode_size_chunks(req).await
    }

    /// 内部：将一批 delta push 到 filer（同步等待响应）
    async fn push_deltas_to_filer(&self, dir_ino: u64, deltas: Vec<DeltaOp>) -> Result<(), String> {
        let delta_count = deltas.len();
        // 构建 wire delta，修复 Remove 的 parent_ino（EntryId 不含 parent_ino，需从 dir_ino 传入）
        let wire_deltas: Vec<_> = deltas
            .iter()
            .map(|d| {
                let mut wire = DeltaWire::from(d);
                if wire.op_type == DeltaOpType::Remove {
                    if let Some(ref mut id) = wire.entry_id {
                        id.parent_ino = dir_ino;
                    }
                }
                wire
            })
            .collect();

        // 附带本地 vclock，让 filer 返回 server_vclock
        let client_vclock = self.dir_cache.get(dir_ino).map(|arc| {
            let orset = arc.read().unwrap();
            crate::VectorClockWire::from(&orset.vclock)
        });

        let req = crate::PushDeltaRequest {
            shard_id: dir_ino,
            client_id: self.client_id.to_string(),
            deltas: wire_deltas,
            client_vclock,
        };

        log::info!(
            "push_deltas_to_filer: dir {} pushing {} deltas (client_id={})",
            dir_ino,
            delta_count,
            self.client_id
        );

        let resp = match self.channel.push_delta(&req).await {
            Ok(r) => r,
            Err(e) => {
                log::warn!("push_deltas_to_filer: dir {} channel error: {}", dir_ino, e);
                return Err(e);
            }
        };
        if !resp.success {
            log::warn!(
                "push_deltas_to_filer: dir {} filer rejected: {}",
                dir_ino,
                resp.error
            );
            return Err(format!("push_delta failed: {}", resp.error));
        }
        log::info!(
            "push_deltas_to_filer: dir {} synced {} deltas, server_vclock entries={}",
            dir_ino,
            delta_count,
            resp.server_vclock.entries.len()
        );
        Ok(())
    }

    /// 启动 change_cache_flusher 后台 task（定时 drain + push_delta）。
    ///
    /// 同一 dir_ino 的 delta 串行 push（保序）；不同 dir_ino 并发 push。
    /// 返回 JoinHandle 供调用方管理生命周期。
    pub fn start_flusher(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let this = self.clone();
        let interval = this.config.sync_interval;
        tokio::spawn(async move {
            log::info!("change_cache_flusher: started (interval={:?})", interval);
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut tick_count: u64 = 0;
            loop {
                ticker.tick().await;
                tick_count += 1;
                let batches = this.change_cache.drain_all();
                if batches.is_empty() {
                    // 每 50 tick（约 5 秒）输出一次心跳日志，确认 flusher 在运行
                    if tick_count.is_multiple_of(50) {
                        log::debug!(
                            "change_cache_flusher: tick {} (idle, no pending deltas)",
                            tick_count
                        );
                    }
                    continue;
                }
                log::info!(
                    "change_cache_flusher: tick {} draining {} dirs, {} total deltas",
                    tick_count,
                    batches.len(),
                    batches.iter().map(|(_, d)| d.len()).sum::<usize>()
                );

                // 不同 dir_ino 并发 push，同一 dir_ino 的 delta 在一个 batch 内保序
                let mut tasks = Vec::with_capacity(batches.len());
                for (dir_ino, deltas) in batches {
                    let this = this.clone();
                    tasks.push(tokio::spawn(async move {
                        if let Err(e) = this.push_deltas_to_filer(dir_ino, deltas).await {
                            log::warn!(
                                "change_cache_flusher: push_delta failed for dir {}: {}",
                                dir_ino,
                                e
                            );
                            // 弱一致语义：push 失败不阻塞，下轮 flusher 不重试（delta 已 drain）
                            // 如需可靠 sync，写路径应用 force_sync
                        }
                    }));
                }
                // 等待本轮所有 push 完成（不阻塞下一轮 ticker）
                for task in tasks {
                    let _ = task.await;
                }
            }
        })
    }

    /// pull + merge delta 到本地副本 + 联动失效
    ///
    /// 返回 `true` 表示拉取到并应用了新 delta；`false` 表示无新 delta 或出错。
    /// Phase 1.8: start_puller 用此返回值驱动自适应退避。
    pub async fn do_pull_and_apply_deltas(&self, dir_ino: u64) -> bool {
        let vclock = self
            .dir_cache
            .get(dir_ino)
            .map(|arc| {
                let orset = arc.read().unwrap();
                orset.vclock.clone()
            })
            .unwrap_or_default();

        let wire_vclock = crate::VectorClockWire::from(&vclock);
        let req = PullDeltaRequest {
            shard_id: dir_ino, // Phase 2: dir_ino 作为 shard_id（与 fuse 现状一致）
            client_id: self.client_id.to_string(),
            client_vclock: Some(wire_vclock),
        };

        let resp = match self.channel.pull_delta(&req).await {
            Ok(r) => r,
            Err(e) => {
                log::warn!("pull_delta failed for dir {}: {}", dir_ino, e);
                return false;
            }
        };

        if resp.deltas.is_empty() {
            return false;
        }

        // merge deltas 到本地副本
        let arc = self
            .dir_cache
            .ensure_replica(dir_ino, || DirORSet::new(dir_ino));
        let mut changed_inodes = Vec::new();
        {
            let mut orset = arc.write().unwrap();
            for wire_delta in &resp.deltas {
                let delta = match powerfs_orset::DeltaOp::try_from(wire_delta) {
                    Ok(d) => d,
                    Err(e) => {
                        log::warn!("delta wire conversion failed for dir {}: {}", dir_ino, e);
                        continue;
                    }
                };
                if let Some(ino) = apply_remote_delta(&mut orset, &delta) {
                    changed_inodes.push(ino);
                }
            }
        }

        // 联动失效 MetadataCache（size/chunks 不失效，仅 attr/dir listing）
        for ino in changed_inodes {
            self.invalidator.invalidate_inode(ino);
        }
        self.invalidator.invalidate_dir(dir_ino);
        true
    }

    /// 启动后台 puller task（定时 pull 所有已缓存目录的 delta）
    ///
    /// Phase 1.8: 自适应退避。连续 `pull_backoff_threshold` 次空响应后，
    /// 间隔翻倍（上限 `pull_max_interval`）。拉取到新 delta 时立即重置为基础间隔。
    /// 这样在空闲时减少不必要的 PullDelta 请求，在有写入活动时保持低延迟同步。
    pub fn start_puller(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let this = self.clone();
        let base_interval = this.config.pull_interval;
        let max_interval = this.config.pull_max_interval;
        let backoff_threshold = this.config.pull_backoff_threshold;
        tokio::spawn(async move {
            let mut consecutive_empty: u32 = 0;
            let mut current_interval = base_interval;
            loop {
                tokio::time::sleep(current_interval).await;
                let dirs = this.dir_cache.cached_dirs();
                let mut any_delta = false;
                for dir_ino in dirs {
                    if this.do_pull_and_apply_deltas(dir_ino).await {
                        any_delta = true;
                    }
                }
                if any_delta {
                    if consecutive_empty > backoff_threshold {
                        log::debug!(
                            "puller: deltas found, resetting interval {:?} → {:?}",
                            current_interval,
                            base_interval
                        );
                    }
                    consecutive_empty = 0;
                    current_interval = base_interval;
                } else {
                    consecutive_empty = consecutive_empty.saturating_add(1);
                    if consecutive_empty > backoff_threshold {
                        let new_interval = (current_interval * 2).min(max_interval);
                        if new_interval != current_interval {
                            log::debug!(
                                "puller backoff: consecutive_empty={}, {:?} → {:?}",
                                consecutive_empty,
                                current_interval,
                                new_interval
                            );
                            current_interval = new_interval;
                        }
                    }
                }
            }
        })
    }

    /// 分配 inode（Phase 3 写路径用，预留段耗尽时调 filer alloc_inode_batch）
    /// 跳过保留 inode：0（无效）和 1（POSIX root）
    pub async fn alloc_inode(&self) -> Result<u64, String> {
        loop {
            {
                let mut alloc = self.inode_allocator.lock().unwrap();
                while let Some(ino) = alloc.alloc() {
                    if ino >= 2 {
                        return Ok(ino);
                    }
                    // 跳过保留 inode 0 和 1
                }
            }
            // 预留段耗尽，申请新批次
            self.refill_inode_batch().await?;
        }
    }

    async fn refill_inode_batch(&self) -> Result<(), String> {
        let count = self.config.inode_batch_size;
        let req = AllocInodeBatchRequest {
            shard_id: 0, // Phase 3 接入时按 shard 精确申请
            count,
            client_id: self.client_id.to_string(),
        };
        let resp = self.channel.alloc_inode_batch(&req).await?;
        if !resp.success {
            return Err(resp.error);
        }
        let mut alloc = self.inode_allocator.lock().unwrap();
        alloc.refill(resp.start_inode, resp.end_inode);
        log::info!(
            "inode batch refilled: [{}, {}) count={}",
            resp.start_inode,
            resp.end_inode,
            count
        );
        Ok(())
    }

    /// 获取 dir_cache 引用（测试/调试用）
    pub fn dir_cache(&self) -> &ShardedDirCache {
        &self.dir_cache
    }
}

#[async_trait::async_trait]
impl CacheCoherence for CrdtReplicaCoherence {
    fn on_local_write(&self, parent_ino: u64, op: &WriteOp) {
        // Phase 3 接入写路径时实现 ChangeCache 缓冲
        // Phase 2 仅记录：本地 apply 到 DirORSet
        let _ = (parent_ino, op);
    }

    fn validate_cache(&self, kind: CacheKind) -> ValidationResult {
        // CRDT 副本恒有效
        let _ = kind;
        ValidationResult::Valid
    }

    async fn on_remote_delta(&self, parent_ino: u64, delta: DeltaWire) {
        // 广播 delta 到达：merge 到本地副本 + 联动失效
        let arc = self
            .dir_cache
            .ensure_replica(parent_ino, || DirORSet::new(parent_ino));
        let mut changed_inode = None;
        {
            let mut orset = arc.write().unwrap();
            if let Ok(d) = powerfs_orset::DeltaOp::try_from(&delta) {
                changed_inode = apply_remote_delta(&mut orset, &d);
            }
        }
        if let Some(ino) = changed_inode {
            self.invalidator.invalidate_inode(ino);
        }
        self.invalidator.invalidate_dir(parent_ino);
    }

    fn record_version(&self, kind: CacheKind, version: u64) {
        let _ = (kind, version);
    }
}

// ===========================================================================
// InodeAllocator: filer 批量授权预留段
// ===========================================================================

/// inode 预留段分配器（Phase 3 写路径用）。
///
/// 维护 [cursor, end) 区间，alloc 返回 cursor++。
/// 耗尽时由 CrdtReplicaCoherence::refill_inode_batch 向 filer 申请新段。
pub struct InodeAllocator {
    cursor: u64,
    end: u64,
    batch_size: u32,
}

impl InodeAllocator {
    pub fn new(batch_size: u32) -> Self {
        Self {
            cursor: 0,
            end: 0,
            batch_size,
        }
    }

    /// 从预留段分配一个 inode；段耗尽返回 None
    pub fn alloc(&mut self) -> Option<u64> {
        if self.cursor < self.end {
            let ino = self.cursor;
            self.cursor += 1;
            Some(ino)
        } else {
            None
        }
    }

    /// 填充新预留段
    pub fn refill(&mut self, start: u64, end: u64) {
        self.cursor = start;
        self.end = end;
    }

    /// 预留段剩余数量
    pub fn remaining(&self) -> u64 {
        self.end.saturating_sub(self.cursor)
    }

    /// batch_size
    pub fn batch_size(&self) -> u32 {
        self.batch_size
    }
}

// ===========================================================================
// delta apply 工具函数
// ===========================================================================

/// 本地 apply delta 到 DirORSet（产生新 delta_log 条目 + vclock increment）。
/// 用于本地写操作（Phase 3 写路径）。
fn apply_local_delta(orset: &mut DirORSet, delta: &DeltaOp) {
    match delta {
        DeltaOp::Add { entry, .. } => {
            orset.add(entry.clone());
        }
        DeltaOp::Remove { id, .. } => {
            orset.remove(id);
        }
        DeltaOp::Rename {
            old_id, new_entry, ..
        } => {
            orset.rename_entry(old_id, &new_entry.id.name, new_entry.id.client_id);
        }
        DeltaOp::SetAttr { .. } => {
            // SetAttr 不影响目录条目集合，跳过（attr 走 MetadataCache）
        }
    }
}

/// merge 远端 delta 到本地 DirORSet（不产生新 delta_log，仅更新 entries/tombstones/vclock）。
/// 返回受影响的 inode（用于联动失效 MetadataCache）。
fn apply_remote_delta(orset: &mut DirORSet, delta: &DeltaOp) -> Option<u64> {
    match delta {
        DeltaOp::Add { entry, vclock } => {
            if !orset.tombstones.contains(&entry.id) {
                // 幂等：如果 entry 已存在，仅 merge vclock，不返回 changed inode。
                // 避免重复 apply 相同 delta 时反复失效 MetadataCache。
                if orset.entries.contains_key(&entry.id) {
                    orset.vclock.merge(vclock);
                    None
                } else {
                    let ino = entry.inode;
                    orset.entries.insert(entry.id.clone(), entry.clone());
                    orset.vclock.merge(vclock);
                    Some(ino)
                }
            } else {
                orset.vclock.merge(vclock);
                None
            }
        }
        DeltaOp::Remove { id, vclock } => {
            // 文件系统语义：删除一个名字时清除所有同名 EntryId（OR-Set 可能有
            // 多个同名 EntryId）。仅删除一个会导致 list_entries 仍返回该名字，
            // rmdir 报 ENOTEMPTY、cp 报 EEXIST。
            // 先尝试精确匹配删除，再按 name 删除所有剩余同名条目。
            let mut removed_inodes: Vec<u64> = Vec::new();
            if let Some(e) = orset.entries.remove(id) {
                removed_inodes.push(e.inode);
            }
            // 按 name 删除所有剩余同名条目
            let keys_to_remove: Vec<_> = orset
                .entries
                .keys()
                .filter(|k| k.name == id.name)
                .cloned()
                .collect();
            for k in keys_to_remove {
                orset.tombstones.insert(k.clone());
                if let Some(e) = orset.entries.remove(&k) {
                    removed_inodes.push(e.inode);
                }
            }
            orset.tombstones.insert(id.clone());
            orset.vclock.merge(vclock);
            // 返回第一个被删除的 inode（用于联动失效 MetadataCache）
            removed_inodes.into_iter().next()
        }
        DeltaOp::Rename {
            old_id,
            new_entry,
            vclock,
        } => {
            let old_ino = orset.entries.remove(old_id).map(|e| e.inode);
            orset.tombstones.insert(old_id.clone());
            if !orset.tombstones.contains(&new_entry.id) {
                orset
                    .entries
                    .insert(new_entry.id.clone(), new_entry.clone());
            }
            orset.vclock.merge(vclock);
            Some(new_entry.inode).or(old_ino)
        }
        DeltaOp::SetAttr { inode, vclock, .. } => {
            // SetAttr 不影响目录条目集合，仅 merge vclock
            orset.vclock.merge(vclock);
            Some(*inode)
        }
    }
}

// ===========================================================================
// 测试
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock::{mock_channel, mock_invalidator};
    use powerfs_orset::{DirEntry, EntryId, VectorClock};

    fn make_coherence() -> Arc<CrdtReplicaCoherence> {
        Arc::new(CrdtReplicaCoherence::new(
            mock_channel(),
            mock_invalidator(),
            1u64,
            CrdtConfig::default(),
        ))
    }

    #[tokio::test]
    async fn test_ensure_replica_creates_empty_orset() {
        let coh = make_coherence();
        let arc = coh.ensure_replica(1).await;
        let orset = arc.read().unwrap();
        assert!(orset.entries.is_empty());
    }

    #[tokio::test]
    async fn test_apply_local_write_add() {
        let coh = make_coherence();
        let entry = DirEntry::new_file(EntryId::new("test.txt", 1, 1), 100, 1, 0o644, 0, 0);
        let mut vc = VectorClock::new();
        vc.increment(1);
        let delta = DeltaOp::Add {
            entry: entry.clone(),
            vclock: vc,
        };
        coh.apply_local_write(1, delta).unwrap();
        assert_eq!(coh.lookup(1, "test.txt"), Some(100));
        assert_eq!(coh.list_entries(1).len(), 1);
    }

    #[tokio::test]
    async fn test_apply_local_write_remove() {
        let coh = make_coherence();
        let entry = DirEntry::new_file(EntryId::new("test.txt", 1, 1), 100, 1, 0o644, 0, 0);
        let mut vc = VectorClock::new();
        vc.increment(1);
        coh.apply_local_write(
            1,
            DeltaOp::Add {
                entry: entry.clone(),
                vclock: vc.clone(),
            },
        )
        .unwrap();

        // remove
        coh.apply_local_write(
            1,
            DeltaOp::Remove {
                id: entry.id.clone(),
                vclock: vc,
            },
        )
        .unwrap();
        assert_eq!(coh.lookup(1, "test.txt"), None);
    }

    #[test]
    fn test_change_cache_push_and_drain() {
        let cache = ChangeCache::new(100, 0.8);
        let entry = DirEntry::new_file(EntryId::new("a.txt", 1, 1), 10, 1, 0o644, 0, 0);
        let mut vc = VectorClock::new();
        vc.increment(1);

        // push 3 deltas to dir 1
        cache
            .push(
                1,
                DeltaOp::Add {
                    entry: entry.clone(),
                    vclock: vc.clone(),
                },
            )
            .unwrap();
        cache
            .push(
                1,
                DeltaOp::Remove {
                    id: entry.id.clone(),
                    vclock: vc.clone(),
                },
            )
            .unwrap();
        cache
            .push(
                2,
                DeltaOp::Add {
                    entry: entry.clone(),
                    vclock: vc,
                },
            )
            .unwrap();

        assert_eq!(cache.total_count(), 3);
        assert!(!cache.is_full());

        // drain dir 1 (2 deltas, 保序)
        let d1 = cache.drain(1);
        assert_eq!(d1.len(), 2);
        assert!(matches!(d1[0], DeltaOp::Add { .. }));
        assert!(matches!(d1[1], DeltaOp::Remove { .. }));

        // drain dir 2 (1 delta)
        let d2 = cache.drain(2);
        assert_eq!(d2.len(), 1);

        assert_eq!(cache.total_count(), 0);
    }

    #[test]
    fn test_change_cache_backpressure() {
        let cache = ChangeCache::new(2, 0.8);
        let entry = DirEntry::new_file(EntryId::new("a.txt", 1, 1), 10, 1, 0o644, 0, 0);
        let mut vc = VectorClock::new();
        vc.increment(1);

        assert!(cache
            .push(
                1,
                DeltaOp::Add {
                    entry: entry.clone(),
                    vclock: vc.clone()
                }
            )
            .is_ok());
        assert!(cache
            .push(
                1,
                DeltaOp::Add {
                    entry: entry.clone(),
                    vclock: vc.clone()
                }
            )
            .is_ok());
        // 第 3 个应被拒（背压）
        assert!(cache.push(1, DeltaOp::Add { entry, vclock: vc }).is_err());
        assert!(cache.is_full());
    }

    #[test]
    fn test_change_cache_drain_all() {
        let cache = ChangeCache::new(100, 0.8);
        let entry = DirEntry::new_file(EntryId::new("a.txt", 1, 1), 10, 1, 0o644, 0, 0);
        let mut vc = VectorClock::new();
        vc.increment(1);

        cache
            .push(
                1,
                DeltaOp::Add {
                    entry: entry.clone(),
                    vclock: vc.clone(),
                },
            )
            .unwrap();
        cache
            .push(
                2,
                DeltaOp::Add {
                    entry: entry.clone(),
                    vclock: vc.clone(),
                },
            )
            .unwrap();
        cache.push(3, DeltaOp::Add { entry, vclock: vc }).unwrap();

        let all = cache.drain_all();
        assert_eq!(all.len(), 3);
        assert_eq!(cache.total_count(), 0);
    }

    #[tokio::test]
    async fn test_force_sync_empty() {
        let coh = make_coherence();
        // 无待 sync delta，force_sync 立即返回 Ok
        coh.force_sync(1).await.unwrap();
    }

    #[tokio::test]
    async fn test_apply_local_write_buffers_to_change_cache() {
        let coh = make_coherence();
        let entry = DirEntry::new_file(EntryId::new("test.txt", 1, 1), 100, 1, 0o644, 0, 0);
        let mut vc = VectorClock::new();
        vc.increment(1);

        // apply_local_write 应将 delta 缓冲到 ChangeCache
        coh.apply_local_write(
            1,
            DeltaOp::Add {
                entry: entry.clone(),
                vclock: vc,
            },
        )
        .unwrap();

        // force_sync 应 drain 并 push（mock channel 返回成功）
        coh.force_sync(1).await.unwrap();
    }

    #[tokio::test]
    async fn test_inode_allocator() {
        let mut alloc = InodeAllocator::new(1024);
        assert_eq!(alloc.alloc(), None);
        alloc.refill(100, 105);
        assert_eq!(alloc.alloc(), Some(100));
        assert_eq!(alloc.alloc(), Some(101));
        assert_eq!(alloc.remaining(), 3);
        assert_eq!(alloc.alloc(), Some(102));
        assert_eq!(alloc.alloc(), Some(103));
        assert_eq!(alloc.alloc(), Some(104));
        assert_eq!(alloc.alloc(), None);
    }

    #[test]
    fn test_sharded_dir_cache_lru_eviction() {
        let cache = ShardedDirCache::new(3);
        for i in 1..=3 {
            cache.insert(i, Arc::new(std::sync::RwLock::new(DirORSet::new(i))));
        }
        assert_eq!(cache.len(), 3);
        // 插入第 4 个，触发淘汰
        cache.insert(4, Arc::new(std::sync::RwLock::new(DirORSet::new(4))));
        // 淘汰后不超过 max
        assert!(cache.len() <= 3);
    }

    #[tokio::test]
    async fn test_alloc_inode_from_batch() {
        let coh = make_coherence();
        // MockDeltaSyncChannel 返回 [1, 1024)；alloc_inode 跳过保留 inode 0/1（POSIX root）
        let ino = coh.alloc_inode().await.unwrap();
        assert_eq!(ino, 2);
        let ino2 = coh.alloc_inode().await.unwrap();
        assert_eq!(ino2, 3);
    }

    #[tokio::test]
    async fn test_local_create_entry() {
        let coh = make_coherence();
        let entry = coh
            .local_create_entry(1, "file.txt", 100, false, 0o644, 0, 0)
            .unwrap();
        assert_eq!(entry.inode, 100);
        assert_eq!(entry.id.name, "file.txt");
        assert_eq!(entry.id.client_id, 1);
        assert_eq!(entry.id.seq, 1);
        // lookup 能找到
        assert_eq!(coh.lookup(1, "file.txt"), Some(100));
        // ChangeCache 有待 sync 的 delta
        assert_eq!(coh.change_cache.total_count(), 1);
    }

    #[tokio::test]
    async fn test_local_create_dir_then_remove() {
        let coh = make_coherence();
        coh.local_create_entry(1, "subdir", 200, true, 0o755, 0, 0)
            .unwrap();
        assert_eq!(coh.lookup(1, "subdir"), Some(200));

        let ino = coh.local_remove_entry(1, "subdir").unwrap();
        assert_eq!(ino, 200);
        assert_eq!(coh.lookup(1, "subdir"), None);
    }

    #[tokio::test]
    async fn test_local_rename_same_dir() {
        let coh = make_coherence();
        coh.local_create_entry(1, "old.txt", 300, false, 0o644, 0, 0)
            .unwrap();
        let ino = coh.local_rename_entry(1, "old.txt", "new.txt").unwrap();
        assert_eq!(ino, 300);
        assert_eq!(coh.lookup(1, "old.txt"), None);
        assert_eq!(coh.lookup(1, "new.txt"), Some(300));
    }

    #[tokio::test]
    async fn test_local_create_vclock_advances() {
        // 多次 local_create_entry 应使 vclock 持续递增，EntryId.seq 不重复
        let coh = make_coherence();
        coh.local_create_entry(1, "a", 10, false, 0o644, 0, 0)
            .unwrap();
        coh.local_create_entry(1, "b", 11, false, 0o644, 0, 0)
            .unwrap();
        coh.local_create_entry(1, "c", 12, false, 0o644, 0, 0)
            .unwrap();
        assert_eq!(coh.lookup(1, "a"), Some(10));
        assert_eq!(coh.lookup(1, "b"), Some(11));
        assert_eq!(coh.lookup(1, "c"), Some(12));
        assert_eq!(coh.change_cache.total_count(), 3);
    }
}
