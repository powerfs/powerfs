//! 元数据管理器
//!
//! 封装目录元数据相关的 OR-Set 缓存和管理：
//! - dir_cache: 每个目录的 OR-Set（dir_inode -> DirORSet）
//! - inode_index: inode 反向索引（ino -> (dir_ino, EntryId)）
//! - inode_paths: inode → 全路径映射（用于 Master 同步）
//! - projection: POSIX 投影层
//! - inode_allocator: inode 分配器
//!
//! 弱一致语义：
//! - 读路径：本地 OR-Set 优先，miss 时回退 Master 拉取
//! - 写路径：本地 OR-Set 即返回成功，异步/best-effort 同步到 Master
//! - 同步失败仅 warn，不影响本地操作成功
//!
//! Phase 1A：单客户端场景，每个 name 只有一个 entry，无冲突

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

use crossbeam::channel::{self, Receiver, Sender};
use log::{debug, error, info, warn};

use crate::client::SyncFuseClient;
use crate::error::FsError;
use crate::inode_allocator::InodeAllocator;
use crate::orset::{now_unix, DirEntry, DirORSet, EntryId, FileType, VectorClock};
use crate::posix_projection::{PosixProjection, VisibleEntry};

// Import Filer types for Delta Sync
use powerfs_filer::powerfs::VectorClock as FilerVectorClock;
use powerfs_master::proto::powerfs::{
    DeltaOp, DirEntryOrset, EntryId as MasterEntryId, RenameOp, SetAttrOp,
    VectorClock as MasterVectorClock,
};

/// 根目录 inode
const ROOT_INO: u64 = 1;

/// Master 来源条目的 client_id（区分本地创建与 Master 同步）
const MASTER_CLIENT_ID: u64 = 0;

/// 目录 OR-Set 缓存类型（简化复杂类型）
type DirCache = HashMap<u64, Arc<RwLock<DirORSet>>>;

/// 分片目录缓存
///
/// Phase 3：Per-Queue单线程消费模式
/// 按 parent inode 号 hash 分片，每个分片有独立的工作线程和无锁队列：
/// - 分片数量 = CPU核心数 * 2
/// - 同目录操作由同一工作线程处理，无需锁
/// - 跨分片操作（如rename）通过消息传递，避免大锁
struct ShardedDirCache {
    shards: Arc<Vec<Arc<RwLock<DirCache>>>>,
    num_shards: usize,
    senders: Arc<Vec<Sender<ShardOp>>>,
    _threads: Vec<thread::JoinHandle<()>>,
}

/// 分片操作类型
enum ShardOp {
    Insert {
        dir_ino: u64,
        orset: Arc<RwLock<DirORSet>>,
    },
    Remove {
        dir_ino: u64,
    },
    Get {
        dir_ino: u64,
        reply: Sender<Option<Arc<RwLock<DirORSet>>>>,
    },
}

impl Clone for ShardedDirCache {
    fn clone(&self) -> Self {
        Self {
            shards: self.shards.clone(),
            num_shards: self.num_shards,
            senders: self.senders.clone(),
            _threads: Vec::new(),
        }
    }
}

impl ShardedDirCache {
    fn new() -> Self {
        let num_shards = std::thread::available_parallelism()
            .map(|n| n.get() * 2)
            .unwrap_or(8);

        let mut shards = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            shards.push(Arc::new(RwLock::new(HashMap::new())));
        }

        let shards_arc = Arc::new(shards);
        let mut senders = Vec::with_capacity(num_shards);
        let mut threads = Vec::with_capacity(num_shards);

        for i in 0..num_shards {
            let (sender, receiver) = channel::unbounded();
            let shard = shards_arc[i].clone();
            let thread = thread::spawn(move || {
                Self::shard_worker(receiver, shard);
            });
            senders.push(sender);
            threads.push(thread);
        }

        Self {
            shards: shards_arc,
            num_shards,
            senders: Arc::new(senders),
            _threads: threads,
        }
    }

    fn shard_worker(receiver: Receiver<ShardOp>, shard: Arc<RwLock<DirCache>>) {
        while let Ok(op) = receiver.recv() {
            match op {
                ShardOp::Insert { dir_ino, orset } => {
                    shard.write().unwrap().insert(dir_ino, orset);
                }
                ShardOp::Remove { dir_ino } => {
                    shard.write().unwrap().remove(&dir_ino);
                }
                ShardOp::Get { dir_ino, reply } => {
                    let result = shard.read().unwrap().get(&dir_ino).cloned();
                    let _ = reply.send(result);
                }
            }
        }
    }

    fn shard_index(&self, dir_ino: u64) -> usize {
        (dir_ino as usize) % self.num_shards
    }

    fn get_shard(&self, dir_ino: u64) -> Arc<RwLock<DirCache>> {
        let idx = self.shard_index(dir_ino);
        self.shards[idx].clone()
    }

    fn insert(&self, dir_ino: u64, orset: Arc<RwLock<DirORSet>>) {
        let idx = self.shard_index(dir_ino);
        let _ = self.senders[idx].send(ShardOp::Insert { dir_ino, orset });
        let (read_sender, read_receiver) = channel::bounded::<Option<Arc<RwLock<DirORSet>>>>(1);
        let _ = self.senders[idx].send(ShardOp::Get {
            dir_ino,
            reply: read_sender,
        });
        let _ = read_receiver.recv();
    }

    fn remove(&self, dir_ino: u64) -> Option<Arc<RwLock<DirORSet>>> {
        let idx = self.shard_index(dir_ino);
        let _ = self.senders[idx].send(ShardOp::Remove { dir_ino });
        let (read_sender, read_receiver) = channel::bounded::<Option<Arc<RwLock<DirORSet>>>>(1);
        let _ = self.senders[idx].send(ShardOp::Get {
            dir_ino,
            reply: read_sender,
        });
        read_receiver.recv().unwrap_or(None)
    }

    fn get(&self, dir_ino: u64) -> Option<Arc<RwLock<DirORSet>>> {
        let idx = self.shard_index(dir_ino);
        let (reply_sender, reply_receiver) = channel::bounded::<Option<Arc<RwLock<DirORSet>>>>(1);
        let _ = self.senders[idx].send(ShardOp::Get {
            dir_ino,
            reply: reply_sender,
        });
        reply_receiver.recv().unwrap_or(None)
    }

    fn try_read(&self, dir_ino: u64) -> Result<Option<Arc<RwLock<DirORSet>>>, ()> {
        let shard = self.get_shard(dir_ino);
        let guard = shard.try_read().map_err(|_| ())?;
        Ok(guard.get(&dir_ino).cloned())
    }

    fn ensure_dir_cache(&self, dir_ino: u64) -> Arc<RwLock<DirORSet>> {
        let shard = self.get_shard(dir_ino);
        {
            let cache = shard.read().unwrap();
            if let Some(orset_arc) = cache.get(&dir_ino) {
                return orset_arc.clone();
            }
        }
        let mut cache = shard.write().unwrap();
        cache
            .entry(dir_ino)
            .or_insert_with(|| Arc::new(RwLock::new(DirORSet::new(dir_ino))))
            .clone()
    }
}

/// 变更操作类型
#[derive(Debug, Clone)]
enum ChangeOp {
    Create(DirEntry),
    Delete(u64, ()),
    Rename(u64, String, u64, String),
    SetAttr(DirEntry),
}

enum InvalidationRequest {
    InvalidateEntry(u64, String),
    InvalidatePath(String),
}

/// 变更缓存配置
const CHANGE_CACHE_BATCH_SIZE: usize = 64;

/// Inode 状态信息（用于引用计数和 generation 管理）
struct InodeState {
    generation: u64,
    ref_count: std::sync::atomic::AtomicU32,
}

impl InodeState {
    fn new(generation: u64) -> Self {
        Self {
            generation,
            ref_count: std::sync::atomic::AtomicU32::new(1),
        }
    }
}

pub struct MetadataManager {
    /// 本地 OR-Set 缓存：按 parent inode hash 分片的 dir_inode -> DirORSet
    dir_cache: ShardedDirCache,
    /// inode 反向索引：ino -> (dir_ino, EntryId)
    inode_index: Arc<RwLock<HashMap<u64, (u64, EntryId)>>>,
    /// inode → 全路径映射（用于 Master 同步时的 path KV 兼容）
    inode_paths: Arc<RwLock<HashMap<u64, String>>>,
    /// inode 状态：ino -> InodeState（引用计数 + generation）
    inode_state: Arc<RwLock<HashMap<u64, InodeState>>>,
    /// POSIX 投影层
    projection: PosixProjection,
    /// Inode 分配器
    inode_allocator: InodeAllocator,
    /// 客户端 ID（数字形式，用于 EntryId）
    client_id: u64,
    /// 客户端 ID（字符串形式，用于 Master API）
    client_id_str: String,
    /// 本地 seq 计数器
    seq_counter: AtomicU64,
    /// gRPC 客户端（可选，None 时仅本地缓存）
    client: Option<Arc<SyncFuseClient>>,
    /// 客户端 VectorClock（用于 Delta Sync）
    client_vclock: Arc<RwLock<VectorClock>>,
    /// 变更缓存发送端（无锁化设计）
    change_sender: Sender<ChangeOp>,
    /// 写操作计数器（用于动态 Delta Sync 间隔）
    write_counter: Arc<std::sync::atomic::AtomicU64>,
    /// 缓存失效请求发送端（异步处理）
    invalidation_sender: Sender<InvalidationRequest>,
}

impl MetadataManager {
    /// 创建本地版 MetadataManager（无 Master 连接，仅本地缓存）
    pub fn new_local(client_id: u64) -> Self {
        let mgr = Self {
            dir_cache: ShardedDirCache::new(),
            inode_index: Arc::new(RwLock::new(HashMap::new())),
            inode_paths: Arc::new(RwLock::new(HashMap::new())),
            inode_state: Arc::new(RwLock::new(HashMap::new())),
            projection: PosixProjection::new(),
            inode_allocator: InodeAllocator::new(client_id),
            client_id,
            client_id_str: client_id.to_string(),
            seq_counter: AtomicU64::new(1),
            client: None,
            client_vclock: Arc::new(RwLock::new(VectorClock::new())),
            change_sender: channel::unbounded().0,
            write_counter: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            invalidation_sender: channel::unbounded().0,
        };
        mgr.init_root();
        mgr
    }

    /// 创建带 Master 连接的 MetadataManager
    pub fn new_with_master(client: Arc<SyncFuseClient>, client_id: u64) -> Self {
        let (change_sender, change_receiver) = channel::unbounded();
        let (invalidation_sender, invalidation_receiver) = channel::unbounded();

        let mgr = Self {
            dir_cache: ShardedDirCache::new(),
            inode_index: Arc::new(RwLock::new(HashMap::new())),
            inode_paths: Arc::new(RwLock::new(HashMap::new())),
            inode_state: Arc::new(RwLock::new(HashMap::new())),
            projection: PosixProjection::new(),
            inode_allocator: InodeAllocator::new(client_id),
            client_id,
            client_id_str: client_id.to_string(),
            seq_counter: AtomicU64::new(1),
            client: Some(client),
            client_vclock: Arc::new(RwLock::new(VectorClock::new())),
            change_sender,
            write_counter: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            invalidation_sender,
        };
        mgr.init_root();
        mgr.start_change_cache_flusher(change_receiver);
        mgr.start_invalidation_processor(invalidation_receiver);
        mgr
    }

    /// 初始化根目录
    fn init_root(&self) {
        // 根目录的 OR-Set
        let root_orset = Arc::new(RwLock::new(DirORSet::new(ROOT_INO)));
        self.dir_cache.insert(ROOT_INO, root_orset);

        // 根目录路径
        self.inode_paths
            .write()
            .unwrap()
            .insert(ROOT_INO, "/".to_string());

        // 根目录 inode 状态
        self.inode_state
            .write()
            .unwrap()
            .insert(ROOT_INO, InodeState::new(0));
    }

    /// 获取 client_id
    pub fn client_id(&self) -> u64 {
        self.client_id
    }

    /// 获取 inode 的 generation
    pub fn get_inode_generation(&self, ino: u64) -> u64 {
        let state_map = self.inode_state.read().unwrap();
        state_map.get(&ino).map(|s| s.generation).unwrap_or(0)
    }

    /// 获取 inode 的引用计数
    pub fn get_inode_ref_count(&self, ino: u64) -> u32 {
        let state_map = self.inode_state.read().unwrap();
        state_map
            .get(&ino)
            .map(|s| s.ref_count.load(std::sync::atomic::Ordering::SeqCst))
            .unwrap_or(0)
    }

    /// 增加 inode 引用计数（文件打开时调用）
    pub fn acquire_inode(&self, ino: u64) {
        let state_map = self.inode_state.read().unwrap();
        if let Some(state) = state_map.get(&ino) {
            state
                .ref_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// 减少 inode 引用计数（文件关闭时调用）
    /// 返回当前引用计数（减1之后的值）
    /// 当引用计数减为0时，清理 inode_state
    pub fn release_inode(&self, ino: u64) -> u32 {
        let state_map = self.inode_state.read().unwrap();
        if let Some(state) = state_map.get(&ino) {
            let new_count = state
                .ref_count
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            if new_count == 1 {
                drop(state_map);
                let mut state_map_write = self.inode_state.write().unwrap();
                state_map_write.remove(&ino);
            }
            new_count - 1
        } else {
            0
        }
    }

    /// 乐观更新 inode size
    /// insize: 操作开始时的 size
    /// outsize: 操作结束后的 size
    /// 如果当前 size != insize，说明有其他操作（如 truncate），放弃更新
    pub fn update_size_optimistic(&self, ino: u64, insize: u64, outsize: u64) -> bool {
        let entry_info = {
            let index = self.inode_index.read().unwrap();
            index.get(&ino).map(|(d, e)| (*d, e.clone()))
        };

        if let Some((dir_ino, entry_id)) = entry_info {
            if let Some(orset_arc) = self.dir_cache.get(dir_ino) {
                let mut orset = orset_arc.write().unwrap();
                if let Some(entry) = orset.entries.get_mut(&entry_id) {
                    if entry.size == insize {
                        entry.size = outsize;
                        return true;
                    }
                }
            }
        }
        false
    }

    // ==================== 读路径 ====================

    /// 查找目录下的条目（lookup）
    ///
    /// 本地 OR-Set 优先，miss 时回退 Master 拉取。
    pub fn lookup(&self, dir_ino: u64, name: &str) -> Result<Option<DirEntry>, FsError> {
        // 先查本地
        if let Some(entry) = self.lookup_local(dir_ino, name) {
            return Ok(Some(entry));
        }

        // 本地未命中：仅当目录 OR-Set 不存在时才从 Master 拉取
        // （如果 OR-Set 已存在但条目不存在，说明已被删除或从未创建，不应从 Master 重新拉取，
        //   否则会用 Master 的旧数据覆盖本地删除——Master 不保存 client_id/seq，
        //   tombstone 无法匹配，会导致已删除的条目复活）
        let need_fetch = self.dir_cache.get(dir_ino).is_none();

        if need_fetch && self.client.is_some() {
            self.fetch_dir_from_master(dir_ino)?;
            // 再查一次本地
            if let Some(entry) = self.lookup_local(dir_ino, name) {
                return Ok(Some(entry));
            }
        }

        Ok(None)
    }

    /// 列出目录下的所有可见条目（readdir）
    ///
    /// 返回 POSIX 投影后的条目列表（同名只返回主版本）。
    pub fn list_dir(&self, dir_ino: u64) -> Result<Vec<VisibleEntry>, FsError> {
        // 第一次尝试：检查本地缓存是否有数据
        // 注意：这里不调用 ensure_dir_cache，避免过早持有 dir_cache 锁
        let local_listing = self.try_list_dir_local(dir_ino);
        if let Some(listing) = local_listing {
            return Ok(listing);
        }

        // 本地为空，尝试从 Master 拉取
        // 关键：此时不持有任何锁，可以安全地获取 dir_cache 写锁
        if self.client.is_some() {
            match self.fetch_dir_from_master_without_deadlock(dir_ino) {
                Ok(_) => (),
                Err(e) => {
                    error!("fetch_dir_from_master failed: {}", e);
                }
            }
        }

        // 第二次尝试：再次读取本地缓存
        let local_listing = self.try_list_dir_local(dir_ino);
        if let Some(listing) = local_listing {
            return Ok(listing);
        }

        // 最后的 fallback：使用非阻塞方式获取缓存
        // 避免在锁竞争时无限阻塞
        let orset_arc_result = self.dir_cache.try_read(dir_ino);
        if let Ok(Some(orset_arc)) = orset_arc_result {
            let orset = orset_arc.try_read();
            if let Ok(orset) = orset {
                return Ok(self.projection.project_listing(&orset));
            }
        }

        Ok(Vec::new())
    }

    /// 尝试从本地缓存列出目录（不触发 Master 拉取）
    ///
    /// 返回 None 表示需要从 Master 拉取
    fn try_list_dir_local(&self, dir_ino: u64) -> Option<Vec<VisibleEntry>> {
        // 先尝试读锁
        let orset_arc = self.dir_cache.get(dir_ino)?;

        let orset = orset_arc.read().ok()?;
        // 【关键修复】当 OR-Set 为空时，返回空向量而不是 None
        // 避免 list_dir 尝试从 Master 拉取数据，导致已删除的文件重新出现
        Some(self.projection.project_listing(&orset))
    }

    /// 按 inode 获取条目（getattr 用）
    pub fn get_entry_by_inode(&self, ino: u64) -> Result<Option<DirEntry>, FsError> {
        // 先查本地索引
        let entry_info = {
            let index = self.inode_index.read().unwrap();
            index.get(&ino).map(|(d, e)| (*d, e.clone()))
        };

        if let Some((dir_ino, entry_id)) = entry_info {
            // 【关键修复】先释放 inode_index 锁，再获取 dir_cache 锁
            // 避免死锁场景：
            // - 线程A（get_entry_by_inode）: 持有 inode_index 读锁 → 等待 dir_cache 读锁
            // - 线程B（rmdir/unlink）: 持有 dir_cache 写锁 → 等待 inode_index 写锁
            let orset_arc_result = self.dir_cache.try_read(dir_ino);
            if let Ok(Some(orset_arc)) = orset_arc_result {
                let orset_result = orset_arc.try_read();
                if let Ok(orset) = orset_result {
                    if let Some(entry) = orset.entries.get(&entry_id) {
                        return Ok(Some(entry.clone()));
                    }
                }
            }
            // 索引存在但 OR-Set 中找不到，非阻塞清理
            let _ = self.try_cleanup_stale_inode(ino);
        }

        if ino == ROOT_INO {
            return Ok(Some(self.make_root_entry()));
        }

        if self.client.is_some() {
            return self.fetch_entry_by_inode_from_master(ino);
        }

        Ok(None)
    }

    /// 获取父目录条目
    pub fn get_parent_dir(&self, dir_ino: u64) -> Result<Option<DirEntry>, FsError> {
        if dir_ino == ROOT_INO {
            return Ok(Some(self.make_root_entry()));
        }
        let entry = self.get_entry_by_inode(dir_ino)?;
        if let Some(e) = entry {
            if e.parent_ino == 0 || e.parent_ino == dir_ino {
                return Ok(Some(e));
            }
            return self.get_entry_by_inode(e.parent_ino);
        }
        Ok(None)
    }

    /// 获取 inode 的全路径（用于 Master 同步）
    pub fn get_path(&self, ino: u64) -> Option<String> {
        let paths = self.inode_paths.read().unwrap();
        paths.get(&ino).cloned()
    }

    // ==================== .conflicts/ 虚拟目录支持 ====================

    /// 获取 .conflicts/ 目录的 inode
    pub fn get_conflict_dir_inode(&self, dir_ino: u64) -> u64 {
        self.projection.get_conflict_dir_inode(dir_ino)
    }

    /// 判断一个 inode 是否为 .conflicts/ 虚拟目录
    pub fn is_conflict_dir_inode(&self, ino: u64) -> bool {
        self.projection.is_conflict_dir_inode(ino)
    }

    /// 从 .conflicts/ inode 获取真实目录 inode
    pub fn get_real_dir_inode(&self, conflict_dir_ino: u64) -> u64 {
        self.projection.get_real_dir_inode(conflict_dir_ino)
    }

    /// 获取 .conflicts/ 虚拟目录的属性
    pub fn get_conflict_dir_attr(&self, real_dir_ino: u64) -> fuser::FileAttr {
        let dir_entry = self.get_entry_by_inode(real_dir_ino).ok().flatten();
        let conflict_dir_ino = self.get_conflict_dir_inode(real_dir_ino);

        let mode = dir_entry
            .as_ref()
            .map(|e| e.mode)
            .unwrap_or(0o755 | libc::S_IFDIR);

        fuser::FileAttr {
            ino: conflict_dir_ino,
            size: 0,
            blocks: 1,
            atime: std::time::UNIX_EPOCH,
            mtime: std::time::UNIX_EPOCH,
            ctime: std::time::UNIX_EPOCH,
            crtime: std::time::UNIX_EPOCH,
            kind: fuser::FileType::Directory,
            perm: (mode & 0o777) as u16,
            nlink: 2,
            uid: 0,
            gid: 0,
            rdev: 0,
            flags: 0,
            blksize: 4096,
        }
    }

    /// 列出 .conflicts/ 目录中的所有冲突条目
    pub fn list_conflict_dir(
        &self,
        dir_ino: u64,
    ) -> Result<Vec<crate::posix_projection::ConflictEntry>, FsError> {
        let orset_arc = self.ensure_dir_cache(dir_ino);
        let orset = orset_arc.read().unwrap();
        Ok(self.projection.list_conflict_dir(&orset))
    }

    // ==================== 写路径 ====================

    /// 创建普通文件
    pub fn create(
        &self,
        dir_ino: u64,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DirEntry, FsError> {
        let inode = self.inode_allocator.allocate();
        let seq = self.next_seq();
        let entry_id = EntryId::new(name, self.client_id, seq);
        let entry = DirEntry::new_file(entry_id, inode, dir_ino, mode, uid, gid);

        self.apply_to_local_orset(dir_ino, entry.clone())?;

        // best-effort 同步到 Master（使用变更缓存）
        self.add_change(ChangeOp::Create(entry.clone()));

        Ok(entry)
    }

    /// 创建目录
    pub fn mkdir(
        &self,
        dir_ino: u64,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DirEntry, FsError> {
        let inode = self.inode_allocator.allocate();
        let seq = self.next_seq();
        let entry_id = EntryId::new(name, self.client_id, seq);
        let entry = DirEntry::new_dir(entry_id, inode, dir_ino, mode, uid, gid);

        // 先调用 apply_to_local_orset（更新 inode_index/inode_paths），再创建 OR-Set
        // 避免死锁：apply_to_local_orset 需要 inode_index.write()，如果先持有 dir_cache.write()
        // 而 get_entry_by_inode 持有 inode_index.read() 并等待 dir_cache.read()，会形成循环等待
        self.apply_to_local_orset(dir_ino, entry.clone())?;

        // 单独获取分片锁创建新目录的 OR-Set
        let new_dir_orset = Arc::new(RwLock::new(DirORSet::new(inode)));
        self.dir_cache.insert(inode, new_dir_orset);

        // CRDT 写路径：仅写入本地 + 异步 push_delta
        // 不再需要同步 RPC 调用，ChangeCache flusher 会批量推送
        self.add_change(ChangeOp::Create(entry.clone()));

        Ok(entry)
    }

    /// 创建符号链接
    pub fn symlink(&self, dir_ino: u64, name: &str, target: &str) -> Result<DirEntry, FsError> {
        let inode = self.inode_allocator.allocate();
        let seq = self.next_seq();
        let entry_id = EntryId::new(name, self.client_id, seq);
        let entry = DirEntry::new_symlink(
            entry_id,
            inode,
            dir_ino,
            0o777 | libc::S_IFLNK,
            target.to_string(),
        );

        self.apply_to_local_orset(dir_ino, entry.clone())?;

        // best-effort 同步到 Master（使用变更缓存）
        self.add_change(ChangeOp::Create(entry.clone()));

        Ok(entry)
    }

    /// 创建特殊文件（mknod）
    pub fn mknod(
        &self,
        dir_ino: u64,
        name: &str,
        mode: u32,
        rdev: u32,
    ) -> Result<DirEntry, FsError> {
        let inode = self.inode_allocator.allocate();
        let seq = self.next_seq();
        let entry_id = EntryId::new(name, self.client_id, seq);

        let file_type = FileType::from_mode(mode);
        let entry = match file_type {
            FileType::Fifo => DirEntry::new_fifo(entry_id, inode, dir_ino, mode, 0, 0),
            FileType::CharDevice => {
                DirEntry::new_chrdev(entry_id, inode, dir_ino, mode, rdev as u64, 0, 0)
            }
            FileType::BlockDevice => {
                DirEntry::new_blkdev(entry_id, inode, dir_ino, mode, rdev as u64, 0, 0)
            }
            FileType::Socket => DirEntry::new_socket(entry_id, inode, dir_ino, mode, 0, 0),
            _ => DirEntry::new_file(entry_id, inode, dir_ino, mode, 0, 0),
        };

        self.apply_to_local_orset(dir_ino, entry.clone())?;

        self.add_change(ChangeOp::Create(entry.clone()));

        Ok(entry)
    }

    /// 创建硬链接
    pub fn link(&self, ino: u64, new_dir_ino: u64, new_name: &str) -> Result<DirEntry, FsError> {
        let (old_dir_ino, _old_entry_id) = {
            let index = self.inode_index.read().unwrap();
            index
                .get(&ino)
                .cloned()
                .ok_or_else(|| FsError::NotFound(format!("link: inode {} not found", ino)))?
        };

        let old_orset_arc = self.ensure_dir_cache(old_dir_ino);
        let old_orset = old_orset_arc.read().unwrap();
        let old_entry = old_orset
            .entries
            .values()
            .find(|e| e.inode == ino)
            .cloned()
            .ok_or_else(|| FsError::NotFound(format!("link: inode {} entry not found", ino)))?;
        drop(old_orset);

        let seq = self.next_seq();
        let new_entry_id = EntryId::new(new_name, self.client_id, seq);
        let mut new_entry = old_entry.clone();
        new_entry.id = new_entry_id;
        new_entry.parent_ino = new_dir_ino;
        new_entry.nlink = old_entry.nlink + 1;
        new_entry.ctime = now_unix();

        self.apply_to_local_orset(new_dir_ino, new_entry.clone())?;

        let orset_arc = self.ensure_dir_cache(old_dir_ino);
        let mut orset = orset_arc.write().unwrap();
        if let Some(e) = orset.entries.get_mut(&old_entry.id) {
            e.nlink += 1;
            e.ctime = now_unix();
        }
        drop(orset);

        self.add_change(ChangeOp::Create(new_entry.clone()));

        Ok(new_entry)
    }

    /// 删除文件（unlink）
    ///
    /// 1. 从本地 OR-Set 查找并移除
    /// 2. 加入 tombstones
    /// 3. best-effort 同步到 Master
    ///
    /// 返回被删除文件的 inode（供调用方清理数据缓存）
    pub fn unlink(&self, dir_ino: u64, name: &str) -> Result<u64, FsError> {
        // 使用分片写锁，确保查找和删除原子操作
        let (inode, _entry_id) = {
            let orset_arc = self
                .dir_cache
                .get(dir_ino)
                .ok_or_else(|| FsError::NotFound(format!("unlink: dir {} not found", dir_ino)))?;

            let mut orset = orset_arc.write().unwrap();
            let entry = self
                .projection
                .project_lookup(&orset, name)
                .ok_or_else(|| {
                    FsError::NotFound(format!("unlink: {} not found in dir {}", name, dir_ino))
                })?;

            if entry.file_type == FileType::Directory {
                return Err(FsError::IsDirectory(format!(
                    "unlink: {} is a directory, use rmdir instead",
                    name
                )));
            }

            orset.remove(&entry.id);
            (entry.inode, entry.id.clone())
        };

        {
            let mut index = self.inode_index.write().unwrap();
            index.remove(&inode);
        }
        {
            let mut paths = self.inode_paths.write().unwrap();
            paths.remove(&inode);
        }

        self.release_inode(inode);

        self.add_change(ChangeOp::Delete(inode, ()));
        Ok(inode)
    }

    /// 删除目录（rmdir）
    pub fn rmdir(&self, dir_ino: u64, name: &str) -> Result<(), FsError> {
        // 使用分片锁：需要获取父目录分片和子目录分片（按序号顺序获取避免死锁）
        let parent_shard_idx = self.dir_cache.shard_index(dir_ino);

        let (child_inode, _entry_id) = {
            // 获取父分片写锁
            let parent_shard = self.dir_cache.shards[parent_shard_idx].clone();
            let mut parent_cache = parent_shard.write().unwrap();

            // 在写锁保护下查找、检查和删除（原子操作）
            let (child_inode, entry_id) = if let Some(parent_orset) = parent_cache.get(&dir_ino) {
                let mut orset = parent_orset.write().unwrap();
                let entry = self
                    .projection
                    .project_lookup(&orset, name)
                    .ok_or_else(|| {
                        FsError::NotFound(format!("rmdir: {} not found in dir {}", name, dir_ino))
                    })?;

                if entry.file_type != FileType::Directory {
                    return Err(FsError::NotDirectory(format!(
                        "rmdir: {} is not a directory",
                        name
                    )));
                }

                let child_inode = entry.inode;

                let is_empty = {
                    let child_shard_idx = self.dir_cache.shard_index(child_inode);
                    if child_shard_idx == parent_shard_idx {
                        if let Some(child_orset) = parent_cache.get(&child_inode) {
                            child_orset.read().unwrap().is_empty()
                        } else {
                            true
                        }
                    } else {
                        let child_shard = self.dir_cache.shards[child_shard_idx].clone();
                        let child_cache = child_shard.read().unwrap();
                        if let Some(child_orset) = child_cache.get(&child_inode) {
                            child_orset.read().unwrap().is_empty()
                        } else {
                            true
                        }
                    }
                };

                if !is_empty {
                    return Err(FsError::NotEmpty(format!(
                        "rmdir: directory {} is not empty",
                        name
                    )));
                }

                orset.remove(&entry.id);
                (child_inode, entry.id.clone())
            } else {
                return Err(FsError::NotFound(format!(
                    "rmdir: {} not found in dir {}",
                    name, dir_ino
                )));
            };

            // 如果子目录和父目录不在同一个分片，需要单独删除
            let child_shard_idx = self.dir_cache.shard_index(child_inode);
            if child_shard_idx != parent_shard_idx {
                let child_shard = self.dir_cache.shards[child_shard_idx].clone();
                let mut child_cache = child_shard.write().unwrap();
                child_cache.remove(&child_inode);
            } else {
                parent_cache.remove(&child_inode);
            }

            (child_inode, entry_id)
        };

        // 清理索引（在 dir_cache 写锁释放后）
        {
            let mut index = self.inode_index.write().unwrap();
            index.remove(&child_inode);
        }
        {
            let mut paths = self.inode_paths.write().unwrap();
            paths.remove(&child_inode);
        }

        self.release_inode(child_inode);

        self.add_change(ChangeOp::Delete(child_inode, ()));
        Ok(())
    }

    /// 递归删除目录及其所有内容
    pub fn rmdir_recursive(&self, dir_ino: u64) -> Result<(), FsError> {
        // 获取目录下的所有子项
        let children = match self.list_dir(dir_ino) {
            Ok(list) => list,
            Err(e) => {
                warn!(
                    "rmdir_recursive: list_dir failed for inode {}: {}",
                    dir_ino, e
                );
                return Ok(());
            }
        };

        // 先删除所有子目录（递归）
        for entry in children.iter() {
            if entry.file_type == FileType::Directory {
                if let Err(e) = self.rmdir_recursive(entry.inode) {
                    warn!(
                        "rmdir_recursive: failed to delete subdir {}: {}",
                        entry.name, e
                    );
                }
            }
        }

        // 再删除所有文件
        for entry in children.iter() {
            if entry.file_type != FileType::Directory {
                if let Err(e) = self.unlink(dir_ino, &entry.name) {
                    warn!(
                        "rmdir_recursive: failed to unlink file {}: {}",
                        entry.name, e
                    );
                }
            }
        }

        // 获取父目录和名称来调用 rmdir
        let (parent_ino, name) = {
            let index = self.inode_index.read().unwrap();
            if let Some(&(p, ref entry_id)) = index.get(&dir_ino) {
                (p, entry_id.name.clone())
            } else {
                return Ok(());
            }
        };

        // 最后删除当前目录
        self.rmdir(parent_ino, &name)
    }

    /// 重命名（rename）
    ///
    /// Phase 1A 弱一致语义：本地 Remove + Add，非原子操作。
    /// 1. 从 old_dir 查找 old_name
    /// 2. 如果 new_name 已存在，tombstone 旧目标（POSIX 覆盖语义）
    /// 3. 创建新 entry（new_name，保留 inode）
    /// 4. 从 old_dir 移除，加入 new_dir
    /// 5. best-effort 同步到 Master
    ///
    /// 返回被覆盖文件的 inode（如果目标已存在），供调用方清理数据缓存。
    pub fn rename(
        &self,
        old_dir: u64,
        old_name: &str,
        new_dir: u64,
        new_name: &str,
    ) -> Result<Option<u64>, FsError> {
        if old_dir == new_dir && old_name == new_name {
            return Ok(None);
        }

        // 【关键修复】先获取父路径，避免在持有 dir_cache 锁时再获取 inode_paths 锁导致死锁
        let parent_path = self.get_path(new_dir).unwrap_or_else(|| "/".to_string());
        let new_path = if parent_path == "/" {
            format!("/{}", new_name)
        } else {
            format!("{}/{}", parent_path, new_name)
        };

        // 检查是否跨分片
        let old_shard_idx = self.dir_cache.shard_index(old_dir);
        let new_shard_idx = self.dir_cache.shard_index(new_dir);
        let is_cross_shard = old_shard_idx != new_shard_idx;

        if is_cross_shard {
            return self.rename_cross_shard(old_dir, old_name, new_dir, new_name, &new_path);
        }

        // 同分片 rename：使用分片写锁
        let shard = self.dir_cache.get_shard(old_dir);

        let (overwritten_inode, inode_to_update, new_entry_id_for_index, dest_inode_to_remove) = {
            let mut cache = shard.write().unwrap();

            let old_entry = if let Some(old_orset) = cache.get(&old_dir) {
                let orset = old_orset.read().unwrap();
                self.projection.project_lookup(&orset, old_name)
            } else {
                None
            }
            .ok_or_else(|| {
                FsError::NotFound(format!("rename: {} not found in dir {}", old_name, old_dir))
            })?;

            let (overwritten_inode, dest_to_remove) = if let Some(new_orset) = cache.get(&new_dir) {
                let orset = new_orset.read().unwrap();
                if let Some(dest) = self.projection.project_lookup(&orset, new_name) {
                    if dest.inode != old_entry.inode {
                        if dest.file_type == FileType::Directory {
                            return Err(FsError::IsDirectory(format!(
                                "rename: cannot overwrite directory {}",
                                new_name
                            )));
                        }
                        (
                            Some(dest.inode),
                            Some((new_dir, dest.id.clone(), dest.inode)),
                        )
                    } else {
                        (None, None)
                    }
                } else {
                    (None, None)
                }
            } else {
                (None, None)
            };

            if let Some((dest_dir, ref dest_id, _)) = dest_to_remove {
                if let Some(orset) = cache.get(&dest_dir) {
                    let mut orset = orset.write().unwrap();
                    orset.remove(dest_id);
                }
            }

            if old_dir == new_dir {
                // 同目录 rename：使用 rename_entry 原子操作，生成单个 DeltaOp::Rename
                let old_entry_id = old_entry.id.clone();
                let new_id = if let Some(orset) = cache.get(&old_dir) {
                    let mut orset = orset.write().unwrap();
                    orset.rename_entry(&old_entry_id, new_name, self.client_id)
                } else {
                    None
                };

                let new_id = new_id.ok_or_else(|| {
                    FsError::NotFound(format!("rename: {} not found in dir {}", old_name, old_dir))
                })?;
                let dest_inode_to_remove = dest_to_remove.map(|(_, _, ino)| ino);
                (
                    overwritten_inode,
                    old_entry.inode,
                    new_id,
                    dest_inode_to_remove,
                )
            } else {
                // 同分片不同目录 rename：remove + add
                let old_entry_id = old_entry.id.clone();
                if let Some(orset) = cache.get(&old_dir) {
                    let mut orset = orset.write().unwrap();
                    orset.remove(&old_entry_id);
                }

                let seq = self.next_seq();
                let new_id = EntryId::new(new_name, self.client_id, seq);
                let mut new_entry = old_entry.clone();
                new_entry.id = new_id;
                new_entry.parent_ino = new_dir;
                new_entry.mtime = now_unix();

                let new_orset_arc = if let Some(orset) = cache.get(&new_dir) {
                    orset.clone()
                } else {
                    let orset = Arc::new(RwLock::new(DirORSet::new(new_dir)));
                    cache.insert(new_dir, orset.clone());
                    orset
                };
                {
                    let mut orset = new_orset_arc.write().unwrap();
                    orset.add(new_entry.clone());
                }

                if new_entry.file_type == FileType::Directory {
                    if let Some(child_orset) = cache.get(&new_entry.inode) {
                        let mut orset = child_orset.write().unwrap();
                        orset.dir_ino = new_entry.inode;
                    }
                }

                let dest_inode_to_remove = dest_to_remove.map(|(_, _, ino)| ino);
                (
                    overwritten_inode,
                    new_entry.inode,
                    new_entry.id.clone(),
                    dest_inode_to_remove,
                )
            }
        };

        // 现在可以安全地更新 inode_index 和 inode_paths（不持有 dir_cache 锁）
        if let Some(ino) = dest_inode_to_remove {
            {
                let mut index = self.inode_index.write().unwrap();
                index.remove(&ino);
            }
            {
                let mut paths = self.inode_paths.write().unwrap();
                paths.remove(&ino);
            }
            self.release_inode(ino);
        }

        {
            let mut index = self.inode_index.write().unwrap();
            index.insert(inode_to_update, (new_dir, new_entry_id_for_index));
        }
        {
            let mut paths = self.inode_paths.write().unwrap();
            paths.insert(inode_to_update, new_path.clone());
        }

        self.add_change(ChangeOp::Rename(
            old_dir,
            old_name.to_string(),
            new_dir,
            new_name.to_string(),
        ));

        Ok(overwritten_inode)
    }

    /// 跨分片 rename：使用创建再删除的方式
    /// 先在新目录创建条目，然后从旧目录删除原条目
    /// 如果创建成功但删除失败，旧条目会保留（正确性优先）
    fn rename_cross_shard(
        &self,
        old_dir: u64,
        old_name: &str,
        new_dir: u64,
        new_name: &str,
        new_path: &str,
    ) -> Result<Option<u64>, FsError> {
        // 先获取旧条目信息（不持有任何锁）
        let old_entry = {
            let orset_arc = self.dir_cache.get(old_dir).ok_or_else(|| {
                FsError::NotFound(format!("rename: old dir {} not found", old_dir))
            })?;
            let orset = orset_arc.read().unwrap();
            self.projection
                .project_lookup(&orset, old_name)
                .ok_or_else(|| {
                    FsError::NotFound(format!("rename: {} not found in dir {}", old_name, old_dir))
                })?
        };

        let old_inode = old_entry.inode;
        let old_entry_id = old_entry.id.clone();

        // 检查目标是否已存在（先获取新目录锁）
        let (overwritten_inode, dest_to_remove) = {
            let orset_arc = self.dir_cache.get(new_dir);
            if let Some(orset_arc) = orset_arc {
                let orset = orset_arc.read().unwrap();
                if let Some(dest) = self.projection.project_lookup(&orset, new_name) {
                    if dest.inode != old_inode {
                        if dest.file_type == FileType::Directory {
                            return Err(FsError::IsDirectory(format!(
                                "rename: cannot overwrite directory {}",
                                new_name
                            )));
                        }
                        (
                            Some(dest.inode),
                            Some((new_dir, dest.id.clone(), dest.inode)),
                        )
                    } else {
                        (None, None)
                    }
                } else {
                    (None, None)
                }
            } else {
                (None, None)
            }
        };

        // 第一步：在新目录创建新条目
        let seq = self.next_seq();
        let new_id = EntryId::new(new_name, self.client_id, seq);
        let mut new_entry = old_entry.clone();
        new_entry.id = new_id;
        new_entry.parent_ino = new_dir;
        new_entry.mtime = now_unix();

        {
            let orset_arc = self.dir_cache.ensure_dir_cache(new_dir);
            let mut orset = orset_arc.write().unwrap();
            orset.add(new_entry.clone());
        }

        // 如果目标存在，从新目录移除旧目标
        if let Some((dest_dir, ref dest_id, _)) = dest_to_remove {
            let orset_arc = self.dir_cache.get(dest_dir);
            if let Some(orset_arc) = orset_arc {
                let mut orset = orset_arc.write().unwrap();
                orset.remove(dest_id);
            }
        }

        // 如果是目录，在新分片创建 OR-Set
        if new_entry.file_type == FileType::Directory {
            let new_dir_orset = Arc::new(RwLock::new(DirORSet::new(new_entry.inode)));
            self.dir_cache.insert(new_entry.inode, new_dir_orset);
        }

        // 第二步：从旧目录删除原条目（best-effort）
        let delete_result = {
            let orset_arc = self.dir_cache.get(old_dir);
            if let Some(orset_arc) = orset_arc {
                let mut orset = orset_arc.write().unwrap();
                orset.remove(&old_entry_id);
                Ok(())
            } else {
                Err(FsError::NotFound(format!(
                    "rename: old dir {} not found during delete",
                    old_dir
                )))
            }
        };

        // 如果删除失败，记录警告但不影响 rename 成功（旧条目会保留，正确性没问题）
        if let Err(e) = delete_result {
            warn!("rename_cross_shard: failed to remove old entry: {}", e);
        }

        // 更新索引（在所有锁释放后）
        if let Some(ino) = dest_to_remove.map(|(_, _, ino)| ino) {
            let mut index = self.inode_index.write().unwrap();
            index.remove(&ino);
            let mut paths = self.inode_paths.write().unwrap();
            paths.remove(&ino);
            self.release_inode(ino);
        }

        {
            let mut index = self.inode_index.write().unwrap();
            index.insert(old_inode, (new_dir, new_entry.id.clone()));
        }
        {
            let mut paths = self.inode_paths.write().unwrap();
            paths.insert(old_inode, new_path.to_string());
        }

        self.add_change(ChangeOp::Rename(
            old_dir,
            old_name.to_string(),
            new_dir,
            new_name.to_string(),
        ));

        Ok(overwritten_inode)
    }

    /// 修改属性（setattr）
    pub fn setattr(
        &self,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        mtime: Option<u64>,
    ) -> Result<DirEntry, FsError> {
        let (dir_ino, entry_id) = {
            let index = self.inode_index.read().unwrap();
            index
                .get(&ino)
                .cloned()
                .ok_or_else(|| FsError::NotFound(format!("setattr: inode {} not found", ino)))?
        };

        let orset_arc = self.ensure_dir_cache(dir_ino);
        let mut orset = orset_arc.write().unwrap();
        let entry = orset
            .entries
            .get_mut(&entry_id)
            .ok_or_else(|| FsError::NotFound(format!("setattr: entry {:?} not found", entry_id)))?;

        if let Some(m) = mode {
            entry.mode = m;
        }
        if let Some(u) = uid {
            entry.uid = u;
        }
        if let Some(g) = gid {
            entry.gid = g;
        }
        if let Some(s) = size {
            entry.size = s;
        }
        if let Some(t) = mtime {
            entry.mtime = t;
        }
        entry.ctime = now_unix();
        let updated = entry.clone();
        drop(orset);

        self.add_change(ChangeOp::SetAttr(updated.clone()));

        Ok(updated)
    }

    /// 更新 entry 的 chunks、size 和 extended 字段（flush_dirty_chunks 用）
    ///
    /// CRDT：本地 OR-Set 即更新成功，通过 ChangeOp::SetAttr 触发异步 push_delta。
    /// 不再调用 Master/Filer 的同步元数据接口。
    pub fn update_entry_chunks(
        &self,
        ino: u64,
        chunks: Vec<crate::orset::CachedFileChunk>,
        size: u64,
        extended: HashMap<String, Vec<u8>>,
    ) {
        let (dir_ino, entry_id) = {
            let index = self.inode_index.read().unwrap();
            match index.get(&ino).cloned() {
                Some(info) => info,
                None => {
                    warn!(
                        "update_entry_chunks: inode {} not found in inode_index",
                        ino
                    );
                    return;
                }
            }
        };

        let orset_arc = self.ensure_dir_cache(dir_ino);
        let mut orset = orset_arc.write().unwrap();
        let entry = match orset.entries.get_mut(&entry_id) {
            Some(e) => e,
            None => {
                warn!(
                    "update_entry_chunks: entry {:?} not found in OR-Set",
                    entry_id
                );
                return;
            }
        };

        entry.chunks = chunks;
        entry.size = size;
        entry.extended = extended;
        entry.ctime = now_unix();
        let updated = entry.clone();
        drop(orset);

        self.add_change(ChangeOp::SetAttr(updated));

        debug!(
            "update_entry_chunks: inode={} updated locally, push_delta queued",
            ino
        );
    }

    // ==================== 内部辅助 ====================

    /// 本地 lookup（不触发 Master 回退）
    pub fn lookup_local(&self, dir_ino: u64, name: &str) -> Option<DirEntry> {
        let orset_arc = self.dir_cache.get(dir_ino)?;
        let orset = orset_arc.read().unwrap();
        self.projection.project_lookup(&orset, name)
    }

    /// 确保目录缓存存在，返回 OR-Set 的 Arc
    fn ensure_dir_cache(&self, dir_ino: u64) -> Arc<RwLock<DirORSet>> {
        self.dir_cache.ensure_dir_cache(dir_ino)
    }

    /// 应用条目到本地 OR-Set
    fn apply_to_local_orset(&self, dir_ino: u64, entry: DirEntry) -> Result<(), FsError> {
        let inode = entry.inode;
        let entry_id = entry.id.clone();
        let entry_name = entry_id.name.clone();

        let orset_arc = self.ensure_dir_cache(dir_ino);
        {
            let mut orset = orset_arc.write().unwrap();
            orset.add(entry);
        }

        // 更新 inode 反向索引
        self.inode_index
            .write()
            .unwrap()
            .insert(inode, (dir_ino, entry_id));

        // 更新路径映射（使用预获取的 entry_name，避免再次获取 inode_index 锁）
        self.update_path_for_inode(inode, dir_ino, &entry_name);

        // 初始化 inode 状态（引用计数初始为1）
        self.inode_state
            .write()
            .unwrap()
            .insert(inode, InodeState::new(0));

        Ok(())
    }

    /// 失效本地缓存条目（用于接收远程删除通知时）
    pub fn invalidate_local_cache_entry(&self, parent_ino: u64, name: &str) {
        if let Err(e) = self
            .invalidation_sender
            .try_send(InvalidationRequest::InvalidateEntry(
                parent_ino,
                name.to_string(),
            ))
        {
            warn!("Failed to send invalidation request: {}", e);
        }
    }

    /// 非阻塞方式清理失效的 inode 条目
    fn try_cleanup_stale_inode(&self, ino: u64) -> bool {
        let mut inode_index = self.inode_index.try_write();
        if inode_index.is_err() {
            return false;
        }
        inode_index.as_mut().unwrap().remove(&ino);

        let mut inode_paths = self.inode_paths.try_write();
        if inode_paths.is_err() {
            return false;
        }
        inode_paths.as_mut().unwrap().remove(&ino);

        debug!("try_cleanup_stale_inode: removed inode {}", ino);
        true
    }

    /// 非阻塞方式失效缓存（用于订阅线程，避免死锁）
    pub fn try_invalidate_local_cache_entry(&self, path: &str) -> bool {
        if let Err(e) = self
            .invalidation_sender
            .try_send(InvalidationRequest::InvalidatePath(path.to_string()))
        {
            warn!("Failed to send invalidation request: {}", e);
            false
        } else {
            true
        }
    }

    /// 更新 inode 的路径映射
    fn update_path_for_inode(&self, ino: u64, parent_ino: u64, name: &str) {
        let parent_path = self.get_path(parent_ino).unwrap_or_else(|| "/".to_string());
        let full_path = if parent_path == "/" {
            format!("/{}", name)
        } else {
            format!("{}/{}", parent_path, name)
        };
        self.inode_paths.write().unwrap().insert(ino, full_path);
    }

    /// 生成下一个 seq 号
    fn next_seq(&self) -> u64 {
        self.seq_counter.fetch_add(1, Ordering::SeqCst)
    }

    /// 构造根目录条目
    fn make_root_entry(&self) -> DirEntry {
        let now = now_unix();
        DirEntry {
            id: EntryId::new("", MASTER_CLIENT_ID, 0),
            inode: ROOT_INO,
            generation: 0,
            file_type: FileType::Directory,
            mode: 0o755 | libc::S_IFDIR,
            uid: 0,
            gid: 0,
            size: 4096,
            mtime: now,
            atime: now,
            ctime: now,
            nlink: 2,
            rdev: 0,
            parent_ino: ROOT_INO,
            chunks: vec![],
            symlink_target: None,
            extended: std::collections::HashMap::new(),
        }
    }

    // ==================== 变更缓存（Change Cache） ====================

    /// 添加变更到变更缓存（无锁化设计）
    /// 使用 crossbeam channel 替代 Mutex，完全消除锁竞争
    fn add_change(&self, op: ChangeOp) {
        self.increment_write_counter();
        if let Err(e) = self.change_sender.try_send(op) {
            warn!("Failed to send change to channel: {}", e);
        }
    }

    fn start_change_cache_flusher(&self, change_receiver: Receiver<ChangeOp>) {
        let client = match &self.client {
            Some(c) => c.clone(),
            None => return,
        };
        let client_id_str = self.client_id_str.clone();
        let inode_paths = self.inode_paths.clone();
        let use_filer = client.has_filer();

        thread::spawn(move || {
            info!("Change cache flusher thread started, filer={}", use_filer);

            loop {
                thread::sleep(Duration::from_millis(10));

                let mut changes = Vec::with_capacity(CHANGE_CACHE_BATCH_SIZE);
                while let Ok(change) = change_receiver.try_recv() {
                    changes.push(change);
                    if changes.len() >= CHANGE_CACHE_BATCH_SIZE {
                        break;
                    }
                }

                if changes.is_empty() {
                    continue;
                }

                info!("Flushing {} pending changes", changes.len());

                // Filer 模式 & Master 模式：统一使用 push_delta 批量同步
                // ChangeOp 转换为 Master DeltaOp，由 client.push_delta 自动转换为 Filer DeltaOp
                let mut deltas = Vec::with_capacity(changes.len());
                for change in changes {
                    let delta = match change {
                        ChangeOp::Create(entry) => {
                            let orset_entry = dir_entry_to_dir_entry_orset(&entry);
                            DeltaOp {
                                op: Some(powerfs_master::proto::powerfs::delta_op::Op::Add(
                                    orset_entry,
                                )),
                                vclock: None,
                            }
                        }
                        ChangeOp::Delete(ino, _) => {
                            let entry_info = {
                                let paths = inode_paths.read().unwrap();
                                paths.get(&ino).cloned()
                            };
                            if let Some(path) = entry_info {
                                let parts: Vec<&str> = path.rsplit('/').collect();
                                let name = parts.first().unwrap_or(&"").to_string();
                                let entry_id = MasterEntryId {
                                    name,
                                    client_id: 0,
                                    seq: 0,
                                };
                                DeltaOp {
                                    op: Some(powerfs_master::proto::powerfs::delta_op::Op::Remove(
                                        entry_id,
                                    )),
                                    vclock: None,
                                }
                            } else {
                                continue;
                            }
                        }
                        ChangeOp::Rename(_old_dir, old_name, new_dir, new_name) => {
                            let old_entry_id = MasterEntryId {
                                name: old_name,
                                client_id: 0,
                                seq: 0,
                            };
                            let new_entry = DirEntryOrset {
                                id: Some(MasterEntryId {
                                    name: new_name,
                                    client_id: 0,
                                    seq: 0,
                                }),
                                inode: 0,
                                parent_ino: new_dir,
                                mode: 0,
                                size: 0,
                                mtime: 0,
                                atime: 0,
                                ctime: 0,
                                nlink: 0,
                                symlink_target: String::new(),
                                file_type: 0,
                                uid: 0,
                                gid: 0,
                                rdev: 0,
                            };
                            DeltaOp {
                                op: Some(powerfs_master::proto::powerfs::delta_op::Op::Rename(
                                    RenameOp {
                                        old_id: Some(old_entry_id),
                                        new_entry: Some(new_entry),
                                    },
                                )),
                                vclock: None,
                            }
                        }
                        ChangeOp::SetAttr(entry) => DeltaOp {
                            op: Some(powerfs_master::proto::powerfs::delta_op::Op::SetAttr(
                                SetAttrOp {
                                    inode: entry.inode,
                                    size: entry.size,
                                    mtime: entry.mtime,
                                    mode: entry.mode,
                                    uid: entry.uid,
                                    gid: entry.gid,
                                    nlink: entry.nlink,
                                    chunks: entry
                                        .chunks
                                        .iter()
                                        .map(|c| powerfs_master::proto::powerfs::FileChunk {
                                            offset: c.offset,
                                            size: c.size,
                                            mtime: c.mtime,
                                            fid: c.fid.clone(),
                                            cookie: c.cookie,
                                            crc32: c.crc32,
                                        })
                                        .collect(),
                                    extended: entry.extended.clone(),
                                },
                            )),
                            vclock: None,
                        },
                    };
                    deltas.push(delta);
                }

                if !deltas.is_empty() {
                    let vclock = MasterVectorClock { entries: vec![] };
                    match client.push_delta(&client_id_str, &deltas, &vclock) {
                        Ok(_) => debug!("push_delta succeeded: {} changes", deltas.len()),
                        Err(e) => warn!("push_delta failed: {}", e),
                    }
                }
            }
        });
    }

    fn start_invalidation_processor(&self, receiver: Receiver<InvalidationRequest>) {
        let dir_cache = self.dir_cache.clone();
        let inode_index = self.inode_index.clone();
        let inode_paths = self.inode_paths.clone();

        thread::spawn(move || {
            info!("Invalidation processor thread started");

            loop {
                match receiver.recv_timeout(Duration::from_millis(500)) {
                    Ok(InvalidationRequest::InvalidateEntry(parent_ino, name)) => {
                        Self::process_invalidate_entry(
                            parent_ino,
                            &name,
                            &dir_cache,
                            &inode_index,
                            &inode_paths,
                        );
                    }
                    Ok(InvalidationRequest::InvalidatePath(path)) => {
                        Self::process_invalidate_path(
                            &path,
                            &dir_cache,
                            &inode_index,
                            &inode_paths,
                        );
                    }
                    Err(crossbeam::channel::RecvTimeoutError::Timeout) => {
                        continue;
                    }
                    Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
                        info!("Invalidation processor channel disconnected, exiting");
                        break;
                    }
                }
            }
        });
    }

    fn process_invalidate_entry(
        parent_ino: u64,
        name: &str,
        dir_cache: &ShardedDirCache,
        inode_index: &Arc<RwLock<HashMap<u64, (u64, EntryId)>>>,
        inode_paths: &Arc<RwLock<HashMap<u64, String>>>,
    ) {
        if let Some(orset_arc) = dir_cache.get(parent_ino) {
            let orset = orset_arc.read().unwrap();
            let entries = orset.get_by_name(name);
            if entries.is_empty() {
                return;
            }
        }

        if let Some(orset_arc) = dir_cache.get(parent_ino) {
            let mut orset = orset_arc.write().unwrap();
            let entries: Vec<EntryId> = orset
                .get_by_name(name)
                .iter()
                .map(|e| e.id.clone())
                .collect();
            for entry_id in entries {
                orset.remove(&entry_id);
            }
        }

        let mut inode_index_guard = inode_index.write().unwrap();
        let mut inode_paths_guard = inode_paths.write().unwrap();

        let mut to_remove = Vec::new();
        for (&inode, (_, entry_id)) in inode_index_guard.iter() {
            if entry_id.name == name {
                to_remove.push(inode);
            }
        }

        for inode in to_remove {
            inode_index_guard.remove(&inode);
            inode_paths_guard.remove(&inode);
        }
    }

    fn process_invalidate_path(
        path: &str,
        dir_cache: &ShardedDirCache,
        inode_index: &Arc<RwLock<HashMap<u64, (u64, EntryId)>>>,
        inode_paths: &Arc<RwLock<HashMap<u64, String>>>,
    ) {
        let mut inode_paths_guard = inode_paths.write().unwrap();
        let inode = inode_paths_guard
            .iter()
            .find(|(_, p)| **p == path)
            .map(|(&ino, _)| ino);

        if let Some(inode) = inode {
            inode_paths_guard.remove(&inode);
            drop(inode_paths_guard);

            let parent_ino;
            let entry_id;
            {
                let mut inode_index_guard = inode_index.write().unwrap();
                if let Some(&(p, ref eid)) = inode_index_guard.get(&inode) {
                    parent_ino = p;
                    entry_id = eid.clone();
                } else {
                    return;
                }
                inode_index_guard.remove(&inode);
            }

            if let Some(orset_arc) = dir_cache.get(parent_ino) {
                let mut orset = orset_arc.write().unwrap();
                orset.remove(&entry_id);
            }
            dir_cache.remove(inode);
        }
    }

    // ==================== Master 同步（best-effort） ====================

    /// 无死锁版本的目录拉取
    ///
    /// 问题分析：list_dir 调用 ensure_dir_cache 获取 dir_cache 读锁，然后获取 orset_arc 锁
    /// 如果在持有 orset_arc 锁的情况下再调用 fetch_dir_from_master，而 fetch_dir_from_master
    /// 内部又调用 ensure_dir_cache（可能需要升级到写锁），就会导致死锁。
    ///
    /// 解决方案：先释放 orset_arc 锁，再调用此函数，此函数内部不假设任何锁已被持有。
    fn fetch_dir_from_master_without_deadlock(&self, dir_ino: u64) -> Result<(), FsError> {
        let client = match &self.client {
            Some(c) => c.clone(),
            None => return Ok(()),
        };

        // 获取父路径（需要短暂持有锁）
        let parent_path = {
            let inode_paths = self.inode_paths.read().unwrap();
            inode_paths
                .get(&dir_ino)
                .cloned()
                .unwrap_or_else(|| "/".to_string())
        };

        // 优先使用 Filer，回退到 Master
        let use_filer = client.has_filer();

        // 先获取目录内容（不持有任何锁）
        let (dir_entries, path_updates, index_updates) = if use_filer {
            let filer_entries = client
                .filer_list_entries(dir_ino, 10000, "")
                .map_err(|e| FsError::MasterError(format!("filer_list_entries: {}", e)))?;

            let num_entries = filer_entries.len();
            if num_entries == 0 {
                return Ok(());
            }

            let mut dir_entries = Vec::with_capacity(num_entries);
            let mut index_updates = Vec::with_capacity(num_entries);
            let mut path_updates = Vec::with_capacity(num_entries);

            for filer_entry in filer_entries {
                let dir_entry = filer_proto_to_dir_entry(&filer_entry, dir_ino);
                let ino = dir_entry.inode;
                let entry_id = dir_entry.id.clone();

                dir_entries.push(dir_entry);
                index_updates.push((ino, (dir_ino, entry_id)));

                let child_path = if parent_path == "/" {
                    format!("/{}", filer_entry.name)
                } else {
                    format!("{}/{}", parent_path, filer_entry.name)
                };
                path_updates.push((ino, child_path));
            }

            (dir_entries, path_updates, index_updates)
        } else {
            let entries = client
                .list_entries(dir_ino, 10000, "")
                .map_err(|e| FsError::MasterError(format!("list_entries: {}", e)))?;

            let num_entries = entries.len();
            if num_entries == 0 {
                return Ok(());
            }

            let mut dir_entries = Vec::with_capacity(num_entries);
            let mut index_updates = Vec::with_capacity(num_entries);
            let mut path_updates = Vec::with_capacity(num_entries);

            for proto_entry in entries {
                let dir_entry = proto_to_dir_entry(&proto_entry, dir_ino);
                let ino = dir_entry.inode;
                let entry_id = dir_entry.id.clone();

                dir_entries.push(dir_entry);
                index_updates.push((ino, (dir_ino, entry_id)));

                let child_path = if parent_path == "/" {
                    format!("/{}", proto_entry.name)
                } else {
                    format!("{}/{}", parent_path, proto_entry.name)
                };
                path_updates.push((ino, child_path));
            }

            (dir_entries, path_updates, index_updates)
        };

        let num_entries = dir_entries.len();

        // 【关键修复】分开更新，避免同时持有 orset_arc 和 inode_index/inode_paths 锁
        // 死锁场景：
        // - 线程A（rmdir）: 持有 orset_arc 读锁 → 等待 inode_index 写锁
        // - 线程B（此函数）: 持有 inode_index 写锁 → 等待 orset_arc 写锁

        // 第一步：更新 OR-Set（只持有 dir_cache 和 orset_arc 锁）
        let orset_arc = self.dir_cache.ensure_dir_cache(dir_ino);

        {
            let mut orset = orset_arc.write().unwrap();
            for dir_entry in dir_entries {
                orset.add(dir_entry);
            }
        }

        // 第二步：更新 inode_index 和 inode_paths（只持有这两个锁）
        {
            let mut inode_index = self.inode_index.write().unwrap();
            for (ino, entry) in index_updates {
                inode_index.insert(ino, entry);
            }
        }

        {
            let mut inode_paths = self.inode_paths.write().unwrap();
            for (ino, path) in path_updates {
                inode_paths.insert(ino, path);
            }
        }

        debug!(
            "fetch_dir_from_master_without_deadlock: dir_ino={}, entries={}, filer={}",
            dir_ino, num_entries, use_filer
        );

        Ok(())
    }

    /// 从 Master/Filer 拉取目录内容，填充本地 OR-Set
    fn fetch_dir_from_master(&self, dir_ino: u64) -> Result<(), FsError> {
        let client = match &self.client {
            Some(c) => c.clone(),
            None => return Ok(()),
        };

        // 【关键修复】先获取父路径（不持有其他锁），避免违反锁顺序
        let parent_path = {
            let paths = self.inode_paths.read().unwrap();
            paths
                .get(&dir_ino)
                .cloned()
                .unwrap_or_else(|| "/".to_string())
        };

        let use_filer = client.has_filer();

        let (dir_entries, index_updates, path_updates) = if use_filer {
            let filer_entries = client
                .filer_list_entries(dir_ino, 10000, "")
                .map_err(|e| FsError::MasterError(format!("filer_list_entries: {}", e)))?;

            let num_entries = filer_entries.len();
            let mut dir_entries = Vec::with_capacity(num_entries);
            let mut index_updates = Vec::with_capacity(num_entries);
            let mut path_updates = Vec::with_capacity(num_entries);

            for filer_entry in filer_entries {
                let dir_entry = filer_proto_to_dir_entry(&filer_entry, dir_ino);
                let ino = dir_entry.inode;
                let entry_id = dir_entry.id.clone();

                dir_entries.push(dir_entry);
                index_updates.push((ino, (dir_ino, entry_id)));

                let child_path = if parent_path == "/" {
                    format!("/{}", filer_entry.name)
                } else {
                    format!("{}/{}", parent_path, filer_entry.name)
                };
                path_updates.push((ino, child_path));
            }

            (dir_entries, index_updates, path_updates)
        } else {
            let entries = client
                .list_entries(dir_ino, 10000, "")
                .map_err(|e| FsError::MasterError(format!("list_entries: {}", e)))?;

            let num_entries = entries.len();
            let mut dir_entries = Vec::with_capacity(num_entries);
            let mut index_updates = Vec::with_capacity(num_entries);
            let mut path_updates = Vec::with_capacity(num_entries);

            for proto_entry in entries {
                let dir_entry = proto_to_dir_entry(&proto_entry, dir_ino);
                let ino = dir_entry.inode;
                let entry_id = dir_entry.id.clone();

                dir_entries.push(dir_entry);
                index_updates.push((ino, (dir_ino, entry_id)));

                let child_path = if parent_path == "/" {
                    format!("/{}", proto_entry.name)
                } else {
                    format!("{}/{}", parent_path, proto_entry.name)
                };
                path_updates.push((ino, child_path));
            }

            (dir_entries, index_updates, path_updates)
        };

        let num_entries = dir_entries.len();

        // 按正确锁顺序更新：dir_cache → orset_arc → inode_index → inode_paths
        let orset_arc = self.ensure_dir_cache(dir_ino);

        {
            let mut orset = orset_arc.write().unwrap();
            for dir_entry in dir_entries {
                orset.add(dir_entry);
            }
        }

        {
            let mut inode_index = self.inode_index.write().unwrap();
            for (ino, entry) in index_updates {
                inode_index.insert(ino, entry);
            }
        }

        {
            let mut inode_paths = self.inode_paths.write().unwrap();
            for (ino, path) in path_updates {
                inode_paths.insert(ino, path);
            }
        }

        debug!(
            "fetch_dir_from_master: dir_ino={}, entries={}, filer={}",
            dir_ino, num_entries, use_filer
        );
        Ok(())
    }

    /// 从 Master/Filer 按 inode 拉取单个条目
    fn fetch_entry_by_inode_from_master(&self, ino: u64) -> Result<Option<DirEntry>, FsError> {
        let client = match &self.client {
            Some(c) => c.clone(),
            None => return Ok(None),
        };

        if client.has_filer() {
            let result = client
                .filer_get_entry_by_inode(ino)
                .map_err(|e| FsError::MasterError(format!("filer_get_entry_by_inode: {}", e)))?;

            match result {
                Some((filer_entry, path)) => {
                    let parent_ino = self.infer_parent_ino_from_path(&path);
                    let dir_entry = filer_proto_to_dir_entry(&filer_entry, parent_ino);
                    let entry_id = dir_entry.id.clone();

                    let orset_arc = self.ensure_dir_cache(parent_ino);
                    {
                        let mut orset = orset_arc.write().unwrap();
                        orset.add(dir_entry.clone());
                    }

                    {
                        let mut inode_index = self.inode_index.write().unwrap();
                        inode_index.insert(ino, (parent_ino, entry_id));
                    }

                    {
                        let mut inode_paths = self.inode_paths.write().unwrap();
                        inode_paths.insert(ino, path);
                    }

                    Ok(Some(dir_entry))
                }
                None => Ok(None),
            }
        } else {
            let result = client
                .get_entry_by_inode(ino)
                .map_err(|e| FsError::MasterError(format!("get_entry_by_inode: {}", e)))?;

            match result {
                Some((proto_entry, path)) => {
                    let parent_ino = self.infer_parent_ino_from_path(&path);
                    let dir_entry = proto_to_dir_entry(&proto_entry, parent_ino);
                    let entry_id = dir_entry.id.clone();

                    let orset_arc = self.ensure_dir_cache(parent_ino);
                    {
                        let mut orset = orset_arc.write().unwrap();
                        orset.add(dir_entry.clone());
                    }

                    {
                        let mut inode_index = self.inode_index.write().unwrap();
                        inode_index.insert(ino, (parent_ino, entry_id));
                    }

                    {
                        let mut inode_paths = self.inode_paths.write().unwrap();
                        inode_paths.insert(ino, path);
                    }

                    Ok(Some(dir_entry))
                }
                None => Ok(None),
            }
        }
    }

    /// 从路径推断 parent inode
    fn infer_parent_ino_from_path(&self, path: &str) -> u64 {
        let parent_path = if let Some(last_slash) = path.rfind('/') {
            if last_slash == 0 {
                "/"
            } else {
                &path[..last_slash]
            }
        } else {
            "/"
        };

        // 查找 parent_path 对应的 inode
        let paths = self.inode_paths.read().unwrap();
        for (&ino, p) in paths.iter() {
            if p == parent_path {
                return ino;
            }
        }
        // 默认根目录
        ROOT_INO
    }

    // ==================== Delta Sync ====================

    pub fn start_delta_sync(&self) {
        info!("Starting Delta Sync for client_id={}", self.client_id_str);
        let client = match &self.client {
            Some(c) => c.clone(),
            None => {
                warn!("No client available, Delta Sync cannot start");
                return;
            }
        };
        let client_id_str = self.client_id_str.clone();
        let dir_cache = self.dir_cache.clone();
        let inode_index = self.inode_index.clone();
        let inode_paths = self.inode_paths.clone();
        let inode_state = self.inode_state.clone();
        let client_vclock = self.client_vclock.clone();
        let write_counter = self.write_counter.clone();
        let invalidation_sender = self.invalidation_sender.clone();

        tokio::spawn(async move {
            info!("Performing initial full sync...");
            let initial_vclock = client_vclock
                .read()
                .expect("client_vclock lock poisoned")
                .clone();
            let initial_proto_vclock = vec_to_proto_vclock(&initial_vclock);

            match client
                .pull_delta_async(&client_id_str, &initial_proto_vclock)
                .await
            {
                Ok(response) => {
                    info!("Initial sync received {} deltas", response.deltas.len());
                    // Filer's PullDeltaResponse uses Filer DeltaOp type
                    // For now, deltas are empty (placeholder implementation in meta_shard_manager)
                    // TODO: Implement DeltaOp conversion from Filer format to Master format
                    if let Some(new_vclock) = response.server_vclock {
                        let mut vclock_guard =
                            client_vclock.write().expect("client_vclock lock poisoned");
                        *vclock_guard = filer_vclock_to_vec(&new_vclock);
                    }
                }
                Err(e) => {
                    warn!("Initial sync failed: {}", e);
                }
            }

            let mut backoff = Duration::from_secs(1);
            let max_backoff = Duration::from_secs(60);
            let mut iteration = 0;

            loop {
                let writes_in_period = write_counter.swap(0, std::sync::atomic::Ordering::Relaxed);
                let interval = calculate_delta_sync_interval(writes_in_period);

                tokio::time::sleep(interval).await;
                iteration += 1;
                info!(
                    "Delta Sync iteration {} starting, interval={:?}, writes_in_period={}",
                    iteration, interval, writes_in_period
                );

                match do_pull_and_apply_deltas(
                    &client,
                    &client_id_str,
                    &dir_cache,
                    &inode_index,
                    &inode_paths,
                    &inode_state,
                    &client_vclock,
                    &invalidation_sender,
                )
                .await
                {
                    Ok(_) => {
                        backoff = Duration::from_secs(1);
                    }
                    Err(e) => {
                        warn!("Delta sync failed: {}, backing off {:?}", e, backoff);
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(max_backoff);
                    }
                }
            }
        });
    }

    fn increment_write_counter(&self) {
        self.write_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn pull_and_apply_deltas(&self) -> Result<(), FsError> {
        let client = match &self.client {
            Some(c) => c.clone(),
            None => return Ok(()),
        };

        let vclock = self
            .client_vclock
            .read()
            .expect("client_vclock lock poisoned")
            .clone();
        let proto_vclock = vec_to_proto_vclock(&vclock);

        let response = client
            .pull_delta(&self.client_id_str, &proto_vclock)
            .map_err(|e| FsError::MasterError(format!("pull_delta: {}", e)))?;

        if !response.deltas.is_empty() {
            info!("pull_delta received {} deltas", response.deltas.len());

            // Filer's PullDeltaResponse uses Filer DeltaOp type
            // For now, deltas are empty (placeholder implementation in meta_shard_manager)
            // TODO: Implement DeltaOp conversion from Filer format to Master format
        }

        if let Some(server_vclock) = response.server_vclock {
            let new_vclock = filer_vclock_to_vec(&server_vclock);
            let mut client_vclock = self
                .client_vclock
                .write()
                .expect("client_vclock lock poisoned");
            *client_vclock = new_vclock;
        }

        Ok(())
    }

    #[allow(dead_code)]
    fn apply_delta(&self, delta: &powerfs_master::proto::powerfs::DeltaOp) {
        match &delta.op {
            Some(powerfs_master::proto::powerfs::delta_op::Op::Add(entry)) => {
                if let Some(id) = &entry.id {
                    let dir_entry = proto_dir_entry_to_local(entry);
                    let dir_ino = entry.parent_ino;

                    let orset_arc = self.ensure_dir_cache(dir_ino);
                    let mut orset = orset_arc.write().expect("orset lock poisoned");
                    orset.add(dir_entry.clone());

                    self.inode_index
                        .write()
                        .expect("inode_index lock poisoned")
                        .insert(
                            entry.inode,
                            (dir_ino, EntryId::new(id.name.clone(), id.client_id, id.seq)),
                        );

                    let parent_path = self.get_path(dir_ino).unwrap_or_else(|| "/".to_string());
                    let child_path = if parent_path == "/" {
                        format!("/{}", id.name)
                    } else {
                        format!("{}/{}", parent_path, id.name)
                    };
                    self.inode_paths
                        .write()
                        .expect("inode_paths lock poisoned")
                        .insert(entry.inode, child_path);

                    // 初始化 inode 状态
                    self.inode_state
                        .write()
                        .expect("inode_state lock poisoned")
                        .insert(entry.inode, InodeState::new(0));
                }
            }
            Some(powerfs_master::proto::powerfs::delta_op::Op::Remove(id)) => {
                let entry_id = EntryId::new(id.name.clone(), id.client_id, id.seq);

                let mut index = self.inode_index.write().expect("inode_index lock poisoned");
                let inode_to_remove: Option<u64> = index
                    .iter()
                    .find(|(_, (_, eid))| eid == &entry_id)
                    .map(|(&ino, _)| ino);

                if let Some(ino) = inode_to_remove {
                    if let Some((dir_ino, eid)) = index.remove(&ino) {
                        let orset_arc = self.ensure_dir_cache(dir_ino);
                        let mut orset = orset_arc.write().expect("orset lock poisoned");
                        orset.remove(&eid);

                        self.inode_paths
                            .write()
                            .expect("inode_paths lock poisoned")
                            .remove(&ino);

                        self.inode_state
                            .write()
                            .expect("inode_state lock poisoned")
                            .remove(&ino);
                    }
                }
            }
            Some(powerfs_master::proto::powerfs::delta_op::Op::Rename(op)) => {
                if let (Some(old_id), Some(new_entry)) = (&op.old_id, &op.new_entry) {
                    if let Some(new_id) = &new_entry.id {
                        let old_entry_id =
                            EntryId::new(old_id.name.clone(), old_id.client_id, old_id.seq);

                        let mut index =
                            self.inode_index.write().expect("inode_index lock poisoned");
                        let inode_to_rename: Option<u64> = index
                            .iter()
                            .find(|(_, (_, eid))| eid == &old_entry_id)
                            .map(|(&ino, _)| ino);

                        if let Some(ino) = inode_to_rename {
                            if let Some((old_dir_ino, _)) = index.get(&ino) {
                                let orset_arc_old = self.ensure_dir_cache(*old_dir_ino);
                                let mut orset_old =
                                    orset_arc_old.write().expect("orset lock poisoned");
                                orset_old.remove(&old_entry_id);
                            }

                            let dir_entry = proto_dir_entry_to_local(new_entry);
                            let new_dir_ino = new_entry.parent_ino;

                            let orset_arc_new = self.ensure_dir_cache(new_dir_ino);
                            let mut orset_new = orset_arc_new.write().expect("orset lock poisoned");
                            orset_new.add(dir_entry.clone());

                            index.insert(
                                ino,
                                (
                                    new_dir_ino,
                                    EntryId::new(new_id.name.clone(), new_id.client_id, new_id.seq),
                                ),
                            );

                            let parent_path = self
                                .get_path(new_dir_ino)
                                .unwrap_or_else(|| "/".to_string());
                            let child_path = if parent_path == "/" {
                                format!("/{}", new_id.name)
                            } else {
                                format!("{}/{}", parent_path, new_id.name)
                            };
                            self.inode_paths
                                .write()
                                .expect("inode_paths lock poisoned")
                                .insert(ino, child_path);
                        }
                    }
                }
            }
            Some(powerfs_master::proto::powerfs::delta_op::Op::SetAttr(op)) => {
                let index = self.inode_index.read().expect("inode_index lock poisoned");
                if let Some((dir_ino, entry_id)) = index.get(&op.inode) {
                    let orset_arc = self.ensure_dir_cache(*dir_ino);
                    let mut orset = orset_arc.write().expect("orset lock poisoned");
                    if let Some(entry) = orset.entries.get_mut(entry_id) {
                        if op.mode != 0 {
                            entry.mode = op.mode;
                        }
                        if op.uid != 0 {
                            entry.uid = op.uid;
                        }
                        if op.gid != 0 {
                            entry.gid = op.gid;
                        }
                        if op.size != 0 {
                            entry.size = op.size;
                        }
                        if op.mtime != 0 {
                            entry.mtime = op.mtime;
                        }
                        if op.nlink != 0 {
                            entry.nlink = op.nlink;
                        }
                    }
                }
            }
            None => {}
        }
    }
}

// ==================== 转换函数 ====================

/// proto Entry → DirEntry
///
/// Master 来源的条目使用 client_id=0, seq=0 作为 EntryId。
fn proto_to_dir_entry(proto: &powerfs_master::proto::powerfs::Entry, parent_ino: u64) -> DirEntry {
    let attrs = proto.attributes.as_ref();
    let mode_val = attrs.map(|a| a.mode).unwrap_or(0);
    let ino = attrs.map(|a| a.ino).unwrap_or(0);
    let uid = attrs.map(|a| a.uid).unwrap_or(0);
    let gid = attrs.map(|a| a.gid).unwrap_or(0);
    let nlink = attrs.map(|a| a.nlink).unwrap_or(1);
    let rdev = attrs.map(|a| a.rdev).unwrap_or(0);
    let size = attrs.map(|a| a.size).unwrap_or(0);
    let mtime = attrs.map(|a| a.mtime).unwrap_or(0);
    let atime = attrs.map(|a| a.atime).unwrap_or(0);
    let ctime = attrs.map(|a| a.ctime).unwrap_or(0);

    let file_type = FileType::from_mode(mode_val);
    let chunks: Vec<crate::orset::CachedFileChunk> = proto
        .chunks
        .iter()
        .map(|c| crate::orset::CachedFileChunk {
            offset: c.offset,
            size: c.size,
            mtime: c.mtime,
            fid: c.fid.clone(),
            cookie: c.cookie,
            crc32: c.crc32,
        })
        .collect();

    DirEntry {
        id: EntryId::new(&proto.name, MASTER_CLIENT_ID, 0),
        inode: ino,
        generation: 0,
        file_type,
        mode: mode_val,
        uid,
        gid,
        size,
        mtime,
        atime,
        ctime,
        nlink,
        rdev,
        parent_ino,
        chunks,
        symlink_target: if proto.symlink_target.is_empty() {
            None
        } else {
            Some(proto.symlink_target.clone())
        },
        extended: proto.extended.clone(),
    }
}

/// DirEntry → proto Entry（用于 Master 同步）
pub fn dir_entry_to_proto(
    entry: &DirEntry,
    parent_path: &str,
) -> powerfs_master::proto::powerfs::Entry {
    let chunks: Vec<powerfs_master::proto::powerfs::FileChunk> = entry
        .chunks
        .iter()
        .map(|c| powerfs_master::proto::powerfs::FileChunk {
            offset: c.offset,
            size: c.size,
            mtime: c.mtime,
            fid: c.fid.clone(),
            cookie: c.cookie,
            crc32: c.crc32,
        })
        .collect();

    powerfs_master::proto::powerfs::Entry {
        name: entry.id.name.clone(),
        directory: parent_path.to_string(),
        attributes: Some(powerfs_master::proto::powerfs::FuseAttributes {
            ino: entry.inode,
            mode: entry.mode,
            nlink: entry.nlink,
            uid: entry.uid,
            gid: entry.gid,
            rdev: entry.rdev,
            size: entry.size,
            blksize: 4096,
            blocks: entry.size.div_ceil(512),
            atime: entry.atime,
            mtime: entry.mtime,
            ctime: entry.ctime,
            crtime: entry.ctime,
            perm: 0,
        }),
        chunks,
        hard_link_id: String::new(),
        hard_link_counter: 0,
        extended: std::collections::HashMap::new(),
        content_size: entry.size,
        disk_size: entry.size,
        ttl: String::new(),
        symlink_target: entry.symlink_target.clone().unwrap_or_default(),
        owner: String::new(),
        generation: 0,
    }
}

/// DirEntry → Filer proto Entry（用于 Filer 同步）
pub fn dir_entry_to_filer_proto(
    entry: &DirEntry,
    parent_path: &str,
) -> powerfs_filer::powerfs::Entry {
    use powerfs_filer::powerfs::{
        Entry as FilerEntry, FileChunk as FilerFileChunk, FuseAttributes as FilerFuseAttributes,
    };

    let chunks: Vec<FilerFileChunk> = entry
        .chunks
        .iter()
        .map(|c| FilerFileChunk {
            offset: c.offset,
            size: c.size,
            mtime: c.mtime,
            fid: c.fid.clone(),
            cookie: c.cookie,
            crc32: c.crc32,
        })
        .collect();

    FilerEntry {
        name: entry.id.name.clone(),
        directory: parent_path.to_string(),
        attributes: Some(FilerFuseAttributes {
            ino: entry.inode,
            mode: entry.mode,
            nlink: entry.nlink,
            uid: entry.uid,
            gid: entry.gid,
            rdev: entry.rdev,
            size: entry.size,
            blksize: 4096,
            blocks: entry.size.div_ceil(512),
            atime: entry.atime,
            mtime: entry.mtime,
            ctime: entry.ctime,
            crtime: entry.ctime,
            perm: 0,
        }),
        chunks,
        hard_link_id: String::new(),
        hard_link_counter: 0,
        extended: std::collections::HashMap::new(),
        content_size: entry.size,
        disk_size: entry.size,
        ttl: String::new(),
        symlink_target: entry.symlink_target.clone().unwrap_or_default(),
        owner: String::new(),
        generation: 0,
    }
}

/// Filer proto Entry → DirEntry（用于从 Filer 拉取数据）
fn filer_proto_to_dir_entry(proto: &powerfs_filer::powerfs::Entry, parent_ino: u64) -> DirEntry {
    let attrs = proto.attributes.as_ref();
    let mode_val = attrs.map(|a| a.mode).unwrap_or(0);
    let ino = attrs.map(|a| a.ino).unwrap_or(0);
    let uid = attrs.map(|a| a.uid).unwrap_or(0);
    let gid = attrs.map(|a| a.gid).unwrap_or(0);
    let nlink = attrs.map(|a| a.nlink).unwrap_or(1);
    let rdev = attrs.map(|a| a.rdev).unwrap_or(0);
    let size = attrs.map(|a| a.size).unwrap_or(0);
    let mtime = attrs.map(|a| a.mtime).unwrap_or(0);
    let atime = attrs.map(|a| a.atime).unwrap_or(0);
    let ctime = attrs.map(|a| a.ctime).unwrap_or(0);

    let file_type = FileType::from_mode(mode_val);
    let chunks: Vec<crate::orset::CachedFileChunk> = proto
        .chunks
        .iter()
        .map(|c| crate::orset::CachedFileChunk {
            offset: c.offset,
            size: c.size,
            mtime: c.mtime,
            fid: c.fid.clone(),
            cookie: c.cookie,
            crc32: c.crc32,
        })
        .collect();

    DirEntry {
        id: EntryId::new(&proto.name, MASTER_CLIENT_ID, 0),
        inode: ino,
        generation: proto.generation,
        file_type,
        mode: mode_val,
        uid,
        gid,
        size,
        mtime,
        atime,
        ctime,
        nlink,
        rdev,
        parent_ino,
        chunks,
        symlink_target: if proto.symlink_target.is_empty() {
            None
        } else {
            Some(proto.symlink_target.clone())
        },
        extended: proto.extended.clone(),
    }
}

#[allow(dead_code)]
fn proto_dir_entry_to_local(entry: &powerfs_master::proto::powerfs::DirEntryOrset) -> DirEntry {
    let id = if let Some(id) = &entry.id {
        EntryId::new(id.name.clone(), id.client_id, id.seq)
    } else {
        EntryId::new("unknown", 0, 0)
    };

    let file_type = match entry.file_type {
        0 => FileType::RegularFile,
        1 => FileType::Directory,
        2 => FileType::Symlink,
        3 => FileType::Fifo,
        4 => FileType::CharDevice,
        5 => FileType::BlockDevice,
        6 => FileType::Socket,
        _ => FileType::RegularFile,
    };

    DirEntry {
        id,
        inode: entry.inode,
        generation: 0,
        file_type,
        mode: entry.mode,
        uid: entry.uid,
        gid: entry.gid,
        size: entry.size,
        mtime: entry.mtime,
        atime: entry.atime,
        ctime: entry.ctime,
        nlink: entry.nlink,
        rdev: entry.rdev,
        parent_ino: entry.parent_ino,
        chunks: vec![],
        symlink_target: if entry.symlink_target.is_empty() {
            None
        } else {
            Some(entry.symlink_target.clone())
        },
        extended: std::collections::HashMap::new(),
    }
}

fn dir_entry_to_dir_entry_orset(entry: &DirEntry) -> powerfs_master::proto::powerfs::DirEntryOrset {
    let file_type = match entry.file_type {
        FileType::RegularFile => 0,
        FileType::Directory => 1,
        FileType::Symlink => 2,
        FileType::Fifo => 3,
        FileType::CharDevice => 4,
        FileType::BlockDevice => 5,
        FileType::Socket => 6,
    };

    powerfs_master::proto::powerfs::DirEntryOrset {
        id: Some(powerfs_master::proto::powerfs::EntryId {
            name: entry.id.name.clone(),
            client_id: entry.id.client_id,
            seq: entry.id.seq,
        }),
        inode: entry.inode,
        parent_ino: entry.parent_ino,
        mode: entry.mode,
        size: entry.size,
        mtime: entry.mtime,
        atime: entry.atime,
        ctime: entry.ctime,
        nlink: entry.nlink,
        symlink_target: entry.symlink_target.clone().unwrap_or_default(),
        file_type,
        uid: entry.uid,
        gid: entry.gid,
        rdev: entry.rdev,
    }
}

fn calculate_delta_sync_interval(writes_in_period: u64) -> Duration {
    const MIN_INTERVAL: Duration = Duration::from_secs(1);
    const MAX_INTERVAL: Duration = Duration::from_secs(60);

    if writes_in_period == 0 {
        MAX_INTERVAL
    } else if writes_in_period < 10 {
        Duration::from_secs(10)
    } else if writes_in_period < 50 {
        Duration::from_secs(5)
    } else if writes_in_period < 100 {
        Duration::from_secs(3)
    } else if writes_in_period < 500 {
        Duration::from_secs(2)
    } else {
        MIN_INTERVAL
    }
}

fn vec_to_proto_vclock(vclock: &VectorClock) -> powerfs_master::proto::powerfs::VectorClock {
    powerfs_master::proto::powerfs::VectorClock {
        entries: vclock
            .iter()
            .map(
                |(&client_id, &seq)| powerfs_master::proto::powerfs::VectorClockEntry {
                    client_id,
                    seq,
                },
            )
            .collect(),
    }
}

#[allow(dead_code)]
fn proto_to_vec_vclock(proto: &powerfs_master::proto::powerfs::VectorClock) -> VectorClock {
    let mut vclock = VectorClock::new();
    for entry in &proto.entries {
        vclock.observe(entry.client_id, entry.seq);
    }
    vclock
}

fn filer_vclock_to_vec(proto: &FilerVectorClock) -> VectorClock {
    let mut vclock = VectorClock::new();
    for entry in &proto.entries {
        vclock.observe(entry.client_id, entry.seq);
    }
    vclock
}

#[allow(dead_code, clippy::too_many_arguments)]
async fn do_pull_and_apply_deltas(
    client: &SyncFuseClient,
    client_id_str: &str,
    dir_cache: &ShardedDirCache,
    inode_index: &Arc<RwLock<HashMap<u64, (u64, EntryId)>>>,
    inode_paths: &Arc<RwLock<HashMap<u64, String>>>,
    inode_state: &Arc<RwLock<HashMap<u64, InodeState>>>,
    client_vclock: &Arc<RwLock<VectorClock>>,
    invalidation_sender: &Sender<InvalidationRequest>,
) -> Result<(), String> {
    let vclock = client_vclock
        .read()
        .expect("client_vclock lock poisoned")
        .clone();
    let proto_vclock = vec_to_proto_vclock(&vclock);

    let response = client
        .pull_delta_async(client_id_str, &proto_vclock)
        .await?;

    if !response.deltas.is_empty() {
        info!(
            "pull_delta received {} deltas from filer",
            response.deltas.len()
        );

        let mut invalidated_dirs = std::collections::HashSet::new();

        // Apply each Filer DeltaOp to local DirORSet cache
        for delta in &response.deltas {
            let affected_dir =
                apply_filer_delta_to_local(delta, dir_cache, inode_index, inode_paths, inode_state);
            // Track affected directories for cache invalidation
            if let Some(dir_ino) = affected_dir {
                invalidated_dirs.insert(dir_ino);
            }
        }

        // Trigger cache invalidation for affected directories
        for dir_ino in &invalidated_dirs {
            let _ = invalidation_sender.try_send(InvalidationRequest::InvalidateEntry(
                *dir_ino,
                String::new(),
            ));
        }
    }

    if let Some(server_vclock) = response.server_vclock {
        let new_vclock = filer_vclock_to_vec(&server_vclock);
        let mut client_vclock = client_vclock.write().expect("client_vclock lock poisoned");
        // Merge server vclock into client vclock (take max for each entry)
        for (client_id, seq) in new_vclock.iter() {
            client_vclock.observe(*client_id, *seq);
        }
    }

    Ok(())
}

// Apply Filer DeltaOp to local OR-Set cache
// Returns Option<u64> - the affected directory inode for cache invalidation
fn apply_filer_delta_to_local(
    delta: &powerfs_filer::powerfs::DeltaOp,
    dir_cache: &ShardedDirCache,
    inode_index: &Arc<RwLock<HashMap<u64, (u64, EntryId)>>>,
    inode_paths: &Arc<RwLock<HashMap<u64, String>>>,
    inode_state: &Arc<RwLock<HashMap<u64, InodeState>>>,
) -> Option<u64> {
    let mut affected_dir: Option<u64> = None;
    match &delta.op {
        Some(powerfs_filer::powerfs::delta_op::Op::Add(entry_orset)) => {
            let dir_ino = entry_orset.parent_ino;
            let entry_id = EntryId::new(
                entry_orset.name.clone(),
                entry_orset.client_id,
                entry_orset.seq,
            );
            let entry_id_for_index = entry_id.clone();

            let dir_entry = DirEntry::new_file(
                entry_id,
                entry_orset.inode,
                entry_orset.parent_ino,
                entry_orset.mode,
                0, // uid
                0, // gid
            );

            let orset_arc = dir_cache.ensure_dir_cache(dir_ino);
            let mut orset = orset_arc.write().expect("orset lock poisoned");
            orset.add(dir_entry);

            // Update inode index
            inode_index
                .write()
                .expect("inode_index lock poisoned")
                .insert(entry_orset.inode, (dir_ino, entry_id_for_index));

            // Update inode path
            let parent_path = inode_paths
                .read()
                .expect("inode_paths lock poisoned")
                .get(&dir_ino)
                .cloned()
                .unwrap_or_else(|| "/".to_string());
            let child_path = if parent_path == "/" {
                format!("/{}", entry_orset.name)
            } else {
                format!("{}/{}", parent_path, entry_orset.name)
            };
            inode_paths
                .write()
                .expect("inode_paths lock poisoned")
                .insert(entry_orset.inode, child_path);

            // Update inode state
            inode_state
                .write()
                .expect("inode_state lock poisoned")
                .insert(entry_orset.inode, InodeState::new(0));

            info!(
                "Applied Add delta from filer: inode={}, parent={}, name={}",
                entry_orset.inode, dir_ino, entry_orset.name
            );
            affected_dir = Some(dir_ino);
        }
        Some(powerfs_filer::powerfs::delta_op::Op::Remove(entry_id_proto)) => {
            let _entry_id = EntryId::new(
                entry_id_proto.name.clone(),
                0, // client_id unknown from filer Remove, use 0 as placeholder
                0, // seq unknown
            );

            // Find inode by parent_ino and name
            let ino_to_remove = {
                let index = inode_index.read().expect("inode_index lock poisoned");
                index
                    .iter()
                    .find(|(_, (dir_ino, eid))| {
                        *dir_ino == entry_id_proto.parent_ino && eid.name == entry_id_proto.name
                    })
                    .map(|(&ino, _)| ino)
            };

            if let Some(ino) = ino_to_remove {
                // Remove from OR-Set
                if let Some((_, eid)) = inode_index
                    .read()
                    .expect("inode_index lock poisoned")
                    .get(&ino)
                {
                    if let Some(orset_arc) = dir_cache.get(entry_id_proto.parent_ino) {
                        let mut orset = orset_arc.write().expect("orset lock poisoned");
                        orset.remove(eid);
                    }
                }

                // Remove from indexes
                inode_index
                    .write()
                    .expect("inode_index lock poisoned")
                    .remove(&ino);
                inode_paths
                    .write()
                    .expect("inode_paths lock poisoned")
                    .remove(&ino);
                inode_state
                    .write()
                    .expect("inode_state lock poisoned")
                    .remove(&ino);

                info!(
                    "Applied Remove delta from filer: inode={}, parent={}, name={}",
                    ino, entry_id_proto.parent_ino, entry_id_proto.name
                );
                affected_dir = Some(entry_id_proto.parent_ino);
            }
        }
        Some(powerfs_filer::powerfs::delta_op::Op::Rename(rename_op)) => {
            // Find the inode by old parent_ino and old_name
            let ino_to_rename = {
                let index = inode_index.read().expect("inode_index lock poisoned");
                index
                    .iter()
                    .find(|(_, (dir_ino, eid))| {
                        *dir_ino == rename_op.old_parent_ino && eid.name == rename_op.old_name
                    })
                    .map(|(&ino, _)| ino)
            };

            if let Some(ino) = ino_to_rename {
                // Remove from old parent's OR-Set
                if let Some((_, old_eid)) = inode_index
                    .read()
                    .expect("inode_index lock poisoned")
                    .get(&ino)
                {
                    if let Some(orset_arc) = dir_cache.get(rename_op.old_parent_ino) {
                        let mut orset = orset_arc.write().expect("orset lock poisoned");
                        orset.remove(old_eid);
                    }
                }

                // Create new entry in new parent's OR-Set
                let new_entry_id = EntryId::new(
                    rename_op.new_name.clone(),
                    0, // client_id unknown
                    0, // seq unknown
                );
                let new_dir_entry = DirEntry::new_file(
                    new_entry_id.clone(),
                    ino,
                    rename_op.new_parent_ino,
                    0o644, // default mode
                    0,     // uid
                    0,     // gid
                );

                let orset_arc_new = dir_cache.ensure_dir_cache(rename_op.new_parent_ino);
                let mut orset_new = orset_arc_new.write().expect("orset lock poisoned");
                orset_new.add(new_dir_entry);

                // Update indexes
                inode_index
                    .write()
                    .expect("inode_index lock poisoned")
                    .insert(ino, (rename_op.new_parent_ino, new_entry_id));

                // Update path
                let parent_path = inode_paths
                    .read()
                    .expect("inode_paths lock poisoned")
                    .get(&rename_op.new_parent_ino)
                    .cloned()
                    .unwrap_or_else(|| "/".to_string());
                let child_path = if parent_path == "/" {
                    format!("/{}", rename_op.new_name)
                } else {
                    format!("{}/{}", parent_path, rename_op.new_name)
                };
                inode_paths
                    .write()
                    .expect("inode_paths lock poisoned")
                    .insert(ino, child_path);

                info!(
                    "Applied Rename delta from filer: inode={}, {} -> {}",
                    ino, rename_op.old_name, rename_op.new_name
                );
                affected_dir = Some(rename_op.new_parent_ino);
            }
        }
        Some(powerfs_filer::powerfs::delta_op::Op::SetAttr(setattr_op)) => {
            // Update inode attributes
            if let Some((dir_ino, entry_id)) = inode_index
                .read()
                .expect("inode_index lock poisoned")
                .get(&setattr_op.inode)
            {
                if let Some(orset_arc) = dir_cache.get(*dir_ino) {
                    let mut orset = orset_arc.write().expect("orset lock poisoned");
                    if let Some(entry) = orset.entries.get_mut(entry_id) {
                        if setattr_op.size > 0 {
                            entry.size = setattr_op.size;
                        }
                        if setattr_op.mtime > 0 {
                            entry.mtime = setattr_op.mtime;
                        }
                    }
                }

                info!(
                    "Applied SetAttr delta from filer: inode={}, size={}, mtime={}",
                    setattr_op.inode, setattr_op.size, setattr_op.mtime
                );
                affected_dir = Some(*dir_ino);
            }
        }
        None => {}
    }
    affected_dir
}

#[allow(dead_code)]
fn apply_delta_helper(
    delta: &powerfs_master::proto::powerfs::DeltaOp,
    dir_cache: &ShardedDirCache,
    inode_index: &Arc<RwLock<HashMap<u64, (u64, EntryId)>>>,
    inode_paths: &Arc<RwLock<HashMap<u64, String>>>,
    inode_state: &Arc<RwLock<HashMap<u64, InodeState>>>,
) {
    match &delta.op {
        Some(powerfs_master::proto::powerfs::delta_op::Op::Add(entry)) => {
            if let Some(id) = &entry.id {
                let dir_entry = proto_dir_entry_to_local(entry);
                let dir_ino = entry.parent_ino;

                let orset_arc = dir_cache.ensure_dir_cache(dir_ino);

                {
                    let mut orset = orset_arc.write().expect("orset lock poisoned");
                    orset.add(dir_entry.clone());
                }

                inode_index
                    .write()
                    .expect("inode_index lock poisoned")
                    .insert(
                        entry.inode,
                        (dir_ino, EntryId::new(id.name.clone(), id.client_id, id.seq)),
                    );

                let parent_path = {
                    let paths = inode_paths.read().expect("inode_paths lock poisoned");
                    paths
                        .get(&dir_ino)
                        .cloned()
                        .unwrap_or_else(|| "/".to_string())
                };
                let child_path = if parent_path == "/" {
                    format!("/{}", id.name)
                } else {
                    format!("{}/{}", parent_path, id.name)
                };
                inode_paths
                    .write()
                    .expect("inode_paths lock poisoned")
                    .insert(entry.inode, child_path);

                inode_state
                    .write()
                    .expect("inode_state lock poisoned")
                    .insert(entry.inode, InodeState::new(0));
            }
        }
        Some(powerfs_master::proto::powerfs::delta_op::Op::Remove(id)) => {
            let entry_id = EntryId::new(id.name.clone(), id.client_id, id.seq);

            let ino_info = {
                let index = inode_index.read().expect("inode_index lock poisoned");
                index
                    .iter()
                    .find(|(_, (_, eid))| eid == &entry_id)
                    .map(|(&ino, &(d, ref e))| (ino, d, e.clone()))
            };

            if let Some((ino, dir_ino, eid)) = ino_info {
                if let Some(orset_arc) = dir_cache.get(dir_ino) {
                    let mut orset = orset_arc.write().expect("orset lock poisoned");
                    orset.remove(&eid);
                }

                {
                    let mut index = inode_index.write().expect("inode_index lock poisoned");
                    index.remove(&ino);
                }

                {
                    let mut paths = inode_paths.write().expect("inode_paths lock poisoned");
                    paths.remove(&ino);
                }

                {
                    let mut state = inode_state.write().expect("inode_state lock poisoned");
                    state.remove(&ino);
                }
            }
        }
        Some(powerfs_master::proto::powerfs::delta_op::Op::Rename(op)) => {
            if let (Some(old_id), Some(new_entry)) = (&op.old_id, &op.new_entry) {
                if let Some(new_id) = &new_entry.id {
                    let old_entry_id =
                        EntryId::new(old_id.name.clone(), old_id.client_id, old_id.seq);

                    let ino_info = {
                        let index = inode_index.read().expect("inode_index lock poisoned");
                        index
                            .iter()
                            .find(|(_, (_, eid))| eid == &old_entry_id)
                            .map(|(&ino, &(d, _))| (ino, d))
                    };

                    if let Some((ino, old_dir_ino)) = ino_info {
                        if let Some(orset_arc) = dir_cache.get(old_dir_ino) {
                            let mut orset = orset_arc.write().expect("orset lock poisoned");
                            orset.remove(&old_entry_id);
                        }

                        let dir_entry = proto_dir_entry_to_local(new_entry);
                        let new_dir_ino = new_entry.parent_ino;

                        let orset_arc_new = dir_cache.ensure_dir_cache(new_dir_ino);
                        let mut orset_new = orset_arc_new.write().expect("orset lock poisoned");
                        orset_new.add(dir_entry.clone());

                        {
                            let mut index = inode_index.write().expect("inode_index lock poisoned");
                            index.insert(
                                ino,
                                (
                                    new_dir_ino,
                                    EntryId::new(new_id.name.clone(), new_id.client_id, new_id.seq),
                                ),
                            );
                        }

                        let parent_path = {
                            let paths = inode_paths.read().expect("inode_paths lock poisoned");
                            paths
                                .get(&new_dir_ino)
                                .cloned()
                                .unwrap_or_else(|| "/".to_string())
                        };
                        let child_path = if parent_path == "/" {
                            format!("/{}", new_id.name)
                        } else {
                            format!("{}/{}", parent_path, new_id.name)
                        };
                        inode_paths
                            .write()
                            .expect("inode_paths lock poisoned")
                            .insert(ino, child_path);
                    }
                }
            }
        }
        Some(powerfs_master::proto::powerfs::delta_op::Op::SetAttr(op)) => {
            let index = inode_index.read().expect("inode_index lock poisoned");
            if let Some((dir_ino, entry_id)) = index.get(&op.inode) {
                if let Some(orset_arc) = dir_cache.get(*dir_ino) {
                    let mut orset = orset_arc.write().expect("orset lock poisoned");
                    if let Some(entry) = orset.entries.get_mut(entry_id) {
                        if op.mode != 0 {
                            entry.mode = op.mode;
                        }
                        if op.uid != 0 {
                            entry.uid = op.uid;
                        }
                        if op.gid != 0 {
                            entry.gid = op.gid;
                        }
                        if op.size != 0 {
                            entry.size = op.size;
                        }
                        if op.mtime != 0 {
                            entry.mtime = op.mtime;
                        }
                        if op.nlink != 0 {
                            entry.nlink = op.nlink;
                        }
                    }
                }
            }
        }
        None => {}
    }
}

/// 用于测试的辅助：获取 VectorClock 引用（验证 vclock 更新）
#[cfg(test)]
impl MetadataManager {
    pub fn dir_orset_vclock(&self, dir_ino: u64) -> Option<crate::orset::VectorClock> {
        self.dir_cache.get(dir_ino).map(|arc| {
            let orset = arc.read().unwrap();
            orset.vclock.clone()
        })
    }

    pub fn dir_orset_len(&self, dir_ino: u64) -> usize {
        self.dir_cache
            .get(dir_ino)
            .map(|arc| {
                let orset = arc.read().unwrap();
                orset.len()
            })
            .unwrap_or(0)
    }

    pub fn inode_index_size(&self) -> usize {
        let inode_index = self.inode_index.read().unwrap();
        inode_index.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_mgr() -> MetadataManager {
        MetadataManager::new_local(1001)
    }

    // ==================== 读路径测试 ====================

    #[test]
    fn test_root_dir_initialized() {
        let mgr = create_mgr();
        // 根目录 OR-Set 存在且为空
        assert_eq!(mgr.dir_orset_len(ROOT_INO), 0);

        // 根目录条目可获取
        let root = mgr.get_entry_by_inode(ROOT_INO).unwrap().unwrap();
        assert_eq!(root.inode, ROOT_INO);
        assert_eq!(root.file_type, FileType::Directory);
        assert_eq!(root.parent_ino, ROOT_INO);
    }

    #[test]
    fn test_lookup_not_found() {
        let mgr = create_mgr();
        let result = mgr.lookup(ROOT_INO, "nonexistent").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_list_dir_empty() {
        let mgr = create_mgr();
        let listing = mgr.list_dir(ROOT_INO).unwrap();
        assert!(listing.is_empty());
    }

    #[test]
    fn test_get_path_root() {
        let mgr = create_mgr();
        assert_eq!(mgr.get_path(ROOT_INO), Some("/".to_string()));
    }

    // ==================== 写路径测试 ====================

    #[test]
    fn test_create_file() {
        let mgr = create_mgr();

        let entry = mgr
            .create(ROOT_INO, "test.txt", 0o644 | libc::S_IFREG, 0, 0)
            .unwrap();
        assert_eq!(entry.id.name, "test.txt");
        assert_eq!(entry.file_type, FileType::RegularFile);
        assert_eq!(entry.parent_ino, ROOT_INO);
        assert_eq!(entry.mode, 0o644 | libc::S_IFREG);
        assert!(entry.inode >= 100);

        // 本地 OR-Set 应包含该条目
        assert_eq!(mgr.dir_orset_len(ROOT_INO), 1);

        // inode 索引应有该条目
        assert_eq!(mgr.inode_index_size(), 1);

        // lookup 应能找到
        let found = mgr.lookup(ROOT_INO, "test.txt").unwrap().unwrap();
        assert_eq!(found.inode, entry.inode);
        assert_eq!(found.id.name, "test.txt");

        // 路径映射应正确
        assert_eq!(mgr.get_path(entry.inode), Some("/test.txt".to_string()));
    }

    #[test]
    fn test_mkdir() {
        let mgr = create_mgr();

        let entry = mgr
            .mkdir(ROOT_INO, "subdir", 0o755 | libc::S_IFDIR, 0, 0)
            .unwrap();
        assert_eq!(entry.id.name, "subdir");
        assert_eq!(entry.file_type, FileType::Directory);

        // 新目录应有空 OR-Set
        assert_eq!(mgr.dir_orset_len(entry.inode), 0);

        // 在新目录中创建文件
        let file_entry = mgr
            .create(entry.inode, "inner.txt", 0o644 | libc::S_IFREG, 0, 0)
            .unwrap();
        assert_eq!(file_entry.parent_ino, entry.inode);

        // 路径应正确
        assert_eq!(
            mgr.get_path(file_entry.inode),
            Some("/subdir/inner.txt".to_string())
        );
    }

    #[test]
    fn test_symlink() {
        let mgr = create_mgr();

        let entry = mgr.symlink(ROOT_INO, "link", "/target/path").unwrap();
        assert_eq!(entry.file_type, FileType::Symlink);
        assert_eq!(entry.symlink_target, Some("/target/path".to_string()));
    }

    #[test]
    fn test_unlink() {
        let mgr = create_mgr();

        mgr.create(ROOT_INO, "to_delete.txt", 0o644 | libc::S_IFREG, 0, 0)
            .unwrap();
        assert_eq!(mgr.dir_orset_len(ROOT_INO), 1);

        mgr.unlink(ROOT_INO, "to_delete.txt").unwrap();
        assert_eq!(mgr.dir_orset_len(ROOT_INO), 0);

        // lookup 应找不到
        assert!(mgr.lookup(ROOT_INO, "to_delete.txt").unwrap().is_none());
    }

    #[test]
    fn test_unlink_not_found() {
        let mgr = create_mgr();
        let result = mgr.unlink(ROOT_INO, "nonexistent");
        assert!(matches!(result, Err(FsError::NotFound(_))));
    }

    #[test]
    fn test_unlink_on_directory_fails() {
        let mgr = create_mgr();
        mgr.mkdir(ROOT_INO, "adir", 0o755 | libc::S_IFDIR, 0, 0)
            .unwrap();

        let result = mgr.unlink(ROOT_INO, "adir");
        assert!(matches!(result, Err(FsError::IsDirectory(_))));
    }

    #[test]
    fn test_rmdir() {
        let mgr = create_mgr();
        mgr.mkdir(ROOT_INO, "to_rmdir", 0o755 | libc::S_IFDIR, 0, 0)
            .unwrap();
        assert_eq!(mgr.dir_orset_len(ROOT_INO), 1);

        mgr.rmdir(ROOT_INO, "to_rmdir").unwrap();
        assert_eq!(mgr.dir_orset_len(ROOT_INO), 0);
    }

    #[test]
    fn test_rmdir_not_empty_fails() {
        let mgr = create_mgr();
        let dir_entry = mgr
            .mkdir(ROOT_INO, "nonempty", 0o755 | libc::S_IFDIR, 0, 0)
            .unwrap();
        mgr.create(dir_entry.inode, "child.txt", 0o644 | libc::S_IFREG, 0, 0)
            .unwrap();

        let result = mgr.rmdir(ROOT_INO, "nonempty");
        assert!(matches!(result, Err(FsError::NotEmpty(_))));
    }

    #[test]
    fn test_rmdir_on_file_fails() {
        let mgr = create_mgr();
        mgr.create(ROOT_INO, "afile.txt", 0o644 | libc::S_IFREG, 0, 0)
            .unwrap();

        let result = mgr.rmdir(ROOT_INO, "afile.txt");
        assert!(matches!(result, Err(FsError::NotDirectory(_))));
    }

    #[test]
    fn test_rename_file() {
        let mgr = create_mgr();

        mgr.create(ROOT_INO, "old_name.txt", 0o644 | libc::S_IFREG, 0, 0)
            .unwrap();
        assert_eq!(mgr.dir_orset_len(ROOT_INO), 1);

        // 重命名
        mgr.rename(ROOT_INO, "old_name.txt", ROOT_INO, "new_name.txt")
            .unwrap();

        // 旧名不存在
        assert!(mgr.lookup(ROOT_INO, "old_name.txt").unwrap().is_none());
        // 新名存在
        let found = mgr.lookup(ROOT_INO, "new_name.txt").unwrap().unwrap();
        assert_eq!(found.id.name, "new_name.txt");
    }

    #[test]
    fn test_rename_across_dirs() {
        let mgr = create_mgr();

        let dir1 = mgr
            .mkdir(ROOT_INO, "dir1", 0o755 | libc::S_IFDIR, 0, 0)
            .unwrap();
        let dir2 = mgr
            .mkdir(ROOT_INO, "dir2", 0o755 | libc::S_IFDIR, 0, 0)
            .unwrap();

        mgr.create(dir1.inode, "mover.txt", 0o644 | libc::S_IFREG, 0, 0)
            .unwrap();

        // 从 dir1 移到 dir2
        mgr.rename(dir1.inode, "mover.txt", dir2.inode, "moved.txt")
            .unwrap();

        // dir1 中不存在
        assert!(mgr.lookup(dir1.inode, "mover.txt").unwrap().is_none());
        // dir2 中存在
        let found = mgr.lookup(dir2.inode, "moved.txt").unwrap().unwrap();
        assert_eq!(found.id.name, "moved.txt");
        assert_eq!(found.parent_ino, dir2.inode);

        // 路径应更新
        assert_eq!(
            mgr.get_path(found.inode),
            Some("/dir2/moved.txt".to_string())
        );
    }

    #[test]
    fn test_rename_directory() {
        let mgr = create_mgr();

        let dir = mgr
            .mkdir(ROOT_INO, "old_dir", 0o755 | libc::S_IFDIR, 0, 0)
            .unwrap();
        mgr.create(dir.inode, "child.txt", 0o644 | libc::S_IFREG, 0, 0)
            .unwrap();

        // 重命名目录
        mgr.rename(ROOT_INO, "old_dir", ROOT_INO, "new_dir")
            .unwrap();

        // 旧名不存在
        assert!(mgr.lookup(ROOT_INO, "old_dir").unwrap().is_none());
        // 新名存在
        let new_dir = mgr.lookup(ROOT_INO, "new_dir").unwrap().unwrap();
        assert_eq!(new_dir.file_type, FileType::Directory);

        // 子文件应仍然可访问
        let child = mgr.lookup(new_dir.inode, "child.txt").unwrap().unwrap();
        assert_eq!(child.id.name, "child.txt");
    }

    #[test]
    fn test_setattr_mode() {
        let mgr = create_mgr();
        let entry = mgr
            .create(ROOT_INO, "chmod.txt", 0o644 | libc::S_IFREG, 0, 0)
            .unwrap();

        let updated = mgr
            .setattr(
                entry.inode,
                Some(0o600 | libc::S_IFREG),
                None,
                None,
                None,
                None,
            )
            .unwrap();
        assert_eq!(updated.mode, 0o600 | libc::S_IFREG);
    }

    #[test]
    fn test_setattr_size() {
        let mgr = create_mgr();
        let entry = mgr
            .create(ROOT_INO, "resize.txt", 0o644 | libc::S_IFREG, 0, 0)
            .unwrap();
        assert_eq!(entry.size, 0);

        let updated = mgr
            .setattr(entry.inode, None, None, None, Some(1024), None)
            .unwrap();
        assert_eq!(updated.size, 1024);
    }

    #[test]
    fn test_setattr_mtime() {
        let mgr = create_mgr();
        let entry = mgr
            .create(ROOT_INO, "mtime.txt", 0o644 | libc::S_IFREG, 0, 0)
            .unwrap();

        let updated = mgr
            .setattr(entry.inode, None, None, None, None, Some(1234567890))
            .unwrap();
        assert_eq!(updated.mtime, 1234567890);
    }

    #[test]
    fn test_setattr_not_found() {
        let mgr = create_mgr();
        let result = mgr.setattr(99999, Some(0o644), None, None, None, None);
        assert!(matches!(result, Err(FsError::NotFound(_))));
    }

    #[test]
    fn test_get_entry_by_inode() {
        let mgr = create_mgr();
        let entry = mgr
            .create(ROOT_INO, "getattr.txt", 0o644 | libc::S_IFREG, 0, 0)
            .unwrap();

        let found = mgr.get_entry_by_inode(entry.inode).unwrap().unwrap();
        assert_eq!(found.inode, entry.inode);
        assert_eq!(found.id.name, "getattr.txt");
    }

    #[test]
    fn test_get_entry_by_inode_root() {
        let mgr = create_mgr();
        let root = mgr.get_entry_by_inode(ROOT_INO).unwrap().unwrap();
        assert_eq!(root.inode, ROOT_INO);
        assert_eq!(root.file_type, FileType::Directory);
    }

    #[test]
    fn test_get_parent_dir() {
        let mgr = create_mgr();
        let dir = mgr
            .mkdir(ROOT_INO, "parent_test", 0o755 | libc::S_IFDIR, 0, 0)
            .unwrap();
        let file = mgr
            .create(dir.inode, "child.txt", 0o644 | libc::S_IFREG, 0, 0)
            .unwrap();

        // 获取文件的父目录
        let parent = mgr.get_parent_dir(file.inode).unwrap().unwrap();
        assert_eq!(parent.inode, dir.inode);

        // 获取目录的父目录（应为根）
        let grandparent = mgr.get_parent_dir(dir.inode).unwrap().unwrap();
        assert_eq!(grandparent.inode, ROOT_INO);
    }

    #[test]
    fn test_list_dir_multiple_entries() {
        let mgr = create_mgr();

        mgr.create(ROOT_INO, "a.txt", 0o644 | libc::S_IFREG, 0, 0)
            .unwrap();
        mgr.create(ROOT_INO, "b.txt", 0o644 | libc::S_IFREG, 0, 0)
            .unwrap();
        mgr.mkdir(ROOT_INO, "c_dir", 0o755 | libc::S_IFDIR, 0, 0)
            .unwrap();

        let listing = mgr.list_dir(ROOT_INO).unwrap();
        assert_eq!(listing.len(), 3);

        // 应按名称排序
        assert_eq!(listing[0].name, "a.txt");
        assert_eq!(listing[1].name, "b.txt");
        assert_eq!(listing[2].name, "c_dir");
    }

    #[test]
    fn test_inode_allocator_increments() {
        let mgr = create_mgr();

        let e1 = mgr
            .create(ROOT_INO, "f1.txt", 0o644 | libc::S_IFREG, 0, 0)
            .unwrap();
        let e2 = mgr
            .create(ROOT_INO, "f2.txt", 0o644 | libc::S_IFREG, 0, 0)
            .unwrap();
        let e3 = mgr
            .create(ROOT_INO, "f3.txt", 0o644 | libc::S_IFREG, 0, 0)
            .unwrap();

        assert_eq!(e2.inode, e1.inode + 1);
        assert_eq!(e3.inode, e2.inode + 1);
    }

    #[test]
    fn test_seq_counter_increments() {
        let mgr = create_mgr();

        let e1 = mgr
            .create(ROOT_INO, "s1.txt", 0o644 | libc::S_IFREG, 0, 0)
            .unwrap();
        let e2 = mgr
            .create(ROOT_INO, "s2.txt", 0o644 | libc::S_IFREG, 0, 0)
            .unwrap();

        // 同一客户端的 seq 应递增
        assert_eq!(e1.id.client_id, 1001);
        assert_eq!(e2.id.client_id, 1001);
        assert!(e2.id.seq > e1.id.seq);
    }

    #[test]
    fn test_full_lifecycle() {
        let mgr = create_mgr();

        // 创建 → 查找 → 修改属性 → 删除
        let entry = mgr
            .create(ROOT_INO, "lifecycle.txt", 0o644 | libc::S_IFREG, 0, 0)
            .unwrap();
        assert!(mgr.lookup(ROOT_INO, "lifecycle.txt").unwrap().is_some());

        mgr.setattr(entry.inode, None, None, None, Some(2048), None)
            .unwrap();
        let found = mgr.get_entry_by_inode(entry.inode).unwrap().unwrap();
        assert_eq!(found.size, 2048);

        mgr.unlink(ROOT_INO, "lifecycle.txt").unwrap();
        assert!(mgr.lookup(ROOT_INO, "lifecycle.txt").unwrap().is_none());
        assert!(mgr.get_entry_by_inode(entry.inode).unwrap().is_none());
    }

    #[test]
    fn test_dir_cache_lazily_created() {
        let mgr = create_mgr();

        // 访问一个未创建的目录（通过 list_dir）
        let listing = mgr.list_dir(99998).unwrap();
        assert!(listing.is_empty());

        // OR-Set 应被惰性创建
        assert_eq!(mgr.dir_orset_len(99998), 0);
    }

    #[test]
    fn test_client_vclock_initialized() {
        let mgr = create_mgr();
        let vclock = mgr
            .client_vclock
            .read()
            .expect("client_vclock lock poisoned");
        assert!(vclock.iter().next().is_none());
    }

    #[test]
    fn test_apply_delta_add() {
        let mgr = create_mgr();

        let delta_op = powerfs_master::proto::powerfs::DeltaOp {
            op: Some(powerfs_master::proto::powerfs::delta_op::Op::Add(
                powerfs_master::proto::powerfs::DirEntryOrset {
                    id: Some(powerfs_master::proto::powerfs::EntryId {
                        name: "delta_file.txt".to_string(),
                        client_id: 2,
                        seq: 1,
                    }),
                    inode: 100,
                    parent_ino: ROOT_INO,
                    mode: 0o644,
                    size: 100,
                    mtime: 1000,
                    atime: 1000,
                    ctime: 1000,
                    nlink: 1,
                    symlink_target: "".to_string(),
                    file_type: 0,
                    uid: 0,
                    gid: 0,
                    rdev: 0,
                },
            )),
            vclock: Some(powerfs_master::proto::powerfs::VectorClock { entries: vec![] }),
        };

        mgr.apply_delta(&delta_op);

        assert_eq!(mgr.dir_orset_len(ROOT_INO), 1);
        assert!(mgr.lookup(ROOT_INO, "delta_file.txt").unwrap().is_some());
        assert_eq!(mgr.inode_index_size(), 1);
    }

    #[test]
    fn test_apply_delta_remove() {
        let mgr = create_mgr();

        let add_delta = powerfs_master::proto::powerfs::DeltaOp {
            op: Some(powerfs_master::proto::powerfs::delta_op::Op::Add(
                powerfs_master::proto::powerfs::DirEntryOrset {
                    id: Some(powerfs_master::proto::powerfs::EntryId {
                        name: "to_remove.txt".to_string(),
                        client_id: 2,
                        seq: 1,
                    }),
                    inode: 100,
                    parent_ino: ROOT_INO,
                    mode: 0o644,
                    size: 100,
                    mtime: 1000,
                    atime: 1000,
                    ctime: 1000,
                    nlink: 1,
                    symlink_target: "".to_string(),
                    file_type: 0,
                    uid: 0,
                    gid: 0,
                    rdev: 0,
                },
            )),
            vclock: Some(powerfs_master::proto::powerfs::VectorClock { entries: vec![] }),
        };
        mgr.apply_delta(&add_delta);
        assert_eq!(mgr.dir_orset_len(ROOT_INO), 1);

        let remove_delta = powerfs_master::proto::powerfs::DeltaOp {
            op: Some(powerfs_master::proto::powerfs::delta_op::Op::Remove(
                powerfs_master::proto::powerfs::EntryId {
                    name: "to_remove.txt".to_string(),
                    client_id: 2,
                    seq: 1,
                },
            )),
            vclock: Some(powerfs_master::proto::powerfs::VectorClock { entries: vec![] }),
        };
        mgr.apply_delta(&remove_delta);

        assert_eq!(mgr.dir_orset_len(ROOT_INO), 0);
        assert!(mgr.lookup(ROOT_INO, "to_remove.txt").unwrap().is_none());
    }

    #[test]
    fn test_apply_delta_setattr() {
        let mgr = create_mgr();

        let add_delta = powerfs_master::proto::powerfs::DeltaOp {
            op: Some(powerfs_master::proto::powerfs::delta_op::Op::Add(
                powerfs_master::proto::powerfs::DirEntryOrset {
                    id: Some(powerfs_master::proto::powerfs::EntryId {
                        name: "setattr_file.txt".to_string(),
                        client_id: 2,
                        seq: 1,
                    }),
                    inode: 100,
                    parent_ino: ROOT_INO,
                    mode: 0o644,
                    size: 100,
                    mtime: 1000,
                    atime: 1000,
                    ctime: 1000,
                    nlink: 1,
                    symlink_target: "".to_string(),
                    file_type: 0,
                    uid: 0,
                    gid: 0,
                    rdev: 0,
                },
            )),
            vclock: Some(powerfs_master::proto::powerfs::VectorClock { entries: vec![] }),
        };
        mgr.apply_delta(&add_delta);

        let setattr_delta = powerfs_master::proto::powerfs::DeltaOp {
            op: Some(powerfs_master::proto::powerfs::delta_op::Op::SetAttr(
                powerfs_master::proto::powerfs::SetAttrOp {
                    inode: 100,
                    mode: 0o755,
                    size: 200,
                    mtime: 2000,
                    uid: 1000,
                    gid: 2000,
                    nlink: 3,
                    chunks: vec![],
                    extended: std::collections::HashMap::new(),
                },
            )),
            vclock: Some(powerfs_master::proto::powerfs::VectorClock { entries: vec![] }),
        };
        mgr.apply_delta(&setattr_delta);

        let entry = mgr.get_entry_by_inode(100).unwrap().unwrap();
        assert_eq!(entry.mode, 0o755);
        assert_eq!(entry.size, 200);
        assert_eq!(entry.mtime, 2000);
        assert_eq!(entry.uid, 1000);
        assert_eq!(entry.gid, 2000);
        assert_eq!(entry.nlink, 3);
    }

    #[test]
    fn test_pull_and_apply_deltas_no_client() {
        let mgr = create_mgr();
        let result = mgr.pull_and_apply_deltas();
        assert!(result.is_ok());
    }

    #[test]
    fn test_vec_to_proto_vclock_conversion() {
        let mut vclock = VectorClock::new();
        vclock.increment(1);
        vclock.increment(2);
        vclock.increment(1);

        let proto = vec_to_proto_vclock(&vclock);
        assert_eq!(proto.entries.len(), 2);

        let entry1 = proto.entries.iter().find(|e| e.client_id == 1).unwrap();
        assert_eq!(entry1.seq, 2);

        let entry2 = proto.entries.iter().find(|e| e.client_id == 2).unwrap();
        assert_eq!(entry2.seq, 1);
    }

    #[test]
    fn test_proto_to_vec_vclock_conversion() {
        let proto = powerfs_master::proto::powerfs::VectorClock {
            entries: vec![
                powerfs_master::proto::powerfs::VectorClockEntry {
                    client_id: 1,
                    seq: 2,
                },
                powerfs_master::proto::powerfs::VectorClockEntry {
                    client_id: 3,
                    seq: 5,
                },
            ],
        };

        let vclock = proto_to_vec_vclock(&proto);
        assert_eq!(vclock.get(1), 2);
        assert_eq!(vclock.get(3), 5);
        assert_eq!(vclock.get(2), 0);
    }
}
