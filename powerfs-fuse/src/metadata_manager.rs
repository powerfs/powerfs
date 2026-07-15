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
use std::time::Duration;

use log::{debug, error, info, warn};

use crate::client::SyncFuseClient;
use crate::error::FsError;
use crate::inode_allocator::InodeAllocator;
use crate::orset::{now_unix, DirEntry, DirORSet, EntryId, FileType, VectorClock};
use crate::posix_projection::{PosixProjection, VisibleEntry};

/// 根目录 inode
const ROOT_INO: u64 = 1;

/// Master 来源条目的 client_id（区分本地创建与 Master 同步）
const MASTER_CLIENT_ID: u64 = 0;

/// 目录 OR-Set 缓存类型（简化复杂类型）
type DirCache = HashMap<u64, Arc<RwLock<DirORSet>>>;

pub struct MetadataManager {
    /// 本地 OR-Set 缓存：dir_inode -> DirORSet
    dir_cache: Arc<RwLock<DirCache>>,
    /// inode 反向索引：ino -> (dir_ino, EntryId)
    inode_index: Arc<RwLock<HashMap<u64, (u64, EntryId)>>>,
    /// inode → 全路径映射（用于 Master 同步时的 path KV 兼容）
    inode_paths: Arc<RwLock<HashMap<u64, String>>>,
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
}

impl MetadataManager {
    /// 创建本地版 MetadataManager（无 Master 连接，仅本地缓存）
    pub fn new_local(client_id: u64) -> Self {
        let mgr = Self {
            dir_cache: Arc::new(RwLock::new(HashMap::new())),
            inode_index: Arc::new(RwLock::new(HashMap::new())),
            inode_paths: Arc::new(RwLock::new(HashMap::new())),
            projection: PosixProjection::new(),
            inode_allocator: InodeAllocator::new(client_id),
            client_id,
            client_id_str: client_id.to_string(),
            seq_counter: AtomicU64::new(1),
            client: None,
            client_vclock: Arc::new(RwLock::new(VectorClock::new())),
        };
        mgr.init_root();
        mgr
    }

    /// 创建带 Master 连接的 MetadataManager
    pub fn new_with_master(client: Arc<SyncFuseClient>, client_id: u64) -> Self {
        let mgr = Self {
            dir_cache: Arc::new(RwLock::new(HashMap::new())),
            inode_index: Arc::new(RwLock::new(HashMap::new())),
            inode_paths: Arc::new(RwLock::new(HashMap::new())),
            projection: PosixProjection::new(),
            inode_allocator: InodeAllocator::new(client_id),
            client_id,
            client_id_str: client_id.to_string(),
            seq_counter: AtomicU64::new(1),
            client: Some(client),
            client_vclock: Arc::new(RwLock::new(VectorClock::new())),
        };
        mgr.init_root();
        mgr
    }

    /// 初始化根目录
    fn init_root(&self) {
        // 根目录的 OR-Set
        let root_orset = Arc::new(RwLock::new(DirORSet::new(ROOT_INO)));
        self.dir_cache.write().unwrap().insert(ROOT_INO, root_orset);

        // 根目录路径
        self.inode_paths
            .write()
            .unwrap()
            .insert(ROOT_INO, "/".to_string());
    }

    /// 获取 client_id
    pub fn client_id(&self) -> u64 {
        self.client_id
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
        let need_fetch = {
            let dir_cache = self.dir_cache.read().unwrap();
            dir_cache.get(&dir_ino).is_none()
        };

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

        // 返回空列表
        let orset_arc = self.ensure_dir_cache(dir_ino);
        let orset = orset_arc.read().unwrap();
        Ok(self.projection.project_listing(&orset))
    }

    /// 尝试从本地缓存列出目录（不触发 Master 拉取）
    ///
    /// 返回 None 表示需要从 Master 拉取
    fn try_list_dir_local(&self, dir_ino: u64) -> Option<Vec<VisibleEntry>> {
        // 先尝试读锁
        let dir_cache = self.dir_cache.read().ok()?;
        let orset_arc = dir_cache.get(&dir_ino)?;

        let orset = orset_arc.read().ok()?;
        if orset.is_empty() {
            return None;
        }

        Some(self.projection.project_listing(&orset))
    }

    /// 按 inode 获取条目（getattr 用）
    pub fn get_entry_by_inode(&self, ino: u64) -> Result<Option<DirEntry>, FsError> {
        // 先查本地索引
        {
            let index = self.inode_index.read().unwrap();
            if let Some((dir_ino, entry_id)) = index.get(&ino) {
                let dir_cache = self.dir_cache.read().unwrap();
                if let Some(orset_arc) = dir_cache.get(dir_ino) {
                    let orset = orset_arc.read().unwrap();
                    if let Some(entry) = orset.entries.get(entry_id) {
                        return Ok(Some(entry.clone()));
                    }
                }
            }
        }

        // 根目录特殊处理
        if ino == ROOT_INO {
            return Ok(Some(self.make_root_entry()));
        }

        // 本地未命中，回退 Master
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
    pub fn create(&self, dir_ino: u64, name: &str, mode: u32) -> Result<DirEntry, FsError> {
        let inode = self.inode_allocator.allocate();
        let seq = self.next_seq();
        let entry_id = EntryId::new(name, self.client_id, seq);
        let entry = DirEntry::new_file(entry_id, inode, dir_ino, mode);

        self.apply_to_local_orset(dir_ino, entry.clone())?;

        // best-effort 同步到 Master
        self.sync_create_to_master(&entry);

        Ok(entry)
    }

    /// 创建目录
    pub fn mkdir(&self, dir_ino: u64, name: &str, mode: u32) -> Result<DirEntry, FsError> {
        let inode = self.inode_allocator.allocate();
        let seq = self.next_seq();
        let entry_id = EntryId::new(name, self.client_id, seq);
        let entry = DirEntry::new_dir(entry_id, inode, dir_ino, mode);

        // 为新目录创建空 OR-Set（先获取 dir_cache 写锁并释放，避免锁顺序死锁）
        let new_dir_orset = Arc::new(RwLock::new(DirORSet::new(inode)));
        {
            let mut dir_cache = self.dir_cache.write().unwrap();
            dir_cache.insert(inode, new_dir_orset);
        }

        self.apply_to_local_orset(dir_ino, entry.clone())?;

        // best-effort 同步到 Master
        self.sync_create_to_master(&entry);

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

        // best-effort 同步到 Master
        self.sync_create_to_master(&entry);

        Ok(entry)
    }

    /// 删除文件（unlink）
    ///
    /// 1. 从本地 OR-Set 查找并移除
    /// 2. 加入 tombstones
    /// 3. best-effort 同步到 Master
    ///
    /// 返回被删除文件的 inode（供调用方清理数据缓存）
    pub fn unlink(&self, dir_ino: u64, name: &str) -> Result<u64, FsError> {
        let entry = self.lookup_local(dir_ino, name).ok_or_else(|| {
            FsError::NotFound(format!("unlink: {} not found in dir {}", name, dir_ino))
        })?;

        if entry.file_type == FileType::Directory {
            return Err(FsError::IsDirectory(format!(
                "unlink: {} is a directory, use rmdir instead",
                name
            )));
        }

        let inode = entry.inode;
        self.remove_from_local_orset(dir_ino, &entry.id)?;
        self.sync_delete_to_master(entry.inode, false);
        Ok(inode)
    }

    /// 删除目录（rmdir）
    pub fn rmdir(&self, dir_ino: u64, name: &str) -> Result<(), FsError> {
        let entry = self.lookup_local(dir_ino, name).ok_or_else(|| {
            FsError::NotFound(format!("rmdir: {} not found in dir {}", name, dir_ino))
        })?;

        if entry.file_type != FileType::Directory {
            return Err(FsError::NotDirectory(format!(
                "rmdir: {} is not a directory",
                name
            )));
        }

        let child_inode = entry.inode;
        let entry_id = entry.id.clone();

        // 一次性获取写锁完成所有操作，避免锁顺序死锁
        let mut dir_cache = self.dir_cache.write().unwrap();

        // 检查目录是否为空
        let is_empty = if let Some(child_orset) = dir_cache.get(&child_inode) {
            let orset = child_orset.read().unwrap();
            orset.is_empty()
        } else {
            true // 缓存已被清理，视为空
        };

        if !is_empty {
            return Err(FsError::NotEmpty(format!(
                "rmdir: directory {} is not empty",
                name
            )));
        }

        // 从父目录 OR-Set 中移除
        if let Some(parent_orset) = dir_cache.get(&dir_ino) {
            let mut orset = parent_orset.write().unwrap();
            orset.remove(&entry_id);
        }

        // 清理该目录的 OR-Set 缓存
        dir_cache.remove(&child_inode);
        drop(dir_cache); // 释放 dir_cache 锁

        // 清理 inode 索引和路径（单独的锁操作）
        {
            let mut index = self.inode_index.write().unwrap();
            index.remove(&child_inode);
        }
        {
            let mut paths = self.inode_paths.write().unwrap();
            paths.remove(&child_inode);
        }

        self.sync_delete_to_master(child_inode, true);
        Ok(())
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
        let old_entry = self.lookup_local(old_dir, old_name).ok_or_else(|| {
            FsError::NotFound(format!("rename: {} not found in dir {}", old_name, old_dir))
        })?;

        // 检查目标是否已存在（POSIX 覆盖语义）
        let overwritten_inode = if old_dir == new_dir && old_name == new_name {
            // 同位置重命名，无操作
            None
        } else {
            self.lookup_local(new_dir, new_name)
                .filter(|dest| dest.inode != old_entry.inode)
                .and_then(|dest| {
                    if dest.file_type == FileType::Directory {
                        // POSIX: 不能用普通文件覆盖目录
                        // （完整实现需检查空目录+类型匹配，Phase 1A 简化为拒绝）
                        return None;
                    }
                    // tombstone 旧目标
                    let _ = self.remove_from_local_orset(new_dir, &dest.id);
                    self.sync_delete_to_master(dest.inode, false);
                    Some(dest.inode)
                })
        };

        // 创建新条目（保留 inode 和大部分属性，更新 name 和 parent_ino）
        let seq = self.next_seq();
        let new_id = EntryId::new(new_name, self.client_id, seq);
        let mut new_entry = old_entry.clone();
        new_entry.id = new_id;
        new_entry.parent_ino = new_dir;
        new_entry.mtime = now_unix();

        // 从旧目录移除
        self.remove_from_local_orset(old_dir, &old_entry.id)?;

        // 加入新目录
        self.apply_to_local_orset(new_dir, new_entry.clone())?;

        // 更新 inode 索引和路径
        {
            let mut index = self.inode_index.write().unwrap();
            index.insert(new_entry.inode, (new_dir, new_entry.id.clone()));
        }
        self.update_path_for_inode(new_entry.inode, new_dir, new_name);

        // 如果是目录，也需要更新其 OR-Set 的 dir_ino
        if new_entry.file_type == FileType::Directory {
            let orset_arc = self.ensure_dir_cache(new_entry.inode);
            let mut orset = orset_arc.write().unwrap();
            orset.dir_ino = new_entry.inode;
        }

        // best-effort 同步到 Master
        self.sync_rename_to_master(old_dir, old_name, new_dir, new_name);

        Ok(overwritten_inode)
    }

    /// 修改属性（setattr）
    pub fn setattr(
        &self,
        ino: u64,
        mode: Option<u32>,
        size: Option<u64>,
        mtime: Option<u64>,
    ) -> Result<DirEntry, FsError> {
        // 查找条目
        let (dir_ino, entry_id) = {
            let index = self.inode_index.read().unwrap();
            index
                .get(&ino)
                .cloned()
                .ok_or_else(|| FsError::NotFound(format!("setattr: inode {} not found", ino)))?
        };

        // 更新本地 OR-Set 中的条目
        let orset_arc = self.ensure_dir_cache(dir_ino);
        let mut orset = orset_arc.write().unwrap();
        let entry = orset
            .entries
            .get_mut(&entry_id)
            .ok_or_else(|| FsError::NotFound(format!("setattr: entry {:?} not found", entry_id)))?;

        if let Some(m) = mode {
            entry.mode = m;
        }
        if let Some(s) = size {
            entry.size = s;
        }
        if let Some(t) = mtime {
            entry.mtime = t;
        }
        let updated = entry.clone();
        drop(orset);

        // best-effort 同步到 Master
        self.sync_setattr_to_master(&updated);

        Ok(updated)
    }

    // ==================== 内部辅助 ====================

    /// 本地 lookup（不触发 Master 回退）
    pub fn lookup_local(&self, dir_ino: u64, name: &str) -> Option<DirEntry> {
        let dir_cache = self.dir_cache.read().unwrap();
        if let Some(orset_arc) = dir_cache.get(&dir_ino) {
            let orset = orset_arc.read().unwrap();
            return self.projection.project_lookup(&orset, name);
        }
        None
    }

    /// 确保目录缓存存在，返回 OR-Set 的 Arc
    fn ensure_dir_cache(&self, dir_ino: u64) -> Arc<RwLock<DirORSet>> {
        // 先尝试读锁
        {
            let dir_cache = self.dir_cache.read().unwrap();
            if let Some(orset_arc) = dir_cache.get(&dir_ino) {
                return orset_arc.clone();
            }
        }
        // 写锁创建
        let mut dir_cache = self.dir_cache.write().unwrap();
        dir_cache
            .entry(dir_ino)
            .or_insert_with(|| Arc::new(RwLock::new(DirORSet::new(dir_ino))))
            .clone()
    }

    /// 应用条目到本地 OR-Set
    fn apply_to_local_orset(&self, dir_ino: u64, entry: DirEntry) -> Result<(), FsError> {
        let inode = entry.inode;
        let entry_id = entry.id.clone();

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

        // 更新路径映射
        self.update_path_for_inode(
            inode,
            dir_ino,
            &self.get_entry_name(inode).unwrap_or_default(),
        );

        Ok(())
    }

    /// 从本地 OR-Set 移除条目
    fn remove_from_local_orset(&self, dir_ino: u64, entry_id: &EntryId) -> Result<(), FsError> {
        let orset_arc = self.ensure_dir_cache(dir_ino);
        {
            let mut orset = orset_arc.write().unwrap();
            orset.remove(entry_id);
        }

        // 清理 inode 索引和路径
        // 需要找到 entry_id 对应的 inode
        let inode_to_remove: Option<u64> = {
            let index = self.inode_index.read().unwrap();
            index
                .iter()
                .find(|(_, (_, id))| id == entry_id)
                .map(|(&ino, _)| ino)
        };
        if let Some(ino) = inode_to_remove {
            self.inode_index.write().unwrap().remove(&ino);
            self.inode_paths.write().unwrap().remove(&ino);
        }

        Ok(())
    }

    /// 获取 inode 对应的条目名（从索引查）
    fn get_entry_name(&self, ino: u64) -> Option<String> {
        let index = self.inode_index.read().unwrap();
        index.get(&ino).map(|(_, id)| id.name.clone())
    }

    /// 失效本地缓存条目（用于接收远程删除通知时）
    pub fn invalidate_local_cache_entry(&self, parent_ino: u64, name: &str) {
        // 1. 查找本地条目
        if let Some(entry) = self.lookup_local(parent_ino, name) {
            let entry_id = entry.id.clone();
            let inode_to_remove = entry.inode;

            // 2. 从父目录 OR-Set 中移除
            if let Some(orset_arc) = self.dir_cache.read().unwrap().get(&parent_ino) {
                let mut orset = orset_arc.write().unwrap();
                orset.remove(&entry_id);
            }

            // 3. 清理子目录的 OR-Set 缓存
            self.dir_cache.write().unwrap().remove(&inode_to_remove);

            // 4. 清理 inode 索引和路径
            self.inode_index.write().unwrap().remove(&inode_to_remove);
            self.inode_paths.write().unwrap().remove(&inode_to_remove);

            info!(
                "Invalidated local cache entry: parent_ino={}, name={}, inode={}",
                parent_ino, name, inode_to_remove
            );
        } else {
            debug!(
                "Entry not found in local cache: parent_ino={}, name={}",
                parent_ino, name
            );
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
            file_type: FileType::Directory,
            mode: 0o755 | libc::S_IFDIR,
            size: 4096,
            mtime: now,
            atime: now,
            ctime: now,
            parent_ino: ROOT_INO,
            chunks: vec![],
            symlink_target: None,
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

        // 先获取目录内容（不持有任何锁）
        let entries = client
            .list_entries(dir_ino, 10000, "")
            .map_err(|e| FsError::MasterError(format!("list_entries: {}", e)))?;

        let num_entries = entries.len();
        if num_entries == 0 {
            return Ok(());
        }

        // 获取父路径（需要短暂持有锁）
        let parent_path = {
            let inode_paths = self.inode_paths.read().unwrap();
            inode_paths
                .get(&dir_ino)
                .cloned()
                .unwrap_or_else(|| "/".to_string())
        };

        // 批量更新：获取 OR-Set 锁
        let orset_arc = {
            let mut dir_cache = self.dir_cache.write().unwrap();
            dir_cache
                .entry(dir_ino)
                .or_insert_with(|| Arc::new(RwLock::new(DirORSet::new(dir_ino))))
                .clone()
        };

        let mut orset = orset_arc.write().unwrap();
        let mut inode_index = self.inode_index.write().unwrap();
        let mut inode_paths = self.inode_paths.write().unwrap();

        for proto_entry in entries {
            let dir_entry = proto_to_dir_entry(&proto_entry, dir_ino);
            let ino = dir_entry.inode;
            let entry_id = dir_entry.id.clone();

            orset.add(dir_entry);
            inode_index.insert(ino, (dir_ino, entry_id));

            let child_path = if parent_path == "/" {
                format!("/{}", proto_entry.name)
            } else {
                format!("{}/{}", parent_path, proto_entry.name)
            };
            inode_paths.insert(ino, child_path);
        }

        debug!(
            "fetch_dir_from_master_without_deadlock: dir_ino={}, entries={}",
            dir_ino, num_entries
        );

        Ok(())
    }

    /// 从 Master 拉取目录内容，填充本地 OR-Set
    fn fetch_dir_from_master(&self, dir_ino: u64) -> Result<(), FsError> {
        let client = match &self.client {
            Some(c) => c.clone(),
            None => return Ok(()),
        };

        let entries = client
            .list_entries(dir_ino, 10000, "")
            .map_err(|e| FsError::MasterError(format!("list_entries: {}", e)))?;

        let orset_arc = self.ensure_dir_cache(dir_ino);
        let mut orset = orset_arc.write().unwrap();

        let parent_path = self.get_path(dir_ino).unwrap_or_else(|| "/".to_string());

        for proto_entry in entries {
            let dir_entry = proto_to_dir_entry(&proto_entry, dir_ino);
            let ino = dir_entry.inode;
            let entry_id = dir_entry.id.clone();

            orset.add(dir_entry);

            // 更新索引
            self.inode_index
                .write()
                .unwrap()
                .insert(ino, (dir_ino, entry_id));

            // 更新路径
            let child_path = if parent_path == "/" {
                format!("/{}", proto_entry.name)
            } else {
                format!("{}/{}", parent_path, proto_entry.name)
            };
            self.inode_paths.write().unwrap().insert(ino, child_path);
        }

        debug!(
            "fetch_dir_from_master: dir_ino={}, entries={}",
            dir_ino,
            orset.len()
        );
        Ok(())
    }

    /// 从 Master 按 inode 拉取单个条目
    fn fetch_entry_by_inode_from_master(&self, ino: u64) -> Result<Option<DirEntry>, FsError> {
        let client = match &self.client {
            Some(c) => c.clone(),
            None => return Ok(None),
        };

        let result = client
            .get_entry_by_inode(ino)
            .map_err(|e| FsError::MasterError(format!("get_entry_by_inode: {}", e)))?;

        match result {
            Some((proto_entry, path)) => {
                // 从 path 推断 parent_ino
                let parent_ino = self.infer_parent_ino_from_path(&path);
                let dir_entry = proto_to_dir_entry(&proto_entry, parent_ino);

                // 更新索引和路径
                let entry_id = dir_entry.id.clone();
                self.inode_index
                    .write()
                    .unwrap()
                    .insert(ino, (parent_ino, entry_id));
                self.inode_paths.write().unwrap().insert(ino, path);

                // 加入父目录的 OR-Set
                let orset_arc = self.ensure_dir_cache(parent_ino);
                let mut orset = orset_arc.write().unwrap();
                orset.add(dir_entry.clone());

                Ok(Some(dir_entry))
            }
            None => Ok(None),
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

    /// 同步创建到 Master（best-effort）
    fn sync_create_to_master(&self, entry: &DirEntry) {
        let client = match &self.client {
            Some(c) => c.clone(),
            None => return,
        };

        let path = match self.get_path(entry.inode) {
            Some(p) => p,
            None => {
                warn!(
                    "sync_create_to_master: no path for inode {}, skip sync",
                    entry.inode
                );
                return;
            }
        };

        let parent_path = if let Some(last_slash) = path.rfind('/') {
            if last_slash == 0 {
                "/".to_string()
            } else {
                path[..last_slash].to_string()
            }
        } else {
            "/".to_string()
        };

        let proto_entry = dir_entry_to_proto(entry, &parent_path);

        if let Err(e) = client.create_entry(proto_entry, &self.client_id_str) {
            warn!(
                "sync_create_to_master failed for inode {} ({}): {}, local entry still valid",
                entry.inode, path, e
            );
        }
    }

    /// 同步删除到 Master（best-effort）
    fn sync_delete_to_master(&self, ino: u64, is_dir: bool) {
        let client = match &self.client {
            Some(c) => c.clone(),
            None => return,
        };

        if let Err(e) = client.delete_entry(ino, is_dir, &self.client_id_str) {
            warn!(
                "sync_delete_to_master failed for inode {}: {}, local deletion still valid",
                ino, e
            );
        }
    }

    /// 同步重命名到 Master（best-effort）
    fn sync_rename_to_master(&self, old_dir: u64, old_name: &str, new_dir: u64, new_name: &str) {
        let client = match &self.client {
            Some(c) => c.clone(),
            None => return,
        };

        if let Err(e) =
            client.rename_entry(old_dir, old_name, new_dir, new_name, &self.client_id_str)
        {
            warn!(
                "sync_rename_to_master failed ({} -> {}): {}, local rename still valid",
                old_name, new_name, e
            );
        }
    }

    /// 同步属性变更到 Master（best-effort）
    fn sync_setattr_to_master(&self, entry: &DirEntry) {
        let client = match &self.client {
            Some(c) => c.clone(),
            None => return,
        };

        let path = match self.get_path(entry.inode) {
            Some(p) => p,
            None => return,
        };

        let parent_path = if let Some(last_slash) = path.rfind('/') {
            if last_slash == 0 {
                "/".to_string()
            } else {
                path[..last_slash].to_string()
            }
        } else {
            "/".to_string()
        };

        let proto_entry = dir_entry_to_proto(entry, &parent_path);

        if let Err(e) = client.update_entry(&proto_entry, &self.client_id_str) {
            warn!(
                "sync_setattr_to_master failed for inode {}: {}, local change still valid",
                entry.inode, e
            );
        }
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
        let client_vclock = self.client_vclock.clone();

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
                    for delta in response.deltas {
                        apply_delta_helper(&delta, &dir_cache, &inode_index, &inode_paths);
                    }
                    if let Some(new_vclock) = response.server_vclock {
                        let mut vclock_guard =
                            client_vclock.write().expect("client_vclock lock poisoned");
                        *vclock_guard = proto_to_vec_vclock(&new_vclock);
                    }
                }
                Err(e) => {
                    warn!("Initial sync failed: {}", e);
                }
            }

            let mut interval = tokio::time::interval(Duration::from_secs(10));
            let mut backoff = Duration::from_secs(1);
            let max_backoff = Duration::from_secs(60);
            let mut iteration = 0;

            loop {
                interval.tick().await;
                iteration += 1;
                info!("Delta Sync iteration {} starting", iteration);

                match do_pull_and_apply_deltas(
                    &client,
                    &client_id_str,
                    &dir_cache,
                    &inode_index,
                    &inode_paths,
                    &client_vclock,
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

            for delta in response.deltas {
                self.apply_delta(&delta);
            }

            if let Some(server_vclock) = response.server_vclock {
                let new_vclock = proto_to_vec_vclock(&server_vclock);
                let mut client_vclock = self
                    .client_vclock
                    .write()
                    .expect("client_vclock lock poisoned");
                *client_vclock = new_vclock;
            }
        }

        Ok(())
    }

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
                        if op.size != 0 {
                            entry.size = op.size;
                        }
                        if op.mtime != 0 {
                            entry.mtime = op.mtime;
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
        file_type,
        mode: mode_val,
        size,
        mtime,
        atime,
        ctime,
        parent_ino,
        chunks,
        symlink_target: if proto.symlink_target.is_empty() {
            None
        } else {
            Some(proto.symlink_target.clone())
        },
    }
}

/// DirEntry → proto Entry（用于 Master 同步）
pub fn dir_entry_to_proto(
    entry: &DirEntry,
    parent_path: &str,
) -> powerfs_master::proto::powerfs::Entry {
    let nlink = if entry.file_type == FileType::Directory {
        2u32
    } else {
        1u32
    };

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
            nlink,
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            rdev: 0,
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
        _ => FileType::RegularFile,
    };

    DirEntry {
        id,
        inode: entry.inode,
        file_type,
        mode: entry.mode,
        size: entry.size,
        mtime: entry.mtime,
        atime: entry.atime,
        ctime: entry.ctime,
        parent_ino: entry.parent_ino,
        chunks: vec![],
        symlink_target: if entry.symlink_target.is_empty() {
            None
        } else {
            Some(entry.symlink_target.clone())
        },
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

fn proto_to_vec_vclock(proto: &powerfs_master::proto::powerfs::VectorClock) -> VectorClock {
    let mut vclock = VectorClock::new();
    for entry in &proto.entries {
        vclock.observe(entry.client_id, entry.seq);
    }
    vclock
}

async fn do_pull_and_apply_deltas(
    client: &SyncFuseClient,
    client_id_str: &str,
    dir_cache: &Arc<RwLock<DirCache>>,
    inode_index: &Arc<RwLock<HashMap<u64, (u64, EntryId)>>>,
    inode_paths: &Arc<RwLock<HashMap<u64, String>>>,
    client_vclock: &Arc<RwLock<VectorClock>>,
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
        info!("pull_delta received {} deltas", response.deltas.len());

        for delta in response.deltas {
            apply_delta_helper(&delta, dir_cache, inode_index, inode_paths);
        }

        if let Some(server_vclock) = response.server_vclock {
            let new_vclock = proto_to_vec_vclock(&server_vclock);
            let mut client_vclock = client_vclock.write().expect("client_vclock lock poisoned");
            *client_vclock = new_vclock;
        }
    }

    Ok(())
}

fn apply_delta_helper(
    delta: &powerfs_master::proto::powerfs::DeltaOp,
    dir_cache: &Arc<RwLock<DirCache>>,
    inode_index: &Arc<RwLock<HashMap<u64, (u64, EntryId)>>>,
    inode_paths: &Arc<RwLock<HashMap<u64, String>>>,
) {
    match &delta.op {
        Some(powerfs_master::proto::powerfs::delta_op::Op::Add(entry)) => {
            if let Some(id) = &entry.id {
                let dir_entry = proto_dir_entry_to_local(entry);
                let dir_ino = entry.parent_ino;

                let orset_arc = {
                    let cache = dir_cache.read().expect("dir_cache lock poisoned");
                    if let Some(arc) = cache.get(&dir_ino) {
                        arc.clone()
                    } else {
                        let mut cache = dir_cache.write().expect("dir_cache lock poisoned");
                        let arc = Arc::new(RwLock::new(DirORSet::new(dir_ino)));
                        cache.insert(dir_ino, arc.clone());
                        arc
                    }
                };

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
            }
        }
        Some(powerfs_master::proto::powerfs::delta_op::Op::Remove(id)) => {
            let entry_id = EntryId::new(id.name.clone(), id.client_id, id.seq);

            let mut index = inode_index.write().expect("inode_index lock poisoned");
            let inode_to_remove: Option<u64> = index
                .iter()
                .find(|(_, (_, eid))| eid == &entry_id)
                .map(|(&ino, _)| ino);

            if let Some(ino) = inode_to_remove {
                if let Some((dir_ino, eid)) = index.remove(&ino) {
                    let orset_arc = {
                        let cache = dir_cache.read().expect("dir_cache lock poisoned");
                        cache.get(&dir_ino).cloned()
                    };
                    if let Some(arc) = orset_arc {
                        let mut orset = arc.write().expect("orset lock poisoned");
                        orset.remove(&eid);
                    }

                    inode_paths
                        .write()
                        .expect("inode_paths lock poisoned")
                        .remove(&ino);
                }
            }
        }
        Some(powerfs_master::proto::powerfs::delta_op::Op::Rename(op)) => {
            if let (Some(old_id), Some(new_entry)) = (&op.old_id, &op.new_entry) {
                if let Some(new_id) = &new_entry.id {
                    let old_entry_id =
                        EntryId::new(old_id.name.clone(), old_id.client_id, old_id.seq);

                    let mut index = inode_index.write().expect("inode_index lock poisoned");
                    let inode_to_rename: Option<u64> = index
                        .iter()
                        .find(|(_, (_, eid))| eid == &old_entry_id)
                        .map(|(&ino, _)| ino);

                    if let Some(ino) = inode_to_rename {
                        if let Some((old_dir_ino, _)) = index.get(&ino) {
                            let orset_arc_old = {
                                let cache = dir_cache.read().expect("dir_cache lock poisoned");
                                cache.get(old_dir_ino).cloned()
                            };
                            if let Some(arc) = orset_arc_old {
                                let mut orset_old = arc.write().expect("orset lock poisoned");
                                orset_old.remove(&old_entry_id);
                            }
                        }

                        let dir_entry = proto_dir_entry_to_local(new_entry);
                        let new_dir_ino = new_entry.parent_ino;

                        let orset_arc_new = {
                            let cache = dir_cache.read().expect("dir_cache lock poisoned");
                            if let Some(arc) = cache.get(&new_dir_ino) {
                                arc.clone()
                            } else {
                                let mut cache = dir_cache.write().expect("dir_cache lock poisoned");
                                let arc = Arc::new(RwLock::new(DirORSet::new(new_dir_ino)));
                                cache.insert(new_dir_ino, arc.clone());
                                arc
                            }
                        };
                        let mut orset_new = orset_arc_new.write().expect("orset lock poisoned");
                        orset_new.add(dir_entry.clone());

                        index.insert(
                            ino,
                            (
                                new_dir_ino,
                                EntryId::new(new_id.name.clone(), new_id.client_id, new_id.seq),
                            ),
                        );

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
                let orset_arc = {
                    let cache = dir_cache.read().expect("dir_cache lock poisoned");
                    cache.get(dir_ino).cloned()
                };
                if let Some(arc) = orset_arc {
                    let mut orset = arc.write().expect("orset lock poisoned");
                    if let Some(entry) = orset.entries.get_mut(entry_id) {
                        if op.mode != 0 {
                            entry.mode = op.mode;
                        }
                        if op.size != 0 {
                            entry.size = op.size;
                        }
                        if op.mtime != 0 {
                            entry.mtime = op.mtime;
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
        let dir_cache = self.dir_cache.read().unwrap();
        dir_cache.get(&dir_ino).map(|arc| {
            let orset = arc.read().unwrap();
            orset.vclock.clone()
        })
    }

    pub fn dir_orset_len(&self, dir_ino: u64) -> usize {
        let dir_cache = self.dir_cache.read().unwrap();
        if let Some(arc) = dir_cache.get(&dir_ino) {
            let orset = arc.read().unwrap();
            orset.len()
        } else {
            0
        }
    }

    pub fn inode_index_size(&self) -> usize {
        self.inode_index.read().unwrap().len()
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
            .create(ROOT_INO, "test.txt", 0o644 | libc::S_IFREG)
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
            .mkdir(ROOT_INO, "subdir", 0o755 | libc::S_IFDIR)
            .unwrap();
        assert_eq!(entry.id.name, "subdir");
        assert_eq!(entry.file_type, FileType::Directory);

        // 新目录应有空 OR-Set
        assert_eq!(mgr.dir_orset_len(entry.inode), 0);

        // 在新目录中创建文件
        let file_entry = mgr
            .create(entry.inode, "inner.txt", 0o644 | libc::S_IFREG)
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

        mgr.create(ROOT_INO, "to_delete.txt", 0o644 | libc::S_IFREG)
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
        mgr.mkdir(ROOT_INO, "adir", 0o755 | libc::S_IFDIR).unwrap();

        let result = mgr.unlink(ROOT_INO, "adir");
        assert!(matches!(result, Err(FsError::IsDirectory(_))));
    }

    #[test]
    fn test_rmdir() {
        let mgr = create_mgr();
        mgr.mkdir(ROOT_INO, "to_rmdir", 0o755 | libc::S_IFDIR)
            .unwrap();
        assert_eq!(mgr.dir_orset_len(ROOT_INO), 1);

        mgr.rmdir(ROOT_INO, "to_rmdir").unwrap();
        assert_eq!(mgr.dir_orset_len(ROOT_INO), 0);
    }

    #[test]
    fn test_rmdir_not_empty_fails() {
        let mgr = create_mgr();
        let dir_entry = mgr
            .mkdir(ROOT_INO, "nonempty", 0o755 | libc::S_IFDIR)
            .unwrap();
        mgr.create(dir_entry.inode, "child.txt", 0o644 | libc::S_IFREG)
            .unwrap();

        let result = mgr.rmdir(ROOT_INO, "nonempty");
        assert!(matches!(result, Err(FsError::NotEmpty(_))));
    }

    #[test]
    fn test_rmdir_on_file_fails() {
        let mgr = create_mgr();
        mgr.create(ROOT_INO, "afile.txt", 0o644 | libc::S_IFREG)
            .unwrap();

        let result = mgr.rmdir(ROOT_INO, "afile.txt");
        assert!(matches!(result, Err(FsError::NotDirectory(_))));
    }

    #[test]
    fn test_rename_file() {
        let mgr = create_mgr();

        mgr.create(ROOT_INO, "old_name.txt", 0o644 | libc::S_IFREG)
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

        let dir1 = mgr.mkdir(ROOT_INO, "dir1", 0o755 | libc::S_IFDIR).unwrap();
        let dir2 = mgr.mkdir(ROOT_INO, "dir2", 0o755 | libc::S_IFDIR).unwrap();

        mgr.create(dir1.inode, "mover.txt", 0o644 | libc::S_IFREG)
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
            .mkdir(ROOT_INO, "old_dir", 0o755 | libc::S_IFDIR)
            .unwrap();
        mgr.create(dir.inode, "child.txt", 0o644 | libc::S_IFREG)
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
            .create(ROOT_INO, "chmod.txt", 0o644 | libc::S_IFREG)
            .unwrap();

        let updated = mgr
            .setattr(entry.inode, Some(0o600 | libc::S_IFREG), None, None)
            .unwrap();
        assert_eq!(updated.mode, 0o600 | libc::S_IFREG);
    }

    #[test]
    fn test_setattr_size() {
        let mgr = create_mgr();
        let entry = mgr
            .create(ROOT_INO, "resize.txt", 0o644 | libc::S_IFREG)
            .unwrap();
        assert_eq!(entry.size, 0);

        let updated = mgr.setattr(entry.inode, None, Some(1024), None).unwrap();
        assert_eq!(updated.size, 1024);
    }

    #[test]
    fn test_setattr_mtime() {
        let mgr = create_mgr();
        let entry = mgr
            .create(ROOT_INO, "mtime.txt", 0o644 | libc::S_IFREG)
            .unwrap();

        let updated = mgr
            .setattr(entry.inode, None, None, Some(1234567890))
            .unwrap();
        assert_eq!(updated.mtime, 1234567890);
    }

    #[test]
    fn test_setattr_not_found() {
        let mgr = create_mgr();
        let result = mgr.setattr(99999, Some(0o644), None, None);
        assert!(matches!(result, Err(FsError::NotFound(_))));
    }

    #[test]
    fn test_get_entry_by_inode() {
        let mgr = create_mgr();
        let entry = mgr
            .create(ROOT_INO, "getattr.txt", 0o644 | libc::S_IFREG)
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
            .mkdir(ROOT_INO, "parent_test", 0o755 | libc::S_IFDIR)
            .unwrap();
        let file = mgr
            .create(dir.inode, "child.txt", 0o644 | libc::S_IFREG)
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

        mgr.create(ROOT_INO, "a.txt", 0o644 | libc::S_IFREG)
            .unwrap();
        mgr.create(ROOT_INO, "b.txt", 0o644 | libc::S_IFREG)
            .unwrap();
        mgr.mkdir(ROOT_INO, "c_dir", 0o755 | libc::S_IFDIR).unwrap();

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
            .create(ROOT_INO, "f1.txt", 0o644 | libc::S_IFREG)
            .unwrap();
        let e2 = mgr
            .create(ROOT_INO, "f2.txt", 0o644 | libc::S_IFREG)
            .unwrap();
        let e3 = mgr
            .create(ROOT_INO, "f3.txt", 0o644 | libc::S_IFREG)
            .unwrap();

        assert_eq!(e2.inode, e1.inode + 1);
        assert_eq!(e3.inode, e2.inode + 1);
    }

    #[test]
    fn test_seq_counter_increments() {
        let mgr = create_mgr();

        let e1 = mgr
            .create(ROOT_INO, "s1.txt", 0o644 | libc::S_IFREG)
            .unwrap();
        let e2 = mgr
            .create(ROOT_INO, "s2.txt", 0o644 | libc::S_IFREG)
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
            .create(ROOT_INO, "lifecycle.txt", 0o644 | libc::S_IFREG)
            .unwrap();
        assert!(mgr.lookup(ROOT_INO, "lifecycle.txt").unwrap().is_some());

        mgr.setattr(entry.inode, None, Some(2048), None).unwrap();
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
                },
            )),
            vclock: Some(powerfs_master::proto::powerfs::VectorClock { entries: vec![] }),
        };
        mgr.apply_delta(&setattr_delta);

        let entry = mgr.get_entry_by_inode(100).unwrap().unwrap();
        assert_eq!(entry.mode, 0o755);
        assert_eq!(entry.size, 200);
        assert_eq!(entry.mtime, 2000);
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
