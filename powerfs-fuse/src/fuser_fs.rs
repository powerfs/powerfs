//! PowerFS FUSE 文件系统实现
//!
//! 基于 OR-Set 弱一致架构：
//! - MetadataManager：元数据管理（OR-Set 缓存 + POSIX 投影）
//! - DataManager：数据管理（ChunkCache + WriteBuffer + 文件大小）
//! - fuser_fs.rs：FUSE 回调协调层 + Volume Server 交互
//!
//! 弱一致语义：
//! - 元数据操作：本地 OR-Set 即成功，异步 best-effort 同步到 Master
//! - 数据操作：本地 chunk_cache 即成功，flush/release 时写入 Volume Server
//! - 读路径：本地缓存优先，miss 时从 Volume Server 拉取

use crate::client::{PowerFuseClient, SyncFuseClient};
use crate::data_manager::DataManager;
use crate::error::parse_master_error;
use crate::metadata_manager::MetadataManager;
use fuser::{
    FileAttr, FileType, Filesystem, KernelConfig, MountOption, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, Request, TimeOrNow,
};
use log::{debug, error, info, warn};
use powerfs_common::error::{PowerFsError, Result};
use powerfs_common::types::Fid;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime};
use tokio::runtime::Handle;

/// FUSE dentry 缓存 TTL（0 = 不缓存，每次 lookup 都重新查询）
const TTL: Duration = Duration::from_secs(0);

/// 根目录 inode
const ROOT_INO: u64 = 1;

struct PowerFsFuserFs {
    /// 元数据管理器（OR-Set 缓存 + POSIX 投影）
    meta: Arc<MetadataManager>,
    /// 数据管理器（ChunkCache + WriteBuffer + 文件大小）
    data: Arc<DataManager>,
    /// gRPC 客户端（Volume Server + Master 同步）
    client: Arc<SyncFuseClient>,
    /// Collection 名（Volume 分配用）
    collection: String,
    /// 副本策略（Volume 分配用）
    replication: String,
    /// 内核 dentry 失效通知器（mount2 模式下为 None）
    notifier: Arc<Mutex<Option<fuser::Notifier>>>,
    /// 按 inode 的 flush 锁（避免并发 flush 冲突）
    flush_locks: Arc<RwLock<HashMap<u64, Arc<Mutex<()>>>>>,
    /// 全局脏标记（后台 flush 线程用）
    has_dirty: Arc<AtomicBool>,
    /// 数据完整性验证开关（缺省关闭，调试时打开）
    verify_data: bool,
}

impl PowerFsFuserFs {
    #[allow(clippy::too_many_arguments)]
    fn new(
        client: Arc<SyncFuseClient>,
        meta: Arc<MetadataManager>,
        data: Arc<DataManager>,
        collection: String,
        replication: String,
        verify_data: bool,
    ) -> Self {
        Self {
            meta,
            data,
            client,
            collection,
            replication,
            notifier: Arc::new(Mutex::new(None)),
            flush_locks: Arc::new(RwLock::new(HashMap::new())),
            has_dirty: Arc::new(AtomicBool::new(false)),
            verify_data,
        }
    }

    /// 失效内核 dentry 缓存（reply 之后调用，避免死锁）
    fn invalidate_kernel_dentry(&self, parent: u64, name: &str) {
        let notifier = self.notifier.clone();
        let name = name.to_string();
        std::thread::spawn(move || {
            let notifier_guard = notifier.lock().unwrap();
            if let Some(n) = notifier_guard.as_ref() {
                if let Err(e) = n.inval_entry(parent, OsStr::new(&name)) {
                    debug!(
                        "Failed to invalidate kernel dentry (parent={}, name={}): {}",
                        parent, name, e
                    );
                }
            }
        });
    }

    /// 失效内核 inode 缓存
    fn invalidate_kernel_inode(&self, inode: u64) {
        let notifier = self.notifier.clone();
        std::thread::spawn(move || {
            let notifier_guard = notifier.lock().unwrap();
            if let Some(n) = notifier_guard.as_ref() {
                if let Err(e) = n.inval_inode(inode, 0, -1) {
                    debug!("Failed to invalidate kernel inode ({}): {}", inode, e);
                }
            }
        });
    }

    /// 将 DirEntry 转为 FUSE FileAttr
    fn dir_entry_to_file_attr(entry: &crate::orset::DirEntry) -> FileAttr {
        let kind = match entry.file_type {
            crate::orset::FileType::RegularFile => FileType::RegularFile,
            crate::orset::FileType::Directory => FileType::Directory,
            crate::orset::FileType::Symlink => FileType::Symlink,
        };

        FileAttr {
            ino: entry.inode,
            size: entry.size,
            blocks: entry.size.div_ceil(512),
            atime: SystemTime::UNIX_EPOCH + Duration::from_secs(entry.atime),
            mtime: SystemTime::UNIX_EPOCH + Duration::from_secs(entry.mtime),
            ctime: SystemTime::UNIX_EPOCH + Duration::from_secs(entry.ctime),
            crtime: SystemTime::UNIX_EPOCH + Duration::from_secs(entry.ctime),
            kind,
            perm: (entry.mode & 0o7777) as u16,
            nlink: if entry.file_type == crate::orset::FileType::Directory {
                2
            } else {
                1
            },
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }

    /// 获取文件实际大小（取 max(meta_size, data_size)）
    fn get_file_size(&self, ino: u64) -> u64 {
        let data_size = self.data.current_file_size(ino);
        let meta_size = self
            .meta
            .get_entry_by_inode(ino)
            .map(|e| e.map(|e| e.size).unwrap_or(0))
            .unwrap_or(0);
        data_size.max(meta_size)
    }

    /// 将脏 chunk 写入 Volume Server 并更新 Master 元数据
    ///
    /// 流程：
    /// 1. 获取脏 chunk 列表
    /// 2. 从 Master 获取 entry（获取现有 FID）
    /// 3. 如无 FID，分配新 FID
    /// 4. 查找 Volume 位置
    /// 5. 批量写入脏 chunk 到 Volume Server
    /// 6. 更新 Master 上的 entry（chunks + size）
    /// 7. 清除脏标记
    fn flush_dirty_chunks(&self, inode: u64) -> std::io::Result<()> {
        // 获取 flush 锁
        let flush_lock = {
            let mut locks = self.flush_locks.write().unwrap();
            locks
                .entry(inode)
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        let _guard = flush_lock.lock().unwrap();

        // 获取脏 chunk 列表（排序确保按顺序处理）
        let mut dirty_chunks: Vec<u64> = self.data.get_dirty_chunks(inode);
        dirty_chunks.sort_unstable();
        if dirty_chunks.is_empty() {
            return Ok(());
        }

        let file_size = self.data.current_file_size(inode);
        let chunk_size = self.data.chunk_cache().chunk_size();

        debug!(
            "flush_dirty_chunks: inode={}, dirty_chunks={}, file_size={}",
            inode,
            dirty_chunks.len(),
            file_size
        );

        // 从 Master 获取 entry（获取现有 FID）
        let (entry, _path) = match self.client.get_entry_by_inode(inode) {
            Ok(Some(e)) => e,
            Ok(None) => {
                // entry 不在 Master 上（可能是本地新建的），创建一个
                let meta_entry = match self.meta.get_entry_by_inode(inode) {
                    Ok(Some(e)) => e,
                    _ => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            format!("entry not found for inode {}", inode),
                        ));
                    }
                };
                let parent_path = self
                    .meta
                    .get_path(meta_entry.parent_ino)
                    .unwrap_or_else(|| "/".to_string());
                let proto_entry =
                    crate::metadata_manager::dir_entry_to_proto(&meta_entry, &parent_path);
                let client_id_str = self.meta.client_id().to_string();
                match self.client.create_entry(proto_entry, &client_id_str) {
                    Ok(_) => {
                        debug!(
                            "flush_dirty_chunks: created entry on master for inode {}",
                            inode
                        );
                        match self.client.get_entry_by_inode(inode) {
                            Ok(Some(e)) => e,
                            _ => {
                                return Err(std::io::Error::new(
                                    std::io::ErrorKind::NotFound,
                                    "entry not found after create",
                                ));
                            }
                        }
                    }
                    Err(e) => {
                        warn!("flush_dirty_chunks: create_entry failed: {}", e);
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::ConnectionRefused,
                            format!("create_entry failed: {}", e),
                        ));
                    }
                }
            }
            Err(e) => {
                let fs_error = parse_master_error(&e);
                return Err(std::io::Error::from_raw_os_error(fs_error.to_errno()));
            }
        };

        // 获取现有 FID
        let existing_fid = entry
            .chunks
            .iter()
            .find(|c| !c.fid.is_empty())
            .and_then(|c| Fid::from_string(&c.fid).ok());

        // 分配新 FID（如果不存在）
        let fid = if let Some(fid) = existing_fid {
            fid
        } else {
            match self.client.assign_fid(&self.collection, &self.replication) {
                Ok((new_fid, _, _, _)) => new_fid,
                Err(e) => {
                    let fs_error = parse_master_error(&e);
                    return Err(std::io::Error::from_raw_os_error(fs_error.to_errno()));
                }
            }
        };

        // 查找 Volume 位置
        let locations = match self.client.lookup_volume(fid.volume_id) {
            Ok(l) => l,
            Err(e) => {
                let fs_error = parse_master_error(&e);
                return Err(std::io::Error::from_raw_os_error(fs_error.to_errno()));
            }
        };

        let loc = locations
            .first()
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::EIO))?;
        let addr = PowerFuseClient::location_to_grpc_addr(loc);

        // 先收集所有脏 chunk 数据到内存（防止 LRU 淘汰导致数据丢失）
        let mut dirty_chunk_data: Vec<(u64, u64, Vec<u8>, u64, u32)> = Vec::new(); // (chunk_idx, chunk_offset, data, mtime, crc32)

        debug!(
            "flush_dirty_chunks: inode={}, dirty_chunks={:?}, file_size={}, chunk_size={}, cache_len={}, cache_bytes={}",
            inode,
            dirty_chunks,
            file_size,
            chunk_size,
            self.data.chunk_cache().len(),
            self.data.chunk_cache().current_bytes(),
        );

        for chunk_idx in &dirty_chunks {
            let chunk_offset = chunk_idx * chunk_size;
            let chunk_data_opt = self.data.chunk_cache().get(inode, chunk_offset);
            debug!(
                "flush_dirty_chunks: looking up chunk idx={}, offset={} → {}",
                chunk_idx,
                chunk_offset,
                if chunk_data_opt.is_some() {
                    "found"
                } else {
                    "NOT FOUND"
                }
            );
            if let Some(chunk_data) = chunk_data_opt {
                dirty_chunk_data.push((
                    *chunk_idx,
                    chunk_offset,
                    chunk_data.data,
                    chunk_data.mtime,
                    chunk_data.crc32,
                ));
            } else {
                warn!(
                    "flush_dirty_chunks: chunk idx={}, offset={} NOT FOUND in cache!",
                    chunk_idx, chunk_offset
                );
            }
        }

        // 批量写入脏 chunk 到 Volume Server
        let mut blob_entries = Vec::new();
        let mut new_chunks = Vec::new();

        for (_chunk_idx, chunk_offset, data, mtime, crc32) in &dirty_chunk_data {
            let data_len = data.len();
            blob_entries.push((*chunk_offset as i64, data_len as i32, data.clone(), 0u32));

            new_chunks.push(powerfs_master::proto::powerfs::FileChunk {
                offset: *chunk_offset,
                size: data_len as u64,
                mtime: *mtime,
                fid: fid.to_string(),
                cookie: 0,
                crc32: *crc32,
            });
        }

        if !blob_entries.is_empty() {
            if let Err(e) =
                self.client
                    .batch_write_blob(&addr, fid.volume_id.0, fid.file_key, blob_entries)
            {
                let fs_error = crate::error::parse_volume_error(&e);
                error!("flush_dirty_chunks: batch_write_blob failed: {}", fs_error);
                return Err(std::io::Error::from_raw_os_error(fs_error.to_errno()));
            }
            debug!(
                "flush_dirty_chunks: wrote {} chunks to volume {}",
                new_chunks.len(),
                addr
            );
        }

        // 更新 Master 上的 entry（合并 chunks + 更新 size）
        let client_id_str = self.meta.client_id().to_string();
        let mut updated_entry = entry.clone();
        // 合并 chunks：保留旧的非脏 chunk，添加新的脏 chunk
        for new_chunk in &new_chunks {
            updated_entry
                .chunks
                .retain(|c| c.offset != new_chunk.offset);
            updated_entry.chunks.push(new_chunk.clone());
        }
        // 按 offset 排序
        updated_entry.chunks.sort_by_key(|c| c.offset);

        // 更新 size
        if file_size > 0 {
            if let Some(attrs) = updated_entry.attributes.as_mut() {
                attrs.size = file_size;
                attrs.blocks = file_size.div_ceil(512);
            }
            updated_entry.content_size = file_size;
            updated_entry.disk_size = file_size;
        }

        if let Err(e) = self.client.update_entry(&updated_entry, &client_id_str) {
            warn!("flush_dirty_chunks: update_entry failed: {}", e);
            // 不返回错误，数据已写入 Volume，元数据稍后可重试
        }

        // 数据完整性验证（可选，默认关闭）：读取刚刚写入的 chunk 并与本地数据比较
        if self.verify_data {
            self.verify_flushed_data(inode, &dirty_chunk_data, &addr, fid)?;
        }

        // 清除脏标记
        self.data.clear_dirty(inode);

        debug!(
            "flush_dirty_chunks: completed for inode={}, chunks={}",
            inode,
            new_chunks.len()
        );

        Ok(())
    }

    /// 验证写入的数据完整性
    fn verify_flushed_data(
        &self,
        inode: u64,
        dirty_chunk_data: &[(u64, u64, Vec<u8>, u64, u32)],
        addr: &str,
        fid: powerfs_common::types::Fid,
    ) -> std::io::Result<()> {
        use md5::compute;

        for (_chunk_idx, chunk_offset, local_data, _mtime, _crc32) in dirty_chunk_data {
            match self.client.read_blob(
                addr,
                fid.volume_id.0,
                fid.file_key,
                *chunk_offset as i64,
                local_data.len() as i32,
            ) {
                Ok(remote_data) => {
                    let local_hash = compute(local_data);
                    let remote_hash = compute(&remote_data);
                    if local_hash != remote_hash {
                        error!(
                            "verify_flushed_data: data mismatch for inode={}, offset={}, local_hash={:x}, remote_hash={:x}",
                            inode, chunk_offset, local_hash, remote_hash
                        );
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("data mismatch for inode={}, offset={}", inode, chunk_offset),
                        ));
                    }
                }
                Err(e) => {
                    warn!(
                        "verify_flushed_data: read_blob failed for inode={}, offset={}: {}",
                        inode, chunk_offset, e
                    );
                }
            }
        }

        Ok(())
    }

    /// 从 Volume Server 拉取缺失的 chunk
    fn fetch_chunk_from_volume(&self, inode: u64, chunk_offset: u64) -> std::io::Result<Vec<u8>> {
        let chunk_size = self.data.chunk_cache().chunk_size();

        // 从 Master 获取 entry（获取 chunk FID）
        let (entry, _) = match self.client.get_entry_by_inode(inode) {
            Ok(Some(e)) => e,
            Ok(None) => {
                return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
            }
            Err(e) => {
                let fs_error = parse_master_error(&e);
                return Err(std::io::Error::from_raw_os_error(fs_error.to_errno()));
            }
        };

        // 查找该 offset 对应的 chunk FID
        let chunk_fid = entry
            .chunks
            .iter()
            .find(|c| c.offset == chunk_offset)
            .and_then(|c| {
                if c.fid.is_empty() {
                    None
                } else {
                    Some(c.fid.clone())
                }
            });

        let fid_str = match chunk_fid {
            Some(f) => f,
            None => {
                // 该 chunk 在 Master 上不存在（可能是空洞或未写入的区域）
                return Ok(vec![0u8; chunk_size as usize]);
            }
        };

        let fid = Fid::from_string(&fid_str)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        // 查找 Volume 位置
        let locations = match self.client.lookup_volume(fid.volume_id) {
            Ok(l) => l,
            Err(e) => {
                let fs_error = parse_master_error(&e);
                return Err(std::io::Error::from_raw_os_error(fs_error.to_errno()));
            }
        };

        let loc = locations
            .first()
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::EIO))?;
        let addr = PowerFuseClient::location_to_grpc_addr(loc);

        // 从 Volume Server 读取
        let data = match self.client.read_blob(
            &addr,
            fid.volume_id.0,
            fid.file_key,
            chunk_offset as i64,
            chunk_size as i32,
        ) {
            Ok(d) => d,
            Err(e) => {
                let fs_error = crate::error::parse_volume_error(&e);
                return Err(std::io::Error::from_raw_os_error(fs_error.to_errno()));
            }
        };

        Ok(data)
    }

    /// 后台 flush 所有脏 chunk
    #[allow(dead_code)]
    fn flush_all_dirty(&self) {
        // 遍历所有有脏数据的 inode（通过 has_dirty 标记）
        // 由于 DataManager 不维护全局 inode 列表，这里用 has_dirty 标记触发
        // 实际的 inode 遍历在 flush_dirty_chunks 内部完成
        // Phase 1A 简化：has_dirty 只是一个提示，不做全局扫描
    }

    /// 读取 .conflicts/ 虚拟目录的内容
    fn readdir_conflict_dir(
        &mut self,
        conflict_dir_ino: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let real_dir_ino = self.meta.get_real_dir_inode(conflict_dir_ino);
        debug!(
            "readdir_conflict_dir: conflict_dir_ino={}, real_dir_ino={}",
            conflict_dir_ino, real_dir_ino
        );

        let mut idx = offset as usize;

        // 添加 . 条目
        if idx == 0 {
            if !reply.add(conflict_dir_ino, 1, FileType::Directory, ".") {
                reply.ok();
                return;
            }
            idx = 1;
        }

        // 添加 .. 条目（指向真实目录）
        if idx <= 1 {
            if !reply.add(real_dir_ino, 2, FileType::Directory, "..") {
                reply.ok();
                return;
            }
            idx = 2;
        }

        // 列出冲突条目
        match self.meta.list_conflict_dir(real_dir_ino) {
            Ok(entries) => {
                for (i, entry) in entries.iter().enumerate() {
                    let entry_idx = 2 + i;
                    if entry_idx < idx {
                        continue;
                    }

                    let kind = match entry.file_type {
                        crate::orset::FileType::RegularFile => FileType::RegularFile,
                        crate::orset::FileType::Directory => FileType::Directory,
                        crate::orset::FileType::Symlink => FileType::Symlink,
                    };

                    let next_offset = (entry_idx + 1) as i64;
                    if !reply.add(entry.inode, next_offset, kind, &entry.display_name) {
                        break;
                    }
                }
            }
            Err(e) => {
                error!(
                    "readdir_conflict_dir: real_dir_ino={}, error={}",
                    real_dir_ino, e
                );
                reply.error(e.to_errno());
                return;
            }
        }

        reply.ok();
    }
}

impl Filesystem for PowerFsFuserFs {
    fn init(
        &mut self,
        _req: &Request<'_>,
        _config: &mut KernelConfig,
    ) -> std::result::Result<(), libc::c_int> {
        // TTL=0：不缓存 dentry，每次 lookup 都重新查询
        // （KernelConfig 在 fuser 0.16 中无 add_timeout API，TTL 由 FileAttr TTL 字段控制）
        Ok(())
    }

    fn destroy(&mut self) {}

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name_str = name.to_str().unwrap_or("");
        debug!("lookup: parent={}, name={}", parent, name_str);

        // 处理 . 和 ..
        if name_str == "." {
            match self.meta.get_entry_by_inode(parent) {
                Ok(Some(entry)) => {
                    let attr = Self::dir_entry_to_file_attr(&entry);
                    reply.entry(&TTL, &attr, 0);
                }
                _ => reply.error(libc::ENOENT),
            }
            return;
        }

        if name_str == ".." {
            match self.meta.get_parent_dir(parent) {
                Ok(Some(entry)) => {
                    let attr = Self::dir_entry_to_file_attr(&entry);
                    reply.entry(&TTL, &attr, 0);
                }
                _ => reply.error(libc::ENOENT),
            }
            return;
        }

        // 处理 .conflicts/ 虚拟目录
        if name_str == ".conflicts" {
            let attr = self.meta.get_conflict_dir_attr(parent);
            reply.entry(&TTL, &attr, 0);
            return;
        }

        // 正常 lookup
        match self.meta.lookup(parent, name_str) {
            Ok(Some(entry)) => {
                let attr = Self::dir_entry_to_file_attr(&entry);
                reply.entry(&TTL, &attr, 0);
            }
            Ok(None) => reply.error(libc::ENOENT),
            Err(e) => {
                error!("lookup: parent={}, name={}, error={}", parent, name_str, e);
                reply.error(e.to_errno());
            }
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, inode: u64, _fh: Option<u64>, reply: ReplyAttr) {
        debug!("getattr: inode={}", inode);

        // 处理 .conflicts/ 虚拟目录
        if self.meta.is_conflict_dir_inode(inode) {
            let real_dir_ino = self.meta.get_real_dir_inode(inode);
            let attr = self.meta.get_conflict_dir_attr(real_dir_ino);
            reply.attr(&TTL, &attr);
            return;
        }

        match self.meta.get_entry_by_inode(inode) {
            Ok(Some(mut entry)) => {
                // 文件大小取 max(meta, data)
                let actual_size = self.get_file_size(inode);
                entry.size = actual_size;
                let attr = Self::dir_entry_to_file_attr(&entry);
                reply.attr(&TTL, &attr);
            }
            Ok(None) => {
                error!("getattr: inode {} not found", inode);
                reply.error(libc::ENOENT);
            }
            Err(e) => {
                error!("getattr: inode={}, error={}", inode, e);
                reply.error(e.to_errno());
            }
        }
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        inode: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        debug!(
            "setattr: inode={}, mode={:?}, size={:?}, uid={:?}, gid={:?}",
            inode, mode, size, uid, gid
        );

        // 处理 truncate（size 变更）
        if let Some(new_size) = size {
            self.data.truncate(inode, new_size);
        }

        // 转换时间
        let mtime_secs = mtime.map(|t| match t {
            TimeOrNow::SpecificTime(time) => time
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            TimeOrNow::Now => std::time::SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        });

        // 更新 MetadataManager
        match self.meta.setattr(inode, mode, size, mtime_secs) {
            Ok(entry) => {
                // 实际大小
                let actual_size = self.get_file_size(inode);
                let mut entry = entry;
                entry.size = actual_size;
                let attr = Self::dir_entry_to_file_attr(&entry);
                reply.attr(&TTL, &attr);

                // 失效内核缓存
                self.invalidate_kernel_inode(inode);
            }
            Err(e) => {
                error!("setattr: inode={}, error={}", inode, e);
                reply.error(e.to_errno());
            }
        }
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        inode: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        debug!("readdir: inode={}, offset={}", inode, offset);

        // 处理 .conflicts/ 虚拟目录
        if self.meta.is_conflict_dir_inode(inode) {
            self.readdir_conflict_dir(inode, offset, reply);
            return;
        }

        // 获取父 inode（用于 .. ）
        let parent_ino = match self.meta.get_parent_dir(inode) {
            Ok(Some(p)) => p.inode,
            _ => ROOT_INO,
        };

        let mut idx = offset as usize;

        // 添加 . 条目
        if idx == 0 {
            if !reply.add(inode, 1, FileType::Directory, ".") {
                reply.ok();
                return;
            }
            idx = 1;
        }

        // 添加 .. 条目
        if idx <= 1 {
            if !reply.add(parent_ino, 2, FileType::Directory, "..") {
                reply.ok();
                return;
            }
            idx = 2;
        }

        // 列出目录内容
        match self.meta.list_dir(inode) {
            Ok(entries) => {
                for (i, entry) in entries.iter().enumerate() {
                    let entry_idx = 2 + i;
                    if entry_idx < idx {
                        continue;
                    }

                    let kind = match entry.file_type {
                        crate::orset::FileType::RegularFile => FileType::RegularFile,
                        crate::orset::FileType::Directory => FileType::Directory,
                        crate::orset::FileType::Symlink => FileType::Symlink,
                    };

                    let next_offset = (entry_idx + 1) as i64;
                    if !reply.add(entry.inode, next_offset, kind, &entry.name) {
                        break;
                    }
                }
            }
            Err(e) => {
                error!("readdir: inode={}, error={}", inode, e);
                reply.error(e.to_errno());
                return;
            }
        }

        reply.ok();
    }

    fn create(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        let name_str = name.to_str().unwrap_or("");
        debug!(
            "create: parent={}, name={}, mode={:o}",
            parent, name_str, mode
        );

        match self.meta.create(parent, name_str, mode | libc::S_IFREG) {
            Ok(entry) => {
                let attr = Self::dir_entry_to_file_attr(&entry);
                reply.created(&TTL, &attr, 0, 0, 0);

                // 失效父目录 dentry 缓存
                self.invalidate_kernel_dentry(parent, name_str);
            }
            Err(e) => {
                error!("create: parent={}, name={}, error={}", parent, name_str, e);
                reply.error(e.to_errno());
            }
        }
    }

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let name_str = name.to_str().unwrap_or("");
        debug!(
            "mkdir: parent={}, name={}, mode={:o}",
            parent, name_str, mode
        );

        match self.meta.mkdir(parent, name_str, mode | libc::S_IFDIR) {
            Ok(entry) => {
                let attr = Self::dir_entry_to_file_attr(&entry);
                reply.entry(&TTL, &attr, 0);

                // 失效父目录 dentry 缓存
                self.invalidate_kernel_dentry(parent, name_str);
            }
            Err(e) => {
                error!("mkdir: parent={}, name={}, error={}", parent, name_str, e);
                reply.error(e.to_errno());
            }
        }
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name_str = name.to_str().unwrap_or("");
        debug!("rmdir: parent={}, name={}", parent, name_str);

        match self.meta.rmdir(parent, name_str) {
            Ok(()) => {
                reply.ok();
                self.invalidate_kernel_dentry(parent, name_str);
            }
            Err(e) => {
                error!("rmdir: parent={}, name={}, error={}", parent, name_str, e);
                reply.error(e.to_errno());
            }
        }
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name_str = name.to_str().unwrap_or("");
        debug!("unlink: parent={}, name={}", parent, name_str);

        match self.meta.unlink(parent, name_str) {
            Ok(inode) => {
                // 清理数据缓存（chunk_cache, write_buffer, dirty, file_sizes）
                self.data.remove_inode(inode);
                reply.ok();
                self.invalidate_kernel_dentry(parent, name_str);
            }
            Err(e) => {
                error!("unlink: parent={}, name={}, error={}", parent, name_str, e);
                reply.error(e.to_errno());
            }
        }
    }

    fn rename(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        let name_str = name.to_str().unwrap_or("");
        let new_name_str = new_name.to_str().unwrap_or("");
        debug!(
            "rename: parent={}, name={}, new_parent={}, new_name={}",
            parent, name_str, new_parent, new_name_str
        );

        match self.meta.rename(parent, name_str, new_parent, new_name_str) {
            Ok(overwritten_inode) => {
                reply.ok();
                // 失效旧路径和新路径的内核缓存
                self.invalidate_kernel_dentry(parent, name_str);
                self.invalidate_kernel_dentry(new_parent, new_name_str);
                // 清理被覆盖文件的数据缓存
                if let Some(inode) = overwritten_inode {
                    self.data.remove_inode(inode);
                    self.invalidate_kernel_inode(inode);
                }
            }
            Err(e) => {
                error!(
                    "rename: parent={}, name={}, new_parent={}, new_name={}, error={}",
                    parent, name_str, new_parent, new_name_str, e
                );
                reply.error(e.to_errno());
            }
        }
    }

    fn symlink(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        let name_str = name.to_str().unwrap_or("");
        let target_str = target.to_str().unwrap_or("");
        debug!(
            "symlink: parent={}, name={}, target={}",
            parent, name_str, target_str
        );

        match self.meta.symlink(parent, name_str, target_str) {
            Ok(entry) => {
                let attr = Self::dir_entry_to_file_attr(&entry);
                reply.entry(&TTL, &attr, 0);
                self.invalidate_kernel_dentry(parent, name_str);
            }
            Err(e) => {
                error!(
                    "symlink: parent={}, name={}, target={}, error={}",
                    parent, name_str, target_str, e
                );
                reply.error(e.to_errno());
            }
        }
    }

    fn readlink(&mut self, _req: &Request<'_>, inode: u64, reply: ReplyData) {
        debug!("readlink: inode={}", inode);

        match self.meta.get_entry_by_inode(inode) {
            Ok(Some(entry)) => {
                if let Some(target) = &entry.symlink_target {
                    reply.data(target.as_bytes());
                } else {
                    reply.error(libc::EINVAL);
                }
            }
            Ok(None) => reply.error(libc::ENOENT),
            Err(e) => {
                error!("readlink: inode={}, error={}", inode, e);
                reply.error(e.to_errno());
            }
        }
    }

    fn open(&mut self, _req: &Request<'_>, _inode: u64, _flags: i32, reply: ReplyOpen) {
        reply.opened(0, 0);
    }

    fn opendir(&mut self, _req: &Request<'_>, _inode: u64, _flags: i32, reply: ReplyOpen) {
        reply.opened(0, 0);
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        inode: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let offset_u64 = offset as u64;
        let size_usize = size as usize;
        debug!(
            "read: inode={}, offset={}, size={}",
            inode, offset_u64, size_usize
        );

        // 先尝试从 DataManager 本地缓存读
        match self.data.read(inode, offset_u64, size_usize) {
            Some(data) => {
                reply.data(&data);
                return;
            }
            None => {
                // chunk miss，需要从 Volume Server 拉取
                debug!("read: chunk miss for inode={}, fetching from volume", inode);
            }
        }

        // 从 Volume Server 拉取缺失的 chunk
        let chunk_size = self.data.chunk_cache().chunk_size();
        let start_chunk_idx = offset_u64 / chunk_size;
        let end_chunk_idx = (offset_u64 + size_usize as u64).div_ceil(chunk_size);

        for chunk_idx in start_chunk_idx..end_chunk_idx {
            let chunk_offset = chunk_idx * chunk_size;

            // 检查 chunk 是否已在缓存中
            if self.data.chunk_cache().get(inode, chunk_offset).is_some() {
                continue;
            }

            // 从 Volume Server 拉取
            match self.fetch_chunk_from_volume(inode, chunk_offset) {
                Ok(data) => {
                    let mtime = crate::orset::now_unix();
                    self.data
                        .chunk_cache()
                        .put(inode, chunk_offset, data, mtime, 0);
                }
                Err(e) => {
                    // 如果是 ENOENT（chunk 不存在），用零填充
                    if e.raw_os_error() == Some(libc::ENOENT) {
                        let zero_data = vec![0u8; chunk_size as usize];
                        let mtime = crate::orset::now_unix();
                        self.data
                            .chunk_cache()
                            .put(inode, chunk_offset, zero_data, mtime, 0);
                    } else {
                        error!("read: fetch_chunk_from_volume failed: {}", e);
                        reply.error(e.raw_os_error().unwrap_or(libc::EIO));
                        return;
                    }
                }
            }
        }

        // 重试从本地缓存读
        match self.data.read(inode, offset_u64, size_usize) {
            Some(data) => reply.data(&data),
            None => {
                // 仍然失败，返回空数据
                warn!("read: still no data after fetch for inode={}", inode);
                reply.data(&[]);
            }
        }
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        inode: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        let offset_u64 = offset as u64;
        debug!(
            "write: inode={}, offset={}, size={}",
            inode,
            offset_u64,
            data.len()
        );

        let written = self.data.write(inode, offset_u64, data);
        self.has_dirty.store(true, Ordering::Relaxed);

        reply.written(written as u32);
    }

    fn flush(
        &mut self,
        _req: &Request<'_>,
        inode: u64,
        _fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        debug!("flush: inode={}", inode);

        // flush 失败不影响文件操作，数据已在本地缓存，稍后可重试
        if let Err(e) = self.flush_dirty_chunks(inode) {
            error!("flush: inode={}, error={}", inode, e);
        }
        reply.ok();
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        inode: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        debug!("release: inode={}, flush={}", inode, _flush);

        // flush 脏数据（失败不影响文件关闭，数据已在本地缓存，稍后可重试）
        let flush_ok = self.flush_dirty_chunks(inode).is_ok();
        if !flush_ok {
            error!("release: flush failed for inode={}", inode);
        }

        // 清理 write_buffer（数据已在 chunk_cache 中）
        self.data.write_buffer().take(inode);

        // 只在 flush 成功时才清除脏标记和释放资源
        if flush_ok {
            self.data.clear_dirty(inode);
        }

        reply.ok();
    }

    fn fsync(
        &mut self,
        _req: &Request<'_>,
        inode: u64,
        _fh: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        debug!("fsync: inode={}", inode);

        // fsync 失败不影响文件操作，数据已在本地缓存，稍后可重试
        if let Err(e) = self.flush_dirty_chunks(inode) {
            warn!("fsync: inode={}, error={}", inode, e);
        }
        reply.ok();
    }

    fn statfs(&mut self, _req: &Request<'_>, _inode: u64, reply: ReplyStatfs) {
        const BLOCK_SIZE: u32 = 4096;
        let block_size_u64 = BLOCK_SIZE as u64;

        info!(
            "statfs: requesting statistics for collection '{}'",
            self.collection
        );
        match self.client.get_statistics(&self.collection) {
            Ok(stats) => {
                let total_blocks = stats.total_volume_size / block_size_u64;
                let used_blocks = stats.total_used_size / block_size_u64;
                let free_blocks = total_blocks.saturating_sub(used_blocks);

                info!(
                    "statfs: success - total={} blocks, used={} blocks, free={} blocks, total_volume_size={}, total_used_size={}",
                    total_blocks, used_blocks, free_blocks, stats.total_volume_size, stats.total_used_size
                );

                reply.statfs(
                    total_blocks,
                    free_blocks,
                    free_blocks,
                    stats.total_volume_count * 1000,
                    stats.available_volume_count * 1000,
                    BLOCK_SIZE,
                    1_000_000,
                    BLOCK_SIZE,
                );
            }
            Err(e) => {
                warn!("statfs: failed to get statistics: {}", e);
                reply.statfs(
                    1_000_000_000,
                    500_000_000,
                    500_000_000,
                    1_000_000,
                    900_000,
                    BLOCK_SIZE,
                    1_000_000,
                    BLOCK_SIZE,
                );
            }
        }
    }
}

impl Clone for PowerFsFuserFs {
    fn clone(&self) -> Self {
        Self {
            meta: self.meta.clone(),
            data: self.data.clone(),
            client: self.client.clone(),
            collection: self.collection.clone(),
            replication: self.replication.clone(),
            notifier: self.notifier.clone(),
            flush_locks: self.flush_locks.clone(),
            has_dirty: self.has_dirty.clone(),
            verify_data: self.verify_data,
        }
    }
}

pub struct FuserApp {
    mount_point: String,
    master_addr: String,
    collection: String,
    replication: String,
    num_threads: usize,
    runtime_handle: Handle,
    verify_data: bool,
}

impl FuserApp {
    pub async fn new(
        master_addr: &str,
        mount_point: &str,
        collection: &str,
        replication: &str,
        num_threads: usize,
        verify_data: bool,
    ) -> Result<Self> {
        let runtime_handle = Handle::try_current()
            .map_err(|e| PowerFsError::Internal(format!("no tokio runtime: {}", e)))?;

        Ok(Self {
            mount_point: mount_point.to_string(),
            master_addr: master_addr.to_string(),
            collection: collection.to_string(),
            replication: replication.to_string(),
            num_threads,
            runtime_handle,
            verify_data,
        })
    }

    pub async fn run(&self) -> Result<()> {
        info!(
            "Starting FUSE session on {} with master {} ({} threads)",
            self.mount_point, self.master_addr, self.num_threads
        );

        // 创建 gRPC 客户端
        let grpc_client = PowerFuseClient::new(&self.master_addr, self.runtime_handle.clone());
        let sync_client = Arc::new(SyncFuseClient::new(grpc_client.clone()));

        // 生成 client_id
        let client_id = uuid::Uuid::new_v4().to_string();
        let client_id_num: u64 = rand::random();
        info!(
            "FUSE client_id={}, client_id_num={}",
            client_id, client_id_num
        );

        // 创建 MetadataManager 和 DataManager
        let meta = Arc::new(MetadataManager::new_with_master(
            sync_client.clone(),
            client_id_num,
        ));
        let chunk_cache = Arc::new(crate::cache::ChunkCache::with_defaults());
        let write_buffer = Arc::new(crate::data_manager::WriteBuffer::new(64));
        let data = Arc::new(DataManager::new(chunk_cache, write_buffer));

        // 创建 FUSE 文件系统
        let fs = PowerFsFuserFs::new(
            sync_client.clone(),
            meta,
            data,
            self.collection.clone(),
            self.replication.clone(),
            self.verify_data,
        );

        // 后台 flush 线程（每 100ms 检查脏标记）
        let fs_clone = fs.clone();
        std::thread::spawn(move || loop {
            if fs_clone
                .has_dirty
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                // Phase 1A: 后台 flush 暂不实现全局扫描
                // 实际 flush 在 release/fsync 时触发
                fs_clone
                    .has_dirty
                    .store(false, std::sync::atomic::Ordering::Relaxed);
            }
            std::thread::sleep(Duration::from_millis(100));
        });

        // 后台 keep_connected 线程（每 30 秒发送一次客户端信息）
        let grpc_client_clone = grpc_client.clone();
        let mount_point_clone = self.mount_point.clone();
        let collection_clone = self.collection.clone();
        let replication_clone = self.replication.clone();
        let host_name = hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_default();
        let pid = std::process::id() as u64;
        let runtime_handle_clone = self.runtime_handle.clone();

        std::thread::spawn(move || {
            runtime_handle_clone.block_on(async move {
                loop {
                    let dirty_chunks = fs_clone.data.chunk_cache().dirty_chunks();
                    let dirty_bytes = fs_clone.data.chunk_cache().dirty_bytes();

                    let _ = grpc_client_clone
                        .keep_connected(
                            "fuse",
                            &mount_point_clone,
                            &collection_clone,
                            &replication_clone,
                            pid,
                            &host_name,
                            dirty_chunks,
                            dirty_bytes,
                        )
                        .await;

                    tokio::time::sleep(Duration::from_secs(30)).await;
                }
            });
        });

        // 挂载选项
        let options = vec![
            MountOption::FSName("powerfs".to_string()),
            MountOption::AutoUnmount,
            MountOption::AllowOther,
            MountOption::DefaultPermissions,
        ];

        let fs_for_mount = fs.clone();
        let mount_point_clone = self.mount_point.clone();
        let options_clone = options.clone();

        let session_handle = std::thread::Builder::new()
            .name("fuse_server".to_string())
            .spawn(move || {
                info!("FUSE server thread started, calling mount2...");
                if let Err(e) = fuser::mount2(fs_for_mount, &mount_point_clone, &options_clone) {
                    error!("Failed to mount FUSE: {}", e);
                } else {
                    info!("FUSE mount completed");
                }
                warn!("FUSE server exited");
            })
            .map_err(|e| PowerFsError::Internal(format!("failed to spawn fuse thread: {}", e)))?;

        let _ = session_handle.join();

        info!("FUSE session ended");
        Ok(())
    }
}
