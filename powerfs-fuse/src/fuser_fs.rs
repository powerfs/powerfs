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
use crate::file_layout::{
    FileLayout, DEFAULT_STRIPE_COUNT, DEFAULT_STRIPE_SIZE, PROMOTE_THRESHOLD,
};
use crate::flush_manager::{FlushConfig, FlushManager};
use crate::metadata_manager::MetadataManager;
use fuser::{
    FileAttr, FileType, Filesystem, KernelConfig, MountOption, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, Request, TimeOrNow,
};
use log::{debug, error, info, warn};
use powerfs_common::error::{PowerFsError, Result};
use powerfs_common::types::Fid;
use powerfs_master::proto::powerfs::StatisticsResponse;
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
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
    /// 全局脏标记（后台 flush 线程用）
    has_dirty: Arc<AtomicBool>,
    /// 后台 flush 管理器
    flush_manager: Option<Arc<FlushManager>>,
    /// 数据完整性验证开关（缺省关闭，调试时打开）
    verify_data: bool,
    /// statfs 缓存（避免每次调用都访问 Master）
    statfs_cache: Arc<Mutex<Option<StatisticsResponse>>>,
    /// statfs 专用 gRPC 客户端（独立通道，高负载时保证 df 正常工作）
    statfs_client: Option<Arc<SyncFuseClient>>,
    /// 文件级租约管理（inode -> lease_id）
    leases: Arc<RwLock<HashMap<u64, String>>>,
    /// 客户端 ID（用于租约获取）
    client_id: String,
    /// 租约续期线程停止信号
    lease_renewer_running: Arc<AtomicBool>,
    /// inode 级写锁（保证 O_APPEND 原子性）
    write_locks: Arc<RwLock<HashMap<u64, Arc<Mutex<()>>>>>,
    /// 租约 epoch 追踪（inode -> epoch）
    lease_epochs: Arc<RwLock<HashMap<u64, u64>>>,
    /// 租约失效通知线程停止信号
    lease_notifier_running: Arc<AtomicBool>,
    /// 已失效的 inode 集合（等待清理）
    invalidated_inodes: Arc<RwLock<HashSet<u64>>>,
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
        statfs_cache_value: Option<StatisticsResponse>,
        statfs_client: Option<Arc<SyncFuseClient>>,
        client_id: String,
    ) -> Self {
        let cache_max = data.chunk_cache().max_bytes();
        let leases: Arc<RwLock<HashMap<u64, String>>> = Arc::new(RwLock::new(HashMap::new()));
        let write_locks: Arc<RwLock<HashMap<u64, Arc<Mutex<()>>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let lease_epochs: Arc<RwLock<HashMap<u64, u64>>> = Arc::new(RwLock::new(HashMap::new()));
        let invalidated_inodes: Arc<RwLock<HashSet<u64>>> = Arc::new(RwLock::new(HashSet::new()));
        let lease_renewer_running = Arc::new(AtomicBool::new(true));
        let lease_notifier_running = Arc::new(AtomicBool::new(true));
        let client_clone = client.clone();
        let leases_clone = leases.clone();
        let lease_renewer_running_clone = lease_renewer_running.clone();
        let lease_epochs_clone = lease_epochs.clone();
        let invalidated_inodes_clone = invalidated_inodes.clone();

        thread::spawn(move || {
            while lease_renewer_running_clone.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_secs(10));
                let leases_map = leases_clone.read().unwrap();
                for (&inode, lease_id) in leases_map.iter() {
                    let renew_result = if client_clone.has_filer() {
                        client_clone.filer_renew_lease(lease_id, 30000)
                    } else {
                        client_clone.renew_lease(lease_id, 30000)
                    };

                    match renew_result {
                        Ok((success, epoch)) => {
                            if success {
                                debug!("lease renewed for inode={}, epoch={}", inode, epoch);
                                let mut epochs = lease_epochs_clone.write().unwrap();
                                epochs.insert(inode, epoch);
                            } else {
                                warn!("lease renew failed for inode={}, lease expired", inode);
                                let mut invalidated = invalidated_inodes_clone.write().unwrap();
                                invalidated.insert(inode);
                            }
                        }
                        Err(e) => {
                            warn!("lease renew error for inode={}: {}", inode, e);
                        }
                    }
                }
            }
        });

        let fs = Self {
            meta,
            data,
            client,
            collection,
            replication,
            notifier: Arc::new(Mutex::new(None)),
            has_dirty: Arc::new(AtomicBool::new(false)),
            flush_manager: None,
            verify_data,
            statfs_cache: Arc::new(Mutex::new(statfs_cache_value)),
            statfs_client,
            leases,
            client_id,
            lease_renewer_running,
            write_locks,
            lease_epochs,
            lease_notifier_running,
            invalidated_inodes,
        };

        let flush_manager = {
            let client_arc = fs.client.clone();
            let meta_arc = fs.meta.clone();
            let data_arc = fs.data.clone();
            let collection = fs.collection.clone();
            let replication = fs.replication.clone();
            let verify_data = fs.verify_data;

            FlushManager::new(
                FlushConfig::default(),
                cache_max,
                move |inode: u64| -> std::result::Result<(), String> {
                    Self::flush_dirty_chunks_static(
                        inode,
                        &client_arc,
                        &meta_arc,
                        &data_arc,
                        &collection,
                        &replication,
                        verify_data,
                    )
                },
            )
        };

        Self {
            flush_manager: Some(flush_manager),
            ..fs
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
            crate::orset::FileType::Fifo => FileType::NamedPipe,
            crate::orset::FileType::CharDevice => FileType::CharDevice,
            crate::orset::FileType::BlockDevice => FileType::BlockDevice,
            crate::orset::FileType::Socket => FileType::Socket,
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
            nlink: entry.nlink,
            uid: entry.uid,
            gid: entry.gid,
            rdev: entry.rdev as u32,
            blksize: 4096,
            flags: 0,
        }
    }

    /// 获取文件实际大小（取 max(meta_size, data_size)）
    ///
    /// 以 meta server 的 size 为准（CAS 策略，可能有其他 client 写入），
    /// 但如果本地有未 flush 的脏数据，本地写入端代表了已知的文件最远端，
    /// 取 max 确保 flush 时不会把 size 传小了。
    /// 如果 meta_size 更大（其他 client 写入），同步到本地 data_manager，
    /// 确保读路径的边界检查正确。
    fn get_file_size(&self, ino: u64) -> u64 {
        let data_size = self.data.current_file_size(ino);
        let meta_result = self.meta.get_entry_by_inode(ino);
        let meta_size = meta_result
            .map(|e| e.map(|e| e.size).unwrap_or(0))
            .unwrap_or(0);
        let effective = data_size.max(meta_size);
        if meta_size > data_size {
            self.data.set_file_size(ino, meta_size);
        }
        debug!(
            "get_file_size: ino={}, data_size={}, meta_size={}, result={}",
            ino, data_size, meta_size, effective
        );
        effective
    }

    fn is_lease_invalidated(&self, inode: u64) -> bool {
        let invalidated = self.invalidated_inodes.read().unwrap();
        invalidated.contains(&inode)
    }

    #[allow(dead_code)]
    fn handle_lease_invalidation(&self, inode: u64) {
        warn!("handle_lease_invalidation: inode={}", inode);

        let mut invalidated = self.invalidated_inodes.write().unwrap();
        invalidated.remove(&inode);

        let mut leases = self.leases.write().unwrap();
        leases.remove(&inode);

        let mut epochs = self.lease_epochs.write().unwrap();
        epochs.remove(&inode);

        let mut locks = self.write_locks.write().unwrap();
        locks.remove(&inode);

        self.data.write_buffer().take(inode);

        self.invalidate_kernel_inode(inode);
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
        match Self::flush_dirty_chunks_static(
            inode,
            &self.client,
            &self.meta,
            &self.data,
            &self.collection,
            &self.replication,
            self.verify_data,
        ) {
            Ok(_) => Ok(()),
            Err(e) => Err(std::io::Error::other(e)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn flush_dirty_chunks_static(
        inode: u64,
        client: &Arc<SyncFuseClient>,
        meta: &Arc<MetadataManager>,
        data: &Arc<DataManager>,
        collection: &str,
        replication: &str,
        verify_data: bool,
    ) -> std::result::Result<(), String> {
        let result = Self::flush_dirty_chunks_inner(
            inode,
            client,
            meta,
            data,
            collection,
            replication,
            verify_data,
        );
        result.map_err(|e| format!("{}", e))
    }

    #[allow(clippy::too_many_arguments, clippy::type_complexity)]
    fn flush_dirty_chunks_inner(
        inode: u64,
        client: &Arc<SyncFuseClient>,
        meta: &Arc<MetadataManager>,
        data: &Arc<DataManager>,
        collection: &str,
        replication: &str,
        _verify_data: bool,
    ) -> std::io::Result<()> {
        // 获取脏 chunk 列表（排序确保按顺序处理）
        let mut dirty_chunks: Vec<u64> = data.get_dirty_chunks(inode);
        dirty_chunks.sort_unstable();
        if dirty_chunks.is_empty() {
            return Ok(());
        }

        let file_size = {
            let data_size = data.current_file_size(inode);
            let meta_size = meta
                .get_entry_by_inode(inode)
                .ok()
                .flatten()
                .map(|e| e.size)
                .unwrap_or(0);
            data_size.max(meta_size)
        };
        let chunk_size = data.chunk_cache().chunk_size();

        debug!(
            "flush_dirty_chunks: inode={}, dirty_chunks={}, file_size={}",
            inode,
            dirty_chunks.len(),
            file_size
        );

        // CRDT: Get entry from local OR-Set (no Master/Filer sync needed)
        let meta_entry = match meta.get_entry_by_inode(inode) {
            Ok(Some(e)) => e,
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("entry not found for inode {}", inode),
                ));
            }
        };

        // Get extended and chunks from local OR-Set entry
        let extended: std::collections::HashMap<String, Vec<u8>> = meta_entry.extended.clone();
        let entry_chunks: Vec<powerfs_master::proto::powerfs::FileChunk> = meta_entry
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

        // 解析现有 FileLayout（从 Entry.extended）
        let mut layout = FileLayout::from_extended(&extended);

        // 判断是否需要从 Flat 提升为 Stripe
        let should_promote =
            layout.as_ref().is_none_or(|l| !l.is_stripe()) && file_size > PROMOTE_THRESHOLD;

        if should_promote {
            info!(
                "flush_dirty_chunks: promoting inode={} to Stripe mode (file_size={} > {})",
                inode, file_size, PROMOTE_THRESHOLD
            );
            // 批量分配 stripe volumes
            match client.assign_stripe_fids(
                collection,
                replication,
                DEFAULT_STRIPE_COUNT,
                DEFAULT_STRIPE_SIZE,
            ) {
                Ok((fids, _locations)) => {
                    if fids.len() >= DEFAULT_STRIPE_COUNT as usize {
                        let volume_ids: Vec<u64> =
                            fids.iter().map(|f| f.volume_id.0 as u64).collect();
                        // round-robin 起始索引由 Master 决定，这里用 0（Master 已通过 fetch_add 错开）
                        let new_layout = FileLayout::stripe(
                            DEFAULT_STRIPE_SIZE,
                            DEFAULT_STRIPE_COUNT,
                            volume_ids.clone(),
                            0,
                        );
                        info!(
                            "flush_dirty_chunks: inode={} assigned stripe volumes={:?}",
                            inode, volume_ids
                        );
                        layout = Some(new_layout);
                    } else {
                        warn!(
                            "flush_dirty_chunks: assign_stripe_fids returned only {} fids, expected {}",
                            fids.len(),
                            DEFAULT_STRIPE_COUNT
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        "flush_dirty_chunks: assign_stripe_fids failed: {}, falling back to Flat",
                        e
                    );
                }
            }
        }

        // 先收集所有脏 chunk 数据到内存（防止 LRU 淘汰导致数据丢失）
        let mut dirty_chunk_data: Vec<(u64, u64, Vec<u8>, u64, u32)> = Vec::new(); // (chunk_idx, chunk_offset, data, mtime, crc32)

        debug!(
            "flush_dirty_chunks: inode={}, dirty_chunks={:?}, file_size={}, chunk_size={}, cache_len={}, cache_bytes={}",
            inode,
            dirty_chunks,
            file_size,
            chunk_size,
            data.chunk_cache().len(),
            data.chunk_cache().current_bytes(),
        );

        for chunk_idx in &dirty_chunks {
            let chunk_offset = chunk_idx * chunk_size;
            let chunk_data_opt = data.chunk_cache().get(inode, chunk_offset);
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

        // 根据 layout 决定写入策略
        let is_stripe = layout.as_ref().is_some_and(|l| l.is_stripe());

        let mut new_chunks = Vec::new();

        if is_stripe {
            // ============ Stripe 模式：按 volume 分组写入 ============
            let stripe_layout = layout.as_ref().unwrap();

            // 为每个 volume 缓存 FID 和 addr
            // 首次写入某个 volume 时分配 file_key
            let mut vol_fids: HashMap<u64, Fid> = HashMap::new();
            let mut vol_addrs: HashMap<u64, String> = HashMap::new();

            // 按 volume 分组脏 chunk
            let mut vol_blob_entries: HashMap<u64, Vec<(i64, i32, Vec<u8>, u32)>> = HashMap::new();
            let mut vol_chunk_info: HashMap<u64, Vec<(u64, u64, u64, u32)>> = HashMap::new(); // (chunk_offset, data_len, mtime, crc32)

            for (_chunk_idx, chunk_offset, data_bytes, mtime, crc32) in &dirty_chunk_data {
                let data_len = data_bytes.len();
                let target_vol_id = stripe_layout.volume_id_for_offset(*chunk_offset);

                match target_vol_id {
                    Some(vid) => {
                        // 查找或分配该 volume 的 FID
                        if let std::collections::hash_map::Entry::Vacant(e) = vol_fids.entry(vid) {
                            let existing = entry_chunks
                                .iter()
                                .find(|c| {
                                    !c.fid.is_empty()
                                        && Fid::from_string(&c.fid)
                                            .map(|f| f.volume_id.0 as u64 == vid)
                                            .unwrap_or(false)
                                })
                                .and_then(|c| Fid::from_string(&c.fid).ok());

                            let fid = match existing {
                                Some(f) => f,
                                None => match client.assign_fid(collection, replication) {
                                    Ok((new_fid, _, _, _)) => new_fid,
                                    Err(e) => {
                                        let fs_error = parse_master_error(&e);
                                        return Err(std::io::Error::from_raw_os_error(
                                            fs_error.to_errno(),
                                        ));
                                    }
                                },
                            };

                            let locations = match client.lookup_volume(fid.volume_id) {
                                Ok(l) => l,
                                Err(e) => {
                                    let fs_error = parse_master_error(&e);
                                    return Err(std::io::Error::from_raw_os_error(
                                        fs_error.to_errno(),
                                    ));
                                }
                            };
                            let loc = locations
                                .first()
                                .ok_or_else(|| std::io::Error::from_raw_os_error(libc::EIO))?;
                            let addr = PowerFuseClient::location_to_grpc_addr(loc);

                            e.insert(fid);
                            vol_addrs.insert(vid, addr);
                        }

                        vol_blob_entries.entry(vid).or_default().push((
                            *chunk_offset as i64,
                            data_len as i32,
                            data_bytes.clone(),
                            0u32,
                        ));
                        vol_chunk_info.entry(vid).or_default().push((
                            *chunk_offset,
                            data_len as u64,
                            *mtime,
                            *crc32,
                        ));
                    }
                    None => {
                        warn!(
                            "flush_dirty_chunks: no volume for offset={}, skipping",
                            chunk_offset
                        );
                    }
                }
            }

            // 按 volume 批量写入
            for (vid, entries) in vol_blob_entries {
                if let Some(fid) = vol_fids.get(&vid) {
                    if let Some(addr) = vol_addrs.get(&vid) {
                        if !entries.is_empty() {
                            // Calculate stripe range for lease acquisition
                            let min_offset = entries.iter().map(|e| e.0).min().unwrap_or(0);
                            let max_end =
                                entries.iter().map(|e| e.0 + e.1 as i64).max().unwrap_or(0);
                            let stripe_size = DEFAULT_STRIPE_SIZE as i64;
                            let stripe_start = (min_offset / stripe_size) as u64;
                            let stripe_end = ((max_end - 1) / stripe_size) as u64 + 1;
                            let stripe_count = stripe_end - stripe_start;

                            // Acquire range lease for the write
                            let lease_token = match client.acquire_range_lease(
                                addr,
                                inode,
                                stripe_start,
                                stripe_count,
                                true, // exclusive write lease
                                DEFAULT_STRIPE_SIZE,
                                30_000, // 30 second lease
                            ) {
                                Ok((token, _expire_ms)) => {
                                    debug!(
                                        "flush_dirty_chunks: acquired range lease for inode={}, stripes={}-{}, token={}",
                                        inode, stripe_start, stripe_count, token
                                    );
                                    Some(token)
                                }
                                Err(e) => {
                                    debug!(
                                        "flush_dirty_chunks: range lease not available for inode={}: {}, proceeding without lease",
                                        inode, e
                                    );
                                    None
                                }
                            };

                            let write_result = client.batch_write_blob(
                                addr,
                                fid.volume_id.0,
                                fid.file_key,
                                entries,
                            );

                            // Release lease after write
                            if let Some(token) = &lease_token {
                                if let Err(e) = client.release_range_lease(addr, token) {
                                    warn!(
                                        "flush_dirty_chunks: failed to release range lease: {}",
                                        e
                                    );
                                }
                            }

                            if let Err(e) = write_result {
                                let fs_error = crate::error::parse_volume_error(&e);
                                error!(
                                    "flush_dirty_chunks: stripe batch_write_blob failed for volume {}: {}",
                                    vid, fs_error
                                );
                                return Err(std::io::Error::from_raw_os_error(fs_error.to_errno()));
                            }
                            debug!(
                                "flush_dirty_chunks: wrote {} chunks to stripe volume {} at {}",
                                vol_chunk_info.get(&vid).map(|v| v.len()).unwrap_or(0),
                                vid,
                                addr
                            );
                        }
                    }
                }
            }

            // 构建 FileChunk 列表
            for (vid, chunk_infos) in vol_chunk_info {
                if let Some(fid) = vol_fids.get(&vid) {
                    for (chunk_offset, data_len, mtime, crc32) in chunk_infos {
                        new_chunks.push(powerfs_master::proto::powerfs::FileChunk {
                            offset: chunk_offset,
                            size: data_len,
                            mtime,
                            fid: fid.to_string(),
                            cookie: 0,
                            crc32,
                        });
                    }
                }
            }
        } else {
            // ============ Flat 模式：原有逻辑，单 volume 写入 ============
            let existing_fid = entry_chunks
                .iter()
                .find(|c| !c.fid.is_empty())
                .and_then(|c| Fid::from_string(&c.fid).ok());

            let fid = if let Some(fid) = existing_fid {
                fid
            } else {
                match client.assign_fid(collection, replication) {
                    Ok((new_fid, _, _, _)) => new_fid,
                    Err(e) => {
                        let fs_error = parse_master_error(&e);
                        return Err(std::io::Error::from_raw_os_error(fs_error.to_errno()));
                    }
                }
            };

            let locations = match client.lookup_volume(fid.volume_id) {
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

            let mut blob_entries = Vec::new();

            for (_chunk_idx, chunk_offset, data_bytes, mtime, crc32) in &dirty_chunk_data {
                let data_len = data_bytes.len();
                if !data_bytes.is_empty() && data_bytes.iter().all(|&b| b == 0) {
                    warn!(
                        "flush_dirty_chunks: ALL ZEROS chunk! ino={}, chunk_offset={}, size={}",
                        inode, chunk_offset, data_len
                    );
                }
                blob_entries.push((
                    *chunk_offset as i64,
                    data_len as i32,
                    data_bytes.clone(),
                    0u32,
                ));

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
                // Calculate stripe range for lease acquisition
                let min_offset = blob_entries.iter().map(|e| e.0).min().unwrap_or(0);
                let max_end = blob_entries
                    .iter()
                    .map(|e| e.0 + e.1 as i64)
                    .max()
                    .unwrap_or(0);
                let stripe_size = DEFAULT_STRIPE_SIZE as i64;
                let stripe_start = (min_offset / stripe_size) as u64;
                let stripe_end = ((max_end - 1) / stripe_size) as u64 + 1;
                let stripe_count = stripe_end - stripe_start;

                // Acquire range lease for the write
                let lease_token = match client.acquire_range_lease(
                    &addr,
                    inode,
                    stripe_start,
                    stripe_count,
                    true,
                    DEFAULT_STRIPE_SIZE,
                    30_000,
                ) {
                    Ok((token, _expire_ms)) => {
                        debug!(
                            "flush_dirty_chunks: acquired range lease for inode={}, stripes={}-{}, token={}",
                            inode, stripe_start, stripe_count, token
                        );
                        Some(token)
                    }
                    Err(e) => {
                        debug!(
                            "flush_dirty_chunks: range lease not available for inode={}: {}, proceeding without lease",
                            inode, e
                        );
                        None
                    }
                };

                let write_result =
                    client.batch_write_blob(&addr, fid.volume_id.0, fid.file_key, blob_entries);

                // Release lease after write
                if let Some(token) = &lease_token {
                    if let Err(e) = client.release_range_lease(&addr, token) {
                        warn!("flush_dirty_chunks: failed to release range lease: {}", e);
                    }
                }

                if let Err(e) = write_result {
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
        }

        // CRDT: Update local OR-Set entry (chunks, size, extended) + async push_delta
        // Merge chunks: keep old non-dirty chunks, add new dirty chunks
        let mut updated_chunks: Vec<crate::orset::CachedFileChunk> = meta_entry.chunks.clone();
        for new_chunk in &new_chunks {
            updated_chunks.retain(|c| c.offset != new_chunk.offset);
            updated_chunks.push(crate::orset::CachedFileChunk {
                offset: new_chunk.offset,
                size: new_chunk.size,
                mtime: new_chunk.mtime,
                fid: new_chunk.fid.clone(),
                cookie: new_chunk.cookie,
                crc32: new_chunk.crc32,
            });
        }
        updated_chunks.sort_by_key(|c| c.offset);

        // Update extended with file layout
        let mut updated_extended = extended.clone();
        if let Some(ref l) = layout {
            if l.is_stripe() {
                l.to_extended(&mut updated_extended);
            }
        }

        let old_size = meta_entry.size;
        let is_truncate = file_size > 0 && file_size < old_size;
        debug!(
            "flush_dirty_chunks: inode={}, old_size={}, new_size={}, is_truncate={}, is_stripe={}",
            inode, old_size, file_size, is_truncate, is_stripe
        );

        // Update local OR-Set entry with new chunks, size, and extended
        let new_size = if file_size > 0 { file_size } else { old_size };
        meta.update_entry_chunks(inode, updated_chunks, new_size, updated_extended);

        // 清除脏标记
        data.clear_dirty(inode);

        debug!(
            "flush_dirty_chunks: completed for inode={}, chunks={}, is_stripe={}",
            inode,
            new_chunks.len(),
            is_stripe
        );

        Ok(())
    }

    #[allow(dead_code)]
    /// 验证写入的数据完整性
    fn verify_flushed_data(
        &self,
        inode: u64,
        dirty_chunk_data: &[(u64, u64, Vec<u8>, u64, u32)],
        addr: &str,
        fid: powerfs_common::types::Fid,
    ) -> std::io::Result<()> {
        Self::verify_flushed_data_static(inode, dirty_chunk_data, addr, fid, &self.client)
    }

    #[allow(dead_code)]
    fn verify_flushed_data_static(
        inode: u64,
        dirty_chunk_data: &[(u64, u64, Vec<u8>, u64, u32)],
        addr: &str,
        fid: powerfs_common::types::Fid,
        client: &Arc<SyncFuseClient>,
    ) -> std::io::Result<()> {
        use md5::compute;

        for (_chunk_idx, chunk_offset, local_data, _mtime, _crc32) in dirty_chunk_data {
            match client.read_blob(
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

        // CRDT: Get chunk FID from local OR-Set (no Master/Filer sync)
        let meta_entry = match self.meta.get_entry_by_inode(inode) {
            Ok(Some(e)) => e,
            _ => {
                return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
            }
        };

        // 查找该 offset 对应的 chunk FID
        let chunk_fid = meta_entry
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
                // 该 chunk 不存在（可能是空洞或未写入的区域）
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
                        crate::orset::FileType::Fifo => FileType::NamedPipe,
                        crate::orset::FileType::CharDevice => FileType::CharDevice,
                        crate::orset::FileType::BlockDevice => FileType::BlockDevice,
                        crate::orset::FileType::Socket => FileType::Socket,
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
                // 调试：符号链接 inode 丢失
                warn!("getattr: inode {} not found (ENOENT)", inode);
                reply.error(libc::ENOENT);
            }
            Err(e) => {
                error!("getattr: inode={}, error={:?}", inode, e);
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
            // 调试：记录 truncate 操作（cp -p 不应触发 size 变更）
            let current_size = self.get_file_size(inode);
            if new_size < current_size {
                warn!(
                    "setattr: TRUNCATE shrinking! inode={}, current={}, new={}",
                    inode, current_size, new_size
                );
            } else if new_size == 0 {
                warn!(
                    "setattr: TRUNCATE to ZERO! inode={}, current={}",
                    inode, current_size
                );
            }
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
        match self.meta.setattr(inode, mode, uid, gid, size, mtime_secs) {
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
                        crate::orset::FileType::Fifo => FileType::NamedPipe,
                        crate::orset::FileType::CharDevice => FileType::CharDevice,
                        crate::orset::FileType::BlockDevice => FileType::BlockDevice,
                        crate::orset::FileType::Socket => FileType::Socket,
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
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        let name_str = name.to_str().unwrap_or("");
        debug!(
            "create: parent={}, name={}, mode={:o}, uid={}, gid={}",
            parent,
            name_str,
            mode,
            req.uid(),
            req.gid()
        );

        match self
            .meta
            .create(parent, name_str, mode | libc::S_IFREG, req.uid(), req.gid())
        {
            Ok(entry) => {
                let attr = Self::dir_entry_to_file_attr(&entry);
                reply.created(&TTL, &attr, 0, 0, 0);

                self.data.set_file_size(entry.inode, 0);

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
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let name_str = name.to_str().unwrap_or("");
        debug!(
            "mkdir: parent={}, name={}, mode={:o}, uid={}, gid={}",
            parent,
            name_str,
            mode,
            req.uid(),
            req.gid()
        );

        match self
            .meta
            .mkdir(parent, name_str, mode | libc::S_IFDIR, req.uid(), req.gid())
        {
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
                debug!(
                    "symlink: created name={} -> target={}, inode={}, parent={}",
                    name_str, target_str, entry.inode, parent
                );
                let attr = Self::dir_entry_to_file_attr(&entry);
                reply.entry(&TTL, &attr, 0);
                self.invalidate_kernel_dentry(parent, name_str);
            }
            Err(e) => {
                error!(
                    "symlink: FAILED parent={}, name={}, target={}, error={:?}",
                    parent, name_str, target_str, e
                );
                reply.error(e.to_errno());
            }
        }
    }

    fn mknod(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        rdev: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let name_str = name.to_str().unwrap_or("");
        debug!(
            "mknod: parent={}, name={}, mode={:o}, rdev={}",
            parent, name_str, mode, rdev
        );

        match self.meta.mknod(parent, name_str, mode, rdev) {
            Ok(entry) => {
                debug!(
                    "mknod: created name={}, inode={}, parent={}",
                    name_str, entry.inode, parent
                );
                let attr = Self::dir_entry_to_file_attr(&entry);
                reply.entry(&TTL, &attr, 0);
                self.invalidate_kernel_dentry(parent, name_str);
            }
            Err(e) => {
                error!(
                    "mknod: FAILED parent={}, name={}, error={:?}",
                    parent, name_str, e
                );
                reply.error(e.to_errno());
            }
        }
    }

    fn link(
        &mut self,
        _req: &Request<'_>,
        inode: u64,
        newparent: u64,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        let newname_str = newname.to_str().unwrap_or("");
        debug!(
            "link: inode={}, newparent={}, newname={}",
            inode, newparent, newname_str
        );

        match self.meta.link(inode, newparent, newname_str) {
            Ok(entry) => {
                debug!(
                    "link: created inode={}, newparent={}, newname={}",
                    inode, newparent, newname_str
                );
                let attr = Self::dir_entry_to_file_attr(&entry);
                reply.entry(&TTL, &attr, 0);
                self.invalidate_kernel_dentry(newparent, newname_str);
                self.invalidate_kernel_inode(inode);
            }
            Err(e) => {
                error!(
                    "link: FAILED inode={}, newparent={}, newname={}, error={:?}",
                    inode, newparent, newname_str, e
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

    fn open(&mut self, _req: &Request<'_>, inode: u64, flags: i32, reply: ReplyOpen) {
        self.meta.acquire_inode(inode);

        let file_size = self.get_file_size(inode);
        self.data.set_file_size(inode, file_size);

        if let Ok(Some(entry)) = self.meta.get_entry_by_inode(inode) {
            let uid = _req.uid();
            let gid = _req.gid();
            let is_read = flags == libc::O_RDONLY;
            let is_write = flags & (libc::O_WRONLY | libc::O_RDWR) != 0;

            let has_permission = if uid == 0 {
                true
            } else if entry.uid == uid {
                if is_read {
                    (entry.mode & 0o400) != 0
                } else if is_write {
                    (entry.mode & 0o200) != 0
                } else {
                    true
                }
            } else if entry.gid == gid {
                if is_read {
                    (entry.mode & 0o040) != 0
                } else if is_write {
                    (entry.mode & 0o020) != 0
                } else {
                    true
                }
            } else {
                if is_read {
                    (entry.mode & 0o004) != 0
                } else if is_write {
                    (entry.mode & 0o002) != 0
                } else {
                    true
                }
            };

            if !has_permission {
                warn!(
                    "open: permission denied for inode={}, uid={}, gid={}, mode={:o}",
                    inode, uid, gid, entry.mode
                );
                reply.error(libc::EACCES);
                return;
            }
        }

        let is_write = flags & libc::O_WRONLY != 0 || flags & libc::O_RDWR != 0;
        if is_write {
            debug!("open: acquiring lease for inode={}", inode);
            let lease_result = if self.client.has_filer() {
                self.client
                    .filer_acquire_lease(inode, &self.client_id, 30000)
            } else {
                match self.meta.get_path(inode) {
                    Some(path) => self.client.acquire_lease(&path, &self.client_id, 30000),
                    None => {
                        warn!("open: cannot get path for inode={}", inode);
                        Err("cannot get path".to_string())
                    }
                }
            };

            match lease_result {
                Ok((lease_id, _epoch)) => {
                    debug!(
                        "open: lease acquired for inode={}, lease_id={}",
                        inode, lease_id
                    );
                    self.leases.write().unwrap().insert(inode, lease_id);
                }
                Err(e) => {
                    warn!("open: failed to acquire lease for inode={}: {}", inode, e);
                }
            }
        }

        reply.opened(0, 0);
    }

    fn opendir(&mut self, _req: &Request<'_>, inode: u64, _flags: i32, reply: ReplyOpen) {
        self.meta.acquire_inode(inode);
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

        let file_size = self.get_file_size(inode);
        if offset_u64 >= file_size {
            reply.data(&[]);
            return;
        }

        // 先尝试从 DataManager 本地缓存读
        match self.data.read(inode, offset_u64, size_usize) {
            Some(data) => {
                // 调试：检测全 0 读取
                if !data.is_empty() && data.iter().all(|&b| b == 0) {
                    warn!(
                        "read: ALL ZEROS from cache! inode={}, offset={}, size={}",
                        inode,
                        offset_u64,
                        data.len()
                    );
                }
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
                    let mtime = crate::orset::now_unix();
                    if e.raw_os_error() == Some(libc::ENOENT) {
                        let zero_data = vec![0u8; chunk_size as usize];
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
            Some(data) => {
                // 调试：检测从 Volume Server 拉取后的全 0 读取
                if !data.is_empty() && data.iter().all(|&b| b == 0) {
                    warn!(
                        "read: ALL ZEROS from volume! inode={}, offset={}, size={}",
                        inode,
                        offset_u64,
                        data.len()
                    );
                }
                reply.data(&data)
            }
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
        flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        if self.is_lease_invalidated(inode) {
            warn!(
                "write: lease invalidated for inode={}, returning error",
                inode
            );
            reply.error(libc::EIO);
            return;
        }

        let is_append = flags & libc::O_APPEND != 0;

        let offset_u64: u64;
        let written: u64;

        if is_append {
            let write_lock = {
                let mut locks = self.write_locks.write().unwrap();
                locks
                    .entry(inode)
                    .or_insert_with(|| Arc::new(Mutex::new(())))
                    .clone()
            };

            let _guard = write_lock.lock().unwrap();

            offset_u64 = self.data.current_file_size(inode);

            debug!(
                "write: inode={}, offset={}, size={}, append={}",
                inode,
                offset_u64,
                data.len(),
                is_append
            );

            if let Some(fm) = &self.flush_manager {
                if fm.is_backpressured() {
                    debug!(
                        "write: backpressure active (dirty={}), waiting for flush",
                        fm.global_dirty_bytes()
                    );
                    fm.wait_for_backpressure_relief(Duration::from_secs(30));
                }
            }

            written = self.data.write(inode, offset_u64, data);
            self.has_dirty.store(true, Ordering::Relaxed);

            if let Some(fm) = &self.flush_manager {
                fm.track_dirty(inode, written as usize);
            }
        } else {
            offset_u64 = offset as u64;

            debug!(
                "write: inode={}, offset={}, size={}, append={}",
                inode,
                offset_u64,
                data.len(),
                is_append
            );

            if let Some(fm) = &self.flush_manager {
                if fm.is_backpressured() {
                    debug!(
                        "write: backpressure active (dirty={}), waiting for flush",
                        fm.global_dirty_bytes()
                    );
                    fm.wait_for_backpressure_relief(Duration::from_secs(30));
                }
            }

            written = self.data.write(inode, offset_u64, data);
            self.has_dirty.store(true, Ordering::Relaxed);

            if let Some(fm) = &self.flush_manager {
                fm.track_dirty(inode, written as usize);
            }
        }

        let insize = self.get_file_size(inode);
        let outsize = (offset_u64 + data.len() as u64).max(insize);
        if outsize > insize {
            self.meta.update_size_optimistic(inode, insize, outsize);
        }

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

        // 清理 write_buffer（数据已在 chunk_cache 中）
        self.data.write_buffer().take(inode);

        // 通知后台 flush 线程处理该 inode 的脏数据
        if let Some(fm) = &self.flush_manager {
            fm.notify_release(inode);
        }

        self.meta.release_inode(inode);

        // 清理租约失效状态
        if self.is_lease_invalidated(inode) {
            self.handle_lease_invalidation(inode);
        }

        // 释放租约
        if let Some(lease_id) = self.leases.write().unwrap().remove(&inode) {
            debug!(
                "release: releasing lease for inode={}, lease_id={}",
                inode, lease_id
            );
            let release_result = if self.client.has_filer() {
                self.client.filer_release_lease(&lease_id)
            } else {
                self.client.release_lease(&lease_id)
            };

            match release_result {
                Ok(_) => {
                    debug!("release: lease released successfully for inode={}", inode);
                }
                Err(e) => {
                    warn!(
                        "release: failed to release lease for inode={}: {}",
                        inode, e
                    );
                }
            }
        }

        reply.ok();
    }

    fn releasedir(
        &mut self,
        _req: &Request<'_>,
        inode: u64,
        _fh: u64,
        _flags: i32,
        reply: ReplyEmpty,
    ) {
        debug!("releasedir: inode={}", inode);
        self.meta.release_inode(inode);
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
        const DEFAULT_TOTAL_BYTES: u64 = 1024 * 1024 * 1024 * 1024;
        const DEFAULT_USED_BYTES: u64 = 0;

        let (total_bytes, used_bytes) = {
            let cache_guard = self.statfs_cache.lock().unwrap();
            match &*cache_guard {
                // master 上报了有效容量
                Some(stats) if stats.total_volume_size > 0 => {
                    (stats.total_volume_size, stats.total_used_size)
                }
                // 缓存为空，或 master 尚未上报容量（total=0）：
                // 使用默认值兜底，避免 df 把挂载点过滤掉（df 会隐藏 blocks_total=0 的 fs）
                _ => (DEFAULT_TOTAL_BYTES, DEFAULT_USED_BYTES),
            }
        };

        let total_blocks = total_bytes / block_size_u64;
        let used_blocks = used_bytes / block_size_u64;
        let free_blocks = total_blocks.saturating_sub(used_blocks);

        reply.statfs(
            total_blocks,
            free_blocks,
            free_blocks,
            1000,
            1000,
            BLOCK_SIZE,
            1_000_000,
            BLOCK_SIZE,
        );
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
            has_dirty: self.has_dirty.clone(),
            flush_manager: self.flush_manager.clone(),
            verify_data: self.verify_data,
            statfs_cache: self.statfs_cache.clone(),
            statfs_client: self.statfs_client.clone(),
            leases: self.leases.clone(),
            client_id: self.client_id.clone(),
            lease_renewer_running: self.lease_renewer_running.clone(),
            write_locks: self.write_locks.clone(),
            lease_epochs: self.lease_epochs.clone(),
            lease_notifier_running: self.lease_notifier_running.clone(),
            invalidated_inodes: self.invalidated_inodes.clone(),
        }
    }
}

pub struct FuserApp {
    mount_point: String,
    master_addresses: Vec<String>,
    filer_addresses: Vec<String>,
    collection: String,
    replication: String,
    num_threads: usize,
    runtime_handle: Handle,
    verify_data: bool,
}

impl FuserApp {
    pub async fn new(
        master_addrs: &[String],
        filer_addrs: &[String],
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
            master_addresses: master_addrs.to_vec(),
            filer_addresses: filer_addrs.to_vec(),
            collection: collection.to_string(),
            replication: replication.to_string(),
            num_threads,
            runtime_handle,
            verify_data,
        })
    }

    pub async fn run(&self) -> Result<()> {
        info!(
            "Starting FUSE session on {} with masters {} ({} threads)",
            self.mount_point,
            self.master_addresses.join(", "),
            self.num_threads
        );

        // 内存泄漏诊断任务：每 30 秒打印一次关键指标
        tokio::spawn(async move {
            let mut prev_snapshot: Option<powerfs_master::tracking_allocator::AllocSnapshot> = None;
            let mut prev_vm_rss: u64 = 0;
            let mut tick = 0u64;
            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;
                tick += 1;

                let snap = powerfs_master::tracking_allocator::ALLOC_STATS.snapshot();
                let vm = powerfs_master::tracking_allocator::read_self_vm();
                let (rss_kb, data_kb, peak_kb) = vm.unwrap_or((0, 0, 0));

                let (delta_live_kb, delta_alloc_mb) = if let Some(prev) = prev_snapshot {
                    let d_live = snap.live_bytes().saturating_sub(prev.live_bytes());
                    let d_alloc = snap.alloc_bytes.saturating_sub(prev.alloc_bytes);
                    (d_live / 1024, d_alloc / 1024 / 1024)
                } else {
                    (0, 0)
                };
                let delta_rss_kb = rss_kb.saturating_sub(prev_vm_rss);

                info!(
                    "MEM_DIAG_FUSE tick={} rss_mb={} data_mb={} peak_mb={} live_mb={} live_cnt={} \
                     delta_live_kb={} delta_rss_kb={} delta_alloc_mb={}",
                    tick,
                    rss_kb / 1024,
                    data_kb / 1024,
                    peak_kb / 1024,
                    snap.live_bytes() / 1024 / 1024,
                    snap.live_count(),
                    delta_live_kb,
                    delta_rss_kb,
                    delta_alloc_mb,
                );

                prev_snapshot = Some(snap);
                prev_vm_rss = rss_kb;
            }
        });

        let master_addrs_ref: Vec<&str> =
            self.master_addresses.iter().map(|s| s.as_str()).collect();
        let filer_addrs_ref: Vec<&str> = self.filer_addresses.iter().map(|s| s.as_str()).collect();
        let grpc_client = PowerFuseClient::new(
            &master_addrs_ref,
            &filer_addrs_ref,
            self.runtime_handle.clone(),
            &self.collection,
        );
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
        meta.start_delta_sync();
        let chunk_cache = Arc::new(crate::cache::ChunkCache::with_defaults());
        let chunk_cache_clone_for_keep_connected = chunk_cache.clone();
        let write_buffer = Arc::new(crate::data_manager::WriteBuffer::new(64));
        let data = Arc::new(DataManager::new(chunk_cache, write_buffer));

        // 预加载 statfs 缓存（启动时只加载一次，之后不更新）
        let statfs_cache_value = match sync_client.inner().get_statistics(&self.collection).await {
            Ok(stats) => {
                info!(
                    "Preloaded statfs cache: total={} bytes",
                    stats.total_volume_size
                );
                Some(stats)
            }
            Err(e) => {
                warn!(
                    "Failed to preload statfs cache, using default values: {}",
                    e
                );
                None
            }
        };

        // 在创建 fs 之前先 clone meta，以便后续元数据订阅线程使用
        let meta_clone_for_subscribe = meta.clone();

        // 创建 statfs 专用 gRPC 客户端（独立通道，高负载时保证 df 正常工作）
        let master_addrs_ref: Vec<&str> =
            self.master_addresses.iter().map(|s| s.as_str()).collect();
        let filer_addrs_ref: Vec<&str> = self.filer_addresses.iter().map(|s| s.as_str()).collect();
        let statfs_client = Some(Arc::new(SyncFuseClient::new(PowerFuseClient::new(
            &master_addrs_ref,
            &filer_addrs_ref,
            self.runtime_handle.clone(),
            &self.collection,
        ))));

        // 创建 FUSE 文件系统
        let fs = PowerFsFuserFs::new(
            sync_client.clone(),
            meta,
            data,
            self.collection.clone(),
            self.replication.clone(),
            self.verify_data,
            statfs_cache_value,
            statfs_client,
            client_id.clone(),
        );

        // 后台 statfs 缓存更新线程（每 5 秒更新一次，及时反映空间使用变化）
        let fs_clone_for_statfs = fs.clone();
        let collection_clone_for_statfs = self.collection.clone();
        std::thread::spawn(move || loop {
            let result = fs_clone_for_statfs
                .statfs_client
                .as_ref()
                .unwrap_or(&fs_clone_for_statfs.client)
                .get_statistics(&collection_clone_for_statfs);
            if let Ok(stats) = result {
                let mut cache_guard = fs_clone_for_statfs.statfs_cache.lock().unwrap();
                *cache_guard = Some(stats);
            }
            std::thread::sleep(Duration::from_secs(5));
        });

        // 后台元数据订阅线程（实时接收 Master 的目录变更通知）
        let meta_clone = meta_clone_for_subscribe;
        let grpc_client_clone_for_subscribe = grpc_client.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                loop {
                    match grpc_client_clone_for_subscribe
                        .subscribe_metadata("/")
                        .await
                    {
                        Ok(mut stream) => {
                            info!("Successfully subscribed to metadata notifications");
                            while let Some(notification) = stream.message().await.unwrap_or(None) {
                                if notification.event_type == 2 {
                                    // DELETE - 注意：使用非阻塞方式处理，避免死锁
                                    let path = notification.path;
                                    if !path.is_empty() {
                                        // 尝试非阻塞地失效缓存
                                        // 如果锁被占用，跳过此次失效（下次 ls 会从 Master 重新拉取）
                                        if meta_clone.try_invalidate_local_cache_entry(&path) {
                                            info!("Successfully invalidated cache for deleted path: {}", path);
                                        } else {
                                            debug!("Skipped cache invalidation for {} (lock contention)", path);
                                        }
                                    }
                                }
                            }
                            info!("Metadata notification stream closed, reconnecting...");
                        }
                        Err(e) => {
                            warn!(
                                "Failed to subscribe to metadata notifications: {}, retrying in 5s",
                                e
                            );
                        }
                    }
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            });
        });

        // 后台 flush 线程（每 100ms 检查脏标记）
        let fs_clone_for_flush = fs.clone();
        std::thread::spawn(move || loop {
            if fs_clone_for_flush
                .has_dirty
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                // Phase 1A: 后台 flush 暂不实现全局扫描
                // 实际 flush 在 release/fsync 时触发
                fs_clone_for_flush
                    .has_dirty
                    .store(false, std::sync::atomic::Ordering::Relaxed);
            }
            std::thread::sleep(Duration::from_millis(100));
        });

        // 后台 keep_connected 线程（持久连接，定期发送客户端信息）
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
                // 持续重连循环：master 重启或网络中断后，keep_connected 流会断开，
                // 必须重新建立流并重新注册 FuseClientInfo，否则 master 端 fuse_clients
                // 为空，前端无法看到 FUSE 连接。
                loop {
                    let _ = grpc_client_clone
                        .keep_connected(
                            "fuse",
                            &mount_point_clone,
                            &collection_clone,
                            &replication_clone,
                            pid,
                            &host_name,
                            chunk_cache_clone_for_keep_connected.clone(),
                        )
                        .await;
                    warn!("keep_connected stream closed, reconnecting in 10 seconds...");
                    tokio::time::sleep(Duration::from_secs(10)).await;
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
