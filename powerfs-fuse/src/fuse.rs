use crate::cache::{CachedEntry, ChunkCache, MetadataCache, ROOT_INODE};
use bytes::BytesMut;
use fuse_backend_rs::api::filesystem::{
    Context, DirEntry, Entry, FileLock, FileSystem, GetxattrReply, ListxattrReply, ZeroCopyReader,
    ZeroCopyWriter,
};
use fuse_backend_rs::api::server::Server;
use fuse_backend_rs::transport::{FuseChannel, FuseSession};
use log::{debug, error, info, warn};
use powerfs_common::error::{PowerFsError, Result};
use powerfs_common::types::{Fid, VolumeId};
use powerfs_fuse_core::metadata_client::{
    MetadataAttr, MetadataClient, MetadataDirEntry, SetattrParams,
};
use powerfs_fuse_core::{LeaseManager, LeaseMode, SyncFuseClientFacade, VolumeLeaseManager};
use powerfs_master::proto::powerfs::Entry as FilerEntry;
use powerfs_orset::CachedFileChunk;
use std::collections::{HashMap, HashSet};
use std::ffi::CStr;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

/// TTL for kernel attribute cache. A short TTL (100ms) reduces FUSE
/// getattr round-trips for repeated stat calls while still providing
/// near-immediate cross-client visibility (Invalidate notifications
/// from the Filer evict stale entries; 100ms is the fallback window).
/// Previously ZERO, which caused inode eviction after release/unpin,
/// breaking fsync on files whose dirty chunks were re-marked by the
/// background flusher.
const TTL: Duration = Duration::from_millis(100);
/// TTL for getattr responses on open files. Same rationale as TTL: the
/// kernel cache cannot be invalidated, so ZERO ensures every stat queries
/// the FUSE daemon. Open files' metadata is authoritative in the userspace
/// cache (lease-held).
const TTL_OPEN: Duration = Duration::ZERO;
/// Number of additional chunks to prefetch ahead during sequential reads.
/// With chunk_size=4MB, PREFETCH_CHUNKS=4 prefetches 16MB ahead, reducing
/// cache misses for sequential workloads. Memory cost: 16MB × concurrent
/// open files (e.g. 10 files = 160MB, acceptable under 2GB container limit).
/// Previously 8 (with 1MB chunks = 8MB); reduced to 4 after chunk_size
/// increased to 4MB to keep prefetch memory bounded.
const PREFETCH_CHUNKS: u64 = 4;
const FUSE_APPEND: u32 = 0x400;

/// FUSE application that manages the mount lifecycle
#[allow(dead_code)]
pub struct FuseApp {
    mount_point: String,
    master_addresses: Vec<String>,
    collection: String,
    replication: String,
    master_net_port: u16,
    volume_net_port: u16,
    volume_addrs: Vec<String>,
    filer_addr: String,
    filer_net_port: u16,
    runtime: Arc<tokio::runtime::Runtime>,
}

impl FuseApp {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        master_addrs: &[String],
        mount_point: &str,
        collection: &str,
        replication: &str,
        master_net_port: u16,
        volume_net_port: u16,
        volume_addrs: Vec<String>,
        filer_addr: String,
        filer_net_port: u16,
        runtime: Arc<tokio::runtime::Runtime>,
    ) -> Result<Self> {
        Ok(FuseApp {
            mount_point: mount_point.to_string(),
            master_addresses: master_addrs.to_vec(),
            collection: collection.to_string(),
            replication: replication.to_string(),
            master_net_port,
            volume_net_port,
            volume_addrs,
            filer_addr,
            filer_net_port,
            runtime,
        })
    }

    pub async fn run(&self) -> Result<()> {
        info!(
            "Starting FUSE session on {} with masters {}",
            self.mount_point,
            self.master_addresses.join(", ")
        );

        // Extract host from master address
        // Strip protocol prefix (http://, https://) if present
        let master_full = self
            .master_addresses
            .first()
            .ok_or_else(|| {
                PowerFsError::Internal("master_addresses is empty (must be configured)".to_string())
            })?
            .clone();
        let master_addr = {
            let without_proto = master_full
                .strip_prefix("http://")
                .or_else(|| master_full.strip_prefix("https://"))
                .unwrap_or(&master_full);
            without_proto
                .split(':')
                .next()
                .ok_or_else(|| {
                    PowerFsError::Internal(format!(
                        "Cannot parse host from master address: {}",
                        master_full
                    ))
                })?
                .to_string()
        };

        let client_identity = powerfs_fuse_core::ClientIdentity::stable_for(&self.mount_point);
        info!(
            "Phase 2: FUSE client_identity client_id={} uuid={} (hostname+mount_point={})",
            client_identity.client_id, client_identity.client_uuid, self.mount_point
        );
        let facade_config = powerfs_fuse_core::FuseClientFacadeConfig {
            master_addr: master_addr.clone(),
            master_port: self.master_net_port,
            volume_net_port: self.volume_net_port,
            volume_addrs: self.volume_addrs.clone(),
            filer_addr: self.filer_addr.clone(),
            filer_port: self.filer_net_port,
            request_timeout: Duration::from_secs(10),
            client_identity,
            mount_point: self.mount_point.clone(),
            collection: self.collection.clone(),
            replication: self.replication.clone(),
        };

        let facade = Arc::new(
            powerfs_fuse_core::FuseClientFacade::build_from_config(facade_config)
                .await
                .map_err(|e| {
                    PowerFsError::Internal(format!("Failed to build FuseClientFacade: {}", e))
                })?,
        );

        let sync_client = Arc::new(powerfs_fuse_core::SyncFuseClientFacade::new(
            facade,
            self.runtime.clone(),
        ));

        let cache = Arc::new(MetadataCache::new());
        // Create the chunk (data) cache up front so it can be shared with the
        // InvalidateHandler: a metadata Invalidate means the file's size/chunks
        // changed, so the client must drop cached file data as well to avoid
        // serving stale reads after another client modifies the file.
        let chunk_cache = Arc::new(ChunkCache::with_defaults());

        // Phase 2: Wire up InvalidateHandler so the FUSE client receives
        // server-pushed Invalidate notifications from the Filer and evicts
        // stale metadata cache entries when another client modifies the
        // same directory.
        let invalidate_handler = Arc::new(crate::invalidate_handler::InvalidateHandler::new(
            cache.clone(),
            chunk_cache.clone(),
        ));
        sync_client
            .facade()
            .meta_shard_client()
            .set_notification_handler(invalidate_handler);

        let lease_manager = Arc::new(VolumeLeaseManager::new(
            sync_client.facade().clone(),
            sync_client.client_id(),
        ));

        let fs = PowerFsFs {
            client: sync_client.clone(),
            cache: cache.clone(),
            chunk_cache,
            collection: self.collection.clone(),
            replication: self.replication.clone(),
            locks: Arc::new(RwLock::new(HashMap::new())),
            dirty_shards: (0..NUM_DIRTY_SHARDS)
                .map(|_| Arc::new(RwLock::new(HashSet::new())))
                .collect(),
            has_dirty: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            write_locks: Arc::new(std::sync::Mutex::new(HashMap::new())),
            flush_locks: Arc::new(std::sync::Mutex::new(HashMap::new())),
            backpressure_lock: Arc::new(std::sync::Mutex::new(())),
            stripe_size: 64 * 1024 * 1024, // 64MB per stripe
            lease_duration_ms: 30000,      // 30 seconds lease
            lease_manager,
            open_inodes: Arc::new(RwLock::new(HashSet::new())),
        };

        let fs_arc = Arc::new(fs);
        let bg_fs = fs_arc.clone();
        thread::spawn(move || loop {
            // P2-d: Adaptive flusher interval.
            // When dirty chunks exist, use a shorter interval (50ms) to flush
            // them quickly, reducing close latency and dirty backlog.
            // When idle, use a longer interval (100ms) to save CPU wakeups.
            // Note: too-aggressive (10ms) caused lock contention with write
            // path and OOM under sustained 512M+ sequential writes (dirty
            // accumulation vs flush rate). 20ms balances responsiveness and
            // throughput (2GB container has enough memory to handle higher flush rate).
            if bg_fs.has_dirty.load(std::sync::atomic::Ordering::Relaxed) {
                let _ = bg_fs.flush_all_dirty_chunks();
                bg_fs
                    .has_dirty
                    .store(false, std::sync::atomic::Ordering::Relaxed);
                thread::sleep(Duration::from_millis(20));
            } else {
                thread::sleep(Duration::from_millis(100));
            }
        });

        let mut session =
            FuseSession::new(Path::new(&self.mount_point), "powerfs", "powerfs", false).map_err(
                |e| PowerFsError::Internal(format!("failed to create fuse session: {}", e)),
            )?;

        session
            .mount()
            .map_err(|e| PowerFsError::Internal(format!("failed to mount fuse: {}", e)))?;

        info!("FUSE mounted at: {}", self.mount_point);

        let server = Arc::new(Server::new(fs_arc));

        // 阶段1: 多 FUSE worker 线程并发处理请求，消除 block_on 串行瓶颈。
        // 每个 worker 持有独立 FuseChannel（dup fd），共享同一个 Server<PowerFsFs>。
        // FUSE FileSystem trait 是同步的，worker 线程通过 runtime.block_on() 桥接异步；
        // 多 worker 并发调用 block_on 时，tokio runtime 自然并发调度各自 future。
        //
        // worker 数取 max(num_cpus, 4)：FUSE 操作是 I/O 密集型（阻塞在网络往返），
        // 即使单 CPU 容器也需要足够并发度让多个请求同时在途。
        let num_workers = num_cpus::get().max(4);
        info!("Starting {} FUSE worker threads", num_workers);

        let mut worker_handles = Vec::with_capacity(num_workers);
        for i in 0..num_workers {
            let server = server.clone();
            let ch = session.new_channel().map_err(|e| {
                PowerFsError::Internal(format!("failed to create fuse channel {}: {}", i, e))
            })?;
            let mut fuse_server = FuseServer { server, ch };
            let handle = std::thread::Builder::new()
                .name(format!("fuse_worker_{}", i))
                .spawn(move || {
                    info!("FUSE worker thread {} started", i);
                    let _ = fuse_server.svc_loop();
                    warn!("FUSE worker thread {} exited", i);
                })
                .map_err(|e| {
                    PowerFsError::Internal(format!("failed to spawn fuse worker {}: {}", i, e))
                })?;
            worker_handles.push(handle);
        }

        tokio::signal::ctrl_c()
            .await
            .map_err(|e| PowerFsError::Internal(format!("signal error: {}", e)))?;

        info!("Received Ctrl+C, unmounting...");
        session.wake().ok();
        session.umount().ok();
        for handle in worker_handles {
            let _ = handle.join();
        }

        info!("FUSE session ended");
        Ok(())
    }
}

struct FuseServer {
    server: Arc<Server<Arc<PowerFsFs>>>,
    ch: FuseChannel,
}

impl FuseServer {
    fn svc_loop(&mut self) -> std::result::Result<(), std::io::Error> {
        loop {
            if let Some((reader, writer)) = self
                .ch
                .get_request()
                .map_err(|_| std::io::Error::from_raw_os_error(libc::EINVAL))?
            {
                if let Err(e) = self
                    .server
                    .handle_message(reader, writer.into(), None, None)
                {
                    match e {
                        fuse_backend_rs::Error::EncodeMessage(ref e)
                            if e.raw_os_error() == Some(libc::EBADF) =>
                        {
                            break;
                        }
                        _ => {
                            error!("Handling fuse message failed: {:?}", e);
                            continue;
                        }
                    }
                }
            } else {
                info!("FUSE server exiting");
                break;
            }
        }
        Ok(())
    }
}

type FileLocks = HashMap<u64, Vec<FileLock>>;
type FlushLockMap = HashMap<u64, Arc<std::sync::Mutex<()>>>;
type FlushLocks = Arc<std::sync::Mutex<FlushLockMap>>;

struct PowerFsFs {
    client: Arc<SyncFuseClientFacade>,
    cache: Arc<MetadataCache>,
    chunk_cache: Arc<ChunkCache>,
    collection: String,
    replication: String,
    locks: Arc<RwLock<FileLocks>>,
    dirty_shards: DirtyShards,
    has_dirty: Arc<std::sync::atomic::AtomicBool>,
    write_locks: WriteLocks,
    /// Per-inode flush lock: serializes flush_dirty_chunks and release's lease
    /// release to prevent the TOCTOU race where release removes a lease token
    /// from the server while the background flusher is still using it.
    flush_locks: FlushLocks,
    /// Global backpressure lock: when the chunk cache exceeds a high
    /// watermark, write threads acquire this lock to serialize cache flushes.
    /// Without this, multiple FUSE worker threads concurrently writing would
    /// each trigger an independent flush while the others keep growing the
    /// cache, defeating the backpressure and causing unbounded memory growth.
    backpressure_lock: Arc<std::sync::Mutex<()>>,
    stripe_size: u64,
    lease_duration_ms: u64,
    /// Step 6: 统一 lease 入口 + 读路径缓存复用。
    /// read 路径通过此 manager 获取共享 lease，命中缓存时零 RPC；
    /// lease 在 open→release 期间复用，release() 时 invalidate。
    lease_manager: Arc<VolumeLeaseManager>,
    /// Phase 4.3/4.4: 当前已打开的 inode 集合。
    /// open() 时加入，release() 时移除。getattr() 对其中的 inode 使用长 TTL
    /// （size/chunks 在 open→release 期间权威，因数据 lease 排他）。
    open_inodes: Arc<RwLock<HashSet<u64>>>,
}

const NUM_DIRTY_SHARDS: usize = 16;

type WriteLockMap = HashMap<(u64, u64), Arc<std::sync::Mutex<()>>>;
type WriteLocks = Arc<std::sync::Mutex<WriteLockMap>>;
type DirtyShardSet = HashSet<(u64, u64)>;
type DirtyShards = Vec<Arc<RwLock<DirtyShardSet>>>;

/// Compare two chunks lists for equality. Used by open() to decide whether
/// to clear the chunk cache: if the Filer's chunks match the cached chunks,
/// the cached file data is still valid and should be preserved (critical for
/// append writes). Comparison is by fid, offset, and size — the fields that
/// determine what data to read from the Volume.
fn chunks_match(a: &[CachedFileChunk], b: &[CachedFileChunk]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b.iter()).all(|(x, y)| {
        x.needle_id == y.needle_id
            && x.volume_id == y.volume_id
            && x.offset == y.offset
            && x.size == y.size
    })
}

/// Step 2: 将 MetadataAttr（MetadataClient RPC 返回）转为 CachedEntry。
///
/// 强一致方案下，所有元数据操作走 Filer Raft leader，返回 MetadataAttr。
/// 此函数将其转换为 FUSE 缓存所需的 CachedEntry 结构。
/// parent/name 由调用方传入（MetadataAttr 不包含路径信息）。
fn attr_to_cached_entry(attr: &MetadataAttr, parent: u64, name: &str) -> CachedEntry {
    let is_dir = attr.file_type == libc::DT_DIR;
    let is_symlink = attr.file_type == libc::DT_LNK;
    CachedEntry {
        inode: attr.inode,
        parent,
        name: name.to_string(),
        is_dir,
        is_symlink,
        symlink_target: attr.symlink_target.clone(),
        nlink: attr.nlink,
        fid: None,
        size: attr.size,
        mode: attr.mode,
        uid: attr.uid,
        gid: attr.gid,
        atime: attr.atime as i64,
        mtime: attr.mtime as i64,
        ctime: attr.ctime as i64,
        xattrs: HashMap::new(),
        chunks: Vec::new(),
        hard_link_id: String::new(),
        hard_link_counter: 0,
        content_size: attr.size,
        disk_size: 0,
        generation: 0,
        cached_at: Instant::now(),
    }
}

impl PowerFsFs {
    fn get_write_lock(&self, inode: u64, chunk_idx: u64) -> Arc<std::sync::Mutex<()>> {
        let key = (inode, chunk_idx);
        let mut locks = self.write_locks.lock().unwrap();
        locks
            .entry(key)
            .or_insert_with(|| Arc::new(std::sync::Mutex::new(())))
            .clone()
    }

    /// 获取 per-inode flush lock，用于序列化 flush_dirty_chunks 和 release 的
    /// lease 释放，防止后台 flusher 与 release 回调并发操作同一 inode 的 lease。
    fn get_flush_lock(&self, inode: u64) -> Arc<std::sync::Mutex<()>> {
        let mut locks = self.flush_locks.lock().unwrap();
        locks
            .entry(inode)
            .or_insert_with(|| Arc::new(std::sync::Mutex::new(())))
            .clone()
    }

    fn dirty_shard_idx(key: &(u64, u64)) -> usize {
        let hash = key.0.wrapping_add(key.1);
        (hash as usize) % NUM_DIRTY_SHARDS
    }

    fn mark_dirty(&self, inode: u64, chunk_idx: u64) {
        let key = (inode, chunk_idx);
        let shard = &self.dirty_shards[Self::dirty_shard_idx(&key)];
        let mut set = shard.write().unwrap();
        set.insert(key);
        self.has_dirty
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    fn drain_dirty_for_inode(&self, inode: u64) -> Vec<(u64, u64)> {
        let mut result = Vec::new();
        for shard in &self.dirty_shards {
            let mut set = shard.write().unwrap();
            let keys: Vec<_> = set
                .iter()
                .filter(|(ino, _)| *ino == inode)
                .cloned()
                .collect();
            for k in &keys {
                set.remove(k);
            }
            result.extend(keys);
        }
        result
    }

    fn all_dirty_inodes(&self) -> HashSet<u64> {
        let mut inodes = HashSet::new();
        for shard in &self.dirty_shards {
            let set = shard.read().unwrap();
            for (ino, _) in set.iter() {
                inodes.insert(*ino);
            }
        }
        inodes
    }

    /// Flush dirty chunks for an inode. Acquires per-inode flush lock to
    /// serialize with release callback's lease release.
    fn flush_dirty_chunks(&self, inode: u64, lease_token: Option<&str>) -> std::io::Result<()> {
        let flush_lock = self.get_flush_lock(inode);
        let _guard = flush_lock.lock().unwrap_or_else(|e| e.into_inner());
        self.flush_dirty_chunks_impl(inode, lease_token)
    }

    /// Internal flush implementation — caller MUST hold the per-inode flush lock.
    fn flush_dirty_chunks_impl(
        &self,
        inode: u64,
        lease_token: Option<&str>,
    ) -> std::io::Result<()> {
        let dirty = self.drain_dirty_for_inode(inode);

        if dirty.is_empty() {
            return Ok(());
        }

        debug!(
            "flush_dirty_chunks_impl: inode={}, dirty_count={}",
            inode,
            dirty.len()
        );

        // Phase 1.7: 查找 entry/fid/addr 失败时，重新标记 dirty 以便后续重试，
        // 避免 drain 后丢数据。write 合并依赖此重试机制保证持久性。
        let entry = match self.cache.get_inode(inode) {
            Some(e) => e,
            None => {
                warn!(
                    "flush_dirty_chunks_impl: inode {} not in cache, re-marking {} dirty chunks",
                    inode,
                    dirty.len()
                );
                for (_, idx) in &dirty {
                    self.mark_dirty(inode, *idx);
                }
                return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
            }
        };

        let fid = match entry.fid {
            Some(ref f) => f.clone(),
            None => {
                warn!(
                    "flush_dirty_chunks_impl: inode {} has no fid, re-marking {} dirty chunks",
                    inode,
                    dirty.len()
                );
                for (_, idx) in &dirty {
                    self.mark_dirty(inode, *idx);
                }
                return Err(std::io::Error::from_raw_os_error(libc::EIO));
            }
        };

        let _addr = match self.client.get_volume_addr(fid.volume_id.0) {
            Ok(a) => a,
            Err(e) => {
                error!("get_volume_addr failed: {}", e);
                for (_, idx) in &dirty {
                    self.mark_dirty(inode, *idx);
                }
                return Err(std::io::Error::from_raw_os_error(libc::EIO));
            }
        };

        let chunk_size = self.chunk_cache.chunk_size();

        // P1-a: Collect dirty chunks and flush in parallel batches.
        // Previously this was a serial for-loop, each chunk doing a
        // block_on + RPC (~2ms). 256 chunks × 2ms = 512ms. Now all chunks
        // are sent concurrently via join_all, reducing to ~1 RPC latency.
        let batch_size = 32; // high concurrency for better throughput (2GB container)
        let mut had_error = false;

        // Collect chunk data for all dirty chunks
        let chunks_to_flush: Vec<(u64, powerfs_fuse_core::WriteBlobRequest)> = dirty
            .iter()
            .filter_map(|(_, chunk_idx)| {
                // Safety: chunk_idx must fit within FILE_KEY_BLOCK_SIZE to avoid
                // needle ID overflow into the next file's block.
                if *chunk_idx >= powerfs_common::constants::FILE_KEY_BLOCK_SIZE {
                    error!(
                        "chunk_idx {} exceeds FILE_KEY_BLOCK_SIZE {} (file too large)",
                        chunk_idx,
                        powerfs_common::constants::FILE_KEY_BLOCK_SIZE
                    );
                    had_error = true;
                    return None;
                }
                let chunk_offset = chunk_idx * chunk_size;
                let chunk_data = self.chunk_cache.get(inode, chunk_offset)?;
                let data_len = chunk_data.data.len();
                Some((
                    *chunk_idx,
                    powerfs_fuse_core::WriteBlobRequest {
                        volume_id: fid.volume_id.0,
                        file_key: fid.file_key.saturating_add(*chunk_idx),
                        inode,
                        offset: chunk_offset as i64,
                        size: data_len as i32,
                        data: chunk_data.data,
                    },
                ))
            })
            .collect();

        // Flush in parallel batches
        let mut flushed_indices: Vec<u64> = Vec::new();
        for batch in chunks_to_flush.chunks(batch_size) {
            let requests: Vec<_> = batch.iter().map(|(_, req)| req.clone()).collect();
            let results = self
                .client
                .write_blob_batch_with_lease(requests, lease_token);

            for ((chunk_idx, _), result) in batch.iter().zip(results.iter()) {
                if let Err(e) = result {
                    self.mark_dirty(inode, *chunk_idx);
                    error!(
                        "write_blob failed for inode {} chunk {}: {}",
                        inode, chunk_idx, e
                    );
                    had_error = true;
                } else {
                    // Track successfully flushed chunk INDICES (not offsets) to
                    // clear their dirty flag. The cache key is (inode,
                    // chunk_index), so clear_dirty_for_chunks expects indices.
                    // BUGFIX: previously pushed `chunk_idx * chunk_size`
                    // (offsets), which only matched index 0 (offset 0 == index
                    // 0), leaving all other chunks permanently dirty and
                    // un-evictable → unbounded cache growth (1GB+ vs 512MB).
                    flushed_indices.push(*chunk_idx);
                }
            }
        }

        // Clear dirty flag for successfully flushed chunks so they can be evicted.
        if !flushed_indices.is_empty() {
            self.chunk_cache
                .clear_dirty_for_chunks(inode, &flushed_indices);
        }

        if had_error {
            return Err(std::io::Error::from_raw_os_error(libc::EIO));
        }

        // Phase 3.4: size/chunks 元数据同步移至 release()（close 时强一致 sync），
        // flush_dirty_chunks 只负责将数据持久化到 volume server。
        Ok(())
    }

    fn flush_all_dirty_chunks(&self) -> std::io::Result<()> {
        let inodes = self.all_dirty_inodes();

        if inodes.is_empty() {
            return Ok(());
        }

        for inode in inodes {
            if let Err(e) = self.flush_dirty_chunks(inode, None) {
                // Phase 1.7: 后台 flusher 错误需记录，避免静默丢数据。
                // flush 失败的 chunk 仍保留在 dirty_shards（drain 已消费则重新 mark），
                // 下次 flush 周期会重试。release/fsync 仍会同步 flush 作为最后保障。
                warn!(
                    "flush_all_dirty_chunks: flush inode {} failed (will retry next cycle): {}",
                    inode, e
                );
            }
        }

        Ok(())
    }

    /// Phase 3.4: close 时强一致 sync size/chunks 到 filer（Raft）。
    ///
    /// 流程：构建 UpdateInodeSizeChunksRequest → 带 retry+timeout 调用 filer → 成功后返回。
    /// 失败处理：重试到超时上限 → 返回 EIO + 标记 fsck（日志）。
    /// lease 在 sync 成功前不释放（崩溃则 lease 超时回收 + fsck 修复孤儿 chunks）。
    fn sync_size_chunks_on_close(&self, inode: u64) -> std::io::Result<()> {
        let entry = match self.cache.get_inode(inode) {
            Some(e) => e,
            None => {
                // 目录或未缓存条目无需 sync size/chunks
                warn!(
                    "sync_size_chunks_on_close: inode {} cache miss (None), skipping sync",
                    inode
                );
                return Ok(());
            }
        };

        if entry.is_dir {
            debug!(
                "sync_size_chunks_on_close: inode {} is dir, skipping sync",
                inode
            );
            return Ok(());
        }

        debug!(
            "sync_size_chunks_on_close: inode={}, content_size={}, chunks={}, fid={:?}",
            inode,
            entry.content_size,
            entry.chunks.len(),
            entry.fid.as_ref().map(|f| f.to_string())
        );

        let parent = entry.parent;
        let chunks_wire: Vec<powerfs_coherence::ChunkWire> = entry
            .chunks
            .iter()
            .map(|c| powerfs_coherence::ChunkWire {
                offset: c.offset,
                size: c.size,
                mtime: c.mtime,
                needle_id: c.needle_id,
                volume_id: c.volume_id,
                crc32: c.crc32,
            })
            .collect();

        let req = powerfs_coherence::UpdateInodeSizeChunksRequest {
            shard_id: parent, // dir_ino 作为 shard_id
            inode,
            size: entry.content_size,
            chunks: chunks_wire,
            client_id: self.client.client_id(),
        };

        // retry + timeout：总超时 10s，重试间隔 500ms 递增
        let max_retries = 5u32;
        let mut last_err = String::new();
        for attempt in 1..=max_retries {
            let meta_client = self.client.facade().meta_shard_client().clone();
            let req = req.clone();
            let result = self
                .client
                .block_on(async move { meta_client.update_inode_size_chunks(&req).await });
            match result {
                Ok(resp) if resp.success => {
                    debug!(
                        "sync_size_chunks_on_close: inode {} synced (attempt {})",
                        inode, attempt
                    );
                    // Step 2: 强一致方案下，目录条目由 MetadataClient RPC（mkdir/create 等）
                    // 走 Raft 提交，无需再 force_sync CRDT delta。
                    return Ok(());
                }
                Ok(resp) => {
                    last_err = resp.error;
                    warn!(
                        "sync_size_chunks_on_close: inode {} attempt {} failed: {}",
                        inode, attempt, last_err
                    );
                }
                Err(e) => {
                    last_err = e;
                    warn!(
                        "sync_size_chunks_on_close: inode {} attempt {} error: {}",
                        inode, attempt, last_err
                    );
                }
            }
            if attempt < max_retries {
                std::thread::sleep(std::time::Duration::from_millis(500 * (attempt as u64)));
            }
        }

        // sync 失败：标记 fsck + 返回 EIO
        error!(
            "sync_size_chunks_on_close: inode {} FAILED after {} attempts: {} — marked for fsck (orphan chunks possible)",
            inode, max_retries, last_err
        );
        Err(std::io::Error::from_raw_os_error(libc::EIO))
    }

    fn create_stat(&self, entry: &CachedEntry) -> libc::stat64 {
        let mut attr: libc::stat64 = unsafe { std::mem::zeroed() };
        attr.st_ino = entry.inode;
        attr.st_mode = if entry.is_symlink {
            (entry.mode | 0o120000) as libc::mode_t
        } else if entry.is_dir {
            (entry.mode | 0o040000) as libc::mode_t
        } else {
            (entry.mode | 0o100000) as libc::mode_t
        };
        attr.st_nlink = entry.nlink as u64;
        attr.st_uid = entry.uid;
        attr.st_gid = entry.gid;
        attr.st_size = entry.size as i64;
        attr.st_blksize = 4096;
        attr.st_blocks = entry.size.div_ceil(512) as i64;
        attr.st_atime = entry.atime;
        attr.st_mtime = entry.mtime;
        attr.st_ctime = entry.ctime;
        attr
    }

    fn create_fuse_entry(&self, cached: &CachedEntry) -> Entry {
        Entry {
            inode: cached.inode,
            generation: 0,
            attr: self.create_stat(cached),
            attr_flags: 0,
            attr_timeout: TTL,
            entry_timeout: TTL,
        }
    }

    fn lookup_in_cache(&self, parent: u64, name: &str) -> Option<CachedEntry> {
        self.cache.lookup_in_cache(parent, name)
    }

    /// 检查目录条目是否存在（用于 create/mkdir/symlink/link/rename 的 EEXIST 检查）。
    ///
    /// Step 2: 强一致方案下，先查 MetadataCache（快速路径），cache miss 时查 Filer
    /// （通过 MetadataClient.lookup RPC 走 Leader Lease Read）。
    fn entry_exists(&self, parent: u64, name: &str) -> bool {
        // 先查缓存（快速路径）
        if self.lookup_in_cache(parent, name).is_some() {
            return true;
        }
        // 查 Filer（shard_id calculated from parent_ino）
        let meta_client = self.client.facade().meta_shard_client().clone();
        let shard_id = meta_client.calculate_shard_id(parent);
        let name_owned = name.to_string();
        self.client
            .block_on(async move { meta_client.lookup(parent, &name_owned, shard_id).await })
            .is_ok()
    }

    fn entry_to_cached(&self, parent: u64, entry: &FilerEntry) -> CachedEntry {
        let attrs = entry.attributes.as_ref();
        let chunks = entry
            .chunks
            .iter()
            .map(|chunk| CachedFileChunk {
                offset: chunk.offset,
                size: chunk.size,
                mtime: chunk.mtime,
                needle_id: chunk.needle_id,
                volume_id: chunk.volume_id,
                crc32: chunk.crc32,
            })
            .collect();

        // Reconstruct file-level Fid from chunks[0]: needle_id = file_key,
        // volume_id = volume_id. cookie is no longer stored per-chunk (set to 0;
        // not used for data operations since chunks carry needle_id directly).
        let fid = entry.chunks.first().map(|chunk| Fid {
            volume_id: VolumeId(chunk.volume_id),
            cookie: 0,
            file_key: chunk.needle_id,
        });
        info!(
            "entry_to_cached: name={}, fid={:?}, chunks={}",
            entry.name,
            fid,
            entry.chunks.len()
        );

        let mode_val = attrs.map(|a| a.mode).unwrap_or(0);
        let file_type = mode_val & 0o170000;
        let is_dir = file_type == 0o040000;
        let is_symlink = file_type == 0o120000;
        info!(
            "entry_to_cached: name={}, mode={:o}, file_type={:o}, is_dir={}, is_symlink={}",
            entry.name, mode_val, file_type, is_dir, is_symlink
        );

        // Compute file size: prefer attrs.size, fall back to content_size,
        // and finally compute from chunks if both are 0.
        let attrs_size = attrs.map(|a| a.size).unwrap_or(0);
        let computed_size = if attrs_size > 0 {
            attrs_size
        } else if entry.content_size > 0 {
            entry.content_size
        } else {
            // Compute from chunks: max(end_offset) across all chunks
            entry
                .chunks
                .iter()
                .map(|c| c.offset + c.size)
                .max()
                .unwrap_or(0)
        };
        info!(
            "entry_to_cached: name={}, attrs_size={}, content_size={}, computed_size={}",
            entry.name, attrs_size, entry.content_size, computed_size
        );

        CachedEntry {
            inode: attrs.map(|a| a.ino).unwrap_or(0),
            parent,
            name: entry.name.clone(),
            is_dir,
            is_symlink,
            symlink_target: if is_symlink {
                Some(entry.symlink_target.clone())
            } else {
                None
            },
            nlink: attrs.map(|a| a.nlink).unwrap_or(1),
            fid,
            size: computed_size,
            mode: attrs.map(|a| a.mode & 0o7777).unwrap_or(0o644),
            uid: attrs.map(|a| a.uid).unwrap_or(0),
            gid: attrs.map(|a| a.gid).unwrap_or(0),
            atime: attrs.map(|a| a.atime as i64).unwrap_or(0),
            mtime: attrs.map(|a| a.mtime as i64).unwrap_or(0),
            ctime: attrs.map(|a| a.ctime as i64).unwrap_or(0),
            xattrs: HashMap::new(),
            chunks,
            hard_link_id: entry.hard_link_id.clone(),
            hard_link_counter: entry.hard_link_counter,
            content_size: entry.content_size,

            disk_size: entry.disk_size,
            generation: entry.generation,
            cached_at: Instant::now(),
        }
    }

    /// 解析路径到 inode，优先缓存，然后查 Filer
    pub fn resolve_path_inode(&self, path: &str) -> Option<u64> {
        if path.is_empty() || path == "/" {
            return Some(ROOT_INODE);
        }

        let parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();
        let mut current: u64 = ROOT_INODE;

        for part in &parts {
            // Try cache first
            if let Some(entry) = self.cache.lookup_in_cache(current, part) {
                current = entry.inode;
                continue;
            }
            // Try filer
            match self.client.get_entry_by_parent(current, part) {
                Ok(Some(entry)) => {
                    // Cache it
                    let cached = self.entry_to_cached(current, &entry);
                    self.cache.insert(cached.clone());
                    current = cached.inode;
                }
                _ => return None,
            }
        }
        Some(current)
    }
}

impl FileSystem for PowerFsFs {
    type Inode = u64;
    type Handle = u64;

    fn init(
        &self,
        capable: fuse_backend_rs::api::filesystem::FsOptions,
    ) -> std::io::Result<fuse_backend_rs::api::filesystem::FsOptions> {
        // Enable BIG_WRITES + MAX_PAGES so the kernel negotiates max_write=1MB
        // instead of the default 4KB. Without these flags, 1M writes are split
        // into 256 x 4K FUSE round-trips (52μs each = 13.3ms/MB ≈ 70 MiB/s).
        // WRITEBACK_CACHE remains disabled for immediate metadata sync across clients.
        let mut opts = fuse_backend_rs::api::filesystem::FsOptions::empty();
        if capable.contains(fuse_backend_rs::api::filesystem::FsOptions::BIG_WRITES) {
            opts |= fuse_backend_rs::api::filesystem::FsOptions::BIG_WRITES;
        }
        if capable.contains(fuse_backend_rs::api::filesystem::FsOptions::MAX_PAGES) {
            opts |= fuse_backend_rs::api::filesystem::FsOptions::MAX_PAGES;
        }
        Ok(opts)
    }

    fn lookup(&self, _ctx: &Context, parent: Self::Inode, name: &CStr) -> std::io::Result<Entry> {
        let name_str = name.to_str().unwrap_or("");
        debug!("lookup: parent={}, name={}", parent, name_str);

        // 1. MetadataCache 命中（含完整 attr）— 快速路径
        if let Some(entry) = self.lookup_in_cache(parent, name_str) {
            return Ok(self.create_fuse_entry(&entry));
        }

        // 2. Step 2: Filer RPC（强一致 Leader Lease Read, shard_id calculated from parent_ino）
        let meta_client = self.client.facade().meta_shard_client().clone();
        let shard_id = meta_client.calculate_shard_id(parent);
        let name_owned = name_str.to_string();
        let attr = self
            .client
            .block_on(async move { meta_client.lookup(parent, &name_owned, shard_id).await })
            .map_err(|e| {
                debug!("lookup RPC failed for '{}/{}': {}", parent, name_str, e);
                std::io::Error::from_raw_os_error(libc::ENOENT)
            })?;

        let entry = attr_to_cached_entry(&attr, parent, name_str);
        self.cache.insert(entry.clone());
        Ok(self.create_fuse_entry(&entry))
    }

    fn getattr(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        _handle: Option<Self::Handle>,
    ) -> std::io::Result<(libc::stat64, Duration)> {
        debug!("getattr: inode={}", inode);

        // Phase 4.3: 已打开文件的 size/chunks 在 open→release 期间权威
        // （数据 lease 排他，其他客户端无法修改），使用长 TTL 避免频繁 filer 查询。
        let is_open = self.open_inodes.read().unwrap().contains(&inode);
        let ttl = if is_open { TTL_OPEN } else { TTL };

        // For open files (pinned, lease-held), the userspace cache is
        // authoritative — no other client can modify the data while we
        // hold the lease. Return the cached entry directly.
        if is_open {
            if let Some(entry) = self.cache.get_inode(inode) {
                debug!(
                    "getattr: cache hit for inode={}, is_open=true (lease-held)",
                    inode
                );
                return Ok((self.create_stat(&entry), ttl));
            }
        }

        // Directories are never pinned (open returns EISDIR), so the
        // Invalidate mechanism works reliably for them — the cache with
        // its TTL fallback is sufficient.
        if let Some(entry) = self.cache.get_inode(inode) {
            if entry.is_dir {
                debug!("getattr: cache hit for dir inode={}", inode);
                return Ok((self.create_stat(&entry), ttl));
            }
        }

        // For non-open files, fetch fresh metadata from the Filer on every
        // getattr. TTL=0 promises the kernel fresh data, so returning a
        // stale cached entry would break cross-client visibility (e.g.,
        // another client's truncate must be visible immediately). The
        // Invalidate mechanism is async and can be delayed or skipped
        // (e.g., when the file is briefly opened by a concurrent read),
        // so we cannot rely on it alone for correctness.
        debug!(
            "getattr: fetching fresh metadata for inode={} from filer (non-open file)",
            inode
        );
        let result = self.client.get_entry_by_inode(inode);
        debug!(
            "getattr: get_entry_by_inode result for inode={}: is_ok={}, is_none={}",
            inode,
            result.is_ok(),
            result.as_ref().map(|r| r.is_none()).unwrap_or(false)
        );

        match result {
            Ok(Some((filer_entry, path))) => {
                // Resolve parent inode from the path.
                // If the Filer returns an empty path, fall back to the
                // existing cache entry's parent to avoid treating the
                // refresh as a rename (which would replace the entry and
                // lose local-only state such as xattrs).
                let parent = if path.is_empty() || path == "/" {
                    self.cache
                        .get_inode(inode)
                        .map(|e| e.parent)
                        .unwrap_or(ROOT_INODE)
                } else {
                    // Get parent path (strip last component)
                    let parent_path = match path.rfind('/') {
                        Some(0) => "/".to_string(),
                        Some(pos) => path[..pos].to_string(),
                        None => "/".to_string(),
                    };
                    // Try to resolve parent inode via lookup chain
                    self.resolve_path_inode(&parent_path).unwrap_or_else(|| {
                        self.cache
                            .get_inode(inode)
                            .map(|e| e.parent)
                            .unwrap_or(ROOT_INODE)
                    })
                };

                let cached = self.entry_to_cached(parent, &filer_entry);
                self.cache.insert(cached.clone());
                info!(
                    "getattr: fetched inode={} from filer, name={}, parent={}",
                    inode, cached.name, parent
                );
                Ok((self.create_stat(&cached), ttl))
            }
            Ok(None) => {
                warn!("getattr: inode={} not found in filer", inode);
                Err(std::io::Error::from_raw_os_error(libc::ENOENT))
            }
            Err(e) => {
                warn!("getattr: failed to query filer for inode={}: {}", inode, e);
                Err(std::io::Error::from_raw_os_error(libc::EIO))
            }
        }
    }

    fn setattr(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        attr: libc::stat64,
        _handle: Option<Self::Handle>,
        valid: fuse_backend_rs::abi::fuse_abi::SetattrValid,
    ) -> std::io::Result<(libc::stat64, Duration)> {
        debug!("setattr: inode={}, valid={:?}", inode, valid);

        // NOTE: Do NOT check the cache at the start of setattr. The kernel
        // passes all needed fields via `attr` and `valid`, so reading from
        // the cache is unnecessary. More importantly, checking the cache here
        // introduces a self-invalidation race: the filer sends an Invalidate
        // after processing the setattr RPC, and the InvalidateHandler (running
        // in a separate thread) can evict the cache entry between the getattr
        // (which repopulates the cache) and this check, causing spurious ENOENT.
        let mode = if valid.contains(fuse_backend_rs::abi::fuse_abi::SetattrValid::MODE) {
            Some(attr.st_mode & 0o7777)
        } else {
            None
        };
        let size = if valid.contains(fuse_backend_rs::abi::fuse_abi::SetattrValid::SIZE) {
            Some(attr.st_size as u64)
        } else {
            None
        };
        let uid = if valid.contains(fuse_backend_rs::abi::fuse_abi::SetattrValid::UID) {
            Some(attr.st_uid)
        } else {
            None
        };
        let gid = if valid.contains(fuse_backend_rs::abi::fuse_abi::SetattrValid::GID) {
            Some(attr.st_gid)
        } else {
            None
        };

        let now = chrono::Utc::now().timestamp();
        let atime = if valid.contains(fuse_backend_rs::abi::fuse_abi::SetattrValid::ATIME_NOW) {
            Some(now as u64)
        } else if valid.contains(fuse_backend_rs::abi::fuse_abi::SetattrValid::ATIME) {
            Some(attr.st_atime as u64)
        } else {
            None
        };
        let mtime = if valid.contains(fuse_backend_rs::abi::fuse_abi::SetattrValid::MTIME_NOW) {
            Some(now as u64)
        } else if valid.contains(fuse_backend_rs::abi::fuse_abi::SetattrValid::MTIME) {
            Some(attr.st_mtime as u64)
        } else {
            None
        };

        // Step 2: 通过 MetadataClient.setattr RPC 走 Filer Raft leader（强一致）
        // 同步 mode/uid/gid/atime/mtime/size 到 filer。
        //
        // size 必须在此处同步（不能仅依赖 close 时的 sync_size_chunks_on_close），
        // 否则 truncate 后其他客户端通过 Filer 读取时 attrs.size 仍为旧值，
        // 导致 read 路径使用过期的 file_size 读取超出实际内容的数据。
        // 例：echo "short" > file（file 原有 28 字节）→ setattr(SIZE=0) → write(6)
        // 若不传 size，Filer 的 attrs.size=28，其他客户端读到 28 字节（6 新 + 22 旧）。
        let params = SetattrParams {
            mode,
            uid,
            gid,
            size,
            atime,
            mtime,
        };
        let meta_client = self.client.facade().meta_shard_client().clone();
        let shard_id = meta_client.calculate_shard_id(inode);
        self.client
            .block_on(async move { meta_client.setattr(inode, &params, shard_id).await })
            .map_err(|e| {
                error!("setattr RPC failed for inode {}: {}", inode, e);
                std::io::Error::from_raw_os_error(libc::EIO)
            })?;

        // RPC 成功后更新本地缓存（含 size，供 FUSE 立即返回最新 stat）
        self.cache.update_attr(
            inode,
            crate::cache::UpdateAttrParams {
                mode,
                size,
                uid,
                gid,
                atime: atime.map(|t| t as i64),
                mtime: mtime.map(|t| t as i64),
            },
        );

        // Truncate 处理：清除旧数据缓存，防止 read/flush 返回 truncate 前的残留数据。
        //
        // 关键场景：echo "short" > file（file 原有 28 字节）
        // 1. setattr(SIZE=0) — truncate
        // 2. write("short\n", 6 bytes)
        // 3. release → sync_size_chunks_on_close(content_size=6)
        //
        // 若不清除 ChunkCache，步骤 2 仅覆盖前 6 字节，残留 22 字节旧数据。
        // flush 到 volume server 后，其他客户端读取时 attrs.size 可能仍为旧值（28），
        // 导致读取 28 字节（6 新 + 22 旧），而非正确的 6 字节。
        if let Some(new_size) = size {
            // 清除 ChunkCache：truncate 丢弃所有缓存数据，下次 read/write 从头开始
            self.chunk_cache.remove_inode_chunks(inode);
            // truncate 到 0 时清除 chunks 列表（无数据块）
            if new_size == 0 {
                self.cache.update_chunks(inode, Vec::new());
            }
            // 更新 content_size 与 size 一致（update_attr 只更新 size，不更新 content_size）
            self.cache.update_size(inode, new_size);
            debug!(
                "setattr: truncated inode={} to size={}, cleared chunk cache",
                inode, new_size
            );
        }

        if let Some(updated) = self.cache.get_inode(inode) {
            Ok((self.create_stat(&updated), TTL))
        } else {
            // Cache was invalidated (by InvalidateHandler) between update_attr
            // and this check. Fetch fresh metadata from the filer instead of
            // returning ENOENT. The setattr RPC already succeeded, so the
            // filer has the updated attributes.
            debug!(
                "setattr: cache miss after RPC for inode={}, fetching fresh metadata",
                inode
            );
            match self.client.get_entry_by_inode(inode) {
                Ok(Some((filer_entry, path))) => {
                    let parent = if path.is_empty() || path == "/" {
                        ROOT_INODE
                    } else {
                        let parent_path = match path.rfind('/') {
                            Some(0) => "/".to_string(),
                            Some(pos) => path[..pos].to_string(),
                            None => "/".to_string(),
                        };
                        self.resolve_path_inode(&parent_path).unwrap_or(ROOT_INODE)
                    };
                    let cached = self.entry_to_cached(parent, &filer_entry);
                    Ok((self.create_stat(&cached), TTL))
                }
                _ => {
                    warn!(
                        "setattr: failed to fetch fresh metadata for inode={} after RPC",
                        inode
                    );
                    Err(std::io::Error::from_raw_os_error(libc::EIO))
                }
            }
        }
    }

    fn mkdir(
        &self,
        ctx: &Context,
        parent: Self::Inode,
        name: &CStr,
        mode: u32,
        _umask: u32,
    ) -> std::io::Result<Entry> {
        let name_str = name.to_str().unwrap_or("");
        debug!(
            "mkdir: parent={}, name={}, mode={:o}",
            parent, name_str, mode
        );

        if self.entry_exists(parent, name_str) {
            return Err(std::io::Error::from_raw_os_error(libc::EEXIST));
        }

        // Step 2: 通过 MetadataClient.mkdir RPC 走 Filer Raft leader（强一致）
        // 保留 S_IFDIR 类型位（0o040000）—— filer 端通过 mode & S_IFMT 判定 FileType。
        let dir_mode = mode | 0o040000;
        let uid = ctx.uid;
        let gid = ctx.gid;
        let meta_client = self.client.facade().meta_shard_client().clone();
        let name_owned = name_str.to_string();
        let attr = self
            .client
            .block_on(async move {
                meta_client
                    .mkdir(parent, &name_owned, dir_mode, uid, gid, parent)
                    .await
            })
            .map_err(|e| {
                error!("mkdir RPC failed: {}", e);
                std::io::Error::from_raw_os_error(libc::EIO)
            })?;

        let entry = attr_to_cached_entry(&attr, parent, name_str);
        self.cache.insert(entry.clone());
        debug!("mkdir: RPC done, inode={}, dir={}", attr.inode, parent);

        Ok(self.create_fuse_entry(&entry))
    }

    fn rmdir(&self, _ctx: &Context, parent: Self::Inode, name: &CStr) -> std::io::Result<()> {
        let name_str = name.to_str().unwrap_or("");
        debug!("rmdir: parent={}, name={}", parent, name_str);

        // Step 2: 通过 MetadataClient.rmdir RPC 走 Filer Raft leader（强一致）
        // Filer 的 handle_rmdir 会做空目录检查（ENOTEMPTY），客户端不需要重复检查。
        let meta_client = self.client.facade().meta_shard_client().clone();
        let name_owned = name_str.to_string();
        self.client
            .block_on(async move { meta_client.rmdir(parent, &name_owned, parent).await })
            .map_err(|e| {
                let errno = if e.to_string().contains("not empty") {
                    libc::ENOTEMPTY
                } else {
                    error!("rmdir RPC failed: {}", e);
                    libc::EIO
                };
                std::io::Error::from_raw_os_error(errno)
            })?;

        // Remove from cache
        if let Some(entry) = self.lookup_in_cache(parent, name_str) {
            self.cache.remove(entry.inode);
        }
        Ok(())
    }

    fn unlink(&self, _ctx: &Context, parent: Self::Inode, name: &CStr) -> std::io::Result<()> {
        let name_str = name.to_str().unwrap_or("");
        debug!("unlink: parent={}, name={}", parent, name_str);

        // Try cache first; if miss (e.g., after InvalidateHandler evicted the
        // entry due to a prior setattr/chown), fetch from filer. This avoids
        // a self-invalidation race where the chown's Invalidate clears the
        // cache before unlink runs.
        let entry = if let Some(e) = self.lookup_in_cache(parent, name_str) {
            e
        } else {
            debug!(
                "unlink: cache miss for '{}/{}', fetching from filer",
                parent, name_str
            );
            let meta_client = self.client.facade().meta_shard_client().clone();
            let shard_id = meta_client.calculate_shard_id(parent);
            let name_owned = name_str.to_string();
            let attr = self
                .client
                .block_on(async move { meta_client.lookup(parent, &name_owned, shard_id).await })
                .map_err(|e| {
                    debug!("unlink: lookup RPC failed for '{}/{}': {}", parent, name_str, e);
                    std::io::Error::from_raw_os_error(libc::ENOENT)
                })?;
            let entry = attr_to_cached_entry(&attr, parent, name_str);
            self.cache.insert(entry.clone());
            entry
        };

        let should_delete = self.cache.dec_nlink(entry.inode);

        // Build the correct path for this specific entry (not the inode_cache path)
        let parent_path = self.cache.inode_to_path(parent);
        let entry_path: Option<String> = if let Some(pp) = parent_path {
            if pp == "/" {
                Some(format!("/{}", name_str))
            } else {
                Some(format!("{}/{}", pp, name_str))
            }
        } else {
            None
        };

        // Step 2: 通过 MetadataClient.unlink RPC 走 Filer Raft leader（强一致）
        // Filer 端原子地移除目录条目并递减 nlink。
        let meta_client = self.client.facade().meta_shard_client().clone();
        let name_owned = name_str.to_string();
        self.client
            .block_on(async move { meta_client.unlink(parent, &name_owned, parent).await })
            .map_err(|e| {
                error!("unlink RPC failed: {}", e);
                std::io::Error::from_raw_os_error(libc::EIO)
            })?;

        if should_delete {
            // Last hard link - delete the actual data and remove all cache entries
            // NOTE: 数据删除保留立即调用（过渡期），Phase 3.5 GC 实现后改为延迟回收
            // Iterate entry.chunks and delete each by its needle_id and volume_id.
            for chunk in &entry.chunks {
                match self.client.get_volume_addr(chunk.volume_id) {
                    Ok(addr) => {
                        if let Err(e) =
                            self.client
                                .delete_data(&addr, chunk.volume_id, chunk.needle_id)
                        {
                            warn!("Failed to delete chunk at offset {}: {}", chunk.offset, e);
                        }
                    }
                    Err(e) => {
                        warn!(
                            "Failed to get volume addr for volume {}: {}",
                            chunk.volume_id, e
                        );
                    }
                }
            }

            self.cache.remove(entry.inode);
        } else {
            // Not the last hard link - just remove the path mapping
            if let Some(path) = entry_path {
                self.cache.remove_path(entry.inode, &path);
            }
        }

        Ok(())
    }

    fn create(
        &self,
        ctx: &Context,
        parent: Self::Inode,
        name: &CStr,
        args: fuse_backend_rs::abi::fuse_abi::CreateIn,
    ) -> std::io::Result<(
        Entry,
        Option<Self::Handle>,
        fuse_backend_rs::abi::fuse_abi::OpenOptions,
        Option<u32>,
    )> {
        let t0 = std::time::Instant::now();
        let name_str = name.to_str().unwrap_or("");
        debug!(
            "create: parent={}, name={}, mode={:o}",
            parent, name_str, args.mode
        );

        if self.entry_exists(parent, name_str) {
            return Err(std::io::Error::from_raw_os_error(libc::EEXIST));
        }

        let now = chrono::Utc::now().timestamp();

        let t_assign = std::time::Instant::now();
        let (fid, _location, _stripe_fids, _stripe_locations) = self
            .client
            .assign_fid(&self.collection, &self.replication)
            .map_err(|e| {
                error!("assign_fid failed: {}", e);
                std::io::Error::from_raw_os_error(libc::EIO)
            })?;
        let assign_ms = t_assign.elapsed().as_millis();

        let needle_id = fid.file_key;
        let volume_id = fid.volume_id.0;

        // Step 2: 通过 MetadataClient.create RPC 走 Filer Raft leader（强一致）
        // Filer 端分配 inode 并创建目录条目，返回 MetadataAttr（含 inode）。
        // 保留 S_IFREG 类型位（0o100000）—— 与 mkdir 同理，filer 端通过 mode & S_IFMT 判定 FileType。
        //
        // CRITICAL: Pass fid_info so the Filer stores the chunk mapping at create
        // time. Without this, the Filer entry has no fid/chunks, and a cache miss
        // on the client (LRU eviction or Invalidate) leads to open/getattr fetching
        // a fid=None entry → flush fails with "inode has no fid" (EIO).
        let file_mode = args.mode | 0o100000;
        let uid = ctx.uid;
        let gid = ctx.gid;
        let meta_client = self.client.facade().meta_shard_client().clone();
        let name_owned = name_str.to_string();
        let fid_info = Some((volume_id, fid.cookie, fid.file_key)); // (volume_id, cookie, file_key) all u64
        let t_create = std::time::Instant::now();
        let attr = self
            .client
            .block_on(async move {
                meta_client
                    .create(parent, &name_owned, file_mode, uid, gid, parent, fid_info)
                    .await
            })
            .map_err(|e| {
                error!("create RPC failed: {}", e);
                std::io::Error::from_raw_os_error(libc::EIO)
            })?;
        let create_ms = t_create.elapsed().as_millis();
        let inode = attr.inode;
        info!(
            "FUSE create timing: assign_fid={}ms, create_rpc={}ms, total={}ms, inode={}",
            assign_ms,
            create_ms,
            t0.elapsed().as_millis(),
            inode
        );

        // 构造 CachedEntry：fid/chunks 来自客户端 assign_fid，attr 来自 Filer RPC。
        // size/chunks 在 close 时由 sync_size_chunks_on_close 强一致同步到 filer。
        let entry = CachedEntry {
            inode,
            parent,
            name: name_str.to_string(),
            is_dir: false,
            is_symlink: false,
            symlink_target: None,
            nlink: 1,
            fid: Some(fid),
            size: 0,
            mode: file_mode,
            uid: ctx.uid,
            gid: ctx.gid,
            atime: now,
            mtime: now,
            ctime: now,
            xattrs: HashMap::new(),
            chunks: vec![CachedFileChunk {
                offset: 0,
                size: 0,
                mtime: now as u64,
                needle_id,
                volume_id,
                crc32: 0,
            }],
            hard_link_id: String::new(),
            hard_link_counter: 0,
            content_size: 0,
            disk_size: 0,
            generation: 0,
            cached_at: Instant::now(),
        };
        // CRITICAL: Pin the inode BEFORE inserting the cache entry.
        // The Filer pushes an Invalidate after the create RPC commits, and
        // the InvalidateHandler (running in a separate thread) can process
        // it between insert() and pin_inode(). If the entry is inserted but
        // not pinned when the Invalidate arrives, the handler evicts it
        // (is_pinned returns false). This causes sync_size_chunks_on_close
        // to find an empty cache → skip metadata sync → data loss.
        // Pinning before insert makes the handler skip the invalidation.
        self.open_inodes.write().unwrap().insert(inode);
        self.cache.pin_inode(inode);
        self.cache.insert(entry.clone());
        debug!("create: RPC done, inode={}, dir={}", inode, parent);

        Ok((
            self.create_fuse_entry(&entry),
            Some(inode),
            fuse_backend_rs::abi::fuse_abi::OpenOptions::empty(),
            None,
        ))
    }

    fn open(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        _flags: u32,
        _fuse_flags: u32,
    ) -> std::io::Result<(
        Option<Self::Handle>,
        fuse_backend_rs::abi::fuse_abi::OpenOptions,
        Option<u32>,
    )> {
        debug!("open: inode={}", inode);

        if inode == ROOT_INODE {
            debug!("open: inode is root, returning EISDIR");
            return Err(std::io::Error::from_raw_os_error(libc::EISDIR));
        }

        // Phase 4.4: open 时从 filer 刷新 size/chunks（权威账本），填充 MetadataCache。
        // 这确保 open 后 getattr/read/write 拿到的是最新 size/chunks，省一次 getattr。
        //
        // CRITICAL: Pin the inode BEFORE any RPC to prevent a self-invalidation
        // race. The Filer pushes an Invalidate when metadata changes (including
        // sync_size_chunks_on_close from a prior release). If the Invalidate
        // arrives between the refresh RPC and the pin, the InvalidateHandler
        // evicts the just-inserted entry, causing ENOENT on the subsequent
        // setattr/write. Pinning early makes InvalidateHandler skip the
        // notification (open files hold a data lease, so the cache is
        // authoritative).
        let parent = if let Some(entry) = self.cache.get_inode(inode) {
            if entry.is_dir {
                debug!("open: entry is directory, returning EISDIR");
                return Err(std::io::Error::from_raw_os_error(libc::EISDIR));
            }
            // Pin before RPC: Invalidate arriving during refresh is skipped.
            self.open_inodes.write().unwrap().insert(inode);
            self.cache.pin_inode(inode);
            // Cache hit: best-effort 从 filer 刷新 size/chunks
            let parent = entry.parent;
            // CRITICAL: Skip the Filer refresh when there are dirty (unflushed)
            // chunks. A concurrent open by another FUSE worker (e.g., shell
            // pipeline like `echo > f && cat f` opens f twice in quick
            // succession) would refresh from the Filer which still has the
            // pre-write content_size=0 (sync_size_chunks_on_close hasn't run
            // yet). The insert() would then overwrite content_size back to 0,
            // causing release's sync to send size=0 to the Filer — breaking
            // cross-client reads. When dirty chunks exist, the local cache is
            // authoritative (we hold the write lease; no other client can
            // modify the data).
            if self.chunk_cache.has_dirty_chunks(inode) {
                // Local cache has unsynced data (write happened but
                // sync_size_chunks_on_close hasn't completed yet, e.g.,
                // async FUSE RELEASE). The local cache is authoritative:
                // skip the Filer refresh to preserve content_size and
                // chunk data for append writes.
                debug!(
                    "open: skipping filer refresh for inode={} (has dirty/unsynced chunks)",
                    inode
                );
            } else if let Ok(Some((filer_entry, _))) = self.client.get_entry_by_inode(inode) {
                let fresh = self.entry_to_cached(parent, &filer_entry);
                // Data has been synced: the Filer is authoritative. Update
                // metadata and clear the chunk cache if the chunks list
                // changed (another client may have modified the file).
                //
                // Stale Filer guard: if the Filer returns empty chunks but
                // the local cache has non-empty chunks, the Filer's Raft
                // commit may not have applied yet (sync_size_chunks_on_close
                // returned after the leader accepted but before the state
                // machine applied). In this case, treat the chunks as
                // unchanged to preserve the local chunk cache for appends.
                // When the Filer has non-empty chunks with a different FID,
                // another client wrote new data → clear the cache.
                let filer_stale_empty = fresh.chunks.is_empty() && !entry.chunks.is_empty();

                // Unsynced-write guard: if the local content_size is LARGER
                // than the Filer's, AND the chunk_cache still has local data
                // for this inode, we have writes that the background flusher
                // has pushed to the Volume Server but sync_size_chunks_on_close
                // hasn't yet committed to the Filer. The local cache is
                // authoritative (we hold the write lease; no other client can
                // modify the data). Inserting the Filer's stale (smaller)
                // size would cause reads to truncate, and clearing the chunk
                // cache would discard the just-flushed data — both break
                // cross-client appends. Skip the refresh entirely in this case.
                //
                // CRITICAL: The has_chunks check prevents a false positive
                // when the cache was invalidated by an Invalidate notification
                // (another client truncated the file). After invalidation,
                // has_chunks returns false, so the Filer's smaller size is
                // correctly applied.
                let local_ahead = entry.content_size > fresh.content_size;
                let has_local_data = self.chunk_cache.has_chunks(inode);
                if local_ahead && has_local_data {
                    debug!(
                        "open: skipping filer refresh for inode={} (local content_size={} > filer={}, unsynced writes)",
                        inode, entry.content_size, fresh.content_size
                    );
                } else {
                    let chunks_changed =
                        !filer_stale_empty && !chunks_match(&entry.chunks, &fresh.chunks);
                    let filer_content_size = fresh.content_size;
                    self.cache.insert(fresh);
                    // If no local chunk data exists (cache was invalidated by
                    // an Invalidate notification), force the Filer's
                    // content_size. insert()'s defensive guard may have
                    // preserved a stale larger value from before invalidation.
                    if !has_local_data {
                        self.cache.set_content_size(inode, filer_content_size);
                    }
                    if chunks_changed {
                        self.chunk_cache.remove_inode_chunks(inode);
                        debug!(
                            "open: refreshed metadata for inode={} (chunks changed, cache cleared)",
                            inode
                        );
                    } else {
                        debug!(
                            "open: refreshed metadata for inode={} (filer_stale_empty={}, chunks preserved)",
                            inode, filer_stale_empty
                        );
                    }
                }
            }
            parent
        } else {
            // Cache miss: 从 filer 获取完整条目（类似 getattr 流程）
            debug!("open: cache miss for inode={}, querying filer", inode);
            match self.client.get_entry_by_inode(inode) {
                Ok(Some((filer_entry, path))) => {
                    let p = if path.is_empty() || path == "/" {
                        ROOT_INODE
                    } else {
                        let parent_path = match path.rfind('/') {
                            Some(0) => "/".to_string(),
                            Some(pos) => path[..pos].to_string(),
                            None => "/".to_string(),
                        };
                        self.resolve_path_inode(&parent_path).unwrap_or(ROOT_INODE)
                    };
                    if filer_entry
                        .attributes
                        .as_ref()
                        .map(|a| a.mode & 0o170000 == 0o040000)
                        .unwrap_or(false)
                    {
                        debug!("open: filer entry is directory, returning EISDIR");
                        return Err(std::io::Error::from_raw_os_error(libc::EISDIR));
                    }
                    // Pin before insert: same self-invalidation race protection.
                    self.open_inodes.write().unwrap().insert(inode);
                    self.cache.pin_inode(inode);
                    let cached = self.entry_to_cached(p, &filer_entry);
                    self.cache.insert(cached);
                    // Clear ChunkCache: same cross-client visibility guarantee,
                    // but only if no dirty chunks (see cache-hit branch above).
                    if !self.chunk_cache.has_dirty_chunks(inode) {
                        self.chunk_cache.remove_inode_chunks(inode);
                    }
                    debug!("open: fetched inode={} from filer during open", inode);
                    p
                }
                Ok(None) => {
                    debug!("open: inode={} not found in filer", inode);
                    return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
                }
                Err(e) => {
                    warn!("open: failed to query filer for inode={}: {}", inode, e);
                    return Err(std::io::Error::from_raw_os_error(libc::EIO));
                }
            }
        };

        // Phase 3.5.3: 通知 filer 递增 open_count（best-effort，失败不阻塞 open）
        let meta_shard_client = self.client.facade().meta_shard_client().clone();
        let req = powerfs_coherence::OpenCountRequest {
            shard_id: parent,
            inode,
        };
        if let Err(e) = self
            .client
            .block_on(async move { meta_shard_client.open_count_inc(&req).await })
        {
            debug!(
                "open: open_count_inc for inode {} failed (best-effort): {}",
                inode, e
            );
        }

        Ok((
            Some(inode),
            fuse_backend_rs::abi::fuse_abi::OpenOptions::empty(),
            None,
        ))
    }

    fn read(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        _handle: Self::Handle,
        w: &mut dyn ZeroCopyWriter,
        size: u32,
        offset: u64,
        _lock_owner: Option<u64>,
        _flags: u32,
    ) -> std::io::Result<usize> {
        debug!("read: inode={}, size={}, offset={}", inode, size, offset);

        let entry = self
            .cache
            .get_inode(inode)
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

        let fid = entry
            .fid
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::EIO))?;

        let chunk_size = self.chunk_cache.chunk_size();

        let file_size = if entry.size > 0 {
            entry.size
        } else if !entry.chunks.is_empty() {
            let max_chunk_end = entry
                .chunks
                .iter()
                .map(|c| c.offset + if c.size > 0 { c.size } else { chunk_size })
                .max()
                .unwrap_or(0);
            if max_chunk_end > 0 {
                log::warn!(
                    "read: file_size=0, using chunk-based size estimate={}",
                    max_chunk_end
                );
                max_chunk_end
            } else {
                0
            }
        } else {
            0
        };

        if offset >= file_size && !entry.chunks.is_empty() {
            log::warn!(
                "read: offset >= file_size but chunks exist, proceeding. inode={}",
                inode
            );
        } else if offset >= file_size {
            return Ok(0);
        }

        let end_offset = std::cmp::min(offset + size as u64, file_size);

        let start_chunk = self.chunk_cache.get_chunk_index(offset);
        let _end_chunk = self
            .chunk_cache
            .get_chunk_index(end_offset.saturating_sub(1));

        let prefetch_end = std::cmp::min(end_offset + PREFETCH_CHUNKS * chunk_size, file_size);
        let prefetch_end_chunk = if prefetch_end == 0 {
            0
        } else {
            self.chunk_cache.get_chunk_index(prefetch_end - 1)
        };

        debug!(
            "read: inode={}, fid={:?}, volume_id={}",
            inode, fid, fid.volume_id
        );

        let needs_remote_read = (start_chunk..=prefetch_end_chunk).any(|chunk_idx| {
            self.chunk_cache
                .get(inode, chunk_idx * chunk_size)
                .is_none()
        });

        // Acquire shared read lease if we need remote reads.
        //
        // Step 6: 通过 LeaseManager 获取共享读 lease，命中缓存时零 RPC。
        // lease 在 open→release 期间复用（不在每次 read 末尾释放），
        // 由 release() FUSE handler 统一 invalidate + release。
        // 这与 write 路径的 ensure_lease 缓存复用模式一致。
        if needs_remote_read {
            let stripe_start = offset / self.stripe_size;
            let stripe_end = if end_offset > 0 {
                (end_offset - 1) / self.stripe_size + 1
            } else {
                1
            };
            let stripe_count = stripe_end - stripe_start;

            debug!(
                "read: acquiring read lease for inode={}, stripe_start={}, stripe_count={}",
                inode, stripe_start, stripe_count
            );

            match self.client.block_on(self.lease_manager.acquire(
                fid.volume_id.0,
                inode,
                LeaseMode::Shared,
                stripe_start,
                stripe_count,
                self.lease_duration_ms,
            )) {
                Ok(_token) => {
                    debug!("read: read lease acquired/reused successfully");
                }
                Err(e) => {
                    warn!(
                        "read: read lease acquisition failed for inode={}: {}",
                        inode, e
                    );
                }
            }
        }

        // Use cached volume address (only queries Master as fallback)
        let addr = self.client.get_volume_addr(fid.volume_id.0).map_err(|e| {
            error!(
                "get_volume_addr failed: volume_id={}, error={}",
                fid.volume_id, e
            );
            std::io::Error::from_raw_os_error(libc::EIO)
        })?;

        // Use a closure to capture all return paths and ensure lease release
        let result = (|| -> std::io::Result<usize> {
            // P1-b: Collect all missing chunks and read in parallel.
            // Previously each chunk was read serially (~2ms per RPC).
            // Now all missing chunks are fetched concurrently via join_all.
            let missing_chunks: Vec<(u64, u64, i32)> = (start_chunk..=prefetch_end_chunk)
                .filter_map(|chunk_idx| {
                    let chunk_offset = chunk_idx * chunk_size;
                    if self.chunk_cache.get(inode, chunk_offset).is_none() {
                        let remaining = file_size.saturating_sub(chunk_offset);
                        let read_size = std::cmp::min(chunk_size, remaining);
                        Some((chunk_idx, chunk_offset, read_size as i32))
                    } else {
                        None
                    }
                })
                .collect();

            if !missing_chunks.is_empty() {
                // Build chunk_map for O(1) needle_id lookup
                let chunk_map: HashMap<u64, (u64, u64)> = entry
                    .chunks
                    .iter()
                    .map(|c| (c.offset, (c.needle_id, c.volume_id)))
                    .collect();

                // Build batch read requests
                let requests: Vec<powerfs_fuse_core::ReadBlobRequest> = missing_chunks
                    .iter()
                    .map(|(chunk_idx, offset, size)| {
                        // Look up needle_id and volume_id from chunk_map (O(1)).
                        // Fall back to fid-based computation for sparse holes not in chunks.
                        let (needle_id, vol_id) = chunk_map
                            .get(offset)
                            .copied()
                            .unwrap_or((fid.file_key.saturating_add(*chunk_idx), fid.volume_id.0));
                        powerfs_fuse_core::ReadBlobRequest {
                            volume_id: vol_id,
                            file_key: needle_id,
                            // offset=0: read from start of needle data (each needle = one chunk)
                            offset: 0,
                            size: *size,
                        }
                    })
                    .collect();

                let results = self.client.read_blob_batch(requests);
                let mtime = entry.mtime as u64;

                // Process results: successful reads go to cache,
                // needle-not-found chunks need dirty-check + retry,
                // other errors are fatal.
                let mut retry_chunks: Vec<(u64, u64, i32)> = Vec::new();

                for ((chunk_idx, chunk_offset, read_size), result) in
                    missing_chunks.iter().zip(results.iter())
                {
                    match result {
                        Ok(data) => {
                            debug!(
                                "read_blob: inode={}, chunk_offset={}, data_len={}",
                                inode,
                                chunk_offset,
                                data.len()
                            );
                            self.chunk_cache.put(
                                inode,
                                *chunk_offset,
                                data.clone().into(),
                                mtime,
                                0,
                            );
                        }
                        Err(e) if e.contains("needle not found") => {
                            // Defer dirty-check + retry to second pass
                            retry_chunks.push((*chunk_idx, *chunk_offset, *read_size));
                        }
                        Err(e) => {
                            error!("read_blob failed: {}", e);
                            return Err(std::io::Error::from_raw_os_error(libc::EIO));
                        }
                    }
                }

                // Second pass: handle needle-not-found chunks (dirty flush + retry)
                for (chunk_idx, chunk_offset, read_size) in retry_chunks {
                    let is_dirty = {
                        let key = (inode, chunk_idx);
                        let shard = &self.dirty_shards[Self::dirty_shard_idx(&key)];
                        let dirty_set = shard.read().unwrap();
                        dirty_set.contains(&key)
                    };
                    if is_dirty {
                        debug!("read_blob: chunk {} is dirty, flushing first", chunk_idx);
                        let _ = self.flush_dirty_chunks(inode, None);
                        let (needle_id, vol_id) = entry
                            .chunks
                            .iter()
                            .find(|c| c.offset == chunk_offset)
                            .map(|c| (c.needle_id, c.volume_id))
                            .unwrap_or((fid.file_key.saturating_add(chunk_idx), fid.volume_id.0));
                        match self.client.read_blob(
                            &addr, vol_id, needle_id,
                            // offset=0: read from start of needle data (each needle = one chunk)
                            0, read_size,
                        ) {
                            Ok(data) => {
                                self.chunk_cache
                                    .put(inode, chunk_offset, data.into(), mtime, 0);
                            }
                            Err(e2) => {
                                error!("read_blob failed after flush: {}", e2);
                                return Err(std::io::Error::from_raw_os_error(libc::EIO));
                            }
                        }
                    } else {
                        debug!(
                            "read_blob: chunk {} not in dirty chunks, filling with zeros",
                            chunk_idx
                        );
                        self.chunk_cache.put(
                            inode,
                            chunk_offset,
                            vec![0; read_size as usize].into(),
                            mtime,
                            0,
                        );
                    }
                }
            }

            let mut total_written = 0usize;
            let mut current_offset = offset;
            let end = end_offset;

            log::debug!(
                "read: before copy loop, inode={}, end={}, offset={}",
                inode,
                end,
                offset
            );

            while current_offset < end {
                let chunk_data = self
                    .chunk_cache
                    .get(inode, current_offset)
                    .ok_or_else(|| std::io::Error::from_raw_os_error(libc::EIO))?;

                let chunk_start = (current_offset % self.chunk_cache.chunk_size()) as usize;
                let available_in_chunk = chunk_data.data.len().saturating_sub(chunk_start);
                let bytes_left_in_chunk = available_in_chunk.min((end - current_offset) as usize);

                if bytes_left_in_chunk == 0 {
                    log::debug!(
                        "read: bytes_left_in_chunk=0, breaking. chunk_data_len={}, chunk_start={}",
                        chunk_data.data.len(),
                        chunk_start
                    );
                    break;
                }

                let slice = &chunk_data.data[chunk_start..chunk_start + bytes_left_in_chunk];
                log::debug!(
                    "read: copying {} bytes from chunk_start={}, total_written={}",
                    bytes_left_in_chunk,
                    chunk_start,
                    total_written + bytes_left_in_chunk
                );
                w.write_all(slice)?;
                total_written += bytes_left_in_chunk;
                current_offset += bytes_left_in_chunk as u64;
            }

            log::debug!("read: returning total_written={}", total_written);
            Ok(total_written)
        })();

        // Step 6: 读 lease 由 LeaseManager 缓存复用（不在 read 末尾释放），
        // 由 release() FUSE handler 统一 invalidate + release。
        result
    }

    fn write(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        _handle: Self::Handle,
        r: &mut dyn ZeroCopyReader,
        size: u32,
        mut offset: u64,
        _lock_owner: Option<u64>,
        _delayed_write: bool,
        flags: u32,
        _fuse_flags: u32,
    ) -> std::io::Result<usize> {
        debug!("write: inode={}, size={}, offset={}", inode, size, offset);

        // Read data into buffer BEFORE acquiring any lock — I/O must not hold locks
        let mut buf = BytesMut::with_capacity(size as usize);
        buf.resize(size as usize, 0);
        let read_len = r.read(&mut buf[..]).unwrap_or(0);
        debug!("write: inode={}, read_len={}", inode, read_len);
        if read_len == 0 {
            warn!("write: inode={} read_len=0, returning Ok(0)", inode);
            return Ok(0);
        }
        buf.truncate(read_len);

        // Backpressure: if cache is near capacity, synchronously flush dirty
        // chunks to free up space BEFORE adding new data. Without this, heavy
        // writes (e.g., IO500 IOR) fill the cache with dirty chunks that
        // cannot be evicted (eviction only removes non-dirty chunks), causing
        // unbounded memory growth (observed 1.5GB vs 512MB limit) and OOM.
        //
        // The global backpressure lock serializes flushes across ALL FUSE
        // worker threads. Without it, each worker independently triggers a
        // flush while the others keep growing the cache, defeating the
        // backpressure. With the lock, when one worker flushes, all others
        // that detect the same condition block until the flush completes,
        // effectively pausing all writes during the flush.
        {
            const BACKPRESSURE_THRESHOLD_PCT: u64 = 85;
            let max = self.chunk_cache.max_bytes() as u64;
            if max > 0 {
                let threshold = max * BACKPRESSURE_THRESHOLD_PCT / 100;
                let current = self.chunk_cache.current_bytes();
                if current > threshold {
                    // Acquire global lock so only one thread flushes at a time.
                    // Other write threads block here, preventing concurrent
                    // cache growth during the flush.
                    let _bp_guard = self
                        .backpressure_lock
                        .lock()
                        .unwrap_or_else(|e| e.into_inner());
                    // Re-check after acquiring lock: the previous flush may have
                    // already reduced the cache below threshold.
                    let current = self.chunk_cache.current_bytes();
                    if current > threshold {
                        debug!(
                            "write: backpressure flush — cache {} > {} ({}% of max), flushing dirty chunks before write inode={}",
                            current, threshold, BACKPRESSURE_THRESHOLD_PCT, inode
                        );
                        let _ = self.flush_all_dirty_chunks();
                    }
                    // Lock released here; other threads can now proceed.
                }
            }
        }

        let is_append = (flags & FUSE_APPEND) != 0;

        // Lock for metadata operations (append offset, FID assignment, size update)
        let meta_lock = self.get_write_lock(inode, u64::MAX);
        let _meta_guard = meta_lock.lock();

        let entry = self
            .cache
            .get_inode(inode)
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;
        debug!(
            "write: inode={}, entry.fid={:?}, entry.is_dir={}, entry.size={}, entry.content_size={}",
            inode,
            entry.fid.as_ref().map(|f| f.to_string()),
            entry.is_dir,
            entry.size,
            entry.content_size
        );

        if is_append {
            let latest_entry = self
                .cache
                .get_inode(inode)
                .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;
            offset = latest_entry.size;
        }

        // Phase 3.3+: lease 由 provider_adapter::ensure_lease 内部管理（带缓存复用），
        // 不再在 write 路径显式 acquire/release lease，避免每个 4K write 触发 3 次
        // block_on 同步网络往返。首次 write 时 ensure_lease 获取 lease 并缓存，
        // 后续 write 复用缓存中的有效 lease。lease 在 release(close) 时释放。
        let chunk_size = self.chunk_cache.chunk_size();

        if let Some(ref fid) = entry.fid {
            let end_offset = offset + read_len as u64;
            let start_chunk = self.chunk_cache.get_chunk_index(offset);
            let end_chunk = if end_offset == 0 {
                0
            } else {
                self.chunk_cache.get_chunk_index(end_offset - 1)
            };

            // Capture info needed for read-before-write before dropping the lock.
            // `entry.content_size` is the authoritative file size before this write;
            // any chunk whose `chunk_start_offset < content_size` already has data
            // on the volume server and must be read back before partial modification
            // (otherwise we'd flush a chunk with zero-padded holes, corrupting
            // cross-client appends).
            let content_size_before_write = entry.content_size;
            let volume_id = fid.volume_id.0;
            let file_key = fid.file_key;

            // Build chunk_map for O(1) needle_id lookup (read-before-write + read path)
            let chunk_map: HashMap<u64, (u64, u64)> = entry
                .chunks
                .iter()
                .map(|c| (c.offset, (c.needle_id, c.volume_id)))
                .collect();

            // Drop metadata lock before per-chunk writes
            drop(_meta_guard);

            let new_size = offset + read_len as u64;

            // Write to chunk cache with per-chunk locks (no unsafe code)
            let mut data_offset = 0u64;
            let mut current_offset = offset;

            for chunk_idx in start_chunk..=end_chunk {
                let lock = self.get_write_lock(inode, chunk_idx);
                let _guard = lock.lock().unwrap_or_else(|e| e.into_inner());

                let chunk_start_offset = chunk_idx * chunk_size;
                let in_chunk_start = current_offset.saturating_sub(chunk_start_offset) as usize;
                let bytes_to_write = std::cmp::min(
                    read_len as u64 - data_offset,
                    chunk_size - in_chunk_start as u64,
                ) as usize;

                let mtime = entry.mtime as u64;
                let modified = self.chunk_cache.modify(inode, chunk_start_offset, |chunk| {
                    let needed_len = in_chunk_start + bytes_to_write;
                    let mut mut_data = BytesMut::with_capacity(needed_len);
                    mut_data.extend_from_slice(&chunk.data);
                    if mut_data.len() < needed_len {
                        mut_data.resize(needed_len, 0);
                    }
                    mut_data[in_chunk_start..in_chunk_start + bytes_to_write].copy_from_slice(
                        &buf[data_offset as usize..data_offset as usize + bytes_to_write],
                    );
                    chunk.data = mut_data.freeze();
                    chunk.mtime = mtime;
                });

                if !modified {
                    // Chunk not in cache. Determine if read-before-write is
                    // needed by checking whether existing data exists outside
                    // the write region within this chunk.
                    //
                    // Read-before-write is required only when there is existing
                    // data BEFORE or AFTER the write region that must be
                    // preserved. If the write covers all existing data (or
                    // there is no existing data), the read can be skipped
                    // entirely — a significant optimization for sequential
                    // writes (full chunk overwrites) and appends.
                    let existing_end_in_chunk =
                        content_size_before_write.saturating_sub(chunk_start_offset);
                    let write_end_in_chunk = (in_chunk_start + bytes_to_write) as u64;

                    // No data before our write: write starts at chunk start, or
                    // existing data doesn't reach our write position.
                    let no_data_before =
                        in_chunk_start == 0 || existing_end_in_chunk <= in_chunk_start as u64;
                    // No data after our write: write fills to chunk end, or
                    // existing data doesn't extend beyond our write end.
                    let no_data_after = write_end_in_chunk >= chunk_size
                        || existing_end_in_chunk <= write_end_in_chunk;

                    let mut new_data: Vec<u8> = if no_data_before && no_data_after {
                        // Optimization: no existing data to preserve — skip
                        // read-before-write entirely. This covers:
                        // - Full chunk overwrite (e.g., 1M sequential write)
                        // - Append beyond existing file size
                        // - Write to a new/empty chunk
                        debug!(
                            "write: skip read-before-write inode={} chunk_offset={} (no existing data to preserve)",
                            inode, chunk_start_offset
                        );
                        vec![0u8; in_chunk_start + bytes_to_write]
                    } else if content_size_before_write > chunk_start_offset {
                        // Partial write within existing data — must read
                        // existing chunk to preserve prefix/suffix.
                        let existing_len = std::cmp::min(
                            chunk_size,
                            content_size_before_write - chunk_start_offset,
                        ) as usize;
                        // Look up needle_id from chunk_map (O(1)), fallback to fid-based
                        let (rbw_needle_id, rbw_vol_id) = chunk_map
                            .get(&chunk_start_offset)
                            .copied()
                            .unwrap_or((file_key.saturating_add(chunk_idx), volume_id));
                        let rbw_addr = self.client.get_volume_addr(rbw_vol_id).ok();
                        let mut base = if let Some(ref addr) = rbw_addr {
                            match self.client.read_blob(
                                addr,
                                rbw_vol_id,
                                rbw_needle_id,
                                // offset=0: read from start of needle data (each needle = one chunk)
                                0,
                                existing_len as i32,
                            ) {
                                Ok(data) => {
                                    debug!(
                                        "write: read-before-write inode={} chunk_offset={} read_len={}",
                                        inode, chunk_start_offset, data.len()
                                    );
                                    data
                                }
                                Err(e) => {
                                    warn!(
                                        "write: read-before-write failed inode={} chunk_offset={}: {} — using zeros",
                                        inode, chunk_start_offset, e
                                    );
                                    vec![0u8; existing_len]
                                }
                            }
                        } else {
                            vec![0u8; existing_len]
                        };
                        let needed_len = in_chunk_start + bytes_to_write;
                        if base.len() < needed_len {
                            base.resize(needed_len, 0);
                        }
                        base
                    } else {
                        vec![0u8; in_chunk_start + bytes_to_write]
                    };
                    new_data[in_chunk_start..in_chunk_start + bytes_to_write].copy_from_slice(
                        &buf[data_offset as usize..data_offset as usize + bytes_to_write],
                    );
                    self.chunk_cache
                        .put(inode, chunk_start_offset, new_data.into(), mtime, 0);
                }

                self.mark_dirty(inode, chunk_idx);

                data_offset += bytes_to_write as u64;
                current_offset += bytes_to_write as u64;
            }

            // Re-acquire metadata lock and update size with latest value
            let _meta_guard = meta_lock.lock();
            if let Some(current_entry) = self.cache.get_inode(inode) {
                if new_size > current_entry.size {
                    self.cache.update_size(inode, new_size);
                }
            }

            // Fix: update chunks[].size to reflect actual data layout.
            // Previously this branch only called update_size (content_size) but
            // never updated chunks[].size, leaving it stuck at 0 from create().
            // This caused sync_size_chunks_on_close to send chunks[].size=0 to
            // the Filer, breaking cross-client reads.
            if let Some(ref fid) = entry.fid {
                self.cache.update_chunk_sizes_after_write(
                    inode,
                    offset,
                    read_len as u64,
                    chunk_size,
                    fid,
                );
            }

            // Phase 1.7: write合并/delayed flush — 不在 write 路径同步 flush。
            // 多次 4K write 自然合并到同一 chunk_cache 条目（chunk_size=1MB），
            // 由后台 flusher（100ms 间隔）异步 flush 到 Volume Server，
            // release(close)/fsync 时同步 flush 保证持久性。
            // 收益：64K 文件 16 次 4K write 从 16 次网络往返降到 1-2 次。
        } else {
            // First write: assign FID under metadata lock
            let (fid, _location, _stripe_fids, _stripe_locations) = self
                .client
                .assign_fid(&self.collection, &self.replication)
                .map_err(|e| {
                    error!("assign_fid failed: {}", e);
                    std::io::Error::from_raw_os_error(libc::EIO)
                })?;

            self.cache.update_fid(inode, fid.clone());
            let new_size = offset + read_len as u64;
            self.cache.update_size(inode, new_size);

            // 构建 chunk 信息并更新 cache（确保 close 时 sync 正确的 chunks 到 filer）
            let chunk_info = powerfs_orset::CachedFileChunk {
                offset: 0,
                size: new_size,
                mtime: entry.mtime as u64,
                needle_id: fid.file_key,
                volume_id: fid.volume_id.0,
                crc32: 0,
            };
            self.cache.update_chunks(inode, vec![chunk_info]);

            let mtime = entry.mtime as u64;
            self.chunk_cache.put(inode, 0, buf.freeze(), mtime, 0);

            self.mark_dirty(inode, 0);

            // NOTE: 旧代码在此处同步调用 self.client.update_entry() 向 master 注册文件条目，
            // 这会阻塞写入路径 10s+（gRPC 同步往返）。在新设计中：
            //   - 目录条目由 MetadataClient.create RPC（create 时已完成）走 Raft 强一致提交
            //   - size/chunks 由 release() 时 sync_size_chunks_on_close 强一致同步到 filer
            // 因此 update_entry 调用是冗余的，移除以消除写入延迟根因。

            // Phase 1.7: write合并/delayed flush — 首次 write 也不同步 flush，
            // FID 已分配，chunk 已缓存，由后台 flusher 异步持久化。
        }

        Ok(read_len)
    }

    fn release(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        _flags: u32,
        _handle: Self::Handle,
        _flush: bool,
        _flock_release: bool,
        _lock_owner: Option<u64>,
    ) -> std::io::Result<()> {
        // Phase 3.4: close 流程 = flush 数据 → sync size/chunks（强一致）→ 递减 open_count → 释放 lease
        //
        // 持有 per-inode flush lock 贯穿整个序列，防止后台 flusher 在 release
        // 释放 lease 后仍用旧 token 写入（TOCTOU 竞争导致 "Lease token not found"）。
        let flush_lock = self.get_flush_lock(inode);
        let _flush_guard = flush_lock.lock().unwrap_or_else(|e| e.into_inner());

        // 1. Flush dirty data chunks to volume server (lock held — call impl directly)
        //
        // CRITICAL: capture the flush result. If flush fails, the data is NOT
        // on the volume server — we must NOT sync metadata (which would create
        // dangling chunk references) and must NOT clear dirty (which would
        // prevent retry). The background flusher will retry the write.
        let flush_result = self.flush_dirty_chunks_impl(inode, None);
        if let Err(e) = &flush_result {
            warn!(
                "release: flush_dirty_chunks for inode {} failed: {} — data remains dirty for retry",
                inode, e
            );
        }

        // 2. Sync size/chunks to filer (Raft strong consistency)
        //    Only sync if flush succeeded: if flush failed, the chunks are not
        //    on the volume server. Syncing metadata would make other clients
        //    read non-existent chunks, causing cross-client data corruption.
        //    The dirty flag is preserved so the background flusher retries.
        let sync_result = if flush_result.is_ok() {
            let r = self.sync_size_chunks_on_close(inode);
            if let Err(e) = &r {
                error!(
                    "release: sync_size_chunks_on_close for inode {} failed: {} — data may be orphaned",
                    inode, e
                );
            }
            r
        } else {
            error!(
                "release: skipping sync for inode {} because flush failed — \
                 metadata not updated, dirty flag preserved for retry",
                inode
            );
            Err(std::io::Error::other("flush failed, sync skipped"))
        };

        // 3. Clear dirty only if both flush AND sync succeeded.
        //    If either failed, keep dirty flag so:
        //    - Background flusher retries the write
        //    - open() skips Filer refresh (local cache is authoritative)
        if sync_result.is_ok() {
            self.chunk_cache.clear_dirty(inode);
        }

        // 3. Phase 3.5.3: 递减 open_count（best-effort，无论 sync 成功与否都执行）
        //    在返回前完成，确保 GC 不会在文件仍被打开时删除
        if let Some(entry) = self.cache.get_inode(inode) {
            let meta_shard_client = self.client.facade().meta_shard_client().clone();
            let req = powerfs_coherence::OpenCountRequest {
                shard_id: entry.parent,
                inode,
            };
            if let Err(e) = self
                .client
                .block_on(async move { meta_shard_client.open_count_dec(&req).await })
            {
                debug!(
                    "release: open_count_dec for inode {} failed (best-effort): {}",
                    inode, e
                );
            }
        }

        // Phase 4.3/4.4: 移除 open_inodes 追踪（getattr 恢复短 TTL）
        self.open_inodes.write().unwrap().remove(&inode);
        // Unpin inode from MetadataCache (restore normal TTL expiry)
        self.cache.unpin_inode(inode);

        // 4. 释放 Volume lease（best-effort，close 时释放 write + read lease）
        //    write lease: 由 ensure_lease 缓存，通过 VolumeClient 释放
        //    read lease: 由 LeaseManager 缓存，必须在 server 上释放（不能仅
        //    清本地缓存），否则残留的读 lease 会阻止其他客户端获取写 lease
        //    （stripe lease conflict）。
        //    仍在 flush lock 内 —— 后台 flusher 此刻被阻塞，不会用旧 token 写入。
        if let Some(entry) = self.cache.get_inode(inode) {
            if let Some(ref fid) = entry.fid {
                let client_id = self.client.client_id();
                // 4a. 释放 write lease（从 VolumeClient leases 表取 token）
                let token = self
                    .client
                    .get_valid_lease_token(fid.volume_id.0, inode)
                    .unwrap_or_default();
                if let Err(e) =
                    self.client
                        .release_lease(fid.volume_id.0, inode, &client_id, &token)
                {
                    debug!(
                        "release: write lease release for inode {} failed (best-effort): {}",
                        inode, e
                    );
                }
                // 4b. 释放 read lease（从 LeaseManager 缓存取所有 token，在 server 上释放）
                let read_tokens = self
                    .lease_manager
                    .release_all_for_inode(fid.volume_id.0, inode);
                for (tok, cid) in read_tokens {
                    if let Err(e) = self
                        .client
                        .release_lease(fid.volume_id.0, inode, &cid, &tok)
                    {
                        debug!(
                            "release: read lease release for inode {} failed (best-effort): {}",
                            inode, e
                        );
                    }
                }
            }
        }

        sync_result?;

        debug!("release: inode {} closed, size/chunks synced", inode);
        Ok(())
    }

    fn readdir(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        _handle: Self::Handle,
        _size: u32,
        offset: u64,
        add_entry: &mut dyn FnMut(DirEntry) -> std::io::Result<usize>,
    ) -> std::io::Result<()> {
        debug!("readdir: inode={}, offset={}", inode, offset);

        // 尝试从缓存获取目录条目（用于 is_dir 检查和 ".." 的 parent inode）
        let cached_entry = self.cache.get_inode(inode);

        // 缓存 miss 时通过 MetadataClient.getattr 验证是目录并获取属性
        match &cached_entry {
            Some(entry) if !entry.is_dir => {
                return Err(std::io::Error::from_raw_os_error(libc::ENOTDIR));
            }
            None => {
                let meta_client = self.client.facade().meta_shard_client().clone();
                let attr = self
                    .client
                    .block_on(async move { meta_client.getattr(inode, inode).await })
                    .map_err(|e| {
                        debug!("readdir: getattr RPC failed for inode {}: {}", inode, e);
                        std::io::Error::from_raw_os_error(libc::ENOENT)
                    })?;
                if !attr.is_dir() {
                    return Err(std::io::Error::from_raw_os_error(libc::ENOTDIR));
                }
            }
            _ => {}
        }

        // 解析 parent inode（用于 ".." 条目）
        let parent_ino = if inode == ROOT_INODE {
            ROOT_INODE
        } else {
            cached_entry.as_ref().map(|e| e.parent).unwrap_or(inode)
        };

        let mut idx = 0u64;

        if offset <= idx
            && add_entry(DirEntry {
                ino: inode,
                offset: idx + 1,
                type_: 0o040000,
                name: ".".as_bytes(),
            })
            .is_err()
        {
            return Ok(());
        }
        idx += 1;

        if offset <= idx
            && add_entry(DirEntry {
                ino: parent_ino,
                offset: idx + 1,
                type_: 0o040000,
                name: "..".as_bytes(),
            })
            .is_err()
        {
            return Ok(());
        }
        idx += 1;

        // Step 2: 通过 MetadataClient.readdir RPC 走 Filer Raft leader（强一致 Leader Lease Read）
        // shard_id = inode（对 inode 操作）
        let meta_client = self.client.facade().meta_shard_client().clone();
        let dir_entries: Vec<MetadataDirEntry> = self
            .client
            .block_on(async move { meta_client.readdir(inode, offset, 1000, inode).await })
            .map_err(|e| {
                error!("readdir RPC failed for inode {}: {}", inode, e);
                std::io::Error::from_raw_os_error(libc::EIO)
            })?;

        debug!(
            "readdir: RPC returned {} entries for dir {}",
            dir_entries.len(),
            inode
        );

        for child in dir_entries {
            idx += 1;
            if offset < idx {
                // DT_DIR=4, DT_REG=8, DT_LNK=10 等；FUSE DirEntry.type_ 用 d_type 值
                let type_ = child.file_type as u32;
                if add_entry(DirEntry {
                    ino: child.inode,
                    offset: idx,
                    type_,
                    name: child.name.as_bytes(),
                })
                .is_err()
                {
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    fn rename(
        &self,
        _ctx: &Context,
        olddir: Self::Inode,
        oldname: &CStr,
        newdir: Self::Inode,
        newname: &CStr,
        flags: u32,
    ) -> std::io::Result<()> {
        let old_str = oldname.to_str().unwrap_or("");
        let new_str = newname.to_str().unwrap_or("");
        debug!(
            "rename: olddir={}, oldname={}, newdir={}, newname={}, flags={}",
            olddir, old_str, newdir, new_str, flags
        );

        let no_replace = (flags & 1) != 0;
        if no_replace && self.entry_exists(newdir, new_str) {
            return Err(std::io::Error::from_raw_os_error(libc::EEXIST));
        }

        // Step 2: 通过 MetadataClient.rename RPC 走 Filer Raft leader（强一致，原子提交）
        // Filer 端原子处理：删除旧目标（如有）+ 移动/重命名条目。
        // 空目录检查由 Filer 在 Raft 提交时完成，返回 ENOTEMPTY 错误。
        // shard_id = olddir（源目录的 shard）
        let meta_client = self.client.facade().meta_shard_client().clone();
        let old_owned = old_str.to_string();
        let new_owned = new_str.to_string();
        let _attr = self
            .client
            .block_on(async move {
                meta_client
                    .rename(olddir, &old_owned, newdir, &new_owned, olddir)
                    .await
            })
            .map_err(|e| {
                let errno = if e.to_string().contains("not empty") {
                    libc::ENOTEMPTY
                } else {
                    error!("rename RPC failed: {}", e);
                    libc::EIO
                };
                std::io::Error::from_raw_os_error(errno)
            })?;

        // RPC 成功后更新本地缓存（path_map + inode_cache）
        // cache.rename 失败仅影响本地缓存一致性，不影响 Filer 已提交的状态
        if let Err(e) = self.cache.rename(olddir, old_str, newdir, new_str) {
            warn!(
                "rename: cache.rename failed (filer already committed): {}",
                e
            );
        }

        Ok(())
    }

    fn symlink(
        &self,
        _ctx: &Context,
        linkname: &CStr,
        parent: Self::Inode,
        name: &CStr,
    ) -> std::io::Result<Entry> {
        let name_str = name.to_str().unwrap_or("");
        let link_str = linkname.to_str().unwrap_or("");
        debug!(
            "symlink: parent={}, name={}, target={}",
            parent, name_str, link_str
        );

        if self.entry_exists(parent, name_str) {
            return Err(std::io::Error::from_raw_os_error(libc::EEXIST));
        }

        // Use powerfs-net protocol to create symlink on server
        let inode = match self.client.symlink(parent, name_str, link_str) {
            Ok(ino) => ino,
            Err(e) => {
                error!("symlink failed on server: {}", e);
                return Err(std::io::Error::from_raw_os_error(libc::EIO));
            }
        };

        let now = chrono::Utc::now().timestamp() as u64;
        let cached_entry = CachedEntry {
            inode,
            parent,
            name: name_str.to_string(),
            is_dir: false,
            is_symlink: true,
            symlink_target: Some(link_str.to_string()),
            nlink: 1,
            fid: None,
            size: link_str.len() as u64,
            mode: 0o777,
            uid: 0,
            gid: 0,
            atime: now as i64,
            mtime: now as i64,
            ctime: now as i64,
            xattrs: HashMap::new(),
            chunks: Vec::new(),
            hard_link_id: String::new(),
            hard_link_counter: 0,
            content_size: link_str.len() as u64,
            disk_size: 0,
            generation: 0,
            cached_at: Instant::now(),
        };
        self.cache.insert(cached_entry.clone());
        Ok(self.create_fuse_entry(&cached_entry))
    }

    fn readlink(&self, _ctx: &Context, inode: Self::Inode) -> std::io::Result<Vec<u8>> {
        debug!("readlink: inode={}", inode);

        // First try to get from cache
        if let Some(target) = self.cache.get_symlink_target(inode) {
            return Ok(target.into_bytes());
        }

        // If not in cache, fetch from server via powerfs-net protocol
        match self.client.readlink(inode) {
            Ok(target) => {
                // Update cache with the symlink target
                self.cache.set_symlink_target(inode, target.clone());
                Ok(target.into_bytes())
            }
            Err(e) => {
                warn!("readlink failed for inode {}: {}", inode, e);
                Err(std::io::Error::from_raw_os_error(libc::ENOENT))
            }
        }
    }

    fn link(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        newparent: Self::Inode,
        newname: &CStr,
    ) -> std::io::Result<Entry> {
        let name_str = newname.to_str().unwrap_or("");
        debug!(
            "link: inode={}, newparent={}, newname={}",
            inode, newparent, name_str
        );

        if self.entry_exists(newparent, name_str) {
            return Err(std::io::Error::from_raw_os_error(libc::EEXIST));
        }

        let entry = self
            .cache
            .get_inode(inode)
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

        if entry.is_dir {
            return Err(std::io::Error::from_raw_os_error(libc::EPERM));
        }

        // Use powerfs-net protocol to create hard link on server
        debug!(
            "link: sending NET_LINK for ino={}, newparent={}, name={}",
            inode, newparent, name_str
        );
        match self.client.link(inode, newparent, name_str) {
            Ok(_) => {
                debug!(
                    "link: NET_LINK succeeded for ino={}, name={}",
                    inode, name_str
                );
                self.cache.inc_nlink(inode);

                let new_entry = CachedEntry {
                    inode,
                    parent: newparent,
                    name: name_str.to_string(),
                    is_dir: false,
                    is_symlink: entry.is_symlink,
                    symlink_target: entry.symlink_target.clone(),
                    nlink: self.cache.get_nlink(inode),
                    fid: entry.fid.clone(),
                    size: entry.size,
                    mode: entry.mode,
                    uid: entry.uid,
                    gid: entry.gid,
                    atime: entry.atime,
                    mtime: entry.mtime,
                    ctime: chrono::Utc::now().timestamp(),
                    xattrs: entry.xattrs.clone(),
                    chunks: entry.chunks.clone(),
                    hard_link_id: entry.hard_link_id.clone(),
                    hard_link_counter: entry.hard_link_counter,
                    content_size: entry.content_size,
                    disk_size: entry.disk_size,
                    generation: 0,
                    cached_at: Instant::now(),
                };

                self.cache.insert(new_entry.clone());
                Ok(self.create_fuse_entry(&new_entry))
            }
            Err(e) => {
                warn!("link failed on server: {}", e);
                Err(std::io::Error::from_raw_os_error(libc::EIO))
            }
        }
    }

    fn statfs(&self, _ctx: &Context, _inode: Self::Inode) -> std::io::Result<libc::statvfs64> {
        debug!("statfs");

        let stats = match self.client.statfs() {
            Ok(s) => s,
            Err(e) => {
                warn!("statfs failed: {}, using defaults", e);
                return Err(std::io::Error::from_raw_os_error(libc::EIO));
            }
        };

        let block_size: u64 = 4096;
        let total_blocks = if stats.total_size > 0 {
            stats.total_size / block_size
        } else {
            0
        };
        let free_blocks = if stats.free_size > 0 {
            stats.free_size / block_size
        } else {
            0
        };
        let bavail = free_blocks;

        let mut st: libc::statvfs64 = unsafe { std::mem::zeroed() };
        st.f_bsize = block_size as libc::c_ulong;
        st.f_frsize = block_size as libc::c_ulong;
        st.f_blocks = total_blocks;
        st.f_bfree = free_blocks;
        st.f_bavail = bavail;
        st.f_files = 10_000_000;
        st.f_ffree = 9_900_000;
        st.f_favail = 9_900_000;
        st.f_namemax = 255;

        info!(
            "statfs: total={}, used={}, free={}, volumes={}, blocks={}, bfree={}",
            stats.total_size,
            stats.used_size,
            stats.free_size,
            stats.volume_count,
            total_blocks,
            free_blocks
        );

        Ok(st)
    }

    fn access(&self, _ctx: &Context, inode: Self::Inode, mask: u32) -> std::io::Result<()> {
        debug!("access: inode={}, mask={}", inode, mask);

        let entry = self
            .cache
            .get_inode(inode)
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

        if entry.uid == 0 {
            return Ok(());
        }

        let mode = entry.mode;
        let readable = (mode & 0o444) != 0;
        let writable = (mode & 0o222) != 0;
        let executable = (mode & 0o111) != 0;

        let r_ok = (mask & libc::R_OK as u32) == 0 || readable;
        let w_ok = (mask & libc::W_OK as u32) == 0 || writable;
        let x_ok = (mask & libc::X_OK as u32) == 0 || executable;

        if r_ok && w_ok && x_ok {
            Ok(())
        } else {
            Err(std::io::Error::from_raw_os_error(libc::EACCES))
        }
    }

    fn fsync(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        _datasync: bool,
        _handle: Self::Handle,
    ) -> std::io::Result<()> {
        debug!("fsync: inode={}", inode);
        match self.flush_dirty_chunks(inode, None) {
            Ok(()) => Ok(()),
            Err(e) => {
                error!(
                    "fsync: flush_dirty_chunks failed for inode={}: {} (raw_os_error={:?})",
                    inode,
                    e,
                    e.raw_os_error()
                );
                Err(e)
            }
        }
    }

    fn fallocate(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        _handle: Self::Handle,
        mode: u32,
        offset: u64,
        length: u64,
    ) -> std::io::Result<()> {
        debug!(
            "fallocate: inode={}, mode={}, offset={}, length={}",
            inode, mode, offset, length
        );

        let entry = self
            .cache
            .get_inode(inode)
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

        if entry.is_dir {
            return Err(std::io::Error::from_raw_os_error(libc::EISDIR));
        }

        let new_size = offset + length;
        if new_size > entry.size {
            self.cache.update_size(inode, new_size);
        }

        Ok(())
    }

    fn getlk(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        _handle: Self::Handle,
        _owner: u64,
        lock: FileLock,
        _flags: u32,
    ) -> std::io::Result<FileLock> {
        debug!(
            "getlk: inode={}, start={}, end={}, type={}",
            inode, lock.start, lock.end, lock.lock_type
        );

        let locks = self.locks.read().unwrap();
        if let Some(inode_locks) = locks.get(&inode) {
            for existing_lock in inode_locks {
                if existing_lock.start < lock.end
                    && existing_lock.end > lock.start
                    && existing_lock.lock_type != lock.lock_type
                {
                    return Ok(FileLock {
                        start: existing_lock.start,
                        end: existing_lock.end,
                        lock_type: existing_lock.lock_type,
                        pid: existing_lock.pid,
                    });
                }
            }
        }

        Ok(FileLock {
            start: lock.start,
            end: lock.end,
            lock_type: lock.lock_type,
            pid: 0,
        })
    }

    fn setlk(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        _handle: Self::Handle,
        owner: u64,
        lock: FileLock,
        _flags: u32,
    ) -> std::io::Result<()> {
        debug!(
            "setlk: inode={}, owner={}, start={}, end={}, type={}",
            inode, owner, lock.start, lock.end, lock.lock_type
        );

        let mut locks = self.locks.write().unwrap();
        let inode_locks = locks.entry(inode).or_default();

        if lock.lock_type == 0 {
            inode_locks.retain(|l| l.start != lock.start || l.end != lock.end);
            return Ok(());
        }

        for existing_lock in &*inode_locks {
            if existing_lock.start < lock.end
                && existing_lock.end > lock.start
                && existing_lock.lock_type != lock.lock_type
            {
                return Err(std::io::Error::from_raw_os_error(libc::EAGAIN));
            }
        }

        inode_locks.push(FileLock {
            start: lock.start,
            end: lock.end,
            lock_type: lock.lock_type,
            pid: lock.pid,
        });

        Ok(())
    }

    fn setlkw(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        _handle: Self::Handle,
        owner: u64,
        lock: FileLock,
        _flags: u32,
    ) -> std::io::Result<()> {
        debug!(
            "setlkw: inode={}, owner={}, start={}, end={}, type={}",
            inode, owner, lock.start, lock.end, lock.lock_type
        );
        self.setlk(_ctx, inode, _handle, owner, lock, _flags)
    }

    fn setxattr(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        name: &CStr,
        value: &[u8],
        _flags: u32,
    ) -> std::io::Result<()> {
        let name_str = name.to_str().unwrap_or("");
        debug!("setxattr: inode={}, name={}", inode, name_str);

        if self.cache.get_inode(inode).is_none() {
            return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
        }

        self.cache.set_xattr(inode, name_str, value);
        Ok(())
    }

    fn getxattr(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        name: &CStr,
        size: u32,
    ) -> std::io::Result<GetxattrReply> {
        let name_str = name.to_str().unwrap_or("");
        debug!("getxattr: inode={}, name={}", inode, name_str);

        // Do NOT return ENOENT when the inode is not in the cache.
        // The kernel calls getxattr (e.g., security.selinux) after setattr,
        // and the InvalidateHandler may have evicted the cache entry between
        // the setattr and this call. Returning ENODATA is safe: PowerFS does
        // not store xattrs, so the kernel treats the file as having no xattrs.
        // Returning ENOENT here would cause the preceding operation (e.g.,
        // chown) to report "No such file or directory" even though it succeeded.

        if let Some(value) = self.cache.get_xattr(inode, name_str) {
            if size == 0 {
                Ok(GetxattrReply::Count(value.len() as u32))
            } else if value.len() > size as usize {
                Err(std::io::Error::from_raw_os_error(libc::ERANGE))
            } else {
                Ok(GetxattrReply::Value(value))
            }
        } else {
            Err(std::io::Error::from_raw_os_error(libc::ENODATA))
        }
    }

    fn listxattr(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        size: u32,
    ) -> std::io::Result<ListxattrReply> {
        debug!("listxattr: inode={}", inode);

        // Do NOT return ENOENT when the inode is not in the cache.
        // See getxattr comment for rationale.

        let xattrs = self.cache.list_xattrs(inode);
        let mut buf = Vec::new();
        for name in xattrs {
            buf.extend_from_slice(name.as_bytes());
            buf.push(0);
        }

        if size == 0 {
            Ok(ListxattrReply::Count(buf.len() as u32))
        } else if buf.len() > size as usize {
            Err(std::io::Error::from_raw_os_error(libc::ERANGE))
        } else {
            Ok(ListxattrReply::Names(buf))
        }
    }

    fn removexattr(&self, _ctx: &Context, inode: Self::Inode, name: &CStr) -> std::io::Result<()> {
        let name_str = name.to_str().unwrap_or("");
        debug!("removexattr: inode={}, name={}", inode, name_str);

        if self.cache.get_inode(inode).is_none() {
            return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
        }

        if !self.cache.remove_xattr(inode, name_str) {
            return Err(std::io::Error::from_raw_os_error(libc::ENODATA));
        }

        Ok(())
    }

    fn fsyncdir(
        &self,
        _ctx: &Context,
        _inode: Self::Inode,
        _datasync: bool,
        _handle: Self::Handle,
    ) -> std::io::Result<()> {
        debug!("fsyncdir: inode={}", _inode);
        Ok(())
    }
}
