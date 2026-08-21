use crate::cache::{
    CachedEntry, ChunkCache, DentryLeaseStatus, EntryState, HoldState, MetadataCache, ROOT_INODE,
};
use bytes::BytesMut;
use dashmap::DashMap;
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
use std::os::unix::io::AsRawFd;
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

/// P2.5: Inline 小文件数据硬上限 (与 Filer 端 `INLINE_HARD_LIMIT` 一致).
/// 超过此大小的文件不能走 Inline 模式. P2.5c: inline buffer 累积超过
/// max_size×1.5 (滞后窗口, 上限此值) 时自动迁移到 Flat (MIGRATE_INLINE_ALLOC).
/// 仅当迁移 RPC 失败时才返回 EFBIG.
const INLINE_HARD_LIMIT: usize = 8 * 1024;

/// P2.5: Inline 模式文件的内存缓冲。open/create 时填充, write 追加并标记 dirty,
/// read 切片返回, release 时若 dirty 则同步到 Filer (inline_data).
///
/// `dirty` 标记用于避免只读 open → release 时把 (可能已过时的) 数据回写 Filer,
/// 防止覆盖其他客户端的并发写入 (Inline 模式无 volume lease 互斥).
/// create 的新文件初始 dirty=false; 首次 write 后 dirty=true.
#[derive(Debug, Default)]
pub struct InlineBuffer {
    pub data: Vec<u8>,
    pub dirty: bool,
    /// Length of data when last synced from Filer (open/create time).
    /// Used to compute the appended delta for atomic append on release.
    /// When the buffer grows beyond original_len and no in-place modification
    /// occurred, release sends only `data[original_len..]` with is_append=true,
    /// allowing the Filer to atomically append without losing other clients' data.
    pub original_len: usize,
    /// Set to true if any write modified data at offset < original_len
    /// (in-place overwrite, not pure append). When true, release falls back
    /// to full-buffer overwrite mode (is_append=false) to preserve the
    /// in-place modifications.
    pub modified_in_place: bool,
    /// Set to true by InvalidateHandler when an invalidation was skipped
    /// because the buffer was dirty. This signals that another client
    /// modified the file on the Filer while we held unsynced local data.
    /// After the buffer is synced (dirty → false), the next open() checks
    /// this flag and forces a Filer refresh to pick up the other client's
    /// changes. Without this, the stale-buffer check (entry.size vs buf_len)
    /// would pass because entry.size was never updated during the skip,
    /// causing cross-client stale reads (L4.21: A sees 175/200 lines).
    pub needs_refresh: bool,
}

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
    /// 所有 Filer 节点地址列表（用于网络错误时轮换重试）
    filer_addrs: Vec<String>,
    filer_net_port: u16,
    /// Lease mode: "range" (方案 D) or "inode" (方案 A)
    lease_mode: String,
    lease_duration_ms: u64,
    lease_renew_interval_ms: u64,
    /// 强制挂载：跳过拓扑健康检查。仅用于运维场景。
    force_mount: bool,
    /// 请求超时 (秒)
    request_timeout_secs: u64,
    /// Admin/debug HTTP server port (0 = disabled).
    admin_port: u16,
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
        filer_addrs: Vec<String>,
        filer_net_port: u16,
        lease_mode: &str,
        lease_duration_ms: u64,
        lease_renew_interval_ms: u64,
        force_mount: bool,
        request_timeout_secs: u64,
        admin_port: u16,
        runtime: Arc<tokio::runtime::Runtime>,
    ) -> Result<Self> {
        // filer_addrs 为空且 filer_addr 也为空时，由 facade 从 master 拓扑发现。
        // 旧逻辑 `vec![filer_addr]` 在 filer_addr="" 时会插入一个空字符串，污染轮换列表。
        let filer_addrs = if filer_addrs.is_empty() {
            if filer_addr.is_empty() {
                Vec::new()
            } else {
                vec![filer_addr.clone()]
            }
        } else {
            filer_addrs
        };
        Ok(FuseApp {
            mount_point: mount_point.to_string(),
            master_addresses: master_addrs.to_vec(),
            collection: collection.to_string(),
            replication: replication.to_string(),
            master_net_port,
            volume_net_port,
            volume_addrs,
            filer_addr,
            filer_addrs,
            filer_net_port,
            lease_mode: lease_mode.to_string(),
            lease_duration_ms,
            lease_renew_interval_ms,
            force_mount,
            request_timeout_secs,
            admin_port,
            runtime,
        })
    }

    pub async fn run(&self) -> Result<()> {
        info!(
            "Starting FUSE session on {} with masters {}",
            self.mount_point,
            self.master_addresses.join(", ")
        );

        // Extract host from each master address (strip protocol prefix and port).
        // All masters share the same powerfs-net port (`master_net_port`).
        if self.master_addresses.is_empty() {
            return Err(PowerFsError::Internal(
                "master_addresses is empty (must be configured)".to_string(),
            ));
        }
        let master_addrs: Vec<String> = self
            .master_addresses
            .iter()
            .map(|full| {
                let without_proto = full
                    .strip_prefix("http://")
                    .or_else(|| full.strip_prefix("https://"))
                    .unwrap_or(full);
                without_proto
                    .split(':')
                    .next()
                    .unwrap_or(without_proto)
                    .to_string()
            })
            .collect();

        let client_identity = powerfs_fuse_core::ClientIdentity::stable_for(&self.mount_point);
        info!(
            "Phase 2: FUSE client_identity client_id={} uuid={} (hostname+mount_point={})",
            client_identity.client_id, client_identity.client_uuid, self.mount_point
        );
        let facade_config = powerfs_fuse_core::FuseClientFacadeConfig {
            master_addrs: master_addrs.clone(),
            master_port: self.master_net_port,
            volume_net_port: self.volume_net_port,
            volume_addrs: self.volume_addrs.clone(),
            filer_addr: self.filer_addr.clone(),
            filer_addrs: self.filer_addrs.clone(),
            filer_port: self.filer_net_port,
            request_timeout: Duration::from_secs(self.request_timeout_secs),
            client_identity,
            mount_point: self.mount_point.clone(),
            collection: self.collection.clone(),
            replication: self.replication.clone(),
            lease_mode: self.lease_mode.clone(),
            lease_duration_ms: self.lease_duration_ms,
            lease_renew_interval_ms: self.lease_renew_interval_ms,
            force_mount: self.force_mount,
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

        // Admin/debug HTTP server is started AFTER `lock_manager` is
        // constructed below (it needs the `FuseLockManager` Arc to
        // expose `/lock-metrics`). The previous early-start location
        // is preserved as a comment marker for reviewers familiar
        // with the original layout.

        let cache = Arc::new(MetadataCache::new());
        // Create the chunk (data) cache up front so it can be shared with the
        // InvalidateHandler: a metadata Invalidate means the file's size/chunks
        // changed, so the client must drop cached file data as well to avoid
        // serving stale reads after another client modifies the file.
        let chunk_cache = Arc::new(ChunkCache::with_defaults());

        // Shared inline buffer map. Created early so it can be shared with
        // the InvalidateHandler, which needs to clear stale inline buffers
        // when another client modifies the file (L4.21 fix).
        let inline_buffers: Arc<DashMap<u64, InlineBuffer>> = Arc::new(DashMap::new());

        // Shared FUSE device file descriptor. Set to -1 until the FUSE session
        // is mounted. The InvalidateHandler uses this fd to send
        // FUSE_NOTIFY_INVAL_INODE notifications to the kernel, which is
        // required for cross-client cache consistency: without it, the kernel
        // continues serving stale page cache to readers after another client
        // has modified the file.
        let fuse_fd = Arc::new(std::sync::atomic::AtomicI32::new(-1));

        // Shared open_inodes tracker. Created early so it can be shared with
        // the InvalidateHandler, which checks it as a secondary guard to
        // prevent evicting inodes that are open but momentarily unpinned
        // (race window between release's unpin and the next open's pin).
        let open_inodes: Arc<RwLock<HashMap<u64, usize>>> = Arc::new(RwLock::new(HashMap::new()));

        // Phase 2: Wire up InvalidateHandler so the FUSE client receives
        // server-pushed Invalidate notifications from the Filer and evicts
        // stale metadata cache entries when another client modifies the
        // same directory.
        //
        // The handler is installed on the shared ClientConnPool so that every
        // Filer connection (current and future, including post-reconnect)
        // receives Invalidate frames. Volume connections in the pool will also
        // carry the handler, but Volume servers never push Invalidate frames,
        // so the handler is simply never invoked for those connections.
        //
        // P2: The handler is constructed with `new_with_fuse_fd_and_open_inodes`
        // so it can send FUSE_NOTIFY_INVAL_INODE messages to the kernel AND
        // check the open_inodes tracker to prevent evicting open inodes.
        // The actual fd value is set via `set_fuse_fd()` after the FUSE
        // session is mounted.
        let invalidate_handler = Arc::new(
            crate::invalidate_handler::InvalidateHandler::new_with_fuse_fd_and_open_inodes(
                cache.clone(),
                chunk_cache.clone(),
                inline_buffers.clone(),
                fuse_fd.clone(),
                open_inodes.clone(),
            ),
        );
        sync_client
            .facade()
            .conn_pool()
            .set_notification_handler(invalidate_handler.clone());
        // Also store on MetaShardClient for API compatibility (callers that
        // query the handler via meta_shard_client()).
        sync_client
            .facade()
            .meta_shard_client()
            .set_notification_handler(invalidate_handler.clone());

        let lease_manager = Arc::new(VolumeLeaseManager::new(
            sync_client.facade().clone(),
            sync_client.client_id(),
        ));

        // Conservative adapter (§4.1): wire up a `FuseLockManager`
        // exposing the unified `powerfs_lock::LockManager` trait,
        // backed by `FacadeLockBackend` (which delegates to the same
        // `FuseClientFacade` + `MetadataCache`). This does NOT replace
        // `VolumeLeaseManager` — existing read/write/release paths keep
        // using it directly. The new `lock_manager` is the entry point
        // for new code paths that prefer the unified trait (and for
        // the kernel C client's wire protocol when it lands in 阶段四).
        //
        // Phase-4 §5.1 Lockify: pass the sync_client's runtime handle
        // so `acquire_local` can spawn the async ownership-sync task
        // even when called from a sync FUSE callback (`block_on` runs
        // inside this runtime, so `tokio::spawn` would also work, but
        // the explicit handle is safer for non-tokio threads).
        let lock_backend = Arc::new(crate::lock_backend::FacadeLockBackend::new(
            sync_client.facade().clone(),
            cache.clone(),
        ));
        let lock_manager = Arc::new(
            powerfs_lock_fuse::FuseLockManager::new(
                lock_backend,
                sync_client.client_id(),
                30_000, // matches `lease_duration_ms`
            )
            .with_lockify(Some(sync_client.runtime().handle().clone())),
        );
        // Wire the lease state into the InvalidateHandler so that server-
        // pushed Invalidate notifications also clear directory leases.
        // Without this, has_valid_dir_lease() would return true for stale
        // leases after another client modifies the directory.
        invalidate_handler.set_lease_state(lock_manager.state().clone());
        // Phase 3 Lease Recall: wire the async lease releaser so the
        // InvalidateHandler can send ReleaseInodeLease RPCs when the
        // server pushes an Invalidate (recall or content change). This
        // ensures the server-side refcount is decremented promptly,
        // allowing MetaCache trim_pass to evict the entry.
        let releaser = Arc::new(crate::lock_backend::FacadeLeaseReleaser::new(
            sync_client.facade().clone(),
            sync_client.client_id(),
            sync_client.runtime().handle().clone(),
        ));
        invalidate_handler.set_lease_releaser(releaser);

        // §13 Cap model: cap state is now embedded in `CachedEntry::cap`.
        // `MetadataCache` exposes `grant_cap` / `take_cap` / `with_cap_mut`
        // / `mark_cap_dirty` / `mark_cap_flushed` / `can_cache_*`.
        //
        // The cap handler (flush + ACK side-effect) is set later, after
        // `PowerFsFs` is constructed, because it needs `PowerFsFs` as the
        // `CapFlusher`.

        // NOTE: FUSE client does NOT expose any listening endpoints.
        // Request statistics are reported to the Master via the periodic
        // KeepConnected heartbeat (TLV protocol); operators query them
        // through `powerfs-cli fuse-stats` which routes exclusively via
        // the Master gRPC interface.
        //
        // The legacy admin_port config field is accepted but IGNORED
        // (the HTTP /stats, /lock-metrics and /health endpoints are
        // never bound) to enforce the design rule that "clients must
        // not expose services".
        if self.admin_port > 0 {
            info!(
                "Legacy admin_port={} ignored — FUSE client exposes no \
                 listening endpoints. Stats are collected via the Master \
                 (use `powerfs-cli fuse-stats` against the master address).",
                self.admin_port
            );
        }

        let fs = PowerFsFs {
            client: sync_client.clone(),
            cache: cache.clone(),
            chunk_cache,
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
            lock_manager,
            open_inodes: open_inodes.clone(),
            open_file_leases: Arc::new(crate::open_file_lease::OpenFileLeaseRegistry::new()),
            inline_buffers: inline_buffers.clone(),
            inline_max_sizes: Arc::new(DashMap::new()),
            last_cache_epoch: std::sync::atomic::AtomicU64::new(0),
            fuse_fd: fuse_fd.clone(),
            readdir_cursors: Arc::new(DashMap::new()),
            pending_unlinks: Arc::new(std::sync::Mutex::new(Vec::new())),
            cap_waiters: Arc::new(crate::client_cap::CapWaiters::new()),
        };

        let fs_arc = Arc::new(fs);

        // §13 Cap model: now that `PowerFsFs` exists, create the cap
        // handler with `fs_arc` as the `CapFlusher` and wire it into
        // the InvalidateHandler. This must happen after `fs_arc` is
        // constructed because `FacadeCapHandler` needs `PowerFsFs` for
        // the flush path (drain_dirty_for_inode + write_blob_batch +
        // sync_size_chunks_on_close).
        let cap_handler = Arc::new(crate::lock_backend::FacadeCapHandler::new(
            sync_client.facade().clone(),
            fs_arc.clone() as Arc<dyn crate::invalidate_handler::CapFlusher>,
            sync_client.client_id(),
            sync_client.runtime().handle().clone(),
        ));
        invalidate_handler.set_cap_handler(cap_handler);

        let bg_fs = fs_arc.clone();
        thread::spawn(move || loop {
            // P2-d: Adaptive flusher interval.
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

        // Batch unlink flusher: drains pending_unlinks every 5ms or when
        // batch reaches 16 entries, sends BatchUnlink RPC grouped by shard.
        let bg_fs_unlink = fs_arc.clone();
        let unlink_runtime = self.runtime.handle().clone();
        thread::spawn(move || loop {
            thread::sleep(Duration::from_millis(5));
            let entries: Vec<(u64, String, u64)> = {
                let mut guard = bg_fs_unlink.pending_unlinks.lock().unwrap();
                if guard.is_empty() {
                    continue;
                }
                std::mem::take(&mut *guard)
            };
            if entries.is_empty() {
                continue;
            }
            // Group by shard_id
            let mut groups: HashMap<u64, Vec<(u64, String)>> = HashMap::new();
            for (parent, name, shard) in entries {
                groups.entry(shard).or_default().push((parent, name));
            }
            let meta_client = bg_fs_unlink.client.facade().meta_shard_client().clone();
            for (shard_id, batch) in groups {
                let mc = meta_client.clone();
                let runtime = unlink_runtime.clone();
                runtime.spawn(async move {
                    match mc.batch_unlink(batch.clone(), shard_id).await {
                        Ok(statuses) => {
                            let failed: Vec<_> = statuses
                                .iter()
                                .filter(|&&s| s != powerfs_net::STATUS_OK as u32)
                                .collect();
                            if !failed.is_empty() {
                                warn!(
                                    "batch_unlink: {}/{} entries failed (shard={})",
                                    failed.len(),
                                    statuses.len(),
                                    shard_id
                                );
                            }
                            debug!(
                                "batch_unlink: {} entries processed (shard={})",
                                statuses.len(),
                                shard_id
                            );
                        }
                        Err(e) => {
                            warn!(
                                "batch_unlink RPC failed for {} entries (shard={}): {} — \
                                 filer GC will clean up orphaned inodes",
                                batch.len(),
                                shard_id,
                                e
                            );
                        }
                    }
                });
            }
        });

        let mut session =
            FuseSession::new(Path::new(&self.mount_point), "powerfs", "powerfs", false).map_err(
                |e| PowerFsError::Internal(format!("failed to create fuse session: {}", e)),
            )?;

        session
            .mount()
            .map_err(|e| PowerFsError::Internal(format!("failed to mount fuse: {}", e)))?;

        // Now that the FUSE session is mounted, extract the /dev/fuse file
        // descriptor and share it with the InvalidateHandler. This enables
        // FUSE_NOTIFY_INVAL_INODE notifications so the kernel drops stale page
        // cache when another client modifies a file — critical for cross-client
        // consistency.
        if let Some(file) = session.get_fuse_file() {
            let raw_fd = file.as_raw_fd();
            invalidate_handler.set_fuse_fd(raw_fd);
            info!(
                "FUSE device fd={} registered with InvalidateHandler for kernel cache invalidation",
                raw_fd
            );
        } else {
            warn!(
                "FUSE session mounted but no device file available; \
                 kernel cache invalidation notifications will be skipped"
            );
        }

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
    /// Conservative adapter (§4.1): unified `LockManager` trait entry
    /// point, backed by `FacadeLockBackend`. Not used by the existing
    /// read/write/release paths (they use `lease_manager` directly
    /// above — migrating them would split the cache and break the
    /// read→release invariant).
    ///
    /// Phase-4 §5.1 Lockify: the manager is built with `with_lockify`
    /// enabled, so `mkdir`/`create`/`mknod`/`symlink` paths call
    /// `lock_manager.acquire_local(inode, ...)` to speculatively
    /// populate the inode lease cache without an RPC. The async sync
    /// (off the critical path) CAS-replaces the local token with a
    /// server-issued token.
    lock_manager: Arc<powerfs_lock_fuse::FuseLockManager>,
    /// Phase 4.3/4.4: 当前已打开的 inode → open count。
    /// open() 时 count+1，release() 时 count-1（减到 0 时移除）。
    /// getattr() 对其中的 inode 使用长 TTL
    /// （size/chunks 在 open→release 期间权威，因数据 lease 排他）。
    /// 使用引用计数而非 HashSet：同一 inode 可被多个 fd 同时打开
    /// （如 dd 关闭后 release 异步执行，此时 fsx 已 open），
    /// HashSet 的 remove 会误删仍在使用的 inode。
    open_inodes: Arc<RwLock<HashMap<u64, usize>>>,
    /// Phase-4 §5.2 (P3): Open-file lease registry. When a file is
    /// opened in inode-lease mode, the inode lease is pre-acquired at
    /// `open()` time and bound here. `flush_dirty_chunks` passes the
    /// bound token to `write_blob_batch_with_lease`, bypassing
    /// `ensure_lease`'s cache lookup on every flush. `release()`
    /// invalidates the binding. See `open_file_lease.rs`.
    open_file_leases: Arc<crate::open_file_lease::OpenFileLeaseRegistry>,
    /// P2.5: Inline 模式文件的写入缓冲。key = inode, value = InlineBuffer.
    ///
    /// 生命周期: create(inline) → 初始化空 buffer; write → 追加并标 dirty;
    /// read → 服务; release → 若 dirty 则取出作为 inline_data 发 Filer, 然后移除。
    ///
    /// 仅 Inline 模式 (MetadataAttr.is_inline) 的 inode 会出现在此 map 中;
    /// Flat 模式文件不经此 buffer (走 chunk_cache + Volume Server).
    inline_buffers: Arc<DashMap<u64, InlineBuffer>>,
    /// P2.5: Inline 模式 inode 的阈值 (来自 CREATE 响应 max_size)。
    /// write 累计超过此阈值 (且未超 8KB 硬上限) 时, 理论上应迁移到 Flat;
    /// 当前 MVP 阶段未实现 MIGRATE_INLINE, 故允许 buffer 增长到 8KB 硬上限,
    /// 超过则返回 EFBIG。此字段保留供后续迁移逻辑使用。
    inline_max_sizes: Arc<DashMap<u64, u32>>,
    /// Last-seen cache epoch from MetaShardClient. When this differs from
    /// the current epoch, it means a Filer leader change occurred and the
    /// cache may have missed Invalidate notifications — call invalidate_all().
    last_cache_epoch: std::sync::atomic::AtomicU64,
    /// L4.21 fix: Shared FUSE device fd for sending kernel cache
    /// invalidation notifications from the release path. After the last
    /// handle of an inline file is closed, we send FUSE_NOTIFY_INVAL_INODE
    /// to drop the kernel page cache — otherwise, stale data from this
    /// client's own writes persists in the page cache, and subsequent
    /// reads (e.g., wc -l) return stale line counts even though the Filer
    /// has the correct data (including other clients' concurrent appends).
    fuse_fd: Arc<std::sync::atomic::AtomicI32>,
    /// readdir pagination cursors: per-directory record of the last entry
    /// name returned in the previous page, so the next readdir(offset=N) can
    /// resume from that name instead of restarting from the first entry.
    ///
    /// Without this the FUSE `offset` (a numeric cookie) was discarded by
    /// `encode_readdir_req` (which hardcoded `LastName=""`), so every readdir
    /// RPC returned the first page — `rm -rf` never enumerated entries beyond
    /// the first page and they survived the deletion (intermittent-delete bug).
    readdir_cursors: Arc<DashMap<u64, ReaddirCursor>>,
    /// Pending batch unlink entries: (parent_ino, name, shard_id).
    /// unlink callback adds entries here and returns immediately (optimistic
    /// delete from cache). A background flush task sends BatchUnlink RPCs
    /// every 5ms or when the batch reaches 16 entries.
    /// On crash, pending entries are lost — filer GC eventually cleans up
    /// orphaned inodes (acceptable in non-critical environments).
    pending_unlinks: Arc<std::sync::Mutex<Vec<(u64, String, u64)>>>,
    /// §13 Cap model: external waiters map for cap upgrades. Cap state
    /// itself lives in `CachedEntry::cap` (via `MetadataCache`), but
    /// waiters must outlive cache entries — kept here as a top-level
    /// structure.
    ///
    /// Currently unused (open never blocks on cap); reserved for future
    /// SHARED_WRITE → EXCLUSIVE upgrade waits in the write path.
    #[allow(dead_code)]
    cap_waiters: Arc<crate::client_cap::CapWaiters>,
}

/// Cursor for last_name-based readdir pagination.
///
/// HISTORICAL NOTE: A previous version stored `next_offset` here and only
/// reused `last_name` when the kernel passed offset==next_offset exactly.
/// That broke whenever the kernel bumped offset by more than 1 (which is
/// permitted after an `add_entry` returned buffer-full) or when FUSE
/// restarted with a different offset base. The current implementation
/// uses relaxed matching: any offset>0 paired with a valid cursor reuses
/// the `last_name` to resume pagination from the Filer, which is correct
/// because entries are ordered by name and `last_name` is always the
/// lexicographically-greatest name returned so far.
#[derive(Clone)]
struct ReaddirCursor {
    last_name: String,
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

/// Map a Filer RPC error to the correct errno.
///
/// The Filer returns status codes (0=OK, 1=ENOENT, 2=EEXIST, etc.) which
/// `send_coherence_msg` embeds in the error string as `"server status N"`.
/// This function parses the status code and maps it to the matching errno,
/// so that applications see correct POSIX errors instead of a blanket EIO.
///
/// For body-embedded error messages (e.g., "not empty" from rmdir), falls
/// back to pattern matching. Only truly unknown/network errors default to EIO.
fn filer_error_to_errno(e: &str) -> i32 {
    // Parse "server status N" pattern (from send_coherence_msg)
    if let Some(pos) = e.find("server status ") {
        let rest = &e[pos + "server status ".len()..];
        if let Ok(status) = rest.split_whitespace().next().unwrap_or("").parse::<u16>() {
            return status_to_errno(status);
        }
    }

    // Fall back to pattern matching for body-embedded error messages
    let lower = e.to_lowercase();
    if lower.contains("not empty") {
        libc::ENOTEMPTY
    } else if lower.contains("not found") || lower.contains("no such file") {
        libc::ENOENT
    } else if lower.contains("already exists") {
        libc::EEXIST
    } else if lower.contains("permission denied") || lower.contains("access denied") {
        libc::EACCES
    } else if lower.contains("not a directory") {
        libc::ENOTDIR
    } else if lower.contains("is a directory") {
        libc::EISDIR
    } else if lower.contains("no space") {
        libc::ENOSPC
    } else if lower.contains("invalid argument") || lower.contains("bad request") {
        libc::EINVAL
    } else {
        libc::EIO
    }
}

/// Map a Filer status code to the matching POSIX errno.
fn status_to_errno(status: u16) -> i32 {
    match status {
        powerfs_net::STATUS_ERR_NOT_FOUND => libc::ENOENT,
        powerfs_net::STATUS_ERR_ALREADY_EXISTS => libc::EEXIST,
        powerfs_net::STATUS_ERR_PERMISSION_DENIED => libc::EACCES,
        powerfs_net::STATUS_ERR_IO => libc::EIO,
        powerfs_net::STATUS_ERR_INVALID_ARG => libc::EINVAL,
        powerfs_net::STATUS_ERR_NOT_DIR => libc::ENOTDIR,
        powerfs_net::STATUS_ERR_IS_DIR => libc::EISDIR,
        powerfs_net::STATUS_ERR_NO_SPACE => libc::ENOSPC,
        powerfs_net::STATUS_ERR_BAD_FD => libc::EBADF,
        powerfs_net::STATUS_ERR_SERVER_ERROR => libc::EIO,
        powerfs_net::STATUS_ERR_BAD_REQUEST => libc::EINVAL,
        _ => libc::EIO,
    }
}

/// P3: Resolve (volume_id, needle_id) for a 1MB chunk at `file_offset` in
/// Stripe mode.
///
/// The Filer pre-allocates one needle per stripe unit at create time. Each
/// stripe unit covers `stripe_size` bytes and is stored on one volume. Within
/// a stripe unit, 1MB sub-chunks use consecutive needle IDs
/// (`base_needle + chunk_idx_within_unit`), same as Flat mode's
/// `file_key + chunk_idx`.
///
/// Algorithm:
/// 1. `stripe_unit_idx = file_offset / stripe_size` (which stripe unit)
/// 2. Look up `chunks[stripe_unit_idx]` for the base needle_id and volume_id
/// 3. `chunk_idx_within_unit = (file_offset % stripe_size) / chunk_size`
/// 4. `needle_id = base_needle + chunk_idx_within_unit`
///
/// Returns `None` if the stripe unit is beyond the pre-allocated range
/// (file larger than `stripe_count × stripe_size`). On-demand allocation
/// for larger files is a future extension.
fn resolve_stripe_chunk(
    placement: &powerfs_layout::Placement,
    chunks: &[CachedFileChunk],
    file_offset: u64,
    chunk_size: u64,
) -> Option<(u64, u64)> {
    let stripe_size = match placement {
        powerfs_layout::Placement::Stripe { stripe_size, .. }
        | powerfs_layout::Placement::WideStripe { stripe_size, .. } => *stripe_size,
        _ => return None,
    };
    let stripe_size = stripe_size.max(1);
    let stripe_unit_idx = (file_offset / stripe_size) as usize;
    if stripe_unit_idx >= chunks.len() {
        return None; // beyond pre-allocated range
    }
    let base = &chunks[stripe_unit_idx];
    let chunk_idx_within_unit = (file_offset % stripe_size) / chunk_size.max(1);
    let needle_id = base.needle_id.saturating_add(chunk_idx_within_unit);
    Some((base.volume_id, needle_id))
}

/// Step 2: 将 MetadataAttr（MetadataClient RPC 返回）转为 CachedEntry。
///
/// 强一致方案下，所有元数据操作走 Filer Raft leader，返回 MetadataAttr。
/// 此函数将其转换为 FUSE 缓存所需的 CachedEntry 结构。
/// parent/name 由调用方传入（MetadataAttr 不包含路径信息）。
fn attr_to_cached_entry(attr: &MetadataAttr, parent: u64, name: &str) -> CachedEntry {
    let is_dir = attr.file_type == libc::DT_DIR;
    let is_symlink = attr.file_type == libc::DT_LNK;
    // P3: Convert ChunkRef from MetadataAttr to CachedFileChunk.
    // For Stripe files, attr.chunks contains the pre-allocated stripe units.
    let chunks: Vec<CachedFileChunk> = attr
        .chunks
        .iter()
        .map(|c| CachedFileChunk {
            offset: c.offset,
            size: c.size,
            mtime: c.mtime,
            needle_id: c.needle_id,
            volume_id: c.volume_id,
            crc32: c.crc32,
        })
        .collect();
    // P3: For Stripe files (placement is Some), fid must be None so write/read
    // paths route to the Stripe branch. For Flat files, reconstruct fid from
    // volume_id/file_key if available.
    let fid = if attr.placement.is_some() {
        None
    } else if let (Some(vol), Some(key)) = (attr.volume_id, attr.file_key) {
        Some(Fid {
            volume_id: VolumeId(vol),
            cookie: 0,
            file_key: key,
        })
    } else if !chunks.is_empty() {
        // Fallback: reconstruct from chunks[0]
        Some(Fid {
            volume_id: VolumeId(chunks[0].volume_id),
            cookie: 0,
            file_key: chunks[0].needle_id,
        })
    } else {
        None
    };
    CachedEntry {
        inode: attr.inode,
        parent,
        name: name.to_string(),
        is_dir,
        is_symlink,
        symlink_target: attr.symlink_target.clone(),
        nlink: attr.nlink,
        fid,
        size: attr.size,
        mode: attr.mode,
        uid: attr.uid,
        gid: attr.gid,
        atime: attr.atime as i64,
        mtime: attr.mtime as i64,
        ctime: attr.ctime,
        xattrs: HashMap::new(),
        chunks,
        hard_link_id: String::new(),
        hard_link_counter: 0,
        content_size: attr.size,
        disk_size: 0,
        generation: 0,
        placement: attr.placement.clone(),
        reliability: attr.reliability.clone(),
        replica_chunks: attr
            .replica_chunks
            .iter()
            .map(|c| CachedFileChunk {
                offset: c.offset,
                size: c.size,
                mtime: c.mtime,
                needle_id: c.needle_id,
                volume_id: c.volume_id,
                crc32: c.crc32,
            })
            .collect(),
        shard_id: attr.shard_id,
        cached_at: Instant::now(),
        state: EntryState::default(),
        hold: HoldState::default(),
        cap: None,
        dentry_lease: None,
        dir_shared_gen: 0,
    }
}

impl PowerFsFs {
    /// L4.21 fix: Send FUSE_NOTIFY_INVAL_INODE to the kernel to invalidate
    /// the page cache for the given inode. This is called from the release
    /// path after the last handle of an inline file is closed, to ensure
    /// that stale page cache (from this client's own writes) doesn't
    /// prevent subsequent reads from seeing other clients' concurrent
    /// appends that were synced to the Filer.
    fn notify_kernel_inval_inode(&self, inode: u64) {
        let fd = self.fuse_fd.load(std::sync::atomic::Ordering::Acquire);
        if fd < 0 {
            return;
        }
        let mut buf = [0u8; 40];
        buf[0..4].copy_from_slice(&40u32.to_ne_bytes());
        buf[4..8].copy_from_slice(&2i32.to_ne_bytes());
        buf[8..16].copy_from_slice(&0u64.to_ne_bytes());
        buf[16..24].copy_from_slice(&inode.to_ne_bytes());
        buf[24..32].copy_from_slice(&0i64.to_ne_bytes());
        buf[32..40].copy_from_slice(&(-1i64).to_ne_bytes());
        let n = unsafe { libc::write(fd, buf.as_ptr() as *const libc::c_void, 40) };
        if n < 0 {
            let err = std::io::Error::last_os_error();
            warn!(
                "release: notify_kernel_inval_inode failed for inode={}: {} (errno={})",
                inode,
                err,
                err.raw_os_error().unwrap_or(0)
            );
        } else {
            debug!(
                "release: sent FUSE_NOTIFY_INVAL_INODE for inode={} (last handle closed, invalidating stale page cache)",
                inode
            );
        }
    }

    /// 方案 B (S5): 返回 inode 的路由 shard_id, 优先用缓存中的权威值。
    ///
    /// 缓存命中时直接用 Filer 返回的 `shard_id`（免 ShardMap::route 计算）;
    /// 缓存 miss 或 `shard_id=None` 时回退到 `calculate_shard_id(inode)`。
    ///
    /// 这是 S5 的核心：正常路径（缓存命中）零计算，直接用 Filer 权威值。
    /// S6: 所有 inode-level / parent-level 操作都通过此方法路由。
    fn routing_shard(&self, inode: u64) -> u64 {
        if let Some(entry) = self.cache.get_inode(inode) {
            if let Some(sid) = entry.shard_id {
                return sid;
            }
        }
        self.client
            .facade()
            .meta_shard_client()
            .calculate_shard_id(inode)
    }

    /// Phase-4 §5.1 Lockify fast path: speculatively populate the
    /// inode lease cache with a local token after a fresh inode is
    /// minted by the Filer (creat/mkdir/mknod/symlink). The async
    /// sync RPC (off the critical path) CAS-replaces the local
    /// token with a server-issued token. On conflict the local
    /// entry is invalidated; on network error it remains valid
    /// until TTL — graceful degradation in both cases.
    ///
    /// Errors are swallowed deliberately: Lockify is opportunistic.
    /// If it fails (e.g. the manager was built without `with_lockify`,
    /// or `duration_ms == 0`), the regular `acquire` path takes over
    /// on the first read/write — correctness is preserved.
    ///
    /// Called from `mkdir`/`create`/`mknod`/`symlink` right after
    /// the Filer RPC returns the new inode.
    fn lockify_declare_new_inode(&self, inode: u64) {
        if let Err(e) = self.lock_manager.acquire_local(
            inode,
            powerfs_lock_fuse::LockMode::Exclusive,
            self.lease_duration_ms,
        ) {
            debug!(
                "lockify self-declare skipped inode={}: {} (opportunistic, regular acquire will take over)",
                inode, e
            );
        }
    }

    /// Invalidate the directory listing cache for `parent_inode` after the
    /// client itself modifies the directory (create/mkdir/unlink/rmdir).
    ///
    /// This does NOT release the directory lease — the lease is kept because
    /// the modification was initiated by this client; subsequent lookups can
    /// still trust the lease for entries that are re-fetched. Other clients'
    /// modifications are detected via the lockify CAS-conflict path, which
    /// invalidates the lease automatically.
    fn invalidate_dir_entries(&self, parent_inode: u64) {
        self.cache.invalidate_dir(parent_inode);
    }

    /// §13 Cap model: acquire structured cap bits from the server for an
    /// inode that is about to be returned as an open file handle.
    ///
    /// Shared by `open()` and `create()` — both return an open file handle
    /// to the kernel, and subsequent read/write/setattr calls rely on the
    /// cap being present in `CachedEntry::cap`. Without this, `mark_dirty_cap_w`
    /// / `mark_dirty_cap_x` are no-ops (cap is None), and a server recall
    /// would immediate-ACK without flushing dirty data → data loss.
    ///
    /// Fast path: if we already have a valid cap with the wanted bits,
    /// skip the CapOpenGrant RPC.
    ///
    /// - `is_write_open`: true for O_WRONLY/O_RDWR (want EXCLUSIVE),
    ///   false for O_RDONLY (want CAP_R).
    /// - Best-effort: on failure, the legacy lease-only path remains active.
    fn acquire_cap_on_open(&self, inode: u64, is_write_open: bool) {
        let want = if is_write_open {
            crate::client_cap::CapSet::EXCLUSIVE
        } else {
            crate::client_cap::CapSet::CAP_R
        };
        let have_cap = self
            .cache
            .get_cap(inode)
            .map(|c| c.issued.contains(want))
            .unwrap_or(false);
        if have_cap {
            debug!(
                "acquire_cap_on_open: cap fast path inode={} — already have {:?}",
                inode, want
            );
            return;
        }
        let facade = self.client.facade().clone();
        let cid = self.client.client_id();
        match self
            .client
            .runtime()
            .block_on(facade.cap_open_grant(inode, &cid, is_write_open))
        {
            Ok((cap_token, caps_bits, epoch, sn, _duration_ms)) => {
                let caps = crate::client_cap::CapSet(caps_bits);
                let cap_id = sn;
                let cap = crate::client_cap::ClientCap::new(
                    cap_id,
                    cap_token,
                    caps,
                    epoch,
                    is_write_open,
                    sn,
                );
                self.cache.grant_cap(inode, cap);
                debug!(
                    "acquire_cap_on_open: cap_open_grant success inode={} caps={:#b} epoch={} sn={}",
                    inode, caps_bits, epoch, sn
                );
            }
            Err(e) => {
                debug!(
                    "acquire_cap_on_open: cap_open_grant failed for inode={} \
                     (best-effort, legacy lease path active): {}",
                    inode, e
                );
            }
        }
    }

    /// Check if the Filer leader has changed since the last call.
    /// If so, invalidate all cached metadata to handle potentially missed
    /// Invalidate notifications during the leader change window.
    /// Inspired by JuiceFS redisCache.onInvalidateConnect (Purge on reconnect).
    fn check_cache_epoch(&self) {
        let current = self.client.facade().meta_shard_client().cache_epoch();
        let last = self
            .last_cache_epoch
            .load(std::sync::atomic::Ordering::Relaxed);
        if current != last {
            log::warn!(
                "check_cache_epoch: epoch changed {} -> {}, invalidating all cached metadata + leases",
                last,
                current
            );
            self.cache.invalidate_all();
            // Clear all directory/file leases: the old leader's leases are
            // no longer valid, and the new leader has no record of them.
            // Without this, has_valid_dir_lease() would return true for
            // stale leases, causing lookup/create to bypass RPCs and read
            // stale dentry cache after a leader switch.
            self.lock_manager.state().clear_all();
            self.last_cache_epoch
                .store(current, std::sync::atomic::Ordering::Relaxed);
        }
    }

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

    /// Check if an inode has any remaining dirty chunks.
    fn has_dirty_for_inode(&self, inode: u64) -> bool {
        for shard in &self.dirty_shards {
            let set = shard.read().unwrap();
            if set.iter().any(|(ino, _)| *ino == inode) {
                return true;
            }
        }
        false
    }

    /// Flush dirty chunks for an inode. Acquires per-inode flush lock to
    /// serialize with release callback's lease release.
    fn flush_dirty_chunks(&self, inode: u64, lease_token: Option<&str>) -> std::io::Result<()> {
        let flush_lock = self.get_flush_lock(inode);
        let _guard = flush_lock.lock().unwrap_or_else(|e| e.into_inner());
        // Phase-4 §5.2 (P3): If the caller didn't supply a lease
        // token, try the open-file-lease registry (bound at open
        // time). Falls through to `None` → `ensure_lease` if no
        // lease is bound or it's expired — graceful degradation.
        let bound_token = if lease_token.is_some() {
            lease_token.map(|s| s.to_string())
        } else {
            self.open_file_leases.get_valid_token(inode)
        };
        self.flush_dirty_chunks_impl(inode, bound_token.as_deref())
    }

    /// Internal flush implementation — caller MUST hold the per-inode flush lock.
    fn flush_dirty_chunks_impl(
        &self,
        inode: u64,
        lease_token: Option<&str>,
    ) -> std::io::Result<()> {
        // RACE_TRACE: Log entry with full context to track the flusher's
        // interaction with the write path and InvalidateHandler.
        let is_pinned_before = self.cache.is_pinned(inode);
        let has_chunks_before = self.chunk_cache.has_chunks(inode);
        debug!(
            "flush_dirty_chunks_impl ENTER: inode={} is_pinned={} has_chunks={} thread={:?}",
            inode,
            is_pinned_before,
            has_chunks_before,
            std::thread::current().id()
        );

        let dirty = self.drain_dirty_for_inode(inode);

        if dirty.is_empty() {
            debug!(
                "flush_dirty_chunks_impl: inode={} no dirty chunks after drain, exiting",
                inode
            );
            return Ok(());
        }

        // EntryState: Dirty→Flushing before starting flush RPC. On failure the
        // entry stays Flushing (no mark_clean); subsequent retries re-enter
        // Flushing (same-state transition is allowed by try_transition).
        self.cache.mark_flushing(inode);

        debug!(
            "flush_dirty_chunks_impl: inode={}, dirty_count={} (drained, has_dirty_after={})",
            inode,
            dirty.len(),
            self.chunk_cache.has_dirty_chunks(inode)
        );

        // Phase 1.7: 查找 entry/fid/addr 失败时，重新标记 dirty 以便后续重试，
        // 避免 drain 后丢数据。write 合并依赖此重试机制保证持久性。
        let entry = match self.cache.get_inode(inode) {
            Some(e) => e,
            None => {
                // RACE_TRACE: This is the drain race window — dirty markers were
                // removed but the inode was evicted before we could use it.
                // Log whether it was pinned/chunks existed to determine which
                // path caused the eviction.
                let is_pinned_now = self.cache.is_pinned(inode);
                let has_chunks_now = self.chunk_cache.has_chunks(inode);
                warn!(
                    "flush_dirty_chunks_impl RACE: inode={} not in cache after drain! \
                     is_pinned_before={} is_pinned_now={} has_chunks_before={} has_chunks_now={} \
                     dirty_count={} thread={:?} \
                     — inode was evicted during drain window (check invalidate_inode logs)",
                    inode,
                    is_pinned_before,
                    is_pinned_now,
                    has_chunks_before,
                    has_chunks_now,
                    dirty.len(),
                    std::thread::current().id()
                );
                for (_, idx) in &dirty {
                    self.mark_dirty(inode, *idx);
                }
                return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
            }
        };

        let chunk_size = self.chunk_cache.chunk_size();

        // === P3: Stripe 模式 flush 分支 ===
        // entry.placement.is_some() && entry.fid.is_none() → Stripe/WideStripe.
        // 每个 dirty chunk 按 resolve_stripe_chunk() 路由到正确的 volume/needle.
        if let Some(placement) = entry.placement.as_ref().filter(|_| entry.fid.is_none()) {
            let stripe_chunks = entry.chunks.clone();

            // chunk_size == stripe_size (both 1MB by default), so each cache
            // entry maps to exactly one stripe unit / one needle_id.
            let chunk_size = self.chunk_cache.chunk_size();

            let batch_size = 32;
            let mut had_error = false;

            let chunks_to_flush: Vec<(u64, powerfs_fuse_core::WriteBlobRequest)> = dirty
                .iter()
                .filter_map(|(_, chunk_idx)| {
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
                    let (vol_id, needle_id) =
                        resolve_stripe_chunk(placement, &stripe_chunks, chunk_offset, chunk_size)?;
                    Some((
                        *chunk_idx,
                        powerfs_fuse_core::WriteBlobRequest {
                            volume_id: vol_id,
                            file_key: needle_id,
                            inode,
                            offset: chunk_offset as i64,
                            size: data_len as i32,
                            data: chunk_data.data,
                        },
                    ))
                })
                .collect();

            let mut flushed_indices: Vec<u64> = Vec::new();
            for batch in chunks_to_flush.chunks(batch_size) {
                let requests: Vec<_> = batch.iter().map(|(_, req)| req.clone()).collect();
                let results = self
                    .client
                    .write_blob_batch_with_lease(requests, lease_token);

                for ((chunk_idx, req), result) in batch.iter().zip(results.iter()) {
                    if let Err(e) = result {
                        self.mark_dirty(inode, *chunk_idx);
                        error!(
                            "write_blob stripe failed for inode {} chunk {}: {}",
                            inode, chunk_idx, e
                        );
                        had_error = true;
                    } else {
                        // Compute CRC32 of flushed data for read-path verification
                        let crc = crc32fast::hash(&req.data);
                        let chunk_offset = *chunk_idx * chunk_size;
                        self.cache.update_chunk_crc32(inode, chunk_offset, crc);
                        flushed_indices.push(*chunk_idx);
                    }
                }
            }

            if !flushed_indices.is_empty() {
                self.chunk_cache
                    .clear_dirty_for_chunks(inode, &flushed_indices);
            }

            // ISSUE-001 diagnostic: log post-flush dirty state for stripe path
            let has_dirty_shards = self.has_dirty_for_inode(inode);
            let has_dirty_cache = self.chunk_cache.has_dirty_chunks(inode);
            if flushed_indices.len() == dirty.len() && (has_dirty_shards || has_dirty_cache) {
                warn!(
                    "flush_dirty_chunks_impl(stripe): inode={} post-flush dirty NOT cleared! \
                     flushed={}/{} has_dirty_shards={} has_dirty_cache={}",
                    inode,
                    flushed_indices.len(),
                    dirty.len(),
                    has_dirty_shards,
                    has_dirty_cache
                );
            } else {
                debug!(
                    "flush_dirty_chunks_impl(stripe): inode={} post-flush OK flushed={}/{} \
                     has_dirty_shards={} has_dirty_cache={}",
                    inode,
                    flushed_indices.len(),
                    dirty.len(),
                    has_dirty_shards,
                    has_dirty_cache
                );
            }

            if had_error {
                // EntryState: Flushing→Dirty on failure (Phase 4). Chunks
                // were already re-marked dirty per-chunk above; transition
                // the entry state back so it's not stuck in Flushing.
                self.cache.mark_dirty(inode);
                return Err(std::io::Error::from_raw_os_error(libc::EIO));
            }
            // EntryState: Flushing→Clean after successful stripe flush RPC.
            self.cache.mark_clean(inode);
            return Ok(());
        }

        let fid = match entry.fid {
            Some(ref f) => f.clone(),
            None => {
                warn!(
                    "flush_dirty_chunks_impl: inode {} has no fid and no placement, re-marking {} dirty chunks",
                    inode,
                    dirty.len()
                );
                for (_, idx) in &dirty {
                    self.mark_dirty(inode, *idx);
                }
                // EntryState: Flushing→Dirty on failure (Phase 4).
                self.cache.mark_dirty(inode);
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
                // EntryState: Flushing→Dirty on failure (Phase 4).
                self.cache.mark_dirty(inode);
                return Err(std::io::Error::from_raw_os_error(libc::EIO));
            }
        };

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

            for ((chunk_idx, req), result) in batch.iter().zip(results.iter()) {
                if let Err(e) = result {
                    self.mark_dirty(inode, *chunk_idx);
                    error!(
                        "write_blob failed for inode {} chunk {}: {}",
                        inode, chunk_idx, e
                    );
                    had_error = true;
                } else {
                    // Compute CRC32 of flushed data for read-path verification
                    let crc = crc32fast::hash(&req.data);
                    let chunk_offset = *chunk_idx * chunk_size;
                    self.cache.update_chunk_crc32(inode, chunk_offset, crc);
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

        // ISSUE-001 diagnostic: log post-flush dirty state to verify clearing.
        // has_dirty_for_inode checks dirty_shards; has_dirty_chunks checks
        // chunk_cache internal dirty flag. Both should be false after a
        // successful flush with all chunks flushed.
        let has_dirty_shards = self.has_dirty_for_inode(inode);
        let has_dirty_cache = self.chunk_cache.has_dirty_chunks(inode);
        if flushed_indices.len() == dirty.len() && (has_dirty_shards || has_dirty_cache) {
            warn!(
                "flush_dirty_chunks_impl: inode={} post-flush dirty NOT cleared! \
                 flushed={}/{} has_dirty_shards={} has_dirty_cache={} — possible re-mark race",
                inode,
                flushed_indices.len(),
                dirty.len(),
                has_dirty_shards,
                has_dirty_cache
            );
        } else {
            debug!(
                "flush_dirty_chunks_impl: inode={} post-flush OK flushed={}/{} \
                 has_dirty_shards={} has_dirty_cache={}",
                inode,
                flushed_indices.len(),
                dirty.len(),
                has_dirty_shards,
                has_dirty_cache
            );
        }

        if had_error {
            // EntryState: Flushing→Dirty on failure (Phase 4). Chunks
            // were already re-marked dirty per-chunk above; transition
            // the entry state back so it's not stuck in Flushing.
            self.cache.mark_dirty(inode);
            return Err(std::io::Error::from_raw_os_error(libc::EIO));
        }

        // EntryState: Flushing→Clean after successful flat flush RPC.
        self.cache.mark_clean(inode);

        // Phase 3.4: size/chunks 元数据同步移至 release()（close 时强一致 sync），
        // flush_dirty_chunks 只负责将数据持久化到 volume server。
        Ok(())
    }

    fn flush_all_dirty_chunks(&self) -> std::io::Result<()> {
        let inodes = self.all_dirty_inodes();

        if inodes.is_empty() {
            return Ok(());
        }

        debug!(
            "flush_all_dirty_chunks: processing {} dirty inodes, thread={:?}",
            inodes.len(),
            std::thread::current().id()
        );

        for inode in inodes {
            if let Err(e) = self.flush_dirty_chunks(inode, None) {
                // Phase 1.7: 后台 flusher 错误需记录，避免静默丢数据。
                // flush 失败的 chunk 仍保留在 dirty_shards（drain 已消费则重新 mark），
                // 下次 flush 周期会重试。release/fsync 仍会同步 flush 作为最后保障。
                warn!(
                    "flush_all_dirty_chunks: flush inode {} failed (will retry next cycle): {}",
                    inode, e
                );
            } else if !self.has_dirty_for_inode(inode) {
                // All dirty chunks for this inode have been successfully flushed.
                // Sync metadata to the Filer so other clients see the latest
                // size/chunks. This also clears dirty markers regardless of
                // whether the inode is still open.
                //
                // CRITICAL: Only unpin if the inode is NOT still open. The
                // background flusher must not unpin inodes that were pinned by
                // open() — only release() should unpin those. Unpinning here
                // creates a window where the InvalidateHandler (triggered by
                // the sync_size_chunks_on_close above) can evict the inode
                // mid-write, causing ENOENT in the write path.
                //
                // The unpin path is reserved for the case where release()
                // failed to flush (and thus didn't unpin), leaving the inode
                // pinned with no open file handle. In that case is_open=false
                // and the flusher correctly cleans up.
                let is_open = self.open_inodes.read().unwrap().contains_key(&inode);

                if let Err(e) = self.sync_size_chunks_on_close(inode) {
                    warn!(
                        "flush_all_dirty_chunks: post-flush sync for inode {} failed: {}",
                        inode, e
                    );
                } else {
                    self.chunk_cache.clear_dirty(inode);
                    if is_open {
                        // Inode is still open: keep it pinned so the write path
                        // and InvalidateHandler continue to protect it. The
                        // dirty markers are cleared (data was flushed), but
                        // the pin stays until release() removes it.
                        debug!(
                            "flush_all_dirty_chunks: flushed inode={} still open, keeping pinned (release will unpin) thread={:?}",
                            inode, std::thread::current().id()
                        );
                    } else {
                        // Inode is not open: release() must have failed to
                        // flush and left it pinned. Now that the flusher has
                        // succeeded, sync metadata and unpin to prevent a
                        // permanent pin leak.
                        //
                        // CRITICAL: Re-check is_open under the write lock and
                        // hold it while unpining. The first is_open check
                        // (before sync_size_chunks_on_close RPC) has a large
                        // race window — an open can happen during the RPC.
                        // Without this re-check, the flusher would unpin an
                        // inode that was just opened, leaving it open but
                        // unpinned → InvalidateHandler evicts mid-write.
                        debug!(
                            "flush_all_dirty_chunks: flushed inode={} not open, syncing + unpinning thread={:?}",
                            inode, std::thread::current().id()
                        );
                        let open_inodes = self.open_inodes.write().unwrap();
                        if !open_inodes.contains_key(&inode) {
                            self.cache.unpin_inode(inode);
                        } else {
                            debug!(
                                "flush_all_dirty_chunks: inode={} was opened during flush, keeping pinned thread={:?}",
                                inode, std::thread::current().id()
                            );
                        }
                    }
                }
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

        // Guard: If the file is in Inline mode (no fid, no chunks) and the
        // inline buffer was already removed by a prior release, skip this sync.
        // The Flat path sends inline_data=None, which would CLEAR the Filer's
        // inline_data — corrupting Inline mode files that were already synced
        // by the inline release path. This happens when multiple overlapping
        // opens exist (FUSE kernel delays releases) and the first release
        // (inline path) removes the buffer before subsequent releases run.
        if entry.fid.is_none()
            && entry.chunks.is_empty()
            && !self.inline_buffers.contains_key(&inode)
        {
            debug!(
                "sync_size_chunks_on_close: inode {} is Inline mode (no fid, no chunks, no buffer) — skipping Flat sync to preserve Filer inline_data",
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
        info!(
            "K3-DBG sync_close: inode={} content_size={} state={:?} chunks={:?}",
            inode,
            entry.content_size,
            entry.state,
            entry
                .chunks
                .iter()
                .map(|c| (c.offset, c.volume_id, c.needle_id, c.size))
                .collect::<Vec<_>>()
        );

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

        // inode-level write → route by routing_shard(inode).
        // Inode records live on their own hash-derived shard (independent of
        // the parent dir entry's shard); routing via `parent` would hit the
        // wrong leader and force a redirect on every close.
        let routing_shard = self.routing_shard(inode);
        let req = powerfs_coherence::UpdateInodeSizeChunksRequest {
            shard_id: routing_shard,
            inode,
            size: entry.content_size,
            chunks: chunks_wire,
            client_id: self.client.client_id(),
            // Flat 路径: 无 inline_data (Inline 模式在 release 中提前返回, 不走此函数)
            inline_data: None,
            is_append: false,
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
        // Determine st_mode: preserve file type bits for special files
        // (FIFO/block/char/socket) created via mknod. The Filer stores the
        // full mode (including S_IFMT bits), so if the mode already has a
        // file type, use it as-is. Otherwise apply type from is_dir/is_symlink
        // flags, defaulting to S_IFREG.
        const S_IFMT: u32 = 0o170000;
        attr.st_mode = if entry.is_symlink {
            ((entry.mode & !S_IFMT) | 0o120000) as libc::mode_t
        } else if entry.is_dir {
            ((entry.mode & !S_IFMT) | 0o040000) as libc::mode_t
        } else if (entry.mode & S_IFMT) != 0 {
            // Special file (FIFO/BLK/CHR/SOCK): mode already carries type bits
            entry.mode as libc::mode_t
        } else {
            // Regular file: mode has no type bits, add S_IFREG
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
        // Dentry lease three-layer check (same as lookup):
        // If the dentry lease is valid, or shared_gen matches + dir complete,
        // a cache miss means the file truly doesn't exist (negative dentry).
        match self.cache.check_dentry_lease(parent, name) {
            DentryLeaseStatus::LeaseValid
            | DentryLeaseStatus::SharedGenValid
            | DentryLeaseStatus::NegativeComplete => {
                debug!(
                    "entry_exists: dentry lease/shgen valid, cache MISS = not exist (parent={}, name={})",
                    parent, name
                );
                return false;
            }
            DentryLeaseStatus::Expired | DentryLeaseStatus::Miss => {
                // Fall through to RPC
            }
        }
        // 无 lease：查 Filer（shard_id calculated from parent_ino）
        let meta_client = self.client.facade().meta_shard_client().clone();
        let shard_id = self.routing_shard(parent);
        let name_owned = name.to_string();
        let t_lookup = std::time::Instant::now();
        let result = self
            .client
            .block_on(async move { meta_client.lookup(parent, &name_owned, shard_id).await })
            .is_ok();
        let lookup_ms = t_lookup.elapsed().as_millis();
        if lookup_ms > 10 {
            info!(
                "entry_exists LOOKUP SLOW: parent={}, name={}, lookup={}ms, result={}",
                parent, name, lookup_ms, result
            );
        }
        result
    }

    /// Lookup "." — return the directory's own attributes.
    ///
    /// The kernel calls `lookup(parent, ".")` when revalidating an expired
    /// dentry for the current directory. Since "." is never stored as a
    /// dir_entry in the Filer, we resolve it locally:
    /// 1. Try the metadata cache (fast path)
    /// 2. On cache miss, fetch via `get_entry_by_inode(parent)` (Filer RPC)
    fn lookup_dot(&self, parent: u64) -> std::io::Result<Entry> {
        debug!("lookup_dot: parent={}", parent);

        // Fast path: cache hit
        if let Some(entry) = self.cache.get_inode(parent) {
            debug!(
                "lookup_dot: cache HIT parent={}, inode={}",
                parent, entry.inode
            );
            return Ok(self.create_fuse_entry(&entry));
        }

        // Cache miss: fetch from Filer via get_entry_by_inode
        debug!(
            "lookup_dot: cache MISS, fetching from filer parent={}",
            parent
        );
        match self.client.get_entry_by_inode(parent) {
            Ok(Some((filer_entry, path))) => {
                // Resolve grandparent from the returned path. The Entry proto
                // has no parent_ino field, so we derive it from the path.
                let grandparent = self.resolve_parent_from_path(parent, &path);
                let cached = self.entry_to_cached(grandparent, &filer_entry);
                self.cache.insert(cached.clone());
                debug!(
                    "lookup_dot: fetched parent={} from filer, inode={}, grandparent={}",
                    parent, cached.inode, grandparent
                );
                Ok(self.create_fuse_entry(&cached))
            }
            Ok(None) => {
                debug!("lookup_dot: parent={} not found in filer", parent);
                Err(std::io::Error::from_raw_os_error(libc::ENOENT))
            }
            Err(e) => {
                warn!(
                    "lookup_dot: failed to query filer for parent={}: {}",
                    parent, e
                );
                Err(std::io::Error::from_raw_os_error(libc::EIO))
            }
        }
    }

    /// Lookup ".." — return the parent directory's attributes.
    ///
    /// The kernel calls `lookup(parent, "..")` when revalidating an expired
    /// dentry for the parent directory. Since ".." is never stored as a
    /// dir_entry in the Filer, we resolve it locally:
    /// 1. Get parent's `parent` inode from cache (or Filer RPC on miss)
    /// 2. Return the grandparent's attributes
    /// 3. Root's ".." returns root itself (POSIX convention)
    fn lookup_dotdot(&self, parent: u64) -> std::io::Result<Entry> {
        debug!("lookup_dotdot: parent={}", parent);

        // Root's ".." is root itself (POSIX convention)
        if parent == ROOT_INODE {
            debug!("lookup_dotdot: parent is ROOT, returning root");
            return self.lookup_dot(ROOT_INODE);
        }

        // Step 1: resolve grandparent inode
        let grandparent = match self.cache.get_inode(parent) {
            Some(entry) => entry.parent,
            None => {
                // Cache miss: fetch parent's metadata from Filer to get its
                // parent_inode. The Entry proto has no parent_ino field, so
                // we resolve the grandparent from the returned path string.
                debug!(
                    "lookup_dotdot: cache MISS for parent={}, fetching from filer",
                    parent
                );
                match self.client.get_entry_by_inode(parent) {
                    Ok(Some((filer_entry, path))) => {
                        let gp = self.resolve_parent_from_path(parent, &path);
                        // Cache the parent entry with the correct grandparent
                        let cached = self.entry_to_cached(gp, &filer_entry);
                        self.cache.insert(cached.clone());
                        debug!(
                            "lookup_dotdot: fetched parent={} from filer, grandparent={}",
                            parent, gp
                        );
                        gp
                    }
                    Ok(None) => {
                        warn!("lookup_dotdot: parent={} not found in filer", parent);
                        return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
                    }
                    Err(e) => {
                        warn!(
                            "lookup_dotdot: failed to query filer for parent={}: {}",
                            parent, e
                        );
                        return Err(std::io::Error::from_raw_os_error(libc::EIO));
                    }
                }
            }
        };

        // Root's parent is root
        if grandparent == ROOT_INODE || grandparent == 0 {
            return self.lookup_dot(ROOT_INODE);
        }

        // Step 2: fetch grandparent's attributes (cache or Filer)
        if let Some(entry) = self.cache.get_inode(grandparent) {
            debug!(
                "lookup_dotdot: cache HIT grandparent={}, inode={}",
                grandparent, entry.inode
            );
            return Ok(self.create_fuse_entry(&entry));
        }

        debug!(
            "lookup_dotdot: cache MISS for grandparent={}, fetching from filer",
            grandparent
        );
        match self.client.get_entry_by_inode(grandparent) {
            Ok(Some((filer_entry, path))) => {
                let great_gp = self.resolve_parent_from_path(grandparent, &path);
                let cached = self.entry_to_cached(great_gp, &filer_entry);
                self.cache.insert(cached.clone());
                debug!(
                    "lookup_dotdot: fetched grandparent={} from filer, inode={}",
                    grandparent, cached.inode
                );
                Ok(self.create_fuse_entry(&cached))
            }
            Ok(None) => {
                warn!(
                    "lookup_dotdot: grandparent={} not found in filer",
                    grandparent
                );
                Err(std::io::Error::from_raw_os_error(libc::ENOENT))
            }
            Err(e) => {
                warn!(
                    "lookup_dotdot: failed to query filer for grandparent={}: {}",
                    grandparent, e
                );
                Err(std::io::Error::from_raw_os_error(libc::EIO))
            }
        }
    }

    /// Resolve the parent inode of `child_inode` from a Filer-returned path.
    ///
    /// `get_entry_by_inode` returns `(Entry, path)` where `path` is the full
    /// POSIX path of the entry (e.g. "/a/b/c"). The Entry proto has no
    /// `parent_ino` field, so we derive the parent inode by:
    /// 1. Stripping the last path component → parent path
    /// 2. Resolving the parent path to an inode via `resolve_path_inode`
    /// 3. Falling back to the cached parent, then ROOT_INODE
    fn resolve_parent_from_path(&self, child_inode: u64, path: &str) -> u64 {
        if path.is_empty() || path == "/" {
            return self
                .cache
                .get_inode(child_inode)
                .map(|e| e.parent)
                .unwrap_or(ROOT_INODE);
        }
        let parent_path = match path.rfind('/') {
            Some(0) => "/".to_string(),
            Some(pos) => path[..pos].to_string(),
            None => "/".to_string(),
        };
        self.resolve_path_inode(&parent_path).unwrap_or_else(|| {
            self.cache
                .get_inode(child_inode)
                .map(|e| e.parent)
                .unwrap_or(ROOT_INODE)
        })
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

        // P4: 映射 replica_chunks (读路径 failover 使用)
        let replica_chunks: Vec<CachedFileChunk> = entry
            .replica_chunks
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
            placement: None,
            reliability: powerfs_layout::reliability::Reliability::default(),
            replica_chunks,
            shard_id: None,
            cached_at: Instant::now(),
            state: EntryState::default(),
            hold: HoldState::default(),
            cap: None,
            dentry_lease: None,
            dir_shared_gen: 0,
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

    /// Sync dirty inline buffer data to the Filer.
    ///
    /// Implements the mark-snapshot → clone → sync → check-dirty-again loop
    /// that handles concurrent writes during sync (including write-after-release
    /// caused by the kernel's WRITEBACK_CACHE mode, where FUSE_WRITE can arrive
    /// AFTER the FUSE_RELEASE callback has already run and cleared dirty).
    ///
    /// Returns `Ok(true)` if data was actually synced, `Ok(false)` if nothing to
    /// sync (not dirty / buffer gone / no delta). Caller decides when to remove
    /// the inline_buffers entry.
    pub(crate) fn sync_inline_buffer(&self, inode: u64, log_prefix: &str) -> Result<bool> {
        // inode-level write → route by routing_shard(inode). Inline data + size
        // are stored on the inode's own shard, NOT the parent dir's shard.
        let routing_shard = self.routing_shard(inode);

        // Loop: snapshot dirty → clone → sync → check-dirty-again
        // If a concurrent write marks the buffer dirty during the sync RPC,
        // we re-sync the updated buffer. Max 3 iterations to avoid infinite
        // loops under sustained write pressure.
        let sync_ok = true;

        for sync_round in 0..3u32 {
            let snapshot: Option<(u64, Option<Vec<u8>>, usize, bool)> = {
                if let Some(inline_buf) = self.inline_buffers.get(&inode) {
                    let size = inline_buf.data.len() as u64;
                    let was_dirty = inline_buf.dirty;
                    let orig_len = inline_buf.original_len;
                    let mod_in_place = inline_buf.modified_in_place;
                    let data = if was_dirty {
                        Some(inline_buf.data.clone())
                    } else {
                        None
                    };
                    Some((size, data, orig_len, mod_in_place))
                } else {
                    None
                }
            };

            let Some((size, data, orig_len, mod_in_place)) = snapshot else {
                return Ok(false); // Buffer gone (migrated / removed), nothing synced
            };

            // Not dirty (read-only or already-synced path): skip sync entirely.
            let Some(data) = data else {
                debug!(
                    "{} inode={} not dirty (round {}), skip sync",
                    log_prefix, inode, sync_round
                );
                return Ok(false);
            };

            // Safety net: if the buffer didn't grow and no in-place modification
            // occurred, there's nothing new to sync. Syncing size=0 would wipe
            // other clients' concurrent-append data.
            let can_append = !mod_in_place && (data.len() > orig_len);
            if !can_append && !mod_in_place && data.len() == orig_len {
                debug!(
                    "{} inode={} no new data to sync (data_len={} == orig_len={}, \
                     mod_in_place={}), skip to avoid overwriting other clients' data",
                    log_prefix,
                    inode,
                    data.len(),
                    orig_len,
                    mod_in_place
                );
                if let Some(mut inline_buf) = self.inline_buffers.get_mut(&inode) {
                    inline_buf.dirty = false;
                }
                return Ok(false);
            }

            let (sync_data, sync_size, is_append) = if can_append {
                let delta = data[orig_len..].to_vec();
                debug!(
                    "{} inode={} append mode, orig_len={}, delta_len={}, total_len={} round {}",
                    log_prefix,
                    inode,
                    orig_len,
                    delta.len(),
                    data.len(),
                    sync_round
                );
                (Some(delta), 0u64, true)
            } else {
                warn!(
                    "{} inode={} OVERWRITE mode (is_append=false), mod_in_place={}, \
                     data_len={}, orig_len={} round {} — may overwrite other clients' data",
                    log_prefix,
                    inode,
                    mod_in_place,
                    data.len(),
                    orig_len,
                    sync_round
                );
                (Some(data), size, false)
            };

            let req = powerfs_coherence::UpdateInodeSizeChunksRequest {
                shard_id: routing_shard,
                inode,
                size: sync_size,
                chunks: Vec::new(),
                client_id: self.client.client_id(),
                inline_data: sync_data,
                is_append,
            };
            let max_retries = 5u32;
            let mut last_err = String::new();
            let mut round_ok = false;
            for attempt in 1..=max_retries {
                let meta_client = self.client.facade().meta_shard_client().clone();
                let req = req.clone();
                let result = self
                    .client
                    .block_on(async move { meta_client.update_inode_size_chunks(&req).await });
                match result {
                    Ok(resp) if resp.success => {
                        info!(
                            "{} inode={} synced size={} (round {} attempt {})",
                            log_prefix, inode, size, sync_round, attempt
                        );
                        round_ok = true;
                        break;
                    }
                    Ok(resp) => {
                        last_err = resp.error;
                        warn!(
                            "{} inode={} round {} attempt {} failed: {}",
                            log_prefix, inode, sync_round, attempt, last_err
                        );
                    }
                    Err(e) => {
                        last_err = e;
                        warn!(
                            "{} inode={} round {} attempt {} error: {}",
                            log_prefix, inode, sync_round, attempt, last_err
                        );
                    }
                }
                if attempt < max_retries {
                    std::thread::sleep(std::time::Duration::from_millis(500 * (attempt as u64)));
                }
            }

            if !round_ok {
                error!(
                    "{} inode={} FAILED after {} attempts: {} — data may be lost",
                    log_prefix, inode, max_retries, last_err
                );
                if let Some(mut inline_buf) = self.inline_buffers.get_mut(&inode) {
                    inline_buf.dirty = true;
                }
                return Err(powerfs_common::PowerFsError::Internal(last_err));
            }

            // After append-mode sync, update original_len to the synced snapshot
            // size so a concurrent write's re-sync sends only the NEW delta
            // (not full buffer → OVERWRITE mode → cross-client data loss).
            if is_append {
                if let Some(mut inline_buf) = self.inline_buffers.get_mut(&inode) {
                    inline_buf.original_len = size as usize;
                }
            }

            // Check if buffer grew during sync (concurrent write). If so, re-sync.
            let current_len = self
                .inline_buffers
                .get(&inode)
                .map(|b| b.data.len())
                .unwrap_or(0);

            if current_len as u64 > size {
                warn!(
                    "{} inode={} buffer grew during sync (synced={}, current={}), re-syncing (round {})",
                    log_prefix, inode, size, current_len, sync_round
                );
                continue;
            }

            // Buffer unchanged — mark as not dirty (caller decides remove).
            let grew_again = {
                if let Some(mut inline_buf) = self.inline_buffers.get_mut(&inode) {
                    let grew = inline_buf.data.len() as u64 > size;
                    if !grew {
                        inline_buf.dirty = false;
                    }
                    grew
                } else {
                    false
                }
            };
            if grew_again {
                warn!(
                    "{} inode={} buffer grew between check and dirty-clear, re-syncing (round {})",
                    log_prefix, inode, sync_round
                );
                continue;
            }

            break;
        }

        Ok(sync_ok)
    }
}

/// Parse POSIX ACL access xattr data to extract the file mode bits.
///
/// `cp -p` uses `fsetxattr("system.posix_acl_access", ...)` instead of
/// `chmod` to preserve file permissions. The FUSE user-space library
/// must parse the ACL and update the file mode itself.
///
/// `cur_mode` provides fallback bits for entries missing from the ACL
/// (e.g. cp -p omits ACL_OTHER when it matches the current other bits).
///
/// Format: 4-byte version (2=ACL_EA_VERSION), then 8-byte entries:
///   tag(2 LE): 1=USER_OBJ 2=USER 4=GROUP_OBJ 8=GROUP 0x20=MASK 0x10=OTHER
///   perm(2 LE): rwx bits
///   id(4 LE): user/group id (unused for mode)
fn parse_posix_acl_mode(acl_data: &[u8], cur_mode: u32) -> Option<u32> {
    if acl_data.len() < 4 {
        return None;
    }
    let version = u32::from_le_bytes([acl_data[0], acl_data[1], acl_data[2], acl_data[3]]);
    if version != 2 {
        return None;
    }
    let mut user_obj_perm: Option<u32> = None;
    let mut group_obj_perm: Option<u32> = None;
    let mut mask_perm: Option<u32> = None;
    let mut other_perm: Option<u32> = None;
    let mut offset = 4;
    while offset + 8 <= acl_data.len() {
        let tag = u16::from_le_bytes([acl_data[offset], acl_data[offset + 1]]);
        let perm = u16::from_le_bytes([acl_data[offset + 2], acl_data[offset + 3]]) as u32;
        match tag {
            1 => user_obj_perm = Some(perm),
            4 => group_obj_perm = Some(perm),
            0x20 => mask_perm = Some(perm),
            0x10 => other_perm = Some(perm),
            _ => {}
        }
        offset += 8;
    }
    let owner = user_obj_perm.unwrap_or((cur_mode >> 6) & 0o7);
    // cp -p writes an incomplete ACL (USER_OBJ, GROUP_OBJ, MASK, no OTHER)
    // where MASK stores the *other* permission bits, not the group mask.
    // Verified empirically on tmpfs: a 640 file gets ACL with
    // USER_OBJ=6, GROUP_OBJ=4, MASK=0 -> resulting mode is 640 (not 600).
    // Standard ACLs (with OTHER entry) follow POSIX semantics:
    // mode.group = MASK, mode.other = OTHER.
    let (group, other) = if let Some(oth) = other_perm {
        // Complete ACL: standard POSIX semantics
        let grp = mask_perm
            .or(group_obj_perm)
            .unwrap_or((cur_mode >> 3) & 0o7);
        (grp, oth)
    } else {
        // Incomplete ACL (cp -p): MASK = other bits, group = GROUP_OBJ
        let grp = group_obj_perm.unwrap_or((cur_mode >> 3) & 0o7);
        let oth = mask_perm.unwrap_or(cur_mode & 0o7);
        (grp, oth)
    };
    let mode = (owner << 6) | (group << 3) | other;
    log::debug!(
        "parse_posix_acl_mode: cur_mode={:o}, user_obj={:?}, group_obj={:?}, mask={:?}, other_entry={:?} -> owner={}, group={}, other={}, mode={:o}",
        cur_mode, user_obj_perm, group_obj_perm, mask_perm, other_perm, owner, group, other, mode
    );
    Some(mode)
}

/// §13 Cap model: `CapFlusher` implementation for `PowerFsFs`.
///
/// Called by `FacadeCapHandler::flush_and_ack` when a `CapRecallNotify`
/// arrives for a cap with dirty CAP_W. This uses the **same** flush path
/// as `release()`:
/// 1. `flush_dirty_chunks(inode, Some(token))` — drains dirty chunks,
///    writes them to the Volume Server via `write_blob_batch_with_lease`.
/// 2. `sync_size_chunks_on_close(inode)` — syncs size + chunks to the
///    Filer via Raft (`UpdateInodeSizeChunks`).
///
/// Both steps acquire the per-inode `flush_lock` to serialize with
/// `release()` and the background flusher, preventing TOCTOU races
/// where the lease token is removed while a flush is in flight.
impl crate::invalidate_handler::CapFlusher for PowerFsFs {
    fn flush_and_sync(&self, inode: u64, lease_token: &str) -> std::io::Result<()> {
        debug!(
            "PowerFsFs::cap_flush_and_sync: inode={} token={} — flushing dirty data",
            inode, lease_token
        );

        // Inline files: data lives in inline_buffers, not chunk_cache.
        // flush_dirty_chunks + sync_size_chunks_on_close only handle Flat
        // mode (chunk-based). For inline files, use sync_inline_buffer
        // which sends the buffer as inline_data via a single Raft commit.
        // Without this, a recall on a dirty inline file would ACK without
        // flushing, losing the dirty data.
        if self.inline_buffers.contains_key(&inode) {
            debug!(
                "PowerFsFs::cap_flush_and_sync: inode={} is inline, using sync_inline_buffer",
                inode
            );
            self.cache.mark_flushing(inode);
            let result = self.sync_inline_buffer(inode, "cap recall flush:");
            match result {
                Ok(_) => {
                    self.cache.mark_clean(inode);
                    self.cache.mark_cap_flushed(inode);
                    debug!(
                        "PowerFsFs::cap_flush_and_sync: inode={} — inline sync succeeded",
                        inode
                    );
                    Ok(())
                }
                Err(e) => {
                    self.cache.mark_dirty(inode);
                    warn!(
                        "PowerFsFs::cap_flush_and_sync: inline sync FAILED for inode={} err={:?} \
                         — dirty data retained for retry",
                        inode, e
                    );
                    Err(std::io::Error::other(format!("inline sync failed: {}", e)))
                }
            }
        } else {
            // Flat/chunk mode: flush dirty chunks to Volume Server, then
            // sync metadata to Filer via Raft.

            // Step 1: Flush dirty chunks to Volume Server. Pass the cap token
            // so write RPCs carry the correct fencing epoch.
            let flush_result = self.flush_dirty_chunks(inode, Some(lease_token));
            if let Err(ref e) = flush_result {
                warn!(
                    "PowerFsFs::cap_flush_and_sync: flush_dirty_chunks failed for inode={} err={:?} \
                     — dirty data retained for retry",
                    inode, e
                );
                return flush_result;
            }

            // Step 2: Sync metadata (size + chunks) to Filer via Raft.
            // This ensures the Filer has the authoritative size before the
            // recall completes and another client opens the file.
            let sync_result = self.sync_size_chunks_on_close(inode);
            if let Err(ref e) = sync_result {
                warn!(
                    "PowerFsFs::cap_flush_and_sync: sync_size_chunks_on_close failed for inode={} err={:?} \
                     — chunks are persisted but metadata sync pending",
                    inode, e
                );
                return sync_result;
            }

            // Step 3: Mark the cap as flushed in the cache entry.
            // This clears `flushing_caps` so subsequent operations know the
            // data is safely persisted.
            self.cache.mark_cap_flushed(inode);

            debug!(
                "PowerFsFs::cap_flush_and_sync: inode={} — flush + sync succeeded",
                inode
            );
            Ok(())
        }
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
        // AUTO_INVAL_DATA: ask the kernel to refresh directory mtime before
        // readdir, so that the readdir cache is properly invalidated after
        // a rename/mkdir/unlink. Without this flag, the kernel uses its
        // cached mtime (which is stale after a rename) and serves stale
        // directory entries from the readdir cache for up to 1s.
        if capable.contains(fuse_backend_rs::api::filesystem::FsOptions::AUTO_INVAL_DATA) {
            opts |= fuse_backend_rs::api::filesystem::FsOptions::AUTO_INVAL_DATA;
        }
        Ok(opts)
    }

    fn lookup(&self, _ctx: &Context, parent: Self::Inode, name: &CStr) -> std::io::Result<Entry> {
        // Check for Filer leader change before any cache access.
        // If the leader changed, invalidate_all() is called to handle
        // potentially missed Invalidate notifications.
        self.check_cache_epoch();

        let name_str = name.to_str().unwrap_or("");
        debug!("lookup: parent={}, name={}", parent, name_str);

        // Intercept "." and ".." — these are never stored as dir_entries
        // in the Filer. The kernel calls lookup for them when revalidating
        // expired dentries (entry_timeout=100ms). Without this interception,
        // "." and ".." are forwarded to the Filer, which returns ENOENT
        // (no dir_entry named "." or ".." exists).
        match name_str {
            "." => return self.lookup_dot(parent),
            ".." => return self.lookup_dotdot(parent),
            _ => {}
        }

        // 1. MetadataCache 命中（含完整 attr）— 快速路径
        if let Some(entry) = self.lookup_in_cache(parent, name_str) {
            debug!(
                "lookup: cache HIT parent={}, name={}, inode={}",
                parent, name_str, entry.inode
            );
            return Ok(self.create_fuse_entry(&entry));
        }

        // 1b. Dentry lease three-layer check (aligned with Ceph):
        //
        // Layer 1: per-dentry lease valid → trust cache (positive or negative)
        // Layer 2: shared_gen matches + dir complete → trust cache
        // Layer 3: RPC to Filer
        //
        // This replaces the old directory-level local lease, which was broken
        // because it was self-declared (acquire_local) — the Filer never knew
        // the client held it, so it couldn't push Invalidate notifications
        // when another client modified the directory. The new mechanism uses
        // Filer-issued per-dentry leases and dir_version (shared_gen) tracking.
        match self.cache.check_dentry_lease(parent, name_str) {
            DentryLeaseStatus::LeaseValid => {
                // Layer 1: dentry lease is valid. If the entry exists in
                // cache, return it; otherwise it's a negative dentry.
                if let Some(entry) = self.cache.get_inode_by_name(parent, name_str) {
                    debug!(
                        "lookup: dentry lease valid, cache HIT (parent={}, name={})",
                        parent, name_str
                    );
                    return Ok(self.create_fuse_entry(&entry));
                }
                debug!(
                    "lookup: dentry lease valid, negative (parent={}, name={})",
                    parent, name_str
                );
                return Ok(Entry {
                    inode: 0,
                    generation: 0,
                    attr: unsafe { std::mem::zeroed() },
                    attr_flags: 0,
                    attr_timeout: Duration::ZERO,
                    entry_timeout: Duration::ZERO,
                });
            }
            DentryLeaseStatus::SharedGenValid => {
                // Layer 2: lease expired but shared_gen matches + dir complete.
                if let Some(entry) = self.cache.get_inode_by_name(parent, name_str) {
                    debug!(
                        "lookup: shared_gen valid, cache HIT (parent={}, name={})",
                        parent, name_str
                    );
                    return Ok(self.create_fuse_entry(&entry));
                }
                // Negative dentry (dir complete + shared_gen match → ENOENT)
                debug!(
                    "lookup: shared_gen valid, negative (parent={}, name={})",
                    parent, name_str
                );
                return Ok(Entry {
                    inode: 0,
                    generation: 0,
                    attr: unsafe { std::mem::zeroed() },
                    attr_flags: 0,
                    attr_timeout: Duration::ZERO,
                    entry_timeout: Duration::ZERO,
                });
            }
            DentryLeaseStatus::NegativeComplete => {
                // No cached entry, but dir is complete → ENOENT
                debug!(
                    "lookup: dir complete, negative (parent={}, name={})",
                    parent, name_str
                );
                return Ok(Entry {
                    inode: 0,
                    generation: 0,
                    attr: unsafe { std::mem::zeroed() },
                    attr_flags: 0,
                    attr_timeout: Duration::ZERO,
                    entry_timeout: Duration::ZERO,
                });
            }
            DentryLeaseStatus::Expired | DentryLeaseStatus::Miss => {
                // Fall through to RPC
                debug!(
                    "lookup: dentry lease expired/miss, querying filer (parent={}, name={})",
                    parent, name_str
                );
            }
        }

        // 2. Step 2: Filer RPC（强一致 Leader Lease Read, shard_id calculated from parent_ino）
        let meta_client = self.client.facade().meta_shard_client().clone();
        let shard_id = self.routing_shard(parent);
        let name_owned = name_str.to_string();
        debug!(
            "lookup: cache MISS, querying filer shard={} parent={}",
            shard_id, parent
        );
        match self
            .client
            .block_on(async move { meta_client.lookup(parent, &name_owned, shard_id).await })
        {
            Ok(attr) => {
                debug!(
                    "lookup: filer returned inode={} for parent={}, name={}, dir_version={}, lease_ttl={}ms",
                    attr.inode, parent, name_str, attr.dir_version, attr.dentry_lease_ttl_ms
                );
                // Update dir_version from Filer response (shared_gen tracking).
                if attr.dir_version > 0 {
                    self.cache.update_dir_version(parent, attr.dir_version);
                }
                let entry = attr_to_cached_entry(&attr, parent, name_str);
                self.cache.insert(entry.clone());
                // Grant dentry lease if the Filer provided a TTL.
                if attr.dentry_lease_ttl_ms > 0 {
                    self.cache.grant_dentry_lease(
                        parent,
                        name_str,
                        attr.dentry_lease_ttl_ms,
                        0, // issuer: filer node id (TODO: from response)
                    );
                }
                Ok(self.create_fuse_entry(&entry))
            }
            Err(e) => {
                debug!("lookup RPC failed for '{}/{}': {}", parent, name_str, e);
                // Even on ENOENT, if the Filer returned dir_version + lease TTL,
                // we can cache the negative dentry. The MetadataAttr error path
                // doesn't carry these fields, so we rely on the Filer's
                // STATUS_ERR_NOT_FOUND response body. For now, return a short
                // negative entry — the dentry lease for negatives will be
                // granted on future lookups when the Filer response carries
                // DirVersion + DentryLeaseTtl in the NOT_FOUND body.
                Ok(Entry {
                    inode: 0,
                    generation: 0,
                    attr: unsafe { std::mem::zeroed() },
                    attr_flags: 0,
                    attr_timeout: Duration::ZERO,
                    entry_timeout: Duration::ZERO,
                })
            }
        }
    }

    fn getattr(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        _handle: Option<Self::Handle>,
    ) -> std::io::Result<(libc::stat64, Duration)> {
        // Check for Filer leader change (cheap: one AtomicU64 load + compare).
        self.check_cache_epoch();
        debug!("getattr: inode={}", inode);

        // Phase 4.3: 已打开文件的 size/chunks 在 open→release 期间权威
        // （数据 lease 排他，其他客户端无法修改），使用长 TTL 避免频繁 filer 查询。
        let is_open = self.open_inodes.read().unwrap().contains_key(&inode);
        let ttl = if is_open { TTL_OPEN } else { TTL };

        // For open files (pinned, lease-held), the userspace cache is
        // authoritative — no other client can modify the data while we
        // hold the lease. Return the cached entry directly.
        // Use peek_inode (not get_inode) to bypass EntryState checks:
        // the InvalidateHandler may mark the entry Stale between a write
        // (which updates local size) and the next getattr. For open files,
        // the local cache is always authoritative regardless of state.
        if is_open {
            if let Some(entry) = self.cache.peek_inode(inode) {
                debug!(
                    "getattr: cache hit for inode={}, is_open=true (lease-held), state={:?}",
                    inode, entry.state
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

        // For non-open files with a Clean cache entry (not Stale), the local
        // cache is authoritative IF the file was just written by this client.
        // After release→mark_clean, the cache has the correct size/chunks from
        // the write path. Going to the Filer would hit the async_meta_persist
        // visibility gap (propose_ff not yet applied → size=0 returned).
        //
        // Only fetch from Filer when the entry is Stale (invalidated by
        // another client's write) or missing (first access / new mount).
        //
        // The Invalidate mechanism ensures cross-client consistency: when
        // another client modifies the file, it sends an Invalidate that marks
        // our entry Stale → next getattr fetches fresh data.
        if let Some(entry) = self.cache.peek_inode(inode) {
            use crate::cache::EntryState;
            if entry.state == EntryState::Clean {
                debug!(
                    "getattr: cache hit for non-open file inode={} (Clean, local authoritative)",
                    inode
                );
                return Ok((self.create_stat(&entry), ttl));
            }
        }

        // Entry is Stale or missing — fetch fresh metadata from the Filer.
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

        // P2.5: Inline 模式 truncate 安全检查. 必须在 Filer setattr RPC 之前,
        // 否则 Filer 已接受 size 变更但客户端返回 EFBIG → 元数据/数据不一致
        // (Filer size=大值, 但 inline buffer 无法扩展到 >8KB).
        // P2.5c: write 路径已实现自动迁移 (MIGRATE_INLINE_ALLOC), 但 truncate
        // 到 >8KB 是罕见场景 (通常文件通过 write 增长, 非 truncate), 暂保留
        // EFBIG 拒绝. 后续可扩展为 truncate 迁移 (resize buffer + migrate).
        if let Some(new_size) = size {
            if self.inline_buffers.contains_key(&inode) && new_size as usize > INLINE_HARD_LIMIT {
                warn!(
                    "setattr inline: inode={} truncate to {} > INLINE_HARD_LIMIT={}, rejecting before RPC",
                    inode, new_size, INLINE_HARD_LIMIT
                );
                return Err(std::io::Error::from_raw_os_error(libc::EFBIG));
            }
        }

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
        let shard_id = self.routing_shard(inode);
        self.client
            .block_on(async move { meta_client.setattr(inode, &params, shard_id).await })
            .map_err(|e| {
                let errno = filer_error_to_errno(&e.to_string());
                if errno == libc::EIO {
                    error!("setattr RPC failed for inode {}: {}", inode, e);
                } else {
                    debug!(
                        "setattr RPC failed for inode {}: {} -> errno={}",
                        inode, e, errno
                    );
                }
                std::io::Error::from_raw_os_error(errno)
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

        // EntryState: 标记 Dirty 以反映本地属性已修改（仅在 size/mode/uid/gid 实际变化时）
        if mode.is_some() || size.is_some() || uid.is_some() || gid.is_some() {
            // §13 Cap model: mark CAP_X dirty so process_recall flushes
            // metadata before ACKing a server recall.
            self.cache.mark_dirty_cap_x(inode);
        }

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
            // P2.5: Inline 模式 truncate — 调整 inline buffer 大小并标记 dirty.
            // (8KB 硬上限已在 RPC 前的早期检查中拒绝, 此处 new_size 必然 <= 8KB)
            if self.inline_buffers.contains_key(&inode) {
                if let Some(mut inline_buf) = self.inline_buffers.get_mut(&inode) {
                    inline_buf.data.resize(new_size as usize, 0);
                    inline_buf.dirty = true;
                }
                debug!(
                    "setattr inline: inode={} truncated buffer to {}",
                    inode, new_size
                );
                // 更新 content_size 与 size 一致, 跳过 chunk_cache 逻辑 (Inline 无 chunks)
                self.cache.update_size(inode, new_size);
            } else {
                // Flat 模式 truncate: 截断 ChunkCache 中超出 new_size 的数据.
                // - 保留 new_size 范围内的脏 chunks (避免未 flush 的写入丢失)
                // - 移除/截断超出 new_size 的 chunks (避免 truncate-down + truncate-up 后读到旧数据)
                // - truncate 到 0 时清除 chunks 列表
                if new_size == 0 {
                    self.chunk_cache.remove_inode_chunks(inode);
                    self.cache.update_chunks(inode, Vec::new());
                } else {
                    self.chunk_cache.truncate_chunks(inode, new_size);
                    // Also truncate the chunks metadata list. Without this,
                    // the read path uses stale chunk entries to fetch data
                    // from the Volume Server, returning pre-truncate data
                    // for regions that should be holes after truncate-up.
                    self.cache.truncate_chunks_metadata(inode, new_size);
                }
                self.cache.update_size(inode, new_size);
                debug!(
                    "setattr: truncated inode={} to size={}, truncated chunk cache + metadata",
                    inode, new_size
                );
            }
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
        let shard_id = self.routing_shard(parent);
        let name_owned = name_str.to_string();
        let attr = self
            .client
            .block_on(async move {
                meta_client
                    .mkdir(parent, &name_owned, dir_mode, uid, gid, shard_id)
                    .await
            })
            .map_err(|e| {
                let errno = filer_error_to_errno(&e.to_string());
                if errno == libc::EIO {
                    error!("mkdir RPC failed: {}", e);
                } else {
                    debug!("mkdir RPC failed: {} -> errno={}", e, errno);
                }
                std::io::Error::from_raw_os_error(errno)
            })?;

        let entry = attr_to_cached_entry(&attr, parent, name_str);
        self.cache.insert(entry.clone());
        debug!(
            "mkdir: RPC done, inode={}, dir={}, mode={:o}, nlink={}, size={}, uid={}, gid={}, mtime={}, atime={}, ctime={}",
            attr.inode, parent, attr.mode, attr.nlink, attr.size, attr.uid, attr.gid, attr.mtime, attr.atime, attr.ctime
        );

        // 修改了父目录内容（新增子目录），invalidate 父目录的 dentry 缓存。
        // 保持目录 lease（修改是自己发起，下次 readdir 重新拉取即可）。
        self.invalidate_dir_entries(parent);

        // Phase-4 §5.1 Lockify: speculatively self-declare inode
        // ownership to avoid a synchronous lease-acquire RPC on the
        // first write into the new directory. Async-synced to filer.
        self.lockify_declare_new_inode(attr.inode);

        Ok(self.create_fuse_entry(&entry))
    }

    fn mknod(
        &self,
        ctx: &Context,
        parent: Self::Inode,
        name: &CStr,
        mode: u32,
        _rdev: u32,
        _umask: u32,
    ) -> std::io::Result<Entry> {
        let name_str = name.to_str().unwrap_or("");
        debug!(
            "mknod: parent={}, name={}, mode={:o}",
            parent, name_str, mode
        );

        if self.entry_exists(parent, name_str) {
            return Err(std::io::Error::from_raw_os_error(libc::EEXIST));
        }

        // The mode from VFS already includes the file type bits
        // (S_IFBLK=0o060000, S_IFCHR=0o020000, S_IFIFO=0o010000, S_IFSOCK=0o140000).
        // Pass it directly to the Filer's create endpoint, which stores the
        // mode via setattr. The Filer skips volume/needle allocation for
        // non-regular files (see handle_create is_special_file check).
        let uid = ctx.uid;
        let gid = ctx.gid;
        let meta_client = self.client.facade().meta_shard_client().clone();
        let shard_id = self.routing_shard(parent);
        let name_owned = name_str.to_string();
        let attr = self
            .client
            .block_on(async move {
                meta_client
                    .create(parent, &name_owned, mode, uid, gid, shard_id, None)
                    .await
            })
            .map_err(|e| {
                let errno = filer_error_to_errno(&e.to_string());
                if errno == libc::EIO {
                    error!("mknod RPC failed: {}", e);
                } else {
                    debug!("mknod RPC failed: {} -> errno={}", e, errno);
                }
                std::io::Error::from_raw_os_error(errno)
            })?;

        let entry = attr_to_cached_entry(&attr, parent, name_str);
        self.cache.insert(entry.clone());
        debug!("mknod: RPC done, inode={}, parent={}", attr.inode, parent);

        // 修改了父目录内容（新增特殊文件），invalidate 父目录的 dentry 缓存。
        self.invalidate_dir_entries(parent);

        // Phase-4 §5.1 Lockify: speculatively self-declare inode
        // ownership for the new special file. Async-synced to filer.
        self.lockify_declare_new_inode(attr.inode);

        Ok(self.create_fuse_entry(&entry))
    }

    fn rmdir(&self, _ctx: &Context, parent: Self::Inode, name: &CStr) -> std::io::Result<()> {
        let name_str = name.to_str().unwrap_or("");
        debug!("rmdir: parent={}, name={}", parent, name_str);

        // Step 2: 通过 MetadataClient.rmdir RPC 走 Filer Raft leader（强一致）
        // Filer 的 handle_rmdir 会做空目录检查（ENOTEMPTY），客户端不需要重复检查。
        let meta_client = self.client.facade().meta_shard_client().clone();
        let shard_id = self.routing_shard(parent);
        let name_owned = name_str.to_string();
        self.client
            .block_on(async move { meta_client.rmdir(parent, &name_owned, shard_id).await })
            .map_err(|e| {
                let errno = filer_error_to_errno(&e.to_string());
                if errno == libc::EIO {
                    error!("rmdir RPC failed: {}", e);
                } else {
                    debug!("rmdir RPC failed: {} -> errno={}", e, errno);
                }
                std::io::Error::from_raw_os_error(errno)
            })?;

        // Remove from cache
        if let Some(entry) = self.lookup_in_cache(parent, name_str) {
            self.cache.remove(entry.inode);
        }
        // 修改了父目录内容（删除子目录），invalidate 父目录的 dentry 缓存。
        self.invalidate_dir_entries(parent);
        Ok(())
    }

    fn unlink(&self, _ctx: &Context, parent: Self::Inode, name: &CStr) -> std::io::Result<()> {
        let name_str = name.to_str().unwrap_or("");
        debug!("unlink: parent={}, name={}", parent, name_str);

        // Try cache first; if miss, use dentry lease check.
        // Only fall back to lookup RPC when dentry lease is expired/miss.
        let entry = if let Some(e) = self.lookup_in_cache(parent, name_str) {
            e
        } else {
            match self.cache.check_dentry_lease(parent, name_str) {
                DentryLeaseStatus::LeaseValid
                | DentryLeaseStatus::SharedGenValid
                | DentryLeaseStatus::NegativeComplete => {
                    // Dentry lease valid: cache is authoritative, MISS = not exist
                    debug!(
                        "unlink: dentry lease valid, cache MISS for '{}/{}' → ENOENT",
                        parent, name_str
                    );
                    return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
                }
                DentryLeaseStatus::Expired | DentryLeaseStatus::Miss => {
                    // Fall through to RPC
                    debug!(
                        "unlink: cache miss for '{}/{}', fetching from filer",
                        parent, name_str
                    );
                    let meta_client = self.client.facade().meta_shard_client().clone();
                    let shard_id = self.routing_shard(parent);
                    let name_owned = name_str.to_string();
                    let attr = self
                        .client
                        .block_on(
                            async move { meta_client.lookup(parent, &name_owned, shard_id).await },
                        )
                        .map_err(|e| {
                            debug!(
                                "unlink: lookup RPC failed for '{}/{}': {}",
                                parent, name_str, e
                            );
                            std::io::Error::from_raw_os_error(libc::ENOENT)
                        })?;
                    let entry = attr_to_cached_entry(&attr, parent, name_str);
                    self.cache.insert(entry.clone());
                    entry
                }
            }
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

        // Optimistic cache update: remove from cache immediately so
        // subsequent lookups see the file as gone. The filer-side delete
        // is batched and will eventually catch up.
        if should_delete {
            // Last hard link - delete the actual data and remove all cache entries
            // §13 Cap model: release the cap before removing the cache entry.
            // take_cap removes the cap from CachedEntry and returns it so we
            // can send CapRelease RPC. Without this, the server keeps the
            // CapHolder until TTL expiry (30s), blocking other clients from
            // getting exclusive caps on a new file that reuses this inode.
            self.cache.mark_cap_flushed(entry.inode);
            if let Some(cap) = self.cache.take_cap(entry.inode) {
                let facade = self.client.facade().clone();
                let client_id = self.client.client_id();
                let cap_token = cap.token.clone();
                let runtime = self.client.runtime().handle().clone();
                let cap_inode = entry.inode;
                runtime.spawn(async move {
                    if let Err(e) = facade.cap_release(cap_inode, &client_id, &cap_token).await {
                        debug!(
                            "unlink: cap_release for inode {} failed (best-effort): {}",
                            cap_inode, e
                        );
                    }
                });
                debug!(
                    "unlink: cap released for inode={} (last link deleted, caps were {:?})",
                    entry.inode, cap.issued
                );
            }
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
            self.inline_buffers.remove(&entry.inode);
            self.inline_max_sizes.remove(&entry.inode);
        } else {
            // Not the last hard link - just remove the path mapping
            if let Some(path) = entry_path {
                self.cache.remove_path(entry.inode, &path);
            }
        }

        // Batch the filer-side unlink: add to pending queue and return
        // immediately. The background flusher sends BatchUnlink RPCs every
        // 5ms, grouping entries by shard. This eliminates the block_on(unlink
        // RPC) from the FUSE callback critical path — unlink becomes a pure
        // cache operation, no runtime worker consumed.
        //
        // Crash safety: if the client crashes before the batch is flushed,
        // the filer still has the entry. The kernel has already removed the
        // dentry from its cache, so the file appears deleted to this client.
        // Other clients see it until filer GC cleans it up (acceptable in
        // non-critical environments per user preference).
        let shard_id = self.routing_shard(parent);
        {
            let mut guard = self.pending_unlinks.lock().unwrap();
            guard.push((parent, name_str.to_string(), shard_id));
            // Flush immediately if batch is full (16 entries)
            if guard.len() >= 16 {
                let entries: Vec<_> = std::mem::take(&mut *guard);
                drop(guard);
                // Group by shard and spawn async send
                let mut groups: HashMap<u64, Vec<(u64, String)>> = HashMap::new();
                for (p, n, s) in entries {
                    groups.entry(s).or_default().push((p, n));
                }
                let meta_client = self.client.facade().meta_shard_client().clone();
                let runtime = self.client.runtime().handle().clone();
                for (sid, batch) in groups {
                    let mc = meta_client.clone();
                    runtime.spawn(async move {
                        match mc.batch_unlink(batch.clone(), sid).await {
                            Ok(statuses) => {
                                let failed: Vec<_> =
                                    statuses.iter().filter(|&&s| s != powerfs_net::STATUS_OK as u32).collect();
                                if !failed.is_empty() {
                                    warn!(
                                        "batch_unlink (inline): {}/{} failed (shard={})",
                                        failed.len(),
                                        statuses.len(),
                                        sid
                                    );
                                }
                            }
                            Err(e) => {
                                warn!(
                                    "batch_unlink (inline) RPC failed (shard={}): {} — GC will cleanup",
                                    sid, e
                                );
                            }
                        }
                    });
                }
            }
        }

        // 修改了父目录内容（删除文件），invalidate 父目录的 dentry 缓存。
        self.invalidate_dir_entries(parent);

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

        // Dentry lease: entry_exists uses the three-layer check
        // (dentry lease → shared_gen → RPC). No need to acquire a
        // directory-level local lease here — the Filer auto-subscribes
        // on lookup/readdir and pushes Invalidate notifications.

        let t_entry = std::time::Instant::now();
        let exists = self.entry_exists(parent, name_str);
        let entry_ms = t_entry.elapsed().as_millis();
        if entry_ms > 10 {
            info!(
                "FUSE create entry_exists slow: parent={}, name={}, exists={}, took={}ms",
                parent, name_str, exists, entry_ms
            );
        }
        if exists {
            return Err(std::io::Error::from_raw_os_error(libc::EEXIST));
        }

        let now = chrono::Utc::now().timestamp();

        // 通过 MetadataClient.create RPC 走 Filer Raft leader（强一致）。
        // Filer 端通过 Zone 自分配 needle_id + volume_id（alloc_for_new_file），
        // 在响应中返回给客户端。客户端必须用 Filer 返回的值构造 fid/chunks，
        // 保证与 Filer 元数据一致。
        //
        // 历史 BUG：旧代码先调用 assign_fid 从 Master 分配 needle_id_A，再把
        // fid_info 传给 Filer；但 Filer handle_create 忽略 fid_info，自己分配
        // needle_id_B。客户端用 needle_id_A 写数据，Filer 元数据存 needle_id_B。
        // sync 失败时元数据永久错乱，重新挂载后读 needle_id_B → needle not found。
        // 修复：删除 assign_fid，完全依赖 Filer 返回的 volume_id/file_key。
        let file_mode = args.mode | 0o100000;
        let uid = ctx.uid;
        let gid = ctx.gid;
        let meta_client = self.client.facade().meta_shard_client().clone();
        let shard_id = self.routing_shard(parent);
        let name_owned = name_str.to_string();
        let t_create = std::time::Instant::now();
        let attr = self
            .client
            .block_on(async move {
                meta_client
                    .create(parent, &name_owned, file_mode, uid, gid, shard_id, None)
                    .await
            })
            .map_err(|e| {
                let errno = filer_error_to_errno(&e.to_string());
                if errno == libc::EIO {
                    error!("create RPC failed: {}", e);
                } else {
                    debug!("create RPC failed: {} -> errno={}", e, errno);
                }
                std::io::Error::from_raw_os_error(errno)
            })?;
        let create_ms = t_create.elapsed().as_millis();
        let inode = attr.inode;
        let t_post_rpc = std::time::Instant::now();

        // Phase-4 §5.1 Lockify: speculatively self-declare inode
        // ownership for the new file before the Inline/Stripe/Flat
        // placement branches diverge. Async-synced to filer; the
        // first write into this inode will hit the lease cache
        // instead of issuing a synchronous acquire RPC.
        self.lockify_declare_new_inode(inode);
        let lockify_ms = t_post_rpc.elapsed().as_millis();

        // 修改了父目录内容（新增文件），invalidate 父目录的 dentry 缓存。
        // 保持目录 lease（修改是自己发起，下次 readdir 重新拉取即可）。
        let t_inval = std::time::Instant::now();
        self.invalidate_dir_entries(parent);
        let inval_ms = t_inval.elapsed().as_millis();

        info!(
            "FUSE create timing: inode={}, create_rpc={}ms, lockify={}ms, inval={}ms, total_after_inval={}ms",
            inode, create_ms, lockify_ms, inval_ms, t0.elapsed().as_millis()
        );

        // P2.5: Inline 模式分支。Filer 在 CREATE 响应中返回
        // Placement::Inline { max_size } (无 volume_id/needle_id)。
        // 客户端初始化空 inline buffer, 后续 write 追加到 buffer,
        // release 时一次性发 Filer (inline_data), 完全绕过 Volume Server。
        if attr.is_inline() {
            let inline_max = attr.inline_max_size.unwrap_or(INLINE_HARD_LIMIT as u32) as usize;
            info!(
                "FUSE create inline: inode={}, max_size={}, create_rpc={}ms, total={}ms",
                inode,
                inline_max,
                create_ms,
                t0.elapsed().as_millis()
            );
            // 初始化空 inline buffer + 记录阈值.
            // dirty=true: CREATE 时 Filer 仅返回 Placement::Inline 但未持久化
            // inline_data (inline_data=None). 即使无 WRITE, release 也必须 sync
            // inline_data=Some(empty) 让 Filer 记录 InlineData{vec![]}, 否则文件
            // 在 Filer 端既无 chunks 又无 inline_data, 重开后 read 走 Flat 路径
            // (fid=None) → EIO. 这也是 P2.5c 0 字节文件优化的基础.
            self.inline_buffers.insert(
                inode,
                InlineBuffer {
                    data: Vec::with_capacity(inline_max),
                    dirty: true,
                    original_len: 0,
                    modified_in_place: false,
                    needs_refresh: false,
                },
            );
            self.inline_max_sizes.insert(inode, inline_max as u32);

            let entry = CachedEntry {
                inode,
                parent,
                name: name_str.to_string(),
                is_dir: false,
                is_symlink: false,
                symlink_target: None,
                nlink: 1,
                fid: None, // Inline 模式无 volume/needle
                size: 0,
                mode: file_mode,
                uid: ctx.uid,
                gid: ctx.gid,
                atime: now,
                mtime: now,
                ctime: now,
                xattrs: HashMap::new(),
                chunks: Vec::new(), // Inline 模式无 chunk 映射
                hard_link_id: String::new(),
                hard_link_counter: 0,
                content_size: 0,
                disk_size: 0,
                generation: 0,
                placement: None,
                reliability: powerfs_layout::reliability::Reliability::default(),
                replica_chunks: Vec::new(),
                shard_id: None,
                cached_at: Instant::now(),
                state: EntryState::default(),
                hold: HoldState::default(),
                cap: None,
                dentry_lease: None,
                dir_shared_gen: 0,
            };
            // Phase 3: use insert_pinned to set hold=Pinned BEFORE insert.
            // The old pattern (pin_inode before insert) was a no-op when the
            // inode was not yet in the cache (entry.hold is authoritative).
            *self.open_inodes.write().unwrap().entry(inode).or_insert(0) += 1;
            self.cache.insert_pinned(entry.clone());
            debug!("create: inline mode, inode={}, dir={}", inode, parent);
            // §13 Cap model: create returns an open handle — acquire cap
            // so write/setattr can mark dirty and recall can flush.
            self.acquire_cap_on_open(inode, true);
            return Ok((
                self.create_fuse_entry(&entry),
                Some(inode),
                fuse_backend_rs::abi::fuse_abi::OpenOptions::empty(),
                None,
            ));
        }

        // === P3: Stripe 模式分支 ===
        // Filer 在 CREATE 响应中返回 Placement::Stripe + PerChunk chunks.
        // 客户端存储 placement + chunks, 后续 write/read 用 Placement::locate()
        // 路由到正确的 volume.
        if attr.is_stripe() {
            let placement = attr.placement.clone().unwrap();
            let stripe_chunks: Vec<CachedFileChunk> = attr
                .chunks
                .iter()
                .map(|c| CachedFileChunk {
                    offset: c.offset,
                    size: c.size,
                    mtime: c.mtime,
                    needle_id: c.needle_id,
                    volume_id: c.volume_id,
                    crc32: c.crc32,
                })
                .collect();
            info!(
                "FUSE create stripe: inode={}, placement={:?}, chunks={}, create_rpc={}ms, total={}ms",
                inode,
                placement,
                stripe_chunks.len(),
                create_ms,
                t0.elapsed().as_millis()
            );

            let entry = CachedEntry {
                inode,
                parent,
                name: name_str.to_string(),
                is_dir: false,
                is_symlink: false,
                symlink_target: None,
                nlink: 1,
                fid: None, // Stripe 模式不用单一 fid
                size: 0,
                mode: file_mode,
                uid: ctx.uid,
                gid: ctx.gid,
                atime: now,
                mtime: now,
                ctime: now,
                xattrs: HashMap::new(),
                chunks: stripe_chunks,
                hard_link_id: String::new(),
                hard_link_counter: 0,
                content_size: 0,
                disk_size: 0,
                generation: 0,
                placement: Some(placement),
                reliability: powerfs_layout::reliability::Reliability::default(),
                replica_chunks: Vec::new(),
                shard_id: None,
                cached_at: Instant::now(),
                state: EntryState::default(),
                hold: HoldState::default(),
                cap: None,
                dentry_lease: None,
                dir_shared_gen: 0,
            };
            *self.open_inodes.write().unwrap().entry(inode).or_insert(0) += 1;
            self.cache.insert_pinned(entry.clone());
            debug!("create: stripe mode, inode={}, dir={}", inode, parent);
            // §13 Cap model: create returns an open handle — acquire cap.
            self.acquire_cap_on_open(inode, true);
            return Ok((
                self.create_fuse_entry(&entry),
                Some(inode),
                fuse_backend_rs::abi::fuse_abi::OpenOptions::empty(),
                None,
            ));
        }

        // === Flat 模式 (原路径) ===
        // 从 Filer 响应提取自分配的 volume_id/needle_id（权威值）
        let volume_id = attr.volume_id.ok_or_else(|| {
            error!(
                "create: Filer response missing volume_id for inode {} (Filer zone not registered?)",
                inode
            );
            std::io::Error::from_raw_os_error(libc::EIO)
        })?;
        let needle_id = attr.file_key.ok_or_else(|| {
            error!(
                "create: Filer response missing file_key for inode {} (Filer zone not registered?)",
                inode
            );
            std::io::Error::from_raw_os_error(libc::EIO)
        })?;
        let fid = Fid {
            volume_id: VolumeId(volume_id),
            cookie: 0,
            file_key: needle_id,
        };
        info!(
            "FUSE create timing: create_rpc={}ms, total={}ms, inode={}, volume_id={}, needle_id={:#x}",
            create_ms,
            t0.elapsed().as_millis(),
            inode,
            volume_id,
            needle_id
        );

        // 构造 CachedEntry：fid/chunks 来自 Filer 返回值（权威）。
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
            placement: None,
            reliability: powerfs_layout::reliability::Reliability::default(),
            replica_chunks: Vec::new(),
            shard_id: None,
            cached_at: Instant::now(),
            state: EntryState::default(),
            hold: HoldState::default(),
            cap: None,
            dentry_lease: None,
            dir_shared_gen: 0,
        };
        // CRITICAL: Pin the inode BEFORE inserting the cache entry.
        // The Filer pushes an Invalidate after the create RPC commits, and
        // Phase 3: use insert_pinned to atomically set hold=Pinned and insert.
        // The old pattern (pin_inode before insert) was a no-op because the
        // inode was not yet in the cache (entry.hold is authoritative, not
        // pinned_inodes). insert_pinned sets hold on the entry before insert,
        // so InvalidateHandler skips the entry from the moment it enters cache.
        *self.open_inodes.write().unwrap().entry(inode).or_insert(0) += 1;
        self.cache.insert_pinned(entry.clone());
        debug!("create: RPC done, inode={}, dir={}", inode, parent);
        // §13 Cap model: create returns an open handle — acquire cap.
        self.acquire_cap_on_open(inode, true);

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
        flags: u32,
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
        let _parent = if let Some(entry) = self.cache.get_inode(inode) {
            if entry.is_dir {
                debug!("open: entry is directory, returning EISDIR");
                return Err(std::io::Error::from_raw_os_error(libc::EISDIR));
            }
            // Pin before RPC: Invalidate arriving during refresh is skipped.
            // CRITICAL: Hold open_inodes lock while calling pin_inode to
            // prevent a concurrent release from unpining between the count
            // increment and the hold increment. Without this, release #1
            // could unpin (hold 1→0=Unpinned) after open #2 increments
            // open_inodes but before pin_inode runs, leaving the inode open
            // but unpinned → InvalidateHandler evicts mid-write (ENOENT).
            {
                let mut open_inodes = self.open_inodes.write().unwrap();
                *open_inodes.entry(inode).or_insert(0) += 1;
                self.cache.pin_inode(inode);
            }
            // Cache hit: best-effort 从 filer 刷新 size/chunks
            let parent = entry.parent;

            let has_inode_lease = self
                .lock_manager
                .state()
                .get_inode(inode)
                .map(|le| le.mode == powerfs_lock_fuse::LockMode::Exclusive)
                .unwrap_or(false);
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
            let has_dirty_inline = self
                .inline_buffers
                .get(&inode)
                .map(|b| b.dirty)
                .unwrap_or(false);
            let has_dirty_chunks = self.chunk_cache.has_dirty_chunks(inode);
            debug!(
                "open: inode={} dirty_check has_inode_lease={} has_dirty_chunks={} has_dirty_inline={} inline_buffers_contains={}",
                inode,
                has_inode_lease,
                has_dirty_chunks,
                has_dirty_inline,
                self.inline_buffers.contains_key(&inode)
            );
            if has_inode_lease {
                debug!(
                    "open: skipping filer refresh for inode={} (Exclusive lease held, local cache authoritative)",
                    inode
                );
            } else if has_dirty_chunks || has_dirty_inline {
                // Local cache has unsynced data (write happened but
                // sync_size_chunks_on_close hasn't completed yet, e.g.,
                // async FUSE RELEASE). The local cache is authoritative:
                // skip the Filer refresh to preserve content_size and
                // chunk data for append writes. This also covers Inline
                // mode: dirty data lives in inline_buffers, not chunk_cache.
                //
                // L4.21 fix: Previously, this block had a "stale delta sync"
                // that synced the dirty delta to the Filer when filer_size >
                // buf_orig_len. This was removed because it races with the
                // release path: both the open's delta sync and the release
                // can sync the same delta concurrently, creating duplicates.
                //
                // The append-mode release already handles concurrent appends
                // correctly: it sends only data[original_len..] with
                // is_append=true, so the Filer atomically appends our delta
                // to its existing data (which includes other clients'
                // appends). No need to pre-sync in open().
                debug!(
                    "open: skipping filer refresh for inode={} (has dirty/unsynced chunks or inline buffer)",
                    inode
                );
            } else if entry.state != EntryState::Stale && !entry.chunks.is_empty() {
                // P4: Trust cache hit on non-Stale entries with known chunks.
                //
                // Rationale: every Filer-driven metadata change (other
                // clients writing, resize, truncate, chmod, unlink, rename,
                // setattr) is pushed to this FUSE client via the
                // coherence Invalidate notification, which transitions the
                // entry to Stale and/or evicts it (see invalidate.rs /
                // InvalidateHandler).  Therefore if state is *not* Stale,
                // no other client has modified this file since the last
                // time our cache was refreshed/inserted.  Skipping the
                // refresh RPC avoids a round-trip on every open of a
                // recently-touched file (common for shell pipelines and
                // re-opens by the same process).
                //
                // Guard: require non-empty chunks. The Filer returns empty
                // chunks for newly-created files before the first
                // sync_size_chunks_on_close has run; in that window we
                // can't tell if another client raced and appended, so we
                // still refresh. Entries where the local cache has chunks
                // AND state != Stale are authoritative.
                debug!(
                    "open: skipping filer refresh for inode={} \
                     (P4 cache trust: state={:?}, chunks={}, no invalidation received)",
                    inode,
                    entry.state,
                    entry.chunks.len()
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
                    let local_cs_before = self
                        .cache
                        .get_inode(inode)
                        .map(|e| e.content_size)
                        .unwrap_or(u64::MAX);
                    // #region debug-point fuse-inline-data-loss-dbg-open-refresh
                    info!(
                        "DBG-INLINE: open refresh inode={} insert_pinned: local_cs={} filer_cs={} has_dirty_chunks={} has_dirty_inline={} hold_pinned={}",
                        inode, local_cs_before, filer_content_size, has_local_data,
                        entry.content_size > fresh.content_size && has_local_data,
                        self.cache.get_inode(inode).map(|e| e.hold.is_pinned()).unwrap_or(false)
                    );
                    // #endregion
                    // P3: Preserve placement from existing entry. The FilerEntry
                    // returned by get_entry_by_inode does not carry FileLayout,
                    // so placement would be lost on refresh. Placement is set at
                    // create time and never changes, so the cached value is
                    // authoritative. Also clear fid for Stripe files to ensure
                    // write/read paths route to the Stripe branch.
                    let mut fresh = fresh;
                    fresh.placement = entry.placement.clone();
                    if fresh.placement.is_some() {
                        fresh.fid = None;
                    }
                    // CRITICAL: Use insert_pinned (not insert) to preserve the
                    // Pinned hold state. The open handler already incremented
                    // open_inodes and called pin_inode before the refresh RPC.
                    // Using plain insert() replaces the pinned entry with an
                    // unpinned one, allowing InvalidateHandler to evict it
                    // mid-write (causing ENOENT in mdtest-hard).
                    self.cache.insert_pinned(fresh);
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
                    // Phase 3: use insert_pinned to set hold=Pinned BEFORE insert.
                    // pin_inode() is a no-op when the inode is not in the cache.
                    *self.open_inodes.write().unwrap().entry(inode).or_insert(0) += 1;
                    let cached = self.entry_to_cached(p, &filer_entry);
                    self.cache.insert_pinned(cached);
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

        // P2.5: Inline 模式刷新. open 时若文件是 inline 模式 (已关闭的 inline
        // 文件被重新打开), 从 Filer getattr 拉取 inline_data 填充 buffer.
        // 数据在 Filer 元数据中, 一次 RPC 拿全, 后续 read 直接从 buffer 服务.
        //
        // 跳过条件:
        // 1. inode 已在 inline_buffers 中 (create 刚创建仍在写, 或
        //    并发 open 第二次进入) — 此时本地 buffer 权威, 无需刷新.
        // 2. entry.fid.is_some() — 文件已迁移到 Flat 模式. Filer 可能仍返回
        //    is_inline()=true (迁移尚未 sync 到 Filer), 重新创建 inline buffer
        //    会导致文件回退到 Inline 模式, 丢失 Flat 路径的 chunks 数据.
        //    (BUG: append 写入时 open 回调将已迁移文件回退为 Inline)
        //
        // L4.21 FIX: 如果 inline buffer 存在但非 dirty, 且 entry.size (刚从
        // Filer 刷新) 与 buffer 数据长度不一致, 说明其他客户端已 append 数据
        // 到 Filer, 本地 buffer 过期. 必须移除 stale buffer 并重新从 Filer 获取,
        // 否则 O_APPEND 写入会用 entry.size 作为 offset, 在 buffer 中产生零填充
        // 间隙 (buffer_len..offset), release 时 delta 包含这些零字节, 导致:
        //   1. 文件内容损坏 (零字节混入)
        //   2. 后续写入 offset < original_len 触发 mod_in_place=true →
        //      can_append=false → OVERWRITE 模式 → 覆盖其他客户端数据
        let skip_inline_refresh = self
            .cache
            .get_inode(inode)
            .map(|e| e.fid.is_some())
            .unwrap_or(false);

        // Detect and remove stale inline buffer (L4.21 root cause).
        // Only remove when NOT dirty (dirty buffer has unsynced local data
        // that is authoritative — we hold the write lease).
        //
        // Two staleness signals:
        // 1. needs_refresh: InvalidateHandler set this flag when it skipped
        //    invalidation because the buffer was dirty. After the buffer is
        //    synced (dirty → false), this flag forces a Filer refresh to pick
        //    up other clients' concurrent appends. Without it, the size check
        //    below passes (entry.size was never updated during the skip),
        //    causing cross-client stale reads (L4.21: A sees 175/200 lines).
        // 2. Size mismatch: entry.size (from Filer) != buf_len (local buffer).
        //    This catches cases where the buffer was populated from a stale
        //    cache entry or the file was modified by other clients while we
        //    had no buffer (e.g., between release and the next open).
        //
        // Track whether the buffer was removed due to staleness. Only in
        // that case do we need to invalidate the kernel page cache after
        // re-fetching from the Filer. If the buffer was simply absent
        // (first open or after normal release), the kernel page cache is
        // either empty or already invalidated by the release path —
        // invalidating again is harmless but unnecessary, and worse, it
        // can discard valid kernel page cache when delayed RELEASEs haven't
        // synced yet (the Filer returns stale data, and the kernel had the
        // correct data from the writes).
        let mut was_stale = false;
        if !skip_inline_refresh {
            if let Some(inline_buf) = self.inline_buffers.get(&inode) {
                if !inline_buf.dirty {
                    let needs_refresh = inline_buf.needs_refresh;
                    let buf_len = inline_buf.data.len() as u64;
                    let entry_size = self.cache.get_inode(inode).map(|e| e.size).unwrap_or(0);
                    if needs_refresh || entry_size != buf_len {
                        warn!(
                            "open: inode={} removing stale inline buffer \
                             (needs_refresh={}, buf_len={} != entry_size={}, not dirty) — re-fetching from Filer",
                            inode, needs_refresh, buf_len, entry_size
                        );
                        drop(inline_buf); // release DashMap read guard before remove
                        self.inline_buffers.remove(&inode);
                        was_stale = true;
                    }
                }
            }
        }

        if !self.inline_buffers.contains_key(&inode) && !skip_inline_refresh {
            let meta_client = self.client.facade().meta_shard_client().clone();
            // Route getattr via the inode's own shard. After the split-create
            // refactor the inode record lives on calculate_shard(inode);
            // any other shard returns "ino not found" → inline_buffers not
            // populated → read falls through to the stripe/Flat path →
            // "placement Inline but no chunks" EIO.
            let routing_shard = self.routing_shard(inode);
            let ino = inode;
            let attr_result = self
                .client
                .block_on(async move { meta_client.getattr(ino, routing_shard).await });
            match attr_result {
                Ok(attr) if attr.is_inline() => {
                    // 更新 cache size 为权威值 (Filer 端 inline 文件的 size)
                    self.cache.set_content_size(inode, attr.size);
                    if let Some(max_size) = attr.inline_max_size {
                        self.inline_max_sizes.insert(inode, max_size);
                    }
                    // 填充 inline buffer (已关闭的 inline 文件数据来自 Filer)
                    let data = attr.inline_data.unwrap_or_default();
                    let data_len = data.len();
                    warn!(
                        "OPEN_DBG: inode={} inline_buf INSERT from filer, data_len={}, attr.size={}, was_stale={}, thread={:?}",
                        inode, data_len, attr.size, was_stale, std::thread::current().id()
                    );
                    self.inline_buffers.insert(
                        inode,
                        InlineBuffer {
                            data,
                            dirty: false,
                            original_len: data_len,
                            modified_in_place: false,
                            needs_refresh: false,
                        },
                    );
                    // L4.21 fix: Invalidate the kernel page cache after
                    // refreshing the inline buffer from the Filer. The
                    // kernel may still hold stale page cache from a previous
                    // open (e.g., during concurrent appends where delayed
                    // RELEASEs keep the inode "open" in the kernel's view,
                    // preventing automatic page cache invalidation even
                    // though keep_cache is not set). Without this, reads
                    // after the open serve from the stale kernel page cache
                    // instead of the freshly-refreshed inline buffer.
                    self.notify_kernel_inval_inode(inode);
                }
                Ok(_) => {
                    // Flat 模式文件: 清理可能残留的 inline buffer (文件已被迁移)
                    if self.inline_buffers.remove(&inode).is_some() {
                        self.inline_max_sizes.remove(&inode);
                        debug!("open: inode={} is flat, removed stale inline buffer", inode);
                    }
                }
                Err(e) => {
                    // getattr 失败不阻塞 open (best-effort, 同 open_count_inc)
                    debug!(
                        "open: inline refresh getattr for inode {} failed (best-effort): {}",
                        inode, e
                    );
                }
            }
        }

        // Phase 3.5.3: 通知 filer 递增 open_count（fire-and-forget，不阻塞 open）
        // 原实现用 block_on 同步等待，每个 open 多一个 block_on 占用 runtime worker。
        // open_count 是 best-effort 统计，失败不影响正确性，改为 spawn 异步发送。
        let meta_shard_client = self.client.facade().meta_shard_client().clone();
        let open_count_shard = self.routing_shard(inode);
        let req = powerfs_coherence::OpenCountRequest {
            shard_id: open_count_shard,
            inode,
        };
        let runtime = self.client.runtime().handle().clone();
        runtime.spawn(async move {
            if let Err(e) = meta_shard_client.open_count_inc(&req).await {
                debug!(
                    "open: open_count_inc for inode {} failed (best-effort): {}",
                    inode, e
                );
            }
        });

        // Phase-4 §5.2 (P3): Pre-acquire the inode lease at open time
        // and bind it to the open-file registry. Subsequent
        // `flush_dirty_chunks` calls pass this token to
        // `write_blob_batch_with_lease`, bypassing `ensure_lease`'s
        // cache lookup + proactive-renew path on every flush.
        //
        // Only applies to inode-lease mode AND files already on the
        // volume server (fid present). Inline files (no fid) don't
        // need a volume-server lease; newly-created files are
        // handled by the Lockify fast path (phase 4 §5.1).
        //
        // Best-effort: if the acquire fails (e.g. Filer temporarily
        // unreachable), the write path's `ensure_lease` will retry on
        // the first flush — correctness is preserved.
        let is_write_open = (flags as i32 & libc::O_ACCMODE) != libc::O_RDONLY;
        if self.client.is_inode_lease_mode() {
            if let Some(entry) = self.cache.get_inode(inode) {
                if entry.fid.is_some() {
                    let client_id = self.client.client_id();
                    let duration_ms = self.lease_duration_ms;
                    match self
                        .client
                        .acquire_inode_lease(inode, &client_id, duration_ms)
                    {
                        Ok((token, _expire_ms)) => {
                            let expire_at = std::time::Instant::now()
                                + std::time::Duration::from_millis(duration_ms);
                            self.open_file_leases.bind(inode, token.clone(), expire_at);
                            debug!("open: pre-acquired inode lease for inode={}", inode);
                        }
                        Err(e) => {
                            debug!(
                                "open: inode lease pre-acquire failed for inode={} \
                                 (best-effort, ensure_lease will retry): {}",
                                inode, e
                            );
                        }
                    }
                }
            }
        }

        // §13 Cap model: acquire structured cap bits from the server.
        // See `acquire_cap_on_open` for details. Applies to ALL files
        // (inline AND flat). Fast path skips RPC if cap already valid.
        self.acquire_cap_on_open(inode, is_write_open);

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

        // P2.5: Inline 模式读取分支. 数据在 inline_buffers 中 (内存),
        // 直接切片返回, 完全绕过 Volume Server + chunk_cache + lease.
        // 仅 Inline 模式 inode (create/open 时插入 inline_buffers) 走此路径.
        //
        // Fallback: if the file is inline (fid=None, chunks empty) but the
        // inline_buffers entry is missing (e.g., open's getattr failed under
        // concurrent access), fetch inline_data from the Filer on-demand.
        // Without this, read falls through to the Flat path which returns EIO
        // because fid is None — causing T8.02 concurrent read failures.
        if self.inline_buffers.get(&inode).is_none()
            && entry.fid.is_none()
            && entry.chunks.is_empty()
        {
            debug!(
                "read: inode={} is inline (no fid, no chunks) but inline_buffers missing, fetching from filer",
                inode
            );
            let meta_client = self.client.facade().meta_shard_client().clone();
            let routing_shard = self.routing_shard(inode);
            let ino = inode;
            match self
                .client
                .block_on(async move { meta_client.getattr(ino, routing_shard).await })
            {
                Ok(attr) if attr.is_inline() => {
                    let data = attr.inline_data.unwrap_or_default();
                    let data_len = data.len();
                    self.cache.set_content_size(inode, attr.size);
                    if let Some(max_size) = attr.inline_max_size {
                        self.inline_max_sizes.insert(inode, max_size);
                    }
                    warn!(
                        "READ_DBG: inode={} inline_buf INSERT from filer (read fallback), data_len={}, attr.size={}, thread={:?}",
                        inode, data_len, attr.size, std::thread::current().id()
                    );
                    self.inline_buffers.insert(
                        inode,
                        InlineBuffer {
                            data,
                            dirty: false,
                            original_len: data_len,
                            modified_in_place: false,
                            needs_refresh: false,
                        },
                    );
                    // Note: Do NOT call notify_kernel_inval_inode here.
                    // The read path is called after open(), which already
                    // handles kernel page cache invalidation when needed
                    // (only when the buffer was stale). Invalidating here
                    // would discard valid kernel page cache when delayed
                    // RELEASEs haven't synced yet.
                }
                Ok(_) => {
                    warn!(
                        "read: inode={} getattr returned non-inline, cannot read inline file",
                        inode
                    );
                }
                Err(e) => {
                    warn!(
                        "read: inode={} getattr failed while fetching inline data: {}",
                        inode, e
                    );
                }
            }
        }

        if let Some(inline_buf) = self.inline_buffers.get(&inode) {
            let file_size = inline_buf.data.len() as u64;
            if offset >= file_size {
                debug!(
                    "read inline: inode={} offset={} >= file_size={}, returning 0",
                    inode, offset, file_size
                );
                return Ok(0);
            }
            let end = std::cmp::min(offset + size as u64, file_size);
            let start = offset as usize;
            let end_idx = end as usize;
            // Clone the slice (≤8KB) to release the DashMap read guard before I/O,
            // avoiding holding the shard lock during write_all.
            let slice = inline_buf.data[start..end_idx].to_vec();
            drop(inline_buf);
            w.write_all(&slice)?;
            let n = end_idx - start;
            debug!(
                "read inline: inode={} offset={} size={} -> {} bytes",
                inode, offset, size, n
            );
            return Ok(n);
        }

        // === P6: EC 降级读分支 ===
        // entry.reliability == Reliability::EC { data, parity } → 文件以纠删码存储.
        // entry.chunks 前 data 个为数据 shard, 后 parity 个为校验 shard.
        // 读路径: 先读 data shard; 任一失败/缺失时读可用 data + parity, 用
        // EcEncoder::decode_missing 重建完整文件数据, 再按 1MB chunk 填充
        // chunk_cache. 后续读取命中 chunk_cache, 无需重复重建.
        if let powerfs_layout::reliability::Reliability::EC { data, parity } = &entry.reliability {
            let data_shards = *data as usize;
            let parity_shards = *parity as usize;
            let total_shards = data_shards + parity_shards;

            let ec_chunks = entry.chunks.clone();
            // Guard: EC path requires complete stripe groups (multiples of total_shards).
            if ec_chunks.is_empty() || ec_chunks.len() % total_shards != 0 {
                log::warn!(
                    "read ec: inode={} has {} chunks, need non-empty multiple of {} (data={}+parity={}), returning EIO",
                    inode,
                    ec_chunks.len(),
                    total_shards,
                    data_shards,
                    parity_shards
                );
                return Err(std::io::Error::from_raw_os_error(libc::EIO));
            }
            let num_groups = ec_chunks.len() / total_shards;
            const EC_SHARD_SIZE: u64 = 1024 * 1024; // 1MB = chunk_size
            let group_data_size = data_shards as u64 * EC_SHARD_SIZE;

            let chunk_size = self.chunk_cache.chunk_size();
            let file_size = entry.size;

            if offset >= file_size {
                return Ok(0);
            }
            let end_offset = std::cmp::min(offset + size as u64, file_size);

            let start_chunk = self.chunk_cache.get_chunk_index(offset);
            let prefetch_end = std::cmp::min(end_offset + PREFETCH_CHUNKS * chunk_size, file_size);
            let prefetch_end_chunk = if prefetch_end == 0 {
                0
            } else {
                self.chunk_cache.get_chunk_index(prefetch_end - 1)
            };

            // Collect missing chunks for remote read/reconstruction
            let missing_chunks: Vec<(u64, u64)> = (start_chunk..=prefetch_end_chunk)
                .filter_map(|chunk_idx| {
                    let chunk_offset = chunk_idx * chunk_size;
                    if self.chunk_cache.get(inode, chunk_offset).is_none() {
                        Some((chunk_idx, chunk_offset))
                    } else {
                        None
                    }
                })
                .collect();

            if !missing_chunks.is_empty() {
                // 按 stripe group 重建缺失的 chunk. 每个 group 独立编码/解码,
                // group 数据 = data_shards × 1MB, 只重建缺失的 group.
                let start_group = (missing_chunks[0].1 / group_data_size) as usize;
                let end_group = (missing_chunks.last().unwrap().1 / group_data_size) as usize;
                let mtime = entry.mtime as u64;

                // EC 编码器在 group 循环外创建一次, 复用.
                let ec_config = powerfs_core::ec_thread::EcConfig {
                    data_shards,
                    parity_shards,
                    ..Default::default()
                };
                let encoder = powerfs_core::ec_thread::EcEncoder::new(ec_config);

                for group_idx in start_group..=end_group {
                    if group_idx >= num_groups {
                        continue;
                    }
                    let group_start = group_idx as u64 * group_data_size;
                    let group_end = std::cmp::min(group_start + group_data_size, file_size);

                    // 跳过该 group 所有 1MB chunk 都已在 chunk_cache 中的情况.
                    let mut all_cached = true;
                    let mut co = 0u64;
                    while co < group_end - group_start {
                        let cache_offset = group_start + co;
                        if cache_offset >= file_size {
                            break;
                        }
                        if self.chunk_cache.get(inode, cache_offset).is_none() {
                            all_cached = false;
                            break;
                        }
                        co += chunk_size;
                    }
                    if all_cached {
                        continue;
                    }

                    let group_base = group_idx * total_shards;

                    // 读取该 group 的所有 shards (data + parity).
                    // 失败/缺失的 shard 置 None, 由 parity 降级重建.
                    let mut shards: Vec<Option<Vec<u8>>> = vec![None; total_shards];
                    let mut read_ok = 0usize;

                    for i in 0..total_shards {
                        let chunk = &ec_chunks[group_base + i];
                        let read_size = chunk.size as i32;
                        match self.client.get_volume_addr(chunk.volume_id) {
                            Ok(addr) => {
                                match self.client.read_blob(
                                    &addr,
                                    chunk.volume_id,
                                    chunk.needle_id,
                                    0,
                                    read_size,
                                ) {
                                    Ok(shard_data) => {
                                        // CRC32 校验: 不匹配视为缺失, 由 parity 重建.
                                        if chunk.crc32 != 0 {
                                            let actual = crc32fast::hash(&shard_data);
                                            if actual != chunk.crc32 {
                                                log::warn!(
                                                    "read ec: inode={} group {} shard {} CRC mismatch expected={:#x} actual={:#x}, will reconstruct",
                                                    inode, group_idx, i, chunk.crc32, actual
                                                );
                                                continue;
                                            }
                                        }
                                        shards[i] = Some(shard_data);
                                        read_ok += 1;
                                    }
                                    Err(e) => {
                                        log::warn!(
                                            "read ec: inode={} group {} shard {} read failed (vol={} needle={:#x}): {}",
                                            inode, group_idx, i, chunk.volume_id, chunk.needle_id, e
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                log::warn!(
                                    "read ec: inode={} group {} shard {} get_volume_addr failed (vol={}): {}",
                                    inode,
                                    group_idx,
                                    i,
                                    chunk.volume_id,
                                    e
                                );
                            }
                        }
                    }

                    let data_available = shards
                        .iter()
                        .take(data_shards)
                        .filter(|s| s.is_some())
                        .count();

                    // 重建该 group 的完整数据 (data_shards × 1MB, 末尾可能含零填充).
                    let group_data: Vec<u8> = if data_available == data_shards {
                        // Fast path: all data shards present — concatenate.
                        let mut gdata = Vec::with_capacity(
                            shards
                                .iter()
                                .take(data_shards)
                                .map(|s| s.as_ref().map(|v| v.len()).unwrap_or(0))
                                .sum(),
                        );
                        for s in shards.iter().take(data_shards) {
                            gdata.extend_from_slice(s.as_ref().unwrap());
                        }
                        gdata
                    } else if read_ok >= data_shards {
                        // Degraded path: 部分数据 shard 缺失, 但 data+parity 足够重建.
                        log::info!(
                            "read ec degraded: inode={} group {} data_available={}/{} total_available={}/{}, reconstructing",
                            inode,
                            group_idx,
                            data_available,
                            data_shards,
                            read_ok,
                            total_shards
                        );
                        match encoder.decode_missing(&mut shards) {
                            Ok(gdata) => gdata,
                            Err(e) => {
                                log::error!(
                                    "read ec: inode={} group {} decode_missing failed: {}, returning EIO",
                                    inode,
                                    group_idx,
                                    e
                                );
                                return Err(std::io::Error::from_raw_os_error(libc::EIO));
                            }
                        }
                    } else {
                        // Not enough shards to reconstruct.
                        log::error!(
                            "read ec: inode={} group {} only {}/{} shards available, need {} to reconstruct, returning EIO",
                            inode,
                            group_idx,
                            read_ok,
                            total_shards,
                            data_shards
                        );
                        return Err(std::io::Error::from_raw_os_error(libc::EIO));
                    };

                    // 用 1MB chunk 填充 chunk_cache, 末尾按 file_size 截断
                    // (最后一个 group 的零填充不写入缓存).
                    let mut off = 0u64;
                    while off < group_data.len() as u64 {
                        let cache_offset = group_start + off;
                        if cache_offset >= file_size {
                            break;
                        }
                        // Bound by both file_size and group_data.len() to prevent
                        // slice out-of-bounds when group_data is shorter than expected
                        // (e.g., EC reconstruction produced fewer bytes than file_size).
                        let actual_end = std::cmp::min(
                            off + chunk_size,
                            std::cmp::min(file_size - group_start, group_data.len() as u64),
                        );
                        let chunk_data = group_data[off as usize..actual_end as usize].to_vec();
                        self.chunk_cache
                            .put(inode, cache_offset, chunk_data.into(), mtime, 0);
                        off += chunk_size;
                    }
                }
            }

            // Copy from chunk_cache to writer (same as Flat/Stripe)
            let mut total_written = 0usize;
            let mut current_offset = offset;
            let end = end_offset;

            while current_offset < end {
                let chunk_data = self
                    .chunk_cache
                    .get(inode, current_offset)
                    .ok_or_else(|| std::io::Error::from_raw_os_error(libc::EIO))?;

                let chunk_start = (current_offset % self.chunk_cache.chunk_size()) as usize;
                let available_in_chunk = chunk_data.data.len().saturating_sub(chunk_start);
                let bytes_left_in_chunk = available_in_chunk.min((end - current_offset) as usize);

                if bytes_left_in_chunk == 0 {
                    // Hole: zero-fill reads beyond actual chunk data but within file_size.
                    let chunk_size = self.chunk_cache.chunk_size();
                    let chunk_end = (current_offset / chunk_size + 1) * chunk_size;
                    let zero_end = std::cmp::min(chunk_end, end);
                    let zero_len = (zero_end - current_offset) as usize;
                    let zeros = vec![0u8; zero_len];
                    w.write_all(&zeros)?;
                    total_written += zero_len;
                    current_offset = zero_end;
                    continue;
                }

                let slice = &chunk_data.data[chunk_start..chunk_start + bytes_left_in_chunk];
                w.write_all(slice)?;
                total_written += bytes_left_in_chunk;
                current_offset += bytes_left_in_chunk as u64;
            }

            debug!(
                "read ec: inode={} offset={} size={} -> {} bytes",
                inode, offset, size, total_written
            );
            return Ok(total_written);
        }

        // === P3: Stripe 模式读取分支 ===
        // entry.placement.is_some() && entry.fid.is_none() → Stripe/WideStripe.
        // chunk_cache 逻辑与 Flat 相同; 差异仅在 cache miss 时按
        // resolve_stripe_chunk() 路由到正确的 volume/needle.
        if let Some(placement) = entry.placement.as_ref().filter(|_| entry.fid.is_none()) {
            let stripe_chunks = entry.chunks.clone();
            // Guard: Stripe path requires at least one chunk to route reads.
            // Empty chunks + placement=Some can happen if metadata is incomplete
            // (e.g., inline file misclassified). Return EIO instead of panicking.
            if stripe_chunks.is_empty() {
                log::warn!(
                    "read stripe: inode={} has placement={:?} but no chunks, returning EIO",
                    inode,
                    placement
                );
                return Err(std::io::Error::from_raw_os_error(libc::EIO));
            }
            // chunk_size == stripe_size (both 1MB by default).
            let chunk_size = self.chunk_cache.chunk_size();

            let file_size = if entry.size > 0 {
                entry.size
            } else if !entry.chunks.is_empty() {
                entry
                    .chunks
                    .iter()
                    .map(|c| c.offset + if c.size > 0 { c.size } else { chunk_size })
                    .max()
                    .unwrap_or(0)
            } else {
                0
            };

            if offset >= file_size {
                return Ok(0);
            }

            let end_offset = std::cmp::min(offset + size as u64, file_size);
            let start_chunk = self.chunk_cache.get_chunk_index(offset);
            let prefetch_end = std::cmp::min(end_offset + PREFETCH_CHUNKS * chunk_size, file_size);
            let prefetch_end_chunk = if prefetch_end == 0 {
                0
            } else {
                self.chunk_cache.get_chunk_index(prefetch_end - 1)
            };

            // Collect missing chunks for remote read
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
                // Pre-resolve volume addresses to populate volume_router cache.
                // After FUSE restart, the router only has volumes from the initial
                // topology fetch. Chunks written to other volumes (assigned by Filer
                // during create/migrate) need on-demand lookup from Master.
                for chunk in &stripe_chunks {
                    let _ = self.client.get_volume_addr(chunk.volume_id);
                }
                // Build chunk_map for O(1) (volume_id, needle_id) lookup.
                // Tuple order MUST match resolve_stripe_chunk's return: (volume_id, needle_id).
                let chunk_map: HashMap<u64, (u64, u64)> = stripe_chunks
                    .iter()
                    .map(|c| (c.offset, (c.volume_id, c.needle_id)))
                    .collect();
                // Build crc_map for read-path data integrity verification
                let crc_map: HashMap<u64, u32> =
                    stripe_chunks.iter().map(|c| (c.offset, c.crc32)).collect();

                let requests: Vec<powerfs_fuse_core::ReadBlobRequest> = missing_chunks
                    .iter()
                    .map(|(_chunk_idx, offset, size)| {
                        let (vol_id, needle_id) =
                            resolve_stripe_chunk(placement, &stripe_chunks, *offset, chunk_size)
                                .unwrap_or_else(|| {
                                    chunk_map.get(offset).copied().unwrap_or((
                                        stripe_chunks[0].volume_id,
                                        stripe_chunks[0].needle_id,
                                    ))
                                });
                        powerfs_fuse_core::ReadBlobRequest {
                            volume_id: vol_id,
                            file_key: needle_id,
                            offset: 0,
                            size: *size,
                        }
                    })
                    .collect();

                let results = self.client.read_blob_batch(requests);
                let mtime = entry.mtime as u64;

                for ((chunk_idx, chunk_offset, read_size), result) in
                    missing_chunks.iter().zip(results.iter())
                {
                    match result {
                        Ok(data) => {
                            // Verify CRC32 if chunk has a non-zero CRC
                            if let Some(&expected_crc) = crc_map.get(chunk_offset) {
                                if expected_crc != 0 {
                                    let actual_crc = crc32fast::hash(data);
                                    if actual_crc != expected_crc {
                                        error!(
                                            "CRC32 mismatch (stripe): inode={} offset={} expected={:#x} actual={:#x}",
                                            inode, chunk_offset, expected_crc, actual_crc
                                        );
                                        return Err(std::io::Error::from_raw_os_error(libc::EIO));
                                    }
                                }
                            }
                            self.chunk_cache.put(
                                inode,
                                *chunk_offset,
                                data.clone().into(),
                                mtime,
                                0,
                            );
                        }
                        Err(e) if e.contains("needle not found") => {
                            // Check if dirty (data in cache but not yet flushed)
                            let is_dirty = {
                                let key = (inode, *chunk_idx);
                                let shard = &self.dirty_shards[Self::dirty_shard_idx(&key)];
                                let dirty_set = shard.read().unwrap();
                                dirty_set.contains(&key)
                            };
                            if is_dirty {
                                let _ = self.flush_dirty_chunks(inode, None);
                                // Retry read after flush
                                let (vol_id, needle_id) = resolve_stripe_chunk(
                                    placement,
                                    &stripe_chunks,
                                    *chunk_offset,
                                    chunk_size,
                                )
                                .unwrap_or((
                                    stripe_chunks[0].volume_id,
                                    stripe_chunks[0].needle_id,
                                ));
                                if let Ok(addr) = self.client.get_volume_addr(vol_id) {
                                    if let Ok(data) = self
                                        .client
                                        .read_blob(&addr, vol_id, needle_id, 0, *read_size)
                                    {
                                        self.chunk_cache.put(
                                            inode,
                                            *chunk_offset,
                                            data.into(),
                                            mtime,
                                            0,
                                        );
                                        continue;
                                    }
                                }
                                // Fallback: fill with zeros
                                self.chunk_cache.put(
                                    inode,
                                    *chunk_offset,
                                    vec![0; *read_size as usize].into(),
                                    mtime,
                                    0,
                                );
                            } else {
                                self.chunk_cache.put(
                                    inode,
                                    *chunk_offset,
                                    vec![0; *read_size as usize].into(),
                                    mtime,
                                    0,
                                );
                            }
                        }
                        Err(e) => {
                            // P4: Stripe 读路径 failover — 主 volume 读取失败时,
                            // 从 replica_chunks 中查找同 offset 的副本 volume 读取.
                            let replica_map: HashMap<u64, (u64, u64)> = entry
                                .replica_chunks
                                .iter()
                                .map(|c| (c.offset, (c.needle_id, c.volume_id)))
                                .collect();
                            let primary_vol =
                                chunk_map.get(chunk_offset).map(|(_, v)| *v).unwrap_or(0);
                            if let Some(&(rep_needle, rep_vol)) = replica_map.get(chunk_offset) {
                                warn!(
                                    "read stripe failover: inode={} offset={} primary vol={} failed: {}, trying replica vol={}",
                                    inode, chunk_offset, primary_vol, e, rep_vol
                                );
                                match self.client.get_volume_addr(rep_vol) {
                                    Ok(rep_addr) => {
                                        match self.client.read_blob(
                                            &rep_addr, rep_vol, rep_needle, 0, *read_size,
                                        ) {
                                            Ok(data) => {
                                                // Verify CRC32 on replica data too
                                                if let Some(&expected_crc) =
                                                    crc_map.get(chunk_offset)
                                                {
                                                    if expected_crc != 0 {
                                                        let actual_crc = crc32fast::hash(&data);
                                                        if actual_crc != expected_crc {
                                                            error!(
                                                                "CRC32 mismatch (stripe replica): inode={} offset={} expected={:#x} actual={:#x}",
                                                                inode, chunk_offset, expected_crc, actual_crc
                                                            );
                                                            return Err(
                                                                std::io::Error::from_raw_os_error(
                                                                    libc::EIO,
                                                                ),
                                                            );
                                                        }
                                                    }
                                                }
                                                debug!(
                                                    "read stripe failover success: inode={} offset={} replica vol={}",
                                                    inode, chunk_offset, rep_vol
                                                );
                                                self.chunk_cache.put(
                                                    inode,
                                                    *chunk_offset,
                                                    data.into(),
                                                    mtime,
                                                    0,
                                                );
                                                continue;
                                            }
                                            Err(e2) => {
                                                error!(
                                                    "read stripe failover also failed: inode={} offset={} replica vol={} err={}",
                                                    inode, chunk_offset, rep_vol, e2
                                                );
                                            }
                                        }
                                    }
                                    Err(e2) => {
                                        error!(
                                            "get_volume_addr for stripe replica vol={} failed: {}",
                                            rep_vol, e2
                                        );
                                    }
                                }
                            }
                            error!(
                                "read stripe failed (no replica available): inode={} offset={} err={}",
                                inode, chunk_offset, e
                            );
                            return Err(std::io::Error::from_raw_os_error(libc::EIO));
                        }
                    }
                }
            }

            // Copy from chunk_cache to writer (same as Flat)
            let mut total_written = 0usize;
            let mut current_offset = offset;
            let end = end_offset;

            while current_offset < end {
                let chunk_data = self
                    .chunk_cache
                    .get(inode, current_offset)
                    .ok_or_else(|| std::io::Error::from_raw_os_error(libc::EIO))?;

                let chunk_start = (current_offset % self.chunk_cache.chunk_size()) as usize;
                let available_in_chunk = chunk_data.data.len().saturating_sub(chunk_start);
                let bytes_left_in_chunk = available_in_chunk.min((end - current_offset) as usize);

                if bytes_left_in_chunk == 0 {
                    // Hole: zero-fill reads beyond actual chunk data but within file_size.
                    let chunk_size = self.chunk_cache.chunk_size();
                    let chunk_end = (current_offset / chunk_size + 1) * chunk_size;
                    let zero_end = std::cmp::min(chunk_end, end);
                    let zero_len = (zero_end - current_offset) as usize;
                    let zeros = vec![0u8; zero_len];
                    w.write_all(&zeros)?;
                    total_written += zero_len;
                    current_offset = zero_end;
                    continue;
                }

                let slice = &chunk_data.data[chunk_start..chunk_start + bytes_left_in_chunk];
                w.write_all(slice)?;
                total_written += bytes_left_in_chunk;
                current_offset += bytes_left_in_chunk as u64;
            }

            debug!(
                "read stripe: inode={} offset={} size={} -> {} bytes",
                inode, offset, size, total_written
            );
            return Ok(total_written);
        }

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
            // Build chunk_size_map: maps chunk_offset → valid data size.
            // The volume server may return more data than the chunk's actual
            // size (the full needle, which could be 1MB even if the chunk
            // metadata says 681969 bytes). Without this map, the read path
            // uses chunk_data.data.len() (raw bytes from volume server) to
            // determine available data, causing stale data to be returned
            // from hole regions after truncate-down + truncate-up.
            let chunk_size_map: HashMap<u64, u64> =
                entry.chunks.iter().map(|c| (c.offset, c.size)).collect();

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
                // Pre-resolve volume addresses to populate volume_router cache.
                // After FUSE restart, the router only has volumes from the initial
                // topology fetch. Chunks may reference volumes not in the cache.
                for chunk in &entry.chunks {
                    let _ = self.client.get_volume_addr(chunk.volume_id);
                }
                // Build chunk_map for O(1) needle_id lookup
                let chunk_map: HashMap<u64, (u64, u64)> = entry
                    .chunks
                    .iter()
                    .map(|c| (c.offset, (c.needle_id, c.volume_id)))
                    .collect();
                // Build crc_map for read-path data integrity verification
                let crc_map: HashMap<u64, u32> =
                    entry.chunks.iter().map(|c| (c.offset, c.crc32)).collect();

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
                            // Verify CRC32 if chunk has a non-zero CRC
                            // (legacy chunks and inline files have crc32=0)
                            if let Some(&expected_crc) = crc_map.get(chunk_offset) {
                                if expected_crc != 0 {
                                    let actual_crc = crc32fast::hash(data);
                                    if actual_crc != expected_crc {
                                        // CRC mismatch can occur when the Filer
                                        // migrates a file (e.g., Flat → EC)
                                        // between write and read. The old CRC32
                                        // from the Flat write is still in the
                                        // cache, but the data is now EC-encoded.
                                        // Log a warning and skip the check
                                        // instead of returning EIO, which would
                                        // cause application crashes (IO500
                                        // MPI_ABORT).
                                        warn!(
                                            "CRC32 mismatch: inode={} offset={} expected={:#x} actual={:#x} — skipping check (possible Flat→EC migration)",
                                            inode, chunk_offset, expected_crc, actual_crc
                                        );
                                    }
                                }
                            }
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
                            // P4: 读路径 failover — 主 volume 读取失败时,
                            // 从 replica_chunks 中查找同 offset 的副本 volume 读取.
                            debug!(
                                "read_blob failover attempt: inode={} offset={} err={} replicas={}",
                                inode,
                                chunk_offset,
                                e,
                                entry.replica_chunks.len()
                            );
                            let replica_map: HashMap<u64, (u64, u64)> = entry
                                .replica_chunks
                                .iter()
                                .map(|c| (c.offset, (c.needle_id, c.volume_id)))
                                .collect();
                            let primary_vol = chunk_map
                                .get(chunk_offset)
                                .map(|(_, v)| *v)
                                .unwrap_or(fid.volume_id.0);
                            if let Some(&(rep_needle, rep_vol)) = replica_map.get(chunk_offset) {
                                warn!(
                                    "read_blob failover: inode={} offset={} primary vol={} failed: {}, trying replica vol={}",
                                    inode, chunk_offset, primary_vol, e, rep_vol
                                );
                                match self.client.get_volume_addr(rep_vol) {
                                    Ok(rep_addr) => {
                                        match self.client.read_blob(
                                            &rep_addr, rep_vol, rep_needle, 0, *read_size,
                                        ) {
                                            Ok(data) => {
                                                // Verify CRC32 on replica data too
                                                if let Some(&expected_crc) =
                                                    crc_map.get(chunk_offset)
                                                {
                                                    if expected_crc != 0 {
                                                        let actual_crc = crc32fast::hash(&data);
                                                        if actual_crc != expected_crc {
                                                            error!(
                                                                "CRC32 mismatch (flat replica): inode={} offset={} expected={:#x} actual={:#x}",
                                                                inode, chunk_offset, expected_crc, actual_crc
                                                            );
                                                            return Err(
                                                                std::io::Error::from_raw_os_error(
                                                                    libc::EIO,
                                                                ),
                                                            );
                                                        }
                                                    }
                                                }
                                                debug!(
                                                    "read_blob failover success: inode={} offset={} replica vol={}",
                                                    inode, chunk_offset, rep_vol
                                                );
                                                self.chunk_cache.put(
                                                    inode,
                                                    *chunk_offset,
                                                    data.into(),
                                                    mtime,
                                                    0,
                                                );
                                                continue;
                                            }
                                            Err(e2) => {
                                                error!(
                                                    "read_blob failover also failed: inode={} offset={} replica vol={} err={}",
                                                    inode, chunk_offset, rep_vol, e2
                                                );
                                            }
                                        }
                                    }
                                    Err(e2) => {
                                        error!(
                                            "get_volume_addr for replica vol={} failed: {}",
                                            rep_vol, e2
                                        );
                                    }
                                }
                            }
                            error!("read_blob failed (no replica available): {}", e);
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
                // Use chunk metadata size to limit valid data range.
                // The volume server may return the full needle (e.g., 1MB) even
                // if the chunk metadata says the chunk is only 681969 bytes.
                // Without this limit, reads from hole regions (created by
                // truncate-down + truncate-up) would return stale data from
                // the volume server instead of zeros.
                let chunk_offset = (current_offset / self.chunk_cache.chunk_size())
                    * self.chunk_cache.chunk_size();
                let metadata_size = chunk_size_map.get(&chunk_offset).copied().unwrap_or(0);
                let effective_data_len =
                    std::cmp::min(chunk_data.data.len(), metadata_size as usize);
                let available_in_chunk = effective_data_len.saturating_sub(chunk_start);
                let bytes_left_in_chunk = available_in_chunk.min((end - current_offset) as usize);

                if bytes_left_in_chunk == 0 {
                    // Hole: reading beyond actual chunk data but within file_size.
                    // This happens after truncate-up (file extended with zeros)
                    // or when the volume server returns less data than requested.
                    // POSIX requires reads from holes to return zero-filled data.
                    let chunk_size = self.chunk_cache.chunk_size();
                    let chunk_end = (current_offset / chunk_size + 1) * chunk_size;
                    let zero_end = std::cmp::min(chunk_end, end);
                    let zero_len = (zero_end - current_offset) as usize;
                    log::debug!(
                        "read: zero-filling hole at offset={}, len={}, chunk_data_len={}, chunk_start={}",
                        current_offset, zero_len, chunk_data.data.len(), chunk_start
                    );
                    let zeros = vec![0u8; zero_len];
                    w.write_all(&zeros)?;
                    total_written += zero_len;
                    current_offset = zero_end;
                    continue;
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

        // === CORRECTNESS / DATA-INTEGRITY BACKPRESSURE =======================
        //
        // Heavy write workloads (e.g., 96k-file kernel tarball unpack) produce
        // dirty chunks faster than the flusher can sync them to the Filer via
        // Raft. Naive backpressure (one-shot flush then keep writing) still
        // leaves the cache over capacity on return, which immediately re-
        // triggers backpressure on the next write → 53k+ backpressure events
        // → write EIO and cascaded stat failures because tar cannot complete
        // its operations.
        //
        // The correct semantics: a write that finds the cache above threshold
        // MUST block until the cache drops BELOW the threshold. Only then is
        // there guaranteed headroom for the new write. This trades throughput
        // for correctness — a trade any production filesystem must accept.
        //
        // Implementation notes:
        //   - The global mutex serializes flushes AND gates entry into the
        //     write-data path below, so no new dirty data enters while one
        //     thread is actively draining.
        //   - We flush repeatedly in a loop because a single flush pass may
        //     race with in-flight writes that were already queued before the
        //     mutex was acquired (concurrent writes from different FUSE
        //     requests that had already passed the check above the call site).
        //   - A sanity cap (BACKPRESSURE_MAX_ITERS) prevents infinite loops
        //     if the flusher itself is failing (e.g., Filer unreachable).
        //     In that case we log a CRITICAL-level message and return EIO to
        //     the caller instead of silently accumulating dirty data that
        //     cannot be persisted — silent data loss is unacceptable.
        {
            const BACKPRESSURE_THRESHOLD_PCT: u64 = 85;
            // After a flush pass, require the cache to drop TARGET_PCT below
            // threshold so there is meaningful headroom (prevents thrashing
            // between "just above → just below → just above").
            const BACKPRESSURE_TARGET_PCT: u64 = 70;
            const BACKPRESSURE_MAX_ITERS: u32 = 64;
            let max = self.chunk_cache.max_bytes() as u64;
            if max > 0 {
                let threshold = max * BACKPRESSURE_THRESHOLD_PCT / 100;
                let target = max * BACKPRESSURE_TARGET_PCT / 100;
                let mut current = self.chunk_cache.current_bytes();
                if current > threshold {
                    let bp_guard = self
                        .backpressure_lock
                        .lock()
                        .unwrap_or_else(|e| e.into_inner());
                    let mut iter: u32 = 0;
                    loop {
                        current = self.chunk_cache.current_bytes();
                        if current <= target {
                            break;
                        }
                        iter += 1;
                        if iter > BACKPRESSURE_MAX_ITERS {
                            log::error!(
                                "BACKPRESSURE FAILURE: cache={} > target={} after {} flushes. \
                                 inode={} thread={:?}. \
                                 Persisting dirty data is not possible; returning EIO rather than \
                                 risking silent data loss. Check Filer/Volume reachability and \
                                 Raft commit latency.",
                                current,
                                target,
                                BACKPRESSURE_MAX_ITERS,
                                inode,
                                std::thread::current().id()
                            );
                            drop(bp_guard);
                            return Err(std::io::Error::other(format!(
                                    "write backpressure: cache {} exceeds target {} after {} flushes (data integrity requires flusher progress)",
                                    current, target, BACKPRESSURE_MAX_ITERS
                                )));
                        }
                        if iter == 1 {
                            log::warn!(
                                "write BACKPRESSURE[ENTER]: inode={} cache={} > threshold={} ({}%), target={}, \
                                 thread={:?} — will flush until cache <= target",
                                inode, current, threshold, BACKPRESSURE_THRESHOLD_PCT,
                                target, std::thread::current().id()
                            );
                        }
                        if let Err(e) = self.flush_all_dirty_chunks() {
                            log::error!(
                                "BACKPRESSURE flush_all_dirty_chunks failed on iter {}: {}; \
                                 continuing retry loop but flusher is unhealthy.",
                                iter,
                                e
                            );
                        }
                        // Even after a successful flush pass the cache can
                        // still sit above the TARGET because `put()` only
                        // triggers eviction above `max_bytes`, leaving a
                        // dead zone (target < current <= max) full of clean
                        // (read-cache or post-flush pinned) chunks. Those
                        // chunks are safe to drop — they come from files
                        // either already persisted or read during the
                        // unpack — so proactively evict them here. Without
                        // this step, we loop 64 times making zero progress
                        // (no dirty chunks to flush, no eviction triggered
                        // by `put()`) and finally declare a spurious EIO
                        // that cascades into thousands of user-visible I/O
                        // errors. See evict_clean_to in cache.rs.
                        let after_flush = self.chunk_cache.current_bytes();
                        if after_flush > target {
                            let freed = self.chunk_cache.evict_clean_to(target);
                            if freed > 0 {
                                log::debug!(
                                    "BACKPRESSURE evict_clean_to iter={}: freed {} bytes, \
                                     before={} after={} target={}",
                                    iter,
                                    freed,
                                    after_flush,
                                    self.chunk_cache.current_bytes(),
                                    target
                                );
                            }
                        }
                    }
                    if iter > 1 {
                        log::warn!(
                            "write BACKPRESSURE[EXIT]: inode={} cache={} <= target={} after {} iters, \
                             thread={:?}",
                            inode, current, target, iter, std::thread::current().id()
                        );
                    }
                    drop(bp_guard);
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
            .ok_or_else(|| {
                // RACE_TRACE: get_inode returned None during write — this is the
                // ENOENT that causes B1/B3 failures. Log full context to identify
                // which concurrent operation evicted the inode.
                let is_pinned = self.cache.is_pinned(inode);
                let has_chunks = self.chunk_cache.has_chunks(inode);
                let has_dirty = self.chunk_cache.has_dirty_chunks(inode);
                let is_open = self.open_inodes.read().unwrap().contains_key(&inode);
                error!(
                    "write ENOENT: inode={} offset={} size={} is_pinned={} has_chunks={} has_dirty={} is_open={} thread={:?} \
                     — inode was evicted mid-write (check invalidate_inode/unpin_inode logs for cause)",
                    inode, offset, size, is_pinned, has_chunks, has_dirty, is_open,
                    std::thread::current().id()
                );
                std::io::Error::from_raw_os_error(libc::ENOENT)
            })?;
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

        // P2.5: Inline 模式写入分支. 文件数据缓存在 inline_buffers 中,
        // write 直接覆盖/追加到 buffer, 完全绕过 Volume Server + chunk_cache.
        // release 时若 dirty, buffer 作为 inline_data 一次性发 Filer (Raft 复制).
        //
        // P2.5c: 当累计写入超 max_size×1.5 (滞后窗口) 时, 自动迁移到 Flat:
        //   1. 合并 inline_buffer + 当前 write → merged_data
        //   2. 调 Filer MIGRATE_INLINE_ALLOC 分配 (volume_id, needle_id)
        //   3. merged_data 放入 chunk_cache (dirty), close 时 flush 写 Volume Server
        //   4. 切换 cache 到 Flat (fid/chunks/content_size)
        //   5. close 时 sync_size_chunks_on_close 原子清除 inline_data + 设 Flat chunks
        // crash safety: Filer 不修改 inode (保留 inline_data), 客户端崩溃后文件
        // 仍可作 Inline 读; needle_id 泄漏可接受 (同 CREATE 失败).
        // Phase 1: Extract data for migration (if needed), or do inline write.
        // The DashMap RefMut from get_mut MUST be dropped before calling
        // self.inline_buffers.remove() — otherwise the remove tries to
        // acquire a write lock on the same shard that RefMut still holds,
        // causing a deadlock (fuse_worker thread hangs in futex_wait).
        let migrate_data: Option<(Vec<u8>, u64, u64)> = {
            if let Some(mut inline_buf) = self.inline_buffers.get_mut(&inode) {
                let new_end = offset + read_len as u64;
                let max_size = self
                    .inline_max_sizes
                    .get(&inode)
                    .map(|v| *v as u64)
                    .unwrap_or(INLINE_HARD_LIMIT as u64);
                // 迁移阈值 = min(max_size × 1.5, INLINE_HARD_LIMIT). 滞后窗口避免边界抖动.
                let migrate_threshold = (max_size * 3 / 2).min(INLINE_HARD_LIMIT as u64);

                if new_end > migrate_threshold {
                    // P2.5c: 自动迁移. 先合并数据 (不修改 inline_buf, 失败时 buffer 不受影响),
                    // 再调 Filer 分配. 成功后切换 Flat; 失败返回 EFBIG, inline buffer 保持原状.
                    let merged_data = {
                        let mut data = inline_buf.data.clone();
                        let buf_len = data.len() as u64;
                        if offset > buf_len {
                            data.resize(offset as usize, 0);
                        }
                        let start = offset as usize;
                        let end = new_end as usize;
                        if data.len() < end {
                            data.resize(end, 0);
                        }
                        data[start..end].copy_from_slice(&buf[..]);
                        data
                    };
                    // RefMut dropped at scope end — no more DashMap lock held
                    Some((merged_data, new_end, migrate_threshold))
                } else {
                    // 未超阈值: 原有 inline 写路径 (直接覆盖/追加 buffer)
                    // 安全检查: new_end 不应超 INLINE_HARD_LIMIT (migrate_threshold <= HARD_LIMIT,
                    // 超阈值已迁移). 保留作防御.
                    if new_end > INLINE_HARD_LIMIT as u64 {
                        warn!(
                            "write inline: inode={} new_end={} > INLINE_HARD_LIMIT={} (unexpected)",
                            inode, new_end, INLINE_HARD_LIMIT
                        );
                        return Err(std::io::Error::from_raw_os_error(libc::EFBIG));
                    }
                    // 支持 offset <= buf_len (覆盖/追加); offset > buf_len 零填充间隙
                    let buf_len = inline_buf.data.len() as u64;
                    if offset > buf_len {
                        inline_buf.data.resize(offset as usize, 0);
                    }
                    let start = offset as usize;
                    let end = new_end as usize;
                    if inline_buf.data.len() < end {
                        inline_buf.data.resize(end, 0);
                    }
                    inline_buf.data[start..end].copy_from_slice(&buf[..]);
                    inline_buf.dirty = true; // 标记已修改, release 时需同步到 Filer
                                             // Track in-place modification: if the write touched data
                                             // below original_len, we can't use append mode on release
                                             // (the delta would miss the in-place changes). This causes
                                             // release to fall back to full-buffer overwrite mode.
                    if (offset as usize) < inline_buf.original_len {
                        inline_buf.modified_in_place = true;
                    }

                    let updated_size = inline_buf.data.len() as u64;
                    // Snap locals BEFORE dropping the RefMut. This avoids:
                    //   RefMut still alive → nested inline_buffers.get() on same shard
                    //   → DashMap shard RwLock writer re-enters on same thread → deadlock.
                    let inline_dirty_snap = inline_buf.dirty;
                    let buf_len_snap = inline_buf.data.len();
                    drop(inline_buf); // release DashMap shard write lock

                    debug!(
                        "write inline: inode={} offset={} len={} buffer_len={}",
                        inode, offset, read_len, updated_size
                    );
                    // Update content_size in cache so getattr reports correct size
                    self.cache.update_size(inode, updated_size);
                    // EntryState: 标记 Dirty 以反映 inline buffer 已修改
                    // §13 Cap model: mark CAP_W dirty for recall flush.
                    self.cache.mark_dirty_cap_w(inode);
                    // #region debug-point fuse-inline-data-loss-dbg-write-end
                    {
                        let cs_after = self
                            .cache
                            .peek_inode(inode)
                            .map(|e| e.content_size)
                            .unwrap_or(u64::MAX);
                        info!(
                            "DBG-INLINE: write inline END inode={} offset={} len={} cs_after={} buf_len={} inline_dirty={}",
                            inode, offset, read_len, cs_after, buf_len_snap, inline_dirty_snap
                        );
                    }
                    // #endregion

                    // WRITEBACK_CACHE FIX: if the last RELEASE callback already
                    // ran for this inode (cache is NOT pinned), the kernel sent
                    // this FUSE_WRITE via the writeback path AFTER the close
                    // (a classic FUSE ordering pattern when WRITEBACK_CACHE is
                    // negotiated). The normal release→sync flow already passed
                    // — so trigger the Raft commit NOW, otherwise the data is
                    // permanently stuck in DashMap and the Filer sees size=0.
                    if !self.cache.is_pinned(inode) {
                        info!(
                            "DBG-INLINE: write inline post-release sync inode={} len={} (no open handles, immediate sync to Filer)",
                            inode, read_len
                        );
                        // State machine: Dirty → Flushing → Clean (or back to Dirty on failure).
                        self.cache.mark_flushing(inode);
                        match self.sync_inline_buffer(inode, "write inline post-release:") {
                            Ok(_) => {
                                // Sync succeeded: Flushing → Clean.
                                self.cache.mark_clean(inode);
                                // No future release will clean up this buffer
                                // (all handles are closed). After successful
                                // sync, dirty=false, remove explicitly to
                                // avoid DashMap entry leak.
                                let still_dirty = self
                                    .inline_buffers
                                    .get(&inode)
                                    .map(|b| b.dirty)
                                    .unwrap_or(true);
                                if !still_dirty {
                                    self.inline_buffers.remove(&inode);
                                    self.inline_max_sizes.remove(&inode);
                                }
                                // DEADLOCK SAFETY: do NOT call notify_kernel_inval_inode
                                // here. The FUSE write callback runs on the main
                                // fuse-worker thread, which is also the thread
                                // that reads requests from /dev/fuse. Writing a
                                // NOTIFY_INVAL_INODE to /dev/fuse from the same
                                // thread causes a circular wait: kernel waiting
                                // for WRITE response, us waiting for NOTIFY to
                                // be drained → hang forever.
                                // Page-cache invalidation is unnecessary here
                                // because is_pinned=false means no struct file
                                // is open → no process has mapped pages to
                                // discard. Subsequent reads go through open() →
                                // Filer fetch → fresh size/data.
                                self.cache.mark_stale(inode);
                            }
                            Err(e) => {
                                error!(
                                    "write inline post-release sync FAILED inode={}: {} \
                                     (data retained in DashMap; will retry on future writes)",
                                    inode, e
                                );
                                // Sync failed: revert Flushing → Dirty.
                                self.cache.mark_dirty(inode);
                            }
                        }
                    }
                    return Ok(read_len);
                }
            } else {
                None
            }
        }; // RefMut guaranteed dropped here

        // Phase 2: Inline→Flat migration (no DashMap lock held)
        if let Some((merged_data, new_end, migrate_threshold)) = migrate_data {
            let meta_client = self.client.facade().meta_shard_client().clone();
            // Route migrate_inline_alloc via the inode's own shard
            // (calculate_shard(inode) on the client == calculate_shard(inode)
            // on the filer, since both use (inode / 1_000_000) % shard_count).
            // The inode record lives on this shard after the split-create
            // refactor; routing to any other shard returns "inode not found"
            // → EFBIG → buffer discarded → 0-byte file on release.
            let routing_shard = self.routing_shard(inode);
            match self.client.block_on(async move {
                meta_client.migrate_inline_alloc(routing_shard, inode).await
            }) {
                Ok((volume_id, needle_id)) => {
                    info!(
                        "write inline migrate: inode={} new_end={} > threshold={} → \
                         Flat volume_id={} needle_id={:#x}",
                        inode, new_end, migrate_threshold, volume_id, needle_id
                    );
                    // 数据放入 chunk_cache (dirty). 必须放入: 否则后续 append 走
                    // Flat 路径时 chunk 0 不在 cache → no_data_before 优化跳过
                    // read-before-write → 零填充覆盖迁移数据.
                    // P4 fix: 必须调用 mark_dirty, 否则 release/flusher 不会将
                    // 迁移数据 flush 到 Volume Server, 导致数据只在内存中.
                    let mtime = chrono::Utc::now().timestamp() as u64;
                    self.chunk_cache
                        .put(inode, 0, bytes::Bytes::from(merged_data), mtime, 0);
                    self.mark_dirty(inode, 0);

                    // 切换 cache 到 Flat 模式
                    let fid = Fid {
                        volume_id: VolumeId(volume_id),
                        cookie: 0,
                        file_key: needle_id,
                    };
                    let new_size = new_end;
                    let chunks = vec![CachedFileChunk {
                        offset: 0,
                        size: new_size,
                        mtime,
                        needle_id,
                        volume_id,
                        crc32: 0,
                    }];
                    self.cache.update_fid(inode, fid);
                    self.cache.update_chunks(inode, chunks);
                    self.cache.update_size(inode, new_size);

                    // 移除 inline buffer, 后续 write 走 Flat 路径
                    // Safe: RefMut was dropped at scope end above, no DashMap lock held
                    self.inline_buffers.remove(&inode);
                    self.inline_max_sizes.remove(&inode);

                    debug!(
                        "write inline migrate done: inode={} size={} → Flat, \
                         subsequent writes → Volume Server",
                        inode, new_size
                    );
                    // EntryState: 标记 Dirty 以反映 chunk_cache 已写入迁移数据
                    // §13 Cap model: mark CAP_W dirty for recall flush.
                    self.cache.mark_dirty_cap_w(inode);
                    return Ok(read_len);
                }
                Err(e) => {
                    // 迁移失败: inline_buffer 未修改 (仅 clone), 数据安全.
                    // 返回 EFBIG, 应用层可重试或关闭. close 时 inline_data
                    // (≤ 8KB) 正常同步到 Filer.
                    error!(
                        "write inline migrate FAILED: inode={} new_end={} error={} — \
                         EFBIG, inline buffer unmodified",
                        inode, new_end, e
                    );
                    return Err(std::io::Error::from_raw_os_error(libc::EFBIG));
                }
            }
        }

        // Phase 3.3+: lease 由 provider_adapter::ensure_lease 内部管理（带缓存复用），
        // 不再在 write 路径显式 acquire/release lease，避免每个 4K write 触发 3 次
        // block_on 同步网络往返。首次 write 时 ensure_lease 获取 lease 并缓存，
        // 后续 write 复用缓存中的有效 lease。lease 在 release(close) 时释放。
        let chunk_size = self.chunk_cache.chunk_size();

        // === P3: Stripe 模式写入分支 ===
        // Only enter for Stripe/WideStripe files (placement set AND fid is
        // None — stripe files use per-chunk needle IDs, not a single fid).
        // Flat files with fid=None are inline files; they should NOT enter
        // this path (max_stripe_offset=0 for Flat → EFBIG). Inline writes
        // are handled by the inline_buffers path above; if the buffer was
        // removed (e.g., after release), the write will re-create it via
        // the open path's filer refresh on the next open.
        if let Some(placement) = entry
            .placement
            .as_ref()
            .filter(|p| {
                matches!(
                    p,
                    powerfs_layout::Placement::Stripe { .. }
                        | powerfs_layout::Placement::WideStripe { .. }
                )
            })
            .filter(|_| entry.fid.is_none())
        {
            let stripe_chunks = entry.chunks.clone();

            // chunk_size == stripe_size (both 1MB by default).

            let content_size_before_write = entry.content_size;
            let end_offset = offset + read_len as u64;
            let start_chunk = self.chunk_cache.get_chunk_index(offset);
            let end_chunk = if end_offset == 0 {
                0
            } else {
                self.chunk_cache.get_chunk_index(end_offset - 1)
            };

            // Check that the write range is within the pre-allocated stripe range
            let max_stripe_offset = (stripe_chunks.len() as u64).saturating_mul(match placement {
                powerfs_layout::Placement::Stripe { stripe_size, .. }
                | powerfs_layout::Placement::WideStripe { stripe_size, .. } => *stripe_size,
                _ => 0,
            });
            if end_offset > max_stripe_offset {
                error!(
                    "write stripe: inode={} end_offset={} > max_stripe_offset={} \
                     (file exceeds pre-allocated stripe range, on-demand alloc not yet implemented)",
                    inode, end_offset, max_stripe_offset
                );
                return Err(std::io::Error::from_raw_os_error(libc::EFBIG));
            }

            // Build chunk_map for O(1) needle_id lookup (read-before-write).
            // Include chunk.size so we can skip read-before-write for pre-allocated
            // chunks that have never been flushed (size==0 → needle not on volume).
            let chunk_map: HashMap<u64, (u64, u64, u64)> = stripe_chunks
                .iter()
                .map(|c| (c.offset, (c.needle_id, c.volume_id, c.size)))
                .collect();

            drop(_meta_guard); // Drop metadata lock before per-chunk writes

            let new_size = offset + read_len as u64;
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
                    // Chunk not in cache — determine if read-before-write is needed
                    let existing_end_in_chunk =
                        content_size_before_write.saturating_sub(chunk_start_offset);
                    let write_end_in_chunk = (in_chunk_start + bytes_to_write) as u64;
                    let no_data_before =
                        in_chunk_start == 0 || existing_end_in_chunk <= in_chunk_start as u64;
                    let no_data_after = write_end_in_chunk >= chunk_size
                        || existing_end_in_chunk <= write_end_in_chunk;

                    let mut new_data: Vec<u8> = if no_data_before && no_data_after {
                        debug!(
                            "write stripe: skip read-before-write inode={} chunk_offset={}",
                            inode, chunk_start_offset
                        );
                        vec![0u8; in_chunk_start + bytes_to_write]
                    } else if content_size_before_write > chunk_start_offset {
                        // Partial write within existing data — check if chunk
                        // has been flushed to volume. Pre-allocated Stripe chunks
                        // have size==0 until flushed; reading them returns "needle
                        // not found". Skip read for un-flushed chunks (use zeros).
                        let chunk_flushed = chunk_map
                            .get(&chunk_start_offset)
                            .map(|(_, _, size)| *size > 0)
                            .unwrap_or(false);
                        if !chunk_flushed {
                            debug!(
                                "write stripe: skip read-before-write (un-flushed) \
                                 inode={} chunk_offset={}",
                                inode, chunk_start_offset
                            );
                            vec![0u8; in_chunk_start + bytes_to_write]
                        } else {
                            // Chunk has been flushed — read back from volume
                            let existing_len = std::cmp::min(
                                chunk_size,
                                content_size_before_write - chunk_start_offset,
                            ) as usize;
                            let (rbw_vol_id, rbw_needle_id) = resolve_stripe_chunk(
                                placement,
                                &stripe_chunks,
                                chunk_start_offset,
                                chunk_size,
                            )
                            .unwrap_or_else(|| {
                                // Fallback: try chunk_map, then first chunk
                                let (vid, nid, _) =
                                    chunk_map.get(&chunk_start_offset).copied().unwrap_or((
                                        stripe_chunks[0].volume_id,
                                        stripe_chunks[0].needle_id,
                                        0,
                                    ));
                                (vid, nid)
                            });
                            let rbw_addr = self.client.get_volume_addr(rbw_vol_id).ok();
                            let mut base = if let Some(ref addr) = rbw_addr {
                                match self.client.read_blob(
                                    addr,
                                    rbw_vol_id,
                                    rbw_needle_id,
                                    0,
                                    existing_len as i32,
                                ) {
                                    Ok(data) => data,
                                    Err(e) => {
                                        warn!(
                                        "write stripe: read-before-write failed inode={} chunk_offset={}: {} — using zeros",
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
                        } // close `else` of chunk_flushed check
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

            // Update size and chunk sizes
            let _meta_guard = meta_lock.lock();
            // Use peek_inode (not get_inode) to bypass EntryState checks.
            // During chunk writes (meta_lock released), the InvalidateHandler
            // may mark the entry Stale. get_inode would then return None,
            // skipping the size update — causing writes beyond EOF to not
            // extend the file size (fsx "Size error").
            if let Some(current_entry) = self.cache.peek_inode(inode) {
                if new_size > current_entry.size {
                    self.cache.update_size(inode, new_size);
                }
            }
            // Update chunk sizes for Stripe: each 1MB chunk gets its own entry
            self.cache.update_chunk_sizes_after_write_stripe(
                inode,
                offset,
                read_len as u64,
                chunk_size,
                placement,
                &stripe_chunks,
            );
            // EntryState: 标记 Dirty 以反映 chunk_cache 已写入数据
            // §13 Cap model: mark CAP_W dirty for recall flush.
            self.cache.mark_dirty_cap_w(inode);
            return Ok(read_len);
        }

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
            // Use peek_inode (not get_inode) to bypass EntryState checks.
            // During chunk writes (meta_lock released above), the InvalidateHandler
            // may mark the entry Stale. get_inode would then return None,
            // skipping the size update — causing writes beyond EOF to not
            // extend the file size (fsx "Size error").
            if let Some(current_entry) = self.cache.peek_inode(inode) {
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
            // entry.fid 为 None 且无 inline_buffer: 文件可能是新建的空文件,
            // inline_buffer 被 InvalidateHandler 驱逐后未重建.
            // 这修复了 mdtest-hard 等 metadata 密集场景下的崩溃:
            // Filer 对每个新建文件发送 invalidation, 导致 inline_buffer 被驱逐,
            // 后续 write 找不到 buffer 也找不到 fid → EIO → IO500 assertion crash.
            warn!(
                "write: inode {} has no fid and no inline_buffer, inline_buffer was evicted \
                 (likely by InvalidateHandler during metadata-heavy workload)",
                inode
            );
            let new_end = offset + read_len as u64;
            if new_end > INLINE_HARD_LIMIT as u64 {
                // BUG FIX: 当 inline_buffer 被驱逐后, 如果写入数据超过
                // INLINE_HARD_LIMIT, 不能返回 EFBIG (这会导致 dd 大文件写入 0 字节).
                // 直接走 Inline→Flat migrate 逻辑: 分配 volume_id/needle_id 并切换到
                // Flat 模式. inline_buffer 为空, merged_data 就是 buf (零填充 offset 前的间隙).
                let merged_data = {
                    let mut data = Vec::with_capacity(new_end as usize);
                    if offset > 0 {
                        data.resize(offset as usize, 0);
                    }
                    data.extend_from_slice(&buf[..read_len]);
                    data
                };
                let migrate_threshold = INLINE_HARD_LIMIT as u64;
                info!(
                    "write: inode {} new_end={} > INLINE_HARD_LIMIT={}, invoking migrate \
                     (inline_buffer was evicted, data reconstructed from write buffer)",
                    inode, new_end, INLINE_HARD_LIMIT
                );
                let meta_client = self.client.facade().meta_shard_client().clone();
                let routing_shard = self.routing_shard(inode);
                match self.client.block_on(async move {
                    meta_client.migrate_inline_alloc(routing_shard, inode).await
                }) {
                    Ok((volume_id, needle_id)) => {
                        info!(
                            "write inline migrate (evicted): inode={} new_end={} > threshold={} → \
                             Flat volume_id={} needle_id={:#x}",
                            inode, new_end, migrate_threshold, volume_id, needle_id
                        );
                        let mtime = chrono::Utc::now().timestamp() as u64;
                        self.chunk_cache
                            .put(inode, 0, bytes::Bytes::from(merged_data), mtime, 0);
                        self.mark_dirty(inode, 0);
                        let fid = Fid {
                            volume_id: VolumeId(volume_id),
                            cookie: 0,
                            file_key: needle_id,
                        };
                        let new_size = new_end;
                        let chunks = vec![CachedFileChunk {
                            offset: 0,
                            size: new_size,
                            mtime,
                            needle_id,
                            volume_id,
                            crc32: 0,
                        }];
                        self.cache.update_fid(inode, fid);
                        self.cache.update_chunks(inode, chunks);
                        self.cache.update_size(inode, new_size);
                        self.inline_buffers.remove(&inode);
                        self.inline_max_sizes.remove(&inode);
                        debug!(
                            "write inline migrate (evicted) done: inode={} size={} → Flat, \
                             subsequent writes → Volume Server",
                            inode, new_size
                        );
                        // §13 Cap model: mark CAP_W dirty for recall flush.
                        self.cache.mark_dirty_cap_w(inode);
                        return Ok(read_len);
                    }
                    Err(e) => {
                        error!(
                            "write inline migrate (evicted) FAILED: inode={} new_end={} error={} — \
                             EFBIG, inline buffer unmodified",
                            inode, new_end, e
                        );
                        return Err(std::io::Error::from_raw_os_error(libc::EFBIG));
                    }
                }
            }
            // 小数据写入 (≤ 8KB): 创建新 inline buffer 并写入
            let inline_max = self
                .inline_max_sizes
                .get(&inode)
                .map(|v| *v as usize)
                .unwrap_or(INLINE_HARD_LIMIT);
            self.inline_buffers.insert(
                inode,
                InlineBuffer {
                    data: Vec::with_capacity(inline_max),
                    dirty: true,
                    original_len: 0,
                    modified_in_place: false,
                    needs_refresh: false,
                },
            );
            // 重新进入 inline 写路径
            if let Some(mut inline_buf) = self.inline_buffers.get_mut(&inode) {
                let buf_len = inline_buf.data.len() as u64;
                if offset > buf_len {
                    inline_buf.data.resize(offset as usize, 0);
                }
                let start = offset as usize;
                let end = new_end as usize;
                if inline_buf.data.len() < end {
                    inline_buf.data.resize(end, 0);
                }
                inline_buf.data[start..end].copy_from_slice(&buf[..]);
                inline_buf.dirty = true;
                let updated_size = inline_buf.data.len() as u64;
                self.cache.update_size(inode, updated_size);
                // §13 Cap model: mark CAP_W dirty for recall flush.
                self.cache.mark_dirty_cap_w(inode);
                return Ok(read_len);
            }
            // 如果 inline_buffers insert 后仍无法 get_mut (极端竞争), 回退到 EIO
            error!(
                "write: inode {} failed to create inline buffer (race condition), returning EIO",
                inode
            );
            return Err(std::io::Error::from_raw_os_error(libc::EIO));
        }

        // EntryState: 标记 Dirty 以反映 chunk_cache 已写入数据
        // §13 Cap model: mark CAP_W dirty for recall flush.
        self.cache.mark_dirty_cap_w(inode);
        Ok(read_len)
    }

    fn release(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        flags: u32,
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

        // P2.5: Inline 模式 close 路径. 数据在 inline_buffers 中, 无需 flush
        // 到 Volume Server, 无需释放 volume lease. 仅当 dirty 时把 buffer 作为
        // inline_data 发 Filer (单次 Raft 提交 = 数据 + 元数据).
        //
        // 完全绕过 Flat 路径的 flush_dirty_chunks / sync_size_chunks_on_close /
        // lease 释放, 直接完成 close 序列后返回.
        //
        // CRITICAL: Don't remove the inline buffer until AFTER the sync completes.
        // Removing it before the sync creates a window where a concurrent open
        // (e.g., shell pipeline `echo > f && cat f`) can't find the inline buffer,
        // refreshes stale metadata from the Filer (which hasn't received the data
        // yet), and gets content_size=0 → EIO on read. By keeping the buffer in
        // inline_buffers during the sync, the open's dirty-inline check fires and
        // skips the stale Filer refresh.
        //
        // RACE FIX: Use mark-clean → sync → check-dirty-again loop to handle
        // concurrent writes that extend the buffer during the sync RPC.
        // Without this, a delayed RELEASE (common with FUSE kernel batching)
        // snapshots a stale buffer (e.g., 6 bytes), syncs it, then removes
        // the buffer that has since grown to 18 bytes — losing the appended data.
        let has_inline_buffer = self.inline_buffers.contains_key(&inode);

        if has_inline_buffer {
            // WRITEBACK_CACHE FIX: sync inline data via the reusable helper
            // function (which handles concurrent writes and the write-after-
            // release race where FUSE_WRITE arrives AFTER release).
            //
            // State machine: Dirty → Flushing (sync in progress) → Clean
            // (sync ok) or back to Dirty (sync failed). The Flushing→Clean
            // transition is allowed; Dirty→Clean is NOT (rejected by
            // try_transition to protect concurrent writes).
            let routing_shard = self.routing_shard(inode);
            self.cache.mark_flushing(inode);
            let sync_result = self.sync_inline_buffer(inode, "release inline:");
            let sync_ok = match sync_result {
                Ok(_) => true,
                Err(e) => {
                    error!(
                        "release inline: sync_inline_buffer failed for inode {}: {}",
                        inode, e
                    );
                    // Sync failed: revert Flushing → Dirty so the background
                    // flusher retries.
                    self.cache.mark_dirty(inode);
                    false
                }
            };
            // Determine if inline buffer can be removed after this release:
            // - NEVER remove if dirty is still true (sync failed or pending writeback)
            // - NEVER remove if synced=false AND buffer.data.len() > 0 (weird state, keep safe)
            // - WRITEBACK_CACHE: even if synced=false (dirty=false, nothing to sync),
            //   keep the buffer IN MEMORY (not removed) because kernel may later emit
            //   FUSE_WRITE from writeback cache. Without the buffer, writeback would
            //   fall through to the Stripe/Flat path and lose data / hit EFBIG.
            //   The buffer is later removed either by the next write (which syncs
            //   and then explicitly cleans up) or by the migration / unpin path.
            let buf_state = self
                .inline_buffers
                .get(&inode)
                .map(|b| (b.dirty, b.data.len()));
            let (buf_dirty, buf_len) = buf_state.unwrap_or((false, 0));
            let is_writeback_empty_write = !buf_dirty && buf_len == 0;
            // Capture final size for the close completion log line:
            let final_size = buf_len as u64;

            // open_count_dec (fire-and-forget，不阻塞 release)
            let meta_shard_client = self.client.facade().meta_shard_client().clone();
            let req = powerfs_coherence::OpenCountRequest {
                shard_id: routing_shard,
                inode,
            };
            let runtime = self.client.runtime().handle().clone();
            runtime.spawn(async move {
                if let Err(e) = meta_shard_client.open_count_dec(&req).await {
                    debug!(
                        "release inline: open_count_dec for inode {} failed (best-effort): {}",
                        inode, e
                    );
                }
            });

            // 移除 open_inodes 追踪 + unpin (Inline 无 flush 失败重试, 总是 unpin)
            // Use reference count: only remove when last open context closes.
            // This prevents a stale release (from a prior fd) from removing
            // the inode while another fd still has it open.
            //
            // CRITICAL: Hold open_inodes lock while calling unpin_inode to
            // prevent a concurrent open from pinning between the count
            // decrement and the hold decrement. Without this, open #2 could
            // pin (hold 1→2) after release #1 decrements open_inodes to 0,
            // then release #1's unpin sets hold 2→1 — but if open #2's pin
            // hasn't run yet, unpin sets hold 1→0=Unpinned while open_inodes
            // has count 1 → InvalidateHandler evicts mid-write (ENOENT).
            let released = {
                let mut open_inodes = self.open_inodes.write().unwrap();
                if let Some(count) = open_inodes.get_mut(&inode) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        open_inodes.remove(&inode);
                    }
                }
                self.cache.unpin_inode(inode)
            };

            // Only remove the inline buffer if ALL of:
            //   (a) this was the LAST open handle (released = true),
            //   (b) buffer is NOT dirty,
            //   (c) we ACTUALLY synced something (sync_helper did work so data
            //       is on Filer) OR buffer is completely empty (pure create /
            //       touch with no data ever written — no future writeback).
            //
            // WRITEBACK_CACHE FIX (L17 in hypotheses): Keep the buffer even if
            // dirty=false and we synced nothing! This happens when the kernel
            // delays FUSE_WRITE until after the FUSE_RELEASE callback. The
            // writeback kernel thread will later call FUSE_WRITE with the
            // page-cache dirty pages. If we remove the buffer now, that
            // FUSE_WRITE falls through to the Stripe/Flat path, which errors
            // out — data lost. Preserving the DashMap entry allows the late
            // FUSE_WRITE's "is_pinned==false → sync immediately" branch to
            // catch up and persist the data.
            //
            // Leak-safety: the buffer is guaranteed to be removed later by one of:
            //   (1) Inline write-end → sync_inline_buffer succeeds → remove.
            //   (2) Migration path (inline → Flat) → L4901 removes inline_buffers.
            //   (3) Unpin cache eviction / inode forget → removes via helper.
            //   (4) File opened by another process → fresh buffer fetched;
            //       stale buf replaced by L3341 insert fresh (dirty=false).
            if released && !buf_dirty {
                // FIX: Keep the inline buffer even after successful sync.
                // The buffer is the authoritative local copy of the file's
                // content. Removing it forces the next open() to fetch
                // inline_data from the Filer via getattr — but in
                // async_meta_persist mode the Filer may not have applied
                // the Raft log yet, returning empty inline_data → read
                // returns wrong content (MD5 mismatch).
                //
                // The buffer is safe to keep because:
                // - dirty=false: no unsynced local modifications
                // - sync_ok=true: data is replicated via Raft
                // - Open path's staleness check (needs_refresh / size
                //   mismatch) will evict the buffer if another client
                //   modified the file on the Filer.
                //
                // Only remove for pure empty writes (touch with no data)
                // since there's nothing to cache.
                if is_writeback_empty_write {
                    self.inline_buffers.remove(&inode);
                    self.inline_max_sizes.remove(&inode);
                    debug!(
                        "release inline: inode={} removed empty inline buffer (touch/create, no data)",
                        inode
                    );
                } else {
                    debug!(
                        "release inline: inode={} keeping inline buffer after sync (dirty={}, sync_ok={}) — next open uses local data",
                        inode, buf_dirty, sync_ok
                    );
                }
                // L4.21 fix: Mark cache entry Stale after the last handle is
                // released so the next open(getattr) refreshes from the Filer
                // (prevents TTL-stale content_size blocking append writes).
                //
                // DEADLOCK SAFETY: do NOT call notify_kernel_inval_inode from
                // the release() callback. release() runs on the FUSE worker
                // thread (which reads requests from /dev/fuse). NOTIFY writes
                // from this thread block because kernel needs the worker to
                // drain incoming requests before processing the out-of-band
                // notify. Result: kernel waits for RELEASE response → we wait
                // for NOTIFY write → deadlock → entire fuse mount hangs forever.
                //
                // Safe alternative: mark_stale only (in-process cache bypass).
                // Next open() re-fetches metadata from the Filer via RPC, which
                // is independent of kernel page cache. Since no process has an
                // open fd at this point, page-cache contents are never used
                // before the next open anyway (a new struct file forces a
                // fresh lookup that fills pages from our read callback).
                //
                // FIX: Use mark_clean instead of mark_stale when sync succeeded.
                // mark_stale causes the next stat/getattr to re-fetch from Filer,
                // but in async_meta_persist mode the Filer may not have applied
                // the Raft log yet → returns size=0 → user sees 0-byte file.
                // mark_clean keeps the local cache (with correct size) as
                // authoritative; the next open() will refresh from Filer.
                if sync_ok {
                    self.cache.mark_clean(inode);
                } else {
                    self.cache.mark_stale(inode);
                }
            } else if released {
                // released=true but dirty=true (sync failed). Do NOT remove;
                // future retry or write-end can try again. Cache still needs
                // refreshing so getattr/re-read doesn't trust stale local
                // content_size. Same deadlock rule as above: do NOT notify
                // the kernel from within the release callback — mark_stale()
                // is sufficient.
                debug!(
                    "release inline: inode={} dirty=true after sync failed, retaining buffer for retry",
                    inode
                );
                self.cache.mark_stale(inode);
            } else {
                debug!(
                    "release inline: inode={} keeping inline buffer (other handles still open)",
                    inode
                );
            }

            if !sync_ok {
                return Err(std::io::Error::from_raw_os_error(libc::EIO));
            }

            // §13 Cap model: on last close with successful sync, release
            // the cap. The inline sync already persisted data via Raft,
            // so we mark flushed + take cap + send CapRelease RPC.
            // Without this, inline files leak caps on the server (blocking
            // future exclusive grants) and the client re-uses a stale cap.
            if released {
                self.cache.mark_cap_flushed(inode);
                if let Some(cap) = self.cache.take_cap(inode) {
                    let facade = self.client.facade().clone();
                    let client_id = self.client.client_id();
                    let cap_token = cap.token.clone();
                    let runtime = self.client.runtime().handle().clone();
                    runtime.spawn(async move {
                        if let Err(e) = facade.cap_release(inode, &client_id, &cap_token).await {
                            debug!(
                                "release inline: cap_release for inode {} failed (best-effort): {}",
                                inode, e
                            );
                        }
                    });
                    debug!(
                        "release inline: cap released for inode={} (last close, caps were {:?})",
                        inode, cap.issued
                    );
                }
            }

            debug!(
                "release inline: inode={} closed, size={}",
                inode, final_size
            );
            return Ok(());
        }

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
            // Skip sync for read-only opens: no data was written, so syncing
            // would overwrite the filer with potentially stale cache data
            // (e.g., a concurrent writer's not-yet-synced inline data).
            let is_readonly = (flags & libc::O_ACCMODE as u32) == libc::O_RDONLY as u32;
            if is_readonly {
                debug!(
                    "release: skipping sync for inode={} (read-only open, no writes)",
                    inode
                );
                Ok(())
            } else {
                let r = self.sync_size_chunks_on_close(inode);
                if let Err(e) = &r {
                    error!(
                        "release: sync_size_chunks_on_close for inode {} failed: {} — data may be orphaned",
                        inode, e
                    );
                }
                r
            }
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
        if self.cache.get_inode(inode).is_some() {
            let meta_shard_client = self.client.facade().meta_shard_client().clone();
            // inode-level state → route by calculate_shard_id(inode)
            let open_count_shard = self.routing_shard(inode);
            let req = powerfs_coherence::OpenCountRequest {
                shard_id: open_count_shard,
                inode,
            };
            let runtime = self.client.runtime().handle().clone();
            runtime.spawn(async move {
                if let Err(e) = meta_shard_client.open_count_dec(&req).await {
                    debug!(
                        "release: open_count_dec for inode {} failed (best-effort): {}",
                        inode, e
                    );
                }
            });
        }

        // Phase 4.3/4.4: 移除 open_inodes 追踪（getattr 恢复短 TTL）
        // Use reference count: only remove when last open context closes.
        //
        // CRITICAL: Hold open_inodes lock while calling unpin_inode (when
        // flush succeeded) to prevent a concurrent open from pinning between
        // the count decrement and the hold decrement. Without this, open #2
        // could pin (hold 1→2) after release #1 decrements open_inodes to 0,
        // then release #1's unpin sets hold 2→1 — but if open #2's pin hasn't
        // run yet, unpin sets hold 1→0=Unpinned while open_inodes has count 1
        // → InvalidateHandler evicts mid-write (ENOENT in mdtest-hard).
        //
        // Only unpin the inode if flush succeeded. If flush failed, dirty
        // chunks remain and the background flusher needs the inode metadata
        // (fid, volume_id) to retry. Unpinning would let the 30s TTL expire
        // the entry, causing "inode not in cache" errors on every retry cycle.
        // The inode stays pinned until the background flusher successfully
        // writes the data (it will call clear_dirty, and the next release of
        // the file — if reopened — will unpin normally).
        if flush_result.is_ok() {
            let mut open_inodes = self.open_inodes.write().unwrap();
            if let Some(count) = open_inodes.get_mut(&inode) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    open_inodes.remove(&inode);
                }
            }
            let last_release = self.cache.unpin_inode(inode);

            // §13 Cap model: on last close, release the cap.
            //
            // The flush+sync above already persisted dirty data (if
            // sync_result.is_ok), so we just need to:
            // 1. Mark cap as flushed (clear flushing_caps)
            // 2. Send CapRelease RPC to the Filer (best-effort)
            // 3. Take the cap out of the cache entry
            //
            // If flush failed, we keep the cap (and the pinned inode)
            // so the background flusher can retry; the cap will be
            // released when the retry succeeds or the inode is evicted.
            if last_release && sync_result.is_ok() {
                self.cache.mark_cap_flushed(inode);
                if let Some(cap) = self.cache.take_cap(inode) {
                    let facade = self.client.facade().clone();
                    let client_id = self.client.client_id();
                    let cap_token = cap.token.clone();
                    let runtime = self.client.runtime().handle().clone();
                    runtime.spawn(async move {
                        if let Err(e) = facade.cap_release(inode, &client_id, &cap_token).await {
                            debug!(
                                "release: cap_release for inode {} failed (best-effort): {}",
                                inode, e
                            );
                        }
                    });
                    debug!(
                        "release: cap released for inode={} (last close, caps were {:?})",
                        inode, cap.issued
                    );
                }
            }
        } else {
            warn!(
                "release: keeping inode {} pinned (flush failed, dirty chunks remain for retry)",
                inode
            );
            let mut open_inodes = self.open_inodes.write().unwrap();
            if let Some(count) = open_inodes.get_mut(&inode) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    open_inodes.remove(&inode);
                }
            }
        }

        // 4. 释放 Volume lease（best-effort，close 时释放 write + read lease）
        //    write lease: 由 ensure_lease 缓存，通过 VolumeClient 释放
        //    read lease: 由 LeaseManager 缓存，必须在 server 上释放（不能仅
        //    清本地缓存），否则残留的读 lease 会阻止其他客户端获取写 lease
        //    （stripe lease conflict）。
        //    仍在 flush lock 内 —— 后台 flusher 此刻被阻塞，不会用旧 token 写入。
        //
        //    If flush failed, keep the write lease so the background flusher
        //    can retry without re-acquiring (which may fail if the volume
        //    server is under load). Read leases are still released to avoid
        //    blocking other clients' write leases on shared files.
        if flush_result.is_ok() {
            if let Some(entry) = self.cache.get_inode(inode) {
                if let Some(ref fid) = entry.fid {
                    let client_id = self.client.client_id();

                    // 方案 A (inode lease mode): release the single inode
                    // metadata lease held from the Filer. Volume Server
                    // leases are not used in this mode.
                    if self.client.is_inode_lease_mode() {
                        if let Some((token, _remaining)) =
                            self.client.get_valid_inode_lease_token(inode)
                        {
                            // release_inode_lease auto-invalidates cache on success
                            if let Err(e) =
                                self.client.release_inode_lease(inode, &client_id, &token)
                            {
                                debug!(
                                    "release: inode lease release for inode {} failed (best-effort): {}",
                                    inode, e
                                );
                                // On failure, manually invalidate to allow re-acquire
                                self.client.invalidate_inode_lease(inode);
                            }
                        }
                    } else {
                        // 方案 D (range lease mode): release per-stripe leases
                        // 4a. 释放 write lease（遍历所有 stripe lease，逐个远程释放）
                        let write_tokens = self
                            .client
                            .get_all_valid_lease_tokens_for_inode(fid.volume_id.0, inode);
                        for (stripe_start, token) in write_tokens {
                            if let Err(e) = self.client.release_lease(
                                fid.volume_id.0,
                                inode,
                                stripe_start,
                                &client_id,
                                &token,
                            ) {
                                debug!(
                                    "release: write lease release for inode {} stripe_start={} failed (best-effort): {}",
                                    inode, stripe_start, e
                                );
                            }
                        }
                        // 4b. 释放 read lease（从 LeaseManager 缓存取所有 token，在 server 上释放）
                        let read_tokens = self
                            .lease_manager
                            .release_all_for_inode(fid.volume_id.0, inode);
                        for (stripe_start, tok, cid) in read_tokens {
                            if let Err(e) = self.client.release_lease(
                                fid.volume_id.0,
                                inode,
                                stripe_start,
                                &cid,
                                &tok,
                            ) {
                                debug!(
                                    "release: read lease release for inode {} stripe_start={} failed (best-effort): {}",
                                    inode, stripe_start, e
                                );
                            }
                        }
                    }
                }
            }
        } else {
            // Flush failed: release read leases only (to avoid blocking other
            // clients), but keep the write lease for background flusher retry.
            if let Some(entry) = self.cache.get_inode(inode) {
                if let Some(ref fid) = entry.fid {
                    // 方案 A: keep the inode lease (for background flusher retry)
                    if self.client.is_inode_lease_mode() {
                        debug!(
                            "release: keeping inode lease for inode {} (flush failed, retry pending)",
                            inode
                        );
                    } else {
                        // 方案 D: release read leases only
                        let read_tokens = self
                            .lease_manager
                            .release_all_for_inode(fid.volume_id.0, inode);
                        for (stripe_start, tok, cid) in read_tokens {
                            if let Err(e) = self.client.release_lease(
                                fid.volume_id.0,
                                inode,
                                stripe_start,
                                &cid,
                                &tok,
                            ) {
                                debug!(
                                    "release: read lease release for inode {} stripe_start={} failed (best-effort): {}",
                                    inode, stripe_start, e
                                );
                            }
                        }
                        debug!(
                            "release: keeping write lease for inode {} (flush failed, retry pending)",
                            inode
                        );
                    }
                }
            }
        }

        // Phase-4 §5.2 (P3): Invalidate the open-file-lease binding.
        // The Filer-side release was handled above (success or
        // best-effort failure); the registry is just a hint and must
        // be cleared so a subsequent open re-binds a fresh token.
        self.open_file_leases.invalidate(inode);

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

        // Note: The old directory-level local lease (acquire_dir_lease) has
        // been replaced by the dentry lease mechanism. readdir now marks the
        // directory as "complete" (I_COMPLETE equivalent) at the end, and
        // lookups use the three-layer check (dentry lease → shared_gen → RPC)
        // to decide whether to trust the local cache. The Filer auto-subscribes
        // the client to the parent inode on lookup/readdir, so Invalidate
        // notifications are pushed when another client modifies the directory.
        //
        // Design: docs/shard-routing-no-forward-principle.md §7 (dentry lease)

        // 尝试从缓存获取目录条目（用于 is_dir 检查和 ".." 的 parent inode）
        let cached_entry = self.cache.get_inode(inode);

        // 缓存 miss 时通过 MetadataClient.getattr 验证是目录并获取属性
        match &cached_entry {
            Some(entry) if !entry.is_dir => {
                return Err(std::io::Error::from_raw_os_error(libc::ENOTDIR));
            }
            None => {
                let meta_client = self.client.facade().meta_shard_client().clone();
                let routing_shard = self.routing_shard(inode);
                let ino = inode;
                let attr = self
                    .client
                    .block_on(async move { meta_client.getattr(ino, routing_shard).await })
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
            match &cached_entry {
                Some(e) => e.parent,
                None => {
                    // Cache miss: fetch from Filer to get the real parent
                    // inode. Using `inode` as fallback would make ".." point
                    // to self, breaking `cd ..` after the dentry cache expires.
                    debug!(
                        "readdir: cache MISS for inode={}, fetching parent from filer",
                        inode
                    );
                    match self.client.get_entry_by_inode(inode) {
                        Ok(Some((filer_entry, path))) => {
                            let gp = self.resolve_parent_from_path(inode, &path);
                            // Cache the entry so subsequent readdir calls hit
                            let cached = self.entry_to_cached(gp, &filer_entry);
                            self.cache.insert(cached.clone());
                            gp
                        }
                        _ => {
                            warn!(
                                "readdir: failed to fetch parent for inode={}, using self as fallback",
                                inode
                            );
                            inode // last resort: point to self
                        }
                    }
                }
            }
        };

        // "." and ".." are only emitted on the first page (offset <= 1).
        // We use fixed DirEntry.offset values (1 and 2) so the kernel does
        // not re-request them after we advance past the first page.
        if offset == 0 {
            match add_entry(DirEntry {
                ino: inode,
                offset: 1,
                type_: 0o040000,
                name: ".".as_bytes(),
            }) {
                Ok(0) | Err(_) => return Ok(()), // buffer full or error
                Ok(_) => {}
            }
        }
        if offset <= 1 {
            match add_entry(DirEntry {
                ino: parent_ino,
                offset: 2,
                type_: 0o040000,
                name: "..".as_bytes(),
            }) {
                Ok(0) | Err(_) => return Ok(()), // buffer full or error
                Ok(_) => {}
            }
        }

        // `idx` must continue monotonically across pages so that
        // DirEntry.offset never regresses. If we are resuming past page 1
        // (offset > 2), start idx at offset; otherwise (page 1) start at 2
        // (after "." and ".."). A regressing offset would make the kernel
        // treat it as a rewind and stop reading the directory — reproducing
        // the readdir-only-first-page bug that caused rm -rf to leave
        // entries beyond the first page unlinked.
        let mut idx = if offset > 2 { offset } else { 2 };

        // Step 2: 通过 MetadataClient.readdir RPC 走 Filer Raft leader（强一致 Leader Lease Read）
        // 方案 B (S5): 优先用缓存的 shard_id (目录 inode 创建时 Filer 返回的权威值),
        // 缓存 miss 时回退到 calculate_shard_id(inode)。
        //
        // Pagination: the Filer uses a last_name cursor (BTreeMap seek), not a
        // numeric offset. We translate the FUSE `offset` cookie into the
        // matching `last_name` using `readdir_cursors`:
        //   - offset == 0  → fresh read; clear cursor; last_name = ""
        //   - offset > 0 with a stored cursor → resume; last_name = cursor.last_name
        //     (We match loosely on offset because the FUSE kernel may pass the
        //     last DirEntry.offset or offset+1, and exact equality was causing
        //     cursor misses that restarted from the first page — reproducing
        //     the very intermittent-delete bug this fixes. For sequential
        //     readers like `rm -rf`/`find`, the cursor is monotonic, so a
        //     stale cursor at worst repeats one page rather than skipping.)
        //   - offset > 0 with no cursor → fallback "" (rewind past cache).
        let last_name: String = if offset == 0 {
            self.readdir_cursors.remove(&inode);
            String::new()
        } else {
            match self.readdir_cursors.get(&inode) {
                Some(c) => c.last_name.clone(),
                None => {
                    debug!(
                        "readdir: cursor miss for inode {} offset {} (rewind/seek), restarting from head",
                        inode, offset
                    );
                    String::new()
                }
            }
        };

        let meta_client = self.client.facade().meta_shard_client().clone();
        let shard_id = self.routing_shard(inode);
        let ln = last_name.clone();
        let dir_entries: Vec<MetadataDirEntry> = self
            .client
            .block_on(async move { meta_client.readdir(inode, &ln, 1000, shard_id).await })
            .map_err(|e| {
                error!("readdir RPC failed for inode {}: {}", inode, e);
                std::io::Error::from_raw_os_error(libc::EIO)
            })?;

        info!(
            "READDIR_DIAG: inode={} offset={} requested_last_name={:?} filer_returned={}",
            inode,
            offset,
            last_name,
            dir_entries.len()
        );

        debug!(
            "readdir: RPC returned {} entries for dir {} (last_name={:?})",
            dir_entries.len(),
            inode,
            last_name
        );

        let mut last_returned: Option<&MetadataDirEntry> = None;
        for child in &dir_entries {
            idx += 1;
            // DT_DIR=4, DT_REG=8, DT_LNK=10 等；FUSE DirEntry.type_ 用 d_type 值
            let type_ = child.file_type as u32;
            // NOTE: we do NOT skip entries with `if offset < idx` here.
            // That check was correct for a single page but, once pagination
            // was added, `idx` resumes from `offset` (which is >= the page's
            // first idx), so `offset < idx` was false for the WHOLE second
            // page — silently skipping every entry past page 1 and making
            // readdir return only the first page forever. Since we now emit
            // "." / ".." with fixed offsets only on page 1, and `idx` starts
            // at `offset` on subsequent pages, every entry on the current
            // page must be emitted unconditionally.
            //
            // CRITICAL: fuse-backend-rs's add_dirent returns Ok(0) — NOT Err —
            // when the kernel readdir buffer is full. Checking only .is_err()
            // caused the loop to continue past the buffer-full point, updating
            // last_returned to the LAST entry in the filer's sort order
            // (e.g. "file_99.txt" in lexicographic order) instead of the last
            // entry actually written to the buffer. The cursor then pointed
            // to a name near the end of the BTreeMap, so the next readdir
            // RPC returned 0 entries — silently dropping ~500/600 entries
            // and causing rm -rf to leave most files behind.
            let added = match add_entry(DirEntry {
                ino: child.inode,
                offset: idx,
                type_,
                name: child.name.as_bytes(),
            }) {
                Ok(0) => {
                    // Buffer full — stop adding. Do NOT update last_returned:
                    // it must stay at the last entry successfully written so
                    // the cursor resumes correctly on the next readdir call.
                    break;
                }
                Ok(n) => {
                    last_returned = Some(child);
                    Some(n)
                }
                Err(_) => {
                    break;
                }
            };

            // === READDIR-DRIVEN ATTR CACHING ===
            // If the Filer's readdir response piggybacked stat fields on the
            // entry (mode/uid/gid/size/mtime/nlink + child_shard_id), seed
            // the client cache with them as a Stale hint. This avoids an
            // extra per-entry getattr RPC for a subsequent `ls -l`, which
            // otherwise would be O(N) cross-shard RPCs when the directory
            // contains children homed on different shards (UpdateChildSummary
            // in §3.2 of the MetaCache design populates exactly these fields
            // on the parent shard leader's readdir path).
            //
            // Rules:
            //   * inserted state = EntryState::Stale — these attrs are a
            //     "cached value of last resort", not the result of a fresh
            //     per-entry lookup RPC. If the user ever does a real stat()
            //     or the TTL expires, we fall back to a full getattr RPC and
            //     the entry moves to Clean / Dirty normally. In particular,
            //     NEVER overwrite an existing cache entry that's in the
            //     Clean, Dirty, or Flushing lifecycle state (those carry
            //     authoritative local data that readdir's indirect payload
            //     must not regress).
            //   * skip `..` / `.` (they're never MetadataDirEntry items).
            //   * cross-shard child_shard_id is stored as `shard_id: Some()`,
            //     so `routing_shard()` uses the Filer-provided value instead
            //     of re-hashing inode (avoids shard_count drift skew).
            if added.is_some() {
                if let Some(attr) = child.attrs.as_ref() {
                    if self.cache.get_inode(child.inode).is_none() {
                        use std::time::Instant;
                        let is_dir = child.file_type == libc::DT_DIR;
                        let is_symlink = child.file_type == libc::DT_LNK;
                        let symlink_target = if is_symlink {
                            attr.symlink_target.clone()
                        } else {
                            None
                        };
                        // Units: readdir response encodes atime/mtime/ctime in
                        // milliseconds, matching the same convention the rest
                        // of attr_from_resp uses for MetadataAttr → CachedEntry.
                        let ms_to_i64 = |ms: u64| -> i64 {
                            if ms <= i64::MAX as u64 {
                                ms as i64
                            } else {
                                i64::MAX
                            }
                        };
                        let cached = CachedEntry {
                            inode: child.inode,
                            parent: inode,
                            name: child.name.clone(),
                            is_dir,
                            is_symlink,
                            symlink_target,
                            nlink: attr.nlink.max(1),
                            fid: None,
                            size: attr.size,
                            // Strip SUID/SGID/sticky + file-type bits → perm
                            // by masking with 0o7777, mirroring what
                            // entry_to_cached does with the raw mode field.
                            mode: attr.mode & 0o7777,
                            uid: attr.uid,
                            gid: attr.gid,
                            atime: ms_to_i64(attr.atime),
                            mtime: ms_to_i64(attr.mtime),
                            ctime: attr.ctime, // already signed ms in MetadataAttr
                            xattrs: HashMap::new(),
                            chunks: Vec::new(),
                            hard_link_id: String::new(),
                            hard_link_counter: 0,
                            content_size: attr.size,
                            disk_size: attr.size,
                            generation: 0,
                            placement: attr.placement.clone(),
                            reliability: attr.reliability.clone(),
                            replica_chunks: Vec::new(),
                            shard_id: if child.child_shard_id != 0 {
                                Some(child.child_shard_id)
                            } else {
                                attr.shard_id
                            },
                            cached_at: Instant::now(),
                            state: EntryState::Stale,
                            hold: HoldState::Unpinned,
                            cap: None,
                            dentry_lease: None,
                            dir_shared_gen: 0,
                        };
                        self.cache.insert(cached);
                    }
                }
            }
        }

        // Record the pagination cursor with the name of the last entry
        // actually returned to the kernel. If we broke out early (buffer
        // full), the next readdir() must resume right after this entry.
        // Only skip the update when nothing was returned (empty page →
        // end-of-directory, or all entries skipped by offset → rewind).
        info!(
            "READDIR_DIAG: inode={} offset={} returned_to_kernel={} last_name={:?} next_idx={}",
            inode,
            offset,
            last_returned.is_some(),
            last_returned.map(|e| e.name.as_str()).unwrap_or("(none)"),
            idx
        );
        if let Some(last) = last_returned {
            self.readdir_cursors.insert(
                inode,
                ReaddirCursor {
                    last_name: last.name.clone(),
                },
            );
        }

        // Mark the directory as complete (I_COMPLETE equivalent) so that
        // subsequent lookups can trust negative dentries (cache miss = ENOENT)
        // without sending an RPC, as long as the dir_version (shared_gen)
        // hasn't changed. This is the Ceph I_COMPLETE mechanism.
        if offset == 0 {
            let dir_version = self.cache.get_dir_version(inode);
            self.cache.mark_dir_complete(inode, dir_version);
            debug!(
                "readdir: marked dir {} complete (version={})",
                inode, dir_version
            );
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
        // shard_id = calculate_shard_id(olddir)（源目录的 shard）
        let meta_client = self.client.facade().meta_shard_client().clone();
        let shard_id = self.routing_shard(olddir);
        let old_owned = old_str.to_string();
        let new_owned = new_str.to_string();
        let _attr = self
            .client
            .block_on(async move {
                meta_client
                    .rename(olddir, &old_owned, newdir, &new_owned, shard_id)
                    .await
            })
            .map_err(|e| {
                let errno = filer_error_to_errno(&e.to_string());
                if errno == libc::EIO {
                    error!("rename RPC failed: {}", e);
                } else {
                    debug!("rename RPC failed: {} -> errno={}", e, errno);
                }
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
            placement: None,
            reliability: powerfs_layout::reliability::Reliability::default(),
            replica_chunks: Vec::new(),
            shard_id: None,
            cached_at: Instant::now(),
            state: EntryState::default(),
            hold: HoldState::default(),
            cap: None,
            dentry_lease: None,
            dir_shared_gen: 0,
        };
        self.cache.insert(cached_entry.clone());

        // Phase-4 §5.1 Lockify: speculatively self-declare inode
        // ownership for the new symlink. Async-synced to filer.
        self.lockify_declare_new_inode(inode);

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
                    placement: None,
                    reliability: powerfs_layout::reliability::Reliability::default(),
                    replica_chunks: Vec::new(),
                    shard_id: None,
                    cached_at: Instant::now(),
                    state: EntryState::default(),
                    hold: HoldState::default(),
                    cap: None,
                    dentry_lease: None,
                    dir_shared_gen: 0,
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

    /// Flush is called by the FUSE kernel on every close() of a file descriptor.
    /// The kernel WAITS for the flush response before returning from close(),
    /// unlike release() which is asynchronous.
    ///
    /// Without implementing flush(), the kernel returns immediately from close()
    /// without waiting for data/metadata sync. This causes a race condition:
    /// a subsequent stat() by another process can read stale metadata from the
    /// Filer before release() has a chance to sync_size_chunks_on_close().
    ///
    /// Fix: sync data to Volume Server + metadata to Filer inside flush(),
    /// so the Filer has the correct size/chunks before close() returns.
    /// This fixes the "Size error" in fsx and dd write-beyond-EOF scenarios.
    fn flush(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        _handle: Self::Handle,
        _lock_owner: u64,
    ) -> std::io::Result<()> {
        let cs = self
            .cache
            .peek_inode(inode)
            .map(|e| e.content_size)
            .unwrap_or(u64::MAX);
        info!("flush: inode={} content_size={}", inode, cs);

        // Inline mode: data is in inline_buffers, persisted on release.
        // flush is a no-op for inline files (data < 8KB, write-close window
        // is short; release handles the Raft commit).
        if self.inline_buffers.contains_key(&inode) {
            debug!(
                "flush: inode={} is inline, no-op (data persisted on release)",
                inode
            );
            return Ok(());
        }

        // Flat/Stripe: flush data to Volume Server, then sync metadata to Filer.
        // This is the same logic as fsync, ensuring the Filer has up-to-date
        // size/chunks before close() returns to the application.
        match self.flush_dirty_chunks(inode, None) {
            Ok(()) => {
                if !self.has_dirty_for_inode(inode) {
                    match self.sync_size_chunks_on_close(inode) {
                        Ok(()) => {
                            debug!("flush: inode={} data + metadata synced", inode);
                            // Don't clear dirty here: release() will do that
                            // after its own flush+sync. Clearing here would
                            // cause release to skip sync, but if a concurrent
                            // write happens between flush and release, the
                            // dirty flag needs to be set by that write. Keeping
                            // dirty set is safe — release will re-flush (no-op)
                            // and re-sync (same data).
                            Ok(())
                        }
                        Err(e) => {
                            error!(
                                "flush: sync_size_chunks_on_close failed for inode={}: {}",
                                inode, e
                            );
                            Err(e)
                        }
                    }
                } else {
                    // Still has dirty chunks (flush didn't fully succeed).
                    // Don't sync metadata — release will retry.
                    debug!(
                        "flush: inode={} still has dirty chunks after flush, skipping metadata sync",
                        inode
                    );
                    Ok(())
                }
            }
            Err(e) => {
                error!(
                    "flush: flush_dirty_chunks failed for inode={}: {} (raw_os_error={:?})",
                    inode,
                    e,
                    e.raw_os_error()
                );
                Err(e)
            }
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
        // P2.5: Inline 模式文件无 chunk_cache dirty 数据 (数据在 inline_buffers).
        // fsync 为 no-op: 数据在 release 时一次性 Raft 提交到 Filer (持久化).
        // (Inline 文件 <8KB, 写入→关闭窗口极短; 中途 fsync 同步留作后续优化.)
        if self.inline_buffers.contains_key(&inode) {
            debug!(
                "fsync: inode={} is inline, no-op (data persisted on release)",
                inode
            );
            return Ok(());
        }
        // Flat/Stripe: flush 数据到 Volume Server, 然后 sync 元数据到 Filer.
        // fsync 必须保证元数据持久化, 否则 FUSE RELEASE (异步) 可能晚于
        // 进程退出/重启, 导致元数据丢失 (P2-2 修复).
        match self.flush_dirty_chunks(inode, None) {
            Ok(()) => {
                // 数据已持久化到 Volume Server, 现在 sync 元数据到 Filer (Raft).
                // 仅当有 dirty chunks 被 flush 时才需要 sync.
                if !self.has_dirty_for_inode(inode) {
                    match self.sync_size_chunks_on_close(inode) {
                        Ok(()) => {
                            debug!("fsync: inode={} data + metadata synced", inode);
                            self.chunk_cache.clear_dirty(inode);
                            Ok(())
                        }
                        Err(e) => {
                            error!(
                                "fsync: sync_size_chunks_on_close failed for inode={}: {}",
                                inode, e
                            );
                            Err(e)
                        }
                    }
                } else {
                    // 仍有 dirty chunks (flush 未完全成功), 不 sync 元数据
                    debug!(
                        "fsync: inode={} still has dirty chunks after flush, skipping metadata sync",
                        inode
                    );
                    Ok(())
                }
            }
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

        // Only support default (allocate) and KEEP_SIZE / PUNCH_HOLE modes.
        const FALLOC_FL_KEEP_SIZE: u32 = 0x01;
        const FALLOC_FL_PUNCH_HOLE: u32 = 0x02;
        if mode & !(FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE) != 0 {
            return Err(std::io::Error::from_raw_os_error(libc::EOPNOTSUPP));
        }
        // PUNCH_HOLE must be combined with KEEP_SIZE.
        if (mode & FALLOC_FL_PUNCH_HOLE) != 0 && (mode & FALLOC_FL_KEEP_SIZE) == 0 {
            return Err(std::io::Error::from_raw_os_error(libc::EOPNOTSUPP));
        }
        if length == 0 {
            return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
        }

        let entry = self
            .cache
            .get_inode(inode)
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

        if entry.is_dir {
            return Err(std::io::Error::from_raw_os_error(libc::EISDIR));
        }

        // When KEEP_SIZE is not set, fallocate may extend the file size.
        if (mode & FALLOC_FL_KEEP_SIZE) == 0 {
            let new_size = offset + length;
            if new_size > entry.size {
                // Sync the new size to the Filer via setattr RPC so that
                // other clients and remounts see the correct size.
                // Without this, stat() would re-fetch size=0 from the Filer.
                let params = SetattrParams {
                    mode: None,
                    uid: None,
                    gid: None,
                    size: Some(new_size),
                    atime: None,
                    mtime: None,
                };
                let meta_client = self.client.facade().meta_shard_client().clone();
                let shard_id = self.routing_shard(inode);
                self.client
                    .block_on(async move { meta_client.setattr(inode, &params, shard_id).await })
                    .map_err(|e| {
                        let errno = filer_error_to_errno(&e.to_string());
                        if errno == libc::EIO {
                            error!("fallocate setattr RPC failed for inode {}: {}", inode, e);
                        } else {
                            debug!(
                                "fallocate setattr RPC failed for inode {}: {} -> errno={}",
                                inode, e, errno
                            );
                        }
                        std::io::Error::from_raw_os_error(errno)
                    })?;

                self.cache.update_size(inode, new_size);
            }
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

        // Do NOT return ENOENT when the cache entry is missing. The Filer
        // may have evicted it via an Invalidate notification (e.g. the Filer
        // notifies about a new inode right after mkdir, causing the local
        // cache to evict the entry we just created). The set_xattr RPC below
        // will query the Filer, which returns NOT_FOUND → ENOENT if the inode
        // truly doesn't exist. Returning ENOENT prematurely here breaks
        // "cp -prf" preserving permissions (system.posix_acl_access).

        // Persist ALL xattrs to Filer via Raft.
        // The `attr`/`setfattr` tool sends names in the "user." namespace.
        // For powerfs.* xattrs, normalize "user.powerfs.*" → "powerfs.*"
        // (Filer stores these without "user." prefix for placement policy).
        // For other xattrs, store with the original name (including "user.").
        // The local cache always keeps the ORIGINAL name so the kernel finds it.
        let normalized_name = if name_str.starts_with("user.powerfs.") {
            &name_str[5..] // strip "user." → "powerfs.*"
        } else {
            name_str
        };

        let meta_client = self.client.facade().meta_shard_client().clone();
        let shard_id = self.routing_shard(inode);
        let value_owned = value.to_vec();
        let name_for_rpc = normalized_name.to_string();
        match self.client.block_on(async move {
            meta_client
                .set_xattr(shard_id, inode, &name_for_rpc, &value_owned)
                .await
        }) {
            Ok(()) => {
                debug!(
                    "setxattr: persisted {} on inode {} to Filer (original={})",
                    normalized_name, inode, name_str
                );
            }
            Err(e) => {
                let errno = filer_error_to_errno(&e.to_string());
                if errno == libc::EIO {
                    warn!(
                        "setxattr: failed to persist {} on inode {}: {}",
                        normalized_name, inode, e
                    );
                } else {
                    debug!(
                        "setxattr: failed to persist {} on inode {}: {} -> errno={}",
                        normalized_name, inode, e, errno
                    );
                }
                return Err(std::io::Error::from_raw_os_error(errno));
            }
        }

        self.cache.set_xattr(inode, name_str, value);

        // Handle POSIX ACL: cp -p and other tools use
        // fsetxattr("system.posix_acl_access") instead of chmod to set file
        // permissions. Parse the ACL data and update the file mode via setattr
        // so cp -prf preserves permissions correctly on FUSE.
        if name_str == "system.posix_acl_access" {
            let cur_mode = self.cache.get_inode(inode).map(|e| e.mode).unwrap_or(0o644);
            if let Some(acl_mode) = parse_posix_acl_mode(value, cur_mode) {
                debug!(
                    "setxattr: ACL mode for inode {} = {:o}, updating via setattr",
                    inode, acl_mode
                );
                let params = SetattrParams {
                    mode: Some(acl_mode),
                    uid: None,
                    gid: None,
                    size: None,
                    atime: None,
                    mtime: None,
                };
                let meta_client = self.client.facade().meta_shard_client().clone();
                let shard_id = self.routing_shard(inode);
                match self
                    .client
                    .block_on(async move { meta_client.setattr(inode, &params, shard_id).await })
                {
                    Ok(_) => {
                        self.cache.update_attr(
                            inode,
                            crate::cache::UpdateAttrParams {
                                mode: Some(acl_mode),
                                size: None,
                                uid: None,
                                gid: None,
                                atime: None,
                                mtime: None,
                            },
                        );
                    }
                    Err(e) => {
                        warn!("setxattr: ACL setattr failed for inode {}: {}", inode, e);
                    }
                }
            }
        }

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

        if let Some(value) = self.cache.get_xattr(inode, name_str) {
            if size == 0 {
                Ok(GetxattrReply::Count(value.len() as u32))
            } else if value.len() > size as usize {
                Err(std::io::Error::from_raw_os_error(libc::ERANGE))
            } else {
                Ok(GetxattrReply::Value(value))
            }
        } else {
            // Cache miss — fetch from Filer for ALL xattr names.
            // Normalize "user.powerfs.*" → "powerfs.*" for the Filer lookup
            // (Filer stores powerfs keys without "user." prefix).
            // Other xattrs are stored with their full name on the Filer.
            let normalized_name = if name_str.starts_with("user.powerfs.") {
                &name_str[5..]
            } else {
                name_str
            };

            let meta_client = self.client.facade().meta_shard_client().clone();
            let shard_id = self.routing_shard(inode);
            let name_for_rpc = normalized_name.to_string();
            match self.client.block_on(async move {
                meta_client.get_xattr(shard_id, inode, &name_for_rpc).await
            }) {
                Ok(value) => {
                    debug!(
                        "getxattr: fetched {} from Filer for inode {} ({} bytes, original={})",
                        normalized_name,
                        inode,
                        value.len(),
                        name_str
                    );
                    // Cache with the ORIGINAL name (kernel uses "user.*").
                    self.cache.set_xattr(inode, name_str, &value);
                    if size == 0 {
                        Ok(GetxattrReply::Count(value.len() as u32))
                    } else if value.len() > size as usize {
                        Err(std::io::Error::from_raw_os_error(libc::ERANGE))
                    } else {
                        Ok(GetxattrReply::Value(value))
                    }
                }
                Err(e) => {
                    debug!(
                        "getxattr: {} not found on Filer for inode {}: {}",
                        normalized_name, inode, e
                    );
                    Err(std::io::Error::from_raw_os_error(libc::ENODATA))
                }
            }
        }
    }

    fn listxattr(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        size: u32,
    ) -> std::io::Result<ListxattrReply> {
        debug!("listxattr: inode={}", inode);

        let xattrs = self.cache.list_xattrs(inode);

        // If the cache has no xattrs, try fetching from the Filer.
        // This handles the case where the inode was loaded via lookup (which
        // doesn't include xattrs) or after a remount (cache is empty).
        if xattrs.is_empty() {
            let meta_client = self.client.facade().meta_shard_client().clone();
            let shard_id = self.routing_shard(inode);
            match self
                .client
                .block_on(async move { meta_client.list_xattr(shard_id, inode).await })
            {
                Ok(keys) => {
                    for key in &keys {
                        // Cache each key with an empty value so listxattr
                        // returns the keys. The actual value will be fetched
                        // by getxattr on demand.
                        if self.cache.get_xattr(inode, key).is_none() {
                            self.cache.set_xattr(inode, key, b"");
                        }
                    }
                    if !keys.is_empty() {
                        debug!(
                            "listxattr: fetched {} keys from Filer for inode {}",
                            keys.len(),
                            inode
                        );
                    }
                }
                Err(e) => {
                    debug!(
                        "listxattr: failed to fetch keys from Filer for inode {}: {}",
                        inode, e
                    );
                }
            }
        }

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

        // Do NOT return ENOENT when the cache entry is missing — see setxattr
        // for rationale. The remove_xattr RPC below will query the Filer,
        // which returns NOT_FOUND → ENOENT if the inode truly doesn't exist.

        // Remove from Filer (persisted via Raft).
        let normalized_name = if name_str.starts_with("user.powerfs.") {
            &name_str[5..]
        } else {
            name_str
        };

        let meta_client = self.client.facade().meta_shard_client().clone();
        let shard_id = self.routing_shard(inode);
        let name_for_rpc = normalized_name.to_string();
        match self.client.block_on(async move {
            meta_client
                .remove_xattr(shard_id, inode, &name_for_rpc)
                .await
        }) {
            Ok(()) => {
                debug!(
                    "removexattr: removed {} from Filer for inode {}",
                    normalized_name, inode
                );
            }
            Err(e) => {
                warn!(
                    "removexattr: failed to remove {} from Filer for inode {}: {}",
                    normalized_name, inode, e
                );
                // Continue to remove from local cache anyway
            }
        }

        if !self.cache.remove_xattr(inode, name_str) {
            // Not in local cache, but may have been removed from Filer
            // Return ENODATA if the Filer also didn't have it
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
