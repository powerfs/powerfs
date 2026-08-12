use crate::cache::{CachedEntry, ChunkCache, EntryState, HoldState, MetadataCache, ROOT_INODE};
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

        // Start admin/debug HTTP server if admin_port is configured.
        // Exposes /stats (request statistics + in-flight tracking) and
        // /health endpoints for `powerfs-cli fuse-stats` to query.
        if self.admin_port > 0 {
            let bind_addr = format!("0.0.0.0:{}", self.admin_port);
            crate::admin_server::AdminServer::start(bind_addr, sync_client.stats().clone());
            info!("Admin/debug server enabled on port {}", self.admin_port);
        } else {
            info!("Admin/debug server disabled (admin_port=0)");
        }

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
        // P2: The handler is constructed with `new_with_fuse_fd` so it can
        // send FUSE_NOTIFY_INVAL_INODE messages to the kernel. The actual fd
        // value is set via `set_fuse_fd()` after the FUSE session is mounted.
        let invalidate_handler = Arc::new(
            crate::invalidate_handler::InvalidateHandler::new_with_fuse_fd(
                cache.clone(),
                chunk_cache.clone(),
                inline_buffers.clone(),
                fuse_fd.clone(),
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
            open_inodes: Arc::new(RwLock::new(HashSet::new())),
            inline_buffers: inline_buffers.clone(),
            inline_max_sizes: Arc::new(DashMap::new()),
            last_cache_epoch: std::sync::atomic::AtomicU64::new(0),
            fuse_fd: fuse_fd.clone(),
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
    /// Phase 4.3/4.4: 当前已打开的 inode 集合。
    /// open() 时加入，release() 时移除。getattr() 对其中的 inode 使用长 TTL
    /// （size/chunks 在 open→release 期间权威，因数据 lease 排他）。
    open_inodes: Arc<RwLock<HashSet<u64>>>,
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
                "check_cache_epoch: epoch changed {} -> {}, invalidating all cached metadata",
                last,
                current
            );
            self.cache.invalidate_all();
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
        self.flush_dirty_chunks_impl(inode, lease_token)
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
                let is_open = self.open_inodes.read().unwrap().contains(&inode);

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
                        debug!(
                            "flush_all_dirty_chunks: flushed inode={} not open, syncing + unpinning thread={:?}",
                            inode, std::thread::current().id()
                        );
                        self.cache.unpin_inode(inode);
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
            "K3-DBG sync_close: inode={} chunks={:?}",
            inode,
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
        // 查 Filer（shard_id calculated from parent_ino）
        let meta_client = self.client.facade().meta_shard_client().clone();
        let shard_id = self.routing_shard(parent);
        let name_owned = name.to_string();
        self.client
            .block_on(async move { meta_client.lookup(parent, &name_owned, shard_id).await })
            .is_ok()
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
                    "lookup: filer returned inode={} for parent={}, name={}",
                    attr.inode, parent, name_str
                );
                let entry = attr_to_cached_entry(&attr, parent, name_str);
                self.cache.insert(entry.clone());
                Ok(self.create_fuse_entry(&entry))
            }
            Err(e) => {
                debug!("lookup RPC failed for '{}/{}': {}", parent, name_str, e);
                // Return a negative Entry (inode=0) with a short entry_timeout.
                // This tells the kernel to cache the negative result for only
                // TTL (100ms), so that subsequent lookups after a rename/create
                // will re-query the FUSE daemon.
                //
                // Returning Err(ENOENT) causes the kernel to use its default
                // negative entry timeout, which can be very long, leading to
                // stale "file not found" results after a cross-directory rename.
                // This is the root cause of R6: mv first checks if the target
                // exists (creating a negative dentry), then renames; the kernel
                // serves the stale negative dentry for the renamed file.
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
            self.cache.mark_dirty(inode);
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
                // Flat 模式 truncate: 清除 ChunkCache，truncate 丢弃所有缓存数据
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
            let shard_id = self.routing_shard(parent);
            let name_owned = name_str.to_string();
            let attr = self
                .client
                .block_on(async move { meta_client.lookup(parent, &name_owned, shard_id).await })
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
        let shard_id = self.routing_shard(parent);
        let name_owned = name_str.to_string();
        self.client
            .block_on(async move { meta_client.unlink(parent, &name_owned, shard_id).await })
            .map_err(|e| {
                let errno = filer_error_to_errno(&e.to_string());
                if errno == libc::EIO {
                    error!("unlink RPC failed: {}", e);
                } else {
                    debug!("unlink RPC failed: {} -> errno={}", e, errno);
                }
                std::io::Error::from_raw_os_error(errno)
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
            // P2.5: 清理可能残留的 inline buffer (文件被 unlink 时仍打开的罕见场景)
            self.inline_buffers.remove(&entry.inode);
            self.inline_max_sizes.remove(&entry.inode);
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
            };
            // Phase 3: use insert_pinned to set hold=Pinned BEFORE insert.
            // The old pattern (pin_inode before insert) was a no-op when the
            // inode was not yet in the cache (entry.hold is authoritative).
            self.open_inodes.write().unwrap().insert(inode);
            self.cache.insert_pinned(entry.clone());
            debug!("create: inline mode, inode={}, dir={}", inode, parent);
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
            };
            self.open_inodes.write().unwrap().insert(inode);
            self.cache.insert_pinned(entry.clone());
            debug!("create: stripe mode, inode={}, dir={}", inode, parent);
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
        };
        // CRITICAL: Pin the inode BEFORE inserting the cache entry.
        // The Filer pushes an Invalidate after the create RPC commits, and
        // Phase 3: use insert_pinned to atomically set hold=Pinned and insert.
        // The old pattern (pin_inode before insert) was a no-op because the
        // inode was not yet in the cache (entry.hold is authoritative, not
        // pinned_inodes). insert_pinned sets hold on the entry before insert,
        // so InvalidateHandler skips the entry from the moment it enters cache.
        self.open_inodes.write().unwrap().insert(inode);
        self.cache.insert_pinned(entry.clone());
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
        let _parent = if let Some(entry) = self.cache.get_inode(inode) {
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
            let has_dirty_inline = self
                .inline_buffers
                .get(&inode)
                .map(|b| b.dirty)
                .unwrap_or(false);
            let has_dirty_chunks = self.chunk_cache.has_dirty_chunks(inode);
            debug!(
                "open: inode={} dirty_check has_dirty_chunks={} has_dirty_inline={} inline_buffers_contains={}",
                inode,
                has_dirty_chunks,
                has_dirty_inline,
                self.inline_buffers.contains_key(&inode)
            );
            if has_dirty_chunks || has_dirty_inline {
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
                    // Phase 3: use insert_pinned to set hold=Pinned BEFORE insert.
                    // pin_inode() is a no-op when the inode is not in the cache.
                    self.open_inodes.write().unwrap().insert(inode);
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

        // Phase 3.5.3: 通知 filer 递增 open_count（best-effort，失败不阻塞 open）
        let meta_shard_client = self.client.facade().meta_shard_client().clone();
        // inode-level state → route by calculate_shard_id(inode)
        let open_count_shard = self.routing_shard(inode);
        let req = powerfs_coherence::OpenCountRequest {
            shard_id: open_count_shard,
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
                        let actual_end = std::cmp::min(off + chunk_size, file_size - group_start);
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
                    break;
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
                    break;
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
                                        error!(
                                            "CRC32 mismatch: inode={} offset={} expected={:#x} actual={:#x}",
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
                        // RACE_TRACE: Backpressure flush can trigger the unpin race
                        // in flush_all_dirty_chunks. Log to correlate with ENOENT.
                        warn!(
                            "write BACKPRESSURE: inode={} cache={} > threshold={} ({}%) thread={:?} \
                             — calling flush_all_dirty_chunks (may unpin still-open inodes)",
                            inode, current, threshold, BACKPRESSURE_THRESHOLD_PCT,
                            std::thread::current().id()
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
            .ok_or_else(|| {
                // RACE_TRACE: get_inode returned None during write — this is the
                // ENOENT that causes B1/B3 failures. Log full context to identify
                // which concurrent operation evicted the inode.
                let is_pinned = self.cache.is_pinned(inode);
                let has_chunks = self.chunk_cache.has_chunks(inode);
                let has_dirty = self.chunk_cache.has_dirty_chunks(inode);
                let is_open = self.open_inodes.read().unwrap().contains(&inode);
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
                    debug!(
                        "write inline: inode={} offset={} len={} buffer_len={}",
                        inode, offset, read_len, updated_size
                    );
                    // Update content_size in cache so getattr reports correct size
                    self.cache.update_size(inode, updated_size);
                    // EntryState: 标记 Dirty 以反映 inline buffer 已修改
                    self.cache.mark_dirty(inode);
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
                    self.cache.mark_dirty(inode);
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
            if let Some(current_entry) = self.cache.get_inode(inode) {
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
            self.cache.mark_dirty(inode);
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
            // entry.fid 为 None：文件已存在但 Filer 元数据缺失 chunk mapping。
            // 这属于元数据异常（create 时 set_chunks 失败，或 Filer 数据损坏）。
            //
            // 旧代码在此调用 assign_fid 从 Master 分配新 needle_id，但这与 Filer
            // Zone 自分配模型冲突：客户端写入用的 needle_id 与 Filer 元数据不一致，
            // 导致重新挂载后读不到数据（与 create 路径相同的 BUG）。
            //
            // 正确处理：返回 EIO，让应用层感知元数据异常并决定恢复策略
            // （如删除文件重建，或由 fsck 工具修复）。不应在 write 路径隐式
            // 分配新 needle_id，那会掩盖根因并造成数据/元数据分裂。
            error!(
                "write: inode {} has no fid (Filer metadata missing chunks), refusing to write. \
                 File may be corrupted; use fsck to repair or recreate the file.",
                inode
            );
            return Err(std::io::Error::from_raw_os_error(libc::EIO));
        }

        // EntryState: 标记 Dirty 以反映 chunk_cache 已写入数据
        self.cache.mark_dirty(inode);
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
            // inode-level write → route by routing_shard(inode). Inline
            // data + size are stored on the inode's own shard, NOT the parent
            // dir's shard. Routing via `parent` would send the close-sync to
            // the wrong leader and corrupt the file (size=0 / inline_data lost).
            let routing_shard = self.routing_shard(inode);

            // Loop: mark-clean → clone → sync → check-dirty-again
            // If a concurrent write marks the buffer dirty during the sync RPC,
            // we re-sync the updated buffer. Max 3 iterations to avoid infinite
            // loops under sustained write pressure.
            let mut sync_ok = true;
            let mut final_size = 0u64;

            for sync_round in 0..3u32 {
                // Step 1: Clone current data WITHOUT marking dirty=false.
                // Marking dirty=false before sync creates a window where a
                // concurrent open sees the buffer as not-dirty and refreshes
                // stale metadata from the Filer (size=0), causing append writes
                // to use offset=0 and overwrite existing data.
                // Instead, keep dirty=true during sync and detect concurrent
                // writes by comparing buffer length before and after sync.
                //
                // Also capture original_len and modified_in_place to decide
                // between append mode (send only delta) and overwrite mode
                // (send full buffer). Append mode prevents lost updates when
                // multiple clients concurrently append to the same inline file.
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
                        final_size = size;
                        Some((size, data, orig_len, mod_in_place))
                    } else {
                        // Buffer was removed by someone else (e.g., migration)
                        None
                    }
                };

                let Some((size, data, orig_len, mod_in_place)) = snapshot else {
                    // Buffer gone, nothing to sync
                    break;
                };

                // Step 2: If not dirty (read-only open), skip sync entirely.
                if data.is_none() {
                    debug!(
                        "release inline: inode={} not dirty (round {}), skip sync",
                        inode, sync_round
                    );
                    break;
                }

                // Step 3: Sync the snapshot to the Filer (outside DashMap lock).
                //
                // Append mode: if the buffer grew (size > original_len) and no
                // in-place modification occurred (pure append), send only the
                // delta (data[original_len..]) with is_append=true. The Filer
                // atomically appends it to the current inline_data, preserving
                // other clients' concurrent appends.
                //
                // Overwrite mode: if the buffer was modified in-place or didn't
                // grow (truncate/overwrite), send the full buffer with
                // is_append=false (existing behavior).
                let data = data.unwrap(); // safe: data.is_none() checked above
                let can_append = !mod_in_place && (data.len() > orig_len);

                // Safety net: if the buffer didn't grow (data.len() == orig_len)
                // and no in-place modification occurred, there's nothing new to
                // sync. This can happen when:
                // 1. A concurrent release already synced the data and cleared
                //    dirty, but a race re-set dirty.
                // 2. An empty-buffer release (data.len() == 0, orig_len == 0)
                //    from a delayed FUSE RELEASE of a `touch`/create-without-write
                //    operation. The kernel delays RELEASE callbacks, so the
                //    `touch` command's release may arrive during concurrent
                //    appends from other clients. Syncing size=0 in OVERWRITE
                //    mode would wipe their data (L4.21 root cause).
                //
                // In both cases, skip the sync — the Filer's state is
                // authoritative (no local data was written).
                if !can_append && !mod_in_place && data.len() == orig_len {
                    debug!(
                        "release inline: inode={} no new data to sync (data_len={} == orig_len={}, \
                         mod_in_place={}, skip to avoid overwriting other clients' data)",
                        inode, data.len(), orig_len, mod_in_place
                    );
                    // Clear dirty (concurrent release may have missed it)
                    if let Some(mut inline_buf) = self.inline_buffers.get_mut(&inode) {
                        inline_buf.dirty = false;
                    }
                    break;
                }

                let (sync_data, sync_size, is_append) = if can_append {
                    let delta = data[orig_len..].to_vec();
                    debug!(
                        "release inline: inode={} append mode, orig_len={}, delta_len={}, total_len={}",
                        inode, orig_len, delta.len(), data.len()
                    );
                    (Some(delta), 0u64, true)
                } else {
                    warn!(
                        "release inline: inode={} OVERWRITE mode (is_append=false), mod_in_place={}, \
                         data_len={}, orig_len={} — this may overwrite other clients' data",
                        inode, mod_in_place, data.len(), orig_len
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
                                "release inline: inode={} synced size={} (round {} attempt {})",
                                inode, size, sync_round, attempt
                            );
                            round_ok = true;
                            break;
                        }
                        Ok(resp) => {
                            last_err = resp.error;
                            warn!(
                                "release inline: inode={} round {} attempt {} failed: {}",
                                inode, sync_round, attempt, last_err
                            );
                        }
                        Err(e) => {
                            last_err = e;
                            warn!(
                                "release inline: inode={} round {} attempt {} error: {}",
                                inode, sync_round, attempt, last_err
                            );
                        }
                    }
                    if attempt < max_retries {
                        std::thread::sleep(std::time::Duration::from_millis(
                            500 * (attempt as u64),
                        ));
                    }
                }

                if !round_ok {
                    error!(
                        "release inline: inode={} FAILED after {} attempts: {} — data may be lost",
                        inode, max_retries, last_err
                    );
                    sync_ok = false;
                    // Re-mark as dirty so a future release can retry
                    if let Some(mut inline_buf) = self.inline_buffers.get_mut(&inode) {
                        inline_buf.dirty = true;
                    }
                    break;
                }

                // After a successful append-mode sync, update original_len to
                // the SYNCED length (the snapshot size), NOT the current buffer
                // length. If the buffer grew during sync (concurrent write),
                // original_len must reflect only what was actually sent in the
                // delta, so the re-sync round sends the NEW delta
                // (data[original_len..]) instead of seeing data.len() ==
                // orig_len and falling back to OVERWRITE mode (which would
                // overwrite other clients' data).
                //
                // BUG: previously set to inline_buf.data.len(), which is the
                // CURRENT (possibly grown) buffer length. This caused the
                // re-sync to see data.len() == orig_len → OVERWRITE mode →
                // cross-client data loss (L4.21).
                if is_append {
                    if let Some(mut inline_buf) = self.inline_buffers.get_mut(&inode) {
                        inline_buf.original_len = size as usize;
                    }
                }

                // Step 4: Check if the buffer grew during the sync.
                // Compare current length with synced size. If the buffer grew,
                // a concurrent write happened — re-sync with the updated data.
                let current_len = self
                    .inline_buffers
                    .get(&inode)
                    .map(|b| b.data.len())
                    .unwrap_or(0);

                if current_len as u64 > size {
                    warn!(
                        "release inline: inode={} buffer grew during sync (synced={}, current={}), re-syncing (round {})",
                        inode, size, current_len, sync_round
                    );
                    continue; // Re-sync with the updated buffer
                }

                // Buffer unchanged — mark as not dirty and remove.
                // Use get_mut to atomically clear dirty and check size again.
                let can_remove = {
                    if let Some(mut inline_buf) = self.inline_buffers.get_mut(&inode) {
                        if inline_buf.data.len() as u64 > size {
                            // Buffer grew between the check above and here
                            false
                        } else {
                            inline_buf.dirty = false;
                            true
                        }
                    } else {
                        false // Buffer removed by someone else
                    }
                };

                if !can_remove {
                    warn!(
                        "release inline: inode={} buffer grew between check and remove, re-syncing (round {})",
                        inode, sync_round
                    );
                    continue;
                }

                break;
            }

            // Remove the inline buffer ONLY if this is the last open handle.
            // If other handles are still open (open_count > 0), keeping the
            // buffer allows subsequent writes on those handles to append to
            // the inline data. Removing it prematurely causes the next write
            // to fall through to the Stripe/Flat path, which returns EFBIG
            // for inline files (no fid, no chunks → max_stripe_offset=0).
            // L4.21 failure: concurrent `>>` appends from bash for-loops
            // overlap (FUSE RELEASE is async), so the second OPEN arrives
            // before the first RELEASE completes. The second write then
            // finds no inline buffer and hits the Stripe path's EFBIG.

            // open_count_dec (best-effort, 同 Flat 路径)
            let meta_shard_client = self.client.facade().meta_shard_client().clone();
            let req = powerfs_coherence::OpenCountRequest {
                shard_id: routing_shard,
                inode,
            };
            if let Err(e) = self
                .client
                .block_on(async move { meta_shard_client.open_count_dec(&req).await })
            {
                debug!(
                    "release inline: open_count_dec for inode {} failed (best-effort): {}",
                    inode, e
                );
            }

            // 移除 open_inodes 追踪 + unpin (Inline 无 flush 失败重试, 总是 unpin)
            self.open_inodes.write().unwrap().remove(&inode);
            let released = self.cache.unpin_inode(inode);

            // Only remove the inline buffer if this was the last open handle
            // (released=true). If other handles are still open (released=false),
            // keep the buffer so concurrent writes can continue appending.
            if released {
                self.inline_buffers.remove(&inode);
                self.inline_max_sizes.remove(&inode);
                // L4.21 fix: Invalidate the kernel page cache after the last
                // handle is closed. During concurrent appends, Invalidates from
                // other clients were skipped (inode was Dirty). After release,
                // the kernel page cache still holds this client's own write
                // data, which is stale — it doesn't include other clients'
                // concurrent appends that were synced to the Filer. Without
                // this notification, subsequent reads (e.g., wc -l) return
                // stale line counts from the page cache.
                self.notify_kernel_inval_inode(inode);
                // Mark cache entry as Stale so the next open/getattr
                // refreshes metadata (size) from the Filer.
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

        // Only unpin the inode if flush succeeded. If flush failed, dirty
        // chunks remain and the background flusher needs the inode metadata
        // (fid, volume_id) to retry. Unpinning would let the 30s TTL expire
        // the entry, causing "inode not in cache" errors on every retry cycle.
        // The inode stays pinned until the background flusher successfully
        // writes the data (it will call clear_dirty, and the next release of
        // the file — if reopened — will unpin normally).
        if flush_result.is_ok() {
            self.cache.unpin_inode(inode);
        } else {
            warn!(
                "release: keeping inode {} pinned (flush failed, dirty chunks remain for retry)",
                inode
            );
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
        // 方案 B (S5): 优先用缓存的 shard_id (目录 inode 创建时 Filer 返回的权威值),
        // 缓存 miss 时回退到 calculate_shard_id(inode)。
        let meta_client = self.client.facade().meta_shard_client().clone();
        let shard_id = self.routing_shard(inode);
        let dir_entries: Vec<MetadataDirEntry> = self
            .client
            .block_on(async move { meta_client.readdir(inode, offset, 1000, shard_id).await })
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
                    placement: None,
                    reliability: powerfs_layout::reliability::Reliability::default(),
                    replica_chunks: Vec::new(),
                    shard_id: None,
                    cached_at: Instant::now(),
                    state: EntryState::default(),
                    hold: HoldState::default(),
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
