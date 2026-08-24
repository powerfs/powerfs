//! InvalidateHandler - Processes server-pushed inode invalidation notifications
//!
//! This module implements the `NotificationHandler` trait for the FUSE client,
//! handling `Invalidate` messages from the Filer to maintain cache consistency.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, RwLock};

use dashmap::DashMap;
use log::{debug, info, warn};
use powerfs_net::serialize::TlvDecoder;
use powerfs_net::{FieldId, MsgType, NetMessage, NotificationHandler};

use crate::cache::{ChunkCache, MetadataCache};
use crate::client_cap::{process_recall, CapSet, RecallAction};
use crate::fuse::InlineBuffer;

/// Phase 3 Lease Recall: async lease releaser for the notification handler.
///
/// Called from the sync `handle_notification` to release the client's
/// inode lease when the server pushes an Invalidate (recall or content
/// change). The implementation should spawn the release RPC on a
/// tokio runtime so the notification thread is not blocked.
///
/// Without this, the client drops its local lease cache on Invalidate
/// but the server still thinks the client holds the lease, pinning the
/// MetaCache refcount and preventing trim_pass from evicting the entry.
pub trait LeaseReleaser: Send + Sync {
    /// Release the inode lease. `token` is the lease token from the
    /// client's `ClientLeaseState`. Best-effort: failures are logged
    /// and the lease will eventually expire on the server side.
    fn release(&self, inode: u64, token: String);
}

/// Cap model handler — called from the sync `handle_notification` to
/// process `CapRecallNotify` and `CapUpgradeNotify` messages from the
/// Filer (§13 Capability model).
///
/// The implementation is expected to:
/// - For `CapRecallNotify` with dirty CAP_W: flush dirty chunks + sync
///   metadata, then send `CapRecallAck`.
/// - For `CapRecallNotify` without dirty data: send `CapRecallAck`
///   immediately.
/// - For `CapUpgradeNotify`: update the local cap and resume local caching.
///
/// The actual flush logic (drain_dirty_for_inode + write_blob_batch +
/// sync_size_chunks) lives in `PowerFsFs`, injected via the `CapFlusher`
/// trait. This keeps the handler thin and testable while avoiding
/// duplicating the complex flush path.
pub trait CapFlusher: Send + Sync {
    /// Flush all dirty chunks for `inode` to the Volume Server, then sync
    /// metadata (size + chunks) to the Filer via Raft. Returns `Ok(())`
    /// if the flush succeeded, `Err` otherwise.
    ///
    /// Called by `CapHandler::flush_and_ack` before sending `CapRecallAck`.
    /// The `lease_token` is the cap token — passed to the Volume Server
    /// write RPCs so they carry the correct fencing epoch.
    fn flush_and_sync(&self, inode: u64, lease_token: &str) -> std::io::Result<()>;

    /// Deadline-aware variant for the cap recall path.
    ///
    /// The server's GATHER timeout is 2000 ms. The cap handler's outer
    /// `tokio::time::timeout(1750 ms)` wraps the `spawn_blocking` call to
    /// this method. If `deadline` is `Some(t)`, the implementation MUST
    /// check `Instant::now() < t` before each retry sleep in
    /// `sync_size_chunks_on_close` and bail early if exceeded — otherwise
    /// the retry loop (5 × 500 ms incremental sleep = 5 s worst case)
    /// easily blows past the 1750 ms budget, the timeout fires with no
    /// ACK sent, and the server force-reclaims at 2000 ms.
    ///
    /// Default: ignore deadline (delegate to `flush_and_sync`).  Concrete
    /// implementations (`PowerFsFs`) override to thread the deadline into
    /// the retry loop.
    fn flush_and_sync_with_deadline(
        &self,
        inode: u64,
        lease_token: &str,
        deadline: Option<std::time::Instant>,
    ) -> std::io::Result<()> {
        let _ = deadline;
        self.flush_and_sync(inode, lease_token)
    }
}

/// Cap model handler — delegates recall ACK to the Filer via
/// `FuseClientFacade::cap_recall_ack`. Flush is performed by the
/// injected `CapFlusher` (implemented by `PowerFsFs`).
pub trait CapHandler: Send + Sync {
    /// Flush dirty data for `inode` (chunks + metadata) and send
    /// `CapRecallAck` to the Filer. Called when a `CapRecallNotify`
    /// arrives and `RecallAction::FlushThenAck` is returned.
    fn flush_and_ack(&self, inode: u64, token: String, epoch: u64);

    /// Send `CapRecallAck` without flushing (no dirty data). Called
    /// when `RecallAction::ImmediateAck` is returned.
    fn immediate_ack(&self, inode: u64, token: String, epoch: u64);

    /// H5: Eager per-inode recall gate cleanup. Called by
    /// `InvalidateHandler` AFTER it has evicted both the metadata
    /// cache entry and the chunk cache for `inode` (server-pushed
    /// Invalidate EVICT path).
    ///
    /// The per-task reclaim at the end of flush_and_ack /
    /// immediate_ack tasks is the leak-proof HashMap cleanup
    /// (strong_count == 1 → remove slot). This callback is just an
    /// eager optimization — without it, a workload that does lots of
    /// small file invalidations would accumulate empty HashMap slots
    /// until the next recall epoch's task reclaims them, which could
    /// take O(minutes).
    ///
    /// Default impl: no-op (some test handlers don't track slots).
    fn on_inode_evicted(&self, _inode: u64) {}
}

/// Handler for server-pushed Invalidate notifications
///
/// On receiving an Invalidate message, checks the cached inode's version
/// and invalidates it if the server's version is newer.
///
/// Both the metadata cache and the chunk (data) cache are invalidated to
/// avoid serving stale data after another client modifies the file. The
/// Filer pushes a single Invalidate when an inode's metadata (including
/// size/chunks) changes, so the client must drop both caches together.
///
/// # Race condition prevention
///
/// The handler tracks the last-processed (inode, version) pair to suppress
/// duplicate notifications. Without this, a race occurs when:
/// 1. First Invalidate(v=N) arrives → skipped (inode is pinned or has dirty chunks)
/// 2. Flusher drains dirty chunks (`drain_dirty_for_inode`) — `has_dirty_chunks`
///    temporarily returns false
/// 3. Duplicate Invalidate(v=N) arrives → dirty check passes → inode evicted
/// 4. Flusher calls `get_inode(inode)` → None → ENOENT
///
/// Additionally, `has_chunks` (not just `has_dirty_chunks`) is checked to
/// protect the drain window: even after dirty markers are drained, the chunk
/// data still lives in the ChunkCache and the flusher needs the cached fid
/// to write it. Invalidating metadata while chunks exist would orphan them.
pub struct InvalidateHandler {
    /// Reference to the FUSE client's metadata cache
    cache: Arc<MetadataCache>,
    /// Reference to the FUSE client's chunk (data) cache
    chunk_cache: Arc<ChunkCache>,
    /// Reference to the FUSE client's inline buffer map.
    /// Used to clear stale inline buffers when another client modifies
    /// the file, ensuring the next read re-fetches fresh data from the Filer.
    inline_buffers: Arc<DashMap<u64, InlineBuffer>>,
    /// Last-processed version per inode. Used to suppress duplicate
    /// Invalidate notifications for the same (inode, version) pair.
    /// Keyed by inode, value is the server version that was processed.
    processed_versions: Arc<RwLock<HashMap<u64, u64>>>,
    /// Last-processed version per (parent, name) dentry. Used to suppress
    /// duplicate dentry-level kernel invalidations for the same
    /// (parent, name, version) tuple. Ceph-aligned defense-in-depth: even
    /// though the Filer excludes the originating client from the dentry
    /// broadcast, a duplicate message may still arrive on network retry /
    /// Filer re-push. Without this guard each duplicate re-spawns a
    /// `FUSE_NOTIFY_INVAL_ENTRY` worker thread that races with the
    /// in-flight VFS call holding `i_rwsem` on the parent dir.
    /// Key: (parent_ino, name). Value: last processed server version.
    processed_dentry_versions: Arc<RwLock<HashMap<(u64, String), u64>>>,
    /// Raw file descriptor for /dev/fuse, used to send kernel cache
    /// invalidation notifications (FUSE_NOTIFY_INVAL_INODE). Set to -1
    /// until the FUSE session is mounted. This is required because the
    /// InvalidateHandler runs in the notification thread (not the FUSE
    /// worker thread), so it cannot use the Server's notify methods.
    fuse_fd: Arc<AtomicI32>,
    /// Reference to the FUSE client's open_inodes tracker.
    /// Used as a secondary check: even if hold is momentarily Unpinned
    /// (during the window between open_inodes increment and pin_inode),
    /// the InvalidateHandler skips invalidation for inodes that are
    /// tracked as open. This prevents the race where Invalidate arrives
    /// between release's unpin and the next open's pin, causing eviction
    /// of an inode that is being opened (ENOENT in mdtest-hard).
    open_inodes: Arc<RwLock<HashMap<u64, usize>>>,
    /// Reference to the FUSE client's lease state (ClientLeaseState).
    /// When an Invalidate notification arrives for a directory inode, the
    /// corresponding directory lease is cleared to prevent the client from
    /// trusting stale dentry cache via has_valid_dir_lease().
    /// Uses RwLock<Option<...>> so it can be set after construction (the
    /// lock_manager is created after the handler in PowerFsFs::new).
    lease_state: RwLock<Option<Arc<powerfs_lock_fuse::ClientLeaseState>>>,
    /// Phase 3 Lease Recall: async lease releaser. When an Invalidate
    /// arrives for an inode we hold a lease on, this spawns a
    /// ReleaseInodeLease RPC so the server decrements our refcount,
    /// allowing MetaCache trim_pass to evict the entry.
    lease_releaser: RwLock<Option<Arc<dyn LeaseReleaser>>>,
    /// §13 Cap model: async cap handler. Called when `CapRecallNotify`
    /// or `CapUpgradeNotify` arrives to flush+ACK or upgrade the cap.
    /// Set after construction once the FUSE client's flusher is ready.
    ///
    /// Note: cap state lives in `CachedEntry::cap` (via `MetadataCache`
    /// methods). No separate cap store — the cache is the single
    /// source of truth.
    cap_handler: RwLock<Option<Arc<dyn CapHandler>>>,
}

impl InvalidateHandler {
    /// Create a new InvalidateHandler with the given metadata and chunk caches
    pub fn new(
        cache: Arc<MetadataCache>,
        chunk_cache: Arc<ChunkCache>,
        inline_buffers: Arc<DashMap<u64, InlineBuffer>>,
    ) -> Self {
        Self {
            cache,
            chunk_cache,
            inline_buffers,
            processed_versions: Arc::new(RwLock::new(HashMap::new())),
            processed_dentry_versions: Arc::new(RwLock::new(HashMap::new())),
            fuse_fd: Arc::new(AtomicI32::new(-1)),
            open_inodes: Arc::new(RwLock::new(HashMap::new())),
            lease_state: RwLock::new(None),
            lease_releaser: RwLock::new(None),
            cap_handler: RwLock::new(None),
        }
    }

    /// Create a new InvalidateHandler with a shared FUSE file descriptor
    /// for kernel cache invalidation.
    pub fn new_with_fuse_fd(
        cache: Arc<MetadataCache>,
        chunk_cache: Arc<ChunkCache>,
        inline_buffers: Arc<DashMap<u64, InlineBuffer>>,
        fuse_fd: Arc<AtomicI32>,
    ) -> Self {
        Self {
            cache,
            chunk_cache,
            inline_buffers,
            processed_versions: Arc::new(RwLock::new(HashMap::new())),
            processed_dentry_versions: Arc::new(RwLock::new(HashMap::new())),
            fuse_fd,
            open_inodes: Arc::new(RwLock::new(HashMap::new())),
            lease_state: RwLock::new(None),
            lease_releaser: RwLock::new(None),
            cap_handler: RwLock::new(None),
        }
    }

    /// Create a new InvalidateHandler with a shared FUSE file descriptor
    /// and the FUSE client's open_inodes tracker. The open_inodes map
    /// is checked as a secondary guard: even if hold is momentarily
    /// Unpinned (race between release's unpin and the next open's pin),
    /// the handler skips invalidation for inodes tracked as open.
    pub fn new_with_fuse_fd_and_open_inodes(
        cache: Arc<MetadataCache>,
        chunk_cache: Arc<ChunkCache>,
        inline_buffers: Arc<DashMap<u64, InlineBuffer>>,
        fuse_fd: Arc<AtomicI32>,
        open_inodes: Arc<RwLock<HashMap<u64, usize>>>,
    ) -> Self {
        Self {
            cache,
            chunk_cache,
            inline_buffers,
            processed_versions: Arc::new(RwLock::new(HashMap::new())),
            processed_dentry_versions: Arc::new(RwLock::new(HashMap::new())),
            fuse_fd,
            open_inodes,
            lease_state: RwLock::new(None),
            lease_releaser: RwLock::new(None),
            cap_handler: RwLock::new(None),
        }
    }

    /// Set the lease state reference so the handler can clear directory
    /// leases when an Invalidate notification arrives for a directory inode.
    /// Called after construction once the FuseLockManager is available.
    pub fn set_lease_state(&self, state: Arc<powerfs_lock_fuse::ClientLeaseState>) {
        *self.lease_state.write().unwrap() = Some(state);
    }

    /// Phase 3 Lease Recall: set the async lease releaser so the
    /// handler can send `ReleaseInodeLease` RPCs when an Invalidate
    /// arrives for an inode the client holds a lease on. Called
    /// after construction once the FuseLockManager + runtime handle
    /// are available.
    pub fn set_lease_releaser(&self, releaser: Arc<dyn LeaseReleaser>) {
        *self.lease_releaser.write().unwrap() = Some(releaser);
    }

    /// §13 Cap model: set the async cap handler. Called after
    /// construction once the FUSE client's flusher + RPC client are
    /// ready. The handler is invoked when `CapRecallNotify` or
    /// `CapUpgradeNotify` arrives.
    ///
    /// Cap state is no longer stored separately — it lives in
    /// `CachedEntry::cap` (via `MetadataCache`). This handler only
    /// provides the flush+ACK side-effect.
    pub fn set_cap_handler(&self, handler: Arc<dyn CapHandler>) {
        *self.cap_handler.write().unwrap() = Some(handler);
    }

    /// Set the FUSE file descriptor (called after the FUSE session is mounted)
    pub fn set_fuse_fd(&self, fd: i32) {
        self.fuse_fd.store(fd, Ordering::Release);
    }

    /// Send a FUSE_NOTIFY_INVAL_INODE notification to the kernel to
    /// invalidate the page cache for the given inode. This is necessary
    /// because invalidating the FUSE client's internal cache doesn't
    /// automatically invalidate the kernel's page cache — without this,
    /// cross-client reads return stale data cached by the kernel.
    ///
    /// Writes the raw FUSE notification message directly to /dev/fuse
    /// via libc::write(). The message format is:
    ///   fuse_out_header { len=40, error=2 (InvalInode), unique=0 }
    ///   fuse_notify_inval_inode_out { ino, off=0, len=-1 (entire file) }
    fn notify_kernel_inval_inode(&self, inode: u64) {
        let fd = self.fuse_fd.load(Ordering::Acquire);
        if fd < 0 {
            debug!(
                "notify_kernel_inval_inode: fuse_fd not set yet, skipping kernel notification for inode={}",
                inode
            );
            return;
        }

        // Pack the notification message as raw bytes
        // OutHeader: len(4) + error(4) + unique(8) = 16 bytes
        // NotifyInvalInodeOut: ino(8) + off(8) + len(8) = 24 bytes
        // Total: 40 bytes
        let mut buf = [0u8; 40];

        // OutHeader
        buf[0..4].copy_from_slice(&40u32.to_ne_bytes()); // len
        buf[4..8].copy_from_slice(&(-2i32).to_ne_bytes()); // -FUSE_NOTIFY_INVAL_INODE
        buf[8..16].copy_from_slice(&0u64.to_ne_bytes()); // unique = 0

        // NotifyInvalInodeOut
        buf[16..24].copy_from_slice(&inode.to_ne_bytes()); // ino
        buf[24..32].copy_from_slice(&0i64.to_ne_bytes()); // off = 0
        buf[32..40].copy_from_slice(&(-1i64).to_ne_bytes()); // len = -1 (entire file)

        // Write to /dev/fuse — this is safe because the fd is a valid
        // /dev/fuse descriptor and we're writing a well-formed notification.
        let ret = unsafe { libc::write(fd, buf.as_ptr() as *const libc::c_void, 40) };
        if ret < 0 {
            let err = std::io::Error::last_os_error();
            warn!(
                "notify_kernel_inval_inode: write to /dev/fuse failed for inode={}: {} (errno={})",
                inode,
                err,
                err.raw_os_error().unwrap_or(0)
            );
        } else {
            info!(
                "notify_kernel_inval_inode: sent FUSE_NOTIFY_INVAL_INODE for inode={} ({} bytes)",
                inode, ret
            );
        }
    }

    /// Send a FUSE_NOTIFY_INVAL_ENTRY notification to the kernel to
    /// invalidate the dentry cache for a directory entry.
    ///
    /// Send a FUSE_NOTIFY_INVAL_ENTRY notification to the kernel to
    /// invalidate the dentry cache for a directory entry.
    ///
    /// # Deadlock Safety
    ///
    /// This method is called from `handle_notification` (the notification
    /// processing path). FUSE_NOTIFY_INVAL_ENTRY requires the kernel to
    /// acquire `i_rwsem` on the parent directory. If another VFS operation
    /// (e.g. stat, readdir) holds `i_rwsem` and is waiting for a FUSE reply,
    /// calling this synchronously would deadlock:
    ///
    ///   1. VFS op A: holds `i_rwsem` on parent, calls FUSE getattr, waits for reply
    ///   2. InvalidateHandler: receives Invalidate, calls notify_kernel_inval_entry
    ///   3. notify_kernel_inval_entry: write(/dev/fuse) blocks waiting for kernel
    ///      to acquire `i_rwsem` — but it's held by VFS op A
    ///   4. FUSE reply for VFS op A can't be processed because notification
    ///      handler is blocked → deadlock
    ///
    /// Solution: spawn a detached thread that does the write. The thread
    /// doesn't hold any VFS locks, so:
    ///   - If `i_rwsem` is free: write completes immediately, dentry cleared.
    ///   - If `i_rwsem` is held by VFS op A: the thread blocks, but the
    ///     notification handler continues unblocked. VFS op A eventually
    ///     gets its FUSE reply, releases `i_rwsem`, and the thread completes.
    ///
    /// Ceph parallel: CEvent::DentryInvalidate is dispatched via a
    /// separate workqueue, not inline on the notification path.
    fn notify_kernel_inval_entry(&self, parent: u64, name: &str) {
        let fd = self.fuse_fd.load(Ordering::Acquire);
        if fd < 0 {
            debug!(
                "notify_kernel_inval_entry: fuse_fd not set yet, skipping for parent={}, name={}",
                parent, name
            );
            return;
        }

        // Pack the notification message as raw bytes
        // OutHeader: 16 bytes
        // NotifyInvalEntryOut: parent(8) + namelen(4) + padding(4) = 16 bytes
        // name: variable (including null terminator + padding to 8-byte boundary)
        //
        // FUSE kernel ABI (libfuse fuse_lowlevel_notify_inval_entry +
        // fs/fuse/dev.c:fuse_notify_inval_entry):
        //   - error field MUST be -FUSE_NOTIFY_INVAL_ENTRY (= -3). The
        //     kernel dispatches notify messages on `error < 0`. A positive
        //     value is treated as a normal reply with unique=0, which the
        //     kernel discards silently or returns EINVAL. The previous
        //     +3 here was the root cause of T2.3.7 link failure: the
        //     dentry cache was never cleared, so ld stat() returned the
        //     OLD target inode (size=8 magic header) instead of the new
        //     one (size=1516 actual .a).
        //   - The wire data carries namelen+1 bytes (with trailing NUL)
        //     even though outarg.namelen excludes the NUL. The kernel
        //     reads `namelen` bytes and appends its own NUL.
        //   - The whole message is padded to 8-byte alignment per FUSE ABI.
        let name_bytes = name.as_bytes();
        let name_len = name_bytes.len();
        let name_with_null_len = name_len + 1; // +1 for null terminator
                                               // FUSE requires the trailing name blob to be padded to 8-byte alignment
        let name_padded = (name_with_null_len + 7) & !7;
        let total_len = 16 + 16 + name_padded;

        let mut buf = vec![0u8; total_len];

        // OutHeader
        buf[0..4].copy_from_slice(&(total_len as u32).to_ne_bytes()); // len
        buf[4..8].copy_from_slice(&(-3i32).to_ne_bytes()); // -FUSE_NOTIFY_INVAL_ENTRY
        buf[8..16].copy_from_slice(&0u64.to_ne_bytes()); // unique = 0

        // NotifyInvalEntryOut
        buf[16..24].copy_from_slice(&parent.to_ne_bytes()); // parent
        buf[24..28].copy_from_slice(&(name_len as u32).to_ne_bytes()); // namelen (without null)
        buf[28..32].copy_from_slice(&0u32.to_ne_bytes()); // padding

        // name (null-terminated + padding)
        buf[32..32 + name_len].copy_from_slice(name_bytes);
        buf[32 + name_len] = 0; // NUL terminator (kernel reads `namelen` bytes then appends own NUL)
                                // buf[32 + name_len + 1 .. total_len] = padding (already zero)

        // Spawn a detached thread to avoid deadlock in the notification path.
        // See the method-level comment for the full deadlock analysis.
        let fd_owned = fd;
        let parent_owned = parent;
        let name_owned = name.to_string();
        std::thread::spawn(move || {
            let ret =
                unsafe { libc::write(fd_owned, buf.as_ptr() as *const libc::c_void, total_len) };
            if ret < 0 {
                let err = std::io::Error::last_os_error();
                log::warn!(
                    "notify_kernel_inval_entry: write to /dev/fuse failed for parent={}, name={}: {} (errno={})",
                    parent_owned, name_owned, err, err.raw_os_error().unwrap_or(0)
                );
            } else {
                log::info!(
                    "notify_kernel_inval_entry: sent FUSE_NOTIFY_INVAL_ENTRY for parent={}, name={} ({} bytes)",
                    parent_owned, name_owned, ret
                );
            }
        });
    }

    /// Check if this (inode, version) pair has already been processed.
    /// Returns true if the same or a newer version was already seen.
    fn is_duplicate(&self, inode: u64, version: u64) -> bool {
        let processed = self.processed_versions.read().unwrap();
        match processed.get(&inode) {
            Some(&last_seen) => version <= last_seen,
            None => false,
        }
    }

    /// Record that we've processed this (inode, version) pair.
    fn mark_processed(&self, inode: u64, version: u64) {
        let mut processed = self.processed_versions.write().unwrap();
        let entry = processed.entry(inode).or_insert(0);
        if version > *entry {
            *entry = version;
        }
    }

    /// Check if a dentry-level (parent, name, version) invalidation has
    /// already been processed. Returns true if the same or a newer
    /// version was already seen for this (parent, name) pair.
    ///
    /// Defense-in-depth on top of the Filer's `exclude_client_id`
    /// optimization: even with the Filer excluding the originator, a
    /// duplicate dentry notification may still arrive (network retry,
    /// Filer re-push on follower promotion, etc.). Each duplicate would
    /// otherwise re-spawn a `FUSE_NOTIFY_INVAL_ENTRY` worker thread that
    /// races with the in-flight VFS call holding `i_rwsem`.
    fn is_duplicate_dentry(&self, parent_ino: u64, name: &str, version: u64) -> bool {
        let processed = self.processed_dentry_versions.read().unwrap();
        match processed.get(&(parent_ino, name.to_string())) {
            Some(&last_seen) => version <= last_seen,
            None => false,
        }
    }

    /// Record that a dentry-level (parent, name, version) invalidation
    /// has been processed.
    fn mark_dentry_processed(&self, parent_ino: u64, name: &str, version: u64) {
        let mut processed = self.processed_dentry_versions.write().unwrap();
        let entry = processed.entry((parent_ino, name.to_string())).or_insert(0);
        if version > *entry {
            *entry = version;
        }
    }
}

impl NotificationHandler for InvalidateHandler {
    fn handle_notification(&self, msg: &NetMessage) {
        let msg_type = match msg.msg_type() {
            Some(t) => t,
            None => {
                warn!(
                    "InvalidateHandler: received notification with unknown msg_type, flags={:#x}",
                    msg.header.flags
                );
                return;
            }
        };

        match msg_type {
            MsgType::Invalidate => {
                let mut dec = TlvDecoder::new(&msg.body);
                let inode = dec.next_u64(FieldId::Ino).unwrap_or(0);
                let version = dec.next_u64(FieldId::Version).unwrap_or(0);
                // Dentry-level fields (optional — present when the Filer
                // sends notify_dentry_change for rename/unlink/create).
                // When present, we call notify_kernel_inval_entry(parent, name)
                // to clear the kernel VFS dentry cache — not just the
                // userspace dentry lease. This aligns with Ceph's
                // CEvent::DentryInvalidate mechanism.
                let parent_ino = dec.next_u64(FieldId::ParentIno).unwrap_or(0);
                let dentry_name = dec.next_string(FieldId::Name).unwrap_or_default();

                if inode == 0 {
                    warn!("InvalidateHandler: received Invalidate with inode=0, ignoring");
                    return;
                }

                // Dentry-level kernel cache invalidation: if the Filer
                // sent (parent, name), notify the kernel to drop the
                // dentry. This forces the next stat/lookup to re-enter
                // FUSE and get the fresh inode (critical for
                // rename-over-replace where the old inode has stale size).
                //
                // Defense-in-depth: dedup by (parent, name, version) so a
                // duplicate broadcast (network retry, follower re-push)
                // does not re-spawn a `FUSE_NOTIFY_INVAL_ENTRY` worker
                // thread that would race with the in-flight VFS call
                // holding `i_rwsem` on the parent dir. The Filer already
                // excludes the originating client from the broadcast;
                // this is the client-side backstop.
                if parent_ino != 0 && !dentry_name.is_empty() {
                    if self.is_duplicate_dentry(parent_ino, &dentry_name, version) {
                        debug!(
                            "InvalidateHandler: skipping duplicate dentry invalidation (parent={}, name={}, v={})",
                            parent_ino, dentry_name, version
                        );
                    } else {
                        debug!(
                            "InvalidateHandler: notify_kernel_inval_entry(parent={}, name={}) for inode={}, v={}",
                            parent_ino, dentry_name, inode, version
                        );
                        self.notify_kernel_inval_entry(parent_ino, &dentry_name);
                        self.mark_dentry_processed(parent_ino, &dentry_name, version);
                    }
                }

                // Never evict the root inode (inode=1). The root must always
                // be in the cache — evicting it breaks all subsequent path
                // resolutions (create/lookup/getattr on /mnt/fuse/* fail with
                // ENOENT/EIO). Instead of evicting, mark it Stale so the next
                // access refreshes attributes from the Filer.
                if inode == crate::cache::ROOT_INODE {
                    debug!(
                        "InvalidateHandler: refreshing root inode (v={}) instead of evicting",
                        version
                    );
                    // Even for the root directory, notify the kernel to drop
                    // cached directory entries so readdir picks up changes
                    // (create/unlink/rename by other clients) promptly.
                    self.notify_kernel_inval_inode(inode);
                    self.cache.mark_stale(inode);
                    self.mark_processed(inode, version);
                    return;
                }

                debug!(
                    "InvalidateHandler: received Invalidate(inode={}, version={})",
                    inode, version
                );

                // Suppress duplicate notifications for the same (inode, version)
                // pair. The Filer may push multiple Invalidate messages for the
                // same version (e.g., on retry, or because multiple operations
                // triggered the same version bump). Without this guard, the
                // first notification is correctly skipped (pinned/dirty), but a
                // duplicate arriving during the flusher's drain window — when
                // `has_dirty_chunks` temporarily returns false — would evict
                // the inode and cause ENOENT in the write path.
                if self.is_duplicate(inode, version) {
                    debug!(
                        "InvalidateHandler: skipping duplicate Invalidate(inode={}, version={}, already processed)",
                        inode, version
                    );
                    return;
                }

                // ── Dir completeness & dentry lease invalidation ──
                //
                // This MUST happen BEFORE any skip (pinned/dirty/open)
                // check and BEFORE the EVICT branch. The Invalidate
                // notification means "this inode's content/metadata changed
                // on the server". If this inode is a directory, ALL its
                // children's dentry leases (positive AND negative) must be
                // cleared, and dir_complete must be set to false —
                // regardless of whether the inode itself is later evicted,
                // skipped (pinned), or marked stale.
                //
                // Why this matters:
                //   Client A creates `util.h` in directory D.
                //   Filer sends Invalidate(D, v) to all clients.
                //   Client B (and A itself) previously did a full readdir on
                //   D, so `is_dir_complete(D)` is true. A prior lookup of
                //   `util.h.gch` (non-existent) cached a negative result
                //   under the dir_complete umbrella (NegativeComplete path
                //   in check_dentry_lease).
                //   If D is pinned (e.g. it's the cwd or an open dir),
                //   the EVICT branch never runs, so invalidate_dir() and
                //   invalidate_dentry_leases() are never called. The stale
                //   dir_complete=true persists. When gcc opens `util.h`,
                //   lookup sees cache miss + dir_complete=true → returns
                //   NegativeComplete → ENOENT → "fatal error: util.h: No
                //   such file or directory".
                //
                // Ceph parallel: MDS pushes a dirfrag invalidate that
                // unconditionally clears I_COMPLETE and all dentry leases
                // on the affected directory, even if the inode itself is
                // cap-held (pinned). The cap only protects the inode's
                // data/metadata, NOT the directory listing completeness.
                //
                // Safety: calling invalidate_dentry_leases(inode) and
                // invalidate_dir(inode) on a non-directory inode is a
                // no-op (no entries match parent=inode, no dir_cache
                // entry exists).
                self.cache.invalidate_dentry_leases(inode);
                self.cache.invalidate_dir(inode);

                // Skip invalidation for pinned (open) inodes. An open file
                // holds a data lease, so the client's cached metadata/data is
                // authoritative. This also prevents a self-invalidation race:
                // when this client's own setattr triggers an Invalidate, the
                // notification would evict the cache entry the client just
                // updated (update_attr doesn't bump generation), causing
                // ENOENT on the subsequent get_inode. Pinned inodes are
                // refreshed from the Filer on open and synced on close, so
                // skipping invalidation here is safe.
                if self.cache.is_pinned(inode) {
                    debug!(
                        "InvalidateHandler: skipping invalidation for pinned inode={} (open, lease-held, server_v={})",
                        inode, version
                    );
                    self.mark_processed(inode, version);
                    return;
                }

                // Secondary guard: check open_inodes tracker. Even if hold
                // is momentarily Unpinned — during the race window between
                // release's unpin_inode (hold → Unpinned) and the next open's
                // pin_inode (hold → Pinned) — the inode is still tracked in
                // open_inodes. Without this check, the InvalidateHandler
                // would evict the inode mid-open, causing ENOENT when the
                // write path tries to access it (mdtest-hard crash).
                //
                // The open path holds open_inodes.write() while calling
                // pin_inode, but the InvalidateHandler doesn't acquire
                // open_inodes — it only checks inode_cache (for is_pinned).
                // This check closes the gap: if open_inodes has the inode,
                // an open is in progress (or just completed) and the cache
                // will be refreshed by the open path.
                if self.open_inodes.read().unwrap().contains_key(&inode) {
                    debug!(
                        "InvalidateHandler: skipping invalidation for open inode={} (is_pinned=false but open_inodes has it, server_v={}) — race window between release unpin and open pin",
                        inode, version
                    );
                    self.mark_processed(inode, version);
                    return;
                }

                // State machine check: skip invalidation for Dirty/Flushing
                // entries. These have local authoritative data that must not
                // be invalidated (core state machine rule: Dirty/Flushing →
                // Stale is forbidden). This complements the has_chunks check
                // below — once write/setattr paths set Dirty state (Phase 2
                // task p2-4), this becomes the primary guard.
                if let Some(state) = self.cache.get_entry_state(inode) {
                    if state == crate::cache::EntryState::Dirty
                        || state == crate::cache::EntryState::Flushing
                    {
                        debug!(
                            "InvalidateHandler: skipping invalidation for inode={} (state={:?}, local authoritative data, server_v={})",
                            inode, state, version
                        );
                        self.mark_processed(inode, version);
                        return;
                    }
                }

                // Skip invalidation if the inode has DIRTY chunks in the
                // ChunkCache. This protects the flusher's drain window:
                //
                // `flush_dirty_chunks_impl` calls `drain_dirty_for_inode`
                // which removes dirty markers BEFORE writing the chunk data.
                // During this window `has_dirty_chunks` returns false, but
                // the entry state is still Dirty (mark_flushing hasn't been
                // called yet), so the Dirty/Flushing state check above
                // catches this case and skips invalidation.
                //
                // After mark_flushing, the state is Flushing (also caught
                // above). After the flush RPC succeeds and mark_clean is
                // called, the chunks are clean and safe to invalidate.
                //
                // CRITICAL: Do NOT use `has_chunks` here. That returns true
                // for clean chunks from reads, which would skip invalidation
                // when another client modifies the file, causing cross-client
                // stale reads (L4.15 failure: B reads old data after A
                // overwrites the file because B's clean read chunks block
                // invalidation).
                if self.chunk_cache.has_dirty_chunks(inode) {
                    debug!(
                        "InvalidateHandler: skipping invalidation for inode={} (has dirty chunks in cache, preserving for flush, server_v={})",
                        inode, version
                    );
                    self.mark_processed(inode, version);
                    return;
                }

                // Check for dirty inline buffer — same protection as dirty
                // chunks: if the inline buffer has unsynced local data, the
                // local cache is authoritative (we hold the write lease).
                // Skip invalidation to preserve the unsynced data for the
                // upcoming release sync.
                //
                // L4.21 fix: Set needs_refresh=true so the next open() knows
                // to re-fetch from the Filer after the buffer is synced.
                // Without this, the stale-buffer check (entry.size vs
                // buf_len) passes because entry.size was never updated
                // during the skip, and the client reads stale data missing
                // the other client's concurrent appends.
                if let Some(mut inline_buf) = self.inline_buffers.get_mut(&inode) {
                    if inline_buf.dirty {
                        inline_buf.needs_refresh = true;
                        debug!(
                            "InvalidateHandler: skipping invalidation for inode={} (has dirty inline buffer, preserving for release sync, server_v={}) — marked needs_refresh=true",
                            inode, version
                        );
                        self.mark_processed(inode, version);
                        return;
                    }
                }

                // Check if our cached version is stale
                if self.cache.is_inode_stale(inode, version) {
                    // FINAL GUARD: Re-check is_open under read lock and HOLD
                    // the lock through eviction. This prevents a concurrent
                    // open from pinning the inode between the initial is_open
                    // check (above) and the actual eviction. Without this, the
                    // following race causes ENOENT in mdtest-hard:
                    //   1. InvalidateHandler: is_open → false (release removed it)
                    //   2. Open #2: acquires open_inodes.write(), adds inode,
                    //      calls pin_inode (hold → Pinned)
                    //   3. InvalidateHandler: invalidate_inode → evicts
                    //      (was_pinned=true because open #2 just pinned it!)
                    //   4. Write: ENOENT (inode was evicted)
                    //
                    // Holding open_inodes.read() blocks the open path's
                    // open_inodes.write(), so open #2 can't pin until after
                    // eviction completes. Then open #2 re-fetches from Filer.
                    let open_guard = self.open_inodes.read().unwrap();
                    if open_guard.contains_key(&inode) {
                        debug!(
                            "InvalidateHandler: skipping invalidation for open inode={} (re-check before eviction, server_v={}) — inode was opened after initial is_open check",
                            inode, version
                        );
                        self.mark_processed(inode, version);
                        return;
                    }

                    // RACE_TRACE: Log eviction decision with full context.
                    // This is where the inode gets evicted — correlate with
                    // unpin_inode and flush_all_dirty_chunks logs to identify
                    // the race that left the inode unprotected.
                    warn!(
                        "InvalidateHandler EVICT: inode={} server_v={} thread={:?} \
                         — evicting stale cache (check preceding unpin_inode/flush_all_dirty_chunks logs)",
                        inode, version, std::thread::current().id()
                    );
                    // Peek the cached entry to get parent inode and name for
                    // dentry cache invalidation. This must happen BEFORE
                    // invalidate_inode() removes the entry from the cache.
                    if let Some(entry) = self.cache.peek_inode(inode) {
                        // Invalidate the PARENT directory's page cache (readdir
                        // results) so subsequent `ls`/`readdir` on the parent
                        // re-fetches from the Filer. This covers the directory
                        // listing consistency case.
                        //
                        // We intentionally do NOT use FUSE_NOTIFY_INVAL_ENTRY
                        // here. That notification requires the kernel to acquire
                        // i_rwsem (inode_lock) on the parent directory, which
                        // can deadlock when another VFS operation (readdir,
                        // lookup, unlink) already holds i_rwsem and is waiting
                        // for a FUSE reply. The deadlock chain:
                        //   1. VFS op (e.g. readdir) acquires i_rwsem(parent)
                        //   2. VFS op sends FUSE request, waits for reply
                        //   3. Notify thread writes FUSE_NOTIFY_INVAL_ENTRY
                        //   4. Kernel fuse_reverse_inval_entry blocks on
                        //      i_rwsem(parent) → write() never returns
                        //   5. If the FUSE session can't process the pending
                        //      request (e.g. single session thread), the
                        //      system deadlocks permanently.
                        //
                        // FUSE_NOTIFY_INVAL_INODE on the parent does NOT
                        // require i_rwsem — it only invalidates the page cache
                        // — so it is safe. Combined with the short
                        // entry_timeout (100ms, set in fuse.rs), stale dentries
                        // expire quickly even without explicit dentry
                        // invalidation.
                        self.notify_kernel_inval_inode(entry.parent);
                    }
                    // Notify the kernel to drop its page cache for this inode
                    // BEFORE we evict our userspace cache. Without this, the
                    // kernel may continue serving stale page cache to readers
                    // even after we've fetched fresh metadata, causing cross-
                    // client inconsistency (client B reads old data despite
                    // client A having written new data).
                    self.notify_kernel_inval_inode(inode);
                    // Drop both metadata and data caches together: an Invalidate
                    // means the inode's size/chunks changed, so cached file data
                    // may no longer correspond to the current chunks list.
                    self.cache.invalidate_inode(inode);
                    self.chunk_cache.remove_inode_chunks(inode);

                    // H5: Eagerly evict the per-inode recall gate from the
                    // FacadeCapHandler's HashMap. The cache entry is gone, so
                    // the next access will allocate a fresh inode cache
                    // entry — any pending recall task for the OLD generation
                    // will fail the sn/epoch check on the server anyway, so
                    // the slot is dead weight. (Per-task strong_count
                    // reclaim is the leak-proof fallback.)
                    if let Some(handler) = self.cap_handler.read().unwrap().as_ref() {
                        handler.on_inode_evicted(inode);
                    }

                    // Dentry lease invalidation: already done unconditionally
                    // above (before skip checks). No need to repeat here.

                    // Clear any directory lease on this inode: the server
                    // invalidated it (another client modified it), so our
                    // Shared lease is no longer valid. Without this,
                    // has_valid_dir_lease() would return true and cause
                    // lookup/create to bypass RPCs, reading stale dentry cache.
                    //
                    // Phase 3: BEFORE dropping the local lease cache entry,
                    // check if we hold an active inode lease. If so, spawn
                    // a ReleaseInodeLease RPC so the server decrements our
                    // refcount — this is critical for lease recall: without
                    // it, the server's MetaCache refcount stays > 0 and
                    // trim_pass can never evict the entry.
                    if let Some(lease_state) = self.lease_state.read().unwrap().as_ref() {
                        if let Some(entry) = lease_state.get_inode(inode) {
                            // We hold a lease — spawn the release RPC.
                            // The local cache entry is dropped below
                            // regardless (invalidate_inode), so the
                            // token must be captured now.
                            if let Some(releaser) = self.lease_releaser.read().unwrap().as_ref() {
                                debug!(
                                    "InvalidateHandler: releasing inode lease \
                                     for inode={} (token={}...) on Invalidate(v={})",
                                    inode, entry.token, version
                                );
                                releaser.release(inode, entry.token.clone());
                            }
                        }
                        lease_state.invalidate_inode(inode);
                    }
                    // Also clear the inline buffer (if not dirty). The buffer
                    // may contain stale data from a previous read; without
                    // clearing it, the next open would see the buffer and skip
                    // the Filer refresh, serving stale data (L4.21 A视角失败:
                    // A's inline buffer didn't include B's appends).
                    // Dirty buffers are protected by the check above and won't
                    // reach this point.
                    if self.inline_buffers.remove(&inode).is_some() {
                        debug!(
                            "InvalidateHandler: cleared inline buffer for inode={} (server_v={})",
                            inode, version
                        );
                    }
                } else {
                    debug!(
                        "InvalidateHandler: skipping invalidation for inode={} (already fresh, server_v={})",
                        inode, version
                    );
                }

                // Record this version as processed regardless of outcome
                // (invalidated or already-fresh) so subsequent duplicate
                // notifications for the same version are suppressed.
                self.mark_processed(inode, version);
            }
            MsgType::CapRecallNotify => {
                // §13 Cap model: server recalls caps from this client.
                // Notification TLV:
                //   FieldId::Ino (u64)
                //   FieldId::LeaseToken (string)
                //   FieldId::CapSet (u8) — recall bits (low 4 meaningful;
                //                       also duplicated inside the packed field)
                //   FieldId::IsWriteOpen (u8) — PACKED: (recall & 0x0F) in low
                //                               nibble, (retained & 0x0F) in HIGH
                //                               nibble. Same wire format as the
                //                               kernel C client, fixed in the
                //                               companion CapRecallNotify decode.
                //   FieldId::CapEpoch (u64)
                //
                // The retained caps MUST be extracted from the HIGH nibble
                // of IsWriteOpen, NOT from a hypothetical second CapSet
                // field (which doesn't exist and caused `retained_bits=0`
                // before, forcing a full cap teardown on every recall).
                let mut dec = TlvDecoder::new(&msg.body);
                let inode = dec.next_u64(FieldId::Ino).unwrap_or(0);
                let token = dec.next_string(FieldId::LeaseToken).unwrap_or_default();
                let _recall_bits = dec.next_u8(FieldId::CapSet).unwrap_or(0);
                // Retained lives in the HIGH nibble of FieldId::IsWriteOpen
                // (packed: low 4 bits = recall copy, high 4 bits = retained).
                // Same wire format the kernel C client decodes via the
                // companion fix in decode_cap_recall_body().
                let packed = dec.next_u8(FieldId::IsWriteOpen).unwrap_or(0);
                let retained_bits = (packed >> 4) & 0x0F;
                let epoch = dec.next_u64(FieldId::CapEpoch).unwrap_or(0);
                let retained = CapSet(retained_bits);

                if inode == 0 {
                    warn!("InvalidateHandler: CapRecallNotify with inode=0, ignoring");
                    return;
                }

                debug!(
                    "InvalidateHandler: CapRecallNotify inode={} retained={:?} epoch={}",
                    inode, retained, epoch
                );

                // Update the cap state (embedded in CachedEntry::cap) and
                // determine the action: dirty recalled bits move to
                // flushing_caps so the caller knows to flush before ACKing.
                let action = self
                    .cache
                    .with_cap_mut(inode, |cap| process_recall(cap, retained, epoch));

                let handler_guard = self.cap_handler.read().unwrap();
                if let Some(handler) = handler_guard.as_ref() {
                    match action {
                        Some(RecallAction::ImmediateAck) => {
                            // Shared cap, no dirty data — ACK immediately.
                            handler.immediate_ack(inode, token, epoch);
                        }
                        Some(RecallAction::FlushThenAck { flushing_caps: _ }) => {
                            // Exclusive cap with dirty data — flush then ACK.
                            handler.flush_and_ack(inode, token, epoch);
                        }
                        None => {
                            // No local cap record (cap not granted or inode
                            // evicted). ACK immediately so the server doesn't
                            // time out waiting for us — we have nothing to flush.
                            warn!(
                                "InvalidateHandler: CapRecallNotify for inode={} but no local cap — sending immediate ACK",
                                inode
                            );
                            handler.immediate_ack(inode, token, epoch);
                        }
                    }
                } else {
                    warn!(
                        "InvalidateHandler: CapRecallNotify for inode={} but no cap_handler registered — recall will time out on server",
                        inode
                    );
                }
            }
            MsgType::CapUpgradeNotify => {
                // §13 Cap model: server upgrades us back to EXCLUSIVE_WRITE.
                // Notification TLV: Ino + LeaseToken + CapSet(granted) +
                // CapEpoch + SN
                let mut dec = TlvDecoder::new(&msg.body);
                let inode = dec.next_u64(FieldId::Ino).unwrap_or(0);
                let _token = dec.next_string(FieldId::LeaseToken).unwrap_or_default();
                let granted_bits = dec.next_u8(FieldId::CapSet).unwrap_or(0);
                let epoch = dec.next_u64(FieldId::CapEpoch).unwrap_or(0);
                let sn = dec.next_u64(FieldId::CapSn).unwrap_or(0);
                let granted = CapSet(granted_bits);

                if inode == 0 {
                    warn!("InvalidateHandler: CapUpgradeNotify with inode=0, ignoring");
                    return;
                }

                debug!(
                    "InvalidateHandler: CapUpgradeNotify inode={} granted={:?} epoch={} sn={}",
                    inode, granted, epoch, sn
                );

                // Apply the upgrade to the cap embedded in CachedEntry.
                let upgraded = self.cache.with_cap_mut(inode, |cap| {
                    cap.apply_upgrade(granted, epoch, sn);
                });
                if upgraded.is_some() {
                    debug!(
                        "InvalidateHandler: cap upgraded for inode={} — local caching resumed",
                        inode
                    );
                } else {
                    warn!(
                        "InvalidateHandler: CapUpgradeNotify for inode={} but no local cap (evicted or not granted)",
                        inode
                    );
                }
            }
            other => {
                debug!(
                    "InvalidateHandler: ignoring non-Invalidate notification type={:?}",
                    other
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::{CachedEntry, ChunkCache, EntryState, HoldState};
    use powerfs_net::serialize::TlvEncoder;
    use std::collections::HashMap;

    fn make_invalidate_msg(inode: u64, version: u64) -> NetMessage {
        let mut enc = TlvEncoder::new();
        enc.add_u64(FieldId::Ino, inode);
        enc.add_u64(FieldId::Version, version);
        let body = enc.into_bytes();
        NetMessage::notification(MsgType::Invalidate, body, Vec::new())
    }

    fn make_test_entry(inode: u64, name: &str, generation: u64) -> CachedEntry {
        CachedEntry {
            inode,
            parent: 1,
            name: name.to_string(),
            is_dir: false,
            is_symlink: false,
            symlink_target: None,
            nlink: 1,
            fid: None,
            size: 0,
            mode: 0o644,
            uid: 0,
            gid: 0,
            atime: 0,
            mtime: 0,
            ctime: 0,
            xattrs: HashMap::new(),
            chunks: Vec::new(),
            hard_link_id: String::new(),
            hard_link_counter: 0,
            content_size: 0,
            disk_size: 0,
            generation,
            placement: None,
            reliability: powerfs_layout::reliability::Reliability::default(),
            replica_chunks: Vec::new(),
            shard_id: None,
            cached_at: std::time::Instant::now(),
            state: EntryState::default(),
            hold: HoldState::default(),
            cap: None,
            dentry_lease: None,
            dir_shared_gen: 0,
        }
    }

    fn make_chunk_cache() -> Arc<ChunkCache> {
        Arc::new(ChunkCache::with_defaults())
    }

    fn make_inline_buffers() -> Arc<DashMap<u64, InlineBuffer>> {
        Arc::new(DashMap::new())
    }

    #[test]
    fn test_invalidate_stale_cache() {
        let cache = Arc::new(MetadataCache::new());
        let inode = cache.allocate_inode();

        cache.insert(make_test_entry(inode, "test.txt", 1));
        assert!(cache.get_inode(inode).is_some());

        // Server sends version=5 (newer than cached 1)
        let handler =
            InvalidateHandler::new(cache.clone(), make_chunk_cache(), make_inline_buffers());
        let msg = make_invalidate_msg(inode, 5);
        handler.handle_notification(&msg);

        // Cache should be invalidated
        assert!(cache.get_inode(inode).is_none());
    }

    #[test]
    fn test_invalidate_skip_fresh_cache() {
        let cache = Arc::new(MetadataCache::new());
        let inode = cache.allocate_inode();

        cache.insert(make_test_entry(inode, "fresh.txt", 10));

        // Server sends version=5 (older than cached 10)
        let handler =
            InvalidateHandler::new(cache.clone(), make_chunk_cache(), make_inline_buffers());
        let msg = make_invalidate_msg(inode, 5);
        handler.handle_notification(&msg);

        // Cache should still be there
        assert!(cache.get_inode(inode).is_some());
    }

    #[test]
    fn test_invalidate_inode_not_in_cache() {
        let cache = Arc::new(MetadataCache::new());
        let handler =
            InvalidateHandler::new(cache.clone(), make_chunk_cache(), make_inline_buffers());

        let msg = make_invalidate_msg(99999, 1);
        handler.handle_notification(&msg);

        assert!(cache.get_inode(99999).is_none());
    }

    #[test]
    fn test_invalidate_zero_inode_ignored() {
        let cache = Arc::new(MetadataCache::new());
        let handler =
            InvalidateHandler::new(cache.clone(), make_chunk_cache(), make_inline_buffers());

        let msg = make_invalidate_msg(0, 1);
        handler.handle_notification(&msg);
    }

    #[test]
    fn test_invalidate_root_inode_never_evicted() {
        let cache = Arc::new(MetadataCache::new());
        let handler =
            InvalidateHandler::new(cache.clone(), make_chunk_cache(), make_inline_buffers());

        // Root inode (1) is initialized by MetadataCache::new()
        assert!(cache.peek_inode(crate::cache::ROOT_INODE).is_some());

        // Send Invalidate for root with a high version
        let msg = make_invalidate_msg(crate::cache::ROOT_INODE, 99999);
        handler.handle_notification(&msg);

        // Root must still be in the cache (not evicted)
        assert!(
            cache.peek_inode(crate::cache::ROOT_INODE).is_some(),
            "root inode must never be evicted by InvalidateHandler"
        );
    }

    #[test]
    fn test_non_invalidate_message_ignored() {
        let cache = Arc::new(MetadataCache::new());
        let handler =
            InvalidateHandler::new(cache.clone(), make_chunk_cache(), make_inline_buffers());

        let msg = NetMessage::notification(MsgType::Ping, Vec::new(), Vec::new());

        handler.handle_notification(&msg);
    }

    #[test]
    fn test_invalidate_skip_pinned_inode() {
        // An open (pinned) inode must not be invalidated, even if the server
        // version is newer. This prevents a self-invalidation race where the
        // client's own setattr triggers an Invalidate that evicts the entry
        // it just updated (update_attr doesn't bump generation).
        let cache = Arc::new(MetadataCache::new());
        let inode = cache.allocate_inode();

        cache.insert(make_test_entry(inode, "open.txt", 1));
        cache.pin_inode(inode);
        assert!(cache.is_pinned(inode));

        // Server sends version=5 (newer than cached 1) — simulates the
        // Invalidate triggered by this client's own setattr.
        let handler =
            InvalidateHandler::new(cache.clone(), make_chunk_cache(), make_inline_buffers());
        let msg = make_invalidate_msg(inode, 5);
        handler.handle_notification(&msg);

        // Pinned inode should still be in cache
        assert!(cache.get_inode(inode).is_some());

        // After unpin, a subsequent Invalidate with the SAME version is
        // suppressed (duplicate). A newer version is needed to invalidate.
        cache.unpin_inode(inode);
        let msg2 = make_invalidate_msg(inode, 5);
        handler.handle_notification(&msg2);
        assert!(
            cache.get_inode(inode).is_some(),
            "duplicate notification should be suppressed"
        );

        // A newer version should invalidate now
        let msg3 = make_invalidate_msg(inode, 6);
        handler.handle_notification(&msg3);
        assert!(cache.get_inode(inode).is_none());
    }

    #[test]
    fn test_duplicate_notification_suppressed_after_pin() {
        // Simulates the large-file write race condition:
        // 1. First Invalidate(v=5) arrives while pinned → skipped
        // 2. Inode is unpinned (e.g., file closed)
        // 3. Duplicate Invalidate(v=5) arrives → MUST be suppressed
        //    (previously this would evict the inode and cause ENOENT)
        let cache = Arc::new(MetadataCache::new());
        let inode = cache.allocate_inode();

        cache.insert(make_test_entry(inode, "write.bin", 1));
        cache.pin_inode(inode);

        let handler =
            InvalidateHandler::new(cache.clone(), make_chunk_cache(), make_inline_buffers());

        // First notification while pinned → skipped
        handler.handle_notification(&make_invalidate_msg(inode, 5));
        assert!(cache.get_inode(inode).is_some());

        // Unpin (simulates close)
        cache.unpin_inode(inode);

        // Duplicate notification with same version → suppressed
        handler.handle_notification(&make_invalidate_msg(inode, 5));
        assert!(
            cache.get_inode(inode).is_some(),
            "duplicate notification must not evict after unpin"
        );
    }

    #[test]
    fn test_duplicate_notification_suppressed_during_flush_drain() {
        // Simulates the flusher drain race:
        // 1. First Invalidate(v=5) arrives, inode has chunks → skipped
        // 2. Flusher drains dirty markers (has_dirty_chunks → false),
        //    but chunk data still exists (has_chunks → true)
        // 3. Duplicate Invalidate(v=5) arrives → suppressed by has_chunks
        //    AND by duplicate version tracking
        use bytes::Bytes;

        let cache = Arc::new(MetadataCache::new());
        let chunk_cache = Arc::new(ChunkCache::with_defaults());
        let inode = cache.allocate_inode();

        cache.insert(make_test_entry(inode, "flush.bin", 1));

        // Add a chunk to the cache (simulates dirty data).
        // `put` automatically marks the chunk as dirty.
        chunk_cache.put(inode, 0, Bytes::from(vec![0u8; 4096]), 0, 0);
        assert!(chunk_cache.has_chunks(inode));
        assert!(chunk_cache.has_dirty_chunks(inode));

        let handler =
            InvalidateHandler::new(cache.clone(), chunk_cache.clone(), make_inline_buffers());

        // First notification → skipped (has chunks)
        handler.handle_notification(&make_invalidate_msg(inode, 5));
        assert!(cache.get_inode(inode).is_some());

        // Simulate drain: clear dirty markers but keep chunk data
        chunk_cache.clear_dirty(inode);
        assert!(!chunk_cache.has_dirty_chunks(inode));
        assert!(chunk_cache.has_chunks(inode));

        // Duplicate notification → still skipped (has_chunks + duplicate)
        handler.handle_notification(&make_invalidate_msg(inode, 5));
        assert!(
            cache.get_inode(inode).is_some(),
            "inode must not be evicted during flush drain window"
        );
    }
}
