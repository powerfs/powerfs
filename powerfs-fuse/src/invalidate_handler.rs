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
use crate::fuse::InlineBuffer;

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
    /// Raw file descriptor for /dev/fuse, used to send kernel cache
    /// invalidation notifications (FUSE_NOTIFY_INVAL_INODE). Set to -1
    /// until the FUSE session is mounted. This is required because the
    /// InvalidateHandler runs in the notification thread (not the FUSE
    /// worker thread), so it cannot use the Server's notify methods.
    fuse_fd: Arc<AtomicI32>,
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
            fuse_fd: Arc::new(AtomicI32::new(-1)),
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
            fuse_fd,
        }
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
        buf[4..8].copy_from_slice(&2i32.to_ne_bytes()); // error = NotifyOpcode::InvalInode
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
    /// # WARNING: DEADLOCK RISK — DO NOT CALL FROM NOTIFICATION PATH
    ///
    /// This method is retained for reference but is NOT called from
    /// `handle_notification`. FUSE_NOTIFY_INVAL_ENTRY requires the kernel
    /// to acquire `i_rwsem` (inode_lock) on the parent directory, which
    /// deadlocks when another VFS operation holds `i_rwsem` and is waiting
    /// for a FUSE reply. See the comment in `handle_notification` for the
    /// full deadlock chain.
    ///
    /// Use `notify_kernel_inval_inode` on the parent directory instead,
    /// which invalidates the page cache without acquiring `i_rwsem`.
    #[allow(dead_code)]
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
        // name: variable (including null terminator)
        let name_bytes = name.as_bytes();
        let name_with_null_len = name_bytes.len() + 1; // +1 for null terminator
        let total_len = 16 + 16 + name_with_null_len;

        let mut buf = vec![0u8; total_len];

        // OutHeader
        buf[0..4].copy_from_slice(&(total_len as u32).to_ne_bytes()); // len
        buf[4..8].copy_from_slice(&3i32.to_ne_bytes()); // error = NotifyOpcode::InvalEntry
        buf[8..16].copy_from_slice(&0u64.to_ne_bytes()); // unique = 0

        // NotifyInvalEntryOut
        buf[16..24].copy_from_slice(&parent.to_ne_bytes()); // parent
        buf[24..28].copy_from_slice(&(name_bytes.len() as u32).to_ne_bytes()); // namelen (without null)
        buf[28..32].copy_from_slice(&0u32.to_ne_bytes()); // padding

        // name (null-terminated)
        buf[32..32 + name_bytes.len()].copy_from_slice(name_bytes);
        // null terminator is already 0 from vec initialization

        let ret = unsafe { libc::write(fd, buf.as_ptr() as *const libc::c_void, total_len) };
        if ret < 0 {
            let err = std::io::Error::last_os_error();
            warn!(
                "notify_kernel_inval_entry: write to /dev/fuse failed for parent={}, name={}: {} (errno={})",
                parent, name, err, err.raw_os_error().unwrap_or(0)
            );
        } else {
            info!(
                "notify_kernel_inval_entry: sent FUSE_NOTIFY_INVAL_ENTRY for parent={}, name={} ({} bytes)",
                parent, name, ret
            );
        }
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

                if inode == 0 {
                    warn!("InvalidateHandler: received Invalidate with inode=0, ignoring");
                    return;
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
