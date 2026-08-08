//! InvalidateHandler - Processes server-pushed inode invalidation notifications
//!
//! This module implements the `NotificationHandler` trait for the FUSE client,
//! handling `Invalidate` messages from the Filer to maintain cache consistency.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use log::{debug, warn};
use powerfs_net::serialize::TlvDecoder;
use powerfs_net::{FieldId, MsgType, NetMessage, NotificationHandler};

use crate::cache::{ChunkCache, MetadataCache};

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
    /// Last-processed version per inode. Used to suppress duplicate
    /// Invalidate notifications for the same (inode, version) pair.
    /// Keyed by inode, value is the server version that was processed.
    processed_versions: Arc<RwLock<HashMap<u64, u64>>>,
}

impl InvalidateHandler {
    /// Create a new InvalidateHandler with the given metadata and chunk caches
    pub fn new(cache: Arc<MetadataCache>, chunk_cache: Arc<ChunkCache>) -> Self {
        Self {
            cache,
            chunk_cache,
            processed_versions: Arc::new(RwLock::new(HashMap::new())),
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

                // Skip invalidation if the inode has ANY chunks (dirty or clean)
                // in the ChunkCache. This is broader than checking only dirty
                // chunks to protect the flusher's drain window:
                //
                // `flush_dirty_chunks_impl` calls `drain_dirty_for_inode` which
                // removes dirty markers BEFORE writing the chunk data. During
                // this window `has_dirty_chunks` returns false, but the chunk
                // data and the cached fid are still needed. If we invalidated
                // metadata here, the flusher would hit ENOENT or "no fid" EIO.
                //
                // Checking `has_chunks` covers both:
                // - Dirty chunks not yet drained (normal write path)
                // - Drained chunks still being written (flush race window)
                // - Clean chunks from recent reads (fid needed for future ops)
                if self.chunk_cache.has_chunks(inode) {
                    debug!(
                        "InvalidateHandler: skipping invalidation for inode={} (has chunks in cache, preserving metadata for flush, server_v={})",
                        inode, version
                    );
                    self.mark_processed(inode, version);
                    return;
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
                    // Drop both metadata and data caches together: an Invalidate
                    // means the inode's size/chunks changed, so cached file data
                    // may no longer correspond to the current chunks list.
                    self.cache.invalidate_inode(inode);
                    self.chunk_cache.remove_inode_chunks(inode);
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
    use crate::cache::{CachedEntry, ChunkCache};
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
            cached_at: std::time::Instant::now(),
        }
    }

    fn make_chunk_cache() -> Arc<ChunkCache> {
        Arc::new(ChunkCache::with_defaults())
    }

    #[test]
    fn test_invalidate_stale_cache() {
        let cache = Arc::new(MetadataCache::new());
        let inode = cache.allocate_inode();

        cache.insert(make_test_entry(inode, "test.txt", 1));
        assert!(cache.get_inode(inode).is_some());

        // Server sends version=5 (newer than cached 1)
        let handler = InvalidateHandler::new(cache.clone(), make_chunk_cache());
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
        let handler = InvalidateHandler::new(cache.clone(), make_chunk_cache());
        let msg = make_invalidate_msg(inode, 5);
        handler.handle_notification(&msg);

        // Cache should still be there
        assert!(cache.get_inode(inode).is_some());
    }

    #[test]
    fn test_invalidate_inode_not_in_cache() {
        let cache = Arc::new(MetadataCache::new());
        let handler = InvalidateHandler::new(cache.clone(), make_chunk_cache());

        let msg = make_invalidate_msg(99999, 1);
        handler.handle_notification(&msg);

        assert!(cache.get_inode(99999).is_none());
    }

    #[test]
    fn test_invalidate_zero_inode_ignored() {
        let cache = Arc::new(MetadataCache::new());
        let handler = InvalidateHandler::new(cache.clone(), make_chunk_cache());

        let msg = make_invalidate_msg(0, 1);
        handler.handle_notification(&msg);
    }

    #[test]
    fn test_non_invalidate_message_ignored() {
        let cache = Arc::new(MetadataCache::new());
        let handler = InvalidateHandler::new(cache.clone(), make_chunk_cache());

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
        let handler = InvalidateHandler::new(cache.clone(), make_chunk_cache());
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

        let handler = InvalidateHandler::new(cache.clone(), make_chunk_cache());

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

        let handler = InvalidateHandler::new(cache.clone(), chunk_cache.clone());

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
