//! InvalidateHandler - Processes server-pushed inode invalidation notifications
//!
//! This module implements the `NotificationHandler` trait for the FUSE client,
//! handling `Invalidate` messages from the Filer to maintain cache consistency.

use std::sync::Arc;

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
pub struct InvalidateHandler {
    /// Reference to the FUSE client's metadata cache
    cache: Arc<MetadataCache>,
    /// Reference to the FUSE client's chunk (data) cache
    chunk_cache: Arc<ChunkCache>,
}

impl InvalidateHandler {
    /// Create a new InvalidateHandler with the given metadata and chunk caches
    pub fn new(cache: Arc<MetadataCache>, chunk_cache: Arc<ChunkCache>) -> Self {
        Self { cache, chunk_cache }
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
                    return;
                }

                // Skip invalidation if the inode has dirty (unflushed) chunks.
                // The dirty data must be preserved, and the flusher needs the
                // cached metadata (specifically the fid) to write the chunks
                // to the volume server. Invalidating the metadata here would
                // cause "inode has no fid" errors in flush_dirty_chunks,
                // leading to EIO and data loss.
                //
                // This race occurs when: create() caches the entry with fid →
                // Filer pushes Invalidate (before open() pins the inode) →
                // cache entry evicted → flusher can't write dirty chunks.
                if self.chunk_cache.has_dirty_chunks(inode) {
                    debug!(
                        "InvalidateHandler: skipping invalidation for inode={} (has dirty chunks, preserving metadata for flush, server_v={})",
                        inode, version
                    );
                    return;
                }

                // Check if our cached version is stale
                if self.cache.is_inode_stale(inode, version) {
                    debug!(
                        "InvalidateHandler: invalidating stale cache for inode={} (server_v={} > cached_v)",
                        inode, version
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
    use powerfs_net::{FrameFlags, FrameHeader};
    use std::collections::HashMap;

    fn make_invalidate_msg(inode: u64, version: u64) -> NetMessage {
        let mut enc = TlvEncoder::new();
        enc.add_u64(FieldId::Ino, inode);
        enc.add_u64(FieldId::Version, version);

        let header = FrameHeader::new(
            MsgType::Invalidate.as_u16(),
            FrameFlags::new(FrameFlags::NOTIFY),
            0,
            0,
        );

        NetMessage::new(header).with_body(enc.into_bytes())
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

        let header = FrameHeader::new(
            MsgType::Ping.as_u16(),
            FrameFlags::new(FrameFlags::NOTIFY),
            0,
            0,
        );
        let msg = NetMessage::new(header);

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

        // After unpin, a subsequent Invalidate should work
        cache.unpin_inode(inode);
        let msg2 = make_invalidate_msg(inode, 5);
        handler.handle_notification(&msg2);
        assert!(cache.get_inode(inode).is_none());
    }
}
