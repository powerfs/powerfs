//! InvalidateHandler - Processes server-pushed inode invalidation notifications
//!
//! This module implements the `NotificationHandler` trait for the FUSE client,
//! handling `Invalidate` messages from the Filer to maintain cache consistency.

use std::sync::Arc;

use log::{debug, warn};
use powerfs_net::serialize::TlvDecoder;
use powerfs_net::{FieldId, MsgType, NetMessage, NotificationHandler};

use crate::cache::MetadataCache;

/// Handler for server-pushed Invalidate notifications
///
/// On receiving an Invalidate message, checks the cached inode's version
/// and invalidates it if the server's version is newer.
pub struct InvalidateHandler {
    /// Reference to the FUSE client's metadata cache
    cache: Arc<MetadataCache>,
}

impl InvalidateHandler {
    /// Create a new InvalidateHandler with the given metadata cache
    pub fn new(cache: Arc<MetadataCache>) -> Self {
        Self { cache }
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

                // Check if our cached version is stale
                if self.cache.is_inode_stale(inode, version) {
                    debug!(
                        "InvalidateHandler: invalidating stale cache for inode={} (server_v={} > cached_v)",
                        inode, version
                    );
                    self.cache.invalidate_inode(inode);
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
    use crate::cache::CachedEntry;
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

    #[test]
    fn test_invalidate_stale_cache() {
        let cache = Arc::new(MetadataCache::new());
        let inode = cache.allocate_inode();

        cache.insert(make_test_entry(inode, "test.txt", 1));
        assert!(cache.get_inode(inode).is_some());

        // Server sends version=5 (newer than cached 1)
        let handler = InvalidateHandler::new(cache.clone());
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
        let handler = InvalidateHandler::new(cache.clone());
        let msg = make_invalidate_msg(inode, 5);
        handler.handle_notification(&msg);

        // Cache should still be there
        assert!(cache.get_inode(inode).is_some());
    }

    #[test]
    fn test_invalidate_inode_not_in_cache() {
        let cache = Arc::new(MetadataCache::new());
        let handler = InvalidateHandler::new(cache.clone());

        let msg = make_invalidate_msg(99999, 1);
        handler.handle_notification(&msg);

        assert!(cache.get_inode(99999).is_none());
    }

    #[test]
    fn test_invalidate_zero_inode_ignored() {
        let cache = Arc::new(MetadataCache::new());
        let handler = InvalidateHandler::new(cache.clone());

        let msg = make_invalidate_msg(0, 1);
        handler.handle_notification(&msg);
    }

    #[test]
    fn test_non_invalidate_message_ignored() {
        let cache = Arc::new(MetadataCache::new());
        let handler = InvalidateHandler::new(cache.clone());

        let header = FrameHeader::new(
            MsgType::Ping.as_u16(),
            FrameFlags::new(FrameFlags::NOTIFY),
            0,
            0,
        );
        let msg = NetMessage::new(header);

        handler.handle_notification(&msg);
    }
}
