//! Event handler trait for server-pushed lock notifications.
//!
//! Implemented by clients (FUSE userspace and kernel) to receive Early
//! Revoke and cache invalidation notifications from the server. These
//! callbacks are the foundation of the §5.2 Early Revoke optimization:
//! the server proactively notifies a lease holder before its TTL expires
//! so the holder can flush dirty data and release early, letting the next
//! queued client acquire without waiting for the full TTL + grace.

use crate::types::Range;

/// Handler for server-pushed lock events.
///
/// Implementors must be non-blocking: the server transport layer
/// (`CHANNEL_LOCK`, see §8.4) calls these on its dedicated lock thread pool
/// and must not be stalled by slow client callbacks. Long work (e.g.,
/// `filemap_fdatawrite_range` in the kernel client) should be offloaded.
pub trait LockEventHandler: Send + Sync {
    /// Called when the server revokes a lease early (§5.2 Early Revoke).
    ///
    /// `token` identifies the lease being revoked. The client should:
    /// 1. Stop issuing new writes under that lease immediately.
    /// 2. Flush any dirty data covered by the lease.
    /// 3. ACK the revoke (via `LockManager::release` or a dedicated ACK RPC).
    ///
    /// If the client does not ACK within the server's revocation timeout
    /// (§8.3 "Revoke after 2s no ACK → mark unresponsive → force reclaim"),
    /// the server will force-release the lease and penalize the client's
    /// health score.
    fn on_revoke(&self, inode: u64, token: &str);

    /// Called when the server invalidates cached data/metadata for an inode.
    ///
    /// - `range == None`: the whole inode (data + metadata) is stale.
    /// - `range == Some(r)`: only the byte range `[r.start, r.end)` is stale.
    ///
    /// The client must drop the corresponding page cache / dentry cache
    /// entries before serving subsequent reads. In the kernel client this
    /// maps to `invalidate_inode_pages2_range`; in FUSE userspace it maps
    /// to `FUSE_NOTIFY_INVAL_INODE` handling.
    fn on_invalidate(&self, inode: u64, range: Option<Range>);
}
