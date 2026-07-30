//! InodeNotifier - Invalidation broadcast for cache consistency
//!
//! This module provides the `InodeNotifier` which manages subscriptions
//! and broadcasts inode invalidation events to connected FUSE clients.
//!
//! ## Architecture
//!
//! ```text
//! FUSE Client A (write)         Filer (InodeNotifier)        FUSE Client B (read)
//!        |                              |                              |
//!        |-- Write complete ---------->|                              |
//!        |                              |-- Invalidate(inode, v) ----->|
//!        |                              |                              |-- Clear cache
//!        |                              |<-------- ACK (optional) -----|
//! ```
//!
//! ## Integration Points
//!
//! - `FilerNetHandler` calls `notify_inode_change()` after metadata mutations
//! - `ServerConnectionManager` provides the notification channel
//! - FUSE clients receive and process Invalidate messages

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use powerfs_net::protocol::{FrameFlags, FrameHeader, MsgType, NetMessage};
use powerfs_net::serialize::TlvEncoder;
use powerfs_net::server_connection::ServerConnectionManager;
use powerfs_net::FieldId;

/// Result type for InodeNotifier operations
pub type NotifyResult<T> = std::result::Result<T, String>;

/// Manages inode subscriptions and broadcasts invalidation notifications
///
/// Thread-safe through RwLock. Integrates with ServerConnectionManager
/// for actual message delivery.
pub struct InodeNotifier {
    /// inode → set of subscribed client_ids
    subscribers: RwLock<HashMap<u64, HashSet<u64>>>,
    /// Reference to the server's connection manager for sending notifications
    connection_manager: Arc<ServerConnectionManager>,
}

impl InodeNotifier {
    /// Create a new InodeNotifier with the given connection manager
    pub fn new(connection_manager: Arc<ServerConnectionManager>) -> Self {
        Self {
            subscribers: RwLock::new(HashMap::new()),
            connection_manager,
        }
    }

    /// Subscribe a client to receive notifications for an inode
    ///
    /// When the inode changes, the client will receive an Invalidate message.
    pub fn subscribe(&self, inode: u64, client_id: u64) {
        let mut subs = self.subscribers.write().unwrap();
        subs.entry(inode).or_default().insert(client_id);
        log::debug!(
            "InodeNotifier: client {} subscribed to inode {}",
            client_id,
            inode
        );
    }

    /// Unsubscribe a client from an inode's notifications
    pub fn unsubscribe(&self, inode: u64, client_id: u64) {
        let mut subs = self.subscribers.write().unwrap();
        if let Some(clients) = subs.get_mut(&inode) {
            clients.remove(&client_id);
            if clients.is_empty() {
                subs.remove(&inode);
            }
        }
        log::debug!(
            "InodeNotifier: client {} unsubscribed from inode {}",
            client_id,
            inode
        );
    }

    /// Unsubscribe a client from all inodes (e.g., on disconnect)
    pub fn unsubscribe_all(&self, client_id: u64) {
        let mut subs = self.subscribers.write().unwrap();
        let mut empty_inodes = Vec::new();
        for (inode, clients) in subs.iter_mut() {
            if clients.remove(&client_id) && clients.is_empty() {
                empty_inodes.push(*inode);
            }
        }
        for inode in empty_inodes {
            subs.remove(&inode);
        }
        log::debug!(
            "InodeNotifier: client {} unsubscribed from all inodes",
            client_id
        );
    }

    /// Notify all subscribers that an inode has changed
    ///
    /// Builds an Invalidate message and sends it to all subscribed clients.
    /// Returns the number of clients notified successfully.
    pub async fn notify(&self, inode: u64, version: u64) -> usize {
        let client_ids: Vec<u64> = {
            let subs = self.subscribers.read().unwrap();
            subs.get(&inode)
                .map(|s| s.iter().copied().collect())
                .unwrap_or_default()
        };

        if client_ids.is_empty() {
            log::debug!("InodeNotifier: no subscribers for inode {}", inode);
            return 0;
        }

        let msg = self.build_invalidate_message(inode, version);
        let mut success_count = 0;

        for client_id in client_ids {
            match self
                .connection_manager
                .send_notification(client_id, msg.clone())
                .await
            {
                Ok(true) => {
                    log::debug!(
                        "InodeNotifier: sent Invalidate(inode={}, v={}) to client {}",
                        inode,
                        version,
                        client_id
                    );
                    success_count += 1;
                }
                Ok(false) => {
                    log::warn!(
                        "InodeNotifier: notification channel full for client {}",
                        client_id
                    );
                }
                Err(e) => {
                    log::warn!(
                        "InodeNotifier: failed to notify client {}: {}",
                        client_id,
                        e
                    );
                    // Client disconnected, clean up subscription
                    self.unsubscribe(inode, client_id);
                }
            }
        }

        success_count
    }

    /// Broadcast an invalidation to all connected clients
    ///
    /// Used for global events like volume reassignment. Returns the number
    /// of clients notified.
    pub async fn broadcast(&self, inode: u64, version: u64) -> usize {
        let msg = self.build_invalidate_message(inode, version);
        let count = self.connection_manager.broadcast_notification(&msg).await;
        log::debug!(
            "InodeNotifier: broadcast Invalidate(inode={}, v={}) to {} clients",
            inode,
            version,
            count
        );
        count
    }

    /// Build an Invalidate message for the given inode and version
    fn build_invalidate_message(&self, inode: u64, version: u64) -> NetMessage {
        let mut enc = TlvEncoder::new();
        enc.add_u64(FieldId::Ino, inode);
        enc.add_u64(FieldId::Version, version);

        let header = FrameHeader::new(
            MsgType::Invalidate.as_u16(),
            FrameFlags::new(FrameFlags::NOTIFY),
            0, // seq is not used for NOTIFY
            0,
        );

        NetMessage::new(header).with_body(enc.into_bytes())
    }

    /// Get the number of subscribers for an inode
    pub fn subscriber_count(&self, inode: u64) -> usize {
        let subs = self.subscribers.read().unwrap();
        subs.get(&inode).map(|s| s.len()).unwrap_or(0)
    }

    /// Get the total number of subscribed inode-client pairs
    pub fn total_subscriptions(&self) -> usize {
        let subs = self.subscribers.read().unwrap();
        subs.values().map(|s| s.len()).sum()
    }

    /// Get the number of unique inodes being watched
    pub fn watched_inode_count(&self) -> usize {
        self.subscribers.read().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use powerfs_net::server_connection::ServerConnectionManager;

    #[test]
    fn test_subscribe_unsubscribe() {
        let mgr = Arc::new(ServerConnectionManager::new());
        let notifier = InodeNotifier::new(mgr);

        notifier.subscribe(1, 100);
        assert_eq!(notifier.subscriber_count(1), 1);

        notifier.subscribe(1, 200);
        assert_eq!(notifier.subscriber_count(1), 2);

        notifier.unsubscribe(1, 100);
        assert_eq!(notifier.subscriber_count(1), 1);

        notifier.unsubscribe(1, 200);
        assert_eq!(notifier.subscriber_count(1), 0);
    }

    #[test]
    fn test_multiple_inodes() {
        let mgr = Arc::new(ServerConnectionManager::new());
        let notifier = InodeNotifier::new(mgr);

        notifier.subscribe(1, 100);
        notifier.subscribe(1, 200);
        notifier.subscribe(2, 100);
        notifier.subscribe(3, 300);

        assert_eq!(notifier.watched_inode_count(), 3);
        assert_eq!(notifier.total_subscriptions(), 4);

        notifier.unsubscribe_all(100);
        assert_eq!(notifier.watched_inode_count(), 2);
        assert_eq!(notifier.total_subscriptions(), 2);
        assert_eq!(notifier.subscriber_count(1), 1); // client 200 still there
    }

    #[test]
    fn test_build_invalidate_message() {
        let mgr = Arc::new(ServerConnectionManager::new());
        let notifier = InodeNotifier::new(mgr);

        let msg = notifier.build_invalidate_message(42, 100);

        assert_eq!(msg.msg_type(), Some(MsgType::Invalidate));
        assert!(msg.header.flags & FrameFlags::NOTIFY != 0);
        assert!(!msg.body.is_empty());
    }
}
