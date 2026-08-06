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

use powerfs_net::server_connection::ServerConnectionManager;

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
    /// Pushes an Invalidate(inode, version) notification to each subscribed
    /// client via the ServerConnectionManager. The message is built inside
    /// the net layer, so this module no longer depends on `protocol` or
    /// `serialize` internals.
    ///
    /// Returns the number of clients notified successfully.
    pub fn notify(&self, inode: u64, version: u64) -> usize {
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

        let mut success_count = 0;

        for client_id in client_ids {
            match self
                .connection_manager
                .push_invalidate_notification(client_id, inode, version)
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
    pub fn broadcast(&self, inode: u64, version: u64) -> usize {
        let count = self
            .connection_manager
            .broadcast_invalidate_notification(inode, version);
        log::debug!(
            "InodeNotifier: broadcast Invalidate(inode={}, v={}) to {} clients",
            inode,
            version,
            count
        );
        count
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
    use powerfs_net::client_conn::ConnRegistry;
    use powerfs_net::server_connection::ServerConnectionManager;

    fn make_manager() -> Arc<ServerConnectionManager> {
        Arc::new(ServerConnectionManager::new(Arc::new(ConnRegistry::new())))
    }

    #[test]
    fn test_subscribe_unsubscribe() {
        let mgr = make_manager();
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
        let mgr = make_manager();
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
    fn test_notify_no_subscribers() {
        let mgr = make_manager();
        let notifier = InodeNotifier::new(mgr);

        // No subscribers → notify is a no-op returning 0.
        assert_eq!(notifier.notify(42, 100), 0);
        assert_eq!(notifier.broadcast(42, 100), 0);
    }

    #[test]
    fn test_notify_drops_disconnected_subscriber() {
        let mgr = make_manager();
        let notifier = InodeNotifier::new(mgr);

        // Subscribe a client that has no live notification channel. The
        // high-level push_invalidate_notification returns Err, and notify
        // should clean up the stale subscription.
        notifier.subscribe(42, 100);
        assert_eq!(notifier.subscriber_count(42), 1);

        let delivered = notifier.notify(42, 100);
        assert_eq!(delivered, 0);
        // Stale subscription should have been removed by the Err path.
        assert_eq!(notifier.subscriber_count(42), 0);
    }
}
