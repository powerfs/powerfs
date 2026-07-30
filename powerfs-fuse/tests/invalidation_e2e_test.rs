//! End-to-end Invalidation Tests
//!
//! These tests validate the complete notification pipeline:
//! Server (Filer) pushes Invalidate → Client (FUSE) receives → Cache invalidated
//!
//! Architecture under test:
//!   InodeNotifier → ServerConnectionManager → mpsc channel → NotificationHandler → MetadataCache

use std::sync::Arc;
use std::time::Instant;

use powerfs_fuse::cache::{CachedEntry, MetadataCache};
use powerfs_fuse::invalidate_handler::InvalidateHandler;
use powerfs_net::protocol::{ClientType, FrameFlags, FrameHeader, MsgType, NetMessage};
use powerfs_net::serialize::TlvEncoder;
use powerfs_net::server_connection::ServerConnectionManager;
use powerfs_net::{FieldId, NotificationHandler};

fn make_entry(inode: u64, parent: u64, name: &str, generation: u64) -> CachedEntry {
    CachedEntry {
        inode,
        parent,
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
        xattrs: std::collections::HashMap::new(),
        chunks: Vec::new(),
        hard_link_id: String::new(),
        hard_link_counter: 0,
        content_size: 0,
        disk_size: 0,
        generation,
        cached_at: Instant::now(),
    }
}

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

/// Helper: simulate the server→client push pipeline and client-side processing
async fn simulate_server_push(
    mgr: &ServerConnectionManager,
    client_id: u64,
    inode: u64,
    version: u64,
    handler: &InvalidateHandler,
    rx: &mut tokio::sync::mpsc::Receiver<NetMessage>,
) -> usize {
    let msg = make_invalidate_msg(inode, version);
    mgr.send_notification(client_id, msg).await.unwrap();

    let mut processed_count = 0;
    while let Ok(received) = rx.try_recv() {
        handler.handle_notification(&received);
        processed_count += 1;
    }
    processed_count
}

#[tokio::test]
async fn test_invalidation_e2e_single_client_cache_cleared() {
    // Setup: Server + Client
    let mgr = Arc::new(ServerConnectionManager::new());
    let cache = Arc::new(MetadataCache::new());
    let handler = InvalidateHandler::new(cache.clone());

    let client_id: u64 = 1001;
    let addr: std::net::SocketAddr = "127.0.0.1:9001".parse().unwrap();
    mgr.register_session(client_id, ClientType::Fuse, addr).await;
    let mut rx = mgr.register_notification_channel(client_id).await;

    // Client B has cached entry for inode 100 with generation=1
    cache.insert(make_entry(100, 1, "test.txt", 1));
    assert!(cache.get_inode(100).is_some(), "Entry should exist before invalidation");

    // Simulate server push with newer version (generation=5)
    let processed = simulate_server_push(&mgr, client_id, 100, 5, &handler, &mut rx).await;
    assert_eq!(processed, 1, "Should process exactly one notification");

    // After invalidation: cache should be cleared because server_v(5) > cached_v(1)
    assert!(
        cache.get_inode(100).is_none(),
        "Entry should be invalidated after newer-version notification"
    );
}

#[tokio::test]
async fn test_invalidation_e2e_older_version_ignored() {
    let mgr = Arc::new(ServerConnectionManager::new());
    let cache = Arc::new(MetadataCache::new());
    let handler = InvalidateHandler::new(cache.clone());

    let client_id: u64 = 1002;
    let addr: std::net::SocketAddr = "127.0.0.1:9002".parse().unwrap();
    mgr.register_session(client_id, ClientType::Fuse, addr).await;
    let mut rx = mgr.register_notification_channel(client_id).await;

    // Client has up-to-date cache (generation=10)
    cache.insert(make_entry(200, 1, "fresh.txt", 10));
    assert!(cache.get_inode(200).is_some());

    // Server sends stale notification (generation=5 < 10)
    let processed = simulate_server_push(&mgr, client_id, 200, 5, &handler, &mut rx).await;
    assert_eq!(processed, 1);

    // Cache should NOT be invalidated because server version is older
    assert!(
        cache.get_inode(200).is_some(),
        "Entry should survive older-version notification"
    );
}

#[tokio::test]
async fn test_invalidation_e2e_multiple_clients_isolation() {
    let mgr = Arc::new(ServerConnectionManager::new());

    let cache_a = Arc::new(MetadataCache::new());
    let cache_b = Arc::new(MetadataCache::new());
    let handler_a = InvalidateHandler::new(cache_a.clone());
    let handler_b = InvalidateHandler::new(cache_b.clone());

    // Register two clients
    let client_a: u64 = 2001;
    let client_b: u64 = 2002;
    let addr_a: std::net::SocketAddr = "127.0.0.1:9101".parse().unwrap();
    let addr_b: std::net::SocketAddr = "127.0.0.1:9102".parse().unwrap();
    mgr.register_session(client_a, ClientType::Fuse, addr_a).await;
    mgr.register_session(client_b, ClientType::Fuse, addr_b).await;
    let mut rx_a = mgr.register_notification_channel(client_a).await;
    let mut rx_b = mgr.register_notification_channel(client_b).await;

    // Both clients cache the same inode (300)
    cache_a.insert(make_entry(300, 1, "shared.txt", 1));
    cache_b.insert(make_entry(300, 1, "shared.txt", 1));

    // Notify only Client A about inode 300 change
    let processed_a =
        simulate_server_push(&mgr, client_a, 300, 2, &handler_a, &mut rx_a).await;
    assert_eq!(processed_a, 1);

    // Client A's cache should be cleared
    assert!(cache_a.get_inode(300).is_none(), "Client A should be invalidated");
    // Client B's cache should remain untouched
    assert!(
        cache_b.get_inode(300).is_some(),
        "Client B should NOT be affected"
    );

    // Now notify Client B as well
    let processed_b =
        simulate_server_push(&mgr, client_b, 300, 2, &handler_b, &mut rx_b).await;
    assert_eq!(processed_b, 1);
    assert!(cache_b.get_inode(300).is_none(), "Client B should now be invalidated too");
}

#[tokio::test]
async fn test_invalidation_e2e_missing_channel_error() {
    let mgr = Arc::new(ServerConnectionManager::new());

    // Send notification to a client that has no channel registered
    let result = mgr.send_notification(9999, make_invalidate_msg(1, 1)).await;
    assert!(result.is_err(), "Should fail for unknown client");
}

#[tokio::test]
async fn test_invalidation_e2e_broadcast_all_clients() {
    let mgr = Arc::new(ServerConnectionManager::new());

    let cache_a = Arc::new(MetadataCache::new());
    let cache_b = Arc::new(MetadataCache::new());
    let handler_a = InvalidateHandler::new(cache_a.clone());
    let handler_b = InvalidateHandler::new(cache_b.clone());

    let client_a: u64 = 3001;
    let client_b: u64 = 3002;
    mgr.register_session(client_a, ClientType::Fuse, "127.0.0.1:9201".parse().unwrap()).await;
    mgr.register_session(client_b, ClientType::Fuse, "127.0.0.1:9202".parse().unwrap()).await;
    let mut rx_a = mgr.register_notification_channel(client_a).await;
    let mut rx_b = mgr.register_notification_channel(client_b).await;

    cache_a.insert(make_entry(400, 1, "broadcast.txt", 1));
    cache_b.insert(make_entry(400, 1, "broadcast.txt", 1));

    // Broadcast to all
    let msg = make_invalidate_msg(400, 2);
    let count = mgr.broadcast_notification(&msg).await;
    assert_eq!(count, 2, "Broadcast should reach both clients");

    // Process notifications on both sides
    while let Ok(received) = rx_a.try_recv() {
        handler_a.handle_notification(&received);
    }
    while let Ok(received) = rx_b.try_recv() {
        handler_b.handle_notification(&received);
    }

    // Both caches should be invalidated
    assert!(cache_a.get_inode(400).is_none());
    assert!(cache_b.get_inode(400).is_none());
}

#[tokio::test]
async fn test_invalidation_e2e_zero_inode_ignored() {
    let mgr = Arc::new(ServerConnectionManager::new());
    let cache = Arc::new(MetadataCache::new());
    let handler = InvalidateHandler::new(cache.clone());

    let client_id: u64 = 4001;
    mgr.register_session(client_id, ClientType::Fuse, "127.0.0.1:9301".parse().unwrap()).await;
    let mut rx = mgr.register_notification_channel(client_id).await;

    // Send notification with inode=0 (should be ignored by handler)
    let processed = simulate_server_push(&mgr, client_id, 0, 5, &handler, &mut rx).await;
    assert_eq!(processed, 1, "Message should be delivered to channel");

    // Verify nothing broke (handler just logs warnings for inode=0)
}

#[tokio::test]
async fn test_invalidation_e2e_multiple_inodes() {
    let mgr = Arc::new(ServerConnectionManager::new());
    let cache = Arc::new(MetadataCache::new());
    let handler = InvalidateHandler::new(cache.clone());

    let client_id: u64 = 5001;
    mgr.register_session(client_id, ClientType::Fuse, "127.0.0.1:9401".parse().unwrap()).await;
    let mut rx = mgr.register_notification_channel(client_id).await;

    // Cache multiple inodes
    cache.insert(make_entry(100, 1, "file1.txt", 1));
    cache.insert(make_entry(200, 1, "file2.txt", 1));
    cache.insert(make_entry(300, 1, "file3.txt", 1));

    // Invalidate only inode 100 and 300
    let msg1 = make_invalidate_msg(100, 2);
    let msg3 = make_invalidate_msg(300, 2);
    mgr.send_notification(client_id, msg1).await.unwrap();
    mgr.send_notification(client_id, msg3).await.unwrap();

    // Process all pending
    let mut processed = 0;
    while let Ok(received) = rx.try_recv() {
        handler.handle_notification(&received);
        processed += 1;
    }
    assert_eq!(processed, 2);

    // Verify: 100 and 300 invalidated, 200 still cached
    assert!(cache.get_inode(100).is_none());
    assert!(cache.get_inode(200).is_some());
    assert!(cache.get_inode(300).is_none());
}

#[tokio::test]
async fn test_invalidation_e2e_idempotent_same_version() {
    let mgr = Arc::new(ServerConnectionManager::new());
    let cache = Arc::new(MetadataCache::new());
    let handler = InvalidateHandler::new(cache.clone());

    let client_id: u64 = 6001;
    mgr.register_session(client_id, ClientType::Fuse, "127.0.0.1:9501".parse().unwrap()).await;
    let mut rx = mgr.register_notification_channel(client_id).await;

    cache.insert(make_entry(500, 1, "stable.txt", 5));

    // Send notification with same version (5 == 5)
    let processed = simulate_server_push(&mgr, client_id, 500, 5, &handler, &mut rx).await;
    assert_eq!(processed, 1);

    // Cache should NOT be invalidated (not stale, same version)
    assert!(cache.get_inode(500).is_some());
}
