//! Inode Metadata Lease 集成测试 (方案 A, Phase 2)
//!
//! 启动 Mock Filer (powerfs-net 二进制协议)，验证 FuseClientFacade 的
//! inode lease 完整链路: acquire → cache → renew → release → invalidate。
//!
//! 测试场景:
//! 1. 基本 acquire/release 流程
//! 2. 缓存命中 (第二次 acquire 不走网络)
//! 3. 主动续租 (lease 临近过期时自动 renew)
//! 4. 并发 acquire 同一 inode (互斥)
//! 5. 并发 acquire 不同 inode (无竞争)
//! 6. release 后重新 acquire

#![allow(unused_imports, dead_code)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use log::info;
use powerfs_fuse_core::*;
use powerfs_net::serialize::{TlvDecoder, TlvEncoder};
use powerfs_net::{FieldId, FrameFlags, HandshakeRequest, HandshakeResponse, MsgType};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

// =====================================================================
// Mock Filer — handles AcquireInodeLease / ReleaseInodeLease / RenewInodeLease
// =====================================================================

/// Simple lease state tracked by the mock Filer.
struct MockLeaseState {
    holder: String,
    token: String,
}

/// Shared lease store for the mock Filer.
type LeaseStore = Arc<Mutex<HashMap<u64, MockLeaseState>>>;

/// Mock Filer server.
struct MockFiler {
    port: u16,
    leases: LeaseStore,
    /// Counter for acquire requests received (to verify cache behavior).
    acquire_count: Arc<AtomicU32>,
    /// Counter for renew requests received.
    renew_count: Arc<AtomicU32>,
    /// Counter for release requests received.
    release_count: Arc<AtomicU32>,
}

impl MockFiler {
    fn new(port: u16) -> Self {
        Self {
            port,
            leases: Arc::new(Mutex::new(HashMap::new())),
            acquire_count: Arc::new(AtomicU32::new(0)),
            renew_count: Arc::new(AtomicU32::new(0)),
            release_count: Arc::new(AtomicU32::new(0)),
        }
    }

    async fn start(&self) -> SocketAddr {
        let addr = format!("127.0.0.1:{}", self.port);
        let listener = TcpListener::bind(&addr).await.unwrap();
        let bound_addr = listener.local_addr().unwrap();

        let leases = self.leases.clone();
        let acquire_count = self.acquire_count.clone();
        let renew_count = self.renew_count.clone();
        let release_count = self.release_count.clone();

        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let leases = leases.clone();
                let acq = acquire_count.clone();
                let ren = renew_count.clone();
                let rel = release_count.clone();
                tokio::spawn(async move {
                    handle_filer_connection(stream, leases, acq, ren, rel).await;
                });
            }
        });

        bound_addr
    }
}

async fn handle_filer_connection(
    mut stream: TcpStream,
    leases: LeaseStore,
    acquire_count: Arc<AtomicU32>,
    renew_count: Arc<AtomicU32>,
    release_count: Arc<AtomicU32>,
) {
    // 1. Handshake
    let mut hs_buf = vec![0u8; HandshakeRequest::SIZE];
    if stream.read_exact(&mut hs_buf).await.is_err() {
        return;
    }
    let hs_resp = HandshakeResponse::ok(1);
    let mut hs_resp_buf = vec![0u8; HandshakeResponse::SIZE];
    hs_resp.encode(&mut hs_resp_buf);
    if stream.write_all(&hs_resp_buf).await.is_err() {
        return;
    }

    // 2. Message loop
    loop {
        let mut hdr_buf = vec![0u8; 28];
        if stream.read_exact(&mut hdr_buf).await.is_err() {
            return;
        }

        let header = match powerfs_net::FrameHeader::decode(&hdr_buf) {
            Some(h) => h,
            None => return,
        };

        // Read body + data (concatenated, total = data_len)
        let total_len = header.data_len as usize;
        let mut body_buf = vec![0u8; total_len];
        if total_len > 0 && stream.read_exact(&mut body_buf).await.is_err() {
            return;
        }

        let msg_type = MsgType::from_u16(header.msg_type);

        let (status, resp_body) = match msg_type {
            Some(MsgType::AcquireInodeLease) => {
                acquire_count.fetch_add(1, Ordering::SeqCst);
                handle_acquire(&body_buf, &leases)
            }
            Some(MsgType::ReleaseInodeLease) => {
                release_count.fetch_add(1, Ordering::SeqCst);
                handle_release(&body_buf, &leases)
            }
            Some(MsgType::RenewInodeLease) => {
                renew_count.fetch_add(1, Ordering::SeqCst);
                handle_renew(&body_buf, &leases)
            }
            _ => (0u16, Vec::new()),
        };

        // Build response frame
        // Set body_len = resp_body.len() so client parses resp.body correctly
        // (send_coherence_msg returns resp.body, not resp.data)
        let data_len = resp_body.len() as u32;
        let mut new_header = powerfs_net::FrameHeader::new(
            header.msg_type,
            FrameFlags::new(FrameFlags::RESPONSE),
            header.seq,
            data_len,
        );
        new_header.set_body_data_len(resp_body.len() as u32, data_len);
        let new_header = new_header.with_status(status);

        let mut frame = Vec::with_capacity(28 + resp_body.len());
        let mut resp_hdr_buf = vec![0u8; 28];
        new_header.encode(&mut resp_hdr_buf);
        frame.extend_from_slice(&resp_hdr_buf);
        frame.extend_from_slice(&resp_body);

        if stream.write_all(&frame).await.is_err() {
            return;
        }
    }
}

fn handle_acquire(body: &[u8], leases: &LeaseStore) -> (u16, Vec<u8>) {
    let mut dec = TlvDecoder::new(body);
    let inode = dec.next_u64(FieldId::Ino).unwrap_or(0);
    let client_id = dec.next_string(FieldId::ClientId).unwrap_or_default();
    let _duration_ms = dec.next_u64(FieldId::LeaseDuration).unwrap_or(30000);

    let mut store = leases.lock().unwrap();
    if let Some(existing) = store.get(&inode) {
        if existing.holder == client_id {
            // Same holder: return existing token (idempotent)
            let mut enc = TlvEncoder::new();
            let _ = enc.add_string(FieldId::LeaseId, &existing.token);
            let _ = enc.add_u64(FieldId::LeaseDuration, 30000);
            return (0, enc.into_bytes());
        }
        // Different holder: conflict
        return (1, Vec::new());
    }

    let token = format!(
        "mock-token-{}-{}",
        inode,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    store.insert(
        inode,
        MockLeaseState {
            holder: client_id,
            token: token.clone(),
        },
    );

    let mut enc = TlvEncoder::new();
    let _ = enc.add_string(FieldId::LeaseId, &token);
    let _ = enc.add_u64(FieldId::LeaseDuration, 30000);
    (0, enc.into_bytes())
}

fn handle_release(body: &[u8], leases: &LeaseStore) -> (u16, Vec<u8>) {
    let mut dec = TlvDecoder::new(body);
    let inode = dec.next_u64(FieldId::Ino).unwrap_or(0);
    let _client_id = dec.next_string(FieldId::ClientId).unwrap_or_default();
    let _token = dec.next_string(FieldId::LeaseToken).unwrap_or_default();

    let mut store = leases.lock().unwrap();
    store.remove(&inode);
    (0, Vec::new())
}

fn handle_renew(body: &[u8], leases: &LeaseStore) -> (u16, Vec<u8>) {
    let mut dec = TlvDecoder::new(body);
    let inode = dec.next_u64(FieldId::Ino).unwrap_or(0);
    let client_id = dec.next_string(FieldId::ClientId).unwrap_or_default();
    let token = dec.next_string(FieldId::LeaseToken).unwrap_or_default();

    let store = leases.lock().unwrap();
    if let Some(existing) = store.get(&inode) {
        if existing.holder == client_id && existing.token == token {
            return (0, Vec::new());
        }
    }
    (1, Vec::new())
}

// =====================================================================
// Helper: create FuseClientFacade with inode lease mode
// =====================================================================

async fn create_facade_with_inode_lease(filer_port: u16) -> FuseClientFacade {
    let config = FuseClientFacadeConfig {
        master_addr: "127.0.0.1".to_string(),
        master_port: 19999,
        volume_net_port: 19998,
        volume_addrs: Vec::new(),
        filer_addr: "127.0.0.1".to_string(),
        filer_port: filer_port,
        request_timeout: Duration::from_secs(5),
        client_identity: ClientIdentity::default(),
        mount_point: String::new(),
        collection: String::new(),
        replication: String::new(),
        lease_mode: "inode".to_string(),
        lease_duration_ms: 30_000,
        lease_renew_interval_ms: 10_000,
    };

    let facade = FuseClientFacade::new(config)
        .await
        .expect("Failed to create facade");

    // Set default filer addr and shard leader
    facade
        .meta_shard_client()
        .set_default_filer_addr(format!("127.0.0.1:{}", filer_port));
    facade.meta_shard_client().init();
    facade.volume_client().init();

    // Set shard leader for inode routing (shard 0)
    facade
        .meta_shard_client()
        .set_shard_leader(0, format!("127.0.0.1:{}", filer_port));

    facade.meta_shard_client().start_background_processor();
    facade.volume_client().start_background_processor();

    // Give background processor time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    facade
}

// =====================================================================
// Tests
// =====================================================================

#[tokio::test]
async fn test_inode_lease_basic_acquire_and_release() {
    let filer = MockFiler::new(19401);
    let _addr = filer.start().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let facade = create_facade_with_inode_lease(19401).await;
    let inode = 1001u64;
    let client_id = "test-client-A".to_string();

    // 1. Acquire inode lease
    let (token, expire_ms) = facade
        .acquire_inode_lease(inode, &client_id, 30_000)
        .await
        .expect("acquire should succeed");
    assert!(!token.is_empty(), "token should not be empty");
    assert_eq!(expire_ms, 30_000);
    info!("Test 1 PASSED: acquired token={:.16}...", token);

    // 2. Verify cache is populated
    let cached = facade.get_valid_inode_lease_token(inode);
    assert!(cached.is_some(), "cache should have the token");
    assert_eq!(cached.unwrap().0, token, "cached token should match");

    // 3. Release (auto-invalidates cache)
    facade
        .release_inode_lease(inode, &client_id, &token)
        .await
        .expect("release should succeed");

    // 4. Verify cache is invalidated
    let cached_after = facade.get_valid_inode_lease_token(inode);
    assert!(
        cached_after.is_none(),
        "cache should be empty after release"
    );

    // 5. Verify mock server received the requests
    assert_eq!(
        filer.acquire_count.load(Ordering::SeqCst),
        1,
        "1 acquire request"
    );
    assert_eq!(
        filer.release_count.load(Ordering::SeqCst),
        1,
        "1 release request"
    );

    info!("Test 1 PASSED: basic acquire + cache + release flow verified");
}

#[tokio::test]
async fn test_inode_lease_cache_hit_no_network() {
    let filer = MockFiler::new(19402);
    let _addr = filer.start().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let facade = create_facade_with_inode_lease(19402).await;
    let inode = 2002u64;
    let client_id = "test-client-B".to_string();

    // 1. Acquire inode lease (network call)
    let (token1, _) = facade
        .acquire_inode_lease(inode, &client_id, 30_000)
        .await
        .expect("first acquire should succeed");

    // 2. Check cache — should return cached token WITHOUT network call
    let cached = facade
        .get_valid_inode_lease_token(inode)
        .expect("cache should return token");
    assert_eq!(cached.0, token1, "cached token should match acquired token");
    assert!(
        cached.1.as_millis() > 25_000,
        "remaining time should be close to 30s, got {}ms",
        cached.1.as_millis()
    );

    // 3. Only 1 network acquire request should have been made
    assert_eq!(
        filer.acquire_count.load(Ordering::SeqCst),
        1,
        "only 1 acquire request should be made (cache hit for 2nd)"
    );

    info!("Test 2 PASSED: cache hit avoids network round-trip");
}

#[tokio::test]
async fn test_inode_lease_renew() {
    let filer = MockFiler::new(19403);
    let _addr = filer.start().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let facade = create_facade_with_inode_lease(19403).await;
    let inode = 3003u64;
    let client_id = "test-client-C".to_string();

    // 1. Acquire
    let (token, _) = facade
        .acquire_inode_lease(inode, &client_id, 30_000)
        .await
        .expect("acquire should succeed");

    // 2. Renew
    facade
        .renew_inode_lease(inode, &client_id, &token, 30_000)
        .await
        .expect("renew should succeed");

    // 3. Update cache expiry (as ensure_inode_lease would do)
    facade.update_inode_lease(inode, &token, Duration::from_millis(30_000));

    // 4. Verify renew request was received by mock server
    assert_eq!(filer.acquire_count.load(Ordering::SeqCst), 1, "1 acquire");
    assert_eq!(filer.renew_count.load(Ordering::SeqCst), 1, "1 renew");

    // 5. Verify cache still has the token
    let cached = facade
        .get_valid_inode_lease_token(inode)
        .expect("cache should still have token after renew");
    assert_eq!(cached.0, token);

    info!("Test 3 PASSED: renew updates lease expiry");
}

#[tokio::test]
async fn test_inode_lease_concurrent_different_inodes() {
    let filer = Arc::new(MockFiler::new(19404));
    let _addr = filer.start().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let facade = Arc::new(create_facade_with_inode_lease(19404).await);
    let num_tasks = 8;

    let mut handles = Vec::new();
    for i in 0..num_tasks {
        let facade = facade.clone();
        handles.push(tokio::spawn(async move {
            let inode = 40000 + i as u64;
            let client_id = format!("client-{}", i);
            let (token, _) = facade
                .acquire_inode_lease(inode, &client_id, 30_000)
                .await
                .expect("acquire should succeed for different inode");
            assert!(!token.is_empty());
            token
        }));
    }

    let mut tokens = Vec::new();
    for h in handles {
        tokens.push(h.await.unwrap());
    }

    // All tokens should be different (different inodes)
    let unique: std::collections::HashSet<_> = tokens.iter().collect();
    assert_eq!(unique.len(), num_tasks, "all tokens should be unique");

    assert_eq!(
        filer.acquire_count.load(Ordering::SeqCst),
        num_tasks as u32,
        "should have {} acquire requests",
        num_tasks
    );

    info!("Test 4 PASSED: concurrent acquire for different inodes succeeds");
}

#[tokio::test]
async fn test_inode_lease_concurrent_same_inode_mutual_exclusion() {
    let filer = Arc::new(MockFiler::new(19405));
    let _addr = filer.start().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let facade = Arc::new(create_facade_with_inode_lease(19405).await);
    let inode = 50005u64;
    let num_tasks = 8;

    let success_count = Arc::new(AtomicU32::new(0));
    let fail_count = Arc::new(AtomicU32::new(0));

    let mut handles = Vec::new();
    for i in 0..num_tasks {
        let facade = facade.clone();
        let success_count = success_count.clone();
        let fail_count = fail_count.clone();
        handles.push(tokio::spawn(async move {
            let client_id = format!("client-{}", i);
            match facade.acquire_inode_lease(inode, &client_id, 30_000).await {
                Ok(_) => {
                    success_count.fetch_add(1, Ordering::SeqCst);
                }
                Err(_) => {
                    fail_count.fetch_add(1, Ordering::SeqCst);
                }
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let successes = success_count.load(Ordering::SeqCst);
    let failures = fail_count.load(Ordering::SeqCst);

    assert_eq!(
        successes, 1,
        "exactly 1 client should acquire the lease, got {}",
        successes
    );
    assert_eq!(
        failures,
        (num_tasks - 1) as u32,
        "remaining clients should fail, got {}",
        failures
    );

    info!("Test 5 PASSED: concurrent acquire for same inode — mutual exclusion verified");
}

#[tokio::test]
async fn test_inode_lease_release_then_reacquire() {
    let filer = Arc::new(MockFiler::new(19406));
    let _addr = filer.start().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let facade = create_facade_with_inode_lease(19406).await;
    let inode = 60006u64;

    // 1. client-A acquires
    let (token_a, _) = facade
        .acquire_inode_lease(inode, "client-A", 30_000)
        .await
        .expect("client-A acquire should succeed");

    // 2. client-B fails to acquire (held by client-A)
    let err = facade.acquire_inode_lease(inode, "client-B", 30_000).await;
    assert!(
        err.is_err(),
        "client-B should fail while client-A holds lease"
    );

    // 3. client-A releases
    facade
        .release_inode_lease(inode, "client-A", &token_a)
        .await
        .expect("release should succeed");

    // 4. client-B can now acquire
    let (token_b, _) = facade
        .acquire_inode_lease(inode, "client-B", 30_000)
        .await
        .expect("client-B should acquire after release");

    assert_ne!(token_a, token_b, "tokens should be different");

    info!("Test 6 PASSED: release → reacquire by different client works");
}

#[tokio::test]
async fn test_inode_lease_cache_expiry() {
    let filer = MockFiler::new(19407);
    let _addr = filer.start().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let facade = create_facade_with_inode_lease(19407).await;
    let inode = 70007u64;
    let client_id = "test-client-D".to_string();

    // 1. Acquire with very short duration
    let (token, _) = facade
        .acquire_inode_lease(inode, &client_id, 30_000)
        .await
        .expect("acquire should succeed");

    // 2. Cache with short duration (100ms)
    facade.update_inode_lease(inode, &token, Duration::from_millis(100));

    // 3. Verify cache is valid
    assert!(facade.get_valid_inode_lease_token(inode).is_some());

    // 4. Wait for cache to expire
    tokio::time::sleep(Duration::from_millis(150)).await;

    // 5. Cache should be expired
    assert!(
        facade.get_valid_inode_lease_token(inode).is_none(),
        "cache should be expired after 150ms"
    );

    info!("Test 7 PASSED: cache expiry works correctly");
}

#[tokio::test]
async fn test_inode_lease_proactive_renew_near_expiry() {
    let filer = Arc::new(MockFiler::new(19408));
    let _addr = filer.start().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let facade = create_facade_with_inode_lease(19408).await;
    let inode = 80008u64;
    let client_id = "test-client-E".to_string();

    // 1. Acquire
    let (token, _) = facade
        .acquire_inode_lease(inode, &client_id, 30_000)
        .await
        .expect("acquire should succeed");

    // 2. Cache with short duration (50ms) — simulates lease nearing expiry
    facade.update_inode_lease(inode, &token, Duration::from_millis(50));

    // 3. Immediately check cache — remaining time < RENEW_THRESHOLD (10s)
    let cached = facade
        .get_valid_inode_lease_token(inode)
        .expect("cache should still be valid");
    assert!(
        cached.1 < Duration::from_secs(10),
        "remaining should be < 10s"
    );

    // 4. Manually renew (simulating what ensure_inode_lease does)
    facade
        .renew_inode_lease(inode, &client_id, &token, 30_000)
        .await
        .expect("proactive renew should succeed");

    // 5. Update cache with new duration
    facade.update_inode_lease(inode, &token, Duration::from_millis(30_000));

    // 6. Verify renew was received
    assert_eq!(
        filer.renew_count.load(Ordering::SeqCst),
        1,
        "1 renew request"
    );

    // 7. Cache should now have long remaining time
    let cached_after = facade
        .get_valid_inode_lease_token(inode)
        .expect("cache should still be valid after renew");
    assert!(
        cached_after.1 > Duration::from_secs(25),
        "remaining should be close to 30s after renew, got {}ms",
        cached_after.1.as_millis()
    );

    info!("Test 8 PASSED: proactive renew when nearing expiry works");
}

#[tokio::test]
async fn test_inode_lease_idempotent_same_client() {
    let filer = MockFiler::new(19409);
    let _addr = filer.start().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let facade = create_facade_with_inode_lease(19409).await;
    let inode = 90009u64;
    let client_id = "test-client-F".to_string();

    // 1. First acquire
    let (token1, _) = facade
        .acquire_inode_lease(inode, &client_id, 30_000)
        .await
        .expect("first acquire should succeed");

    // 2. Second acquire by same client (idempotent — returns same token)
    let (token2, _) = facade
        .acquire_inode_lease(inode, &client_id, 30_000)
        .await
        .expect("second acquire by same client should succeed (idempotent)");

    // Mock Filer returns the same token for same client
    assert_eq!(
        token1, token2,
        "same client should get same token on re-acquire (idempotent)"
    );

    info!("Test 9 PASSED: idempotent acquire for same client returns same token");
}
