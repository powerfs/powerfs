//! Mock Server 集成测试 - 验证 FuseClientFacade 完整请求链路
//!
//! 本测试启动三个最小化 TCP Mock Server（master, filer, volume），
//! 实现完整的 powerfs-net 二进制协议握手和帧交换，
//! 验证 FuseClientFacade 的 request → queue → background processor → network → response 全链路。
//!
//! 协议格式：
//!   握手: 18 字节请求 / 18 字节响应
//!   帧:   28 字节头 + body + data
//!   响应: 28 字节头 + JSON body

#![allow(unused_imports, dead_code)]

use std::net::SocketAddr;
type MockHandler = Arc<dyn Fn(MsgType, &[u8]) -> Option<(u16, Vec<u8>)> + Send + Sync>;
use std::sync::Arc;
use std::time::Duration;

use log::info;
use powerfs_common::traits::{MetadataProvider, VolumeProvider};
use powerfs_common::types::VolumeId;
use powerfs_fuse_core::*;
use powerfs_net::{FrameFlags, HandshakeRequest, HandshakeResponse, MsgType};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Mock server that handles handshake and returns canned JSON responses
struct MockServer {
    addr: String,
    port: u16,
    handler: MockHandler,
}

impl MockServer {
    fn new<F>(port: u16, handler: F) -> Self
    where
        F: Fn(MsgType, &[u8]) -> Option<(u16, Vec<u8>)> + Send + Sync + 'static,
    {
        Self {
            addr: "127.0.0.1".to_string(),
            port,
            handler: Arc::new(handler),
        }
    }

    async fn start(&self) -> SocketAddr {
        let addr = format!("{}:{}", self.addr, self.port);
        let listener = TcpListener::bind(&addr).await.unwrap();
        let bound_addr = listener.local_addr().unwrap();

        let handler = self.handler.clone();
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let handler = handler.clone();
                tokio::spawn(async move {
                    handle_connection(stream, handler).await;
                });
            }
        });

        bound_addr
    }
}

async fn handle_connection(mut stream: TcpStream, handler: MockHandler) {
    // 1. Handle handshake
    let mut hs_buf = vec![0u8; HandshakeRequest::SIZE];
    if stream.read_exact(&mut hs_buf).await.is_err() {
        return;
    }

    let hs_req = HandshakeRequest::decode(&hs_buf);
    if hs_req.is_none() {
        return;
    }

    let hs_resp = HandshakeResponse::ok(1);
    let mut hs_resp_buf = vec![0u8; HandshakeResponse::SIZE];
    hs_resp.encode(&mut hs_resp_buf);
    if stream.write_all(&hs_resp_buf).await.is_err() {
        return;
    }

    // 2. Handle frame requests
    loop {
        // Read frame header (28 bytes)
        let mut hdr_buf = vec![0u8; 28];
        if stream.read_exact(&mut hdr_buf).await.is_err() {
            break;
        }

        let header = match powerfs_net::FrameHeader::decode(&hdr_buf) {
            Some(h) => h,
            None => break,
        };

        // Read body + data
        let total_data_len = header.data_len as usize;
        let mut data_buf = vec![0u8; total_data_len];
        if total_data_len > 0 && stream.read_exact(&mut data_buf).await.is_err() {
            break;
        }

        // Parse body from data_buf (body + data concatenated)
        let body = data_buf;

        let msg_type = MsgType::from_u16(header.msg_type);
        info!(
            "MockServer: received frame seq={} msg_type={:?} body_len={}",
            header.seq,
            msg_type,
            body.len()
        );

        let response = handler(msg_type.unwrap_or(MsgType::Ping), &body).unwrap_or_default();

        let (status, resp_body) = response;

        // Build response frame with correct status
        let data_len = resp_body.len() as u32;
        let new_header = powerfs_net::FrameHeader::new(
            header.msg_type,
            FrameFlags::new(FrameFlags::RESPONSE),
            header.seq,
            data_len,
        )
        .with_status(status);

        let mut frame = Vec::with_capacity(28 + resp_body.len());
        let mut hdr_buf_new = vec![0u8; 28];
        new_header.encode(&mut hdr_buf_new);
        frame.extend_from_slice(&hdr_buf_new);
        frame.extend_from_slice(&resp_body);

        if stream.write_all(&frame).await.is_err() {
            break;
        }
    }
}

// ============================================================================
// Test Cases
// ============================================================================

/// 构建成功的 FacadeResponse JSON
fn success_response_json(data: &serde_json::Value) -> Vec<u8> {
    let resp = serde_json::json!({
        "success": true,
        "data": data,
        "error": null,
    });
    serde_json::to_vec(&resp).unwrap()
}

/// 构建错误的 FacadeResponse JSON
fn error_response_json(error: &str) -> Vec<u8> {
    let resp = serde_json::json!({
        "success": false,
        "data": serde_json::Value::Null,
        "error": error,
    });
    serde_json::to_vec(&resp).unwrap()
}

/// 测试完整的 FuseClientFacade 请求链路
///
/// 启动三个 Mock Server，验证：
/// 1. 握手成功
/// 2. 请求提交 → 队列 → 后台处理器 → 网络 → 响应 全链路
/// 3. Facade 的 _and_wait 方法正确返回结果
#[tokio::test]
async fn test_facade_end_to_end_with_mock_servers() {
    // ---- Mock Master Server (port 19333) ----
    let master_server = MockServer::new(19333, |msg_type, _body| {
        match msg_type {
            MsgType::GetTopology => {
                // Return empty topology (success)
                Some((0, vec![]))
            }
            _ => {
                info!("MockMaster: unhandled msg_type={:?}", msg_type);
                Some((0, vec![]))
            }
        }
    });
    let master_addr = master_server.start().await;
    info!("Mock Master server on {}", master_addr);

    // ---- Mock Filer Server (port 19343) ----
    let filer_server = MockServer::new(19343, |msg_type, body| {
        match msg_type {
            MsgType::Lookup => {
                // Return a simple entry
                let data = serde_json::json!({
                    "name": "test.txt",
                    "directory": "/",
                    "attributes": {
                        "ino": 100,
                        "mode": 0o100644,
                        "uid": 1000,
                        "gid": 1000,
                        "atime": "2025-01-01T00:00:00Z",
                        "mtime": "2025-01-01T00:00:00Z",
                        "ctime": "2025-01-01T00:00:00Z",
                        "crtime": "2025-01-01T00:00:00Z",
                    },
                    "chunks": [],
                    "hard_link_id": "",
                    "hard_link_counter": 0,
                    "content_size": 0,
                    "disk_size": 0,
                });
                Some((0, success_response_json(&data)))
            }
            MsgType::AssignVolumeV2 => {
                // Control requests go to filer_client
                let data = serde_json::json!({
                    "volume_id": 1,
                    "cookie": 100,
                    "file_key": 200,
                    "locations": [{
                        "url": "127.0.0.1:19344",
                        "public_url": "127.0.0.1:19344",
                    }]
                });
                Some((0, success_response_json(&data)))
            }
            MsgType::Create => {
                let data = serde_json::json!({
                    "ino": 200,
                });
                Some((0, success_response_json(&data)))
            }
            MsgType::GetAttr => {
                let data = serde_json::json!({
                    "name": "test.txt",
                    "directory": "/",
                    "attributes": {
                        "ino": 100,
                        "mode": 0o100644,
                        "uid": 1000,
                        "gid": 1000,
                        "atime": "2025-01-01T00:00:00Z",
                        "mtime": "2025-01-01T00:00:00Z",
                        "ctime": "2025-01-01T00:00:00Z",
                        "crtime": "2025-01-01T00:00:00Z",
                    },
                    "chunks": [],
                    "hard_link_id": "",
                    "hard_link_counter": 0,
                    "content_size": 0,
                    "disk_size": 0,
                });
                Some((0, success_response_json(&data)))
            }
            _ => {
                info!(
                    "MockFiler: unhandled msg_type={:?} body_len={}",
                    msg_type,
                    body.len()
                );
                Some((0, vec![]))
            }
        }
    });
    let filer_addr = filer_server.start().await;
    info!("Mock Filer server on {}", filer_addr);

    // ---- Mock Volume Server (port 19344 - dedicated for this test) ----
    let volume_server = MockServer::new(19344, |msg_type, body| {
        match msg_type {
            MsgType::ReadNeedleBlob => {
                // Return some dummy data
                let data = vec![1u8, 2, 3, 4, 5];
                let resp = serde_json::json!({
                    "data": data,
                });
                Some((0, success_response_json(&resp)))
            }
            MsgType::WriteNeedle => Some((0, success_response_json(&serde_json::json!({})))),
            MsgType::RangeLease => Some((
                0,
                success_response_json(&serde_json::json!({
                    "lease_id": "test-lease",
                    "expires_at": "2025-01-02T00:00:00Z",
                })),
            )),
            MsgType::LookupVolume => {
                let data = serde_json::json!({
                    "locations": [{
                        "url": "127.0.0.1:19344",
                        "public_url": "127.0.0.1:19344",
                    }]
                });
                Some((0, success_response_json(&data)))
            }
            _ => {
                info!(
                    "MockVolume: unhandled msg_type={:?} body_len={}",
                    msg_type,
                    body.len()
                );
                Some((0, vec![]))
            }
        }
    });
    let _volume_addr = volume_server.start().await;
    info!("Mock Volume server on 127.0.0.1:19344");

    // Give servers time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // ---- Create FuseClientFacade ----
    let config = FuseClientFacadeConfig {
        master_addr: "127.0.0.1".to_string(),
        master_port: 19333,
        volume_net_port: 19344,
        volume_addrs: Vec::new(),
        filer_addr: "127.0.0.1".to_string(),
        filer_port: 19343,
        request_timeout: Duration::from_secs(5),
        client_identity: ClientIdentity::default(),
    };

    let facade = FuseClientFacade::new(config)
        .await
        .expect("Failed to create facade");

    // Initialize clients first (loads topology, sets state to Ready)
    // Need to set default filer addr before init for default routes
    facade
        .meta_shard_client()
        .set_default_filer_addr("127.0.0.1:19343".to_string());
    facade.meta_shard_client().init();
    facade.volume_client().init();

    // ---- Setup routing info AFTER init (init may sync from topology) ----
    // shard 0 is used by FacadeVolumeProvider::assign_volume (hardcoded shard_id=0)
    facade
        .meta_shard_client()
        .set_shard_leader(0, "127.0.0.1:19343".to_string());
    facade
        .meta_shard_client()
        .set_shard_leader(1, "127.0.0.1:19343".to_string());
    // volume addr should include port for proper connection
    facade
        .volume_client()
        .set_volume_info(1, "127.0.0.1:19344".to_string());

    // Start background processors
    facade.meta_shard_client().start_background_processor();
    facade.volume_client().start_background_processor();

    // Give background processor time to start and process any pending initial requests
    tokio::time::sleep(Duration::from_millis(100)).await;

    // ---- Test 1: Metadata request (Lookup) ----
    info!("Test 1: Submitting metadata lookup request...");
    let lookup_payload = serde_json::to_vec(&serde_json::json!({
        "parent_ino": 1,
        "name": "test.txt",
    }))
    .unwrap();

    let result = facade
        .submit_metadata_request_with_type(
            RequestKind::Metadata,
            1,
            lookup_payload,
            MsgType::Lookup,
        )
        .await;

    assert!(
        result.is_ok(),
        "Metadata lookup should succeed, got: {:?}",
        result.err()
    );
    let result = result.unwrap();
    assert!(result.data.is_some(), "Should have response data");
    info!("Test 1 PASSED: Metadata lookup succeeded");

    // ---- Test 2: Control request (AssignVolumeV2) ----
    info!("Test 2: Submitting assign volume request...");
    let assign_payload = serde_json::to_vec(&serde_json::json!({
        "collection": "test-collection",
        "replication": "1",
    }))
    .unwrap();

    let result = facade
        .submit_control_request_with_type(
            RequestKind::Control,
            1,
            assign_payload,
            MsgType::AssignVolumeV2,
        )
        .await;

    assert!(
        result.is_ok(),
        "Assign volume should succeed, got: {:?}",
        result.err()
    );
    let result = result.unwrap();
    assert!(result.data.is_some(), "Should have response data");
    info!("Test 2 PASSED: Assign volume succeeded");

    // ---- Test 3: Data request (Read) ----
    info!("Test 3: Submitting data read request...");
    let read_payload = serde_json::to_vec(&serde_json::json!({
        "volume_id": 1,
        "file_key": 200,
        "offset": 0,
        "size": 1024,
    }))
    .unwrap();

    let result = facade
        .submit_data_request_with_type(RequestKind::Read, 1, read_payload, MsgType::ReadNeedleBlob)
        .await;

    assert!(
        result.is_ok(),
        "Data read should succeed, got: {:?}",
        result.err()
    );
    info!("Test 3 PASSED: Data read succeeded");

    // ---- Test 4: Lease request ----
    info!("Test 4: Submitting lease request...");
    let lease_payload = serde_json::to_vec(&serde_json::json!({
        "volume_id": 1,
    }))
    .unwrap();

    let result = facade.submit_lease_request(1, lease_payload).await;

    assert!(
        result.is_ok(),
        "Lease request should succeed, got: {:?}",
        result.err()
    );
    info!("Test 4 PASSED: Lease request succeeded");

    // ---- Test 5: Management request ----
    info!("Test 5: Submitting management request...");
    let mgmt_payload = serde_json::to_vec(&serde_json::json!({
        "action": "lookup_volume",
    }))
    .unwrap();

    let result = facade.submit_mgmt_request(1, mgmt_payload).await;

    assert!(
        result.is_ok(),
        "Management request should succeed, got: {:?}",
        result.err()
    );
    info!("Test 5 PASSED: Management request succeeded");

    // ---- Test 6: Multiple concurrent requests ----
    info!("Test 6: Submitting concurrent requests...");

    // Use original facade for concurrent test
    let facade_arc = Arc::new(facade);
    let mut join_handles = Vec::new();

    for i in 0..3 {
        let facade = facade_arc.clone();
        let handle = tokio::spawn(async move {
            let payload = serde_json::to_vec(&serde_json::json!({
                "parent_ino": 1,
                "name": format!("file_{}.txt", i),
            }))
            .unwrap();

            facade
                .submit_metadata_request_with_type(
                    RequestKind::Metadata,
                    1,
                    payload,
                    MsgType::Lookup,
                )
                .await
        });
        join_handles.push(handle);
    }

    for handle in join_handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok(), "Concurrent request should succeed");
    }
    info!("Test 6 PASSED: Concurrent requests succeeded");

    // ---- Cleanup ----
    facade_arc.close();
    info!("All tests passed!");
}

/// 测试 FacadeVolumeProvider 通过 Mock Server
#[tokio::test]
async fn test_facade_volume_provider_with_mock() {
    // Setup mock servers (reuse from previous test pattern)
    let master_server = MockServer::new(19433, |msg_type, _body| match msg_type {
        MsgType::GetTopology => Some((0, vec![])),
        MsgType::Assign => {
            // Master allocates volume_id via Raft — return TLV-encoded fields
            let mut enc = powerfs_net::TlvEncoder::new();
            let _ = enc.add_u64(powerfs_net::FieldId::VolumeId, 1);
            let _ = enc.add_u64(powerfs_net::FieldId::Cookie, 100);
            let _ = enc.add_u64(powerfs_net::FieldId::FileKey, 200);
            let _ = enc.add_string(powerfs_net::FieldId::Owner, "127.0.0.1:19444");
            let _ = enc.add_u64(powerfs_net::FieldId::Entries, 1);
            Some((0, enc.into_bytes()))
        }
        MsgType::LookupVolume => {
            // Master returns volume → server mapping as TLV
            let mut enc = powerfs_net::TlvEncoder::new();
            let _ = enc.add_u64(powerfs_net::FieldId::Limit, 1);
            let _ = enc.add_string(powerfs_net::FieldId::Owner, "127.0.0.1:19444");
            Some((0, enc.into_bytes()))
        }
        _ => Some((0, vec![])),
    });
    master_server.start().await;

    let filer_server = MockServer::new(19443, |msg_type, _body| match msg_type {
        MsgType::AssignVolumeV2 => {
            // Control requests go to filer_client
            let data = serde_json::json!({
                "volume_id": 1,
                "cookie": 100,
                "file_key": 200,
                "locations": [{
                    "url": "127.0.0.1:19444",
                    "public_url": "127.0.0.1:19444",
                }]
            });
            Some((0, success_response_json(&data)))
        }
        MsgType::LookupVolume => {
            let data = serde_json::json!({
                "locations": [{
                    "url": "127.0.0.1:19444",
                    "public_url": "127.0.0.1:19444",
                }]
            });
            Some((0, success_response_json(&data)))
        }
        _ => Some((0, vec![])),
    });
    filer_server.start().await;

    let volume_server = MockServer::new(19444, |msg_type, _body| match msg_type {
        MsgType::ReadNeedleBlob => {
            Some((0, success_response_json(&serde_json::json!({"data": []}))))
        }
        MsgType::LookupVolume => {
            let data = serde_json::json!({
                "locations": [{
                    "url": "127.0.0.1:19444",
                    "public_url": "127.0.0.1:19444",
                }]
            });
            Some((0, success_response_json(&data)))
        }
        _ => Some((0, vec![])),
    });
    volume_server.start().await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    let config = FuseClientFacadeConfig {
        master_addr: "127.0.0.1".to_string(),
        master_port: 19433,
        volume_net_port: 19444,
        volume_addrs: Vec::new(),
        filer_addr: "127.0.0.1".to_string(),
        filer_port: 19443,
        request_timeout: Duration::from_secs(5),
        client_identity: ClientIdentity::default(),
    };

    let facade = Arc::new(FuseClientFacade::new(config).await.unwrap());

    // Setup routing (shard 0 for assign_volume, shard 1 for lookup_volume)
    facade
        .meta_shard_client()
        .set_shard_leader(0, "127.0.0.1:19443".to_string());
    facade
        .meta_shard_client()
        .set_shard_leader(1, "127.0.0.1:19443".to_string());
    facade
        .volume_client()
        .set_volume_info(1, "127.0.0.1".to_string());
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Test FacadeVolumeProvider
    let provider = FacadeVolumeProvider::new(facade.clone());

    // Test assign_volume
    let result = provider.assign_volume("test", "1").await;
    assert!(
        result.is_ok(),
        "assign_volume should succeed: {:?}",
        result.err()
    );
    let (fid, locations) = result.unwrap();
    assert_eq!(fid.volume_id.0, 1);
    assert!(!locations.is_empty());
    info!(
        "assign_volume: fid={:?}, locations={}",
        fid,
        locations.len()
    );

    // Test lookup_volume
    let result = provider.lookup_volume(VolumeId(1)).await;
    assert!(
        result.is_ok(),
        "lookup_volume should succeed: {:?}",
        result.err()
    );
    let locations = result.unwrap();
    assert!(!locations.is_empty());
    info!("lookup_volume: {} locations", locations.len());

    facade.close();
    info!("FacadeVolumeProvider tests passed!");
}

/// 测试 FacadeMetadataProvider 通过 Mock Server
#[tokio::test]
async fn test_facade_metadata_provider_with_mock() {
    let master_server = MockServer::new(19533, |msg_type, _body| match msg_type {
        MsgType::GetTopology => Some((0, vec![])),
        _ => Some((0, vec![])),
    });
    master_server.start().await;

    let filer_server = MockServer::new(19543, |msg_type, _body| match msg_type {
        MsgType::Lookup => {
            let data = serde_json::json!({
                "name": "hello.txt",
                "directory": "/",
                "attributes": {
                    "ino": 100,
                    "mode": 0o100644,
                    "uid": 0,
                    "gid": 0,
                    "atime": "2025-01-01T00:00:00Z",
                    "mtime": "2025-01-01T00:00:00Z",
                    "ctime": "2025-01-01T00:00:00Z",
                    "crtime": "2025-01-01T00:00:00Z",
                },
                "chunks": [],
                "hard_link_id": "",
                "hard_link_counter": 0,
                "content_size": 42,
                "disk_size": 42,
            });
            Some((0, success_response_json(&data)))
        }
        MsgType::GetAttr => {
            let data = serde_json::json!({
                "name": "hello.txt",
                "directory": "/",
                "attributes": {
                    "ino": 100,
                    "mode": 0o100644,
                    "uid": 0,
                    "gid": 0,
                    "atime": "2025-01-01T00:00:00Z",
                    "mtime": "2025-01-01T00:00:00Z",
                    "ctime": "2025-01-01T00:00:00Z",
                    "crtime": "2025-01-01T00:00:00Z",
                },
                "chunks": [],
                "hard_link_id": "",
                "hard_link_counter": 0,
                "content_size": 42,
                "disk_size": 42,
            });
            Some((0, success_response_json(&data)))
        }
        _ => Some((0, vec![])),
    });
    filer_server.start().await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    let config = FuseClientFacadeConfig {
        master_addr: "127.0.0.1".to_string(),
        master_port: 19533,
        volume_net_port: 19544,
        volume_addrs: Vec::new(),
        filer_addr: "127.0.0.1".to_string(),
        filer_port: 19543,
        request_timeout: Duration::from_secs(5),
        client_identity: ClientIdentity::default(),
    };

    let facade = Arc::new(FuseClientFacade::new(config).await.unwrap());

    // Initialize clients first
    facade
        .meta_shard_client()
        .set_default_filer_addr("127.0.0.1:19543".to_string());
    facade.meta_shard_client().init();
    facade.volume_client().init();

    // Setup routing AFTER init
    facade
        .meta_shard_client()
        .set_shard_leader(1, "127.0.0.1:19543".to_string());

    // Start background processors
    facade.meta_shard_client().start_background_processor();
    facade.volume_client().start_background_processor();

    let provider = FacadeMetadataProvider::new(facade.clone());

    // Test get_entry
    let result = provider.get_entry("/hello.txt").await;
    assert!(
        result.is_ok(),
        "get_entry should succeed: {:?}",
        result.err()
    );
    let entry = result.unwrap();
    assert!(entry.is_some());
    let entry = entry.unwrap();
    assert_eq!(entry.name, "hello.txt");
    info!("get_entry: name={}", entry.name);

    // Test get_entry_by_inode
    let result = provider.get_entry_by_inode(100).await;
    assert!(result.is_ok(), "get_entry_by_inode should succeed");
    info!("get_entry_by_inode passed");

    facade.close();
    info!("FacadeMetadataProvider tests passed!");
}
