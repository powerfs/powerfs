//! Volume gRPC 服务集成回归（S7：双引擎同测试集）。
//!
//! 同一组测试在 v1 needle 引擎与 v2 WAL 引擎上各跑一遍（方案附录 B S7
//! 验收项）。测试体只断言协议面语义（成功标志、数据回读一致），不感知
//! 引擎实现；引擎分支由 `setup_server_and_client_with_engine` 的
//! StorageManager 装配决定（volume_engine=needle|wal 的等价接线）。

use powerfs_common::types::NodeId;
use powerfs_core::storage::StorageManager;
use powerfs_core::volume_engine::EngineKind;
use powerfs_volume::proto::{
    CreateVolumeRequest, DeleteNeedleRequest, DeleteVolumeRequest, ReadNeedleBlobRequest,
    ReadNeedleMetaRequest, ReadNeedleRequest, VolumeAdminCheckpointRequest, VolumeAdminGcRequest,
    VolumeAdminStatsRequest, VolumeResizeRequest, VolumeServiceClient, VolumeServiceServer,
    WriteNeedleBlobRequest, WriteNeedleRequest,
};
use powerfs_volume::server::VolumeServer;
use std::net::SocketAddr;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::sync::oneshot;
use tonic::transport::Channel;

async fn setup_server_and_client_with_engine(
    engine: EngineKind,
) -> (VolumeServiceClient<Channel>, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_str().unwrap().to_string();
    let node_id = NodeId("test-node".to_string());

    let storage_manager = Arc::new(
        StorageManager::new_for_engine(
            node_id.clone(),
            data_path.clone(),
            None,
            false,
            engine,
            64 << 20,
        )
        .expect("Failed to create storage manager"),
    );
    let server = VolumeServer::new(
        storage_manager,
        node_id,
        "127.0.0.1",
        50051,
        8080,
        &data_path,
    );

    let (tx, rx) = oneshot::channel();

    tokio::spawn(async move {
        let addr: SocketAddr = "[::1]:0".parse().unwrap();
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let addr = listener.local_addr().unwrap();
        tx.send(addr).unwrap();

        tonic::transport::Server::builder()
            .add_service(VolumeServiceServer::new(server))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let addr = rx.await.unwrap();

    let client = VolumeServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();

    (client, temp_dir)
}

macro_rules! grpc_test_suite {
    ($mod_name:ident, $engine:expr) => {
        mod $mod_name {
            use super::*;

            async fn setup() -> (VolumeServiceClient<Channel>, TempDir) {
                setup_server_and_client_with_engine($engine).await
            }

            #[tokio::test]
            async fn test_volume_create() {
                let (mut client, _temp_dir) = setup().await;

                let response = client
                    .create_volume(CreateVolumeRequest {
                        volume_id: 1,
                        size: 10 * 1024 * 1024,
                        collection: String::new(),
                    })
                    .await
                    .unwrap();

                assert!(response.into_inner().success);

                let response = client
                    .create_volume(CreateVolumeRequest {
                        volume_id: 1,
                        size: 10 * 1024 * 1024,
                        collection: String::new(),
                    })
                    .await;
                assert!(response.is_err());
            }

            #[tokio::test]
            async fn test_volume_delete() {
                let (mut client, _temp_dir) = setup().await;

                client
                    .create_volume(CreateVolumeRequest {
                        volume_id: 2,
                        size: 10 * 1024 * 1024,
                        collection: String::new(),
                    })
                    .await
                    .unwrap();

                let response = client
                    .delete_volume(DeleteVolumeRequest { volume_id: 2 })
                    .await
                    .unwrap();

                assert!(response.into_inner().success);

                let response = client
                    .delete_volume(DeleteVolumeRequest { volume_id: 999 })
                    .await;
                assert!(response.is_err());
            }

            #[tokio::test]
            async fn test_volume_write_needle() {
                let (mut client, _temp_dir) = setup().await;

                client
                    .create_volume(CreateVolumeRequest {
                        volume_id: 3,
                        size: 10 * 1024 * 1024,
                        collection: String::new(),
                    })
                    .await
                    .unwrap();

                let data = b"hello powerfs".to_vec();
                let response = client
                    .write_needle(WriteNeedleRequest {
                        volume_id: 3,
                        file_key: 100,
                        data: data.clone(),
                        cookie: 0,
                        ttl: "".to_string(),
                    })
                    .await
                    .unwrap();

                let resp = response.into_inner();
                assert!(resp.success);
                assert_eq!(resp.volume_id, 3);
                assert_eq!(resp.file_key, 100);
            }

            #[tokio::test]
            async fn test_volume_read_needle() {
                let (mut client, _temp_dir) = setup().await;

                client
                    .create_volume(CreateVolumeRequest {
                        volume_id: 4,
                        size: 10 * 1024 * 1024,
                        collection: String::new(),
                    })
                    .await
                    .unwrap();

                let data = b"read test data".to_vec();
                client
                    .write_needle(WriteNeedleRequest {
                        volume_id: 4,
                        file_key: 200,
                        data: data.clone(),
                        cookie: 0,
                        ttl: "".to_string(),
                    })
                    .await
                    .unwrap();

                let response = client
                    .read_needle(ReadNeedleRequest {
                        volume_id: 4,
                        file_key: 200,
                        cookie: 0,
                    })
                    .await
                    .unwrap();

                let resp = response.into_inner();
                assert!(resp.success);
                assert_eq!(resp.data, data);
            }

            #[tokio::test]
            async fn test_volume_delete_needle() {
                let (mut client, _temp_dir) = setup().await;

                client
                    .create_volume(CreateVolumeRequest {
                        volume_id: 5,
                        size: 10 * 1024 * 1024,
                        collection: String::new(),
                    })
                    .await
                    .unwrap();

                client
                    .write_needle(WriteNeedleRequest {
                        volume_id: 5,
                        file_key: 300,
                        data: b"to delete".to_vec(),
                        cookie: 0,
                        ttl: "".to_string(),
                    })
                    .await
                    .unwrap();

                let response = client
                    .delete_needle(DeleteNeedleRequest {
                        volume_id: 5,
                        file_key: 300,
                        cookie: 0,
                    })
                    .await
                    .unwrap();

                assert!(response.into_inner().success);

                let response = client
                    .read_needle(ReadNeedleRequest {
                        volume_id: 5,
                        file_key: 300,
                        cookie: 0,
                    })
                    .await;
                assert!(response.is_err());
            }

            #[tokio::test]
            async fn test_volume_write_blob() {
                let (mut client, _temp_dir) = setup().await;

                client
                    .create_volume(CreateVolumeRequest {
                        volume_id: 6,
                        size: 10 * 1024 * 1024,
                        collection: String::new(),
                    })
                    .await
                    .unwrap();

                let blob_data = b"blob segment".to_vec();
                let response = client
                    .write_needle_blob(WriteNeedleBlobRequest {
                        volume_id: 6,
                        file_key: 400,
                        offset: 0,
                        size: blob_data.len() as i32,
                        needle_blob: blob_data,
                        cookie: 0,
                    })
                    .await
                    .unwrap();

                assert!(response.into_inner().success);
            }

            #[tokio::test]
            async fn test_volume_read_blob() {
                let (mut client, _temp_dir) = setup().await;

                client
                    .create_volume(CreateVolumeRequest {
                        volume_id: 7,
                        size: 10 * 1024 * 1024,
                        collection: String::new(),
                    })
                    .await
                    .unwrap();

                let data = b"read blob test".to_vec();
                client
                    .write_needle(WriteNeedleRequest {
                        volume_id: 7,
                        file_key: 500,
                        data: data.clone(),
                        cookie: 0,
                        ttl: "".to_string(),
                    })
                    .await
                    .unwrap();

                let response = client
                    .read_needle_blob(ReadNeedleBlobRequest {
                        volume_id: 7,
                        file_key: 500,
                        offset: 0,
                        size: data.len() as i32,
                    })
                    .await
                    .unwrap();

                let resp = response.into_inner();
                assert!(resp.success);
                assert!(!resp.needle_blob.is_empty());
            }

            #[tokio::test]
            async fn test_volume_read_meta() {
                let (mut client, _temp_dir) = setup().await;

                client
                    .create_volume(CreateVolumeRequest {
                        volume_id: 8,
                        size: 10 * 1024 * 1024,
                        collection: String::new(),
                    })
                    .await
                    .unwrap();

                client
                    .write_needle(WriteNeedleRequest {
                        volume_id: 8,
                        file_key: 600,
                        data: b"meta test".to_vec(),
                        cookie: 12345,
                        ttl: "7d".to_string(),
                    })
                    .await
                    .unwrap();

                let response = client
                    .read_needle_meta(ReadNeedleMetaRequest {
                        volume_id: 8,
                        file_key: 600,
                    })
                    .await
                    .unwrap();

                let resp = response.into_inner();
                assert!(resp.success);
            }
        }
    };
}

// v1 needle 引擎（既有行为回归）。
grpc_test_suite!(needle_engine, EngineKind::Needle);
// v2 WAL 引擎（S7 并行切换验收：同一测试集）。
grpc_test_suite!(wal_engine, EngineKind::Wal);

// ============================================================================
// P2 T8：WAL 远程管理面 RPC（stats/gc/checkpoint/resize）
// ============================================================================

#[tokio::test]
async fn wal_admin_rpcs_end_to_end() {
    let (mut client, _temp_dir) = setup_server_and_client_with_engine(EngineKind::Wal).await;

    client
        .create_volume(CreateVolumeRequest {
            volume_id: 71,
            size: 10 * 1024 * 1024,
            collection: String::new(),
        })
        .await
        .unwrap();

    client
        .write_needle(WriteNeedleRequest {
            volume_id: 71,
            file_key: 7001,
            data: b"wal-admin-rpc".to_vec(),
            cookie: 0,
            ttl: "".to_string(),
        })
        .await
        .unwrap();

    // stats：写入后 active needle=1、容量已限定、未满。
    let stats = client
        .volume_admin_stats(VolumeAdminStatsRequest { volume_id: 71 })
        .await
        .unwrap()
        .into_inner();
    assert!(stats.success, "stats failed: {}", stats.error);
    assert_eq!(stats.active_count, 1);
    assert_eq!(stats.volume_size, 10 * 1024 * 1024);
    assert!(stats.free_bytes < stats.volume_size);
    assert!(!stats.is_full);

    // checkpoint：成功落盘并返回 seq。
    let ckpt = client
        .volume_admin_checkpoint(VolumeAdminCheckpointRequest { volume_id: 71 })
        .await
        .unwrap()
        .into_inner();
    assert!(ckpt.success, "checkpoint failed: {}", ckpt.error);
    assert!(ckpt.ckpt_seq >= 1);
    assert!(ckpt.applied_lsn >= 1);

    // gc：成功执行一轮（无回收对象时各项为 0 也算通过）。
    let gc = client
        .volume_admin_gc(VolumeAdminGcRequest { volume_id: 71 })
        .await
        .unwrap()
        .into_inner();
    assert!(gc.success, "gc failed: {}", gc.error);

    // resize：shrink 到低于占用（13B 数据）必须拒绝。
    let shrink = client
        .volume_resize(VolumeResizeRequest {
            volume_id: 71,
            new_size: 1,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(
        !shrink.success,
        "shrink below occupied bytes must be rejected"
    );
    assert!(
        shrink.error.contains("shrink") || shrink.error.contains("rejected"),
        "unexpected shrink error: {}",
        shrink.error
    );

    // resize：grow 成功后 stats 反映新容量。
    let grow = client
        .volume_resize(VolumeResizeRequest {
            volume_id: 71,
            new_size: 64 * 1024 * 1024,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(grow.success, "grow failed: {}", grow.error);
    let stats2 = client
        .volume_admin_stats(VolumeAdminStatsRequest { volume_id: 71 })
        .await
        .unwrap()
        .into_inner();
    assert!(stats2.success);
    assert_eq!(stats2.volume_size, 64 * 1024 * 1024);
    assert_eq!(stats2.active_count, 1);
}

#[tokio::test]
async fn needle_admin_rpcs_rejected() {
    let (mut client, _temp_dir) = setup_server_and_client_with_engine(EngineKind::Needle).await;

    client
        .create_volume(CreateVolumeRequest {
            volume_id: 72,
            size: 10 * 1024 * 1024,
            collection: String::new(),
        })
        .await
        .unwrap();

    // v1 引擎：stats 明确报 WAL-only；其余管理 RPC 返回 InvalidRequest。
    let stats = client
        .volume_admin_stats(VolumeAdminStatsRequest { volume_id: 72 })
        .await
        .unwrap()
        .into_inner();
    assert!(!stats.success);
    assert!(
        stats.error.contains("WAL-engine"),
        "unexpected stats error: {}",
        stats.error
    );

    let gc = client
        .volume_admin_gc(VolumeAdminGcRequest { volume_id: 72 })
        .await
        .unwrap()
        .into_inner();
    assert!(!gc.success);
    assert!(
        gc.error.to_lowercase().contains("wal-engine"),
        "unexpected gc error: {}",
        gc.error
    );

    let ckpt = client
        .volume_admin_checkpoint(VolumeAdminCheckpointRequest { volume_id: 72 })
        .await
        .unwrap()
        .into_inner();
    assert!(!ckpt.success);
    assert!(
        ckpt.error.to_lowercase().contains("wal-engine"),
        "unexpected checkpoint error: {}",
        ckpt.error
    );

    let resize = client
        .volume_resize(VolumeResizeRequest {
            volume_id: 72,
            new_size: 4096,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!resize.success);
    assert!(
        resize.error.to_lowercase().contains("wal-engine"),
        "unexpected resize error: {}",
        resize.error
    );
}

#[tokio::test]
async fn admin_rpcs_unknown_volume_fail_gracefully() {
    let (mut client, _temp_dir) = setup_server_and_client_with_engine(EngineKind::Wal).await;

    let stats = client
        .volume_admin_stats(VolumeAdminStatsRequest { volume_id: 4040 })
        .await
        .unwrap()
        .into_inner();
    assert!(!stats.success);
    assert!(
        stats.error.contains("not found"),
        "unexpected error: {}",
        stats.error
    );

    let resize = client
        .volume_resize(VolumeResizeRequest {
            volume_id: 4040,
            new_size: 1024,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!resize.success);
    assert!(resize.error.contains("not found"));
}
