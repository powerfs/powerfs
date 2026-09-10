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
    ReadNeedleMetaRequest, ReadNeedleRequest, VolumeServiceClient, VolumeServiceServer,
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
