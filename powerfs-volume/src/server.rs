#![allow(clippy::result_large_err)]

use crate::proto::{VolumeService, VolumeServiceServer};
use bytes::Bytes;
use log::{debug, info, warn};
use powerfs_common::{
    error::{PowerFsError, Result},
    types::{NeedleId, NodeId, VolumeId},
};
use powerfs_core::storage::StorageManager;
use std::sync::Arc;
use tonic::{transport::Server, Request, Response, Status};

pub struct VolumeServer {
    storage_manager: Arc<StorageManager>,
    node_id: NodeId,
}

impl VolumeServer {
    pub fn new(storage_manager: Arc<StorageManager>, node_id: NodeId) -> Self {
        VolumeServer {
            storage_manager,
            node_id,
        }
    }

    pub async fn start(self, address: &str) -> Result<()> {
        let addr: std::net::SocketAddr = address.parse()?;

        info!("Starting PowerFS Volume server on: {}", addr);

        Server::builder()
            .add_service(
                VolumeServiceServer::new(self)
                    .max_decoding_message_size(256 * 1024 * 1024)
                    .max_encoding_message_size(256 * 1024 * 1024),
            )
            .serve(addr)
            .await
            .map_err(PowerFsError::TonicTransport)
    }
}

#[tonic::async_trait]
impl VolumeService for VolumeServer {
    async fn create_volume(
        &self,
        request: Request<crate::proto::CreateVolumeRequest>,
    ) -> std::result::Result<Response<crate::proto::CreateVolumeResponse>, Status> {
        let req = request.into_inner();

        let volume_id = VolumeId(req.volume_id);

        let result = self.storage_manager.create_volume(volume_id, req.size);

        match result {
            Ok(info) => {
                debug!("Created volume: {:?}", info.id);
                Ok(Response::new(crate::proto::CreateVolumeResponse {
                    success: true,
                    volume_id: info.id.0,
                }))
            }
            Err(e) => {
                warn!("Failed to create volume: {}", e);
                Err(Status::internal(format!("{}", e)))
            }
        }
    }

    async fn delete_volume(
        &self,
        request: Request<crate::proto::DeleteVolumeRequest>,
    ) -> std::result::Result<Response<crate::proto::DeleteVolumeResponse>, Status> {
        let req = request.into_inner();

        let volume_id = VolumeId(req.volume_id);

        match self.storage_manager.delete_volume(&volume_id) {
            Ok(_) => {
                debug!("Deleted volume: {:?}", volume_id);
                Ok(Response::new(crate::proto::DeleteVolumeResponse {
                    success: true,
                }))
            }
            Err(e) => {
                warn!("Failed to delete volume: {}", e);
                Err(Status::internal(format!("{}", e)))
            }
        }
    }

    async fn write_needle(
        &self,
        request: Request<crate::proto::WriteNeedleRequest>,
    ) -> std::result::Result<Response<crate::proto::WriteNeedleResponse>, Status> {
        let req = request.into_inner();

        let volume_id = VolumeId(req.volume_id);
        let file_key = req.file_key;

        let storage_manager = self.storage_manager.clone();

        tokio::task::spawn_blocking(move || {
            if let Some(volume) = storage_manager.get_volume(&volume_id) {
                let result = volume.write_needle(file_key, Bytes::from(req.data));
                match result {
                    Ok(info) => Ok(Response::new(crate::proto::WriteNeedleResponse {
                        success: true,
                        volume_id: volume_id.0,
                        file_key: info.id.0,
                        offset: info.offset,
                        cookie: 0,
                    })),
                    Err(e) => Err(Status::internal(format!("{}", e))),
                }
            } else {
                Err(Status::not_found(format!(
                    "volume not found: {}",
                    volume_id.0
                )))
            }
        })
        .await
        .unwrap()
    }

    async fn read_needle(
        &self,
        request: Request<crate::proto::ReadNeedleRequest>,
    ) -> std::result::Result<Response<crate::proto::ReadNeedleResponse>, Status> {
        let req = request.into_inner();

        let volume_id = VolumeId(req.volume_id);
        let needle_id = NeedleId(req.file_key);

        let storage_manager = self.storage_manager.clone();

        tokio::task::spawn_blocking(move || {
            if let Some(volume) = storage_manager.get_volume(&volume_id) {
                let result = volume.read_needle(&needle_id);
                match result {
                    Ok(data) => Ok(Response::new(crate::proto::ReadNeedleResponse {
                        success: true,
                        data: data.to_vec(),
                        cookie: 0,
                        last_modified: 0,
                    })),
                    Err(e) => Err(Status::internal(format!("{}", e))),
                }
            } else {
                Err(Status::not_found(format!(
                    "volume not found: {}",
                    volume_id.0
                )))
            }
        })
        .await
        .unwrap()
    }

    async fn delete_needle(
        &self,
        request: Request<crate::proto::DeleteNeedleRequest>,
    ) -> std::result::Result<Response<crate::proto::DeleteNeedleResponse>, Status> {
        let req = request.into_inner();

        let volume_id = VolumeId(req.volume_id);
        let needle_id = NeedleId(req.file_key);

        let storage_manager = self.storage_manager.clone();

        tokio::task::spawn_blocking(move || {
            if let Some(volume) = storage_manager.get_volume(&volume_id) {
                let result = volume.delete_needle(&needle_id);
                match result {
                    Ok(_) => Ok(Response::new(crate::proto::DeleteNeedleResponse {
                        success: true,
                    })),
                    Err(e) => Err(Status::internal(format!("{}", e))),
                }
            } else {
                Err(Status::not_found(format!(
                    "volume not found: {}",
                    volume_id.0
                )))
            }
        })
        .await
        .unwrap()
    }

    async fn list_volumes(
        &self,
        _request: Request<crate::proto::ListVolumesRequest>,
    ) -> std::result::Result<Response<crate::proto::ListVolumesResponse>, Status> {
        let volumes = self.storage_manager.list_volumes();

        let volume_infos: Vec<crate::proto::VolumeInfo> = volumes
            .into_iter()
            .map(|v| crate::proto::VolumeInfo {
                volume_id: v.id.0,
                node_id: v.node_id.0,
                size: v.size,
                used: v.used,
                replica_count: v.replica_count,
                state: v.state as i32,
                next_file_key: v.next_file_key,
                read_only: false,
                collection: "".to_string(),
                replication: "".to_string(),
                ttl: "".to_string(),
            })
            .collect();

        Ok(Response::new(crate::proto::ListVolumesResponse {
            volumes: volume_infos,
        }))
    }

    async fn get_node_info(
        &self,
        _request: Request<crate::proto::GetNodeInfoRequest>,
    ) -> std::result::Result<Response<crate::proto::GetNodeInfoResponse>, Status> {
        let info = crate::proto::GetNodeInfoResponse {
            node_id: self.node_id.0.clone(),
            total_space: self.storage_manager.total_space(),
            used_space: self.storage_manager.used_space(),
            volume_count: self.storage_manager.volume_count() as u32,
        };

        Ok(Response::new(info))
    }

    async fn write_needle_blob(
        &self,
        request: Request<crate::proto::WriteNeedleBlobRequest>,
    ) -> std::result::Result<Response<crate::proto::WriteNeedleBlobResponse>, Status> {
        let req = request.into_inner();
        let volume_id = VolumeId(req.volume_id);
        let file_key = req.file_key;
        let offset = req.offset;
        let size = req.size;
        let cookie = req.cookie;

        let storage_manager = self.storage_manager.clone();

        tokio::task::spawn_blocking(move || {
            if let Some(volume) = storage_manager.get_volume(&volume_id) {
                let result = volume.write_needle_blob(
                    file_key,
                    offset,
                    size,
                    Bytes::from(req.needle_blob),
                    cookie,
                );
                match result {
                    Ok(_) => Ok(Response::new(crate::proto::WriteNeedleBlobResponse {
                        success: true,
                    })),
                    Err(e) => Err(Status::internal(format!("{}", e))),
                }
            } else {
                Err(Status::not_found(format!(
                    "volume not found: {}",
                    volume_id.0
                )))
            }
        })
        .await
        .unwrap()
    }

    async fn read_needle_blob(
        &self,
        request: Request<crate::proto::ReadNeedleBlobRequest>,
    ) -> std::result::Result<Response<crate::proto::ReadNeedleBlobResponse>, Status> {
        let req = request.into_inner();
        let volume_id = VolumeId(req.volume_id);
        let offset = req.offset;
        let size = req.size;

        let storage_manager = self.storage_manager.clone();

        tokio::task::spawn_blocking(move || {
            if let Some(volume) = storage_manager.get_volume(&volume_id) {
                let result = volume.read_needle_blob(offset, size);
                match result {
                    Ok(data) => Ok(Response::new(crate::proto::ReadNeedleBlobResponse {
                        success: true,
                        needle_blob: data.to_vec(),
                    })),
                    Err(e) => Err(Status::internal(format!("{}", e))),
                }
            } else {
                Err(Status::not_found(format!(
                    "volume not found: {}",
                    volume_id.0
                )))
            }
        })
        .await
        .unwrap()
    }

    async fn read_needle_meta(
        &self,
        request: Request<crate::proto::ReadNeedleMetaRequest>,
    ) -> std::result::Result<Response<crate::proto::ReadNeedleMetaResponse>, Status> {
        let req = request.into_inner();
        let volume_id = VolumeId(req.volume_id);
        let file_key = req.file_key;

        let storage_manager = self.storage_manager.clone();

        tokio::task::spawn_blocking(move || {
            if let Some(volume) = storage_manager.get_volume(&volume_id) {
                if let Some(info) = volume.read_needle_meta(file_key) {
                    Ok(Response::new(crate::proto::ReadNeedleMetaResponse {
                        success: true,
                        cookie: 0,
                        last_modified: info.created_at.timestamp_nanos_opt().unwrap_or(0) as u64,
                        crc: info.checksum as u32,
                        ttl: "".to_string(),
                        append_at_ns: info.created_at.timestamp_nanos_opt().unwrap_or(0) as u64,
                    }))
                } else {
                    Err(Status::not_found(format!("needle not found: {}", file_key)))
                }
            } else {
                Err(Status::not_found(format!(
                    "volume not found: {}",
                    volume_id.0
                )))
            }
        })
        .await
        .unwrap()
    }

    async fn batch_delete(
        &self,
        request: Request<crate::proto::BatchDeleteRequest>,
    ) -> std::result::Result<Response<crate::proto::BatchDeleteResponse>, Status> {
        let req = request.into_inner();

        let mut results = Vec::new();

        for file_id in req.file_ids {
            let parts: Vec<&str> = file_id.split(',').collect();
            if parts.len() >= 3 {
                if let (Ok(volume_id), Ok(file_key)) =
                    (parts[0].parse::<u32>(), parts[1].parse::<u64>())
                {
                    let volume_id = VolumeId(volume_id);
                    let needle_id = NeedleId(file_key);

                    let storage_manager = self.storage_manager.clone();
                    let result = tokio::task::spawn_blocking(move || {
                        if let Some(volume) = storage_manager.get_volume(&volume_id) {
                            volume.delete_needle(&needle_id)
                        } else {
                            Err(PowerFsError::VolumeNotFound(volume_id))
                        }
                    })
                    .await
                    .unwrap();

                    match result {
                        Ok(_) => results.push(crate::proto::DeleteResult {
                            file_id: file_id.clone(),
                            status: 0,
                            error: "".to_string(),
                            size: 0,
                        }),
                        Err(e) => results.push(crate::proto::DeleteResult {
                            file_id: file_id.clone(),
                            status: -1,
                            error: format!("{}", e),
                            size: 0,
                        }),
                    }
                }
            }
        }

        Ok(Response::new(crate::proto::BatchDeleteResponse { results }))
    }

    async fn volume_status(
        &self,
        request: Request<crate::proto::VolumeStatusRequest>,
    ) -> std::result::Result<Response<crate::proto::VolumeStatusResponse>, Status> {
        let req = request.into_inner();
        let volume_id = VolumeId(req.volume_id);

        let storage_manager = self.storage_manager.clone();

        tokio::task::spawn_blocking(move || {
            if let Some(volume) = storage_manager.get_volume(&volume_id) {
                Ok(Response::new(crate::proto::VolumeStatusResponse {
                    success: true,
                    is_read_only: volume.is_read_only(),
                    volume_size: volume.size(),
                    file_count: volume.count() as u64,
                    file_deleted_count: volume.deleted_count() as u64,
                }))
            } else {
                Err(Status::not_found(format!(
                    "volume not found: {}",
                    volume_id.0
                )))
            }
        })
        .await
        .unwrap()
    }
}
