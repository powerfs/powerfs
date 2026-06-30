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
use uuid::Uuid;

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
            .add_service(VolumeServiceServer::new(self))
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

        let volume_id = VolumeId(
            Uuid::parse_str(&req.volume_id)
                .map_err(|e| Status::invalid_argument(format!("invalid volume id: {}", e)))?,
        );

        let result = self
            .storage_manager
            .create_volume(volume_id.clone(), req.size);

        match result {
            Ok(info) => {
                debug!("Created volume: {:?}", info.id);
                Ok(Response::new(crate::proto::CreateVolumeResponse {
                    success: true,
                    volume_id: info.id.0.to_string(),
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

        let volume_id = VolumeId(
            Uuid::parse_str(&req.volume_id)
                .map_err(|e| Status::invalid_argument(format!("invalid volume id: {}", e)))?,
        );

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

        let volume_id = VolumeId(
            Uuid::parse_str(&req.volume_id)
                .map_err(|e| Status::invalid_argument(format!("invalid volume id: {}", e)))?,
        );

        let storage_manager = self.storage_manager.clone();

        tokio::task::spawn_blocking(move || {
            if let Some(volume) = storage_manager.get_volume(&volume_id) {
                let result = volume.write_needle(Bytes::from(req.data));
                match result {
                    Ok(info) => Ok(Response::new(crate::proto::WriteNeedleResponse {
                        success: true,
                        needle_id: info.id.0.to_string(),
                        offset: info.offset,
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

        let volume_id = VolumeId(
            Uuid::parse_str(&req.volume_id)
                .map_err(|e| Status::invalid_argument(format!("invalid volume id: {}", e)))?,
        );

        let needle_id = NeedleId(
            Uuid::parse_str(&req.needle_id)
                .map_err(|e| Status::invalid_argument(format!("invalid needle id: {}", e)))?,
        );

        let storage_manager = self.storage_manager.clone();

        tokio::task::spawn_blocking(move || {
            if let Some(volume) = storage_manager.get_volume(&volume_id) {
                let result = volume.read_needle(&needle_id);
                match result {
                    Ok(data) => Ok(Response::new(crate::proto::ReadNeedleResponse {
                        success: true,
                        data: data.to_vec(),
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

        let volume_id = VolumeId(
            Uuid::parse_str(&req.volume_id)
                .map_err(|e| Status::invalid_argument(format!("invalid volume id: {}", e)))?,
        );

        let needle_id = NeedleId(
            Uuid::parse_str(&req.needle_id)
                .map_err(|e| Status::invalid_argument(format!("invalid needle id: {}", e)))?,
        );

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
                volume_id: v.id.0.to_string(),
                node_id: v.node_id.0,
                size: v.size,
                used: v.used,
                replica_count: v.replica_count,
                state: v.state as i32,
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
}
