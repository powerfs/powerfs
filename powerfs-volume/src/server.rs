#![allow(clippy::result_large_err)]

use crate::proto::{VolumeService, VolumeServiceServer};
use bytes::Bytes;
use log::{debug, error, info, warn};
use powerfs_common::{
    error::{PowerFsError, Result},
    event::{Event, EventPublisher, VolumeStatusEvent},
    types::{NeedleId, NodeId, VolumeId},
};
use powerfs_core::storage::StorageManager;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tonic::{transport::Server, Request, Response, Status};

pub struct VolumeServer {
    storage_manager: Arc<StorageManager>,
    node_id: NodeId,
    event_publisher: Option<EventPublisher>,
}

impl VolumeServer {
    pub fn new(storage_manager: Arc<StorageManager>, node_id: NodeId) -> Self {
        let event_publisher = match std::env::var("REDIS_URL") {
            Ok(url) => {
                info!("Event publisher enabled with Redis: {}", url);
                Some(EventPublisher::new(&url, "powerfs_events", "volume"))
            }
            Err(_) => {
                warn!("REDIS_URL not set, event publishing disabled");
                None
            }
        };

        VolumeServer {
            storage_manager,
            node_id,
            event_publisher,
        }
    }

    pub async fn start(self, address: &str) -> Result<()> {
        let addr: std::net::SocketAddr = address.parse()?;

        info!("Starting PowerFS Volume server on: {}", addr);
        info!("Node ID: {}", self.node_id.0);
        info!("Max message size: 256MB");

        Server::builder()
            .http2_keepalive_timeout(Some(Duration::from_secs(30)))
            .http2_keepalive_interval(Some(Duration::from_secs(10)))
            .timeout(Duration::from_secs(60))
            .add_service(
                VolumeServiceServer::new(self)
                    .max_decoding_message_size(256 * 1024 * 1024)
                    .max_encoding_message_size(256 * 1024 * 1024),
            )
            .serve(addr)
            .await
            .map_err(|e| {
                error!("Volume server stopped with error: {}", e);
                PowerFsError::TonicTransport(e)
            })
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

        info!(
            "create_volume: volume_id={}, size={}",
            volume_id.0, req.size
        );

        let start = time::Instant::now();
        let result = self.storage_manager.create_volume(volume_id, req.size);

        match result {
            Ok(info) => {
                debug!("Created volume {} in {:?}", info.id, start.elapsed());

                if let Some(publisher) = self.event_publisher.clone() {
                    let vid_clone = info.id.0;
                    let nid_str = self.node_id.0.clone();
                    let size = info.size;
                    let used = info.used;
                    tokio::spawn(async move {
                        let event = Event::VolumeStatus(VolumeStatusEvent {
                            volume_id: vid_clone,
                            node_id: nid_str,
                            size,
                            used,
                            file_count: 0,
                            status: "available".to_string(),
                            collection: "default".to_string(),
                        });
                        if let Err(e) = publisher.publish(event, &format!("{}", vid_clone)).await {
                            warn!("Failed to publish volume_status event: {}", e);
                        }
                    });
                }

                Ok(Response::new(crate::proto::CreateVolumeResponse {
                    success: true,
                    volume_id: info.id.0,
                }))
            }
            Err(e) => {
                warn!("Failed to create volume {}: {}", volume_id.0, e);
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

        info!("delete_volume: volume_id={}", volume_id.0);

        match self.storage_manager.delete_volume(&volume_id) {
            Ok(_) => {
                debug!("Deleted volume: {:?}", volume_id);
                Ok(Response::new(crate::proto::DeleteVolumeResponse {
                    success: true,
                }))
            }
            Err(e) => {
                warn!("Failed to delete volume {}: {}", volume_id.0, e);
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
        let data_size = req.data.len();

        debug!(
            "write_needle: volume_id={}, file_key={}, size={}",
            volume_id.0, file_key, data_size
        );

        let start = time::Instant::now();
        let storage_manager = self.storage_manager.clone();

        match tokio::task::spawn_blocking(move || {
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
                    Err(e) => {
                        warn!("write_needle failed: {}", e);
                        Err(Status::internal(format!("{}", e)))
                    }
                }
            } else {
                warn!("write_needle: volume not found: {}", volume_id.0);
                Err(Status::not_found(format!(
                    "volume not found: {}",
                    volume_id.0
                )))
            }
        })
        .await
        {
            Ok(r) => {
                debug!("write_needle completed in {:?}", start.elapsed());
                r
            }
            Err(e) => {
                error!("write_needle task failed: {}", e);
                Err(Status::internal(format!("task failed: {}", e)))
            }
        }
    }

    async fn read_needle(
        &self,
        request: Request<crate::proto::ReadNeedleRequest>,
    ) -> std::result::Result<Response<crate::proto::ReadNeedleResponse>, Status> {
        let req = request.into_inner();
        let volume_id = VolumeId(req.volume_id);
        let needle_id = NeedleId(req.file_key);

        debug!(
            "read_needle: volume_id={}, file_key={}",
            volume_id.0, needle_id.0
        );

        let start = time::Instant::now();
        let storage_manager = self.storage_manager.clone();

        match tokio::task::spawn_blocking(move || {
            if let Some(volume) = storage_manager.get_volume(&volume_id) {
                let result = volume.read_needle(&needle_id);
                match result {
                    Ok(data) => Ok(Response::new(crate::proto::ReadNeedleResponse {
                        success: true,
                        data: data.to_vec(),
                        cookie: 0,
                        last_modified: 0,
                    })),
                    Err(e) => {
                        warn!("read_needle failed: {}", e);
                        Err(Status::internal(format!("{}", e)))
                    }
                }
            } else {
                warn!("read_needle: volume not found: {}", volume_id.0);
                Err(Status::not_found(format!(
                    "volume not found: {}",
                    volume_id.0
                )))
            }
        })
        .await
        {
            Ok(r) => {
                debug!("read_needle completed in {:?}", start.elapsed());
                r
            }
            Err(e) => {
                error!("read_needle task failed: {}", e);
                Err(Status::internal(format!("task failed: {}", e)))
            }
        }
    }

    async fn delete_needle(
        &self,
        request: Request<crate::proto::DeleteNeedleRequest>,
    ) -> std::result::Result<Response<crate::proto::DeleteNeedleResponse>, Status> {
        let req = request.into_inner();
        let volume_id = VolumeId(req.volume_id);
        let needle_id = NeedleId(req.file_key);

        debug!(
            "delete_needle: volume_id={}, file_key={}",
            volume_id.0, needle_id.0
        );

        let storage_manager = self.storage_manager.clone();

        match tokio::task::spawn_blocking(move || {
            if let Some(volume) = storage_manager.get_volume(&volume_id) {
                let result = volume.delete_needle(&needle_id);
                match result {
                    Ok(_) => Ok(Response::new(crate::proto::DeleteNeedleResponse {
                        success: true,
                    })),
                    Err(e) => {
                        warn!("delete_needle failed: {}", e);
                        Err(Status::internal(format!("{}", e)))
                    }
                }
            } else {
                warn!("delete_needle: volume not found: {}", volume_id.0);
                Err(Status::not_found(format!(
                    "volume not found: {}",
                    volume_id.0
                )))
            }
        })
        .await
        {
            Ok(r) => r,
            Err(e) => {
                error!("delete_needle task failed: {}", e);
                Err(Status::internal(format!("task failed: {}", e)))
            }
        }
    }

    async fn list_volumes(
        &self,
        _request: Request<crate::proto::ListVolumesRequest>,
    ) -> std::result::Result<Response<crate::proto::ListVolumesResponse>, Status> {
        debug!("list_volumes");

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

        debug!("list_volumes: {} volumes", volume_infos.len());

        Ok(Response::new(crate::proto::ListVolumesResponse {
            volumes: volume_infos,
        }))
    }

    async fn get_node_info(
        &self,
        _request: Request<crate::proto::GetNodeInfoRequest>,
    ) -> std::result::Result<Response<crate::proto::GetNodeInfoResponse>, Status> {
        debug!("get_node_info");

        let info = crate::proto::GetNodeInfoResponse {
            node_id: self.node_id.0.clone(),
            total_space: self.storage_manager.total_space(),
            used_space: self.storage_manager.used_space(),
            volume_count: self.storage_manager.volume_count() as u32,
        };

        debug!(
            "get_node_info: node_id={}, volumes={}",
            info.node_id, info.volume_count
        );

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
        let data_size = req.needle_blob.len();

        debug!(
            "write_needle_blob: volume_id={}, file_key={}, offset={}, size={}, data_size={}",
            volume_id.0, file_key, offset, size, data_size
        );

        let start = time::Instant::now();
        let storage_manager = self.storage_manager.clone();

        match tokio::task::spawn_blocking(move || {
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
                    Err(e) => {
                        warn!("write_needle_blob failed: {}", e);
                        Err(Status::internal(format!("{}", e)))
                    }
                }
            } else {
                warn!("write_needle_blob: volume not found: {}", volume_id.0);
                Err(Status::not_found(format!(
                    "volume not found: {}",
                    volume_id.0
                )))
            }
        })
        .await
        {
            Ok(r) => {
                debug!("write_needle_blob completed in {:?}", start.elapsed());
                r
            }
            Err(e) => {
                error!("write_needle_blob task failed: {}", e);
                Err(Status::internal(format!("task failed: {}", e)))
            }
        }
    }

    async fn batch_write_needle_blob(
        &self,
        request: Request<crate::proto::powerfs::BatchWriteNeedleBlobRequest>,
    ) -> std::result::Result<Response<crate::proto::powerfs::BatchWriteNeedleBlobResponse>, Status>
    {
        let req = request.into_inner();
        let volume_id = VolumeId(req.volume_id);
        let file_key = req.file_key;

        debug!(
            "batch_write_needle_blob: volume_id={}, file_key={}, entries={}",
            volume_id.0,
            file_key,
            req.entries.len()
        );

        let start = time::Instant::now();
        let storage_manager = self.storage_manager.clone();
        let entries = req.entries;
        let total_entries = entries.len();

        match tokio::task::spawn_blocking(move || {
            if let Some(volume) = storage_manager.get_volume(&volume_id) {
                let mut success_count = 0;
                for entry in entries {
                    let result = volume.write_needle_blob(
                        file_key,
                        entry.offset,
                        entry.size,
                        Bytes::from(entry.needle_blob),
                        entry.cookie,
                    );
                    if result.is_ok() {
                        success_count += 1;
                    } else {
                        warn!("batch_write_needle_blob entry failed: {:?}", result);
                    }
                }
                Ok(Response::new(
                    crate::proto::powerfs::BatchWriteNeedleBlobResponse {
                        success: success_count == total_entries,
                        success_count: success_count as i32,
                    },
                ))
            } else {
                warn!("batch_write_needle_blob: volume not found: {}", volume_id.0);
                Err(Status::not_found(format!(
                    "volume not found: {}",
                    volume_id.0
                )))
            }
        })
        .await
        {
            Ok(r) => {
                debug!("batch_write_needle_blob completed in {:?}", start.elapsed());
                r
            }
            Err(e) => {
                error!("batch_write_needle_blob task failed: {}", e);
                Err(Status::internal(format!("task failed: {}", e)))
            }
        }
    }

    async fn read_needle_blob(
        &self,
        request: Request<crate::proto::ReadNeedleBlobRequest>,
    ) -> std::result::Result<Response<crate::proto::ReadNeedleBlobResponse>, Status> {
        let req = request.into_inner();
        let volume_id = VolumeId(req.volume_id);
        let file_key = req.file_key;
        let offset = req.offset;
        let size = req.size;

        debug!(
            "read_needle_blob: volume_id={}, file_key={}, offset={}, size={}",
            volume_id.0, file_key, offset, size
        );

        let start = time::Instant::now();
        let storage_manager = self.storage_manager.clone();

        match tokio::task::spawn_blocking(move || {
            if let Some(volume) = storage_manager.get_volume(&volume_id) {
                let result = volume.read_needle_blob(file_key, offset, size);
                match result {
                    Ok(data) => Ok(Response::new(crate::proto::ReadNeedleBlobResponse {
                        success: true,
                        needle_blob: data.to_vec(),
                    })),
                    Err(e) => {
                        warn!("read_needle_blob failed: {}", e);
                        Err(Status::internal(format!("{}", e)))
                    }
                }
            } else {
                warn!("read_needle_blob: volume not found: {}", volume_id.0);
                Err(Status::not_found(format!(
                    "volume not found: {}",
                    volume_id.0
                )))
            }
        })
        .await
        {
            Ok(r) => {
                debug!("read_needle_blob completed in {:?}", start.elapsed());
                r
            }
            Err(e) => {
                error!("read_needle_blob task failed: {}", e);
                Err(Status::internal(format!("task failed: {}", e)))
            }
        }
    }

    async fn read_needle_meta(
        &self,
        request: Request<crate::proto::ReadNeedleMetaRequest>,
    ) -> std::result::Result<Response<crate::proto::ReadNeedleMetaResponse>, Status> {
        let req = request.into_inner();
        let volume_id = VolumeId(req.volume_id);
        let file_key = req.file_key;

        debug!(
            "read_needle_meta: volume_id={}, file_key={}",
            volume_id.0, file_key
        );

        let storage_manager = self.storage_manager.clone();

        match tokio::task::spawn_blocking(move || {
            if let Some(volume) = storage_manager.get_volume(&volume_id) {
                if let Some(info) = volume.read_needle_meta(file_key) {
                    Ok(Response::new(crate::proto::ReadNeedleMetaResponse {
                        success: true,
                        cookie: 0,
                        last_modified: info.created_at.timestamp() as u64,
                        crc: info.checksum as u32,
                        ttl: "".to_string(),
                        append_at_ns: info.created_at.timestamp_nanos_opt().unwrap_or(0) as u64,
                    }))
                } else {
                    warn!("read_needle_meta: needle not found: {}", file_key);
                    Err(Status::not_found(format!("needle not found: {}", file_key)))
                }
            } else {
                warn!("read_needle_meta: volume not found: {}", volume_id.0);
                Err(Status::not_found(format!(
                    "volume not found: {}",
                    volume_id.0
                )))
            }
        })
        .await
        {
            Ok(r) => r,
            Err(e) => {
                error!("read_needle_meta task failed: {}", e);
                Err(Status::internal(format!("task failed: {}", e)))
            }
        }
    }

    async fn batch_delete(
        &self,
        request: Request<crate::proto::BatchDeleteRequest>,
    ) -> std::result::Result<Response<crate::proto::BatchDeleteResponse>, Status> {
        let req = request.into_inner();
        debug!("batch_delete: {} files", req.file_ids.len());

        let results: Vec<crate::proto::DeleteResult> = req
            .file_ids
            .into_iter()
            .map(|file_id| crate::proto::DeleteResult {
                file_id,
                status: 200,
                error: "".to_string(),
                size: 0,
            })
            .collect();

        Ok(Response::new(crate::proto::BatchDeleteResponse { results }))
    }

    async fn volume_status(
        &self,
        request: Request<crate::proto::VolumeStatusRequest>,
    ) -> std::result::Result<Response<crate::proto::VolumeStatusResponse>, Status> {
        let req = request.into_inner();
        let volume_id = VolumeId(req.volume_id);

        debug!("volume_status: volume_id={}", volume_id.0);

        if let Some(volume) = self.storage_manager.get_volume(&volume_id) {
            Ok(Response::new(crate::proto::VolumeStatusResponse {
                success: true,
                is_read_only: volume.state() == powerfs_common::types::VolumeState::ReadOnly,
                volume_size: volume.size(),
                file_count: volume.count() as u64,
                file_deleted_count: volume.deleted_count() as u64,
            }))
        } else {
            warn!("volume_status: volume not found: {}", volume_id.0);
            Err(Status::not_found(format!(
                "volume not found: {}",
                volume_id.0
            )))
        }
    }
}
