#![allow(clippy::result_large_err)]

use crate::proto::{VolumeService, VolumeServiceServer};
use crate::range_lease::RangeLeaseManager;
use bytes::Bytes;
use chrono::{Duration as ChronoDuration, Utc};
use log::{debug, error, info, warn};
use powerfs_common::{
    collect_system_metrics,
    error::{PowerFsError, Result},
    event::{Event, NodeStatusEvent, NullEventProvider, VolumeStatusEvent},
    traits::EventProvider,
    types::{NeedleId, NodeId, VolumeId},
};
use powerfs_core::storage::StorageManager;
use std::sync::Arc;
use std::time::{Duration, Instant};
use sysinfo::System;
use tokio::time;
use tonic::{transport::Server, Request, Response, Status};

const MAX_MERGE_ENTRIES: usize = 100;
const DEFAULT_STRIPE_SIZE: u64 = 64 * 1024 * 1024;

pub struct VolumeServer {
    storage_manager: Arc<StorageManager>,
    node_id: NodeId,
    event_provider: Arc<dyn EventProvider>,
    ip: String,
    grpc_port: u32,
    http_port: u32,
    data_dir: String,
    range_lease_mgr: Arc<RangeLeaseManager>,
}

impl VolumeServer {
    pub fn new(
        storage_manager: Arc<StorageManager>,
        node_id: NodeId,
        ip: &str,
        grpc_port: u32,
        http_port: u32,
        data_dir: &str,
    ) -> Self {
        let event_provider: Arc<dyn EventProvider> = match std::env::var("REDIS_URL") {
            #[cfg(feature = "redis-event")]
            Ok(url) => {
                info!("Event provider enabled with Redis: {}", url);
                Arc::new(powerfs_common::event::RedisEventProvider::new(
                    &url,
                    "powerfs_events",
                    "volume",
                ))
            }
            _ => {
                warn!("REDIS_URL not set, using null event provider");
                Arc::new(NullEventProvider)
            }
        };

        VolumeServer {
            storage_manager,
            node_id,
            event_provider,
            ip: ip.to_string(),
            grpc_port,
            http_port,
            data_dir: data_dir.to_string(),
            range_lease_mgr: Arc::new(RangeLeaseManager::new(DEFAULT_STRIPE_SIZE)),
        }
    }

    pub async fn start(mut self, address: &str) -> Result<()> {
        let addr: std::net::SocketAddr = address.parse()?;

        info!("Starting PowerFS Volume server on: {}", addr);
        info!("Node ID: {}", self.node_id.0);
        info!("Max message size: 256MB");

        self.start_node_status_publisher().await;

        // Range lease cleanup task: every 5 seconds remove expired leases
        let lease_mgr = self.range_lease_mgr.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let removed = lease_mgr.cleanup_expired();
                if removed > 0 {
                    debug!("Cleaned up {} expired range leases", removed);
                }
            }
        });

        // 内存泄漏诊断任务：每 30 秒打印一次关键指标
        tokio::spawn(async move {
            let mut prev_snapshot: Option<powerfs_master::tracking_allocator::AllocSnapshot> = None;
            let mut prev_vm_rss: u64 = 0;
            let mut tick = 0u64;
            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;
                tick += 1;

                let snap = powerfs_master::tracking_allocator::ALLOC_STATS.snapshot();
                let vm = powerfs_master::tracking_allocator::read_self_vm();
                let (rss_kb, data_kb, peak_kb) = vm.unwrap_or((0, 0, 0));

                let (delta_live_kb, delta_alloc_mb) = if let Some(prev) = prev_snapshot {
                    let d_live = snap.live_bytes().saturating_sub(prev.live_bytes());
                    let d_alloc = snap.alloc_bytes.saturating_sub(prev.alloc_bytes);
                    (d_live / 1024, d_alloc / 1024 / 1024)
                } else {
                    (0, 0)
                };
                let delta_rss_kb = rss_kb.saturating_sub(prev_vm_rss);

                info!(
                    "MEM_DIAG_VOLUME tick={} rss_mb={} data_mb={} peak_mb={} live_mb={} live_cnt={} \
                     delta_live_kb={} delta_rss_kb={} delta_alloc_mb={}",
                    tick,
                    rss_kb / 1024,
                    data_kb / 1024,
                    peak_kb / 1024,
                    snap.live_bytes() / 1024 / 1024,
                    snap.live_count(),
                    delta_live_kb,
                    delta_rss_kb,
                    delta_alloc_mb,
                );

                prev_snapshot = Some(snap);
                prev_vm_rss = rss_kb;
            }
        });

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

    async fn start_node_status_publisher(&mut self) {
        let provider = self.event_provider.clone();
        let node_id_str = self.node_id.0.clone();
        let ip = self.ip.clone();
        let grpc_port = self.grpc_port;
        let http_port = self.http_port;
        let data_dir = self.data_dir.clone();
        let storage_manager = self.storage_manager.clone();

        tokio::spawn(async move {
            info!("Starting node status publisher");

            let mut sys = System::new_all();
            sys.refresh_all();
            tokio::time::sleep(Duration::from_secs(1)).await;
            sys.refresh_all();

            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;

                let metrics = collect_system_metrics(&mut sys, &data_dir);

                let event = Event::NodeStatus(NodeStatusEvent {
                    node_id: node_id_str.clone(),
                    node_type: "volume".to_string(),
                    address: ip.clone(),
                    grpc_port,
                    http_port,
                    status: "healthy".to_string(),
                    cpu_usage: metrics.cpu_usage,
                    mem_usage: metrics.mem_usage,
                    disk_usage: metrics.disk_usage,
                    network_rx: metrics.network_rx,
                    network_tx: metrics.network_tx,
                    uptime: metrics.uptime,
                    volume_count: storage_manager.volume_count() as u32,
                    is_leader: false,
                    raft_term: 0,
                });

                if let Err(e) = provider.publish(event, &node_id_str).await {
                    warn!("Failed to publish node_status event: {}", e);
                }

                let volumes = storage_manager.list_volumes();
                for volume in volumes {
                    let volume_event = Event::VolumeStatus(VolumeStatusEvent {
                        volume_id: volume.id.0,
                        node_id: node_id_str.clone(),
                        size: volume.size,
                        used: volume.used,
                        file_count: volume.next_file_key - 1,
                        status: match volume.state {
                            powerfs_common::types::VolumeState::Creating => "creating",
                            powerfs_common::types::VolumeState::Available => "available",
                            powerfs_common::types::VolumeState::Full => "full",
                            powerfs_common::types::VolumeState::ReadOnly => "read_only",
                            powerfs_common::types::VolumeState::Deleting => "deleting",
                        }
                        .to_string(),
                        collection: volume.collection.0.clone(),
                    });

                    if let Err(e) = provider
                        .publish(volume_event, &format!("{}", volume.id.0))
                        .await
                    {
                        warn!(
                            "Failed to publish volume_status event for volume {}: {}",
                            volume.id.0, e
                        );
                    }
                }
            }
        });
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

                let provider = self.event_provider.clone();
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
                    if let Err(e) = provider.publish(event, &format!("{}", vid_clone)).await {
                        warn!("Failed to publish volume_status event: {}", e);
                    }
                });

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

    async fn restore_needle(
        &self,
        request: Request<crate::proto::RestoreNeedleRequest>,
    ) -> std::result::Result<Response<crate::proto::RestoreNeedleResponse>, Status> {
        let req = request.into_inner();
        let volume_id = VolumeId(req.volume_id);
        let needle_id = NeedleId(req.file_key);

        debug!(
            "restore_needle: volume_id={}, file_key={}",
            volume_id.0, needle_id.0
        );

        let storage_manager = self.storage_manager.clone();

        match tokio::task::spawn_blocking(move || {
            if let Some(volume) = storage_manager.get_volume(&volume_id) {
                let result = volume.restore_needle(&needle_id);
                match result {
                    Ok(_) => Ok(Response::new(crate::proto::RestoreNeedleResponse {
                        success: true,
                    })),
                    Err(e) => {
                        warn!("restore_needle failed: {}", e);
                        Err(Status::internal(format!("{}", e)))
                    }
                }
            } else {
                warn!("restore_needle: volume not found: {}", volume_id.0);
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
                error!("restore_needle task failed: {}", e);
                Err(Status::internal(format!("task failed: {}", e)))
            }
        }
    }

    async fn worm_lock(
        &self,
        request: Request<crate::proto::WormLockRequest>,
    ) -> std::result::Result<Response<crate::proto::WormLockResponse>, Status> {
        let req = request.into_inner();
        let volume_id = VolumeId(req.volume_id);
        let needle_id = NeedleId(req.file_key);
        let retention_days = req.retention_days;

        debug!(
            "worm_lock: volume_id={}, file_key={}, retention_days={}",
            volume_id.0, needle_id.0, retention_days
        );

        let storage_manager = self.storage_manager.clone();

        match tokio::task::spawn_blocking(move || {
            if let Some(volume) = storage_manager.get_volume(&volume_id) {
                let result = volume.worm_lock(&needle_id, retention_days);
                match result {
                    Ok(_) => {
                        let retention_until = Utc::now() + ChronoDuration::days(retention_days);
                        Ok(Response::new(crate::proto::WormLockResponse {
                            success: true,
                            retention_until: retention_until.to_rfc3339(),
                        }))
                    }
                    Err(e) => {
                        warn!("worm_lock failed: {}", e);
                        Err(Status::internal(format!("{}", e)))
                    }
                }
            } else {
                warn!("worm_lock: volume not found: {}", volume_id.0);
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
                error!("worm_lock task failed: {}", e);
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

                if entries.len() <= MAX_MERGE_ENTRIES {
                    if entries.len() <= 1 {
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
                                warn!(
                                    "batch_write_needle_blob entry failed: file_key={}, offset={}, size={}, err={:?}",
                                    file_key, entry.offset, entry.size, result
                                );
                            }
                        }
                    } else {
                        info!(
                            "batch_write_needle_blob: merging {} entries for file_key={}",
                            entries.len(),
                            file_key
                        );
                        let mut max_end: i64 = 0;
                        for entry in &entries {
                            let end = entry.offset + entry.size as i64;
                            if end > max_end {
                                max_end = end;
                            }
                        }

                        let existing_data_opt = volume
                            .read_needle_meta(file_key)
                            .and_then(|meta| {
                                let data_size = meta.data_size as i32;
                                if data_size > 0 {
                                    volume.read_needle_blob(file_key, 0, data_size).ok()
                                } else {
                                    None
                                }
                            });

                        let merged_data: Vec<u8>;
                        if let Some(existing_data) = existing_data_opt {
                            let existing_len = existing_data.len();
                            let total_len = existing_len.max(max_end as usize);
                            let mut data = vec![0u8; total_len];
                            data[..existing_len].copy_from_slice(&existing_data);
                            for entry in &entries {
                                let offset = entry.offset as usize;
                                let len = entry.size as usize;
                                if offset + len <= data.len() {
                                    data[offset..offset + len]
                                        .copy_from_slice(&entry.needle_blob[..len]);
                                }
                            }
                            merged_data = data;
                        } else {
                            let mut data = vec![0u8; max_end as usize];
                            for entry in &entries {
                                let offset = entry.offset as usize;
                                let len = entry.size as usize;
                                if offset + len <= data.len() {
                                    data[offset..offset + len]
                                        .copy_from_slice(&entry.needle_blob[..len]);
                                }
                            }
                            merged_data = data;
                        }

                        match volume.write_needle(file_key, Bytes::from(merged_data)) {
                            Ok(_) => success_count = entries.len() as i32,
                            Err(e) => {
                                warn!("batch_write_needle_blob merged write failed: {:?}", e);
                            }
                        }
                    }
                } else {
                    info!(
                        "batch_write_needle_blob: splitting {} entries into batches for file_key={}",
                        entries.len(),
                        file_key
                    );
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
                            warn!(
                                "batch_write_needle_blob entry failed: file_key={}, offset={}, size={}, err={:?}",
                                file_key, entry.offset, entry.size, result
                            );
                        }
                    }
                }
                Ok(Response::new(
                    crate::proto::powerfs::BatchWriteNeedleBlobResponse {
                        success: success_count == total_entries as i32,
                        success_count,
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

    async fn acquire_range_lease(
        &self,
        request: Request<crate::proto::RangeLeaseRequest>,
    ) -> std::result::Result<Response<crate::proto::RangeLeaseResponse>, Status> {
        let req = request.into_inner();
        info!(
            "acquire_range_lease: inode={}, stripe_start={}, stripe_count={}, client={}, exclusive={}",
            req.inode, req.stripe_start, req.stripe_count, req.client_id, req.exclusive
        );

        let result = self.range_lease_mgr.acquire(
            req.inode,
            req.stripe_start,
            req.stripe_count,
            &req.client_id,
            req.duration_ms,
            req.exclusive,
            req.stripe_size,
        );

        match result {
            Ok(lease) => {
                let granted_stripes: Vec<u64> =
                    (lease.stripe_start..lease.stripe_start + lease.stripe_count).collect();
                let expire_at_ms = {
                    let remaining = lease.expire_at.saturating_duration_since(Instant::now());
                    remaining.as_millis() as u64
                };
                Ok(Response::new(crate::proto::RangeLeaseResponse {
                    success: true,
                    error: String::new(),
                    lease_token: lease.token.clone(),
                    epoch: lease.epoch,
                    granted_stripes,
                    expire_at_ms,
                }))
            }
            Err(e) => {
                warn!("acquire_range_lease failed: {}", e);
                Ok(Response::new(crate::proto::RangeLeaseResponse {
                    success: false,
                    error: e,
                    lease_token: String::new(),
                    epoch: 0,
                    granted_stripes: vec![],
                    expire_at_ms: 0,
                }))
            }
        }
    }

    async fn release_range_lease(
        &self,
        request: Request<crate::proto::RangeLeaseReleaseRequest>,
    ) -> std::result::Result<Response<crate::proto::RangeLeaseReleaseResponse>, Status> {
        let req = request.into_inner();
        debug!(
            "release_range_lease: token={}, client={}",
            req.lease_token, req.client_id
        );

        match self
            .range_lease_mgr
            .release(&req.lease_token, &req.client_id)
        {
            Ok(()) => Ok(Response::new(crate::proto::RangeLeaseReleaseResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => {
                warn!("release_range_lease failed: {}", e);
                Ok(Response::new(crate::proto::RangeLeaseReleaseResponse {
                    success: false,
                    error: e,
                }))
            }
        }
    }

    async fn renew_range_lease(
        &self,
        request: Request<crate::proto::RangeLeaseRenewRequest>,
    ) -> std::result::Result<Response<crate::proto::RangeLeaseRenewResponse>, Status> {
        let req = request.into_inner();
        debug!(
            "renew_range_lease: token={}, client={}",
            req.lease_token, req.client_id
        );

        match self
            .range_lease_mgr
            .renew(&req.lease_token, &req.client_id, req.duration_ms)
        {
            Ok(()) => Ok(Response::new(crate::proto::RangeLeaseRenewResponse {
                success: true,
                error: String::new(),
                epoch: 0,
            })),
            Err(e) => {
                warn!("renew_range_lease failed: {}", e);
                Ok(Response::new(crate::proto::RangeLeaseRenewResponse {
                    success: false,
                    error: e,
                    epoch: 0,
                }))
            }
        }
    }
}
