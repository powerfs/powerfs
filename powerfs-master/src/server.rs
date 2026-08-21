use super::kv_cache_service::KvCacheServiceImpl;
use super::master::{
    AddNodeParams, FilerNodeInfo, FuseClientInfo, MasterNode, UpdateNodeVolumesParams,
};
use super::metrics::{ASSIGN_REQUEST_COUNT, LOOKUP_REQUEST_COUNT, REQUEST_COUNT};
use super::proto::powerfs::*;
use super::proto::*;
use futures::Stream;
use log::{error, info, warn};
use powerfs_allocator::config::{MigrationPolicy, RebalancePolicy};
use powerfs_allocator::management::{ManagementApi, RebalanceAction};
use powerfs_allocator::{MigrationState, MigrationTaskStatus, MigrationType};
use powerfs_common::constants::DEFAULT_VOLUME_SIZE;
use powerfs_common::types::VolumeId;
use powerfs_core::kv_cache::KVCacheEngine;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tonic::{transport::Server, Request, Response, Status, Streaming};
use uuid::Uuid;

pub struct MasterGrpcServer {
    master: Arc<MasterNode>,
    kv_cache: Arc<KVCacheEngine>,
}

impl MasterGrpcServer {
    pub fn new(master: Arc<MasterNode>, kv_cache: Arc<KVCacheEngine>) -> Self {
        MasterGrpcServer { master, kv_cache }
    }

    pub async fn start(self, addr: std::net::SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        let kv_svc = KvCacheServiceImpl {
            engine: self.kv_cache.clone(),
            volume_client_pool: self.master.volume_client_pool.clone(),
            master: self.master.clone(),
        };

        let max_concurrent_streams = 64;

        Server::builder()
            .http2_keepalive_interval(Some(Duration::from_secs(5)))
            .http2_keepalive_timeout(Some(Duration::from_secs(15)))
            .max_concurrent_streams(max_concurrent_streams)
            .add_service(MasterServiceServer::new(self))
            .add_service(KvCacheServiceServer::new(kv_svc))
            .serve(addr)
            .await?;
        Ok(())
    }
}

// ========================================================================
// proto <-> Rust conversion helpers for the Collection P0 attribute model
// ========================================================================

fn rust_status_to_proto(status: crate::collection::CollectionStatus) -> i32 {
    match status {
        crate::collection::CollectionStatus::Active => CollectionStatus::Active as i32,
        crate::collection::CollectionStatus::Readonly => CollectionStatus::Readonly as i32,
        crate::collection::CollectionStatus::Archived => CollectionStatus::Archived as i32,
        crate::collection::CollectionStatus::Deleted => CollectionStatus::Deleted as i32,
    }
}

fn proto_status_to_rust(status: i32) -> crate::collection::CollectionStatus {
    match status {
        x if x == CollectionStatus::Readonly as i32 => {
            crate::collection::CollectionStatus::Readonly
        }
        x if x == CollectionStatus::Archived as i32 => {
            crate::collection::CollectionStatus::Archived
        }
        x if x == CollectionStatus::Deleted as i32 => crate::collection::CollectionStatus::Deleted,
        // Unspecified (0) or Active (1) both map to Active.
        _ => crate::collection::CollectionStatus::Active,
    }
}

fn redundancy_to_proto(r: &crate::collection::RedundancyMode) -> Redundancy {
    Redundancy {
        mode: Some(match r {
            crate::collection::RedundancyMode::Replication { copies } => {
                powerfs::redundancy::Mode::Replication(ReplicationMode { copies: *copies })
            }
            crate::collection::RedundancyMode::ErasureCoding {
                data_shards,
                parity_shards,
                algorithm,
            } => powerfs::redundancy::Mode::ErasureCoding(ErasureCodingMode {
                data_shards: *data_shards,
                parity_shards: *parity_shards,
                algorithm: algorithm.clone(),
            }),
        }),
    }
}

fn storage_policy_to_proto(p: &crate::collection::StoragePolicy) -> StoragePolicy {
    StoragePolicy {
        name: p.name.clone(),
        redundancy: Some(redundancy_to_proto(&p.redundancy)),
        min_write_nodes: p.min_write_nodes,
    }
}

fn volume_allocation_to_proto(a: &crate::collection::VolumeAllocationMode) -> VolumeAllocation {
    VolumeAllocation {
        mode: Some(match a {
            crate::collection::VolumeAllocationMode::Auto { count, volume_size } => {
                powerfs::volume_allocation::Mode::Auto(AutoAllocation {
                    count: *count,
                    volume_size: *volume_size,
                })
            }
            crate::collection::VolumeAllocationMode::Manual { volume_ids } => {
                powerfs::volume_allocation::Mode::Manual(ManualAllocation {
                    volume_ids: volume_ids.clone(),
                })
            }
            crate::collection::VolumeAllocationMode::Hybrid {
                fixed_volume_ids,
                auto_count,
            } => powerfs::volume_allocation::Mode::Hybrid(HybridAllocation {
                fixed_volume_ids: fixed_volume_ids.clone(),
                auto_count: *auto_count,
            }),
        }),
    }
}

fn collection_info_to_proto(info: crate::collection::CollectionInfo) -> CollectionInfo {
    CollectionInfo {
        name: info.name,
        status: rust_status_to_proto(info.status),
        storage_policy: Some(storage_policy_to_proto(&info.storage_policy)),
        disk_type: info.disk_type,
        capacity_quota_bytes: info.capacity_quota_bytes,
        volume_count: info.volume_count,
        ttl_seconds: info.ttl_seconds,
        created_at: info.created_at,
        updated_at: info.updated_at,
        description: info.description,
        volume_allocation: Some(volume_allocation_to_proto(&info.volume_allocation)),
        excluded_volume_ids: info.excluded_volume_ids,
    }
}

fn redundancy_from_proto(r: &Redundancy) -> crate::collection::RedundancyMode {
    match &r.mode {
        Some(powerfs::redundancy::Mode::Replication(rep)) => {
            crate::collection::RedundancyMode::Replication { copies: rep.copies }
        }
        Some(powerfs::redundancy::Mode::ErasureCoding(ec)) => {
            crate::collection::RedundancyMode::ErasureCoding {
                data_shards: ec.data_shards,
                parity_shards: ec.parity_shards,
                algorithm: ec.algorithm.clone(),
            }
        }
        None => crate::collection::RedundancyMode::default(),
    }
}

fn storage_policy_from_proto(p: &StoragePolicy) -> crate::collection::StoragePolicy {
    crate::collection::StoragePolicy {
        name: p.name.clone(),
        redundancy: p
            .redundancy
            .as_ref()
            .map(redundancy_from_proto)
            .unwrap_or_default(),
        min_write_nodes: p.min_write_nodes,
    }
}

fn volume_allocation_from_proto(a: &VolumeAllocation) -> crate::collection::VolumeAllocationMode {
    match &a.mode {
        Some(powerfs::volume_allocation::Mode::Auto(auto)) => {
            crate::collection::VolumeAllocationMode::Auto {
                count: auto.count,
                volume_size: auto.volume_size,
            }
        }
        Some(powerfs::volume_allocation::Mode::Manual(manual)) => {
            crate::collection::VolumeAllocationMode::Manual {
                volume_ids: manual.volume_ids.clone(),
            }
        }
        Some(powerfs::volume_allocation::Mode::Hybrid(hybrid)) => {
            crate::collection::VolumeAllocationMode::Hybrid {
                fixed_volume_ids: hybrid.fixed_volume_ids.clone(),
                auto_count: hybrid.auto_count,
            }
        }
        None => crate::collection::VolumeAllocationMode::default(),
    }
}

fn collection_info_from_create_request(
    req: &CreateCollectionRequest,
) -> crate::collection::CollectionInfo {
    let now = chrono::Utc::now().timestamp();
    crate::collection::CollectionInfo {
        name: req.name.clone(),
        status: proto_status_to_rust(req.status),
        storage_policy: req
            .storage_policy
            .as_ref()
            .map(storage_policy_from_proto)
            .unwrap_or_default(),
        disk_type: req.disk_type.clone(),
        capacity_quota_bytes: req.capacity_quota_bytes,
        volume_count: req.volume_count,
        ttl_seconds: req.ttl_seconds,
        created_at: now,
        updated_at: now,
        description: req.description.clone(),
        volume_allocation: req
            .volume_allocation
            .as_ref()
            .map(volume_allocation_from_proto)
            .unwrap_or_default(),
        excluded_volume_ids: req.excluded_volume_ids.clone(),
    }
}

fn collection_info_from_update_request(
    req: &UpdateCollectionRequest,
    volume_count: u32,
    created_at: i64,
) -> crate::collection::CollectionInfo {
    let now = chrono::Utc::now().timestamp();
    crate::collection::CollectionInfo {
        name: req.name.clone(),
        status: proto_status_to_rust(req.status),
        storage_policy: req
            .storage_policy
            .as_ref()
            .map(storage_policy_from_proto)
            .unwrap_or_default(),
        disk_type: req.disk_type.clone(),
        capacity_quota_bytes: req.capacity_quota_bytes,
        volume_count,
        ttl_seconds: req.ttl_seconds,
        created_at,
        updated_at: now,
        description: req.description.clone(),
        volume_allocation: req
            .volume_allocation
            .as_ref()
            .map(volume_allocation_from_proto)
            .unwrap_or_default(),
        excluded_volume_ids: req.excluded_volume_ids.clone(),
    }
}

fn collection_stats_to_proto(stats: crate::collection::CollectionStats) -> CollectionStatsInfo {
    CollectionStatsInfo {
        used_bytes: stats.used_bytes,
        file_count: stats.file_count,
        volume_count: stats.volume_count,
        writable_volume_count: stats.writable_volume_count,
        read_ops: stats.read_ops,
        write_ops: stats.write_ops,
        read_bytes: stats.read_bytes,
        write_bytes: stats.write_bytes,
    }
}

#[tonic::async_trait]
impl MasterService for MasterGrpcServer {
    type SendHeartbeatStream =
        Pin<Box<dyn Stream<Item = Result<HeartbeatResponse, Status>> + Send + 'static>>;

    type KeepConnectedStream =
        Pin<Box<dyn Stream<Item = Result<KeepConnectedResponse, Status>> + Send + 'static>>;

    async fn send_heartbeat(
        &self,
        request: Request<Streaming<Heartbeat>>,
    ) -> Result<Response<Self::SendHeartbeatStream>, Status> {
        static HB_COUNT: AtomicU64 = AtomicU64::new(0);
        let count = HB_COUNT.fetch_add(1, Ordering::Relaxed);
        info!("GRPC_DEBUG: send_heartbeat call #{}", count);

        let mut stream = request.into_inner();
        let master = self.master.clone();

        let (tx, rx) = tokio::sync::mpsc::channel(100);

        tokio::spawn(async move {
            info!("GRPC_DEBUG: heartbeat task started for call #{}", count);
            let mut iteration = 0;
            loop {
                let heartbeat = match stream.message().await {
                    Ok(Some(hb)) => hb,
                    Ok(None) => {
                        info!(
                            "GRPC_DEBUG: heartbeat stream closed normally for call #{}",
                            count
                        );
                        break;
                    }
                    Err(e) => {
                        warn!(
                            "GRPC_DEBUG: heartbeat stream error for call #{}: {}",
                            count, e
                        );
                        break;
                    }
                };

                iteration += 1;
                if iteration % 100 == 0 {
                    info!(
                        "GRPC_DEBUG: heartbeat loop iteration #{} for call #{}",
                        iteration, count
                    );
                }

                let node_id = powerfs_common::types::NodeId(heartbeat.id.clone());
                let node_id_clone = node_id.clone();

                info!(
                    "GRPC_DEBUG: processing heartbeat from node {} with {} volumes",
                    node_id.0,
                    heartbeat.volumes.len()
                );

                if heartbeat.volumes.is_empty()
                    && heartbeat.new_volumes.is_empty()
                    && heartbeat.deleted_volumes.is_empty()
                {
                    info!("GRPC_DEBUG: calling add_node for node {}", node_id.0);
                    let add_result = master
                        .add_node(AddNodeParams {
                            node_id: node_id.clone(),
                            address: heartbeat.ip.clone(),
                            rack: heartbeat.rack.clone(),
                            data_center: heartbeat.data_center.clone(),
                            http_port: heartbeat.port,
                            grpc_port: heartbeat.grpc_port,
                            public_url: heartbeat.public_url.clone(),
                        })
                        .await;
                    if let Err(e) = add_result {
                        warn!("GRPC_DEBUG: add_node failed for {}: {}", node_id.0, e);
                        let leader = master.get_leader().await;
                        info!("GRPC_DEBUG: returning leader address: {}", leader);
                        let (error_code, error_msg) = match e {
                            powerfs_common::error::PowerFsError::NotLeader => {
                                ("LEADER_CHANGED".to_string(), e.to_string())
                            }
                            _ => ("NON_RETRYABLE".to_string(), e.to_string()),
                        };
                        let _ = tx
                            .send(Ok(HeartbeatResponse {
                                volume_size_limit: DEFAULT_VOLUME_SIZE,
                                leader,
                                metrics_address: String::new(),
                                metrics_interval_seconds: 0,
                                preallocate: false,
                                error: error_msg,
                                error_code,
                            }))
                            .await;
                        continue;
                    }
                } else {
                    info!(
                        "GRPC_DEBUG: calling update_node_volumes for node {}",
                        node_id.0
                    );
                    let update_result = master
                        .update_node_volumes(UpdateNodeVolumesParams {
                            node_id: node_id.clone(),
                            volumes: heartbeat.volumes.clone(),
                            new_volumes: heartbeat.new_volumes.clone(),
                            deleted_volumes: heartbeat.deleted_volumes.clone(),
                            ip: heartbeat.ip.clone(),
                            grpc_port: heartbeat.grpc_port,
                            http_port: heartbeat.port,
                            net_port: heartbeat.net_port,
                        })
                        .await;
                    if let Err(e) = update_result {
                        warn!(
                            "GRPC_DEBUG: update_node_volumes failed for {}: {}",
                            node_id.0, e
                        );
                        let leader = master.get_leader().await;
                        info!("GRPC_DEBUG: returning leader address: {}", leader);
                        let (error_code, error_msg) = match e {
                            powerfs_common::error::PowerFsError::NotLeader => {
                                ("LEADER_CHANGED".to_string(), e.to_string())
                            }
                            _ => ("NON_RETRYABLE".to_string(), e.to_string()),
                        };
                        let _ = tx
                            .send(Ok(HeartbeatResponse {
                                volume_size_limit: DEFAULT_VOLUME_SIZE,
                                leader,
                                metrics_address: String::new(),
                                metrics_interval_seconds: 0,
                                preallocate: false,
                                error: error_msg,
                                error_code,
                            }))
                            .await;
                        continue;
                    }
                }

                let leader = master.get_leader().await;

                info!(
                    "GRPC_DEBUG: sending heartbeat response to node {}",
                    node_id_clone.0
                );

                if tx
                    .send(Ok(HeartbeatResponse {
                        volume_size_limit: DEFAULT_VOLUME_SIZE,
                        leader,
                        metrics_address: String::new(),
                        metrics_interval_seconds: 0,
                        preallocate: false,
                        error: String::new(),
                        error_code: String::new(),
                    }))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        use futures::StreamExt;
        use tokio_stream::wrappers::ReceiverStream;
        let output = ReceiverStream::new(rx).boxed();

        Ok(Response::new(Box::pin(output)))
    }

    async fn lookup_volume(
        &self,
        request: Request<LookupVolumeRequest>,
    ) -> Result<Response<LookupVolumeResponse>, Status> {
        REQUEST_COUNT.inc();
        LOOKUP_REQUEST_COUNT.inc();

        let req = request.into_inner();
        let mut locations = Vec::new();

        for volume_id_str in req.volume_or_file_ids {
            let parts: Vec<&str> = volume_id_str.split(',').collect();
            let vid_str = if parts.len() > 1 {
                parts[0]
            } else {
                &volume_id_str
            };

            if let Ok(vid) = u64::from_str(vid_str) {
                let volume_id = VolumeId(vid);
                match self.master.get_volume(&volume_id).await {
                    Ok(info) => {
                        if let Some(node) = self.master.get_node(&info.node_id) {
                            let location = Location {
                                url: node.url(),
                                public_url: node.public_url.clone(),
                                grpc_port: node.grpc_port,
                                data_center: node.data_center_id.to_string(),
                            };
                            locations.push(VolumeIdLocation {
                                volume_or_file_id: volume_id_str,
                                locations: vec![location],
                                error: String::new(),
                                auth: String::new(),
                            });
                        } else {
                            locations.push(VolumeIdLocation {
                                volume_or_file_id: volume_id_str,
                                locations: vec![],
                                error: "node not found".to_string(),
                                auth: String::new(),
                            });
                        }
                    }
                    Err(_) => {
                        locations.push(VolumeIdLocation {
                            volume_or_file_id: volume_id_str,
                            locations: vec![],
                            error: "volume not found".to_string(),
                            auth: String::new(),
                        });
                    }
                }
            } else {
                locations.push(VolumeIdLocation {
                    volume_or_file_id: volume_id_str,
                    locations: vec![],
                    error: "invalid volume id".to_string(),
                    auth: String::new(),
                });
            }
        }

        Ok(Response::new(LookupVolumeResponse {
            volume_id_locations: locations,
        }))
    }

    async fn assign(
        &self,
        request: Request<AssignRequest>,
    ) -> Result<Response<AssignResponse>, Status> {
        REQUEST_COUNT.inc();
        ASSIGN_REQUEST_COUNT.inc();

        if !self.master.is_leader().await {
            let leader = self.master.get_leader().await;
            return Err(Status::failed_precondition(format!(
                "not leader; current leader is {}",
                leader
            )));
        }

        let req = request.into_inner();

        let stripe_count = if req.stripe_count > 1 {
            req.stripe_count
        } else {
            1
        };

        if stripe_count > 1 {
            // Stripe mode: batch assign volumes via assign_stripe_volumes
            match self
                .master
                .assign_stripe_volumes(stripe_count, &req.replication, &req.collection)
                .await
            {
                Ok((volume_ids, _start_idx)) => {
                    let mut stripe_fids = Vec::new();
                    let mut stripe_locations = Vec::new();

                    for &vid in &volume_ids {
                        let vid_vol = VolumeId(vid);
                        let cookie = rand::random::<u32>() as u64;
                        let file_key = self.master.allocate_file_key(&vid_vol).await.unwrap_or(1);
                        let fid = powerfs_common::types::Fid {
                            volume_id: vid_vol,
                            cookie,
                            file_key,
                        };
                        stripe_fids.push(fid.to_string());

                        // lookup volume location
                        if let Some(vol_info) = self.master.get_volume_info(&vid_vol) {
                            if let Some(node) = self.master.get_node(&vol_info.node_id) {
                                stripe_locations.push(Location {
                                    url: node.url(),
                                    public_url: node.public_url.clone(),
                                    grpc_port: node.grpc_port,
                                    data_center: node.data_center_id.to_string(),
                                });
                            }
                        }
                    }

                    let primary_fid = stripe_fids.first().cloned().unwrap_or_default();
                    let primary_location = stripe_locations.first().cloned();
                    let replicas = stripe_locations.clone();

                    return Ok(Response::new(AssignResponse {
                        fid: primary_fid,
                        count: req.count,
                        error: String::new(),
                        auth: String::new(),
                        replicas,
                        location: primary_location,
                        stripe_fids,
                        stripe_locations,
                    }));
                }
                Err(e) => return Err(Status::internal(format!("{}", e))),
            }
        }

        // Original single-volume path (stripe_count == 1)
        let mut stripe_fids = Vec::new();
        let mut stripe_locations = Vec::new();

        for _ in 0..stripe_count {
            match self
                .master
                .assign_volume(&req.replication, &req.collection)
                .await
            {
                Ok((fid, nodes)) => {
                    stripe_fids.push(fid.to_string());
                    for (i, node) in nodes.iter().enumerate() {
                        let location = Location {
                            url: node.url(),
                            public_url: node.public_url.clone(),
                            grpc_port: node.grpc_port,
                            data_center: node.data_center_id.to_string(),
                        };
                        if i == 0 {
                            stripe_locations.push(location);
                        }
                    }
                }
                Err(e) => return Err(Status::internal(format!("{}", e))),
            }
        }

        let primary_fid = stripe_fids.first().cloned().unwrap_or_default();
        let primary_location = stripe_locations.first().cloned();
        let replicas = stripe_locations.clone();

        Ok(Response::new(AssignResponse {
            fid: primary_fid,
            count: req.count,
            error: String::new(),
            auth: String::new(),
            replicas,
            location: primary_location,
            stripe_fids,
            stripe_locations,
        }))
    }

    async fn volume_list(
        &self,
        _request: Request<VolumeListRequest>,
    ) -> Result<Response<VolumeListResponse>, Status> {
        let nodes = self.master.list_nodes().await;
        let mut data_nodes = Vec::new();

        for node in nodes {
            let volumes = self.master.get_node_volumes(&node.id);
            let mut volume_infos = Vec::new();

            for volume in volumes {
                volume_infos.push(VolumeShortInfo {
                    volume_id: volume.id.0,
                    size: volume.size,
                    read_only: volume.state == powerfs_common::types::VolumeState::ReadOnly,
                    collection: volume.collection.0.clone(),
                    replica_placement: volume.replica_count,
                    ttl: volume.ttl.0 as u32,
                    disk_type: volume.disk_type.0.clone(),
                    used: volume.used,
                    file_count: (volume.next_file_key - 1)
                        / powerfs_common::constants::FILE_KEY_BLOCK_SIZE,
                    compact_status: 0,
                    append_offset: 0,
                });
            }

            data_nodes.push(DataNodeInfo {
                id: node.id.0.clone(),
                address: node.address.clone(),
                grpc_port: node.grpc_port,
                data_center: node.data_center_id.to_string(),
                rack: node.rack_id.to_string(),
                volumes: volume_infos,
            });
        }

        Ok(Response::new(VolumeListResponse {
            data_nodes,
            volume_size_limit: DEFAULT_VOLUME_SIZE,
        }))
    }

    async fn keep_connected(
        &self,
        request: Request<Streaming<KeepConnectedRequest>>,
    ) -> Result<Response<Self::KeepConnectedStream>, Status> {
        let mut stream = request.into_inner();
        let master = self.master.clone();

        let (tx, rx) = tokio::sync::mpsc::channel(1000);

        // Generate client_id upfront so we can return the response stream immediately.
        // This avoids a deadlock where the server waits for the first message while
        // the client waits for response headers before sending it.
        let client_id = format!("client_{}", Uuid::new_v4());
        master.add_client(client_id.clone(), tx);

        let cid = client_id.clone();
        let output = async_stream::stream! {
            let mut rx = rx;
            let mut registered = false;

            loop {
                tokio::select! {
                    Some(update) = rx.recv() => {
                        let mut new_vids = Vec::new();
                        let mut deleted_vids = Vec::new();

                        for vid in update.new_vids {
                            new_vids.push(vid);
                        }
                        for vid in update.deleted_vids {
                            deleted_vids.push(vid);
                        }

                        yield Ok(KeepConnectedResponse {
                            volume_location: Some(VolumeLocation {
                                url: String::new(),
                                public_url: String::new(),
                                new_vids,
                                deleted_vids,
                                leader: update.leader,
                                data_center: String::new(),
                                grpc_port: 0,
                            }),
                        });
                    }
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {
                        if registered {
                            master.update_fuse_client_heartbeat(&cid);
                        }
                        let leader = master.get_leader().await;
                        yield Ok(KeepConnectedResponse {
                            volume_location: Some(VolumeLocation {
                                url: String::new(),
                                public_url: String::new(),
                                new_vids: vec![],
                                deleted_vids: vec![],
                                leader,
                                data_center: String::new(),
                                grpc_port: 0,
                            }),
                        });
                    }
                    msg = stream.message() => {
                        match msg {
                            Ok(Some(request)) => {
                                let now = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_secs();

                                let fuse_info = FuseClientInfo {
                                    client_id: cid.clone(),
                                    client_type: request.client_type.clone(),
                                    mount_point: request.mount_point.clone(),
                                    collection: request.collection.clone(),
                                    replication: request.replication.clone(),
                                    host: request.host.clone(),
                                    pid: request.pid,
                                    connected_at: if !registered { now } else { 0 },
                                    last_heartbeat: now,
                                    dirty_chunks: request.dirty_chunks,
                                    dirty_bytes: request.dirty_bytes,
                                    stats: request.stats.clone(),
                                };
                                master.register_fuse_client(fuse_info);
                                registered = true;
                                continue;
                            }
                            Ok(None) => {
                                // Client closed the stream; clean up and exit.
                                master.remove_client(&cid);
                                return;
                            }
                            Err(_) => {
                                // Stream error; clean up and exit.
                                master.remove_client(&cid);
                                return;
                            }
                        }
                    }
                }
            }
        };

        Ok(Response::new(Box::pin(output)))
    }

    async fn ping(&self, _request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        let start = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;

        Ok(Response::new(PingResponse {
            start_time_ns: start,
            remote_time_ns: 0,
            stop_time_ns: start,
        }))
    }

    async fn volume_grow(
        &self,
        request: Request<VolumeGrowRequest>,
    ) -> Result<Response<VolumeGrowResponse>, Status> {
        if !self.master.is_leader().await {
            let leader = self.master.get_leader().await;
            return Err(Status::failed_precondition(format!(
                "not leader; current leader is {}",
                leader
            )));
        }

        let req = request.into_inner();

        let mut new_volume_ids = Vec::new();
        let mut locations = Vec::new();

        // Use the smart assigner with optional preferred primary node.
        // This replaces the previous create-and-discard retry loop that
        // created up to `count * 10` volumes and threw away mismatches.
        let preferred_node: Option<&str> = if req.data_node.is_empty() {
            None
        } else {
            Some(req.data_node.as_str())
        };

        // Small bounded retry for transient Raft conflicts only (e.g. another
        // leader proposal racing us). Node-targeting is handled by the smart
        // assigner directly, so we do not need the old `* 10` multiplier.
        const MAX_TRANSIENT_RETRIES: u32 = 3;

        for attempt in 1..=MAX_TRANSIENT_RETRIES {
            while (new_volume_ids.len() as u32) < req.count {
                match self
                    .master
                    .create_new_volume_with_preference(
                        &req.replication,
                        &req.collection,
                        preferred_node,
                    )
                    .await
                {
                    Ok((fid, nodes)) => {
                        new_volume_ids.push(fid.volume_id.0);
                        for node in &nodes {
                            locations.push(Location {
                                url: node.url(),
                                public_url: node.public_url.clone(),
                                grpc_port: node.grpc_port,
                                data_center: node.data_center_id.to_string(),
                            });
                        }
                    }
                    Err(e) => {
                        // If we already produced some volumes, return them
                        // with the error rather than discarding progress.
                        if !new_volume_ids.is_empty() {
                            return Ok(Response::new(VolumeGrowResponse {
                                new_volume_ids,
                                locations,
                                error: e.to_string(),
                            }));
                        }
                        // Hard-fail on the first attempt's error unless it
                        // looks like a transient Raft conflict.
                        let msg = e.to_string();
                        let is_transient = msg.contains("not leader")
                            || msg.contains("raft")
                            || msg.contains("timeout");
                        if attempt == MAX_TRANSIENT_RETRIES || !is_transient {
                            return Ok(Response::new(VolumeGrowResponse {
                                new_volume_ids,
                                locations,
                                error: msg,
                            }));
                        }
                        // Transient: break inner loop and retry from the top.
                        break;
                    }
                }
            }
            if (new_volume_ids.len() as u32) >= req.count {
                break;
            }
            // Brief backoff before a transient retry.
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        Ok(Response::new(VolumeGrowResponse {
            new_volume_ids,
            locations,
            error: String::new(),
        }))
    }

    async fn create_collection(
        &self,
        request: Request<CreateCollectionRequest>,
    ) -> Result<Response<CreateCollectionResponse>, Status> {
        if !self.master.is_leader().await {
            return Err(Status::failed_precondition(format!(
                "not leader; current leader is {}",
                self.master.get_leader_grpc_addr().await
            )));
        }

        let req = request.into_inner();
        let info = collection_info_from_create_request(&req);

        match self.master.create_collection_via_raft(info.clone()).await {
            Ok(()) => Ok(Response::new(CreateCollectionResponse {
                success: true,
                error: String::new(),
                collection: Some(collection_info_to_proto(info)),
            })),
            Err(e) => Ok(Response::new(CreateCollectionResponse {
                success: false,
                error: e.to_string(),
                collection: None,
            })),
        }
    }

    async fn delete_collection(
        &self,
        request: Request<DeleteCollectionRequest>,
    ) -> Result<Response<DeleteCollectionResponse>, Status> {
        if !self.master.is_leader().await {
            return Err(Status::failed_precondition(format!(
                "not leader; current leader is {}",
                self.master.get_leader_grpc_addr().await
            )));
        }

        let req = request.into_inner();

        match self.master.delete_collection_via_raft(&req.name).await {
            Ok(_) => Ok(Response::new(DeleteCollectionResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(DeleteCollectionResponse {
                success: false,
                error: e.to_string(),
            })),
        }
    }

    async fn get_collection(
        &self,
        request: Request<GetCollectionRequest>,
    ) -> Result<Response<GetCollectionResponse>, Status> {
        let req = request.into_inner();

        match self.master.get_collection_info(&req.name).await {
            Some(info) => Ok(Response::new(GetCollectionResponse {
                success: true,
                error: String::new(),
                collection: Some(collection_info_to_proto(info)),
            })),
            None => Ok(Response::new(GetCollectionResponse {
                success: false,
                error: "collection not found".to_string(),
                collection: None,
            })),
        }
    }

    async fn list_collections(
        &self,
        _request: Request<ListCollectionsRequest>,
    ) -> Result<Response<ListCollectionsResponse>, Status> {
        let collections = self.master.list_collection_infos().await;
        let collection_infos = collections
            .into_iter()
            .map(collection_info_to_proto)
            .collect();

        Ok(Response::new(ListCollectionsResponse {
            collections: collection_infos,
            error: String::new(),
        }))
    }

    async fn update_collection(
        &self,
        request: Request<UpdateCollectionRequest>,
    ) -> Result<Response<UpdateCollectionResponse>, Status> {
        if !self.master.is_leader().await {
            return Err(Status::failed_precondition(format!(
                "not leader; current leader is {}",
                self.master.get_leader_grpc_addr().await
            )));
        }

        let req = request.into_inner();
        // Preserve volume_count and created_at from the existing collection:
        // UpdateCollectionRequest does not carry volume_count, and the
        // CollectionManager preserves created_at on update.
        let existing = self.master.get_collection_info(&req.name).await;
        let (volume_count, created_at) = match &existing {
            Some(e) => (e.volume_count, e.created_at),
            None => (0, chrono::Utc::now().timestamp()),
        };
        let info = collection_info_from_update_request(&req, volume_count, created_at);

        match self
            .master
            .update_collection_via_raft(&req.name, info.clone())
            .await
        {
            Ok(()) => Ok(Response::new(UpdateCollectionResponse {
                success: true,
                error: String::new(),
                collection: Some(collection_info_to_proto(info)),
            })),
            Err(e) => Ok(Response::new(UpdateCollectionResponse {
                success: false,
                error: e.to_string(),
                collection: None,
            })),
        }
    }

    async fn get_collection_stats(
        &self,
        request: Request<GetCollectionStatsRequest>,
    ) -> Result<Response<GetCollectionStatsResponse>, Status> {
        let req = request.into_inner();

        match self.master.get_collection_stats(&req.name).await {
            Some(stats) => Ok(Response::new(GetCollectionStatsResponse {
                success: true,
                error: String::new(),
                stats: Some(collection_stats_to_proto(stats)),
            })),
            None => Ok(Response::new(GetCollectionStatsResponse {
                success: false,
                error: "collection not found".to_string(),
                stats: None,
            })),
        }
    }

    async fn get_statistics(
        &self,
        _request: Request<StatisticsRequest>,
    ) -> Result<Response<StatisticsResponse>, Status> {
        let stats = self.master.get_statistics().await;
        Ok(Response::new(stats))
    }

    async fn get_fuse_clients(
        &self,
        _request: Request<FuseClientsRequest>,
    ) -> Result<Response<FuseClientsResponse>, Status> {
        let clients = self.master.get_fuse_clients();
        let mut proto_clients = Vec::new();
        for client in clients {
            proto_clients.push(powerfs::fuse_clients_response::FuseClientInfo {
                client_id: client.client_id,
                client_type: client.client_type,
                mount_point: client.mount_point,
                collection: client.collection,
                replication: client.replication,
                host: client.host,
                pid: client.pid,
                connected_at: client.connected_at,
                last_heartbeat: client.last_heartbeat,
                dirty_chunks: client.dirty_chunks,
                dirty_bytes: client.dirty_bytes,
                stats: client.stats,
            });
        }
        Ok(Response::new(FuseClientsResponse {
            clients: proto_clients,
            error: String::new(),
        }))
    }

    async fn get_conflicts(
        &self,
        _request: Request<GetConflictsRequest>,
    ) -> Result<Response<GetConflictsResponse>, Status> {
        Err(Status::unimplemented(
            "filesystem metadata operations moved to Filer Raft",
        ))
    }

    async fn resolve_conflict(
        &self,
        _request: Request<ResolveConflictRequest>,
    ) -> Result<Response<ResolveConflictResponse>, Status> {
        Err(Status::unimplemented(
            "filesystem metadata operations moved to Filer Raft",
        ))
    }

    async fn set_merge_policy(
        &self,
        _request: Request<SetMergePolicyRequest>,
    ) -> Result<Response<SetMergePolicyResponse>, Status> {
        Err(Status::unimplemented(
            "filesystem metadata operations moved to Filer Raft",
        ))
    }

    async fn auto_resolve_conflicts(
        &self,
        _request: Request<AutoResolveConflictsRequest>,
    ) -> Result<Response<AutoResolveConflictsResponse>, Status> {
        Err(Status::unimplemented(
            "filesystem metadata operations moved to Filer Raft",
        ))
    }

    async fn batch_detect_conflicts(
        &self,
        _request: Request<BatchDetectConflictsRequest>,
    ) -> Result<Response<BatchDetectConflictsResponse>, Status> {
        Err(Status::unimplemented(
            "filesystem metadata operations moved to Filer Raft",
        ))
    }

    async fn batch_resolve_conflicts(
        &self,
        _request: Request<BatchResolveConflictsRequest>,
    ) -> Result<Response<BatchResolveConflictsResponse>, Status> {
        Err(Status::unimplemented(
            "filesystem metadata operations moved to Filer Raft",
        ))
    }

    async fn get_conflict_stats(
        &self,
        _request: Request<GetConflictStatsRequest>,
    ) -> Result<Response<GetConflictStatsResponse>, Status> {
        Err(Status::unimplemented(
            "filesystem metadata operations moved to Filer Raft",
        ))
    }

    async fn batch_ignore_conflicts(
        &self,
        _request: Request<BatchIgnoreConflictsRequest>,
    ) -> Result<Response<BatchIgnoreConflictsResponse>, Status> {
        Err(Status::unimplemented(
            "filesystem metadata operations moved to Filer Raft",
        ))
    }

    async fn delete_volume(
        &self,
        request: Request<DeleteVolumeRequest>,
    ) -> Result<Response<DeleteVolumeResponse>, Status> {
        if !self.master.is_leader().await {
            return Err(Status::failed_precondition(format!(
                "not leader; current leader is {}",
                self.master.get_leader_grpc_addr().await
            )));
        }

        let req = request.into_inner();
        let volume_id = VolumeId(req.volume_id);

        match self.master.delete_volume(&volume_id).await {
            Ok(_) => Ok(Response::new(DeleteVolumeResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(DeleteVolumeResponse {
                success: false,
                error: e.to_string(),
            })),
        }
    }

    async fn get_cluster_info(
        &self,
        _request: Request<ClusterInfoRequest>,
    ) -> Result<Response<ClusterInfoResponse>, Status> {
        let cluster_info = self.master.get_cluster_info().await;
        Ok(Response::new(cluster_info))
    }

    async fn get_master_status(
        &self,
        _request: Request<MasterStatusRequest>,
    ) -> Result<Response<MasterStatusResponse>, Status> {
        let is_leader = self.master.is_leader().await;
        let raft_term = self.master.raft_term();
        let address = self.master.raft_address().to_string();

        let node_info = MasterNodeInfo {
            node_id: address.clone(),
            address: address.clone(),
            grpc_port: self.master.address().port() as u32,
            is_leader,
            status: if is_leader {
                "leader".to_string()
            } else {
                "follower".to_string()
            },
            raft_term,
            cpu_usage: 0.0,
            mem_usage: 0.0,
            disk_usage: 0.0,
            uptime: 0,
            metrics_port: self.master.metrics_port() as u32,
        };

        Ok(Response::new(MasterStatusResponse {
            nodes: vec![node_info],
            leader_id: if is_leader { address } else { String::new() },
            raft_term,
            total_masters: 1,
            healthy_masters: 1,
        }))
    }

    async fn transfer_leader(
        &self,
        request: Request<TransferLeaderRequest>,
    ) -> Result<Response<TransferLeaderResponse>, Status> {
        let req = request.into_inner();
        info!(
            "Received transfer_leader request to node {}",
            req.target_node_id
        );

        // 单节点模式下不支持 leader 转移
        match self.master.raft_transfer_leader(req.target_node_id).await {
            Ok(()) => Ok(Response::new(TransferLeaderResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => {
                warn!("Leader transfer failed: {}", e);
                Ok(Response::new(TransferLeaderResponse {
                    success: false,
                    error: e,
                }))
            }
        }
    }

    // ===== Debug & Log Level Control gRPC handlers =====
    // Per-master local log level (affects only the currently connected master node)
    async fn get_log_level(
        &self,
        _request: Request<GetLogLevelRequest>,
    ) -> Result<Response<GetLogLevelResponse>, Status> {
        Ok(Response::new(GetLogLevelResponse {
            level: powerfs_common::dynamic_log::get_log_level().to_string(),
        }))
    }

    async fn set_log_level(
        &self,
        request: Request<SetLogLevelRequest>,
    ) -> Result<Response<SetLogLevelResponse>, Status> {
        let req = request.into_inner();
        if req.level.is_empty() {
            return Ok(Response::new(SetLogLevelResponse {
                success: false,
                level: String::new(),
                prev: String::new(),
                error: "missing 'level' field".into(),
            }));
        }
        let prev = powerfs_common::dynamic_log::get_log_level().to_string();
        match powerfs_common::dynamic_log::set_log_level(&req.level) {
            Ok(()) => {
                info!("log level changed via gRPC: {} -> {}", prev, req.level);
                Ok(Response::new(SetLogLevelResponse {
                    success: true,
                    level: req.level,
                    prev,
                    error: String::new(),
                }))
            }
            Err(e) => Ok(Response::new(SetLogLevelResponse {
                success: false,
                level: String::new(),
                prev,
                error: e,
            })),
        }
    }

    // Cluster-wide centralized debug config (shared across all nodes via polling)
    async fn get_debug_configs(
        &self,
        _request: Request<GetDebugConfigsRequest>,
    ) -> Result<Response<GetDebugConfigsResponse>, Status> {
        let store = self.master.debug_config();
        let configs = store
            .list_all()
            .into_iter()
            .map(|(node, cfg)| {
                let has_log_level = cfg.log_level.is_some();
                let has_target_filter = cfg.target_filter.is_some();
                DebugConfigEntry {
                    node,
                    has_log_level,
                    log_level: cfg.log_level.unwrap_or_default(),
                    has_target_filter,
                    target_filter: cfg.target_filter.unwrap_or_default(),
                    flags: cfg.flags,
                }
            })
            .collect();
        Ok(Response::new(GetDebugConfigsResponse { configs }))
    }

    async fn update_debug_config(
        &self,
        request: Request<UpdateDebugConfigRequest>,
    ) -> Result<Response<UpdateDebugConfigResponse>, Status> {
        let req = request.into_inner();
        if req.node.is_empty() {
            return Ok(Response::new(UpdateDebugConfigResponse {
                success: false,
                updated: None,
                error: "missing 'node' field".into(),
            }));
        }
        let level = if req.has_level { Some(req.level) } else { None };
        let target_filter = if req.has_target_filter {
            Some(req.target_filter)
        } else {
            None
        };
        let (flag, on) = if req.has_flag {
            (Some(req.flag), Some(req.on))
        } else {
            (None, None)
        };
        let update = crate::debug_config::DebugConfigUpdate {
            node: req.node.clone(),
            level,
            target_filter,
            flag,
            on,
        };
        let store = self.master.debug_config();
        let updated = store.apply_update(update);
        let entry = DebugConfigEntry {
            node: req.node,
            has_log_level: updated.log_level.is_some(),
            log_level: updated.log_level.unwrap_or_default(),
            has_target_filter: updated.target_filter.is_some(),
            target_filter: updated.target_filter.unwrap_or_default(),
            flags: updated.flags,
        };
        Ok(Response::new(UpdateDebugConfigResponse {
            success: true,
            updated: Some(entry),
            error: String::new(),
        }))
    }

    async fn clear_debug_config(
        &self,
        request: Request<ClearDebugConfigRequest>,
    ) -> Result<Response<ClearDebugConfigResponse>, Status> {
        let req = request.into_inner();
        if req.node.is_empty() {
            return Ok(Response::new(ClearDebugConfigResponse {
                success: false,
                node: req.node,
                removed: false,
                error: "missing 'node' field".into(),
            }));
        }
        let store = self.master.debug_config();
        let removed = store.clear(&req.node);
        Ok(Response::new(ClearDebugConfigResponse {
            success: true,
            node: req.node,
            removed,
            error: String::new(),
        }))
    }

    async fn get_filer_for_inode(
        &self,
        request: Request<GetFilerForInodeRequest>,
    ) -> Result<Response<GetFilerForInodeResponse>, Status> {
        let req = request.into_inner();
        let inode = req.inode;

        match self.master.get_filer_for_inode(inode) {
            Some(filer_address) => {
                let shard_id = self.master.get_shard_for_inode(inode);
                Ok(Response::new(GetFilerForInodeResponse {
                    filer_address,
                    shard_id,
                    success: true,
                    error: String::new(),
                }))
            }
            None => Ok(Response::new(GetFilerForInodeResponse {
                filer_address: String::new(),
                shard_id: 0,
                success: false,
                error: format!("No filer found for inode {}", inode),
            })),
        }
    }

    async fn list_filers(
        &self,
        _request: Request<ListFilersRequest>,
    ) -> Result<Response<ListFilersResponse>, Status> {
        let filers = self.master.list_filers();
        let filer_infos: Vec<FilerInfo> = filers
            .into_iter()
            .map(|f| FilerInfo {
                node_id: f.node_id,
                address: f.address,
                grpc_port: f.grpc_port,
                http_port: f.http_port,
                is_healthy: f.is_healthy,
                leader_count: f.leader_count,
                total_shards: f.total_shards,
                net_port: f.net_port,
                metrics_port: f.metrics_port,
            })
            .collect();

        Ok(Response::new(ListFilersResponse {
            filers: filer_infos,
            success: true,
            error: String::new(),
        }))
    }

    async fn register_filer(
        &self,
        request: Request<RegisterFilerRequest>,
    ) -> Result<Response<RegisterFilerResponse>, Status> {
        let req = request.into_inner();

        /* Leader 检查: filer 注册必须在 leader 上执行,
         * 否则 filer_nodes 不会对 ListFilers 可见 (内存态, 非 Raft 复制).
         * 返回 failed_precondition 使 ResilientMasterClient 自动 failover.
         * 注意: 必须用 get_leader_grpc_addr (gRPC端口), 不是 get_leader (net端口),
         * 否则 ResilientMasterClient 无法匹配已配置的 gRPC 端点. */
        if !self.master.is_leader().await {
            let leader = self.master.get_leader_grpc_addr().await;
            return Err(Status::failed_precondition(format!(
                "not leader; current leader is {}",
                leader
            )));
        }

        let filer_info = crate::master::FilerNodeInfo {
            node_id: req.node_id.clone(),
            address: req.address.clone(),
            grpc_port: req.grpc_port,
            http_port: req.http_port,
            net_port: req.net_port,
            metrics_port: req.metrics_port,
            is_healthy: true,
            leader_count: 0,
            total_shards: req.shard_count,
            shard_ids: req.shard_ids.clone(),
        };

        info!(
            "Registering filer: node_id={}, addr={}, net_port={}, shards={:?}",
            req.node_id, req.address, req.net_port, req.shard_ids
        );

        self.master.register_filer(filer_info);

        Ok(Response::new(RegisterFilerResponse {
            success: true,
            error: String::new(),
        }))
    }

    async fn get_filer_stats(
        &self,
        request: Request<FilerStatsRequest>,
    ) -> Result<Response<FilerStatsResponse>, Status> {
        let req = request.into_inner();

        if !self.master.is_leader().await {
            let leader = self.master.get_leader_grpc_addr().await;
            return Err(Status::failed_precondition(format!(
                "not leader; current leader is {}",
                leader
            )));
        }

        let filers: Vec<crate::master::FilerNodeInfo> = self
            .master
            .list_filers()
            .into_iter()
            .filter(|f| req.node_id.is_empty() || f.node_id == req.node_id)
            .collect();

        let mut futs = Vec::with_capacity(filers.len());
        for f in filers {
            futs.push(tokio::task::spawn_blocking(move || {
                fetch_filer_stats_sync(f)
            }));
        }
        let results = futures::future::join_all(futs).await;

        let mut stats = Vec::with_capacity(results.len());
        for r in results {
            match r {
                Ok(s) => stats.push(s),
                Err(e) => {
                    return Err(Status::internal(format!(
                        "spawn_blocking for GetFilerStats failed: {}",
                        e
                    )));
                }
            }
        }

        Ok(Response::new(FilerStatsResponse {
            stats,
            error: String::new(),
        }))
    }

    async fn get_shard_mapping(
        &self,
        _request: Request<GetShardMappingRequest>,
    ) -> Result<Response<GetShardMappingResponse>, Status> {
        let mappings = self.master.get_shard_mapping();
        let shard_mappings: Vec<ShardMapping> = mappings
            .into_iter()
            .map(|(shard_id, filer_address)| ShardMapping {
                shard_id,
                filer_address,
                leader_address: String::new(),
            })
            .collect();

        Ok(Response::new(GetShardMappingResponse {
            mappings: shard_mappings,
            success: true,
            error: String::new(),
        }))
    }

    type StreamMutateEntryStream =
        Pin<Box<dyn Stream<Item = Result<MutateEntryResponse, Status>> + Send + 'static>>;

    type SubscribeMetadataStream =
        Pin<Box<dyn Stream<Item = Result<MetadataNotification, Status>> + Send + 'static>>;

    async fn lookup_directory_entry(
        &self,
        _request: Request<LookupDirectoryEntryRequest>,
    ) -> Result<Response<LookupDirectoryEntryResponse>, Status> {
        Err(Status::unimplemented(
            "filesystem metadata operations moved to Filer Raft",
        ))
    }

    async fn get_entry(
        &self,
        _request: Request<GetEntryRequest>,
    ) -> Result<Response<GetEntryResponse>, Status> {
        Err(Status::unimplemented(
            "filesystem metadata operations moved to Filer Raft",
        ))
    }

    async fn get_entry_by_inode(
        &self,
        _request: Request<GetEntryByInodeRequest>,
    ) -> Result<Response<GetEntryByInodeResponse>, Status> {
        Err(Status::unimplemented(
            "filesystem metadata operations moved to Filer Raft",
        ))
    }

    async fn create_entry(
        &self,
        _request: Request<CreateEntryRequest>,
    ) -> Result<Response<CreateEntryResponse>, Status> {
        Err(Status::unimplemented(
            "filesystem metadata operations moved to Filer Raft",
        ))
    }

    async fn update_entry(
        &self,
        _request: Request<UpdateEntryRequest>,
    ) -> Result<Response<UpdateEntryResponse>, Status> {
        Err(Status::unimplemented(
            "filesystem metadata operations moved to Filer Raft",
        ))
    }

    async fn delete_entry(
        &self,
        _request: Request<DeleteEntryRequest>,
    ) -> Result<Response<DeleteEntryResponse>, Status> {
        Err(Status::unimplemented(
            "filesystem metadata operations moved to Filer Raft",
        ))
    }

    async fn rename_entry(
        &self,
        _request: Request<powerfs::RenameEntryRequest>,
    ) -> Result<Response<powerfs::RenameEntryResponse>, Status> {
        Err(Status::unimplemented(
            "filesystem metadata operations moved to Filer Raft",
        ))
    }

    async fn list_entries(
        &self,
        _request: Request<ListEntriesRequest>,
    ) -> Result<Response<ListEntriesResponse>, Status> {
        Err(Status::unimplemented(
            "filesystem metadata operations moved to Filer Raft",
        ))
    }

    async fn stream_mutate_entry(
        &self,
        _request: Request<Streaming<MutateEntryRequest>>,
    ) -> Result<Response<Self::StreamMutateEntryStream>, Status> {
        Err(Status::unimplemented(
            "filesystem metadata operations moved to Filer Raft",
        ))
    }

    async fn subscribe_metadata(
        &self,
        _request: Request<SubscribeMetadataRequest>,
    ) -> Result<Response<Self::SubscribeMetadataStream>, Status> {
        Err(Status::unimplemented(
            "filesystem metadata operations moved to Filer Raft",
        ))
    }

    async fn acquire_lease(
        &self,
        _request: Request<powerfs::LeaseRequest>,
    ) -> Result<Response<powerfs::LeaseResponse>, Status> {
        Err(Status::unimplemented(
            "filesystem metadata operations moved to Filer Raft",
        ))
    }

    async fn release_lease(
        &self,
        _request: Request<powerfs::LeaseReleaseRequest>,
    ) -> Result<Response<powerfs::LeaseReleaseResponse>, Status> {
        Err(Status::unimplemented(
            "filesystem metadata operations moved to Filer Raft",
        ))
    }

    async fn renew_lease(
        &self,
        _request: Request<powerfs::LeaseRenewRequest>,
    ) -> Result<Response<powerfs::LeaseRenewResponse>, Status> {
        Err(Status::unimplemented(
            "filesystem metadata operations moved to Filer Raft",
        ))
    }

    async fn register_job_client(
        &self,
        _request: Request<powerfs::JobRegistrationRequest>,
    ) -> Result<Response<powerfs::JobRegistrationResponse>, Status> {
        Err(Status::unimplemented(
            "filesystem metadata operations moved to Filer Raft",
        ))
    }

    async fn deregister_job_client(
        &self,
        _request: Request<powerfs::JobDeregistrationRequest>,
    ) -> Result<Response<powerfs::JobDeregistrationResponse>, Status> {
        Err(Status::unimplemented(
            "filesystem metadata operations moved to Filer Raft",
        ))
    }

    async fn complete_job(
        &self,
        _request: Request<powerfs::JobCompletionRequest>,
    ) -> Result<Response<powerfs::JobCompletionResponse>, Status> {
        Err(Status::unimplemented(
            "filesystem metadata operations moved to Filer Raft",
        ))
    }

    async fn get_job_info(
        &self,
        _request: Request<powerfs::JobInfoRequest>,
    ) -> Result<Response<powerfs::JobInfoResponse>, Status> {
        Err(Status::unimplemented(
            "filesystem metadata operations moved to Filer Raft",
        ))
    }

    async fn push_delta(
        &self,
        _request: Request<PushDeltaRequest>,
    ) -> Result<Response<PushDeltaResponse>, Status> {
        Err(Status::unimplemented(
            "filesystem metadata operations moved to Filer Raft",
        ))
    }

    async fn pull_delta(
        &self,
        _request: Request<PullDeltaRequest>,
    ) -> Result<Response<PullDeltaResponse>, Status> {
        Err(Status::unimplemented(
            "filesystem metadata operations moved to Filer Raft",
        ))
    }

    // ========================================================================
    // Allocator management RPCs (rebalance + volume scaling)
    // ========================================================================

    async fn trigger_rebalance_check(
        &self,
        request: Request<TriggerRebalanceCheckRequest>,
    ) -> Result<Response<TriggerRebalanceCheckResponse>, Status> {
        let req = request.into_inner();
        let mgmt = match self.master.management_api() {
            Some(m) => m,
            None => {
                return Ok(Response::new(TriggerRebalanceCheckResponse {
                    success: false,
                    error: "allocator management API not initialized".to_string(),
                    actions: vec![],
                }))
            }
        };
        match mgmt.trigger_rebalance_check(req.dry_run) {
            Ok(actions) => Ok(Response::new(TriggerRebalanceCheckResponse {
                success: true,
                error: String::new(),
                actions: actions.into_iter().map(rebalance_action_to_proto).collect(),
            })),
            Err(e) => Ok(Response::new(TriggerRebalanceCheckResponse {
                success: false,
                error: e.to_string(),
                actions: vec![],
            })),
        }
    }

    async fn pause_all_migrations(
        &self,
        _request: Request<PauseAllMigrationsRequest>,
    ) -> Result<Response<MigrationControlResponse>, Status> {
        match self.master.management_api() {
            Some(m) => match m.pause_all_migrations() {
                Ok(()) => Ok(Response::new(MigrationControlResponse {
                    success: true,
                    error: String::new(),
                })),
                Err(e) => Ok(Response::new(MigrationControlResponse {
                    success: false,
                    error: e.to_string(),
                })),
            },
            None => Ok(Response::new(MigrationControlResponse {
                success: false,
                error: "allocator management API not initialized".to_string(),
            })),
        }
    }

    async fn resume_migrations(
        &self,
        _request: Request<ResumeMigrationsRequest>,
    ) -> Result<Response<MigrationControlResponse>, Status> {
        match self.master.management_api() {
            Some(m) => match m.resume_migrations() {
                Ok(()) => Ok(Response::new(MigrationControlResponse {
                    success: true,
                    error: String::new(),
                })),
                Err(e) => Ok(Response::new(MigrationControlResponse {
                    success: false,
                    error: e.to_string(),
                })),
            },
            None => Ok(Response::new(MigrationControlResponse {
                success: false,
                error: "allocator management API not initialized".to_string(),
            })),
        }
    }

    async fn cancel_migration(
        &self,
        request: Request<CancelMigrationRequest>,
    ) -> Result<Response<MigrationControlResponse>, Status> {
        let req = request.into_inner();
        match self.master.management_api() {
            Some(m) => match m.cancel_migration(&req.task_id) {
                Ok(()) => Ok(Response::new(MigrationControlResponse {
                    success: true,
                    error: String::new(),
                })),
                Err(e) => Ok(Response::new(MigrationControlResponse {
                    success: false,
                    error: e.to_string(),
                })),
            },
            None => Ok(Response::new(MigrationControlResponse {
                success: false,
                error: "allocator management API not initialized".to_string(),
            })),
        }
    }

    async fn get_migration_tasks(
        &self,
        _request: Request<GetMigrationTasksRequest>,
    ) -> Result<Response<GetMigrationTasksResponse>, Status> {
        let tasks = self.master.migration_tasks();
        Ok(Response::new(GetMigrationTasksResponse {
            tasks: tasks.into_iter().map(migration_task_to_proto).collect(),
            error: String::new(),
        }))
    }

    async fn create_volume_managed(
        &self,
        request: Request<CreateVolumeManagedRequest>,
    ) -> Result<Response<CreateVolumeManagedResponse>, Status> {
        let req = request.into_inner();
        let node_id = if req.node_id.is_empty() {
            None
        } else {
            Some(req.node_id)
        };
        match self.master.management_api() {
            Some(m) => match m.create_volume(req.zone_id, node_id, req.size) {
                Ok(volume_id) => Ok(Response::new(CreateVolumeManagedResponse {
                    success: true,
                    error: String::new(),
                    volume_id,
                })),
                Err(e) => Ok(Response::new(CreateVolumeManagedResponse {
                    success: false,
                    error: e.to_string(),
                    volume_id: 0,
                })),
            },
            None => Ok(Response::new(CreateVolumeManagedResponse {
                success: false,
                error: "allocator management API not initialized".to_string(),
                volume_id: 0,
            })),
        }
    }

    async fn drain_volume_managed(
        &self,
        request: Request<VolumeIdRequest>,
    ) -> Result<Response<VolumeManageResponse>, Status> {
        let req = request.into_inner();
        match self.master.management_api() {
            Some(m) => match m.drain_volume(req.volume_id) {
                Ok(()) => Ok(Response::new(VolumeManageResponse {
                    success: true,
                    error: String::new(),
                })),
                Err(e) => Ok(Response::new(VolumeManageResponse {
                    success: false,
                    error: e.to_string(),
                })),
            },
            None => Ok(Response::new(VolumeManageResponse {
                success: false,
                error: "allocator management API not initialized".to_string(),
            })),
        }
    }

    async fn remove_volume_managed(
        &self,
        request: Request<VolumeIdRequest>,
    ) -> Result<Response<VolumeManageResponse>, Status> {
        let req = request.into_inner();
        match self.master.management_api() {
            Some(m) => match m.remove_volume(req.volume_id) {
                Ok(()) => Ok(Response::new(VolumeManageResponse {
                    success: true,
                    error: String::new(),
                })),
                Err(e) => Ok(Response::new(VolumeManageResponse {
                    success: false,
                    error: e.to_string(),
                })),
            },
            None => Ok(Response::new(VolumeManageResponse {
                success: false,
                error: "allocator management API not initialized".to_string(),
            })),
        }
    }

    async fn update_migration_policy(
        &self,
        request: Request<UpdateMigrationPolicyRequest>,
    ) -> Result<Response<PolicyUpdateResponse>, Status> {
        let req = request.into_inner();
        let policy = MigrationPolicy {
            max_concurrent_migrations: req.max_concurrent_migrations,
            max_bandwidth_mbps: req.max_bandwidth_mbps,
            load_pause_threshold: req.load_pause_threshold,
            load_resume_threshold: req.load_resume_threshold,
            scan_interval_secs: req.scan_interval_secs,
        };
        match self.master.management_api() {
            Some(m) => match m.update_migration_policy(policy) {
                Ok(()) => Ok(Response::new(PolicyUpdateResponse {
                    success: true,
                    error: String::new(),
                })),
                Err(e) => Ok(Response::new(PolicyUpdateResponse {
                    success: false,
                    error: e.to_string(),
                })),
            },
            None => Ok(Response::new(PolicyUpdateResponse {
                success: false,
                error: "allocator management API not initialized".to_string(),
            })),
        }
    }

    async fn update_rebalance_policy(
        &self,
        request: Request<UpdateRebalancePolicyRequest>,
    ) -> Result<Response<PolicyUpdateResponse>, Status> {
        let req = request.into_inner();
        let policy = RebalancePolicy {
            volume_full_threshold: req.volume_full_threshold,
            near_full_exclude_ratio: req.near_full_exclude_ratio,
            load_imbalance_threshold: req.load_imbalance_threshold,
            cold_data_threshold_hours: req.cold_data_threshold_hours,
            min_migration_chunk_count: req.min_migration_chunk_count,
        };
        match self.master.management_api() {
            Some(m) => match m.update_rebalance_policy(policy) {
                Ok(()) => Ok(Response::new(PolicyUpdateResponse {
                    success: true,
                    error: String::new(),
                })),
                Err(e) => Ok(Response::new(PolicyUpdateResponse {
                    success: false,
                    error: e.to_string(),
                })),
            },
            None => Ok(Response::new(PolicyUpdateResponse {
                success: false,
                error: "allocator management API not initialized".to_string(),
            })),
        }
    }

    async fn set_node_maintenance(
        &self,
        request: Request<SetNodeMaintenanceRequest>,
    ) -> Result<Response<MigrationControlResponse>, Status> {
        let req = request.into_inner();
        match self.master.management_api() {
            Some(m) => match m.set_node_maintenance(&req.node_id, req.enabled) {
                Ok(()) => Ok(Response::new(MigrationControlResponse {
                    success: true,
                    error: String::new(),
                })),
                Err(e) => Ok(Response::new(MigrationControlResponse {
                    success: false,
                    error: e.to_string(),
                })),
            },
            None => Ok(Response::new(MigrationControlResponse {
                success: false,
                error: "allocator management API not initialized".to_string(),
            })),
        }
    }

    async fn pin_volume(
        &self,
        request: Request<PinVolumeRequest>,
    ) -> Result<Response<VolumeManageResponse>, Status> {
        let req = request.into_inner();
        match self.master.management_api() {
            Some(m) => match m.pin_volume_to_node(req.volume_id, &req.node_id) {
                Ok(()) => Ok(Response::new(VolumeManageResponse {
                    success: true,
                    error: String::new(),
                })),
                Err(e) => Ok(Response::new(VolumeManageResponse {
                    success: false,
                    error: e.to_string(),
                })),
            },
            None => Ok(Response::new(VolumeManageResponse {
                success: false,
                error: "allocator management API not initialized".to_string(),
            })),
        }
    }

    async fn unpin_volume(
        &self,
        request: Request<VolumeIdRequest>,
    ) -> Result<Response<VolumeManageResponse>, Status> {
        let req = request.into_inner();
        match self.master.management_api() {
            Some(m) => match m.unpin_volume(req.volume_id) {
                Ok(()) => Ok(Response::new(VolumeManageResponse {
                    success: true,
                    error: String::new(),
                })),
                Err(e) => Ok(Response::new(VolumeManageResponse {
                    success: false,
                    error: e.to_string(),
                })),
            },
            None => Ok(Response::new(VolumeManageResponse {
                success: false,
                error: "allocator management API not initialized".to_string(),
            })),
        }
    }

    async fn set_placement_strategy(
        &self,
        request: Request<SetPlacementStrategyRequest>,
    ) -> Result<Response<VolumeManageResponse>, Status> {
        let req = request.into_inner();
        match self.master.management_api() {
            Some(m) => match m.set_placement_strategy(&req.strategy) {
                Ok(()) => Ok(Response::new(VolumeManageResponse {
                    success: true,
                    error: String::new(),
                })),
                Err(e) => Ok(Response::new(VolumeManageResponse {
                    success: false,
                    error: e.to_string(),
                })),
            },
            None => Ok(Response::new(VolumeManageResponse {
                success: false,
                error: "allocator management API not initialized".to_string(),
            })),
        }
    }
}

// ========================================================================
// Conversion helpers: allocator types → proto types
// ========================================================================

fn rebalance_action_to_proto(action: RebalanceAction) -> RebalanceActionInfo {
    let mut info = RebalanceActionInfo {
        action_type: 0,
        from_volume: 0,
        to_volume: 0,
        from_node: String::new(),
        to_node: String::new(),
        zone_id: 0,
        size: 0,
        needle_ids: Vec::new(),
        volume_ids: Vec::new(),
    };
    match action {
        RebalanceAction::MigrateColdData {
            from_volume,
            to_volume,
            needle_ids,
        } => {
            info.action_type = rebalance_action_info::ActionType::MigrateColdData as i32;
            info.from_volume = from_volume;
            info.to_volume = to_volume;
            info.needle_ids = needle_ids;
        }
        RebalanceAction::MigrateHotData {
            from_node,
            to_node,
            volume_ids,
        } => {
            info.action_type = rebalance_action_info::ActionType::MigrateHotData as i32;
            info.from_node = from_node;
            info.to_node = to_node;
            info.volume_ids = volume_ids;
        }
        RebalanceAction::RequestVolumeGrow { zone_id, size } => {
            info.action_type = rebalance_action_info::ActionType::RequestVolumeGrow as i32;
            info.zone_id = zone_id;
            info.size = size;
        }
    }
    info
}

fn migration_task_to_proto(task: MigrationTaskStatus) -> MigrationTaskInfo {
    MigrationTaskInfo {
        task_id: task.task_id,
        action_type: match task.action_type {
            MigrationType::ColdData => "cold_data".to_string(),
            MigrationType::HotData => "hot_data".to_string(),
            MigrationType::VolumeGrow => "volume_grow".to_string(),
        },
        state: match task.state {
            MigrationState::Pending => "pending".to_string(),
            MigrationState::Running => "running".to_string(),
            MigrationState::PausedByLoad => "paused_by_load".to_string(),
            MigrationState::Completed => "completed".to_string(),
            MigrationState::Failed => "failed".to_string(),
        },
        progress: task.progress,
        bytes_migrated: task.bytes_migrated,
        bytes_total: task.bytes_total,
        pause_reason: task.pause_reason.unwrap_or_default(),
    }
}

fn http_get_sync(addr: &str, path: &str) -> std::result::Result<String, String> {
    let socket_addr = addr
        .to_socket_addrs()
        .map_err(|e| format!("invalid addr '{}': {}", addr, e))?
        .next()
        .ok_or_else(|| format!("no address resolved for '{}'", addr))?;

    let mut stream = TcpStream::connect_timeout(&socket_addr, Duration::from_secs(5))
        .map_err(|e| format!("connect {} failed: {}", addr, e))?;

    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .map_err(|e| format!("set_read_timeout: {}", e))?;

    let request = format!(
        "GET {} HTTP/1.0\r\nHost: powerfs-master\r\nConnection: close\r\n\r\n",
        path
    );

    stream
        .write_all(request.as_bytes())
        .map_err(|e| format!("write request: {}", e))?;

    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .map_err(|e| format!("read response: {}", e))?;

    let response_str = String::from_utf8_lossy(&response);
    let body_start = response_str
        .find("\r\n\r\n")
        .ok_or_else(|| "malformed HTTP response".to_string())?;

    let status_line = response_str.lines().next().unwrap_or("");
    let status_ok = status_line.split_whitespace().nth(1) == Some("200");

    let body = response_str[body_start + 4..].to_string();

    if status_ok {
        Ok(body)
    } else {
        Err(format!("HTTP status: {}", status_line))
    }
}

fn fetch_filer_stats_sync(filer: FilerNodeInfo) -> FilerNodeStats {
    // FilerNodeInfo.address is currently "ip:net_port" (used for filer<->volume TLV).
    // Strip the port and use the IP with service-specific ports carried at
    // registration time.
    let ip_only = filer
        .address
        .split(':')
        .next()
        .unwrap_or(&filer.address)
        .to_string();

    let mut fetch_error_parts: Vec<String> = Vec::new();

    let meta_cache_stats_json = if filer.metrics_port != 0 {
        let addr = format!("{}:{}", ip_only, filer.metrics_port);
        match http_get_sync(&addr, "/admin/meta-cache-stats") {
            Ok(s) => s,
            Err(e) => {
                fetch_error_parts.push(format!("/admin/meta-cache-stats: {}", e));
                error!(
                    "GetFilerStats: /admin/meta-cache-stats failed for {} addr={}: {}",
                    filer.node_id, addr, e
                );
                String::new()
            }
        }
    } else {
        String::new()
    };

    let lease_stats_json = if filer.metrics_port != 0 {
        let addr = format!("{}:{}", ip_only, filer.metrics_port);
        match http_get_sync(&addr, "/admin/lease-stats") {
            Ok(s) => s,
            Err(e) => {
                fetch_error_parts.push(format!("/admin/lease-stats: {}", e));
                error!(
                    "GetFilerStats: /admin/lease-stats failed for {} addr={}: {}",
                    filer.node_id, addr, e
                );
                String::new()
            }
        }
    } else {
        String::new()
    };

    let shards_json = if filer.http_port != 0 {
        let addr = format!("{}:{}", ip_only, filer.http_port);
        match http_get_sync(&addr, "/admin/shards") {
            Ok(s) => s,
            Err(e) => {
                fetch_error_parts.push(format!("/admin/shards: {}", e));
                error!(
                    "GetFilerStats: /admin/shards failed for {} addr={}: {}",
                    filer.node_id, addr, e
                );
                String::new()
            }
        }
    } else {
        String::new()
    };

    FilerNodeStats {
        node_id: filer.node_id,
        address: filer.address,
        is_healthy: filer.is_healthy,
        leader_count: filer.leader_count as u32,
        total_shards: filer.total_shards as u32,
        http_port: filer.http_port,
        metrics_port: filer.metrics_port,
        meta_cache_stats_json,
        lease_stats_json,
        shards_json,
        fetch_error: fetch_error_parts.join("; "),
    }
}
