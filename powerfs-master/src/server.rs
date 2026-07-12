use super::kv_cache_service::KvCacheServiceImpl;
use super::master::{AddNodeParams, MasterNode, UpdateNodeVolumesParams};
use super::metrics::{ASSIGN_REQUEST_COUNT, LOOKUP_REQUEST_COUNT, REQUEST_COUNT};
use super::proto::*;
use futures::Stream;
use log::{debug, info, warn};
use powerfs_common::constants::DEFAULT_VOLUME_SIZE;
use powerfs_common::types::VolumeId;
use powerfs_core::kv_cache::KVCacheEngine;
use std::collections::HashMap;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio_stream::StreamExt;
use tonic::{transport::Channel, transport::Server, Request, Response, Status, Streaming};
use uuid::Uuid;

pub struct MasterGrpcServer {
    master: Arc<MasterNode>,
    kv_cache: Arc<KVCacheEngine>,
    leader_channels: Arc<tokio::sync::RwLock<HashMap<String, Channel>>>,
}

impl MasterGrpcServer {
    pub fn new(master: Arc<MasterNode>, kv_cache: Arc<KVCacheEngine>) -> Self {
        MasterGrpcServer {
            master,
            kv_cache,
            leader_channels: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        }
    }

    pub async fn start(self, addr: std::net::SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        let kv_svc = KvCacheServiceImpl {
            engine: self.kv_cache.clone(),
            volume_client_pool: self.master.volume_client_pool.clone(),
            master: self.master.clone(),
        };

        Server::builder()
            .add_service(MasterServiceServer::new(self))
            .add_service(KvCacheServiceServer::new(kv_svc))
            .serve(addr)
            .await?;
        Ok(())
    }

    async fn get_leader_client(
        &self,
    ) -> Option<crate::proto::powerfs::master_service_client::MasterServiceClient<Channel>> {
        let leader = self.master.get_leader().await;
        if leader.is_empty() {
            return None;
        }

        {
            let channels = self.leader_channels.read().await;
            if let Some(ch) = channels.get(&leader) {
                return Some(
                    crate::proto::powerfs::master_service_client::MasterServiceClient::new(
                        ch.clone(),
                    ),
                );
            }
        }

        let addr = format!("http://{}", leader);
        let channel = match Channel::from_shared(addr)
            .map_err(|e| {
                warn!("Invalid leader address: {}", e);
                e
            })
            .ok()?
            .connect()
            .await
        {
            Ok(ch) => ch,
            Err(e) => {
                warn!("Failed to connect to leader {}: {}", leader, e);
                return None;
            }
        };

        let mut channels = self.leader_channels.write().await;
        channels.insert(leader, channel.clone());
        Some(crate::proto::powerfs::master_service_client::MasterServiceClient::new(channel))
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
        let mut stream = request.into_inner();
        let master = self.master.clone();

        let (tx, rx) = tokio::sync::mpsc::channel(100);

        tokio::spawn(async move {
            while let Some(heartbeat) = stream.message().await.unwrap_or(None) {
                debug!("Received heartbeat from: {}", heartbeat.id);

                let node_id = powerfs_common::types::NodeId(heartbeat.id.clone());

                if heartbeat.volumes.is_empty()
                    && heartbeat.new_volumes.is_empty()
                    && heartbeat.deleted_volumes.is_empty()
                {
                    if let Err(e) = master
                        .add_node(AddNodeParams {
                            node_id,
                            address: heartbeat.ip.clone(),
                            rack: heartbeat.rack.clone(),
                            data_center: heartbeat.data_center.clone(),
                            http_port: heartbeat.port,
                            grpc_port: heartbeat.grpc_port,
                            public_url: heartbeat.public_url.clone(),
                        })
                        .await
                    {
                        debug!("Failed to add node: {}", e);
                    }
                } else {
                    if let Err(e) = master
                        .update_node_volumes(UpdateNodeVolumesParams {
                            node_id,
                            volumes: heartbeat.volumes.clone(),
                            new_volumes: heartbeat.new_volumes.clone(),
                            deleted_volumes: heartbeat.deleted_volumes.clone(),
                            ip: heartbeat.ip.clone(),
                            grpc_port: heartbeat.grpc_port,
                            http_port: heartbeat.port,
                        })
                        .await
                    {
                        debug!("Failed to update node volumes: {}", e);
                    }
                }

                let leader = master.get_leader().await;

                if tx
                    .send(Ok(HeartbeatResponse {
                        volume_size_limit: DEFAULT_VOLUME_SIZE,
                        leader,
                        metrics_address: String::new(),
                        metrics_interval_seconds: 0,
                        preallocate: false,
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

            if let Ok(vid) = u32::from_str(vid_str) {
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
            if let Some(mut client) = self.get_leader_client().await {
                let req = request.into_inner();
                match client.assign(Request::new(req)).await {
                    Ok(resp) => return Ok(resp),
                    Err(e) => return Err(e),
                }
            }
            return Err(Status::unavailable(
                "not leader and no leader client available",
            ));
        }

        let req = request.into_inner();

        let stripe_count = if req.stripe_count > 1 {
            req.stripe_count
        } else {
            1
        };

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
        let client_id = format!("client_{}", Uuid::new_v4());

        master.add_client(client_id.clone(), tx);

        let output = async_stream::stream! {
            let mut rx = rx;

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
                    _ = stream.message() => {
                        continue;
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
        // Forward to leader if not leader
        if !self.master.is_leader().await {
            if let Some(mut client) = self.get_leader_client().await {
                let req = request.into_inner();
                match client.volume_grow(Request::new(req)).await {
                    Ok(resp) => return Ok(resp),
                    Err(e) => return Err(e),
                }
            }
            return Err(Status::unavailable(
                "not leader and no leader client available",
            ));
        }

        let req = request.into_inner();

        // Use assign_volume logic to allocate new volumes
        let mut new_volume_ids = Vec::new();
        let mut locations = Vec::new();

        for _ in 0..req.count {
            match self
                .master
                .create_new_volume(&req.replication, &req.collection)
                .await
            {
                Ok((fid, nodes)) => {
                    new_volume_ids.push(fid.volume_id.0);
                    for node in nodes {
                        locations.push(Location {
                            url: node.url(),
                            public_url: node.public_url.clone(),
                            grpc_port: node.grpc_port,
                            data_center: node.data_center_id.to_string(),
                        });
                    }
                }
                Err(e) => {
                    return Ok(Response::new(VolumeGrowResponse {
                        new_volume_ids,
                        locations,
                        error: e.to_string(),
                    }));
                }
            }
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
            if let Some(mut client) = self.get_leader_client().await {
                let req = request.into_inner();
                match client.create_collection(Request::new(req)).await {
                    Ok(resp) => return Ok(resp),
                    Err(e) => return Err(e),
                }
            }
            return Err(Status::unavailable(
                "not leader and no leader client available",
            ));
        }

        let req = request.into_inner();

        let ttl: i32 = req.ttl.parse().unwrap_or(0);

        match self
            .master
            .create_collection(
                &req.name,
                &req.replication,
                ttl,
                &req.disk_type,
                req.max_volume_count,
            )
            .await
        {
            Ok(config) => Ok(Response::new(CreateCollectionResponse {
                success: true,
                error: String::new(),
                collection: Some(CollectionInfo {
                    name: config.name.0,
                    replication: config.replication.to_string_format(),
                    ttl: config.ttl.to_string(),
                    disk_type: config.disk_type.0,
                    max_volume_count: config.max_volume_count,
                    volume_count: config.volume_count,
                    created_at: config.created_at.timestamp() as u64,
                    modified_at: config.modified_at.timestamp() as u64,
                }),
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
            if let Some(mut client) = self.get_leader_client().await {
                let req = request.into_inner();
                match client.delete_collection(Request::new(req)).await {
                    Ok(resp) => return Ok(resp),
                    Err(e) => return Err(e),
                }
            }
            return Err(Status::unavailable(
                "not leader and no leader client available",
            ));
        }

        let req = request.into_inner();

        match self.master.delete_collection(&req.name).await {
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

        match self.master.get_collection(&req.name).await {
            Some(config) => Ok(Response::new(GetCollectionResponse {
                success: true,
                error: String::new(),
                collection: Some(CollectionInfo {
                    name: config.name.0,
                    replication: config.replication.to_string_format(),
                    ttl: config.ttl.to_string(),
                    disk_type: config.disk_type.0,
                    max_volume_count: config.max_volume_count,
                    volume_count: config.volume_count,
                    created_at: config.created_at.timestamp() as u64,
                    modified_at: config.modified_at.timestamp() as u64,
                }),
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
        let collections = self.master.list_collections().await;

        let mut collection_infos = Vec::new();
        for config in collections {
            collection_infos.push(CollectionInfo {
                name: config.name.0,
                replication: config.replication.to_string_format(),
                ttl: config.ttl.to_string(),
                disk_type: config.disk_type.0,
                max_volume_count: config.max_volume_count,
                volume_count: config.volume_count,
                created_at: config.created_at.timestamp() as u64,
                modified_at: config.modified_at.timestamp() as u64,
            });
        }

        Ok(Response::new(ListCollectionsResponse {
            collections: collection_infos,
            error: String::new(),
        }))
    }

    async fn get_statistics(
        &self,
        _request: Request<StatisticsRequest>,
    ) -> Result<Response<StatisticsResponse>, Status> {
        let stats = self.master.get_statistics().await;
        Ok(Response::new(stats))
    }

    async fn delete_volume(
        &self,
        request: Request<DeleteVolumeRequest>,
    ) -> Result<Response<DeleteVolumeResponse>, Status> {
        if !self.master.is_leader().await {
            if let Some(mut client) = self.get_leader_client().await {
                let req = request.into_inner();
                match client.delete_volume(Request::new(req)).await {
                    Ok(resp) => return Ok(resp),
                    Err(e) => return Err(e),
                }
            }
            return Err(Status::unavailable(
                "not leader and no leader client available",
            ));
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

    type StreamMutateEntryStream =
        Pin<Box<dyn Stream<Item = Result<MutateEntryResponse, Status>> + Send + 'static>>;

    type SubscribeMetadataStream =
        Pin<Box<dyn Stream<Item = Result<MetadataNotification, Status>> + Send + 'static>>;

    async fn lookup_directory_entry(
        &self,
        request: Request<LookupDirectoryEntryRequest>,
    ) -> Result<Response<LookupDirectoryEntryResponse>, Status> {
        let req = request.into_inner();
        let dir_tree = self.master.directory_tree.clone();
        let name = req.name.clone();

        let entry = tokio::task::spawn_blocking(move || dir_tree.lookup(req.parent_ino, &name))
            .await
            .unwrap();

        if let Some(entry) = entry {
            Ok(Response::new(LookupDirectoryEntryResponse {
                found: true,
                entry: Some(entry),
                error: String::new(),
            }))
        } else {
            Ok(Response::new(LookupDirectoryEntryResponse {
                found: false,
                entry: None,
                error: String::new(),
            }))
        }
    }

    async fn get_entry(
        &self,
        request: Request<GetEntryRequest>,
    ) -> Result<Response<GetEntryResponse>, Status> {
        let req = request.into_inner();
        let dir_tree = self.master.directory_tree.clone();
        let path = req.path.clone();

        let entry = tokio::task::spawn_blocking(move || dir_tree.get_entry(&path))
            .await
            .unwrap();

        if let Some(entry) = entry {
            Ok(Response::new(GetEntryResponse {
                found: true,
                entry: Some(entry),
                error: String::new(),
            }))
        } else {
            Ok(Response::new(GetEntryResponse {
                found: false,
                entry: None,
                error: String::new(),
            }))
        }
    }

    async fn get_entry_by_inode(
        &self,
        request: Request<GetEntryByInodeRequest>,
    ) -> Result<Response<GetEntryByInodeResponse>, Status> {
        let req = request.into_inner();
        let dir_tree = self.master.directory_tree.clone();
        let inode = req.inode;

        let result = tokio::task::spawn_blocking(move || dir_tree.get_entry_by_inode(inode))
            .await
            .unwrap();

        if let Some((entry, path)) = result {
            let mode_val = entry.attributes.as_ref().map(|a| a.mode).unwrap_or(0);
            let file_type = mode_val & 0o170000;
            info!(
                "get_entry_by_inode response: inode={}, name={}, path={}, mode={:o}, file_type={:o}, is_symlink={}, symlink_target='{}'",
                inode, entry.name, path, mode_val, file_type, file_type == 0o120000, entry.symlink_target
            );
            Ok(Response::new(GetEntryByInodeResponse {
                found: true,
                entry: Some(entry),
                path,
                error: String::new(),
            }))
        } else {
            Ok(Response::new(GetEntryByInodeResponse {
                found: false,
                entry: None,
                path: String::new(),
                error: String::new(),
            }))
        }
    }

    async fn create_entry(
        &self,
        request: Request<CreateEntryRequest>,
    ) -> Result<Response<CreateEntryResponse>, Status> {
        let req = request.into_inner();
        let dir_tree = self.master.directory_tree.clone();
        let client_id = req.client_id.clone();
        let entry = req.entry.unwrap_or_default();
        info!(
            "create_entry request: name={}, directory={}, client_id={}, mode={:o}, symlink_target='{}'",
            entry.name.as_str(),
            entry.directory.as_str(),
            client_id,
            entry.attributes.as_ref().map(|a| a.mode).unwrap_or(0),
            entry.symlink_target
        );

        let inode = tokio::task::spawn_blocking(move || dir_tree.create_entry(entry, &client_id))
            .await
            .unwrap();

        match inode {
            Ok(inode) => Ok(Response::new(CreateEntryResponse {
                success: true,
                error: String::new(),
                inode,
            })),
            Err(e) => Ok(Response::new(CreateEntryResponse {
                success: false,
                error: e.to_string(),
                inode: 0,
            })),
        }
    }

    async fn update_entry(
        &self,
        request: Request<UpdateEntryRequest>,
    ) -> Result<Response<UpdateEntryResponse>, Status> {
        let req = request.into_inner();
        let dir_tree = self.master.directory_tree.clone();
        let client_id = req.client_id.clone();
        let entry = req.entry.unwrap_or_default();
        let entry_ref = &entry;
        info!(
            "update_entry request: name={}, directory={}, client_id={}, content_size={}, size={}, mode={:o}, symlink_target='{}'",
            entry_ref.name.as_str(),
            entry_ref.directory.as_str(),
            client_id,
            entry_ref.content_size,
            entry_ref.attributes.as_ref().map(|a| a.size).unwrap_or(0),
            entry_ref.attributes.as_ref().map(|a| a.mode).unwrap_or(0),
            entry_ref.symlink_target
        );

        let result = tokio::task::spawn_blocking(move || dir_tree.update_entry(entry, &client_id))
            .await
            .unwrap();

        match result {
            Ok(_) => Ok(Response::new(UpdateEntryResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(UpdateEntryResponse {
                success: false,
                error: e.to_string(),
            })),
        }
    }

    async fn delete_entry(
        &self,
        request: Request<DeleteEntryRequest>,
    ) -> Result<Response<DeleteEntryResponse>, Status> {
        let req = request.into_inner();
        let dir_tree = self.master.directory_tree.clone();
        let ino = req.ino;
        let client_id = req.client_id.clone();

        let result = tokio::task::spawn_blocking(move || dir_tree.delete_entry(ino, &client_id))
            .await
            .unwrap();

        match result {
            Ok(_) => Ok(Response::new(DeleteEntryResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(DeleteEntryResponse {
                success: false,
                error: e.to_string(),
            })),
        }
    }

    async fn rename_entry(
        &self,
        request: Request<powerfs::RenameEntryRequest>,
    ) -> Result<Response<powerfs::RenameEntryResponse>, Status> {
        let req = request.into_inner();
        info!(
            "rename_entry request: old_parent_ino={}, old_name={}, new_parent_ino={}, new_name={}, client_id={}",
            req.old_parent_ino, req.old_name, req.new_parent_ino, req.new_name, req.client_id
        );
        let dir_tree = self.master.directory_tree.clone();
        let old_parent_ino = req.old_parent_ino;
        let old_name = req.old_name.clone();
        let new_parent_ino = req.new_parent_ino;
        let new_name = req.new_name.clone();
        let client_id = req.client_id.clone();

        let result = tokio::task::spawn_blocking(move || {
            dir_tree.rename_entry(
                old_parent_ino,
                &old_name,
                new_parent_ino,
                &new_name,
                &client_id,
            )
        })
        .await
        .unwrap();

        match result {
            Ok(success) => {
                info!("rename_entry result: success={}", success);
                Ok(Response::new(powerfs::RenameEntryResponse {
                    success,
                    error: String::new(),
                }))
            }
            Err(e) => {
                info!("rename_entry error: {}", e);
                Ok(Response::new(powerfs::RenameEntryResponse {
                    success: false,
                    error: e.to_string(),
                }))
            }
        }
    }

    async fn list_entries(
        &self,
        request: Request<ListEntriesRequest>,
    ) -> Result<Response<ListEntriesResponse>, Status> {
        let req = request.into_inner();
        let dir_tree = self.master.directory_tree.clone();
        let parent_ino = req.parent_ino;
        let limit = req.limit;
        let last_name = req.last_name.clone();

        let entries = tokio::task::spawn_blocking(move || {
            dir_tree.list_entries(parent_ino, limit, &last_name)
        })
        .await
        .unwrap();

        Ok(Response::new(ListEntriesResponse {
            entries,
            has_more: false,
            error: String::new(),
        }))
    }

    async fn stream_mutate_entry(
        &self,
        request: Request<Streaming<MutateEntryRequest>>,
    ) -> Result<Response<Self::StreamMutateEntryStream>, Status> {
        let mut stream = request.into_inner();
        let dir_tree = self.master.directory_tree.clone();

        let (tx, rx) = tokio::sync::mpsc::channel(100);

        tokio::spawn(async move {
            while let Some(req) = stream.message().await.unwrap_or(None) {
                let dir_tree_clone = dir_tree.clone();
                let result = match req.mutation {
                    Some(crate::proto::powerfs::mutate_entry_request::Mutation::Create(
                        create_req,
                    )) => {
                        let client_id = create_req.client_id.clone();
                        let entry = create_req.entry.unwrap_or_default();
                        tokio::task::spawn_blocking(move || {
                            dir_tree_clone.create_entry(entry, &client_id)
                        })
                        .await
                        .unwrap()
                        .map(|_| ())
                        .map_err(|e| e.to_string())
                    }
                    Some(crate::proto::powerfs::mutate_entry_request::Mutation::Update(
                        update_req,
                    )) => {
                        let client_id = update_req.client_id.clone();
                        let entry = update_req.entry.unwrap_or_default();
                        tokio::task::spawn_blocking(move || {
                            dir_tree_clone.update_entry(entry, &client_id)
                        })
                        .await
                        .unwrap()
                        .map_err(|e| e.to_string())
                    }
                    Some(crate::proto::powerfs::mutate_entry_request::Mutation::Delete(
                        delete_req,
                    )) => {
                        let client_id = delete_req.client_id.clone();
                        let ino = delete_req.ino;
                        tokio::task::spawn_blocking(move || {
                            dir_tree_clone.delete_entry(ino, &client_id)
                        })
                        .await
                        .unwrap()
                        .map(|_| ())
                        .map_err(|e| e.to_string())
                    }
                    None => Ok(()),
                };

                let _ = tx
                    .send(MutateEntryResponse {
                        success: result.is_ok(),
                        error: result.err().unwrap_or_default(),
                    })
                    .await;
            }
        });

        #[allow(clippy::result_large_err)]
        let output_stream = tokio_stream::wrappers::ReceiverStream::new(rx).map(Ok);
        Ok(Response::new(Box::pin(output_stream)))
    }

    async fn subscribe_metadata(
        &self,
        request: Request<SubscribeMetadataRequest>,
    ) -> Result<Response<Self::SubscribeMetadataStream>, Status> {
        let req = request.into_inner();
        let dir_tree = self.master.directory_tree.clone();

        let path_prefix = if req.path_prefix.is_empty() {
            "/".to_string()
        } else {
            req.path_prefix
        };

        dir_tree.add_subscriber(&path_prefix);

        let mut rx = dir_tree.subscribe();

        let output_stream = async_stream::stream! {
            while let Ok(notification) = rx.recv().await {
                if notification.path.starts_with(&path_prefix) || path_prefix == "/" {
                    yield Ok(notification);
                }
            }
        };

        Ok(Response::new(Box::pin(output_stream)))
    }

    async fn acquire_lease(
        &self,
        request: Request<powerfs::LeaseRequest>,
    ) -> Result<Response<powerfs::LeaseResponse>, Status> {
        let req = request.into_inner();
        let dir_tree = self.master.directory_tree.clone();
        let path = req.path.clone();
        let client_id = req.client_id.clone();
        let duration_ms = req.duration_ms;

        let (lease_id, epoch) = tokio::task::spawn_blocking(move || {
            let lease_id = dir_tree.acquire_lease(&path, &client_id, duration_ms);
            let epoch = dir_tree.get_epoch();
            (lease_id, epoch)
        })
        .await
        .unwrap();

        Ok(Response::new(powerfs::LeaseResponse {
            success: true,
            error: String::new(),
            lease_id,
            duration_ms,
            epoch,
        }))
    }

    async fn release_lease(
        &self,
        request: Request<powerfs::LeaseReleaseRequest>,
    ) -> Result<Response<powerfs::LeaseReleaseResponse>, Status> {
        let req = request.into_inner();
        let dir_tree = self.master.directory_tree.clone();
        let lease_id = req.lease_id.clone();

        let success = tokio::task::spawn_blocking(move || dir_tree.release_lease(&lease_id))
            .await
            .unwrap();

        Ok(Response::new(powerfs::LeaseReleaseResponse {
            success,
            error: if success {
                String::new()
            } else {
                "Lease not found".to_string()
            },
        }))
    }

    async fn renew_lease(
        &self,
        request: Request<powerfs::LeaseRenewRequest>,
    ) -> Result<Response<powerfs::LeaseRenewResponse>, Status> {
        let req = request.into_inner();
        let dir_tree = self.master.directory_tree.clone();
        let lease_id = req.lease_id.clone();
        let duration_ms = req.duration_ms;

        let result =
            tokio::task::spawn_blocking(move || dir_tree.renew_lease(&lease_id, duration_ms))
                .await
                .unwrap();

        match result {
            Some(epoch) => Ok(Response::new(powerfs::LeaseRenewResponse {
                success: true,
                error: String::new(),
                epoch,
            })),
            None => Ok(Response::new(powerfs::LeaseRenewResponse {
                success: false,
                error: "Lease not found".to_string(),
                epoch: 0,
            })),
        }
    }

    async fn register_job_client(
        &self,
        request: Request<powerfs::JobRegistrationRequest>,
    ) -> Result<Response<powerfs::JobRegistrationResponse>, Status> {
        let req = request.into_inner();
        let dir_tree = self.master.directory_tree.clone();
        let job_id = req.job_id.clone();
        let job_name = req.job_name.clone();
        let client_id = req.client_id.clone();

        let success = tokio::task::spawn_blocking(move || {
            dir_tree.register_job_client(&job_id, &job_name, &client_id)
        })
        .await
        .unwrap();

        Ok(Response::new(powerfs::JobRegistrationResponse {
            success,
            error: if success {
                String::new()
            } else {
                "Failed to register job client".to_string()
            },
        }))
    }

    async fn deregister_job_client(
        &self,
        request: Request<powerfs::JobDeregistrationRequest>,
    ) -> Result<Response<powerfs::JobDeregistrationResponse>, Status> {
        let req = request.into_inner();
        let dir_tree = self.master.directory_tree.clone();
        let job_id = req.job_id.clone();
        let client_id = req.client_id.clone();

        let success = tokio::task::spawn_blocking(move || {
            dir_tree.deregister_job_client(&job_id, &client_id)
        })
        .await
        .unwrap();

        Ok(Response::new(powerfs::JobDeregistrationResponse {
            success,
            error: if success {
                String::new()
            } else {
                "Job not found".to_string()
            },
        }))
    }

    async fn complete_job(
        &self,
        request: Request<powerfs::JobCompletionRequest>,
    ) -> Result<Response<powerfs::JobCompletionResponse>, Status> {
        let req = request.into_inner();
        let dir_tree = self.master.directory_tree.clone();
        let job_id = req.job_id.clone();

        let result = tokio::task::spawn_blocking(move || dir_tree.complete_job(&job_id))
            .await
            .unwrap();

        match result {
            Some(invalidated_entries) => Ok(Response::new(powerfs::JobCompletionResponse {
                success: true,
                error: String::new(),
                invalidated_entries,
            })),
            None => Err(Status::not_found("Job not found")),
        }
    }

    async fn get_job_info(
        &self,
        request: Request<powerfs::JobInfoRequest>,
    ) -> Result<Response<powerfs::JobInfoResponse>, Status> {
        let req = request.into_inner();
        let dir_tree = self.master.directory_tree.clone();
        let job_id = req.job_id.clone();

        let result = tokio::task::spawn_blocking(move || dir_tree.get_job_info(&job_id))
            .await
            .unwrap();

        match result {
            Some(job) => {
                let job_ctx = powerfs::JobContext {
                    job_id: job.job_id,
                    job_name: job.job_name,
                    client_ids: job.client_ids.into_iter().collect(),
                    start_time: job.start_time,
                    end_time: job.end_time,
                    is_active: job.is_active,
                };
                Ok(Response::new(powerfs::JobInfoResponse {
                    found: true,
                    job: Some(job_ctx),
                    error: String::new(),
                }))
            }
            None => Ok(Response::new(powerfs::JobInfoResponse {
                found: false,
                job: None,
                error: "Job not found".to_string(),
            })),
        }
    }
}
