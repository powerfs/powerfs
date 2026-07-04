use super::kv_cache_service::KvCacheServiceImpl;
use super::master::{AddNodeParams, MasterNode, UpdateNodeVolumesParams};
use super::metrics::{ASSIGN_REQUEST_COUNT, LOOKUP_REQUEST_COUNT, REQUEST_COUNT};
use super::proto::*;
use futures::Stream;
use log::{debug, warn};
use powerfs_common::constants::DEFAULT_VOLUME_SIZE;
use powerfs_common::types::VolumeId;
use powerfs_core::kv_cache::KVCacheEngine;
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
        let addr = format!("http://{}", leader);
        match crate::proto::powerfs::master_service_client::MasterServiceClient::connect(addr).await
        {
            Ok(client) => Some(client),
            Err(e) => {
                warn!("Failed to connect to leader {}: {}", leader, e);
                None
            }
        }
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
                .assign_volume(&req.replication, &req.collection)
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

        if let Some(entry) = dir_tree.lookup(&req.directory, &req.name) {
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

        if let Some(entry) = dir_tree.get_entry(&req.path) {
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

    async fn create_entry(
        &self,
        request: Request<CreateEntryRequest>,
    ) -> Result<Response<CreateEntryResponse>, Status> {
        let req = request.into_inner();
        let dir_tree = self.master.directory_tree.clone();

        match dir_tree.create_entry(req.entry.unwrap_or_default()) {
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

        match dir_tree.update_entry(&req.entry.unwrap_or_default()) {
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

        match dir_tree.delete_entry(&req.path) {
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

    async fn list_entries(
        &self,
        request: Request<ListEntriesRequest>,
    ) -> Result<Response<ListEntriesResponse>, Status> {
        let req = request.into_inner();
        let dir_tree = self.master.directory_tree.clone();

        let entries = dir_tree.list_entries(&req.directory, req.limit, &req.last_name);

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
                let result = match req.mutation {
                    Some(crate::proto::powerfs::mutate_entry_request::Mutation::Create(
                        create_req,
                    )) => dir_tree
                        .create_entry(create_req.entry.unwrap_or_default())
                        .map(|_| ())
                        .map_err(|e| e.to_string()),
                    Some(crate::proto::powerfs::mutate_entry_request::Mutation::Update(
                        update_req,
                    )) => dir_tree
                        .update_entry(&update_req.entry.unwrap_or_default())
                        .map_err(|e| e.to_string()),
                    Some(crate::proto::powerfs::mutate_entry_request::Mutation::Delete(
                        delete_req,
                    )) => dir_tree
                        .delete_entry(&delete_req.path)
                        .map(|_| ())
                        .map_err(|e| e.to_string()),
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
}
