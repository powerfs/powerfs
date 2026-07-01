use super::master::MasterNode;
use super::proto::*;
use futures::Stream;
use log::{debug, warn};
use powerfs_common::constants::DEFAULT_VOLUME_SIZE;
use powerfs_common::types::VolumeId;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tonic::{transport::Channel, transport::Server, Request, Response, Status, Streaming};
use uuid::Uuid;

pub struct MasterGrpcServer {
    master: Arc<MasterNode>,
}

impl MasterGrpcServer {
    pub fn new(master: Arc<MasterNode>) -> Self {
        MasterGrpcServer { master }
    }

    pub async fn start(self, addr: std::net::SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        Server::builder()
            .add_service(MasterServiceServer::new(self))
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
                        .add_node(
                            node_id,
                            heartbeat.ip.clone(),
                            heartbeat.rack.clone(),
                            heartbeat.data_center.clone(),
                            heartbeat.port,
                            heartbeat.grpc_port,
                            heartbeat.public_url.clone(),
                        )
                        .await
                    {
                        debug!("Failed to add node: {}", e);
                    }
                } else {
                    if let Err(e) = master
                        .update_node_volumes(
                            &node_id,
                            &heartbeat.volumes,
                            &heartbeat.new_volumes,
                            &heartbeat.deleted_volumes,
                            &heartbeat.ip,
                            heartbeat.grpc_port,
                            heartbeat.port,
                        )
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
        // Forward to leader if not leader
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

        match self
            .master
            .assign_volume(&req.replication, &req.collection)
            .await
        {
            Ok((fid, nodes)) => {
                let mut replicas = Vec::new();
                let mut primary_location: Option<Location> = None;

                for (i, node) in nodes.iter().enumerate() {
                    let location = Location {
                        url: node.url(),
                        public_url: node.public_url.clone(),
                        grpc_port: node.grpc_port,
                        data_center: node.data_center_id.to_string(),
                    };
                    if i == 0 {
                        primary_location = Some(location.clone());
                    }
                    replicas.push(location);
                }

                Ok(Response::new(AssignResponse {
                    fid: fid.to_string(),
                    count: req.count,
                    error: String::new(),
                    auth: String::new(),
                    replicas,
                    location: primary_location,
                }))
            }
            Err(e) => Err(Status::internal(format!("{}", e))),
        }
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
}
