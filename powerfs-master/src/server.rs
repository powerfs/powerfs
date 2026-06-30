use super::master::MasterNode;
use super::proto::*;
use futures::Stream;
use log::debug;
use powerfs_common::constants::DEFAULT_VOLUME_SIZE;
use powerfs_common::types::VolumeId;
use std::pin::Pin;
use std::sync::Arc;
use tonic::{transport::Server, Request, Response, Status, Streaming};
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
}

#[tonic::async_trait]
impl MasterService for MasterGrpcServer {
    type SendHeartbeatStream =
        Pin<Box<dyn Stream<Item = Result<HeartbeatResponse, Status>> + Send + 'static>>;

    async fn send_heartbeat(
        &self,
        request: Request<Streaming<Heartbeat>>,
    ) -> Result<Response<Self::SendHeartbeatStream>, Status> {
        let mut stream = request.into_inner();
        let master = self.master.clone();

        let output = async_stream::stream! {
            while let Some(heartbeat) = stream.message().await.unwrap_or(None) {
                debug!("Received heartbeat from: {}", heartbeat.id);

                let node_id = NodeId(heartbeat.id.clone());

                if heartbeat.volumes.is_empty() {
                    if let Err(e) = master.add_node(
                        node_id,
                        heartbeat.ip.clone(),
                        heartbeat.rack.clone(),
                        heartbeat.data_center.clone(),
                    ).await {
                        debug!("Failed to add node: {}", e);
                    }
                } else {
                    if let Err(e) = master.update_node_volumes(
                        &node_id,
                        &heartbeat.volumes,
                        &heartbeat.ip,
                        heartbeat.grpc_port,
                    ).await {
                        debug!("Failed to update node volumes: {}", e);
                    }
                }

                let leader = format!("{}", master.id());

                yield Ok(HeartbeatResponse {
                    volume_size_limit: DEFAULT_VOLUME_SIZE,
                    leader,
                });
            }
        };

        Ok(Response::new(Box::pin(output)))
    }

    async fn lookup_volume(
        &self,
        request: Request<LookupVolumeRequest>,
    ) -> Result<Response<LookupVolumeResponse>, Status> {
        let req = request.into_inner();
        let mut locations = Vec::new();

        for volume_id_str in req.volume_or_file_ids {
            if let Ok(volume_id) = Uuid::parse_str(&volume_id_str) {
                let volume_id = VolumeId(volume_id);
                match self.master.get_volume(&volume_id).await {
                    Ok(info) => {
                        if let Some(node) = self.master.get_node(&info.node_id) {
                            let location = Location {
                                url: format!("{}:{}", node.address, node.grpc_port),
                                public_url: node.address.clone(),
                                grpc_port: node.grpc_port,
                                data_center: node.data_center.clone(),
                            };
                            locations.push(VolumeIdLocation {
                                volume_or_file_id: volume_id_str,
                                locations: vec![location],
                                error: "".to_string(),
                            });
                        } else {
                            locations.push(VolumeIdLocation {
                                volume_or_file_id: volume_id_str,
                                locations: vec![],
                                error: "node not found".to_string(),
                            });
                        }
                    }
                    Err(_) => {
                        locations.push(VolumeIdLocation {
                            volume_or_file_id: volume_id_str,
                            locations: vec![],
                            error: "volume not found".to_string(),
                        });
                    }
                }
            } else {
                locations.push(VolumeIdLocation {
                    volume_or_file_id: volume_id_str,
                    locations: vec![],
                    error: "invalid volume id".to_string(),
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
        let req = request.into_inner();

        match self
            .master
            .assign_volume(&req.replication, &req.collection)
            .await
        {
            Ok((volume_id, node_info)) => {
                let location = Location {
                    url: format!("{}:{}", node_info.address, node_info.grpc_port),
                    public_url: node_info.address.clone(),
                    grpc_port: node_info.grpc_port,
                    data_center: node_info.data_center.clone(),
                };

                Ok(Response::new(AssignResponse {
                    fid: format!("{},{}", volume_id.0, 0),
                    count: req.count,
                    error: "".to_string(),
                    replicas: vec![location],
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
                    volume_id: volume.id.0.to_string(),
                    size: volume.size,
                    read_only: volume.state == VolumeState::ReadOnly,
                });
            }

            data_nodes.push(DataNodeInfo {
                id: node.id.0.clone(),
                address: node.address.clone(),
                grpc_port: node.grpc_port,
                data_center: node.data_center.clone(),
                rack: node.rack.clone(),
                volumes: volume_infos,
            });
        }

        Ok(Response::new(VolumeListResponse {
            data_nodes,
            volume_size_limit: DEFAULT_VOLUME_SIZE,
        }))
    }

    async fn ping(&self, _request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        let start = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;

        Ok(Response::new(PingResponse {
            start_time_ns: start,
            stop_time_ns: start,
        }))
    }
}

use powerfs_common::types::VolumeState;
