use crate::master::{AddNodeParams, MasterNode};
use crate::proto::*;
use log::{info, warn};
use powerfs_common::types::NodeId;
use protobuf::Message as ProtobufMessage;
use raft::eraftpb::Message as EraftpbMessage;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tonic::{Request, Response, Status, Streaming};

pub struct RaftGrpcServer {
    master: Arc<MasterNode>,
}

impl RaftGrpcServer {
    pub fn new(master: Arc<MasterNode>) -> Self {
        RaftGrpcServer { master }
    }

    pub async fn start(self, addr: std::net::SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        tonic::transport::Server::builder()
            .add_service(RaftServiceServer::new(self))
            .serve(addr)
            .await?;
        Ok(())
    }
}

#[tonic::async_trait]
impl RaftService for RaftGrpcServer {
    async fn propose(
        &self,
        request: Request<ProposeRequest>,
    ) -> Result<Response<ProposeResponse>, Status> {
        let req = request.into_inner();
        let command = req.command;

        match self.master.raft_propose(command).await {
            Ok(index) => Ok(Response::new(ProposeResponse {
                success: true,
                error: String::new(),
                index,
            })),
            Err(e) => Ok(Response::new(ProposeResponse {
                success: false,
                error: e,
                index: 0,
            })),
        }
    }

    type RaftMessageStreamStream =
        Pin<Box<dyn Stream<Item = Result<RaftMessage, Status>> + Send + 'static>>;

    async fn raft_message_stream(
        &self,
        request: Request<Streaming<RaftMessage>>,
    ) -> Result<Response<Self::RaftMessageStreamStream>, Status> {
        static RAFT_STREAM_COUNT: AtomicU64 = AtomicU64::new(0);
        let count = RAFT_STREAM_COUNT.fetch_add(1, Ordering::Relaxed);
        info!("RAFT_STREAM_DEBUG: raft_message_stream call #{}", count);

        let mut incoming_stream = request.into_inner();
        let step_tx = self.master.raft_step_tx();
        let message_tx = self.master.raft_message_tx();

        let (tx, rx) = tokio::sync::mpsc::channel(1000);

        tokio::spawn(async move {
            while let Ok(Some(msg)) = incoming_stream.message().await {
                let raft_msg = match EraftpbMessage::parse_from_bytes(&msg.message) {
                    Ok(m) => m,
                    Err(e) => {
                        warn!("Failed to parse raft message: {}", e);
                        continue;
                    }
                };
                if step_tx.send(raft_msg).await.is_err() {
                    warn!("Failed to send raft message to step channel");
                }
            }
        });

        let master_clone = self.master.clone();
        let tx_clone = tx.clone();
        tokio::spawn(async move {
            let mut message_rx = message_tx.subscribe();
            info!(
                "RAFT_STREAM_DEBUG: subscribed to broadcast channel for stream #{}",
                count
            );
            while let Ok(msg) = message_rx.recv().await {
                let raft_msg = RaftMessage {
                    from_id: master_clone.id().to_string().parse().unwrap_or(0),
                    to_id: msg.to_id,
                    message: msg.message.to_vec(),
                };
                if tx_clone.send(Ok(raft_msg)).await.is_err() {
                    warn!("Failed to send raft message to stream");
                    break;
                }
            }
            info!(
                "RAFT_STREAM_DEBUG: unsubscribed from broadcast channel for stream #{}",
                count
            );
        });

        let output_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(output_stream)))
    }

    async fn send_raft_message(
        &self,
        request: Request<RaftMessage>,
    ) -> Result<Response<RaftMessageResponse>, Status> {
        let req = request.into_inner();

        let raft_msg = match EraftpbMessage::parse_from_bytes(&req.message) {
            Ok(m) => m,
            Err(e) => {
                warn!("Failed to parse raft message: {}", e);
                return Ok(Response::new(RaftMessageResponse {
                    success: false,
                    error: format!("failed to parse raft message: {}", e),
                }));
            }
        };

        let step_tx = self.master.raft_step_tx();
        if step_tx.send(raft_msg).await.is_err() {
            return Ok(Response::new(RaftMessageResponse {
                success: false,
                error: "failed to send message to step channel".to_string(),
            }));
        }

        Ok(Response::new(RaftMessageResponse {
            success: true,
            error: String::new(),
        }))
    }

    async fn get_cluster_info(
        &self,
        _request: Request<ClusterInfoRequest>,
    ) -> Result<Response<ClusterInfoResponse>, Status> {
        let cluster_info = self.master.get_cluster_info().await;
        Ok(Response::new(cluster_info))
    }

    async fn add_node(
        &self,
        request: Request<AddNodeRequest>,
    ) -> Result<Response<AddNodeResponse>, Status> {
        let req = request.into_inner();
        let node_id = req.node_id;
        let address = req.address;

        info!("Adding node: id={}, address={}", node_id, address);

        let params = AddNodeParams {
            node_id: NodeId(node_id.to_string()),
            address: address.clone(),
            rack: String::new(),
            data_center: String::new(),
            http_port: 8080,
            grpc_port: 9090,
            public_url: format!("http://{}:8080", address),
        };

        let result = self.master.add_node(params).await;

        match result {
            Ok(_) => Ok(Response::new(AddNodeResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(AddNodeResponse {
                success: false,
                error: e.to_string(),
            })),
        }
    }

    async fn remove_node(
        &self,
        request: Request<RemoveNodeRequest>,
    ) -> Result<Response<RemoveNodeResponse>, Status> {
        let req = request.into_inner();
        let node_id = NodeId(req.node_id.to_string());

        info!("Removing node: id={}", node_id);

        let result = self.master.remove_node(&node_id).await;

        match result {
            Ok(_) => Ok(Response::new(RemoveNodeResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(RemoveNodeResponse {
                success: false,
                error: e.to_string(),
            })),
        }
    }

    async fn transfer_leader(
        &self,
        request: Request<TransferLeaderRequest>,
    ) -> Result<Response<TransferLeaderResponse>, Status> {
        let req = request.into_inner();
        let target_id = req.target_node_id;

        info!("Transferring leadership to: id={}", target_id);

        let result = self.master.raft_transfer_leader(target_id);

        match result {
            Ok(_) => Ok(Response::new(TransferLeaderResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(TransferLeaderResponse {
                success: false,
                error: e,
            })),
        }
    }
}

use futures::Stream;
use std::pin::Pin;
