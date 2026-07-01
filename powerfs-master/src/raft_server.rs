//! Raft gRPC service implementation
//!
//! This module provides the RaftService implementation for inter-node communication.

use crate::proto::powerfs::raft_service_server::RaftService;
use crate::proto::{
    AddNodeRequest, AddNodeResponse, ClusterInfoRequest, ClusterInfoResponse, ProposeRequest,
    ProposeResponse, RaftMessage, RaftServiceClient,
};
use crate::raft_node::RaftNode;
use futures::StreamExt;
use log::{error, info, warn};
use protobuf::Message;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::timeout;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

/// RaftServiceServer handles Raft communication between master nodes
pub struct RaftServiceServer {
    /// The RaftNode managed by this server
    raft_node: Arc<RwLock<RaftNode>>,
    /// Client for sending messages to other peers
    peer_clients: Arc<RwLock<Vec<RaftGrpcClient>>>,
}

impl RaftServiceServer {
    pub fn new(raft_node: RaftNode) -> Self {
        Self {
            raft_node: Arc::new(RwLock::new(raft_node)),
            peer_clients: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Add a peer client for sending messages
    pub async fn add_peer(&self, address: String) -> Result<(), String> {
        let client = RaftGrpcClient::connect(address)
            .await
            .map_err(|e| format!("failed to connect to peer: {}", e))?;

        self.peer_clients.write().await.push(client);
        Ok(())
    }

    /// Get the raft node
    pub fn get_raft_node(&self) -> Arc<RwLock<RaftNode>> {
        self.raft_node.clone()
    }

    /// Broadcast a message to all peers
    pub async fn broadcast_message(&self, msg: RaftMessage) -> Result<(), String> {
        let clients = self.peer_clients.read().await;
        for client in clients.iter() {
            if let Err(e) = client.send_message(msg.clone()).await {
                warn!("Failed to send message to peer: {}", e);
            }
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl RaftService for RaftServiceServer {
    /// Propose a command to the Raft cluster
    async fn propose(
        &self,
        request: Request<ProposeRequest>,
    ) -> Result<Response<ProposeResponse>, Status> {
        let req = request.into_inner();

        info!("Received propose request: {} bytes", req.command.len());

        let raft_node = self.raft_node.read().await;

        // Check if leader
        if !raft_node.is_leader() {
            return Ok(Response::new(ProposeResponse {
                success: false,
                error: "not the leader".to_string(),
                index: 0,
            }));
        }

        // Propose the command
        match timeout(
            Duration::from_secs(5),
            raft_node.propose(req.command),
        )
        .await
        {
            Ok(Ok(index)) => Ok(Response::new(ProposeResponse {
                success: true,
                error: String::new(),
                index,
            })),
            Ok(Err(e)) => Ok(Response::new(ProposeResponse {
                success: false,
                error: e,
                index: 0,
            })),
            Err(_) => Ok(Response::new(ProposeResponse {
                success: false,
                error: "timeout".to_string(),
                index: 0,
            })),
        }
    }

    /// Bidirectional streaming for Raft messages
    type RaftMessageStreamStream = ReceiverStream<Result<RaftMessage, Status>>;

    async fn raft_message_stream(
        &self,
        request: Request<Streaming<RaftMessage>>,
    ) -> Result<Response<Self::RaftMessageStreamStream>, Status> {
        let mut stream = request.into_inner();
        let raft_node = self.raft_node.clone();

        let (_tx, rx) = tokio::sync::mpsc::channel(100);

        // Spawn a task to handle incoming messages
        tokio::spawn(async move {
            while let Some(msg) = stream.next().await {
                match msg {
                    Ok(raft_msg) => {
                        // Parse the Raft message
                        if let Ok(internal_msg) =
                            <raft::eraftpb::Message as Message>::parse_from_bytes(
                                &raft_msg.message,
                            ) {
                            let mut node = raft_node.write().await;
                            if let Err(e) = node.step(internal_msg) {
                                error!("Failed to step message: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        error!("Error receiving message: {}", e);
                        break;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    /// Get cluster information
    async fn get_cluster_info(
        &self,
        _request: Request<ClusterInfoRequest>,
    ) -> Result<Response<ClusterInfoResponse>, Status> {
        let raft_node = self.raft_node.read().await;
        let info = raft_node.get_cluster_info();

        Ok(Response::new(ClusterInfoResponse {
            node_id: info.node_id,
            address: info.address,
            is_leader: info.is_leader,
            term: info.term,
            peers: info.peers,
        }))
    }

    /// Add a new node to the cluster
    async fn add_node(
        &self,
        request: Request<AddNodeRequest>,
    ) -> Result<Response<AddNodeResponse>, Status> {
        let req = request.into_inner();

        let is_leader = self.raft_node.read().await.is_leader();

        if !is_leader {
            return Ok(Response::new(AddNodeResponse {
                success: false,
                error: "not the leader".to_string(),
            }));
        }

        let peer = crate::raft_node::Peer {
            id: req.node_id,
            address: req.address.clone(),
        };

        let mut node = self.raft_node.write().await;
        match node.add_peer(peer) {
            Ok(()) => {
                info!("Added peer: {} at {}", req.node_id, req.address);
                Ok(Response::new(AddNodeResponse {
                    success: true,
                    error: String::new(),
                }))
            }
            Err(e) => Ok(Response::new(AddNodeResponse {
                success: false,
                error: e,
            })),
        }
    }
}

/// Client for connecting to other Raft nodes
pub struct RaftGrpcClient {
    address: String,
    client: Option<RaftServiceClient<tonic::transport::Channel>>,
}

impl RaftGrpcClient {
    /// Connect to a Raft peer
    pub async fn connect(address: String) -> Result<Self, String> {
        let addr = format!("http://{}", address);
        let client = RaftServiceClient::connect(addr.clone())
            .await
            .map_err(|e| format!("failed to connect to peer {}: {}", address, e))?;

        Ok(Self {
            address,
            client: Some(client),
        })
    }

    /// Send a Raft message to this peer
    pub async fn send_message(&self, msg: RaftMessage) -> Result<(), String> {
        if let Some(ref client) = self.client {
            let mut client = client.clone();
            let request = Request::new(
                tokio_stream::once(msg)
            );

            // Use the bidirectional streaming RPC
            let _response = client
                .raft_message_stream(request)
                .await
                .map_err(|e| format!("failed to send raft message: {}", e))?;

            Ok(())
        } else {
            Err("client not connected".to_string())
        }
    }

    /// Get the peer address
    pub fn address(&self) -> &str {
        &self.address
    }
}
