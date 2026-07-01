use crate::proto::*;
use crate::raft_node::OutgoingMessage;
use log::{info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::transport::Channel;

pub struct RaftGrpcClient {
    clients: Arc<RwLock<HashMap<String, RaftServiceClient<Channel>>>>,
    max_retries: usize,
    retry_delay_ms: u64,
}

impl RaftGrpcClient {
    pub fn new(max_retries: usize, retry_delay_ms: u64) -> Self {
        RaftGrpcClient {
            clients: Arc::new(RwLock::new(HashMap::new())),
            max_retries,
            retry_delay_ms,
        }
    }

    async fn get_client(&self, address: &str) -> Result<RaftServiceClient<Channel>, String> {
        let mut clients = self.clients.write().await;

        if let Some(client) = clients.get(address).cloned() {
            return Ok(client);
        }

        let addr = format!("http://{}", address);
        let client = RaftServiceClient::connect(addr)
            .await
            .map_err(|e| format!("failed to connect to {}: {}", address, e))?;

        clients.insert(address.to_string(), client.clone());
        Ok(client)
    }

    pub async fn propose(&self, address: &str, command: Vec<u8>) -> Result<u64, String> {
        let request = ProposeRequest { command };

        for attempt in 0..=self.max_retries {
            match self.get_client(address).await {
                Ok(mut client) => match client.propose(request.clone()).await {
                    Ok(response) => {
                        let resp = response.into_inner();
                        if resp.success {
                            return Ok(resp.index);
                        } else {
                            return Err(resp.error);
                        }
                    }
                    Err(e) => {
                        warn!("Propose attempt {} failed for {}: {}", attempt, address, e);
                        self.clients.write().await.remove(address);
                    }
                },
                Err(e) => {
                    warn!(
                        "Failed to get client for {} on attempt {}: {}",
                        address, attempt, e
                    );
                }
            }

            if attempt < self.max_retries {
                tokio::time::sleep(tokio::time::Duration::from_millis(self.retry_delay_ms)).await;
            }
        }

        Err(format!(
            "failed to propose to {} after {} attempts",
            address,
            self.max_retries + 1
        ))
    }

    pub async fn send_raft_message(
        &self,
        address: &str,
        message: OutgoingMessage,
    ) -> Result<(), String> {
        let request = RaftMessage {
            from_id: 0,
            to_id: message.to_id,
            message: message.message,
        };

        for attempt in 0..=self.max_retries {
            match self.get_client(address).await {
                Ok(mut client) => match client.send_raft_message(request.clone()).await {
                    Ok(response) => {
                        let resp = response.into_inner();
                        if resp.success {
                            return Ok(());
                        } else {
                            return Err(resp.error);
                        }
                    }
                    Err(e) => {
                        warn!(
                            "Send message attempt {} failed for {}: {}",
                            attempt, address, e
                        );
                        self.clients.write().await.remove(address);
                    }
                },
                Err(e) => {
                    warn!(
                        "Failed to get client for {} on attempt {}: {}",
                        address, attempt, e
                    );
                }
            }

            if attempt < self.max_retries {
                tokio::time::sleep(tokio::time::Duration::from_millis(self.retry_delay_ms)).await;
            }
        }

        Err(format!(
            "failed to send raft message to {} after {} attempts",
            address,
            self.max_retries + 1
        ))
    }

    pub async fn add_node(
        &self,
        address: &str,
        node_id: u64,
        node_address: &str,
    ) -> Result<(), String> {
        let request = AddNodeRequest {
            node_id,
            address: node_address.to_string(),
        };

        for attempt in 0..=self.max_retries {
            match self.get_client(address).await {
                Ok(mut client) => match client.add_node(request.clone()).await {
                    Ok(response) => {
                        let resp = response.into_inner();
                        if resp.success {
                            return Ok(());
                        } else {
                            return Err(resp.error);
                        }
                    }
                    Err(e) => {
                        warn!("Add node attempt {} failed for {}: {}", attempt, address, e);
                        self.clients.write().await.remove(address);
                    }
                },
                Err(e) => {
                    warn!(
                        "Failed to get client for {} on attempt {}: {}",
                        address, attempt, e
                    );
                }
            }

            if attempt < self.max_retries {
                tokio::time::sleep(tokio::time::Duration::from_millis(self.retry_delay_ms)).await;
            }
        }

        Err(format!(
            "failed to add node via {} after {} attempts",
            address,
            self.max_retries + 1
        ))
    }

    pub async fn remove_node(&self, address: &str, node_id: u64) -> Result<(), String> {
        let request = RemoveNodeRequest { node_id };

        for attempt in 0..=self.max_retries {
            match self.get_client(address).await {
                Ok(mut client) => match client.remove_node(request.clone()).await {
                    Ok(response) => {
                        let resp = response.into_inner();
                        if resp.success {
                            return Ok(());
                        } else {
                            return Err(resp.error);
                        }
                    }
                    Err(e) => {
                        warn!(
                            "Remove node attempt {} failed for {}: {}",
                            attempt, address, e
                        );
                        self.clients.write().await.remove(address);
                    }
                },
                Err(e) => {
                    warn!(
                        "Failed to get client for {} on attempt {}: {}",
                        address, attempt, e
                    );
                }
            }

            if attempt < self.max_retries {
                tokio::time::sleep(tokio::time::Duration::from_millis(self.retry_delay_ms)).await;
            }
        }

        Err(format!(
            "failed to remove node via {} after {} attempts",
            address,
            self.max_retries + 1
        ))
    }

    pub async fn get_cluster_info(&self, address: &str) -> Result<ClusterInfoResponse, String> {
        let request = ClusterInfoRequest {};

        for attempt in 0..=self.max_retries {
            match self.get_client(address).await {
                Ok(mut client) => match client.get_cluster_info(request.clone()).await {
                    Ok(response) => return Ok(response.into_inner()),
                    Err(e) => {
                        warn!(
                            "Get cluster info attempt {} failed for {}: {}",
                            attempt, address, e
                        );
                        self.clients.write().await.remove(address);
                    }
                },
                Err(e) => {
                    warn!(
                        "Failed to get client for {} on attempt {}: {}",
                        address, attempt, e
                    );
                }
            }

            if attempt < self.max_retries {
                tokio::time::sleep(tokio::time::Duration::from_millis(self.retry_delay_ms)).await;
            }
        }

        Err(format!(
            "failed to get cluster info from {} after {} attempts",
            address,
            self.max_retries + 1
        ))
    }

    pub async fn transfer_leader(&self, address: &str, target_id: u64) -> Result<(), String> {
        let request = TransferLeaderRequest {
            target_node_id: target_id,
        };

        for attempt in 0..=self.max_retries {
            match self.get_client(address).await {
                Ok(mut client) => match client.transfer_leader(request.clone()).await {
                    Ok(response) => {
                        let resp = response.into_inner();
                        if resp.success {
                            return Ok(());
                        } else {
                            return Err(resp.error);
                        }
                    }
                    Err(e) => {
                        warn!(
                            "Transfer leader attempt {} failed for {}: {}",
                            attempt, address, e
                        );
                        self.clients.write().await.remove(address);
                    }
                },
                Err(e) => {
                    warn!(
                        "Failed to get client for {} on attempt {}: {}",
                        address, attempt, e
                    );
                }
            }

            if attempt < self.max_retries {
                tokio::time::sleep(tokio::time::Duration::from_millis(self.retry_delay_ms)).await;
            }
        }

        Err(format!(
            "failed to transfer leader via {} after {} attempts",
            address,
            self.max_retries + 1
        ))
    }

    pub async fn invalidate_client(&self, address: &str) {
        let mut clients = self.clients.write().await;
        clients.remove(address);
        info!("Invalidated client for {}", address);
    }
}
