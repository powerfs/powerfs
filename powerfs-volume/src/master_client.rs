use log::{debug, info, warn};
use powerfs_common::types::NodeId;
use powerfs_master::proto::{
    powerfs::master_service_client::MasterServiceClient, Heartbeat, VolumeGrowRequest,
    VolumeGrowResponse, VolumeShortInfo,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::StreamExt as _;
use tonic::transport::Channel;

#[derive(Clone)]
pub struct MasterClient {
    master_addresses: Vec<String>,
    current_master_index: Arc<AtomicUsize>,
    node_id: NodeId,
    grpc_port: u32,
    http_port: u32,
    data_center: String,
    rack: String,
    public_url: String,
    ip: String,
    heartbeat_tx: Arc<broadcast::Sender<Heartbeat>>,
}

#[derive(Clone)]
pub struct NewMasterClientParams<'a> {
    pub master_addresses: &'a [&'a str],
    pub node_id: NodeId,
    pub grpc_port: u32,
    pub http_port: u32,
    pub data_center: &'a str,
    pub rack: &'a str,
    pub public_url: &'a str,
    pub ip: &'a str,
}

impl MasterClient {
    pub fn new(params: NewMasterClientParams<'_>) -> Self {
        let (tx, _) = broadcast::channel(10);
        
        MasterClient {
            master_addresses: params.master_addresses.iter().map(|s| s.to_string()).collect(),
            current_master_index: Arc::new(AtomicUsize::new(0)),
            node_id: params.node_id,
            grpc_port: params.grpc_port,
            http_port: params.http_port,
            data_center: params.data_center.to_string(),
            rack: params.rack.to_string(),
            public_url: params.public_url.to_string(),
            ip: params.ip.to_string(),
            heartbeat_tx: Arc::new(tx),
        }
    }

    fn current_master(&self) -> String {
        let idx = self.current_master_index.load(Ordering::Relaxed);
        self.master_addresses.get(idx)
            .cloned()
            .unwrap_or_else(|| self.master_addresses[0].clone())
    }

    fn next_master(&self) {
        let len = self.master_addresses.len();
        let current = self.current_master_index.load(Ordering::Relaxed);
        self.current_master_index.store((current + 1) % len, Ordering::Relaxed);
    }

    async fn try_connect(&self) -> Result<(MasterServiceClient<Channel>, String), Box<dyn std::error::Error + Send + Sync>> {
        let mut tried = 0;
        let max_tries = self.master_addresses.len();
        
        loop {
            let addr = self.current_master();
            let address = format!("http://{}", addr);
            debug!("Trying to connect to master: {}", address);
            
            match Channel::from_shared(address)?.connect().await {
                Ok(channel) => {
                    info!("Connected to master: {}", addr);
                    return Ok((MasterServiceClient::new(channel), addr));
                }
                Err(e) => {
                    warn!("Failed to connect to master {}: {}", addr, e);
                    self.next_master();
                    tried += 1;
                    if tried >= max_tries {
                        break;
                    }
                }
            }
        }
        
        Err("Failed to connect to any master node".into())
    }

    pub async fn start_heartbeat(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (mut client, addr) = self.try_connect().await?;

        let rx = self.heartbeat_tx.subscribe();
        let stream = tokio_stream::wrappers::BroadcastStream::new(rx)
            .filter_map(|r| r.ok());

        let response_stream = match client.send_heartbeat(tonic::Request::new(stream)).await {
            Ok(rs) => rs,
            Err(e) => {
                warn!("Failed to send heartbeat stream to {}: {}", addr, e);
                self.next_master();
                return Box::pin(self.start_heartbeat()).await;
            }
        };
        
        let mut responses = response_stream.into_inner();
        let master_addresses = self.master_addresses.clone();
        let current_master_index = self.current_master_index.clone();

        tokio::spawn(async move {
            while let Some(response) = responses.next().await {
                match response {
                    Ok(resp) => {
                        debug!(
                            "Heartbeat response: leader={}, volume_size_limit={}",
                            resp.leader, resp.volume_size_limit
                        );
                        
                        if !resp.leader.is_empty() {
                            if let Some(idx) = master_addresses.iter().position(|a| a.eq(&resp.leader)) {
                                let current = current_master_index.load(Ordering::Relaxed);
                                if idx != current {
                                    info!("Switching to leader master: {}", resp.leader);
                                    current_master_index.store(idx, Ordering::Relaxed);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Heartbeat error: {}", e);
                        break;
                    }
                }
            }
            warn!("Heartbeat stream ended, reconnecting...");
            current_master_index.store(
                (current_master_index.load(Ordering::Relaxed) + 1) % master_addresses.len(),
                Ordering::Relaxed
            );
        });

        Ok(())
    }

    pub async fn send_heartbeat(
        &self,
        volumes: Vec<VolumeShortInfo>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let heartbeat = Heartbeat {
            ip: self.ip.clone(),
            port: self.http_port,
            public_url: self.public_url.clone(),
            max_file_key: 0,
            data_center: self.data_center.clone(),
            rack: self.rack.clone(),
            admin_port: 0,
            volumes: volumes.clone(),
            new_volumes: Vec::new(),
            deleted_volumes: Vec::new(),
            has_no_volumes: volumes.is_empty(),
            grpc_port: self.grpc_port,
            id: self.node_id.0.clone(),
        };

        self.heartbeat_tx.send(heartbeat).map(|_| ()).map_err(|e| e.into())
    }

    pub async fn grow(
        &self,
        replication: &str,
        collection: &str,
        count: u32,
    ) -> Result<VolumeGrowResponse, Box<dyn std::error::Error + Send + Sync>> {
        let mut tried = 0;
        let max_tries = self.master_addresses.len();
        
        loop {
            let addr = self.current_master();
            let address = format!("http://{}", addr);
            debug!("Trying to grow volumes via master: {}", address);

            match Channel::from_shared(address)?.connect().await {
                Ok(channel) => {
                    let mut client = MasterServiceClient::new(channel);

                    let request = VolumeGrowRequest {
                        replication: replication.to_string(),
                        collection: collection.to_string(),
                        ttl: String::new(),
                        data_center: self.data_center.clone(),
                        rack: self.rack.clone(),
                        data_node: self.node_id.0.clone(),
                        disk_type: String::new(),
                        count,
                    };

                    match client.volume_grow(tonic::Request::new(request)).await {
                        Ok(response) => {
                            info!("Volume grow successful via master: {}", addr);
                            return Ok(response.into_inner());
                        }
                        Err(e) => {
                            warn!("Volume grow failed on master {}: {}", addr, e);
                            self.next_master();
                            tried += 1;
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to connect to master {}: {}", addr, e);
                    self.next_master();
                    tried += 1;
                }
            }
            
            if tried >= max_tries {
                break;
            }
        }

        Err("Failed to grow volumes via any master node".into())
    }
}
