use log::{debug, info, warn};
use powerfs_common::types::NodeId;
use powerfs_master::proto::{
    powerfs::master_service_client::MasterServiceClient, Heartbeat, VolumeGrowRequest,
    VolumeGrowResponse, VolumeShortInfo,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
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
    heartbeat_running: Arc<AtomicBool>,
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
        let (tx, _) = broadcast::channel(100);

        MasterClient {
            master_addresses: params
                .master_addresses
                .iter()
                .map(|s| s.to_string())
                .collect(),
            current_master_index: Arc::new(AtomicUsize::new(0)),
            node_id: params.node_id,
            grpc_port: params.grpc_port,
            http_port: params.http_port,
            data_center: params.data_center.to_string(),
            rack: params.rack.to_string(),
            public_url: params.public_url.to_string(),
            ip: params.ip.to_string(),
            heartbeat_tx: Arc::new(tx),
            heartbeat_running: Arc::new(AtomicBool::new(false)),
        }
    }

    fn current_master(&self) -> String {
        let idx = self.current_master_index.load(Ordering::Relaxed);
        self.master_addresses
            .get(idx)
            .cloned()
            .unwrap_or_else(|| self.master_addresses[0].clone())
    }

    fn next_master(&self) {
        let current = self.current_master_index.load(Ordering::Relaxed);
        let next = (current + 1) % self.master_addresses.len();
        self.current_master_index.store(next, Ordering::Relaxed);
    }

    async fn try_connect(
        &self,
    ) -> Result<(MasterServiceClient<Channel>, String), Box<dyn std::error::Error + Send + Sync>>
    {
        let mut tried = 0;
        let max_tries = self.master_addresses.len();

        loop {
            let addr = self.current_master();
            let address = format!("http://{}", addr);
            debug!("Trying to connect to master: {}", address);

            match Channel::from_shared(address)?.connect().await {
                Ok(channel) => {
                    return Ok((MasterServiceClient::new(channel), addr));
                }
                Err(e) => {
                    warn!("Failed to connect to master {}: {}", addr, e);
                    tried += 1;
                    if tried >= max_tries {
                        return Err("Failed to connect to any master node".into());
                    }
                    self.next_master();
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
            }
        }
    }

    pub async fn start_heartbeat(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if !self
            .heartbeat_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            info!("VOLUME_HEARTBEAT: heartbeat already running, skipping");
            return Ok(());
        }

        info!("VOLUME_HEARTBEAT: initializing heartbeat connection");

        let client_clone = self.clone();

        tokio::spawn(async move {
            client_clone.run_heartbeat_daemon().await;
            client_clone
                .heartbeat_running
                .store(false, Ordering::Release);
        });

        Ok(())
    }

    async fn run_heartbeat_daemon(&self) {
        loop {
            match self.run_heartbeat_session().await {
                Ok(_) => {
                    info!("VOLUME_HEARTBEAT: heartbeat session completed");
                }
                Err(e) => {
                    warn!("VOLUME_HEARTBEAT: session error: {}", e);
                }
            }

            tokio::time::sleep(Duration::from_secs(5)).await;
            self.next_master();
        }
    }

    async fn run_heartbeat_session(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (mut client, addr) = self.try_connect().await?;

        let rx = self.heartbeat_tx.subscribe();
        let stream = tokio_stream::wrappers::BroadcastStream::new(rx).filter_map(|r| r.ok());

        info!("VOLUME_HEARTBEAT: connected to master at {}", addr);

        let response_stream = client.send_heartbeat(tonic::Request::new(stream)).await?;
        let mut responses = response_stream.into_inner();

        let master_addresses = self.master_addresses.clone();
        let current_master_index = self.current_master_index.clone();

        while let Some(response) = responses.next().await {
            match response {
                Ok(resp) => {
                    debug!(
                        "Heartbeat response: leader={}, volume_size_limit={}",
                        resp.leader, resp.volume_size_limit
                    );

                    if !resp.leader.is_empty() {
                        if let Some(idx) = master_addresses.iter().position(|a| a.eq(&resp.leader))
                        {
                            let current = current_master_index.load(Ordering::Relaxed);
                            if idx != current {
                                info!("Switching to leader master: {}", resp.leader);
                                current_master_index.store(idx, Ordering::Relaxed);
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("Heartbeat stream error: {}", e);
                    return Err(e.into());
                }
            }
        }

        warn!("Heartbeat stream closed by master");
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

        self.heartbeat_tx
            .send(heartbeat)
            .map(|_| ())
            .map_err(|e| e.into())
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
                        ttl: "".to_string(),
                        data_center: self.data_center.clone(),
                        rack: self.rack.clone(),
                        data_node: "".to_string(),
                        disk_type: "".to_string(),
                        count,
                    };

                    match client.volume_grow(request).await {
                        Ok(response) => {
                            return Ok(response.into_inner());
                        }
                        Err(e) => {
                            warn!("Failed to grow volumes via {}: {}", addr, e);
                            tried += 1;
                            if tried >= max_tries {
                                return Err(e.into());
                            }
                            self.next_master();
                            tokio::time::sleep(Duration::from_secs(3)).await;
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to connect to master {}: {}", addr, e);
                    tried += 1;
                    if tried >= max_tries {
                        return Err(e.into());
                    }
                    self.next_master();
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
            }
        }
    }
}
