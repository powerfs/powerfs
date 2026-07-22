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
            let session_result = self.run_heartbeat_session().await;
            let was_leader_changed = match &session_result {
                Err(e) => e.to_string().contains("Leader changed"),
                _ => false,
            };

            match session_result {
                Ok(_) => {
                    info!("VOLUME_HEARTBEAT: heartbeat session completed");
                }
                Err(e) => {
                    warn!("VOLUME_HEARTBEAT: session error: {}", e);
                }
            }

            tokio::time::sleep(Duration::from_secs(2)).await;
            // LEADER_CHANGED 已经在 session 中更新了正确的 leader 索引，
            // 不需要再切换到下一个 master
            if !was_leader_changed {
                self.next_master();
            }
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
                        "Heartbeat response: leader={}, volume_size_limit={}, error={}, error_code={}",
                        resp.leader, resp.volume_size_limit, resp.error, resp.error_code
                    );

                    if !resp.error.is_empty() {
                        warn!(
                            "Heartbeat error from master: {} (code: {})",
                            resp.error, resp.error_code
                        );
                        match resp.error_code.as_str() {
                            "LEADER_CHANGED" => {
                                if !resp.leader.is_empty() {
                                    if let Some(idx) =
                                        master_addresses.iter().position(|a| a.eq(&resp.leader))
                                    {
                                        let current = current_master_index.load(Ordering::Relaxed);
                                        if idx != current {
                                            info!("Switching to leader master: {}", resp.leader);
                                            current_master_index.store(idx, Ordering::Relaxed);
                                        }
                                    }
                                }
                                return Err("Leader changed, reconnecting".into());
                            }
                            "RETRYABLE" => {
                                continue;
                            }
                            "RATE_LIMITED" => {
                                tokio::time::sleep(Duration::from_secs(5)).await;
                                continue;
                            }
                            _ => {
                                return Err(format!("Heartbeat failed: {}", resp.error).into());
                            }
                        }
                    }

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
        let max_retries = 3;
        let mut attempt = 0;

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
                        data_node: self.node_id.0.clone(),
                        disk_type: "".to_string(),
                        count,
                    };

                    match client.volume_grow(request).await {
                        Ok(response) => {
                            let resp = response.into_inner();
                            if !resp.error.is_empty() {
                                return Err(resp.error.into());
                            }
                            return Ok(resp);
                        }
                        Err(e) => {
                            attempt += 1;
                            let msg = format!("{}", e);
                            warn!(
                                "Failed to grow volumes via {} (attempt {}): {}",
                                addr, attempt, e
                            );
                            if msg.contains("not leader") {
                                if let Some(start) = msg.find("current leader is ") {
                                    let leader_addr = msg[start + 18..].trim();
                                    if !leader_addr.is_empty() {
                                        info!("Trying to switch to leader: {}", leader_addr);
                                        if let Some((index, _)) = self
                                            .master_addresses
                                            .iter()
                                            .enumerate()
                                            .find(|(_, a)| **a == leader_addr)
                                        {
                                            self.current_master_index
                                                .store(index, Ordering::Relaxed);
                                            info!("Switched to leader at index {}", index);
                                        }
                                    }
                                }
                            }
                            if attempt >= max_retries {
                                return Err(msg.into());
                            }
                            self.next_master();
                            tokio::time::sleep(Duration::from_millis(
                                500 * (1u64 << (attempt - 1)),
                            ))
                            .await;
                        }
                    }
                }
                Err(e) => {
                    attempt += 1;
                    warn!(
                        "Failed to connect to master {} (attempt {}): {}",
                        addr, attempt, e
                    );
                    if attempt >= max_retries {
                        return Err(e.into());
                    }
                    self.next_master();
                    tokio::time::sleep(Duration::from_millis(500 * (1u64 << (attempt - 1)))).await;
                }
            }
        }
    }
}
