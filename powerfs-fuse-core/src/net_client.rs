//! PowerFS Net Client - powerfs-net binary protocol client for FUSE
//!
//! This module provides a client that communicates with PowerFS Master/Volume
//! servers using the lightweight powerfs-net binary protocol instead of gRPC.

use log::info;
use powerfs_net::{ClientConfig, ClientType, NetResult, PowerFsNetClient};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Configuration for the net client
#[derive(Debug, Clone)]
pub struct NetClientConfig {
    pub master_addr: String,
    pub master_net_port: u16,
    pub volume_net_port: u16,
    pub filer_addr: String,
    pub filer_net_port: u16,
    pub client_id: u64,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
}

impl Default for NetClientConfig {
    fn default() -> Self {
        Self {
            master_addr: "127.0.0.1".into(),
            master_net_port: 9334,
            volume_net_port: 8081,
            filer_addr: "127.0.0.1".into(),
            filer_net_port: 9334,
            client_id: 0,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(10),
        }
    }
}

/// A unified client that can talk to both Master and Volume via powerfs-net
#[derive(Clone)]
pub struct PowerFuseNetClient {
    master_client: Arc<PowerFsNetClient>,
    filer_client: Arc<PowerFsNetClient>,
    volume_clients: Arc<RwLock<Vec<Arc<PowerFsNetClient>>>>,
    config: NetClientConfig,
}

impl PowerFuseNetClient {
    pub async fn new(config: NetClientConfig) -> NetResult<Self> {
        let master_client = Arc::new(PowerFsNetClient::new(ClientConfig {
            addr: config.master_addr.clone(),
            port: config.master_net_port,
            client_id: config.client_id,
            client_type: ClientType::Fuse,
            connect_timeout: config.connect_timeout,
            request_timeout: config.request_timeout,
            max_retries: 3,
            retry_delay: Duration::from_millis(100),
            heartbeat_interval: Duration::from_secs(30),
            max_inflight_requests: 256,
        }));

        master_client.connect().await?;
        info!(
            "PowerFuseNetClient connected to Master at {}:{}",
            config.master_addr, config.master_net_port
        );

        let filer_client = Arc::new(PowerFsNetClient::new(ClientConfig {
            addr: config.filer_addr.clone(),
            port: config.filer_net_port,
            client_id: config.client_id,
            client_type: ClientType::Fuse,
            connect_timeout: config.connect_timeout,
            request_timeout: config.request_timeout,
            max_retries: 3,
            retry_delay: Duration::from_millis(100),
            heartbeat_interval: Duration::from_secs(30),
            max_inflight_requests: 256,
        }));

        filer_client.connect().await?;
        info!(
            "PowerFuseNetClient connected to Filer at {}:{}",
            config.filer_addr, config.filer_net_port
        );

        Ok(Self {
            master_client,
            filer_client,
            volume_clients: Arc::new(RwLock::new(Vec::new())),
            config,
        })
    }

    /// Get or create a volume client for the given address and port
    pub async fn get_volume_client(
        &self,
        addr: &str,
        port: u16,
    ) -> NetResult<Arc<PowerFsNetClient>> {
        {
            let clients = self.volume_clients.read().await;
            for client in clients.iter() {
                if client.is_connected() {
                    let cfg = &client.config;
                    if cfg.addr == addr && cfg.port == port {
                        return Ok(client.clone());
                    }
                }
            }
        }

        let new_client = Arc::new(PowerFsNetClient::new(ClientConfig {
            addr: addr.to_string(),
            port,
            client_id: self.config.client_id,
            client_type: ClientType::Fuse,
            connect_timeout: self.config.connect_timeout,
            request_timeout: self.config.request_timeout,
            max_retries: 3,
            retry_delay: Duration::from_millis(100),
            heartbeat_interval: Duration::from_secs(30),
            max_inflight_requests: 256,
        }));

        new_client.connect().await?;

        self.volume_clients.write().await.push(new_client.clone());
        Ok(new_client)
    }

    /// 获取 master 客户端引用
    pub fn master_client(&self) -> &Arc<PowerFsNetClient> {
        &self.master_client
    }

    /// 获取 filer 客户端引用
    pub fn filer_client(&self) -> &Arc<PowerFsNetClient> {
        &self.filer_client
    }

    /// Check if master connection is alive
    pub fn is_connected(&self) -> bool {
        self.master_client.is_connected()
    }

    /// Get the master address
    pub fn master_addr(&self) -> &str {
        &self.config.master_addr
    }

    /// Get the master net port
    pub fn master_net_port(&self) -> u16 {
        self.config.master_net_port
    }

    /// Get the volume net port
    pub fn volume_net_port(&self) -> u16 {
        self.config.volume_net_port
    }
}
