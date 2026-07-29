//! PowerFS Net Client - powerfs-net binary protocol client for FUSE
//!
//! This module provides a client that communicates with PowerFS Master/Volume
//! servers using the lightweight powerfs-net binary protocol instead of gRPC.

use log::info;
use powerfs_net::{ClientConfig, ClientType, NetResult, PowerFsNetClient};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Configuration for the net client
#[derive(Debug, Clone)]
pub struct NetClientConfig {
    pub master_addr: String,
    pub master_net_port: u16,
    pub volume_net_port: u16,
    pub volume_addrs: Vec<String>,
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
            volume_addrs: Vec::new(),
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
    filer_clients: Arc<RwLock<HashMap<String, Arc<PowerFsNetClient>>>>,
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

        let default_filer_key = format!("{}:{}", config.filer_addr, config.filer_net_port);
        let mut filer_clients_map = HashMap::new();
        filer_clients_map.insert(default_filer_key, filer_client.clone());

        Ok(Self {
            master_client,
            filer_client,
            filer_clients: Arc::new(RwLock::new(filer_clients_map)),
            volume_clients: Arc::new(RwLock::new(Vec::new())),
            config,
        })
    }

    /// Create a new wrapper client with a specific master client (for leader redirect)
    pub fn new_with_master(config: NetClientConfig, master_client: Arc<PowerFsNetClient>) -> Self {
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

        let default_filer_key = format!("{}:{}", config.filer_addr, config.filer_net_port);
        let mut filer_clients_map = HashMap::new();
        filer_clients_map.insert(default_filer_key, filer_client.clone());

        Self {
            master_client,
            filer_client,
            filer_clients: Arc::new(RwLock::new(filer_clients_map)),
            volume_clients: Arc::new(RwLock::new(Vec::new())),
            config,
        }
    }

    /// Get the config
    pub fn config(&self) -> &NetClientConfig {
        &self.config
    }

    /// Get or create a filer client for the given "host:port" address string.
    /// Automatically translates Raft/gRPC ports (8888/8889/8890) to the configured filer net port.
    pub async fn get_filer_client(&self, raw_addr: &str) -> NetResult<Arc<PowerFsNetClient>> {
        // Translate known non-net ports (8888=HTTP, 8889=gRPC/Raft, 8890=internal) to filer net port
        let addr_str = {
            let mut translated = raw_addr.to_string();
            for bad_port in &["8888", "8889", "8890"] {
                let suffix = format!(":{}", bad_port);
                if translated.ends_with(&suffix) {
                    translated = format!(
                        "{}:{}",
                        &translated[..translated.len() - suffix.len()],
                        self.config.filer_net_port
                    );
                    break;
                }
            }
            if translated != raw_addr {
                log::info!(
                    "get_filer_client: translated filer addr {} -> {} (using net port {})",
                    raw_addr,
                    translated,
                    self.config.filer_net_port
                );
            }
            translated
        };

        {
            let clients = self.filer_clients.read().await;
            if let Some(client) = clients.get(&addr_str) {
                if client.is_connected() {
                    return Ok(client.clone());
                }
            }
        }

        // Parse "host:port"
        let (host, port) = match addr_str.rsplit_once(':') {
            Some((h, p)) => match p.parse::<u16>() {
                Ok(port) => (h.to_string(), port),
                Err(_) => (addr_str.to_string(), self.config.filer_net_port),
            },
            None => (addr_str.to_string(), self.config.filer_net_port),
        };

        let new_client = Arc::new(PowerFsNetClient::new(ClientConfig {
            addr: host,
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
        self.filer_clients
            .write()
            .await
            .insert(addr_str.to_string(), new_client.clone());
        Ok(new_client)
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

    /// Get the volume addrs list
    pub fn volume_addrs(&self) -> &[String] {
        &self.config.volume_addrs
    }

    /// Get the filer leader address as "host:port" string
    pub fn filer_leader_addr(&self) -> String {
        format!("{}:{}", self.config.filer_addr, self.config.filer_net_port)
    }

    /// Get the filer leader host
    pub fn filer_host(&self) -> &str {
        &self.config.filer_addr
    }

    /// Get the volume net port from config
    pub fn volume_port(&self) -> u16 {
        self.config.volume_net_port
    }
}
