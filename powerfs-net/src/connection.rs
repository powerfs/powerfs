//! Connection management for powerfs-net
//!
//! Provides connection pooling, reconnection, and heartbeat management.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use log::{error, info, warn};
use tokio::sync::RwLock;
use tokio::time::interval;

use crate::client::{ClientConfig, PowerFsNetClient};
use crate::errors::NetResult;

/// Connection pool configuration
#[derive(Debug, Clone)]
pub struct PoolConfig {
    pub max_connections: usize,
    pub idle_timeout: Duration,
    pub reconnect_interval: Duration,
    pub heartbeat_interval: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 4,
            idle_timeout: Duration::from_secs(300),
            reconnect_interval: Duration::from_secs(5),
            heartbeat_interval: Duration::from_secs(30),
        }
    }
}

/// Connection manager with pooling and auto-reconnect
pub struct ConnectionManager {
    base_config: ClientConfig,
    pool_config: PoolConfig,
    clients: Arc<RwLock<Vec<Arc<PowerFsNetClient>>>>,
    next_client_id: AtomicU64,
    running: Arc<parking_lot::Mutex<bool>>,
}

impl ConnectionManager {
    pub fn new(base_config: ClientConfig, pool_config: PoolConfig) -> Self {
        Self {
            base_config,
            pool_config,
            clients: Arc::new(RwLock::new(Vec::new())),
            next_client_id: AtomicU64::new(1),
            running: Arc::new(parking_lot::Mutex::new(false)),
        }
    }

    /// Create and connect a new client
    pub async fn create_connection(&self, addr: Option<&str>) -> NetResult<Arc<PowerFsNetClient>> {
        let client_id = self.next_client_id.fetch_add(1, Ordering::Relaxed);

        let mut config = self.base_config.clone();
        config.client_id = client_id;
        if let Some(a) = addr {
            if let Some((host, port)) = a.split_once(':') {
                config.addr = host.to_string();
                if let Ok(p) = port.parse::<u16>() {
                    config.port = p;
                }
            }
        }

        let client = Arc::new(PowerFsNetClient::new(config));
        client.connect().await?;

        self.clients.write().await.push(client.clone());
        info!(
            "Created connection #{} to {}:{}",
            client_id, client.config.addr, client.config.port
        );

        Ok(client)
    }

    /// Get a connection from the pool (or create one if pool is empty)
    pub async fn get_connection(&self) -> NetResult<Arc<PowerFsNetClient>> {
        let clients = self.clients.read().await;

        // Find first connected client
        for client in clients.iter() {
            if client.is_connected() {
                return Ok(client.clone());
            }
        }
        drop(clients);

        // Create new connection
        self.create_connection(None).await
    }

    /// Remove a connection from the pool
    pub async fn remove_connection(&self, client: &Arc<PowerFsNetClient>) {
        let mut clients = self.clients.write().await;
        clients.retain(|c| !Arc::ptr_eq(c, client));
        info!("Removed connection from pool");
    }

    /// Get number of active connections
    pub async fn active_connections(&self) -> usize {
        self.clients.read().await.len()
    }

    /// Start background heartbeat and cleanup tasks
    pub async fn start_background_tasks(&self) {
        *self.running.lock() = true;
        let clients = self.clients.clone();
        let pool_config = self.pool_config.clone();
        let running = self.running.clone();

        // Heartbeat task
        tokio::spawn(async move {
            let mut interval = interval(pool_config.heartbeat_interval);
            loop {
                if !*running.lock() {
                    break;
                }
                interval.tick().await;

                let clients = clients.read().await;
                for client in clients.iter() {
                    if client.is_connected() {
                        if let Err(e) = client.ping().await {
                            warn!("Heartbeat failed: {:?}", e);
                        }
                    }
                }
            }
        });

        // Reconnect task
        let clients = self.clients.clone();
        let pool_config = self.pool_config.clone();
        let running = self.running.clone();
        tokio::spawn(async move {
            let mut interval = interval(pool_config.reconnect_interval);
            loop {
                if !*running.lock() {
                    break;
                }
                interval.tick().await;

                // Remove disconnected clients and try to reconnect
                let clients_guard = clients.write().await;
                let disconnected: Vec<Arc<PowerFsNetClient>> = clients_guard
                    .iter()
                    .filter(|c| !c.is_connected())
                    .cloned()
                    .collect();

                for client in disconnected {
                    info!("Reconnecting client...");
                    if let Err(e) = client.connect().await {
                        error!("Reconnect failed: {:?}", e);
                    }
                }

                // Remove clients that failed to reconnect after multiple attempts
                // For simplicity, we'll keep them and let reconnect handle it
            }
        });
    }

    /// Stop background tasks
    pub fn stop(&self) {
        *self.running.lock() = false;
    }

    /// Close all connections
    pub async fn close_all(&self) {
        self.stop();
        let clients = self.clients.read().await;
        for client in clients.iter() {
            let _ = client.disconnect().await;
        }
        info!("All connections closed");
    }
}

/// Multi-address connection manager (for Master + Volume)
pub struct MultiAddrManager {
    master_manager: ConnectionManager,
    volume_manager: ConnectionManager,
}

impl MultiAddrManager {
    pub fn new(
        master_config: ClientConfig,
        volume_config: ClientConfig,
        pool_config: PoolConfig,
    ) -> Self {
        Self {
            master_manager: ConnectionManager::new(master_config, pool_config.clone()),
            volume_manager: ConnectionManager::new(volume_config, pool_config),
        }
    }

    pub async fn connect_master(&self) -> NetResult<Arc<PowerFsNetClient>> {
        self.master_manager.create_connection(None).await
    }

    pub async fn connect_volume(&self) -> NetResult<Arc<PowerFsNetClient>> {
        self.volume_manager.create_connection(None).await
    }

    pub async fn get_master(&self) -> NetResult<Arc<PowerFsNetClient>> {
        self.master_manager.get_connection().await
    }

    pub async fn get_volume(&self) -> NetResult<Arc<PowerFsNetClient>> {
        self.volume_manager.get_connection().await
    }

    pub async fn close_all(&self) {
        self.master_manager.close_all().await;
        self.volume_manager.close_all().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::ClientConfig;

    #[test]
    fn test_pool_config_default() {
        let config = PoolConfig::default();
        assert_eq!(config.max_connections, 4);
        assert_eq!(config.heartbeat_interval, Duration::from_secs(30));
    }

    #[test]
    fn test_connection_manager_creation() {
        let _manager = ConnectionManager::new(ClientConfig::default(), PoolConfig::default());
    }
}
