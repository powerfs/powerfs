//! Client-side connection pool — unified connection management for FUSE clients.
//!
//! Replaces the scattered `DashMap<String, Arc<PowerFsNetClient>>` pattern
//! previously duplicated across `MetaShardClient`, `VolumeClient`, and
//! `MasterClient`.
//!
//! # Architecture
//!
//! ```text
//! FuseClientFacade
//!  ├─ MasterClient    ──┐
//!  ├─ MetaShardClient ──┼──► ClientConnPool ──► DashMap<addr, Arc<PowerFsNetClient>>
//!  └─ VolumeClient    ──┘         │
//!                                ├─ get_or_connect(addr)  → lazy connect + reuse
//!                                ├─ background health check (ping + reconnect)
//!                                └─ close_all()           → unified cleanup
//! ```
//!
//! # Key properties
//!
//! - **Connection reuse**: same `addr:port` → same `Arc<PowerFsNetClient>`
//! - **Lazy connect**: connection established on first `get_or_connect`
//! - **Auto-reconnect**: background task pings and reconnects dead connections
//! - **Notification handler**: optional, installed on every new connection
//!   and preserved across reconnects (stored inside `PowerFsNetClient`)

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use log::{debug, error, info, warn};
use tokio::time::interval;

use crate::client::{ClientConfig, NotificationHandler, PowerFsNetClient};
use crate::errors::NetResult;
use crate::protocol::{ ClientType, NetMessage, CHANNEL_DATA, CHANNEL_META};

/// Describes a server endpoint to connect to.
///
/// All FUSE-side connections to Master / Filer / Volume servers are described
/// by this struct. The `service_type` field is informational (used for
/// logging / metrics) — the wire protocol uses `ClientType::Fuse` for all
/// client→server connections regardless of the server's role.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct ServerEndpoint {
    /// Server address (IP or hostname), e.g. `"172.20.0.11"`.
    pub addr: String,
    /// Server port, e.g. `9334`.
    pub port: u16,
    /// What kind of server lives at this endpoint (for logging/metrics).
    pub service_type: ClientType,
}

impl ServerEndpoint {
    pub fn new(addr: impl Into<String>, port: u16, service_type: ClientType) -> Self {
        Self {
            addr: addr.into(),
            port,
            service_type,
        }
    }

    /// Master server endpoint.
    pub fn master(addr: impl Into<String>, port: u16) -> Self {
        Self::new(addr, port, ClientType::Master)
    }

    /// Filer server endpoint.
    pub fn filer(addr: impl Into<String>, port: u16) -> Self {
        Self::new(addr, port, ClientType::Filer)
    }

    /// Volume server endpoint.
    pub fn volume(addr: impl Into<String>, port: u16) -> Self {
        Self::new(addr, port, ClientType::Volume)
    }

    /// Format as `"addr:port"` (the DashMap key).
    pub fn addr_key(&self) -> String {
        format!("{}:{}", self.addr, self.port)
    }
}

impl std::fmt::Display for ServerEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{} ({:?})", self.addr, self.port, self.service_type)
    }
}

/// Build the DashMap key for a connection.
///
/// Format: `"addr:port#channel"` — the `#` separator avoids ambiguity with
/// IPv6 addresses (which contain `:`). The `channel` suffix distinguishes
/// the data TCP connection from the meta TCP connection for the same
/// `addr:port`, mirroring the kernel-side
/// `POWERFS_NET_SERVER_VOLUME` / `POWERFS_NET_SERVER_VOLUME_META` split.
fn make_pool_key(addr: &str, port: u16, channel: u8) -> String {
    format!("{}:{}#{}", addr, port, channel)
}

/// Human-readable label for a channel value (for logging).
fn channel_label(channel: u8) -> &'static str {
    if channel == CHANNEL_META {
        "meta"
    } else {
        "data"
    }
}

/// Configuration for the connection pool.
#[derive(Debug, Clone)]
pub struct ClientPoolConfig {
    /// Interval between health-check (ping) rounds.
    pub health_check_interval: Duration,
    /// Connect timeout for new connections.
    pub connect_timeout: Duration,
    /// Request timeout for individual requests.
    pub request_timeout: Duration,
    /// Max in-flight requests per connection.
    pub max_inflight_per_conn: u32,
    /// Max retries for connect.
    pub max_retries: u32,
    /// Retry delay between connect attempts.
    pub retry_delay: Duration,
}

impl Default for ClientPoolConfig {
    fn default() -> Self {
        Self {
            health_check_interval: Duration::from_secs(15),
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
            max_inflight_per_conn: 256,
            max_retries: 3,
            retry_delay: Duration::from_millis(100),
        }
    }
}

/// Wrapper to convert `Arc<dyn NotificationHandler>` into
/// `Box<dyn NotificationHandler>` so the same handler can be installed on
/// every new `PowerFsNetClient` created by the pool.
struct SharedNotificationHandler(Arc<dyn NotificationHandler + Send + Sync>);

impl NotificationHandler for SharedNotificationHandler {
    fn handle_notification(&self, msg: &NetMessage) {
        self.0.handle_notification(msg);
    }
}

/// Unified client-side connection pool.
///
/// Manages `PowerFsNetClient` instances keyed by `"addr:port"`. All FUSE-side
/// clients (MasterClient, MetaShardClient, VolumeClient) obtain connections
/// from a shared pool instance, eliminating duplicated connection maps and
/// health-check logic.
///
/// # Lifecycle
///
/// 1. `ClientConnPool::new(client_id, pool_config, handler)` — create pool
/// 2. `pool.start_health_check()` — spawn background ping + reconnect task
/// 3. `pool.get_or_connect(addr, port)` — get or create a connection
/// 4. `pool.close_all()` — disconnect everything (on unmount/shutdown)
pub struct ClientConnPool {
    /// Connections keyed by `"addr:port"`.
    connections: DashMap<String, Arc<PowerFsNetClient>>,
    /// Per-key mutexes to prevent concurrent connection creation for the same
    /// `addr:port:channel` key. Without this, multiple threads calling
    /// `get_or_connect` simultaneously (e.g. at FUSE startup when data/lease/
    /// mgmt processors all start at once) create duplicate TCP connections
    /// with the same `client_id`, causing server-side ConnRegistry collisions.
    per_key_locks: DashMap<String, Arc<tokio::sync::Mutex<()>>>,
    /// FUSE client ID (shared across all connections).
    client_id: u64,
    /// Pool configuration.
    config: ClientPoolConfig,
    /// Optional notification handler installed on every new connection.
    /// Behind a RwLock so it can be set after pool creation (the InvalidateHandler
    /// is created in fuse.rs after the pool is constructed).
    notification_handler: parking_lot::RwLock<Option<Arc<dyn NotificationHandler + Send + Sync>>>,
    /// Running flag for background health-check task.
    running: Arc<AtomicBool>,
}

impl ClientConnPool {
    /// Create a new connection pool.
    ///
    /// `notification_handler` is installed on every new connection and
    /// preserved across reconnects. Pass `None` if server-pushed
    /// notifications are not needed.
    pub fn new(
        client_id: u64,
        config: ClientPoolConfig,
        notification_handler: Option<Arc<dyn NotificationHandler + Send + Sync>>,
    ) -> Self {
        Self {
            connections: DashMap::new(),
            per_key_locks: DashMap::new(),
            client_id,
            config,
            notification_handler: parking_lot::RwLock::new(notification_handler),
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Get an existing connection or create a new one.
    ///
    /// If a connection to `addr:port` with the given `channel` already exists
    /// and is connected, it is reused. Otherwise a new `PowerFsNetClient` is
    /// created, connected, and stored in the pool.
    ///
    /// `channel` selects the physical TCP connection:
    /// - [`CHANNEL_DATA`] (0): data path (write/read needle, large frames)
    /// - [`CHANNEL_META`] (1): meta path (lease acquire/renew/release, small frames)
    ///
    /// Same `addr:port` with different `channel` values produce independent
    /// TCP connections, physically isolating large data frames from small
    /// lease requests. This mirrors the kernel-side
    /// `POWERFS_NET_SERVER_VOLUME` / `POWERFS_NET_SERVER_VOLUME_META` split.
    pub async fn get_or_connect(
        &self,
        addr: &str,
        port: u16,
        channel: u8,
    ) -> NetResult<Arc<PowerFsNetClient>> {
        let key = make_pool_key(addr, port, channel);

        // Fast path: reuse existing connected client.
        if let Some(entry) = self.connections.get(&key) {
            if entry.is_connected() {
                debug!(
                    "ClientConnPool: reuse existing connection {} (channel={})",
                    key,
                    channel_label(channel)
                );
                return Ok(entry.clone());
            }
            // Stale connection — drop the reference and create a new one below.
            debug!(
                "ClientConnPool: stale connection {} (channel={}), creating new",
                key,
                channel_label(channel)
            );
            drop(entry);
        }

        // Slow path: acquire per-key lock to prevent concurrent connection
        // creation. Without this, multiple threads (e.g. FUSE startup with
        // data/lease/mgmt processors) create duplicate TCP connections with
        // the same client_id, causing server-side ConnRegistry collisions.
        let key_lock = self
            .per_key_locks
            .entry(key.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let _guard = key_lock.lock().await;

        // Double-check after acquiring lock: another task may have created
        // the connection while we were waiting for the lock.
        if let Some(entry) = self.connections.get(&key) {
            if entry.is_connected() {
                debug!(
                    "ClientConnPool: reuse connection {} created by another task (channel={})",
                    key,
                    channel_label(channel)
                );
                return Ok(entry.clone());
            }
            drop(entry);
        }

        debug!(
            "ClientConnPool: slow path creating connection {} (channel={}, client_id={})",
            key,
            channel_label(channel),
            self.client_id
        );
        self.create_connection(addr, port, channel, &key).await
    }

    /// Get an existing connection or create a new one via `ServerEndpoint`.
    pub async fn get_or_connect_endpoint(
        &self,
        endpoint: &ServerEndpoint,
    ) -> NetResult<Arc<PowerFsNetClient>> {
        self.get_or_connect(&endpoint.addr, endpoint.port, CHANNEL_DATA)
            .await
    }

    /// Get an existing connection or create a new one from an `"addr:port"` string.
    ///
    /// Convenience method matching the existing `DashMap<String, ...>` usage
    /// pattern in MetaShardClient / VolumeClient. If the string lacks a port
    /// (no `:`), port 9334 is assumed.
    ///
    /// Uses [`CHANNEL_DATA`] (data path). For lease requests, use
    /// [`get_or_connect_addr_channel`] with [`CHANNEL_META`].
    pub async fn get_or_connect_addr(&self, addr_port: &str) -> NetResult<Arc<PowerFsNetClient>> {
        self.get_or_connect_addr_channel(addr_port, CHANNEL_DATA)
            .await
    }

    /// Same as [`get_or_connect_addr`] but allows selecting the channel.
    ///
    /// Use [`CHANNEL_META`] for lease requests (acquire/renew/release) to
    /// avoid being blocked by large data frames on the data connection.
    pub async fn get_or_connect_addr_channel(
        &self,
        addr_port: &str,
        channel: u8,
    ) -> NetResult<Arc<PowerFsNetClient>> {
        let (addr, port) = match addr_port.rsplit_once(':') {
            Some((h, p)) => (h, p.parse::<u16>().unwrap_or(9334)),
            None => (addr_port, 9334),
        };
        self.get_or_connect(addr, port, channel).await
    }

    /// Peek at an existing connection without creating a new one.
    ///
    /// Returns `None` if no connection exists or it is not connected.
    /// Useful for checking connection state without triggering a connect.
    ///
    /// Uses [`CHANNEL_DATA`]. For other channels use [`get_if_connected_channel`].
    pub fn get_if_connected(&self, addr_port: &str) -> Option<Arc<PowerFsNetClient>> {
        self.get_if_connected_channel(addr_port, CHANNEL_DATA)
    }

    /// Same as [`get_if_connected`] but allows selecting the channel.
    pub fn get_if_connected_channel(
        &self,
        addr_port: &str,
        channel: u8,
    ) -> Option<Arc<PowerFsNetClient>> {
        let (addr, port) = match addr_port.rsplit_once(':') {
            Some((h, p)) => (h, p.parse::<u16>().unwrap_or(9334)),
            None => (addr_port, 9334),
        };
        let key = make_pool_key(addr, port, channel);
        let entry = self.connections.get(&key)?;
        if entry.is_connected() {
            Some(entry.clone())
        } else {
            None
        }
    }

    /// Create, connect, and store a new `PowerFsNetClient`.
    async fn create_connection(
        &self,
        addr: &str,
        port: u16,
        channel: u8,
        key: &str,
    ) -> NetResult<Arc<PowerFsNetClient>> {
        debug!(
            "ClientConnPool: create_connection addr={}:{}, channel={}, client_id={}, key={}",
            addr, port, channel_label(channel), self.client_id, key
        );
        let config = ClientConfig {
            addr: addr.to_string(),
            port,
            client_id: self.client_id,
            client_type: ClientType::Fuse,
            channel,
            connect_timeout: self.config.connect_timeout,
            request_timeout: self.config.request_timeout,
            max_retries: self.config.max_retries,
            retry_delay: self.config.retry_delay,
            heartbeat_interval: self.config.health_check_interval,
            max_inflight_requests: self.config.max_inflight_per_conn,
        };

        let client = Arc::new(PowerFsNetClient::new(config));

        // Install notification handler if configured.
        if let Some(handler) = self.notification_handler.read().clone() {
            let wrapper = SharedNotificationHandler(handler);
            client.set_notification_handler(Box::new(wrapper));
        }

        client.connect().await?;

        // Store in pool (replacing any stale entry).
        self.connections.insert(key.to_string(), client.clone());

        info!(
            "ClientConnPool: connected to {} (channel={}, client_id={})",
            key,
            channel_label(channel),
            self.client_id
        );
        Ok(client)
    }

    /// Remove a connection from the pool (e.g. permanently failed server).
    ///
    /// Removes both data and meta connections for the given `addr:port`.
    pub async fn remove(&self, addr: &str, port: u16) {
        for channel in [CHANNEL_DATA, CHANNEL_META] {
            let key = make_pool_key(addr, port, channel);
            if let Some((_, client)) = self.connections.remove(&key) {
                info!(
                    "ClientConnPool: removed connection {} (channel={})",
                    key,
                    channel_label(channel)
                );
                let _ = client.disconnect().await;
            }
        }
    }

    /// Get a snapshot of all connections (for admin/monitoring).
    pub fn list_connections(&self) -> Vec<(String, Arc<PowerFsNetClient>)> {
        self.connections
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect()
    }

    /// Number of connections in the pool (connected or not).
    pub fn len(&self) -> usize {
        self.connections.len()
    }

    /// Whether the pool is empty.
    pub fn is_empty(&self) -> bool {
        self.connections.is_empty()
    }

    /// Install (or replace) the notification handler on the pool.
    ///
    /// The handler is installed on every new connection created after this
    /// call, and also retroactively applied to all existing connections in
    /// the pool.
    ///
    /// This allows the pool to be created first (e.g. inside
    /// `FuseClientFacade::build_from_config`) and the handler to be
    /// installed later (e.g. from `fuse.rs` after the metadata cache is
    /// initialized).
    pub fn set_notification_handler(&self, handler: Arc<dyn NotificationHandler + Send + Sync>) {
        // Update the stored handler so future connections pick it up.
        *self.notification_handler.write() = Some(handler.clone());

        // Retroactively apply to existing connections.
        for entry in self.connections.iter() {
            let wrapper = SharedNotificationHandler(handler.clone());
            entry.set_notification_handler(Box::new(wrapper));
        }
    }

    /// Start the background health-check task.
    ///
    /// Spawns a tokio task that periodically pings each connection and
    /// reconnects any that have dropped. Must be called from a tokio runtime
    /// context. Calling twice is a no-op. If no tokio runtime is active
    /// (e.g. in unit tests), the call silently skips spawning — the pool
    /// still functions but without background health checks.
    pub fn start_health_check(&self) {
        if self.running.swap(true, Ordering::SeqCst) {
            return; // already running
        }

        // Guard against calling outside a tokio runtime (e.g. unit tests).
        // Without this, `tokio::spawn` panics with "no reactor running".
        let handle = match tokio::runtime::Handle::try_current() {
            Ok(h) => h,
            Err(_) => {
                self.running.store(false, Ordering::SeqCst);
                log::debug!("ClientConnPool: start_health_check skipped (no tokio runtime active)");
                return;
            }
        };

        let connections = self.connections.clone();
        let interval_duration = self.config.health_check_interval;
        let running = self.running.clone();

        handle.spawn(async move {
            let mut ticker = interval(interval_duration);
            // Skip the first immediate tick.
            ticker.tick().await;

            info!(
                "ClientConnPool: health check started (interval={:?})",
                interval_duration
            );

            loop {
                if !running.load(Ordering::SeqCst) {
                    break;
                }
                ticker.tick().await;

                // Snapshot addresses to avoid holding DashMap locks during network I/O.
                let entries: Vec<(String, Arc<PowerFsNetClient>)> = connections
                    .iter()
                    .map(|e| (e.key().clone(), e.value().clone()))
                    .collect();

                for (addr_key, client) in entries {
                    if !running.load(Ordering::SeqCst) {
                        break;
                    }

                    if client.is_connected() {
                        // Health probe: ping.
                        match client.ping().await {
                            Ok(_) => {
                                debug!("ClientConnPool: ping ok for {}", addr_key);
                            }
                            Err(e) => {
                                warn!(
                                    "ClientConnPool: ping failed for {}, reconnecting: {:?}",
                                    addr_key, e
                                );
                                if let Err(e) = client.reconnect_internal().await {
                                    error!(
                                        "ClientConnPool: reconnect failed for {}: {:?}",
                                        addr_key, e
                                    );
                                }
                            }
                        }
                    } else {
                        // Not connected — try to reconnect.
                        info!(
                            "ClientConnPool: {} not connected, attempting reconnect",
                            addr_key
                        );
                        if let Err(e) = client.reconnect_internal().await {
                            error!("ClientConnPool: reconnect failed for {}: {:?}", addr_key, e);
                        }
                    }
                }
            }

            info!("ClientConnPool: health check stopped");
        });
    }

    /// Stop the background health-check task.
    pub fn stop_health_check(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// Disconnect all connections and clear the pool.
    ///
    /// This is async because `PowerFsNetClient::disconnect` is async (it
    /// aborts background tasks and clears pending requests). The caller is
    /// responsible for awaiting this before dropping the pool.
    pub async fn close_all(&self) {
        self.stop_health_check();

        let keys: Vec<String> = self.connections.iter().map(|e| e.key().clone()).collect();
        for key in keys {
            if let Some((_, client)) = self.connections.remove(&key) {
                let _ = client.disconnect().await;
                debug!("ClientConnPool: disconnected {}", key);
            }
        }
        info!(
            "ClientConnPool: all connections closed ({})",
            self.connections.len()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_endpoint_construction() {
        let ep = ServerEndpoint::master("172.20.0.11", 9334);
        assert_eq!(ep.addr, "172.20.0.11");
        assert_eq!(ep.port, 9334);
        assert_eq!(ep.service_type, ClientType::Master);
        assert_eq!(ep.addr_key(), "172.20.0.11:9334");

        let filer_ep = ServerEndpoint::filer("10.0.0.5", 9334);
        assert_eq!(filer_ep.service_type, ClientType::Filer);

        let vol_ep = ServerEndpoint::volume("10.0.0.21", 8901);
        assert_eq!(vol_ep.service_type, ClientType::Volume);
    }

    #[test]
    fn test_server_endpoint_equality_and_hash() {
        let a = ServerEndpoint::master("1.2.3.4", 1000);
        let b = ServerEndpoint::master("1.2.3.4", 1000);
        let c = ServerEndpoint::filer("1.2.3.4", 1000);

        assert_eq!(a, b);
        assert_ne!(a, c); // different service_type

        // Hash consistency
        let mut set = std::collections::HashSet::new();
        set.insert(a.clone());
        assert!(set.contains(&b));
        assert!(!set.contains(&c));
    }

    #[test]
    fn test_pool_config_default() {
        let cfg = ClientPoolConfig::default();
        assert_eq!(cfg.health_check_interval, Duration::from_secs(15));
        assert_eq!(cfg.max_inflight_per_conn, 256);
    }

    #[test]
    fn test_pool_creation() {
        let pool = ClientConnPool::new(42, ClientPoolConfig::default(), None);
        assert_eq!(pool.client_id, 42);
        assert!(pool.is_empty());
        assert_eq!(pool.len(), 0);
    }

    #[tokio::test]
    async fn test_pool_remove_nonexistent() {
        let pool = ClientConnPool::new(1, ClientPoolConfig::default(), None);
        // Should not panic on removing a nonexistent connection.
        pool.remove("1.2.3.4", 9999).await;
        assert!(pool.is_empty());
    }

    #[tokio::test]
    async fn test_pool_close_all_empty() {
        let pool = ClientConnPool::new(1, ClientPoolConfig::default(), None);
        pool.close_all().await;
        assert!(pool.is_empty());
    }

    #[tokio::test]
    async fn test_pool_close_all_on_drop() {
        let pool = ClientConnPool::new(1, ClientPoolConfig::default(), None);
        pool.close_all().await;
        drop(pool);
        // No panic — connections were properly closed before drop.
    }

    #[tokio::test]
    async fn test_pool_get_or_connect_failure() {
        // Connecting to an unreachable port should return an error, not panic.
        let pool = ClientConnPool::new(1, ClientPoolConfig::default(), None);
        let result = pool.get_or_connect("127.0.0.1", 1, CHANNEL_DATA).await;
        assert!(result.is_err());
        // The failed connection should not be stored in the pool.
        assert!(pool.is_empty());
    }

    #[test]
    fn test_shared_notification_handler() {
        use std::sync::atomic::{AtomicU32, Ordering};

        static COUNT: AtomicU32 = AtomicU32::new(0);

        struct CountingHandler;
        impl NotificationHandler for CountingHandler {
            fn handle_notification(&self, _msg: &NetMessage) {
                COUNT.fetch_add(1, Ordering::SeqCst);
            }
        }

        let arc_handler: Arc<dyn NotificationHandler + Send + Sync> = Arc::new(CountingHandler);
        let shared = SharedNotificationHandler(arc_handler);
        let msg = NetMessage::new(crate::protocol::FrameHeader::new(
            0,
            crate::protocol::FrameFlags::new(0),
            0,
            0,
        ));

        shared.handle_notification(&msg);
        shared.handle_notification(&msg);
        assert_eq!(COUNT.load(Ordering::SeqCst), 2);
    }
}
