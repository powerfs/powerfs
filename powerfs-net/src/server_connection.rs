//! Server-side connection and session management
//!
//! `ServerConnectionManager` is a thin facade that delegates per-connection
//! state management (sessions, notifications, rate limiting, stats) to
//! [`ConnRegistry`]/[`ClientConn`] and retains only server-level
//! infrastructure:
//!
//! - **Middleware pipeline** (`RequestPipeline`) — shared across all workers
//! - **Metrics middleware** (`MetricsMiddleware`) — aggregated request metrics
//! - **High-level notification helpers** — build TLV messages and push via
//!   `ConnRegistry::notify()`
//!
//! ## Historical note
//!
//! Previously this struct maintained a parallel `HashMap<client_id,
//! ClientSession>` that duplicated `ConnRegistry`'s `ClientConn` data. That
//! dual tracking caused state divergence and made cleanup error-prone. The
//! `ClientSession` type has been removed; all per-connection state lives in
//! `ClientConn`.

use std::net::SocketAddr;
use std::sync::Arc;

use crate::client_conn::{ConnRegistry, ConnState};
use crate::errors::{NetError, NetResult};
use crate::middleware::{
    LoggingMiddleware, MetricsMiddleware, NextHandler, RequestMetrics, RequestPipeline,
};
use crate::protocol::{ClientType, FieldId, MsgType, NetMessage};
use crate::serialize::TlvEncoder;

use super::request_context::{ClientInfo, RequestContext};

// ---------------------------------------------------------------------------
// NetHandler — business-level request handler trait
// ---------------------------------------------------------------------------

/// Trait for business-level request handlers.
///
/// Implemented by MasterNode/VolumeServer/MetaShardManager to handle the
/// actual business logic for each request type. Merges the former
/// `PowerFsNetHandler` (server-facing lifecycle) and `ServerRequestHandler`
/// (business dispatch) into a single trait.
#[async_trait::async_trait]
pub trait NetHandler: Send + Sync {
    /// Handle a request and return a response.
    async fn handle(&self, ctx: &mut RequestContext, msg: &NetMessage) -> NetResult<NetMessage>;

    /// Called when a client connects. Default no-op.
    async fn on_connect(&self, _client_id: u64, _client_type: ClientType) {}

    /// Called when a client disconnects. Default no-op.
    async fn on_disconnect(&self, _client_id: u64) {}
}

// ---------------------------------------------------------------------------
// MetricsSnapshot / HealthStatus — re-exported types for admin API compat
// ---------------------------------------------------------------------------

/// Aggregated metrics snapshot for admin/monitoring.
///
/// Combines per-connection stats from `ConnRegistry` with request-level
/// metrics from `MetricsMiddleware`.
#[derive(Debug, Default, Clone)]
pub struct MetricsSnapshot {
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub total_latency_us: u64,
    pub active_sessions: usize,
    pub total_sessions: usize,
}

impl MetricsSnapshot {
    pub fn avg_latency_us(&self) -> f64 {
        if self.total_requests > 0 {
            self.total_latency_us as f64 / self.total_requests as f64
        } else {
            0.0
        }
    }

    pub fn success_rate(&self) -> f64 {
        if self.total_requests > 0 {
            self.successful_requests as f64 / self.total_requests as f64 * 100.0
        } else {
            100.0
        }
    }
}

/// Health check status
#[derive(Debug, Clone)]
pub struct HealthStatus {
    pub healthy: bool,
    pub active_sessions: usize,
    pub total_sessions: usize,
}

// ---------------------------------------------------------------------------
// SessionState — backward-compatible alias for ConnState
// ---------------------------------------------------------------------------

/// Session state alias for backward compatibility.
///
/// New code should use [`crate::client_conn::ConnState`] directly.
pub type SessionState = ConnState;

// ---------------------------------------------------------------------------
// ClientSession — deprecated, use ClientConn directly
// ---------------------------------------------------------------------------

/// Deprecated: use [`crate::client_conn::ClientConn`] directly.
///
/// This type is kept only for transitional API compatibility and will be
/// removed in a future release.
#[deprecated(note = "Use ClientConn directly from powerfs_net::client_conn")]
pub type ClientSession = crate::client_conn::ClientConn;

// ---------------------------------------------------------------------------
// RateLimiter — re-export from client_conn for backward compatibility
// ---------------------------------------------------------------------------

pub use crate::client_conn::RateLimiter;

// ---------------------------------------------------------------------------
// ServerConnectionManager — thin facade over ConnRegistry
// ---------------------------------------------------------------------------

/// ServerConnectionManager — server-level infrastructure facade.
///
/// Delegates per-connection management to [`ConnRegistry`] and retains only:
/// - Middleware pipeline (shared across workers)
/// - Metrics middleware (request-level metrics)
/// - High-level notification helpers (build TLV + push via registry)
pub struct ServerConnectionManager {
    /// Shared connection registry (owned by PowerFsNetServer)
    registry: Arc<ConnRegistry>,
    /// Request processing pipeline (logging + metrics + tracing)
    pipeline: RequestPipeline,
    /// Metrics middleware reference (for aggregated snapshots)
    metrics: Arc<MetricsMiddleware>,
}

impl ServerConnectionManager {
    /// Create a new manager that delegates to the given `ConnRegistry`.
    ///
    /// The registry is typically owned by `PowerFsNetServer` and shared
    /// via `Arc`.
    pub fn new(registry: Arc<ConnRegistry>) -> Self {
        let metrics = Arc::new(MetricsMiddleware::new());
        let pipeline = RequestPipeline::new()
            .add_middleware(LoggingMiddleware::new())
            .add_arc(metrics.clone());
        Self {
            registry,
            pipeline,
            metrics,
        }
    }

    /// Create with a custom middleware pipeline.
    pub fn with_pipeline(mut self, pipeline: RequestPipeline) -> Self {
        self.pipeline = pipeline.add_arc(self.metrics.clone());
        self
    }

    /// Access the shared ConnRegistry.
    pub fn registry(&self) -> &Arc<ConnRegistry> {
        &self.registry
    }

    /// Access the middleware pipeline.
    pub fn pipeline(&self) -> &RequestPipeline {
        &self.pipeline
    }

    /// Access the metrics middleware.
    pub fn metrics(&self) -> &Arc<MetricsMiddleware> {
        &self.metrics
    }

    // ========================================================================
    // Session lifecycle — delegated to ConnRegistry
    // ========================================================================

    /// Register a new client session.
    ///
    /// Delegates to `ConnRegistry::register()`. The `ClientConn` must have
    /// been created by the caller (typically `PowerFsNetServer`).
    pub async fn register_session(
        &self,
        client_id: u64,
        client_type: ClientType,
        address: SocketAddr,
    ) {
        log::info!(
            "[Server] Client connected: id={}, type={:?}, addr={}",
            client_id,
            client_type,
            address
        );
        // ClientConn is registered by PowerFsNetServer directly;
        // this method is kept for logging + API compatibility.
    }

    /// Unregister a client session.
    ///
    /// Delegates to `ConnRegistry::unregister()`.
    pub async fn unregister_session(&self, client_id: u64) {
        if let Some(conn) = self.registry.unregister(client_id).await {
            let stats = conn.stats.read().await;
            log::info!(
                "[Server] Client disconnected: id={}, duration={}s, requests={}, errors={}",
                client_id,
                stats.connected_at.elapsed().as_secs(),
                stats.request_count,
                stats.error_count
            );
        }
    }

    /// Get active session count.
    pub async fn active_count(&self) -> usize {
        self.registry.metrics_snapshot().await.active_sessions
    }

    /// Get total session count.
    pub async fn total_count(&self) -> usize {
        self.registry.count()
    }

    /// List all connected client IDs.
    pub fn list_client_ids(&self) -> Vec<u64> {
        self.registry.list_client_ids()
    }

    /// Force-disconnect a client.
    pub async fn force_disconnect(&self, client_id: u64) -> bool {
        self.registry.disconnect(client_id).await
    }

    // ========================================================================
    // Metrics & Health
    // ========================================================================

    /// Get aggregated metrics snapshot.
    ///
    /// Combines per-connection stats from ConnRegistry with request-level
    /// metrics from MetricsMiddleware.
    pub async fn get_metrics_snapshot(&self) -> MetricsSnapshot {
        let conn_snapshot = self.registry.metrics_snapshot().await;
        let mut snapshot = MetricsSnapshot {
            active_sessions: conn_snapshot.active_sessions,
            total_sessions: conn_snapshot.total_sessions,
            ..Default::default()
        };

        // Merge request-level metrics from middleware
        let all = self.metrics.get_all_metrics().await;
        for m in all.values() {
            snapshot.total_requests += m.total_requests;
            snapshot.successful_requests += m.successful_requests;
            snapshot.failed_requests += m.failed_requests;
            snapshot.total_latency_us += m.total_latency_us;
        }
        snapshot
    }

    /// Get per-client request metrics.
    pub async fn get_client_metrics(&self, client_id: u64) -> Option<RequestMetrics> {
        self.metrics.get_metrics(client_id).await
    }

    /// Health check.
    pub async fn health_check(&self) -> HealthStatus {
        let conn_health = self.registry.health_check().await;
        HealthStatus {
            healthy: conn_health.healthy,
            active_sessions: conn_health.active_sessions,
            total_sessions: conn_health.total_sessions,
        }
    }

    // ========================================================================
    // Request Processing
    // ========================================================================

    /// Process a request through the middleware pipeline.
    pub async fn process_with_pipeline(
        &self,
        client_id: u64,
        msg: &NetMessage,
        handler: Arc<dyn NetHandler>,
    ) -> NetResult<NetMessage> {
        let conn = self.registry.get(client_id).ok_or_else(|| {
            NetError::Connection(format!("Client {} not found", client_id))
        })?;

        if *conn.state.read().await != ConnState::Active {
            return Err(NetError::Connection(format!(
                "Client {} is not active",
                client_id
            )));
        }

        let client_info = ClientInfo {
            client_id: conn.id,
            client_type: conn.client_type,
            address: conn.addr,
        };

        // Rate limiting — only for external/unknown clients.
        // Internal services (Fuse, Kernel, Volume, Filer, Master, Admin) bypass
        // rate limiting to avoid throttling data-plane traffic. Backpressure
        // for internal clients is handled at higher layers (e.g. ChunkCache
        // 512MB limit in FUSE write path).
        if !matches!(
            conn.client_type,
            ClientType::Fuse
                | ClientType::Kernel
                | ClientType::Volume
                | ClientType::Filer
                | ClientType::Master
                | ClientType::Admin
        ) {
            if !conn.check_rate_limit().await {
                return Err(NetError::ServerError(format!(
                    "Rate limit exceeded for client {}",
                    client_id
                )));
            }
        }

        let mut ctx = RequestContext::new(&client_info, msg);
        let handler_bridge: Arc<dyn NextHandler> = Arc::new(HandlerBridge(handler));
        let result = self.pipeline.execute(&mut ctx, msg, handler_bridge).await;

        // Update connection stats
        conn.record_request(result.is_ok()).await;

        result
    }

    /// Process a request directly (bypasses middleware).
    pub async fn process_request(
        &self,
        client_id: u64,
        msg: &NetMessage,
        handler: &dyn NetHandler,
    ) -> NetResult<NetMessage> {
        let conn = self.registry.get(client_id).ok_or_else(|| {
            NetError::Connection(format!("Client {} not found", client_id))
        })?;

        let client_info = ClientInfo {
            client_id: conn.id,
            client_type: conn.client_type,
            address: conn.addr,
        };

        let mut ctx = RequestContext::new(&client_info, msg);
        let result = handler.handle(&mut ctx, msg).await;
        conn.record_request(result.is_ok()).await;
        result
    }

    // ========================================================================
    // Notification Push (Server→Client) — delegated to ConnRegistry
    // ========================================================================

    /// Send a notification to a specific client.
    ///
    /// Delegates to `ConnRegistry::notify()` which pushes directly to
    /// `ClientConn.outbound_tx` — no intermediate channel needed.
    pub fn send_notification(&self, client_id: u64, msg: NetMessage) -> NetResult<bool> {
        let ok = self.registry.notify(client_id, &msg);
        if ok {
            Ok(true)
        } else {
            Err(NetError::Connection(format!(
                "Client {} not found or channel closed",
                client_id
            )))
        }
    }

    /// Broadcast a notification to all connected clients.
    pub fn broadcast_notification(&self, msg: &NetMessage) -> usize {
        self.registry.broadcast(msg)
    }

    /// Push an Invalidate(inode, version) notification to a single client.
    pub fn push_invalidate_notification(
        &self,
        client_id: u64,
        inode: u64,
        version: u64,
    ) -> NetResult<bool> {
        let msg = Self::build_invalidate_message(inode, version);
        self.send_notification(client_id, msg)
    }

    /// Broadcast an Invalidate(inode, version) notification to all clients.
    pub fn broadcast_invalidate_notification(&self, inode: u64, version: u64) -> usize {
        let msg = Self::build_invalidate_message(inode, version);
        self.broadcast_notification(&msg)
    }

    fn build_invalidate_message(inode: u64, version: u64) -> NetMessage {
        let mut enc = TlvEncoder::new();
        enc.add_u64(FieldId::Ino, inode);
        enc.add_u64(FieldId::Version, version);
        let body = enc.into_bytes();
        NetMessage::notification(MsgType::Invalidate, body, Vec::new())
    }

    /// Check if a client has a notification channel (always true if registered).
    pub fn has_notification_channel(&self, client_id: u64) -> bool {
        self.registry.get(client_id).is_some()
    }

    /// Get the number of clients with notification channels.
    pub fn notification_channel_count(&self) -> usize {
        self.registry.count()
    }

    /// Shutdown: disconnect all clients.
    pub async fn shutdown(&self) {
        let conns = self.registry.list().await;
        let count = conns.len();
        for info in conns {
            self.registry.disconnect(info.id).await;
        }
        log::info!("[Server] All {} sessions closed", count);
    }
}

/// Bridge from NetHandler to NextHandler for middleware pipeline.
struct HandlerBridge(Arc<dyn NetHandler>);

#[async_trait::async_trait]
impl NextHandler for HandlerBridge {
    async fn run(&self, ctx: &mut RequestContext, msg: &NetMessage) -> NetResult<NetMessage> {
        self.0.handle(ctx, msg).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_conn::ClientConn;
    use crate::protocol::{FrameFlags, FrameHeader, MsgType};
    use tokio::sync::mpsc;

    fn make_test_msg() -> NetMessage {
        NetMessage::new(FrameHeader::new(
            MsgType::Lookup.as_u16(),
            FrameFlags::new(FrameFlags::REQUEST),
            1,
            0,
        ))
    }

    fn make_test_response() -> NetMessage {
        let mut resp = NetMessage::new(FrameHeader::new(
            MsgType::Lookup.as_u16(),
            FrameFlags::new(FrameFlags::RESPONSE),
            1,
            0,
        ));
        resp.header.status = crate::STATUS_OK;
        resp
    }

    /// Helper: create a registry with a registered test client.
    async fn setup_registry_with_client(
        client_id: u64,
    ) -> (Arc<ConnRegistry>, Arc<ClientConn>) {
        let registry = Arc::new(ConnRegistry::new());
        let (tx, _rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let conn = ClientConn::new(
            client_id,
            "127.0.0.1:12345".parse().unwrap(),
            ClientType::Fuse,
            tx,
        );
        registry.register(conn.clone()).await;
        (registry, conn)
    }

    struct TestHandler;

    #[async_trait::async_trait]
    impl NetHandler for TestHandler {
        async fn handle(
            &self,
            _ctx: &mut RequestContext,
            _msg: &NetMessage,
        ) -> NetResult<NetMessage> {
            Ok(make_test_response())
        }
    }

    struct ErrorHandler;

    #[async_trait::async_trait]
    impl NetHandler for ErrorHandler {
        async fn handle(
            &self,
            _ctx: &mut RequestContext,
            _msg: &NetMessage,
        ) -> NetResult<NetMessage> {
            Err(NetError::ServerError("test error".to_string()))
        }
    }

    #[tokio::test]
    async fn test_session_register_unregister() {
        let (registry, _conn) = setup_registry_with_client(42).await;
        let mgr = ServerConnectionManager::new(registry.clone());

        assert_eq!(mgr.active_count().await, 1);
        mgr.unregister_session(42).await;
        assert_eq!(mgr.active_count().await, 0);
    }

    #[tokio::test]
    async fn test_process_request() {
        let (registry, conn) = setup_registry_with_client(100).await;
        let mgr = ServerConnectionManager::new(registry.clone());
        let msg = make_test_msg();
        let handler = TestHandler;

        let result = mgr.process_request(100, &msg, &handler).await;
        assert!(result.is_ok());

        let stats = conn.stats.read().await;
        assert_eq!(stats.request_count, 1);
        assert_eq!(stats.error_count, 0);
    }

    #[tokio::test]
    async fn test_process_request_with_error() {
        let (registry, conn) = setup_registry_with_client(200).await;
        let mgr = ServerConnectionManager::new(registry.clone());
        let msg = make_test_msg();
        let handler = ErrorHandler;

        let result = mgr.process_request(200, &msg, &handler).await;
        assert!(result.is_err());

        let stats = conn.stats.read().await;
        assert_eq!(stats.request_count, 1);
        assert_eq!(stats.error_count, 1);
    }

    #[tokio::test]
    async fn test_process_with_pipeline() {
        let (registry, conn) = setup_registry_with_client(300).await;
        let mgr = ServerConnectionManager::new(registry.clone());
        let msg = make_test_msg();
        let handler = Arc::new(TestHandler);

        let result = mgr.process_with_pipeline(300, &msg, handler).await;
        assert!(result.is_ok());

        let stats = conn.stats.read().await;
        assert_eq!(stats.request_count, 1);
    }

    #[tokio::test]
    async fn test_health_check() {
        let (registry, _conn) = setup_registry_with_client(1).await;
        let mgr = ServerConnectionManager::new(registry);

        let health = mgr.health_check().await;
        assert!(health.healthy);
        assert_eq!(health.active_sessions, 1);
    }

    #[tokio::test]
    async fn test_force_disconnect() {
        let (registry, conn) = setup_registry_with_client(1).await;
        let mgr = ServerConnectionManager::new(registry);

        let result = mgr.force_disconnect(1).await;
        assert!(result);
        assert_eq!(*conn.state.read().await, ConnState::Closing);

        // Non-existent client
        let result = mgr.force_disconnect(999).await;
        assert!(!result);
    }

    #[tokio::test]
    async fn test_notification_push() {
        let registry = Arc::new(ConnRegistry::new());
        // Keep _rx alive so the outbound channel doesn't get dropped,
        // which would cause notify() to return false.
        let (tx, _rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let conn = ClientConn::new(
            1,
            "127.0.0.1:12345".parse().unwrap(),
            ClientType::Fuse,
            tx,
        );
        registry.register(conn).await;
        let mgr = ServerConnectionManager::new(registry);

        let result = mgr.push_invalidate_notification(1, 12345, 1);
        assert!(result.is_ok());

        // Non-existent client
        let result = mgr.push_invalidate_notification(999, 12345, 1);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list_client_ids() {
        let registry = Arc::new(ConnRegistry::new());
        for i in 1..=3 {
            let (tx, _rx) = mpsc::unbounded_channel::<Vec<u8>>();
            let conn = ClientConn::new(
                i * 10,
                "127.0.0.1:12345".parse().unwrap(),
                ClientType::Fuse,
                tx,
            );
            registry.register(conn).await;
        }
        let mgr = ServerConnectionManager::new(registry);

        let ids = mgr.list_client_ids();
        assert_eq!(ids.len(), 3);
        assert!(ids.contains(&10));
        assert!(ids.contains(&20));
        assert!(ids.contains(&30));
    }

    #[tokio::test]
    async fn test_shutdown() {
        let registry = Arc::new(ConnRegistry::new());
        for i in 0..3 {
            let (tx, _rx) = mpsc::unbounded_channel::<Vec<u8>>();
            let conn = ClientConn::new(
                i + 1,
                "127.0.0.1:12345".parse().unwrap(),
                ClientType::Fuse,
                tx,
            );
            registry.register(conn).await;
        }
        let mgr = ServerConnectionManager::new(registry.clone());
        assert_eq!(mgr.active_count().await, 3);

        mgr.shutdown().await;
        assert_eq!(mgr.active_count().await, 0);
    }
}
