//! Server-side connection and session management
//!
//! Provides `ClientSession` for per-connection state tracking and
//! `ServerConnectionManager` for managing multiple client sessions
//! with automatic cleanup, request pipeline, metrics, and notification push.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{mpsc, RwLock};

use crate::errors::{NetError, NetResult};
use crate::middleware::{
    LoggingMiddleware, MetricsMiddleware, NextHandler, RequestMetrics, RequestPipeline,
};
use crate::protocol::{ClientType, FieldId, MsgType, NetMessage};
use crate::serialize::TlvEncoder;

use super::request_context::{ClientInfo, RequestContext};

/// Simple token bucket rate limiter for per-client rate limiting
#[derive(Debug, Clone)]
pub struct RateLimiter {
    max_tokens: u64,
    refill_rate: f64,
    tokens: f64,
    last_refill: Instant,
}

impl RateLimiter {
    pub fn new(max_tokens: u64, refill_rate_per_sec: f64) -> Self {
        Self {
            max_tokens,
            refill_rate: refill_rate_per_sec,
            tokens: max_tokens as f64,
            last_refill: Instant::now(),
        }
    }

    /// Try to consume one token. Returns true if allowed.
    pub fn try_acquire(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill);
        let refill = elapsed.as_secs_f64() * self.refill_rate;
        self.tokens = (self.tokens + refill).min(self.max_tokens as f64);
        self.last_refill = now;

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    pub fn available_tokens(&self) -> u64 {
        self.tokens as u64
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        // 1000 tokens max, 100 tokens/sec refill (10 req/s sustained)
        Self::new(1000, 100.0)
    }
}

/// State of a client session
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionState {
    Active,
    Suspended,
    Closing,
    Closed,
}

/// Per-connection session tracking
#[derive(Debug, Clone)]
pub struct ClientSession {
    pub client_id: u64,
    pub client_type: ClientType,
    pub address: SocketAddr,
    pub state: SessionState,
    pub connected_at: Instant,
    pub last_activity: Instant,
    pub request_count: u64,
    pub error_count: u64,
    pub rate_limiter: RateLimiter,
}

impl ClientSession {
    pub fn new(client_id: u64, client_type: ClientType, address: SocketAddr) -> Self {
        let now = Instant::now();
        Self {
            client_id,
            client_type,
            address,
            state: SessionState::Active,
            connected_at: now,
            last_activity: now,
            request_count: 0,
            error_count: 0,
            rate_limiter: RateLimiter::default(),
        }
    }

    pub fn with_rate_limiter(
        client_id: u64,
        client_type: ClientType,
        address: SocketAddr,
        rate_limiter: RateLimiter,
    ) -> Self {
        let mut session = Self::new(client_id, client_type, address);
        session.rate_limiter = rate_limiter;
        session
    }

    /// Check if the client is within the rate limit
    pub fn check_rate_limit(&mut self) -> bool {
        self.rate_limiter.try_acquire()
    }

    pub fn available_rate_tokens(&self) -> u64 {
        self.rate_limiter.available_tokens()
    }

    pub fn duration_secs(&self) -> u64 {
        self.connected_at.elapsed().as_secs()
    }

    pub fn idle_secs(&self) -> u64 {
        self.last_activity.elapsed().as_secs()
    }

    pub fn update_activity(&mut self) {
        self.last_activity = Instant::now();
        self.request_count += 1;
    }

    pub fn record_error(&mut self) {
        self.error_count += 1;
    }

    pub fn set_state(&mut self, state: SessionState) {
        self.state = state;
    }

    pub fn client_info(&self) -> ClientInfo {
        ClientInfo {
            client_id: self.client_id,
            client_type: self.client_type,
            address: self.address,
        }
    }
}

/// Trait for business-level request handlers
///
/// Unified handler trait for processing net requests.
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

/// Aggregated metrics snapshot for admin/monitoring
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

/// ServerConnectionManager - manages client sessions, request processing, and notification push
pub struct ServerConnectionManager {
    sessions: RwLock<HashMap<u64, ClientSession>>,
    pipeline: RequestPipeline,
    metrics: Arc<MetricsMiddleware>,
    /// Notification channels for each client (client_id -> sender)
    notification_channels: RwLock<HashMap<u64, mpsc::Sender<NetMessage>>>,
}

/// Default channel size for notification queue per client
pub const DEFAULT_NOTIFICATION_CHANNEL_SIZE: usize = 64;

impl ServerConnectionManager {
    pub fn new() -> Self {
        let metrics = Arc::new(MetricsMiddleware::new());
        let pipeline = RequestPipeline::new()
            .add_middleware(LoggingMiddleware::new())
            .add_arc(metrics.clone());
        Self {
            sessions: RwLock::new(HashMap::new()),
            pipeline,
            metrics,
            notification_channels: RwLock::new(HashMap::new()),
        }
    }

    pub fn with_pipeline(mut self, pipeline: RequestPipeline) -> Self {
        // Ensure the manager's metrics middleware is always part of the pipeline
        // so that get_metrics_snapshot() reflects the actual request counts.
        self.pipeline = pipeline.add_arc(self.metrics.clone());
        self
    }

    pub fn pipeline(&self) -> &RequestPipeline {
        &self.pipeline
    }

    pub fn metrics(&self) -> &Arc<MetricsMiddleware> {
        &self.metrics
    }

    /// Register a new client connection with the given client_id
    pub async fn register_session(
        &self,
        client_id: u64,
        client_type: ClientType,
        address: SocketAddr,
    ) {
        let session = ClientSession::new(client_id, client_type, address);
        let mut sessions = self.sessions.write().await;
        sessions.insert(client_id, session);
        drop(sessions);

        log::info!(
            "[Server] Client connected: id={}, type={:?}, addr={}",
            client_id,
            client_type,
            address
        );
    }

    /// Remove a client session and its notification channel
    pub async fn unregister_session(&self, client_id: u64) {
        // Remove notification channel first
        {
            let mut channels = self.notification_channels.write().await;
            if let Some(tx) = channels.remove(&client_id) {
                // Drop the sender, which will signal the receiver to close
                drop(tx);
                log::debug!(
                    "[Server] Removed notification channel for client {}",
                    client_id
                );
            }
        }

        let mut sessions = self.sessions.write().await;
        if let Some(session) = sessions.remove(&client_id) {
            log::info!(
                "[Server] Client disconnected: id={}, duration={}s, requests={}, errors={}",
                client_id,
                session.duration_secs(),
                session.request_count,
                session.error_count
            );
        }
    }

    pub async fn get_active_sessions(&self) -> Vec<ClientSession> {
        let sessions = self.sessions.read().await;
        sessions
            .values()
            .filter(|s| s.state == SessionState::Active)
            .cloned()
            .collect()
    }

    pub async fn get_session(&self, client_id: u64) -> Option<ClientSession> {
        let sessions = self.sessions.read().await;
        sessions.get(&client_id).cloned()
    }

    pub async fn active_count(&self) -> usize {
        let sessions = self.sessions.read().await;
        sessions
            .values()
            .filter(|s| s.state == SessionState::Active)
            .count()
    }

    pub async fn total_count(&self) -> usize {
        self.sessions.read().await.len()
    }

    /// Get aggregated metrics snapshot across all clients
    pub async fn get_metrics_snapshot(&self) -> MetricsSnapshot {
        let all = self.metrics.get_all_metrics().await;
        let mut snapshot = MetricsSnapshot::default();
        for m in all.values() {
            snapshot.total_requests += m.total_requests;
            snapshot.successful_requests += m.successful_requests;
            snapshot.failed_requests += m.failed_requests;
            snapshot.total_latency_us += m.total_latency_us;
        }
        let active = self.sessions.read().await;
        snapshot.active_sessions = active
            .values()
            .filter(|s| s.state == SessionState::Active)
            .count();
        snapshot.total_sessions = active.len();
        drop(active);
        snapshot
    }

    /// Get per-client metrics
    pub async fn get_client_metrics(&self, client_id: u64) -> Option<RequestMetrics> {
        self.metrics.get_metrics(client_id).await
    }

    /// Health check for monitoring systems
    pub async fn health_check(&self) -> HealthStatus {
        let sessions = self.sessions.read().await;
        let active = sessions
            .values()
            .filter(|s| s.state == SessionState::Active)
            .count();
        let total = sessions.len();
        drop(sessions);
        HealthStatus {
            healthy: true,
            active_sessions: active,
            total_sessions: total,
        }
    }

    /// List all connected client IDs
    pub async fn list_client_ids(&self) -> Vec<u64> {
        let sessions = self.sessions.read().await;
        sessions.keys().copied().collect()
    }

    /// Force-disconnect a client session
    pub async fn force_disconnect(&self, client_id: u64) -> bool {
        let mut sessions = self.sessions.write().await;
        if let Some(session) = sessions.get_mut(&client_id) {
            session.set_state(SessionState::Closed);
            drop(sessions);
            self.unregister_session(client_id).await;
            true
        } else {
            false
        }
    }

    /// Process a request directly (bypasses middleware, for simple cases)
    pub async fn process_request(
        &self,
        client_id: u64,
        msg: &NetMessage,
        handler: &dyn NetHandler,
    ) -> NetResult<NetMessage> {
        let session_info = self.get_client_info(client_id).await?;
        self.touch_session(client_id).await;

        let mut ctx = RequestContext::new(&session_info, msg);
        let result = handler.handle(&mut ctx, msg).await;

        if result.is_err() {
            self.record_session_error(client_id).await;
        }

        result
    }

    /// Process a request through the middleware pipeline
    pub async fn process_with_pipeline(
        &self,
        client_id: u64,
        msg: &NetMessage,
        handler: Arc<dyn NetHandler>,
    ) -> NetResult<NetMessage> {
        let session_info = self.get_client_info(client_id).await?;
        self.touch_session(client_id).await;

        let mut ctx = RequestContext::new(&session_info, msg);
        let handler_arc: Arc<dyn NextHandler> = Arc::new(HandlerBridge(handler));

        let result = self.pipeline.execute(&mut ctx, msg, handler_arc).await;

        if result.is_err() {
            self.record_session_error(client_id).await;
        }

        result
    }

    async fn get_client_info(&self, client_id: u64) -> NetResult<ClientInfo> {
        let sessions = self.sessions.read().await;
        let session = sessions
            .get(&client_id)
            .ok_or_else(|| NetError::Connection(format!("Session {} not found", client_id)))?;
        if session.state != SessionState::Active {
            return Err(NetError::Connection(format!(
                "Session {} is not active (state={:?})",
                client_id, session.state
            )));
        }
        Ok(session.client_info())
    }

    async fn touch_session(&self, client_id: u64) {
        let mut sessions = self.sessions.write().await;
        if let Some(session) = sessions.get_mut(&client_id) {
            session.update_activity();
        }
    }

    async fn record_session_error(&self, client_id: u64) {
        let mut sessions = self.sessions.write().await;
        if let Some(session) = sessions.get_mut(&client_id) {
            session.record_error();
        }
    }

    // ========================================================================
    // Notification Push Support (Server→Client)
    // ========================================================================

    /// Register a notification channel for a client
    ///
    /// This is called by the server when a client connection is established.
    /// The receiver end of the channel should be polled in the connection's
    /// message loop to send notifications to the client.
    pub async fn register_notification_channel(
        &self,
        client_id: u64,
    ) -> mpsc::Receiver<NetMessage> {
        let (tx, rx) = mpsc::channel(DEFAULT_NOTIFICATION_CHANNEL_SIZE);
        let mut channels = self.notification_channels.write().await;
        channels.insert(client_id, tx);
        log::debug!(
            "[Server] Registered notification channel for client {}",
            client_id
        );
        rx
    }

    /// Send a notification message to a specific client
    ///
    /// Returns Ok(true) if the notification was queued successfully.
    /// Returns Ok(false) if the client is not connected or channel is full.
    /// Returns Err if the client doesn't exist.
    pub async fn send_notification(&self, client_id: u64, msg: NetMessage) -> NetResult<bool> {
        let msg_type = msg.msg_type();
        let channels = self.notification_channels.read().await;
        if let Some(tx) = channels.get(&client_id) {
            match tx.try_send(msg) {
                Ok(_) => {
                    log::debug!(
                        "[Server] Queued notification for client {} type={:?}",
                        client_id,
                        msg_type
                    );
                    Ok(true)
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    log::warn!(
                        "[Server] Notification channel full for client {}",
                        client_id
                    );
                    Ok(false)
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    log::warn!(
                        "[Server] Notification channel closed for client {}",
                        client_id
                    );
                    Err(NetError::Connection(format!(
                        "Client {} notification channel closed",
                        client_id
                    )))
                }
            }
        } else {
            log::debug!("[Server] No notification channel for client {}", client_id);
            Err(NetError::Connection(format!(
                "Client {} not found",
                client_id
            )))
        }
    }

    /// Send a notification to all connected clients
    ///
    /// Returns the number of clients that received the notification.
    pub async fn broadcast_notification(&self, msg: &NetMessage) -> usize {
        let channels = self.notification_channels.read().await;
        let mut success_count = 0;
        for (client_id, tx) in channels.iter() {
            if tx.try_send(msg.clone()).is_ok() {
                success_count += 1;
            } else {
                log::debug!(
                    "[Server] Failed to queue notification for client {}",
                    client_id
                );
            }
        }
        success_count
    }

    // ========================================================================
    // High-level typed notification helpers
    //
    // These build the NetMessage internally so upper-layer services (e.g.
    // InodeNotifier) do not need to depend on `protocol::{FrameHeader,
    // FrameFlags}`, `serialize::TlvEncoder`, or `FieldId` directly.
    // ========================================================================

    /// Push an Invalidate(inode, version) notification to a single client.
    ///
    /// Returns `Ok(true)` if queued, `Ok(false)` if the channel is full,
    /// `Err` if the client has no notification channel.
    pub async fn push_invalidate_notification(
        &self,
        client_id: u64,
        inode: u64,
        version: u64,
    ) -> NetResult<bool> {
        let msg = Self::build_invalidate_message(inode, version);
        self.send_notification(client_id, msg).await
    }

    /// Broadcast an Invalidate(inode, version) notification to all clients.
    ///
    /// Returns the number of clients that received the notification.
    pub async fn broadcast_invalidate_notification(&self, inode: u64, version: u64) -> usize {
        let msg = Self::build_invalidate_message(inode, version);
        self.broadcast_notification(&msg).await
    }

    /// Build an Invalidate notification message (shared by push + broadcast).
    fn build_invalidate_message(inode: u64, version: u64) -> NetMessage {
        let mut enc = TlvEncoder::new();
        enc.add_u64(FieldId::Ino, inode);
        enc.add_u64(FieldId::Version, version);
        let body = enc.into_bytes();
        NetMessage::notification(MsgType::Invalidate, body, Vec::new())
    }

    /// Check if a client has a notification channel registered
    pub async fn has_notification_channel(&self, client_id: u64) -> bool {
        let channels = self.notification_channels.read().await;
        channels.contains_key(&client_id)
    }

    /// Get the number of clients with notification channels
    pub async fn notification_channel_count(&self) -> usize {
        self.notification_channels.read().await.len()
    }

    pub async fn shutdown(&self) {
        // Clear notification channels
        {
            let mut channels = self.notification_channels.write().await;
            channels.clear();
        }

        let mut sessions = self.sessions.write().await;
        let count = sessions.len();
        for session in sessions.values_mut() {
            session.set_state(SessionState::Closed);
        }
        sessions.clear();
        log::info!("[Server] All {} sessions closed", count);
    }
}

impl Default for ServerConnectionManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Bridge from NetHandler to NextHandler for middleware pipeline
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
    use crate::protocol::{FrameFlags, FrameHeader, MsgType};

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
        let mgr = ServerConnectionManager::new();
        let addr: SocketAddr = "127.0.0.1:12345".parse().unwrap();
        let id: u64 = 42;

        mgr.register_session(id, ClientType::Fuse, addr).await;
        assert_eq!(mgr.active_count().await, 1);

        let session = mgr.get_session(id).await.unwrap();
        assert_eq!(session.client_id, id);
        assert_eq!(session.client_type, ClientType::Fuse);
        assert_eq!(session.state, SessionState::Active);

        mgr.unregister_session(id).await;
        assert_eq!(mgr.active_count().await, 0);
        assert!(mgr.get_session(id).await.is_none());
    }

    #[tokio::test]
    async fn test_session_multiple_clients() {
        let mgr = ServerConnectionManager::new();

        for i in 0..5 {
            let addr: SocketAddr = format!("127.0.0.1:{}", 12345 + i).parse().unwrap();
            mgr.register_session(i + 1, ClientType::Fuse, addr).await;
        }

        assert_eq!(mgr.active_count().await, 5);
        assert_eq!(mgr.total_count().await, 5);
    }

    #[tokio::test]
    async fn test_process_request() {
        let mgr = ServerConnectionManager::new();
        let addr: SocketAddr = "127.0.0.1:12345".parse().unwrap();
        let client_id: u64 = 100;
        mgr.register_session(client_id, ClientType::Fuse, addr)
            .await;
        let msg = make_test_msg();
        let handler = TestHandler;

        let result = mgr.process_request(client_id, &msg, &handler).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_ok());

        let session = mgr.get_session(client_id).await.unwrap();
        assert_eq!(session.request_count, 1);
        assert_eq!(session.error_count, 0);
    }

    #[tokio::test]
    async fn test_process_request_with_error() {
        let mgr = ServerConnectionManager::new();
        let addr: SocketAddr = "127.0.0.1:12345".parse().unwrap();
        let client_id: u64 = 200;
        mgr.register_session(client_id, ClientType::Fuse, addr)
            .await;
        let msg = make_test_msg();
        let handler = ErrorHandler;

        let result = mgr.process_request(client_id, &msg, &handler).await;
        assert!(result.is_err());

        let session = mgr.get_session(client_id).await.unwrap();
        assert_eq!(session.request_count, 1);
        assert_eq!(session.error_count, 1);
    }

    #[tokio::test]
    async fn test_process_request_session_not_found() {
        let mgr = ServerConnectionManager::new();
        let msg = make_test_msg();
        let handler = TestHandler;

        let result = mgr.process_request(99999, &msg, &handler).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_process_with_pipeline() {
        let mgr = ServerConnectionManager::new();
        let addr: SocketAddr = "127.0.0.1:12345".parse().unwrap();
        let client_id: u64 = 300;
        mgr.register_session(client_id, ClientType::Fuse, addr)
            .await;
        let msg = make_test_msg();
        let handler = Arc::new(TestHandler);

        let result = mgr.process_with_pipeline(client_id, &msg, handler).await;
        assert!(result.is_ok());
        let resp = result.unwrap();
        assert_eq!(resp.header.status, crate::STATUS_OK);

        let session = mgr.get_session(client_id).await.unwrap();
        assert_eq!(session.request_count, 1);
    }

    #[tokio::test]
    async fn test_shutdown() {
        let mgr = ServerConnectionManager::new();
        for i in 0..3 {
            let addr: SocketAddr = format!("127.0.0.1:{}", 12345 + i).parse().unwrap();
            mgr.register_session(i + 1, ClientType::Fuse, addr).await;
        }
        assert_eq!(mgr.active_count().await, 3);
        mgr.shutdown().await;
        assert_eq!(mgr.active_count().await, 0);
        assert_eq!(mgr.total_count().await, 0);
    }

    #[tokio::test]
    async fn test_metrics_snapshot() {
        let mgr = ServerConnectionManager::new();
        let addr: SocketAddr = "127.0.0.1:12345".parse().unwrap();

        mgr.register_session(1, ClientType::Fuse, addr).await;
        mgr.register_session(2, ClientType::Admin, addr).await;

        // Process some requests through the pipeline
        for _ in 0..5 {
            let msg = make_test_msg();
            let handler = Arc::new(TestHandler);
            let _ = mgr.process_with_pipeline(1, &msg, handler).await;
        }

        // Process one failing request
        let msg = make_test_msg();
        let handler = Arc::new(ErrorHandler);
        let _ = mgr.process_with_pipeline(1, &msg, handler).await;

        let snapshot = mgr.get_metrics_snapshot().await;
        assert_eq!(snapshot.total_requests, 6);
        assert_eq!(snapshot.successful_requests, 5);
        assert_eq!(snapshot.failed_requests, 1);
        assert_eq!(snapshot.active_sessions, 2);
        assert!(snapshot.avg_latency_us() >= 0.0);
        assert!(snapshot.success_rate() > 80.0);
    }

    #[tokio::test]
    async fn test_health_check() {
        let mgr = ServerConnectionManager::new();
        let addr: SocketAddr = "127.0.0.1:12345".parse().unwrap();

        mgr.register_session(1, ClientType::Fuse, addr).await;
        mgr.register_session(2, ClientType::Kernel, addr).await;

        let health = mgr.health_check().await;
        assert!(health.healthy);
        assert_eq!(health.active_sessions, 2);
        assert_eq!(health.total_sessions, 2);
    }

    #[tokio::test]
    async fn test_list_client_ids() {
        let mgr = ServerConnectionManager::new();
        let addr: SocketAddr = "127.0.0.1:12345".parse().unwrap();

        mgr.register_session(10, ClientType::Fuse, addr).await;
        mgr.register_session(20, ClientType::Kernel, addr).await;
        mgr.register_session(30, ClientType::Admin, addr).await;

        let ids = mgr.list_client_ids().await;
        assert_eq!(ids.len(), 3);
        assert!(ids.contains(&10));
        assert!(ids.contains(&20));
        assert!(ids.contains(&30));
    }

    #[tokio::test]
    async fn test_force_disconnect() {
        let mgr = ServerConnectionManager::new();
        let addr: SocketAddr = "127.0.0.1:12345".parse().unwrap();

        mgr.register_session(1, ClientType::Fuse, addr).await;
        assert_eq!(mgr.active_count().await, 1);

        let result = mgr.force_disconnect(1).await;
        assert!(result);
        assert_eq!(mgr.active_count().await, 0);

        // Non-existent client
        let result = mgr.force_disconnect(999).await;
        assert!(!result);
    }

    #[tokio::test]
    async fn test_multiple_clients_pipeline() {
        let mgr = ServerConnectionManager::new();
        let handler = Arc::new(TestHandler);

        // Register 10 clients and process requests
        for i in 1..=10 {
            let addr: SocketAddr = format!("127.0.0.1:{}", 12000 + i).parse().unwrap();
            mgr.register_session(i, ClientType::Fuse, addr).await;
        }

        for i in 1..=10 {
            let msg = make_test_msg();
            let result = mgr.process_with_pipeline(i, &msg, handler.clone()).await;
            assert!(result.is_ok());
        }

        let snapshot = mgr.get_metrics_snapshot().await;
        assert_eq!(snapshot.total_requests, 10);
        assert_eq!(snapshot.successful_requests, 10);
        assert_eq!(snapshot.active_sessions, 10);
    }
}
