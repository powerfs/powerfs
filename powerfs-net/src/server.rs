//! PowerFS Net Server - Rust implementation
//!
//! Provides a server that accepts connections and dispatches
//! requests to handler implementations.
//!
//! # Integration with ServerConnectionManager
//!
//! When a `ServerConnectionManager` is provided via `bind_with_manager`,
//! client sessions are automatically registered on handshake and
//! unregistered on disconnect. This enables middleware processing
//! (logging, metrics, rate limiting) for all requests.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use log::{debug, error, info, warn};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock};
use tokio::time::Instant;

use crate::errors::{NetError, NetResult};
use crate::protocol::*;
use crate::server_connection::ServerConnectionManager;

/// Handler trait for processing net requests
#[async_trait::async_trait]
pub trait PowerFsNetHandler: Send + Sync {
    /// Handle a request and return a response
    async fn handle_request(&self, client_id: u64, msg: &NetMessage) -> NetResult<NetMessage>;

    /// Called when a client connects
    async fn on_connect(&self, _client_id: u64, _client_type: ClientType) {}

    /// Called when a client disconnects
    async fn on_disconnect(&self, _client_id: u64) {}
}

/// PowerFS Net Server
pub struct PowerFsNetServer {
    listener: TcpListener,
    handler: Arc<dyn PowerFsNetHandler>,
    manager: Option<Arc<ServerConnectionManager>>,
    shutdown: Arc<RwLock<ShutdownState>>,
}

#[derive(Default)]
struct ShutdownState {
    shutting_down: bool,
    active_connections: u64,
}

impl PowerFsNetServer {
    pub async fn bind(
        addr: &str,
        port: u16,
        handler: Arc<dyn PowerFsNetHandler>,
    ) -> NetResult<Self> {
        Self::bind_inner(addr, port, handler, None).await
    }

    /// Bind with automatic session management via ServerConnectionManager
    pub async fn bind_with_manager(
        addr: &str,
        port: u16,
        handler: Arc<dyn PowerFsNetHandler>,
        manager: Arc<ServerConnectionManager>,
    ) -> NetResult<Self> {
        Self::bind_inner(addr, port, handler, Some(manager)).await
    }

    async fn bind_inner(
        addr: &str,
        port: u16,
        handler: Arc<dyn PowerFsNetHandler>,
        manager: Option<Arc<ServerConnectionManager>>,
    ) -> NetResult<Self> {
        let socket_addr: SocketAddr = format!("{}:{}", addr, port)
            .parse()
            .map_err(|e| NetError::Protocol(format!("invalid address: {}", e)))?;

        let listener = TcpListener::bind(socket_addr).await?;
        info!(
            "PowerFS Net server listening on {}:{} (session management: {})",
            addr,
            port,
            if manager.is_some() {
                "enabled"
            } else {
                "disabled"
            }
        );

        Ok(Self {
            listener,
            handler,
            manager,
            shutdown: Arc::new(RwLock::new(ShutdownState::default())),
        })
    }

    /// Get the local address
    pub fn local_addr(&self) -> NetResult<SocketAddr> {
        self.listener.local_addr().map_err(NetError::Io)
    }

    /// Get the connection manager, if session management is enabled
    pub fn manager(&self) -> Option<&Arc<ServerConnectionManager>> {
        self.manager.as_ref()
    }

    /// Start serving (runs until stopped)
    pub async fn serve(&self) -> NetResult<()> {
        info!("Starting to accept connections...");

        loop {
            if self.is_shutting_down().await {
                info!("Server is shutting down, stopping accept loop");
                break;
            }

            match self.listener.accept().await {
                Ok((stream, addr)) => {
                    if self.is_shutting_down().await {
                        info!("Rejecting new connection during shutdown from {}", addr);
                        break;
                    }
                    info!("New connection from {}", addr);
                    self.increment_connections().await;
                    let handler = self.handler.clone();
                    let manager = self.manager.clone();
                    let shutdown = self.shutdown.clone();
                    tokio::spawn(async move {
                        let result = Self::handle_connection(stream, handler, manager).await;
                        Self::decrement_connections(&shutdown).await;
                        if let Err(e) = result {
                            error!("Connection error from {}: {:?}", addr, e);
                        }
                    });
                }
                Err(e) => {
                    error!("Accept error: {:?}", e);
                }
            }
        }

        Ok(())
    }

    /// Start serving with graceful shutdown support
    /// Returns when shutdown is signaled and all connections are drained
    pub async fn serve_with_shutdown(&self, timeout: Duration) -> NetResult<()> {
        info!("Starting to accept connections (graceful shutdown enabled)...");

        loop {
            if self.is_shutting_down().await {
                info!("Shutdown signaled, draining connections...");
                break;
            }

            let accept_result = {
                let shutdown = self.shutdown.clone();
                tokio::select! {
                    result = self.listener.accept() => Some(result),
                    _ = async {
                        let mut state = shutdown.write().await;
                        state.shutting_down = true;
                    } => None,
                }
            };

            if let Some(Ok((stream, addr))) = accept_result {
                if self.is_shutting_down().await {
                    break;
                }
                info!("New connection from {}", addr);
                self.increment_connections().await;
                let handler = self.handler.clone();
                let manager = self.manager.clone();
                let shutdown = self.shutdown.clone();
                tokio::spawn(async move {
                    let result = Self::handle_connection(stream, handler, manager).await;
                    Self::decrement_connections(&shutdown).await;
                    if let Err(e) = result {
                        error!("Connection error from {}: {:?}", addr, e);
                    }
                });
            }
        }

        // Drain active connections with timeout
        self.drain_connections(timeout).await;

        // Force disconnect any remaining sessions
        if let Some(ref mgr) = self.manager {
            let sessions = mgr.list_client_ids().await;
            for id in sessions {
                mgr.force_disconnect(id).await;
            }
        }

        info!("Server shut down gracefully");
        Ok(())
    }

    /// Signal the server to shut down gracefully
    pub async fn signal_shutdown(&self) {
        let mut state = self.shutdown.write().await;
        if !state.shutting_down {
            state.shutting_down = true;
            info!("Shutdown signal received");
        }
    }

    /// Wait for active connections to drain
    async fn drain_connections(&self, timeout: Duration) {
        let start = Instant::now();
        loop {
            let remaining = self.active_connections().await;
            if remaining == 0 {
                info!("All connections drained");
                break;
            }
            if start.elapsed() >= timeout {
                warn!(
                    "Shutdown timeout reached, {} connections remaining",
                    remaining
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn is_shutting_down(&self) -> bool {
        self.shutdown.read().await.shutting_down
    }

    async fn active_connections(&self) -> u64 {
        self.shutdown.read().await.active_connections
    }

    async fn increment_connections(&self) {
        let mut state = self.shutdown.write().await;
        state.active_connections += 1;
    }

    async fn decrement_connections(shutdown: &Arc<RwLock<ShutdownState>>) {
        let mut state = shutdown.write().await;
        state.active_connections = state.active_connections.saturating_sub(1);
    }

    /// Serve until SIGTERM/SIGINT is received, then gracefully shut down
    pub async fn serve_until_signal(&self, timeout: Duration) -> NetResult<()> {
        let shutdown = self.shutdown.clone();
        let signal_handle = tokio::spawn(async move {
            match tokio::signal::ctrl_c().await {
                Ok(()) => {
                    info!("Received shutdown signal (Ctrl+C or SIGTERM)");
                    let mut state = shutdown.write().await;
                    state.shutting_down = true;
                }
                Err(e) => {
                    warn!("Failed to listen for signal: {:?}", e);
                    let mut state = shutdown.write().await;
                    state.shutting_down = true;
                }
            }
        });

        let result = self.serve().await;
        signal_handle.abort();

        // Drain connections
        self.drain_connections(timeout).await;

        // Force disconnect remaining
        if let Some(ref mgr) = self.manager {
            let sessions = mgr.list_client_ids().await;
            for id in sessions {
                mgr.force_disconnect(id).await;
            }
        }

        info!("Server shut down gracefully");
        result
    }

    /// Handle a single connection
    async fn handle_connection(
        stream: TcpStream,
        handler: Arc<dyn PowerFsNetHandler>,
        manager: Option<Arc<ServerConnectionManager>>,
    ) -> NetResult<()> {
        let peer = stream.peer_addr()?;
        info!("Handling connection from {}", peer);

        stream.set_nodelay(true)?;
        let stream = Arc::new(Mutex::new(stream));

        // Phase 1: Handshake + auto session registration
        let (client_id, _client_type) =
            Self::handle_handshake(stream.clone(), handler.clone(), manager.clone(), peer).await?;

        // Register notification channel for this client
        let notification_rx = if let Some(ref mgr) = manager {
            Some(mgr.register_notification_channel(client_id).await)
        } else {
            None
        };

        // Phase 2: Message loop - blocks until client disconnects or error
        let result = Self::message_loop(
            stream.clone(),
            handler.clone(),
            manager.clone(),
            client_id,
            notification_rx,
        )
        .await;

        // Phase 3: Auto session unregistration + notify handler
        if let Some(ref mgr) = manager {
            mgr.unregister_session(client_id).await;
        }
        handler.on_disconnect(client_id).await;

        result
    }

    /// Handle handshake and return (client_id, client_type)
    async fn handle_handshake(
        stream: Arc<Mutex<TcpStream>>,
        handler: Arc<dyn PowerFsNetHandler>,
        manager: Option<Arc<ServerConnectionManager>>,
        peer_addr: SocketAddr,
    ) -> NetResult<(u64, ClientType)> {
        let (client_id, client_type) = {
            let mut s = stream.lock().await;
            let mut req_buf = vec![0u8; HandshakeRequest::SIZE];
            s.read_exact(&mut req_buf).await?;

            let req = HandshakeRequest::decode(&req_buf)
                .ok_or_else(|| NetError::Protocol("invalid handshake request".into()))?;

            if req.magic != *PROTOCOL_MAGIC {
                return Err(NetError::Protocol("invalid magic".into()));
            }

            let client_type = ClientType::from_u8(req.client_type)
                .ok_or_else(|| NetError::Protocol("unknown client type".into()))?;

            info!(
                "Handshake: client_id={} client_type={:?} addr={}",
                req.client_id, client_type, peer_addr
            );

            // Send handshake response
            let resp = HandshakeResponse::ok(0);
            let mut resp_buf = vec![0u8; HandshakeResponse::SIZE];
            resp.encode(&mut resp_buf);
            s.write_all(&resp_buf).await?;

            (req.client_id, client_type)
        };

        // Register session with manager (if enabled)
        if let Some(ref mgr) = manager {
            mgr.register_session(client_id, client_type, peer_addr)
                .await;
        }

        // Notify handler
        handler.on_connect(client_id, client_type).await;

        Ok((client_id, client_type))
    }

    /// Main message loop for a connection
    ///
    /// Uses tokio::select! to simultaneously handle:
    /// - Incoming client requests
    /// - Outgoing notifications from ServerConnectionManager
    async fn message_loop(
        stream: Arc<Mutex<TcpStream>>,
        handler: Arc<dyn PowerFsNetHandler>,
        _manager: Option<Arc<ServerConnectionManager>>,
        client_id: u64,
        mut notification_rx: Option<tokio::sync::mpsc::Receiver<NetMessage>>,
    ) -> NetResult<()> {
        use tokio::io::AsyncReadExt;

        // If we have a notification channel, spawn a task to handle notifications
        // Otherwise, just handle requests
        loop {
            // Try to read a header with a small timeout to allow checking notifications
            let read_result = {
                let mut s = stream.lock().await;
                let mut hdr_buf = vec![0u8; FrameHeader::SIZE];

                // Use a timeout to periodically check for notifications
                let read_future = s.read_exact(&mut hdr_buf);
                match tokio::time::timeout(std::time::Duration::from_millis(100), read_future).await
                {
                    Ok(result) => result.map(|_| hdr_buf),
                    Err(_) => {
                        // Timeout - check for notifications
                        if let Some(ref mut rx) = notification_rx {
                            while let Ok(msg) = rx.try_recv() {
                                // Send notification to client
                                debug!(
                                    "Sending notification to client {}: type={:?}",
                                    client_id,
                                    msg.msg_type()
                                );
                                let mut frame = Vec::with_capacity(
                                    FrameHeader::SIZE + msg.body.len() + msg.data.len(),
                                );
                                let mut hdr_buf = vec![0u8; FrameHeader::SIZE];
                                msg.header.encode(&mut hdr_buf);
                                frame.extend_from_slice(&hdr_buf);
                                frame.extend_from_slice(&msg.body);
                                frame.extend_from_slice(&msg.data);
                                s.write_all(&frame).await?;
                            }
                        }
                        continue;
                    }
                }
            };

            // Handle read result
            let header = match read_result {
                Ok(hdr_buf) => FrameHeader::decode(&hdr_buf)
                    .ok_or_else(|| NetError::Protocol("invalid frame header".into()))?,
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        info!("Client disconnected");
                        return Ok(());
                    }
                    return Err(NetError::Io(e));
                }
            };

            // Read data
            let body = {
                let data_len = header.data_len as usize;
                if data_len > 0 {
                    let mut s = stream.lock().await;
                    let mut data = Vec::with_capacity(data_len);
                    let mut remaining = data_len;

                    while remaining > 0 {
                        let chunk = std::cmp::min(remaining, 4096);
                        let mut buf = vec![0u8; chunk];
                        s.read_exact(&mut buf).await?;
                        data.extend_from_slice(&buf);
                        remaining -= chunk;
                    }
                    data
                } else {
                    Vec::new()
                }
            };

            let message = NetMessage::new(header.clone()).with_body(body);

            debug!(
                "Received message: seq={} type={:?} body_len={}",
                message.header.seq,
                message.msg_type(),
                message.body.len()
            );

            // Handle control frames
            if let Some(MsgType::Ping) = message.msg_type() {
                let resp_header = FrameHeader::new(
                    MsgType::Ping.as_u16(),
                    FrameFlags::new(FrameFlags::RESPONSE),
                    message.header.seq,
                    0,
                )
                .with_status(STATUS_OK);

                let mut s = stream.lock().await;
                let mut frame = Vec::with_capacity(FrameHeader::SIZE);
                let mut hdr_buf = vec![0u8; FrameHeader::SIZE];
                resp_header.encode(&mut hdr_buf);
                frame.extend_from_slice(&hdr_buf);
                s.write_all(&frame).await?;
                continue;
            }

            // Handle request
            if message.is_request() {
                debug!(
                    "Processing request seq={} type={:?}",
                    message.header.seq,
                    message.msg_type()
                );
                let response = handler.handle_request(client_id, &message).await?;
                debug!(
                    "Request seq={} handled, status={}",
                    message.header.seq, response.header.status
                );

                // Send response
                {
                    let mut s = stream.lock().await;
                    let mut frame = Vec::with_capacity(
                        FrameHeader::SIZE + response.body.len() + response.data.len(),
                    );
                    let mut hdr_buf = vec![0u8; FrameHeader::SIZE];
                    response.header.encode(&mut hdr_buf);
                    frame.extend_from_slice(&hdr_buf);
                    frame.extend_from_slice(&response.body);
                    frame.extend_from_slice(&response.data);
                    debug!(
                        "Sending response for seq={}, frame_len={}",
                        message.header.seq,
                        frame.len()
                    );
                    s.write_all(&frame).await?;
                    debug!("Response sent for seq={}", message.header.seq);
                }
            }

            // Handle notify (no response expected)
            if message.header.flags & FrameFlags::NOTIFY != 0 {
                debug!("Received notify: seq={}", message.header.seq);
            }
        }
    }
}

/// Simple handler for testing
pub struct EchoHandler;

#[async_trait::async_trait]
impl PowerFsNetHandler for EchoHandler {
    async fn handle_request(&self, _client_id: u64, msg: &NetMessage) -> NetResult<NetMessage> {
        let resp_header = FrameHeader::new(
            msg.header.msg_type,
            FrameFlags::new(FrameFlags::RESPONSE),
            msg.header.seq,
            msg.body.len() as u32,
        )
        .with_status(STATUS_OK);

        Ok(NetMessage::new(resp_header).with_body(msg.body.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_server_start_stop() {
        let handler = Arc::new(EchoHandler);
        let server = PowerFsNetServer::bind("127.0.0.1", 0, handler)
            .await
            .unwrap();
        let addr = server.local_addr().unwrap();

        assert!(addr.port() > 0);
        info!("Server bound to {}", addr);
    }

    #[tokio::test]
    async fn test_server_with_manager() {
        let handler = Arc::new(EchoHandler);
        let manager = Arc::new(ServerConnectionManager::new());
        let server = PowerFsNetServer::bind_with_manager("127.0.0.1", 0, handler, manager)
            .await
            .unwrap();

        assert!(server.manager().is_some());
        let addr = server.local_addr().unwrap();
        assert!(addr.port() > 0);
    }

    /// End-to-end test: real TCP connection with handshake + request/response
    #[tokio::test]
    async fn test_e2e_handshake_and_request() {
        use crate::client::PowerFsNetClient;
        use crate::handler_adapter::ManagedNetHandler;
        use crate::middleware::PipelineBuilder;

        // EchoHandler as a ServerRequestHandler
        struct EchoRequestHandler;
        #[async_trait::async_trait]
        impl crate::server_connection::ServerRequestHandler for EchoRequestHandler {
            async fn handle(
                &self,
                _ctx: &mut crate::request_context::RequestContext,
                msg: &NetMessage,
            ) -> NetResult<NetMessage> {
                let resp_header = FrameHeader::new(
                    msg.header.msg_type,
                    FrameFlags::new(FrameFlags::RESPONSE),
                    msg.header.seq,
                    msg.body.len() as u32,
                )
                .with_status(STATUS_OK);
                Ok(NetMessage::new(resp_header).with_body(msg.body.clone()))
            }
        }

        let pipeline = PipelineBuilder::full_tracing();
        let manager = Arc::new(ServerConnectionManager::new().with_pipeline(pipeline));
        let handler =
            Arc::new(EchoRequestHandler) as Arc<dyn crate::server_connection::ServerRequestHandler>;
        let managed = Arc::new(ManagedNetHandler::from_arc(manager.clone(), handler))
            as Arc<dyn PowerFsNetHandler>;

        let server = PowerFsNetServer::bind_with_manager("127.0.0.1", 0, managed, manager.clone())
            .await
            .unwrap();
        let addr = server.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            server.serve().await.unwrap();
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let client_id = 42u64;
        let client = PowerFsNetClient::new(crate::client::ClientConfig {
            addr: "127.0.0.1".into(),
            port: addr.port(),
            client_id,
            client_type: ClientType::Fuse,
            ..Default::default()
        });

        client.connect().await.unwrap();

        // Send lookup request - the wire protocol concatenates body+data,
        // so the echoed response contains both in body
        let msg = client
            .send_request(MsgType::Lookup, b"test_body", b"test_data")
            .await
            .unwrap();

        assert!(msg.is_ok());
        assert_eq!(msg.msg_type(), Some(MsgType::Lookup));
        // Wire protocol: body+data concatenated, echoed back in response.body
        assert_eq!(msg.body, b"test_bodytest_data");
        assert!(msg.data.is_empty());

        // Send another request with only body
        let msg2 = client
            .send_request(MsgType::GetAttr, b"attr_body", &[])
            .await
            .unwrap();

        assert!(msg2.is_ok());
        assert_eq!(msg2.msg_type(), Some(MsgType::GetAttr));
        assert_eq!(msg2.body, b"attr_body");

        // Verify session state
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let sessions = manager.active_count().await;
        assert!(sessions >= 1);

        let health = manager.health_check().await;
        assert!(health.healthy);

        let snapshot = manager.get_metrics_snapshot().await;
        assert!(snapshot.total_requests >= 2);
        assert!(snapshot.successful_requests >= 2);

        client.disconnect().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        server_handle.abort();
    }

    /// Test that server handles concurrent clients correctly with pipeline metrics
    #[tokio::test]
    async fn test_e2e_concurrent_clients() {
        use crate::client::PowerFsNetClient;
        use crate::handler_adapter::ManagedNetHandler;
        use crate::middleware::PipelineBuilder;

        struct EchoRequestHandler;
        #[async_trait::async_trait]
        impl crate::server_connection::ServerRequestHandler for EchoRequestHandler {
            async fn handle(
                &self,
                _ctx: &mut crate::request_context::RequestContext,
                msg: &NetMessage,
            ) -> NetResult<NetMessage> {
                let resp_header = FrameHeader::new(
                    msg.header.msg_type,
                    FrameFlags::new(FrameFlags::RESPONSE),
                    msg.header.seq,
                    msg.body.len() as u32,
                )
                .with_status(STATUS_OK);
                Ok(NetMessage::new(resp_header).with_body(msg.body.clone()))
            }
        }

        let pipeline = PipelineBuilder::default_build();
        let manager = Arc::new(ServerConnectionManager::new().with_pipeline(pipeline));
        let handler =
            Arc::new(EchoRequestHandler) as Arc<dyn crate::server_connection::ServerRequestHandler>;
        let managed = Arc::new(ManagedNetHandler::from_arc(manager.clone(), handler))
            as Arc<dyn PowerFsNetHandler>;

        let server = PowerFsNetServer::bind_with_manager("127.0.0.1", 0, managed, manager.clone())
            .await
            .unwrap();
        let addr = server.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            server.serve().await.unwrap();
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let mut handles = Vec::new();
        for client_id in 1..=5 {
            let port = addr.port();
            handles.push(tokio::spawn(async move {
                let client = PowerFsNetClient::new(crate::client::ClientConfig {
                    addr: "127.0.0.1".into(),
                    port,
                    client_id,
                    client_type: ClientType::Fuse,
                    ..Default::default()
                });

                client.connect().await.unwrap();

                for i in 0..10 {
                    let msg = client
                        .send_request(MsgType::Lookup, format!("body_{}", i).as_bytes(), &[])
                        .await
                        .unwrap();
                    assert!(msg.is_ok());
                }

                client.disconnect().await.unwrap();
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        // Note: each client disconnect sends a close frame through the pipeline,
        // so total_requests = 5*10 (test requests) + 5 (disconnect frames) = 55
        let snapshot = manager.get_metrics_snapshot().await;
        assert!(snapshot.total_requests >= 50);
        assert!(snapshot.successful_requests >= 50);
        assert_eq!(snapshot.failed_requests, 0);

        server_handle.abort();
    }
}
