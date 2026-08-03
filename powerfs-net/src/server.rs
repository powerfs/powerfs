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
use tokio::sync::mpsc;
use tokio::sync::RwLock;
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

        // Phase 1: Handshake + auto session registration
        // The handshake needs both read and write on the raw stream; it returns
        // ownership so we can split the stream afterwards.
        let (stream, client_id, _client_type) =
            Self::handle_handshake(stream, handler.clone(), manager.clone(), peer).await?;

        // Register notification channel for this client
        let notification_rx = if let Some(ref mgr) = manager {
            Some(mgr.register_notification_channel(client_id).await)
        } else {
            None
        };

        // Split the TcpStream into independent read and write halves.
        //
        // Previously the stream was wrapped in `Arc<Mutex<TcpStream>>` and shared
        // between the read loop and per-request handler tasks.  The read loop
        // held the mutex for up to 100 ms during its `read_exact` timeout (used
        // to poll for notifications), which blocked every response write for the
        // same duration — producing the consistent ~100 ms latency observed on
        // every metadata operation.
        //
        // `into_split()` returns `OwnedReadHalf` / `OwnedWriteHalf` that share
        // no lock, fully decoupling the read path from the write path.
        let (read_half, write_half) = stream.into_split();

        // Dedicated write task: owns the write half and drains an mpsc channel.
        // All response frames and notification frames are pushed here, so no
        // caller ever blocks on a stream lock.
        let (write_tx, mut write_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;
            let mut wh = write_half;
            while let Some(frame) = write_rx.recv().await {
                if let Err(e) = wh.write_all(&frame).await {
                    warn!("server write_task: write error: {:?}", e);
                    break;
                }
            }
            info!("server write_task stopped for client {}", client_id);
        });

        // Phase 2: Message loop - blocks until client disconnects or error
        let result = Self::message_loop(
            read_half,
            write_tx,
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

    /// Handle handshake and return the stream along with (client_id, client_type).
    /// The stream is returned so the caller can split it into read/write halves.
    async fn handle_handshake(
        mut stream: TcpStream,
        handler: Arc<dyn PowerFsNetHandler>,
        manager: Option<Arc<ServerConnectionManager>>,
        peer_addr: SocketAddr,
    ) -> NetResult<(TcpStream, u64, ClientType)> {
        let mut req_buf = vec![0u8; HandshakeRequest::SIZE];
        stream.read_exact(&mut req_buf).await?;

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
        stream.write_all(&resp_buf).await?;

        let client_id = req.client_id;

        // Register session with manager (if enabled)
        if let Some(ref mgr) = manager {
            mgr.register_session(client_id, client_type, peer_addr)
                .await;
        }

        // Notify handler
        handler.on_connect(client_id, client_type).await;

        Ok((stream, client_id, client_type))
    }

    /// Main message loop for a connection
    ///
    /// The read half is exclusively owned by this loop (no lock).  All writes
    /// — responses and notifications — go through a dedicated write task via
    /// an mpsc channel, so the read loop never blocks a response write.
    async fn message_loop(
        mut read_half: tokio::net::tcp::OwnedReadHalf,
        write_tx: mpsc::UnboundedSender<Vec<u8>>,
        handler: Arc<dyn PowerFsNetHandler>,
        _manager: Option<Arc<ServerConnectionManager>>,
        client_id: u64,
        notification_rx: Option<mpsc::Receiver<NetMessage>>,
    ) -> NetResult<()> {
        use tokio::io::AsyncReadExt;

        // Spawn a notification forwarder: receives notifications from the
        // ServerConnectionManager and pushes them onto the write channel.
        // This replaces the old 100 ms read-timeout polling, eliminating the
        // stream-lock contention that caused ~100 ms metadata latency.
        if let Some(mut rx) = notification_rx {
            let write_tx = write_tx.clone();
            tokio::spawn(async move {
                while let Some(msg) = rx.recv().await {
                    debug!(
                        "Sending notification to client {}: type={:?}",
                        client_id,
                        msg.msg_type()
                    );
                    let frame = build_frame(&msg.header, &msg.body, &msg.data);
                    if write_tx.send(frame).is_err() {
                        break; // write channel closed — connection dropped
                    }
                }
            });
        }

        loop {
            // Read header — blocking, no timeout, no lock.  The read half is
            // exclusively owned by this loop, so handler response writes (via
            // the write task) never contend with reads.
            let mut hdr_buf = vec![0u8; FrameHeader::SIZE];
            if let Err(e) = read_half.read_exact(&mut hdr_buf).await {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    info!("Client disconnected");
                    return Ok(());
                }
                return Err(NetError::Io(e));
            }

            let header = match FrameHeader::decode(&hdr_buf) {
                Some(h) => h,
                None => {
                    warn!("invalid frame header, skipping");
                    continue;
                }
            };

            // Read body + data (header.data_len covers both segments)
            let data_len = header.data_len as usize;
            let mut body = Vec::with_capacity(data_len);
            if data_len > 0 {
                body.resize(data_len, 0u8);
                read_half.read_exact(&mut body).await?;
            }

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
                let frame = build_frame(&resp_header, &[], &[]);
                let _ = write_tx.send(frame);
                continue;
            }

            // Handle notify (no response expected) — checked before request
            // handling because the latter moves `message` into a spawned task.
            if message.header.flags & FrameFlags::NOTIFY != 0 {
                debug!("Received notify: seq={}", message.header.seq);
            }

            // Handle request — spawn concurrent task so the loop can continue
            // reading the next request without waiting for handler completion.
            // This is critical for volume server throughput: without concurrency,
            // a slow WriteNeedle blocks all subsequent requests on the same
            // connection, causing client-side timeouts.
            //
            // Concurrency is naturally bounded by the client's
            // TransportChannel::max_concurrent (data=32, lease=4, mgmt=4).
            if message.is_request() {
                let handler = handler.clone();
                let write_tx = write_tx.clone();
                let seq = message.header.seq;
                let msg_type_u16 = message
                    .msg_type()
                    .map(|t| t.as_u16())
                    .unwrap_or(message.header.msg_type);
                debug!(
                    "Spawning handler for request seq={} type={:?}",
                    seq,
                    message.msg_type()
                );
                tokio::spawn(async move {
                    let response = match handler.handle_request(client_id, &message).await {
                        Ok(resp) => {
                            debug!("Request seq={} handled, status={}", seq, resp.header.status);
                            resp
                        }
                        Err(e) => {
                            error!("Request seq={} handler error: {:?}", seq, e);
                            let header = FrameHeader::new(
                                msg_type_u16,
                                FrameFlags::new(FrameFlags::RESPONSE),
                                seq,
                                0,
                            )
                            .with_status(STATUS_ERR_SERVER_ERROR);
                            NetMessage::new(header)
                        }
                    };

                    // Send response via the write channel — no stream lock.
                    let frame = build_frame(&response.header, &response.body, &response.data);
                    if write_tx.send(frame).is_err() {
                        error!(
                            "Failed to send response for seq={}: write channel closed",
                            seq
                        );
                    }
                });
            }
        }
    }
}

/// Build a wire frame from a header, body, and data segment.
fn build_frame(header: &FrameHeader, body: &[u8], data: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(FrameHeader::SIZE + body.len() + data.len());
    let mut hdr_buf = vec![0u8; FrameHeader::SIZE];
    header.encode(&mut hdr_buf);
    frame.extend_from_slice(&hdr_buf);
    frame.extend_from_slice(body);
    frame.extend_from_slice(data);
    frame
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
