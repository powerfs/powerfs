//! PowerFS Net Server - Acceptor + IoLoop + Worker 架构
//!
//! 架构概览:
//! ```text
//!   Acceptor (1 task)        → accept + handshake + create ClientConn
//!   IoLoop × N (tokio tasks) → read frames → Work → WorkQueue, write responses
//!   Worker pool (Semaphore)  → process Work, bounded concurrency
//!   ConnRegistry             → client_id → Arc<ClientConn>, holder → client_id
//! ```
//!
//! 关键改进 (vs 旧 per-conn spawn 模型):
//!   - 固定线程数 (N IoLoops + M Workers), 不随客户端数增长
//!   - ClientConn 抽象: 统一连接状态、holder、lease 管理
//!   - IO 与业务分离: IoLoop 只收发, Worker 只处理
//!   - 断连清理统一: IoLoop 退出时 registry.unregister + handler.on_disconnect

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use log::{debug, error, info, warn};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, RwLock};
use tokio::time::Instant;

use crate::client_conn::{ClientConn, ConnRegistry};
use crate::errors::{NetError, NetResult};
use crate::io_loop::IoLoop;
use crate::protocol::*;
use crate::server_connection::{NetHandler, ServerConnectionManager};
use crate::work::Work;
use crate::worker::Worker;

/// Server configuration (tunable parameters)
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// IO Loop 数量 (默认 = CPU 核数)
    pub num_io_loops: usize,
    /// Worker 并发数 (默认 = CPU 核数 × 2)
    pub num_workers: usize,
    /// WorkQueue 容量 (有界, 防止积压)
    pub work_queue_capacity: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        let cpus = num_cpus();
        Self {
            num_io_loops: cpus,
            num_workers: cpus * 2,
            work_queue_capacity: 4096,
        }
    }
}

/// 获取 CPU 核数 (兼容 non-std 环境)
fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

/// PowerFS Net Server (Acceptor + IoLoop + Worker 架构)
pub struct PowerFsNetServer {
    listener: TcpListener,
    handler: Arc<dyn NetHandler>,
    manager: Option<Arc<ServerConnectionManager>>,
    registry: Arc<ConnRegistry>,
    config: ServerConfig,
    shutdown: Arc<RwLock<ShutdownState>>,
}

#[derive(Default)]
struct ShutdownState {
    shutting_down: bool,
    active_connections: u64,
}

impl PowerFsNetServer {
    pub async fn bind(addr: &str, port: u16, handler: Arc<dyn NetHandler>) -> NetResult<Self> {
        Self::bind_inner(addr, port, handler, ServerConfig::default()).await
    }

    /// Bind with automatic session management via ServerConnectionManager.
    ///
    /// The server creates a `ConnRegistry` internally and wraps it in a
    /// `ServerConnectionManager` with the default middleware pipeline.
    /// Access the manager via `server.manager()` after binding.
    pub async fn bind_with_manager(
        addr: &str,
        port: u16,
        handler: Arc<dyn NetHandler>,
    ) -> NetResult<Self> {
        Self::bind_with_pipeline(addr, port, handler, None, ServerConfig::default()).await
    }

    /// Bind with an externally-created `ConnRegistry`.
    ///
    /// Use this when other components (e.g. `InodeNotifier`) need to share
    /// the same connection registry as the server. The server creates a
    /// `ServerConnectionManager` from the provided registry.
    pub async fn bind_with_registry(
        addr: &str,
        port: u16,
        handler: Arc<dyn NetHandler>,
        registry: Arc<ConnRegistry>,
    ) -> NetResult<Self> {
        Self::bind_with_registry_and_config(addr, port, handler, registry, ServerConfig::default())
            .await
    }

    /// Bind with an externally-created `ConnRegistry` and custom config.
    pub async fn bind_with_registry_and_config(
        addr: &str,
        port: u16,
        handler: Arc<dyn NetHandler>,
        registry: Arc<ConnRegistry>,
        config: ServerConfig,
    ) -> NetResult<Self> {
        let socket_addr: SocketAddr = format!("{}:{}", addr, port)
            .parse()
            .map_err(|e| NetError::Protocol(format!("invalid address: {}", e)))?;

        let listener = TcpListener::bind(socket_addr).await?;
        let manager = Arc::new(ServerConnectionManager::new(registry.clone()));

        info!(
            "PowerFS Net server listening on {}:{} (io_loops={}, workers={}, queue_cap={}, session_mgmt=enabled, shared_registry)",
            addr,
            port,
            config.num_io_loops,
            config.num_workers,
            config.work_queue_capacity,
        );

        Ok(Self {
            listener,
            handler,
            manager: Some(manager),
            registry,
            config,
            shutdown: Arc::new(RwLock::new(ShutdownState::default())),
        })
    }

    /// Bind with a custom middleware pipeline and server configuration.
    ///
    /// If `pipeline` is `None`, a default pipeline (logging + metrics) is used.
    pub async fn bind_with_pipeline(
        addr: &str,
        port: u16,
        handler: Arc<dyn NetHandler>,
        pipeline: Option<crate::middleware::RequestPipeline>,
        config: ServerConfig,
    ) -> NetResult<Self> {
        let socket_addr: SocketAddr = format!("{}:{}", addr, port)
            .parse()
            .map_err(|e| NetError::Protocol(format!("invalid address: {}", e)))?;

        let listener = TcpListener::bind(socket_addr).await?;
        let registry = Arc::new(ConnRegistry::new());

        let manager = {
            let mgr = ServerConnectionManager::new(registry.clone());
            let mgr = if let Some(p) = pipeline {
                mgr.with_pipeline(p)
            } else {
                mgr
            };
            Arc::new(mgr)
        };

        info!(
            "PowerFS Net server listening on {}:{} (io_loops={}, workers={}, queue_cap={}, session_mgmt=enabled)",
            addr,
            port,
            config.num_io_loops,
            config.num_workers,
            config.work_queue_capacity,
        );

        Ok(Self {
            listener,
            handler,
            manager: Some(manager),
            registry,
            config,
            shutdown: Arc::new(RwLock::new(ShutdownState::default())),
        })
    }

    /// Bind with custom server configuration (IoLoop/Worker counts, queue size).
    ///
    /// Creates a `ServerConnectionManager` with the default pipeline.
    pub async fn bind_with_config(
        addr: &str,
        port: u16,
        handler: Arc<dyn NetHandler>,
        config: ServerConfig,
    ) -> NetResult<Self> {
        Self::bind_with_pipeline(addr, port, handler, None, config).await
    }

    async fn bind_inner(
        addr: &str,
        port: u16,
        handler: Arc<dyn NetHandler>,
        config: ServerConfig,
    ) -> NetResult<Self> {
        let socket_addr: SocketAddr = format!("{}:{}", addr, port)
            .parse()
            .map_err(|e| NetError::Protocol(format!("invalid address: {}", e)))?;

        let listener = TcpListener::bind(socket_addr).await?;
        info!(
            "PowerFS Net server listening on {}:{} (io_loops={}, workers={}, queue_cap={}, session_mgmt=disabled)",
            addr,
            port,
            config.num_io_loops,
            config.num_workers,
            config.work_queue_capacity,
        );

        Ok(Self {
            listener,
            handler,
            manager: None,
            registry: Arc::new(ConnRegistry::new()),
            config,
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

    /// Get the connection registry (for admin/monitoring)
    pub fn registry(&self) -> &Arc<ConnRegistry> {
        &self.registry
    }

    // ========================================================================
    // Server lifecycle
    // ========================================================================

    /// Start serving (runs until stopped)
    pub async fn serve(&self) -> NetResult<()> {
        info!("Starting to accept connections...");

        // 1. 创建 WorkQueue
        let (work_tx, work_rx) = mpsc::channel::<Work>(self.config.work_queue_capacity);

        // 2. 启动 IoLoop 池 (N 个)
        let io_loops = self.spawn_io_loops(work_tx.clone());

        // 3. 启动 Worker 池 (Semaphore 限并发)
        self.spawn_worker_pool(work_rx);

        // 4. Acceptor 循环
        self.acceptor_loop(io_loops).await
    }

    /// Start serving with graceful shutdown support
    pub async fn serve_with_shutdown(&self, timeout: Duration) -> NetResult<()> {
        info!("Starting to accept connections (graceful shutdown enabled)...");

        let (work_tx, work_rx) = mpsc::channel::<Work>(self.config.work_queue_capacity);
        let io_loops = self.spawn_io_loops(work_tx);
        self.spawn_worker_pool(work_rx);

        // Acceptor loop with shutdown check
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
                self.handle_new_connection(stream, addr, &io_loops).await;
            }
        }

        // Drain: wait for connections to close
        self.drain_connections(timeout).await;

        // Force disconnect remaining
        self.force_disconnect_all().await;

        info!("Server shut down gracefully");
        Ok(())
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

        self.drain_connections(timeout).await;
        self.force_disconnect_all().await;

        info!("Server shut down gracefully");
        result
    }

    /// Signal the server to shut down gracefully
    pub async fn signal_shutdown(&self) {
        let mut state = self.shutdown.write().await;
        if !state.shutting_down {
            state.shutting_down = true;
            info!("Shutdown signal received");
        }
    }

    // ========================================================================
    // Acceptor + IoLoop + Worker setup
    // ========================================================================

    /// 启动 N 个 IoLoop, 返回 IoLoop 引用列表
    fn spawn_io_loops(&self, work_tx: mpsc::Sender<Work>) -> Vec<Arc<IoLoop>> {
        let n = self.config.num_io_loops;
        let mut loops = Vec::with_capacity(n);
        for i in 0..n {
            let io_loop = Arc::new(IoLoop::new(
                i,
                work_tx.clone(),
                self.registry.clone(),
                self.handler.clone(),
                self.manager.clone(),
            ));
            loops.push(io_loop);
        }
        info!("Started {} IO Loops", n);
        loops
    }

    /// 启动 Worker 池: 单 dispatcher task, Semaphore 限制并发
    fn spawn_worker_pool(&self, work_rx: mpsc::Receiver<Work>) {
        let handler = self.handler.clone();
        let manager = self.manager.clone();
        let num_workers = self.config.num_workers;
        let semaphore = Arc::new(tokio::sync::Semaphore::new(num_workers));

        tokio::spawn(async move {
            info!("Started Worker pool (max_concurrent={})", num_workers);
            let mut work_rx = work_rx;

            while let Some(work) = work_rx.recv().await {
                // 获取许可 (限制并发数)
                let permit = match semaphore.clone().acquire_owned().await {
                    Ok(permit) => permit,
                    Err(e) => {
                        error!("Worker pool: semaphore closed: {:?}", e);
                        break;
                    }
                };

                // spawn 处理任务
                let handler = handler.clone();
                let manager = manager.clone();
                tokio::spawn(async move {
                    let worker = Worker::new(0, handler, manager);
                    worker.process_work(work).await;
                    drop(permit);
                });
            }

            info!("Worker pool stopped (WorkQueue closed)");
        });
    }

    /// Acceptor 主循环: accept → handshake → create ClientConn → assign IoLoop
    async fn acceptor_loop(&self, io_loops: Vec<Arc<IoLoop>>) -> NetResult<()> {
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
                    self.handle_new_connection(stream, addr, &io_loops).await;
                }
                Err(e) => {
                    error!("Accept error: {:?}", e);
                }
            }
        }

        Ok(())
    }

    /// 处理新连接: handshake → create ClientConn → register → assign IoLoop
    async fn handle_new_connection(
        &self,
        stream: TcpStream,
        addr: SocketAddr,
        io_loops: &[Arc<IoLoop>],
    ) {
        self.increment_connections().await;

        let handler = self.handler.clone();
        let manager = self.manager.clone();
        let registry = self.registry.clone();

        // handshake 需要读写 stream, 完成后返回 stream + client_id
        let peer = addr;
        let (stream, client_id, client_type) =
            match Self::handle_handshake(stream, handler.clone(), manager.clone(), peer).await {
                Ok(result) => result,
                Err(e) => {
                    error!("Handshake failed from {}: {:?}", peer, e);
                    self.decrement_connections().await;
                    return;
                }
            };

        // 发送 handshake response (在 handshake 内部已完成, 这里 stream 已可拆分)
        stream.set_nodelay(true).ok();

        // 创建 outbound channel: Worker/notify → write_task
        let (outbound_tx, outbound_rx) = mpsc::unbounded_channel::<Vec<u8>>();

        // 创建 ClientConn
        let conn = ClientConn::new(client_id, peer, client_type, outbound_tx);

        // 注册到 ConnRegistry (单一数据源: 状态/lease/统计/通知都在 ClientConn 中)
        registry.register(conn.clone()).await;

        // 注册到 ServerConnectionManager (仅日志, session 数据由 ConnRegistry 管理)
        if let Some(ref mgr) = manager {
            mgr.register_session(client_id, client_type, peer).await;
        }

        // 通知推送: ServerConnectionManager.send_notification() 直接调用
        // ConnRegistry::notify() → ClientConn::notify() → outbound_tx,
        // 无需中间 channel 转发任务。

        // 通知 handler
        handler.on_connect(client_id, client_type).await;

        // 分配到 IoLoop (hash % N)
        let io_loop_idx = (client_id as usize) % io_loops.len();
        let io_loop = io_loops[io_loop_idx].clone();

        debug!("Assigned client {} to IoLoop {}", client_id, io_loop_idx);

        // IoLoop.manage 会 spawn task 管理该连接
        // 连接断开后 IoLoop 会自动执行断连清理 + decrement_connections
        io_loop.manage(stream, conn, outbound_rx);

        // IoLoop.manage 是 fire-and-forget, 但我们需要在断连时 decrement
        // 使用一个监控 task
        // 实际上 IoLoop 内部 spawn 了 task, 我们无法直接等待它
        // 断连清理由 IoLoop 内部完成 (registry.unregister + handler.on_disconnect)
        // active_connections 计数通过 registry.active_count() 获取, 不需要手动 decrement
        // 但为兼容现有 shutdown 逻辑, 保留 active_connections 计数
        // IoLoop 断连后 registry.unregister 会减少计数, 但 ShutdownState.active_connections
        // 需要单独减少. 这里通过 registry 监控实现.
        let registry_for_monitor = self.registry.clone();
        let shutdown_for_monitor = self.shutdown.clone();
        let client_id_for_monitor = client_id;
        tokio::spawn(async move {
            // 等待连接从 registry 消失
            // (IoLoop 断连清理时会调用 registry.unregister)
            loop {
                if registry_for_monitor.get(client_id_for_monitor).is_none() {
                    // 连接已注销, 减少 active_connections
                    let mut state = shutdown_for_monitor.write().await;
                    state.active_connections = state.active_connections.saturating_sub(1);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        });

        info!("New connection from {} (client_id={})", peer, client_id);
    }

    // ========================================================================
    // Handshake
    // ========================================================================

    /// Handle handshake and return the stream along with (client_id, client_type)
    async fn handle_handshake(
        mut stream: TcpStream,
        handler: Arc<dyn NetHandler>,
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

        // Register session with manager (if enabled) — done by caller
        // Notify handler — done by caller
        let _ = (handler, manager); // suppress unused warnings

        Ok((stream, client_id, client_type))
    }

    // ========================================================================
    // Shutdown helpers
    // ========================================================================

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

    async fn force_disconnect_all(&self) {
        // 通过 registry 断开所有连接 (单一数据源, 无需 manager 重复)
        let conns = self.registry.list().await;
        for info in conns {
            self.registry.disconnect(info.id).await;
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

    async fn decrement_connections(&self) {
        let mut state = self.shutdown.write().await;
        state.active_connections = state.active_connections.saturating_sub(1);
    }
}

/// Build a wire frame from a header, body, and data segment.
/// (保留为公共函数, 供测试和外部使用)
pub fn build_frame(header: &FrameHeader, body: &[u8], data: &[u8]) -> Vec<u8> {
    let mut hdr = header.clone();
    hdr.set_body_data_len(body.len() as u32, body.len() as u32 + data.len() as u32);

    let mut frame = Vec::with_capacity(FrameHeader::SIZE + body.len() + data.len());
    let mut hdr_buf = vec![0u8; FrameHeader::SIZE];
    hdr.encode(&mut hdr_buf);
    frame.extend_from_slice(&hdr_buf);
    frame.extend_from_slice(body);
    frame.extend_from_slice(data);
    frame
}

/// Simple handler for testing
pub struct EchoHandler;

#[async_trait::async_trait]
impl NetHandler for EchoHandler {
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
        let server = PowerFsNetServer::bind_with_manager("127.0.0.1", 0, handler)
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
        use crate::middleware::PipelineBuilder;

        struct EchoRequestHandler;
        #[async_trait::async_trait]
        impl NetHandler for EchoRequestHandler {
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
        let handler = Arc::new(EchoRequestHandler) as Arc<dyn NetHandler>;

        let server = PowerFsNetServer::bind_with_pipeline(
            "127.0.0.1",
            0,
            handler,
            Some(pipeline),
            ServerConfig::default(),
        )
        .await
        .unwrap();
        let addr = server.local_addr().unwrap();
        let manager = server.manager().unwrap().clone();

        let server_handle = tokio::spawn(async move {
            server.serve().await.unwrap();
        });

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let client_id = 42u64;
        let client = PowerFsNetClient::new(crate::client::ClientConfig {
            addr: "127.0.0.1".into(),
            port: addr.port(),
            client_id,
            client_type: ClientType::Fuse,
            ..Default::default()
        });

        client.connect().await.unwrap();

        let msg = client
            .send_request(MsgType::Lookup, b"test_body", b"test_data")
            .await
            .unwrap();

        assert!(msg.is_ok());
        assert_eq!(msg.msg_type(), Some(MsgType::Lookup));
        assert_eq!(msg.body, b"test_body");
        assert!(msg.data.is_empty());

        let msg2 = client
            .send_request(MsgType::GetAttr, b"attr_body", &[])
            .await
            .unwrap();

        assert!(msg2.is_ok());
        assert_eq!(msg2.msg_type(), Some(MsgType::GetAttr));
        assert_eq!(msg2.body, b"attr_body");

        // Verify session state
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let sessions = manager.active_count().await;
        assert!(sessions >= 1, "should have at least 1 active session");

        let health = manager.health_check().await;
        assert!(health.healthy);

        let snapshot = manager.get_metrics_snapshot().await;
        assert!(snapshot.total_requests >= 2);

        client.disconnect().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        server_handle.abort();
    }

    /// Test concurrent clients with pipeline metrics
    #[tokio::test]
    async fn test_e2e_concurrent_clients() {
        use crate::client::PowerFsNetClient;
        use crate::middleware::PipelineBuilder;

        struct EchoRequestHandler;
        #[async_trait::async_trait]
        impl NetHandler for EchoRequestHandler {
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
        let handler = Arc::new(EchoRequestHandler) as Arc<dyn NetHandler>;

        let server = PowerFsNetServer::bind_with_pipeline(
            "127.0.0.1",
            0,
            handler,
            Some(pipeline),
            ServerConfig::default(),
        )
        .await
        .unwrap();
        let addr = server.local_addr().unwrap();
        let manager = server.manager().unwrap().clone();

        let server_handle = tokio::spawn(async move {
            server.serve().await.unwrap();
        });

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

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

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let snapshot = manager.get_metrics_snapshot().await;
        assert!(
            snapshot.total_requests >= 50,
            "expected >= 50 requests, got {}",
            snapshot.total_requests
        );

        server_handle.abort();
    }
}
