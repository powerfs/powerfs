//! PowerFS Net Client - Rust implementation
//!
//! Provides a client that connects to PowerFS servers using the
//! powerfs-net binary protocol.

use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use log::{debug, error, info, warn};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, Mutex, Semaphore};

use crate::errors::{NetError, NetResult};
use crate::protocol::*;
use crate::serialize::{DirEntry, EntryInfo};

/// Drain all pending requests from a DashMap and notify each waiter with an
/// error (empty-header) NetMessage.  Used by send_task/recv_loop/disconnect
/// when the connection breaks so that no caller hangs waiting for a response
/// that will never arrive.
///
/// DashMap has no `drain()` method like HashMap, so we collect keys first
/// (fast, read-only shard locks), then remove each entry (write shard lock).
/// This avoids holding any single shard lock for the duration of all sends.
fn drain_pending_with_error(pr: &DashMap<u32, oneshot::Sender<NetMessage>>) {
    let keys: Vec<u32> = pr.iter().map(|e| *e.key()).collect();
    for key in keys {
        if let Some((_, sender)) = pr.remove(&key) {
            let _ = sender.send(NetMessage::new(FrameHeader::new(
                0,
                FrameFlags::new(0),
                0,
                0,
            )));
        }
    }
}

/// Configuration for the net client
#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub addr: String,
    pub port: u16,
    pub client_id: u64,
    pub client_type: ClientType,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub max_retries: u32,
    pub retry_delay: Duration,
    pub heartbeat_interval: Duration,
    pub max_inflight_requests: u32,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1".into(),
            port: 9333,
            client_id: 0,
            client_type: ClientType::Fuse,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
            max_retries: 3,
            retry_delay: Duration::from_millis(100),
            heartbeat_interval: Duration::from_secs(30),
            max_inflight_requests: 256,
        }
    }
}

/// Trait for handling server-pushed notifications (Server→Client)
///
/// Implement this trait to process asynchronous messages from the server,
/// such as inode invalidation events.
pub trait NotificationHandler: Send + Sync {
    /// Called when a NOTIFY frame is received from the server
    fn handle_notification(&self, msg: &NetMessage);
}

/// PowerFS Net Client
pub struct PowerFsNetClient {
    pub config: ClientConfig,
    write_half: Arc<Mutex<Option<OwnedWriteHalf>>>,
    read_half: Arc<Mutex<Option<OwnedReadHalf>>>,
    seq_counter: AtomicU32,
    inflight_sem: Arc<Semaphore>,
    connected: Arc<parking_lot::Mutex<bool>>,
    /// Optional handler for server-pushed notifications
    notification_handler: Arc<parking_lot::Mutex<Option<Box<dyn NotificationHandler>>>>,
    /// Pending requests waiting for responses (seq → oneshot sender).
    /// Keys are inserted by send_request_internal and removed by recv_loop.
    ///
    /// Uses DashMap (16-way sharded locks) instead of a single Mutex<HashMap>
    /// to reduce lock contention under high concurrency.  Each shard has its
    /// own RwLock, so concurrent insert/remove on different seqs proceed in
    /// parallel.
    pending_requests: Arc<DashMap<u32, oneshot::Sender<NetMessage>>>,
    /// Handle for the background receive loop task.
    recv_loop_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    /// Sender for frames to the dedicated send_task (eliminates write_half lock contention).
    /// None when not connected. Each frame is a complete NetMessage frame to write_all.
    frame_tx: Arc<Mutex<Option<mpsc::UnboundedSender<Vec<u8>>>>>,
    /// Handle for the background send task.
    send_task_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    /// Reconnect coordination flag: prevents concurrent reconnect_internal calls.
    reconnecting: Arc<AtomicBool>,
}

impl PowerFsNetClient {
    pub fn new(config: ClientConfig) -> Self {
        Self {
            inflight_sem: Arc::new(Semaphore::new(config.max_inflight_requests as usize)),
            config,
            write_half: Arc::new(Mutex::new(None)),
            read_half: Arc::new(Mutex::new(None)),
            seq_counter: AtomicU32::new(0),
            connected: Arc::new(parking_lot::Mutex::new(false)),
            notification_handler: Arc::new(parking_lot::Mutex::new(None)),
            pending_requests: Arc::new(DashMap::new()),
            recv_loop_handle: Arc::new(Mutex::new(None)),
            frame_tx: Arc::new(Mutex::new(None)),
            send_task_handle: Arc::new(Mutex::new(None)),
            reconnecting: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Set a notification handler to receive server-pushed messages
    pub fn set_notification_handler(&self, handler: Box<dyn NotificationHandler>) {
        let mut h = self.notification_handler.lock();
        *h = Some(handler);
    }

    /// Connect to the server
    pub async fn connect(&self) -> NetResult<()> {
        // Check if already connected (frame_tx exists and connected flag is set)
        if *self.connected.lock() && self.frame_tx.lock().await.is_some() {
            return Ok(());
        }

        // Fast path: if addr is already an IP, construct SocketAddr directly (no DNS)
        let addr: SocketAddr = match self.config.addr.parse::<IpAddr>() {
            Ok(ip) => SocketAddr::new(ip, self.config.port),
            Err(_) => {
                // Hostname: use DNS resolution
                let addr_str = format!("{}:{}", self.config.addr, self.config.port);
                addr_str
                    .to_socket_addrs()
                    .map_err(|e| NetError::Connection(format!("DNS resolution failed: {}", e)))?
                    .next()
                    .ok_or_else(|| NetError::Connection("no addresses resolved".into()))?
            }
        };

        info!("Connecting to {}:{}", self.config.addr, self.config.port);

        let connect_result =
            tokio::time::timeout(self.config.connect_timeout, TcpStream::connect(addr)).await;

        let mut tcp_stream = connect_result.map_err(|_| NetError::Timeout)??;
        tcp_stream.set_nodelay(true)?;

        #[cfg(unix)]
        {
            use socket2::TcpKeepalive;
            use std::os::unix::io::{AsRawFd, FromRawFd, IntoRawFd};
            let raw_fd = tcp_stream.as_raw_fd();
            let sock2 = unsafe { socket2::Socket::from_raw_fd(raw_fd) };
            let ka = TcpKeepalive::new()
                .with_time(Duration::from_secs(60))
                .with_interval(Duration::from_secs(10))
                .with_retries(3);
            if let Err(e) = sock2.set_tcp_keepalive(&ka) {
                warn!("failed to set TCP keepalive (continuing): {}", e);
            }
            let _ = sock2.into_raw_fd();
        }
        #[cfg(not(unix))]
        {
            let _ = socket2::TcpKeepalive::new();
        }

        // Send handshake
        let req = HandshakeRequest::new(self.config.client_type, self.config.client_id);
        let mut buf = vec![0u8; HandshakeRequest::SIZE];
        req.encode(&mut buf);
        tcp_stream.write_all(&buf).await?;

        // Receive handshake response
        let mut resp_buf = vec![0u8; HandshakeResponse::SIZE];
        tcp_stream.read_exact(&mut resp_buf).await?;

        let resp = HandshakeResponse::decode(&resp_buf)
            .ok_or_else(|| NetError::Protocol("invalid handshake response".into()))?;

        if !resp.is_ok() {
            return Err(NetError::Connection("handshake rejected".into()));
        }

        info!("Connected to server_id={}", resp.server_id);

        // Split stream into read and write halves for pipeline mode
        let (read_half, write_half) = tcp_stream.into_split();
        *self.write_half.lock().await = Some(write_half);
        *self.read_half.lock().await = Some(read_half);
        *self.connected.lock() = true;

        // Start background send task (owns write_half via mpsc, no lock contention)
        self.start_send_task().await;

        // Start background receive loop
        self.start_recv_loop().await;

        Ok(())
    }

    /// Start the background send task that owns write_half and writes frames
    /// received from the mpsc channel. This eliminates write_half lock contention
    /// among concurrent requests.
    async fn start_send_task(&self) {
        // Abort any existing send_task
        let mut handle_guard = self.send_task_handle.lock().await;
        if let Some(handle) = handle_guard.take() {
            handle.abort();
        }

        // Take ownership of write_half from the Mutex (send_task owns it)
        let write_half = self.write_half.lock().await.take();
        let wh = match write_half {
            Some(w) => w,
            None => {
                warn!("start_send_task: write_half is None");
                return;
            }
        };

        // Create mpsc channel for sending frames
        let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
        *self.frame_tx.lock().await = Some(tx);

        let connected = self.connected.clone();
        let pending_requests = self.pending_requests.clone();
        let write_timeout = self.config.request_timeout;

        let handle = tokio::spawn(async move {
            info!(
                "PowerFsNetClient: send_task started (write_timeout={:?})",
                write_timeout
            );
            let mut wh = wh;
            while let Some(frame) = rx.recv().await {
                // Write frame with a timeout. If write_all blocks (TCP buffer
                // full because the server is slow or unresponsive), the timeout
                // fires, the connection is marked dead, and ALL pending requests
                // are drained with error responses. This prevents a single
                // stuck write from blocking the entire send queue for 30s.
                //
                // Note: a timed-out write_all may leave a partial frame in the
                // TCP buffer, corrupting the stream.  This is acceptable
                // because we mark the connection as dead and force a reconnect.
                match tokio::time::timeout(write_timeout, wh.write_all(&frame)).await {
                    Ok(Ok(())) => { /* frame sent, response will arrive via recv_loop */ }
                    Ok(Err(e)) => {
                        warn!("send_task: write error: {:?}", e);
                        *connected.lock() = false;
                        drain_pending_with_error(&pending_requests);
                        break;
                    }
                    Err(_) => {
                        warn!(
                            "send_task: write timeout after {:?} (connection may be stuck); \
                             draining all pending requests and marking connection dead",
                            write_timeout
                        );
                        *connected.lock() = false;
                        drain_pending_with_error(&pending_requests);
                        break;
                    }
                }
            }
            info!("PowerFsNetClient: send_task stopped");
        });

        *handle_guard = Some(handle);
    }

    /// Start the background receive loop that reads responses and dispatches
    /// them to pending requests by seq number.
    async fn start_recv_loop(&self) {
        // Abort any existing recv_loop
        let mut handle_guard = self.recv_loop_handle.lock().await;
        if let Some(handle) = handle_guard.take() {
            handle.abort();
        }

        let read_half = self.read_half.clone();
        let pending_requests = self.pending_requests.clone();
        let notification_handler = self.notification_handler.clone();
        let connected = self.connected.clone();

        let handle = tokio::spawn(async move {
            info!("PowerFsNetClient: recv_loop started");
            loop {
                // Acquire read_half lock just to get a mutable reference,
                // but we need to hold it for the entire read to prevent
                // reconnection races.
                let mut rh = read_half.lock().await;
                let reader = match rh.as_mut() {
                    Some(r) => r,
                    None => {
                        debug!("recv_loop: read_half is None, exiting");
                        break;
                    }
                };

                // Read header — no timeout; block until data arrives or
                // connection breaks.  A timeout here would prematurely kill
                // the recv_loop during idle periods (no pending requests),
                // causing the next request to fail with "not connected".
                let mut hdr_buf = vec![0u8; FrameHeader::SIZE];
                let read_result = reader.read_exact(&mut hdr_buf).await;

                let header = match read_result {
                    Ok(_) => match FrameHeader::decode(&hdr_buf) {
                        Some(h) => h,
                        None => {
                            warn!("recv_loop: invalid header, skipping");
                            continue;
                        }
                    },
                    Err(e) => {
                        warn!("recv_loop: header read error: {:?}", e);
                        *connected.lock() = false;
                        // Notify all pending requests of the error
                        drain_pending_with_error(&pending_requests);
                        break;
                    }
                };

                // Read body + data (header.data_len covers both body and data segments)
                let data_len = header.data_len as usize;
                let mut all_data = Vec::with_capacity(data_len);
                if data_len > 0 {
                    all_data.resize(data_len, 0u8);
                    if let Err(e) = reader.read_exact(&mut all_data).await {
                        warn!("recv_loop: data read error: {:?}", e);
                        *connected.lock() = false;
                        // Notify all pending requests of the error
                        drain_pending_with_error(&pending_requests);
                        break;
                    }
                }

                let message = NetMessage::new(header).with_body(all_data);

                let seq = message.header.seq;

                // Handle NOTIFY frames (server-pushed notifications)
                if message.header.is_notify() {
                    debug!(
                        "recv_loop: received NOTIFY frame type={:?}",
                        message.msg_type()
                    );
                    let handler = notification_handler.lock();
                    if let Some(ref h) = *handler {
                        h.handle_notification(&message);
                    }
                    continue;
                }

                // Dispatch to pending request by seq (DashMap: no async lock)
                if let Some((_, sender)) = pending_requests.remove(&seq) {
                    debug!(
                        "recv_loop: dispatched response seq={}, status={}",
                        seq, message.header.status
                    );
                    let _ = sender.send(message);
                } else {
                    warn!("recv_loop: no pending request for seq={}, dropping", seq);
                }
            }
            info!("PowerFsNetClient: recv_loop stopped");
        });

        *handle_guard = Some(handle);
    }

    /// Disconnect from the server
    pub async fn disconnect(&self) -> NetResult<()> {
        // Stop send_task (drop frame_tx to signal send_task to exit)
        {
            let mut frame_tx_guard = self.frame_tx.lock().await;
            *frame_tx_guard = None;
        }
        {
            let mut handle_guard = self.send_task_handle.lock().await;
            if let Some(handle) = handle_guard.take() {
                handle.abort();
            }
        }

        // Stop recv_loop
        {
            let mut handle_guard = self.recv_loop_handle.lock().await;
            if let Some(handle) = handle_guard.take() {
                handle.abort();
            }
        }

        // Clear write_half and read_half
        *self.write_half.lock().await = None;
        *self.read_half.lock().await = None;

        // Clear pending requests
        drain_pending_with_error(&self.pending_requests);

        *self.connected.lock() = false;
        Ok(())
    }

    /// Check if connected
    pub fn is_connected(&self) -> bool {
        *self.connected.lock()
    }

    /// Send a request and wait for response
    /// Uses the background send_task (mpsc channel) to write frames without
    /// lock contention, and the background recv_loop to dispatch responses.
    /// On any error (including timeout), the request fails but the connection
    /// is NOT destroyed (only real I/O errors trigger reconnect).
    pub async fn send_request(
        &self,
        msg_type: MsgType,
        body: &[u8],
        data: &[u8],
    ) -> NetResult<NetMessage> {
        debug!("send_request: type={:?}", msg_type);
        // Auto-reconnect if stream is broken (with coordination to prevent
        // concurrent reconnect storms)
        {
            let frame_tx = self.frame_tx.lock().await;
            let connected = frame_tx.is_some() && *self.connected.lock();
            debug!(
                "send_request: frame_tx_is_some={}, connected={}",
                frame_tx.is_some(),
                connected
            );
            if !connected {
                drop(frame_tx);
                // Coordinate reconnect: only one request reconnects at a time
                if self
                    .reconnecting
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    warn!("send_request: stream broken, reconnecting...");
                    let result = self.reconnect_internal().await;
                    self.reconnecting.store(false, Ordering::Release);
                    result?;
                } else {
                    // Another request is already reconnecting — wait for it
                    debug!("send_request: waiting for concurrent reconnect...");
                    let waited = Duration::from_millis(0);
                    let max_wait = self.config.connect_timeout * 3;
                    let start = std::time::Instant::now();
                    while self.reconnecting.load(Ordering::Acquire) && start.elapsed() < max_wait {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    if !*self.connected.lock() {
                        return Err(NetError::Connection(
                            "reconnect by concurrent caller failed".into(),
                        ));
                    }
                    debug!(
                        "send_request: concurrent reconnect done, waited {:?}",
                        waited
                    );
                }
            }
        }

        let _permit = self.inflight_sem.clone().acquire_owned().await;
        self.send_request_internal(msg_type, body, data).await
    }

    /// Internal send request (after connection is verified).
    ///
    /// Pipeline mode: register a oneshot channel in `pending_requests`, push
    /// the frame to the background send_task via mpsc channel (no lock
    /// contention), then await the response from the background recv_loop.
    ///
    /// Key fix: request timeout does NOT destroy the connection. Only real
    /// I/O errors in send_task trigger connection teardown. This prevents
    /// in-flight data loss when one request times out.
    async fn send_request_internal(
        &self,
        msg_type: MsgType,
        body: &[u8],
        data: &[u8],
    ) -> NetResult<NetMessage> {
        let seq = self.seq_counter.fetch_add(1, Ordering::Relaxed) + 1;

        let frame = build_frame(
            msg_type.as_u16(),
            FrameFlags::new(FrameFlags::REQUEST),
            seq,
            body,
            data,
        );

        debug!(
            "Sending request: type={:?} seq={} body_len={} data_len={}",
            msg_type,
            seq,
            body.len(),
            data.len()
        );

        // Create oneshot channel and register pending request
        let (tx, rx) = oneshot::channel::<NetMessage>();
        self.pending_requests.insert(seq, tx);

        // Push frame to send_task via mpsc channel (no write_half lock needed)
        {
            let frame_tx = self.frame_tx.lock().await;
            match frame_tx.as_ref() {
                Some(sender) => {
                    if let Err(e) = sender.send(frame) {
                        // send_task has exited (connection broken)
                        warn!(
                            "send_request_internal: frame_tx send failed for seq={}: {}",
                            seq, e
                        );
                        self.pending_requests.remove(&seq);
                        *self.connected.lock() = false;
                        return Err(NetError::NotConnected);
                    }
                    debug!(
                        "send_request_internal: frame pushed to send_task, seq={}",
                        seq
                    );
                }
                None => {
                    self.pending_requests.remove(&seq);
                    *self.connected.lock() = false;
                    warn!("send_request_internal: frame_tx is None");
                    return Err(NetError::NotConnected);
                }
            }
        }

        // Wait for response via oneshot (with timeout).
        // Timeout here does NOT destroy the connection — the frame may still
        // be in the send_task's queue or in the TCP buffer. Other requests'
        // responses can still arrive via recv_loop.
        debug!("send_request_internal: waiting for response, seq={}", seq);
        match tokio::time::timeout(self.config.request_timeout, rx).await {
            Ok(Ok(response)) => {
                debug!("send_request_internal: response received, seq={}", seq);
                Ok(response)
            }
            Ok(Err(_recv_err)) => {
                // oneshot sender was dropped (likely recv_loop exited and
                // drained pending_requests, or send_task drained on error)
                warn!("send_request_internal: sender dropped for seq={}", seq);
                Err(NetError::Connection("connection terminated".into()))
            }
            Err(_elapsed) => {
                warn!("send_request_internal: response timeout for seq={}", seq);
                // Remove pending request on timeout (do NOT set connected=false)
                self.pending_requests.remove(&seq);
                Err(NetError::Timeout)
            }
        }
    }

    /// Reconnect to the server (called after a connection failure or a
    /// health-check ping failure).  Up to 3 attempts with short linear
    /// backoff; on final failure the caller should try again later (we
    /// intentionally do not loop forever here).
    pub async fn reconnect_internal(&self) -> NetResult<()> {
        // Stop send_task (drop frame_tx to signal send_task to exit)
        {
            let mut frame_tx_guard = self.frame_tx.lock().await;
            *frame_tx_guard = None;
        }
        {
            let mut handle_guard = self.send_task_handle.lock().await;
            if let Some(handle) = handle_guard.take() {
                handle.abort();
            }
        }

        // Stop recv_loop, clear halves before reconnecting.
        // connect() will restart send_task, recv_loop and set new halves.
        {
            let mut handle_guard = self.recv_loop_handle.lock().await;
            if let Some(handle) = handle_guard.take() {
                handle.abort();
            }
        }
        *self.write_half.lock().await = None;
        *self.read_half.lock().await = None;
        *self.connected.lock() = false;

        // Try up to 3 times with backoff
        for attempt in 1..=3 {
            info!("Reconnect attempt {}", attempt);
            match self.connect().await {
                Ok(()) => {
                    info!("Reconnected successfully");
                    return Ok(());
                }
                Err(e) => {
                    warn!("Reconnect attempt {} failed: {}", attempt, e);
                    if attempt < 3 {
                        tokio::time::sleep(Duration::from_millis(100 * attempt as u64)).await;
                    }
                }
            }
        }
        error!("Failed to reconnect after 3 attempts");
        Err(NetError::Connection("reconnection failed".into()))
    }

    /// Send a notification (no response expected)
    pub async fn send_notify(&self, msg_type: MsgType, body: &[u8]) -> NetResult<()> {
        if !self.is_connected() {
            return Err(NetError::NotConnected);
        }

        let seq = self.seq_counter.fetch_add(1, Ordering::Relaxed) + 1;

        let frame = build_frame(
            msg_type.as_u16(),
            FrameFlags::new(FrameFlags::NOTIFY),
            seq,
            body,
            &[],
        );

        // Push to send_task via mpsc (consistent with send_request_internal)
        let frame_tx = self.frame_tx.lock().await;
        match frame_tx.as_ref() {
            Some(sender) => {
                sender
                    .send(frame)
                    .map_err(|_| NetError::Connection("send_task exited".into()))?;
                debug!("Sent notify: type={:?} seq={}", msg_type, seq);
                Ok(())
            }
            None => Err(NetError::NotConnected),
        }
    }

    /// Send a ping
    pub async fn ping(&self) -> NetResult<()> {
        let _resp = self.send_request_internal(MsgType::Ping, &[], &[]).await?;
        Ok(())
    }

    // ========================================================================
    // High-level convenience methods
    // ========================================================================

    /// Lookup a directory entry
    pub async fn lookup(&self, parent_ino: u64, name: &str) -> NetResult<EntryInfo> {
        let body = crate::serialize::encode_lookup_req(parent_ino, name)?;
        let resp = self.send_request(MsgType::Lookup, &body, &[]).await?;

        if !resp.is_ok() {
            return Err(NetError::ServerError(format!(
                "lookup failed: status={}",
                resp.header.status
            )));
        }

        crate::serialize::decode_entry_resp(&resp.body)
    }

    /// Create a file or directory
    pub async fn create(
        &self,
        parent_ino: u64,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> NetResult<EntryInfo> {
        let body = crate::serialize::encode_create_req(parent_ino, name, mode, uid, gid, None)?;
        let resp = self.send_request(MsgType::Create, &body, &[]).await?;

        if !resp.is_ok() {
            return Err(NetError::ServerError(format!(
                "create failed: status={}",
                resp.header.status
            )));
        }

        crate::serialize::decode_entry_resp(&resp.body)
    }

    /// Delete a file or directory
    pub async fn delete(&self, ino: u64, is_dir: bool) -> NetResult<()> {
        let body = crate::serialize::encode_delete_req(ino, is_dir)?;
        let msg_type = if is_dir {
            MsgType::Rmdir
        } else {
            MsgType::Unlink
        };
        let resp = self.send_request(msg_type, &body, &[]).await?;

        if !resp.is_ok() {
            return Err(NetError::ServerError(format!(
                "delete failed: status={}",
                resp.header.status
            )));
        }

        Ok(())
    }

    /// Rename a file or directory
    pub async fn rename(
        &self,
        old_parent_ino: u64,
        old_name: &str,
        new_parent_ino: u64,
        new_name: &str,
    ) -> NetResult<()> {
        let body = crate::serialize::encode_rename_req(
            old_parent_ino,
            old_name,
            new_parent_ino,
            new_name,
        )?;
        let resp = self.send_request(MsgType::Rename, &body, &[]).await?;

        if !resp.is_ok() {
            return Err(NetError::ServerError(format!(
                "rename failed: status={}",
                resp.header.status
            )));
        }

        Ok(())
    }

    /// Create a symbolic link
    pub async fn create_symlink(
        &self,
        parent_ino: u64,
        name: &str,
        target: &str,
    ) -> NetResult<EntryInfo> {
        let body = crate::serialize::encode_symlink_req(parent_ino, name, target)?;
        let resp = self.send_request(MsgType::Symlink, &body, &[]).await?;

        if !resp.is_ok() {
            return Err(NetError::ServerError(format!(
                "symlink failed: status={}",
                resp.header.status
            )));
        }

        crate::serialize::decode_entry_resp(&resp.body)
    }

    /// Read a symbolic link target
    pub async fn readlink(&self, ino: u64) -> NetResult<String> {
        let body = crate::serialize::encode_readlink_req(ino)?;
        let resp = self.send_request(MsgType::Readlink, &body, &[]).await?;

        if !resp.is_ok() {
            return Err(NetError::ServerError(format!(
                "readlink failed: status={}",
                resp.header.status
            )));
        }

        crate::serialize::decode_readlink_resp(&resp.body)
    }

    /// Create a hard link
    pub async fn create_hard_link(&self, ino: u64, parent_ino: u64, name: &str) -> NetResult<()> {
        let body = crate::serialize::encode_link_req(ino, parent_ino, name)?;
        let resp = self.send_request(MsgType::Link, &body, &[]).await?;

        if !resp.is_ok() {
            return Err(NetError::ServerError(format!(
                "link failed: status={}",
                resp.header.status
            )));
        }

        Ok(())
    }

    /// Get file attributes
    pub async fn getattr(&self, ino: u64) -> NetResult<EntryInfo> {
        let body = crate::serialize::encode_getattr_req(ino)?;
        let resp = self.send_request(MsgType::GetAttr, &body, &[]).await?;

        if !resp.is_ok() {
            return Err(NetError::ServerError(format!(
                "getattr failed: status={}",
                resp.header.status
            )));
        }

        crate::serialize::decode_entry_resp(&resp.body)
    }

    /// Set file attributes
    pub async fn setattr(
        &self,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
    ) -> NetResult<EntryInfo> {
        let body = crate::serialize::encode_setattr_req(ino, mode, uid, gid, size)?;
        let resp = self.send_request(MsgType::SetAttr, &body, &[]).await?;

        if !resp.is_ok() {
            return Err(NetError::ServerError(format!(
                "setattr failed: status={}",
                resp.header.status
            )));
        }

        crate::serialize::decode_entry_resp(&resp.body)
    }

    /// Read directory entries
    pub async fn readdir(&self, ino: u64, offset: u64, count: u32) -> NetResult<Vec<DirEntry>> {
        let body = crate::serialize::encode_readdir_req(ino, offset, count)?;
        let resp = self.send_request(MsgType::ReadDir, &body, &[]).await?;

        if !resp.is_ok() {
            return Err(NetError::ServerError(format!(
                "readdir failed: status={}",
                resp.header.status
            )));
        }

        crate::serialize::decode_readdir_resp(&resp.body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_client_creation() {
        let config = ClientConfig::default();
        let client = PowerFsNetClient::new(config);
        assert!(!client.is_connected());
    }

    #[tokio::test]
    async fn test_not_connected_error() {
        let config = ClientConfig::default();
        let client = PowerFsNetClient::new(config);
        let result = client.send_request(MsgType::Ping, &[], &[]).await;
        assert!(result.is_err());
    }
}
