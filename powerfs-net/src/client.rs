//! PowerFS Net Client - Rust implementation
//!
//! Provides a client that connects to PowerFS servers using the
//! powerfs-net binary protocol.

use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use log::{debug, error, info, warn};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, Semaphore};

use crate::errors::{NetError, NetResult};
use crate::protocol::*;
use crate::serialize::{DirEntry, EntryInfo};

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
    stream: Arc<Mutex<Option<TcpStream>>>,
    seq_counter: AtomicU32,
    inflight_sem: Arc<Semaphore>,
    connected: Arc<parking_lot::Mutex<bool>>,
    /// Optional handler for server-pushed notifications
    notification_handler: Arc<parking_lot::Mutex<Option<Box<dyn NotificationHandler>>>>,
}

impl PowerFsNetClient {
    pub fn new(config: ClientConfig) -> Self {
        Self {
            inflight_sem: Arc::new(Semaphore::new(config.max_inflight_requests as usize)),
            config,
            stream: Arc::new(Mutex::new(None)),
            seq_counter: AtomicU32::new(0),
            connected: Arc::new(parking_lot::Mutex::new(false)),
            notification_handler: Arc::new(parking_lot::Mutex::new(None)),
        }
    }

    /// Set a notification handler to receive server-pushed messages
    pub fn set_notification_handler(&self, handler: Box<dyn NotificationHandler>) {
        let mut h = self.notification_handler.lock();
        *h = Some(handler);
    }

    /// Connect to the server
    pub async fn connect(&self) -> NetResult<()> {
        let mut stream = self.stream.lock().await;
        if stream.is_some() {
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

        // Connect via tokio TcpStream, then enable TCP keepalive through
        // socket2 using the raw fd.  Keepalive detects half-dead
        // connections (idle 60s -> every 10s, 3 retries) that otherwise
        // reveal themselves as "early eof" on the first user write after
        // a far-end silent close (NAT idle drop, LB idle timeout, etc.).
        let connect_result =
            tokio::time::timeout(self.config.connect_timeout, TcpStream::connect(addr)).await;

        let mut tcp_stream = connect_result.map_err(|_| NetError::Timeout)??;
        tcp_stream.set_nodelay(true)?;

        #[cfg(unix)]
        {
            use socket2::TcpKeepalive;
            use std::os::unix::io::{AsRawFd, FromRawFd, IntoRawFd};
            // SAFETY: fd belongs to tcp_stream and is valid.  We temporarily
            // re-wrap as socket2 to configure keepalive, then forget the
            // duplicate fd so the socket lives on owned by tcp_stream.
            let raw_fd = tcp_stream.as_raw_fd();
            let sock2 = unsafe { socket2::Socket::from_raw_fd(raw_fd) };
            let ka = TcpKeepalive::new()
                .with_time(Duration::from_secs(60))
                .with_interval(Duration::from_secs(10))
                .with_retries(3);
            if let Err(e) = sock2.set_tcp_keepalive(&ka) {
                warn!("failed to set TCP keepalive (continuing): {}", e);
            }
            // Don't close the fd on drop – it still belongs to tcp_stream.
            let _ = sock2.into_raw_fd();
        }
        #[cfg(not(unix))]
        {
            // Non-unix platforms still get nodelay above; skip keepalive.
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

        *stream = Some(tcp_stream);
        *self.connected.lock() = true;

        Ok(())
    }

    /// Disconnect from the server
    pub async fn disconnect(&self) -> NetResult<()> {
        let mut stream = self.stream.lock().await;
        if let Some(mut s) = stream.take() {
            // Send close frame
            let frame = build_frame(
                MsgType::Handshake.as_u16(),
                FrameFlags::new(FrameFlags::REQUEST),
                0,
                &[],
                &[],
            );
            let _ = s.write_all(&frame).await;
            drop(s);
        }
        *self.connected.lock() = false;
        Ok(())
    }

    /// Check if connected
    pub fn is_connected(&self) -> bool {
        *self.connected.lock()
    }

    /// Send a request and wait for response
    /// Uses a single lock hold for both send and receive to prevent
    /// response interleaving with concurrent requests.
    /// On any error (including timeout), the stream is cleared so the
    /// next request will establish a fresh connection.
    pub async fn send_request(
        &self,
        msg_type: MsgType,
        body: &[u8],
        data: &[u8],
    ) -> NetResult<NetMessage> {
        debug!("send_request: type={:?}", msg_type);
        // Auto-reconnect if stream is broken
        {
            let stream = self.stream.lock().await;
            let connected = !stream.is_none() && *self.connected.lock();
            debug!(
                "send_request: stream_is_some={}, connected={}",
                !stream.is_none(),
                connected
            );
            if !connected {
                drop(stream);
                warn!("send_request: stream broken, reconnecting...");
                self.reconnect_internal().await?;
            }
        }

        let _permit = self.inflight_sem.clone().acquire_owned().await;
        self.send_request_internal(msg_type, body, data).await
    }

    /// Internal send request (after connection is verified)
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

        // Hold the stream lock for the entire send+receive to prevent
        // response interleaving with concurrent requests
        debug!("send_request_internal: acquiring stream lock");
        let mut stream = self.stream.lock().await;
        debug!("send_request_internal: stream lock acquired");

        // Check we have a valid stream
        if stream.is_none() {
            *self.connected.lock() = false;
            warn!("send_request_internal: stream is None");
            return Err(NetError::NotConnected);
        }

        // Send frame
        debug!("send_request_internal: sending frame, seq={}", seq);
        let send_result = tokio::time::timeout(
            self.config.request_timeout,
            stream.as_mut().unwrap().write_all(&frame),
        )
        .await;

        match send_result {
            Ok(Ok(_)) => {
                debug!("send_request_internal: frame sent, seq={}", seq);
            }
            Ok(Err(e)) => {
                // Clear stream on send error
                warn!("send_request_internal: send error: {:?}", e);
                *stream = None;
                drop(stream);
                self.handle_send_error(&e).await?;
                return Err(NetError::Protocol(e.to_string()));
            }
            Err(_elapsed) => {
                // Clear stream on send timeout
                warn!("send_request_internal: send timeout");
                *stream = None;
                drop(stream);
                self.handle_timeout().await?;
                return Err(NetError::Timeout);
            }
        }

        // Receive response (still holding the lock)
        debug!("send_request_internal: waiting for response, seq={}", seq);
        match self.recv_response_locked(seq, &mut stream).await {
            Ok(response) => {
                debug!("send_request_internal: response received, seq={}", seq);
                Ok(response)
            }
            Err(e) => {
                // Any receive error (including timeout) means the stream
                // state is corrupted. Clear it so we reconnect next time.
                warn!("send_request_internal: recv error: {:?}", e);
                *stream = None;
                drop(stream);
                *self.connected.lock() = false;
                Err(e)
            }
        }
    }

    /// Reconnect to the server (called after a connection failure or a
    /// health-check ping failure).  Up to 3 attempts with short linear
    /// backoff; on final failure the caller should try again later (we
    /// intentionally do not loop forever here).
    pub async fn reconnect_internal(&self) -> NetResult<()> {
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

        let mut stream = self.stream.lock().await;
        let s = stream.as_mut().ok_or(NetError::NotConnected)?;
        s.write_all(&frame).await?;

        debug!("Sent notify: type={:?} seq={}", msg_type, seq);
        Ok(())
    }

    /// Receive response for a specific sequence number (called with stream lock already held)
    ///
    /// This method loops to handle interleaved NOTIFY frames: if a NOTIFY
    /// frame arrives while waiting for a RESPONSE, it dispatches the
    /// notification to the registered handler and continues reading.
    async fn recv_response_locked(
        &self,
        expected_seq: u32,
        stream: &mut tokio::sync::MutexGuard<'_, Option<TcpStream>>,
    ) -> NetResult<NetMessage> {
        loop {
            let s = stream.as_mut().ok_or(NetError::NotConnected)?;

            debug!(
                "recv_response_locked: reading header, expected_seq={}",
                expected_seq
            );
            // Read header
            let mut hdr_buf = vec![0u8; FrameHeader::SIZE];
            let recv_result =
                tokio::time::timeout(self.config.request_timeout, s.read_exact(&mut hdr_buf)).await;

            match recv_result {
                Ok(Ok(_)) => {
                    debug!(
                        "recv_response_locked: header received for seq={}",
                        expected_seq
                    );
                }
                Ok(Err(e)) => {
                    let err_msg = e.to_string();
                    warn!("recv_response_locked: header read error: {:?}", e);
                    self.handle_recv_error(&e).await?;
                    return Err(NetError::Protocol(err_msg));
                }
                Err(_elapsed) => {
                    warn!(
                        "recv_response_locked: header read timeout for seq={}",
                        expected_seq
                    );
                    return Err(NetError::Timeout);
                }
            }

            let header = FrameHeader::decode(&hdr_buf)
                .ok_or_else(|| NetError::Protocol("invalid response header".into()))?;

            // Read body + data
            let data_len = header.data_len as usize;
            let mut all_data = Vec::with_capacity(data_len);
            if data_len > 0 {
                let mut remaining = data_len;
                while remaining > 0 {
                    let chunk = std::cmp::min(remaining, 4096);
                    let mut buf = vec![0u8; chunk];
                    s.read_exact(&mut buf).await?;
                    all_data.extend_from_slice(&buf);
                    remaining -= chunk;
                }
            }

            let message = NetMessage::new(header).with_body(all_data);

            // Check if this is a NOTIFY frame (server-pushed notification)
            if message.header.is_notify() {
                debug!(
                    "recv_response_locked: received NOTIFY frame type={:?}, dispatching to handler",
                    message.msg_type()
                );
                let handler = self.notification_handler.lock();
                if let Some(ref h) = *handler {
                    h.handle_notification(&message);
                }
                // Continue reading for the actual response
                continue;
            }

            debug!(
                "recv_response_locked: got response seq={}, expected={}",
                message.header.seq, expected_seq
            );
            if message.header.seq != expected_seq {
                error!(
                    "Response seq mismatch: expected {}, got {}",
                    expected_seq, message.header.seq
                );
                return Err(NetError::Protocol(format!(
                    "seq mismatch: expected {}, got {}",
                    expected_seq, message.header.seq
                )));
            }

            debug!(
                "Received response: seq={} status={} data_len={}",
                message.header.seq,
                message.header.status,
                message.body.len()
            );

            return Ok(message);
        }
    }

    /// Receive response for a specific sequence number (standalone, acquires its own lock)
    async fn recv_response(&self, expected_seq: u32) -> NetResult<NetMessage> {
        let mut stream = self.stream.lock().await;
        self.recv_response_locked(expected_seq, &mut stream).await
    }

    async fn handle_send_error(&self, e: &std::io::Error) -> NetResult<()> {
        error!("Send error: {:?}", e);
        *self.connected.lock() = false;
        Ok(())
    }

    async fn handle_recv_error(&self, e: &std::io::Error) -> NetResult<()> {
        error!("Receive error: {:?}", e);
        *self.connected.lock() = false;
        Ok(())
    }

    async fn handle_timeout(&self) -> NetResult<()> {
        error!("Request timeout");
        *self.connected.lock() = false;
        Ok(())
    }

    /// Send a ping
    pub async fn ping(&self) -> NetResult<()> {
        let frame = build_frame(
            MsgType::Ping.as_u16(),
            FrameFlags::new(FrameFlags::REQUEST),
            0,
            &[],
            &[],
        );

        {
            let mut stream = self.stream.lock().await;
            let s = stream.as_mut().ok_or(NetError::NotConnected)?;
            s.write_all(&frame).await?;
        }

        // Receive ping response
        let _resp = self.recv_response(0).await?;
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
        let body = crate::serialize::encode_create_req(parent_ino, name, mode, uid, gid)?;
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

    /// Read data from a file
    pub async fn read_data(&self, ino: u64, offset: u64, length: u32) -> NetResult<Vec<u8>> {
        let body = crate::serialize::encode_data_req(ino, offset, length)?;
        let resp = self.send_request(MsgType::Read, &body, &[]).await?;

        if !resp.is_ok() {
            return Err(NetError::ServerError(format!(
                "read failed: status={}",
                resp.header.status
            )));
        }

        Ok(resp.data.clone())
    }

    /// Write data to a file
    pub async fn write_data(&self, ino: u64, offset: u64, data: &[u8]) -> NetResult<()> {
        let body = crate::serialize::encode_data_req(ino, offset, data.len() as u32)?;
        let resp = self.send_request(MsgType::Write, &body, data).await?;

        if !resp.is_ok() {
            return Err(NetError::ServerError(format!(
                "write failed: status={}",
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
