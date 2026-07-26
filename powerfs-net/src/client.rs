//! PowerFS Net Client - Rust implementation
//!
//! Provides a client that connects to PowerFS servers using the
//! powerfs-net binary protocol.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use log::{debug, error, info, warn};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, Semaphore};

use crate::errors::{NetError, NetResult};
use crate::protocol::*;
use crate::serialize::EntryInfo;

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

/// PowerFS Net Client
pub struct PowerFsNetClient {
    pub config: ClientConfig,
    stream: Arc<Mutex<Option<TcpStream>>>,
    seq_counter: AtomicU32,
    inflight_sem: Arc<Semaphore>,
    connected: Arc<parking_lot::Mutex<bool>>,
}

impl PowerFsNetClient {
    pub fn new(config: ClientConfig) -> Self {
        Self {
            inflight_sem: Arc::new(Semaphore::new(config.max_inflight_requests as usize)),
            config,
            stream: Arc::new(Mutex::new(None)),
            seq_counter: AtomicU32::new(0),
            connected: Arc::new(parking_lot::Mutex::new(false)),
        }
    }

    /// Connect to the server
    pub async fn connect(&self) -> NetResult<()> {
        let mut stream = self.stream.lock().await;
        if stream.is_some() {
            return Ok(());
        }

        let addr: SocketAddr = format!("{}:{}", self.config.addr, self.config.port)
            .parse()
            .map_err(|e| NetError::Connection(format!("invalid address: {}", e)))?;

        info!("Connecting to {}:{}", self.config.addr, self.config.port);

        let connect_result =
            tokio::time::timeout(self.config.connect_timeout, TcpStream::connect(addr)).await;

        let mut tcp_stream = connect_result.map_err(|_| NetError::Timeout)??;
        tcp_stream.set_nodelay(true)?;

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
    pub async fn send_request(
        &self,
        msg_type: MsgType,
        body: &[u8],
        data: &[u8],
    ) -> NetResult<NetMessage> {
        if !self.is_connected() {
            return Err(NetError::NotConnected);
        }

        let _permit = self.inflight_sem.clone().acquire_owned().await;
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

        // Send frame
        {
            let mut stream = self.stream.lock().await;
            let s = stream.as_mut().ok_or(NetError::NotConnected)?;

            let send_result =
                tokio::time::timeout(self.config.request_timeout, s.write_all(&frame)).await;

            match send_result {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    let err_msg = e.to_string();
                    self.handle_send_error(&e).await?;
                    return Err(NetError::Protocol(err_msg));
                }
                Err(_elapsed) => {
                    self.handle_timeout().await?;
                    return Err(NetError::Timeout);
                }
            }
        }

        // Receive response
        let response = self.recv_response(seq).await?;

        Ok(response)
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

    /// Receive response for a specific sequence number
    async fn recv_response(&self, expected_seq: u32) -> NetResult<NetMessage> {
        let mut stream = self.stream.lock().await;
        let s = stream.as_mut().ok_or(NetError::NotConnected)?;

        // Read header
        let mut hdr_buf = vec![0u8; FrameHeader::SIZE];
        let recv_result =
            tokio::time::timeout(self.config.request_timeout, s.read_exact(&mut hdr_buf)).await;

        match recv_result {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                let err_msg = e.to_string();
                self.handle_recv_error(&e).await?;
                return Err(NetError::Protocol(err_msg));
            }
            Err(_elapsed) => {
                return Err(NetError::Timeout);
            }
        }

        let header = FrameHeader::decode(&hdr_buf)
            .ok_or_else(|| NetError::Protocol("invalid response header".into()))?;

        if header.seq != expected_seq {
            warn!(
                "Response seq mismatch: expected {}, got {}",
                expected_seq, header.seq
            );
        }

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

        debug!(
            "Received response: seq={} status={} data_len={}",
            message.header.seq,
            message.header.status,
            message.body.len()
        );

        Ok(message)
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
