//! PowerFS Net Server - Rust implementation
//!
//! Provides a server that accepts connections and dispatches
//! requests to handler implementations.

use std::net::SocketAddr;
use std::sync::Arc;

use log::{debug, error, info};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

use crate::errors::{NetError, NetResult};
use crate::protocol::*;

/// Handler trait for processing net requests
#[async_trait::async_trait]
pub trait PowerFsNetHandler: Send + Sync {
    /// Handle a request and return a response
    async fn handle_request(&self, msg: &NetMessage) -> NetResult<NetMessage>;

    /// Called when a client connects
    async fn on_connect(&self, _client_id: u64, _client_type: ClientType) {}

    /// Called when a client disconnects
    async fn on_disconnect(&self, _client_id: u64) {}
}

/// PowerFS Net Server
pub struct PowerFsNetServer {
    listener: TcpListener,
    handler: Arc<dyn PowerFsNetHandler>,
}

impl PowerFsNetServer {
    pub async fn bind(
        addr: &str,
        port: u16,
        handler: Arc<dyn PowerFsNetHandler>,
    ) -> NetResult<Self> {
        let socket_addr: SocketAddr = format!("{}:{}", addr, port)
            .parse()
            .map_err(|e| NetError::Protocol(format!("invalid address: {}", e)))?;

        let listener = TcpListener::bind(socket_addr).await?;
        info!("PowerFS Net server listening on {}:{}", addr, port);

        Ok(Self { listener, handler })
    }

    /// Start serving (runs until stopped)
    pub async fn serve(&self) -> NetResult<()> {
        info!("Starting to accept connections...");

        loop {
            match self.listener.accept().await {
                Ok((stream, addr)) => {
                    info!("New connection from {}", addr);
                    let handler = self.handler.clone();
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(stream, handler).await {
                            error!("Connection error from {}: {:?}", addr, e);
                        }
                    });
                }
                Err(e) => {
                    error!("Accept error: {:?}", e);
                }
            }
        }
    }

    /// Get the local address
    pub fn local_addr(&self) -> NetResult<SocketAddr> {
        self.listener.local_addr().map_err(NetError::Io)
    }

    /// Handle a single connection
    async fn handle_connection(
        stream: TcpStream,
        handler: Arc<dyn PowerFsNetHandler>,
    ) -> NetResult<()> {
        let peer = stream.peer_addr()?;
        info!("Handling connection from {}", peer);

        stream.set_nodelay(true)?;
        let stream = Arc::new(Mutex::new(stream));

        // Phase 1: Handshake
        let client_id = Self::handle_handshake(stream.clone(), handler.clone()).await?;

        handler.on_disconnect(client_id).await;

        Ok(())
    }

    /// Handle handshake and return client_id
    async fn handle_handshake(
        stream: Arc<Mutex<TcpStream>>,
        handler: Arc<dyn PowerFsNetHandler>,
    ) -> NetResult<u64> {
        // Read handshake request
        let client_id = {
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
                "Handshake: client_id={} client_type={:?}",
                req.client_id, client_type
            );

            // Send handshake response
            let resp = HandshakeResponse::ok(0);
            let mut resp_buf = vec![0u8; HandshakeResponse::SIZE];
            resp.encode(&mut resp_buf);
            s.write_all(&resp_buf).await?;

            // Notify handler
            handler.on_connect(req.client_id, client_type).await;

            req.client_id
        };

        // Phase 2: Message loop
        Self::message_loop(stream.clone(), handler.clone()).await?;

        Ok(client_id)
    }

    /// Main message loop for a connection
    async fn message_loop(
        stream: Arc<Mutex<TcpStream>>,
        handler: Arc<dyn PowerFsNetHandler>,
    ) -> NetResult<()> {
        loop {
            // Read header
            let header = {
                let mut s = stream.lock().await;
                let mut hdr_buf = vec![0u8; FrameHeader::SIZE];

                let read_result = s.read_exact(&mut hdr_buf).await;
                if read_result.is_err() {
                    info!("Client disconnected");
                    return Ok(());
                }

                FrameHeader::decode(&hdr_buf)
                    .ok_or_else(|| NetError::Protocol("invalid frame header".into()))?
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

            // Split body into body and data segments
            // Convention: For simplicity, all data is in "body"
            // In the future, we could have a separate data segment for large payloads

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
                let response = handler.handle_request(&message).await?;

                // Send response - use the response header directly to preserve status
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
                    s.write_all(&frame).await?;
                }
            }

            // Handle notify (no response expected)
            if message.header.flags & FrameFlags::NOTIFY != 0 {
                // Just log and continue
                debug!("Received notify: seq={}", message.header.seq);
            }
        }
    }
}

/// Simple handler for testing
pub struct EchoHandler;

#[async_trait::async_trait]
impl PowerFsNetHandler for EchoHandler {
    async fn handle_request(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        // Echo back the request as response
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
}
