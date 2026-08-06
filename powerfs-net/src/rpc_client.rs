//! Unified RPC client for inter-service TLV communication.
//!
//! Replaces the per-service hand-rolled connect→handshake→frame→read loops
//! that were duplicated across Master/Filer/Volume (raft messages, register
//! filer, heartbeat). All inter-service RPCs now go through this single API.
//!
//! # Two usage modes
//!
//! - **One-shot** ([`call_once`]): opens a fresh short-lived connection per
//!   call. Suitable for low-frequency RPCs (RegisterFiler, zone queries).
//!   No state to manage; the caller just gets a reply.
//! - **Persistent** ([`NetRpcClient`]): keeps the connection open across
//!   multiple `call()`s. Suitable for high-frequency RPCs (Raft messages,
//!   Heartbeat) where connection setup cost matters.
//!
//! # Wire sequence (both modes)
//!
//! 1. TCP connect (with timeout)
//! 2. powerfs-net handshake (HandshakeRequest with channel → HandshakeResponse)
//! 3. Send TLV frame (`build_frame_with_route_hash` with `FrameFlags::REQUEST`)
//! 4. Read response FrameHeader + body
//!
//! Redirection (STATUS_ERR_REDIRECT) is NOT handled here — it is a routing
//! concern that belongs to the caller (e.g. heartbeat/master-client loops
//! track the leader and retry). This module only handles the transport.

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::errors::{NetError, NetResult};
use crate::protocol::{
    build_frame_with_route_hash, calc_route_hash, FrameFlags, FrameHeader, HandshakeRequest,
    HandshakeResponse,
};
use crate::{ClientType, MsgType, STATUS_OK};

/// Default timeouts for one-shot RPCs.
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);

/// Reply returned by an RPC call.
#[derive(Debug, Clone)]
pub struct RpcReply {
    /// Frame status code (e.g. STATUS_OK, STATUS_ERR_REDIRECT, ...).
    pub status: u16,
    /// Response body bytes (TLV-encoded payload or error string).
    pub body: Vec<u8>,
}

impl RpcReply {
    /// True if the server returned STATUS_OK.
    pub fn is_ok(&self) -> bool {
        self.status == STATUS_OK
    }

    /// Returns the body as a UTF-8 lossy string (useful for error bodies).
    pub fn body_str(&self) -> std::borrow::Cow<'_, str> {
        String::from_utf8_lossy(&self.body)
    }
}

/// Configuration for a one-shot or persistent RPC call.
#[derive(Debug, Clone)]
pub struct RpcOpts {
    pub connect_timeout: Duration,
    pub handshake_timeout: Duration,
    pub response_timeout: Duration,
}

impl Default for RpcOpts {
    fn default() -> Self {
        Self {
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
            response_timeout: DEFAULT_RESPONSE_TIMEOUT,
        }
    }
}

/// Perform a one-shot RPC: open a fresh connection, send one request, read
/// one reply, then drop the connection.
///
/// Suitable for low-frequency calls (RegisterFiler, zone queries). Each call
/// is independent; no channel caching is performed.
///
/// `channel` selects the physical path (CHANNEL_DATA or CHANNEL_META). For
/// inter-service RPCs (Master/Filer/Volume), CHANNEL_DATA is the norm.
///
/// The caller supplies a stable `client_id` (used in the handshake) — callers
/// typically derive it from `SystemTime::now()`.
pub async fn call_once(
    addr: &str,
    client_type: ClientType,
    client_id: u64,
    channel: u8,
    msg_type: MsgType,
    body: &[u8],
) -> NetResult<RpcReply> {
    call_once_with(
        addr,
        client_type,
        client_id,
        channel,
        msg_type,
        body,
        RpcOpts::default(),
    )
    .await
}

/// Same as [`call_once`] but with explicit timeouts.
pub async fn call_once_with(
    addr: &str,
    client_type: ClientType,
    client_id: u64,
    channel: u8,
    msg_type: MsgType,
    body: &[u8],
    opts: RpcOpts,
) -> NetResult<RpcReply> {
    let mut conn = RpcConnection::connect(addr, client_type, client_id, channel, &opts).await?;
    conn.call(msg_type, body, &opts).await
}

/// Persistent RPC client — keeps the connection open across multiple calls.
///
/// Suitable for high-frequency RPCs (Raft messages, Heartbeat). Acquire via
/// [`NetRpcClient::connect`], then issue repeated [`NetRpcClient::call`]
/// requests. If the connection breaks, call [`NetRpcClient::reconnect`].
pub struct NetRpcClient {
    conn: RpcConnection,
    seq: AtomicU32,
    opts: RpcOpts,
    addr: String,
    client_type: ClientType,
    client_id: u64,
    channel: u8,
}

impl NetRpcClient {
    /// Connect to `addr` and complete the handshake. The connection is kept
    /// open until the client is dropped or an I/O error occurs.
    pub async fn connect(
        addr: &str,
        client_type: ClientType,
        client_id: u64,
        channel: u8,
    ) -> NetResult<Self> {
        Self::connect_with(addr, client_type, client_id, channel, RpcOpts::default()).await
    }

    /// Connect with explicit timeouts.
    pub async fn connect_with(
        addr: &str,
        client_type: ClientType,
        client_id: u64,
        channel: u8,
        opts: RpcOpts,
    ) -> NetResult<Self> {
        let conn = RpcConnection::connect(addr, client_type, client_id, channel, &opts).await?;
        Ok(Self {
            conn,
            seq: AtomicU32::new(1),
            opts,
            addr: addr.to_string(),
            client_type,
            client_id,
            channel,
        })
    }

    /// Reconnect after a connection failure. Reuses the stored addr /
    /// client_type / client_id / channel / opts.
    pub async fn reconnect(&mut self) -> NetResult<()> {
        log::info!(
            "NetRpcClient: reconnecting to {} (client_type={:?}, channel={}, client_id={})",
            self.addr,
            self.client_type,
            self.channel,
            self.client_id
        );
        self.conn =
            RpcConnection::connect(&self.addr, self.client_type, self.client_id, self.channel, &self.opts)
                .await?;
        Ok(())
    }

    /// Send a request on the persistent connection and read the reply.
    /// On I/O error, the caller should call [`reconnect`](Self::reconnect).
    pub async fn call(&mut self, msg_type: MsgType, body: &[u8]) -> NetResult<RpcReply> {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        self.conn.call_seq(msg_type, body, seq, &self.opts).await
    }

    /// Like [`call`](Self::call) but auto-reconnects once on failure.
    /// Suitable for Raft message forwarding where transparent retry is wanted.
    pub async fn call_with_retry(&mut self, msg_type: MsgType, body: &[u8]) -> NetResult<RpcReply> {
        match self.call(msg_type, body).await {
            Ok(reply) => Ok(reply),
            Err(e) => {
                log::warn!(
                    "NetRpcClient: call failed ({}), reconnecting to {}",
                    e,
                    self.addr
                );
                self.reconnect().await?;
                self.call(msg_type, body).await
            }
        }
    }

    /// Whether the underlying connection is still usable.
    pub fn is_connected(&self) -> bool {
        // RpcConnection doesn't track state; treat existence as connected.
        // A broken connection will surface as an error on the next call.
        true
    }
}

// ---------------------------------------------------------------------------
// Internal: a single connection with handshake done.
// ---------------------------------------------------------------------------

struct RpcConnection {
    writer: tokio::net::tcp::OwnedWriteHalf,
    reader: tokio::net::tcp::OwnedReadHalf,
    client_id: u64,
    channel: u8,
}

impl RpcConnection {
    async fn connect(
        addr: &str,
        client_type: ClientType,
        client_id: u64,
        channel: u8,
        opts: &RpcOpts,
    ) -> NetResult<Self> {
        // 1. TCP connect with timeout.
        let stream = tokio::time::timeout(opts.connect_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| NetError::Timeout)??;
        let (reader, writer) = stream.into_split();

        let mut conn = Self {
            reader,
            writer,
            client_id,
            channel,
        };

        // 2. powerfs-net handshake (carries channel for server-side validation).
        conn.handshake(client_type, client_id, channel, opts).await?;

        Ok(conn)
    }

    async fn handshake(
        &mut self,
        client_type: ClientType,
        client_id: u64,
        channel: u8,
        opts: &RpcOpts,
    ) -> NetResult<()> {
        let hs_req = HandshakeRequest::new(client_type, client_id, channel);
        let mut hs_buf = vec![0u8; HandshakeRequest::SIZE];
        hs_req.encode(&mut hs_buf);
        self.writer
            .write_all(&hs_buf)
            .await
            .map_err(|e| NetError::Connection(format!("send handshake: {e}")))?;

        let mut hs_resp_buf = vec![0u8; HandshakeResponse::SIZE];
        tokio::time::timeout(
            opts.handshake_timeout,
            self.reader.read_exact(&mut hs_resp_buf),
        )
        .await
        .map_err(|_| NetError::Timeout)?
        .map_err(|e| NetError::Connection(format!("read handshake: {e}")))?;
        let hs_resp = HandshakeResponse::decode(&hs_resp_buf)
            .ok_or_else(|| NetError::Protocol("invalid handshake response".into()))?;
        if hs_resp.status != 0 {
            return Err(NetError::Connection(
                "handshake rejected by peer".to_string(),
            ));
        }
        let ch_str = if channel == crate::protocol::CHANNEL_META {
            "meta"
        } else {
            "data"
        };
        log::debug!(
            "RpcConnection: handshake ok addr client_id={} channel={} route_hash=0x{:02x}",
            client_id,
            ch_str,
            calc_route_hash(client_id, channel)
        );
        Ok(())
    }

    /// Send a request and read the reply. Uses seq=1 by default (one-shot).
    async fn call(
        &mut self,
        msg_type: MsgType,
        body: &[u8],
        opts: &RpcOpts,
    ) -> NetResult<RpcReply> {
        self.call_seq(msg_type, body, 1, opts).await
    }

    async fn call_seq(
        &mut self,
        msg_type: MsgType,
        body: &[u8],
        seq: u32,
        opts: &RpcOpts,
    ) -> NetResult<RpcReply> {
        // 3. Build & send TLV request frame with route_hash (client_id + channel).
        //    Server-side io_loop validates route_hash to detect frames on wrong
        //    connections. Using build_frame (route_hash=0) would bypass the
        //    channel check, which is fragile — build_frame_with_route_hash is
        //    the correct path for all client→server requests.
        let frame = build_frame_with_route_hash(
            msg_type as u16,
            FrameFlags::new(FrameFlags::REQUEST),
            seq,
            body,
            &[],
            self.client_id,
            self.channel,
        );
        self.writer
            .write_all(&frame)
            .await
            .map_err(|e| NetError::Connection(format!("send frame: {e}")))?;

        // 4. Read response header.
        let mut hdr_buf = [0u8; FrameHeader::SIZE];
        tokio::time::timeout(opts.response_timeout, self.reader.read_exact(&mut hdr_buf))
            .await
            .map_err(|_| NetError::Timeout)?
            .map_err(|e| NetError::Connection(format!("read header: {e}")))?;
        let hdr = FrameHeader::decode(&hdr_buf)
            .ok_or_else(|| NetError::Protocol("invalid response header".into()))?;

        // 5. Read response body (body + data combined; callers decode TLV).
        let total = hdr.data_len as usize;
        let body = if total == 0 {
            Vec::new()
        } else {
            let mut buf = vec![0u8; total];
            tokio::time::timeout(opts.response_timeout, self.reader.read_exact(&mut buf))
                .await
                .map_err(|_| NetError::Timeout)?
                .map_err(|e| NetError::Connection(format!("read body: {e}")))?;
            buf
        };

        Ok(RpcReply {
            status: hdr.status,
            body,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rpc_reply_is_ok() {
        assert!(RpcReply {
            status: STATUS_OK,
            body: vec![]
        }
        .is_ok());
        assert!(!RpcReply {
            status: 0x0001,
            body: vec![]
        }
        .is_ok());
    }

    #[test]
    fn rpc_opts_default() {
        let o = RpcOpts::default();
        assert_eq!(o.connect_timeout, DEFAULT_CONNECT_TIMEOUT);
        assert_eq!(o.handshake_timeout, DEFAULT_HANDSHAKE_TIMEOUT);
        assert_eq!(o.response_timeout, DEFAULT_RESPONSE_TIMEOUT);
    }

    #[tokio::test]
    async fn call_once_to_closed_port_fails() {
        // Nothing listens on 127.0.0.1:1 on most CI envs.
        let res = call_once(
            "127.0.0.1:1",
            ClientType::Master,
            1,
            crate::protocol::CHANNEL_DATA,
            MsgType::Ping,
            &[],
        )
        .await;
        assert!(res.is_err());
    }
}
