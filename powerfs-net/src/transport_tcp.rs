//! TCP Transport — 现有 TCP 连接的 Transport trait 实现
//!
//! 从 server.rs / client.rs / io_loop.rs 的直接 TcpStream 使用重构为
//! 通过 Transport trait 创建连接, 为 RDMA 扩展铺路。

use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};

use crate::errors::{NetError, NetResult};
use crate::transport::{Transport, TransportListener, TransportStream};

/// TCP 传输层
pub struct TcpTransport;

/// TCP TransportStream — 包装 TcpStream
pub struct TcpTransportStream {
    stream: TcpStream,
    peer: SocketAddr,
}

impl TransportStream for TcpTransportStream {
    fn split(
        self: Box<Self>,
    ) -> (
        Box<dyn AsyncRead + Send + Unpin>,
        Box<dyn AsyncWrite + Send + Unpin>,
    ) {
        let (read_half, write_half) = self.stream.into_split();
        (Box::new(read_half), Box::new(write_half))
    }

    fn peer_addr(&self) -> SocketAddr {
        self.peer
    }
}

/// TCP 监听器
pub struct TcpListenerAdapter {
    listener: TcpListener,
}

#[async_trait::async_trait]
impl TransportListener for TcpListenerAdapter {
    async fn accept(&self) -> NetResult<Box<dyn TransportStream>> {
        let (stream, peer) = self
            .listener
            .accept()
            .await
            .map_err(|e| NetError::Connection(format!("TcpListener accept failed: {}", e)))?;
        stream.set_nodelay(true).ok();
        // 接受侧也启用 TCP keepalive, 防止对端 silent death 时连接长时间卡住
        #[cfg(unix)]
        apply_tcp_keepalive(&stream);
        Ok(Box::new(TcpTransportStream { stream, peer }))
    }

    fn local_addr(&self) -> NetResult<SocketAddr> {
        self.listener
            .local_addr()
            .map_err(|e| NetError::Connection(format!("local_addr failed: {}", e)))
    }
}

#[async_trait::async_trait]
impl Transport for TcpTransport {
    async fn connect(&self, addr: SocketAddr) -> NetResult<Box<dyn TransportStream>> {
        let stream = TcpStream::connect(addr).await.map_err(|e| {
            NetError::Connection(format!("TcpStream connect to {} failed: {}", addr, e))
        })?;
        stream.set_nodelay(true).ok();
        // 启用 TCP keepalive (idle=60s, interval=10s, retries=3), 防止对端
        // silent death 时连接长时间卡住. RDMA 传输不需要此设置.
        #[cfg(unix)]
        apply_tcp_keepalive(&stream);
        let peer = stream.peer_addr().unwrap_or(addr);
        Ok(Box::new(TcpTransportStream { stream, peer }))
    }

    async fn bind(&self, addr: SocketAddr) -> NetResult<Box<dyn TransportListener>> {
        let listener = TcpListener::bind(addr).await.map_err(|e| {
            NetError::Connection(format!("TcpListener bind on {} failed: {}", addr, e))
        })?;
        Ok(Box::new(TcpListenerAdapter { listener }))
    }

    fn name(&self) -> &'static str {
        "tcp"
    }
}

/// 在 Unix 平台上为 TcpStream 设置 TCP keepalive 参数.
///
/// 参数: idle=60s, interval=10s, retries=3. 失败时仅告警, 不影响连接.
#[cfg(unix)]
fn apply_tcp_keepalive(stream: &TcpStream) {
    use socket2::TcpKeepalive;
    use std::os::unix::io::{AsRawFd, FromRawFd, IntoRawFd};

    let raw_fd = stream.as_raw_fd();
    // SAFETY: from_raw_fd 不获取所有权, 我们在结尾用 into_raw_fd 把 fd 还回去,
    // 不让 socket2::Socket 闭包时关闭 fd (TcpStream 仍持有).
    let sock2 = unsafe { socket2::Socket::from_raw_fd(raw_fd) };
    let ka = TcpKeepalive::new()
        .with_time(Duration::from_secs(60))
        .with_interval(Duration::from_secs(10))
        .with_retries(3);
    if let Err(e) = sock2.set_tcp_keepalive(&ka) {
        log::warn!("failed to set TCP keepalive (continuing): {}", e);
    }
    let _ = sock2.into_raw_fd();
}

/// 为 TcpStream 直接实现 TransportStream (零开销, 无 wrapper struct)
///
/// 这样上层代码可以直接将 `TcpStream` 传入需要 `Box<dyn TransportStream>` 的地方,
/// 无需额外包装。但当前用 `TcpTransportStream` 包装以保留 peer_addr。
impl TransportStream for tokio::net::TcpStream {
    fn split(
        self: Box<Self>,
    ) -> (
        Box<dyn AsyncRead + Send + Unpin>,
        Box<dyn AsyncWrite + Send + Unpin>,
    ) {
        let (read_half, write_half) = (*self).into_split();
        (Box::new(read_half), Box::new(write_half))
    }

    fn peer_addr(&self) -> SocketAddr {
        tokio::net::TcpStream::peer_addr(self).unwrap_or("0.0.0.0:0".parse().unwrap())
    }
}
