//! RDMA Transport — 基于 ibverbs 的 RDMA 传输层实现
//!
//! 设计参考: BeeGFS RDMASocket + 预注册 MR Pool
//!
//! # 核心机制
//!
//! 1. **Stream 仿真**: RDMA send/recv 是消息语义, 通过内部缓冲区
//!    包装为 AsyncRead + AsyncWrite stream 接口
//! 2. **预注册 MR Pool**: 启动时预分配并 ibv_reg_mr 注册 N 个 buffer,
//!    避免每次发送的动态 MR 注册开销 (10-100μs/次)
//! 3. **帧边界对齐**: 一次 RDMA send = 一个完整 powerfs-net 帧
//!    (FrameHeader 16B + body + data)
//!
//! # 编译控制
//!
//! 仅 `--features rdma` 时编译, 默认不引入 ibverbs 依赖。

#![cfg(feature = "rdma")]

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::Mutex;

use crate::errors::{NetError, NetResult};
use crate::transport::{Transport, TransportConfig, TransportListener, TransportStream};

/// RDMA 传输层
#[allow(dead_code)]
pub struct RdmaTransport {
    config: TransportConfig,
    /// RDMA 设备上下文 (ibv_context + protection domain)
    /// 实际字段在接入 async-rdma crate 后填充
    _device: Arc<()>,
}

impl RdmaTransport {
    pub fn new(_config: TransportConfig) -> NetResult<Self> {
        // TODO: 接入 async-rdma crate
        // 1. ibv_open_device() 获取 device context
        // 2. ibv_alloc_pd() 获取 protection domain
        // 3. 创建预注册 MR pool
        // 4. 创建 CQ (completion queue)
        Err(NetError::Config(
            "RDMA transport not yet implemented (requires async-rdma crate integration)"
                .to_string(),
        ))
    }
}

/// RDMA stream — 将 RDMA send/recv 包装为 AsyncRead + AsyncWrite
pub struct RdmaStream {
    /// RDMA 连接 handle (QP + 共享 CQ + MR pool)
    /// 实际字段在接入 async-rdma crate 后填充
    _channel: Arc<()>,
    /// 预注册的 recv buffer pool (RDMA recv → 缓冲 → AsyncRead)
    _recv_buf: Arc<Mutex<Vec<u8>>>,
    /// 预注册的 send buffer pool (AsyncWrite → 缓冲 → RDMA send)
    _send_buf: Arc<Mutex<Vec<u8>>>,
    /// 对端地址
    peer: SocketAddr,
}

impl TransportStream for RdmaStream {
    fn split(
        self: Box<Self>,
    ) -> (Box<dyn AsyncRead + Send + Unpin>, Box<dyn AsyncWrite + Send + Unpin>) {
        // RDMA split: reader 和 writer 共享 channel 但有独立的 buffer 和 CQ 轮询
        let channel = self._channel.clone();
        let recv_buf = self._recv_buf.clone();
        let send_buf = self._send_buf.clone();

        let reader = RdmaReadHalf {
            _channel: channel.clone(),
            _recv_buf: recv_buf,
        };
        let writer = RdmaWriteHalf {
            _channel: channel,
            _send_buf: send_buf,
        };

        (Box::new(reader), Box::new(writer))
    }

    fn peer_addr(&self) -> SocketAddr {
        self.peer
    }
}

/// RDMA 读端 (共享 channel)
struct RdmaReadHalf {
    _channel: Arc<()>,
    _recv_buf: Arc<Mutex<Vec<u8>>>,
}

impl AsyncRead for RdmaReadHalf {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        _buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        // TODO: 从 RDMA recv CQ 轮询完成事件 → 拷贝到 buf
        std::task::Poll::Ready(Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "RDMA read not yet implemented",
        )))
    }
}

/// RDMA 写端 (共享 channel)
struct RdmaWriteHalf {
    _channel: Arc<()>,
    _send_buf: Arc<Mutex<Vec<u8>>>,
}

impl AsyncWrite for RdmaWriteHalf {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        _buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        // TODO: 拷贝到 send buffer → post RDMA send work request
        std::task::Poll::Ready(Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "RDMA write not yet implemented",
        )))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        // TODO: 发送 RDMA send with invalidate 或 close QP
        std::task::Poll::Ready(Ok(()))
    }
}

/// RDMA 监听器
pub struct RdmaListenerAdapter {
    _addr: SocketAddr,
}

#[async_trait::async_trait]
impl TransportListener for RdmaListenerAdapter {
    async fn accept(&self) -> NetResult<Box<dyn TransportStream>> {
        // TODO: rdma_cm accept + 创建 QP + 预注册 MR pool
        Err(NetError::Config(
            "RDMA listener not yet implemented".to_string(),
        ))
    }

    fn local_addr(&self) -> NetResult<SocketAddr> {
        Ok(self._addr)
    }
}

#[async_trait::async_trait]
impl Transport for RdmaTransport {
    async fn connect(&self, addr: SocketAddr) -> NetResult<Box<dyn TransportStream>> {
        // TODO: rdma_cm resolve_addr + resolve_route + connect + QP init + MR pool
        let _ = addr;
        Err(NetError::Config(
            "RDMA connect not yet implemented".to_string(),
        ))
    }

    async fn bind(&self, addr: SocketAddr) -> NetResult<Box<dyn TransportListener>> {
        // TODO: rdma_cm create_id + bind + listen
        Ok(Box::new(RdmaListenerAdapter { _addr: addr }))
    }

    fn name(&self) -> &'static str {
        "rdma"
    }
}
