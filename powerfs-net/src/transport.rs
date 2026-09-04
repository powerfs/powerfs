//! Transport trait — 可配置传输层抽象 (TCP / RDMA)
//!
//! 设计参考: BeeGFS Socket 抽象层 (Socket.h + StreamCommSocket.h + RDMASocket.h)
//!
//! 核心目标: 将连接建立与数据传输抽象化, IoLoop/Worker/ClientConn 代码不
//! 感知底层传输类型 (TCP 或 RDMA)。上层代码通过 `Arc<dyn Transport>` 创建
//! 连接, 通过 `Box<dyn TransportStream>` 读写帧。
//!
//! # 架构
//!
//! ```text
//!   powerfs-net 上层 (server / io_loop / client)
//!         │
//!         ▼
//!   Transport trait (connect / bind)
//!         │
//!    ┌────┴────┐
//!    ▼         ▼
//!   TCP       RDMA
//!   (stream)  (stream 仿真)
//!    │         │
//!    ▼         ▼
//!   TcpStream  RdmaStream
//!   (AsyncRead  (AsyncRead
//!    +AsyncWrite) +AsyncWrite)
//! ```
//!
//! # TransportStream split
//!
//! `TransportStream` 需要拆分为独立的读/写端, 因为 IoLoop 使用独立的
//! read_task / write_task。TCP 通过 `TcpStream::into_split()` 实现;
//! RDMA 通过共享 `Arc<RdmaChannel>` 的独立 reader/writer 实现。

use std::net::SocketAddr;

use tokio::io::{AsyncRead, AsyncWrite};

use crate::errors::{NetError, NetResult};

/// 传输层连接 (双工 stream, 可 split 为独立的 read/write 端)
///
/// 实现者: `TcpStream` (TCP), `RdmaStream` (RDMA stream 仿真)
pub trait TransportStream: Send {
    /// 拆分为独立的读端和写端
    ///
    /// 消费 self, 返回 (read_half, write_half)。
    /// 两个 half 独立使用, 不共享可变状态。
    fn split(
        self: Box<Self>,
    ) -> (
        Box<dyn AsyncRead + Send + Unpin>,
        Box<dyn AsyncWrite + Send + Unpin>,
    );

    /// 对端地址 (用于日志和调试)
    fn peer_addr(&self) -> SocketAddr;
}

/// 传输层: 服务端监听器
#[async_trait::async_trait]
pub trait TransportListener: Send + Sync {
    /// 接受一个新连接 (阻塞直到有连接到达)
    async fn accept(&self) -> NetResult<Box<dyn TransportStream>>;

    /// 本地监听地址
    fn local_addr(&self) -> NetResult<SocketAddr>;
}

/// 传输层: 创建连接 (客户端) 和监听器 (服务端)
///
/// 实现者: `TcpTransport`, `RdmaTransport`, `AutoTransport`
#[async_trait::async_trait]
pub trait Transport: Send + Sync {
    /// 创建到 addr 的客户端连接
    async fn connect(&self, addr: SocketAddr) -> NetResult<Box<dyn TransportStream>>;

    /// 在 addr 上创建服务端监听
    async fn bind(&self, addr: SocketAddr) -> NetResult<Box<dyn TransportListener>>;

    /// 传输类型名称 ("tcp" / "rdma" / "auto")
    fn name(&self) -> &'static str;
}

/// 传输层配置 (从 TOML 配置文件加载)
#[derive(Debug, Clone)]
pub struct TransportConfig {
    /// 传输类型: "auto" | "tcp" | "rdma"
    pub transport: String,
    /// RDMA 失败时是否回退到 TCP
    pub tcp_fallback: bool,
    /// TCP 接口 (如 "eth0", 用于绑定)
    pub tcp_interface: Option<String>,
    /// RDMA 设备名 (如 "mlx5_0", None = 自动选择)
    pub rdma_device: Option<String>,
    /// 预注册 RDMA buffer 数量
    pub rdma_buf_num: usize,
    /// 预注册 RDMA buffer 大小 (bytes)
    pub rdma_buf_size: usize,
    /// 每个节点并行连接数 (多 QP)
    pub conn_per_node: usize,
    /// 要求二进制必须以 --features rdma 编译 (#47 硬化).
    /// 为 true 时, 若当前二进制未编译 rdma feature, `create_transport`
    /// 返回 fatal error 而非静默降级为 TCP. 防止 RDMA 部署中
    /// 误用 TCP-only 二进制导致 RDMA listener 缺失 → 客户端 stale cache.
    pub require_rdma: bool,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            transport: "auto".to_string(),
            tcp_fallback: true,
            tcp_interface: None,
            rdma_device: None,
            // [RC18b ROOT CAUSE 24 FIX] SEND 与 RECV 共享同一个 MrPool (ctrl).
            // pre-post N RECV + SEND 侧并发 acquire N*2 响应帧时, 若 buf_num 仅
            // 等于 RECV posted 数 (默认 32) 则 pool.free = 0, SEND acquire
            // 永远 None, 响应根本无法发出 → 内核客户端 RPC 超时 → EAGAIN.
            // 对称内核 side CTRL_BUF_NUM=128 = 32 RECV posted + 64 SEND
            // inflight + 32 slack.
            rdma_buf_num: 128,
            rdma_buf_size: 65536,
            conn_per_node: 1,
            require_rdma: false,
        }
    }
}

/// 根据配置创建 Transport 实例
///
/// - "auto": 检测 RDMA 硬件, 有则用 RDMA + TCP fallback, 无则用 TCP
/// - "tcp": 强制 TCP
/// - "rdma": 强制 RDMA, 无硬件则报错
///
/// # #47 硬化: `require_rdma = true` 时, 若二进制未编译 `rdma` feature,
/// 所有 transport 类型均返回 fatal error, 防止 RDMA 部署中误用 TCP-only
/// 二进制导致 RDMA listener 缺失 → 客户端静默服务 stale cache.
pub fn create_transport(config: &TransportConfig) -> NetResult<std::sync::Arc<dyn Transport>> {
    // #47 硬化: require_rdma + transport=tcp 是矛盾配置, 优先拒绝
    // (在 feature 守卫之前检查, 因为矛盾配置无论 rdma feature 是否
    // 编译都应报错).
    if config.require_rdma && config.transport == "tcp" {
        return Err(NetError::Config(
            "require_rdma=true is incompatible with transport='tcp' \
             (use transport='auto' or 'rdma' in RDMA deployments)"
                .to_string(),
        ));
    }

    // #47 硬化: require_rdma 守卫 — require_rdma=true 但二进制未编译
    // rdma feature 时, 拒绝启动 (此时 transport 已不是 tcp).
    if config.require_rdma {
        #[cfg(not(feature = "rdma"))]
        {
            return Err(NetError::Config(format!(
                "require_rdma=true but this binary was NOT compiled with --features rdma; \
                 rebuild with: cargo build --release --features rdma \
                 (transport='{}')",
                config.transport
            )));
        }
        #[cfg(feature = "rdma")]
        {
            log::debug!("require_rdma=true: binary has rdma feature, proceeding");
        }
    }

    match config.transport.as_str() {
        "tcp" => {
            Ok(std::sync::Arc::new(crate::transport_tcp::TcpTransport))
        }
        "rdma" => {
            #[cfg(feature = "rdma")]
            {
                let rdma = crate::transport_rdma::RdmaTransport::new(config.clone())?;
                Ok(std::sync::Arc::new(rdma))
            }
            #[cfg(not(feature = "rdma"))]
            {
                Err(NetError::Config(
                    "transport=rdma but powerfs-net was not compiled with 'rdma' feature"
                        .to_string(),
                ))
            }
        }
        "auto" => {
            #[cfg(feature = "rdma")]
            {
                // 尝试创建 RDMA transport, 成功则用 AutoTransport (RDMA + TCP fallback)
                match crate::transport_rdma::RdmaTransport::new(config.clone()) {
                    Ok(rdma) => {
                        log::info!("AutoTransport: RDMA available, using RDMA with TCP fallback");
                        let auto = AutoTransport {
                            tcp: crate::transport_tcp::TcpTransport,
                            rdma: Some(std::sync::Arc::new(rdma)),
                            fallback: config.tcp_fallback,
                        };
                        Ok(std::sync::Arc::new(auto))
                    }
                    Err(e) => {
                        log::info!("AutoTransport: RDMA not available ({}), using TCP only", e);
                        Ok(std::sync::Arc::new(crate::transport_tcp::TcpTransport))
                    }
                }
            }
            #[cfg(not(feature = "rdma"))]
            {
                // require_rdma 守卫已在函数入口拦截, 到这里说明 require_rdma=false.
                log::info!("AutoTransport: RDMA feature not compiled, using TCP");
                Ok(std::sync::Arc::new(crate::transport_tcp::TcpTransport))
            }
        }
        other => Err(NetError::Config(format!(
            "unknown transport type: '{}' (expected: auto, tcp, rdma)",
            other
        ))),
    }
}

/// AutoTransport: RDMA 优先, 失败时回退 TCP
///
/// 注意: `rdma` 和 `fallback` 字段仅在 `feature = "rdma"` 时被读取,
/// 非 RDMA 编译路径下会触发 dead_code 告警, 此处显式 allow.
#[allow(dead_code)]
pub struct AutoTransport {
    tcp: crate::transport_tcp::TcpTransport,
    #[cfg(feature = "rdma")]
    rdma: Option<std::sync::Arc<crate::transport_rdma::RdmaTransport>>,
    #[cfg(not(feature = "rdma"))]
    rdma: Option<()>,
    fallback: bool,
}

#[async_trait::async_trait]
impl Transport for AutoTransport {
    async fn connect(&self, addr: SocketAddr) -> NetResult<Box<dyn TransportStream>> {
        // 1. 尝试 RDMA (如果可用)
        #[cfg(feature = "rdma")]
        if let Some(rdma) = &self.rdma {
            match rdma.connect(addr).await {
                Ok(stream) => return Ok(stream),
                Err(e) if self.fallback => {
                    log::warn!(
                        "AutoTransport: RDMA connect to {} failed ({}), falling back to TCP",
                        addr,
                        e
                    );
                }
                Err(e) => return Err(e),
            }
        }
        // 2. 回退到 TCP
        self.tcp.connect(addr).await
    }

    async fn bind(&self, addr: SocketAddr) -> NetResult<Box<dyn TransportListener>> {
        // 服务端: 尝试 RDMA 监听, 失败则 TCP
        #[cfg(feature = "rdma")]
        if let Some(rdma) = &self.rdma {
            match rdma.bind(addr).await {
                Ok(listener) => return Ok(listener),
                Err(e) if self.fallback => {
                    log::warn!(
                        "AutoTransport: RDMA bind on {} failed ({}), falling back to TCP",
                        addr,
                        e
                    );
                }
                Err(e) => return Err(e),
            }
        }
        self.tcp.bind(addr).await
    }

    fn name(&self) -> &'static str {
        #[cfg(feature = "rdma")]
        {
            if self.rdma.is_some() {
                return "auto(rdma+tcp)";
            }
        }
        "auto(tcp)"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_tcp_transport() {
        let cfg = TransportConfig {
            transport: "tcp".to_string(),
            ..Default::default()
        };
        let t = create_transport(&cfg).expect("tcp transport should succeed");
        assert_eq!(t.name(), "tcp");
    }

    #[test]
    fn test_unknown_transport_returns_error() {
        let cfg = TransportConfig {
            transport: "quic".to_string(),
            ..Default::default()
        };
        match create_transport(&cfg) {
            Err(NetError::Config(_)) => {}
            Err(e) => panic!("expected Config error, got {:?}", e),
            Ok(_) => panic!("expected error for unknown transport"),
        }
    }

    #[test]
    fn test_rdma_without_feature_returns_error() {
        // Without the `rdma` feature compiled in, transport=rdma should
        // return a Config error, not crash.
        #[cfg(not(feature = "rdma"))]
        {
            let cfg = TransportConfig {
                transport: "rdma".to_string(),
                ..Default::default()
            };
            match create_transport(&cfg) {
                Err(NetError::Config(_)) => {}
                Err(e) => panic!("expected Config error, got {:?}", e),
                Ok(_) => panic!("expected error for rdma without feature"),
            }
        }
        // When the `rdma` feature IS compiled in, this test is a no-op
        // because calling create_transport("rdma") would initialize RDMA
        // hardware, which may SIGSEGV on systems with broken drivers.
        // Use `cargo test -- --ignored test_rdma_hardware` for real
        // hardware testing.
        #[cfg(feature = "rdma")]
        {
            // No-op: hardware initialization is tested separately.
        }
    }

    #[test]
    fn test_auto_transport_falls_back_to_tcp() {
        // "auto" should always succeed — if RDMA is unavailable it
        // falls back to TCP. If RDMA hardware crashes (SIGSEGV in C
        // library), this test would fail, which is the correct
        // behavior on broken hardware.
        //
        // To avoid the SIGSEGV risk on systems with broken RDMA
        // drivers, we skip this test when the rdma feature is enabled
        // and we know the hardware is problematic. The fallback logic
        // is tested via the "tcp" path above.
        #[cfg(not(feature = "rdma"))]
        {
            let cfg = TransportConfig {
                transport: "auto".to_string(),
                ..Default::default()
            };
            let t = create_transport(&cfg).expect("auto should always succeed");
            // Without rdma feature, auto falls back to plain TcpTransport.
            assert_eq!(t.name(), "tcp");
        }
        #[cfg(feature = "rdma")]
        {
            // When rdma feature is enabled, auto tries to init RDMA
            // hardware first. On systems with broken drivers this
            // crashes, so we skip the test. The fallback logic is
            // covered by the non-rdma build path.
        }
    }

    #[test]
    fn test_transport_config_default() {
        let cfg = TransportConfig::default();
        assert_eq!(cfg.transport, "auto");
        assert!(cfg.tcp_fallback);
        assert_eq!(cfg.rdma_buf_num, 128);
        assert_eq!(cfg.rdma_buf_size, 65536);
        assert_eq!(cfg.conn_per_node, 1);
        assert!(!cfg.require_rdma);
    }

    /// #47 硬化: require_rdma=true 且未编译 rdma feature 时,
    /// create_transport 必须返回 fatal error 而非静默降级 TCP.
    #[test]
    fn test_require_rdma_without_feature_returns_error() {
        #[cfg(not(feature = "rdma"))]
        {
            let cfg = TransportConfig {
                transport: "auto".to_string(),
                require_rdma: true,
                ..Default::default()
            };
            match create_transport(&cfg) {
                Err(NetError::Config(msg)) => {
                    assert!(
                        msg.contains("require_rdma"),
                        "error should mention require_rdma, got: {}",
                        msg
                    );
                }
                Err(e) => panic!("expected Config error, got {:?}", e),
                Ok(_) => panic!("require_rdma=true must fail without rdma feature"),
            }
        }
        #[cfg(feature = "rdma")]
        {
            // With rdma feature, require_rdma=true is satisfied.
            // Don't actually call create_transport to avoid hardware init.
        }
    }

    /// #47 硬化: require_rdma=true + transport=tcp 是矛盾配置, 必须拒绝.
    #[test]
    fn test_require_rdma_with_tcp_returns_error() {
        let cfg = TransportConfig {
            transport: "tcp".to_string(),
            require_rdma: true,
            ..Default::default()
        };
        match create_transport(&cfg) {
            Err(NetError::Config(msg)) => {
                assert!(
                    msg.contains("incompatible"),
                    "error should mention incompatible, got: {}",
                    msg
                );
            }
            Err(e) => panic!("expected Config error, got {:?}", e),
            Ok(_) => panic!("require_rdma=true + tcp must fail"),
        }
    }
}
