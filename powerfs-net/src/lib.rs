//! PowerFS Net - Lightweight binary network protocol
//!
//! This crate provides a unified communication layer for both FUSE (Rust)
//! and kernel (C) clients to communicate with PowerFS servers (Master, Volume).
//!
//! # Architecture
//!
//! ```text
//! FUSE Client (Rust)          Kernel Client (C)
//!        │                           │
//!        ▼                           ▼
//!   powerfs-net               powerfs-net (C impl)
//!   (Rust impl)               (same wire protocol)
//!        │                           │
//!        ▼                           ▼
//!   TCP Socket  ─────────────────►  Master/Volume Server
//! ```

pub mod admin_server;
pub mod client;
pub mod client_conn;
pub mod client_pool;
pub mod errors;
pub mod flow_admin;
pub mod flow_control;
pub mod flow_policy;
pub mod io_loop;
pub mod middleware;
pub mod protocol;
pub mod request_context;
pub mod rpc_client;
pub mod serialize;
pub mod server;
pub mod server_connection;
pub mod work;
pub mod worker;

pub use admin_server::{AdminServer, AdminServerConfig};

pub use client::{
    ClientConfig, ClientEventListener, ClientMetrics, ClientMetricsSnapshot, ClientState,
    NotificationHandler, PowerFsNetClient,
};
pub use client_conn::{
    ClientConn, ClientConnInfo, ClientPolicy, ClientStats, CloseHandle, ConnHealthStatus,
    ConnMetricsSnapshot, ConnRegistry, ConnState, OutboundTx, RateLimiter,
};
pub use client_pool::{ClientConnPool, ClientPoolConfig, ServerEndpoint};
pub use errors::{NetError, NetResult};
pub use flow_control::{
    Channel, ConnStats, ConnStatsSnapshot, FlowController, GlobalStats, GlobalStatsSnapshot,
    SlowConnTracker, SlowConnTrackerConfig, SlowStateChange,
};
pub use flow_policy::{
    AdaptiveConcurrencyPolicy, AdmissionDecision, FlowCtx, FlowPolicy, NullPolicy, PolicySnapshot,
    RejectReason,
};
pub use io_loop::IoLoop;
pub use middleware::{
    FnHandler, LoggingMiddleware, MetricsMiddleware, Middleware, NextHandler, PipelineBuilder,
    RateLimitMiddleware, RequestMetrics, RequestPipeline, TracingMiddleware,
};
pub use protocol::*;
pub use request_context::{ClientInfo, RequestContext, TraceId};
pub use rpc_client::{call_once, call_once_with, NetRpcClient, RpcOpts, RpcReply};
pub use serialize::{DirEntry, EntryInfo, TlvDecoder, TlvEncoder};
pub use server::{PowerFsNetServer, ServerConfig};
pub use server_connection::{
    HealthStatus, MetricsSnapshot, NetHandler, ServerConnectionManager, SessionState,
};
pub use work::Work;
pub use worker::Worker;
