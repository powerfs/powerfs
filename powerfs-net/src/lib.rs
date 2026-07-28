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

pub mod client;
pub mod connection;
pub mod errors;
pub mod handler_adapter;
pub mod middleware;
pub mod protocol;
pub mod request_context;
pub mod serialize;
pub mod server;
pub mod server_connection;

pub use client::{ClientConfig, PowerFsNetClient};
pub use connection::ConnectionManager;
pub use errors::{NetError, NetResult};
pub use handler_adapter::{LegacyHandler, ManagedNetHandler};
pub use middleware::{
    FnHandler, LoggingMiddleware, MetricsMiddleware, Middleware, NextHandler, PipelineBuilder,
    RateLimitMiddleware, RequestMetrics, RequestPipeline, TracingMiddleware,
};
pub use protocol::*;
pub use request_context::{ClientInfo, RequestContext, TraceId};
pub use serialize::{DirEntry, EntryInfo, TlvDecoder, TlvEncoder};
pub use server::{PowerFsNetHandler, PowerFsNetServer};
pub use server_connection::{
    ClientSession, HealthStatus, MetricsSnapshot, ServerConnectionManager, ServerRequestHandler,
    SessionState,
};
