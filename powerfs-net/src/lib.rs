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
pub mod protocol;
pub mod serialize;
pub mod server;

pub use client::{ClientConfig, PowerFsNetClient};
pub use connection::ConnectionManager;
pub use errors::{NetError, NetResult};
pub use protocol::*;
pub use serialize::EntryInfo;
pub use server::{PowerFsNetHandler, PowerFsNetServer};
