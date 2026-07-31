//! High-level TLV client for the PowerFS Master service.
//!
//! Provides [`TlvMasterClient`] — a reusable client that communicates
//! with Master via the powerfs-net binary protocol (TLV), with
//! automatic leader discovery (`STATUS_ERR_REDIRECT`), connection
//! failover, and request retry.
//!
//! This crate is intentionally free of FUSE-specific logic so that
//! both the FUSE client and the kernel filesystem client can share
//! the same protocol and leader-discovery semantics.
//!
//! # Quick start
//!
//! ```ignore
//! use powerfs_master_net::TlvMasterClient;
//!
//! let client = TlvMasterClient::new(vec![("172.30.0.11".into(), 9334)], Default::default());
//! client.connect().await?;
//! let topo = client.get_topology().await?;
//! ```

pub mod client;
pub mod error;
pub mod types;

pub use client::{TlvMasterClient, TlvMasterClientConfig};
pub use error::{MasterNetError, MasterNetResult};
pub use types::{AssignResult, TopologyInfo, VolumeLocation, VolumeRoute};
