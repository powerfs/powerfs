//! powerfs-lock-fuse: FUSE userspace Rust impl of `powerfs_lock::LockManager`.
//!
//! This crate is the conservative adapter layer described in
//! `docs/lock-optimization-plan.md` §4.1 ("保守适配器" strategy). It wraps
//! the existing FUSE lease primitives (`VolumeLeaseManager` /
//! `FuseClientFacade`) with the unified `LockManager` trait, so that:
//! - The FUSE Rust client has a single entry point matching the kernel C
//!   client's wire protocol (see `docs/lock-protocol.md`).
//! - Prometheus-style lock metrics are exposed from one place.
//! - Future migration of `fuse.rs` business paths to `LockManager` can
//!   proceed incrementally without touching `cache.rs`'s `HoldState`
//!   (which stays in place — see §4.1 risk note on bidirectional
//!   dependency).
//!
//! # Architecture
//!
//! ```text
//! LockManager::acquire(LockRequest)
//!        │
//!        ▼
//! FuseLockManager
//!   ├── ClientLeaseState  (per-inode lease cache: token, expire_at, mode)
//!   ├── FuseLockBackend   (trait: abstracts FuseClientFacade RPCs)
//!   └── LockMetrics       (counters: acquire_total, conflict_total, ...)
//! ```
//!
//! The `FuseLockBackend` trait decouples this crate from `powerfs-fuse-core`,
//! avoiding a heavy dependency and circular-import risk. `powerfs-fuse-core`
//! (or `powerfs-fuse`) provides a concrete impl bridging to
//! `FuseClientFacade`.

pub mod backend;
pub mod manager;
pub mod metrics;
pub mod state;

pub use backend::FuseLockBackend;
pub use manager::FuseLockManager;
pub use metrics::{LockMetrics, LockMetricsSnapshot};
pub use state::ClientLeaseState;
