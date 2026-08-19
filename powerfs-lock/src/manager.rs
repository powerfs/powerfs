//! The unified `LockManager` trait.
//!
//! This trait is the single client-side entry point for lock
//! acquisition/release/renew. Implementations route requests to the Filer
//! (`InodeLeaseStore`, for inode-level locks) or to the Volume Server
//! (`RangeLeaseStore`, for range-level locks) based on the request shape.
//!
//! See `docs/lock-optimization-plan.md` §3.1 and §7.1.

use crate::error::LockError;
use crate::event::LockEventHandler;
use crate::types::{LockGrant, LockRequest};
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;

/// Unified client-side lock manager.
///
/// Client-form agnostic: the FUSE userspace Rust client implements this via
/// `powerfs-lock-fuse`; the in-kernel C client implements the same protocol
/// independently (see `docs/lock-protocol.md`).
///
/// # Routing
///
/// - Inode-level request (`LockRequest::is_inode_level()`)
///   → Filer's `InodeLeaseStore` (reuses `powerfs-lease` with `InodeKey`).
/// - Range-level request (`LockRequest::is_range_level()`)
///   → Volume's `RangeLeaseStore` (reuses `powerfs-lease` with `StripeKey`).
///
/// The two backends are mutually exclusive at runtime (client config
/// `lease_mode = "inode" | "range"`), so no cross-server coordination.
#[async_trait]
pub trait LockManager: Send + Sync {
    /// Acquire a lock (lease).
    ///
    /// Returns `LockGrant` on success, or `LockError` on failure
    /// (`Conflict`, `Quarantined`, `Network`, etc.).
    async fn acquire(&self, req: LockRequest) -> Result<LockGrant, LockError>;

    /// Release a held lease by token. Idempotent: releasing an already-
    /// released or expired lease returns `Ok(())`.
    async fn release(&self, inode: u64, token: &str) -> Result<(), LockError>;

    /// Renew a held lease. Extends the TTL by `timeout`.
    async fn renew(&self, inode: u64, token: &str, timeout: Duration) -> Result<(), LockError>;

    /// Register a handler for server-pushed notifications (Early Revoke,
    /// invalidate). The manager should hold a `Weak<dyn LockEventHandler>`
    /// to avoid reference cycles with the handler's potential back-reference
    /// to the manager.
    fn register_handler(&self, handler: Arc<dyn LockEventHandler>);
}
