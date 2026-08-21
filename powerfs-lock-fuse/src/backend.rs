//! Backend trait that abstracts the FUSE client's RPC facade.
//!
//! `FuseLockManager` calls through this trait instead of depending on
//! `powerfs-fuse-core::FuseClientFacade` directly, so `powerfs-lock-fuse`
//! stays a light crate (no transitive `powerfs-master` / `powerfs-net`
//! dependencies). The concrete impl lives in `powerfs-fuse-core` (or
//! `powerfs-fuse`) and bridges to the actual `FuseClientFacade`.
//!
//! # Methods
//!
//! - `acquire_inode_lease` / `release_inode_lease` / `renew_inode_lease`:
//!   inode-level leases managed by the Filer (方案 A).
//! - `acquire_range_lease` / `release_range_lease`: range-level leases
//!   managed by the Volume Server (方案 D). Used when the `LockRequest`
//!   carries a `Range`.
//! - `lookup_volume_id`: maps an inode to its volume_id, needed because
//!   `LockRequest` carries only `inode` (the kernel C client has no file
//!   handle context).

use async_trait::async_trait;

/// Backend abstracting the FUSE client's lease RPCs.
///
/// All methods are async and `'static`-send (no borrowing of `self`'s
/// lifetime), so `FuseLockManager` can drive them on any tokio runtime.
#[async_trait]
pub trait FuseLockBackend: Send + Sync {
    /// Acquire an inode-level lease from the Filer (方案 A).
    ///
    /// Returns `(token, expire_at_ms)`.
    async fn acquire_inode_lease(
        &self,
        inode: u64,
        client_id: &str,
        duration_ms: u64,
    ) -> Result<(String, u64), String>;

    /// Release an inode-level lease. Idempotent.
    async fn release_inode_lease(
        &self,
        inode: u64,
        client_id: &str,
        token: &str,
    ) -> Result<(), String>;

    /// Renew an inode-level lease. Extends TTL by `duration_ms`.
    async fn renew_inode_lease(
        &self,
        inode: u64,
        client_id: &str,
        token: &str,
        duration_ms: u64,
    ) -> Result<(), String>;

    /// Acquire a range-level lease from the Volume Server (方案 D).
    ///
    /// `exclusive = true` for write locks, `false` for read locks.
    /// Returns the lease token.
    ///
    /// Argument count mirrors `FuseClientFacade::acquire_lease` so the
    /// `FacadeLockBackend` impl is a 1:1 delegation (no parameter
    /// reshaping). Clippy's 7-arg threshold is exceeded intentionally.
    #[allow(clippy::too_many_arguments)]
    async fn acquire_range_lease(
        &self,
        volume_id: u64,
        inode: u64,
        stripe_start: u64,
        stripe_count: u64,
        client_id: &str,
        exclusive: bool,
        duration_ms: u64,
    ) -> Result<String, String>;

    /// Release a range-level lease. Idempotent.
    async fn release_range_lease(
        &self,
        volume_id: u64,
        inode: u64,
        stripe_start: u64,
        client_id: &str,
        token: &str,
    ) -> Result<(), String>;

    /// Look up the `volume_id` for an inode. Used when a range-level
    /// `LockRequest` arrives with only `inode` (no file-handle context,
    /// as is the case for the kernel C client).
    async fn lookup_volume_id(&self, inode: u64) -> Result<u64, String>;
}
