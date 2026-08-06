//! Client-side lease manager trait (async, with optional caching).
//!
//! This trait is implemented by the FUSE client's `VolumeLeaseManager` (which
//! adds stripe-level caching) and potentially by other clients. The trait
//! returns `'static` futures so callers can drive them via
//! `SyncFuseClientFacade::block_on` or any tokio runtime.

use crate::token::{LeaseMode, LeaseToken};
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

/// Lease state info (for monitoring/queries).
#[derive(Clone, Debug)]
pub struct LeaseState {
    pub token: LeaseToken,
    pub mode: LeaseMode,
    pub expire_at: Instant,
    pub volume_id: u64,
    pub inode: u64,
    pub stripe_start: u64,
    pub stripe_count: u64,
}

/// Client-side lease manager interface (async, with caching).
///
/// Implementors typically wrap a `FuseClientFacade` and add stripe-granularity
/// lease caching: cache hits return zero-RPC, misses send an AcquireLease RPC.
pub trait LeaseManager: Send + Sync {
    /// Acquire a lease (cache-reuse enabled).
    ///
    /// - Cache hit (valid, non-expired) → zero RPC return
    /// - Cache miss/expired → RPC acquire and cache
    fn acquire(
        &self,
        volume_id: u64,
        inode: u64,
        mode: LeaseMode,
        stripe_start: u64,
        stripe_count: u64,
        duration_ms: u64,
    ) -> Pin<Box<dyn Future<Output = Result<LeaseToken, crate::LeaseError>> + Send + 'static>>;

    /// Release a lease (send ReleaseLease RPC and remove from cache).
    fn release(
        &self,
        volume_id: u64,
        inode: u64,
        token: &LeaseToken,
    ) -> Pin<Box<dyn Future<Output = Result<(), crate::LeaseError>> + Send + 'static>>;

    /// Query lease state (monitoring). Returns first matching (volume_id, inode) entry.
    fn state(&self, volume_id: u64, inode: u64) -> Option<LeaseState>;

    /// Release all leases for a given (volume_id, inode) pair.
    /// Returns the list of (stripe_start, token, client_id) triples that were
    /// released.
    ///
    /// This is used by FUSE `release()` to clean up all read leases held by
    /// this client on a file before close, preventing server-side lease
    /// accumulation that blocks other clients' write leases.
    ///
    /// `stripe_start` is included so the caller can release each lease on the
    /// correct stripe (the server keys leases by (inode, stripe_start)).
    fn release_all_for_inode(&self, volume_id: u64, inode: u64) -> Vec<(u64, String, String)>;

    /// Invalidate (drop local cache for) all leases on a given (volume_id, inode).
    /// Does NOT notify the server — use `release_all_for_inode` for that.
    fn invalidate(&self, volume_id: u64, inode: u64);

    /// Check remaining duration of a cached lease, if any.
    fn remaining(&self, volume_id: u64, inode: u64) -> Option<Duration>;
}
