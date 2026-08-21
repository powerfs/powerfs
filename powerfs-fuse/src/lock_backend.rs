//! `FuseLockBackend` impl bridging `powerfs-lock-fuse` to the existing
//! `FuseClientFacade` + `MetadataCache`.
//!
//! This is the conservative-adapter wiring described in
//! `docs/lock-optimization-plan.md` §4.1. It does NOT replace
//! `VolumeLeaseManager`; it provides a `FuseLockManager` instance
//! ready for new code paths that prefer the unified `LockManager`
//! trait. Existing read/write/release paths still call
//! `VolumeLeaseManager` directly (kept as-is per the conservative-
//! adapter risk note on bidirectional dependency).
//!
//! # Method mapping
//!
//! | `FuseLockBackend` method    | `FuseClientFacade` method           |
//! |----------------------------|-------------------------------------|
//! | `acquire_inode_lease`      | `facade.acquire_inode_lease`        |
//! | `release_inode_lease`      | `facade.release_inode_lease`        |
//! | `renew_inode_lease`        | `facade.renew_inode_lease`          |
//! | `acquire_range_lease`     | `facade.acquire_lease`              |
//! | `release_range_lease`     | `facade.release_lease`              |
//! | `lookup_volume_id`        | `cache.get_inode(inode).fid`        |
//!
//! The `lookup_volume_id` impl is synchronous (no RPC) — it just
//! reads the cached inode → fid mapping. Callers in the FUSE path
//! always have metadata cached before issuing a range lease request
//! (the read/write path fetches metadata first).

use crate::cache::MetadataCache;
use async_trait::async_trait;
use log::debug;
use powerfs_fuse_core::FuseClientFacade;
use powerfs_lock_fuse::FuseLockBackend;
use std::sync::Arc;

/// Backend bridging `FuseLockBackend` to `FuseClientFacade` + the
/// FUSE client's inode metadata cache.
pub struct FacadeLockBackend {
    facade: Arc<FuseClientFacade>,
    cache: Arc<MetadataCache>,
}

impl FacadeLockBackend {
    pub fn new(facade: Arc<FuseClientFacade>, cache: Arc<MetadataCache>) -> Self {
        Self { facade, cache }
    }
}

/// Resolve a `volume_id` from the inode metadata cache.
///
/// Pure function over `&MetadataCache` — extracted so it can be
/// tested without constructing a `FuseClientFacade`. The FUSE read/
/// write paths always populate the cache before issuing a range
/// lease request (metadata is fetched first); if the entry is
/// Stale/Tombstone, `get_inode` returns `None`, surfacing as an
/// error here so the caller knows to refresh metadata first.
pub fn volume_id_from_cache(cache: &MetadataCache, inode: u64) -> Result<u64, String> {
    cache
        .get_inode(inode)
        .and_then(|e| e.fid)
        .map(|fid| fid.volume_id.0)
        .ok_or_else(|| format!("no cached volume_id for inode {}", inode))
}

#[async_trait]
impl FuseLockBackend for FacadeLockBackend {
    async fn acquire_inode_lease(
        &self,
        inode: u64,
        client_id: &str,
        duration_ms: u64,
    ) -> Result<(String, u64), String> {
        self.facade
            .acquire_inode_lease(inode, client_id, duration_ms)
            .await
    }

    async fn release_inode_lease(
        &self,
        inode: u64,
        client_id: &str,
        token: &str,
    ) -> Result<(), String> {
        self.facade
            .release_inode_lease(inode, client_id, token)
            .await
    }

    async fn renew_inode_lease(
        &self,
        inode: u64,
        client_id: &str,
        token: &str,
        duration_ms: u64,
    ) -> Result<(), String> {
        self.facade
            .renew_inode_lease(inode, client_id, token, duration_ms)
            .await
    }

    async fn acquire_range_lease(
        &self,
        volume_id: u64,
        inode: u64,
        stripe_start: u64,
        stripe_count: u64,
        client_id: &str,
        exclusive: bool,
        duration_ms: u64,
    ) -> Result<String, String> {
        self.facade
            .acquire_lease(
                volume_id,
                inode,
                stripe_start,
                stripe_count,
                client_id,
                exclusive,
                duration_ms,
            )
            .await
    }

    async fn release_range_lease(
        &self,
        volume_id: u64,
        inode: u64,
        stripe_start: u64,
        client_id: &str,
        token: &str,
    ) -> Result<(), String> {
        self.facade
            .release_lease(volume_id, inode, stripe_start, client_id, token)
            .await
    }

    async fn lookup_volume_id(&self, inode: u64) -> Result<u64, String> {
        volume_id_from_cache(&self.cache, inode)
    }
}

/// Phase 3 Lease Recall: concrete `LeaseReleaser` that wraps the
/// `FuseClientFacade` + a tokio runtime handle so the sync
/// `InvalidateHandler` can spawn async `ReleaseInodeLease` RPCs.
///
/// Constructed in `PowerFsFs::new` after the sync client (which owns
/// the runtime) is available.
pub struct FacadeLeaseReleaser {
    facade: Arc<FuseClientFacade>,
    client_id: String,
    handle: tokio::runtime::Handle,
}

impl FacadeLeaseReleaser {
    pub fn new(
        facade: Arc<FuseClientFacade>,
        client_id: String,
        handle: tokio::runtime::Handle,
    ) -> Self {
        Self {
            facade,
            client_id,
            handle,
        }
    }
}

impl crate::invalidate_handler::LeaseReleaser for FacadeLeaseReleaser {
    fn release(&self, inode: u64, token: String) {
        let facade = self.facade.clone();
        let client_id = self.client_id.clone();
        self.handle.spawn(async move {
            if let Err(e) = facade.release_inode_lease(inode, &client_id, &token).await {
                debug!(
                    "FacadeLeaseReleaser: release_inode_lease inode={} failed: {} \
                     (will TTL on server side)",
                    inode, e
                );
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::MetadataCache;

    /// Cache miss: `volume_id_from_cache` returns a descriptive error
    /// mentioning the inode. The hit path is exercised by integration
    /// tests via the live FUSE client (constructing a `CachedEntry` in
    /// unit tests would require touching 20+ fields — the cache-hit
    /// branch is one line of `Option::and_then` and is sufficiently
    /// covered by the integration tests).
    #[test]
    fn test_volume_id_from_cache_miss_returns_error() {
        let cache = MetadataCache::default();
        let err = volume_id_from_cache(&cache, 999).unwrap_err();
        assert!(
            err.contains("no cached volume_id"),
            "expected descriptive error, got: {}",
            err
        );
        assert!(err.contains("999"), "error must mention the inode");
    }
}
