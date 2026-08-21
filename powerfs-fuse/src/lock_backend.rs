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

/// §13 Cap model: concrete `CapHandler` that wraps the
/// `FuseClientFacade` + `CapFlusher` + a tokio runtime handle so the
/// sync `InvalidateHandler` can spawn async `CapRecallAck` RPCs.
///
/// # Flush strategy
///
/// `flush_and_ack` is called when a `CapRecallNotify` arrives for a cap
/// with dirty CAP_W. The handler:
/// 1. Spawns a tokio task.
/// 2. Calls `CapFlusher::flush_and_sync` — this drains dirty chunks,
///    writes them to the Volume Server, and syncs metadata to the Filer
///    via Raft. This is the **same** flush path used by `release()`,
///    ensuring consistency.
/// 3. On flush success: sends `cap_recall_ack` to the Filer, completing
///    the recall.
/// 4. On flush failure: does NOT send ACK. The server's 2s recall
///    timeout will force-reclaim the cap (with a health penalty). The
///    dirty data remains in the local cache for the background flusher
///    to retry. This is the safest option — sending ACK without
///    successful flush would lose dirty data.
pub struct FacadeCapHandler {
    facade: Arc<FuseClientFacade>,
    flusher: Arc<dyn crate::invalidate_handler::CapFlusher>,
    client_id: String,
    handle: tokio::runtime::Handle,
}

impl FacadeCapHandler {
    pub fn new(
        facade: Arc<FuseClientFacade>,
        flusher: Arc<dyn crate::invalidate_handler::CapFlusher>,
        client_id: String,
        handle: tokio::runtime::Handle,
    ) -> Self {
        Self {
            facade,
            flusher,
            client_id,
            handle,
        }
    }
}

impl crate::invalidate_handler::CapHandler for FacadeCapHandler {
    fn flush_and_ack(&self, inode: u64, token: String, _epoch: u64) {
        let facade = self.facade.clone();
        let flusher = self.flusher.clone();
        let client_id = self.client_id.clone();
        let token_for_flush = token.clone();
        self.handle.spawn(async move {
            debug!(
                "FacadeCapHandler: flush_and_ack inode={} token={} — flushing dirty data before ACK",
                inode, token_for_flush
            );
            // Step 1: Flush dirty chunks + sync metadata. This uses the
            // same path as release() (drain_dirty_for_inode →
            // write_blob_batch_with_lease → sync_size_chunks_on_close),
            // ensuring dirty data is safely persisted before we ACK.
            let flush_result = flusher.flush_and_sync(inode, &token_for_flush);
            match flush_result {
                Ok(()) => {
                    debug!(
                        "FacadeCapHandler: flush succeeded for inode={}, sending CapRecallAck",
                        inode
                    );
                    // Step 2: Send CapRecallAck — the server will complete
                    // the recall and grant caps to the waiting client.
                    if let Err(e) = facade.cap_recall_ack(inode, &client_id, &token).await {
                        debug!(
                            "FacadeCapHandler: cap_recall_ack inode={} failed: {} \
                             (server will force-reclaim after 2s timeout)",
                            inode, e
                        );
                    }
                }
                Err(e) => {
                    // Flush failed — do NOT send ACK. The server's 2s
                    // recall timeout will force-reclaim the cap. Dirty
                    // data remains in the local cache for the background
                    // flusher to retry. Sending ACK without successful
                    // flush would lose dirty data.
                    debug!(
                        "FacadeCapHandler: flush FAILED for inode={} err={:?} — NOT sending ACK \
                         (server will force-reclaim after 2s timeout; dirty data retained for retry)",
                        inode, e
                    );
                }
            }
        });
    }

    fn immediate_ack(&self, inode: u64, token: String, _epoch: u64) {
        let facade = self.facade.clone();
        let client_id = self.client_id.clone();
        self.handle.spawn(async move {
            debug!(
                "FacadeCapHandler: immediate_ack inode={} token={}",
                inode, token
            );
            if let Err(e) = facade.cap_recall_ack(inode, &client_id, &token).await {
                debug!(
                    "FacadeCapHandler: cap_recall_ack inode={} failed: {} \
                     (server will force-reclaim after 2s timeout)",
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
