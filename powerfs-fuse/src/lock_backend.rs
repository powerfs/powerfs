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
use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};

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
    /// Per-inode serialization for concurrent recall processing.
    ///
    /// Without this, recall epoch=N spawns a task that enters
    /// `sync_inline_buffer` (Raft network blocking), while epoch=N+1
    /// arrives and calls `immediate_ack()` in a SECOND independent task
    /// that sends ACK without waiting for epoch=N's Raft to commit.
    /// The server sees epoch=N+1's ACK → promotes waiting client →
    /// that client reads a stale `content_size` from filer → overwrites
    /// epoch=N's still-in-flight dirty data.
    ///
    /// Fix: both `flush_and_ack` and `immediate_ack` acquire the SAME
    /// per-inode `tokio::Mutex` inside their spawned tasks, guaranteeing
    /// recall processing for the same inode is strictly serial.
    recall_locks: Arc<StdMutex<HashMap<u64, Arc<tokio::sync::Mutex<()>>>>>,
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
            recall_locks: Arc::new(StdMutex::new(HashMap::new())),
        }
    }

    /// Get or create the per-inode recall serialization gate.
    fn lock_for(&self, inode: u64) -> Arc<tokio::sync::Mutex<()>> {
        let mut guards = self.recall_locks.lock().unwrap();
        guards
            .entry(inode)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }
}

impl crate::invalidate_handler::CapHandler for FacadeCapHandler {
    fn flush_and_ack(&self, inode: u64, token: String, _epoch: u64) {
        let facade = self.facade.clone();
        let flusher = self.flusher.clone();
        let client_id = self.client_id.clone();
        let token_for_flush = token.clone();
        let lock = self.lock_for(inode);
        let handler = Arc::downgrade(&self.recall_locks);
        self.handle.spawn(async move {
            // H3 + H8: Total work must complete within 1750ms (BEFORE server's
            // 2000ms gather_timeout fires). Critically: flush_and_sync() is a
            // SYNCHRONOUS blocking function (Raft RPCs + volume blob writes
            // with retries). We CANNOT run it inline in the async task —
            // tokio::time::timeout only yields at `.await` points, so a
            // blocking inline call would:
            //   - Starve the tokio runtime thread (H8: all async tasks stall)
            //   - Make the 1750ms timeout 100% ineffective (guard stays held
            //     until sync code returns, causing CASCADE force-reclaims:
            //     every recall epoch times out → every client gets reclaimed)
            //
            // Fix (H7): run only the SYNC flush portion inside
            // `tokio::task::spawn_blocking` (dedicated blocking worker pool),
            // then `.await` its JoinHandle — awaiting is a proper yield point
            // so the outer 1750ms timeout CAN interrupt the wait, dropping
            // `_guard` immediately at timeout. The spawn_blocking worker
            // continues the abandoned flush in background (can't be killed
            // without pthread_cancel) but we've already released the
            // per-inode serialization gate, allowing fresh recall epochs
            // to make progress instead of guaranteed cascade timeout.
            let work = async {
                let _guard = lock.lock().await;
                debug!(
                    "FacadeCapHandler: flush_and_ack inode={} token={} — flushing dirty data before ACK",
                    inode, token_for_flush
                );
                // Sync blocking flush → offload to blocking pool
                let flush_result = tokio::task::spawn_blocking({
                    let f = flusher.clone();
                    let t = token_for_flush.clone();
                    move || f.flush_and_sync(inode, &t)
                })
                .await;
                let flush_result = match flush_result {
                    Ok(res) => res,
                    Err(join_err) => Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("spawn_blocking panicked: {}", join_err),
                    )),
                };
                match flush_result {
                    Ok(()) => {
                        debug!(
                            "FacadeCapHandler: flush succeeded for inode={}, sending CapRecallAck",
                            inode
                        );
                        if let Err(e) = facade.cap_recall_ack(inode, &client_id, &token).await {
                            debug!(
                                "FacadeCapHandler: cap_recall_ack inode={} failed: {} \
                                 (server will force-reclaim after 2s timeout)",
                                inode, e
                            );
                        }
                    }
                    Err(e) => {
                        debug!(
                            "FacadeCapHandler: flush FAILED for inode={} err={:?} — NOT sending ACK \
                             (server will force-reclaim after 2s timeout; dirty data retained for retry)",
                            inode, e
                        );
                    }
                }
                // _guard dropped here → per-inode serialization released
            };

            match tokio::time::timeout(
                std::time::Duration::from_millis(1750),
                work,
            )
            .await
            {
                Ok(()) => {}
                Err(_elapsed) => {
                    log::warn!(
                        "FacadeCapHandler: flush_and_ack TIMEOUT inode={} after 1750ms — \
                         per-inode lock released (spawn_blocking flush thread continues in \
                         background, ignored). Server will likely force-reclaim this cap.",
                        inode
                    );
                }
            }

            // H2: reclaim HashMap slot if we're the last ref.
            if let Some(recall_locks) = handler.upgrade() {
                if Arc::strong_count(&lock) == 1 {
                    let mut g = recall_locks.lock().unwrap();
                    if g.get(&inode).map(|e| Arc::strong_count(e)) == Some(1) {
                        g.remove(&inode);
                    }
                }
            }
        });
    }

    fn immediate_ack(&self, inode: u64, token: String, _epoch: u64) {
        let facade = self.facade.clone();
        let client_id = self.client_id.clone();
        let lock = self.lock_for(inode);
        let handler = Arc::downgrade(&self.recall_locks);
        self.handle.spawn(async move {
            // H3: same timeout rationale as flush_and_ack. Here the only
            // blocking work is the cap_recall_ack network call; still bound
            // it to keep the per-inode lock bounded.
            let work = async {
                let _guard = lock.lock().await;
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
            };

            match tokio::time::timeout(std::time::Duration::from_millis(1750), work).await {
                Ok(()) => {}
                Err(_elapsed) => {
                    log::warn!(
                        "FacadeCapHandler: immediate_ack TIMEOUT inode={} after 1750ms — \
                         releasing per-inode lock.",
                        inode
                    );
                }
            }

            // H2: reclaim HashMap slot.
            if let Some(recall_locks) = handler.upgrade() {
                if Arc::strong_count(&lock) == 1 {
                    let mut g = recall_locks.lock().unwrap();
                    if g.get(&inode).map(|e| Arc::strong_count(e)) == Some(1) {
                        g.remove(&inode);
                    }
                }
            }
        });
    }

    /// H2 / H5: Eager HashMap cleanup when the server-pushed Invalidate
    /// EVICT path drops our cached inode entry. Called by
    /// `InvalidateHandler` right after `cache.invalidate_inode()` +
    /// `chunk_cache.remove_inode_chunks()`.
    ///
    /// This is a best-effort eager reclaim. The leak-proof cleanup is
    /// the strong_count==1 check at the END of every flush_and_ack /
    /// immediate_ack spawned task.
    fn on_inode_evicted(&self, inode: u64) {
        let mut guards = self.recall_locks.lock().unwrap();
        guards.remove(&inode);
        let capacity = guards.capacity();
        let len = guards.len();
        if capacity > 1024 && len < capacity / 4 {
            guards.shrink_to(len.next_power_of_two());
        }
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
