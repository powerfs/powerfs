//! Client-side lease manager — wraps `powerfs_lease::LeaseManager` with
//! PowerFS-specific FuseClientFacade integration and stripe-level caching.
//!
//! The `LeaseManager` trait, `LeaseMode`, `LeaseToken`, `LeaseState`, and
//! `LeaseGuard` are re-exported from the `powerfs-lease` crate. This module
//! provides `VolumeLeaseManager`, the concrete implementation that adds:
//! - stripe-granularity lease caching (zero-RPC on cache hit)
//! - `FuseClientFacade` async RPC for acquire/release
//! - `release_all_for_inode` for close-time cleanup of all read leases

pub use powerfs_lease::{LeaseGuard, LeaseManager, LeaseMode, LeaseState, LeaseToken};

use crate::fuse_client_facade::FuseClientFacade;
use powerfs_lease::LeaseError;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Lease cache key.
#[derive(Clone, PartialEq, Eq, Hash)]
struct LeaseKey {
    volume_id: u64,
    inode: u64,
    stripe_start: u64,
    stripe_count: u64,
    exclusive: bool,
}

/// Lease cache entry.
struct LeaseCacheEntry {
    token: String,
    expire_at: Instant,
    mode: LeaseMode,
}

/// `VolumeLeaseManager` — wraps [`FuseClientFacade`] to implement
/// [`powerfs_lease::LeaseManager`] with stripe-granularity lease caching.
///
/// `FuseClientFacade::acquire_lease` internally updates `VolumeClient`'s
/// (volume_id, inode) lease table, so the file `release()` path's existing
/// `release_lease` call can still find the token to release; this struct
/// additionally maintains a stripe-granularity cache for read-path zero-RPC reuse.
pub struct VolumeLeaseManager {
    facade: Arc<FuseClientFacade>,
    client_id: Arc<String>,
    cache: Arc<RwLock<HashMap<LeaseKey, LeaseCacheEntry>>>,
}

impl VolumeLeaseManager {
    pub fn new(facade: Arc<FuseClientFacade>, client_id: String) -> Self {
        Self {
            facade,
            client_id: Arc::new(client_id),
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Synchronously check cache for a valid (non-expired) lease.
    fn check_cache(&self, key: &LeaseKey) -> Option<String> {
        let cache = self.cache.read().unwrap();
        if let Some(entry) = cache.get(key) {
            if Instant::now() < entry.expire_at {
                return Some(entry.token.clone());
            }
        }
        None
    }

    /// Invalidate all cached leases for a given (volume_id, inode).
    ///
    /// Called by file `release()` on close to avoid reusing released tokens.
    pub fn invalidate(&self, volume_id: u64, inode: u64) {
        let mut cache = self.cache.write().unwrap();
        cache.retain(|k, _| !(k.volume_id == volume_id && k.inode == inode));
    }

    /// Release all leases for a given (volume_id, inode): extracts all tokens
    /// from the cache, clears the cache, then releases each on the volume server.
    ///
    /// This complements `invalidate()`: `invalidate()` only clears the local
    /// cache without notifying the server, causing server-side read lease
    /// accumulation that blocks other clients' write leases (stripe lease
    /// conflict). This method is called on close to ensure read leases are
    /// released on the server as well.
    pub fn release_all_for_inode(&self, volume_id: u64, inode: u64) -> Vec<(String, String)> {
        // 1. Extract all matching (token, client_id) and clear cache
        let tokens: Vec<(String, String)> = {
            let mut cache = self.cache.write().unwrap();
            let keys_to_remove: Vec<LeaseKey> = cache
                .keys()
                .filter(|k| k.volume_id == volume_id && k.inode == inode)
                .cloned()
                .collect();
            let mut result = Vec::new();
            for key in keys_to_remove {
                if let Some(entry) = cache.remove(&key) {
                    result.push((entry.token.clone(), (*self.client_id).clone()));
                }
            }
            result
        };
        // 2. Return (token, client_id) list for async release by caller
        tokens
    }
}

/// Convert a string error to `LeaseError` for the trait impl.
fn map_err(e: String) -> LeaseError {
    LeaseError::Internal(e)
}

impl powerfs_lease::LeaseManager for VolumeLeaseManager {
    fn acquire(
        &self,
        volume_id: u64,
        inode: u64,
        mode: LeaseMode,
        stripe_start: u64,
        stripe_count: u64,
        duration_ms: u64,
    ) -> Pin<Box<dyn Future<Output = Result<LeaseToken, LeaseError>> + Send + 'static>> {
        let key = LeaseKey {
            volume_id,
            inode,
            stripe_start,
            stripe_count,
            exclusive: mode.is_exclusive(),
        };

        // 1. Synchronous cache check: hit → zero RPC return.
        if let Some(token) = self.check_cache(&key) {
            log::debug!(
                "lease: cache hit volume={} inode={} stripe=[{},{}) exclusive={}",
                volume_id,
                inode,
                stripe_start,
                stripe_start + stripe_count,
                key.exclusive
            );
            return Box::pin(async move { Ok(LeaseToken::new(token)) });
        }

        // 2. Cache miss: clone Arc to construct 'static future, send RPC and cache.
        let facade = self.facade.clone();
        let client_id = self.client_id.clone();
        let cache = self.cache.clone();
        Box::pin(async move {
            let token = facade
                .acquire_lease(
                    volume_id,
                    inode,
                    stripe_start,
                    stripe_count,
                    &client_id,
                    mode.is_exclusive(),
                    duration_ms,
                )
                .await
                .map_err(map_err)?;

            {
                let mut guard = cache.write().unwrap();
                guard.insert(
                    key.clone(),
                    LeaseCacheEntry {
                        token: token.clone(),
                        expire_at: Instant::now() + Duration::from_millis(duration_ms),
                        mode,
                    },
                );
            }

            log::debug!(
                "lease: acquired volume={} inode={} stripe=[{},{}) exclusive={}",
                volume_id,
                inode,
                stripe_start,
                stripe_start + stripe_count,
                key.exclusive
            );
            Ok(LeaseToken::new(token))
        })
    }

    fn release(
        &self,
        volume_id: u64,
        inode: u64,
        token: &LeaseToken,
    ) -> Pin<Box<dyn Future<Output = Result<(), LeaseError>> + Send + 'static>> {
        let facade = self.facade.clone();
        let client_id = self.client_id.clone();
        let cache = self.cache.clone();
        let token_str = token.as_str().to_string();
        Box::pin(async move {
            // Remove from cache first (by (volume_id, inode)) to avoid reusing released token.
            {
                let mut guard = cache.write().unwrap();
                guard.retain(|k, _| !(k.volume_id == volume_id && k.inode == inode));
            }
            facade
                .release_lease(volume_id, inode, &client_id, &token_str)
                .await
                .map_err(map_err)?;
            Ok(())
        })
    }

    fn state(&self, volume_id: u64, inode: u64) -> Option<LeaseState> {
        let cache = self.cache.read().unwrap();
        for (k, v) in cache.iter() {
            if k.volume_id == volume_id && k.inode == inode {
                return Some(LeaseState {
                    token: LeaseToken::new(v.token.clone()),
                    mode: v.mode,
                    expire_at: v.expire_at,
                    volume_id: k.volume_id,
                    inode: k.inode,
                    stripe_start: k.stripe_start,
                    stripe_count: k.stripe_count,
                });
            }
        }
        None
    }

    fn release_all_for_inode(&self, volume_id: u64, inode: u64) -> Vec<(String, String)> {
        // Delegate to the inherent method
        VolumeLeaseManager::release_all_for_inode(self, volume_id, inode)
    }

    fn invalidate(&self, volume_id: u64, inode: u64) {
        VolumeLeaseManager::invalidate(self, volume_id, inode)
    }

    fn remaining(&self, volume_id: u64, inode: u64) -> Option<Duration> {
        let cache = self.cache.read().unwrap();
        for (k, v) in cache.iter() {
            if k.volume_id == volume_id && k.inode == inode {
                return Some(v.expire_at.saturating_duration_since(Instant::now()));
            }
        }
        None
    }
}
