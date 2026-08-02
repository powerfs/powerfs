//! RAII guard for lease lifecycle management.
//!
//! `LeaseGuard` ensures a lease is released even if the caller forgets to
//! explicitly release it (e.g., due to an early return or panic). This
//! eliminates the class of bugs where a lease token is acquired but never
//! released due to manual lifecycle management.
//!
//! # Usage
//!
//! ```ignore
//! let guard = LeaseGuard::new(token, manager, volume_id, inode, expire_at);
//! // ... use guard.token() for operations ...
//! guard.release().await?; // explicit release
//! // or just let guard drop — best-effort async release
//! ```

use crate::manager::LeaseManager;
use crate::token::LeaseToken;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

/// RAII guard for a held lease.
///
/// On drop, if not already released, spawns a best-effort async task to
/// release the lease on the server. This prevents lease leaks when the
/// caller's code path exits early.
pub struct LeaseGuard {
    token: LeaseToken,
    manager: Weak<dyn LeaseManager>,
    volume_id: u64,
    inode: u64,
    expire_at: Instant,
    released: bool,
}

impl LeaseGuard {
    /// Create a new guard. The `manager` is stored as `Weak` to avoid
    /// reference cycles if the manager holds guards (it typically does not).
    pub fn new(
        token: LeaseToken,
        manager: Weak<dyn LeaseManager>,
        volume_id: u64,
        inode: u64,
        expire_at: Instant,
    ) -> Self {
        Self {
            token,
            manager,
            volume_id,
            inode,
            expire_at,
            released: false,
        }
    }

    /// Create a guard from a strong `Arc<dyn LeaseManager>`.
    pub fn from_strong(
        token: LeaseToken,
        manager: Arc<dyn LeaseManager>,
        volume_id: u64,
        inode: u64,
        expire_at: Instant,
    ) -> Self {
        Self::new(token, Arc::downgrade(&manager), volume_id, inode, expire_at)
    }

    /// The lease token.
    pub fn token(&self) -> &LeaseToken {
        &self.token
    }

    /// The volume ID this lease is on.
    pub fn volume_id(&self) -> u64 {
        self.volume_id
    }

    /// The inode this lease protects.
    pub fn inode(&self) -> u64 {
        self.inode
    }

    /// Remaining duration before this lease expires.
    pub fn remaining(&self) -> Duration {
        self.expire_at.saturating_duration_since(Instant::now())
    }

    /// Whether this lease has expired.
    pub fn is_expired(&self) -> bool {
        Instant::now() >= self.expire_at
    }

    /// The absolute expiry instant.
    pub fn expire_at(&self) -> Instant {
        self.expire_at
    }

    /// Explicitly release the lease.
    ///
    /// This is preferred over relying on `Drop` as it allows the caller to
    /// handle release errors (e.g., retry on transient network failures).
    /// After calling this, `Drop` will be a no-op.
    ///
    /// Note: This method requires a tokio runtime context to be available
    /// for the async release. If called outside a runtime, use `mark_released()`
    /// and release manually.
    pub async fn release(mut self) -> Result<(), crate::LeaseError> {
        self.released = true;
        if let Some(mgr) = self.manager.upgrade() {
            mgr.release(self.volume_id, self.inode, &self.token).await?;
        }
        Ok(())
    }

    /// Mark the guard as released without sending an RPC.
    ///
    /// Use this when the caller has already released the lease via another
    /// path (e.g., `release_all_for_inode`) and just wants to prevent the
    /// `Drop` impl from sending a duplicate release.
    pub fn mark_released(&mut self) {
        self.released = true;
    }

    /// Check if this guard has been released (explicitly or via `mark_released`).
    pub fn is_released(&self) -> bool {
        self.released
    }
}

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        if !self.released {
            // Best-effort async release: spawn a task if we can upgrade the
            // manager weak ref. If upgrade fails (manager already dropped)
            // or no tokio runtime is available, the lease will eventually
            // expire on the server (TTL-based cleanup).
            if let Some(mgr) = self.manager.upgrade() {
                let volume_id = self.volume_id;
                let inode = self.inode;
                let token = self.token.clone();
                // Fire-and-forget: spawn on tokio runtime. The JoinHandle is
                // dropped (detached), allowing the task to complete in the
                // background. If not in a runtime context, the lease will
                // rely on server-side TTL expiry.
                tokio::spawn(async move {
                    let _ = mgr.release(volume_id, inode, &token).await;
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manager::LeaseState;
    use crate::token::LeaseMode;
    use std::sync::Mutex;

    /// A mock manager that records releases for testing.
    struct MockManager {
        releases: Mutex<Vec<(u64, u64, LeaseToken)>>,
    }

    impl MockManager {
        fn new() -> Self {
            Self {
                releases: Mutex::new(Vec::new()),
            }
        }

        fn release_count(&self) -> usize {
            self.releases.lock().unwrap().len()
        }
    }

    impl LeaseManager for MockManager {
        fn acquire(
            &self,
            _volume_id: u64,
            _inode: u64,
            _mode: LeaseMode,
            _stripe_start: u64,
            _stripe_count: u64,
            _duration_ms: u64,
        ) -> Pin<Box<dyn Future<Output = Result<LeaseToken, crate::LeaseError>> + Send + 'static>>
        {
            Box::pin(async { Ok(LeaseToken::new("mock-token".to_string())) })
        }

        fn release(
            &self,
            volume_id: u64,
            inode: u64,
            token: &LeaseToken,
        ) -> Pin<Box<dyn Future<Output = Result<(), crate::LeaseError>> + Send + 'static>> {
            let token = token.clone();
            self.releases
                .lock()
                .unwrap()
                .push((volume_id, inode, token));
            Box::pin(async { Ok(()) })
        }

        fn state(&self, _volume_id: u64, _inode: u64) -> Option<LeaseState> {
            None
        }

        fn release_all_for_inode(&self, _volume_id: u64, _inode: u64) -> Vec<(String, String)> {
            Vec::new()
        }

        fn invalidate(&self, _volume_id: u64, _inode: u64) {}

        fn remaining(&self, _volume_id: u64, _inode: u64) -> Option<Duration> {
            None
        }
    }

    use std::future::Future;
    use std::pin::Pin;

    #[tokio::test]
    async fn test_guard_explicit_release() {
        let mgr = Arc::new(MockManager::new());
        let guard = LeaseGuard::from_strong(
            LeaseToken::new("tok-1".to_string()),
            mgr.clone(),
            1,
            100,
            Instant::now() + Duration::from_secs(30),
        );

        assert!(!guard.is_released());
        guard.release().await.unwrap();
        assert_eq!(mgr.release_count(), 1);
    }

    #[tokio::test]
    async fn test_guard_drop_releases() {
        let mgr = Arc::new(MockManager::new());
        {
            let _guard = LeaseGuard::from_strong(
                LeaseToken::new("tok-2".to_string()),
                mgr.clone(),
                1,
                100,
                Instant::now() + Duration::from_secs(30),
            );
            // guard drops here
        }

        // Give the spawned task time to complete
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(mgr.release_count(), 1);
    }

    #[tokio::test]
    async fn test_guard_mark_released_no_drop_release() {
        let mgr = Arc::new(MockManager::new());
        {
            let mut guard = LeaseGuard::from_strong(
                LeaseToken::new("tok-3".to_string()),
                mgr.clone(),
                1,
                100,
                Instant::now() + Duration::from_secs(30),
            );
            guard.mark_released();
            assert!(guard.is_released());
            // guard drops here — should NOT call release
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(mgr.release_count(), 0);
    }

    #[test]
    fn test_remaining_and_expired() {
        let mgr = Arc::new(MockManager::new());
        let guard = LeaseGuard::from_strong(
            LeaseToken::new("tok-4".to_string()),
            mgr,
            1,
            100,
            Instant::now() + Duration::from_secs(30),
        );

        assert!(!guard.is_expired());
        assert!(guard.remaining() <= Duration::from_secs(30));
        assert!(guard.remaining() > Duration::from_secs(28));
    }
}
