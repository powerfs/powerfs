pub mod local_lock;
pub mod raft_lease_lock;

use std::sync::Arc;
use std::time::Duration;

pub use local_lock::LockGuard as LocalLockGuard;
pub use raft_lease_lock::LeaseLockGuard;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockLevel {
    Local,
    RaftLease,
}

pub struct LockManager {
    local_manager: Arc<local_lock::LeaderLocalLockManager>,
    lease_manager: Arc<raft_lease_lock::RaftLeaseLockManager>,
}

impl LockManager {
    pub fn new() -> Self {
        let local_manager = Arc::new(local_lock::LeaderLocalLockManager::new());
        let lease_manager = Arc::new(raft_lease_lock::RaftLeaseLockManager::new(
            local_manager.clone(),
        ));
        LockManager {
            local_manager,
            lease_manager,
        }
    }

    pub async fn acquire(&self, key: &str, level: LockLevel) -> LockHandle {
        match level {
            LockLevel::Local => {
                let guard = self.local_manager.acquire(key).await;
                LockHandle::Local(guard)
            }
            LockLevel::RaftLease => {
                let guard = self
                    .lease_manager
                    .acquire(key, Duration::from_secs(30))
                    .await
                    .unwrap();
                LockHandle::Lease(guard)
            }
        }
    }

    pub async fn try_acquire(&self, key: &str, level: LockLevel) -> Option<LockHandle> {
        match level {
            LockLevel::Local => self
                .local_manager
                .try_acquire(key)
                .await
                .map(LockHandle::Local),
            LockLevel::RaftLease => self
                .lease_manager
                .acquire(key, Duration::from_secs(30))
                .await
                .ok()
                .map(LockHandle::Lease),
        }
    }

    pub async fn renew(&self, key: &str, level: LockLevel) -> bool {
        match level {
            LockLevel::Local => true,
            LockLevel::RaftLease => {
                if let Some(state) = self.lease_manager.get_active_lock(key) {
                    self.lease_manager.renew(key, &state.holder).await.is_ok()
                } else {
                    false
                }
            }
        }
    }

    pub fn get_lock_count(&self) -> usize {
        self.local_manager.get_lock_count()
    }

    pub fn is_locked(&self, key: &str) -> bool {
        self.local_manager.is_locked(key)
    }

    pub fn local_manager(&self) -> &Arc<local_lock::LeaderLocalLockManager> {
        &self.local_manager
    }

    pub fn lease_manager(&self) -> &Arc<raft_lease_lock::RaftLeaseLockManager> {
        &self.lease_manager
    }
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

pub enum LockHandle {
    Local(local_lock::LockGuard),
    Lease(raft_lease_lock::LeaseLockGuard),
}

impl LockHandle {
    pub fn key(&self) -> &str {
        match self {
            LockHandle::Local(guard) => guard.key(),
            LockHandle::Lease(guard) => guard.key(),
        }
    }

    pub fn release(&mut self) {
        match self {
            LockHandle::Local(guard) => guard.release(),
            LockHandle::Lease(guard) => guard.release(),
        }
    }
}

impl Drop for LockHandle {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_lock_manager_local_acquire() {
        let manager = LockManager::new();

        let guard = manager.acquire("test-key", LockLevel::Local).await;
        assert_eq!(guard.key(), "test-key");
    }

    #[tokio::test]
    async fn test_lock_manager_lease_acquire() {
        let manager = LockManager::new();

        let guard = manager.acquire("test-key", LockLevel::RaftLease).await;
        assert_eq!(guard.key(), "test-key");
    }

    #[tokio::test]
    async fn test_lock_manager_try_acquire_local() {
        let manager = LockManager::new();

        let guard = manager.try_acquire("test-key", LockLevel::Local).await;
        assert!(guard.is_some());
    }

    #[tokio::test]
    async fn test_lock_manager_try_acquire_lease() {
        let manager = LockManager::new();

        let guard = manager.try_acquire("test-key", LockLevel::RaftLease).await;
        assert!(guard.is_some());
    }

    #[tokio::test]
    async fn test_lock_manager_renew_lease() {
        let manager = LockManager::new();

        let _guard = manager.acquire("test-key", LockLevel::RaftLease).await;
        let result = manager.renew("test-key", LockLevel::RaftLease).await;
        assert!(result);
    }

    #[tokio::test]
    async fn test_lock_manager_release() {
        let manager = LockManager::new();

        let mut guard = manager.acquire("test-key", LockLevel::Local).await;
        guard.release();
    }
}
