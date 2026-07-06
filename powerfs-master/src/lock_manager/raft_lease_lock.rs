use crossbeam::sync::ShardedLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct LeaseLockState {
    pub holder: String,
    acquired_at: Instant,
    _ttl: Duration,
    renew_count: u64,
}

pub struct LeaseLockGuard {
    key: String,
    holder: String,
    _manager: Arc<RaftLeaseLockManager>,
    local_guard: super::local_lock::LockGuard,
    released: AtomicBool,
}

impl std::fmt::Debug for LeaseLockGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaseLockGuard")
            .field("key", &self.key)
            .field("holder", &self.holder)
            .field("released", &self.released.load(Ordering::Acquire))
            .finish()
    }
}

impl Drop for LeaseLockGuard {
    fn drop(&mut self) {
        if !self.released.load(Ordering::Acquire) {
            self.released.store(true, Ordering::Release);
        }
    }
}

impl LeaseLockGuard {
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn holder(&self) -> &str {
        &self.holder
    }

    pub fn release(&mut self) {
        self.released.store(true, Ordering::Release);
        self.local_guard.release();
    }

    pub fn is_released(&self) -> bool {
        self.released.load(Ordering::Acquire)
    }
}

#[derive(Debug, Clone)]
pub struct RaftLeaseLockManager {
    local_locks: Arc<super::local_lock::LeaderLocalLockManager>,
    active_locks: Arc<ShardedLock<HashMap<String, LeaseLockState>>>,
}

impl RaftLeaseLockManager {
    pub fn new(local_locks: Arc<super::local_lock::LeaderLocalLockManager>) -> Self {
        RaftLeaseLockManager {
            local_locks,
            active_locks: Arc::new(ShardedLock::new(HashMap::new())),
        }
    }

    pub async fn acquire(&self, key: &str, ttl: Duration) -> Result<LeaseLockGuard, ()> {
        let holder = format!("{:?}:{}", std::thread::current().id(), uuid::Uuid::new_v4());

        let local_guard = self.local_locks.acquire(key).await;

        self.active_locks.write().unwrap().insert(
            key.to_string(),
            LeaseLockState {
                holder: holder.clone(),
                acquired_at: Instant::now(),
                _ttl: ttl,
                renew_count: 0,
            },
        );

        Ok(LeaseLockGuard {
            key: key.to_string(),
            holder,
            _manager: Arc::new(self.clone()),
            local_guard,
            released: AtomicBool::new(false),
        })
    }

    pub async fn renew(&self, key: &str, holder: &str) -> Result<(), ()> {
        if let Some(state) = self.active_locks.write().unwrap().get_mut(key) {
            if state.holder == holder {
                state.acquired_at = Instant::now();
                state.renew_count += 1;
                Ok(())
            } else {
                Err(())
            }
        } else {
            Err(())
        }
    }

    pub async fn release(&self, key: &str, holder: &str) -> Result<(), ()> {
        let mut map = self.active_locks.write().unwrap();
        if let Some(state) = map.get(key) {
            if state.holder == holder {
                map.remove(key);
                Ok(())
            } else {
                Err(())
            }
        } else {
            Err(())
        }
    }

    pub fn get_active_lock(&self, key: &str) -> Option<LeaseLockState> {
        self.active_locks.read().unwrap().get(key).cloned()
    }

    pub fn get_active_lock_count(&self) -> usize {
        self.active_locks.read().unwrap().len()
    }
}

impl Default for RaftLeaseLockManager {
    fn default() -> Self {
        Self::new(Arc::new(super::local_lock::LeaderLocalLockManager::new()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_acquire_and_release() {
        let manager = RaftLeaseLockManager::default();

        let mut guard = manager
            .acquire("test-key", Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(guard.key(), "test-key");
        assert!(!guard.is_released());

        guard.release();
        assert!(guard.is_released());
    }

    #[tokio::test]
    async fn test_renew() {
        let manager = RaftLeaseLockManager::default();

        let mut guard = manager
            .acquire("test-key", Duration::from_secs(30))
            .await
            .unwrap();
        let result = manager.renew("test-key", guard.holder()).await;
        assert!(result.is_ok());

        guard.release();
    }

    #[tokio::test]
    async fn test_release_wrong_holder() {
        let manager = RaftLeaseLockManager::default();

        let mut guard = manager
            .acquire("test-key", Duration::from_secs(30))
            .await
            .unwrap();
        let result = manager.release("test-key", "wrong-holder").await;
        assert!(result.is_err());

        guard.release();
    }

    #[tokio::test]
    async fn test_get_active_lock() {
        let manager = RaftLeaseLockManager::default();

        let mut guard = manager
            .acquire("test-key", Duration::from_secs(30))
            .await
            .unwrap();
        let state = manager.get_active_lock("test-key");
        assert!(state.is_some());
        assert_eq!(state.unwrap().holder, guard.holder());

        guard.release();
    }

    #[tokio::test]
    async fn test_concurrent_acquire_same_key() {
        let manager = RaftLeaseLockManager::default();

        let _guard1 = manager
            .acquire("shared-key", Duration::from_secs(30))
            .await
            .unwrap();

        let manager_clone = manager.clone();
        let _guard2 = tokio::spawn(async move {
            let _ = manager_clone
                .acquire("shared-key", Duration::from_secs(30))
                .await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let active_count = manager.get_active_lock_count();
        assert_eq!(active_count, 1);
    }
}
