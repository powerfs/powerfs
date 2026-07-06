use crossbeam::sync::ShardedLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[derive(Debug)]
struct LockEntry {
    held: AtomicBool,
    notify: tokio::sync::Notify,
}

pub struct LockGuard {
    key: String,
    entry: Arc<LockEntry>,
}

impl std::fmt::Debug for LockGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LockGuard").field("key", &self.key).finish()
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        self.entry.held.store(false, Ordering::Release);
        self.entry.notify.notify_one();
    }
}

impl LockGuard {
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn release(&mut self) {
        self.entry.held.store(false, Ordering::Release);
        self.entry.notify.notify_one();
    }
}

#[derive(Debug, Clone)]
pub struct LeaderLocalLockManager {
    locks: Arc<ShardedLock<HashMap<String, Arc<LockEntry>>>>,
}

impl LeaderLocalLockManager {
    pub fn new() -> Self {
        LeaderLocalLockManager {
            locks: Arc::new(ShardedLock::new(HashMap::new())),
        }
    }

    pub async fn acquire(&self, key: &str) -> LockGuard {
        let entry = {
            let mut map = self.locks.write().unwrap();
            map.entry(key.to_string())
                .or_insert_with(|| {
                    Arc::new(LockEntry {
                        held: AtomicBool::new(false),
                        notify: tokio::sync::Notify::new(),
                    })
                })
                .clone()
        };

        loop {
            if !entry.held.swap(true, Ordering::AcqRel) {
                return LockGuard {
                    key: key.to_string(),
                    entry,
                };
            }
            entry.notify.notified().await;
        }
    }

    pub async fn try_acquire(&self, key: &str) -> Option<LockGuard> {
        let entry = {
            let mut map = self.locks.write().unwrap();
            map.entry(key.to_string())
                .or_insert_with(|| {
                    Arc::new(LockEntry {
                        held: AtomicBool::new(false),
                        notify: tokio::sync::Notify::new(),
                    })
                })
                .clone()
        };

        if !entry.held.swap(true, Ordering::AcqRel) {
            Some(LockGuard {
                key: key.to_string(),
                entry,
            })
        } else {
            None
        }
    }

    pub fn get_lock_count(&self) -> usize {
        self.locks.read().unwrap().len()
    }

    pub fn is_locked(&self, key: &str) -> bool {
        if let Some(entry) = self.locks.read().unwrap().get(key) {
            entry.held.load(Ordering::Acquire)
        } else {
            false
        }
    }
}

impl Default for LeaderLocalLockManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    #[tokio::test]
    async fn test_acquire_and_release() {
        let manager = LeaderLocalLockManager::new();

        let guard = manager.acquire("test-key").await;
        assert_eq!(guard.key(), "test-key");

        drop(guard);
    }

    #[tokio::test]
    async fn test_try_acquire_success() {
        let manager = LeaderLocalLockManager::new();

        let guard = manager.try_acquire("test-key").await;
        assert!(guard.is_some());
        assert_eq!(guard.unwrap().key(), "test-key");
    }

    #[tokio::test]
    async fn test_try_acquire_failure() {
        let manager = LeaderLocalLockManager::new();

        let _guard1 = manager.acquire("test-key").await;

        let guard2 = manager.try_acquire("test-key").await;
        assert!(guard2.is_none());
    }

    #[tokio::test]
    async fn test_concurrent_access() {
        let manager = LeaderLocalLockManager::new();
        let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let mut tasks = Vec::new();

        for _ in 0..10 {
            let manager_clone = manager.clone();
            let counter_clone = counter.clone();

            let task = tokio::spawn(async move {
                let _guard = manager_clone.acquire("shared-key").await;
                let prev = counter_clone.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(5)).await;
                let current = counter_clone.load(Ordering::SeqCst);

                assert_eq!(current, prev + 1);
            });

            tasks.push(task);
        }

        for task in tasks {
            task.await.unwrap();
        }

        assert_eq!(counter.load(Ordering::SeqCst), 10);
    }

    #[tokio::test]
    async fn test_different_keys() {
        let manager = LeaderLocalLockManager::new();

        let _guard1 = manager.acquire("key1").await;
        let _guard2 = manager.acquire("key2").await;

        assert_eq!(manager.get_lock_count(), 2);
    }

    #[tokio::test]
    async fn test_lock_guard_release() {
        let manager = LeaderLocalLockManager::new();

        let mut guard = manager.acquire("test-key").await;
        guard.release();
    }

    #[tokio::test]
    async fn test_lock_reacquire_after_release() {
        let manager = LeaderLocalLockManager::new();

        let guard1 = manager.acquire("test-key").await;
        drop(guard1);

        let guard2 = manager.try_acquire("test-key").await;
        assert!(guard2.is_some());
    }
}
