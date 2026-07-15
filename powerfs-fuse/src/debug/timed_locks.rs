//! 带超时检测的锁包装器
//! 
//! 当获取锁的时间超过阈值时自动打印警告日志，帮助调试死锁问题

use std::sync::{Mutex, RwLock, TryLockError, TryLockResult};
use std::time::{Duration, Instant};

use log::warn;

/// 锁获取超时警告阈值（毫秒）
const LOCK_WARN_THRESHOLD_MS: u64 = 100;

/// 带超时检测的 Mutex 包装器
pub struct TimedMutex<T> {
    inner: Mutex<T>,
    name: &'static str,
}

impl<T> TimedMutex<T> {
    pub const fn new(value: T, name: &'static str) -> Self {
        Self {
            inner: Mutex::new(value),
            name,
        }
    }

    pub fn lock(&self) -> TimedMutexGuard<'_, T> {
        let start = Instant::now();
        
        // 先尝试非阻塞获取
        match self.inner.try_lock() {
            Ok(guard) => {
                TimedMutexGuard { inner: guard }
            }
            Err(TryLockError::WouldBlock) => {
                // 记录等待开始
                warn!(
                    "TimedMutex[{}]: waiting for lock (would block)",
                    self.name
                );
                
                // 阻塞获取
                let guard = self.inner.lock().unwrap_or_else(|e| {
                    panic!("TimedMutex[{}] lock poisoned: {}", self.name, e);
                });
                
                let elapsed = start.elapsed();
                if elapsed > Duration::from_millis(LOCK_WARN_THRESHOLD_MS) {
                    warn!(
                        "TimedMutex[{}]: lock acquired after {:?} (threshold: {}ms)",
                        self.name, elapsed, LOCK_WARN_THRESHOLD_MS
                    );
                }
                
                TimedMutexGuard { inner: guard }
            }
            Err(TryLockError::Poisoned(e)) => {
                panic!("TimedMutex[{}] lock poisoned: {}", self.name, e);
            }
        }
    }

    pub fn try_lock(&self) -> TryLockResult<TimedMutexGuard<'_, T>> {
        self.inner
            .try_lock()
            .map(|guard| TimedMutexGuard { inner: guard })
    }
}

pub struct TimedMutexGuard<'a, T> {
    inner: std::sync::MutexGuard<'a, T>,
}

impl<'a, T> std::ops::Deref for TimedMutexGuard<'a, T> {
    type Target = T;
    
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'a, T> std::ops::DerefMut for TimedMutexGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

/// 带超时检测的 RwLock 包装器
pub struct TimedRwLock<T> {
    inner: RwLock<T>,
    name: &'static str,
}

impl<T> TimedRwLock<T> {
    pub const fn new(value: T, name: &'static str) -> Self {
        Self {
            inner: RwLock::new(value),
            name,
        }
    }

    pub fn read(&self) -> TimedRwLockReadGuard<'_, T> {
        let start = Instant::now();
        
        match self.inner.try_read() {
            Ok(guard) => {
                TimedRwLockReadGuard { inner: guard }
            }
            Err(TryLockError::WouldBlock) => {
                warn!(
                    "TimedRwLock[{}]: waiting for read lock (would block)",
                    self.name
                );
                
                let guard = self.inner.read().unwrap_or_else(|e| {
                    panic!("TimedRwLock[{}] read lock poisoned: {}", self.name, e);
                });
                
                let elapsed = start.elapsed();
                if elapsed > Duration::from_millis(LOCK_WARN_THRESHOLD_MS) {
                    warn!(
                        "TimedRwLock[{}]: read lock acquired after {:?} (threshold: {}ms)",
                        self.name, elapsed, LOCK_WARN_THRESHOLD_MS
                    );
                }
                
                TimedRwLockReadGuard { inner: guard }
            }
            Err(TryLockError::Poisoned(e)) => {
                panic!("TimedRwLock[{}] read lock poisoned: {}", self.name, e);
            }
        }
    }

    pub fn write(&self) -> TimedRwLockWriteGuard<'_, T> {
        let start = Instant::now();
        
        match self.inner.try_write() {
            Ok(guard) => {
                TimedRwLockWriteGuard { inner: guard }
            }
            Err(TryLockError::WouldBlock) => {
                warn!(
                    "TimedRwLock[{}]: waiting for write lock (would block)",
                    self.name
                );
                
                let guard = self.inner.write().unwrap_or_else(|e| {
                    panic!("TimedRwLock[{}] write lock poisoned: {}", self.name, e);
                });
                
                let elapsed = start.elapsed();
                if elapsed > Duration::from_millis(LOCK_WARN_THRESHOLD_MS) {
                    warn!(
                        "TimedRwLock[{}]: write lock acquired after {:?} (threshold: {}ms)",
                        self.name, elapsed, LOCK_WARN_THRESHOLD_MS
                    );
                }
                
                TimedRwLockWriteGuard { inner: guard }
            }
            Err(TryLockError::Poisoned(e)) => {
                panic!("TimedRwLock[{}] write lock poisoned: {}", self.name, e);
            }
        }
    }

    pub fn try_read(&self) -> TryLockResult<TimedRwLockReadGuard<'_, T>> {
        self.inner
            .try_read()
            .map(|guard| TimedRwLockReadGuard { inner: guard })
    }

    pub fn try_write(&self) -> TryLockResult<TimedRwLockWriteGuard<'_, T>> {
        self.inner
            .try_write()
            .map(|guard| TimedRwLockWriteGuard { inner: guard })
    }
}

pub struct TimedRwLockReadGuard<'a, T> {
    inner: std::sync::RwLockReadGuard<'a, T>,
}

impl<'a, T> std::ops::Deref for TimedRwLockReadGuard<'a, T> {
    type Target = T;
    
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub struct TimedRwLockWriteGuard<'a, T> {
    inner: std::sync::RwLockWriteGuard<'a, T>,
}

impl<'a, T> std::ops::Deref for TimedRwLockWriteGuard<'a, T> {
    type Target = T;
    
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'a, T> std::ops::DerefMut for TimedRwLockWriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn timed_mutex_basic() {
        let mutex = TimedMutex::new(0, "test_mutex");
        let mut guard = mutex.lock();
        *guard = 1;
        assert_eq!(*guard, 1);
    }

    #[test]
    fn timed_rwlock_basic() {
        let rwlock = TimedRwLock::new(0, "test_rwlock");
        
        // 读锁
        let guard = rwlock.read();
        assert_eq!(*guard, 0);
        
        // 写锁
        let mut w_guard = rwlock.write();
        *w_guard = 1;
        assert_eq!(*w_guard, 1);
    }

    #[test]
    fn timed_mutex_concurrent() {
        let mutex = Arc::new(TimedMutex::new(0, "concurrent_mutex"));
        let mut handles = vec![];

        for _ in 0..10 {
            let mutex = Arc::clone(&mutex);
            handles.push(thread::spawn(move || {
                let mut guard = mutex.lock();
                *guard += 1;
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let guard = mutex.lock();
        assert_eq!(*guard, 10);
    }
}
