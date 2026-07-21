use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

const IP_LIMIT: i64 = 10;
const USER_LIMIT: i64 = 5;
const LOCKOUT_ATTEMPTS: i64 = 5;
const LOCKOUT_DURATION_SECONDS: u64 = 900;
const WINDOW_SECONDS: u64 = 60;

struct RateEntry {
    count: i64,
    created_at: u64,
}

struct LockEntry {
    locked_until: u64,
}

pub struct RateLimiter {
    ip_entries: Arc<RwLock<HashMap<String, RateEntry>>>,
    user_entries: Arc<RwLock<HashMap<String, RateEntry>>>,
    lock_entries: Arc<RwLock<HashMap<String, LockEntry>>>,
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

impl RateLimiter {
    pub fn new() -> Self {
        Self {
            ip_entries: Arc::new(RwLock::new(HashMap::new())),
            user_entries: Arc::new(RwLock::new(HashMap::new())),
            lock_entries: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    pub async fn check_login(&self, ip: &str, username: &str) -> Result<bool, String> {
        let now = Self::now();

        let mut locks = self
            .lock_entries
            .write()
            .map_err(|e| format!("Failed to acquire lock entry lock: {}", e))?;
        if let Some(lock) = locks.get(username) {
            if lock.locked_until > now {
                return Ok(false);
            } else {
                locks.remove(username);
            }
        }
        drop(locks);

        let mut ip_entries = self
            .ip_entries
            .write()
            .map_err(|e| format!("Failed to acquire IP entry lock: {}", e))?;
        let ip_count = match ip_entries.get_mut(ip) {
            Some(entry) => {
                if now - entry.created_at > WINDOW_SECONDS {
                    entry.count = 1;
                    entry.created_at = now;
                    1
                } else {
                    entry.count += 1;
                    entry.count
                }
            }
            None => {
                ip_entries.insert(
                    ip.to_string(),
                    RateEntry {
                        count: 1,
                        created_at: now,
                    },
                );
                1
            }
        };
        drop(ip_entries);

        let mut user_entries = self
            .user_entries
            .write()
            .map_err(|e| format!("Failed to acquire user entry lock: {}", e))?;
        let user_count = match user_entries.get_mut(username) {
            Some(entry) => {
                if now - entry.created_at > WINDOW_SECONDS {
                    entry.count = 1;
                    entry.created_at = now;
                    1
                } else {
                    entry.count += 1;
                    entry.count
                }
            }
            None => {
                user_entries.insert(
                    username.to_string(),
                    RateEntry {
                        count: 1,
                        created_at: now,
                    },
                );
                1
            }
        };
        drop(user_entries);

        if user_count >= LOCKOUT_ATTEMPTS {
            let mut locks = self
                .lock_entries
                .write()
                .map_err(|e| format!("Failed to acquire lock entry lock: {}", e))?;
            locks.insert(
                username.to_string(),
                LockEntry {
                    locked_until: now + LOCKOUT_DURATION_SECONDS,
                },
            );
        }

        Ok(ip_count <= IP_LIMIT && user_count <= USER_LIMIT)
    }

    pub async fn reset_login(&self, username: &str) -> Result<(), String> {
        let mut user_entries = self
            .user_entries
            .write()
            .map_err(|e| format!("Failed to acquire user entry lock: {}", e))?;
        user_entries.remove(username);
        drop(user_entries);

        let mut lock_entries = self
            .lock_entries
            .write()
            .map_err(|e| format!("Failed to acquire lock entry lock: {}", e))?;
        lock_entries.remove(username);
        Ok(())
    }
}
