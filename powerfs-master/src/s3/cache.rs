use crate::proto::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// 缓存条目
struct CacheEntry<T> {
    value: T,
    expires_at: Instant,
}

impl<T> CacheEntry<T> {
    fn new(value: T, ttl: Duration) -> Self {
        Self {
            value,
            expires_at: Instant::now() + ttl,
        }
    }

    fn is_expired(&self) -> bool {
        Instant::now() > self.expires_at
    }
}

/// Bucket存在性缓存
/// 缓存bucket是否存在，避免每次请求都查询master
pub struct BucketCache {
    cache: Arc<RwLock<HashMap<String, CacheEntry<bool>>>>,
    ttl: Duration,
}

impl BucketCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            ttl,
        }
    }

    pub async fn get(&self, bucket: &str) -> Option<bool> {
        let cache = self.cache.read().await;
        cache.get(bucket).and_then(|entry| {
            if entry.is_expired() {
                None
            } else {
                Some(entry.value)
            }
        })
    }

    pub async fn set(&self, bucket: &str, exists: bool) {
        let mut cache = self.cache.write().await;
        cache.insert(bucket.to_string(), CacheEntry::new(exists, self.ttl));
    }

    pub async fn remove(&self, bucket: &str) {
        let mut cache = self.cache.write().await;
        cache.remove(bucket);
    }

    pub async fn clear(&self) {
        let mut cache = self.cache.write().await;
        cache.clear();
    }
}

impl Default for BucketCache {
    fn default() -> Self {
        Self::new(Duration::from_secs(60))
    }
}

/// Volume位置缓存
/// 缓存volume_id -> volume_server地址的映射
pub struct VolumeLocationCache {
    cache: Arc<RwLock<HashMap<u32, CacheEntry<String>>>>,
    ttl: Duration,
}

impl VolumeLocationCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            ttl,
        }
    }

    pub async fn get(&self, volume_id: u32) -> Option<String> {
        let cache = self.cache.read().await;
        cache.get(&volume_id).and_then(|entry| {
            if entry.is_expired() {
                None
            } else {
                Some(entry.value.clone())
            }
        })
    }

    pub async fn set(&self, volume_id: u32, address: String) {
        let mut cache = self.cache.write().await;
        cache.insert(volume_id, CacheEntry::new(address, self.ttl));
    }

    pub async fn remove(&self, volume_id: u32) {
        let mut cache = self.cache.write().await;
        cache.remove(&volume_id);
    }

    pub async fn clear(&self) {
        let mut cache = self.cache.write().await;
        cache.clear();
    }
}

impl Default for VolumeLocationCache {
    fn default() -> Self {
        Self::new(Duration::from_secs(300))
    }
}

/// Entry元数据缓存
/// 缓存对象路径 -> Entry的映射
pub struct EntryCache {
    cache: Arc<RwLock<HashMap<String, CacheEntry<Entry>>>>,
    ttl: Duration,
    max_size: usize,
}

impl EntryCache {
    pub fn new(ttl: Duration, max_size: usize) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            ttl,
            max_size,
        }
    }

    pub async fn get(&self, path: &str) -> Option<Entry> {
        let cache = self.cache.read().await;
        cache.get(path).and_then(|entry| {
            if entry.is_expired() {
                None
            } else {
                Some(entry.value.clone())
            }
        })
    }

    pub async fn set(&self, path: &str, entry: Entry) {
        let mut cache = self.cache.write().await;

        // LRU淘汰：如果超过最大大小，删除过期的条目
        if cache.len() >= self.max_size {
            let expired_keys: Vec<String> = cache
                .iter()
                .filter(|(_, e)| e.is_expired())
                .map(|(k, _)| k.clone())
                .collect();
            for key in expired_keys {
                cache.remove(&key);
            }

            // 如果还是太大，删除最旧的条目
            if cache.len() >= self.max_size {
                // 简单策略：随机删除一半
                let keys: Vec<String> = cache.keys().take(self.max_size / 2).cloned().collect();
                for key in keys {
                    cache.remove(&key);
                }
            }
        }

        cache.insert(path.to_string(), CacheEntry::new(entry, self.ttl));
    }

    pub async fn remove(&self, path: &str) {
        let mut cache = self.cache.write().await;
        cache.remove(path);
    }

    pub async fn clear(&self) {
        let mut cache = self.cache.write().await;
        cache.clear();
    }
}

impl Default for EntryCache {
    fn default() -> Self {
        Self::new(Duration::from_secs(30), 10000)
    }
}

/// S3 Gateway 统一缓存管理器
pub struct S3Cache {
    pub bucket_cache: BucketCache,
    pub volume_location_cache: VolumeLocationCache,
    pub entry_cache: EntryCache,
}

impl S3Cache {
    pub fn new() -> Self {
        Self {
            bucket_cache: BucketCache::default(),
            volume_location_cache: VolumeLocationCache::default(),
            entry_cache: EntryCache::default(),
        }
    }

    pub fn with_ttls(bucket_ttl: Duration, volume_ttl: Duration, entry_ttl: Duration) -> Self {
        Self {
            bucket_cache: BucketCache::new(bucket_ttl),
            volume_location_cache: VolumeLocationCache::new(volume_ttl),
            entry_cache: EntryCache::new(entry_ttl, 10000),
        }
    }

    pub async fn clear_all(&self) {
        self.bucket_cache.clear().await;
        self.volume_location_cache.clear().await;
        self.entry_cache.clear().await;
    }
}

impl Default for S3Cache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_bucket_cache() {
        let cache = BucketCache::new(Duration::from_secs(10));

        assert!(cache.get("test-bucket").await.is_none());

        cache.set("test-bucket", true).await;
        assert_eq!(cache.get("test-bucket").await, Some(true));

        cache.remove("test-bucket").await;
        assert!(cache.get("test-bucket").await.is_none());
    }

    #[tokio::test]
    async fn test_volume_location_cache() {
        let cache = VolumeLocationCache::new(Duration::from_secs(10));

        assert!(cache.get(1).await.is_none());

        cache.set(1, "192.168.1.1:8080".to_string()).await;
        assert_eq!(cache.get(1).await, Some("192.168.1.1:8080".to_string()));
    }

    #[tokio::test]
    async fn test_cache_expiration() {
        let cache = BucketCache::new(Duration::from_millis(10));

        cache.set("test-bucket", true).await;
        assert_eq!(cache.get("test-bucket").await, Some(true));

        // 等待过期
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(cache.get("test-bucket").await.is_none());
    }
}
