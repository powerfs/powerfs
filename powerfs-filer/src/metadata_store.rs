use chrono::DateTime;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[cfg(feature = "redis-event")]
use redis::AsyncCommands;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketInfo {
    pub name: String,
    pub volume_ids: Vec<u32>,
    pub size_limit: u64,
    pub used_size: u64,
    pub creation_time: DateTime<Utc>,
    pub replication: String,
    pub collection: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkInfo {
    pub offset: u64,
    pub size: u64,
    pub mtime: u64,
    pub fid: String,
    pub cookie: u64,
    pub crc32: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntryInfo {
    pub bucket: String,
    pub key: String,
    pub fid: String,
    pub volume_id: u32,
    pub size: u64,
    pub mtime: DateTime<Utc>,
    pub etag: String,
    pub chunks: Vec<ChunkInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeRoute {
    pub volume_id: u32,
    pub server_addr: String,
    pub server_id: String,
    pub size: u64,
    pub used: u64,
    pub state: String,
}

pub struct MetadataStore {
    #[cfg(feature = "redis-event")]
    client: Arc<redis::Client>,
    #[cfg(not(feature = "redis-event"))]
    buckets: Arc<RwLock<HashMap<String, BucketInfo>>>,
    #[cfg(not(feature = "redis-event"))]
    entries: Arc<RwLock<HashMap<String, EntryInfo>>>,
    #[cfg(not(feature = "redis-event"))]
    volume_routes: Arc<RwLock<HashMap<u32, VolumeRoute>>>,
}

impl MetadataStore {
    #[cfg(feature = "redis-event")]
    pub fn new(client: redis::Client) -> Self {
        Self {
            client: Arc::new(client),
        }
    }

    #[cfg(not(feature = "redis-event"))]
    pub fn new(_: ()) -> Self {
        Self {
            buckets: Arc::new(RwLock::new(HashMap::new())),
            entries: Arc::new(RwLock::new(HashMap::new())),
            volume_routes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn get_bucket(&self, name: &str) -> Option<BucketInfo> {
        #[cfg(feature = "redis-event")]
        {
            let key = format!("bucket:{}", name);
            let mut con = match self.client.get_async_connection().await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Failed to get redis connection: {}", e);
                    return None;
                }
            };
            let result: Option<String> = con.get(key).await.ok();
            result.and_then(|s| serde_json::from_str(&s).ok())
        }
        #[cfg(not(feature = "redis-event"))]
        {
            self.buckets.read().ok().and_then(|b| b.get(name).cloned())
        }
    }

    pub async fn put_bucket(&self, name: &str, info: &BucketInfo) -> bool {
        #[cfg(feature = "redis-event")]
        {
            let key = format!("bucket:{}", name);
            let mut con = match self.client.get_async_connection().await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Failed to get redis connection: {}", e);
                    return false;
                }
            };
            let value = serde_json::to_string(info).unwrap_or_default();
            let _: () = con.set(key, value).await.unwrap_or(());
            true
        }
        #[cfg(not(feature = "redis-event"))]
        {
            self.buckets
                .write()
                .map(|mut b| b.insert(name.to_string(), info.clone()))
                .is_ok()
        }
    }

    pub async fn delete_bucket(&self, name: &str) -> bool {
        #[cfg(feature = "redis-event")]
        {
            let key = format!("bucket:{}", name);
            let mut con = match self.client.get_async_connection().await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Failed to get redis connection: {}", e);
                    return false;
                }
            };
            let _: () = con.del(key).await.unwrap_or(());
            true
        }
        #[cfg(not(feature = "redis-event"))]
        {
            self.buckets.write().map(|mut b| b.remove(name)).is_ok()
        }
    }

    pub async fn list_bucket_names(&self) -> Vec<String> {
        #[cfg(feature = "redis-event")]
        {
            let mut con = match self.client.get_async_connection().await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Failed to get redis connection: {}", e);
                    return Vec::new();
                }
            };
            let pattern = "bucket:*".to_string();
            let keys: Vec<String> = con.keys(pattern).await.unwrap_or_default();
            keys.into_iter()
                .filter(|k| k.starts_with("bucket:"))
                .map(|k| k.strip_prefix("bucket:").unwrap_or(&k).to_string())
                .collect()
        }
        #[cfg(not(feature = "redis-event"))]
        {
            self.buckets
                .read()
                .ok()
                .map(|b| b.keys().cloned().collect())
                .unwrap_or_default()
        }
    }

    pub async fn get_entry(&self, bucket: &str, key: &str) -> Option<EntryInfo> {
        #[cfg(feature = "redis-event")]
        {
            let redis_key = format!("entry:{}/{}", bucket, key);
            let mut con = match self.client.get_async_connection().await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Failed to get redis connection: {}", e);
                    return None;
                }
            };
            let result: Option<String> = con.get(redis_key).await.ok();
            result.and_then(|s| serde_json::from_str(&s).ok())
        }
        #[cfg(not(feature = "redis-event"))]
        {
            let entry_key = format!("{}:{}", bucket, key);
            self.entries
                .read()
                .ok()
                .and_then(|e| e.get(&entry_key).cloned())
        }
    }

    pub async fn put_entry(&self, bucket: &str, key: &str, info: &EntryInfo) -> bool {
        #[cfg(feature = "redis-event")]
        {
            let redis_key = format!("entry:{}/{}", bucket, key);
            let mut con = match self.client.get_async_connection().await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Failed to get redis connection: {}", e);
                    return false;
                }
            };
            let value = serde_json::to_string(info).unwrap_or_default();
            let _: () = con.set(redis_key, value).await.unwrap_or(());
            true
        }
        #[cfg(not(feature = "redis-event"))]
        {
            let entry_key = format!("{}:{}", bucket, key);
            self.entries
                .write()
                .map(|mut e| e.insert(entry_key, info.clone()))
                .is_ok()
        }
    }

    pub async fn delete_entry(&self, bucket: &str, key: &str) -> bool {
        #[cfg(feature = "redis-event")]
        {
            let redis_key = format!("entry:{}/{}", bucket, key);
            let mut con = match self.client.get_async_connection().await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Failed to get redis connection: {}", e);
                    return false;
                }
            };
            let _: () = con.del(redis_key).await.unwrap_or(());
            true
        }
        #[cfg(not(feature = "redis-event"))]
        {
            let entry_key = format!("{}:{}", bucket, key);
            self.entries
                .write()
                .map(|mut e| e.remove(&entry_key))
                .is_ok()
        }
    }

    pub async fn list_entries(&self, bucket: &str) -> Vec<EntryInfo> {
        #[cfg(feature = "redis-event")]
        {
            let pattern = format!("entry:{}/*", bucket);
            let mut con = match self.client.get_async_connection().await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Failed to get redis connection: {}", e);
                    return Vec::new();
                }
            };
            let pattern_str = pattern.to_string();
            let keys: Vec<String> = con.keys(pattern_str).await.unwrap_or_default();
            let mut entries: Vec<EntryInfo> = Vec::new();
            for key in keys {
                let result: Option<String> = con.get(&key).await.ok();
                if let Some(s) = result {
                    if let Ok(entry) = serde_json::from_str(&s) {
                        entries.push(entry);
                    }
                }
            }
            entries.sort_by(|a, b| a.key.cmp(&b.key));
            entries
        }
        #[cfg(not(feature = "redis-event"))]
        {
            self.entries
                .read()
                .ok()
                .map(|e| {
                    e.iter()
                        .filter(|(k, _)| k.starts_with(&format!("{}:", bucket)))
                        .map(|(_, v)| v.clone())
                        .collect()
                })
                .unwrap_or_default()
        }
    }

    pub async fn get_volume_route(&self, volume_id: u32) -> Option<VolumeRoute> {
        #[cfg(feature = "redis-event")]
        {
            let key = format!("volume:{}", volume_id);
            let mut con = match self.client.get_async_connection().await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Failed to get redis connection: {}", e);
                    return None;
                }
            };
            let result: Option<String> = con.get(key).await.ok();
            result.and_then(|s| serde_json::from_str(&s).ok())
        }
        #[cfg(not(feature = "redis-event"))]
        {
            self.volume_routes
                .read()
                .ok()
                .and_then(|v| v.get(&volume_id).cloned())
        }
    }

    pub async fn put_volume_route(&self, volume_id: u32, route: &VolumeRoute) -> bool {
        #[cfg(feature = "redis-event")]
        {
            let key = format!("volume:{}", volume_id);
            let mut con = match self.client.get_async_connection().await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Failed to get redis connection: {}", e);
                    return false;
                }
            };
            let value = serde_json::to_string(route).unwrap_or_default();
            let _: () = con.set(key, value).await.unwrap_or(());
            true
        }
        #[cfg(not(feature = "redis-event"))]
        {
            self.volume_routes
                .write()
                .map(|mut v| v.insert(volume_id, route.clone()))
                .is_ok()
        }
    }

    pub async fn delete_volume_route(&self, volume_id: u32) -> bool {
        #[cfg(feature = "redis-event")]
        {
            let key = format!("volume:{}", volume_id);
            let mut con = match self.client.get_async_connection().await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Failed to get redis connection: {}", e);
                    return false;
                }
            };
            let _: () = con.del(key).await.unwrap_or(());
            true
        }
        #[cfg(not(feature = "redis-event"))]
        {
            self.volume_routes
                .write()
                .map(|mut v| v.remove(&volume_id))
                .is_ok()
        }
    }
}
