use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::error::Result;
use crate::event::{Event, EventEnvelope};
use crate::types::{Collection, Fid, NodeId, VolumeId, VolumeInfo};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Location {
    pub url: String,
    pub public_url: String,
    pub grpc_port: u32,
    pub data_center: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStats {
    pub total_space: u64,
    pub used_space: u64,
    pub cpu_usage: f64,
    pub memory_usage: f64,
    pub volume_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeFilters {
    pub collection: Option<Collection>,
    pub state: Option<String>,
    pub node_id: Option<NodeId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionInfo {
    pub session_id: String,
    pub block_count: u64,
    pub total_size: u64,
    pub created_at: DateTime<Utc>,
    pub last_accessed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionStats {
    pub session_id: String,
    pub block_count: u64,
    pub total_size: u64,
    pub hit_count: u64,
    pub miss_count: u64,
}

#[derive(Debug)]
pub struct EventStream {
    pub receiver: tokio::sync::mpsc::Receiver<EventEnvelope>,
}

#[async_trait]
pub trait VolumeProvider: Send + Sync {
    async fn assign_volume(
        &self,
        collection: &str,
        replication: &str,
    ) -> Result<(Fid, Vec<Location>)>;

    async fn lookup_volume(&self, volume_id: VolumeId) -> Result<Vec<Location>>;

    async fn heartbeat(&self, node_id: &NodeId, stats: &NodeStats) -> Result<()>;

    async fn list_volumes(&self, filters: &VolumeFilters) -> Result<Vec<VolumeInfo>>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntryAttributes {
    pub ino: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub atime: DateTime<Utc>,
    pub mtime: DateTime<Utc>,
    pub ctime: DateTime<Utc>,
    pub crtime: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileChunk {
    pub offset: u64,
    pub size: u64,
    pub mtime: u64,
    pub fid: String,
    pub cookie: u32,
    pub crc32: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub name: String,
    pub directory: String,
    pub attributes: Option<EntryAttributes>,
    pub chunks: Vec<FileChunk>,
    pub hard_link_id: String,
    pub hard_link_counter: u32,
    pub extended: HashMap<String, Vec<u8>>,
    pub content_size: u64,
    pub disk_size: u64,
    pub ttl: String,
    pub symlink_target: String,
    pub owner: String,
    pub generation: u64,
}

#[async_trait]
pub trait MetadataProvider: Send + Sync {
    async fn get_entry(&self, path: &str) -> Result<Option<Entry>>;

    async fn get_entry_by_inode(&self, inode: u64) -> Result<Option<(Entry, String)>>;

    async fn create_entry(&self, entry: &Entry, client_id: &str) -> Result<u64>;

    async fn update_entry(
        &self,
        entry: &Entry,
        client_id: &str,
        old_size: u64,
        is_truncate: bool,
    ) -> Result<u64>;

    async fn delete_entry(&self, inode: u64, is_dir: bool, client_id: &str) -> Result<()>;

    async fn list_entries(&self, inode: u64, limit: u32, client_id: &str) -> Result<Vec<Entry>>;
}

#[async_trait]
pub trait KvCacheProvider: Send + Sync {
    async fn put_block(&self, session_id: &str, block_id: u64, data: &[u8]) -> Result<()>;

    async fn get_block(&self, session_id: &str, block_id: u64) -> Result<Option<Vec<u8>>>;

    async fn list_sessions(&self) -> Result<Vec<SessionInfo>>;

    async fn evict_session(&self, session_id: &str) -> Result<()>;

    async fn get_session_stats(&self, session_id: &str) -> Result<Option<SessionStats>>;
}

#[async_trait]
pub trait EventProvider: Send + Sync {
    async fn publish(&self, event: Event, source_id: &str) -> Result<()>;

    async fn subscribe(&self, stream_key: &str) -> Result<EventStream>;

    async fn read_history(
        &self,
        stream_key: &str,
        start: &str,
        count: usize,
    ) -> Result<Vec<EventEnvelope>>;
}

#[async_trait]
pub trait StorageProvider: Send + Sync {
    async fn write_blob(
        &self,
        volume_id: u32,
        file_key: u64,
        offset: i64,
        size: i32,
        data: &[u8],
    ) -> Result<()>;

    async fn batch_write_blob(
        &self,
        volume_id: u32,
        file_key: u64,
        entries: &[(i64, i32, Vec<u8>, u32)],
    ) -> Result<()>;

    async fn read_blob(
        &self,
        volume_id: u32,
        file_key: u64,
        offset: i64,
        size: i32,
    ) -> Result<Vec<u8>>;

    async fn delete_blob(&self, volume_id: u32, file_key: u64) -> Result<()>;
}
