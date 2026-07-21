use chrono::Utc;
use powerfs_common::error::{PowerFsError, Result};
use sha2::{Digest, Sha256};
use std::sync::Arc;

use crate::bucket_manager::BucketManager;
use crate::metadata_store::{ChunkInfo, EntryInfo, MetadataStore};

pub struct EntryManager {
    metadata_store: Arc<MetadataStore>,
    bucket_manager: Arc<BucketManager>,
}

impl EntryManager {
    pub fn new(metadata_store: Arc<MetadataStore>, bucket_manager: Arc<BucketManager>) -> Self {
        Self {
            metadata_store,
            bucket_manager,
        }
    }

    pub async fn put_entry(
        &self,
        bucket: &str,
        key: &str,
        data: &[u8],
        fid: &str,
        volume_id: u32,
    ) -> Result<EntryInfo> {
        if self.bucket_manager.get_bucket(bucket).await.is_none() {
            return Err(PowerFsError::DirectoryNotFound(bucket.to_string()));
        }

        let size = data.len() as u64;

        let mut hasher = Sha256::new();
        hasher.update(data);
        let etag = hex::encode(hasher.finalize());

        let chunks = vec![ChunkInfo {
            offset: 0,
            size,
            mtime: Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
            fid: fid.to_string(),
            cookie: 0,
            crc32: 0,
        }];

        let entry_info = EntryInfo {
            bucket: bucket.to_string(),
            key: key.to_string(),
            fid: fid.to_string(),
            volume_id,
            size,
            mtime: Utc::now(),
            etag,
            chunks,
        };

        self.metadata_store
            .put_entry(bucket, key, &entry_info)
            .await;

        Ok(entry_info)
    }

    pub async fn get_entry(&self, bucket: &str, key: &str) -> Option<EntryInfo> {
        self.metadata_store.get_entry(bucket, key).await
    }

    pub async fn delete_entry(&self, bucket: &str, key: &str) -> Result<bool> {
        if self.bucket_manager.get_bucket(bucket).await.is_none() {
            return Err(PowerFsError::DirectoryNotFound(bucket.to_string()));
        }

        if self.metadata_store.get_entry(bucket, key).await.is_none() {
            return Err(PowerFsError::FileNotFound(key.to_string()));
        }

        Ok(self.metadata_store.delete_entry(bucket, key).await)
    }

    pub async fn list_entries(&self, bucket: &str) -> Result<Vec<EntryInfo>> {
        if self.bucket_manager.get_bucket(bucket).await.is_none() {
            return Err(PowerFsError::DirectoryNotFound(bucket.to_string()));
        }

        Ok(self.metadata_store.list_entries(bucket).await)
    }

    pub async fn entry_exists(&self, bucket: &str, key: &str) -> bool {
        self.metadata_store.get_entry(bucket, key).await.is_some()
    }
}
