use chrono::Utc;
use powerfs_common::error::{PowerFsError, Result};
use powerfs_master::s3::MasterApi;
use std::sync::Arc;

use crate::metadata_store::{BucketInfo, MetadataStore, VolumeRoute};

pub struct BucketManager {
    metadata_store: Arc<MetadataStore>,
    master_api: Arc<MasterApi>,
}

impl BucketManager {
    pub fn new(metadata_store: Arc<MetadataStore>, master_api: Arc<MasterApi>) -> Self {
        Self {
            metadata_store,
            master_api,
        }
    }

    pub async fn create_bucket(
        &self,
        bucket: &str,
        replication: &str,
        collection: &str,
    ) -> Result<BucketInfo> {
        if self.metadata_store.get_bucket(bucket).await.is_some() {
            return Err(PowerFsError::FileExists(bucket.to_string()));
        }

        let collection = if collection.is_empty() {
            "default"
        } else {
            collection
        };

        let (fid, nodes) = self
            .master_api
            .assign_volume(replication, collection)
            .await?;

        let bucket_info = BucketInfo {
            name: bucket.to_string(),
            volume_ids: vec![fid.volume_id.0],
            size_limit: 0,
            used_size: 0,
            creation_time: Utc::now(),
            replication: replication.to_string(),
            collection: collection.to_string(),
        };

        self.metadata_store.put_bucket(bucket, &bucket_info).await;

        for node in nodes {
            let route = VolumeRoute {
                volume_id: fid.volume_id.0,
                server_addr: format!("{}:{}", node.address, node.grpc_port),
                server_id: node.id.to_string(),
                size: 0,
                used: 0,
                state: "available".to_string(),
            };
            self.metadata_store
                .put_volume_route(fid.volume_id.0, &route)
                .await;
        }

        Ok(bucket_info)
    }

    pub async fn delete_bucket(&self, bucket: &str) -> Result<bool> {
        let bucket_info = match self.metadata_store.get_bucket(bucket).await {
            Some(b) => b,
            None => {
                return Err(PowerFsError::DirectoryNotFound(bucket.to_string()));
            }
        };

        let entries = self.metadata_store.list_entries(bucket).await;
        if !entries.is_empty() {
            return Err(PowerFsError::InvalidRequest(
                "The bucket you tried to delete is not empty".to_string(),
            ));
        }

        for volume_id in &bucket_info.volume_ids {
            self.metadata_store.delete_volume_route(*volume_id).await;
        }

        Ok(self.metadata_store.delete_bucket(bucket).await)
    }

    pub async fn get_bucket(&self, bucket: &str) -> Option<BucketInfo> {
        self.metadata_store.get_bucket(bucket).await
    }

    pub async fn list_buckets(&self) -> Vec<BucketInfo> {
        let names = self.metadata_store.list_bucket_names().await;
        let mut buckets = Vec::new();
        for name in names {
            if let Some(bucket) = self.metadata_store.get_bucket(&name).await {
                buckets.push(bucket);
            }
        }
        buckets
    }

    pub async fn get_bucket_volume_ids(&self, bucket: &str) -> Option<Vec<u64>> {
        self.metadata_store
            .get_bucket(bucket)
            .await
            .map(|b| b.volume_ids)
    }

    pub async fn get_bucket_primary_volume(&self, bucket: &str) -> Option<u64> {
        self.metadata_store
            .get_bucket(bucket)
            .await
            .and_then(|b| b.volume_ids.first().cloned())
    }

    pub async fn allocate_volume_for_bucket(&self, bucket: &str, replication: &str) -> Result<u64> {
        // Use the bucket's recorded collection so newly allocated volumes stay
        // in the same collection pool.
        let collection = self
            .metadata_store
            .get_bucket(bucket)
            .await
            .map(|b| b.collection)
            .unwrap_or_else(|| "default".to_string());

        let (fid, nodes) = self
            .master_api
            .assign_volume(replication, &collection)
            .await?;

        if let Some(mut bucket_info) = self.metadata_store.get_bucket(bucket).await {
            bucket_info.volume_ids.push(fid.volume_id.0);
            self.metadata_store.put_bucket(bucket, &bucket_info).await;
        }

        for node in nodes {
            let route = VolumeRoute {
                volume_id: fid.volume_id.0,
                server_addr: format!("{}:{}", node.address, node.grpc_port),
                server_id: node.id.to_string(),
                size: 0,
                used: 0,
                state: "available".to_string(),
            };
            self.metadata_store
                .put_volume_route(fid.volume_id.0, &route)
                .await;
        }

        Ok(fid.volume_id.0)
    }

    /// Dynamically assign a volume for an S3 object using the bucket's
    /// collection. Returns `(volume_id, server_addr, fid_string)` so the
    /// caller can write the needle and record metadata.
    ///
    /// This replaces the legacy behaviour of always writing to
    /// `bucket_info.volume_ids[0]`: objects now spread across the
    /// collection's writable volume pool.
    pub async fn assign_volume_for_object(&self, bucket: &str) -> Result<(u64, String, String)> {
        let bucket_info = self
            .metadata_store
            .get_bucket(bucket)
            .await
            .ok_or_else(|| PowerFsError::DirectoryNotFound(bucket.to_string()))?;

        let (fid, nodes) = self
            .master_api
            .assign_volume(&bucket_info.replication, &bucket_info.collection)
            .await?;

        let node = nodes.into_iter().next().ok_or_else(|| {
            PowerFsError::InvalidRequest("no volume server for assigned volume".to_string())
        })?;
        let server_addr = format!("{}:{}", node.address, node.grpc_port);

        // Cache the route so subsequent reads can resolve the volume.
        let route = VolumeRoute {
            volume_id: fid.volume_id.0,
            server_addr: server_addr.clone(),
            server_id: node.id.to_string(),
            size: 0,
            used: 0,
            state: "available".to_string(),
        };
        self.metadata_store
            .put_volume_route(fid.volume_id.0, &route)
            .await;

        let volume_id = fid.volume_id.0;
        let fid_str = format!("{},{},{}", fid.volume_id.0, fid.cookie, fid.file_key);
        Ok((volume_id, server_addr, fid_str))
    }
}
