use async_trait::async_trait;
use chrono::{DateTime, Utc};
use powerfs_common::{
    error::{PowerFsError, Result},
    traits::{
        Entry, EntryAttributes, FileChunk, Location, MetadataProvider, NodeStats, StorageProvider,
        VolumeFilters, VolumeProvider,
    },
    types::{Fid, NodeId, VolumeId, VolumeInfo},
};
use powerfs_master::proto::powerfs::Entry as FilerEntry;
use std::sync::Arc;

use crate::client::PowerFuseClient;

pub struct FuseVolumeProvider {
    client: Arc<PowerFuseClient>,
}

impl FuseVolumeProvider {
    pub fn new(client: Arc<PowerFuseClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl VolumeProvider for FuseVolumeProvider {
    async fn assign_volume(
        &self,
        collection: &str,
        replication: &str,
    ) -> Result<(Fid, Vec<Location>)> {
        let (fid, location, _stripe_fids, _stripe_locations) = self
            .client
            .assign_fid(collection, replication)
            .await
            .map_err(|e| PowerFsError::Internal(format!("assign_volume failed: {}", e)))?;

        let locations = location
            .into_iter()
            .map(|loc| Location {
                url: loc.url,
                public_url: loc.public_url,
                grpc_port: loc.grpc_port,
                data_center: loc.data_center,
            })
            .collect();
        Ok((fid, locations))
    }

    async fn lookup_volume(&self, volume_id: VolumeId) -> Result<Vec<Location>> {
        let locations = self
            .client
            .lookup_volume(volume_id)
            .await
            .map_err(|e| PowerFsError::Internal(format!("lookup_volume failed: {}", e)))?;
        let locations = locations
            .into_iter()
            .map(|loc| Location {
                url: loc.url,
                public_url: loc.public_url,
                grpc_port: loc.grpc_port,
                data_center: loc.data_center,
            })
            .collect();
        Ok(locations)
    }

    async fn heartbeat(&self, _node_id: &NodeId, _stats: &NodeStats) -> Result<()> {
        Ok(())
    }

    async fn list_volumes(&self, _filters: &VolumeFilters) -> Result<Vec<VolumeInfo>> {
        Ok(Vec::new())
    }
}

pub struct FuseMetadataProvider {
    client: Arc<PowerFuseClient>,
}

impl FuseMetadataProvider {
    pub fn new(client: Arc<PowerFuseClient>) -> Self {
        Self { client }
    }
}

fn proto_entry_to_trait_entry(entry: &FilerEntry) -> Entry {
    let attributes = entry.attributes.as_ref().map(|attrs| EntryAttributes {
        ino: attrs.ino,
        mode: attrs.mode,
        uid: attrs.uid,
        gid: attrs.gid,
        atime: DateTime::from_timestamp(attrs.atime as i64, 0).unwrap_or_else(Utc::now),
        mtime: DateTime::from_timestamp(attrs.mtime as i64, 0).unwrap_or_else(Utc::now),
        ctime: DateTime::from_timestamp(attrs.ctime as i64, 0).unwrap_or_else(Utc::now),
        crtime: DateTime::from_timestamp(attrs.crtime as i64, 0).unwrap_or_else(Utc::now),
    });

    let chunks = entry
        .chunks
        .iter()
        .map(|chunk| FileChunk {
            offset: chunk.offset,
            size: chunk.size,
            mtime: chunk.mtime,
            fid: chunk.fid.clone(),
            cookie: chunk.cookie,
            crc32: chunk.crc32,
        })
        .collect();

    Entry {
        name: entry.name.clone(),
        directory: entry.directory.clone(),
        attributes,
        chunks,
        hard_link_id: entry.hard_link_id.clone(),
        hard_link_counter: entry.hard_link_counter,
        extended: entry.extended.clone(),
        content_size: entry.content_size,
        disk_size: entry.disk_size,
        ttl: entry.ttl.clone(),
        symlink_target: entry.symlink_target.clone(),
        owner: entry.owner.clone(),
        generation: entry.generation,
    }
}

fn trait_entry_to_proto_entry(entry: &Entry) -> FilerEntry {
    let attributes =
        entry
            .attributes
            .as_ref()
            .map(|attrs| powerfs_master::proto::powerfs::FuseAttributes {
                ino: attrs.ino,
                mode: attrs.mode,
                nlink: 1,
                uid: attrs.uid,
                gid: attrs.gid,
                rdev: 0,
                size: entry.content_size,
                blksize: 4096,
                blocks: entry.content_size.div_ceil(512),
                atime: attrs.atime.timestamp() as u64,
                mtime: attrs.mtime.timestamp() as u64,
                ctime: attrs.ctime.timestamp() as u64,
                crtime: attrs.crtime.timestamp() as u64,
                perm: 0,
            });

    let chunks = entry
        .chunks
        .iter()
        .map(|chunk| powerfs_master::proto::powerfs::FileChunk {
            offset: chunk.offset,
            size: chunk.size,
            mtime: chunk.mtime,
            fid: chunk.fid.clone(),
            cookie: chunk.cookie,
            crc32: chunk.crc32,
        })
        .collect();

    FilerEntry {
        name: entry.name.clone(),
        directory: entry.directory.clone(),
        attributes,
        chunks,
        hard_link_id: entry.hard_link_id.clone(),
        hard_link_counter: entry.hard_link_counter,
        extended: entry.extended.clone(),
        content_size: entry.content_size,
        disk_size: entry.disk_size,
        ttl: entry.ttl.clone(),
        symlink_target: entry.symlink_target.clone(),
        owner: entry.owner.clone(),
        generation: entry.generation,
    }
}

#[async_trait]
impl MetadataProvider for FuseMetadataProvider {
    async fn get_entry(&self, path: &str) -> Result<Option<Entry>> {
        let result = self
            .client
            .get_entry(path)
            .await
            .map_err(|e| PowerFsError::Internal(format!("get_entry failed: {}", e)))?;
        Ok(result.map(|entry| proto_entry_to_trait_entry(&entry)))
    }

    async fn get_entry_by_inode(&self, inode: u64) -> Result<Option<(Entry, String)>> {
        let result = self
            .client
            .get_entry_by_inode(inode)
            .await
            .map_err(|e| PowerFsError::Internal(format!("get_entry_by_inode failed: {}", e)))?;
        Ok(result.map(|(entry, path)| (proto_entry_to_trait_entry(&entry), path)))
    }

    async fn create_entry(&self, entry: &Entry, client_id: &str) -> Result<u64> {
        let proto_entry = trait_entry_to_proto_entry(entry);
        let inode = self
            .client
            .create_entry(proto_entry, client_id)
            .await
            .map_err(|e| PowerFsError::Internal(format!("create_entry failed: {}", e)))?;
        Ok(inode)
    }

    async fn update_entry(
        &self,
        entry: &Entry,
        client_id: &str,
        old_size: u64,
        is_truncate: bool,
    ) -> Result<u64> {
        let proto_entry = trait_entry_to_proto_entry(entry);
        let size = self
            .client
            .update_entry(&proto_entry, client_id, old_size, is_truncate)
            .await
            .map_err(|e| PowerFsError::Internal(format!("update_entry failed: {}", e)))?;
        Ok(size)
    }

    async fn delete_entry(&self, inode: u64, is_dir: bool, client_id: &str) -> Result<()> {
        let _ = self
            .client
            .delete_entry(inode, is_dir, client_id)
            .await
            .map_err(|e| PowerFsError::Internal(format!("delete_entry failed: {}", e)))?;
        Ok(())
    }

    async fn list_entries(&self, inode: u64, limit: u32, client_id: &str) -> Result<Vec<Entry>> {
        let entries = self
            .client
            .list_entries(inode, limit as u64, client_id)
            .await
            .map_err(|e| PowerFsError::Internal(format!("list_entries failed: {}", e)))?;
        Ok(entries
            .into_iter()
            .map(|e| proto_entry_to_trait_entry(&e))
            .collect())
    }
}

pub struct FuseStorageProvider {
    client: Arc<PowerFuseClient>,
}

impl FuseStorageProvider {
    pub fn new(client: Arc<PowerFuseClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl StorageProvider for FuseStorageProvider {
    async fn write_blob(
        &self,
        volume_id: u32,
        file_key: u64,
        offset: i64,
        size: i32,
        data: &[u8],
    ) -> Result<()> {
        let locations = self
            .client
            .lookup_volume(VolumeId(volume_id))
            .await
            .map_err(|e| PowerFsError::Internal(format!("lookup_volume failed: {}", e)))?;

        if let Some(loc) = locations.first() {
            let addr = PowerFuseClient::location_to_grpc_addr(loc);
            self.client
                .write_blob(&addr, volume_id, file_key, offset, size, data.to_vec(), 0)
                .await
                .map_err(|e| PowerFsError::Internal(format!("write_blob failed: {}", e)))?;
        }

        Ok(())
    }

    async fn batch_write_blob(
        &self,
        volume_id: u32,
        file_key: u64,
        entries: &[(i64, i32, Vec<u8>, u32)],
    ) -> Result<()> {
        let locations = self
            .client
            .lookup_volume(VolumeId(volume_id))
            .await
            .map_err(|e| PowerFsError::Internal(format!("lookup_volume failed: {}", e)))?;

        if let Some(loc) = locations.first() {
            let addr = PowerFuseClient::location_to_grpc_addr(loc);
            self.client
                .batch_write_blob(&addr, volume_id, file_key, entries.to_vec())
                .await
                .map_err(|e| PowerFsError::Internal(format!("batch_write_blob failed: {}", e)))?;
        }

        Ok(())
    }

    async fn read_blob(
        &self,
        volume_id: u32,
        file_key: u64,
        offset: i64,
        size: i32,
    ) -> Result<Vec<u8>> {
        let locations = self
            .client
            .lookup_volume(VolumeId(volume_id))
            .await
            .map_err(|e| PowerFsError::Internal(format!("lookup_volume failed: {}", e)))?;

        if let Some(loc) = locations.first() {
            let addr = PowerFuseClient::location_to_grpc_addr(loc);
            let data = self
                .client
                .read_blob(&addr, volume_id, file_key, offset, size)
                .await
                .map_err(|e| PowerFsError::Internal(format!("read_blob failed: {}", e)))?;
            Ok(data)
        } else {
            Err(PowerFsError::VolumeNotFound(VolumeId(volume_id)))
        }
    }

    async fn delete_blob(&self, volume_id: u32, file_key: u64) -> Result<()> {
        let locations = self
            .client
            .lookup_volume(VolumeId(volume_id))
            .await
            .map_err(|e| PowerFsError::Internal(format!("lookup_volume failed: {}", e)))?;

        if let Some(loc) = locations.first() {
            let addr = PowerFuseClient::location_to_grpc_addr(loc);
            self.client
                .delete_data(&addr, volume_id, file_key)
                .await
                .map_err(|e| PowerFsError::Internal(format!("delete_blob failed: {}", e)))?;
        }

        Ok(())
    }
}
