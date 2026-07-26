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
use crate::net_client::{AssignResult, PowerFuseNetClient, VolumeLocation};

// ============================================================================
// gRPC-based providers (existing, kept for S3/KV compatibility)
// ============================================================================

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

// ============================================================================
// Net-based providers (powerfs-net binary protocol, parallel to gRPC)
// ============================================================================

/// Convert VolumeLocation from net client to Location trait
fn net_location_to_location(loc: &VolumeLocation) -> Location {
    Location {
        url: loc.url.clone(),
        public_url: loc.url.clone(),
        grpc_port: 0,
        data_center: loc.data_center.clone(),
    }
}

/// Convert AssignResult to (Fid, Vec<Location>)
fn assign_result_to_fid_locations(result: &AssignResult) -> (Fid, Vec<Location>) {
    let fid = Fid::from_string(&result.fid).unwrap_or(Fid {
        volume_id: VolumeId(0),
        cookie: 0,
        file_key: 0,
    });
    let location = Location {
        url: result.location_url.clone(),
        public_url: result.location_url.clone(),
        grpc_port: 0,
        data_center: String::new(),
    };
    (fid, vec![location])
}

pub struct NetFuseVolumeProvider {
    client: Arc<PowerFuseNetClient>,
}

impl NetFuseVolumeProvider {
    pub fn new(client: Arc<PowerFuseNetClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl VolumeProvider for NetFuseVolumeProvider {
    async fn assign_volume(
        &self,
        collection: &str,
        replication: &str,
    ) -> Result<(Fid, Vec<Location>)> {
        let result = self
            .client
            .assign_volume(collection, replication)
            .await
            .map_err(|e| PowerFsError::Internal(format!("net assign_volume failed: {}", e)))?;

        Ok(assign_result_to_fid_locations(&result))
    }

    async fn lookup_volume(&self, volume_id: VolumeId) -> Result<Vec<Location>> {
        let locations = self
            .client
            .lookup_volume(volume_id.0)
            .await
            .map_err(|e| PowerFsError::Internal(format!("net lookup_volume failed: {}", e)))?;

        Ok(locations.iter().map(net_location_to_location).collect())
    }

    async fn heartbeat(&self, _node_id: &NodeId, _stats: &NodeStats) -> Result<()> {
        Ok(())
    }

    async fn list_volumes(&self, _filters: &VolumeFilters) -> Result<Vec<VolumeInfo>> {
        Ok(Vec::new())
    }
}

pub struct NetFuseMetadataProvider {
    client: Arc<PowerFuseNetClient>,
}

impl NetFuseMetadataProvider {
    pub fn new(client: Arc<PowerFuseNetClient>) -> Self {
        Self { client }
    }
}

/// Convert EntryInfo from net client to Entry trait
fn entry_info_to_entry(info: &powerfs_net::EntryInfo, path: &str) -> Entry {
    let attributes = EntryAttributes {
        ino: info.ino,
        mode: info.mode,
        uid: info.uid,
        gid: info.gid,
        atime: DateTime::from_timestamp(info.atime as i64, 0).unwrap_or_else(Utc::now),
        mtime: DateTime::from_timestamp(info.mtime as i64, 0).unwrap_or_else(Utc::now),
        ctime: DateTime::from_timestamp(info.ctime as i64, 0).unwrap_or_else(Utc::now),
        crtime: DateTime::from_timestamp(info.ctime as i64, 0).unwrap_or_else(Utc::now),
    };

    Entry {
        name: path.to_string(),
        directory: String::new(),
        attributes: Some(attributes),
        chunks: Vec::new(),
        hard_link_id: String::new(),
        hard_link_counter: 0,
        extended: std::collections::HashMap::new(),
        content_size: info.size,
        disk_size: info.size,
        ttl: String::new(),
        symlink_target: info.symlink_target.clone().unwrap_or_default(),
        owner: String::new(),
        generation: 0,
    }
}

#[async_trait]
impl MetadataProvider for NetFuseMetadataProvider {
    async fn get_entry(&self, path: &str) -> Result<Option<Entry>> {
        // Parse path to get parent_ino and name
        let (parent_ino, name) = parse_path_to_parent_name(path);
        match self.client.lookup(parent_ino, &name).await {
            Ok(info) => Ok(Some(entry_info_to_entry(&info, path))),
            Err(_) => Ok(None),
        }
    }

    async fn get_entry_by_inode(&self, inode: u64) -> Result<Option<(Entry, String)>> {
        // Net protocol needs path for lookup, but we only have inode
        // This is a limitation - return not found for now
        let _ = inode;
        Ok(None)
    }

    async fn create_entry(&self, entry: &Entry, _client_id: &str) -> Result<u64> {
        let parent_ino = 1; // Root inode
        let name = entry.name.clone();
        let mode = entry
            .attributes
            .as_ref()
            .map(|a| a.mode as u64)
            .unwrap_or(0o644);
        let uid = entry.attributes.as_ref().map(|a| a.uid as u64).unwrap_or(0);
        let gid = entry.attributes.as_ref().map(|a| a.gid as u64).unwrap_or(0);

        let ino = if entry
            .attributes
            .as_ref()
            .map(|a| a.mode & 0o170000 == 0o040000)
            .unwrap_or(false)
        {
            self.client
                .mkdir(parent_ino, &name, mode, uid, gid)
                .await
                .map_err(|e| PowerFsError::Internal(format!("net mkdir failed: {}", e)))?
        } else {
            self.client
                .create_file(parent_ino, &name, mode, uid, gid)
                .await
                .map_err(|e| PowerFsError::Internal(format!("net create failed: {}", e)))?
        };

        Ok(ino)
    }

    async fn update_entry(
        &self,
        entry: &Entry,
        _client_id: &str,
        _old_size: u64,
        _is_truncate: bool,
    ) -> Result<u64> {
        let ino = entry.attributes.as_ref().map(|a| a.ino).unwrap_or(0);
        let size = Some(entry.content_size);
        let mode = entry.attributes.as_ref().map(|a| a.mode as u64);
        let uid = entry.attributes.as_ref().map(|a| a.uid as u64);
        let gid = entry.attributes.as_ref().map(|a| a.gid as u64);

        self.client
            .setattr(ino, size, mode, uid, gid)
            .await
            .map_err(|e| PowerFsError::Internal(format!("net setattr failed: {}", e)))?;

        Ok(entry.content_size)
    }

    async fn delete_entry(&self, inode: u64, is_dir: bool, _client_id: &str) -> Result<()> {
        // Net protocol unlink/rmdir uses parent_ino + name, but we only have inode
        // This is a limitation - for now we use inode as parent_ino with empty name
        // The server side needs to handle this mapping
        let _ = inode;
        let _ = is_dir;
        // TODO: Need to resolve inode -> (parent_ino, name) mapping
        Ok(())
    }

    async fn list_entries(&self, inode: u64, limit: u32, _client_id: &str) -> Result<Vec<Entry>> {
        let entries = self
            .client
            .readdir(inode, limit as u64, "")
            .await
            .map_err(|e| PowerFsError::Internal(format!("net readdir failed: {}", e)))?;

        Ok(entries
            .into_iter()
            .map(|info| entry_info_to_entry(&info, &info.name))
            .collect())
    }
}

pub struct NetFuseStorageProvider {
    client: Arc<PowerFuseNetClient>,
}

impl NetFuseStorageProvider {
    pub fn new(client: Arc<PowerFuseNetClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl StorageProvider for NetFuseStorageProvider {
    async fn write_blob(
        &self,
        volume_id: u32,
        file_key: u64,
        offset: i64,
        size: i32,
        data: &[u8],
    ) -> Result<()> {
        // Get volume location first
        let locations = self
            .client
            .lookup_volume(volume_id)
            .await
            .map_err(|e| PowerFsError::Internal(format!("net lookup_volume failed: {}", e)))?;

        if let Some(loc) = locations.first() {
            // Parse URL to get addr and port
            let (addr, port) = parse_volume_url(&loc.url, self.client.volume_net_port());
            self.client
                .write_blob(
                    &addr,
                    port,
                    volume_id,
                    file_key,
                    offset,
                    size as u64,
                    data.to_vec(),
                )
                .await
                .map_err(|e| PowerFsError::Internal(format!("net write_blob failed: {}", e)))?;
        }

        Ok(())
    }

    async fn batch_write_blob(
        &self,
        volume_id: u32,
        file_key: u64,
        entries: &[(i64, i32, Vec<u8>, u32)],
    ) -> Result<()> {
        for (offset, size, data, _cookie) in entries {
            self.write_blob(volume_id, file_key, *offset, *size, data)
                .await?;
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
            .lookup_volume(volume_id)
            .await
            .map_err(|e| PowerFsError::Internal(format!("net lookup_volume failed: {}", e)))?;

        if let Some(loc) = locations.first() {
            let (addr, port) = parse_volume_url(&loc.url, self.client.volume_net_port());
            let data = self
                .client
                .read_blob(&addr, port, volume_id, file_key, offset, size as u64)
                .await
                .map_err(|e| PowerFsError::Internal(format!("net read_blob failed: {}", e)))?;
            Ok(data)
        } else {
            Err(PowerFsError::VolumeNotFound(VolumeId(volume_id)))
        }
    }

    async fn delete_blob(&self, volume_id: u32, file_key: u64) -> Result<()> {
        let locations = self
            .client
            .lookup_volume(volume_id)
            .await
            .map_err(|e| PowerFsError::Internal(format!("net lookup_volume failed: {}", e)))?;

        if let Some(loc) = locations.first() {
            let (addr, port) = parse_volume_url(&loc.url, self.client.volume_net_port());
            self.client
                .delete_data(&addr, port, volume_id, file_key)
                .await
                .map_err(|e| PowerFsError::Internal(format!("net delete_blob failed: {}", e)))?;
        }

        Ok(())
    }
}

// ============================================================================
// Helper functions
// ============================================================================

/// Parse a file path into (parent_ino, name)
/// Format: "/dir1/dir2/file" -> parent_ino from dir2 inode, name = "file"
fn parse_path_to_parent_name(path: &str) -> (u64, String) {
    let path = path.trim_start_matches('/');
    if path.is_empty() {
        return (1, String::new()); // Root
    }

    let parts: Vec<&str> = path.split('/').collect();
    if parts.len() == 1 {
        return (1, parts[0].to_string()); // Direct child of root
    }

    // For now, return root inode (1) and the last component
    // In a full implementation, we'd need to resolve parent inode
    let name = parts.last().unwrap_or(&"").to_string();
    (1, name)
}

/// Parse a volume URL to extract addr and port
/// URL format: "host:port" or just "host" (uses default port)
fn parse_volume_url(url: &str, default_port: u16) -> (String, u16) {
    if let Some((host, port_str)) = url.split_once(':') {
        let port = port_str.parse().unwrap_or(default_port);
        (host.to_string(), port)
    } else {
        (url.to_string(), default_port)
    }
}
