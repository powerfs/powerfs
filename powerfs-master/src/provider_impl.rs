use async_trait::async_trait;
use powerfs_common::{
    error::{PowerFsError, Result},
    traits::{
        Entry, EntryAttributes, FileChunk, Location, MetadataProvider, NodeStats, VolumeFilters,
        VolumeProvider,
    },
    types::{DataNodeInfo, Fid, NodeId, VolumeId, VolumeInfo},
};

use crate::directory_tree::DirectoryTree;
use crate::master::MasterNode;

#[async_trait]
impl VolumeProvider for MasterNode {
    async fn assign_volume(
        &self,
        collection: &str,
        replication: &str,
    ) -> Result<(Fid, Vec<Location>)> {
        let (fid, nodes) = self.assign_volume(replication, collection).await?;
        let locations = nodes.into_iter().map(node_to_location).collect();
        Ok((fid, locations))
    }

    async fn lookup_volume(&self, volume_id: VolumeId) -> Result<Vec<Location>> {
        let volume_id_str = volume_id.0.to_string();
        let result = self.lookup_volume(&[volume_id_str]).await;
        if let Some(nodes) = result.get(&volume_id) {
            let locations = nodes.iter().cloned().map(node_to_location).collect();
            Ok(locations)
        } else {
            Err(PowerFsError::VolumeNotFound(volume_id))
        }
    }

    async fn heartbeat(&self, node_id: &NodeId, stats: &NodeStats) -> Result<()> {
        if let Some(mut node) = self.get_node(node_id) {
            node.total_space = stats.total_space;
            node.used_space = stats.used_space;
            node.last_heartbeat = chrono::Utc::now();
            node.volume_count = stats.volume_count;
            Ok(())
        } else {
            Err(PowerFsError::InvalidRequest(format!(
                "node not found: {}",
                node_id
            )))
        }
    }

    async fn list_volumes(&self, filters: &VolumeFilters) -> Result<Vec<VolumeInfo>> {
        let volumes = self.list_volumes().await;
        let mut result: Vec<VolumeInfo> = volumes;

        if let Some(collection) = &filters.collection {
            result.retain(|v| v.collection == *collection);
        }
        if let Some(state) = &filters.state {
            result.retain(|v| {
                let state_str = match v.state {
                    powerfs_common::types::VolumeState::Creating => "creating",
                    powerfs_common::types::VolumeState::Available => "available",
                    powerfs_common::types::VolumeState::Full => "full",
                    powerfs_common::types::VolumeState::ReadOnly => "readonly",
                    powerfs_common::types::VolumeState::Deleting => "deleting",
                };
                state_str == state
            });
        }
        if let Some(node_id) = &filters.node_id {
            result.retain(|v| v.node_id == *node_id);
        }

        Ok(result)
    }
}

fn node_to_location(node: DataNodeInfo) -> Location {
    Location {
        url: node.url(),
        public_url: node.public_url,
        grpc_port: node.grpc_port,
        data_center: node.data_center_id.to_string(),
    }
}

fn proto_entry_to_trait_entry(entry: crate::proto::Entry) -> Entry {
    let attributes = entry.attributes.map(|attrs| EntryAttributes {
        ino: attrs.ino,
        mode: attrs.mode,
        uid: attrs.uid,
        gid: attrs.gid,
        atime: chrono::DateTime::from_timestamp(attrs.atime as i64, 0)
            .unwrap_or_else(chrono::Utc::now),
        mtime: chrono::DateTime::from_timestamp(attrs.mtime as i64, 0)
            .unwrap_or_else(chrono::Utc::now),
        ctime: chrono::DateTime::from_timestamp(attrs.ctime as i64, 0)
            .unwrap_or_else(chrono::Utc::now),
        crtime: chrono::DateTime::from_timestamp(attrs.crtime as i64, 0)
            .unwrap_or_else(chrono::Utc::now),
    });

    let chunks = entry
        .chunks
        .into_iter()
        .map(|chunk| FileChunk {
            offset: chunk.offset,
            size: chunk.size,
            mtime: chunk.mtime,
            fid: chunk.fid.to_string(),
            cookie: chunk.cookie,
            crc32: chunk.crc32,
        })
        .collect();

    Entry {
        name: entry.name,
        directory: entry.directory,
        attributes,
        chunks,
        hard_link_id: entry.hard_link_id,
        hard_link_counter: entry.hard_link_counter,
        extended: entry.extended,
        content_size: entry.content_size,
        disk_size: entry.disk_size,
        ttl: entry.ttl,
        symlink_target: entry.symlink_target,
        owner: entry.owner,
        generation: entry.generation,
    }
}

fn trait_entry_to_proto_entry(entry: &Entry) -> crate::proto::Entry {
    let attributes = entry
        .attributes
        .as_ref()
        .map(|attrs| crate::proto::FuseAttributes {
            mode: attrs.mode,
            uid: attrs.uid,
            gid: attrs.gid,
            atime: attrs.atime.timestamp() as u64,
            mtime: attrs.mtime.timestamp() as u64,
            ctime: attrs.ctime.timestamp() as u64,
            crtime: attrs.crtime.timestamp() as u64,
            ino: 0,
            nlink: 1,
            size: entry.content_size,
            blksize: 4096,
            blocks: entry.disk_size.div_ceil(4096),
            rdev: 0,
            perm: 0,
        });

    let chunks = entry
        .chunks
        .iter()
        .map(|chunk| crate::proto::FileChunk {
            offset: chunk.offset,
            size: chunk.size,
            mtime: chunk.mtime,
            fid: chunk.fid.clone(),
            cookie: chunk.cookie,
            crc32: chunk.crc32,
        })
        .collect();

    crate::proto::Entry {
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
impl MetadataProvider for DirectoryTree {
    async fn get_entry(&self, path: &str) -> Result<Option<Entry>> {
        let entry = self.get_entry(path);
        Ok(entry.map(proto_entry_to_trait_entry))
    }

    async fn get_entry_by_inode(&self, inode: u64) -> Result<Option<(Entry, String)>> {
        let result = self.get_entry_by_inode(inode);
        Ok(result.map(|(entry, path)| (proto_entry_to_trait_entry(entry), path)))
    }

    async fn create_entry(&self, entry: &Entry, client_id: &str) -> Result<u64> {
        let proto_entry = trait_entry_to_proto_entry(entry);
        let inode = self
            .create_entry(proto_entry, client_id)
            .map_err(|e| PowerFsError::Internal(format!("rocksdb error: {}", e)))?;
        if inode == 0 {
            Err(PowerFsError::DirectoryNotFound(entry.directory.clone()))
        } else {
            Ok(inode)
        }
    }

    async fn update_entry(
        &self,
        entry: &Entry,
        client_id: &str,
        old_size: u64,
        is_truncate: bool,
    ) -> Result<u64> {
        let proto_entry = trait_entry_to_proto_entry(entry);
        let inode = self
            .update_entry(proto_entry, client_id, old_size, is_truncate)
            .map_err(|e| PowerFsError::Internal(format!("rocksdb error: {}", e)))?;
        Ok(inode)
    }

    async fn delete_entry(&self, inode: u64, _is_dir: bool, client_id: &str) -> Result<()> {
        let result = self
            .delete_entry(inode, client_id)
            .map_err(|e| PowerFsError::Internal(format!("rocksdb error: {}", e)))?;
        if !result {
            Err(PowerFsError::FileNotFound(format!("inode {}", inode)))
        } else {
            Ok(())
        }
    }

    async fn list_entries(&self, inode: u64, limit: u32, _client_id: &str) -> Result<Vec<Entry>> {
        let entries = self.list_entries(inode, limit as u64, "");
        let entries = entries
            .into_iter()
            .map(proto_entry_to_trait_entry)
            .collect();
        Ok(entries)
    }
}
