use async_trait::async_trait;
use powerfs_common::{
    error::{PowerFsError, Result},
    traits::{Entry, EntryAttributes, FileChunk, MetadataProvider},
};

use crate::meta_shard_manager::MetaShardManager;

fn inode_info_to_entry(info: &crate::shard_store::InodeInfo) -> Entry {
    let attributes = Some(EntryAttributes {
        ino: info.inode,
        mode: info.mode,
        uid: info.uid,
        gid: info.gid,
        atime: chrono::DateTime::from_timestamp(info.atime as i64, 0)
            .unwrap_or_else(chrono::Utc::now),
        mtime: chrono::DateTime::from_timestamp(info.mtime as i64, 0)
            .unwrap_or_else(chrono::Utc::now),
        ctime: chrono::DateTime::from_timestamp(info.ctime as i64, 0)
            .unwrap_or_else(chrono::Utc::now),
        crtime: chrono::DateTime::from_timestamp(info.ctime as i64, 0)
            .unwrap_or_else(chrono::Utc::now),
    });

    let chunks = if let Some(fid) = &info.fid {
        vec![FileChunk {
            offset: 0,
            size: info.size,
            mtime: info.mtime,
            fid: fid.clone(),
            cookie: 0,
            crc32: 0,
        }]
    } else {
        Vec::new()
    };

    Entry {
        name: info.name.clone(),
        directory: "/".to_string(),
        attributes,
        chunks,
        hard_link_id: String::new(),
        hard_link_counter: 0,
        extended: std::collections::HashMap::new(),
        content_size: info.size,
        disk_size: info.blocks * 4096,
        ttl: String::new(),
        symlink_target: String::new(),
        owner: String::new(),
        generation: 0,
    }
}

#[async_trait]
impl MetadataProvider for MetaShardManager {
    async fn get_entry(&self, path: &str) -> Result<Option<Entry>> {
        let parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();
        if parts.is_empty() {
            return Ok(None);
        }

        let bucket = parts[0];
        let key = parts.get(1).unwrap_or(&"");

        let bucket_root_inode = match self.ensure_bucket_root(bucket).await {
            Ok(inode) => inode,
            Err(_) => return Ok(None),
        };

        if key.is_empty() {
            let info = self.get_inode(bucket_root_inode);
            return Ok(info.map(|i| inode_info_to_entry(&i)));
        }

        let info = self.get_object_entry(bucket_root_inode, key);
        Ok(info.map(|i| inode_info_to_entry(&i)))
    }

    async fn get_entry_by_inode(&self, inode: u64) -> Result<Option<(Entry, String)>> {
        let info = self.get_inode(inode);
        Ok(info.map(|i| (inode_info_to_entry(&i), "/".to_string())))
    }

    async fn create_entry(&self, entry: &Entry, _client_id: &str) -> Result<u64> {
        let parts: Vec<&str> = entry
            .directory
            .split('/')
            .filter(|p| !p.is_empty())
            .collect();
        if parts.is_empty() {
            return Err(PowerFsError::InvalidRequest(
                "directory required".to_string(),
            ));
        }

        let bucket = parts[0];
        let bucket_root_inode = self
            .ensure_bucket_root(bucket)
            .await
            .map_err(|e| PowerFsError::Internal(format!("failed to ensure bucket root: {}", e)))?;

        let is_dir = entry
            .attributes
            .as_ref()
            .map(|a| (a.mode & 0o40000) != 0)
            .unwrap_or(false);

        let inode = if is_dir {
            self.create_directory(bucket_root_inode, &entry.name)
                .await
                .map(|info| info.inode)
        } else {
            self.create_file(bucket_root_inode, &entry.name)
                .await
                .map(|info| info.inode)
        };

        inode.map_err(|e| PowerFsError::Internal(format!("failed to create entry: {}", e)))
    }

    async fn update_entry(
        &self,
        entry: &Entry,
        _client_id: &str,
        _old_size: u64,
        _is_truncate: bool,
    ) -> Result<u64> {
        let inode = entry.attributes.as_ref().map(|a| a.ino).unwrap_or(0);
        if inode == 0 {
            return Err(PowerFsError::InvalidRequest("inode required".to_string()));
        }

        let shard_strategy = self.get_shard_strategy();
        let shard_id = shard_strategy.calculate_shard(inode);
        self.update_entry(inode, shard_id, entry.content_size)
            .await
            .map_err(|e| PowerFsError::Internal(format!("failed to update entry: {}", e)))?;
        Ok(inode)
    }

    async fn delete_entry(&self, inode: u64, is_dir: bool, _client_id: &str) -> Result<()> {
        let shard_strategy = self.get_shard_strategy();
        let shard_id = shard_strategy.calculate_shard(inode);
        if is_dir {
            self.delete_directory_by_inode(inode, shard_id)
                .await
                .map_err(|e| PowerFsError::Internal(format!("failed to delete directory: {}", e)))
        } else {
            self.delete_file_by_inode(inode, shard_id)
                .await
                .map_err(|e| PowerFsError::Internal(format!("failed to delete file: {}", e)))
        }
    }

    async fn list_entries(&self, inode: u64, limit: u32, _client_id: &str) -> Result<Vec<Entry>> {
        let shard_strategy = self.get_shard_strategy();
        let shard_id = shard_strategy.calculate_shard(inode);
        let entries = self
            .list_entries(inode, shard_id, limit as usize)
            .await
            .map_err(|e| PowerFsError::Internal(format!("failed to list entries: {}", e)))?;
        Ok(entries
            .into_iter()
            .map(|i| inode_info_to_entry(&i))
            .collect())
    }
}
