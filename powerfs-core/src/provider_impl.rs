use async_trait::async_trait;
use chrono::Utc;
use powerfs_common::{
    error::{PowerFsError, Result},
    traits::{KvCacheProvider, SessionInfo, SessionStats, StorageProvider},
};

use crate::kv_cache::{KVCacheEngine, PinMode};
use crate::storage::StorageManager;
use powerfs_common::types::VolumeId;

#[async_trait]
impl KvCacheProvider for KVCacheEngine {
    async fn put_block(&self, session_id: &str, block_id: u64, data: &[u8]) -> Result<()> {
        let _ = self.put_block(session_id, block_id as u32, 0, data, "", 0, PinMode::None);
        Ok(())
    }

    async fn get_block(&self, _session_id: &str, block_id: u64) -> Result<Option<Vec<u8>>> {
        let result = self.get_block_data(block_id);
        Ok(result.map(|(_, data)| data))
    }

    async fn list_sessions(&self) -> Result<Vec<SessionInfo>> {
        let (session_ids, _) = self.list_sessions(1000, "");
        let mut sessions = Vec::new();

        for session_id in session_ids {
            if let Some(session) = self.get_session(&session_id) {
                let blocks = self.get_session_blocks(&session_id);
                let total_size: u64 = blocks.iter().map(|b| b.size_bytes).sum();

                sessions.push(SessionInfo {
                    session_id: session_id.clone(),
                    block_count: session.block_ids.len() as u64,
                    total_size,
                    created_at: Utc::now(),
                    last_accessed_at: Utc::now(),
                });
            }
        }

        Ok(sessions)
    }

    async fn evict_session(&self, session_id: &str) -> Result<()> {
        self.delete_session(session_id)
            .map_err(|e| PowerFsError::Internal(format!("kv cache error: {}", e)))
    }

    async fn get_session_stats(&self, session_id: &str) -> Result<Option<SessionStats>> {
        if let Some(_session) = self.get_session(session_id) {
            let blocks = self.get_session_blocks(session_id);
            let total_size: u64 = blocks.iter().map(|b| b.size_bytes).sum();

            Ok(Some(SessionStats {
                session_id: session_id.to_string(),
                block_count: blocks.len() as u64,
                total_size,
                hit_count: 0,
                miss_count: 0,
            }))
        } else {
            Ok(None)
        }
    }
}

#[async_trait]
impl StorageProvider for StorageManager {
    async fn write_blob(
        &self,
        volume_id: u32,
        file_key: u64,
        offset: i64,
        size: i32,
        data: &[u8],
    ) -> Result<()> {
        let volume = self
            .get_volume(&VolumeId(volume_id))
            .ok_or_else(|| PowerFsError::VolumeNotFound(VolumeId(volume_id)))?;
        volume.write_needle_blob(file_key, offset, size, bytes::Bytes::from(data.to_vec()), 0)
    }

    async fn batch_write_blob(
        &self,
        volume_id: u32,
        file_key: u64,
        entries: &[(i64, i32, Vec<u8>, u32)],
    ) -> Result<()> {
        let volume = self
            .get_volume(&VolumeId(volume_id))
            .ok_or_else(|| PowerFsError::VolumeNotFound(VolumeId(volume_id)))?;

        for (offset, size, data, _cookie) in entries {
            volume.write_needle_blob(
                file_key,
                *offset,
                *size,
                bytes::Bytes::from(data.clone()),
                0,
            )?;
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
        let volume = self
            .get_volume(&VolumeId(volume_id))
            .ok_or_else(|| PowerFsError::VolumeNotFound(VolumeId(volume_id)))?;
        let bytes = volume.read_needle_blob(file_key, offset, size)?;
        Ok(bytes.to_vec())
    }

    async fn delete_blob(&self, volume_id: u32, file_key: u64) -> Result<()> {
        let volume = self
            .get_volume(&VolumeId(volume_id))
            .ok_or_else(|| PowerFsError::VolumeNotFound(VolumeId(volume_id)))?;
        volume.delete_needle(&powerfs_common::types::NeedleId(file_key))
    }
}
