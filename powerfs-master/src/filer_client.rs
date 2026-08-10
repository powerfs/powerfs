//! Filer management client: gRPC client for calling filer-side shard scaling
//! RPCs (AddShard / DrainShard / RemoveShard) and migration support RPCs
//! (FindInodesByVolume / UpdateInodeSizeChunks).
//!
//! Used by `MasterManagementApi` to delegate shard scaling and by
//! `MasterMigrationExecutor` for needle→inode reverse lookup and chunk updates.

use std::sync::Arc;

use tonic::transport::Channel;

use crate::filer_proto::powerfs::filer_meta_service_client::FilerMetaServiceClient;
use crate::filer_proto::powerfs::{
    AddShardRequest, DrainShardRequest, FindInodesByVolumeRequest, RemoveShardRequest,
    UpdateInodeSizeChunksRequest,
};

use crate::master::MasterNode;

/// Filer chunk info returned by `find_inodes_by_volume`.
pub struct FilerChunkEntry {
    pub inode: u64,
    pub shard_id: u64,
    pub needle_id: u64,
    pub volume_id: u64,
    pub offset: u64,
    pub size: u64,
    pub file_size: u64,
}

/// Filer management client: calls filer-side shard scaling RPCs.
///
/// Each call creates a fresh gRPC channel to the target filer. This is
/// acceptable because shard scaling is a rare admin operation (not on the
/// hot path). A channel pool can be added later if needed.
pub struct FilerManagementClient {
    master: Arc<MasterNode>,
}

impl FilerManagementClient {
    pub fn new(master: Arc<MasterNode>) -> Self {
        Self { master }
    }

    /// Pick the first available filer address from the master's registered
    /// filers. Returns `None` if no filers are registered.
    fn first_filer_address(&self) -> Option<String> {
        let filers = self.master.list_filers();
        filers
            .into_iter()
            .find(|f| f.is_healthy)
            .map(|f| format!("{}:{}", f.address, f.grpc_port))
    }

    /// Call the filer's `AddShard` RPC.
    ///
    /// `split_from = None` tells the filer to auto-select the largest active
    /// shard. Returns the split plan on success.
    pub async fn add_shard(
        &self,
        split_from: Option<u64>,
        dry_run: bool,
    ) -> Result<powerfs_allocator::ShardSplitPlan, String> {
        let addr = self
            .first_filer_address()
            .ok_or("no healthy filer available")?;
        let channel = Channel::from_shared(format!("http://{}", addr))
            .map_err(|e| format!("invalid filer address: {}", e))?
            .connect()
            .await
            .map_err(|e| format!("failed to connect to filer {}: {}", addr, e))?;
        let mut client = FilerMetaServiceClient::new(channel);

        let req = AddShardRequest {
            split_from: split_from.unwrap_or(0),
            dry_run,
        };
        let resp = client
            .add_shard(req)
            .await
            .map_err(|e| format!("AddShard RPC failed: {}", e))?
            .into_inner();

        if !resp.success {
            return Err(resp.error);
        }

        Ok(powerfs_allocator::ShardSplitPlan {
            split_from: powerfs_allocator::ShardId(resp.split_from),
            split_point: resp.split_point,
            new_shard_id: powerfs_allocator::ShardId(resp.new_shard_id),
            new_range: (resp.new_range_start, resp.new_range_end),
            affected_future_allocations: resp.affected_future_allocations,
        })
    }

    /// Call the filer's `DrainShard` RPC.
    pub async fn drain_shard(&self, shard_id: u64) -> Result<(), String> {
        let addr = self
            .first_filer_address()
            .ok_or("no healthy filer available")?;
        let channel = Channel::from_shared(format!("http://{}", addr))
            .map_err(|e| format!("invalid filer address: {}", e))?
            .connect()
            .await
            .map_err(|e| format!("failed to connect to filer {}: {}", addr, e))?;
        let mut client = FilerMetaServiceClient::new(channel);

        let resp = client
            .drain_shard(DrainShardRequest { shard_id })
            .await
            .map_err(|e| format!("DrainShard RPC failed: {}", e))?
            .into_inner();

        if !resp.success {
            return Err(resp.error);
        }
        Ok(())
    }

    /// Call the filer's `RemoveShard` RPC.
    pub async fn remove_shard(&self, shard_id: u64) -> Result<(), String> {
        let addr = self
            .first_filer_address()
            .ok_or("no healthy filer available")?;
        let channel = Channel::from_shared(format!("http://{}", addr))
            .map_err(|e| format!("invalid filer address: {}", e))?
            .connect()
            .await
            .map_err(|e| format!("failed to connect to filer {}: {}", addr, e))?;
        let mut client = FilerMetaServiceClient::new(channel);

        let resp = client
            .remove_shard(RemoveShardRequest { shard_id })
            .await
            .map_err(|e| format!("RemoveShard RPC failed: {}", e))?
            .into_inner();

        if !resp.success {
            return Err(resp.error);
        }
        Ok(())
    }

    /// Find inodes that have chunks on `volume_id` (optionally filtered by
    /// `needle_ids`). Used by the migration executor for needle→inode reverse
    /// lookup before copying data and updating chunk mappings.
    pub async fn find_inodes_by_volume(
        &self,
        volume_id: u64,
        needle_ids: &[u64],
    ) -> Result<Vec<FilerChunkEntry>, String> {
        let addr = self.first_filer_address().ok_or("no healthy filer available")?;
        let channel = Channel::from_shared(format!("http://{}", addr))
            .map_err(|e| format!("invalid filer address: {}", e))?
            .connect()
            .await
            .map_err(|e| format!("failed to connect to filer {}: {}", addr, e))?;
        let mut client = FilerMetaServiceClient::new(channel);

        let resp = client
            .find_inodes_by_volume(FindInodesByVolumeRequest {
                volume_id,
                needle_ids: needle_ids.to_vec(),
            })
            .await
            .map_err(|e| format!("FindInodesByVolume RPC failed: {}", e))?
            .into_inner();

        if !resp.error.is_empty() {
            return Err(resp.error);
        }

        Ok(resp
            .entries
            .into_iter()
            .map(|e| FilerChunkEntry {
                inode: e.inode,
                shard_id: e.shard_id,
                needle_id: e.needle_id,
                volume_id: e.volume_id,
                offset: e.offset,
                size: e.size,
                file_size: e.file_size,
            })
            .collect())
    }

    /// Update an inode's size and chunk list on the filer (Raft-replicated).
    /// Used by the migration executor after copying data to a new volume.
    pub async fn update_inode_size_chunks(
        &self,
        shard_id: u64,
        inode: u64,
        size: u64,
        chunks: &[(u64, u64, u64, u64, u32)], // (offset, size, needle_id, volume_id, crc32)
        client_id: &str,
    ) -> Result<(), String> {
        let addr = self.first_filer_address().ok_or("no healthy filer available")?;
        let channel = Channel::from_shared(format!("http://{}", addr))
            .map_err(|e| format!("invalid filer address: {}", e))?
            .connect()
            .await
            .map_err(|e| format!("failed to connect to filer {}: {}", addr, e))?;
        let mut client = FilerMetaServiceClient::new(channel);

        let proto_chunks: Vec<crate::filer_proto::powerfs::FileChunk> = chunks
            .iter()
            .map(|(offset, size, needle_id, volume_id, crc32)| {
                crate::filer_proto::powerfs::FileChunk {
                    offset: *offset,
                    size: *size,
                    mtime: 0,
                    needle_id: *needle_id,
                    volume_id: *volume_id,
                    crc32: *crc32,
                }
            })
            .collect();

        let resp = client
            .update_inode_size_chunks(UpdateInodeSizeChunksRequest {
                shard_id,
                inode,
                size,
                chunks: proto_chunks,
                client_id: client_id.to_string(),
            })
            .await
            .map_err(|e| format!("UpdateInodeSizeChunks RPC failed: {}", e))?
            .into_inner();

        if !resp.success {
            return Err(resp.error);
        }
        Ok(())
    }
}
