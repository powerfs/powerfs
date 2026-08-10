//! Filer management client: gRPC client for calling filer-side shard scaling
//! RPCs (AddShard / DrainShard / RemoveShard).
//!
//! Used by `MasterManagementApi` to delegate shard scaling operations to the
//! filer that owns the shard being split/drained/removed.

use std::sync::Arc;

use tonic::transport::Channel;

use crate::filer_proto::powerfs::filer_meta_service_client::FilerMetaServiceClient;
use crate::filer_proto::powerfs::{AddShardRequest, DrainShardRequest, RemoveShardRequest};

use crate::master::MasterNode;

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
}
