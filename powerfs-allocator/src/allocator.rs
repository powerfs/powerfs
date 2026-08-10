//! Allocation trait and request/decision types (Modules 3 + 4).
//!
//! The allocator is a stateless function: `(snapshot, request) → decision`.
//! Services confirm the suggested needle_id atomically after receiving a decision.

use crate::config::RebalancePolicy;
use crate::error::AllocError;
use crate::snapshot::{ClusterSnapshot, NodeRuntime, VolumeRuntime};

/// Allocation request — issued by clients (filer, master).
#[derive(Clone, Debug)]
pub enum AllocationRequest {
    SingleFile(SingleFileReq),
    StripeFile(StripeFileReq),
    InodeBatch(InodeBatchReq),
    VolumeAssign(VolumeAssignReq),
}

#[derive(Clone, Debug)]
pub struct SingleFileReq {
    pub collection: String,
    pub replication: String,
    pub file_size_hint: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct StripeFileReq {
    pub collection: String,
    pub stripe_count: u32,
    pub stripe_size: u64,
}

#[derive(Clone, Debug)]
pub struct InodeBatchReq {
    pub count: u32,
    pub client_id: String,
}

#[derive(Clone, Debug)]
pub struct VolumeAssignReq {
    pub volume_id: u64,
    pub replica_count: usize,
    pub preferred_node: Option<String>,
}

/// Allocation decision — output of the allocator.
#[derive(Clone, Debug)]
pub enum AllocationDecision {
    SingleFile(SingleFileDecision),
    StripeFile(StripeFileDecision),
    InodeBatch(InodeBatchDecision),
    VolumeAssign(VolumeAssignDecision),
}

#[derive(Clone, Debug)]
pub struct SingleFileDecision {
    pub volume_id: u64,
    pub zone_id: u32,
    pub node_id: String,
    /// Suggested needle_id; service confirms atomically.
    pub suggested_needle_id: u64,
    /// Score (for debugging/monitoring).
    pub score: f64,
    /// Backup volume_ids (retry on CAS failure).
    pub alternatives: Vec<u64>,
}

#[derive(Clone, Debug)]
pub struct StripeFileDecision {
    pub placements: Vec<SingleFileDecision>,
    /// Zones actually used (for anti-affinity verification).
    pub used_zones: Vec<u32>,
}

#[derive(Clone, Debug)]
pub struct InodeBatchDecision {
    pub start_inode: u64,
    pub end_inode: u64,
    pub shard_id: u64,
}

#[derive(Clone, Debug)]
pub struct VolumeAssignDecision {
    pub volume_id: u64,
    pub assigned_nodes: Vec<String>,
}

/// Stateless allocator: input snapshot + request, output decision.
///
/// Implementations should be cheap to clone (Arc-wrapped internally if needed).
/// Services confirm the suggested needle_id via atomic CAS after receiving
/// a decision; on conflict they retry with `alternatives`.
pub trait Allocator: Send + Sync {
    fn allocate(
        &self,
        snapshot: &ClusterSnapshot,
        request: &AllocationRequest,
    ) -> Result<AllocationDecision, AllocError>;
}

/// Default scoring function: space + load composite score.
///
/// Returns `None` if the volume should be excluded (too full or unhealthy).
/// This is extracted as a free function so strategies can reuse or override it.
pub fn score_volume(
    vol: &VolumeRuntime,
    node: &NodeRuntime,
    policy: &RebalancePolicy,
) -> Option<f64> {
    // Exclude non-Active volumes
    if vol.state != crate::snapshot::VolumeRuntimeState::Active {
        return None;
    }
    // Exclude volumes on non-allocatable nodes
    if !node.is_allocatable() {
        return None;
    }

    let usage_ratio = vol.usage_ratio();

    // Hard exclude: usage >= near_full_exclude_ratio → never allocate
    if usage_ratio >= policy.near_full_exclude_ratio {
        return None;
    }

    // Soft penalty: usage > volume_full_threshold → score downgrade
    let space_score = if usage_ratio > policy.volume_full_threshold {
        vol.space_ratio() * 0.3
    } else {
        vol.space_ratio()
    };

    let load_penalty = node.load_score; // 0=idle, 1=saturated
    Some(space_score * 0.6 + (1.0 - load_penalty) * 0.4)
}
