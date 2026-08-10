//! powerfs-allocator: pluggable resource allocation strategy crate.
//!
//! Decouples allocation policy from service state. Services build a
//! [`snapshot::ClusterSnapshot`] from heartbeats, pass it to the
//! [`Allocator`] trait, and receive an [`AllocationDecision`].
//!
//! ## Six modules
//!
//! 1. **Static config** ([`config`]) — topology + policy thresholds
//! 2. **Dynamic state** ([`snapshot`]) — heartbeat-aggregated runtime snapshot
//! 3. **Allocation request** ([`allocator::AllocationRequest`]) — client request
//! 4. **Allocation decision** ([`allocator::AllocationDecision`]) — allocator output
//! 5. **Status query** ([`status::StatusQuery`]) — read-only monitoring interface
//! 6. **Management API** ([`management::ManagementApi`]) — write ops + scaling
//!
//! ## Shard routing
//!
//! [`shard_map::ShardMap`] replaces the old modulo-based `calculate_shard`
//! with a range-based mapping table, enabling shard addition without
//! metadata migration.

pub mod allocator;
pub mod config;
pub mod error;
pub mod filer_alloc;
pub mod management;
pub mod shard_map;
pub mod snapshot;
pub mod status;
pub mod volume_assigner;

// Re-export key types for convenience.
pub use allocator::{
    score_volume, AllocationDecision, AllocationRequest, Allocator, InodeBatchDecision,
    InodeBatchReq, SingleFileDecision, SingleFileReq, StripeFileDecision, StripeFileReq,
    VolumeAssignDecision, VolumeAssignReq,
};
pub use config::{
    ClusterStaticConfig, MigrationPolicy, PlacementPolicyConfig, RebalancePolicy, ZoneConfig,
};
pub use error::{AllocError, ManageError, ShardError, ShardId};
pub use filer_alloc::{FilerAllocator, VolumePick, ZoneView};
pub use management::{
    ManagementApi, MigrationExecutionResult, MigrationRejection, RebalanceAction,
    RejectionReason, ShardSplitPlan,
};
pub use shard_map::{ShardMap, ShardState};
pub use snapshot::{
    ClusterSnapshot, NodeRuntime, NodeRuntimeState, ShardRuntime, VolumeLoad, VolumeRuntime,
    VolumeRuntimeState,
};
pub use status::{
    AllocationStats, AllocationStatsCollector, ClusterOverview, MigrationState,
    MigrationTaskStatus, MigrationType, NodeLoadReport, SnapshotStatusQuery, StatusQuery,
    VolumeDetail,
};
pub use volume_assigner::{
    AssignContext, AssignerType, ConsistentHashAssigner, RoundRobinAssigner, SmartVolumeAssigner,
    VolumeAssigner,
};
