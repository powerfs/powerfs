//! Allocator error types.

use thiserror::Error;

/// Shard identifier (re-exported for convenience; canonical definition lives
/// here to break the filer → raft_group_manager dependency).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ShardId(pub u64);

impl std::fmt::Display for ShardId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "shard-{}", self.0)
    }
}

/// Allocation errors returned by [`crate::Allocator::allocate`].
#[derive(Debug, Clone, Error)]
pub enum AllocError {
    /// All volumes are full or excluded — maps to `ENOSPACE`.
    #[error("no space available: all volumes full or excluded")]
    NoSpace,

    /// No healthy node available for placement.
    #[error("no healthy node available")]
    NoHealthyNode,

    /// Could not satisfy anti-affinity constraints (e.g. not enough zones).
    #[error("anti-affinity constraints cannot be satisfied")]
    AntiAffinityFailed,

    /// The snapshot is too stale for a reliable decision.
    #[error("snapshot is stale (version {0})")]
    SnapshotStale(u64),

    /// Strategy-specific error.
    #[error("strategy error: {0}")]
    StrategyError(String),
}

/// Errors from shard routing / ShardMap operations.
#[derive(Debug, Clone, Error)]
pub enum ShardError {
    #[error("shard {0} not found")]
    ShardNotFound(ShardId),

    #[error("shard {0} is not draining, cannot remove")]
    ShardNotDraining(ShardId),

    #[error("shard {0} already exists")]
    ShardAlreadyExists(ShardId),

    #[error("invalid split point {0}")]
    InvalidSplitPoint(u64),

    #[error("no active shard available to split")]
    NoActiveShardToSplit,
}

/// Errors from management API operations.
#[derive(Debug, Clone, Error)]
pub enum ManageError {
    #[error("strategy '{0}' not found")]
    StrategyNotFound(String),

    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("resource {0} not found")]
    ResourceNotFound(String),

    #[error("operation not permitted in current state: {0}")]
    InvalidState(String),

    #[error("migration rejected: {0}")]
    MigrationRejected(String),
}
