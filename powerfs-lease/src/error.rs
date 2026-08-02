//! Lease error type.

use thiserror::Error;

/// Errors returned by lease operations.
#[derive(Debug, Error)]
pub enum LeaseError {
    #[error("lease token not found")]
    NotFound,
    #[error("lease holder mismatch: expected {expected}, got {actual}")]
    HolderMismatch { expected: String, actual: String },
    #[error("lease expired")]
    Expired,
    #[error("lease expired beyond grace period")]
    ExpiredBeyondGrace,
    #[error("lease conflict: {0}")]
    Conflict(String),
    #[error("lease key not covered by this lease")]
    KeyNotCovered,
    #[error("internal error: {0}")]
    Internal(String),
}
