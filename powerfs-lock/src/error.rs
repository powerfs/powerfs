//! Error type for lock operations.
//!
//! Mirrors `powerfs_lease::LeaseError` shape (so filer/volume backends can
//! map their errors 1:1), plus extra variants for the fault-isolation layer
//! (client quarantine) and network transport.

use thiserror::Error;

/// Errors returned by `LockManager` operations.
#[derive(Debug, Error)]
pub enum LockError {
    #[error("lock token not found")]
    NotFound,

    #[error("lock holder mismatch: expected {expected}, got {actual}")]
    HolderMismatch { expected: String, actual: String },

    #[error("lock expired")]
    Expired,

    #[error("lock expired beyond grace period")]
    ExpiredBeyondGrace,

    #[error("lock conflict: {0}")]
    Conflict(String),

    #[error("lock key not covered by this lease")]
    KeyNotCovered,

    /// Returned by the fault-isolation layer when a client has been
    /// quarantined (health score < 10 — see §8.2 Layer 3).
    #[error("client quarantined: {0}")]
    Quarantined(String),

    /// Transport-level error (RPC failure, connection reset, etc.).
    #[error("network error: {0}")]
    Network(String),

    #[error("internal error: {0}")]
    Internal(String),
}

impl LockError {
    /// Convenience: whether this error is a transient retryable failure.
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            LockError::Expired | LockError::ExpiredBeyondGrace | LockError::Network(_)
        )
    }

    /// Convenience: whether this error indicates the client should back off
    /// (quarantine or persistent conflict).
    pub fn is_backoff(&self) -> bool {
        matches!(self, LockError::Quarantined(_) | LockError::Conflict(_))
    }
}
