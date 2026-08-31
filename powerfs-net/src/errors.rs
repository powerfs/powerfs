//! Error types for powerfs-net

use thiserror::Error;

#[derive(Error, Debug)]
pub enum NetError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Serialization error: {0}")]
    Serialize(String),

    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Timeout")]
    Timeout,

    /// Idle "try again later" condition returned by async I/O primitives
    /// that the caller would otherwise have to string-match against.
    /// Specifically used by `async_get_cm_event` for `rdma_cm` poll
    /// timeouts / EINTR, so the acceptor loop can `continue` cleanly
    /// without logging ERROR-level noise (ROOT36).
    #[error("Would block")]
    WouldBlock,

    #[error("Not connected")]
    NotConnected,

    #[error("Invalid response: {0}")]
    InvalidResponse(String),

    #[error("Server error: {0}")]
    ServerError(String),

    #[error("CRC mismatch")]
    CrcMismatch,

    #[error("Invalid magic")]
    InvalidMagic,

    #[error("Unknown message type: {0}")]
    UnknownMsgType(u16),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Transport error: {0}")]
    Transport(String),
}

pub type NetResult<T> = Result<T, NetError>;

impl NetError {
    /// Returns true if the error represents a client disconnect (EOF).
    ///
    /// Used by IoLoop to distinguish clean disconnects from protocol errors.
    pub fn is_eof(&self) -> bool {
        matches!(self, NetError::Protocol(msg) if msg.contains("EOF"))
    }
}
