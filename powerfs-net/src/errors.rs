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
}

pub type NetResult<T> = Result<T, NetError>;
