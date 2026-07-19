use crate::types::{NeedleId, VolumeId};
use raft;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PowerFsError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serde json error: {0}")]
    SerdeJson(#[from] serde_json::Error),

    #[error("tonic transport error: {0}")]
    TonicTransport(#[from] tonic::transport::Error),

    #[error("tonic status error: {0}")]
    TonicStatus(Box<tonic::Status>),

    #[error("protobuf decode error: {0}")]
    ProstDecode(#[from] prost::DecodeError),

    #[error("protobuf encode error: {0}")]
    ProstEncode(#[from] prost::EncodeError),

    #[error("uuid parse error: {0}")]
    UuidParse(#[from] uuid::Error),

    #[error("address parse error: {0}")]
    AddrParse(#[from] std::net::AddrParseError),

    #[error("raft error: {0}")]
    Raft(Box<raft::Error>),

    #[error("volume not found: {0}")]
    VolumeNotFound(VolumeId),

    #[error("needle not found: {0}")]
    NeedleNotFound(NeedleId),

    #[error("volume already exists: {0}")]
    VolumeExists(VolumeId),

    #[error("invalid volume state: {0}")]
    InvalidVolumeState(String),

    #[error("invalid master state: {0}")]
    InvalidMasterState(String),

    #[error("invalid request: {0}")]
    InvalidRequest(String),

    #[error("internal error: {0}")]
    Internal(String),

    #[error("timeout")]
    Timeout,

    #[error("connection refused")]
    ConnectionRefused,

    #[error("not leader")]
    NotLeader,

    #[error("quorum not reached")]
    QuorumNotReached,

    #[error("checksum mismatch")]
    ChecksumMismatch,

    #[error("out of space")]
    OutOfSpace,

    #[error("permission denied")]
    PermissionDenied,

    #[error("file not found: {0}")]
    FileNotFound(String),

    #[error("directory not found: {0}")]
    DirectoryNotFound(String),

    #[error("file already exists: {0}")]
    FileExists(String),

    #[error("path too long")]
    PathTooLong,

    #[error("invalid path: {0}")]
    InvalidPath(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("rate limited")]
    RateLimited,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorKind {
    NonRetryable(String),
    Retryable(String),
    LeaderChanged(String),
    RateLimited(std::time::Duration),
}

impl PowerFsError {
    pub fn error_kind(&self) -> ErrorKind {
        match self {
            PowerFsError::NotLeader => ErrorKind::LeaderChanged(String::new()),
            PowerFsError::InvalidRequest(msg) => ErrorKind::NonRetryable(msg.clone()),
            PowerFsError::InvalidVolumeState(msg) => ErrorKind::NonRetryable(msg.clone()),
            PowerFsError::InvalidMasterState(msg) => ErrorKind::NonRetryable(msg.clone()),
            PowerFsError::VolumeNotFound(_) => {
                ErrorKind::NonRetryable("volume not found".to_string())
            }
            PowerFsError::NeedleNotFound(_) => {
                ErrorKind::NonRetryable("needle not found".to_string())
            }
            PowerFsError::VolumeExists(_) => {
                ErrorKind::NonRetryable("volume already exists".to_string())
            }
            PowerFsError::FileNotFound(msg) => ErrorKind::NonRetryable(msg.clone()),
            PowerFsError::DirectoryNotFound(msg) => ErrorKind::NonRetryable(msg.clone()),
            PowerFsError::FileExists(msg) => ErrorKind::NonRetryable(msg.clone()),
            PowerFsError::InvalidPath(msg) => ErrorKind::NonRetryable(msg.clone()),
            PowerFsError::PathTooLong => ErrorKind::NonRetryable("path too long".to_string()),
            PowerFsError::PermissionDenied => {
                ErrorKind::NonRetryable("permission denied".to_string())
            }
            PowerFsError::ChecksumMismatch => {
                ErrorKind::NonRetryable("checksum mismatch".to_string())
            }
            PowerFsError::OutOfSpace => ErrorKind::NonRetryable("out of space".to_string()),
            PowerFsError::Timeout => ErrorKind::Retryable("timeout".to_string()),
            PowerFsError::ConnectionRefused => {
                ErrorKind::Retryable("connection refused".to_string())
            }
            PowerFsError::RateLimited => ErrorKind::RateLimited(std::time::Duration::from_secs(5)),
            PowerFsError::QuorumNotReached => {
                ErrorKind::Retryable("quorum not reached".to_string())
            }
            PowerFsError::TonicTransport(_) => ErrorKind::Retryable("transport error".to_string()),
            PowerFsError::TonicStatus(status) => {
                let msg = status.message().to_string();
                if msg.contains("not leader") {
                    ErrorKind::LeaderChanged(String::new())
                } else {
                    ErrorKind::Retryable(msg)
                }
            }
            _ => ErrorKind::Retryable(self.to_string()),
        }
    }
}

pub type Result<T> = std::result::Result<T, PowerFsError>;
