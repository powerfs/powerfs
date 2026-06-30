use thiserror::Error;
use crate::types::{VolumeId, NeedleId};
use raft;

#[derive(Error, Debug)]
pub enum PowerFsError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serde json error: {0}")]
    SerdeJson(#[from] serde_json::Error),

    #[error("tonic transport error: {0}")]
    TonicTransport(#[from] tonic::transport::Error),

    #[error("tonic status error: {0}")]
    TonicStatus(#[from] tonic::Status),

    #[error("protobuf decode error: {0}")]
    ProstDecode(#[from] prost::DecodeError),

    #[error("protobuf encode error: {0}")]
    ProstEncode(#[from] prost::EncodeError),

    #[error("uuid parse error: {0}")]
    UuidParse(#[from] uuid::Error),

    #[error("address parse error: {0}")]
    AddrParse(#[from] std::net::AddrParseError),

    #[error("raft error: {0}")]
    Raft(#[from] raft::Error),

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
}

pub type Result<T> = std::result::Result<T, PowerFsError>;
