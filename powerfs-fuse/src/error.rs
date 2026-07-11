use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum FsError {
    MasterNotConnected(String),
    MasterError(String),
    VolumeNotConnected(String),
    VolumeError(String),
    NotFound(String),
    PermissionDenied(String),
    IoError(String),
    InvalidArgument(String),
    AlreadyExists(String),
    NotDirectory(String),
    IsDirectory(String),
    NotEmpty(String),
    Other(String),
}

impl fmt::Display for FsError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FsError::MasterNotConnected(msg) => write!(f, "Master not connected: {}", msg),
            FsError::MasterError(msg) => write!(f, "Master error: {}", msg),
            FsError::VolumeNotConnected(msg) => write!(f, "Volume not connected: {}", msg),
            FsError::VolumeError(msg) => write!(f, "Volume error: {}", msg),
            FsError::NotFound(msg) => write!(f, "Not found: {}", msg),
            FsError::PermissionDenied(msg) => write!(f, "Permission denied: {}", msg),
            FsError::IoError(msg) => write!(f, "IO error: {}", msg),
            FsError::InvalidArgument(msg) => write!(f, "Invalid argument: {}", msg),
            FsError::AlreadyExists(msg) => write!(f, "Already exists: {}", msg),
            FsError::NotDirectory(msg) => write!(f, "Not a directory: {}", msg),
            FsError::IsDirectory(msg) => write!(f, "Is a directory: {}", msg),
            FsError::NotEmpty(msg) => write!(f, "Directory not empty: {}", msg),
            FsError::Other(msg) => write!(f, "Error: {}", msg),
        }
    }
}

impl From<String> for FsError {
    fn from(msg: String) -> Self {
        FsError::Other(msg)
    }
}

impl From<&str> for FsError {
    fn from(msg: &str) -> Self {
        FsError::Other(msg.to_string())
    }
}

impl FsError {
    pub fn to_errno(&self) -> i32 {
        match self {
            FsError::MasterNotConnected(_) => libc::ENOTCONN,
            FsError::MasterError(_) => libc::EIO,
            FsError::VolumeNotConnected(_) => libc::ENOTCONN,
            FsError::VolumeError(_) => libc::EIO,
            FsError::NotFound(_) => libc::ENOENT,
            FsError::PermissionDenied(_) => libc::EACCES,
            FsError::IoError(_) => libc::EIO,
            FsError::InvalidArgument(_) => libc::EINVAL,
            FsError::AlreadyExists(_) => libc::EEXIST,
            FsError::NotDirectory(_) => libc::ENOTDIR,
            FsError::IsDirectory(_) => libc::EISDIR,
            FsError::NotEmpty(_) => libc::ENOTEMPTY,
            FsError::Other(_) => libc::EIO,
        }
    }

    pub fn is_connection_error(&self) -> bool {
        matches!(
            self,
            FsError::MasterNotConnected(_) | FsError::VolumeNotConnected(_)
        )
    }
}

pub fn parse_master_error(msg: &str) -> FsError {
    if msg.contains("connection")
        || msg.contains("connect")
        || msg.contains("refused")
        || msg.contains("timeout")
        || msg.contains("not connected")
    {
        FsError::MasterNotConnected(msg.to_string())
    } else {
        FsError::MasterError(msg.to_string())
    }
}

pub fn parse_volume_error(msg: &str) -> FsError {
    if msg.contains("connection")
        || msg.contains("connect")
        || msg.contains("refused")
        || msg.contains("timeout")
        || msg.contains("not connected")
    {
        FsError::VolumeNotConnected(msg.to_string())
    } else {
        FsError::VolumeError(msg.to_string())
    }
}