use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageBackendError {
    #[error("Device not found: {0}")]
    DeviceNotFound(String),

    #[error("Volume not found: {0}")]
    VolumeNotFound(u64),

    #[error("Volume already exists: {0}")]
    VolumeExists(u64),

    #[error("Device full: {device_id}, requested: {requested}, available: {available}")]
    DeviceFull {
        device_id: String,
        requested: u64,
        available: u64,
    },

    #[error("No available device with enough space: requested {0} bytes")]
    NoAvailableDevice(u64),

    #[error("Device already excluded: {0}")]
    DeviceAlreadyExcluded(String),

    #[error("Device not excluded: {0}")]
    DeviceNotExcluded(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("SPDK IO error: {0}")]
    SpdkIoError(String),

    #[error("Checksum mismatch")]
    ChecksumMismatch,

    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    #[error("Backend error: {0}")]
    BackendError(String),
}
