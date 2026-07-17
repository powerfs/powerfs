pub mod error;
pub mod factory;
pub mod local_fs;
pub mod migration;
#[cfg(feature = "spdk")]
pub mod spdk_backend;
pub mod types;

pub use error::StorageBackendError;
pub use factory::BackendFactory;
pub use local_fs::LocalFsBackend;
pub use migration::*;
#[cfg(feature = "spdk")]
pub use spdk_backend::SpdkBackend;
pub use types::*;

use bytes::Bytes;
use std::result::Result as StdResult;

pub type StorageResult<T> = StdResult<T, StorageBackendError>;

pub trait StorageBackend: Sync + Send + 'static {
    fn list_devices(&self) -> StorageResult<Vec<StorageDevice>>;
    fn get_device(&self, device_id: &str) -> StorageResult<StorageDevice>;
    fn get_device_health(&self, device_id: &str) -> StorageResult<DeviceHealth>;

    fn allocate_volume(
        &self,
        volume_id: u64,
        size: u64,
        preferred_device_id: Option<&str>,
    ) -> StorageResult<AllocateVolumeResult>;

    fn delete_volume(&self, volume_id: u64) -> StorageResult<()>;

    fn get_volume_info(&self, volume_id: u64) -> StorageResult<VolumeStorageInfo>;
    fn get_volume_device(&self, volume_id: u64) -> StorageResult<String>;

    fn read_needle(&self, volume_id: u64, offset: u64, size: u32) -> StorageResult<Bytes>;
    fn write_needle(&self, volume_id: u64, offset: u64, data: &[u8]) -> StorageResult<u32>;
    fn sync_volume(&self, volume_id: u64) -> StorageResult<()>;
    fn truncate_volume(&self, volume_id: u64, new_size: u64) -> StorageResult<()>;

    fn get_volumes_on_device(&self, device_id: &str) -> StorageResult<Vec<u64>>;
    fn get_volume_set(&self, device_id: &str) -> StorageResult<VolumeSet>;

    fn exclude_device(&self, device_id: &str, reason: String) -> StorageResult<()>;
    fn include_device(&self, device_id: &str) -> StorageResult<()>;
    fn is_device_excluded(&self, device_id: &str) -> bool;
    fn list_excluded_devices(&self) -> Vec<ExcludedDevice>;

    fn health_check(&self) -> StorageResult<HealthStatus>;
}
