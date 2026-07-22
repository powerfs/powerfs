use crate::storage_backend::*;
use bytes::Bytes;
use chrono::Utc;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::RwLock;

type Result<T> = StorageResult<T>;

const VOLUME_ALIGNMENT: u64 = 4096;

/// 1MB needle block alignment, used by the volume server for write alignment.
pub const NEEDLE_BLOCK_SIZE: usize = 1024 * 1024;

/// Query the underlying filesystem capacity via statvfs.
///
/// Returns `(total_bytes, available_bytes)`. On failure, falls back to a
/// conservative 100GB/90GB default so the volume server can still boot.
fn get_fs_capacity(path: &str) -> (u64, u64) {
    use nix::sys::statvfs;
    match statvfs::statvfs(std::path::Path::new(path)) {
        Ok(stat) => {
            let block_size = stat.fragment_size();
            let total = stat.blocks() * block_size;
            let free = stat.blocks_available() * block_size;
            (total, free)
        }
        Err(_) => (100 * 1024 * 1024 * 1024, 90 * 1024 * 1024 * 1024),
    }
}

struct VolumeMeta {
    volume_id: u64,
    device_id: String,
    total_size: u64,
    used_size: u64,
    physical_offset: u64,
    state: VolumeState,
    data_file: PathBuf,
}

pub struct LocalFsBackend {
    devices: RwLock<HashMap<String, DeviceState>>,
    volumes: RwLock<HashMap<u64, VolumeMeta>>,
    excluded_devices: RwLock<HashMap<String, ExcludedDevice>>,
    base_path: PathBuf,
    node_id: String,
    _checksum_algo: ChecksumAlgorithm,
}

struct DeviceState {
    info: StorageDevice,
    free_offset: u64,
    _data_file: PathBuf,
}

impl LocalFsBackend {
    pub fn new(
        base_path: &str,
        node_id: &str,
        device_name: &str,
        device_capacity: Option<u64>,
    ) -> Result<Self> {
        let base_path = PathBuf::from(base_path);
        std::fs::create_dir_all(&base_path)?;

        let device_id = format!("local_fs_{}", device_name);

        // Capacity is configurable; fall back to the real filesystem capacity
        // reported by statvfs. No device backing file is preallocated anymore:
        // volumes are sparse files and actual used space is read from statvfs.
        let path_str = base_path.to_str().unwrap_or(".");
        let (fs_total, fs_free) = get_fs_capacity(path_str);
        let total_capacity = device_capacity.unwrap_or(fs_total);
        let used_space = fs_total.saturating_sub(fs_free);
        let free_space = fs_free;

        let device = StorageDevice {
            device_id: device_id.clone(),
            device_type: DeviceType::LocalFile,
            total_capacity,
            used_space,
            free_space,
            location: DeviceLocation {
                node_id: node_id.to_string(),
                device_id: device_id.clone(),
                zone: "default".to_string(),
                rack: None,
                data_center: None,
            },
            status: DeviceStatus::Online,
        };

        let mut devices = HashMap::new();
        devices.insert(
            device_id.clone(),
            DeviceState {
                info: device,
                free_offset: 0,
                _data_file: base_path.clone(),
            },
        );

        Ok(LocalFsBackend {
            devices: RwLock::new(devices),
            volumes: RwLock::new(HashMap::new()),
            excluded_devices: RwLock::new(HashMap::new()),
            base_path,
            node_id: node_id.to_string(),
            _checksum_algo: ChecksumAlgorithm::default(),
        })
    }

    pub fn add_device(&self, device_name: &str, device_capacity: Option<u64>) -> Result<String> {
        let device_id = format!("local_fs_{}", device_name);

        let path_str = self.base_path.to_str().unwrap_or(".");
        let (fs_total, fs_free) = get_fs_capacity(path_str);
        let total_capacity = device_capacity.unwrap_or(fs_total);
        let used_space = fs_total.saturating_sub(fs_free);
        let free_space = fs_free;

        let device = StorageDevice {
            device_id: device_id.clone(),
            device_type: DeviceType::LocalFile,
            total_capacity,
            used_space,
            free_space,
            location: DeviceLocation {
                node_id: self.node_id.clone(),
                device_id: device_id.clone(),
                zone: "default".to_string(),
                rack: None,
                data_center: None,
            },
            status: DeviceStatus::Online,
        };

        let mut devices = self.devices.write().unwrap();
        devices.insert(
            device_id.clone(),
            DeviceState {
                info: device,
                free_offset: 0,
                _data_file: self.base_path.clone(),
            },
        );

        Ok(device_id)
    }

    fn align_up(value: u64, align: u64) -> u64 {
        if value.is_multiple_of(align) {
            value
        } else {
            value + align - (value % align)
        }
    }
}

impl StorageBackend for LocalFsBackend {
    fn list_devices(&self) -> Result<Vec<StorageDevice>> {
        let devices = self.devices.read().unwrap();
        Ok(devices.values().map(|d| d.info.clone()).collect())
    }

    fn get_device(&self, device_id: &str) -> Result<StorageDevice> {
        let devices = self.devices.read().unwrap();
        devices
            .get(device_id)
            .map(|d| d.info.clone())
            .ok_or_else(|| StorageBackendError::DeviceNotFound(device_id.to_string()))
    }

    fn get_device_health(&self, device_id: &str) -> Result<DeviceHealth> {
        let device = self.get_device(device_id)?;
        let utilization = if device.total_capacity > 0 {
            (device.used_space as f64 / device.total_capacity as f64) * 100.0
        } else {
            0.0
        };

        let health_status = if utilization >= 95.0 {
            HealthStatus::Critical
        } else if utilization >= 85.0 {
            HealthStatus::Warning
        } else {
            HealthStatus::Healthy
        };

        Ok(DeviceHealth {
            device_id: device.device_id.clone(),
            device_type: device.device_type,
            capacity_bytes: device.total_capacity,
            used_bytes: device.used_space,
            available_bytes: device.free_space,
            utilization_percent: utilization,
            read_iops: 0.0,
            write_iops: 0.0,
            read_bandwidth_bps: 0,
            write_bandwidth_bps: 0,
            avg_latency_us: 0.0,
            p99_latency_us: 0.0,
            smart_info: None,
            health_status,
            last_checked_at: Utc::now(),
        })
    }

    fn allocate_volume(
        &self,
        volume_id: u64,
        size: u64,
        preferred_device_id: Option<&str>,
    ) -> Result<AllocateVolumeResult> {
        {
            let volumes = self.volumes.read().unwrap();
            if volumes.contains_key(&volume_id) {
                return Err(StorageBackendError::VolumeExists(volume_id));
            }
        }

        let aligned_size = Self::align_up(size, VOLUME_ALIGNMENT);

        let mut devices = self.devices.write().unwrap();
        let excluded = self.excluded_devices.read().unwrap();

        let candidate_device_ids: Vec<String> = if let Some(pref) = preferred_device_id {
            if excluded.contains_key(pref) {
                return Err(StorageBackendError::DeviceAlreadyExcluded(pref.to_string()));
            }
            if !devices.contains_key(pref) {
                return Err(StorageBackendError::DeviceNotFound(pref.to_string()));
            }
            vec![pref.to_string()]
        } else {
            devices
                .keys()
                .filter(|id| !excluded.contains_key(id.as_str()))
                .cloned()
                .collect()
        };

        let mut selected_device: Option<String> = None;
        let mut alloc_offset: u64 = 0;

        // Volume 是稀疏文件，size 只是逻辑上限，不预占物理空间。
        // 选择第一个可用设备即可，不做 size 检查。
        for device_id in &candidate_device_ids {
            if let Some(device) = devices.get_mut(device_id) {
                let offset = Self::align_up(device.free_offset, VOLUME_ALIGNMENT);
                alloc_offset = offset;
                selected_device = Some(device_id.clone());
                break;
            }
        }

        let device_id = selected_device.ok_or(StorageBackendError::NoAvailableDevice(size))?;

        // Volumes are sparse files: do NOT debit the device's used/free space.
        // Actual disk usage is observed via statvfs rather than tracked here.
        if let Some(device) = devices.get_mut(&device_id) {
            device.free_offset = alloc_offset + aligned_size;
        }

        let data_file = self.base_path.join(format!("volume_{}.dat", volume_id));
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&data_file)?;
        // Sparse file: set the logical length without allocating blocks.
        file.set_len(size)?;

        let volume_meta = VolumeMeta {
            volume_id,
            device_id: device_id.clone(),
            total_size: size,
            used_size: 0,
            physical_offset: alloc_offset,
            state: VolumeState::Active,
            data_file,
        };

        let mut volumes = self.volumes.write().unwrap();
        volumes.insert(volume_id, volume_meta);

        Ok(AllocateVolumeResult {
            volume_id,
            device_id,
            allocated_size: aligned_size,
            volume_offset: alloc_offset,
        })
    }

    fn delete_volume(&self, volume_id: u64) -> Result<()> {
        let mut volumes = self.volumes.write().unwrap();
        let volume_meta = volumes
            .remove(&volume_id)
            .ok_or(StorageBackendError::VolumeNotFound(volume_id))?;

        // statvfs remains the source of truth for device used/free space, so
        // there is nothing to restore here — just drop the volume file.
        if volume_meta.data_file.exists() {
            std::fs::remove_file(&volume_meta.data_file)?;
        }

        Ok(())
    }

    fn get_volume_info(&self, volume_id: u64) -> Result<VolumeStorageInfo> {
        let volumes = self.volumes.read().unwrap();
        let volume = volumes
            .get(&volume_id)
            .ok_or(StorageBackendError::VolumeNotFound(volume_id))?;

        Ok(VolumeStorageInfo {
            volume_id: volume.volume_id,
            device_id: volume.device_id.clone(),
            total_size: volume.total_size,
            used_size: volume.used_size,
            physical_offset: volume.physical_offset,
            volume_state: volume.state,
        })
    }

    fn get_volume_device(&self, volume_id: u64) -> Result<String> {
        let volumes = self.volumes.read().unwrap();
        volumes
            .get(&volume_id)
            .map(|v| v.device_id.clone())
            .ok_or(StorageBackendError::VolumeNotFound(volume_id))
    }

    fn read_needle(&self, volume_id: u64, offset: u64, size: u32) -> Result<Bytes> {
        let volumes = self.volumes.read().unwrap();
        let volume = volumes
            .get(&volume_id)
            .ok_or(StorageBackendError::VolumeNotFound(volume_id))?;

        if offset + size as u64 > volume.total_size {
            return Err(StorageBackendError::InvalidOperation(
                "read beyond volume size".to_string(),
            ));
        }

        let mut file = File::open(&volume.data_file)?;
        file.seek(SeekFrom::Start(offset))?;

        let mut buf = vec![0u8; size as usize];
        file.read_exact(&mut buf)?;

        Ok(Bytes::from(buf))
    }

    fn write_needle(&self, volume_id: u64, offset: u64, data: &[u8]) -> Result<u32> {
        let data_file = {
            let volumes = self.volumes.read().unwrap();
            let volume = volumes
                .get(&volume_id)
                .ok_or(StorageBackendError::VolumeNotFound(volume_id))?;

            if offset + data.len() as u64 > volume.total_size {
                return Err(StorageBackendError::InvalidOperation(
                    "write beyond volume size".to_string(),
                ));
            }

            volume.data_file.clone()
        };

        let mut file = OpenOptions::new().read(true).write(true).open(&data_file)?;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(data)?;
        file.sync_data()?;

        let mut volumes = self.volumes.write().unwrap();
        if let Some(vol) = volumes.get_mut(&volume_id) {
            let new_used = offset + data.len() as u64;
            if new_used > vol.used_size {
                vol.used_size = new_used;
            }
        }

        Ok(data.len() as u32)
    }

    fn sync_volume(&self, volume_id: u64) -> Result<()> {
        let volumes = self.volumes.read().unwrap();
        let volume = volumes
            .get(&volume_id)
            .ok_or(StorageBackendError::VolumeNotFound(volume_id))?;

        let file = File::open(&volume.data_file)?;
        file.sync_all()?;
        Ok(())
    }

    fn truncate_volume(&self, volume_id: u64, new_size: u64) -> Result<()> {
        let data_file = {
            let mut volumes = self.volumes.write().unwrap();
            let volume = volumes
                .get_mut(&volume_id)
                .ok_or(StorageBackendError::VolumeNotFound(volume_id))?;

            if new_size > volume.total_size {
                return Err(StorageBackendError::InvalidOperation(
                    "cannot truncate to larger size".to_string(),
                ));
            }

            volume.used_size = new_size.min(volume.used_size);
            volume.data_file.clone()
        };

        let file = OpenOptions::new().write(true).open(&data_file)?;
        file.set_len(new_size)?;
        file.sync_all()?;
        Ok(())
    }

    fn get_volumes_on_device(&self, device_id: &str) -> Result<Vec<u64>> {
        let _ = self.get_device(device_id)?;
        let volumes = self.volumes.read().unwrap();
        let vols: Vec<u64> = volumes
            .values()
            .filter(|v| v.device_id == device_id)
            .map(|v| v.volume_id)
            .collect();
        Ok(vols)
    }

    fn get_volume_set(&self, device_id: &str) -> Result<VolumeSet> {
        let device = self.get_device(device_id)?;
        let vols = self.get_volumes_on_device(device_id)?;

        let health = self.get_device_health(device_id)?;

        Ok(VolumeSet {
            device_id: device_id.to_string(),
            volumes: vols,
            total_capacity: device.total_capacity,
            total_used: device.used_space,
            total_free: device.free_space,
            health_status: health.health_status,
        })
    }

    fn exclude_device(&self, device_id: &str, reason: String) -> Result<()> {
        let _ = self.get_device(device_id)?;

        let mut excluded = self.excluded_devices.write().unwrap();
        if excluded.contains_key(device_id) {
            return Err(StorageBackendError::DeviceAlreadyExcluded(
                device_id.to_string(),
            ));
        }

        excluded.insert(
            device_id.to_string(),
            ExcludedDevice {
                device_id: device_id.to_string(),
                reason,
                excluded_at: Utc::now(),
                excluded_by: "system".to_string(),
                auto_drain: false,
            },
        );

        let mut devices = self.devices.write().unwrap();
        if let Some(device) = devices.get_mut(device_id) {
            device.info.status = DeviceStatus::Excluded;
        }

        Ok(())
    }

    fn include_device(&self, device_id: &str) -> Result<()> {
        let mut excluded = self.excluded_devices.write().unwrap();
        if excluded.remove(device_id).is_none() {
            return Err(StorageBackendError::DeviceNotExcluded(
                device_id.to_string(),
            ));
        }

        let mut devices = self.devices.write().unwrap();
        if let Some(device) = devices.get_mut(device_id) {
            device.info.status = DeviceStatus::Online;
        }

        Ok(())
    }

    fn is_device_excluded(&self, device_id: &str) -> bool {
        self.excluded_devices
            .read()
            .unwrap()
            .contains_key(device_id)
    }

    fn list_excluded_devices(&self) -> Vec<ExcludedDevice> {
        self.excluded_devices
            .read()
            .unwrap()
            .values()
            .cloned()
            .collect()
    }

    fn health_check(&self) -> StorageResult<HealthStatus> {
        let devices = self.devices.read().unwrap();
        let mut overall = HealthStatus::Healthy;

        for device in devices.values() {
            let util = if device.info.total_capacity > 0 {
                (device.info.used_space as f64 / device.info.total_capacity as f64) * 100.0
            } else {
                0.0
            };

            let status = if util >= 95.0 {
                HealthStatus::Critical
            } else if util >= 85.0 {
                HealthStatus::Warning
            } else {
                HealthStatus::Healthy
            };

            overall = match (overall, status) {
                (HealthStatus::Critical, _) | (_, HealthStatus::Critical) => HealthStatus::Critical,
                (HealthStatus::Warning, _) | (_, HealthStatus::Warning) => HealthStatus::Warning,
                _ => HealthStatus::Healthy,
            };
        }

        Ok(overall)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn create_test_backend() -> (LocalFsBackend, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let backend = LocalFsBackend::new(path, "test-node", "dev0", None).unwrap();
        (backend, dir)
    }

    #[test]
    fn test_list_devices_empty() {
        let dir = tempdir().unwrap();
        let backend =
            LocalFsBackend::new(dir.path().to_str().unwrap(), "test-node", "dev0", None).unwrap();
        let devices = backend.list_devices().unwrap();
        assert_eq!(devices.len(), 1);
        assert_eq!(devices[0].device_type, DeviceType::LocalFile);
    }

    #[test]
    fn test_get_device() {
        let (backend, _dir) = create_test_backend();
        let devices = backend.list_devices().unwrap();
        let device_id = &devices[0].device_id;
        let device = backend.get_device(device_id).unwrap();
        assert_eq!(device.device_id, *device_id);
    }

    #[test]
    fn test_get_device_not_found() {
        let (backend, _dir) = create_test_backend();
        let result = backend.get_device("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_allocate_volume() {
        let (backend, _dir) = create_test_backend();
        let result = backend.allocate_volume(1, 10 * 1024 * 1024, None).unwrap();
        assert_eq!(result.volume_id, 1);
        assert_eq!(result.allocated_size, 10 * 1024 * 1024);
    }

    #[test]
    fn test_allocate_volume_exists() {
        let (backend, _dir) = create_test_backend();
        backend.allocate_volume(1, 10 * 1024 * 1024, None).unwrap();
        let result = backend.allocate_volume(1, 5 * 1024 * 1024, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_delete_volume() {
        let (backend, _dir) = create_test_backend();
        backend.allocate_volume(1, 10 * 1024 * 1024, None).unwrap();
        let info = backend.get_volume_info(1).unwrap();
        assert_eq!(info.volume_state, VolumeState::Active);
        backend.delete_volume(1).unwrap();
        let result = backend.get_volume_info(1);
        assert!(result.is_err());
    }

    #[test]
    fn test_read_write_needle() {
        let (backend, _dir) = create_test_backend();
        backend.allocate_volume(1, 10 * 1024 * 1024, None).unwrap();

        let data = b"hello world";
        let written = backend.write_needle(1, 0, data).unwrap();
        assert_eq!(written, data.len() as u32);

        let read = backend.read_needle(1, 0, data.len() as u32).unwrap();
        assert_eq!(read.as_ref(), data);
    }

    #[test]
    fn test_read_out_of_bounds() {
        let (backend, _dir) = create_test_backend();
        backend.allocate_volume(1, 1024, None).unwrap();
        let result = backend.read_needle(1, 0, 2048);
        assert!(result.is_err());
    }

    #[test]
    fn test_write_out_of_bounds() {
        let (backend, _dir) = create_test_backend();
        backend.allocate_volume(1, 1024, None).unwrap();
        let result = backend.write_needle(1, 0, &[0u8; 2048]);
        assert!(result.is_err());
    }

    #[test]
    fn test_exclude_device() {
        let (backend, _dir) = create_test_backend();
        let devices = backend.list_devices().unwrap();
        let device_id = &devices[0].device_id;

        assert!(!backend.is_device_excluded(device_id));
        backend
            .exclude_device(device_id, "test reason".to_string())
            .unwrap();
        assert!(backend.is_device_excluded(device_id));

        let excluded = backend.list_excluded_devices();
        assert_eq!(excluded.len(), 1);
        assert_eq!(excluded[0].reason, "test reason");
    }

    #[test]
    fn test_include_device() {
        let (backend, _dir) = create_test_backend();
        let devices = backend.list_devices().unwrap();
        let device_id = &devices[0].device_id;

        backend
            .exclude_device(device_id, "test".to_string())
            .unwrap();
        assert!(backend.is_device_excluded(device_id));

        backend.include_device(device_id).unwrap();
        assert!(!backend.is_device_excluded(device_id));
    }

    #[test]
    fn test_volumes_on_device() {
        let (backend, _dir) = create_test_backend();
        let devices = backend.list_devices().unwrap();
        let device_id = &devices[0].device_id;

        backend.allocate_volume(1, 1024, None).unwrap();
        backend.allocate_volume(2, 2048, None).unwrap();

        let vols = backend.get_volumes_on_device(device_id).unwrap();
        assert_eq!(vols.len(), 2);
    }

    #[test]
    fn test_volume_set() {
        let (backend, _dir) = create_test_backend();
        let devices = backend.list_devices().unwrap();
        let device_id = &devices[0].device_id;

        backend.allocate_volume(1, 1024 * 1024, None).unwrap();
        backend.allocate_volume(2, 2 * 1024 * 1024, None).unwrap();

        let vs = backend.get_volume_set(device_id).unwrap();
        assert_eq!(vs.volumes.len(), 2);
        assert!(vs.total_used > 0);
    }

    #[test]
    fn test_health_check() {
        let (backend, _dir) = create_test_backend();
        let status = backend.health_check().unwrap();
        assert_eq!(status, HealthStatus::Healthy);
    }

    #[test]
    fn test_device_health() {
        let (backend, _dir) = create_test_backend();
        let devices = backend.list_devices().unwrap();
        let device_id = &devices[0].device_id;

        let health = backend.get_device_health(device_id).unwrap();
        // used_space now reflects real filesystem usage reported by statvfs,
        // so utilization is host-dependent. Validate the invariants only.
        assert!(health.utilization_percent >= 0.0 && health.utilization_percent <= 100.0);
        assert!(health.available_bytes <= health.capacity_bytes);
    }

    #[test]
    fn test_sync_volume() {
        let (backend, _dir) = create_test_backend();
        backend.allocate_volume(1, 1024, None).unwrap();
        backend.write_needle(1, 0, b"test").unwrap();
        backend.sync_volume(1).unwrap();
    }

    #[test]
    fn test_get_volume_device() {
        let (backend, _dir) = create_test_backend();
        backend.allocate_volume(1, 1024, None).unwrap();
        let device_id = backend.get_volume_device(1).unwrap();
        assert!(!device_id.is_empty());
    }

    #[test]
    fn test_add_device() {
        let (backend, _dir) = create_test_backend();
        let _new_id = backend.add_device("dev1", Some(50 * 1024 * 1024)).unwrap();
        let devices = backend.list_devices().unwrap();
        assert_eq!(devices.len(), 2);
    }
}
