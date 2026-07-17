#![cfg(feature = "spdk")]

use crate::storage_backend::*;
use bytes::Bytes;
use chrono::Utc;
use libc::{c_char, c_int, c_uint, c_ulonglong, size_t};
use std::collections::HashMap;
use std::ffi::CString;
use std::sync::RwLock;

type Result<T> = StorageResult<T>;

const VOLUME_ALIGNMENT: u64 = 4096;

#[repr(C)]
struct SpdkSegHandle {
    _private: [u8; 0],
}

extern "C" {
    fn spdk_initialize_env() -> bool;
    fn spdk_cleanup();
    fn spdk_open_segment(tr_str: *const c_char) -> *mut SpdkSegHandle;
    fn spdk_close_segment(seg: *mut SpdkSegHandle);
    fn spdk_get_block_size(seg: *mut SpdkSegHandle) -> c_uint;
    fn spdk_read(
        seg: *mut SpdkSegHandle,
        buf: *mut u8,
        lba: c_ulonglong,
        lba_count: c_uint,
    ) -> c_int;
    fn spdk_write(
        seg: *mut SpdkSegHandle,
        buf: *const u8,
        lba: c_ulonglong,
        lba_count: c_uint,
    ) -> c_int;
    #[allow(dead_code)]
    fn spdk_probe_segment(
        tr_str: *const c_char,
        timeout_ms: c_uint,
        error_reason: *mut c_char,
        error_reason_buf_size: size_t,
    ) -> bool;
    fn spdk_get_ns_size(seg: *mut SpdkSegHandle) -> c_ulonglong;
}

struct VolumeMeta {
    volume_id: u64,
    device_id: String,
    total_size: u64,
    used_size: u64,
    physical_offset: u64,
    state: VolumeState,
}

struct SpdkDevice {
    info: StorageDevice,
    free_offset: u64,
    seg_handle: *mut SpdkSegHandle,
    block_size: u32,
}

unsafe impl Send for SpdkDevice {}
unsafe impl Sync for SpdkDevice {}

pub struct SpdkBackend {
    devices: RwLock<HashMap<String, SpdkDevice>>,
    volumes: RwLock<HashMap<u64, VolumeMeta>>,
    excluded_devices: RwLock<HashMap<String, ExcludedDevice>>,
    node_id: String,
    _checksum_algo: ChecksumAlgorithm,
}

impl SpdkBackend {
    pub fn new(node_id: &str) -> Result<Self> {
        let _ = unsafe { spdk_initialize_env() };

        Ok(SpdkBackend {
            devices: RwLock::new(HashMap::new()),
            volumes: RwLock::new(HashMap::new()),
            excluded_devices: RwLock::new(HashMap::new()),
            node_id: node_id.to_string(),
            _checksum_algo: ChecksumAlgorithm::default(),
        })
    }

    pub fn add_device(
        &self,
        device_name: &str,
        tr_str: &str,
        total_capacity: Option<u64>,
    ) -> Result<String> {
        let device_id = format!("spdk_nvme_{}", device_name);

        let tr_cstr = CString::new(tr_str).map_err(|_| {
            StorageBackendError::InvalidOperation("invalid transport string".to_string())
        })?;

        let seg_handle = unsafe { spdk_open_segment(tr_cstr.as_ptr()) };
        if seg_handle.is_null() {
            return Err(StorageBackendError::InvalidOperation(
                "failed to open SPDK segment".to_string(),
            ));
        }

        let block_size = unsafe { spdk_get_block_size(seg_handle) };
        if block_size == 0 {
            unsafe { spdk_close_segment(seg_handle) };
            return Err(StorageBackendError::InvalidOperation(
                "invalid block size".to_string(),
            ));
        }

        let ns_size = unsafe { spdk_get_ns_size(seg_handle) };
        let actual_capacity = total_capacity.unwrap_or(ns_size);

        let used_space = 0u64;
        let free_space = actual_capacity - used_space;

        let device = StorageDevice {
            device_id: device_id.clone(),
            device_type: DeviceType::SpdkNvme,
            total_capacity: actual_capacity,
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
            SpdkDevice {
                info: device,
                free_offset: 0,
                seg_handle,
                block_size,
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

impl Drop for SpdkBackend {
    fn drop(&mut self) {
        unsafe {
            spdk_cleanup();
        }
    }
}

impl StorageBackend for SpdkBackend {
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

        for device_id in &candidate_device_ids {
            if let Some(device) = devices.get_mut(device_id) {
                if device.info.free_space >= aligned_size {
                    let offset = Self::align_up(device.free_offset, VOLUME_ALIGNMENT);
                    if offset + aligned_size <= device.info.total_capacity {
                        alloc_offset = offset;
                        selected_device = Some(device_id.clone());
                        break;
                    }
                }
            }
        }

        let device_id =
            selected_device.ok_or(StorageBackendError::NoAvailableDevice(aligned_size))?;

        if let Some(device) = devices.get_mut(&device_id) {
            device.free_offset = alloc_offset + aligned_size;
            device.info.used_space += aligned_size;
            device.info.free_space -= aligned_size;
        }

        let volume_meta = VolumeMeta {
            volume_id,
            device_id: device_id.clone(),
            total_size: size,
            used_size: 0,
            physical_offset: alloc_offset,
            state: VolumeState::Active,
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

        let mut devices = self.devices.write().unwrap();
        if let Some(device) = devices.get_mut(&volume_meta.device_id) {
            device.info.used_space = device
                .info
                .used_space
                .saturating_sub(volume_meta.total_size);
            device.info.free_space = device
                .info
                .free_space
                .saturating_add(volume_meta.total_size);
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

        let devices = self.devices.read().unwrap();
        let device = devices
            .get(&volume.device_id)
            .ok_or(StorageBackendError::DeviceNotFound(
                volume.device_id.clone(),
            ))?;

        let physical_offset = volume.physical_offset + offset;
        let block_size = device.block_size as u64;
        let lba = physical_offset / block_size;
        let lba_count = (size as u64).div_ceil(block_size) as u32;

        let mut buf = vec![0u8; (lba_count as u64 * block_size) as usize];
        let ret = unsafe {
            spdk_read(
                device.seg_handle,
                buf.as_mut_ptr(),
                lba as c_ulonglong,
                lba_count,
            )
        };

        if ret != 0 {
            return Err(StorageBackendError::SpdkIoError(format!(
                "SPDK read failed: {}",
                ret
            )));
        }

        let data_offset = (physical_offset % block_size) as usize;
        let result = buf[data_offset..data_offset + size as usize].to_vec();

        Ok(Bytes::from(result))
    }

    fn write_needle(&self, volume_id: u64, offset: u64, data: &[u8]) -> Result<u32> {
        let volumes = self.volumes.read().unwrap();
        let volume = volumes
            .get(&volume_id)
            .ok_or(StorageBackendError::VolumeNotFound(volume_id))?;

        if offset + data.len() as u64 > volume.total_size {
            return Err(StorageBackendError::InvalidOperation(
                "write beyond volume size".to_string(),
            ));
        }

        let devices = self.devices.read().unwrap();
        let device = devices
            .get(&volume.device_id)
            .ok_or(StorageBackendError::DeviceNotFound(
                volume.device_id.clone(),
            ))?;

        let physical_offset = volume.physical_offset + offset;
        let block_size = device.block_size as u64;
        let lba = physical_offset / block_size;
        let lba_count = (data.len() as u64).div_ceil(block_size) as u32;

        let aligned_size = (lba_count as u64 * block_size) as usize;
        let mut aligned_buf = vec![0u8; aligned_size];
        let data_offset = (physical_offset % block_size) as usize;
        aligned_buf[data_offset..data_offset + data.len()].copy_from_slice(data);

        let ret = unsafe {
            spdk_write(
                device.seg_handle,
                aligned_buf.as_ptr(),
                lba as c_ulonglong,
                lba_count,
            )
        };

        if ret != 0 {
            return Err(StorageBackendError::SpdkIoError(format!(
                "SPDK write failed: {}",
                ret
            )));
        }

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
        let _volume = volumes
            .get(&volume_id)
            .ok_or(StorageBackendError::VolumeNotFound(volume_id))?;

        Ok(())
    }

    fn truncate_volume(&self, volume_id: u64, new_size: u64) -> Result<()> {
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

    #[test]
    fn test_spdk_backend_new() {
        let result = SpdkBackend::new("test-node");
        assert!(result.is_ok());
        let backend = result.unwrap();
        let devices = backend.list_devices().unwrap();
        assert_eq!(devices.len(), 0);
    }

    #[test]
    fn test_spdk_backend_empty_list_devices() {
        let result = SpdkBackend::new("test-node");
        if result.is_ok() {
            let backend = result.unwrap();
            let devices = backend.list_devices().unwrap();
            assert_eq!(devices.len(), 0);
        }
    }

    #[test]
    fn test_spdk_backend_get_device_not_found() {
        let result = SpdkBackend::new("test-node");
        if result.is_ok() {
            let backend = result.unwrap();
            let result = backend.get_device("nonexistent");
            assert!(result.is_err());
        }
    }

    #[test]
    fn test_spdk_backend_allocate_volume_no_devices() {
        let result = SpdkBackend::new("test-node");
        if result.is_ok() {
            let backend = result.unwrap();
            let result = backend.allocate_volume(1, 1024, None);
            assert!(result.is_err());
        }
    }

    #[test]
    fn test_spdk_backend_exclude_device_not_found() {
        let result = SpdkBackend::new("test-node");
        if result.is_ok() {
            let backend = result.unwrap();
            let result = backend.exclude_device("nonexistent", "test".to_string());
            assert!(result.is_err());
        }
    }

    #[test]
    fn test_spdk_backend_add_device() {
        let result = SpdkBackend::new("test-node");
        if result.is_ok() {
            let backend = result.unwrap();
            let device_id = backend.add_device("dev0", "trtype:tcp traddr:127.0.0.1 trsvcid:4420 subnqn:nqn.2016-06.io.spdk:cnode1 ns:1", None).unwrap();
            assert!(device_id.starts_with("spdk_nvme_"));

            let devices = backend.list_devices().unwrap();
            assert_eq!(devices.len(), 1);
            assert_eq!(devices[0].device_type, DeviceType::SpdkNvme);
        }
    }
}
