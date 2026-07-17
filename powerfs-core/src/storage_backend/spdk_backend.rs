use crate::checksum::ChecksumAlgorithm;
use crate::error::{PowerFsError, StorageBackendError};
use crate::Result;
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_ulonglong, c_void};
use std::sync::{Arc, RwLock};

type SpdkBdev = c_void;
type SpdkBdevDesc = c_void;
type SpdkIoChannel = c_void;

struct SpdkEnvOpts {
    name: [c_char; 64],
    config_file: *const c_char,
    master_core: c_int,
    num_cores: c_int,
    reactor_mask: u64,
    mem_channel: c_int,
    mem_size: u32,
    hugepage_single_segments: u8,
    hugepage_dir: [c_char; 256],
    unlink: u8,
    log_level: [c_char; 64],
    log_file: [c_char; 256],
    panic_on_abort: u8,
    enable_trace: u8,
    trace_file: [c_char; 256],
    enable_core_dump: u8,
    core_dump_dir: [c_char; 256],
}

extern "C" {
    fn spdk_env_opts_init(opts: *mut SpdkEnvOpts);
    fn spdk_env_init(opts: *const SpdkEnvOpts) -> c_int;
    fn spdk_env_fini();
    fn spdk_subsystem_init() -> c_int;
    fn spdk_subsystem_init_from_json_config(config_file: *const c_char) -> c_int;
    fn spdk_subsystem_fini();
    fn spdk_bdev_first() -> *mut SpdkBdev;
    fn spdk_bdev_next(bdev: *mut SpdkBdev) -> *mut SpdkBdev;
    fn spdk_bdev_get_name(bdev: *mut SpdkBdev) -> *const c_char;
    fn spdk_bdev_open_ext(
        bdev_name: *const c_char,
        write: bool,
        event_cb: *const c_void,
        eventctx: *mut c_void,
        desc: *mut *mut SpdkBdevDesc,
    ) -> c_int;
    fn spdk_bdev_close(desc: *mut SpdkBdevDesc);
    fn spdk_bdev_get_block_size(desc: *mut SpdkBdevDesc) -> u32;
    fn spdk_bdev_get_num_blocks(desc: *mut SpdkBdevDesc) -> u64;
    fn spdk_bdev_get_io_channel(desc: *mut SpdkBdevDesc) -> *mut SpdkIoChannel;
    fn spdk_put_io_channel(ch: *mut SpdkIoChannel);
    fn spdk_bdev_read_blocks(
        desc: *mut SpdkBdevDesc,
        ch: *mut SpdkIoChannel,
        buf: *mut u8,
        lba: c_ulonglong,
        lba_count: u32,
        cb: *const c_void,
        cb_arg: *mut c_void,
    ) -> c_int;
    fn spdk_bdev_write_blocks(
        desc: *mut SpdkBdevDesc,
        ch: *mut SpdkIoChannel,
        buf: *mut u8,
        lba: c_ulonglong,
        lba_count: u32,
        cb: *const c_void,
        cb_arg: *mut c_void,
    ) -> c_int;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VolumeState {
    Online,
    Offline,
    Degraded,
}

pub struct StorageDevice {
    pub device_id: String,
    pub name: String,
    pub total_capacity: u64,
    pub used_space: u64,
    pub free_space: u64,
    pub status: String,
    pub block_size: u32,
}

struct ExcludedDevice {
    device_id: String,
    reason: String,
    timestamp: u64,
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
    desc: *mut SpdkBdevDesc,
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
        let mut opts: SpdkEnvOpts = unsafe { std::mem::zeroed() };
        unsafe {
            spdk_env_opts_init(&mut opts);
        }

        let ret = unsafe { spdk_env_init(&opts) };
        if ret != 0 {
            return Err(StorageBackendError::InvalidOperation(
                format!("failed to initialize SPDK environment: {}", ret).to_string(),
            ));
        }

        let ret = unsafe { spdk_subsystem_init() };
        if ret != 0 {
            unsafe { spdk_env_fini() };
            return Err(StorageBackendError::InvalidOperation(
                format!("failed to initialize SPDK subsystems: {}", ret).to_string(),
            ));
        }

        Ok(SpdkBackend {
            devices: RwLock::new(HashMap::new()),
            volumes: RwLock::new(HashMap::new()),
            excluded_devices: RwLock::new(HashMap::new()),
            node_id: node_id.to_string(),
            _checksum_algo: ChecksumAlgorithm::default(),
        })
    }

    pub fn attach_nvme_controller(&self, name: &str, traddr: &str) -> Result<()> {
        let config_json = format!(
            r#"
{{
  "subsystems": [
    {{
      "subsystem": "bdev",
      "config": [
        {{
          "method": "bdev_nvme_attach_controller",
          "params": {{
            "name": "{}",
            "trtype": "PCIe",
            "traddr": "{}"
          }}
        }}
      ]
    }}
  ]
}}
"#,
            name, traddr
        );

        let config_file = "/tmp/powerfs_spdk_config.json";
        std::fs::write(config_file, config_json)
            .map_err(|e| StorageBackendError::InvalidOperation(format!("failed to write config: {}", e)))?;

        let config_cstr = CString::new(config_file)
            .map_err(|e| StorageBackendError::InvalidOperation(format!("invalid config path: {}", e)))?;

        let ret = unsafe { spdk_subsystem_init_from_json_config(config_cstr.as_ptr()) };
        if ret != 0 {
            return Err(StorageBackendError::InvalidOperation(
                format!("failed to attach NVMe controller {} at {}: {}", name, traddr, ret).to_string(),
            ));
        }

        Ok(())
    }

    pub fn add_device(
        &self,
        device_name: &str,
        bdev_name: &str,
        total_capacity: Option<u64>,
    ) -> Result<String> {
        let device_id = format!("spdk_nvme_{}", device_name);

        let bdev_cstr = CString::new(bdev_name)
            .map_err(|_| {
                StorageBackendError::InvalidOperation("invalid bdev name".to_string())
            })?;

        let mut desc: *mut SpdkBdevDesc = std::ptr::null_mut();
        let ret = unsafe {
            spdk_bdev_open_ext(
                bdev_cstr.as_ptr(),
                true,
                std::ptr::null(),
                std::ptr::null_mut(),
                &mut desc,
            )
        };

        if ret != 0 || desc.is_null() {
            return Err(StorageBackendError::InvalidOperation(
                format!("failed to open bdev {}: {}", bdev_name, ret).to_string(),
            ));
        }

        let block_size = unsafe { spdk_bdev_get_block_size(desc) };
        if block_size == 0 {
            unsafe { spdk_bdev_close(desc) };
            return Err(StorageBackendError::InvalidOperation(
                "invalid block size".to_string(),
            ));
        }

        let num_blocks = unsafe { spdk_bdev_get_num_blocks(desc) };
        let ns_size = num_blocks * block_size as u64;
        let actual_capacity = total_capacity.unwrap_or(ns_size);

        let used_space = 0u64;
        let free_space = actual_capacity - used_space;

        let device_info = StorageDevice {
            device_id: device_id.clone(),
            name: bdev_name.to_string(),
            total_capacity: actual_capacity,
            used_space,
            free_space,
            status: "online".to_string(),
            block_size,
        };

        let spdk_device = SpdkDevice {
            info: device_info,
            free_offset: 0,
            desc,
            block_size,
        };

        self.devices.write().unwrap().insert(device_id.clone(), spdk_device);

        Ok(device_id)
    }

    pub fn list_bdevs(&self) -> Vec<String> {
        let mut bdevs: Vec<String> = Vec::new();

        unsafe {
            let mut bdev = spdk_bdev_first();
            while !bdev.is_null() {
                let name_ptr = spdk_bdev_get_name(bdev);
                if !name_ptr.is_null() {
                    let name = CStr::from_ptr(name_ptr).to_str().unwrap_or("unknown");
                    bdevs.push(name.to_string());
                }
                bdev = spdk_bdev_next(bdev);
            }
        }
        bdevs
    }

    fn align_up(value: u64, align: u64) -> u64 {
        if value.is_multiple_of(align) {
            value
        } else {
            value + align - (value % align)
        }
    }

    fn sync_read(
        &self,
        desc: *mut SpdkBdevDesc,
        buf: *mut u8,
        lba: u64,
        lba_count: u32,
    ) -> Result<()> {
        let ch = unsafe { spdk_bdev_get_io_channel(desc) };
        if ch.is_null() {
            return Err(StorageBackendError::SpdkIoError(
                "failed to get IO channel".to_string(),
            ));
        }

        let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let result = Arc::new(std::sync::atomic::AtomicI32::new(0));

        let completed_clone = completed.clone();
        let result_clone = result.clone();

        unsafe extern "C" fn read_cb(
            _ch: *mut SpdkIoChannel,
            ctx: *mut c_void,
            rc: c_int,
        ) {
            let (completed, result) = &*(ctx as *const (Arc<std::sync::atomic::AtomicBool>, Arc<std::sync::atomic::AtomicI32>));
            result.store(rc, std::sync::atomic::Ordering::Relaxed);
            completed.store(true, std::sync::atomic::Ordering::Relaxed);
        }

        let ctx = Box::new((completed_clone, result_clone));
        let ctx_ptr = Box::into_raw(ctx) as *mut c_void;

        let ret = unsafe {
            spdk_bdev_read_blocks(desc, ch, buf, lba as c_ulonglong, lba_count, read_cb as *const c_void, ctx_ptr)
        };

        if ret != 0 {
            unsafe { spdk_put_io_channel(ch) };
            unsafe { Box::from_raw(ctx_ptr) };
            return Err(StorageBackendError::SpdkIoError(
                format!("read failed: {}", ret).to_string(),
            ));
        }

        while !completed.load(std::sync::atomic::Ordering::Relaxed) {
            std::thread::yield_now();
        }

        let rc = result.load(std::sync::atomic::Ordering::Relaxed);
        unsafe { spdk_put_io_channel(ch) };
        unsafe { Box::from_raw(ctx_ptr) };

        if rc != 0 {
            return Err(StorageBackendError::SpdkIoError(
                format!("read callback failed: {}", rc).to_string(),
            ));
        }

        Ok(())
    }

    fn sync_write(
        &self,
        desc: *mut SpdkBdevDesc,
        buf: *mut u8,
        lba: u64,
        lba_count: u32,
    ) -> Result<()> {
        let ch = unsafe { spdk_bdev_get_io_channel(desc) };
        if ch.is_null() {
            return Err(StorageBackendError::SpdkIoError(
                "failed to get IO channel".to_string(),
            ));
        }

        let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let result = Arc::new(std::sync::atomic::AtomicI32::new(0));

        let completed_clone = completed.clone();
        let result_clone = result.clone();

        unsafe extern "C" fn write_cb(
            _ch: *mut SpdkIoChannel,
            ctx: *mut c_void,
            rc: c_int,
        ) {
            let (completed, result) = &*(ctx as *const (Arc<std::sync::atomic::AtomicBool>, Arc<std::sync::atomic::AtomicI32>));
            result.store(rc, std::sync::atomic::Ordering::Relaxed);
            completed.store(true, std::sync::atomic::Ordering::Relaxed);
        }

        let ctx = Box::new((completed_clone, result_clone));
        let ctx_ptr = Box::into_raw(ctx) as *mut c_void;

        let ret = unsafe {
            spdk_bdev_write_blocks(desc, ch, buf, lba as c_ulonglong, lba_count, write_cb as *const c_void, ctx_ptr)
        };

        if ret != 0 {
            unsafe { spdk_put_io_channel(ch) };
            unsafe { Box::from_raw(ctx_ptr) };
            return Err(StorageBackendError::SpdkIoError(
                format!("write failed: {}", ret).to_string(),
            ));
        }

        while !completed.load(std::sync::atomic::Ordering::Relaxed) {
            std::thread::yield_now();
        }

        let rc = result.load(std::sync::atomic::Ordering::Relaxed);
        unsafe { spdk_put_io_channel(ch) };
        unsafe { Box::from_raw(ctx_ptr) };

        if rc != 0 {
            return Err(StorageBackendError::SpdkIoError(
                format!("write callback failed: {}", rc).to_string(),
            ));
        }

        Ok(())
    }
}

impl Drop for SpdkBackend {
    fn drop(&mut self) {
        for (_, device) in self.devices.write().unwrap().drain() {
            unsafe { spdk_bdev_close(device.desc) };
        }
        unsafe { spdk_subsystem_fini() };
        unsafe { spdk_env_fini() };
    }
}

impl crate::storage_backend::StorageBackend for SpdkBackend {
    fn create_volume(
        &self,
        device_id: &str,
        volume_id: u64,
        size: u64,
    ) -> Result<()> {
        let device = self.devices.read().unwrap().get(device_id)
            .ok_or_else(|| StorageBackendError::DeviceNotFound(device_id.to_string()))?;

        let aligned_size = Self::align_up(size, 4096);
        if aligned_size > device.info.free_space {
            return Err(StorageBackendError::InsufficientSpace(
                device.info.free_space,
                aligned_size,
            ));
        }

        let mut devices = self.devices.write().unwrap();
        let device = devices.get_mut(device_id).unwrap();
        let physical_offset = device.free_offset;
        device.free_offset += aligned_size;
        device.info.used_space += aligned_size;
        device.info.free_space -= aligned_size;

        let volume_meta = VolumeMeta {
            volume_id,
            device_id: device_id.to_string(),
            total_size: aligned_size,
            used_size: 0,
            physical_offset,
            state: VolumeState::Online,
        };

        self.volumes.write().unwrap().insert(volume_id, volume_meta);

        Ok(())
    }

    fn open_volume(&self, volume_id: u64) -> Result<()> {
        let volume = self.volumes.read().unwrap().get(&volume_id)
            .ok_or_else(|| StorageBackendError::VolumeNotFound(volume_id))?;

        let mut volumes = self.volumes.write().unwrap();
        if let Some(v) = volumes.get_mut(&volume_id) {
            v.state = VolumeState::Online;
        }

        Ok(())
    }

    fn close_volume(&self, volume_id: u64) -> Result<()> {
        let volume = self.volumes.read().unwrap().get(&volume_id)
            .ok_or_else(|| StorageBackendError::VolumeNotFound(volume_id))?;

        let mut volumes = self.volumes.write().unwrap();
        if let Some(v) = volumes.get_mut(&volume_id) {
            v.state = VolumeState::Offline;
        }

        Ok(())
    }

    fn delete_volume(&self, volume_id: u64) -> Result<()> {
        let volume = self.volumes.read().unwrap().get(&volume_id)
            .ok_or_else(|| StorageBackendError::VolumeNotFound(volume_id))?;

        let device_id = volume.device_id.clone();

        let mut devices = self.devices.write().unwrap();
        if let Some(device) = devices.get_mut(&device_id) {
            device.info.used_space -= volume.total_size;
            device.info.free_space += volume.total_size;
        }

        self.volumes.write().unwrap().remove(&volume_id);

        Ok(())
    }

    fn read_volume(
        &self,
        volume_id: u64,
        offset: u64,
        length: u64,
        buffer: &mut [u8],
    ) -> Result<()> {
        let volume = self.volumes.read().unwrap().get(&volume_id)
            .ok_or_else(|| StorageBackendError::VolumeNotFound(volume_id))?;

        if volume.state != VolumeState::Online {
            return Err(StorageBackendError::InvalidOperation(
                "volume is not online".to_string(),
            ));
        }

        let device = self.devices.read().unwrap().get(&volume.device_id)
            .ok_or_else(|| StorageBackendError::DeviceNotFound(volume.device_id.clone()))?;

        let physical_offset = volume.physical_offset + offset;
        let lba = physical_offset / device.block_size as u64;
        let lba_count = ((length + device.block_size as u64 - 1) / device.block_size as u64) as u32;

        if buffer.len() < (lba_count * device.block_size) as usize {
            return Err(StorageBackendError::InvalidOperation(
                "buffer too small".to_string(),
            ));
        }

        self.sync_read(device.desc, buffer.as_mut_ptr(), lba, lba_count)?;

        Ok(())
    }

    fn write_volume(
        &self,
        volume_id: u64,
        offset: u64,
        length: u64,
        buffer: &[u8],
    ) -> Result<()> {
        let volume = self.volumes.read().unwrap().get(&volume_id)
            .ok_or_else(|| StorageBackendError::VolumeNotFound(volume_id))?;

        if volume.state != VolumeState::Online {
            return Err(StorageBackendError::InvalidOperation(
                "volume is not online".to_string(),
            ));
        }

        let device = self.devices.read().unwrap().get(&volume.device_id)
            .ok_or_else(|| StorageBackendError::DeviceNotFound(volume.device_id.clone()))?;

        let physical_offset = volume.physical_offset + offset;
        let lba = physical_offset / device.block_size as u64;
        let lba_count = ((length + device.block_size as u64 - 1) / device.block_size as u64) as u32;

        if buffer.len() < (lba_count * device.block_size) as usize {
            return Err(StorageBackendError::InvalidOperation(
                "buffer too small".to_string(),
            ));
        }

        let mut aligned_buf = vec![0u8; (lba_count * device.block_size) as usize];
        aligned_buf[..buffer.len()].copy_from_slice(buffer);

        self.sync_write(device.desc, aligned_buf.as_mut_ptr(), lba, lba_count)?;

        let mut volumes = self.volumes.write().unwrap();
        if let Some(v) = volumes.get_mut(&volume_id) {
            let new_end = offset + length;
            if new_end > v.used_size {
                v.used_size = new_end;
            }
        }

        Ok(())
    }

    fn list_devices(&self) -> Result<Vec<StorageDevice>> {
        let devices = self.devices.read().unwrap();
        Ok(devices.values().map(|d| d.info.clone()).collect())
    }

    fn list_volumes(&self) -> Result<Vec<(u64, String, u64, u64, VolumeState)>> {
        let volumes = self.volumes.read().unwrap();
        Ok(volumes.values().map(|v| {
            (v.volume_id, v.device_id.clone(), v.total_size, v.used_size, v.state)
        }).collect())
    }

    fn get_device_info(&self, device_id: &str) -> Result<StorageDevice> {
        let devices = self.devices.read().unwrap();
        devices.get(device_id)
            .map(|d| d.info.clone())
            .ok_or_else(|| StorageBackendError::DeviceNotFound(device_id.to_string()))
    }

    fn get_volume_info(&self, volume_id: u64) -> Result<(u64, u64, VolumeState)> {
        let volumes = self.volumes.read().unwrap();
        volumes.get(&volume_id)
            .map(|v| (v.total_size, v.used_size, v.state))
            .ok_or_else(|| StorageBackendError::VolumeNotFound(volume_id))
    }

    fn exclude_device(&self, device_id: &str, reason: &str) -> Result<()> {
        let device = self.devices.read().unwrap().get(device_id)
            .ok_or_else(|| StorageBackendError::DeviceNotFound(device_id.to_string()))?;

        let excluded = ExcludedDevice {
            device_id: device_id.to_string(),
            reason: reason.to_string(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };

        self.excluded_devices.write().unwrap().insert(device_id.to_string(), excluded);

        Ok(())
    }

    fn restore_device(&self, device_id: &str) -> Result<()> {
        self.excluded_devices.write().unwrap().remove(device_id);
        Ok(())
    }

    fn get_excluded_devices(&self) -> Result<Vec<(String, String, u64)>> {
        let excluded = self.excluded_devices.read().unwrap();
        Ok(excluded.values().map(|e| {
            (e.device_id.clone(), e.reason.clone(), e.timestamp)
        }).collect())
    }
}
