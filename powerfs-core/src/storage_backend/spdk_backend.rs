use crate::storage_backend::*;
use bytes::Bytes;
use chrono::Utc;
use log::{error, info, warn};
use std::collections::HashMap;
use std::sync::RwLock;

type Result<T> = StorageResult<T>;

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
    bdev_name: String,
}

unsafe impl Send for SpdkDevice {}
unsafe impl Sync for SpdkDevice {}

pub struct SpdkBackend {
    devices: RwLock<HashMap<String, SpdkDevice>>,
    volumes: RwLock<HashMap<u64, VolumeMeta>>,
    excluded_devices: RwLock<HashMap<String, ExcludedDevice>>,
    node_id: String,
    rpc_socket_path: String,
    _checksum_algo: ChecksumAlgorithm,
}

impl SpdkBackend {
    pub fn new(node_id: &str, rpc_socket_path: Option<&str>) -> Result<Self> {
        let socket_path = rpc_socket_path
            .map(|s| s.to_string())
            .unwrap_or_else(|| crate::storage_backend::spdk_rpc::DEFAULT_SPDK_RPC_SOCKET.to_string());

        Ok(SpdkBackend {
            devices: RwLock::new(HashMap::new()),
            volumes: RwLock::new(HashMap::new()),
            excluded_devices: RwLock::new(HashMap::new()),
            node_id: node_id.to_string(),
            rpc_socket_path: socket_path,
            _checksum_algo: ChecksumAlgorithm::default(),
        })
    }

    pub fn new_with_env(node_id: &str) -> Self {
        SpdkBackend {
            devices: RwLock::new(HashMap::new()),
            volumes: RwLock::new(HashMap::new()),
            excluded_devices: RwLock::new(HashMap::new()),
            node_id: node_id.to_string(),
            rpc_socket_path: crate::storage_backend::spdk_rpc::DEFAULT_SPDK_RPC_SOCKET.to_string(),
            _checksum_algo: ChecksumAlgorithm::default(),
        }
    }

    pub fn add_device(
        &self,
        device_name: &str,
        bdev_name: &str,
        total_capacity: Option<u64>,
    ) -> Result<String> {
        let device_id = format!("spdk_nvme_{}", device_name);

        let actual_capacity = total_capacity.unwrap_or_else(|| {
            self.get_bdev_size(bdev_name).unwrap_or(0)
        });
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

        let spdk_device = SpdkDevice {
            info: device,
            free_offset: 0,
            bdev_name: bdev_name.to_string(),
        };

        self.devices
            .write()
            .unwrap()
            .insert(device_id.clone(), spdk_device);

        Ok(device_id)
    }

    #[cfg(feature = "spdk")]
    fn get_bdev_size(&self, bdev_name: &str) -> Result<u64> {
        use crate::storage_backend::spdk_rpc::SpdkRpcClient;
        use std::time::Duration;

        let client = SpdkRpcClient::new(&self.rpc_socket_path);
        let rt = tokio::runtime::Runtime::new().unwrap();
        let bdevs = rt.block_on(async {
            client.wait_ready(Duration::from_secs(5)).await?;
            client.list_bdevs().await
        })?;

        for bdev in bdevs {
            if bdev == bdev_name {
                return Ok(0);
            }
        }
        Ok(0)
    }

    #[cfg(feature = "spdk-stub")]
    fn get_bdev_size(&self, _bdev_name: &str) -> Result<u64> {
        Ok(1024 * 1024 * 1024)
    }

    /// 根据配置文件异步 attach 所有设备
    ///
    /// # Feature 行为
    /// - `spdk` feature: 通过 SPDK JSON-RPC (Unix socket) 调用 `bdev_nvme_attach_controller`。
    /// - `spdk-stub` feature: 不走 RPC,直接调 `add_device`,用于测试 attach 流程逻辑。
    pub async fn attach_devices_from_config(
        &self,
        devices: &[SpdkDeviceConfig],
        rpc_socket: Option<&str>,
    ) -> Vec<AttachDeviceResult> {
        let mut results = Vec::with_capacity(devices.len());

        if devices.is_empty() {
            warn!("attach_devices_from_config called with empty device list");
            return results;
        }

        info!(
            "Attaching {} SPDK device(s) from config",
            devices.len()
        );

        #[cfg(feature = "spdk")]
        {
            results = self.attach_devices_via_rpc(devices, rpc_socket).await;
        }

        #[cfg(feature = "spdk-stub")]
        {
            results = self.attach_devices_stub(devices).await;
        }

        let ok = results.iter().filter(|r| r.success).count();
        let fail = results.len() - ok;
        if fail == 0 {
            info!("All {} device(s) attached successfully", ok);
        } else {
            warn!(
                "{} device(s) attached, {} failed (see previous logs)",
                ok, fail
            );
        }

        results
    }

    #[cfg(feature = "spdk")]
    async fn attach_devices_via_rpc(
        &self,
        devices: &[SpdkDeviceConfig],
        rpc_socket: Option<&str>,
    ) -> Vec<AttachDeviceResult> {
        use crate::storage_backend::spdk_rpc::SpdkRpcClient;
        use std::time::Duration;

        let socket = rpc_socket.unwrap_or(&self.rpc_socket_path);
        let client = SpdkRpcClient::new(socket);

        match client.wait_fully_ready(Duration::from_secs(30)).await {
            Ok(_) => info!("SPDK RPC server ready at {}, start attaching devices", socket),
            Err(e) => {
                error!(
                    "SPDK RPC server not ready at {} after 30s: {} — skipping all device attach",
                    socket, e
                );
                return devices
                    .iter()
                    .map(|d| AttachDeviceResult::failed(d.name.clone(), format!("RPC not ready: {}", e)))
                    .collect();
            }
        }

        let mut results = Vec::with_capacity(devices.len());
        for device in devices {
            match self.attach_one_device_via_rpc(&client, device).await {
                Ok(bdev_name) => {
                    match self.add_device(&device.name, &bdev_name, device.capacity) {
                        Ok(device_id) => {
                            info!(
                                "Device {} attached via RPC: bdev={}, device_id={}",
                                device.name, bdev_name, device_id
                            );
                            results.push(AttachDeviceResult::ok(
                                device.name.clone(),
                                device_id,
                                Some(bdev_name),
                            ));
                        }
                        Err(e) => {
                            error!(
                                "Device {} RPC attach succeeded (bdev={}) but add_device failed: {}",
                                device.name, bdev_name, e
                            );
                            results.push(AttachDeviceResult::failed(
                                device.name.clone(),
                                format!("add_device failed: {}", e),
                            ));
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "Device {} RPC attach failed: {} — skipping (other devices continue)",
                        device.name, e
                    );
                    results.push(AttachDeviceResult::failed(
                        device.name.clone(),
                        format!("RPC attach failed: {}", e),
                    ));
                }
            }
        }
        results
    }

    #[cfg(feature = "spdk")]
    async fn attach_one_device_via_rpc(
        &self,
        client: &crate::storage_backend::spdk_rpc::SpdkRpcClient,
        device: &SpdkDeviceConfig,
    ) -> Result<String> {
        use crate::storage_backend::spdk_rpc::parse_transport_string;

        let params = parse_transport_string(&device.transport_string).map_err(|e| {
            StorageBackendError::InvalidOperation(format!(
                "invalid transport_string for device {}: {}",
                device.name, e
            ))
        })?;

        let bdev_names = client
            .attach_nvme_controller(
                &device.name,
                &params.trtype,
                &params.traddr,
                params.trsvcid.as_deref(),
                params.subnqn.as_deref(),
            )
            .await?;

        bdev_names
            .into_iter()
            .next()
            .ok_or_else(|| {
                StorageBackendError::InvalidOperation(format!(
                    "SPDK RPC attach returned no bdevs for device {}",
                    device.name
                ))
            })
    }

    #[cfg(feature = "spdk-stub")]
    async fn attach_devices_stub(&self, devices: &[SpdkDeviceConfig]) -> Vec<AttachDeviceResult> {
        let mut results = Vec::with_capacity(devices.len());
        for device in devices {
            match self.add_device(&device.name, &device.transport_string, device.capacity) {
                Ok(device_id) => {
                    info!(
                        "[stub] Device {} attached: device_id={}",
                        device.name, device_id
                    );
                    results.push(AttachDeviceResult::ok(device.name.clone(), device_id, None));
                }
                Err(e) => {
                    warn!("[stub] Device {} attach failed: {}", device.name, e);
                    results.push(AttachDeviceResult::failed(
                        device.name.clone(),
                        format!("add_device failed: {}", e),
                    ));
                }
            }
        }
        results
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
    fn drop(&mut self) {}
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

        let aligned_size = Self::align_up(size, 4096);

        let excluded = self.excluded_devices.read().unwrap();

        let candidate_device_ids: Vec<String> = {
            let devices = self.devices.read().unwrap();
            if let Some(pref) = preferred_device_id {
                vec![pref.to_string()]
            } else {
                devices
                    .keys()
                    .filter(|id| !excluded.contains_key(*id))
                    .cloned()
                    .collect()
            }
        };

        let mut selected_device_id: Option<String> = None;
        let mut physical_offset: u64 = 0;

        {
            let mut devices = self.devices.write().unwrap();
            for device_id in candidate_device_ids {
                if let Some(device) = devices.get_mut(&device_id) {
                    if device.info.free_space >= aligned_size {
                        physical_offset = device.free_offset;
                        device.free_offset += aligned_size;
                        device.info.used_space += aligned_size;
                        device.info.free_space -= aligned_size;
                        selected_device_id = Some(device_id);
                        break;
                    }
                }
            }
        }

        let device_id = selected_device_id
            .ok_or_else(|| StorageBackendError::NoAvailableDevice(aligned_size))?;

        let volume_meta = VolumeMeta {
            volume_id,
            device_id: device_id.clone(),
            total_size: aligned_size,
            used_size: 0,
            physical_offset,
            state: VolumeState::Active,
        };

        self.volumes.write().unwrap().insert(volume_id, volume_meta);

        Ok(AllocateVolumeResult {
            volume_id,
            device_id,
            allocated_size: aligned_size,
            volume_offset: physical_offset,
        })
    }

    fn delete_volume(&self, volume_id: u64) -> Result<()> {
        let (device_id, total_size) = {
            let volumes = self.volumes.read().unwrap();
            let volume = volumes
                .get(&volume_id)
                .ok_or_else(|| StorageBackendError::VolumeNotFound(volume_id))?;
            (volume.device_id.clone(), volume.total_size)
        };

        let mut devices = self.devices.write().unwrap();
        if let Some(device) = devices.get_mut(&device_id) {
            device.info.used_space -= total_size;
            device.info.free_space += total_size;
        }

        self.volumes.write().unwrap().remove(&volume_id);

        Ok(())
    }

    fn get_volume_info(&self, volume_id: u64) -> Result<VolumeStorageInfo> {
        let volumes = self.volumes.read().unwrap();
        volumes
            .get(&volume_id)
            .map(|v| VolumeStorageInfo {
                volume_id: v.volume_id,
                device_id: v.device_id.clone(),
                total_size: v.total_size,
                used_size: v.used_size,
                physical_offset: v.physical_offset,
                volume_state: v.state,
            })
            .ok_or_else(|| StorageBackendError::VolumeNotFound(volume_id))
    }

    fn get_volume_device(&self, volume_id: u64) -> Result<String> {
        let volumes = self.volumes.read().unwrap();
        volumes
            .get(&volume_id)
            .map(|v| v.device_id.clone())
            .ok_or_else(|| StorageBackendError::VolumeNotFound(volume_id))
    }

    fn read_needle(&self, volume_id: u64, offset: u64, size: u32) -> Result<Bytes> {
        let (device_id, physical_offset, state) = {
            let volumes = self.volumes.read().unwrap();
            let volume = volumes
                .get(&volume_id)
                .ok_or_else(|| StorageBackendError::VolumeNotFound(volume_id))?;
            (
                volume.device_id.clone(),
                volume.physical_offset,
                volume.state,
            )
        };

        if state != VolumeState::Active {
            return Err(StorageBackendError::InvalidOperation(
                "volume is not active".to_string(),
            ));
        }

        let bdev_name = {
            let devices = self.devices.read().unwrap();
            let device = devices
                .get(&device_id)
                .ok_or_else(|| StorageBackendError::DeviceNotFound(device_id))?;
            device.bdev_name.clone()
        };

        let physical_offset = physical_offset + offset;

        #[cfg(feature = "spdk")]
        {
            use crate::storage_backend::spdk_rpc::SpdkRpcClient;
            use std::time::Duration;

            let client = SpdkRpcClient::new(&self.rpc_socket_path);
            let rt = tokio::runtime::Runtime::new().unwrap();
            let data = rt.block_on(async {
                client.wait_ready(Duration::from_secs(5)).await?;
                client.read_bdev(&bdev_name, physical_offset, size as u64).await
            })?;
            return Ok(Bytes::copy_from_slice(&data));
        }

        #[cfg(feature = "spdk-stub")]
        {
            let mut buf = vec![0u8; size as usize];
            return Ok(Bytes::copy_from_slice(&buf));
        }
    }

    fn write_needle(&self, volume_id: u64, offset: u64, data: &[u8]) -> Result<u32> {
        let (device_id, physical_offset, state) = {
            let volumes = self.volumes.read().unwrap();
            let volume = volumes
                .get(&volume_id)
                .ok_or_else(|| StorageBackendError::VolumeNotFound(volume_id))?;
            (
                volume.device_id.clone(),
                volume.physical_offset,
                volume.state,
            )
        };

        if state != VolumeState::Active {
            return Err(StorageBackendError::InvalidOperation(
                "volume is not active".to_string(),
            ));
        }

        let bdev_name = {
            let devices = self.devices.read().unwrap();
            let device = devices
                .get(&device_id)
                .ok_or_else(|| StorageBackendError::DeviceNotFound(device_id))?;
            device.bdev_name.clone()
        };

        let physical_offset = physical_offset + offset;

        #[cfg(feature = "spdk")]
        {
            use crate::storage_backend::spdk_rpc::SpdkRpcClient;
            use std::time::Duration;

            let aligned_size = Self::align_up(data.len() as u64, 512);
            let mut aligned_buf = vec![0u8; aligned_size as usize];
            aligned_buf[..data.len()].copy_from_slice(data);

            let client = SpdkRpcClient::new(&self.rpc_socket_path);
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                client.wait_ready(Duration::from_secs(5)).await?;
                client.write_bdev(&bdev_name, physical_offset, &aligned_buf).await
            })?;
        }

        #[cfg(feature = "spdk-stub")]
        {}

        let mut volumes = self.volumes.write().unwrap();
        if let Some(v) = volumes.get_mut(&volume_id) {
            let new_end = offset + data.len() as u64;
            if new_end > v.used_size {
                v.used_size = new_end;
            }
        }

        Ok(data.len() as u32)
    }

    fn sync_volume(&self, _volume_id: u64) -> Result<()> {
        Ok(())
    }

    fn truncate_volume(&self, volume_id: u64, new_size: u64) -> Result<()> {
        let total_size = {
            let volumes = self.volumes.read().unwrap();
            let volume = volumes
                .get(&volume_id)
                .ok_or_else(|| StorageBackendError::VolumeNotFound(volume_id))?;
            volume.total_size
        };

        if new_size > total_size {
            return Err(StorageBackendError::InvalidOperation(
                "truncate size exceeds volume size".to_string(),
            ));
        }

        let mut volumes = self.volumes.write().unwrap();
        if let Some(v) = volumes.get_mut(&volume_id) {
            v.used_size = new_size;
        }

        Ok(())
    }

    fn get_volumes_on_device(&self, device_id: &str) -> Result<Vec<u64>> {
        let volumes = self.volumes.read().unwrap();
        Ok(volumes
            .values()
            .filter(|v| v.device_id == device_id)
            .map(|v| v.volume_id)
            .collect())
    }

    fn get_volume_set(&self, device_id: &str) -> Result<VolumeSet> {
        let device = self.get_device(device_id)?;
        let volumes = self.get_volumes_on_device(device_id)?;

        Ok(VolumeSet {
            device_id: device_id.to_string(),
            volumes,
            total_capacity: device.total_capacity,
            total_used: device.used_space,
            total_free: device.free_space,
            health_status: HealthStatus::Healthy,
        })
    }

    fn exclude_device(&self, device_id: &str, reason: String) -> Result<()> {
        let _ = self.get_device(device_id)?;

        let excluded = ExcludedDevice {
            device_id: device_id.to_string(),
            reason,
            excluded_at: Utc::now(),
            excluded_by: "powerfs".to_string(),
            auto_drain: false,
        };

        self.excluded_devices
            .write()
            .unwrap()
            .insert(device_id.to_string(), excluded);

        Ok(())
    }

    fn include_device(&self, device_id: &str) -> Result<()> {
        self.excluded_devices.write().unwrap().remove(device_id);
        Ok(())
    }

    fn is_device_excluded(&self, device_id: &str) -> bool {
        self.excluded_devices
            .read()
            .unwrap()
            .contains_key(device_id)
    }

    fn list_excluded_devices(&self) -> Vec<ExcludedDevice> {
        let excluded = self.excluded_devices.read().unwrap();
        excluded.values().cloned().collect()
    }

    fn health_check(&self) -> Result<HealthStatus> {
        let devices = self.devices.read().unwrap();
        if devices.is_empty() {
            return Ok(HealthStatus::Warning);
        }

        for device in devices.values() {
            if device.info.status != DeviceStatus::Online {
                return Ok(HealthStatus::Degraded);
            }
        }

        Ok(HealthStatus::Healthy)
    }
}
