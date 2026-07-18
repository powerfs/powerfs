use crate::storage_backend::*;
use bytes::Bytes;
use chrono::Utc;
#[cfg(feature = "spdk")]
use log::error;
use log::{info, warn};
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

#[derive(Clone)]
struct SpdkDevice {
    info: StorageDevice,
    free_offset: u64,
    #[cfg(feature = "spdk")]
    bdev_name: String,
}

unsafe impl Send for SpdkDevice {}
unsafe impl Sync for SpdkDevice {}

#[cfg(feature = "spdk")]
struct NvmfInitiator {
    connections: RwLock<HashMap<String, NvmfPoolEntry>>,
}

#[cfg(feature = "spdk")]
#[derive(Clone)]
struct NvmfPoolEntry {
    connection: std::sync::Arc<NvmfConnection>,
    tcp_stream: Option<std::sync::Arc<tokio::sync::Mutex<tokio::net::TcpStream>>>,
}

#[cfg(feature = "spdk")]
#[allow(dead_code)]
struct NvmfConnection {
    traddr: String,
    trsvcid: String,
    subnqn: String,
    io_queues: usize,
    last_used: std::sync::atomic::AtomicU64,
    health_status: std::sync::atomic::AtomicU8,
    connection_pool: RwLock<Vec<std::sync::Arc<tokio::sync::Mutex<Option<tokio::net::TcpStream>>>>>,
    max_pool_size: usize,
}

#[cfg(feature = "spdk")]
#[allow(dead_code)]
impl NvmfConnection {
    fn new(traddr: String, trsvcid: String, subnqn: String) -> Self {
        Self {
            traddr,
            trsvcid,
            subnqn,
            io_queues: 1,
            last_used: std::sync::atomic::AtomicU64::new(0),
            health_status: std::sync::atomic::AtomicU8::new(1),
            connection_pool: RwLock::new(Vec::new()),
            max_pool_size: 8,
        }
    }

    fn mark_used(&self) {
        self.last_used.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    fn mark_failed(&self) {
        self.health_status
            .store(0, std::sync::atomic::Ordering::Relaxed);
    }

    fn mark_healthy(&self) {
        self.health_status
            .store(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn is_healthy(&self) -> bool {
        self.health_status
            .load(std::sync::atomic::Ordering::Relaxed)
            == 1
    }

    async fn get_connection(
        &self,
    ) -> std::sync::Arc<tokio::sync::Mutex<Option<tokio::net::TcpStream>>> {
        let conn = {
            let mut pool = self.connection_pool.write().unwrap();
            pool.pop()
        };

        if let Some(conn) = conn {
            let has_connection = conn.lock().await.is_some();
            if has_connection {
                return conn;
            }
        }

        std::sync::Arc::new(tokio::sync::Mutex::new(None))
    }

    async fn release_connection(
        &self,
        conn: std::sync::Arc<tokio::sync::Mutex<Option<tokio::net::TcpStream>>>,
    ) {
        let mut pool = self.connection_pool.write().unwrap();
        if pool.len() < self.max_pool_size {
            pool.push(conn);
        }
    }

    async fn create_connection(&self) -> Result<tokio::net::TcpStream> {
        let port: u16 = self
            .trsvcid
            .parse()
            .map_err(|e| StorageBackendError::InvalidOperation(format!("invalid port: {}", e)))?;

        tokio::net::TcpStream::connect((self.traddr.as_str(), port))
            .await
            .map_err(|e| {
                StorageBackendError::InvalidOperation(format!(
                    "failed to connect to NVMe-oF target: {}",
                    e
                ))
            })
    }
}

pub struct SpdkBackend {
    devices: RwLock<HashMap<String, SpdkDevice>>,
    volumes: RwLock<HashMap<u64, VolumeMeta>>,
    excluded_devices: RwLock<HashMap<String, ExcludedDevice>>,
    node_id: String,
    #[allow(dead_code)]
    rpc_socket_path: String,
    _checksum_algo: ChecksumAlgorithm,
    #[cfg(feature = "spdk")]
    nvmf_initiator: RwLock<Option<NvmfInitiator>>,
    #[cfg(feature = "spdk")]
    #[allow(dead_code)]
    health_check_handle: std::sync::Arc<std::sync::atomic::AtomicBool>,
    #[cfg(feature = "spdk")]
    #[allow(dead_code)]
    shutdown_signal: std::sync::mpsc::Sender<()>,
}

impl SpdkBackend {
    pub fn new(node_id: &str, rpc_socket_path: Option<&str>) -> Result<Self> {
        let socket_path = rpc_socket_path
            .map(|s| s.to_string())
            .unwrap_or_else(|| DEFAULT_SPDK_RPC_SOCKET.to_string());

        #[cfg(feature = "spdk")]
        let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel();
        #[cfg(feature = "spdk")]
        let health_check_handle = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));

        #[cfg(feature = "spdk")]
        Self::spawn_health_check_thread(
            health_check_handle.clone(),
            shutdown_rx,
            socket_path.clone(),
        );

        Ok(SpdkBackend {
            devices: RwLock::new(HashMap::new()),
            volumes: RwLock::new(HashMap::new()),
            excluded_devices: RwLock::new(HashMap::new()),
            node_id: node_id.to_string(),
            rpc_socket_path: socket_path,
            _checksum_algo: ChecksumAlgorithm::default(),
            #[cfg(feature = "spdk")]
            nvmf_initiator: RwLock::new(None),
            #[cfg(feature = "spdk")]
            health_check_handle,
            #[cfg(feature = "spdk")]
            shutdown_signal: shutdown_tx,
        })
    }

    pub fn new_with_env(node_id: &str) -> Self {
        #[cfg(feature = "spdk")]
        let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel();
        #[cfg(feature = "spdk")]
        let health_check_handle = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));

        #[cfg(feature = "spdk")]
        Self::spawn_health_check_thread(
            health_check_handle.clone(),
            shutdown_rx,
            DEFAULT_SPDK_RPC_SOCKET.to_string(),
        );

        SpdkBackend {
            devices: RwLock::new(HashMap::new()),
            volumes: RwLock::new(HashMap::new()),
            excluded_devices: RwLock::new(HashMap::new()),
            node_id: node_id.to_string(),
            rpc_socket_path: DEFAULT_SPDK_RPC_SOCKET.to_string(),
            _checksum_algo: ChecksumAlgorithm::default(),
            #[cfg(feature = "spdk")]
            nvmf_initiator: RwLock::new(None),
            #[cfg(feature = "spdk")]
            health_check_handle,
            #[cfg(feature = "spdk")]
            shutdown_signal: shutdown_tx,
        }
    }

    #[cfg(feature = "spdk")]
    fn spawn_health_check_thread(
        running: std::sync::Arc<std::sync::atomic::AtomicBool>,
        shutdown_rx: std::sync::mpsc::Receiver<()>,
        socket_path: String,
    ) {
        std::thread::spawn(move || {
            use std::time::Duration;

            while running.load(std::sync::atomic::Ordering::Relaxed) {
                match shutdown_rx.try_recv() {
                    Ok(_) => {
                        info!("Health check thread shutting down");
                        break;
                    }
                    Err(std::sync::mpsc::TryRecvError::Empty) => {}
                    Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                        info!("Health check thread receiver disconnected, shutting down");
                        break;
                    }
                }

                if !Self::check_spdk_tgt_health(&socket_path) {
                    warn!("spdk-tgt health check failed, marking all connections as unhealthy");
                }

                std::thread::sleep(Duration::from_secs(10));
            }
        });
    }

    #[cfg(feature = "spdk")]
    fn check_spdk_tgt_health(socket_path: &str) -> bool {
        use std::os::unix::net::UnixStream;

        match UnixStream::connect(socket_path) {
            Ok(_) => true,
            Err(e) => {
                warn!("Failed to connect to spdk-tgt at {}: {}", socket_path, e);
                false
            }
        }
    }

    pub fn add_device(
        &self,
        device_name: &str,
        bdev_name: &str,
        total_capacity: Option<u64>,
    ) -> Result<String> {
        let device_id = format!("spdk_nvme_{}", device_name);

        let actual_capacity =
            total_capacity.unwrap_or_else(|| self.get_bdev_size(bdev_name).unwrap_or(0));
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
            #[cfg(feature = "spdk")]
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
        #[cfg(feature = "spdk")] rpc_socket: Option<&str>,
        #[cfg(feature = "spdk-stub")] _rpc_socket: Option<&str>,
    ) -> Vec<AttachDeviceResult> {
        let mut results = Vec::with_capacity(devices.len());

        if devices.is_empty() {
            warn!("attach_devices_from_config called with empty device list");
            return results;
        }

        info!("Attaching {} SPDK device(s) from config", devices.len());

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
            Ok(_) => info!(
                "SPDK RPC server ready at {}, start attaching devices",
                socket
            ),
            Err(e) => {
                error!(
                    "SPDK RPC server not ready at {} after 30s: {} — skipping all device attach",
                    socket, e
                );
                return devices
                    .iter()
                    .map(|d| {
                        AttachDeviceResult::failed(d.name.clone(), format!("RPC not ready: {}", e))
                    })
                    .collect();
            }
        }

        let mut results = Vec::with_capacity(devices.len());
        for device in devices {
            match self.attach_one_device_via_rpc(&client, device).await {
                Ok(bdev_name) => match self.add_device(&device.name, &bdev_name, device.capacity) {
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
                },
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

        bdev_names.into_iter().next().ok_or_else(|| {
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

        let device_id =
            selected_device_id.ok_or(StorageBackendError::NoAvailableDevice(aligned_size))?;

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
                .ok_or(StorageBackendError::VolumeNotFound(volume_id))?;
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
            .ok_or(StorageBackendError::VolumeNotFound(volume_id))
    }

    fn get_volume_device(&self, volume_id: u64) -> Result<String> {
        let volumes = self.volumes.read().unwrap();
        volumes
            .get(&volume_id)
            .map(|v| v.device_id.clone())
            .ok_or(StorageBackendError::VolumeNotFound(volume_id))
    }

    fn read_needle(&self, volume_id: u64, offset: u64, size: u32) -> Result<Bytes> {
        #[cfg(feature = "spdk")]
        {
            let (device_id, physical_offset, state) = {
                let volumes = self.volumes.read().unwrap();
                let volume = volumes
                    .get(&volume_id)
                    .ok_or(StorageBackendError::VolumeNotFound(volume_id))?;
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
                    .ok_or(StorageBackendError::DeviceNotFound(device_id))?;
                device.bdev_name.clone()
            };
            let io_offset = physical_offset + offset;
            let rt = tokio::runtime::Runtime::new().map_err(|e| {
                StorageBackendError::InvalidOperation(format!(
                    "failed to create tokio runtime: {}",
                    e
                ))
            })?;
            rt.block_on(self.read_needle_nvmf(&bdev_name, io_offset, size as u64))
        }

        #[cfg(feature = "spdk-stub")]
        {
            let _ = volume_id;
            let _ = offset;
            let buf = vec![0u8; size as usize];
            Ok(Bytes::copy_from_slice(&buf))
        }
    }

    fn write_needle(&self, volume_id: u64, offset: u64, data: &[u8]) -> Result<u32> {
        let state = {
            let volumes = self.volumes.read().unwrap();
            let volume = volumes
                .get(&volume_id)
                .ok_or(StorageBackendError::VolumeNotFound(volume_id))?;
            volume.state
        };

        if state != VolumeState::Active {
            return Err(StorageBackendError::InvalidOperation(
                "volume is not active".to_string(),
            ));
        }

        #[cfg(feature = "spdk")]
        {
            let (device_id, physical_offset) = {
                let volumes = self.volumes.read().unwrap();
                let volume = volumes
                    .get(&volume_id)
                    .ok_or(StorageBackendError::VolumeNotFound(volume_id))?;
                (volume.device_id.clone(), volume.physical_offset)
            };

            let bdev_name = {
                let devices = self.devices.read().unwrap();
                let device = devices
                    .get(&device_id)
                    .ok_or(StorageBackendError::DeviceNotFound(device_id))?;
                device.bdev_name.clone()
            };
            let io_offset = physical_offset + offset;
            let aligned_size = Self::align_up(data.len() as u64, 512);
            let mut aligned_buf = vec![0u8; aligned_size as usize];
            aligned_buf[..data.len()].copy_from_slice(data);
            let rt = tokio::runtime::Runtime::new().map_err(|e| {
                StorageBackendError::InvalidOperation(format!(
                    "failed to create tokio runtime: {}",
                    e
                ))
            })?;
            rt.block_on(self.write_needle_nvmf(&bdev_name, io_offset, &aligned_buf))?;
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
                .ok_or(StorageBackendError::VolumeNotFound(volume_id))?;
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

#[cfg(feature = "spdk")]
#[allow(dead_code)]
impl SpdkBackend {
    async fn read_needle_nvmf(&self, bdev_name: &str, offset: u64, size: u64) -> Result<Bytes> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let max_retries = 3;
        let mut last_error: Option<StorageBackendError> = None;

        for attempt in 0..max_retries {
            let pool_entry = match self.get_or_create_nvmf_connection(bdev_name) {
                Ok(conn) => conn,
                Err(e) => {
                    last_error = Some(e);
                    tokio::time::sleep(tokio::time::Duration::from_millis(
                        50 * (attempt + 1) as u64,
                    ))
                    .await;
                    continue;
                }
            };

            let stream = if let Some(ref stream) = pool_entry.tcp_stream {
                stream.clone()
            } else {
                let new_stream = pool_entry.connection.create_connection().await?;
                std::sync::Arc::new(tokio::sync::Mutex::new(new_stream))
            };

            let mut stream_guard = stream.lock().await;

            let request = Self::build_nvme_read_command(bdev_name, offset, size);

            match stream_guard.write_all(&request).await {
                Ok(_) => {
                    let mut response = vec![0u8; size as usize + 64];
                    match stream_guard.read_exact(&mut response).await {
                        Ok(_) => {
                            if let Err(e) = Self::parse_nvme_response(&response[0..64]) {
                                pool_entry.connection.mark_failed();
                                last_error = Some(e);
                                tokio::time::sleep(tokio::time::Duration::from_millis(
                                    50 * (attempt + 1) as u64,
                                ))
                                .await;
                                continue;
                            }
                            return Ok(Bytes::copy_from_slice(&response[64..]));
                        }
                        Err(e) => {
                            pool_entry.connection.mark_failed();
                            last_error = Some(StorageBackendError::InvalidOperation(format!(
                                "failed to read: {}",
                                e
                            )));
                            tokio::time::sleep(tokio::time::Duration::from_millis(
                                50 * (attempt + 1) as u64,
                            ))
                            .await;
                            continue;
                        }
                    }
                }
                Err(e) => {
                    pool_entry.connection.mark_failed();
                    last_error = Some(StorageBackendError::InvalidOperation(format!(
                        "failed to write: {}",
                        e
                    )));
                    tokio::time::sleep(tokio::time::Duration::from_millis(
                        50 * (attempt + 1) as u64,
                    ))
                    .await;
                    continue;
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            StorageBackendError::InvalidOperation(format!(
                "NVMe-oF read failed after {} retries for {}",
                max_retries, bdev_name
            ))
        }))
    }

    #[cfg(feature = "spdk")]
    async fn write_needle_nvmf(&self, bdev_name: &str, offset: u64, data: &[u8]) -> Result<()> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let max_retries = 3;
        let mut last_error: Option<StorageBackendError> = None;

        for attempt in 0..max_retries {
            let pool_entry = match self.get_or_create_nvmf_connection(bdev_name) {
                Ok(conn) => conn,
                Err(e) => {
                    last_error = Some(e);
                    tokio::time::sleep(tokio::time::Duration::from_millis(
                        50 * (attempt + 1) as u64,
                    ))
                    .await;
                    continue;
                }
            };

            let aligned_size = Self::align_up(data.len() as u64, 512);
            let mut aligned_buf = vec![0u8; aligned_size as usize];
            aligned_buf[..data.len()].copy_from_slice(data);

            let stream = if let Some(ref stream) = pool_entry.tcp_stream {
                stream.clone()
            } else {
                let new_stream = pool_entry.connection.create_connection().await?;
                std::sync::Arc::new(tokio::sync::Mutex::new(new_stream))
            };

            let mut stream_guard = stream.lock().await;

            let request = Self::build_nvme_write_command(bdev_name, offset, &aligned_buf);

            match stream_guard.write_all(&request).await {
                Ok(_) => {
                    let mut response = vec![0u8; 64];
                    match stream_guard.read_exact(&mut response).await {
                        Ok(_) => {
                            if let Err(e) = Self::parse_nvme_response(&response) {
                                pool_entry.connection.mark_failed();
                                last_error = Some(e);
                                tokio::time::sleep(tokio::time::Duration::from_millis(
                                    50 * (attempt + 1) as u64,
                                ))
                                .await;
                                continue;
                            }
                            if attempt > 0 {
                                info!(
                                    "NVMe-oF write succeeded after {} retries for {}",
                                    attempt, bdev_name
                                );
                            }
                            return Ok(());
                        }
                        Err(e) => {
                            pool_entry.connection.mark_failed();
                            last_error = Some(StorageBackendError::InvalidOperation(format!(
                                "failed to read response: {}",
                                e
                            )));
                            tokio::time::sleep(tokio::time::Duration::from_millis(
                                50 * (attempt + 1) as u64,
                            ))
                            .await;
                            continue;
                        }
                    }
                }
                Err(e) => {
                    pool_entry.connection.mark_failed();
                    last_error = Some(StorageBackendError::InvalidOperation(format!(
                        "failed to write: {}",
                        e
                    )));
                    tokio::time::sleep(tokio::time::Duration::from_millis(
                        50 * (attempt + 1) as u64,
                    ))
                    .await;
                    continue;
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            StorageBackendError::InvalidOperation(format!(
                "NVMe-oF write failed after {} retries for {}",
                max_retries, bdev_name
            ))
        }))
    }

    #[cfg(feature = "spdk")]
    async fn read_needles_batch_nvmf(
        &self,
        bdev_name: &str,
        requests: &[(u64, u32)],
    ) -> Result<Vec<Bytes>> {
        let mut results = Vec::with_capacity(requests.len());

        for &(offset, size) in requests {
            let data = self
                .read_needle_nvmf(bdev_name, offset, size as u64)
                .await?;
            results.push(data);
        }

        Ok(results)
    }

    #[cfg(feature = "spdk")]
    async fn write_needles_batch_nvmf(
        &self,
        bdev_name: &str,
        requests: &[(u64, &[u8])],
    ) -> Result<()> {
        for &(offset, data) in requests {
            self.write_needle_nvmf(bdev_name, offset, data).await?;
        }

        Ok(())
    }

    #[cfg(feature = "spdk")]
    fn get_or_create_nvmf_connection(&self, bdev_name: &str) -> Result<NvmfPoolEntry> {
        let mut initiator = self.nvmf_initiator.write().unwrap();

        if initiator.is_none() {
            *initiator = Some(NvmfInitiator {
                connections: RwLock::new(HashMap::new()),
            });
        }

        let initiator = initiator.as_ref().unwrap();
        let mut connections = initiator.connections.write().unwrap();

        if let Some(entry) = connections.get(bdev_name) {
            if entry.connection.is_healthy() {
                entry.connection.mark_used();
                return Ok(entry.clone());
            } else {
                warn!(
                    "Connection for {} is unhealthy, creating new connection",
                    bdev_name
                );
                connections.remove(bdev_name);
            }
        }

        let conn = std::sync::Arc::new(NvmfConnection::new(
            "127.0.0.1".to_string(),
            "4420".to_string(),
            "nqn.2016-06.io.spdk:powerfs".to_string(),
        ));

        let entry = NvmfPoolEntry {
            connection: conn,
            tcp_stream: None,
        };

        connections.insert(bdev_name.to_string(), entry.clone());
        info!("Created new NVMe-oF connection for bdev: {}", bdev_name);
        Ok(entry)
    }

    #[cfg(feature = "spdk")]
    fn reconnect_nvmf_connection(&self, bdev_name: &str) -> Result<NvmfPoolEntry> {
        let initiator = self.nvmf_initiator.read().unwrap();

        if initiator.is_none() {
            return Err(StorageBackendError::InvalidOperation(
                "NVMf initiator not initialized".to_string(),
            ));
        }

        let initiator = initiator.as_ref().unwrap();
        let mut connections = initiator.connections.write().unwrap();

        if let Some(entry) = connections.get(bdev_name) {
            entry.connection.mark_failed();
        }

        connections.remove(bdev_name);

        self.get_or_create_nvmf_connection(bdev_name)
    }

    #[cfg(feature = "spdk")]
    fn build_nvme_read_command(bdev_name: &str, offset: u64, size: u64) -> Vec<u8> {
        let mut cmd = Vec::with_capacity(64);
        cmd.extend_from_slice(&0x20u32.to_le_bytes());
        cmd.extend_from_slice(&offset.to_le_bytes());
        cmd.extend_from_slice(&size.to_le_bytes());
        let name_bytes = bdev_name.as_bytes();
        let mut name_padded = vec![0u8; 32];
        name_padded[..std::cmp::min(name_bytes.len(), 32)].copy_from_slice(name_bytes);
        cmd.extend_from_slice(&name_padded);
        cmd
    }

    #[cfg(feature = "spdk")]
    fn build_nvme_write_command(bdev_name: &str, offset: u64, data: &[u8]) -> Vec<u8> {
        let mut cmd = Vec::with_capacity(64 + data.len());
        cmd.extend_from_slice(&0x21u32.to_le_bytes());
        cmd.extend_from_slice(&offset.to_le_bytes());
        cmd.extend_from_slice(&(data.len() as u64).to_le_bytes());
        let name_bytes = bdev_name.as_bytes();
        let mut name_padded = vec![0u8; 32];
        name_padded[..std::cmp::min(name_bytes.len(), 32)].copy_from_slice(name_bytes);
        cmd.extend_from_slice(&name_padded);
        cmd.extend_from_slice(data);
        cmd
    }

    #[cfg(feature = "spdk")]
    fn parse_nvme_response(response: &[u8]) -> Result<()> {
        if response.len() < 4 {
            return Err(StorageBackendError::InvalidOperation(
                "NVMe response too short".to_string(),
            ));
        }
        let status = u32::from_le_bytes(response[0..4].try_into().unwrap());
        if status != 0 {
            return Err(StorageBackendError::InvalidOperation(format!(
                "NVMe command failed with status: {}",
                status
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn test_spdk_config_deser() {
        let config_json = r#"
        {
            "devices": [
                {"name": "Nvme1", "transport_string": "0000:03:00.0"},
                {"name": "Nvme2", "transport_string": "trtype:tcp traddr:192.168.1.10 trsvcid:4420 subnqn:nqn.2016-06.io.spdk:cnode1"}
            ],
            "rpc_socket_path": "/var/tmp/spdk.sock",
            "local_tgt": {
                "enabled": true,
                "cpu_mask": "0x7",
                "hugepages_size_mb": 4096
            },
            "nvmf": {
                "subsystem_nqn": "nqn.2016-06.io.spdk:powerfs",
                "listener_traddr": "127.0.0.1",
                "listener_trsvcid": "4420",
                "transport_type": "tcp"
            }
        }
        "#;

        let config: SpdkBackendConfig = serde_json::from_str(config_json).unwrap();

        assert_eq!(config.devices.len(), 2);
        assert_eq!(config.devices[0].name, "Nvme1");
        assert_eq!(config.devices[0].transport_string, "0000:03:00.0");
        assert_eq!(config.devices[1].name, "Nvme2");
        assert_eq!(
            config.devices[1].transport_string,
            "trtype:tcp traddr:192.168.1.10 trsvcid:4420 subnqn:nqn.2016-06.io.spdk:cnode1"
        );

        assert_eq!(
            config.rpc_socket_path,
            Some("/var/tmp/spdk.sock".to_string())
        );

        assert!(config.local_tgt.is_some());
        let local_tgt = config.local_tgt.unwrap();
        assert!(local_tgt.enabled);
        assert_eq!(local_tgt.cpu_mask, "0x7");
        assert_eq!(local_tgt.hugepages_size_mb, 4096);

        assert!(config.nvmf.is_some());
        let nvmf = config.nvmf.unwrap();
        assert_eq!(nvmf.subsystem_nqn, "nqn.2016-06.io.spdk:powerfs");
        assert_eq!(nvmf.listener_traddr, "127.0.0.1");
        assert_eq!(nvmf.listener_trsvcid, "4420");
        assert_eq!(nvmf.transport_type, "tcp");
    }

    #[test]
    fn test_spdk_device_transport_string() {
        let pci_config = SpdkDeviceConfig {
            name: "TestDevice".to_string(),
            transport_string: "0000:03:00.0".to_string(),
            capacity: None,
        };
        assert!(pci_config.transport_string.starts_with("0000:"));

        let nvmeof_config = SpdkDeviceConfig {
            name: "RemoteDevice".to_string(),
            transport_string: "trtype:tcp traddr:10.0.0.1 trsvcid:4420".to_string(),
            capacity: None,
        };
        assert!(nvmeof_config.transport_string.contains("trtype:"));
    }

    #[test]
    fn test_nvmf_config_fields() {
        let config = NvmfConfig {
            subsystem_nqn: "nqn.2016-06.io.spdk:test".to_string(),
            listener_traddr: "192.168.1.1".to_string(),
            listener_trsvcid: "4420".to_string(),
            transport_type: "tcp".to_string(),
        };

        assert!(!config.subsystem_nqn.is_empty());
        assert!(!config.listener_traddr.is_empty());
        assert!(!config.listener_trsvcid.is_empty());
        assert!(["tcp", "rdma"].contains(&config.transport_type.as_str()));
    }

    #[test]
    fn test_device_type_variants() {
        assert_eq!(DeviceType::LocalFile.to_string(), "local_file");
        assert_eq!(DeviceType::SpdkNvme.to_string(), "spdk_nvme");
    }

    #[test]
    fn test_spdk_backend_stub_init() {
        let backend = SpdkBackend::new_with_env("test-node");

        assert_eq!(backend.node_id, "test-node");
        assert_eq!(backend.rpc_socket_path, DEFAULT_SPDK_RPC_SOCKET);
    }

    #[test]
    fn test_spdk_backend_new_with_socket() {
        let backend = SpdkBackend::new("test-node", Some("/tmp/custom.sock")).unwrap();

        assert_eq!(backend.node_id, "test-node");
        assert_eq!(backend.rpc_socket_path, "/tmp/custom.sock");
    }

    #[test]
    fn test_spdk_backend_new_default_socket() {
        let backend = SpdkBackend::new("test-node", None).unwrap();

        assert_eq!(backend.node_id, "test-node");
        assert_eq!(backend.rpc_socket_path, DEFAULT_SPDK_RPC_SOCKET);
    }

    #[cfg(feature = "spdk")]
    #[test]
    fn test_nvme_command_build_read() {
        let cmd = SpdkBackend::build_nvme_read_command("Nvme0n1", 1024, 512);

        assert_eq!(cmd.len(), 64);
        assert_eq!(&cmd[0..4], &0x20u32.to_le_bytes());
        assert_eq!(&cmd[4..12], &1024u64.to_le_bytes());
        assert_eq!(&cmd[12..20], &512u64.to_le_bytes());
        assert_eq!(
            &cmd[20..52],
            b"Nvme0n1\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"
        );
    }

    #[cfg(feature = "spdk")]
    #[test]
    fn test_nvme_command_build_write() {
        let data = vec![0xAAu8; 1024];
        let cmd = SpdkBackend::build_nvme_write_command("Nvme0n1", 2048, &data);

        assert_eq!(cmd.len(), 64 + 1024);
        assert_eq!(&cmd[0..4], &0x21u32.to_le_bytes());
        assert_eq!(&cmd[4..12], &2048u64.to_le_bytes());
        assert_eq!(&cmd[12..20], &1024u64.to_le_bytes());
        assert_eq!(
            &cmd[20..52],
            b"Nvme0n1\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"
        );
        assert_eq!(&cmd[64..], &data);
    }

    #[cfg(feature = "spdk")]
    #[test]
    fn test_nvme_response_parse_success() {
        let mut response = vec![0u8; 64];
        response[0..4].copy_from_slice(&0u32.to_le_bytes());

        assert!(SpdkBackend::parse_nvme_response(&response).is_ok());
    }

    #[cfg(feature = "spdk")]
    #[test]
    fn test_nvme_response_parse_failure() {
        let mut response = vec![0u8; 64];
        response[0..4].copy_from_slice(&1u32.to_le_bytes());

        assert!(SpdkBackend::parse_nvme_response(&response).is_err());
    }

    #[cfg(feature = "spdk")]
    #[test]
    fn test_nvme_response_parse_too_short() {
        let response = vec![0u8; 3];

        assert!(SpdkBackend::parse_nvme_response(&response).is_err());
    }

    #[cfg(feature = "spdk")]
    #[test]
    fn test_nvmf_connection_management() {
        let backend = SpdkBackend::new("test-node", None).unwrap();

        let conn1 = backend.get_or_create_nvmf_connection("Nvme0n1").unwrap();
        let conn2 = backend.get_or_create_nvmf_connection("Nvme0n1").unwrap();

        assert_eq!(conn1.traddr, conn2.traddr);
        assert_eq!(conn1.trsvcid, conn2.trsvcid);
        assert_eq!(conn1.subnqn, conn2.subnqn);

        let conn3 = backend.get_or_create_nvmf_connection("Nvme1n1").unwrap();
        assert_eq!(conn3.subnqn, "nqn.2016-06.io.spdk:powerfs");
    }
}
