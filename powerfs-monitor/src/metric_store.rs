use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use serde::Serialize;
use tokio::sync::RwLock;

use crate::event::{ClusterMetrics, KVMetrics, NodeStatusEvent, VolumeStatusEvent};

#[derive(Debug, Clone, Serialize)]
pub struct NodeInfo {
    pub id: String,
    pub node_type: String,
    pub address: String,
    pub grpc_port: u32,
    pub http_port: u32,
    pub status: String,
    pub cpu_usage: f64,
    pub mem_usage: f64,
    pub disk_usage: f64,
    pub network_rx: u64,
    pub network_tx: u64,
    pub uptime: u64,
    pub volume_count: u32,
    pub is_leader: bool,
    pub raft_term: u64,
    /// Wall-clock instant of the last NodeStatusEvent received from this
    /// node. Used by `mark_stale_nodes_offline` to flip nodes that have
    /// stopped heartbeating to `offline`. Not serialized — internal only,
    /// the API consumer never sees this (and historically never did).
    #[serde(skip)]
    pub last_seen: Instant,
}

#[derive(Debug, Clone, Serialize)]
pub struct VolumeInfo {
    pub id: u64,
    pub node_id: String,
    pub size: u64,
    pub used: u64,
    pub file_count: u64,
    pub status: String,
    pub collection: String,
    pub created_at: String,
    #[serde(default)]
    pub read_only: bool,
    #[serde(default)]
    pub replica_placement: u32,
    #[serde(default)]
    pub ttl: u32,
    #[serde(default)]
    pub disk_type: String,
    #[serde(default)]
    pub compact_status: u32,
    #[serde(default)]
    pub append_offset: u64,
    // I/O performance counters
    #[serde(default)]
    pub read_ops: u64,
    #[serde(default)]
    pub write_ops: u64,
    #[serde(default)]
    pub read_bytes: u64,
    #[serde(default)]
    pub write_bytes: u64,
    #[serde(default)]
    pub read_avg_latency_us: u64,
    #[serde(default)]
    pub write_avg_latency_us: u64,
    #[serde(default)]
    pub read_p50_latency_us: u64,
    #[serde(default)]
    pub read_p99_latency_us: u64,
    #[serde(default)]
    pub write_p50_latency_us: u64,
    #[serde(default)]
    pub write_p99_latency_us: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct KVSessionInfo {
    pub id: String,
    pub model_name: String,
    pub layer_count: u32,
    pub block_count: u64,
    pub memory_used: u64,
    pub hit_ratio: f64,
    pub eviction_count: u64,
    pub created_at: String,
}

// ========== StorageDevices (MVP: 每个 volume 节点聚合一个 logical device) ==========

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DeviceType {
    Ssd,
    Nvme,
    Hdd,
    Logical,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DeviceStatus {
    Online,
    Offline,
    Draining,
    Excluded,
    ReadOnly,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DeviceHealth {
    Healthy,
    Warning,
    Critical,
    Unknown,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeviceLocation {
    pub node_id: String,
    pub rack: String,
    pub slot: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct StorageDevice {
    pub device_id: String,
    pub device_type: DeviceType,
    pub total_capacity: u64,
    pub used_space: u64,
    pub free_space: u64,
    pub location: DeviceLocation,
    pub status: DeviceStatus,
    pub health: DeviceHealth,
    pub volume_count: u64,
    pub last_check: String,
}

// ========== Data Migration Tasks ==========

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MigrationTaskStatus {
    Pending,
    Running,
    Paused,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize)]
pub struct DataMigrationTask {
    pub task_id: String,
    pub source_device_id: String,
    pub target_device_id: Option<String>,
    pub total_bytes: u64,
    pub migrated_bytes: u64,
    pub status: MigrationTaskStatus,
    pub start_time: String,
    pub end_time: Option<String>,
    pub error_message: Option<String>,
    pub reason: String,
}

pub struct MetricStore {
    nodes: RwLock<HashMap<String, NodeInfo>>,
    volumes: RwLock<HashMap<u64, VolumeInfo>>,
    kv_sessions: RwLock<HashMap<String, KVSessionInfo>>,
    cluster_metrics: RwLock<ClusterMetrics>,
    kv_metrics: RwLock<KVMetrics>,
    collection_names: RwLock<HashSet<String>>,
    // Storage devices: derived (keyed by node_id, one logical device per volume node),
    // but allow manual override of status/health via exclude/restore/drain actions.
    storage_devices: RwLock<HashMap<String, StorageDevice>>,
    // Per-device manual overrides: status overrides live separately from derived data
    // so they survive re-derivation on volume updates.
    device_status_overrides: RwLock<HashMap<String, DeviceStatus>>,
    device_health_overrides: RwLock<HashMap<String, DeviceHealth>>,
    // Migration tasks
    migration_tasks: RwLock<HashMap<String, DataMigrationTask>>,
    start_time: Instant,
}

impl MetricStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn update_node(&self, event: NodeStatusEvent) {
        {
            let mut nodes = self.nodes.write().await;
            nodes.insert(
                event.node_id.clone(),
                NodeInfo {
                    id: event.node_id,
                    node_type: event.node_type,
                    address: event.address,
                    grpc_port: event.grpc_port,
                    http_port: event.http_port,
                    status: event.status,
                    cpu_usage: event.cpu_usage,
                    mem_usage: event.mem_usage,
                    disk_usage: event.disk_usage,
                    network_rx: event.network_rx,
                    network_tx: event.network_tx,
                    uptime: event.uptime,
                    volume_count: event.volume_count,
                    is_leader: event.is_leader,
                    raft_term: event.raft_term,
                    last_seen: Instant::now(),
                },
            );
        }
        self.update_cluster_metrics().await;
        self.derive_storage_devices().await;
    }

    /// Mark nodes that haven't sent a NodeStatusEvent within `timeout_secs` as
    /// `offline`. This is the heartbeat-timeout mechanism — without it, a
    /// crashed volume/filer/master process would stay "healthy" in memory
    /// forever, masking real outages from every downstream view (Nodes page,
    /// ClusterTopology, Dashboard KPIs, StorageDevices derive, Master Raft
    /// quorum count).
    ///
    /// Preserves the original `status` string of master nodes (leader /
    /// follower) because Raft role is orthogonal to process liveness and
    /// the MasterRaft page reads role off the status field. Volume/filer
    /// nodes (which always publish "healthy" when alive) are simply
    /// rewritten to "offline".
    pub async fn mark_stale_nodes_offline(&self, timeout_secs: u64) {
        let now = Instant::now();
        let timeout = std::time::Duration::from_secs(timeout_secs);
        let mut changed = false;
        {
            let mut nodes = self.nodes.write().await;
            for n in nodes.values_mut() {
                if now.duration_since(n.last_seen) > timeout {
                    // Master role is encoded in the status field (leader /
                    // follower) and is NOT liveness — don't clobber it. The
                    // MasterRaft page derives "healthy" from role + last_seen.
                    if n.node_type == "master" {
                        continue;
                    }
                    if n.status != "offline" {
                        n.status = "offline".to_string();
                        changed = true;
                    }
                }
            }
        }
        if changed {
            self.update_cluster_metrics().await;
            self.derive_storage_devices().await;
        }
    }

    pub async fn update_volume(&self, event: VolumeStatusEvent) {
        {
            let mut volumes = self.volumes.write().await;
            volumes.insert(
                event.volume_id,
                VolumeInfo {
                    id: event.volume_id,
                    node_id: event.node_id,
                    size: event.size,
                    used: event.used,
                    file_count: event.file_count,
                    status: event.status,
                    collection: event.collection.clone(),
                    created_at: chrono::Utc::now().to_rfc3339(),
                    read_only: event.read_only,
                    replica_placement: event.replica_placement,
                    ttl: event.ttl,
                    disk_type: event.disk_type,
                    compact_status: event.compact_status,
                    append_offset: event.append_offset,
                    read_ops: event.read_ops,
                    write_ops: event.write_ops,
                    read_bytes: event.read_bytes,
                    write_bytes: event.write_bytes,
                    read_avg_latency_us: event.read_avg_latency_us,
                    write_avg_latency_us: event.write_avg_latency_us,
                    read_p50_latency_us: event.read_p50_latency_us,
                    read_p99_latency_us: event.read_p99_latency_us,
                    write_p50_latency_us: event.write_p50_latency_us,
                    write_p99_latency_us: event.write_p99_latency_us,
                },
            );
        }

        {
            let mut collections = self.collection_names.write().await;
            collections.insert(event.collection);
        }

        self.update_cluster_metrics().await;
        self.derive_storage_devices().await;
    }

    pub async fn update_kv_session(&self, event: crate::event::KVSessionEvent) {
        {
            let mut sessions = self.kv_sessions.write().await;
            match event.event_type.as_str() {
                "create" | "update" => {
                    sessions.insert(
                        event.session_id.clone(),
                        KVSessionInfo {
                            id: event.session_id,
                            model_name: event.model_name,
                            layer_count: event.layer_count,
                            block_count: event.block_count,
                            memory_used: event.memory_used,
                            hit_ratio: event.hit_ratio,
                            eviction_count: event.eviction_count,
                            created_at: chrono::Utc::now().to_rfc3339(),
                        },
                    );
                }
                "delete" => {
                    sessions.remove(&event.session_id);
                }
                _ => {}
            }
        }
        self.update_kv_metrics().await;
    }

    pub async fn update_cluster_metrics(&self) {
        let nodes = self.nodes.read().await;
        let volumes = self.volumes.read().await;
        let collections = self.collection_names.read().await;

        let mut total_storage: u64 = 0;
        let mut used_storage: u64 = 0;
        let mut file_count: u64 = 0;

        for volume in volumes.values() {
            total_storage += volume.size;
            used_storage += volume.used;
            file_count += volume.file_count;
        }

        let mut metrics = self.cluster_metrics.write().await;
        *metrics = ClusterMetrics {
            node_count: nodes.len() as u32,
            volume_count: volumes.len() as u32,
            collection_count: collections.len() as u32,
            is_leader: metrics.is_leader,
            raft_term: metrics.raft_term,
            uptime: self.start_time.elapsed().as_secs(),
            total_storage,
            used_storage,
            file_count,
        };
    }

    pub async fn update_kv_metrics(&self) {
        let sessions = self.kv_sessions.read().await;

        let mut block_count: u64 = 0;
        let mut memory_used: u64 = 0;
        let mut total_hit_ratio: f64 = 0.0;
        let mut eviction_count: u64 = 0;

        for session in sessions.values() {
            block_count += session.block_count;
            memory_used += session.memory_used;
            total_hit_ratio += session.hit_ratio;
            eviction_count += session.eviction_count;
        }

        let avg_hit_ratio = if sessions.is_empty() {
            0.0
        } else {
            total_hit_ratio / sessions.len() as f64
        };

        let mut metrics = self.kv_metrics.write().await;
        *metrics = KVMetrics {
            session_count: sessions.len() as u32,
            block_count,
            memory_used,
            hit_ratio: avg_hit_ratio,
            eviction_count,
            put_count: metrics.put_count,
            get_count: metrics.get_count,
            avg_latency: metrics.avg_latency,
        };
    }

    pub async fn get_nodes(&self) -> Vec<NodeInfo> {
        self.nodes.read().await.values().cloned().collect()
    }

    pub async fn get_node(&self, id: &str) -> Option<NodeInfo> {
        self.nodes.read().await.get(id).cloned()
    }

    pub async fn get_volumes(&self) -> Vec<VolumeInfo> {
        self.volumes.read().await.values().cloned().collect()
    }

    pub async fn get_volume(&self, id: u64) -> Option<VolumeInfo> {
        self.volumes.read().await.get(&id).cloned()
    }

    pub async fn get_kv_sessions(&self) -> Vec<KVSessionInfo> {
        self.kv_sessions.read().await.values().cloned().collect()
    }

    pub async fn get_kv_session(&self, id: &str) -> Option<KVSessionInfo> {
        self.kv_sessions.read().await.get(id).cloned()
    }

    pub async fn delete_node(&self, id: &str) -> bool {
        self.nodes.write().await.remove(id).is_some()
    }

    pub async fn delete_volume(&self, id: u64) -> bool {
        self.volumes.write().await.remove(&id).is_some()
    }

    pub async fn delete_kv_session(&self, id: &str) -> bool {
        self.kv_sessions.write().await.remove(id).is_some()
    }

    pub async fn get_cluster_metrics(&self) -> ClusterMetrics {
        self.cluster_metrics.read().await.clone()
    }

    pub async fn get_kv_metrics(&self) -> KVMetrics {
        self.kv_metrics.read().await.clone()
    }

    pub async fn set_leader_info(&self, is_leader: bool, raft_term: u64) {
        let mut metrics = self.cluster_metrics.write().await;
        metrics.is_leader = is_leader;
        metrics.raft_term = raft_term;
    }

    pub async fn increment_kv_put(&self) {
        let mut metrics = self.kv_metrics.write().await;
        metrics.put_count += 1;
    }

    pub async fn increment_kv_get(&self) {
        let mut metrics = self.kv_metrics.write().await;
        metrics.get_count += 1;
    }

    // ========== StorageDevices: derive from volumes + nodes ==========

    /// Re-derive storage devices from current volumes/nodes state, preserving
    /// any manual status/health overrides (Excluded / Draining etc.). Called
    /// from update_volume / update_cluster_metrics so the device view stays
    /// in sync with the actual cluster state.
    pub async fn derive_storage_devices(&self) {
        let volumes = self.volumes.read().await;
        let nodes = self.nodes.read().await;
        let overrides = self.device_status_overrides.read().await;
        let health_overrides = self.device_health_overrides.read().await;

        // Aggregate volume metrics per node_id
        let mut aggr: HashMap<String, (u64, u64, u64)> = HashMap::new();
        for v in volumes.values() {
            let (cap, used, cnt) = aggr.entry(v.node_id.clone()).or_insert((0, 0, 0));
            *cap += v.size;
            *used += v.used;
            *cnt += 1;
        }

        let mut devices = HashMap::new();
        let now = chrono::Utc::now().to_rfc3339();

        // 1. One logical device per known volume node (has volumes OR has a volume-type node)
        let mut all_node_ids: HashSet<String> = HashSet::new();
        for nid in aggr.keys() {
            all_node_ids.insert(nid.clone());
        }
        for (nid, n) in nodes.iter() {
            if n.node_type == "volume" {
                all_node_ids.insert(nid.clone());
            }
        }

        for nid in all_node_ids {
            let (cap, used, cnt) = aggr.get(&nid).copied().unwrap_or((0, 0, 0));
            let node = nodes.get(&nid);

            // Device type heuristic: infer from disk_type on volumes, else Logical.
            let mut device_type = DeviceType::Logical;
            for v in volumes.values() {
                if v.node_id == nid && !v.disk_type.is_empty() {
                    let dt = v.disk_type.to_ascii_lowercase();
                    if dt.contains("nvme") {
                        device_type = DeviceType::Nvme;
                        break;
                    } else if dt.contains("ssd") {
                        device_type = DeviceType::Ssd;
                        break;
                    } else if dt.contains("hdd") || dt.contains("sata") {
                        device_type = DeviceType::Hdd;
                    }
                }
            }

            // Status: derive from the node's liveness status. Producers
            // publish different status strings per node type (master ->
            // leader/follower, volume/filer -> healthy, watchdog ->
            // offline), so we treat any "alive" status as Online. cap == 0
            // also forces Offline (no capacity advertised).
            let node_alive = node
                .as_ref()
                .map(|n| matches!(n.status.as_str(), "online" | "healthy" | "leader" | "follower"))
                .unwrap_or(false);
            let default_status = if cap == 0 || !node_alive {
                DeviceStatus::Offline
            } else {
                DeviceStatus::Online
            };
            let status = overrides.get(&nid).copied().unwrap_or(default_status);

            let used_ratio = if cap == 0 { 0.0 } else { used as f64 / cap as f64 };
            let default_health = if !node_alive {
                DeviceHealth::Unknown
            } else if used_ratio > 0.95 || node.map(|n| n.disk_usage > 95.0).unwrap_or(false) {
                DeviceHealth::Critical
            } else if used_ratio > 0.8 || node.map(|n| n.disk_usage > 80.0).unwrap_or(false) {
                DeviceHealth::Warning
            } else {
                DeviceHealth::Healthy
            };
            let health = health_overrides.get(&nid).copied().unwrap_or(default_health);

            let device_id = format!("dev:{}", nid);
            let rack = node
                .as_ref()
                .map(|n| n.address.split(':').next().unwrap_or("rack-0").to_string())
                .unwrap_or_else(|| "rack-0".to_string());

            devices.insert(
                nid.clone(),
                StorageDevice {
                    device_id: device_id.clone(),
                    device_type,
                    total_capacity: cap,
                    used_space: used,
                    free_space: cap.saturating_sub(used),
                    location: DeviceLocation {
                        node_id: nid.clone(),
                        rack,
                        slot: format!("slot-{}", nid.split('-').next_back().unwrap_or("0")),
                    },
                    status,
                    health,
                    volume_count: cnt,
                    last_check: now.clone(),
                },
            );
        }

        *self.storage_devices.write().await = devices;
    }

    pub async fn get_storage_devices(&self, node_id: Option<&str>) -> Vec<StorageDevice> {
        self.derive_storage_devices().await;
        let devs = self.storage_devices.read().await;
        match node_id {
            Some(nid) => devs
                .iter()
                .filter(|(k, _)| *k == nid)
                .map(|(_, d)| d.clone())
                .collect(),
            None => devs.values().cloned().collect(),
        }
    }

    pub async fn get_storage_device(&self, device_id: &str) -> Option<StorageDevice> {
        self.derive_storage_devices().await;
        let devs = self.storage_devices.read().await;
        devs.values().find(|d| d.device_id == device_id).cloned()
    }

    /// Convert device_id -> node_id (MVP: device_id is "dev:<node_id>").
    fn device_node_id(device_id: &str) -> Option<String> {
        device_id.strip_prefix("dev:").map(|s| s.to_string())
    }

    pub async fn exclude_device(&self, device_id: &str) -> Result<(), String> {
        let nid = Self::device_node_id(device_id).ok_or_else(|| "invalid device_id".to_string())?;
        self.derive_storage_devices().await;
        {
            let devs = self.storage_devices.read().await;
            if !devs.contains_key(&nid) {
                return Err(format!("device {} not found", device_id));
            }
        }
        self.device_status_overrides
            .write()
            .await
            .insert(nid, DeviceStatus::Excluded);
        self.derive_storage_devices().await;
        Ok(())
    }

    pub async fn restore_device(&self, device_id: &str) -> Result<(), String> {
        let nid = Self::device_node_id(device_id).ok_or_else(|| "invalid device_id".to_string())?;
        self.derive_storage_devices().await;
        {
            let devs = self.storage_devices.read().await;
            if !devs.contains_key(&nid) {
                return Err(format!("device {} not found", device_id));
            }
        }
        // Remove any manual override so derive_cluster decides based on actual state.
        self.device_status_overrides.write().await.remove(&nid);
        self.derive_storage_devices().await;
        Ok(())
    }

    /// Drain device: set status to Draining and spawn a migration task that
    /// moves its used bytes to another device (in-memory mock progress).
    pub async fn drain_device(&self, device_id: &str) -> Result<DataMigrationTask, String> {
        let nid = Self::device_node_id(device_id).ok_or_else(|| "invalid device_id".to_string())?;
        self.derive_storage_devices().await;
        let source = {
            let devs = self.storage_devices.read().await;
            devs.get(&nid).cloned().ok_or_else(|| format!("device {} not found", device_id))?
        };
        // Pick target: any other online device with space.
        let target_id: Option<String> = {
            let devs = self.storage_devices.read().await;
            devs.values()
                .filter(|d| d.device_id != source.device_id && d.status == DeviceStatus::Online && d.free_space >= source.used_space)
                .max_by_key(|d| d.free_space)
                .map(|d| d.device_id.clone())
        };
        let task_id = format!(
            "mig-{}-{}",
            nid,
            chrono::Utc::now().format("%Y%m%d%H%M%S")
        );
        let total_bytes = source.used_space;
        let task = DataMigrationTask {
            task_id: task_id.clone(),
            source_device_id: source.device_id.clone(),
            target_device_id: target_id,
            total_bytes,
            migrated_bytes: 0,
            status: MigrationTaskStatus::Running,
            start_time: chrono::Utc::now().to_rfc3339(),
            end_time: None,
            error_message: None,
            reason: "drain".to_string(),
        };
        self.device_status_overrides
            .write()
            .await
            .insert(nid, DeviceStatus::Draining);
        self.migration_tasks
            .write()
            .await
            .insert(task_id.clone(), task.clone());
        self.derive_storage_devices().await;
        Ok(task)
    }

    pub async fn get_migration_tasks(&self) -> Vec<DataMigrationTask> {
        self.migration_tasks.read().await.values().cloned().collect()
    }

    pub async fn cancel_migration(&self, task_id: &str) -> Result<(), String> {
        let mut tasks = self.migration_tasks.write().await;
        let task = tasks.get_mut(task_id).ok_or_else(|| format!("task {} not found", task_id))?;
        if matches!(task.status, MigrationTaskStatus::Completed | MigrationTaskStatus::Failed | MigrationTaskStatus::Cancelled) {
            return Err(format!("task {} already in terminal state {:?}", task_id, task.status));
        }
        task.status = MigrationTaskStatus::Cancelled;
        task.end_time = Some(chrono::Utc::now().to_rfc3339());
        // Clear draining override if this was the only draining task on its source.
        let src_node = Self::device_node_id(&task.source_device_id);
        drop(tasks);
        if let Some(nid) = src_node {
            let still_draining = self.migration_tasks.read().await.values().any(|t| {
                Self::device_node_id(&t.source_device_id).as_deref() == Some(&nid)
                    && matches!(t.status, MigrationTaskStatus::Running | MigrationTaskStatus::Paused)
            });
            if !still_draining {
                let mut overrides = self.device_status_overrides.write().await;
                if matches!(overrides.get(&nid), Some(DeviceStatus::Draining)) {
                    overrides.remove(&nid);
                }
            }
            self.derive_storage_devices().await;
        }
        Ok(())
    }

    pub async fn pause_migration(&self, task_id: &str) -> Result<(), String> {
        let mut tasks = self.migration_tasks.write().await;
        let task = tasks.get_mut(task_id).ok_or_else(|| format!("task {} not found", task_id))?;
        if task.status != MigrationTaskStatus::Running {
            return Err(format!("task {} is not running (status={:?})", task_id, task.status));
        }
        task.status = MigrationTaskStatus::Paused;
        Ok(())
    }

    pub async fn resume_migration(&self, task_id: &str) -> Result<(), String> {
        let mut tasks = self.migration_tasks.write().await;
        let task = tasks.get_mut(task_id).ok_or_else(|| format!("task {} not found", task_id))?;
        if task.status != MigrationTaskStatus::Paused {
            return Err(format!("task {} is not paused (status={:?})", task_id, task.status));
        }
        task.status = MigrationTaskStatus::Running;
        Ok(())
    }

    /// Background ticker: advances in-flight migration task progress so the UI
    /// observes a realistic drain flow. The ticker also re-derives storage
    /// devices so volume_count/capacity updates flow through.
    pub async fn tick_migrations(&self) {
        let mut tasks = self.migration_tasks.write().await;
        let mut finished_sources: Vec<String> = Vec::new();
        for task in tasks.values_mut() {
            if task.status != MigrationTaskStatus::Running {
                continue;
            }
            let step = std::cmp::max(1, task.total_bytes / 20);
            task.migrated_bytes = std::cmp::min(task.total_bytes, task.migrated_bytes + step);
            if task.migrated_bytes >= task.total_bytes {
                task.status = MigrationTaskStatus::Completed;
                task.end_time = Some(chrono::Utc::now().to_rfc3339());
                finished_sources.push(task.source_device_id.clone());
            }
        }
        drop(tasks);
        if !finished_sources.is_empty() {
            let mut overrides = self.device_status_overrides.write().await;
            for sid in finished_sources {
                if let Some(nid) = Self::device_node_id(&sid) {
                    // Clear draining override when all draining tasks for this node finish.
                    let other_active = self.migration_tasks.read().await.values().any(|t| {
                        Self::device_node_id(&t.source_device_id).as_deref() == Some(&nid)
                            && matches!(t.status, MigrationTaskStatus::Running | MigrationTaskStatus::Paused)
                    });
                    if !other_active && matches!(overrides.get(&nid), Some(DeviceStatus::Draining)) {
                        overrides.remove(&nid);
                    }
                }
            }
        }
        self.derive_storage_devices().await;
    }
}

impl Default for MetricStore {
    fn default() -> Self {
        Self {
            nodes: RwLock::new(HashMap::new()),
            volumes: RwLock::new(HashMap::new()),
            kv_sessions: RwLock::new(HashMap::new()),
            cluster_metrics: RwLock::new(ClusterMetrics {
                node_count: 0,
                volume_count: 0,
                collection_count: 0,
                is_leader: false,
                raft_term: 0,
                uptime: 0,
                total_storage: 0,
                used_storage: 0,
                file_count: 0,
            }),
            kv_metrics: RwLock::new(KVMetrics {
                session_count: 0,
                block_count: 0,
                memory_used: 0,
                hit_ratio: 0.0,
                eviction_count: 0,
                put_count: 0,
                get_count: 0,
                avg_latency: 0.0,
            }),
            collection_names: RwLock::new(HashSet::new()),
            storage_devices: RwLock::new(HashMap::new()),
            device_status_overrides: RwLock::new(HashMap::new()),
            device_health_overrides: RwLock::new(HashMap::new()),
            migration_tasks: RwLock::new(HashMap::new()),
            start_time: Instant::now(),
        }
    }
}

pub type MetricStoreRef = Arc<MetricStore>;
