use crate::repair_thread::{RepairPriority, RepairQueue, RepairTask};
use crate::storage::StorageManager;
use chrono::{DateTime, Duration, Utc};
use log::{info, warn};
use powerfs_common::types::{NeedleId, VolumeId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use tokio::time;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScrubState {
    Idle,
    Running,
    Paused,
    Completed,
    Failed(String),
}

#[derive(Debug, Clone)]
pub struct VolumeScrubStatus {
    pub volume_id: VolumeId,
    pub state: ScrubState,
    pub progress: f64,
    pub total_needles: u64,
    pub verified_needles: u64,
    pub corrupted_needles: u64,
    pub last_scrub_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub error: Option<String>,
}

impl Default for VolumeScrubStatus {
    fn default() -> Self {
        VolumeScrubStatus {
            volume_id: VolumeId(0),
            state: ScrubState::Idle,
            progress: 0.0,
            total_needles: 0,
            verified_needles: 0,
            corrupted_needles: 0,
            last_scrub_at: None,
            started_at: None,
            error: None,
        }
    }
}

pub struct BitrotScanner {
    storage_manager: Arc<StorageManager>,
    scan_interval: Duration,
    repair_queue: Option<Arc<RepairQueue>>,
    status_map: Mutex<HashMap<VolumeId, VolumeScrubStatus>>,
}

impl BitrotScanner {
    pub fn start(
        storage_manager: Arc<StorageManager>,
        scan_interval_hours: u32,
        repair_queue: Option<Arc<RepairQueue>>,
    ) -> oneshot::Sender<()> {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        tokio::spawn(async move {
            let scanner = Arc::new(BitrotScanner {
                storage_manager,
                scan_interval: Duration::hours(scan_interval_hours as i64),
                repair_queue,
                status_map: Mutex::new(HashMap::new()),
            });
            scanner.run(shutdown_rx).await;
        });

        shutdown_tx
    }

    fn init_status(&self, volume_id: VolumeId) {
        let mut map = self.status_map.lock().unwrap();
        map.entry(volume_id).or_insert_with(|| VolumeScrubStatus {
            volume_id,
            ..Default::default()
        });
    }

    fn update_status(&self, volume_id: &VolumeId, updater: impl FnOnce(&mut VolumeScrubStatus)) {
        let mut map = self.status_map.lock().unwrap();
        if let Some(status) = map.get_mut(volume_id) {
            updater(status);
        }
    }

    pub fn get_status(&self, volume_id: &VolumeId) -> Option<VolumeScrubStatus> {
        self.status_map.lock().unwrap().get(volume_id).cloned()
    }

    pub fn get_all_status(&self) -> Vec<VolumeScrubStatus> {
        self.status_map.lock().unwrap().values().cloned().collect()
    }

    async fn run(self: Arc<Self>, mut shutdown_rx: oneshot::Receiver<()>) {
        info!(
            "Bitrot scanner started with interval: {:?}",
            self.scan_interval
        );

        loop {
            tokio::select! {
                _ = &mut shutdown_rx => {
                    info!("Bitrot scanner shutting down");
                    return;
                }
                _ = time::sleep(self.scan_interval.to_std().unwrap()) => {
                    self.scan_all_volumes().await;
                }
            }
        }
    }

    pub async fn trigger_scan_all(&self) {
        info!("Manual trigger: scanning all volumes");
        self.scan_all_volumes().await;
    }

    pub async fn trigger_scan_volume(&self, volume_id: &VolumeId) -> Result<(), String> {
        info!("Manual trigger: scanning volume {}", volume_id.0);
        self.init_status(*volume_id);
        self.scan_volume(volume_id).await.map_err(|e| e.to_string())
    }

    async fn scan_all_volumes(&self) {
        info!("Starting bitrot scan of all volumes");

        let volumes = self.storage_manager.list_volumes();
        let mut scanned = 0;
        let mut corrupted_total = 0;

        for volume_info in volumes {
            if volume_info.state == powerfs_common::types::VolumeState::Available {
                self.init_status(volume_info.id);
                match self.scan_volume(&volume_info.id).await {
                    Ok(_) => {
                        scanned += 1;
                        if let Some(status) = self.get_status(&volume_info.id) {
                            corrupted_total += status.corrupted_needles;
                        }
                    }
                    Err(e) => {
                        warn!("Failed to scan volume {}: {}", volume_info.id.0, e);
                        self.update_status(&volume_info.id, |s| {
                            s.state = ScrubState::Failed(e.to_string());
                            s.error = Some(e.to_string());
                        });
                    }
                }
            }
        }

        info!(
            "Bitrot scan completed: {} volumes scanned, {} corrupted needles found",
            scanned, corrupted_total
        );
    }

    async fn scan_volume(
        &self,
        volume_id: &VolumeId,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Scanning volume: {}", volume_id.0);

        self.update_status(volume_id, |s| {
            s.state = ScrubState::Running;
            s.started_at = Some(Utc::now());
            s.progress = 0.0;
            s.verified_needles = 0;
            s.corrupted_needles = 0;
            s.error = None;
        });

        let volume = self
            .storage_manager
            .get_volume(volume_id)
            .ok_or_else(|| format!("volume {} not found", volume_id.0))?;

        let scrub_result = volume.scrub_volume();

        if scrub_result.corrupted > 0 {
            warn!(
                "Found {} corrupted needles in volume {}",
                scrub_result.corrupted, volume_id.0
            );
            self.submit_repair_tasks(volume_id, &scrub_result.corrupted_needles)
                .await;
        }

        self.update_status(volume_id, |s| {
            s.state = ScrubState::Completed;
            s.total_needles = scrub_result.total;
            s.verified_needles = scrub_result.verified;
            s.corrupted_needles = scrub_result.corrupted;
            s.progress = if scrub_result.total > 0 { 1.0 } else { 0.0 };
            s.last_scrub_at = Some(Utc::now());
        });

        info!(
            "Scanned volume {}: total={}, verified={}, corrupted={}, skipped={}, errors={}",
            volume_id.0,
            scrub_result.total,
            scrub_result.verified,
            scrub_result.corrupted,
            scrub_result.skipped,
            scrub_result.errors
        );

        Ok(())
    }

    async fn submit_repair_tasks(&self, volume_id: &VolumeId, corrupted: &[NeedleId]) {
        if let Some(ref queue) = self.repair_queue {
            for needle_id in corrupted {
                let task = RepairTask {
                    volume_id: volume_id.0,
                    needle_id: needle_id.0,
                    priority: RepairPriority::High,
                    error_type: "checksum_mismatch".to_string(),
                    created_at: Utc::now(),
                    attempts: 0,
                };
                queue.add_task(task).await;
            }
            info!(
                "Submitted {} repair tasks for volume {}",
                corrupted.len(),
                volume_id.0
            );
        }
    }
}
