use crate::repair_thread::{RepairPriority, RepairQueue, RepairTask};
use crate::storage::StorageManager;
use chrono::{Duration, Utc};
use log::{info, warn};
use powerfs_common::types::VolumeId;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::time;

pub struct BitrotScanner {
    storage_manager: Arc<StorageManager>,
    scan_interval: Duration,
    repair_queue: Option<Arc<RepairQueue>>,
}

impl BitrotScanner {
    pub fn start(
        storage_manager: Arc<StorageManager>,
        scan_interval_hours: u32,
        repair_queue: Option<Arc<RepairQueue>>,
    ) -> oneshot::Sender<()> {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        tokio::spawn(async move {
            let scanner = BitrotScanner {
                storage_manager,
                scan_interval: Duration::hours(scan_interval_hours as i64),
                repair_queue,
            };
            scanner.run(shutdown_rx).await;
        });

        shutdown_tx
    }

    async fn run(self, mut shutdown_rx: oneshot::Receiver<()>) {
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

    async fn scan_all_volumes(&self) {
        info!("Starting bitrot scan");

        let volumes = self.storage_manager.list_volumes();
        for volume_info in volumes {
            if volume_info.state == powerfs_common::types::VolumeState::Available {
                if let Err(e) = self.scan_volume(&volume_info.id).await {
                    warn!("Failed to scan volume {}: {}", volume_info.id.0, e);
                }
            }
        }

        info!("Bitrot scan completed");
    }

    async fn scan_volume(&self, volume_id: &VolumeId) -> Result<(), Box<dyn std::error::Error>> {
        info!("Scanning volume: {}", volume_id.0);

        if let Some(volume) = self.storage_manager.get_volume(volume_id) {
            let needles = volume.index().iter();
            let count = needles.len();

            for (needle_id, info) in needles {
                if info.deleted_at.is_none() {
                    if let Err(e) = self.verify_needle(&volume, &needle_id, &info).await {
                        warn!(
                            "Checksum mismatch for needle {} in volume {}: {}",
                            needle_id.0, volume_id.0, e
                        );
                    }
                }
            }

            info!("Scanned {} needles in volume {}", count, volume_id.0);
        }

        Ok(())
    }

    async fn verify_needle(
        &self,
        volume: &Arc<crate::volume::Volume>,
        needle_id: &powerfs_common::types::NeedleId,
        info: &powerfs_common::types::NeedleInfo,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let data = volume.read_needle(needle_id)?;

        let computed_checksum =
            powerfs_common::utils::Checksum::compute(&data, info.checksum_algorithm);
        if computed_checksum.as_u64() != info.checksum {
            if let Some(ref queue) = self.repair_queue {
                let task = RepairTask {
                    volume_id: volume.id().0,
                    needle_id: needle_id.0,
                    priority: RepairPriority::High,
                    error_type: "checksum_mismatch".to_string(),
                    created_at: Utc::now(),
                    attempts: 0,
                };
                queue.add_task(task).await;
            }
            return Err("checksum mismatch".into());
        }

        Ok(())
    }
}
