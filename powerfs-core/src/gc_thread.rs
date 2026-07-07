use crate::storage::StorageManager;
use chrono::Duration;
use log::{info, warn};
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::time;

pub struct GcThread {
    storage_manager: Arc<StorageManager>,
    interval: Duration,
}

impl GcThread {
    pub fn start(storage_manager: Arc<StorageManager>, interval_hours: u32) -> oneshot::Sender<()> {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        tokio::spawn(async move {
            let gc = GcThread {
                storage_manager,
                interval: Duration::hours(interval_hours as i64),
            };
            gc.run(shutdown_rx).await;
        });

        shutdown_tx
    }

    async fn run(self, mut shutdown_rx: oneshot::Receiver<()>) {
        info!("GC thread started with interval: {:?}", self.interval);

        loop {
            tokio::select! {
                _ = &mut shutdown_rx => {
                    info!("GC thread shutting down");
                    return;
                }
                _ = time::sleep(self.interval.to_std().unwrap()) => {
                    self.run_cleanup().await;
                }
            }
        }
    }

    async fn run_cleanup(&self) {
        info!("Starting GC cleanup");

        let volumes = self.storage_manager.list_volumes();
        let mut total_cleaned = 0;

        for volume_info in volumes {
            if let Some(volume) = self.storage_manager.get_volume(&volume_info.id) {
                match volume.gc_cleanup() {
                    Ok(count) => {
                        total_cleaned += count;
                        if count > 0 {
                            info!(
                                "Cleaned {} expired needles from volume {}",
                                count, volume_info.id.0
                            );
                        }
                    }
                    Err(e) => {
                        warn!("GC cleanup failed for volume {}: {}", volume_info.id.0, e);
                    }
                }
            }
        }

        info!("GC cleanup completed, total cleaned: {}", total_cleaned);
    }
}
