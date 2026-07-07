use crate::storage::StorageManager;
use chrono::{DateTime, Duration, Utc};
use log::{info, warn};
use powerfs_common::types::{NeedleId, VolumeId};
use priority_queue::PriorityQueue;
use std::sync::Arc;
use tokio::sync::{oneshot, RwLock};
use tokio::time;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairPriority {
    Critical,
    High,
    Medium,
    Low,
}

impl PartialOrd for RepairPriority {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RepairPriority {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (RepairPriority::Critical, _) => std::cmp::Ordering::Greater,
            (_, RepairPriority::Critical) => std::cmp::Ordering::Less,
            (RepairPriority::High, _) => std::cmp::Ordering::Greater,
            (_, RepairPriority::High) => std::cmp::Ordering::Less,
            (RepairPriority::Medium, _) => std::cmp::Ordering::Greater,
            (_, RepairPriority::Medium) => std::cmp::Ordering::Less,
            (RepairPriority::Low, RepairPriority::Low) => std::cmp::Ordering::Equal,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RepairTask {
    pub volume_id: u32,
    pub needle_id: u64,
    pub priority: RepairPriority,
    pub error_type: String,
    pub created_at: DateTime<Utc>,
    pub attempts: u32,
}

pub struct RepairQueue {
    queue: RwLock<PriorityQueue<(u32, u64), RepairPriority>>,
    tasks: RwLock<std::collections::HashMap<(u32, u64), RepairTask>>,
}

impl Default for RepairQueue {
    fn default() -> Self {
        RepairQueue {
            queue: RwLock::new(PriorityQueue::new()),
            tasks: RwLock::new(std::collections::HashMap::new()),
        }
    }
}

impl RepairQueue {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn add_task(&self, task: RepairTask) {
        let key = (task.volume_id, task.needle_id);
        {
            let mut tasks = self.tasks.write().await;
            tasks.insert(key, task.clone());
        }
        {
            let mut queue = self.queue.write().await;
            queue.push(key, task.priority);
        }
        info!(
            "Added repair task: volume={}, needle={}, priority={:?}",
            task.volume_id, task.needle_id, task.priority
        );
    }

    pub async fn get_next_task(&self) -> Option<RepairTask> {
        let mut queue = self.queue.write().await;
        if let Some((key, _)) = queue.pop() {
            let mut tasks = self.tasks.write().await;
            tasks.remove(&key)
        } else {
            None
        }
    }

    pub async fn task_count(&self) -> usize {
        self.queue.read().await.len()
    }

    pub async fn remove_task(&self, volume_id: u32, needle_id: u64) {
        let key = (volume_id, needle_id);
        {
            let mut queue = self.queue.write().await;
            queue.remove(&key);
        }
        {
            let mut tasks = self.tasks.write().await;
            tasks.remove(&key);
        }
    }
}

pub struct RepairThread {
    storage_manager: Arc<StorageManager>,
    repair_queue: Arc<RepairQueue>,
    max_attempts: u32,
}

impl RepairThread {
    pub fn start(
        storage_manager: Arc<StorageManager>,
        repair_queue: Arc<RepairQueue>,
        max_attempts: u32,
    ) -> oneshot::Sender<()> {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        tokio::spawn(async move {
            let repair = RepairThread {
                storage_manager,
                repair_queue,
                max_attempts,
            };
            repair.run(shutdown_rx).await;
        });

        shutdown_tx
    }

    async fn run(self, mut shutdown_rx: oneshot::Receiver<()>) {
        info!("Repair thread started");

        loop {
            tokio::select! {
                _ = &mut shutdown_rx => {
                    info!("Repair thread shutting down");
                    return;
                }
                _ = time::sleep(Duration::seconds(5).to_std().unwrap()) => {
                    self.process_tasks().await;
                }
            }
        }
    }

    async fn process_tasks(&self) {
        let count = self.repair_queue.task_count().await;
        if count == 0 {
            return;
        }

        info!("Processing {} repair tasks", count);

        while let Some(mut task) = self.repair_queue.get_next_task().await {
            if task.attempts >= self.max_attempts {
                warn!(
                    "Max attempts reached for task: volume={}, needle={}",
                    task.volume_id, task.needle_id
                );
                continue;
            }

            task.attempts += 1;

            if self.attempt_repair(&task).await.is_err() {
                warn!(
                    "Repair attempt {} failed for volume={}, needle={}",
                    task.attempts, task.volume_id, task.needle_id
                );
                self.repair_queue.add_task(task).await;
            } else {
                info!(
                    "Repair succeeded for volume={}, needle={}",
                    task.volume_id, task.needle_id
                );
            }
        }
    }

    async fn attempt_repair(&self, task: &RepairTask) -> Result<(), ()> {
        let volume_id = VolumeId(task.volume_id);

        if let Some(volume) = self.storage_manager.get_volume(&volume_id) {
            if let Some(info) = volume.get_needle_info(&NeedleId(task.needle_id)) {
                if info.deleted_at.is_some() {
                    return Err(());
                }

                if let Ok(data) = volume.read_needle(&NeedleId(task.needle_id)) {
                    if let Ok(info) = volume.write_needle(task.needle_id, data.clone()) {
                        info!(
                            "Repaired needle: volume={}, needle={}, offset={}",
                            task.volume_id, task.needle_id, info.offset
                        );
                        return Ok(());
                    }
                }
            }
        }

        Err(())
    }
}
