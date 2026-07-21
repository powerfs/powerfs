use log::{debug, info, warn};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::time::interval;

use crate::raft_group_manager::{RaftGroupManager, ShardId};
use crate::shard_strategy::ShardStrategy;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NodeMetrics {
    pub node_id: String,
    pub address: String,
    pub cpu_usage: f64,
    pub memory_usage: f64,
    pub disk_available: u64,
    pub disk_total: u64,
    pub disk_iops: u64,
    pub network_bandwidth: u64,
    pub leader_count: u64,
    pub qps: u64,
    pub is_healthy: bool,
    pub health_score: f64,
    pub consecutive_high_load: usize,
}

impl NodeMetrics {
    pub fn new(node_id: &str, address: &str) -> Self {
        Self {
            node_id: node_id.to_string(),
            address: address.to_string(),
            cpu_usage: 0.0,
            memory_usage: 0.0,
            disk_available: 0,
            disk_total: 1,
            disk_iops: 0,
            network_bandwidth: 0,
            leader_count: 0,
            qps: 0,
            is_healthy: true,
            health_score: 1.0,
            consecutive_high_load: 0,
        }
    }

    pub fn disk_available_ratio(&self) -> f64 {
        if self.disk_total == 0 {
            0.0
        } else {
            self.disk_available as f64 / self.disk_total as f64
        }
    }
}

#[derive(Debug, Clone)]
pub struct MigrationPlan {
    pub shard_id: ShardId,
    pub from_node_id: u64,
    pub from_node_address: String,
    pub to_node_id: u64,
    pub to_node_address: String,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    pub check_interval: Duration,
    pub max_transfers_per_round: usize,
    pub transfer_interval: Duration,
    pub cooldown_periods: usize,
    pub leader_imbalance_threshold: f64,
    pub cpu_threshold: f64,
    pub memory_threshold: f64,
    pub disk_threshold: f64,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(60),
            max_transfers_per_round: 2,
            transfer_interval: Duration::from_secs(10),
            cooldown_periods: 5,
            leader_imbalance_threshold: 1.5,
            cpu_threshold: 0.8,
            memory_threshold: 0.85,
            disk_threshold: 0.1,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SchedulerStatus {
    pub is_running: bool,
    pub last_check_time: u64,
    pub total_migrations: u64,
    pub successful_migrations: u64,
    pub failed_migrations: u64,
    pub node_count: usize,
    pub shard_count: usize,
    pub leader_distribution: HashMap<String, u64>,
}

pub struct ShardScheduler {
    raft_group_manager: Arc<RaftGroupManager>,
    shard_strategy: Arc<ShardStrategy>,
    node_metrics: RwLock<HashMap<String, NodeMetrics>>,
    config: SchedulerConfig,
    running: Arc<RwLock<bool>>,
    total_migrations: Arc<std::sync::atomic::AtomicU64>,
    successful_migrations: Arc<std::sync::atomic::AtomicU64>,
    failed_migrations: Arc<std::sync::atomic::AtomicU64>,
    last_check_time: Arc<std::sync::atomic::AtomicU64>,
}

impl ShardScheduler {
    pub fn new(
        raft_group_manager: Arc<RaftGroupManager>,
        shard_strategy: Arc<ShardStrategy>,
    ) -> Self {
        Self {
            raft_group_manager,
            shard_strategy,
            node_metrics: RwLock::new(HashMap::new()),
            config: SchedulerConfig::default(),
            running: Arc::new(RwLock::new(false)),
            total_migrations: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            successful_migrations: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            failed_migrations: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            last_check_time: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    pub fn new_with_config(
        raft_group_manager: Arc<RaftGroupManager>,
        shard_strategy: Arc<ShardStrategy>,
        config: SchedulerConfig,
    ) -> Self {
        Self {
            raft_group_manager,
            shard_strategy,
            node_metrics: RwLock::new(HashMap::new()),
            config,
            running: Arc::new(RwLock::new(false)),
            total_migrations: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            successful_migrations: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            failed_migrations: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            last_check_time: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    pub fn set_config(&mut self, config: SchedulerConfig) {
        self.config = config;
    }

    pub fn register_node(&self, node_id: &str, address: &str) {
        let mut metrics = self.node_metrics.write().unwrap();
        metrics.entry(node_id.to_string()).or_insert_with(|| {
            info!("Registered node: id={}, address={}", node_id, address);
            NodeMetrics::new(node_id, address)
        });
    }

    pub fn update_node_metrics(&self, node_id: &str, metrics: NodeMetrics) {
        let mut node_metrics = self.node_metrics.write().unwrap();
        if node_metrics.contains_key(node_id) {
            node_metrics.insert(node_id.to_string(), metrics);
            debug!("Updated metrics for node: {}", node_id);
        } else {
            warn!("Node {} not registered, ignoring metrics update", node_id);
        }
    }

    pub fn get_node_metrics(&self, node_id: &str) -> Option<NodeMetrics> {
        self.node_metrics.read().unwrap().get(node_id).cloned()
    }

    pub fn list_node_metrics(&self) -> Vec<NodeMetrics> {
        self.node_metrics
            .read()
            .unwrap()
            .values()
            .cloned()
            .collect()
    }

    fn calculate_node_score(&self, node_id: &str) -> f64 {
        let node_metrics_lock = self.node_metrics.read().unwrap();
        let metrics = match node_metrics_lock.get(node_id) {
            Some(m) => m,
            None => return f64::MAX,
        };

        if !metrics.is_healthy {
            return f64::MAX;
        }

        let avg_leader_count = self.average_leader_count();
        let max_qps = self.max_qps();

        let cpu_score = metrics.cpu_usage * 0.20;
        let memory_score = metrics.memory_usage * 0.15;
        let disk_score = (1.0 - metrics.disk_available_ratio()) * 0.15;

        let leader_score = if avg_leader_count > 0.0 {
            (metrics.leader_count as f64 / avg_leader_count) * 0.30
        } else {
            0.0
        };

        let qps_score = if max_qps > 0 {
            (metrics.qps as f64 / max_qps as f64) * 0.20
        } else {
            0.0
        };

        cpu_score + memory_score + disk_score + leader_score + qps_score
    }

    fn calculate_node_score_by_address(&self, address: &str) -> f64 {
        let node_metrics_lock = self.node_metrics.read().unwrap();
        for (id, metrics) in node_metrics_lock.iter() {
            if metrics.address == address {
                return self.calculate_node_score(id);
            }
        }
        f64::MAX
    }

    fn average_leader_count(&self) -> f64 {
        let metrics = self.node_metrics.read().unwrap();
        if metrics.is_empty() {
            return 0.0;
        }
        let total: u64 = metrics.values().map(|m| m.leader_count).sum();
        total as f64 / metrics.len() as f64
    }

    fn max_qps(&self) -> u64 {
        let metrics = self.node_metrics.read().unwrap();
        metrics.values().map(|m| m.qps).max().unwrap_or(0)
    }

    async fn collect_shard_leader_distribution(&self) -> HashMap<String, Vec<ShardId>> {
        let mut distribution: HashMap<String, Vec<ShardId>> = HashMap::new();
        let shards = self.raft_group_manager.list_shards().await;

        for shard_id in shards {
            if let Some(leader_addr) = self.raft_group_manager.get_shard_leader(shard_id).await {
                distribution.entry(leader_addr).or_default().push(shard_id);
            }
        }

        distribution
    }

    async fn collect_node_leader_counts(&self) {
        let distribution = self.collect_shard_leader_distribution().await;
        let mut metrics = self.node_metrics.write().unwrap();

        for (addr, shards) in distribution {
            for node_metrics in metrics.values_mut() {
                if node_metrics.address == addr {
                    node_metrics.leader_count = shards.len() as u64;
                }
            }
        }
    }

    async fn analyze_leader_distribution(&self) -> Vec<MigrationPlan> {
        let distribution = self.collect_shard_leader_distribution().await;
        let mut plans = Vec::new();

        if distribution.is_empty() {
            return plans;
        }

        let total_leaders: usize = distribution.values().map(|v| v.len()).sum();
        let node_count = distribution.len();
        let avg_leaders = total_leaders as f64 / node_count as f64;
        let threshold = avg_leaders * self.config.leader_imbalance_threshold;

        info!(
            "Leader distribution analysis: {} total leaders, {} nodes, avg={:.2}, threshold={:.2}",
            total_leaders, node_count, avg_leaders, threshold
        );

        let mut high_load_nodes: Vec<(f64, &String, &Vec<ShardId>)> = distribution
            .iter()
            .filter(|(_, shards)| shards.len() as f64 > threshold)
            .map(|(addr, shards)| {
                let score = self.calculate_node_score_by_address(addr);
                (score, addr, shards)
            })
            .collect();

        let mut low_load_nodes: Vec<(f64, &String, &Vec<ShardId>)> = distribution
            .iter()
            .filter(|(_, shards)| (shards.len() as f64) < avg_leaders * 0.7)
            .map(|(addr, shards)| {
                let score = self.calculate_node_score_by_address(addr);
                (score, addr, shards)
            })
            .collect();

        high_load_nodes.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        low_load_nodes.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

        for (_high_score, high_addr, high_shards) in high_load_nodes {
            for &shard_id in high_shards {
                if plans.len() >= self.config.max_transfers_per_round {
                    return plans;
                }

                for (_low_score, low_addr, _) in &low_load_nodes {
                    if self.can_transfer_leader(shard_id, low_addr).await {
                        let from_node_id = self.get_node_id_by_address(high_addr);
                        let to_node_id = self.get_node_id_by_address(low_addr);

                        plans.push(MigrationPlan {
                            shard_id,
                            from_node_id,
                            from_node_address: high_addr.clone(),
                            to_node_id,
                            to_node_address: low_addr.to_string(),
                            reason: format!(
                                "leader imbalance: {} has {} leaders (threshold {:.2})",
                                high_addr,
                                high_shards.len(),
                                threshold
                            ),
                        });
                        break;
                    }
                }
            }
        }

        plans
    }

    async fn can_transfer_leader(&self, shard_id: ShardId, target_addr: &str) -> bool {
        let group_arc = match self.raft_group_manager.get_group(shard_id).await {
            Some(g) => g,
            None => return false,
        };

        let group = group_arc.read().await;
        let peers = group.get_peers();

        peers.iter().any(|p| p.address == target_addr)
    }

    fn get_node_id_by_address(&self, address: &str) -> u64 {
        let metrics = self.node_metrics.read().unwrap();
        for (id, m) in metrics.iter() {
            if m.address == address {
                return id.parse().unwrap_or(0);
            }
        }
        0
    }

    async fn execute_migrations(&self, plans: Vec<MigrationPlan>) {
        for plan in plans {
            info!(
                "Executing migration: shard {} from {} to {}",
                plan.shard_id.0, plan.from_node_address, plan.to_node_address
            );

            self.total_migrations
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            let result = self
                .raft_group_manager
                .transfer_shard_leader(plan.shard_id, plan.to_node_id)
                .await;

            match result {
                Ok(_) => {
                    info!(
                        "Migration successful: shard {} transferred to {}",
                        plan.shard_id.0, plan.to_node_address
                    );
                    self.successful_migrations
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Err(e) => {
                    warn!(
                        "Migration failed: shard {} to {} - {}",
                        plan.shard_id.0, plan.to_node_address, e
                    );
                    self.failed_migrations
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }

            tokio::time::sleep(self.config.transfer_interval).await;
        }
    }

    async fn check_and_balance(&self) {
        self.last_check_time.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            std::sync::atomic::Ordering::Relaxed,
        );

        info!("Starting shard balancing check...");

        self.collect_node_leader_counts().await;

        let plans = self.analyze_leader_distribution().await;

        if !plans.is_empty() {
            info!("Found {} migration plans", plans.len());
            for plan in &plans {
                info!(
                    "Plan: shard {} -> {}",
                    plan.shard_id.0, plan.to_node_address
                );
            }
            self.execute_migrations(plans).await;
        } else {
            info!("No migrations needed, cluster is balanced");
        }
    }

    pub async fn run(&self) {
        info!(
            "Starting ShardScheduler with interval {:?}",
            self.config.check_interval
        );

        {
            let mut running = self.running.write().unwrap();
            *running = true;
        }

        let mut tick_interval = interval(self.config.check_interval);

        while *self.running.read().unwrap() {
            tokio::select! {
                _ = tick_interval.tick() => {
                    self.check_and_balance().await;
                }
            }
        }

        info!("ShardScheduler stopped");
    }

    pub async fn stop(&self) {
        info!("Stopping ShardScheduler...");
        let mut running = self.running.write().unwrap();
        *running = false;
    }

    pub async fn get_status(&self) -> SchedulerStatus {
        let distribution = self.collect_shard_leader_distribution().await;
        let leader_distribution: HashMap<String, u64> = distribution
            .into_iter()
            .map(|(k, v)| (k, v.len() as u64))
            .collect();

        SchedulerStatus {
            is_running: *self.running.read().unwrap(),
            last_check_time: self
                .last_check_time
                .load(std::sync::atomic::Ordering::Relaxed),
            total_migrations: self
                .total_migrations
                .load(std::sync::atomic::Ordering::Relaxed),
            successful_migrations: self
                .successful_migrations
                .load(std::sync::atomic::Ordering::Relaxed),
            failed_migrations: self
                .failed_migrations
                .load(std::sync::atomic::Ordering::Relaxed),
            node_count: self.node_metrics.read().unwrap().len(),
            shard_count: self.shard_strategy.get_shard_count() as usize,
            leader_distribution,
        }
    }

    pub async fn trigger_balance(&self) {
        info!("Manual balance triggered");
        self.check_and_balance().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_scheduler_initialization() {
        let tmp_dir = tempdir().unwrap();
        let data_path = tmp_dir.path().to_str().unwrap().to_string();

        let shard_strategy = Arc::new(ShardStrategy::new(4));
        let raft_group_manager = Arc::new(RaftGroupManager::new(
            1,
            "127.0.0.1:50051".to_string(),
            data_path,
        ));
        let scheduler = ShardScheduler::new(raft_group_manager, shard_strategy);

        let status = scheduler.get_status().await;
        assert!(!status.is_running);
        assert_eq!(status.node_count, 0);
        assert_eq!(status.shard_count, 4);
    }

    #[tokio::test]
    async fn test_register_node() {
        let tmp_dir = tempdir().unwrap();
        let data_path = tmp_dir.path().to_str().unwrap().to_string();

        let shard_strategy = Arc::new(ShardStrategy::new(4));
        let raft_group_manager = Arc::new(RaftGroupManager::new(
            1,
            "127.0.0.1:50051".to_string(),
            data_path,
        ));
        let scheduler = ShardScheduler::new(raft_group_manager, shard_strategy);

        scheduler.register_node("1", "127.0.0.1:50051");
        scheduler.register_node("2", "127.0.0.1:50052");
        scheduler.register_node("3", "127.0.0.1:50053");

        let status = scheduler.get_status().await;
        assert_eq!(status.node_count, 3);

        let metrics = scheduler.get_node_metrics("1").unwrap();
        assert_eq!(metrics.node_id, "1");
        assert_eq!(metrics.address, "127.0.0.1:50051");
        assert!(metrics.is_healthy);
    }

    #[tokio::test]
    async fn test_calculate_node_score() {
        let tmp_dir = tempdir().unwrap();
        let data_path = tmp_dir.path().to_str().unwrap().to_string();

        let shard_strategy = Arc::new(ShardStrategy::new(4));
        let raft_group_manager = Arc::new(RaftGroupManager::new(
            1,
            "127.0.0.1:50051".to_string(),
            data_path,
        ));
        let scheduler = ShardScheduler::new(raft_group_manager, shard_strategy);

        scheduler.register_node("1", "127.0.0.1:50051");

        let mut metrics = scheduler.get_node_metrics("1").unwrap();
        metrics.cpu_usage = 0.5;
        metrics.memory_usage = 0.6;
        metrics.disk_available = 50 * 1024 * 1024 * 1024;
        metrics.disk_total = 100 * 1024 * 1024 * 1024;
        metrics.leader_count = 2;
        metrics.qps = 100;

        scheduler.update_node_metrics("1", metrics);

        let score = scheduler.calculate_node_score("1");
        assert!(score < 1.0);
        assert!(score > 0.0);
    }

    #[tokio::test]
    async fn test_node_metrics_disk_ratio() {
        let mut metrics = NodeMetrics::new("1", "127.0.0.1:50051");
        metrics.disk_available = 50;
        metrics.disk_total = 100;

        assert_eq!(metrics.disk_available_ratio(), 0.5);

        metrics.disk_total = 0;
        assert_eq!(metrics.disk_available_ratio(), 0.0);
    }

    #[tokio::test]
    async fn test_scheduler_config_default() {
        let config = SchedulerConfig::default();

        assert_eq!(config.check_interval, Duration::from_secs(60));
        assert_eq!(config.max_transfers_per_round, 2);
        assert_eq!(config.transfer_interval, Duration::from_secs(10));
        assert_eq!(config.cooldown_periods, 5);
        assert_eq!(config.leader_imbalance_threshold, 1.5);
    }

    #[tokio::test]
    async fn test_scheduler_status() {
        let tmp_dir = tempdir().unwrap();
        let data_path = tmp_dir.path().to_str().unwrap().to_string();

        let shard_strategy = Arc::new(ShardStrategy::new(4));
        let raft_group_manager = Arc::new(RaftGroupManager::new(
            1,
            "127.0.0.1:50051".to_string(),
            data_path,
        ));
        let scheduler = ShardScheduler::new(raft_group_manager, shard_strategy);

        scheduler.register_node("1", "127.0.0.1:50051");

        let status = scheduler.get_status().await;
        assert!(!status.is_running);
        assert_eq!(status.node_count, 1);
        assert_eq!(status.total_migrations, 0);
        assert_eq!(status.successful_migrations, 0);
        assert_eq!(status.failed_migrations, 0);
    }
}
