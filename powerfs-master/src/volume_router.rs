use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Instant, Duration};

use log::{info, warn};
use powerfs_common::types::{DataNodeInfo, DiskType, NodeId, VolumeId, VolumeState};

use crate::master::MasterNode;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LoadBalanceStrategy {
    RoundRobin,
    LeastLoaded,
    Random,
}

#[derive(Debug)]
pub struct VolumeRouter {
    master: Arc<MasterNode>,
    strategy: RwLock<LoadBalanceStrategy>,
    volume_node_cache: RwLock<HashMap<VolumeId, Vec<DataNodeInfo>>>,
    round_robin_counters: RwLock<HashMap<VolumeId, usize>>,
    node_load_cache: RwLock<HashMap<NodeId, NodeLoad>>,
    last_load_update: RwLock<Instant>,
}

#[derive(Debug, Clone)]
struct NodeLoad {
    disk_usage: f64,
    cpu_usage: f64,
    memory_usage: f64,
    active_connections: u32,
    last_update: Instant,
}

impl Default for NodeLoad {
    fn default() -> Self {
        NodeLoad {
            disk_usage: 0.0,
            cpu_usage: 0.0,
            memory_usage: 0.0,
            active_connections: 0,
            last_update: Instant::now(),
        }
    }
}

impl VolumeRouter {
    pub fn new(master: Arc<MasterNode>) -> Self {
        VolumeRouter {
            master,
            strategy: RwLock::new(LoadBalanceStrategy::RoundRobin),
            volume_node_cache: RwLock::new(HashMap::new()),
            round_robin_counters: RwLock::new(HashMap::new()),
            node_load_cache: RwLock::new(HashMap::new()),
            last_load_update: RwLock::new(Instant::now()),
        }
    }

    pub fn set_strategy(&self, strategy: LoadBalanceStrategy) {
        let old_strategy = *self.strategy.read().unwrap();
        *self.strategy.write().unwrap() = strategy;
        info!("Volume router strategy changed from {:?} to {:?}", old_strategy, strategy);
    }

    pub fn get_strategy(&self) -> LoadBalanceStrategy {
        *self.strategy.read().unwrap()
    }

    pub fn get_volume_nodes(&self, volume_id: VolumeId) -> Vec<DataNodeInfo> {
        self.refresh_cache_if_needed();

        if let Some(nodes) = self.volume_node_cache.read().unwrap().get(&volume_id) {
            return nodes.clone();
        }

        self.fetch_and_cache_volume_nodes(volume_id)
    }

    pub fn select_node(&self, volume_id: VolumeId) -> Option<DataNodeInfo> {
        let nodes = self.get_volume_nodes(volume_id);
        if nodes.is_empty() {
            warn!("No nodes found for volume {}", volume_id);
            return None;
        }

        let strategy = self.get_strategy();
        match strategy {
            LoadBalanceStrategy::RoundRobin => self.select_round_robin(volume_id, &nodes),
            LoadBalanceStrategy::LeastLoaded => self.select_least_loaded(&nodes),
            LoadBalanceStrategy::Random => self.select_random(&nodes),
        }
    }

    pub fn select_nodes(&self, volume_id: VolumeId, count: usize) -> Vec<DataNodeInfo> {
        let nodes = self.get_volume_nodes(volume_id);
        if nodes.is_empty() {
            return Vec::new();
        }

        let strategy = self.get_strategy();
        match strategy {
            LoadBalanceStrategy::RoundRobin => self.select_multiple_round_robin(volume_id, &nodes, count),
            LoadBalanceStrategy::LeastLoaded => self.select_multiple_least_loaded(&nodes, count),
            LoadBalanceStrategy::Random => self.select_multiple_random(&nodes, count),
        }
    }

    pub fn get_all_available_volumes(&self) -> Vec<VolumeId> {
        let volumes = self.master.volumes.read().unwrap();
        volumes
            .iter()
            .filter(|(_, info)| info.state == VolumeState::Up)
            .map(|(id, _)| *id)
            .collect()
    }

    pub fn get_volumes_by_disk_type(&self, disk_type: DiskType) -> Vec<VolumeId> {
        let volumes = self.master.volumes.read().unwrap();
        volumes
            .iter()
            .filter(|(_, info)| info.state == VolumeState::Up && info.disk_type == disk_type)
            .map(|(id, _)| *id)
            .collect()
    }

    pub fn get_volume_load(&self, volume_id: VolumeId) -> f64 {
        let nodes = self.get_volume_nodes(volume_id);
        if nodes.is_empty() {
            return 1.0;
        }

        let load_cache = self.node_load_cache.read().unwrap();
        let total_load: f64 = nodes
            .iter()
            .filter_map(|node| load_cache.get(&node.node_id))
            .map(|load| (load.disk_usage + load.cpu_usage + load.memory_usage) / 3.0)
            .sum();

        total_load / nodes.len() as f64
    }

    pub fn update_node_load(&self, node_id: NodeId, disk_usage: f64, cpu_usage: f64, memory_usage: f64, connections: u32) {
        let mut load_cache = self.node_load_cache.write().unwrap();
        let entry = load_cache.entry(node_id).or_insert_with(Default::default);
        entry.disk_usage = disk_usage;
        entry.cpu_usage = cpu_usage;
        entry.memory_usage = memory_usage;
        entry.active_connections = connections;
        entry.last_update = Instant::now();
    }

    fn refresh_cache_if_needed(&self) {
        let last_update = *self.last_load_update.read().unwrap();
        if last_update.elapsed() > Duration::from_secs(30) {
            self.update_all_volume_caches();
            *self.last_load_update.write().unwrap() = Instant::now();
        }
    }

    fn update_all_volume_caches(&self) {
        let volumes = self.master.volumes.read().unwrap();
        let mut volume_cache = self.volume_node_cache.write().unwrap();

        for (volume_id, info) in volumes.iter() {
            if info.state == VolumeState::Up {
                if let Some(node) = self.master.get_node_info(&info.node_id) {
                    volume_cache.insert(*volume_id, vec![node]);
                }
            } else {
                volume_cache.remove(volume_id);
            }
        }
    }

    fn fetch_and_cache_volume_nodes(&self, volume_id: VolumeId) -> Vec<DataNodeInfo> {
        let info = match self.master.get_volume_info(&volume_id) {
            Some(i) => i,
            None => return Vec::new(),
        };

        if info.state != VolumeState::Up {
            warn!("Volume {} is not in Up state", volume_id);
            return Vec::new();
        }

        let node = match self.master.get_node_info(&info.node_id) {
            Some(n) => n,
            None => return Vec::new(),
        };

        let nodes = vec![node];
        self.volume_node_cache.write().unwrap().insert(volume_id, nodes.clone());
        nodes
    }

    fn select_round_robin(&self, volume_id: VolumeId, nodes: &[DataNodeInfo]) -> Option<DataNodeInfo> {
        let mut counters = self.round_robin_counters.write().unwrap();
        let counter = counters.entry(volume_id).or_insert(0);
        let index = *counter % nodes.len();
        *counter += 1;
        Some(nodes[index].clone())
    }

    fn select_least_loaded(&self, nodes: &[DataNodeInfo]) -> Option<DataNodeInfo> {
        let load_cache = self.node_load_cache.read().unwrap();
        
        nodes
            .iter()
            .map(|node| {
                let load = load_cache.get(&node.id).map_or(0.5, |l| {
                    (l.disk_usage + l.cpu_usage + l.memory_usage) / 3.0
                });
                (node, load)
            })
            .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(node, _)| node.clone())
    }

    fn select_random(&self, nodes: &[DataNodeInfo]) -> Option<DataNodeInfo> {
        if nodes.is_empty() {
            return None;
        }
        let index = rand::random::<usize>() % nodes.len();
        Some(nodes[index].clone())
    }

    fn select_multiple_round_robin(&self, volume_id: VolumeId, nodes: &[DataNodeInfo], count: usize) -> Vec<DataNodeInfo> {
        let mut result = Vec::with_capacity(count.min(nodes.len()));
        let mut counters = self.round_robin_counters.write().unwrap();
        let counter = counters.entry(volume_id).or_insert(0);

        for i in 0..count.min(nodes.len()) {
            let index = (*counter + i) % nodes.len();
            result.push(nodes[index].clone());
        }
        *counter += count;
        result
    }

    fn select_multiple_least_loaded(&self, nodes: &[DataNodeInfo], count: usize) -> Vec<DataNodeInfo> {
        let load_cache = self.node_load_cache.read().unwrap();
        
        let mut nodes_with_load: Vec<_> = nodes
            .iter()
            .map(|node| {
                let load = load_cache.get(&node.id).map_or(0.5, |l| {
                    (l.disk_usage + l.cpu_usage + l.memory_usage) / 3.0
                });
                (node, load)
            })
            .collect();

        nodes_with_load.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        
        nodes_with_load
            .into_iter()
            .take(count)
            .map(|(node, _)| node.clone())
            .collect()
    }

    fn select_multiple_random(&self, nodes: &[DataNodeInfo], count: usize) -> Vec<DataNodeInfo> {
        let mut shuffled: Vec<_> = nodes.to_vec();
        for i in (1..shuffled.len()).rev() {
            let j = rand::random::<usize>() % (i + 1);
            shuffled.swap(i, j);
        }
        shuffled.into_iter().take(count).collect()
    }
}

pub struct VolumePlacementManager {
    router: Arc<VolumeRouter>,
    replica_count: RwLock<usize>,
    max_volume_size: RwLock<u64>,
    used_volume_space: RwLock<HashMap<VolumeId, u64>>,
}

impl VolumePlacementManager {
    pub fn new(router: Arc<VolumeRouter>) -> Self {
        VolumePlacementManager {
            router,
            replica_count: RwLock::new(3),
            max_volume_size: RwLock::new(1024 * 1024 * 1024 * 100),
            used_volume_space: RwLock::new(HashMap::new()),
        }
    }

    pub fn set_replica_count(&self, count: usize) {
        *self.replica_count.write().unwrap() = count;
    }

    pub fn set_max_volume_size(&self, size: u64) {
        *self.max_volume_size.write().unwrap() = size;
    }

    pub fn select_volumes_for_new_file(&self, file_size: u64) -> Vec<VolumeId> {
        let volumes = self.router.get_all_available_volumes();
        if volumes.is_empty() {
            return Vec::new();
        }

        let replica_count = *self.replica_count.read().unwrap();
        let max_size = *self.max_volume_size.read().unwrap();
        let used_space = self.used_volume_space.read().unwrap();

        let mut candidates: Vec<_> = volumes
            .iter()
            .filter(|vid| {
                let used = used_space.get(vid).copied().unwrap_or(0);
                used + file_size <= max_size
            })
            .map(|vid| {
                let load = self.router.get_volume_load(*vid);
                let used = used_space.get(vid).copied().unwrap_or(0);
                (*vid, load, used)
            })
            .collect();

        candidates.sort_by(|a, b| {
            a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.2.cmp(&b.2))
        });

        let selected: Vec<VolumeId> = candidates
            .into_iter()
            .take(replica_count)
            .map(|(vid, _, _)| vid)
            .collect();

        if selected.is_empty() {
            warn!("No available volumes for file of size {}", file_size);
        }

        selected
    }

    pub fn record_file_write(&self, volume_ids: &[VolumeId], file_size: u64) {
        let mut used_space = self.used_volume_space.write().unwrap();
        for &vid in volume_ids {
            *used_space.entry(vid).or_insert(0) += file_size;
        }
    }

    pub fn record_file_delete(&self, volume_ids: &[VolumeId], file_size: u64) {
        let mut used_space = self.used_volume_space.write().unwrap();
        for &vid in volume_ids {
            let entry = used_space.entry(vid).or_insert(0);
            *entry = entry.saturating_sub(file_size);
        }
    }

    pub fn balance_volumes(&self) -> Vec<(VolumeId, VolumeId, u64)> {
        let volumes = self.router.get_all_available_volumes();
        if volumes.len() < 2 {
            return Vec::new();
        }

        let max_size = *self.max_volume_size.read().unwrap();
        let mut used_space = self.used_volume_space.write().unwrap();

        let total_used: u64 = used_space.values().sum();
        let ideal_per_volume = total_used / volumes.len() as u64;
        let threshold = max_size / 10;

        let mut overfull: Vec<_> = volumes
            .iter()
            .filter(|vid| {
                let used = used_space.get(vid).copied().unwrap_or(0);
                used > ideal_per_volume + threshold
            })
            .map(|vid| (*vid, used_space.get(vid).copied().unwrap_or(0)))
            .collect();

        let mut underfull: Vec<_> = volumes
            .iter()
            .filter(|vid| {
                let used = used_space.get(vid).copied().unwrap_or(0);
                used < ideal_per_volume - threshold
            })
            .map(|vid| (*vid, used_space.get(vid).copied().unwrap_or(0)))
            .collect();

        overfull.sort_by(|a, b| b.1.cmp(&a.1));
        underfull.sort_by(|a, b| a.1.cmp(&b.1));

        let mut migrations = Vec::new();
        let mut o_iter = overfull.iter();
        let mut u_iter = underfull.iter();

        while let (Some(&(over_vid, over_used)), Some(&(under_vid, under_used))) = (o_iter.next(), u_iter.next()) {
            let amount_to_move = (over_used - ideal_per_volume).min(ideal_per_volume - under_used);
            if amount_to_move > 0 {
                migrations.push((over_vid, under_vid, amount_to_move));
                *used_space.get_mut(&over_vid).unwrap() -= amount_to_move;
                *used_space.get_mut(&under_vid).unwrap() += amount_to_move;
            }
        }

        if !migrations.is_empty() {
            info!("Scheduled {} volume rebalancing migrations", migrations.len());
        }

        migrations
    }
}
