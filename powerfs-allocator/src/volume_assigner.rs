//! Volume-to-Node assignment strategies (moved from powerfs-master).
//!
//! Stateless: receives node list + context, returns assigned nodes.

use std::collections::HashSet;

use powerfs_common::types::{DataNodeInfo, NodeState};

/// Volume assignment trait: select `replica_count` nodes for a volume.
pub trait VolumeAssigner: Sync + Send {
    fn assign(
        &self,
        volume_id: u64,
        nodes: &[DataNodeInfo],
        replica_count: usize,
    ) -> Vec<DataNodeInfo>;
}

#[derive(Debug, Clone)]
pub struct RoundRobinAssigner;

impl VolumeAssigner for RoundRobinAssigner {
    fn assign(
        &self,
        volume_id: u64,
        nodes: &[DataNodeInfo],
        replica_count: usize,
    ) -> Vec<DataNodeInfo> {
        if nodes.is_empty() {
            return Vec::new();
        }
        let node_idx = volume_id as usize % nodes.len();
        let mut selected = Vec::with_capacity(replica_count);
        for i in 0..replica_count {
            let idx = (node_idx + i) % nodes.len();
            selected.push(nodes[idx].clone());
        }
        selected
    }
}

#[derive(Debug, Clone)]
pub struct ConsistentHashAssigner;

impl VolumeAssigner for ConsistentHashAssigner {
    fn assign(
        &self,
        volume_id: u64,
        nodes: &[DataNodeInfo],
        replica_count: usize,
    ) -> Vec<DataNodeInfo> {
        if nodes.is_empty() {
            return Vec::new();
        }
        let node_idx = volume_id as usize % nodes.len();
        let mut selected = Vec::with_capacity(replica_count);
        for i in 0..replica_count {
            let idx = (node_idx + i) % nodes.len();
            selected.push(nodes[idx].clone());
        }
        selected
    }
}

/// Context flags that influence how a [`SmartVolumeAssigner`] selects nodes.
#[derive(Debug, Clone, Default)]
pub struct AssignContext {
    pub rack_awareness_enabled: bool,
    pub data_center_awareness_enabled: bool,
    pub preferred_node: Option<String>,
}

/// Smart assigner: node-state filtering + capacity/load scoring + rack/DC isolation.
#[derive(Debug, Clone, Default)]
pub struct SmartVolumeAssigner;

impl SmartVolumeAssigner {
    fn score(node: &DataNodeInfo) -> Option<f64> {
        if node.maintenance_mode || node.state.is_unhealthy() {
            return None;
        }

        let state_factor: f64 = match node.state {
            NodeState::Healthy => 1.0,
            NodeState::Ready => 0.9,
            NodeState::SoftError => 0.6,
            NodeState::FailSlow => {
                let severity = node.degrade_severity.min(100) as f64;
                1.0 - (severity / 100.0) * 0.5
            }
            _ => return None,
        };

        let capacity_factor = if node.total_space > 0 {
            let free_ratio = 1.0 - (node.used_space as f64 / node.total_space as f64);
            0.5 + 0.5 * free_ratio.clamp(0.0, 1.0)
        } else {
            0.5
        };

        let load_factor = if node.volume_count > 0 {
            0.7 + 0.3 / (1.0 + node.volume_count as f64 * 0.01)
        } else {
            1.0
        };

        Some(state_factor * capacity_factor * load_factor)
    }

    pub fn assign_with_context(
        &self,
        _volume_id: u64,
        nodes: &[DataNodeInfo],
        replica_count: usize,
        ctx: &AssignContext,
    ) -> Vec<DataNodeInfo> {
        if nodes.is_empty() || replica_count == 0 {
            return Vec::new();
        }

        let mut scored: Vec<(&DataNodeInfo, f64)> = nodes
            .iter()
            .filter_map(|n| Self::score(n).map(|s| (n, s)))
            .collect();

        if scored.is_empty() {
            return Vec::new();
        }

        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        let mut selected: Vec<DataNodeInfo> = Vec::with_capacity(replica_count);
        let mut used_racks: HashSet<String> = HashSet::new();
        let mut used_dcs: HashSet<String> = HashSet::new();

        if let Some(pref) = &ctx.preferred_node {
            if let Some((node, _)) = scored.iter().find(|(n, _)| &n.id.0 == pref) {
                selected.push((*node).clone());
                used_racks.insert(node.rack_id.0.clone());
                used_dcs.insert(node.data_center_id.0.clone());
            }
        }

        for (node, _) in &scored {
            if selected.len() >= replica_count {
                break;
            }
            if selected.iter().any(|n| n.id == node.id) {
                continue;
            }
            if ctx.rack_awareness_enabled && used_racks.contains(&node.rack_id.0) {
                let remaining_candidates = scored
                    .iter()
                    .filter(|(n, _)| {
                        !selected.iter().any(|s| s.id == n.id) && !used_racks.contains(&n.rack_id.0)
                    })
                    .count();
                let needed = replica_count - selected.len();
                if remaining_candidates >= needed {
                    continue;
                }
            }
            if ctx.data_center_awareness_enabled && used_dcs.contains(&node.data_center_id.0) {
                let remaining_candidates = scored
                    .iter()
                    .filter(|(n, _)| {
                        !selected.iter().any(|s| s.id == n.id)
                            && !used_dcs.contains(&n.data_center_id.0)
                    })
                    .count();
                let needed = replica_count - selected.len();
                if remaining_candidates >= needed {
                    continue;
                }
            }
            selected.push((*node).clone());
            used_racks.insert(node.rack_id.0.clone());
            used_dcs.insert(node.data_center_id.0.clone());
        }

        if selected.len() < replica_count {
            for (node, _) in &scored {
                if selected.len() >= replica_count {
                    break;
                }
                if selected.iter().any(|n| n.id == node.id) {
                    continue;
                }
                selected.push((*node).clone());
            }
        }

        selected
    }
}

impl VolumeAssigner for SmartVolumeAssigner {
    fn assign(
        &self,
        volume_id: u64,
        nodes: &[DataNodeInfo],
        replica_count: usize,
    ) -> Vec<DataNodeInfo> {
        self.assign_with_context(
            volume_id,
            nodes,
            replica_count,
            &AssignContext {
                rack_awareness_enabled: true,
                data_center_awareness_enabled: false,
                preferred_node: None,
            },
        )
    }
}

#[derive(Debug, Clone)]
pub enum AssignerType {
    RoundRobin,
    ConsistentHash,
    Smart,
}

impl AssignerType {
    pub fn create(self) -> Box<dyn VolumeAssigner> {
        match self {
            AssignerType::RoundRobin => Box::new(RoundRobinAssigner),
            AssignerType::ConsistentHash => Box::new(ConsistentHashAssigner),
            AssignerType::Smart => Box::new(SmartVolumeAssigner),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use powerfs_common::types::{DataCenterId, DataNodeInfo, NodeId, NodeState, RackId};

    fn make_node(
        id: &str,
        rack: &str,
        dc: &str,
        state: NodeState,
        used_space: u64,
        total_space: u64,
        volume_count: u32,
    ) -> DataNodeInfo {
        DataNodeInfo {
            id: NodeId(id.to_string()),
            address: "127.0.0.1".to_string(),
            rack_id: RackId(rack.to_string()),
            data_center_id: DataCenterId(dc.to_string()),
            total_space,
            used_space,
            volume_count,
            state,
            last_heartbeat: Default::default(),
            grpc_port: 8080,
            http_port: 8080,
            public_url: String::new(),
            maintenance_mode: false,
            soft_error_type: None,
            degrade_type: None,
            degrade_severity: 0,
            state_since: 0,
            cpu_usage: 0.0,
            memory_usage: 0.0,
        }
    }

    fn create_test_nodes(count: usize) -> Vec<DataNodeInfo> {
        (0..count)
            .map(|i| DataNodeInfo {
                id: NodeId(format!("volume-server-{}", i + 1)),
                address: format!("172.20.0.{}", 21 + i),
                rack_id: RackId(format!("rack-{}", (i % 2) + 1)),
                data_center_id: DataCenterId("dc-1".to_string()),
                total_space: 100 * 1024 * 1024 * 1024,
                used_space: 0,
                volume_count: 0,
                state: Default::default(),
                last_heartbeat: Default::default(),
                grpc_port: 8080 + i as u32,
                http_port: 8080 + i as u32,
                public_url: format!("http://172.20.0.{}:{}", 21 + i, 8080 + i),
                maintenance_mode: false,
                soft_error_type: None,
                degrade_type: None,
                degrade_severity: 0,
                state_since: 0,
                cpu_usage: 0.0,
                memory_usage: 0.0,
            })
            .collect()
    }

    #[test]
    fn test_round_robin_empty_nodes() {
        let assigner = RoundRobinAssigner;
        assert!(assigner.assign(1, &[], 1).is_empty());
    }

    #[test]
    fn test_round_robin_three_nodes() {
        let assigner = RoundRobinAssigner;
        let nodes = create_test_nodes(3);
        assert_eq!(assigner.assign(0, &nodes, 1)[0].id.0, "volume-server-1");
        assert_eq!(assigner.assign(1, &nodes, 1)[0].id.0, "volume-server-2");
        assert_eq!(assigner.assign(2, &nodes, 1)[0].id.0, "volume-server-3");
        assert_eq!(assigner.assign(3, &nodes, 1)[0].id.0, "volume-server-1");
    }

    #[test]
    fn test_smart_filters_unhealthy() {
        let assigner = SmartVolumeAssigner;
        let nodes = vec![
            make_node("n1", "r1", "dc1", NodeState::Fault, 0, 100, 0),
            make_node("n2", "r1", "dc1", NodeState::Maintenance, 0, 100, 0),
            make_node("ok", "r1", "dc1", NodeState::Healthy, 0, 100, 0),
        ];
        let result = assigner.assign(1, &nodes, 1);
        assert_eq!(result[0].id.0, "ok");
    }

    #[test]
    fn test_smart_rack_isolation() {
        let assigner = SmartVolumeAssigner;
        let nodes = vec![
            make_node("n1", "r1", "dc1", NodeState::Healthy, 10, 100, 0),
            make_node("n2", "r1", "dc1", NodeState::Healthy, 20, 100, 0),
            make_node("n3", "r2", "dc1", NodeState::Healthy, 10, 100, 0),
            make_node("n4", "r2", "dc1", NodeState::Healthy, 20, 100, 0),
        ];
        let ctx = AssignContext {
            rack_awareness_enabled: true,
            data_center_awareness_enabled: false,
            preferred_node: None,
        };
        let result = assigner.assign_with_context(1, &nodes, 3, &ctx);
        assert_eq!(result.len(), 3);
        let racks: HashSet<_> = result.iter().map(|n| n.rack_id.0.clone()).collect();
        assert_eq!(racks.len(), 2);
    }

    #[test]
    fn test_smart_preferred_node() {
        let assigner = SmartVolumeAssigner;
        let nodes = vec![
            make_node("n1", "r1", "dc1", NodeState::Healthy, 0, 100, 0),
            make_node("n2", "r2", "dc1", NodeState::Healthy, 0, 100, 0),
        ];
        let ctx = AssignContext {
            rack_awareness_enabled: true,
            data_center_awareness_enabled: false,
            preferred_node: Some("n2".to_string()),
        };
        let result = assigner.assign_with_context(1, &nodes, 1, &ctx);
        assert_eq!(result[0].id.0, "n2");
    }

    #[test]
    fn test_smart_prefers_lower_load() {
        let assigner = SmartVolumeAssigner;
        let nodes = vec![
            make_node("light", "r1", "dc1", NodeState::Healthy, 10, 100, 1),
            make_node("heavy", "r1", "dc1", NodeState::Healthy, 90, 100, 100),
        ];
        let ctx = AssignContext::default();
        let result = assigner.assign_with_context(1, &nodes, 1, &ctx);
        assert_eq!(result[0].id.0, "light");
    }
}
