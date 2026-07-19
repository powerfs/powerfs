use powerfs_common::types::{DataNodeInfo, NodeState};
use std::collections::HashSet;

pub trait VolumeAssigner: Sync + Send {
    fn assign(
        &self,
        volume_id: u32,
        nodes: &[DataNodeInfo],
        replica_count: usize,
    ) -> Vec<DataNodeInfo>;
}

#[derive(Debug, Clone)]
pub struct RoundRobinAssigner;

impl VolumeAssigner for RoundRobinAssigner {
    fn assign(
        &self,
        volume_id: u32,
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
        volume_id: u32,
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
///
/// Passed in by the caller (e.g. the master) at assign time so the assigner
/// remains stateless and free of circular references back to `MasterNode`.
#[derive(Debug, Clone, Default)]
pub struct AssignContext {
    /// When true, prefer placing replicas on different racks. Falls back to
    /// ignoring rack isolation if there are not enough racks.
    pub rack_awareness_enabled: bool,
    /// When true, prefer placing replicas in different data centers. Falls
    /// back similarly.
    pub data_center_awareness_enabled: bool,
    /// If set, the assigner tries to place the *primary* replica on this
    /// node. `None` means no preference.
    pub preferred_node: Option<String>,
}

/// Smart assigner: combines node-state filtering, capacity/load scoring, and
/// rack/DC fault-domain isolation. Replaces the create-and-discard retry loop
/// previously used by `volume_grow` when a specific target node was requested.
///
/// The assigner is stateless: configuration is supplied per-call via an
/// [`AssignContext`]. This avoids holding a back-reference to `MasterNode`.
#[derive(Debug, Clone, Default)]
pub struct SmartVolumeAssigner;

impl SmartVolumeAssigner {
    /// Score a candidate node. Higher is better. Returns `None` if the node
    /// is not assignable (filtered out).
    fn score(node: &DataNodeInfo) -> Option<f64> {
        // Hard filter: maintenance or unhealthy states are never assignable.
        if node.maintenance_mode || node.state.is_unhealthy() {
            return None;
        }

        // State multiplier.
        let state_factor: f64 = match node.state {
            NodeState::Healthy => 1.0,
            NodeState::Ready => 0.9,
            NodeState::SoftError => 0.6,
            NodeState::FailSlow => {
                // Severity ranges 0..=100; higher severity lowers the score.
                let severity = node.degrade_severity.min(100) as f64;
                1.0 - (severity / 100.0) * 0.5
            }
            // Unreachable: filtered above.
            _ => return None,
        };

        // Capacity score: free-space ratio in [0,1].
        let capacity_factor = if node.total_space > 0 {
            let free_ratio = 1.0 - (node.used_space as f64 / node.total_space as f64);
            0.5 + 0.5 * free_ratio.clamp(0.0, 1.0)
        } else {
            0.5
        };

        // Load score: fewer existing volumes is better.
        let load_factor = if node.volume_count > 0 {
            0.7 + 0.3 / (1.0 + node.volume_count as f64 * 0.01)
        } else {
            1.0
        };

        Some(state_factor * capacity_factor * load_factor)
    }

    /// Assignment entry point that takes an explicit context.
    pub fn assign_with_context(
        &self,
        _volume_id: u32,
        nodes: &[DataNodeInfo],
        replica_count: usize,
        ctx: &AssignContext,
    ) -> Vec<DataNodeInfo> {
        if nodes.is_empty() || replica_count == 0 {
            return Vec::new();
        }

        // 1. Filter + score candidates.
        let mut scored: Vec<(&DataNodeInfo, f64)> = nodes
            .iter()
            .filter_map(|n| Self::score(n).map(|s| (n, s)))
            .collect();

        if scored.is_empty() {
            return Vec::new();
        }

        // Sort by score descending. Use a stable total order via partial_cmp.
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        let mut selected: Vec<DataNodeInfo> = Vec::with_capacity(replica_count);
        let mut used_racks: HashSet<String> = HashSet::new();
        let mut used_dcs: HashSet<String> = HashSet::new();

        // 2. Pin preferred node first (if requested and still a candidate).
        if let Some(pref) = &ctx.preferred_node {
            if let Some((node, _)) = scored.iter().find(|(n, _)| &n.id.0 == pref) {
                selected.push((*node).clone());
                used_racks.insert(node.rack_id.0.clone());
                used_dcs.insert(node.data_center_id.0.clone());
            }
        }

        // 3. Pick remaining replicas with fault-domain isolation.
        for (node, _) in &scored {
            if selected.len() >= replica_count {
                break;
            }
            if selected.iter().any(|n| n.id == node.id) {
                continue;
            }
            // Rack isolation: skip if same rack already used AND we still have
            // enough candidates to satisfy `replica_count` without reusing the
            // rack. The fallback (no rack constraint) is handled below if we
            // cannot fill the slots.
            if ctx.rack_awareness_enabled && used_racks.contains(&node.rack_id.0) {
                let remaining_candidates = scored
                    .iter()
                    .filter(|(n, _)| {
                        !selected.iter().any(|s| s.id == n.id)
                            && !used_racks.contains(&n.rack_id.0)
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

        // 4. Fallback: if fault-domain constraints prevented us from filling
        // all slots, relax them and pick the best remaining candidates.
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
        volume_id: u32,
        nodes: &[DataNodeInfo],
        replica_count: usize,
    ) -> Vec<DataNodeInfo> {
        // Default context: rack+DC awareness on, no preferred node.
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
    use powerfs_common::types::{
        DataCenterId, DataNodeInfo, DegradeType, NodeId, NodeState, RackId, SoftErrorType,
    };

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
            })
            .collect()
    }

    // --- Existing RoundRobin tests (unchanged behavior) ---

    #[test]
    fn test_round_robin_empty_nodes() {
        let assigner = RoundRobinAssigner;
        let nodes = vec![];
        let result = assigner.assign(1, &nodes, 1);
        assert!(result.is_empty());
    }

    #[test]
    fn test_round_robin_single_node() {
        let assigner = RoundRobinAssigner;
        let nodes = create_test_nodes(1);
        let result = assigner.assign(1, &nodes, 1);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id.0, "volume-server-1");
    }

    #[test]
    fn test_round_robin_three_nodes() {
        let assigner = RoundRobinAssigner;
        let nodes = create_test_nodes(3);

        let r0 = assigner.assign(0, &nodes, 1);
        assert_eq!(r0[0].id.0, "volume-server-1");

        let r1 = assigner.assign(1, &nodes, 1);
        assert_eq!(r1[0].id.0, "volume-server-2");

        let r2 = assigner.assign(2, &nodes, 1);
        assert_eq!(r2[0].id.0, "volume-server-3");

        let r3 = assigner.assign(3, &nodes, 1);
        assert_eq!(r3[0].id.0, "volume-server-1");
    }

    #[test]
    fn test_round_robin_replica_count() {
        let assigner = RoundRobinAssigner;
        let nodes = create_test_nodes(3);

        let r0 = assigner.assign(0, &nodes, 2);
        assert_eq!(r0.len(), 2);
        assert_eq!(r0[0].id.0, "volume-server-1");
        assert_eq!(r0[1].id.0, "volume-server-2");

        let r1 = assigner.assign(1, &nodes, 3);
        assert_eq!(r1.len(), 3);
        assert_eq!(r1[0].id.0, "volume-server-2");
        assert_eq!(r1[1].id.0, "volume-server-3");
        assert_eq!(r1[2].id.0, "volume-server-1");
    }

    #[test]
    fn test_round_robin_volume_id_zero() {
        let assigner = RoundRobinAssigner;
        let nodes = create_test_nodes(3);

        let r0 = assigner.assign(0, &nodes, 1);
        assert_eq!(r0[0].id.0, "volume-server-1");
    }

    #[test]
    fn test_assigner_type_enum() {
        let rr = AssignerType::RoundRobin.create();
        let ch = AssignerType::ConsistentHash.create();
        let sm = AssignerType::Smart.create();

        let nodes = create_test_nodes(3);
        let rr_result = rr.assign(1, &nodes, 1);
        let ch_result = ch.assign(1, &nodes, 1);
        let sm_result = sm.assign(1, &nodes, 1);

        assert_eq!(rr_result.len(), 1);
        assert_eq!(ch_result.len(), 1);
        assert_eq!(sm_result.len(), 1);
    }

    // --- SmartVolumeAssigner tests ---

    #[test]
    fn test_smart_empty_nodes() {
        let assigner = SmartVolumeAssigner;
        let result = assigner.assign(1, &[], 1);
        assert!(result.is_empty());
    }

    #[test]
    fn test_smart_filters_unhealthy() {
        let assigner = SmartVolumeAssigner;
        let nodes = vec![
            make_node("n1", "r1", "dc1", NodeState::Fault, 0, 100, 0),
            make_node("n2", "r1", "dc1", NodeState::Maintenance, 0, 100, 0),
            make_node("n3", "r1", "dc1", NodeState::Unavailable, 0, 100, 0),
            make_node("n4", "r1", "dc1", NodeState::Init, 0, 100, 0),
            make_node("n5", "r1", "dc1", NodeState::Degraded, 0, 100, 0),
            make_node("ok", "r1", "dc1", NodeState::Healthy, 0, 100, 0),
        ];
        let result = assigner.assign(1, &nodes, 1);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id.0, "ok");
    }

    #[test]
    fn test_smart_filters_maintenance() {
        let assigner = SmartVolumeAssigner;
        let mut n = make_node("n1", "r1", "dc1", NodeState::Healthy, 0, 100, 0);
        n.maintenance_mode = true;
        let result = assigner.assign(1, &[n], 1);
        assert!(result.is_empty());
    }

    #[test]
    fn test_smart_rack_isolation() {
        let assigner = SmartVolumeAssigner;
        // Two racks, two nodes each. Asking for 3 replicas should use both
        // racks (max 2 distinct racks) and pick a third from one of them.
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
        // Both racks should be represented (we have only 2 racks).
        assert_eq!(racks.len(), 2);
    }

    #[test]
    fn test_smart_rack_isolation_fallback() {
        let assigner = SmartVolumeAssigner;
        // Only one rack: should fall back to placing all replicas there.
        let nodes = vec![
            make_node("n1", "r1", "dc1", NodeState::Healthy, 10, 100, 0),
            make_node("n2", "r1", "dc1", NodeState::Healthy, 20, 100, 0),
        ];
        let ctx = AssignContext {
            rack_awareness_enabled: true,
            data_center_awareness_enabled: false,
            preferred_node: None,
        };
        let result = assigner.assign_with_context(1, &nodes, 2, &ctx);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_smart_preferred_node() {
        let assigner = SmartVolumeAssigner;
        let nodes = vec![
            make_node("n1", "r1", "dc1", NodeState::Healthy, 0, 100, 0),
            make_node("n2", "r2", "dc1", NodeState::Healthy, 0, 100, 0),
            make_node("n3", "r3", "dc1", NodeState::Healthy, 0, 100, 0),
        ];
        let ctx = AssignContext {
            rack_awareness_enabled: true,
            data_center_awareness_enabled: false,
            preferred_node: Some("n2".to_string()),
        };
        let result = assigner.assign_with_context(1, &nodes, 1, &ctx);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id.0, "n2");
    }

    #[test]
    fn test_smart_preferred_node_ignored_if_unhealthy() {
        let assigner = SmartVolumeAssigner;
        let nodes = vec![
            make_node("preferred", "r1", "dc1", NodeState::Fault, 0, 100, 0),
            make_node("other", "r2", "dc1", NodeState::Healthy, 0, 100, 0),
        ];
        let ctx = AssignContext {
            rack_awareness_enabled: false,
            data_center_awareness_enabled: false,
            preferred_node: Some("preferred".to_string()),
        };
        let result = assigner.assign_with_context(1, &nodes, 1, &ctx);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id.0, "other");
    }

    #[test]
    fn test_smart_prefers_lower_load() {
        let assigner = SmartVolumeAssigner;
        // Same rack/dc/state, but n1 has fewer volumes and more free space.
        let nodes = vec![
            make_node("light", "r1", "dc1", NodeState::Healthy, 10, 100, 1),
            make_node("heavy", "r1", "dc1", NodeState::Healthy, 90, 100, 100),
        ];
        let ctx = AssignContext::default();
        let result = assigner.assign_with_context(1, &nodes, 1, &ctx);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id.0, "light");
    }

    #[test]
    fn test_smart_failslow_severity() {
        let assigner = SmartVolumeAssigner;
        let mut low_severity =
            make_node("low", "r1", "dc1", NodeState::FailSlow, 10, 100, 0);
        low_severity.degrade_severity = 20;
        low_severity.degrade_type = Some(DegradeType::NetworkDegrade);
        let mut high_severity =
            make_node("high", "r1", "dc1", NodeState::FailSlow, 10, 100, 0);
        high_severity.degrade_severity = 90;
        high_severity.degrade_type = Some(DegradeType::NetworkDegrade);
        let healthy = make_node("healthy", "r1", "dc1", NodeState::Healthy, 10, 100, 0);
        let nodes = vec![low_severity, high_severity, healthy];

        let ctx = AssignContext::default();
        let result = assigner.assign_with_context(1, &nodes, 1, &ctx);
        // Healthy should win.
        assert_eq!(result[0].id.0, "healthy");

        // Without the healthy node, low-severity should beat high-severity.
        let nodes2 = vec![
            make_node("low", "r1", "dc1", NodeState::FailSlow, 10, 100, 0),
            make_node("high", "r1", "dc1", NodeState::FailSlow, 10, 100, 0),
        ];
        let mut nodes2 = nodes2;
        nodes2[0].degrade_severity = 20;
        nodes2[1].degrade_severity = 90;
        let result2 = assigner.assign_with_context(1, &nodes2, 1, &ctx);
        assert_eq!(result2[0].id.0, "low");
    }

    #[test]
    fn test_smart_soft_error_deprioritized() {
        let assigner = SmartVolumeAssigner;
        let mut soft =
            make_node("soft", "r1", "dc1", NodeState::SoftError, 10, 100, 0);
        soft.soft_error_type = Some(SoftErrorType::CpuPressure);
        let healthy = make_node("healthy", "r1", "dc1", NodeState::Healthy, 10, 100, 0);
        let nodes = vec![soft, healthy];

        let ctx = AssignContext::default();
        let result = assigner.assign_with_context(1, &nodes, 1, &ctx);
        assert_eq!(result[0].id.0, "healthy");
    }

    #[test]
    fn test_smart_zero_replica_count() {
        let assigner = SmartVolumeAssigner;
        let nodes = create_test_nodes(3);
        let ctx = AssignContext::default();
        let result = assigner.assign_with_context(1, &nodes, 0, &ctx);
        assert!(result.is_empty());
    }

    #[test]
    fn test_smart_insufficient_healthy_nodes() {
        let assigner = SmartVolumeAssigner;
        // Only 2 healthy nodes but we ask for 3 replicas.
        let nodes = vec![
            make_node("n1", "r1", "dc1", NodeState::Healthy, 0, 100, 0),
            make_node("n2", "r1", "dc1", NodeState::Healthy, 0, 100, 0),
            make_node("n3", "r1", "dc1", NodeState::Fault, 0, 100, 0),
        ];
        let ctx = AssignContext::default();
        let result = assigner.assign_with_context(1, &nodes, 3, &ctx);
        assert_eq!(result.len(), 2);
    }
}