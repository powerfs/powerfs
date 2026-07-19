use powerfs_common::types::DataNodeInfo;

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

#[derive(Debug, Clone)]
pub enum AssignerType {
    RoundRobin,
    ConsistentHash,
}

impl AssignerType {
    pub fn create(self) -> Box<dyn VolumeAssigner> {
        match self {
            AssignerType::RoundRobin => Box::new(RoundRobinAssigner),
            AssignerType::ConsistentHash => Box::new(ConsistentHashAssigner),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use powerfs_common::types::{DataCenterId, DataNodeInfo, NodeId, RackId};

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
            })
            .collect()
    }

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

        let nodes = create_test_nodes(3);
        let rr_result = rr.assign(1, &nodes, 1);
        let ch_result = ch.assign(1, &nodes, 1);

        assert_eq!(rr_result.len(), 1);
        assert_eq!(ch_result.len(), 1);
    }
}
