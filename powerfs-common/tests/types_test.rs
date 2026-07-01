use powerfs_common::types::*;
use std::str::FromStr;

// ============================================================================
// VolumeId tests
// ============================================================================

#[test]
fn test_volume_id_display() {
    let vid = VolumeId(42);
    assert_eq!(vid.to_string(), "42");
}

#[test]
fn test_volume_id_from_u32() {
    let vid: VolumeId = 100u32.into();
    assert_eq!(vid.0, 100);
}

#[test]
fn test_volume_id_from_str_valid() {
    let vid = VolumeId::from_str("12345").unwrap();
    assert_eq!(vid.0, 12345);
}

#[test]
fn test_volume_id_from_str_invalid() {
    assert!(VolumeId::from_str("abc").is_err());
    assert!(VolumeId::from_str("").is_err());
}

#[test]
fn test_volume_id_from_str_max_value() {
    let vid = VolumeId::from_str("4294967295").unwrap(); // u32::MAX
    assert_eq!(vid.0, u32::MAX);
}

#[test]
fn test_volume_id_from_str_overflow() {
    assert!(VolumeId::from_str("4294967296").is_err()); // u32::MAX + 1
}

#[test]
fn test_volume_id_from_str_zero() {
    let vid = VolumeId::from_str("0").unwrap();
    assert_eq!(vid.0, 0);
}

// ============================================================================
// NeedleId tests
// ============================================================================

#[test]
fn test_needle_id_display() {
    let nid = NeedleId(123456789);
    assert_eq!(nid.to_string(), "123456789");
}

#[test]
fn test_needle_id_equality() {
    let a = NeedleId(1);
    let b = NeedleId(1);
    let c = NeedleId(2);
    assert_eq!(a, b);
    assert_ne!(a, c);
}

// ============================================================================
// FileId tests
// ============================================================================

#[test]
fn test_file_id_display() {
    let fid = FileId("test-file-uuid".to_string());
    assert_eq!(fid.to_string(), "test-file-uuid");
}

#[test]
fn test_file_id_equality() {
    let a = FileId("a".to_string());
    let b = FileId("a".to_string());
    let c = FileId("b".to_string());
    assert_eq!(a, b);
    assert_ne!(a, c);
}

// ============================================================================
// NodeId tests
// ============================================================================

#[test]
fn test_node_id_display() {
    let nid = NodeId("node-1".to_string());
    assert_eq!(nid.to_string(), "node-1");
}

// ============================================================================
// DataCenterId tests
// ============================================================================

#[test]
fn test_data_center_id_display() {
    let dcid = DataCenterId("dc-west".to_string());
    assert_eq!(dcid.to_string(), "dc-west");
}

// ============================================================================
// RackId tests
// ============================================================================

#[test]
fn test_rack_id_display() {
    let rid = RackId("rack-01".to_string());
    assert_eq!(rid.to_string(), "rack-01");
}

// ============================================================================
// Collection tests
// ============================================================================

#[test]
fn test_collection_display() {
    let c = Collection("images".to_string());
    assert_eq!(c.to_string(), "images");
}

#[test]
fn test_collection_default() {
    let c = Collection::default();
    assert_eq!(c.0, "default");
}

// ============================================================================
// DiskType tests
// ============================================================================

#[test]
fn test_disk_type_display() {
    let dt = DiskType("ssd".to_string());
    assert_eq!(dt.to_string(), "ssd");
}

#[test]
fn test_disk_type_default() {
    let dt = DiskType::default();
    assert_eq!(dt.0, "");
}

// ============================================================================
// Ttl tests
// ============================================================================

#[test]
fn test_ttl_display_nonzero() {
    let ttl = Ttl(3600);
    assert_eq!(ttl.to_string(), "3600");
}

#[test]
fn test_ttl_display_zero() {
    let ttl = Ttl(0);
    assert_eq!(ttl.to_string(), "");
}

#[test]
fn test_ttl_display_negative() {
    let ttl = Ttl(-1);
    assert_eq!(ttl.to_string(), "-1");
}

#[test]
fn test_ttl_default() {
    let ttl = Ttl::default();
    assert_eq!(ttl.0, 0);
}

// ============================================================================
// ReplicaPlacement tests
// ============================================================================

#[test]
fn test_replica_placement_default() {
    let rp = ReplicaPlacement::default();
    assert_eq!(rp.copies, 1);
    assert!(!rp.same_rack);
    assert!(!rp.same_data_center);
    assert_eq!(rp.get_copy_count(), 1);
}

#[test]
fn test_replica_placement_from_string_empty() {
    let rp = ReplicaPlacement::from_string("").unwrap();
    assert_eq!(rp.copies, 1);
    assert!(!rp.same_rack);
    assert!(!rp.same_data_center);
}

#[test]
fn test_replica_placement_from_string_001() {
    // 1 copy, different data center
    let rp = ReplicaPlacement::from_string("001").unwrap();
    assert_eq!(rp.copies, 1);
    assert!(!rp.same_rack);
    assert!(!rp.same_data_center);
}

#[test]
fn test_replica_placement_from_string_010() {
    // 1 copy, same rack, different data center
    let rp = ReplicaPlacement::from_string("010").unwrap();
    assert_eq!(rp.copies, 1);
    assert!(rp.same_rack);
    assert!(rp.same_data_center);
}

#[test]
fn test_replica_placement_from_string_100() {
    // 1 copy, same data center
    let rp = ReplicaPlacement::from_string("100").unwrap();
    assert_eq!(rp.copies, 1);
    assert!(!rp.same_rack);
    assert!(rp.same_data_center);
}

#[test]
fn test_replica_placement_from_string_011() {
    // 2 copies: 1 same rack + 1 different dc
    let rp = ReplicaPlacement::from_string("011").unwrap();
    assert_eq!(rp.copies, 2);
    assert!(rp.same_rack);
    assert!(rp.same_data_center);
}

#[test]
fn test_replica_placement_from_string_111() {
    // 3 copies: 1 same dc + 1 same rack + 1 different dc
    let rp = ReplicaPlacement::from_string("111").unwrap();
    assert_eq!(rp.copies, 3);
    assert!(rp.same_rack);
    assert!(rp.same_data_center);
}

#[test]
fn test_replica_placement_from_string_002() {
    // 2 copies, both different data centers
    let rp = ReplicaPlacement::from_string("002").unwrap();
    assert_eq!(rp.copies, 2);
    assert!(!rp.same_rack);
    assert!(!rp.same_data_center);
}

#[test]
fn test_replica_placement_from_string_simple_number() {
    // Simple format "3" → 3 copies
    let rp = ReplicaPlacement::from_string("3").unwrap();
    assert_eq!(rp.copies, 3);
    assert!(!rp.same_rack);
    assert!(!rp.same_data_center);
}

#[test]
fn test_replica_placement_from_string_simple_number_large() {
    let rp = ReplicaPlacement::from_string("10").unwrap();
    assert_eq!(rp.copies, 10);
}

#[test]
fn test_replica_placement_from_string_invalid() {
    assert!(ReplicaPlacement::from_string("abc").is_err());
    assert!(ReplicaPlacement::from_string("ab0").is_err());
}

#[test]
fn test_replica_placement_get_copy_count() {
    let rp = ReplicaPlacement::from_string("123").unwrap(); // 1+2+3 = 6 copies
    assert_eq!(rp.get_copy_count(), 6);
}

#[test]
fn test_replica_placement_to_string_format_same_dc_and_rack() {
    let rp = ReplicaPlacement {
        copies: 2,
        same_rack: true,
        same_data_center: true,
    };
    assert_eq!(rp.to_string_format(), "200");
}

#[test]
fn test_replica_placement_to_string_format_same_rack_only() {
    let rp = ReplicaPlacement {
        copies: 3,
        same_rack: true,
        same_data_center: false,
    };
    assert_eq!(rp.to_string_format(), "030");
}

#[test]
fn test_replica_placement_to_string_format_neither() {
    let rp = ReplicaPlacement {
        copies: 1,
        same_rack: false,
        same_data_center: false,
    };
    assert_eq!(rp.to_string_format(), "001");
}

#[test]
fn test_replica_placement_from_string_all_zeros() {
    let rp = ReplicaPlacement::from_string("000").unwrap();
    assert_eq!(rp.copies, 0);
    assert!(!rp.same_rack);
    assert!(!rp.same_data_center);
}

#[test]
fn test_replica_placement_from_string_large_digits() {
    let rp = ReplicaPlacement::from_string("929").unwrap();
    assert_eq!(rp.copies, 20);
    assert!(rp.same_rack);
    assert!(rp.same_data_center);
}

// ============================================================================
// Fid tests
// ============================================================================

#[test]
fn test_fid_display() {
    let fid = Fid {
        volume_id: VolumeId(1),
        cookie: 123,
        file_key: 456,
    };
    assert_eq!(fid.to_string(), "1,123,456");
}

#[test]
fn test_fid_from_string_valid() {
    let fid = Fid::from_string("3,5,7").unwrap();
    assert_eq!(fid.volume_id, VolumeId(3));
    assert_eq!(fid.cookie, 5);
    assert_eq!(fid.file_key, 7);
}

#[test]
fn test_fid_from_string_volume_id_zero() {
    let fid = Fid::from_string("0,0,0").unwrap();
    assert_eq!(fid.volume_id, VolumeId(0));
    assert_eq!(fid.cookie, 0);
    assert_eq!(fid.file_key, 0);
}

#[test]
fn test_fid_from_string_max_values() {
    let fid = Fid::from_string("4294967295,18446744073709551615,18446744073709551615").unwrap();
    assert_eq!(fid.volume_id, VolumeId(u32::MAX));
    assert_eq!(fid.cookie, u64::MAX);
    assert_eq!(fid.file_key, u64::MAX);
}

#[test]
fn test_fid_from_string_missing_parts() {
    assert!(Fid::from_string("1,2").is_err());
    assert!(Fid::from_string("1").is_err());
    assert!(Fid::from_string("").is_err());
}

#[test]
fn test_fid_from_string_too_many_parts() {
    assert!(Fid::from_string("1,2,3,4").is_err());
}

#[test]
fn test_fid_from_string_invalid_numbers() {
    assert!(Fid::from_string("abc,2,3").is_err());
    assert!(Fid::from_string("1,abc,3").is_err());
    assert!(Fid::from_string("1,2,abc").is_err());
}

// ============================================================================
// VolumeState tests
// ============================================================================

#[test]
fn test_volume_state_default() {
    assert_eq!(VolumeState::default(), VolumeState::Creating);
}

#[test]
fn test_volume_state_all_variants() {
    let states = vec![
        VolumeState::Creating,
        VolumeState::Available,
        VolumeState::Full,
        VolumeState::ReadOnly,
        VolumeState::Deleting,
    ];
    // Ensure all variants are different
    for i in 0..states.len() {
        for j in i + 1..states.len() {
            assert_ne!(states[i], states[j]);
        }
    }
}

// ============================================================================
// NodeState tests
// ============================================================================

#[test]
fn test_node_state_default() {
    assert_eq!(NodeState::default(), NodeState::Healthy);
}

#[test]
fn test_node_state_all_variants() {
    let states = vec![
        NodeState::Healthy,
        NodeState::Degraded,
        NodeState::Unavailable,
    ];
    for i in 0..states.len() {
        for j in i + 1..states.len() {
            assert_ne!(states[i], states[j]);
        }
    }
}

// ============================================================================
// DataNodeInfo tests
// ============================================================================

#[test]
fn test_data_node_info_url() {
    let node = DataNodeInfo {
        id: NodeId("n1".to_string()),
        address: "192.168.1.1".to_string(),
        rack_id: RackId("r1".to_string()),
        data_center_id: DataCenterId("dc1".to_string()),
        total_space: 1000,
        used_space: 500,
        volume_count: 5,
        state: NodeState::Healthy,
        last_heartbeat: chrono::Utc::now(),
        grpc_port: 8080,
        http_port: 8081,
        public_url: "".to_string(),
        maintenance_mode: false,
    };
    assert_eq!(node.url(), "192.168.1.1:8081");
}

// ============================================================================
// Topology tests
// ============================================================================

#[test]
fn test_topology_new_is_empty() {
    let topology = Topology::new();
    assert!(topology.data_centers.is_empty());
    assert!(topology.list_all_nodes().is_empty());
}

#[test]
fn test_topology_get_or_create_data_center() {
    let mut topology = Topology::new();
    let dc = topology.get_or_create_data_center(DataCenterId("dc1".to_string()));
    assert_eq!(dc.id, DataCenterId("dc1".to_string()));
    assert!(dc.racks.is_empty());
    assert_eq!(topology.data_centers.len(), 1);
}

#[test]
fn test_topology_get_or_create_data_center_idempotent() {
    let mut topology = Topology::new();
    topology.get_or_create_data_center(DataCenterId("dc1".to_string()));
    topology.get_or_create_data_center(DataCenterId("dc1".to_string()));
    assert_eq!(topology.data_centers.len(), 1);
}

#[test]
fn test_topology_get_or_create_rack() {
    let mut topology = Topology::new();
    let rack =
        topology.get_or_create_rack(DataCenterId("dc1".to_string()), RackId("r1".to_string()));
    assert_eq!(rack.id, RackId("r1".to_string()));
    assert_eq!(rack.data_center_id, DataCenterId("dc1".to_string()));
    assert!(rack.nodes.is_empty());
}

#[test]
fn test_topology_get_or_create_node() {
    let mut topology = Topology::new();
    let node = DataNodeInfo::new(
        NodeId("n1".to_string()),
        "192.168.1.1".to_string(),
        RackId("r1".to_string()),
        DataCenterId("dc1".to_string()),
        8080,
        9090,
        "http://n1.local".to_string(),
    );
    let node_ref = topology.get_or_create_node(node);
    assert_eq!(node_ref.id, NodeId("n1".to_string()));
    assert_eq!(node_ref.address, "192.168.1.1");
    assert_eq!(node_ref.http_port, 8080);
    assert_eq!(node_ref.grpc_port, 9090);
    assert_eq!(node_ref.public_url, "http://n1.local");
    assert_eq!(node_ref.state, NodeState::Healthy);
}

#[test]
fn test_topology_get_node_existing() {
    let mut topology = Topology::new();
    let node = DataNodeInfo::new(
        NodeId("n1".to_string()),
        "192.168.1.1".to_string(),
        RackId("r1".to_string()),
        DataCenterId("dc1".to_string()),
        8080,
        9090,
        "".to_string(),
    );
    topology.get_or_create_node(node);
    let found = topology.get_node(&NodeId("n1".to_string()));
    assert!(found.is_some());
    assert_eq!(found.unwrap().address, "192.168.1.1");
}

#[test]
fn test_topology_get_node_nonexistent() {
    let topology = Topology::new();
    let found = topology.get_node(&NodeId("n1".to_string()));
    assert!(found.is_none());
}

#[test]
fn test_topology_get_node_mut_existing() {
    let mut topology = Topology::new();
    let node = DataNodeInfo::new(
        NodeId("n1".to_string()),
        "192.168.1.1".to_string(),
        RackId("r1".to_string()),
        DataCenterId("dc1".to_string()),
        8080,
        9090,
        "".to_string(),
    );
    topology.get_or_create_node(node);
    let node = topology.get_node_mut(&NodeId("n1".to_string()));
    assert!(node.is_some());
    node.unwrap().used_space = 500;
    // Verify the change persisted
    let found = topology.get_node(&NodeId("n1".to_string()));
    assert_eq!(found.unwrap().used_space, 500);
}

#[test]
fn test_topology_get_node_mut_nonexistent() {
    let mut topology = Topology::new();
    let found = topology.get_node_mut(&NodeId("n1".to_string()));
    assert!(found.is_none());
}

#[test]
fn test_topology_remove_node_existing() {
    let mut topology = Topology::new();
    let node = DataNodeInfo::new(
        NodeId("n1".to_string()),
        "192.168.1.1".to_string(),
        RackId("r1".to_string()),
        DataCenterId("dc1".to_string()),
        8080,
        9090,
        "".to_string(),
    );
    topology.get_or_create_node(node);
    let removed = topology.remove_node(&NodeId("n1".to_string()));
    assert!(removed.is_some());
    assert!(topology.get_node(&NodeId("n1".to_string())).is_none());
}

#[test]
fn test_topology_remove_node_nonexistent() {
    let mut topology = Topology::new();
    let removed = topology.remove_node(&NodeId("n1".to_string()));
    assert!(removed.is_none());
}

#[test]
fn test_topology_list_all_nodes() {
    let mut topology = Topology::new();
    let node1 = DataNodeInfo::new(
        NodeId("n1".to_string()),
        "10.0.0.1".to_string(),
        RackId("r1".to_string()),
        DataCenterId("dc1".to_string()),
        8080,
        9090,
        "".to_string(),
    );
    topology.get_or_create_node(node1);
    let node2 = DataNodeInfo::new(
        NodeId("n2".to_string()),
        "10.0.0.2".to_string(),
        RackId("r2".to_string()),
        DataCenterId("dc1".to_string()),
        8080,
        9090,
        "".to_string(),
    );
    topology.get_or_create_node(node2);
    let node3 = DataNodeInfo::new(
        NodeId("n3".to_string()),
        "10.0.1.1".to_string(),
        RackId("r1".to_string()),
        DataCenterId("dc2".to_string()),
        8080,
        9090,
        "".to_string(),
    );
    topology.get_or_create_node(node3);
    let nodes = topology.list_all_nodes();
    assert_eq!(nodes.len(), 3);
}

#[test]
fn test_topology_multiple_racks_same_dc() {
    let mut topology = Topology::new();
    topology.get_or_create_rack(DataCenterId("dc1".to_string()), RackId("r1".to_string()));
    topology.get_or_create_rack(DataCenterId("dc1".to_string()), RackId("r2".to_string()));
    let dc = topology.get_or_create_data_center(DataCenterId("dc1".to_string()));
    assert_eq!(dc.racks.len(), 2);
}

// ============================================================================
// RaftConfig tests
// ============================================================================

#[test]
fn test_raft_config_default() {
    let config = RaftConfig::default();
    assert_eq!(config.heartbeat_interval, 100);
    assert_eq!(config.election_timeout_min, 300);
    assert_eq!(config.election_timeout_max, 500);
    assert_eq!(config.snapshot_interval, 60000);
    assert_eq!(config.max_log_entries, 10000);
}

// ============================================================================
// ClusterConfig tests
// ============================================================================

#[test]
fn test_cluster_config_default() {
    let config = ClusterConfig::default();
    assert_eq!(config.replication_factor, 3);
    assert_eq!(config.volume_size_limit, 1024 * 1024 * 1024 * 1024);
    assert_eq!(config.max_volumes_per_node, 100);
    assert!(config.rack_awareness_enabled);
    assert!(!config.data_center_awareness_enabled);
}
