use powerfs_common::storage::keys;

// ============================================================================
// Key prefix constants tests
// ============================================================================

#[test]
fn test_cluster_prefix() {
    assert_eq!(keys::CLUSTER_PREFIX, b"cluster/");
}

#[test]
fn test_volume_prefix() {
    assert_eq!(keys::VOLUME_PREFIX, b"volume/");
}

#[test]
fn test_node_prefix() {
    assert_eq!(keys::NODE_PREFIX, b"node/");
}

// ============================================================================
// volume_to_node_key tests
// ============================================================================

#[test]
fn test_volume_to_node_key_format() {
    let key = keys::volume_to_node_key("42");
    assert_eq!(key, b"volume/node/42");
}

#[test]
fn test_volume_to_node_key_empty_volume() {
    let key = keys::volume_to_node_key("");
    assert_eq!(key, b"volume/node/");
}

#[test]
fn test_volume_to_node_key_large_id() {
    let key = keys::volume_to_node_key("4294967295");
    assert_eq!(key, b"volume/node/4294967295");
}

// ============================================================================
// node_to_volumes_key tests
// ============================================================================

#[test]
fn test_node_to_volumes_key_format() {
    let key = keys::node_to_volumes_key("node-123");
    assert_eq!(key, b"node/volumes/node-123");
}

#[test]
fn test_node_to_volumes_key_empty_node() {
    let key = keys::node_to_volumes_key("");
    assert_eq!(key, b"node/volumes/");
}

#[test]
fn test_node_to_volumes_key_uuid_node() {
    let key = keys::node_to_volumes_key("550e8400-e29b-41d4-a716-446655440000");
    assert!(key.starts_with(b"node/volumes/"));
    assert!(key.len() > b"node/volumes/".len());
}

// ============================================================================
// volume_info_key tests
// ============================================================================

#[test]
fn test_volume_info_key_format() {
    let key = keys::volume_info_key("99");
    assert_eq!(key, b"volume/99");
}

#[test]
fn test_volume_info_key_empty() {
    let key = keys::volume_info_key("");
    assert_eq!(key, b"volume/");
}

// ============================================================================
// node_info_key tests
// ============================================================================

#[test]
fn test_node_info_key_format() {
    let key = keys::node_info_key("worker-01");
    assert_eq!(key, b"node/worker-01");
}

#[test]
fn test_node_info_key_empty() {
    let key = keys::node_info_key("");
    assert_eq!(key, b"node/");
}

// ============================================================================
// CLUSTER_CONFIG_KEY tests
// ============================================================================

#[test]
fn test_cluster_config_key() {
    assert_eq!(keys::CLUSTER_CONFIG_KEY, b"cluster/config");
}

// ============================================================================
// LEADER_INFO_KEY tests
// ============================================================================

#[test]
fn test_leader_info_key() {
    assert_eq!(keys::LEADER_INFO_KEY, b"cluster/leader");
}

// ============================================================================
// Key uniqueness tests
// ============================================================================

#[test]
fn test_volume_to_node_key_is_unique() {
    let key1 = keys::volume_to_node_key("1");
    let key2 = keys::volume_to_node_key("2");
    assert_ne!(key1, key2);
}

#[test]
fn test_node_to_volumes_key_is_unique() {
    let key1 = keys::node_to_volumes_key("node-a");
    let key2 = keys::node_to_volumes_key("node-b");
    assert_ne!(key1, key2);
}

#[test]
fn test_key_prefixes_dont_collide() {
    let vkey = keys::volume_info_key("test");
    let nkey = keys::node_info_key("test");
    assert_ne!(vkey, nkey);
}

#[test]
fn test_volume_to_node_and_volume_info_keys_differ() {
    let k1 = keys::volume_to_node_key("5");
    let k2 = keys::volume_info_key("5");
    assert_ne!(k1, k2);
}
