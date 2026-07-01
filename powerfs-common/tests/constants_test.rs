use powerfs_common::constants::*;

// ============================================================================
// Needle-related constants
// ============================================================================

#[test]
fn test_needle_header_size() {
    assert_eq!(NEEDLE_HEADER_SIZE, NEEDLE_ID_SIZE + 4);
    assert_eq!(NEEDLE_HEADER_SIZE, 12); // 8 (id) + 4 (data size)
}

#[test]
fn test_needle_footer_size() {
    assert_eq!(NEEDLE_FOOTER_SIZE, 8); // checksum
}

#[test]
fn test_needle_min_size() {
    assert_eq!(NEEDLE_MIN_SIZE, NEEDLE_HEADER_SIZE + NEEDLE_FOOTER_SIZE);
    assert_eq!(NEEDLE_MIN_SIZE, 20);
}

#[test]
fn test_needle_id_size() {
    assert_eq!(NEEDLE_ID_SIZE, 8);
}

#[test]
fn test_needle_checksum_size() {
    assert_eq!(NEEDLE_CHECKSUM_SIZE, 8);
}

// ============================================================================
// Volume-related constants
// ============================================================================

#[test]
fn test_volume_index_size() {
    assert_eq!(VOLUME_INDEX_SIZE, 64);
}

#[test]
fn test_volume_index_offset() {
    assert_eq!(VOLUME_INDEX_OFFSET, 0);
}

#[test]
fn test_volume_data_offset() {
    assert_eq!(VOLUME_DATA_OFFSET, 1024 * 1024);
}

// ============================================================================
// Port constants
// ============================================================================

#[test]
fn test_default_ports() {
    assert_eq!(MASTER_DEFAULT_PORT, 9333);
    assert_eq!(VOLUME_DEFAULT_PORT, 8080);
    assert_eq!(FUSE_DEFAULT_PORT, 7373);
}

// ============================================================================
// Heartbeat constants
// ============================================================================

#[test]
fn test_heartbeat_constants() {
    assert_eq!(HEARTBEAT_INTERVAL_MS, 100);
    assert_eq!(HEARTBEAT_TIMEOUT_MS, 500);
}

// ============================================================================
// System constants
// ============================================================================

#[test]
fn test_max_path_length() {
    assert_eq!(MAX_PATH_LENGTH, 4096);
}

#[test]
fn test_default_volume_size() {
    assert_eq!(DEFAULT_VOLUME_SIZE, 1024 * 1024 * 1024 * 1024); // 1 TB
}

#[test]
fn test_default_replica_count() {
    assert_eq!(DEFAULT_REPLICA_COUNT, 3);
}

// ============================================================================
// Algorithm/version constants
// ============================================================================

#[test]
fn test_checksum_algorithm() {
    assert_eq!(CHECKSUM_ALGORITHM, "BLAKE3");
}

#[test]
fn test_metadata_version() {
    assert_eq!(METADATA_VERSION, "v1");
}

#[test]
fn test_powerfs_version() {
    assert_eq!(POWERFS_VERSION, "0.1.0");
}

// ============================================================================
// Block size constants
// ============================================================================

#[test]
fn test_block_sizes() {
    assert_eq!(DEFAULT_BLOCK_SIZE, 64 * 1024);
    assert_eq!(MAX_BLOCK_SIZE, 1024 * 1024);
}

// ============================================================================
// Cache size constants
// ============================================================================

#[test]
fn test_cache_sizes() {
    assert_eq!(LRU_CACHE_SIZE, 100000);
    assert_eq!(INDEX_CACHE_SIZE, 10000);
}
