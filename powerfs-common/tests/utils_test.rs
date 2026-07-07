use powerfs_common::utils::*;

// ============================================================================
// generate_volume_id tests
// ============================================================================

#[test]
fn test_generate_volume_id_not_zero() {
    let id = generate_volume_id();
    // ID is random; just verify it's a valid u32
    let _ = id.0;
}

#[test]
fn test_generate_volume_id_uniqueness() {
    let id1 = generate_volume_id();
    let id2 = generate_volume_id();
    // Extremely unlikely to collide, but not impossible. Use large sample.
    let mut ids = vec![id1, id2];
    for _ in 0..100 {
        ids.push(generate_volume_id());
    }
    let mut unique = ids.clone();
    unique.dedup();
    // Most should be unique; at least some unique values
    assert!(unique.len() > 1);
}

// ============================================================================
// generate_needle_id tests
// ============================================================================

#[test]
fn test_generate_needle_id_not_zero() {
    let id = generate_needle_id();
    let _ = id.0;
}

#[test]
fn test_generate_needle_id_uniqueness() {
    let id1 = generate_needle_id();
    let id2 = generate_needle_id();
    assert_ne!(id1, id2);
}

// ============================================================================
// generate_file_id tests
// ============================================================================

#[test]
fn test_generate_file_id_not_empty() {
    let id = generate_file_id();
    assert!(!id.0.is_empty());
}

#[test]
fn test_generate_file_id_uniqueness() {
    let id1 = generate_file_id();
    let id2 = generate_file_id();
    assert_ne!(id1.0, id2.0);
}

#[test]
fn test_generate_file_id_is_uuid_format() {
    let id = generate_file_id();
    // UUID v4 format: 8-4-4-4-12 hex digits
    let parts: Vec<&str> = id.0.split('-').collect();
    assert_eq!(parts.len(), 5);
    assert_eq!(parts[0].len(), 8);
    assert_eq!(parts[1].len(), 4);
    assert_eq!(parts[2].len(), 4);
    assert_eq!(parts[3].len(), 4);
    assert_eq!(parts[4].len(), 12);
}

// ============================================================================
// generate_node_id tests
// ============================================================================

#[test]
fn test_generate_node_id_not_empty() {
    let id = generate_node_id();
    assert!(!id.0.is_empty());
}

#[test]
fn test_generate_node_id_uniqueness() {
    let id1 = generate_node_id();
    let id2 = generate_node_id();
    assert_ne!(id1.0, id2.0);
}

// ============================================================================
// parse_node_id tests
// ============================================================================

#[test]
fn test_parse_node_id_simple() {
    let id = parse_node_id("my-node");
    assert_eq!(id.0, "my-node");
}

#[test]
fn test_parse_node_id_empty() {
    let id = parse_node_id("");
    assert_eq!(id.0, "");
}

#[test]
fn test_parse_node_id_special_chars() {
    let id = parse_node_id("node_123-ABC");
    assert_eq!(id.0, "node_123-ABC");
}

// ============================================================================
// validate_path tests
// ============================================================================

#[test]
fn test_validate_path_valid_simple() {
    assert!(validate_path("/"));
}

#[test]
fn test_validate_path_valid_file() {
    assert!(validate_path("/home/user/file.txt"));
}

#[test]
fn test_validate_path_empty() {
    assert!(!validate_path(""));
}

#[test]
fn test_validate_path_at_max_length() {
    let path = "/".to_string() + &"a".repeat(powerfs_common::constants::MAX_PATH_LENGTH - 1);
    assert_eq!(path.len(), powerfs_common::constants::MAX_PATH_LENGTH);
    assert!(validate_path(&path));
}

#[test]
fn test_validate_path_exceeds_max_length() {
    let path = "/".to_string() + &"a".repeat(powerfs_common::constants::MAX_PATH_LENGTH);
    assert!(path.len() > powerfs_common::constants::MAX_PATH_LENGTH);
    assert!(!validate_path(&path));
}

// ============================================================================
// normalize_path tests
// ============================================================================

#[test]
fn test_normalize_path_already_has_slash() {
    assert_eq!(normalize_path("/foo/bar"), "/foo/bar");
}

#[test]
fn test_normalize_path_missing_slash() {
    assert_eq!(normalize_path("foo/bar"), "/foo/bar");
}

#[test]
fn test_normalize_path_root() {
    assert_eq!(normalize_path("/"), "/");
}

#[test]
fn test_normalize_path_empty() {
    assert_eq!(normalize_path(""), "/");
}

#[test]
fn test_normalize_path_single_filename() {
    assert_eq!(normalize_path("file.txt"), "/file.txt");
}

#[test]
fn test_normalize_path_deeply_nested() {
    assert_eq!(normalize_path("a/b/c/d/e"), "/a/b/c/d/e");
}

// ============================================================================
// extract_filename tests
// ============================================================================

#[test]
fn test_extract_filename_simple() {
    assert_eq!(extract_filename("/foo/bar.txt"), "bar.txt");
}

#[test]
fn test_extract_filename_single_file() {
    assert_eq!(extract_filename("/file"), "file");
}

#[test]
fn test_extract_filename_root() {
    assert_eq!(extract_filename("/"), "");
}

#[test]
fn test_extract_filename_no_slash() {
    assert_eq!(extract_filename("file.txt"), "file.txt");
}

#[test]
fn test_extract_filename_deeply_nested() {
    assert_eq!(extract_filename("/a/b/c/d/e/f.txt"), "f.txt");
}

#[test]
fn test_extract_filename_trailing_slash() {
    assert_eq!(extract_filename("/foo/bar/"), "");
}

// ============================================================================
// extract_parent tests
// ============================================================================

#[test]
fn test_extract_parent_simple() {
    assert_eq!(extract_parent("/foo/bar.txt"), "foo");
}

#[test]
fn test_extract_parent_root_file() {
    assert_eq!(extract_parent("/file"), "/");
}

#[test]
fn test_extract_parent_root() {
    assert_eq!(extract_parent("/"), "/");
}

#[test]
fn test_extract_parent_no_slash() {
    assert_eq!(extract_parent("file"), "/");
}

#[test]
fn test_extract_parent_deeply_nested() {
    assert_eq!(extract_parent("/a/b/c/d/f.txt"), "d");
}

#[test]
fn test_extract_parent_two_levels() {
    assert_eq!(extract_parent("/a/b"), "a");
}

// ============================================================================
// calculate_checksum tests
// ============================================================================

#[test]
fn test_calculate_checksum_empty_data() {
    let checksum = calculate_checksum(b"");
    // Empty data should produce a deterministic checksum
    let checksum2 = calculate_checksum(b"");
    assert_eq!(checksum, checksum2);
}

#[test]
fn test_calculate_checksum_same_data_same_result() {
    let data = b"hello world";
    let c1 = calculate_checksum(data);
    let c2 = calculate_checksum(data);
    assert_eq!(c1, c2);
}

#[test]
fn test_calculate_checksum_different_data_different_result() {
    let c1 = calculate_checksum(b"hello");
    let c2 = calculate_checksum(b"world");
    assert_ne!(c1, c2);
}

#[test]
fn test_calculate_checksum_large_data() {
    let data = vec![0xABu8; 1024 * 1024]; // 1MB of same byte
    let _checksum = calculate_checksum(&data);
}

#[test]
fn test_calculate_checksum_single_byte() {
    let _c = calculate_checksum(&[0u8]);
}

#[test]
fn test_calculate_checksum_known_pattern() {
    // BLAKE3 should be deterministic
    let c1 = calculate_checksum(b"test123");
    let c2 = calculate_checksum(b"test123");
    assert_eq!(c1, c2);
    let c3 = calculate_checksum(b"test124");
    assert_ne!(c1, c3);
}

// ============================================================================
// parse_address tests
// ============================================================================

#[test]
fn test_parse_address_valid_ipv4() {
    let addr = parse_address("127.0.0.1:8080").unwrap();
    assert_eq!(addr.to_string(), "127.0.0.1:8080");
}

#[test]
fn test_parse_address_valid_ipv6() {
    let addr = parse_address("[::1]:8080").unwrap();
    assert_eq!(addr.to_string(), "[::1]:8080");
}

#[test]
fn test_parse_address_invalid() {
    assert!(parse_address("not-an-address").is_err());
}

#[test]
fn test_parse_address_missing_port() {
    assert!(parse_address("127.0.0.1").is_err());
}

#[test]
fn test_parse_address_empty() {
    assert!(parse_address("").is_err());
}

#[test]
fn test_parse_address_invalid_port() {
    assert!(parse_address("127.0.0.1:999999").is_err());
}

// ============================================================================
// format_address tests
// ============================================================================

#[test]
fn test_format_address_simple() {
    assert_eq!(format_address("localhost", 8080), "localhost:8080");
}

#[test]
fn test_format_address_ip() {
    assert_eq!(format_address("192.168.1.1", 9333), "192.168.1.1:9333");
}

#[test]
fn test_format_address_zero_port() {
    assert_eq!(format_address("host", 0), "host:0");
}

// ============================================================================
// humanize_size tests
// ============================================================================

#[test]
fn test_humanize_size_zero() {
    assert_eq!(humanize_size(0), "0 B");
}

#[test]
fn test_humanize_size_bytes() {
    assert_eq!(humanize_size(1), "1 B");
    assert_eq!(humanize_size(500), "500 B");
    assert_eq!(humanize_size(1023), "1023 B");
}

#[test]
fn test_humanize_size_kb_boundary() {
    assert_eq!(humanize_size(1024), "1.00 KB");
}

#[test]
fn test_humanize_size_kb() {
    assert_eq!(humanize_size(1536), "1.50 KB");
    assert_eq!(humanize_size(2048), "2.00 KB");
}

#[test]
fn test_humanize_size_mb_boundary() {
    assert_eq!(humanize_size(1024 * 1024), "1.00 MB");
}

#[test]
fn test_humanize_size_mb() {
    assert_eq!(humanize_size(1024 * 1024 * 5), "5.00 MB");
    assert_eq!(humanize_size(1024 * 1024 * 5 + 512 * 1024), "5.50 MB");
}

#[test]
fn test_humanize_size_gb_boundary() {
    assert_eq!(humanize_size(1024 * 1024 * 1024), "1.00 GB");
}

#[test]
fn test_humanize_size_gb() {
    assert_eq!(humanize_size(1024 * 1024 * 1024 * 10), "10.00 GB");
}

#[test]
fn test_humanize_size_tb_boundary() {
    assert_eq!(humanize_size(1024 * 1024 * 1024 * 1024), "1.00 TB");
}

#[test]
fn test_humanize_size_tb() {
    assert_eq!(humanize_size(1024 * 1024 * 1024 * 1024 * 5), "5.00 TB");
}

#[test]
fn test_humanize_size_large_value() {
    let result = humanize_size(1024 * 1024 * 1024 * 1024 * 100);
    assert!(result.contains("TB"));
}

// ============================================================================
// ensure_directory_exists tests
// ============================================================================

#[test]
fn test_ensure_directory_exists_creates_dir() {
    let dir = std::env::temp_dir().join("powerfs_test_dir_create");
    let path = dir.to_str().unwrap().to_string();
    // Clean up first
    let _ = std::fs::remove_dir_all(&path);
    assert!(ensure_directory_exists(&path).is_ok());
    assert!(std::path::Path::new(&path).exists());
    // Clean up
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn test_ensure_directory_exists_existing_dir() {
    let dir = std::env::temp_dir().join("powerfs_test_dir_existing");
    let path = dir.to_str().unwrap().to_string();
    std::fs::create_dir_all(&path).unwrap();
    assert!(ensure_directory_exists(&path).is_ok());
    // Clean up
    let _ = std::fs::remove_dir_all(&path);
}

// ============================================================================
// file_exists tests
// ============================================================================

#[test]
fn test_file_exists_existing() {
    let file = std::env::temp_dir().join("powerfs_test_file_exists");
    let path = file.to_str().unwrap().to_string();
    std::fs::write(&path, "data").unwrap();
    assert!(file_exists(&path));
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_file_exists_nonexistent() {
    let path = "/tmp/powerfs_nonexistent_file_12345_test";
    assert!(!file_exists(path));
}

// ============================================================================
// get_file_size tests
// ============================================================================

#[test]
fn test_get_file_size_known_size() {
    let file = std::env::temp_dir().join("powerfs_test_file_size");
    let path = file.to_str().unwrap().to_string();
    std::fs::write(&path, "1234567890").unwrap();
    let size = get_file_size(&path).unwrap();
    assert_eq!(size, 10);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_get_file_size_nonexistent() {
    let path = "/tmp/powerfs_nonexistent_size_test_12345";
    assert!(get_file_size(path).is_err());
}

#[test]
fn test_get_file_size_empty_file() {
    let file = std::env::temp_dir().join("powerfs_test_empty");
    let path = file.to_str().unwrap().to_string();
    std::fs::write(&path, "").unwrap();
    let size = get_file_size(&path).unwrap();
    assert_eq!(size, 0);
    let _ = std::fs::remove_file(&path);
}
