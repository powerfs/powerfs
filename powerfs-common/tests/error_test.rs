use powerfs_common::error::*;
use powerfs_common::types::{NeedleId, VolumeId};

// ============================================================================
// PowerFsError display tests
// ============================================================================

#[test]
fn test_error_volume_not_found_display() {
    let err = PowerFsError::VolumeNotFound(VolumeId(42));
    assert_eq!(err.to_string(), "volume not found: 42");
}

#[test]
fn test_error_needle_not_found_display() {
    let err = PowerFsError::NeedleNotFound(NeedleId(123));
    assert_eq!(err.to_string(), "needle not found: 123");
}

#[test]
fn test_error_volume_exists_display() {
    let err = PowerFsError::VolumeExists(VolumeId(7));
    assert_eq!(err.to_string(), "volume already exists: 7");
}

#[test]
fn test_error_invalid_volume_state_display() {
    let err = PowerFsError::InvalidVolumeState("corrupted".to_string());
    assert_eq!(err.to_string(), "invalid volume state: corrupted");
}

#[test]
fn test_error_invalid_master_state_display() {
    let err = PowerFsError::InvalidMasterState("unknown".to_string());
    assert_eq!(err.to_string(), "invalid master state: unknown");
}

#[test]
fn test_error_invalid_request_display() {
    let err = PowerFsError::InvalidRequest("bad input".to_string());
    assert_eq!(err.to_string(), "invalid request: bad input");
}

#[test]
fn test_error_internal_display() {
    let err = PowerFsError::Internal("something broke".to_string());
    assert_eq!(err.to_string(), "internal error: something broke");
}

#[test]
fn test_error_timeout_display() {
    assert_eq!(PowerFsError::Timeout.to_string(), "timeout");
}

#[test]
fn test_error_connection_refused_display() {
    assert_eq!(
        PowerFsError::ConnectionRefused.to_string(),
        "connection refused"
    );
}

#[test]
fn test_error_not_leader_display() {
    assert_eq!(PowerFsError::NotLeader.to_string(), "not leader");
}

#[test]
fn test_error_quorum_not_reached_display() {
    assert_eq!(
        PowerFsError::QuorumNotReached.to_string(),
        "quorum not reached"
    );
}

#[test]
fn test_error_checksum_mismatch_display() {
    assert_eq!(
        PowerFsError::ChecksumMismatch.to_string(),
        "checksum mismatch"
    );
}

#[test]
fn test_error_out_of_space_display() {
    assert_eq!(PowerFsError::OutOfSpace.to_string(), "out of space");
}

#[test]
fn test_error_permission_denied_display() {
    assert_eq!(
        PowerFsError::PermissionDenied.to_string(),
        "permission denied"
    );
}

#[test]
fn test_error_file_not_found_display() {
    let err = PowerFsError::FileNotFound("/tmp/test.txt".to_string());
    assert_eq!(err.to_string(), "file not found: /tmp/test.txt");
}

#[test]
fn test_error_directory_not_found_display() {
    let err = PowerFsError::DirectoryNotFound("/tmp/test".to_string());
    assert_eq!(err.to_string(), "directory not found: /tmp/test");
}

#[test]
fn test_error_file_exists_display() {
    let err = PowerFsError::FileExists("/tmp/test.txt".to_string());
    assert_eq!(err.to_string(), "file already exists: /tmp/test.txt");
}

#[test]
fn test_error_path_too_long_display() {
    assert_eq!(PowerFsError::PathTooLong.to_string(), "path too long");
}

#[test]
fn test_error_invalid_path_display() {
    let err = PowerFsError::InvalidPath("bad///path".to_string());
    assert_eq!(err.to_string(), "invalid path: bad///path");
}

#[test]
fn test_error_storage_display() {
    let err = PowerFsError::Storage("disk full".to_string());
    assert_eq!(err.to_string(), "storage error: disk full");
}

// ============================================================================
// PowerFsError from conversions
// ============================================================================

#[test]
fn test_error_from_io_error() {
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
    let err: PowerFsError = io_err.into();
    assert!(matches!(err, PowerFsError::Io(_)));
    assert_eq!(err.to_string(), "io error: file missing");
}

#[test]
fn test_error_from_io_error_kind() {
    let io_err = std::io::Error::from(std::io::ErrorKind::PermissionDenied);
    let err: PowerFsError = io_err.into();
    assert!(matches!(err, PowerFsError::Io(_)));
}

// ============================================================================
// Result alias tests
// ============================================================================

#[test]
fn test_result_ok() {
    let result: Result<i32> = Ok(42);
    assert!(result.is_ok());
    assert_eq!(result.as_ref().unwrap(), &42);
}

#[test]
fn test_result_err() {
    let result: Result<i32> = Err(PowerFsError::Timeout);
    assert!(result.is_err());
}

// ============================================================================
// Error debug format
// ============================================================================

#[test]
fn test_error_debug_format() {
    let err = PowerFsError::VolumeNotFound(VolumeId(1));
    let debug_str = format!("{:?}", err);
    assert!(debug_str.contains("VolumeNotFound"));
    assert!(debug_str.contains("1"));
}

#[test]
fn test_error_struct_variant_debug() {
    let err = PowerFsError::InvalidRequest("test".to_string());
    let debug_str = format!("{:?}", err);
    assert!(debug_str.contains("InvalidRequest"));
    assert!(debug_str.contains("test"));
}
