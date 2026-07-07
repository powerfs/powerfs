use bytes::Bytes;
use powerfs_common::types::{NeedleId, VolumeId};
use powerfs_core::needle::Needle;
use std::io::{Cursor, Seek, SeekFrom, Write};

// ============================================================================
// Needle::new tests
// ============================================================================

#[test]
fn test_needle_new_basic() {
    let id = NeedleId(1);
    let vid = VolumeId(100);
    let data = Bytes::from("hello world");
    let needle = Needle::new(id.clone(), vid, data.clone());

    assert_eq!(needle.id, id);
    assert_eq!(needle.volume_id, vid);
    assert_eq!(needle.data, data);
    assert_eq!(needle.offset, 0);
    assert_ne!(needle.checksum, 0);
}

#[test]
fn test_needle_new_checksum_deterministic() {
    let data = Bytes::from("test data");
    let n1 = Needle::new(NeedleId(1), VolumeId(1), data.clone());
    let n2 = Needle::new(NeedleId(2), VolumeId(2), data.clone());
    assert_eq!(n1.checksum, n2.checksum);
}

#[test]
fn test_needle_new_empty_data() {
    let data = Bytes::from("");
    let needle = Needle::new(NeedleId(0), VolumeId(0), data);
    // Should have a valid checksum even for empty data
    assert_eq!(needle.size(), 20); // header(12) + data(0) + footer(8)
}

// ============================================================================
// Needle::size tests
// ============================================================================

#[test]
fn test_needle_size_empty_data() {
    let needle = Needle::new(NeedleId(1), VolumeId(1), Bytes::from(""));
    // NEEDLE_HEADER_SIZE(12) + 0 data + NEEDLE_FOOTER_SIZE(8) = 20
    assert_eq!(needle.size(), 20);
}

#[test]
fn test_needle_size_with_data() {
    let needle = Needle::new(NeedleId(1), VolumeId(1), Bytes::from("hello"));
    assert_eq!(needle.size(), 12 + 5 + 8); // 25
}

#[test]
fn test_needle_size_large_data() {
    let data = Bytes::from(vec![0u8; 1024 * 1024]); // 1MB
    let needle = Needle::new(NeedleId(1), VolumeId(1), data);
    assert_eq!(needle.size(), 12 + 1024 * 1024 + 8);
}

// ============================================================================
// Needle::data_size tests
// ============================================================================

#[test]
fn test_needle_data_size() {
    let needle = Needle::new(NeedleId(1), VolumeId(1), Bytes::from("abcdefghij"));
    assert_eq!(needle.data_size(), 10);
}

// ============================================================================
// Needle::to_bytes and Needle::from_bytes round-trip tests
// ============================================================================

#[test]
fn test_needle_to_bytes_and_from_bytes_roundtrip() {
    let needle = Needle::new(
        NeedleId(42),
        VolumeId(7),
        Bytes::from("round-trip test data"),
    );
    let bytes = needle.to_bytes();

    let restored = Needle::from_bytes(&bytes, VolumeId(7), 1024).unwrap();
    assert_eq!(restored.id, NeedleId(42));
    assert_eq!(restored.volume_id, VolumeId(7));
    assert_eq!(restored.data, Bytes::from("round-trip test data"));
    assert_eq!(restored.offset, 1024);
    assert_eq!(restored.checksum, needle.checksum);
}

#[test]
fn test_needle_to_bytes_empty_data() {
    let needle = Needle::new(NeedleId(1), VolumeId(1), Bytes::from(""));
    let bytes = needle.to_bytes();
    // header(12) + empty data + footer(8) = 20
    assert_eq!(bytes.len(), 20);

    let restored = Needle::from_bytes(&bytes, VolumeId(1), 0).unwrap();
    assert_eq!(restored.data, Bytes::from(""));
    assert_eq!(restored.data_size(), 0);
}

#[test]
fn test_needle_to_bytes_single_byte() {
    let needle = Needle::new(NeedleId(99), VolumeId(1), Bytes::from(vec![0xAB]));
    let bytes = needle.to_bytes();
    assert_eq!(bytes.len(), 21); // 12 + 1 + 8

    let restored = Needle::from_bytes(&bytes, VolumeId(1), 500).unwrap();
    assert_eq!(restored.data, Bytes::from(vec![0xAB]));
}

#[test]
fn test_needle_to_bytes_binary_data() {
    let data = vec![0x00, 0xFF, 0xAB, 0xCD, 0x12, 0x34];
    let needle = Needle::new(NeedleId(1), VolumeId(1), Bytes::from(data.clone()));
    let bytes = needle.to_bytes();
    let restored = Needle::from_bytes(&bytes, VolumeId(1), 0).unwrap();
    assert_eq!(restored.data, Bytes::from(data));
}

#[test]
fn test_needle_to_bytes_max_id() {
    let needle = Needle::new(NeedleId(u64::MAX), VolumeId(1), Bytes::from("max"));
    let bytes = needle.to_bytes();
    let restored = Needle::from_bytes(&bytes, VolumeId(1), 0).unwrap();
    assert_eq!(restored.id, NeedleId(u64::MAX));
}

#[test]
fn test_needle_to_bytes_zero_id() {
    let needle = Needle::new(NeedleId(0), VolumeId(0), Bytes::from("zero"));
    let bytes = needle.to_bytes();
    let restored = Needle::from_bytes(&bytes, VolumeId(0), 0).unwrap();
    assert_eq!(restored.id, NeedleId(0));
}

// ============================================================================
// Needle::from_bytes error tests
// ============================================================================

#[test]
fn test_needle_from_bytes_too_short() {
    let bytes = vec![0u8; 10]; // Less than NEEDLE_HEADER_SIZE + NEEDLE_FOOTER_SIZE (20)
    let result = Needle::from_bytes(&bytes, VolumeId(1), 0);
    assert!(result.is_err());
}

#[test]
fn test_needle_from_bytes_exactly_min_size() {
    // Minimum: header(12) + footer(8) = 20, but data_size is encoded in header.
    // If header says data_size=0, total is 20. Let's build such bytes.
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&1u64.to_be_bytes()); // id=1
    bytes.extend_from_slice(&0u32.to_be_bytes()); // data_size=0
                                                  // no data
    bytes.extend_from_slice(&calculate_test_checksum(b"").to_be_bytes()); // checksum

    let result = Needle::from_bytes(&bytes, VolumeId(1), 0);
    assert!(result.is_ok());
    let needle = result.unwrap();
    assert_eq!(needle.data_size(), 0);
}

#[test]
fn test_needle_from_bytes_size_mismatch() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&1u64.to_be_bytes()); // id=1
    bytes.extend_from_slice(&10u32.to_be_bytes()); // data_size=10, but we provide 5
    bytes.extend_from_slice(b"hello"); // only 5 bytes
    bytes.extend_from_slice(&0u64.to_be_bytes()); // checksum placeholder

    let result = Needle::from_bytes(&bytes, VolumeId(1), 0);
    assert!(result.is_err());
}

#[test]
fn test_needle_from_bytes_checksum_mismatch() {
    let data = Bytes::from("hello");
    let needle = Needle::new(NeedleId(1), VolumeId(1), data);
    let mut bytes = needle.to_bytes().to_vec();
    // Corrupt the data portion (bytes after header, before footer)
    bytes[12] ^= 0xFF; // flip bits in first data byte

    let result = Needle::from_bytes(&bytes, VolumeId(1), 0);
    assert!(result.is_err());
}

#[test]
fn test_needle_from_bytes_corrupted_checksum() {
    let needle = Needle::new(NeedleId(1), VolumeId(1), Bytes::from("hello"));
    let mut bytes = needle.to_bytes().to_vec();
    // Corrupt the checksum
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF;

    let result = Needle::from_bytes(&bytes, VolumeId(1), 0);
    assert!(result.is_err());
}

// ============================================================================
// Needle::read_from tests
// ============================================================================

#[test]
fn test_needle_read_from_basic() {
    let needle = Needle::new(NeedleId(100), VolumeId(5), Bytes::from("read test"));
    let mut cursor = Cursor::new(Vec::new());
    needle.write_to(&mut cursor, 0).unwrap();

    cursor.seek(SeekFrom::Start(0)).unwrap();
    let restored = Needle::read_from(&mut cursor, 0, VolumeId(5)).unwrap();
    assert_eq!(restored.id, NeedleId(100));
    assert_eq!(restored.data, Bytes::from("read test"));
    assert_eq!(restored.checksum, needle.checksum);
}

#[test]
fn test_needle_read_from_nonzero_offset() {
    let mut cursor = Cursor::new(Vec::new());
    // Write some padding first
    cursor.write_all(&[0u8; 100]).unwrap();

    let needle = Needle::new(NeedleId(42), VolumeId(3), Bytes::from("offset test"));
    needle.write_to(&mut cursor, 100).unwrap();

    let restored = Needle::read_from(&mut cursor, 100, VolumeId(3)).unwrap();
    assert_eq!(restored.id, NeedleId(42));
    assert_eq!(restored.data, Bytes::from("offset test"));
    assert_eq!(restored.offset, 100);
}

#[test]
fn test_needle_read_from_checksum_mismatch() {
    let needle = Needle::new(NeedleId(1), VolumeId(1), Bytes::from("data"));
    let mut cursor = Cursor::new(Vec::new());
    needle.write_to(&mut cursor, 0).unwrap();

    // Corrupt data in cursor
    let mut vec = cursor.into_inner();
    vec[12] ^= 0xFF;
    let mut cursor = Cursor::new(vec);

    let result = Needle::read_from(&mut cursor, 0, VolumeId(1));
    assert!(result.is_err());
}

// ============================================================================
// Needle::write_to tests
// ============================================================================

#[test]
fn test_needle_write_to_basic() {
    let needle = Needle::new(NeedleId(7), VolumeId(99), Bytes::from("write test"));
    let mut cursor = Cursor::new(Vec::new());
    needle.write_to(&mut cursor, 0).unwrap();

    let written = cursor.into_inner();
    let expected = needle.to_bytes().to_vec();
    assert_eq!(written, expected);
}

#[test]
fn test_needle_write_to_at_offset() {
    let needle = Needle::new(NeedleId(1), VolumeId(1), Bytes::from("offset write"));
    let mut cursor = Cursor::new(vec![0u8; 50]);
    needle.write_to(&mut cursor, 50).unwrap();

    let vec = cursor.into_inner();
    // First 50 bytes should be zeros
    assert!(vec[0..50].iter().all(|&b| b == 0));
    // After 50, should be needle data
    assert!(vec.len() >= 50 + needle.size());
}

#[test]
fn test_needle_write_to_multiple() {
    let n1 = Needle::new(NeedleId(1), VolumeId(1), Bytes::from("first"));
    let n2 = Needle::new(NeedleId(2), VolumeId(1), Bytes::from("second"));

    let mut cursor = Cursor::new(Vec::new());
    n1.write_to(&mut cursor, 0).unwrap();
    n2.write_to(&mut cursor, n1.size() as u64).unwrap();

    let vec = cursor.into_inner();
    let r1 = Needle::from_bytes(&vec[0..n1.size()], VolumeId(1), 0).unwrap();
    let r2 = Needle::from_bytes(
        &vec[n1.size()..n1.size() + n2.size()],
        VolumeId(1),
        n1.size() as u64,
    )
    .unwrap();

    assert_eq!(r1.data, Bytes::from("first"));
    assert_eq!(r2.data, Bytes::from("second"));
}

// ============================================================================
// Needle::to_info tests
// ============================================================================

#[test]
fn test_needle_to_info() {
    let needle = Needle::new(NeedleId(10), VolumeId(20), Bytes::from("info test"));
    let info = needle.to_info();

    assert_eq!(info.id, NeedleId(10));
    assert_eq!(info.volume_id, VolumeId(20));
    assert_eq!(info.data_size, 9);
    assert_eq!(info.offset, 0);
    assert_eq!(info.checksum, needle.checksum);
}

#[test]
fn test_needle_to_info_with_offset() {
    let needle = Needle::new(NeedleId(1), VolumeId(1), Bytes::from("data"));
    let mut cursor = Cursor::new(Vec::new());
    needle.write_to(&mut cursor, 0).unwrap();

    // Read back with offset
    let restored = Needle::read_from(&mut cursor, 0, VolumeId(1)).unwrap();
    let info = restored.to_info();
    assert_eq!(info.offset, 0);
}

// ============================================================================
// Helper function
// ============================================================================

fn calculate_test_checksum(data: &[u8]) -> u64 {
    use crc32c::crc32c;
    let crc = crc32c(data);
    let mut result = 0u64;
    for (i, byte) in crc.to_be_bytes().iter().enumerate() {
        result |= (*byte as u64) << (8 * i);
    }
    result
}

// ============================================================================
// Boundary tests
// ============================================================================

#[test]
fn test_needle_boundary_large_data() {
    let data_size = 10 * 1024 * 1024;
    let data = Bytes::from(vec![0xAAu8; data_size]);
    let needle = Needle::new(NeedleId(1), VolumeId(1), data.clone());

    assert_eq!(needle.data_size(), data_size);
    assert_eq!(needle.size(), 12 + data_size + 8);

    let bytes = needle.to_bytes();
    assert_eq!(bytes.len(), needle.size());

    let restored = Needle::from_bytes(&bytes, VolumeId(1), 0).unwrap();
    assert_eq!(restored.data, data);
}

#[test]
fn test_needle_boundary_special_chars() {
    let data = vec![
        0x00, 0x01, 0x7F, 0xFF, 0x80, 0x9F, 0xEF, 0xBF, 0xBD, b'\n', b'\r', b'\t', b'\0', b'"',
        b'\'', b'\\', b'/',
    ];
    let needle = Needle::new(NeedleId(1), VolumeId(1), Bytes::from(data.clone()));
    let bytes = needle.to_bytes();
    let restored = Needle::from_bytes(&bytes, VolumeId(1), 0).unwrap();
    assert_eq!(restored.data, Bytes::from(data));
}

#[test]
fn test_needle_boundary_empty_id() {
    let needle = Needle::new(NeedleId(0), VolumeId(1), Bytes::from("empty id"));
    let bytes = needle.to_bytes();
    let restored = Needle::from_bytes(&bytes, VolumeId(1), 0).unwrap();
    assert_eq!(restored.id, NeedleId(0));
}

#[test]
fn test_needle_boundary_max_volume_id() {
    let needle = Needle::new(NeedleId(1), VolumeId(u32::MAX), Bytes::from("max volume"));
    assert_eq!(needle.volume_id, VolumeId(u32::MAX));
}

#[test]
fn test_needle_boundary_large_offset() {
    let needle = Needle::new(NeedleId(1), VolumeId(1), Bytes::from("test"));
    let info = needle.to_info();
    assert_eq!(info.offset, 0);

    let large_offset = 1024 * 1024 * 1024;
    let mut cursor = Cursor::new(vec![0u8; large_offset as usize + needle.size()]);
    needle.write_to(&mut cursor, large_offset).unwrap();

    let restored = Needle::read_from(&mut cursor, large_offset, VolumeId(1)).unwrap();
    assert_eq!(restored.offset, large_offset);
}
