pub const NEEDLE_HEADER_SIZE: usize = NEEDLE_ID_SIZE + 4;
pub const NEEDLE_FOOTER_SIZE: usize = 8;
pub const NEEDLE_MIN_SIZE: usize = NEEDLE_HEADER_SIZE + NEEDLE_FOOTER_SIZE;
pub const NEEDLE_ID_SIZE: usize = 8;
pub const NEEDLE_CHECKSUM_SIZE: usize = 8;

pub const VOLUME_INDEX_SIZE: usize = 64;
pub const VOLUME_INDEX_OFFSET: u64 = 0;
pub const VOLUME_DATA_OFFSET: u64 = 1024 * 1024;

pub const MASTER_DEFAULT_PORT: u16 = 9333;
pub const VOLUME_DEFAULT_PORT: u16 = 8080;
pub const FUSE_DEFAULT_PORT: u16 = 7373;

pub const HEARTBEAT_INTERVAL_MS: u64 = 100;
pub const HEARTBEAT_TIMEOUT_MS: u64 = 500;

pub const MAX_PATH_LENGTH: usize = 4096;

pub const DEFAULT_VOLUME_SIZE: u64 = 1024 * 1024 * 1024 * 1024;
pub const DEFAULT_REPLICA_COUNT: u32 = 3;

pub const CHECKSUM_ALGORITHM: &str = "BLAKE3";

pub const METADATA_VERSION: &str = "v1";

pub const POWERFS_VERSION: &str = "0.1.0";

pub const DEFAULT_BLOCK_SIZE: usize = 64 * 1024;
pub const MAX_BLOCK_SIZE: usize = 1024 * 1024;

/// Number of needle IDs reserved per file_key allocation.
///
/// Each file gets a non-overlapping block of needle IDs: [file_key, file_key + FILE_KEY_BLOCK_SIZE).
/// Chunks within a file use needle_id = file_key + chunk_idx, so consecutive files
/// never collide. With 2MB chunks, this supports files up to 2TB (1M chunks × 2MB).
/// u64 capacity: 2^64 / 1M = 1.8×10^13 files per volume (practically unlimited).
pub const FILE_KEY_BLOCK_SIZE: u64 = 1_048_576; // 1M chunks per file = 2TB max @ 2MB chunks

pub const LRU_CACHE_SIZE: usize = 100_000;
pub const INDEX_CACHE_SIZE: usize = 10_000;
