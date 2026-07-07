use crate::types::{FileId, NeedleId, NodeId, VolumeId};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use uuid::Uuid;

pub fn generate_volume_id() -> VolumeId {
    VolumeId(rand::thread_rng().gen::<u32>())
}

pub fn generate_needle_id() -> NeedleId {
    NeedleId(rand::thread_rng().gen::<u64>())
}

pub fn generate_file_id() -> FileId {
    FileId(Uuid::new_v4().to_string())
}

pub fn generate_node_id() -> NodeId {
    NodeId(Uuid::new_v4().to_string())
}

pub fn parse_node_id(s: &str) -> NodeId {
    NodeId(s.to_string())
}

pub fn validate_path(path: &str) -> bool {
    !path.is_empty() && path.len() <= crate::constants::MAX_PATH_LENGTH
}

pub fn normalize_path(path: &str) -> String {
    let mut normalized = path.to_string();
    if !normalized.starts_with('/') {
        normalized.insert(0, '/');
    }
    normalized
}

pub fn extract_filename(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

pub fn extract_parent(path: &str) -> &str {
    path.rsplit('/')
        .nth(1)
        .map_or("/", |p| if p.is_empty() { "/" } else { p })
}

pub fn calculate_checksum(data: &[u8]) -> u64 {
    use blake3::Hasher;
    let mut hasher = Hasher::new();
    hasher.update(data);
    let hash = hasher.finalize();
    let mut result = 0u64;
    for (i, byte) in hash.as_bytes().iter().take(8).enumerate() {
        result |= (*byte as u64) << (8 * i);
    }
    result
}

pub fn parse_address(addr: &str) -> Result<SocketAddr, std::io::Error> {
    addr.parse().map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid address: {}", e),
        )
    })
}

pub fn format_address(host: &str, port: u16) -> String {
    format!("{}:{}", host, port)
}

pub fn humanize_size(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    match bytes {
        0..KB => format!("{} B", bytes),
        KB..MB => format!("{:.2} KB", bytes as f64 / KB as f64),
        MB..GB => format!("{:.2} MB", bytes as f64 / MB as f64),
        GB..TB => format!("{:.2} GB", bytes as f64 / GB as f64),
        _ => format!("{:.2} TB", bytes as f64 / TB as f64),
    }
}

pub fn ensure_directory_exists(path: &str) -> Result<(), std::io::Error> {
    std::fs::create_dir_all(path)
}

pub fn file_exists(path: &str) -> bool {
    std::fs::metadata(path).is_ok()
}

pub fn get_file_size(path: &str) -> Result<u64, std::io::Error> {
    std::fs::metadata(path).map(|m| m.len())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ChecksumAlgorithm {
    #[default]
    CRC32C,
    CRC64,
    Blake3,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checksum {
    pub algorithm: ChecksumAlgorithm,
    pub value: Vec<u8>,
}

impl Checksum {
    pub fn compute(data: &[u8], algorithm: ChecksumAlgorithm) -> Self {
        match algorithm {
            ChecksumAlgorithm::CRC32C => {
                let crc = crc32c::crc32c(data);
                Checksum {
                    algorithm: ChecksumAlgorithm::CRC32C,
                    value: crc.to_be_bytes().to_vec(),
                }
            }
            ChecksumAlgorithm::CRC64 => {
                let mut digest = crc64fast::Digest::new();
                digest.write(data);
                let crc = digest.sum64();
                Checksum {
                    algorithm: ChecksumAlgorithm::CRC64,
                    value: crc.to_be_bytes().to_vec(),
                }
            }
            ChecksumAlgorithm::Blake3 => {
                let hash = blake3::hash(data);
                Checksum {
                    algorithm: ChecksumAlgorithm::Blake3,
                    value: hash.as_bytes().to_vec(),
                }
            }
        }
    }

    pub fn verify(&self, data: &[u8]) -> bool {
        let computed = Self::compute(data, self.algorithm);
        computed.value == self.value
    }

    pub fn from_bytes(algorithm: ChecksumAlgorithm, bytes: &[u8]) -> Self {
        Checksum {
            algorithm,
            value: bytes.to_vec(),
        }
    }

    pub fn as_u64(&self) -> u64 {
        let mut result = 0u64;
        for (i, byte) in self.value.iter().take(8).enumerate() {
            result |= (*byte as u64) << (8 * i);
        }
        result
    }
}
