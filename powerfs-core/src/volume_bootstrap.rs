use powerfs_common::error::{PowerFsError, Result};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::path::Path;

const MAGIC: [u8; 4] = *b"PVOL";
const BOOTSTRAP_VERSION: u16 = 1;

/// Volume 引导文件 — 极小元数据，用于快速识别和验证 Volume
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeBootstrap {
    pub magic: [u8; 4],
    pub version: u16,
    pub volume_id: u64,
    pub db_path: String,
    pub data_path: String,
    pub created_at: i64,
    pub checksum: u32,
}

impl VolumeBootstrap {
    /// 创建新的引导文件
    pub fn new(volume_id: u64, db_path: &str, data_path: &str) -> Self {
        let mut boot = Self {
            magic: MAGIC,
            version: BOOTSTRAP_VERSION,
            volume_id,
            db_path: db_path.to_string(),
            data_path: data_path.to_string(),
            created_at: chrono::Utc::now().timestamp(),
            checksum: 0,
        };
        boot.checksum = boot.calculate_checksum();
        boot
    }

    /// 从文件加载引导信息
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Err(PowerFsError::Internal(format!(
                "Bootstrap file not found: {}",
                path.display()
            )));
        }

        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(|e| PowerFsError::Internal(format!("Failed to open bootstrap file: {}", e)))?;

        let mut data = Vec::new();
        file.read_to_end(&mut data)
            .map_err(|e| PowerFsError::Internal(format!("Failed to read bootstrap file: {}", e)))?;

        let boot: Self = bincode::deserialize(&data).map_err(|e| {
            PowerFsError::Internal(format!("Failed to deserialize bootstrap: {}", e))
        })?;

        if boot.magic != MAGIC {
            return Err(PowerFsError::Internal(
                "Invalid bootstrap magic, not a PowerFS volume".to_string(),
            ));
        }

        let stored_checksum = boot.checksum;
        let mut verify = boot.clone();
        verify.checksum = 0;
        let calculated = verify.calculate_checksum();
        if stored_checksum != calculated {
            return Err(PowerFsError::Internal(format!(
                "Bootstrap checksum mismatch: stored={}, calculated={}",
                stored_checksum, calculated
            )));
        }

        Ok(boot)
    }

    /// 保存引导信息到文件
    pub fn save(&self, path: &Path) -> Result<()> {
        let data = bincode::serialize(self)
            .map_err(|e| PowerFsError::Internal(format!("Failed to serialize bootstrap: {}", e)))?;

        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .map_err(|e| PowerFsError::Internal(format!("Failed to create bootstrap file: {}", e)))?;

        file.write_all(&data)
            .map_err(|e| PowerFsError::Internal(format!("Failed to write bootstrap file: {}", e)))?;

        Ok(())
    }

    /// 计算校验和（CRC32，不包含 checksum 字段本身）
    fn calculate_checksum(&self) -> u32 {
        let mut clone = self.clone();
        clone.checksum = 0;
        let data = bincode::serialize(&clone).unwrap_or_default();
        crc32fast::hash(&data)
    }

    /// 验证 volume_id 是否匹配
    pub fn verify_volume_id(&self, expected_id: u64) -> Result<()> {
        if self.volume_id != expected_id {
            return Err(PowerFsError::Internal(format!(
                "Volume ID mismatch: bootstrap={}, expected={}",
                self.volume_id, expected_id
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bootstrap_save_load() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("volume.meta");

        let boot = VolumeBootstrap::new(42, "volume_db", "volume.data");
        boot.save(&path).unwrap();

        let loaded = VolumeBootstrap::load(&path).unwrap();
        assert_eq!(loaded.volume_id, 42);
        assert_eq!(loaded.magic, MAGIC);
        assert_eq!(loaded.version, BOOTSTRAP_VERSION);
    }

    #[test]
    fn test_bootstrap_corruption_detection() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("volume.meta");

        let boot = VolumeBootstrap::new(1, "db", "data");
        boot.save(&path).unwrap();

        // Corrupt the file
        let mut data = std::fs::read(&path).unwrap();
        if data.len() > 10 {
            data[10] ^= 0xFF;
        }
        std::fs::write(&path, &data).unwrap();

        let result = VolumeBootstrap::load(&path);
        assert!(result.is_err());
    }
}
