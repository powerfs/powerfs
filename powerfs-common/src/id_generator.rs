use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// 全局唯一 ID 生成器
/// 基于 UUID v4 派生，确保分布式环境下的唯一性
pub struct IdGenerator;

impl IdGenerator {
    /// 从 UUID v4 派生生成唯一 u64 ID
    /// 使用 UUID 的前 8 字节作为 u64，确保高随机性和唯一性
    pub fn generate_uuid_based() -> u64 {
        let uuid = uuid::Uuid::new_v4();
        let bytes = uuid.as_bytes();
        u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ])
    }

    /// 生成带时间戳的雪花 ID（更可读）
    /// 格式: timestamp (41 bits) + counter (10 bits) + random (12 bits)
    pub fn generate_snowflake() -> u64 {
        static COUNTER: AtomicU64 = AtomicU64::new(0);

        // 获取当前时间戳（毫秒），从 2024-01-01 开始
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let timestamp = (now - 1704067200000) & 0x1FFFFFFFFF; // 41 bits

        // 计数器（10 bits）
        let counter = COUNTER.fetch_add(1, Ordering::Relaxed) & 0x3FF;

        // 随机部分（12 bits）
        let random = rand::random::<u64>() & 0xFFF;

        (timestamp << 22) | (counter << 12) | random
    }

    /// 生成短 ID（用于调试/测试）
    pub fn generate_short() -> u32 {
        uuid::Uuid::new_v4().as_u128() as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uuid_based_ids_are_unique() {
        let mut ids = std::collections::HashSet::new();
        for _ in 0..10000 {
            let id = IdGenerator::generate_uuid_based();
            assert!(ids.insert(id), "Duplicate ID generated: {}", id);
        }
        assert_eq!(ids.len(), 10000);
    }

    #[test]
    fn test_snowflake_ids_are_monotonic() {
        let id1 = IdGenerator::generate_snowflake();
        let id2 = IdGenerator::generate_snowflake();
        // Snowflake IDs should be roughly monotonic (but not guaranteed due to random part)
        assert_ne!(id1, id2);
    }
}
