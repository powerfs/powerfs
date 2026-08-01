use serde::{Deserialize, Serialize};
use std::fmt;
use std::fs;
use std::path::PathBuf;
use uuid::Uuid;

/// 客户端唯一身份标识
///
/// 每个 FUSE 客户端实例都有一个唯一的 ClientIdentity。
/// 它由两部分组成:
/// - client_id: 用于底层通信的数字 ID (用于 powerfs-net 握手)
/// - client_uuid: 用于幂等性保证的 UUID (嵌入在请求中)
///
/// ClientIdentity 可以持久化到磁盘，以便客户端重启后恢复。
#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct ClientIdentity {
    /// 用于底层通信的客户端 ID
    pub client_id: u64,
    /// 用于幂等性保证的 UUID
    pub client_uuid: String,
}

impl ClientIdentity {
    /// 创建新的客户端身份
    pub fn new() -> Self {
        Self {
            client_id: rand::random::<u64>() % (i64::MAX as u64), // 确保为正数
            client_uuid: Uuid::new_v4().to_string(),
        }
    }

    /// 创建稳定的客户端身份（基于 hostname + mount_point hash）
    ///
    /// **关键**：CRDT 的 Add-Wins 策略用 client_id 区分操作来源。如果 client_id
    /// 在重启后变化，filer 端会将同客户端的旧 Add（旧 client_id）和新 Remove
    /// （新 client_id）误判为不同客户端的并发操作，触发 Add-Wins 跳过 Remove，
    /// 导致删除不生效。
    ///
    /// 此方法基于 hostname（区分节点）+ mount_point（区分同节点不同挂载点）
    /// hash 生成稳定 client_id，重启后保持不变：
    /// - 同节点同挂载点重启 → client_id 不变（CRDT 操作连续）
    /// - 同节点不同挂载点 → client_id 不同（mount_point 不同）
    /// - 不同节点 → client_id 不同（hostname 不同）
    pub fn stable_for(mount_point: &str) -> Self {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        // hostname: 不同容器/节点得到不同 hash
        if let Ok(hostname) = std::env::var("HOSTNAME") {
            hostname.hash(&mut hasher);
        } else if let Ok(hostname) = std::fs::read_to_string("/etc/hostname") {
            hostname.trim().hash(&mut hasher);
        }
        // mount_point: 同节点不同挂载点得到不同 hash
        mount_point.hash(&mut hasher);
        let client_id = hasher.finish() % (i64::MAX as u64);
        Self {
            client_id,
            client_uuid: Uuid::new_v4().to_string(),
        }
    }

    /// 从文件加载或创建新的身份
    ///
    /// 如果文件存在，则加载；否则创建新的并保存。
    pub fn load_or_create(path: &PathBuf) -> std::io::Result<Self> {
        if path.exists() {
            Self::load(path)
        } else {
            let identity = Self::new();
            identity.save(path)?;
            Ok(identity)
        }
    }

    /// 从文件加载身份
    pub fn load(path: &PathBuf) -> std::io::Result<Self> {
        let content = fs::read_to_string(path)?;
        serde_json::from_str(&content)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    /// 保存身份到文件
    pub fn save(&self, path: &PathBuf) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let content = serde_json::to_string_pretty(self)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        fs::write(path, content)
    }

    /// 获取用于底层通信的 ID
    pub fn as_client_id(&self) -> u64 {
        self.client_id
    }

    /// 获取用于幂等性的 UUID
    pub fn as_client_uuid(&self) -> &str {
        &self.client_uuid
    }
}

impl fmt::Display for ClientIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Client({},{})", self.client_id, self.client_uuid)
    }
}

impl Default for ClientIdentity {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_client_identity_unique() {
        let id1 = ClientIdentity::new();
        let id2 = ClientIdentity::new();
        assert_ne!(id1.client_id, id2.client_id);
        assert_ne!(id1.client_uuid, id2.client_uuid);
    }

    #[test]
    fn test_client_identity_uuid_format() {
        let id = ClientIdentity::new();
        // UUID v4 format: 8-4-4-4-12 hex chars
        assert_eq!(id.client_uuid.len(), 36);
        assert!(id.client_uuid.contains('-'));
    }

    #[test]
    fn test_client_identity_save_load() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("client_identity.json");

        let id1 = ClientIdentity::new();
        id1.save(&path).unwrap();

        let id2 = ClientIdentity::load(&path).unwrap();
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_client_identity_load_or_create_new() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("new_identity.json");

        // 应该创建新的并保存
        let id1 = ClientIdentity::load_or_create(&path).unwrap();
        assert!(path.exists());

        // 再次加载应该返回相同的 ID
        let id2 = ClientIdentity::load_or_create(&path).unwrap();
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_client_identity_display() {
        let id = ClientIdentity::new();
        let display = format!("{}", id);
        assert!(display.starts_with("Client("));
        assert!(display.ends_with(')'));
    }
}
