use rocksdb::{ColumnFamily, DB};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// 资源类型枚举
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ResourceType {
    #[serde(rename = "kv_namespace")]
    KvNamespace,
    #[serde(rename = "s3_bucket")]
    S3Bucket,
}

impl ResourceType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ResourceType::KvNamespace => "kv_namespace",
            ResourceType::S3Bucket => "s3_bucket",
        }
    }
}

impl std::str::FromStr for ResourceType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "kv_namespace" => Ok(ResourceType::KvNamespace),
            "s3_bucket" => Ok(ResourceType::S3Bucket),
            other => Err(format!("Unknown resource type: {}", other)),
        }
    }
}

impl std::fmt::Display for ResourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// 资源归属记录：将某个资源关联到某个用户
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceOwner {
    pub id: String,
    pub user_id: String,
    pub resource_type: ResourceType,
    pub resource_id: String,
    /// 权限列表，如 ["read", "write", "delete"]；admin 隐式拥有全部权限
    pub permissions: Vec<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl ResourceOwner {
    pub fn new(
        user_id: &str,
        resource_type: ResourceType,
        resource_id: &str,
        permissions: Vec<String>,
    ) -> Self {
        let now = chrono::Utc::now();
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            user_id: user_id.to_string(),
            resource_type,
            resource_id: resource_id.to_string(),
            permissions,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn has_permission(&self, perm: &str) -> bool {
        self.permissions.iter().any(|p| p == perm)
    }
}

/// 复合键：resource_type|resource_id -> ResourceOwner JSON
/// 用于按资源快速查找归属
fn resource_key(resource_type: &ResourceType, resource_id: &str) -> Vec<u8> {
    format!("{}|{}", resource_type.as_str(), resource_id).into_bytes()
}

/// 用户索引键：user_id|resource_type|resource_id -> 空
/// 用于按用户快速枚举其拥有的所有资源
fn user_index_key(user_id: &str, resource_type: &ResourceType, resource_id: &str) -> Vec<u8> {
    format!("{}|{}|{}", user_id, resource_type.as_str(), resource_id).into_bytes()
}

const CF_RESOURCE_OWNERS: &str = "resource_owners";

/// 资源归属存储，与 UserStore 共享同一个 auth.db RocksDB 实例
pub struct ResourceOwnerStore {
    db: Arc<DB>,
}

impl ResourceOwnerStore {
    /// 基于已有的 UserStore 创建（共享 RocksDB 实例）
    pub fn from_user_store(user_store: &crate::auth::UserStore) -> Self {
        Self {
            db: user_store.db_handle(),
        }
    }

    fn cf(&self) -> &ColumnFamily {
        self.db
            .cf_handle(CF_RESOURCE_OWNERS)
            .expect("resource_owners CF missing")
    }

    /// 记录某个资源的归属（创建/更新）
    pub fn set_owner(&self, owner: &ResourceOwner) -> Result<(), String> {
        let rkey = resource_key(&owner.resource_type, &owner.resource_id);
        let value = serde_json::to_vec(owner).map_err(|e| format!("Serialize error: {}", e))?;

        self.db
            .put_cf(self.cf(), rkey, &value)
            .map_err(|e| format!("RocksDB put error: {}", e))?;

        // 更新用户索引（key 存在即可，value 复用 owner.id 便于反查）
        let ukey = user_index_key(&owner.user_id, &owner.resource_type, &owner.resource_id);
        self.db
            .put_cf(self.cf(), ukey, owner.id.as_bytes())
            .map_err(|e| format!("RocksDB index put error: {}", e))?;

        Ok(())
    }

    /// 按资源查找归属关系
    pub fn get_owner(
        &self,
        resource_type: &ResourceType,
        resource_id: &str,
    ) -> Result<Option<ResourceOwner>, String> {
        let key = resource_key(resource_type, resource_id);
        match self.db.get_cf(self.cf(), &key) {
            Ok(Some(data)) => {
                let owner: ResourceOwner = serde_json::from_slice(&data)
                    .map_err(|e| format!("Deserialize error: {}", e))?;
                Ok(Some(owner))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(format!("RocksDB get error: {}", e)),
        }
    }

    /// 判断某用户是否拥有某资源（任意权限）
    pub fn is_owner(
        &self,
        user_id: &str,
        resource_type: &ResourceType,
        resource_id: &str,
    ) -> Result<bool, String> {
        match self.get_owner(resource_type, resource_id)? {
            Some(owner) => Ok(owner.user_id == user_id),
            None => Ok(false),
        }
    }

    /// 检查用户对某资源是否有指定权限
    /// 注意：admin 权限由调用方判断，此方法只检查资源归属层面
    pub fn check_permission(
        &self,
        user_id: &str,
        resource_type: &ResourceType,
        resource_id: &str,
        permission: &str,
    ) -> Result<bool, String> {
        match self.get_owner(resource_type, resource_id)? {
            Some(owner) => Ok(owner.user_id == user_id && owner.has_permission(permission)),
            None => Ok(false),
        }
    }

    /// 列出用户拥有的所有资源（按资源类型过滤）
    pub fn list_user_resources(
        &self,
        user_id: &str,
        resource_type: Option<&ResourceType>,
    ) -> Result<Vec<ResourceOwner>, String> {
        let prefix = match resource_type {
            Some(rt) => format!("{}|{}|", user_id, rt.as_str()),
            None => format!("{}|", user_id),
        };

        let mode = rocksdb::IteratorMode::From(prefix.as_bytes(), rocksdb::Direction::Forward);
        let iter = self.db.iterator_cf(self.cf(), mode);

        let mut results = Vec::new();
        for item in iter {
            let (key, _value) = item.map_err(|e| format!("Iterator error: {}", e))?;

            // 前缀匹配检查
            if !key.starts_with(prefix.as_bytes()) {
                break;
            }

            // 解析 resource_type 和 resource_id
            let key_str = String::from_utf8_lossy(&key);
            // key 格式：user_id|resource_type|resource_id
            let parts: Vec<&str> = key_str.splitn(3, '|').collect();
            if parts.len() != 3 {
                continue;
            }
            let rt: ResourceType = match parts[1].parse() {
                Ok(t) => t,
                Err(_) => continue,
            };
            let resource_id = parts[2];

            // 查询完整的 ResourceOwner 记录
            if let Some(owner) = self.get_owner(&rt, resource_id)? {
                results.push(owner);
            }
        }

        results.sort_by_key(|o| o.created_at);
        Ok(results)
    }

    /// 删除资源归属记录
    pub fn delete_owner(
        &self,
        resource_type: &ResourceType,
        resource_id: &str,
    ) -> Result<bool, String> {
        let owner = self.get_owner(resource_type, resource_id)?;
        match owner {
            Some(owner) => {
                let rkey = resource_key(resource_type, resource_id);
                self.db
                    .delete_cf(self.cf(), &rkey)
                    .map_err(|e| format!("RocksDB delete error: {}", e))?;

                let ukey = user_index_key(&owner.user_id, resource_type, resource_id);
                self.db
                    .delete_cf(self.cf(), &ukey)
                    .map_err(|e| format!("RocksDB index delete error: {}", e))?;

                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// 用户被删除时，清理该用户所有资源归属记录
    pub fn clear_user_resources(&self, user_id: &str) -> Result<usize, String> {
        let resources = self.list_user_resources(user_id, None)?;
        let mut count = 0;
        for owner in &resources {
            if self
                .delete_owner(&owner.resource_type, &owner.resource_id)
                .unwrap_or(false)
            {
                count += 1;
            }
        }
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::user_store::UserStore;

    #[test]
    fn test_resource_owner_crud() {
        let tmp = tempfile::tempdir().unwrap();
        let store = UserStore::new(tmp.path().to_str().unwrap()).unwrap();
        let owner_store = ResourceOwnerStore::from_user_store(&store);

        // 创建用户以获得 user_id（这里直接用 UUID 模拟）
        let user_id = "test-user-id";

        // set owner
        let owner = ResourceOwner::new(
            user_id,
            ResourceType::S3Bucket,
            "my-bucket",
            vec!["read".to_string(), "write".to_string()],
        );
        owner_store.set_owner(&owner).unwrap();

        // get owner
        let fetched = owner_store
            .get_owner(&ResourceType::S3Bucket, "my-bucket")
            .unwrap()
            .unwrap();
        assert_eq!(fetched.user_id, user_id);
        assert_eq!(fetched.resource_id, "my-bucket");
        assert_eq!(fetched.permissions.len(), 2);

        // is_owner
        assert!(owner_store
            .is_owner(user_id, &ResourceType::S3Bucket, "my-bucket")
            .unwrap());
        assert!(!owner_store
            .is_owner("other-user", &ResourceType::S3Bucket, "my-bucket")
            .unwrap());

        // check_permission
        assert!(owner_store
            .check_permission(user_id, &ResourceType::S3Bucket, "my-bucket", "read")
            .unwrap());
        assert!(!owner_store
            .check_permission(user_id, &ResourceType::S3Bucket, "my-bucket", "delete")
            .unwrap());

        // list user resources
        let resources = owner_store
            .list_user_resources(user_id, Some(&ResourceType::S3Bucket))
            .unwrap();
        assert_eq!(resources.len(), 1);

        // add a KV namespace
        let kv_owner = ResourceOwner::new(
            user_id,
            ResourceType::KvNamespace,
            "kv-session-1",
            vec!["read".to_string()],
        );
        owner_store.set_owner(&kv_owner).unwrap();

        let all = owner_store.list_user_resources(user_id, None).unwrap();
        assert_eq!(all.len(), 2);

        let kv_only = owner_store
            .list_user_resources(user_id, Some(&ResourceType::KvNamespace))
            .unwrap();
        assert_eq!(kv_only.len(), 1);

        // delete owner
        assert!(owner_store
            .delete_owner(&ResourceType::S3Bucket, "my-bucket")
            .unwrap());
        assert!(owner_store
            .get_owner(&ResourceType::S3Bucket, "my-bucket")
            .unwrap()
            .is_none());

        // clear user resources
        let cleared = owner_store.clear_user_resources(user_id).unwrap();
        assert_eq!(cleared, 1);
    }

    #[test]
    fn test_resource_type_roundtrip() {
        for rt in &[ResourceType::KvNamespace, ResourceType::S3Bucket] {
            let s = rt.as_str();
            let back: ResourceType = s.parse().unwrap();
            assert_eq!(*rt, back);
        }

        assert!("invalid".parse::<ResourceType>().is_err());
    }
}
