use rocksdb::{ColumnFamily, DB};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// S3 AccessKey 状态
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum S3KeyStatus {
    #[serde(rename = "active")]
    #[default]
    Active,
    #[serde(rename = "inactive")]
    Inactive,
}

/// S3 AccessKey 模型：关联到具体用户
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3AccessKey {
    pub id: String,
    pub user_id: String,
    pub access_key: String,
    /// SecretKey 的 HMAC-SHA256 哈希（不存储明文）
    pub secret_key_hash: String,
    pub status: S3KeyStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub last_used_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl S3AccessKey {
    pub fn new(user_id: &str, access_key: &str, secret_key_hash: &str) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            user_id: user_id.to_string(),
            access_key: access_key.to_string(),
            secret_key_hash: secret_key_hash.to_string(),
            status: S3KeyStatus::Active,
            created_at: chrono::Utc::now(),
            last_used_at: None,
        }
    }

    pub fn is_active(&self) -> bool {
        self.status == S3KeyStatus::Active
    }
}

/// 生成随机 AccessKey（16 字节 → 32 字符 hex）
pub fn generate_access_key() -> String {
    use rand::RngCore;
    let mut bytes = [0u8; 16];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    hex::encode(bytes)
}

/// 生成随机 SecretKey（32 字节 → 64 字符 hex）
pub fn generate_secret_key() -> String {
    use rand::RngCore;
    let mut bytes = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    hex::encode(bytes)
}

/// 使用 HMAC-SHA256 对 secret_key 进行哈希存储
pub fn hash_secret_key(secret_key: &str, hmac_key: &str) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(hmac_key.as_bytes()).expect("HMAC key length error");
    mac.update(secret_key.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// API 返回的 AccessKey 信息（不含 secret_key_hash）
#[derive(Debug, Clone, Serialize)]
pub struct S3AccessKeyInfo {
    pub id: String,
    pub user_id: String,
    pub access_key: String,
    pub status: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub last_used_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl From<&S3AccessKey> for S3AccessKeyInfo {
    fn from(k: &S3AccessKey) -> Self {
        Self {
            id: k.id.clone(),
            user_id: k.user_id.clone(),
            access_key: k.access_key.clone(),
            status: match k.status {
                S3KeyStatus::Active => "active".to_string(),
                S3KeyStatus::Inactive => "inactive".to_string(),
            },
            created_at: k.created_at,
            last_used_at: k.last_used_at,
        }
    }
}

const CF_S3_KEYS: &str = "s3_keys";
const CF_S3_KEY_USER_INDEX: &str = "s3_key_user_index";

/// S3 AccessKey 存储，与 UserStore 共享 auth.db
pub struct S3AccessKeyStore {
    db: Arc<DB>,
}

impl S3AccessKeyStore {
    pub fn from_user_store(user_store: &crate::auth::UserStore) -> Self {
        Self {
            db: user_store.db_handle(),
        }
    }

    fn cf_keys(&self) -> &ColumnFamily {
        self.db.cf_handle(CF_S3_KEYS).expect("s3_keys CF missing")
    }

    fn cf_user_index(&self) -> &ColumnFamily {
        self.db
            .cf_handle(CF_S3_KEY_USER_INDEX)
            .expect("s3_key_user_index CF missing")
    }

    pub fn create_key(&self, key: &S3AccessKey) -> Result<(), String> {
        let kkey = key.id.as_bytes();
        let value = serde_json::to_vec(key).map_err(|e| format!("Serialize error: {}", e))?;

        self.db
            .put_cf(self.cf_keys(), kkey, &value)
            .map_err(|e| format!("RocksDB put error: {}", e))?;

        // 用户索引：user_id|key_id -> 空
        let ukey = format!("{}|{}", key.user_id, key.id);
        self.db
            .put_cf(self.cf_user_index(), ukey.as_bytes(), b"1")
            .map_err(|e| format!("RocksDB index put error: {}", e))?;

        Ok(())
    }

    pub fn get_key_by_id(&self, id: &str) -> Result<Option<S3AccessKey>, String> {
        match self.db.get_cf(self.cf_keys(), id.as_bytes()) {
            Ok(Some(data)) => {
                let key: S3AccessKey = serde_json::from_slice(&data)
                    .map_err(|e| format!("Deserialize error: {}", e))?;
                Ok(Some(key))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(format!("RocksDB get error: {}", e)),
        }
    }

    /// 按 access_key 查找（用于认证场景）
    pub fn find_by_access_key(&self, access_key: &str) -> Result<Option<S3AccessKey>, String> {
        let iter = self
            .db
            .iterator_cf(self.cf_keys(), rocksdb::IteratorMode::Start);
        for item in iter {
            let (_k, value) = item.map_err(|e| format!("Iterator error: {}", e))?;
            let key: S3AccessKey =
                serde_json::from_slice(&value).map_err(|e| format!("Deserialize error: {}", e))?;
            if key.access_key == access_key {
                return Ok(Some(key));
            }
        }
        Ok(None)
    }

    /// 列出用户的所有 AccessKey
    pub fn list_user_keys(&self, user_id: &str) -> Result<Vec<S3AccessKey>, String> {
        let prefix = format!("{}|", user_id);
        let mode = rocksdb::IteratorMode::From(prefix.as_bytes(), rocksdb::Direction::Forward);
        let iter = self.db.iterator_cf(self.cf_user_index(), mode);

        let mut results = Vec::new();
        for item in iter {
            let (key, _value) = item.map_err(|e| format!("Iterator error: {}", e))?;
            if !key.starts_with(prefix.as_bytes()) {
                break;
            }
            let key_str = String::from_utf8_lossy(&key);
            let parts: Vec<&str> = key_str.splitn(2, '|').collect();
            if parts.len() != 2 {
                continue;
            }
            let key_id = parts[1];
            if let Some(key) = self.get_key_by_id(key_id)? {
                results.push(key);
            }
        }
        results.sort_by_key(|k| k.created_at);
        Ok(results)
    }

    pub fn update_status(&self, id: &str, status: S3KeyStatus) -> Result<bool, String> {
        let mut key = match self.get_key_by_id(id)? {
            Some(k) => k,
            None => return Ok(false),
        };
        key.status = status;
        let value = serde_json::to_vec(&key).map_err(|e| format!("Serialize error: {}", e))?;
        self.db
            .put_cf(self.cf_keys(), id.as_bytes(), &value)
            .map_err(|e| format!("RocksDB put error: {}", e))?;
        Ok(true)
    }

    pub fn delete_key(&self, id: &str) -> Result<bool, String> {
        let key = self.get_key_by_id(id)?;
        match key {
            Some(key) => {
                self.db
                    .delete_cf(self.cf_keys(), id.as_bytes())
                    .map_err(|e| format!("RocksDB delete error: {}", e))?;
                let ukey = format!("{}|{}", key.user_id, key.id);
                self.db
                    .delete_cf(self.cf_user_index(), ukey.as_bytes())
                    .map_err(|e| format!("RocksDB index delete error: {}", e))?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// 用户被删除时，清理其所有 AccessKey
    pub fn clear_user_keys(&self, user_id: &str) -> Result<usize, String> {
        let keys = self.list_user_keys(user_id)?;
        let mut count = 0;
        for key in &keys {
            if self.delete_key(&key.id).unwrap_or(false) {
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
    fn test_s3_access_key_crud() {
        let tmp = tempfile::tempdir().unwrap();
        let store = UserStore::new(tmp.path().to_str().unwrap()).unwrap();
        let key_store = S3AccessKeyStore::from_user_store(&store);

        let user_id = "test-user";
        let access_key = generate_access_key();
        let secret_key = generate_secret_key();
        let secret_hash = hash_secret_key(&secret_key, "test-hmac-key");

        let key = S3AccessKey::new(user_id, &access_key, &secret_hash);
        key_store.create_key(&key).unwrap();

        // get by id
        let fetched = key_store.get_key_by_id(&key.id).unwrap().unwrap();
        assert_eq!(fetched.access_key, access_key);
        assert_eq!(fetched.user_id, user_id);
        assert!(fetched.is_active());

        // find by access_key
        let found = key_store.find_by_access_key(&access_key).unwrap().unwrap();
        assert_eq!(found.id, key.id);

        // list user keys
        let keys = key_store.list_user_keys(user_id).unwrap();
        assert_eq!(keys.len(), 1);

        // update status
        assert!(key_store
            .update_status(&key.id, S3KeyStatus::Inactive)
            .unwrap());
        let updated = key_store.get_key_by_id(&key.id).unwrap().unwrap();
        assert!(!updated.is_active());

        // delete
        assert!(key_store.delete_key(&key.id).unwrap());
        assert!(key_store.get_key_by_id(&key.id).unwrap().is_none());
    }

    #[test]
    fn test_clear_user_keys() {
        let tmp = tempfile::tempdir().unwrap();
        let store = UserStore::new(tmp.path().to_str().unwrap()).unwrap();
        let key_store = S3AccessKeyStore::from_user_store(&store);

        let user_id = "multi-key-user";
        for _ in 0..3 {
            let ak = generate_access_key();
            let sk = generate_secret_key();
            let key = S3AccessKey::new(user_id, &ak, &hash_secret_key(&sk, "k"));
            key_store.create_key(&key).unwrap();
        }

        let keys = key_store.list_user_keys(user_id).unwrap();
        assert_eq!(keys.len(), 3);

        let cleared = key_store.clear_user_keys(user_id).unwrap();
        assert_eq!(cleared, 3);
        assert!(key_store.list_user_keys(user_id).unwrap().is_empty());
    }
}
