use rocksdb::{ColumnFamily, DB};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum KVKeyStatus {
    #[serde(rename = "active")]
    #[default]
    Active,
    #[serde(rename = "inactive")]
    Inactive,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KVAccessKey {
    pub id: String,
    pub user_id: String,
    pub access_key: String,
    pub secret_key_hash: String,
    pub status: KVKeyStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub last_used_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl KVAccessKey {
    pub fn new(user_id: &str, access_key: &str, secret_key_hash: &str) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            user_id: user_id.to_string(),
            access_key: access_key.to_string(),
            secret_key_hash: secret_key_hash.to_string(),
            status: KVKeyStatus::Active,
            created_at: chrono::Utc::now(),
            last_used_at: None,
        }
    }

    pub fn is_active(&self) -> bool {
        self.status == KVKeyStatus::Active
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct KVAccessKeyInfo {
    pub id: String,
    pub user_id: String,
    pub access_key: String,
    pub status: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub last_used_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl From<&KVAccessKey> for KVAccessKeyInfo {
    fn from(k: &KVAccessKey) -> Self {
        Self {
            id: k.id.clone(),
            user_id: k.user_id.clone(),
            access_key: k.access_key.clone(),
            status: match k.status {
                KVKeyStatus::Active => "active".to_string(),
                KVKeyStatus::Inactive => "inactive".to_string(),
            },
            created_at: k.created_at,
            last_used_at: k.last_used_at,
        }
    }
}

const CF_KV_KEYS: &str = "kv_keys";
const CF_KV_KEY_USER_INDEX: &str = "kv_key_user_index";

pub struct KVAccessKeyStore {
    db: Arc<DB>,
}

impl KVAccessKeyStore {
    pub fn from_user_store(user_store: &crate::auth::UserStore) -> Self {
        Self {
            db: user_store.db_handle(),
        }
    }

    fn cf_keys(&self) -> &ColumnFamily {
        self.db.cf_handle(CF_KV_KEYS).expect("kv_keys CF missing")
    }

    fn cf_user_index(&self) -> &ColumnFamily {
        self.db
            .cf_handle(CF_KV_KEY_USER_INDEX)
            .expect("kv_key_user_index CF missing")
    }

    pub fn create_key(&self, key: &KVAccessKey) -> Result<(), String> {
        let kkey = key.id.as_bytes();
        let value = serde_json::to_vec(key).map_err(|e| format!("Serialize error: {}", e))?;

        self.db
            .put_cf(self.cf_keys(), kkey, &value)
            .map_err(|e| format!("RocksDB put error: {}", e))?;

        let ukey = format!("{}|{}", key.user_id, key.id);
        self.db
            .put_cf(self.cf_user_index(), ukey.as_bytes(), b"1")
            .map_err(|e| format!("RocksDB index put error: {}", e))?;

        Ok(())
    }

    pub fn get_key_by_id(&self, id: &str) -> Result<Option<KVAccessKey>, String> {
        match self.db.get_cf(self.cf_keys(), id.as_bytes()) {
            Ok(Some(data)) => {
                let key: KVAccessKey = serde_json::from_slice(&data)
                    .map_err(|e| format!("Deserialize error: {}", e))?;
                Ok(Some(key))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(format!("RocksDB get error: {}", e)),
        }
    }

    pub fn find_by_access_key(&self, access_key: &str) -> Result<Option<KVAccessKey>, String> {
        let iter = self
            .db
            .iterator_cf(self.cf_keys(), rocksdb::IteratorMode::Start);
        for item in iter {
            let (_k, value) = item.map_err(|e| format!("Iterator error: {}", e))?;
            let key: KVAccessKey =
                serde_json::from_slice(&value).map_err(|e| format!("Deserialize error: {}", e))?;
            if key.access_key == access_key {
                return Ok(Some(key));
            }
        }
        Ok(None)
    }

    pub fn list_user_keys(&self, user_id: &str) -> Result<Vec<KVAccessKey>, String> {
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

    pub fn update_status(&self, id: &str, status: KVKeyStatus) -> Result<bool, String> {
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
