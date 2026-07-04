use crate::kv_cache::{KVCacheEngine, KVDtype};
use std::path::Path;
use std::sync::Arc;

const SESSION_PREFIX: &[u8] = b"s:";
const BLOCK_FID_PREFIX: &[u8] = b"bf:";

pub struct KVPersistStore {
    db: Arc<rocksdb::DB>,
}

impl KVPersistStore {
    pub fn new(path: &str) -> Result<Self, String> {
        let p = Path::new(path);
        if !p.exists() {
            std::fs::create_dir_all(p).map_err(|e| format!("create dir failed: {}", e))?;
        }

        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);

        let db =
            rocksdb::DB::open(&opts, path).map_err(|e| format!("open rocksdb failed: {}", e))?;

        Ok(Self { db: Arc::new(db) })
    }

    pub fn save_session(&self, session_id: &str, meta: &SessionMeta) -> Result<(), String> {
        let key = [SESSION_PREFIX, session_id.as_bytes()].concat();
        let value = serde_json::to_vec(meta).map_err(|e| format!("serialize failed: {}", e))?;
        self.db
            .put(key, value)
            .map_err(|e| format!("put failed: {}", e))
    }

    pub fn load_session(&self, session_id: &str) -> Result<Option<SessionMeta>, String> {
        let key = [SESSION_PREFIX, session_id.as_bytes()].concat();
        match self.db.get(key).map_err(|e| format!("get failed: {}", e))? {
            Some(v) => {
                let meta: SessionMeta =
                    serde_json::from_slice(&v).map_err(|e| format!("deserialize failed: {}", e))?;
                Ok(Some(meta))
            }
            None => Ok(None),
        }
    }

    pub fn delete_session(&self, session_id: &str) -> Result<(), String> {
        let key = [SESSION_PREFIX, session_id.as_bytes()].concat();
        self.db
            .delete(key)
            .map_err(|e| format!("delete failed: {}", e))?;

        // Delete all blocks for this session (simplified: prefix scan)
        let block_prefix = format!("b:{}:", session_id);
        let mut iter = self.db.prefix_iterator(block_prefix.as_bytes());
        let mut keys = Vec::new();
        while let Some(Ok((k, _))) = iter.next() {
            keys.push(k.to_vec());
        }

        for k in keys {
            let _ = self.db.delete(k);
        }

        Ok(())
    }

    pub fn save_block(&self, session_id: &str, block_id: u64, data: &[u8]) -> Result<(), String> {
        let key = format!("b:{}:{}", session_id, block_id);
        self.db
            .put(key.as_bytes(), data)
            .map_err(|e| format!("put block failed: {}", e))
    }

    pub fn load_block(&self, session_id: &str, block_id: u64) -> Result<Option<Vec<u8>>, String> {
        let key = format!("b:{}:{}", session_id, block_id);
        match self
            .db
            .get(key.as_bytes())
            .map_err(|e| format!("get block failed: {}", e))?
        {
            Some(v) => Ok(Some(v)),
            None => Ok(None),
        }
    }

    pub fn delete_block(&self, session_id: &str, block_id: u64) -> Result<(), String> {
        let key = format!("b:{}:{}", session_id, block_id);
        self.db
            .delete(key.as_bytes())
            .map_err(|e| format!("delete block failed: {}", e))?;

        let fid_key = format!("bf:{}", block_id);
        self.db
            .delete(fid_key.as_bytes())
            .map_err(|e| format!("delete block-fid mapping failed: {}", e))
    }

    pub fn save_block_fid(&self, block_id: u64, fid: &str) -> Result<(), String> {
        let key = format!("bf:{}", block_id);
        self.db
            .put(key.as_bytes(), fid.as_bytes())
            .map_err(|e| format!("save block-fid mapping failed: {}", e))
    }

    pub fn load_block_fid(&self, block_id: u64) -> Result<Option<String>, String> {
        let key = format!("bf:{}", block_id);
        match self
            .db
            .get(key.as_bytes())
            .map_err(|e| format!("load block-fid mapping failed: {}", e))?
        {
            Some(v) => Ok(Some(String::from_utf8_lossy(&v).to_string())),
            None => Ok(None),
        }
    }

    pub fn delete_block_fid(&self, block_id: u64) -> Result<(), String> {
        let key = format!("bf:{}", block_id);
        self.db
            .delete(key.as_bytes())
            .map_err(|e| format!("delete block-fid mapping failed: {}", e))
    }

    pub fn list_sessions(&self) -> Result<Vec<String>, String> {
        let mut sessions = Vec::new();
        for (key, _) in self.db.prefix_iterator(SESSION_PREFIX).flatten() {
            if key.starts_with(SESSION_PREFIX) {
                let name = &key[SESSION_PREFIX.len()..];
                if let Ok(s) = std::str::from_utf8(name) {
                    sessions.push(s.to_string());
                }
            }
        }
        Ok(sessions)
    }

    pub fn db(&self) -> Arc<rocksdb::DB> {
        self.db.clone()
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SessionMeta {
    pub session_id: String,
    pub model_name: String,
    pub num_layers: u32,
    pub num_heads: u32,
    pub head_dim: u32,
    pub dtype: String,
    pub block_ids: Vec<u64>,
    pub ttl_seconds: u64,
}

impl SessionMeta {
    pub fn dtype_enum(&self) -> KVDtype {
        KVDtype::parse(&self.dtype).unwrap_or(KVDtype::FP16)
    }
}

pub struct PersistentKVCache {
    pub engine: Arc<KVCacheEngine>,
    pub store: Arc<KVPersistStore>,
}

impl PersistentKVCache {
    pub fn new(engine: Arc<KVCacheEngine>, store: Arc<KVPersistStore>) -> Self {
        Self { engine, store }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_session(
        &self,
        session_id: &str,
        model_name: &str,
        num_layers: u32,
        num_heads: u32,
        head_dim: u32,
        dtype: KVDtype,
        ttl_seconds: u64,
    ) -> Result<(), String> {
        self.engine.create_session(
            session_id,
            model_name,
            num_layers,
            num_heads,
            head_dim,
            dtype,
            ttl_seconds,
        )?;

        let meta = SessionMeta {
            session_id: session_id.to_string(),
            model_name: model_name.to_string(),
            num_layers,
            num_heads,
            head_dim,
            dtype: dtype.as_str().to_string(),
            block_ids: Vec::new(),
            ttl_seconds,
        };
        self.store.save_session(session_id, &meta)?;

        Ok(())
    }

    pub fn put_block(
        &self,
        session_id: &str,
        layer_id: u32,
        num_tokens: u32,
        data: &[u8],
        fid: &str,
        block_index: u32,
    ) -> Result<u64, String> {
        let block_id = self
            .engine
            .put_block(session_id, layer_id, num_tokens, data, fid, block_index)?;
        self.store.save_block(session_id, block_id, data)?;
        self.store.save_block_fid(block_id, fid)?;
        Ok(block_id)
    }

    pub fn get_block_data(&self, block_id: u64) -> Option<Vec<u8>> {
        if let Some((_meta, data)) = self.engine.get_block_data(block_id) {
            return Some(data);
        }
        None
    }

    pub fn delete_session(&self, session_id: &str) -> Result<(), String> {
        self.engine.delete_session(session_id)?;
        self.store.delete_session(session_id)?;
        Ok(())
    }

    pub fn engine(&self) -> Arc<KVCacheEngine> {
        self.engine.clone()
    }

    pub fn store(&self) -> Arc<KVPersistStore> {
        self.store.clone()
    }
}
