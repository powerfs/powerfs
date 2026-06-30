//! Storage backend abstraction layer
//! 
//! This module provides a unified interface for different storage backends.
//! Currently supports RocksDB, with extension interfaces预留 for future backends.

use crate::error::Result;

/// Storage backend trait
/// 
/// Defines the interface that all storage backends must implement.
/// This allows for easy swapping of storage implementations.
#[allow(clippy::result_large_err)]
pub trait StorageBackend: Send + Sync {
    /// Store a key-value pair
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;
    
    /// Retrieve a value by key
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    
    /// Delete a key
    fn delete(&self, key: &[u8]) -> Result<()>;
    
    /// List all keys with a given prefix
    fn list(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>>;
    
    /// Check if a key exists
    fn exists(&self, key: &[u8]) -> Result<bool> {
        Ok(self.get(key)?.is_some())
    }
    
    /// Get the number of keys
    fn len(&self) -> Result<u64>;
    
    /// Check if the backend is empty
    fn is_empty(&self) -> Result<bool> {
        self.len().map(|n| n == 0)
    }
}

/// Key prefixes for organizing data
pub mod keys {
    /// Cluster topology keys
    pub const CLUSTER_PREFIX: &[u8] = b"cluster/";
    
    /// Volume mapping keys
    pub const VOLUME_PREFIX: &[u8] = b"volume/";
    
    /// Node info keys
    pub const NODE_PREFIX: &[u8] = b"node/";
    
    /// Volume to node mapping
    pub fn volume_to_node_key(volume_id: &str) -> Vec<u8> {
        let mut key = VOLUME_PREFIX.to_vec();
        key.extend_from_slice(b"node/");
        key.extend_from_slice(volume_id.as_bytes());
        key
    }
    
    /// Node to volumes mapping
    pub fn node_to_volumes_key(node_id: &str) -> Vec<u8> {
        let mut key = NODE_PREFIX.to_vec();
        key.extend_from_slice(b"volumes/");
        key.extend_from_slice(node_id.as_bytes());
        key
    }
    
    /// Volume info key
    pub fn volume_info_key(volume_id: &str) -> Vec<u8> {
        let mut key = VOLUME_PREFIX.to_vec();
        key.extend_from_slice(volume_id.as_bytes());
        key
    }
    
    /// Node info key
    pub fn node_info_key(node_id: &str) -> Vec<u8> {
        let mut key = NODE_PREFIX.to_vec();
        key.extend_from_slice(node_id.as_bytes());
        key
    }
    
    /// Cluster config key
    pub const CLUSTER_CONFIG_KEY: &[u8] = b"cluster/config";
    
    /// Leader info key
    pub const LEADER_INFO_KEY: &[u8] = b"cluster/leader";
}

pub mod rocksdb;

pub use rocksdb::RocksDbBackend;
