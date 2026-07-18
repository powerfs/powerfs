use bytes::Bytes;
use powerfs_core::storage_backend::{LocalFsBackend, StorageBackend, StorageBackendError};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum S3TestClientError {
    #[error("storage backend error: {0}")]
    Storage(#[from] StorageBackendError),
    #[error("bucket not found: {0}")]
    BucketNotFound(String),
    #[error("object not found: {0}")]
    ObjectNotFound(String),
    #[error("invalid data: {0}")]
    InvalidData(String),
}

pub struct S3TestClient {
    backend: Arc<dyn StorageBackend>,
    buckets: HashMap<String, u64>,
    next_volume_id: u64,
}

impl S3TestClient {
    pub fn new(node_id: &str) -> Result<Self, S3TestClientError> {
        let backend = Arc::new(LocalFsBackend::new("/tmp/powerfs-test", node_id, "default", 1024 * 1024 * 1024)?);
        Ok(Self {
            backend,
            buckets: HashMap::new(),
            next_volume_id: 1,
        })
    }

    pub fn new_with_backend(backend: Arc<dyn StorageBackend>) -> Self {
        Self {
            backend,
            buckets: HashMap::new(),
            next_volume_id: 1,
        }
    }

    pub fn create_bucket(&mut self, name: &str) -> Result<(), S3TestClientError> {
        if self.buckets.contains_key(name) {
            return Ok(());
        }

        self.buckets.insert(name.to_string(), self.next_volume_id);
        self.next_volume_id += 1;
        Ok(())
    }

    pub fn delete_bucket(&mut self, name: &str) -> Result<(), S3TestClientError> {
        self.buckets.remove(name);
        Ok(())
    }

    pub fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: &[u8],
    ) -> Result<(), S3TestClientError> {
        let volume_id = self
            .buckets
            .get(bucket)
            .ok_or_else(|| S3TestClientError::BucketNotFound(bucket.to_string()))?;

        let object_data = Self::serialize_object(key, data);
        self.backend.write_needle(*volume_id, 0, &object_data)?;
        Ok(())
    }

    pub fn get_object(&self, bucket: &str, key: &str) -> Result<Option<Bytes>, S3TestClientError> {
        let volume_id = self
            .buckets
            .get(bucket)
            .ok_or_else(|| S3TestClientError::BucketNotFound(bucket.to_string()))?;

        let buffer = self.backend.read_needle(*volume_id, 0, 1048576)?;

        if buffer.is_empty() {
            return Ok(None);
        }

        let (stored_key, value) = Self::deserialize_object(&buffer);

        if stored_key == key {
            Ok(Some(Bytes::copy_from_slice(value)))
        } else {
            Ok(None)
        }
    }

    pub fn delete_object(&self, bucket: &str, key: &str) -> Result<(), S3TestClientError> {
        let volume_id = self
            .buckets
            .get(bucket)
            .ok_or_else(|| S3TestClientError::BucketNotFound(bucket.to_string()))?;

        let _ = volume_id;
        let _ = key;
        Ok(())
    }

    pub fn list_buckets(&self) -> Vec<String> {
        self.buckets.keys().cloned().collect()
    }

    pub fn list_objects(&self, bucket: &str) -> Result<Vec<String>, S3TestClientError> {
        let volume_id = self
            .buckets
            .get(bucket)
            .ok_or_else(|| S3TestClientError::BucketNotFound(bucket.to_string()))?;

        let buffer = self.backend.read_needle(*volume_id, 0, 1048576)?;

        if buffer.is_empty() {
            return Ok(Vec::new());
        }

        let (key, _) = Self::deserialize_object(&buffer);
        Ok(vec![key.to_string()])
    }

    fn serialize_object(key: &str, data: &[u8]) -> Vec<u8> {
        let key_len = key.len() as u32;
        let data_len = data.len() as u32;

        let mut result = Vec::with_capacity(8 + key_len as usize + data_len as usize);
        result.extend_from_slice(&key_len.to_be_bytes());
        result.extend_from_slice(&data_len.to_be_bytes());
        result.extend_from_slice(key.as_bytes());
        result.extend_from_slice(data);
        result
    }

    fn deserialize_object(buffer: &[u8]) -> (&str, &[u8]) {
        if buffer.len() < 8 {
            return ("", &[]);
        }

        let key_len = u32::from_be_bytes(buffer[0..4].try_into().unwrap()) as usize;
        let data_len = u32::from_be_bytes(buffer[4..8].try_into().unwrap()) as usize;

        if buffer.len() < 8 + key_len + data_len {
            return ("", &[]);
        }

        let key = std::str::from_utf8(&buffer[8..8 + key_len]).unwrap_or("");
        let data = &buffer[8 + key_len..8 + key_len + data_len];
        (key, data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_client_creation() {
        let client = S3TestClient::new("test-node");
        assert!(client.is_ok());
    }

    #[test]
    fn test_s3_bucket_operations() {
        let mut client = S3TestClient::new("test-node").unwrap();

        client.create_bucket("mybucket").unwrap();
        assert!(client.list_buckets().contains(&"mybucket".to_string()));

        client.delete_bucket("mybucket").unwrap();
        assert!(!client.list_buckets().contains(&"mybucket".to_string()));
    }

    #[test]
    fn test_s3_invalid_bucket() {
        let client = S3TestClient::new("test-node").unwrap();

        let result = client.put_object("nonexistent", "key", b"data");
        assert!(matches!(result, Err(S3TestClientError::BucketNotFound(_))));
    }

    #[test]
    fn test_s3_list_buckets_empty() {
        let client = S3TestClient::new("test-node").unwrap();
        let buckets = client.list_buckets();
        assert!(buckets.is_empty());
    }
}
