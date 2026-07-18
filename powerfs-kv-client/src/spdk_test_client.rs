use bytes::Bytes;
use powerfs_core::storage_backend::{SpdkBackend, StorageBackend, StorageBackendError};
use std::borrow::Borrow;

#[derive(Debug, thiserror::Error)]
pub enum TestClientError {
    #[error("storage backend error: {0}")]
    Storage(#[from] StorageBackendError),
    #[error("volume not found: {0}")]
    VolumeNotFound(u64),
    #[error("invalid data: {0}")]
    InvalidData(String),
}

pub struct SpdkTestClient {
    backend: SpdkBackend,
}

impl SpdkTestClient {
    pub fn new(node_id: &str) -> Result<Self, TestClientError> {
        let backend = SpdkBackend::new(node_id, None)?;
        Ok(Self { backend })
    }

    pub fn new_with_socket(node_id: &str, socket_path: &str) -> Result<Self, TestClientError> {
        let backend = SpdkBackend::new(node_id, Some(socket_path))?;
        Ok(Self { backend })
    }

    pub fn write_key(
        &self,
        volume_id: u64,
        key: &str,
        value: &[u8],
    ) -> Result<(), TestClientError> {
        let key_bytes = key.as_bytes();
        let key_len = key_bytes.len() as u32;

        let header = [
            (key_len >> 24) as u8,
            (key_len >> 16) as u8,
            (key_len >> 8) as u8,
            key_len as u8,
        ];

        let mut data = Vec::with_capacity(4 + key_len as usize + value.len());
        data.extend_from_slice(&header);
        data.extend_from_slice(key_bytes);
        data.extend_from_slice(value);

        let offset = 0;
        self.backend.write_needle(volume_id, offset, &data)?;
        Ok(())
    }

    pub fn read_key(&self, volume_id: u64, key: &str) -> Result<Option<Bytes>, TestClientError> {
        let buffer = self.backend.read_needle(volume_id, 0, 4096)?;

        if buffer.len() < 4 {
            return Ok(None);
        }

        let key_len = u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;

        if buffer.len() < 4 + key_len {
            return Ok(None);
        }

        let stored_key = &buffer[4..4 + key_len];

        if stored_key != key.as_bytes() {
            return Ok(None);
        }

        let value = &buffer[4 + key_len..];
        Ok(Some(Bytes::copy_from_slice(value)))
    }

    pub fn delete_key(&self, volume_id: u64) -> Result<(), TestClientError> {
        Ok(())
    }

    pub fn batch_write(
        &self,
        volume_id: u64,
        entries: &[(&str, &[u8])],
    ) -> Result<Vec<bool>, TestClientError> {
        let mut results = Vec::with_capacity(entries.len());

        for (key, value) in entries {
            let result = self.write_key(volume_id, key, value).is_ok();
            results.push(result);
        }

        Ok(results)
    }

    pub fn batch_read(
        &self,
        volume_id: u64,
        keys: &[&str],
    ) -> Result<Vec<Option<Bytes>>, TestClientError> {
        let mut results = Vec::with_capacity(keys.len());

        for key in keys {
            let result = self.read_key(volume_id, key)?;
            results.push(result);
        }

        Ok(results)
    }

    pub fn get_backend(&self) -> &SpdkBackend {
        &self.backend
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_creation() {
        let client = SpdkTestClient::new("test-node");
        assert!(client.is_ok());
    }

    #[test]
    fn test_client_creation_with_socket() {
        let client = SpdkTestClient::new_with_socket("test-node", "/var/tmp/spdk.sock");
        assert!(client.is_ok());
    }

    #[test]
    fn test_client_returns_backend() {
        let client = SpdkTestClient::new("test-node").unwrap();
        let _backend = client.get_backend();
    }
}
