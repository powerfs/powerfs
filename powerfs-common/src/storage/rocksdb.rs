//! RocksDB storage backend implementation

use crate::error::{PowerFsError, Result};
use crate::storage::StorageBackend;
use rocksdb::{Direction, IteratorMode, Options, DB};
use std::path::Path;

/// RocksDB storage backend
///
/// High-performance persistent key-value store based on RocksDB.
/// Provides optimized storage for metadata and indexes.
pub struct RocksDbBackend {
    db: DB,
}

#[allow(clippy::result_large_err)]
impl RocksDbBackend {
    /// Open or create a RocksDB database
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_options(path, &Options::default())
    }

    /// Open or create a RocksDB database with custom options
    pub fn open_with_options<P: AsRef<Path>>(path: P, options: &Options) -> Result<Self> {
        let db = DB::open(options, path).map_err(|e| PowerFsError::Storage(e.to_string()))?;

        Ok(RocksDbBackend { db })
    }

    /// Open with default optimized options for PowerFS
    pub fn open_optimized<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut opts = Options::default();

        // Create directory if not exists
        let path_str = path.as_ref();
        if !path_str.exists() {
            std::fs::create_dir_all(path_str).map_err(PowerFsError::Io)?;
        }

        // Optimize for our use case
        opts.create_if_missing(true);
        opts.set_max_open_files(1024);
        opts.set_write_buffer_size(64 * 1024 * 1024);
        opts.set_max_write_buffer_number(3);
        opts.set_target_file_size_base(64 * 1024 * 1024);

        // Enable compression
        opts.set_compression_type(rocksdb::DBCompressionType::Snappy);

        Self::open_with_options(path, &opts)
    }

    /// Compact the database
    pub fn compact(&self) -> Result<()> {
        self.db.compact_range::<&str, _>(None::<&str>, None::<&str>);
        Ok(())
    }

    /// Flush memtable to disk
    pub fn flush(&self) -> Result<()> {
        self.db
            .flush()
            .map_err(|e| PowerFsError::Storage(e.to_string()))
    }
}

#[allow(clippy::result_large_err)]
impl StorageBackend for RocksDbBackend {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db
            .put(key, value)
            .map_err(|e| PowerFsError::Storage(e.to_string()))
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.db
            .get(key)
            .map_err(|e| PowerFsError::Storage(e.to_string()))
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.db
            .delete(key)
            .map_err(|e| PowerFsError::Storage(e.to_string()))
    }

    fn list(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
        let mode = IteratorMode::From(prefix, Direction::Forward);
        let iter = self.db.iterator(mode);

        let mut keys = Vec::new();
        for item in iter {
            let (key, _) = item.map_err(|e| PowerFsError::Storage(e.to_string()))?;
            if key.starts_with(prefix) {
                keys.push(key.to_vec());
            } else {
                break;
            }
        }

        Ok(keys)
    }

    fn len(&self) -> Result<u64> {
        let mut count: u64 = 0;
        let iter = self.db.iterator(IteratorMode::Start);
        for item in iter {
            let _ = item.map_err(|e| PowerFsError::Storage(e.to_string()))?;
            count += 1;
        }
        Ok(count)
    }
}

impl Drop for RocksDbBackend {
    fn drop(&mut self) {
        let _ = self.db.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_rocksdb_basic_operations() {
        let temp_dir = TempDir::new().unwrap();

        let backend = RocksDbBackend::open_optimized(temp_dir.path()).unwrap();

        backend.put(b"key1", b"value1").unwrap();
        let result = backend.get(b"key1").unwrap();
        assert_eq!(result, Some(b"value1".to_vec()));

        backend.put(b"key1", b"value2").unwrap();
        let result = backend.get(b"key1").unwrap();
        assert_eq!(result, Some(b"value2".to_vec()));

        backend.delete(b"key1").unwrap();
        let result = backend.get(b"key1").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_rocksdb_list() {
        let temp_dir = TempDir::new().unwrap();

        let backend = RocksDbBackend::open_optimized(temp_dir.path()).unwrap();

        backend.put(b"prefix/a", b"1").unwrap();
        backend.put(b"prefix/b", b"2").unwrap();
        backend.put(b"prefix/c", b"3").unwrap();
        backend.put(b"other/d", b"4").unwrap();

        let keys = backend.list(b"prefix/").unwrap();
        assert_eq!(keys.len(), 3);
    }
}
