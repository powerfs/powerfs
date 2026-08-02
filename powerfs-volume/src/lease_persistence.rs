//! RocksDB-backed lease persistence for Volume Server.
//!
//! Stores lease entries in a `leases` column family (token → serialized entry)
//! and the epoch counter in a `meta` column family. On Volume Server startup,
//! `RangeLeaseManager::load_from_persistence()` recovers non-expired leases
//! and restores the epoch counter to prevent fence token ABA.

use powerfs_lease::{LeaseError, LeasePersistence};
use rocksdb::{ColumnFamilyDescriptor, Options, DB};
use std::sync::Arc;

/// Column family names
const CF_LEASES: &str = "leases";
const CF_META: &str = "meta";
const EPOCH_KEY: &str = "epoch_counter";

/// RocksDB-based lease persistence backend.
pub struct RocksDBLeasePersistence {
    db: Arc<DB>,
}

impl RocksDBLeasePersistence {
    /// Open (or create) a RocksDB instance at the given path.
    pub fn open(path: &str) -> Result<Self, LeaseError> {
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        let leases_cf = ColumnFamilyDescriptor::new(CF_LEASES, Options::default());
        let meta_cf = ColumnFamilyDescriptor::new(CF_META, Options::default());

        let db =
            DB::open_cf_descriptors(&db_opts, path, vec![leases_cf, meta_cf]).map_err(|e| {
                LeaseError::Internal(format!("Failed to open RocksDB at {}: {}", path, e))
            })?;

        Ok(Self { db: Arc::new(db) })
    }

    fn cf_leases(&self) -> Result<&rocksdb::ColumnFamily, LeaseError> {
        self.db
            .cf_handle(CF_LEASES)
            .ok_or_else(|| LeaseError::Internal("leases column family not found".into()))
    }

    fn cf_meta(&self) -> Result<&rocksdb::ColumnFamily, LeaseError> {
        self.db
            .cf_handle(CF_META)
            .ok_or_else(|| LeaseError::Internal("meta column family not found".into()))
    }
}

impl LeasePersistence for RocksDBLeasePersistence {
    fn save(&self, token: &str, data: &[u8]) -> Result<(), LeaseError> {
        let cf = self.cf_leases()?;
        self.db
            .put_cf(cf, token, data)
            .map_err(|e| LeaseError::Internal(format!("RocksDB put failed: {}", e)))
    }

    fn delete(&self, token: &str) -> Result<(), LeaseError> {
        let cf = self.cf_leases()?;
        // delete_cf returns Ok even if key doesn't exist (idempotent)
        self.db
            .delete_cf(cf, token)
            .map_err(|e| LeaseError::Internal(format!("RocksDB delete failed: {}", e)))
    }

    fn load_all(&self) -> Result<Vec<(String, Vec<u8>)>, LeaseError> {
        let cf = self.cf_leases()?;
        let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::Start);

        let mut result = Vec::new();
        for item in iter {
            let (key, value) =
                item.map_err(|e| LeaseError::Internal(format!("RocksDB iterator error: {}", e)))?;
            let token = String::from_utf8(key.to_vec())
                .map_err(|e| LeaseError::Internal(format!("invalid token utf8: {}", e)))?;
            result.push((token, value.to_vec()));
        }
        Ok(result)
    }

    fn save_epoch(&self, epoch: u64) -> Result<(), LeaseError> {
        let cf = self.cf_meta()?;
        self.db
            .put_cf(cf, EPOCH_KEY, epoch.to_le_bytes())
            .map_err(|e| LeaseError::Internal(format!("RocksDB put epoch failed: {}", e)))
    }

    fn load_epoch(&self) -> Result<u64, LeaseError> {
        let cf = self.cf_meta()?;
        match self.db.get_cf(cf, EPOCH_KEY) {
            Ok(Some(bytes)) => {
                if bytes.len() == 8 {
                    Ok(u64::from_le_bytes(bytes.as_slice().try_into().unwrap()))
                } else {
                    // Corrupt or missing — start from 0
                    Ok(0)
                }
            }
            Ok(None) => Ok(0),
            Err(e) => Err(LeaseError::Internal(format!(
                "RocksDB get epoch failed: {}",
                e
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use powerfs_lease::{LeaseKey, LeaseStore};
    use tempfile::TempDir;

    #[derive(Clone, PartialEq, Eq, Hash, Debug)]
    struct TestKey {
        inode: u64,
        stripe: u64,
        count: u64,
    }

    impl LeaseKey for TestKey {
        fn group_id(&self) -> u64 {
            self.inode
        }
        fn conflicts(&self, other: &Self) -> bool {
            self.inode == other.inode
        }
        fn encode(&self) -> Vec<u8> {
            let mut buf = Vec::with_capacity(24);
            buf.extend_from_slice(&self.inode.to_le_bytes());
            buf.extend_from_slice(&self.stripe.to_le_bytes());
            buf.extend_from_slice(&self.count.to_le_bytes());
            buf
        }
        fn decode(data: &[u8]) -> Result<Self, LeaseError> {
            if data.len() < 24 {
                return Err(LeaseError::Internal("too short".into()));
            }
            Ok(Self {
                inode: u64::from_le_bytes(data[0..8].try_into().unwrap()),
                stripe: u64::from_le_bytes(data[8..16].try_into().unwrap()),
                count: u64::from_le_bytes(data[16..24].try_into().unwrap()),
            })
        }
    }

    #[test]
    fn test_rocksdb_persistence_basic() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_str().unwrap();

        let p = RocksDBLeasePersistence::open(path).unwrap();

        // Save entries
        p.save("tok-1", &[1, 2, 3]).unwrap();
        p.save("tok-2", &[4, 5, 6]).unwrap();

        // Load all
        let all = p.load_all().unwrap();
        assert_eq!(all.len(), 2);

        // Delete one
        p.delete("tok-1").unwrap();
        let all = p.load_all().unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].0, "tok-2");

        // Epoch persistence
        p.save_epoch(42).unwrap();
        assert_eq!(p.load_epoch().unwrap(), 42);
    }

    #[test]
    fn test_rocksdb_persistence_reopen() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_str().unwrap();

        // First session: save data
        {
            let p = RocksDBLeasePersistence::open(path).unwrap();
            p.save("tok-1", &[1, 2, 3]).unwrap();
            p.save_epoch(99).unwrap();
        }

        // Second session: reopen and verify
        {
            let p = RocksDBLeasePersistence::open(path).unwrap();
            let all = p.load_all().unwrap();
            assert_eq!(all.len(), 1);
            assert_eq!(all[0].0, "tok-1");
            assert_eq!(all[0].1, vec![1, 2, 3]);
            assert_eq!(p.load_epoch().unwrap(), 99);
        }
    }

    #[test]
    fn test_store_with_persistence_roundtrip() {
        use powerfs_lease::{LeaseMode, MemoryLeaseStore};
        use std::time::Duration;
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_str().unwrap();

        // First session: acquire a lease with persistence
        let key = TestKey {
            inode: 1,
            stripe: 0,
            count: 4,
        };
        let stored_token;
        {
            let p = RocksDBLeasePersistence::open(path).unwrap();
            let store = MemoryLeaseStore::new().with_persistence(p);
            let entry = store
                .acquire(
                    key,
                    "client-a",
                    LeaseMode::Exclusive,
                    Duration::from_secs(60),
                )
                .unwrap();
            stored_token = entry.token;
            assert_eq!(store.active_count(), 1);
        }

        // Second session: new store, load from persistence
        {
            let p = RocksDBLeasePersistence::open(path).unwrap();
            let store = MemoryLeaseStore::<TestKey>::new().with_persistence(p);
            let restored = store.load_from_persistence().unwrap();
            assert_eq!(restored, 1);
            assert_eq!(store.active_count(), 1);

            // The restored lease should be valid
            assert!(store.validate_token(&stored_token, "client-a").is_ok());
        }
    }
}
