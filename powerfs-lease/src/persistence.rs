//! Lease persistence trait — optional backend for crash recovery.
//!
//! When attached to a [`crate::MemoryLeaseStore`], lease entries are persisted
//! on acquire/renew/release and loaded on startup. This enables lease state
//! recovery after a Volume Server crash, preventing:
//! - "Lease token not found" errors for clients with valid leases
//! - Concurrent write conflicts when the server loses track of granted leases
//!
//! # Serialization
//!
//! [`crate::LeaseEntry`] is serialized to a compact binary format:
//! ```text
//! [token_len: u32][token: bytes]
//! [holder_len: u32][holder: bytes]
//! [mode: u8]              // 0=shared, 1=exclusive
//! [expire_at_unix_ms: u64]
//! [epoch: u64]
//! [key_len: u32][key: bytes]
//! ```
//!
//! `Instant` is stored as Unix epoch milliseconds and converted back on load.
//! Expired entries are skipped during load.

use crate::error::LeaseError;
use crate::store::LeaseEntry;
use crate::token::LeaseMode;
use crate::LeaseKey;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Byte-based persistence backend (no generic on key type).
///
/// The store handles serialization/deserialization of `LeaseEntry<K>`; the
/// persistence backend only deals with raw bytes keyed by token string.
/// This keeps the trait simple and allows different backends (RocksDB, LMDB, etc.).
pub trait LeasePersistence: Send + Sync {
    /// Save a lease entry (serialized bytes) by token.
    fn save(&self, token: &str, data: &[u8]) -> Result<(), LeaseError>;

    /// Delete a lease entry by token.
    fn delete(&self, token: &str) -> Result<(), LeaseError>;

    /// Load all lease entries as (token, serialized_bytes) pairs.
    fn load_all(&self) -> Result<Vec<(String, Vec<u8>)>, LeaseError>;

    /// Save the current epoch counter (for fence token persistence).
    fn save_epoch(&self, epoch: u64) -> Result<(), LeaseError>;

    /// Load the persisted epoch counter.
    fn load_epoch(&self) -> Result<u64, LeaseError>;
}

// --- Serialization helpers ---

/// Serialize a `LeaseEntry<K>` to bytes.
pub fn encode_entry<K: LeaseKey>(entry: &LeaseEntry<K>) -> Vec<u8> {
    let mut buf = Vec::with_capacity(128);

    // token
    write_bytes(&mut buf, entry.token.as_bytes());
    // holder
    write_bytes(&mut buf, entry.holder.as_bytes());
    // mode
    buf.push(if entry.mode.is_exclusive() { 1 } else { 0 });
    // expire_at as Unix millis
    let expire_ms = instant_to_unix_millis(entry.expire_at);
    buf.extend_from_slice(&expire_ms.to_le_bytes());
    // epoch
    buf.extend_from_slice(&entry.epoch.to_le_bytes());
    // key
    let key_bytes = entry.key.encode();
    write_bytes(&mut buf, &key_bytes);

    buf
}

/// Deserialize a `LeaseEntry<K>` from bytes.
///
/// Returns `None` if the lease has already expired (caller should skip it).
pub fn decode_entry<K: LeaseKey>(data: &[u8]) -> Result<Option<LeaseEntry<K>>, LeaseError> {
    let mut pos = 0;

    let token = String::from_utf8(read_bytes(data, &mut pos)?)
        .map_err(|e| LeaseError::Internal(format!("invalid token utf8: {}", e)))?;
    let holder = String::from_utf8(read_bytes(data, &mut pos)?)
        .map_err(|e| LeaseError::Internal(format!("invalid holder utf8: {}", e)))?;

    let mode_byte = read_u8(data, &mut pos)?;
    let mode = if mode_byte == 1 {
        LeaseMode::Exclusive
    } else {
        LeaseMode::Shared
    };

    let expire_ms = read_u64(data, &mut pos)?;
    let epoch = read_u64(data, &mut pos)?;

    let key_bytes = read_bytes(data, &mut pos)?;
    let key = K::decode(&key_bytes)?;

    // Convert Unix millis back to Instant
    let expire_at = unix_millis_to_instant(expire_ms);

    // Skip expired entries
    if Instant::now() > expire_at {
        return Ok(None);
    }

    Ok(Some(LeaseEntry {
        key,
        holder,
        token,
        mode,
        acquired_at: Instant::now(), // approximate — not critical for recovery
        expire_at,
        epoch,
    }))
}

/// Write a length-prefixed byte slice.
fn write_bytes(buf: &mut Vec<u8>, data: &[u8]) {
    let len = data.len() as u32;
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(data);
}

/// Read a length-prefixed byte slice.
fn read_bytes(data: &[u8], pos: &mut usize) -> Result<Vec<u8>, LeaseError> {
    let len = read_u32(data, pos)? as usize;
    if *pos + len > data.len() {
        return Err(LeaseError::Internal("unexpected end of data".into()));
    }
    let result = data[*pos..*pos + len].to_vec();
    *pos += len;
    Ok(result)
}

fn read_u8(data: &[u8], pos: &mut usize) -> Result<u8, LeaseError> {
    if *pos >= data.len() {
        return Err(LeaseError::Internal("unexpected end of data".into()));
    }
    let v = data[*pos];
    *pos += 1;
    Ok(v)
}

fn read_u32(data: &[u8], pos: &mut usize) -> Result<u32, LeaseError> {
    if *pos + 4 > data.len() {
        return Err(LeaseError::Internal("unexpected end of data".into()));
    }
    let v = u32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    Ok(v)
}

fn read_u64(data: &[u8], pos: &mut usize) -> Result<u64, LeaseError> {
    if *pos + 8 > data.len() {
        return Err(LeaseError::Internal("unexpected end of data".into()));
    }
    let v = u64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    Ok(v)
}

/// Convert an `Instant` to Unix epoch milliseconds.
/// Uses the offset between `SystemTime::now()` and `Instant::now()` at call time.
fn instant_to_unix_millis(instant: Instant) -> u64 {
    let now_system = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let now_instant = Instant::now();
    // If instant is in the future relative to now_instant, add the difference
    // If instant is in the past, subtract the difference
    let offset = if instant >= now_instant {
        now_system + (instant - now_instant)
    } else {
        now_system.saturating_sub(now_instant - instant)
    };
    offset.as_millis() as u64
}

/// Convert Unix epoch milliseconds back to an `Instant`.
fn unix_millis_to_instant(unix_ms: u64) -> Instant {
    let now_system = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let now_instant = Instant::now();

    let target = Duration::from_millis(unix_ms);
    if target >= now_system {
        now_instant + (target - now_system)
    } else {
        // Already expired — return a past instant
        now_instant
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, PartialEq, Eq, Hash, Debug)]
    struct TestKey {
        id: u64,
        start: u64,
        count: u64,
    }

    impl LeaseKey for TestKey {
        fn group_id(&self) -> u64 {
            self.id
        }
        fn conflicts(&self, other: &Self) -> bool {
            self.id == other.id
        }
        fn encode(&self) -> Vec<u8> {
            let mut buf = Vec::with_capacity(24);
            buf.extend_from_slice(&self.id.to_le_bytes());
            buf.extend_from_slice(&self.start.to_le_bytes());
            buf.extend_from_slice(&self.count.to_le_bytes());
            buf
        }
        fn decode(data: &[u8]) -> Result<Self, LeaseError> {
            if data.len() < 24 {
                return Err(LeaseError::Internal("key too short".into()));
            }
            Ok(Self {
                id: u64::from_le_bytes(data[0..8].try_into().unwrap()),
                start: u64::from_le_bytes(data[8..16].try_into().unwrap()),
                count: u64::from_le_bytes(data[16..24].try_into().unwrap()),
            })
        }
    }

    #[test]
    fn test_encode_decode_entry() {
        let entry = LeaseEntry {
            key: TestKey {
                id: 1,
                start: 0,
                count: 4,
            },
            holder: "client-a".to_string(),
            token: "lease-0-abc".to_string(),
            mode: LeaseMode::Exclusive,
            acquired_at: Instant::now(),
            expire_at: Instant::now() + Duration::from_secs(30),
            epoch: 42,
        };

        let bytes = encode_entry(&entry);
        let decoded = decode_entry::<TestKey>(&bytes).unwrap().unwrap();

        assert_eq!(decoded.key, entry.key);
        assert_eq!(decoded.holder, entry.holder);
        assert_eq!(decoded.token, entry.token);
        assert_eq!(decoded.mode, entry.mode);
        assert_eq!(decoded.epoch, entry.epoch);
    }

    #[test]
    fn test_decode_expired_skipped() {
        let entry = LeaseEntry {
            key: TestKey {
                id: 1,
                start: 0,
                count: 4,
            },
            holder: "client-a".to_string(),
            token: "lease-0-abc".to_string(),
            mode: LeaseMode::Exclusive,
            acquired_at: Instant::now(),
            expire_at: Instant::now() - Duration::from_secs(1), // already expired
            epoch: 42,
        };

        let bytes = encode_entry(&entry);
        let result = decode_entry::<TestKey>(&bytes).unwrap();
        assert!(result.is_none());
    }

    /// A simple in-memory persistence backend for testing.
    pub struct MockPersistence {
        entries: std::sync::Mutex<std::collections::HashMap<String, Vec<u8>>>,
        epoch: std::sync::Mutex<u64>,
    }

    impl MockPersistence {
        pub fn new() -> Self {
            Self {
                entries: std::sync::Mutex::new(std::collections::HashMap::new()),
                epoch: std::sync::Mutex::new(0),
            }
        }

        pub fn count(&self) -> usize {
            self.entries.lock().unwrap().len()
        }
    }

    impl Default for MockPersistence {
        fn default() -> Self {
            Self::new()
        }
    }

    impl LeasePersistence for MockPersistence {
        fn save(&self, token: &str, data: &[u8]) -> Result<(), LeaseError> {
            self.entries
                .lock()
                .unwrap()
                .insert(token.to_string(), data.to_vec());
            Ok(())
        }

        fn delete(&self, token: &str) -> Result<(), LeaseError> {
            self.entries.lock().unwrap().remove(token);
            Ok(())
        }

        fn load_all(&self) -> Result<Vec<(String, Vec<u8>)>, LeaseError> {
            Ok(self
                .entries
                .lock()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect())
        }

        fn save_epoch(&self, epoch: u64) -> Result<(), LeaseError> {
            *self.epoch.lock().unwrap() = epoch;
            Ok(())
        }

        fn load_epoch(&self) -> Result<u64, LeaseError> {
            Ok(*self.epoch.lock().unwrap())
        }
    }

    #[test]
    fn test_mock_persistence_save_load() {
        let p = MockPersistence::new();
        p.save("tok-1", &[1, 2, 3]).unwrap();
        p.save("tok-2", &[4, 5, 6]).unwrap();
        assert_eq!(p.count(), 2);

        let all = p.load_all().unwrap();
        assert_eq!(all.len(), 2);

        p.delete("tok-1").unwrap();
        assert_eq!(p.count(), 1);

        p.save_epoch(42).unwrap();
        assert_eq!(p.load_epoch().unwrap(), 42);
    }
}
