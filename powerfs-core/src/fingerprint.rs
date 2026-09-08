//! Content fingerprint for write prediction & dedup (Phase C-0).
//!
//! SHA-256-based 256-bit fingerprint computed on the client side after
//! WriteCoalescer flush. The fingerprint is sent to the Filer for
//! matching against the `FingerprintIndex`. An LRU cache avoids
//! recomputing hashes for identical data seen recently.
//!
//! SHA-256 is chosen over Blake3 for kernel compatibility: the Linux
//! kernel crypto API provides SHA-256 (CONFIG_CRYPTO_SHA256=y) on all
//! kernels, while Blake3/Blake2s are optional and not available in the
//! PowerFS QEMU VM kernel. Using the same hash on both kernel and Rust
//! clients ensures fingerprints match for cross-client dedup.
//!
//! See `docs/write-prediction-dedup-design.md` §3.2.

use std::sync::Mutex;

use sha2::{Digest, Sha256};

/// 256-bit content fingerprint (SHA-256).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Fingerprint(pub [u8; 32]);

impl Fingerprint {
    /// Compute SHA-256 hash of `data`.
    ///
    /// Throughput on modern x86 is ~500 MB/s (software) / ~2 GB/s (SHA-NI),
    /// so a 256 KiB needle takes <0.5 ms — negligible vs network I/O.
    pub fn compute(data: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(data);
        let hash = hasher.finalize();
        let mut buf = [0u8; 32];
        buf.copy_from_slice(&hash);
        Self(buf)
    }

    /// Return the first 64 bytes of `data` as a lightweight secondary
    /// verification prefix. The Filer stores this alongside the
    /// fingerprint so that a Bloom false-positive or hash collision
    /// can be detected without the full data.
    pub fn extract_prefix(data: &[u8]) -> [u8; 64] {
        let mut prefix = [0u8; 64];
        let len = data.len().min(64);
        prefix[..len].copy_from_slice(&data[..len]);
        prefix
    }

    /// Render as hex string for logging.
    pub fn to_hex(&self) -> String {
        self.0.iter().map(|b| format!("{:02x}", b)).collect()
    }
}

/// Result of a fingerprint lookup on the Filer side.
#[derive(Clone, Debug)]
pub enum LookupResult {
    /// Fingerprint matches an active needle — can reference directly.
    Match {
        needle_id: u64,
        volume_id: u64,
        crc32: u32,
        data_size: u64,
        refcount: u32,
    },
    /// Fingerprint matches a tombstoned needle — can recover from
    /// tombstone pool without re-transmitting data.
    Recoverable {
        needle_id: u64,
        volume_id: u64,
        crc32: u32,
        data_size: u64,
    },
    /// No match (or Bloom false positive, or expired tombstone).
    NoMatch,
}

/// Client-side LRU cache: fingerprint → whether the Filer had a match.
/// Avoids sending repeated lookup RPCs for the same content.
pub struct FingerprintCache {
    inner: Mutex<lru::LruCache<Fingerprint, CachedLookup>>,
}

#[derive(Clone, Copy)]
#[allow(dead_code)]
struct CachedLookup {
    /// `true` = Match/Recoverable, `false` = NoMatch.
    pub hit: bool,
    /// Data prefix stored for collision detection.
    pub prefix: [u8; 64],
}

impl FingerprintCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(lru::LruCache::new(
                std::num::NonZeroUsize::new(capacity)
                    .unwrap_or_else(|| panic!("FingerprintCache capacity must be > 0")),
            )),
        }
    }

    /// Look up a cached result. Returns `Some(bool)` on cache hit
    /// (`true` = Match/Recoverable, `false` = NoMatch), or `None` on miss.
    pub fn get(&self, fp: &Fingerprint) -> Option<bool> {
        self.inner.lock().unwrap().get(fp).map(|c| c.hit)
    }

    /// Insert a lookup result into the cache.
    pub fn insert(&self, fp: Fingerprint, hit: bool, prefix: [u8; 64]) {
        self.inner
            .lock()
            .unwrap()
            .put(fp, CachedLookup { hit, prefix });
    }

    /// Current number of entries (for metrics).
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fingerprint_deterministic() {
        let data = b"hello world";
        let fp1 = Fingerprint::compute(data);
        let fp2 = Fingerprint::compute(data);
        assert_eq!(fp1, fp2);
    }

    #[test]
    fn test_fingerprint_different_data() {
        let fp1 = Fingerprint::compute(b"hello world");
        let fp2 = Fingerprint::compute(b"hello earth");
        assert_ne!(fp1, fp2);
    }

    #[test]
    fn test_prefix_extraction() {
        let data = b"abcdefghij";
        let prefix = Fingerprint::extract_prefix(data);
        assert_eq!(&prefix[..10], data);
        assert_eq!(&prefix[10..], &[0u8; 54][..]);
    }

    #[test]
    fn test_cache_basic() {
        let cache = FingerprintCache::new(256);
        let fp = Fingerprint::compute(b"test data");
        let prefix = Fingerprint::extract_prefix(b"test data");

        assert!(cache.get(&fp).is_none());

        cache.insert(fp, true, prefix);
        assert_eq!(cache.get(&fp), Some(true));

        let fp2 = Fingerprint::compute(b"no match data");
        cache.insert(fp2, false, Fingerprint::extract_prefix(b"no match data"));
        assert_eq!(cache.get(&fp2), Some(false));
    }

    #[test]
    fn test_large_data() {
        let data = vec![0xABu8; 1024 * 1024]; // 1 MiB
        let fp = Fingerprint::compute(&data);
        assert_eq!(fp.0.len(), 32);
        assert!(!fp.to_hex().is_empty());
    }
}
