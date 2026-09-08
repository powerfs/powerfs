//! Filer-side fingerprint index for write prediction & dedup (Phase C-0).
//!
//! Two-tier lookup:
//! 1. BloomFilter — O(1) in-memory initial filter (false positive rate ~1%)
//! 2. HashMap<Fingerprint, NeedleRef> — exact match with needle metadata
//!
//! `NeedleRef.status` is a tri-state:
//! - `Active`: needle is live, can reference directly
//! - `Tombstoned`: needle logically deleted, retention not expired — can recover
//! - `Expired`: retention expired, needle physically reclaimed — clean index
//!
//! See `docs/write-prediction-dedup-design.md` §3.3, §3.5.

use std::collections::HashMap;
use std::sync::Mutex;

use powerfs_core::fingerprint::{Fingerprint, LookupResult};

// ┌─────────────────────────────────────────────────────────────┐
// │ BloomFilter                                                  │
// └─────────────────────────────────────────────────────────────┘

/// Lightweight Bloom filter (no external dependency).
///
/// Uses k = 7 hash functions derived from the 256-bit Blake3 output,
/// avoiding the need for a separate hash crate. False positive rate
/// is ~1% at ~10 bits per entry.
pub struct BloomFilter {
    bits: Vec<u64>,
    num_bits: usize,
    num_hashes: usize,
}

impl BloomFilter {
    pub fn new(expected_items: usize, fpr: f64) -> Self {
        // m = -n * ln(p) / (ln(2)^2)
        let m = {
            let ln2 = std::f64::consts::LN_2;
            (-(expected_items as f64) * fpr.ln() / (ln2 * ln2)).ceil() as usize
        };
        let num_bits = m.max(64);
        let num_words = (num_bits + 63) / 64;
        let num_hashes = ((num_bits as f64 / expected_items as f64) * std::f64::consts::LN_2)
            .ceil()
            .max(1.0) as usize;

        Self {
            bits: vec![0u64; num_words],
            num_bits,
            num_hashes,
        }
    }

    fn positions(&self, fp: &Fingerprint) -> Vec<usize> {
        let data = fp.0;
        (0..self.num_hashes)
            .map(|i| {
                let offset = (i * 4) % 28;
                let chunk = u32::from_le_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]) as usize;
                chunk % self.num_bits
            })
            .collect()
    }

    pub fn insert(&mut self, fp: &Fingerprint) {
        for pos in self.positions(fp) {
            let word = pos / 64;
            let bit = pos % 64;
            self.bits[word] |= 1u64 << bit;
        }
    }

    pub fn may_contain(&self, fp: &Fingerprint) -> bool {
        for pos in self.positions(fp) {
            let word = pos / 64;
            let bit = pos % 64;
            if self.bits[word] & (1u64 << bit) == 0 {
                return false;
            }
        }
        true
    }

    /// Reset the bloom filter (e.g., after rebuild).
    pub fn clear(&mut self) {
        self.bits.iter_mut().for_each(|w| *w = 0);
    }

    pub fn num_bits(&self) -> usize {
        self.num_bits
    }

    pub fn num_hashes(&self) -> usize {
        self.num_hashes
    }
}

// ┌─────────────────────────────────────────────────────────────┐
// │ NeedleRef                                                    │
// └─────────────────────────────────────────────────────────────┘

/// Status of a needle tracked by the fingerprint index.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NeedleStatus {
    /// Needle is live and referenced by at least one chunk.
    Active,
    /// Needle is logically deleted (deleted_at set), retention not expired.
    /// Data is still physically on the volume server and can be recovered.
    Tombstoned,
    /// Retention expired, needle physically reclaimed. Index entry pending removal.
    Expired,
}

/// Metadata stored in the fingerprint HashMap.
#[derive(Clone, Debug)]
pub struct NeedleRef {
    pub needle_id: u64,
    pub volume_id: u64,
    pub crc32: u32,
    pub data_size: u64,
    pub refcount: u32,
    pub status: NeedleStatus,
    /// First 64 bytes of the original data — used for secondary verification
    /// to guard against Bloom false positives and hash collisions.
    pub prefix: [u8; 64],
}

impl NeedleRef {
    pub fn new(
        needle_id: u64,
        volume_id: u64,
        crc32: u32,
        data_size: u64,
        prefix: [u8; 64],
    ) -> Self {
        Self {
            needle_id,
            volume_id,
            crc32,
            data_size,
            refcount: 1,
            status: NeedleStatus::Active,
            prefix,
        }
    }
}

// ┌─────────────────────────────────────────────────────────────┐
// │ FingerprintIndex                                             │
// └─────────────────────────────────────────────────────────────┘

/// Filer-side fingerprint index: Bloom filter + HashMap.
///
/// All operations go through a single `Mutex`. The critical section is
/// pure HashMap/Bloom ops — sub-microsecond — so a single lock is fine.
pub struct FingerprintIndex {
    inner: Mutex<Inner>,
}

struct Inner {
    bloom: BloomFilter,
    map: HashMap<Fingerprint, NeedleRef>,
}

impl Default for FingerprintIndex {
    fn default() -> Self {
        Self::new(1_000_000, 0.01)
    }
}

impl FingerprintIndex {
    pub fn new(expected_items: usize, fpr: f64) -> Self {
        Self {
            inner: Mutex::new(Inner {
                bloom: BloomFilter::new(expected_items, fpr),
                map: HashMap::new(),
            }),
        }
    }

    /// Look up a fingerprint. Returns `Match`, `Recoverable`, or `NoMatch`.
    /// On `Expired`, cleans up the stale index entry before returning `NoMatch`.
    pub fn lookup(&self, fp: &Fingerprint, prefix: &[u8; 64]) -> LookupResult {
        let mut inner = self.inner.lock().unwrap();

        // ① Bloom filter initial filter
        if !inner.bloom.may_contain(fp) {
            return LookupResult::NoMatch;
        }

        // ② Exact match
        let Some(existing) = inner.map.get(fp) else {
            return LookupResult::NoMatch;
        };

        // ③ Secondary verification: compare data prefix
        if existing.prefix != *prefix {
            return LookupResult::NoMatch;
        }

        // ④ Decide based on needle status
        match existing.status {
            NeedleStatus::Active => LookupResult::Match {
                needle_id: existing.needle_id,
                volume_id: existing.volume_id,
                crc32: existing.crc32,
                data_size: existing.data_size,
                refcount: existing.refcount,
            },
            NeedleStatus::Tombstoned => LookupResult::Recoverable {
                needle_id: existing.needle_id,
                volume_id: existing.volume_id,
                crc32: existing.crc32,
                data_size: existing.data_size,
            },
            NeedleStatus::Expired => {
                // Clean up stale entry
                inner.map.remove(fp);
                // Note: Bloom filter can't easily remove; it's rebuilt periodically
                LookupResult::NoMatch
            }
        }
    }

    /// Insert a new fingerprint entry (after writing a new needle).
    /// If the fingerprint already exists, update the entry (e.g., old one
    /// was Expired, now replaced with new needle).
    pub fn insert(&self, fp: Fingerprint, needle_ref: NeedleRef) {
        let mut inner = self.inner.lock().unwrap();
        inner.bloom.insert(&fp);
        inner.map.insert(fp, needle_ref);
    }

    /// Increment refcount for an existing active needle.
    pub fn increment_refcount(&self, fp: &Fingerprint) -> Option<u32> {
        let mut inner = self.inner.lock().unwrap();
        let entry = inner.map.get_mut(fp)?;
        if entry.status != NeedleStatus::Active {
            return None;
        }
        entry.refcount += 1;
        Some(entry.refcount)
    }

    /// Decrement refcount. When refcount reaches 0, mark as Tombstoned
    /// (do NOT physically delete — retention period must elapse first).
    /// Returns the updated `NeedleRef` (cloned) if the fingerprint existed.
    pub fn decrement_refcount(&self, fp: &Fingerprint) -> Option<NeedleRef> {
        let mut inner = self.inner.lock().unwrap();
        let entry = inner.map.get_mut(fp)?;
        if entry.status != NeedleStatus::Active {
            return None;
        }
        if entry.refcount > 0 {
            entry.refcount -= 1;
        }
        if entry.refcount == 0 {
            entry.status = NeedleStatus::Tombstoned;
        }
        Some(entry.clone())
    }

    /// Mark a needle as tombstoned (logical deletion).
    /// Called when inode is deleted and refcount reaches 0.
    pub fn mark_tombstoned(&self, fp: &Fingerprint) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if let Some(entry) = inner.map.get_mut(fp) {
            if entry.refcount == 0 && entry.status == NeedleStatus::Active {
                entry.status = NeedleStatus::Tombstoned;
                return true;
            }
        }
        false
    }

    /// Recover a tombstoned needle: clear deleted_at, set refcount=1,
    /// status → Active. Returns `true` if recovery succeeded.
    pub fn recover(&self, fp: &Fingerprint) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if let Some(entry) = inner.map.get_mut(fp) {
            if entry.status == NeedleStatus::Tombstoned {
                entry.status = NeedleStatus::Active;
                entry.refcount = 1;
                return true;
            }
        }
        false
    }

    /// Mark as expired (retention elapsed). The next lookup will clean
    /// up the entry and Bloom filter will be rebuilt.
    pub fn mark_expired(&self, fp: &Fingerprint) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if let Some(entry) = inner.map.get_mut(fp) {
            if entry.status == NeedleStatus::Tombstoned {
                entry.status = NeedleStatus::Expired;
                return true;
            }
        }
        false
    }

    /// Remove an entry by needle_id (used by scrubber after physical
    /// needle deletion). Returns true if removed.
    pub fn remove_by_needle(&self, needle_id: u64, volume_id: u64) -> bool {
        let mut inner = self.inner.lock().unwrap();
        let to_remove: Option<Fingerprint> = inner.map.iter().find_map(|(fp, nr)| {
            if nr.needle_id == needle_id && nr.volume_id == volume_id {
                Some(*fp)
            } else {
                None
            }
        });
        if let Some(fp) = to_remove {
            inner.map.remove(&fp);
            true
        } else {
            false
        }
    }

    /// Current number of entries (for metrics).
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().map.is_empty()
    }

    /// Get all tombstoned entries (for scrubber scanning).
    pub fn get_tombstoned(&self) -> Vec<(Fingerprint, NeedleRef)> {
        self.inner
            .lock()
            .unwrap()
            .map
            .iter()
            .filter_map(|(fp, nr)| {
                if nr.status == NeedleStatus::Tombstoned {
                    Some((*fp, nr.clone()))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Rebuild the Bloom filter from the current HashMap entries.
    /// Called periodically to clean up stale Bloom bits.
    pub fn rebuild_bloom(&self) {
        let mut inner = self.inner.lock().unwrap();
        let fps: Vec<Fingerprint> = inner.map.keys().copied().collect();
        inner.bloom.clear();
        for fp in &fps {
            inner.bloom.insert(fp);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_fp(data: &[u8]) -> (Fingerprint, [u8; 64]) {
        let fp = Fingerprint::compute(data);
        let prefix = Fingerprint::extract_prefix(data);
        (fp, prefix)
    }

    #[test]
    fn test_bloom_basic() {
        let mut bloom = BloomFilter::new(1000, 0.01);
        let fp1 = Fingerprint::compute(b"hello");
        let fp2 = Fingerprint::compute(b"world");
        let _fp3 = Fingerprint::compute(b"missing");

        bloom.insert(&fp1);
        bloom.insert(&fp2);

        assert!(bloom.may_contain(&fp1));
        assert!(bloom.may_contain(&fp2));
        // fp3 might be a false positive but rarely
    }

    #[test]
    fn test_index_insert_and_lookup_match() {
        let index = FingerprintIndex::default();
        let (fp, prefix) = make_fp(b"test data content");

        let nr = NeedleRef::new(100, 200, 0xDEAD, 1024, prefix);
        index.insert(fp, nr);

        match index.lookup(&fp, &prefix) {
            LookupResult::Match {
                needle_id,
                volume_id,
                ..
            } => {
                assert_eq!(needle_id, 100);
                assert_eq!(volume_id, 200);
            }
            _ => panic!("expected Match"),
        }
    }

    #[test]
    fn test_index_no_match() {
        let index = FingerprintIndex::default();
        let (fp, prefix) = make_fp(b"nonexistent data");

        match index.lookup(&fp, &prefix) {
            LookupResult::NoMatch => {}
            _ => panic!("expected NoMatch"),
        }
    }

    #[test]
    fn test_index_prefix_mismatch() {
        let index = FingerprintIndex::default();
        let (fp, prefix) = make_fp(b"original data");
        let nr = NeedleRef::new(100, 200, 0xDEAD, 1024, prefix);
        index.insert(fp, nr);

        // Different prefix → NoMatch (collision detection)
        let wrong_prefix = [0xFF; 64];
        match index.lookup(&fp, &wrong_prefix) {
            LookupResult::NoMatch => {}
            _ => panic!("expected NoMatch on prefix mismatch"),
        }
    }

    #[test]
    fn test_index_refcount_increment() {
        let index = FingerprintIndex::default();
        let (fp, prefix) = make_fp(b"refcount test");
        let nr = NeedleRef::new(100, 200, 0, 1024, prefix);
        index.insert(fp, nr);

        assert_eq!(index.increment_refcount(&fp), Some(2));
        assert_eq!(index.increment_refcount(&fp), Some(3));
    }

    #[test]
    fn test_tombstone_and_recover() {
        let index = FingerprintIndex::default();
        let (fp, prefix) = make_fp(b"tombstone test");
        let nr = NeedleRef::new(100, 200, 0, 1024, prefix);
        index.insert(fp, nr);

        // Decrement refcount to 0 → auto-mark as Tombstoned
        let updated = index.decrement_refcount(&fp).expect("refcount decrement");
        assert_eq!(updated.refcount, 0);
        assert_eq!(updated.status, NeedleStatus::Tombstoned);

        // Lookup should return Recoverable
        match index.lookup(&fp, &prefix) {
            LookupResult::Recoverable { needle_id, .. } => {
                assert_eq!(needle_id, 100);
            }
            _ => panic!("expected Recoverable"),
        }

        // Recover
        assert!(index.recover(&fp));

        // Lookup should return Match again
        match index.lookup(&fp, &prefix) {
            LookupResult::Match { refcount, .. } => {
                assert_eq!(refcount, 1);
            }
            _ => panic!("expected Match after recover"),
        }
    }

    #[test]
    fn test_expired_cleanup() {
        let index = FingerprintIndex::default();
        let (fp, prefix) = make_fp(b"expired test");
        let nr = NeedleRef::new(100, 200, 0, 1024, prefix);
        index.insert(fp, nr);

        // Decrement to 0 → Tombstoned, then mark Expired
        index.decrement_refcount(&fp);
        assert!(index.mark_expired(&fp));

        // Lookup should clean up and return NoMatch
        match index.lookup(&fp, &prefix) {
            LookupResult::NoMatch => {}
            _ => panic!("expected NoMatch for expired"),
        }
    }

    #[test]
    fn test_remove_by_needle() {
        let index = FingerprintIndex::default();
        let (fp, prefix) = make_fp(b"remove test");
        let nr = NeedleRef::new(42, 99, 0, 1024, prefix);
        index.insert(fp, nr);

        assert!(index.remove_by_needle(42, 99));
        assert!(!index.remove_by_needle(42, 99)); // already removed
    }

    #[test]
    fn test_get_tombstoned() {
        let index = FingerprintIndex::default();

        let (fp1, prefix1) = make_fp(b"tomb1");
        let (fp2, prefix2) = make_fp(b"tomb2");
        let (fp3, prefix3) = make_fp(b"active");

        index.insert(fp1, NeedleRef::new(1, 10, 0, 100, prefix1));
        index.insert(fp2, NeedleRef::new(2, 20, 0, 100, prefix2));
        index.insert(fp3, NeedleRef::new(3, 30, 0, 100, prefix3));

        index.decrement_refcount(&fp1);
        index.decrement_refcount(&fp2);

        let tombstoned = index.get_tombstoned();
        assert_eq!(tombstoned.len(), 2);
    }
}
