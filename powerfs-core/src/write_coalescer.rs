//! Write-back coalescing buffer for per-[`NeedleId`] partial over-writes.
//!
//! [`Volume::write_needle_blob`] historically handled each tiny over-write as a
//! full read-modify-write of the whole needle plus one `append_needle_version`
//! (new 1 MiB physical needle + mark the previous version deleted + RocksDB
//! atomic insert).  That turned 4 × 256 KiB random writes of the same 1 MiB
//! file into 4 full-sized needle reads, 4 full-sized needle writes and 12
//! RocksDB mutations – ~8× byte and IO amplification.
//!
//! This module provides a very small, synchronous, bounded in-memory buffer
//! (per [`Volume`]) that:
//!
//! 1. Stashes the merged logical data of a dirty needle in RAM.
//! 2. Applies subsequent partial writes to the same [`NeedleId`] as pure
//!    memory `copy_from_slice`, without touching the storage backend at all.
//! 3. Satisfies reads directly from the dirty buffer when present, so a
//!    write-then-read never sees stale data from the backend.
//! 4. Materialises a single new physical needle (one `append_needle_version`
//!    or `write_needle`) only when *any* of the flush triggers is reached:
//!    * the entry has lived longer than [`CoalescerConfig::deadline`],
//!    * the entry has absorbed >= [`CoalescerConfig::min_pending_writes`]
//!      partial blob writes,
//!    * the total in-memory dirty bytes exceed
//!      [`CoalescerConfig::max_dirty_bytes_per_entry`], or
//!    * [`WriteCoalescer::flush_all`] is called explicitly (Drop/shutdown).
//!
//! The whole structure is guarded by a single `Mutex` because the critical
//! section is purely a few `copy_from_slice`s plus HashMap operations –
//! typically sub-microsecond – and the real cost we are eliminating is the
//! backend RMW cycle (microseconds → milliseconds on spinning media / S3).

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use bytes::Bytes;
use powerfs_common::types::NeedleId;

/// Tunables for the write coalescer.
#[derive(Clone, Debug)]
pub struct CoalescerConfig {
    /// Maximum time a dirty needle entry can live in RAM before being forced
    /// to flush.  Kept intentionally small (default 4 ms) so the coalescer
    /// never introduces user-visible write-loss windows beyond the flush
    /// trigger on a quiet system.
    pub deadline: Duration,
    /// Number of partial `write_needle_blob` calls that must accumulate on
    /// the same [`NeedleId`] before the next incoming write triggers an
    /// eager flush.  Default 8.
    pub min_pending_writes: usize,
    /// Safety bound: if a single entry's merged payload grows beyond this
    /// size we flush immediately, no matter how young it is.  Default 1 MiB.
    pub max_dirty_bytes_per_entry: usize,
    /// Upper bound on the total dirty bytes across all entries in the
    /// coalescer.  When this is exceeded we flush the oldest entries until
    /// we are back under budget.  Default 16 MiB.
    pub max_dirty_bytes_total: usize,
    /// Disable coalescing entirely (for callers that want the previous
    /// synchronous, no-buffering behaviour).
    pub disabled: bool,
}

impl Default for CoalescerConfig {
    fn default() -> Self {
        Self {
            deadline: Duration::from_millis(4),
            min_pending_writes: 8,
            max_dirty_bytes_per_entry: 1 << 20, // 1 MiB
            max_dirty_bytes_total: 16 << 20,    // 16 MiB
            disabled: false,
        }
    }
}

/// A single dirty [`NeedleId`] entry.
pub(super) struct DirtyEntry {
    /// Merged logical data – exactly the byte array that will be handed to
    /// `append_needle_version` / `write_needle` on flush.  Always the full
    /// logical needle payload, never a partial slice.
    pub(super) merged: Vec<u8>,
    /// Number of `write_needle_blob` calls that have been merged into this
    /// entry since the last flush.
    pub(super) pending_writes: usize,
    /// Wall-clock deadline at which this entry *must* be flushed.  Set once
    /// when the entry is first created.
    pub(super) flush_after: Instant,
    /// If the needle already existed in the index when this entry was
    /// created, we remember its `NeedleId`-compatible key and rely on the
    /// caller to pass the correct `base_info` at flush-time via the
    /// `FlushOp` callback.  For brand-new needles this is the initial
    /// zero-padded `merged` that will become the first write.
    ///
    /// (The actual backend needle metadata is owned by the caller, i.e. the
    /// [`Volume`], because only the Volume knows how to look it up against
    /// the RocksDB index.  We keep the coalescer storage-backend-agnostic.)
    pub(super) is_new_needle: bool,
}

/// Caller-defined operation executed once per dirty entry at flush time.
/// This is how the [`Volume`] plugs the coalescer into its existing
/// `write_needle` / `append_needle_version` machinery without us having to
/// drag the whole Volume into this module.
///
/// Returns `Err(())` only so the coalescer can keep going (we do not want a
/// single bad needle to stop shutdown flush); the Volume is responsible for
/// propagating the real error out of the write-side entry point.
#[allow(dead_code)]
type FlushOp = dyn FnMut(NeedleId, Vec<u8>, bool) -> Result<(), ()>;

pub struct WriteCoalescer {
    config: CoalescerConfig,
    inner: Mutex<WriteCoalescerInner>,
}

struct WriteCoalescerInner {
    entries: HashMap<NeedleId, DirtyEntry>,
    /// Running sum of `entry.merged.capacity()` across all entries, kept so
    /// `max_dirty_bytes_total` can be enforced in O(1).
    dirty_bytes_total: usize,
}

impl WriteCoalescer {
    pub fn new(config: CoalescerConfig) -> Self {
        Self {
            config,
            inner: Mutex::new(WriteCoalescerInner {
                entries: HashMap::new(),
                dirty_bytes_total: 0,
            }),
        }
    }

    pub fn config(&self) -> &CoalescerConfig {
        &self.config
    }

    /// Record a partial write of `data` at `offset` for `needle_id`, merging
    /// it into the in-memory buffer.
    ///
    /// * `full_size_hint` – required size of the logical needle data.  When
    ///   creating a brand-new dirty entry for a not-yet-existing needle, the
    ///   merged vector is (at least) this big, pre-zeroed.  For *existing*
    ///   needles the caller must have already materialised `existing_data`
    ///   from the backend, so we know the full length.
    /// * `existing_data` – if the needle already exists on the backend, the
    ///   caller should supply the current merged bytes here.  We use this as
    ///   the initial merged buffer so the first partial write does not need
    ///   its own separate RMW (the point of this module is to absorb the
    ///   *next* N RMWs after that).
    ///
    /// Returns `Some((id, merged_vec, is_new_needle))` if the caller must
    /// flush this entry synchronously before this call returns – either
    /// because the deadline/pending count fired, or because the capacity
    /// budget is exhausted and we evicted it.
    pub(super) fn record_write(
        &self,
        needle_id: &NeedleId,
        offset: usize,
        data: &[u8],
        full_size_hint: usize,
        existing_data: Option<Vec<u8>>,
    ) -> Option<(NeedleId, Vec<u8>, bool)> {
        // Compute `is_new_needle` flag *before* consuming `existing_data`.
        let is_new_from_arg = existing_data.is_none();
        if self.config.disabled {
            // Fast path for "no coalescing": synthesise the merged buffer
            // and immediately hand it back as a forced flush.
            let mut merged = match existing_data {
                Some(v) => v,
                None => vec![0u8; full_size_hint],
            };
            let end = offset + data.len();
            if end > merged.len() {
                merged.resize(end, 0);
            }
            merged[offset..end].copy_from_slice(data);
            return Some((needle_id.clone(), merged, is_new_from_arg));
        }

        let now = Instant::now();
        let mut inner = self.inner.lock().expect("coalescer mutex poisoned");
        let cfg = &self.config;

        // ---- 1. Ensure entry exists; do NOT hold an `entries` borrow ----
        // across accesses to `dirty_bytes_total`.
        let existed_before = inner.entries.contains_key(needle_id);
        if !existed_before {
            let initial = match existing_data {
                Some(v) => v,
                None => vec![0u8; full_size_hint],
            };
            inner.entries.insert(
                needle_id.clone(),
                DirtyEntry {
                    merged: initial,
                    pending_writes: 0,
                    flush_after: now + cfg.deadline,
                    is_new_needle: is_new_from_arg,
                },
            );
        }

        // Subtract old length contribution ONLY for pre-existing entries.
        // Brand-new ones weren't counted in dirty_bytes_total yet so the
        // subtraction would be incorrect.
        let old_len = if existed_before {
            inner
                .entries
                .get(needle_id)
                .map(|e| e.merged.len())
                .unwrap_or(0)
        } else {
            0
        };
        inner.dirty_bytes_total = inner.dirty_bytes_total.saturating_sub(old_len);

        // ---- 2. Patch merged data with the incoming write. ----
        // We re-borrow just the entry &mut, apply patch, read trigger info,
        // then drop the entry borrow BEFORE touching dirty_bytes_total again.
        let (_, need_flush_now, new_len) = {
            let e = inner
                .entries
                .get_mut(needle_id)
                .expect("inserted above or already present");
            let was_empty = e.pending_writes == 0;
            let end = offset + data.len();
            if end > e.merged.len() {
                e.merged.resize(end, 0);
            }
            e.merged[offset..end].copy_from_slice(data);
            e.pending_writes += 1;
            let trigger = !was_empty
                && (now >= e.flush_after
                    || e.pending_writes >= cfg.min_pending_writes
                    || e.merged.len() > cfg.max_dirty_bytes_per_entry);
            (was_empty, trigger, e.merged.len())
        };

        inner.dirty_bytes_total = inner.dirty_bytes_total.saturating_add(new_len);

        // ---- 3. Total dirty-bytes budget handling.
        // Instead of synchronously evicting victims on THIS caller's thread
        // (which introduces tail latency: writing needle B blocks on
        // flushing needle A's backend I/O), we simply mark victims'
        // `flush_after` to the past.  An external caller (the Volume's
        // periodic opportunistic flush or the FUSE scheduler ticker) will
        // subsequently call flush_expired() to drain them asynchronously.
        // If _we_ are the oldest, just mark ourselves for self-flush: the
        // next round of flush_expired picks us, or the size/age trigger
        // fires synchronously on a subsequent write.
        let mut need_flush_this = need_flush_now;
        if inner.dirty_bytes_total > cfg.max_dirty_bytes_total {
            let oldest_key = inner
                .entries
                .iter()
                .min_by_key(|(_, e)| e.flush_after)
                .map(|(k, _)| k.clone());
            match oldest_key {
                None => {}
                Some(ref k) if k == needle_id => {
                    need_flush_this = true;
                }
                Some(k) => {
                    // Non-blocking eviction: just push flush_after into
                    // the past so flush_expired() will pick it up ASAP.
                    if let Some(victim) = inner.entries.get_mut(&k) {
                        victim.flush_after =
                            now.checked_sub(Duration::from_nanos(1)).unwrap_or(now);
                    }
                    // We deliberately mark at most one victim per call to
                    // keep the latency of record_write bounded; the
                    // budget overshoot is temporary – flush_expired drains
                    // all expired entries in bulk on its next invocation.
                }
            }
        }

        drop(inner);

        // ---- 4. Return self flush ONLY when the caller's own needle hit
        // a per-entry synchronous trigger (size / count / age).  Victims
        // are never returned synchronously – they flow via flush_expired.
        if need_flush_this {
            let mut inner = self.inner.lock().expect("coalescer mutex poisoned");
            if let Some(e) = inner.entries.remove(needle_id) {
                inner.dirty_bytes_total = inner.dirty_bytes_total.saturating_sub(e.merged.len());
                Some((needle_id.clone(), e.merged, e.is_new_needle))
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Try to read a range directly from the dirty buffer for `needle_id`,
    /// returning `Some(bytes)` on hit.  The returned slice is always a
    /// trimmed copy to the requested `offset..offset+size`, clamped to the
    /// actual merged length.
    pub(super) fn read_if_dirty(
        &self,
        needle_id: &NeedleId,
        offset: usize,
        size: usize,
    ) -> Option<Bytes> {
        let inner = self.inner.lock().expect("coalescer mutex poisoned");
        let entry = inner.entries.get(needle_id)?;
        if offset >= entry.merged.len() {
            return Some(Bytes::new());
        }
        let end = (offset + size).min(entry.merged.len());
        Some(Bytes::from(entry.merged[offset..end].to_vec()))
    }

    /// True iff `needle_id` currently has an un-flushed dirty buffer.
    pub fn is_dirty(&self, needle_id: &NeedleId) -> bool {
        self.inner
            .lock()
            .expect("coalescer mutex poisoned")
            .entries
            .contains_key(needle_id)
    }

    /// Number of currently dirty entries.  Exposed for tests and metrics.
    pub fn dirty_entry_count(&self) -> usize {
        self.inner
            .lock()
            .expect("coalescer mutex poisoned")
            .entries
            .len()
    }

    /// Internal helper: running sum of `merged.len()` across all dirty
    /// entries; tests assert budget tracking against this.
    #[cfg(test)]
    pub fn debug_dirty_total_bytes(&self) -> usize {
        let inner = self.inner.lock().expect("coalescer mutex poisoned");
        inner.dirty_bytes_total
    }

    /// Iterate every dirty entry, remove it, and run `op((id, merged,
    /// is_new_needle))` on it.  Returns the number of entries flushed.
    ///
    /// This is the only path that flushes entries without simultaneously
    /// replacing them with a newer partial write.  It is idempotent – once
    /// `flush_all` returns, `dirty_entry_count()` is 0.
    pub fn flush_all<F: FnMut(NeedleId, Vec<u8>, bool) -> Result<(), ()>>(
        &self,
        mut op: F,
    ) -> usize {
        // We take the whole map out under the lock, then release it before
        // calling into user code – flush op can do heavy I/O and we do not
        // want to hold the coalescer mutex while that happens.
        let mut entries = {
            let mut inner = self.inner.lock().expect("coalescer mutex poisoned");
            let taken = std::mem::take(&mut inner.entries);
            inner.dirty_bytes_total = 0;
            taken
        };
        let total = entries.len();
        for (id, entry) in entries.drain() {
            let _ = op(id, entry.merged, entry.is_new_needle);
        }
        total
    }

    /// Flush only the entries whose `flush_after` deadline has elapsed at
    /// `now`.  Returns how many were flushed.  Called from the Volume's
    /// periodic daemon to make sure dirty writes do not sit around forever
    /// waiting for min_pending_writes / size triggers that never arrive.
    pub fn flush_expired<F: FnMut(NeedleId, Vec<u8>, bool) -> Result<(), ()>>(
        &self,
        mut op: F,
    ) -> usize {
        let now = Instant::now();
        let expired: Vec<(NeedleId, DirtyEntry)> = {
            let mut inner = self.inner.lock().expect("coalescer mutex poisoned");
            let ids: Vec<NeedleId> = inner
                .entries
                .iter()
                .filter(|(_, e)| now >= e.flush_after)
                .map(|(k, _)| k.clone())
                .collect();
            let mut out = Vec::with_capacity(ids.len());
            for k in &ids {
                if let Some(v) = inner.entries.remove(k) {
                    inner.dirty_bytes_total =
                        inner.dirty_bytes_total.saturating_sub(v.merged.len());
                    out.push((k.clone(), v));
                }
            }
            out
        };
        let n = expired.len();
        for (id, e) in expired {
            let _ = op(id, e.merged, e.is_new_needle);
        }
        n
    }
}

impl Default for WriteCoalescer {
    fn default() -> Self {
        Self::new(CoalescerConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_record_returns_none_below_thresholds() {
        let coal = WriteCoalescer::new(CoalescerConfig::default());
        let id = NeedleId(42);
        let size_hint = 1024;
        let data = b"hello".to_vec();
        let res = coal.record_write(&id, 0, &data, size_hint, None);
        assert!(res.is_none(), "first write should stay buffered");
        assert_eq!(coal.dirty_entry_count(), 1);
        assert!(coal.is_dirty(&id));
    }

    #[test]
    fn coalesces_multiple_writes_into_one_merged() {
        let coal = WriteCoalescer::new(CoalescerConfig::default());
        let id = NeedleId(1);
        let hint = 16;
        // Four non-overlapping writes.
        coal.record_write(&id, 0, b"aaaa", hint, None);
        coal.record_write(&id, 4, b"bbbb", hint, None);
        coal.record_write(&id, 8, b"cccc", hint, None);
        coal.record_write(&id, 12, b"dddd", hint, None);
        // Reads must return latest merged.
        let got = coal.read_if_dirty(&id, 0, 16).unwrap();
        assert_eq!(got.as_ref(), b"aaaabbbbccccdddd");
        // No flush yet; still in buffer.
        assert_eq!(coal.dirty_entry_count(), 1);
    }

    #[test]
    fn flush_all_clears_everything_and_runs_op_for_each() {
        let coal = WriteCoalescer::new(CoalescerConfig::default());
        coal.record_write(&NeedleId(1), 0, b"aa", 2, None);
        coal.record_write(&NeedleId(2), 0, b"bb", 2, None);
        let mut seen = Vec::new();
        let flushed = coal.flush_all(|id, v, is_new| {
            seen.push((id.0, v, is_new));
            Ok(())
        });
        assert_eq!(flushed, 2);
        assert_eq!(coal.dirty_entry_count(), 0);
        seen.sort_by_key(|(k, _, _)| *k);
        assert_eq!(seen[0], (1, b"aa".to_vec(), true));
        assert_eq!(seen[1], (2, b"bb".to_vec(), true));
    }

    #[test]
    fn disabled_returns_every_write_as_forced_flush() {
        let coal = WriteCoalescer::new(CoalescerConfig {
            disabled: true,
            ..CoalescerConfig::default()
        });
        let res = coal.record_write(&NeedleId(7), 0, b"x", 1, None);
        let (id, v, is_new) = res.expect("disabled mode always forces flush");
        assert_eq!(id.0, 7);
        assert_eq!(v, b"x");
        assert!(is_new);
        assert_eq!(coal.dirty_entry_count(), 0);
    }

    #[test]
    fn min_pending_writes_triggers_flush() {
        let cfg = CoalescerConfig {
            min_pending_writes: 3,
            deadline: Duration::from_secs(30), // large, disabled
            max_dirty_bytes_per_entry: usize::MAX,
            max_dirty_bytes_total: usize::MAX,
            disabled: false,
        };
        let coal = WriteCoalescer::new(cfg);
        let id = NeedleId(5);
        let hint = 4;
        coal.record_write(&id, 0, b"1111", hint, None);
        coal.record_write(&id, 0, b"2222", hint, None);
        let third = coal.record_write(&id, 0, b"3333", hint, None);
        let (fid, fv, _) = third.expect("3rd write should hit min_pending_writes");
        assert_eq!(fid.0, id.0);
        assert_eq!(fv, b"3333");
        assert_eq!(coal.dirty_entry_count(), 0);
    }

    #[test]
    fn total_bytes_budget_evicts_oldest_deadline() {
        let cfg = CoalescerConfig {
            max_dirty_bytes_total: 10, // absurdly tight
            deadline: Duration::from_secs(30),
            min_pending_writes: usize::MAX,
            max_dirty_bytes_per_entry: usize::MAX,
            disabled: false,
        };
        let coal = WriteCoalescer::new(cfg);
        // id=1 first, so it has the earliest deadline.
        let r1 = coal.record_write(&NeedleId(1), 0, b"1234567890", 10, None);
        assert!(r1.is_none());
        // id=2 exceeds budget: record_write no longer blocks the caller by
        // returning a victim synchronously.  Instead it marks id=1 as
        // expired (flush_after pushed to the past) and returns None.
        let evict_from_write = coal.record_write(&NeedleId(2), 0, b"aa", 2, None);
        assert!(evict_from_write.is_none(), "budget path is non-blocking");
        assert_eq!(coal.dirty_entry_count(), 2, "both still in buffer");
        // On the next flush_expired() call, id=1 is drained out of the
        // buffer and handed to the caller-provided op.
        let mut flushed = Vec::new();
        let n = coal.flush_expired(|id, v, _is_new| {
            flushed.push((id.0, v));
            Ok(())
        });
        assert_eq!(n, 1);
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].0, 1);
        assert_eq!(flushed[0].1, b"1234567890");
        // id=2 is still dirty in the buffer since we only drained expired.
        assert_eq!(coal.dirty_entry_count(), 1);
        assert!(coal.is_dirty(&NeedleId(2)));
    }
}
