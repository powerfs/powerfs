//! Client-side Capability (cap) — per-inode cap record embedded in
//! `CachedEntry::cap`.
//!
//! # Design
//!
//! Caps live **directly inside `CachedEntry::cap`** — an `Option<ClientCap>`
//! per inode. This matches the server-side `CachedInode::client_caps` shape
//! and keeps the cap lifecycle bound to the cached inode: when the cache
//! evicts an inode, its cap goes with it (no orphaned records).
//!
//! # Field overview
//!
//! | Field            | Meaning                                                  |
//! |------------------|----------------------------------------------------------|
//! | `cap_id`         | Server-assigned monotonic cap id                         |
//! | `token`          | Server-issued token, used in flush/release RPCs          |
//! | `issued`         | Currently issued cap bits (CAP_R / CAP_W / CAP_X)        |
//! | `implemented`    | Server-confirmed applied caps (subset of `issued`)       |
//! | `wanted`         | Advisory: caps this client wants                         |
//! | `seq`/`issue_seq`| Last seq / issue seq from server                         |
//! | `mseq`           | Migration seq — bumped on Filer leader handoff           |
//! | `epoch`          | Epoch at grant time — used for fencing                   |
//! | `dirty_caps`     | Unsynced local state bits (set by write/setattr)         |
//! | `flushing_caps`  | Bits currently being flushed (cleared on flush success)  |
//! | `is_writer`      | True if opened O_WRONLY/O_RDWR                           |
//!
//! # Lifecycle
//!
//! - `open()` → server `CapOpenGrant` response fills `CachedEntry::cap`
//!   via `MetadataCache::grant_cap`.
//! - `write()` / `setattr()` → `mark_dirty(CAP_W|CAP_X)` on the cap.
//! - `release()` → if `dirty_caps` non-empty, flush first; then send
//!   `CapRelease` RPC and `take_cap` (clears the field).
//! - Server `CapRecallNotify` → `apply_recall()`: S cap ACKs immediately,
//!   X cap flushes dirty data first, then ACKs.
//! - Server `CapUpgradeNotify` → `apply_upgrade()`: restore EXCLUSIVE caps.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::Instant;

use log::debug;

/// Cap bits (mirrors server-side `CapSet`). Keep in sync with
/// `powerfs_filer::cap_manager::CapSet`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CapSet(pub u8);

impl CapSet {
    pub const NONE: CapSet = CapSet(0);
    pub const CAP_R: CapSet = CapSet(0b001);
    pub const CAP_W: CapSet = CapSet(0b010);
    pub const CAP_X: CapSet = CapSet(0b100);
    /// R + W + X — exclusive write holder.
    pub const EXCLUSIVE: CapSet = CapSet(0b111);

    pub fn contains(self, other: Self) -> bool {
        (self.0 & other.0) == other.0
    }
    pub fn has_any(self, other: Self) -> bool {
        (self.0 & other.0) != 0
    }
    pub fn is_empty(self) -> bool {
        self.0 == 0
    }
    pub fn is_exclusive(self) -> bool {
        self.contains(Self::CAP_W) && self.contains(Self::CAP_X)
    }
    pub fn union(self, other: Self) -> Self {
        CapSet(self.0 | other.0)
    }
    pub fn remove(self, other: Self) -> Self {
        CapSet(self.0 & !other.0)
    }
    pub fn intersection(self, other: Self) -> Self {
        CapSet(self.0 & other.0)
    }
}

/// Client-side cap record, embedded per-inode in `CachedEntry::cap`.
///
/// The client holds at most one cap per inode (single Filer leader),
/// so this is a single `Option<ClientCap>` rather than a map.
#[derive(Clone, Debug)]
pub struct ClientCap {
    /// Server-assigned monotonic cap id.
    pub cap_id: u64,
    /// Server-issued token, used in flush/release RPCs.
    pub token: String,
    /// Currently issued caps.
    pub issued: CapSet,
    /// Implemented caps — what the server has confirmed applied.
    /// Always a subset of `issued`.
    pub implemented: CapSet,
    /// What this client wants (advisory).
    pub wanted: CapSet,
    /// Last seq sent by server.
    pub seq: u64,
    /// Last issue seq.
    pub issue_seq: u64,
    /// Migration seq — bumped on Filer leader handoff.
    pub mseq: u32,
    /// Epoch at grant time — used for fencing (matches server `CapHolder::epoch`).
    pub epoch: u64,
    /// Dirty cap bits. Set by `mark_dirty()` when local writes/setattrs
    /// happen under CAP_W/CAP_X. Cleared on successful flush + sync.
    pub dirty_caps: CapSet,
    /// Flushing cap bits. Set while a flush RPC is in flight; cleared
    /// when release/recall ACK succeeds.
    pub flushing_caps: CapSet,
    /// True if the client opened with O_WRONLY/O_RDWR (a writer).
    pub is_writer: bool,
    /// Grant timestamp.
    pub granted_at: Instant,
}

impl ClientCap {
    pub fn new(
        cap_id: u64,
        token: String,
        issued: CapSet,
        epoch: u64,
        is_writer: bool,
        seq: u64,
    ) -> Self {
        Self {
            cap_id,
            token,
            issued,
            implemented: issued,
            wanted: issued,
            seq,
            issue_seq: seq,
            mseq: 0,
            epoch,
            dirty_caps: CapSet::NONE,
            flushing_caps: CapSet::NONE,
            is_writer,
            granted_at: Instant::now(),
        }
    }

    /// Mark the given cap bits as dirty (local unsynced state).
    pub fn mark_dirty(&mut self, cap: CapSet) {
        self.dirty_caps = self.dirty_caps.union(cap);
    }

    /// Issued caps minus dirty and flushing (clean issued bits).
    pub fn issued_non_dirty(&self) -> CapSet {
        self.issued
            .remove(self.dirty_caps)
            .remove(self.flushing_caps)
    }

    /// True if this cap currently allows local write caching.
    pub fn can_cache_writes(&self) -> bool {
        self.issued.contains(CapSet::CAP_W)
    }

    /// True if this cap currently allows local read caching.
    pub fn can_cache_reads(&self) -> bool {
        self.issued.contains(CapSet::CAP_R)
    }

    /// True if this cap allows local metadata modification (setattr/truncate).
    pub fn can_modify_meta(&self) -> bool {
        self.issued.contains(CapSet::CAP_X)
    }

    /// Apply a server grant/update.
    /// Returns the old issued caps for diffing.
    pub fn apply_grant(&mut self, issued: CapSet, seq: u64, epoch: u64) -> CapSet {
        let old = self.issued;
        self.issued = issued;
        self.implemented = issued; // simplified: assume server-implemented == issued
        self.seq = seq;
        self.issue_seq = seq;
        self.epoch = epoch;
        // Clear dirty bits that are now re-issued.
        self.dirty_caps = self.dirty_caps.remove(issued);
        old
    }

    /// Apply a server recall — moves recalled-and-dirty bits to
    /// `flushing_caps` so the caller knows what to flush before ACKing.
    pub fn apply_recall(&mut self, retained: CapSet, new_epoch: u64) -> CapSet {
        let recalled = self.issued.remove(retained);
        // Dirty bits being recalled must be flushed.
        let dirty_recalled = self.dirty_caps.intersection(recalled);
        if !dirty_recalled.is_empty() {
            self.flushing_caps = self.flushing_caps.union(dirty_recalled);
            self.dirty_caps = self.dirty_caps.remove(dirty_recalled);
        }
        self.issued = retained;
        self.epoch = new_epoch;
        recalled
    }

    /// Mark flush complete — clear `flushing_caps`.
    pub fn mark_flushed(&mut self) {
        self.flushing_caps = CapSet::NONE;
    }

    /// Apply a server upgrade notification — restore EXCLUSIVE caps.
    pub fn apply_upgrade(&mut self, granted: CapSet, epoch: u64, seq: u64) {
        self.issued = self.issued.union(granted);
        self.implemented = self.implemented.union(granted);
        self.epoch = epoch;
        self.seq = seq;
        self.issue_seq = seq;
    }
}

/// Result of a `CapRecallNotify` processing — tells the caller what to do.
#[derive(Debug)]
pub enum RecallAction {
    /// Cap was Shared (read-only), no dirty data — ACK immediately.
    ImmediateAck,
    /// Cap had CAP_W+CAP_X with dirty data — caller must flush first,
    /// then ACK. `flushing_caps` indicates which bits are being flushed.
    FlushThenAck { flushing_caps: CapSet },
}

/// Process a `CapRecallNotify` for an inode's cap. Returns the action
/// to take.
///
/// - `cap`: the mutable cap record (from `CachedEntry::cap`).
/// - `retained_caps`: caps the client is allowed to keep after recall.
/// - `new_epoch`: the new fencing epoch from the server.
pub fn process_recall(cap: &mut ClientCap, retained_caps: CapSet, new_epoch: u64) -> RecallAction {
    let recalled = cap.apply_recall(retained_caps, new_epoch);
    if cap.flushing_caps.is_empty() {
        // No dirty data being flushed — ACK immediately.
        debug!(
            "process_recall: recalled={:?} retained={:?} — ImmediateAck",
            recalled, retained_caps
        );
        RecallAction::ImmediateAck
    } else {
        // Dirty data needs flushing first.
        let flushing = cap.flushing_caps;
        debug!(
            "process_recall: recalled={:?} retained={:?} — FlushThenAck(flushing={:?})",
            recalled, retained_caps, flushing
        );
        RecallAction::FlushThenAck {
            flushing_caps: flushing,
        }
    }
}

/// External waiters map for cap upgrades.
///
/// Kept outside `CachedEntry` because the cache entry may be evicted
/// while a waiter is still pending — the waiter must be woken regardless.
/// In practice this is rare (waiters are short-lived: only used when a
/// SHARED_WRITE client wants to upgrade to EXCLUSIVE for a write).
///
/// PowerFS note: `open()` never blocks — the Cap model returns whatever
/// caps are available immediately. Waiters only matter for explicit
/// `wait_for_cap` callers (currently unused; reserved for future
/// write-path blocking when SHARED_WRITE must upgrade).
#[derive(Default)]
pub struct CapWaiters {
    waiters: Mutex<HashMap<u64, VecDeque<tokio::sync::oneshot::Sender<CapSet>>>>,
}

impl CapWaiters {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a waiter for cap changes on an inode.
    pub fn wait_for_cap(&self, inode: u64) -> tokio::sync::oneshot::Receiver<CapSet> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.waiters
            .lock()
            .unwrap()
            .entry(inode)
            .or_default()
            .push_back(tx);
        rx
    }

    /// Wake all waiters for an inode with the current issued caps.
    pub fn notify(&self, inode: u64, issued: CapSet) {
        let mut waiters = self.waiters.lock().unwrap();
        if let Some(queue) = waiters.remove(&inode) {
            for tx in queue {
                let _ = tx.send(issued);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cap_grant_and_recall() {
        let mut cap = ClientCap::new(1, "tok-1".into(), CapSet::EXCLUSIVE, 1, true, 100);
        assert!(cap.can_cache_writes());
        assert!(cap.can_modify_meta());

        // Write marks CAP_W dirty.
        cap.mark_dirty(CapSet::CAP_W);

        // Server recalls CAP_W+CAP_X (degrade to SHARED_WRITE).
        let action = process_recall(&mut cap, CapSet::CAP_R, 2);
        match action {
            RecallAction::FlushThenAck { flushing_caps } => {
                assert!(flushing_caps.contains(CapSet::CAP_W));
            }
            _ => panic!("expected FlushThenAck because CAP_W was dirty"),
        }

        // After flush, mark_flushed clears flushing_caps.
        cap.mark_flushed();
        assert!(!cap.can_cache_writes()); // CAP_W recalled
        assert!(cap.can_cache_reads()); // CAP_R retained
    }

    #[test]
    fn test_immediate_ack_for_read_only_cap() {
        let mut cap = ClientCap::new(2, "tok-2".into(), CapSet::CAP_R, 1, false, 200);

        // No dirty data → ImmediateAck.
        let action = process_recall(&mut cap, CapSet::NONE, 2);
        assert!(matches!(action, RecallAction::ImmediateAck));
    }

    #[test]
    fn test_upgrade_restores_exclusive() {
        let mut cap = ClientCap::new(3, "tok-3".into(), CapSet::CAP_R, 1, true, 300);
        assert!(!cap.can_cache_writes());

        // Server upgrades — grant CAP_W+CAP_X back.
        cap.apply_upgrade(CapSet::CAP_W.union(CapSet::CAP_X), 2, 301);
        assert!(cap.can_cache_writes());
        assert!(cap.can_modify_meta());
    }

    #[test]
    fn test_cap_waiters() {
        let waiters = CapWaiters::new();
        let rx = waiters.wait_for_cap(42);
        waiters.notify(42, CapSet::EXCLUSIVE);
        let issued = rx.blocking_recv().unwrap();
        assert!(issued.is_exclusive());
    }
}
