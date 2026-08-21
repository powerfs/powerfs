//! Capability (Cap) model for multi-client cache coherence.
//!
//! See `docs/lease-design.md` §13 "Capability 模型取代互斥锁模型".
//!
//! Core principle: **Lease ≠ mutex write lock; Lease = cache permission grant.**
//!
//! - `open()` never blocks; multiple clients can `open(O_RDWR)` the same file
//!   concurrently and all succeed (POSIX-compliant).
//! - Capabilities control whether a client may cache data/metadata locally,
//!   not whether it may open/write.
//! - On write-write conflict, the server recalls `CAP_W`+`CAP_X` from the
//!   existing holder and degrades all writers to `SHARED_WRITE` (synchronous
//!   IO, no local write cache). `open` still returns OK immediately.
//!
//! # Three-state machine (per-inode, server-driven)
//!
//! ```text
//! FREE ──open(RDWR)──> EXCLUSIVE_WRITE (holder: R+W+X)
//!                           │
//!                           │ open(RDWR) by C2 [conflict]
//!                           │ → recall C1's CAP_W+CAP_X (async)
//!                           │ → C1 flush + invalidate + ACK
//!                           ▼
//!                       SHARED_WRITE (all writers: no CAP_W, sync IO)
//!                           │
//!                           │ all writers close
//!                           │ (0 writers remaining)
//!                           ▼
//!                       FREE (or 1 writer → upgrade back to EXCLUSIVE_WRITE)
//! ```
//!
//! # Capabilities
//!
//! | Cap   | Allows                                   | Release cost           |
//! |-------|------------------------------------------|------------------------|
//! | CAP_R | local read cache (read/getattr no RPC)   | no dirty data, instant |
//! | CAP_W | local dirty write cache (write no RPC)   | must flush dirty pages |
//! | CAP_X | local metadata modify (setattr/truncate) | must sync meta to Filer|

use crate::early_grant::SnAllocator;
use crate::meta_cache::MetaCache;
use std::collections::{HashMap, VecDeque};
use std::ops::BitOr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

/// Default lease duration (30s) — matches the inode lease default.
const DEFAULT_CAP_DURATION_MS: u64 = 30_000;

/// Default recall timeout (2s) — server waits this long for the holder's
/// `RecallAck` before force-reclaiming its caps (§13.6.1 fencing token).
const DEFAULT_RECALL_TIMEOUT_MS: u64 = 2_000;

/// A bit-set of capabilities granted to a client for an inode.
///
/// Encoded as `u8` so it can travel in protocol headers cheaply. Combinations:
/// - `CAP_R` alone: SHARED_READ participant
/// - `CAP_R | CAP_W | CAP_X`: EXCLUSIVE_WRITE holder
/// - empty set: SHARED_WRITE participant (open succeeded but no local cache)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct CapSet(pub u8);

impl CapSet {
    pub const NONE: CapSet = CapSet(0);
    pub const CAP_R: CapSet = CapSet(0b001);
    pub const CAP_W: CapSet = CapSet(0b010);
    pub const CAP_X: CapSet = CapSet(0b100);
    /// Full set granted to a single exclusive writer.
    pub const EXCLUSIVE: CapSet = CapSet(0b111);

    pub fn has_r(self) -> bool {
        self.0 & Self::CAP_R.0 != 0
    }
    pub fn has_w(self) -> bool {
        self.0 & Self::CAP_W.0 != 0
    }
    pub fn has_x(self) -> bool {
        self.0 & Self::CAP_X.0 != 0
    }
    pub fn is_exclusive(self) -> bool {
        self.0 == Self::EXCLUSIVE.0
    }
    pub fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// Remove a subset of caps (used on recall — e.g. remove W+X, keep R).
    pub fn remove(self, other: CapSet) -> CapSet {
        CapSet(self.0 & !other.0)
    }

    /// Add a subset of caps (used on upgrade — grant W+X back).
    pub fn union(self, other: CapSet) -> CapSet {
        CapSet(self.0 | other.0)
    }
}

impl BitOr for CapSet {
    type Output = CapSet;
    fn bitor(self, rhs: CapSet) -> CapSet {
        CapSet(self.0 | rhs.0)
    }
}

/// Per-inode cap state on the server. Drives which caps each client may hold.
///
/// The state machine transitions are server-driven (§13.2.2):
/// - `FREE → SHARED_READ`: open(RDONLY) by C1, grant CAP_R
/// - `FREE → EXCLUSIVE_WRITE`: open(RDWR) by C1, grant CAP_R+W+X
/// - `SHARED_READ → SHARED_READ`: open(RDONLY) by C2, grant CAP_R (compatible)
/// - `SHARED_READ → SHARED_WRITE`: open(RDWR) by C2, recall readers' CAP_R,
///   degrade (optional — readers can keep CAP_R if no writer has dirty data)
/// - `EXCLUSIVE_WRITE → SHARED_WRITE`: open(RDWR) by C2, recall C1's CAP_W+CAP_X
/// - `EXCLUSIVE_WRITE → SHARED_READ`: open(RDONLY) by C2, recall C1's CAP_W+CAP_X
/// - `SHARED_WRITE → EXCLUSIVE_WRITE`: all but one writer close, upgrade survivor
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CapState {
    /// No open file — no caps outstanding.
    Free,
    /// One or more readers hold CAP_R. Writers are absent.
    SharedRead,
    /// Exactly one writer holds CAP_R+W+X (exclusive local cache).
    ExclusiveWrite,
    /// Two or more writers; no one holds CAP_W (synchronous IO).
    /// Readers (if any) may still hold CAP_R.
    SharedWrite,
}

/// What `open_grant` returns to the caller (the net handler). The net handler
/// forwards `granted_caps` to the client in the open response; `recall_tasks`
/// are dispatched asynchronously (recall is fire-and-forget — `open` does not
/// wait for recall ACK).
#[derive(Debug, Clone)]
pub struct OpenGrantResult {
    /// Caps granted to the opener. May be `EXCLUSIVE` (single writer),
    /// `CAP_R` (reader), or `NONE` (SHARED_WRITE participant).
    pub granted_caps: CapSet,
    /// Server-issued token for this client's cap grant. Used in subsequent
    /// `release_cap` / `recall_ack` calls.
    pub token: String,
    /// Fencer epoch (§13.6.1). The client must include this in storage IO
    /// requests; the storage layer rejects IO with a stale epoch.
    pub epoch: u64,
    /// Lease TTL in milliseconds.
    pub duration_ms: u64,
    /// Global sequence number (§5.2 / §13.6.1). Orders IO across cap
    /// handoffs so a rolled-back grant's IO is sequenced behind the new
    /// grant's IO.
    pub sn: u64,
    /// Recall tasks the net layer must dispatch asynchronously. `open`
    /// returns immediately; the recall pushes run in the background.
    pub recall_tasks: Vec<RecallTask>,
}

/// A recall notification to push to an existing cap holder.
///
/// `caps_to_recall` determines what the client must do:
/// - `CAP_W | CAP_X`: client must flush dirty data + sync metadata + ACK
/// - `CAP_R`: client invalidates read cache + ACK (no flush, no dirty data)
/// - `EXCLUSIVE`: client flushes + invalidates everything + ACK (full revoke)
#[derive(Debug, Clone)]
pub struct RecallTask {
    pub holder: String,
    pub token: String,
    pub caps_to_recall: CapSet,
    /// New caps the holder retains after recall (e.g. `CAP_R` if downgraded
    /// from EXCLUSIVE_WRITE to SHARED_READ). `NONE` if fully revoked.
    pub retained_caps: CapSet,
    /// Epoch bump for fencing — the holder's subsequent IO with the old
    /// epoch will be rejected by the storage layer.
    pub new_epoch: u64,
}

/// Per-inode cap holder record (server-side).
///
/// An in-memory record indexed by `client_id` and embedded in the inode
/// (`CachedInode::client_caps`). Tracks the issued/pending cap bits, the
/// revocation history, and carries a back-reference to the inode it
/// belongs to.
///
/// # Field overview
///
/// | Field              | Meaning                                                |
/// |--------------------|--------------------------------------------------------|
/// | `inode`            | Back-reference to the inode (for cap→inode traversal)  |
/// | `client_id`        | String client id                                       |
/// | `cap_id`           | Global monotonic id (was `token`)                      |
/// | `caps` + `pending` | `caps`=issued, `pending`=target after recall           |
/// | `wanted`           | What the client wants (advisory)                       |
/// | `revokes`          | Revoke history (VecDeque) for race handling            |
/// | `last_seq`/`last_issue_seq` | Sequencing                                     |
/// | `mseq`             | Migration seq (leader handoff)                         |
/// | `state_flags`      | New/Importing/ClientWriteable... state bits            |
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct CapHolder {
    /// Back-reference to the inode this cap belongs to. Stored
    /// explicitly because caps live in a `HashMap` inside `CachedInode`.
    pub inode: u64,
    /// Client identifier.
    pub client_id: String,
    /// Server-issued token. **Legacy** field kept for protocol compat;
    /// new code should prefer `cap_id`.
    pub token: String,
    /// Monotonic cap id. Globally unique per `(inode, client, grant)`
    /// triple; increments on every new grant.
    pub cap_id: u64,
    /// Currently issued caps. The bits the client is
    /// allowed to use right now.
    pub caps: CapSet,
    /// Pending caps. The target state after in-flight
    /// revocations complete. `pending & ~caps` = bits being recalled.
    pub pending: CapSet,
    /// What the client wants. Advisory — the server
    /// may grant more or fewer caps than wanted based on conflicts.
    pub wanted: CapSet,
    /// Revoke history. Each entry records the
    /// `before` caps, the seq at which the revoke was issued, and the
    /// last_issue seq. Used to handle "client already released but
    /// server still waiting for ACK" races.
    pub revokes: VecDeque<RevokeRecord>,
    /// Last sequence number sent to client.
    pub last_seq: u64,
    /// Last sequence number at which we issued.
    pub last_issue_seq: u64,
    /// Migration sequence. Bumped on leader handoff to
    /// disambiguate caps from different leader generations.
    pub mseq: u32,
    /// State flags (state bits).
    pub state_flags: CapStateFlags,
    /// True if the client opened with O_WRONLY/O_RDWR (a writer). Stays
    /// true even after cap degradation to NONE (SHARED_WRITE participant)
    /// — used by `logical_state()` to distinguish SHARED_WRITE writers
    /// (caps=NONE, is_writer=true) from SHARED_READ readers (caps=CAP_R,
    /// is_writer=false).
    pub is_writer: bool,
    pub acquired_at: Instant,
    pub expire_at: Instant,
    /// Fencer epoch at grant time. Increments on every recall/force-reclaim
    /// so stale IO from an unresponsive client is fenced off.
    pub epoch: u64,
    /// True once a recall has been sent and we're waiting for `recall_ack`.
    /// Prevents duplicate recall pushes for the same transition.
    pub recall_in_flight: bool,
}

/// A revoke record in the cap's revoke history.
///
/// When the server recalls caps, it pushes a `RevokeRecord` so that
/// late-arriving release/ACK messages can be matched against the right
/// revoke epoch. `recall_ack` walks this list to find the matching
/// record and clean up stale entries once the ACK arrives.
#[derive(Clone, Debug)]
pub struct RevokeRecord {
    /// Caps held by the client immediately before this revoke.
    pub before: CapSet,
    /// Seq at which the revoke was issued.
    pub seq: u64,
    /// `last_issue` at the time of revoke.
    pub last_issue: u64,
}

/// State flags for cap holder lifecycle (NEW / IMPORTING / ...).
///
/// Kept as a bitflags-style struct (manual since we're on stable Rust
/// without the bitflags crate in this workspace).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CapStateFlags(pub u32);

impl CapStateFlags {
    pub const NOTABLE: CapStateFlags = CapStateFlags(1 << 0);
    pub const NEW: CapStateFlags = CapStateFlags(1 << 1);
    pub const IMPORTING: CapStateFlags = CapStateFlags(1 << 2);
    pub const NEED_SNAPFLUSH: CapStateFlags = CapStateFlags(1 << 3);
    pub const CLIENT_WRITEABLE: CapStateFlags = CapStateFlags(1 << 4);
    pub const NO_INLINE: CapStateFlags = CapStateFlags(1 << 5);
    pub const NO_POOLNS: CapStateFlags = CapStateFlags(1 << 6);
    pub const NO_QUOTA: CapStateFlags = CapStateFlags(1 << 7);

    pub fn contains(self, other: Self) -> bool {
        (self.0 & other.0) == other.0
    }
    pub fn insert(&mut self, other: Self) {
        self.0 |= other.0;
    }
    pub fn remove(&mut self, other: Self) {
        self.0 &= !other.0;
    }
    pub fn is_new(self) -> bool {
        self.contains(Self::NEW)
    }
    pub fn is_client_writeable(self) -> bool {
        self.contains(Self::CLIENT_WRITEABLE)
    }
}

impl CapHolder {
    fn is_expired(&self) -> bool {
        Instant::now() > self.expire_at
    }

    /// Issue caps: update `_pending`/`_issued`
    /// and push a `RevokeRecord` if any bits are being revoked.
    pub fn issue(&mut self, target: CapSet, seq: u64) {
        if self.pending.has_bits_not_in(target) {
            // Revoking (and maybe adding) bits — record the revoke.
            self.revokes.push_back(RevokeRecord {
                before: self.pending,
                seq: self.last_seq,
                last_issue: self.last_issue_seq,
            });
            self.pending = target;
            self.caps = self.caps.union(target);
            self.state_flags.insert(CapStateFlags::NOTABLE);
        } else if target.has_bits_not_in(self.pending) {
            // Adding bits only.
            self.pending = self.pending.union(target);
            self.caps = self.caps.union(target);
            // Drop obsolete revokes whose bits we now have again.
            while let Some(front) = self.revokes.front() {
                if front.before.remove(self.pending).is_empty() {
                    self.revokes.pop_front();
                } else {
                    break;
                }
            }
        }
        self.last_seq = seq;
    }

    /// Mirror of Ceph `Capability::revoking()`: bits issued but not pending.
    pub fn revoking(&self) -> CapSet {
        self.caps.remove(self.pending)
    }
}

/// Helper trait for `CapSet` bit operations used by `CapHolder::issue`.
trait CapSetExt {
    fn has_bits_not_in(self, other: CapSet) -> bool;
}

impl CapSetExt for CapSet {
    fn has_bits_not_in(self, other: CapSet) -> bool {
        (self.0 & !other.0) != 0
    }
}

/// Per-client session state — the PowerFS analogue of Ceph's `Session`
/// class (`ceph/src/mds/SessionMap.h`).
///
/// Ceph's `Session` holds an `xlist<Capability*> caps` — a doubly-linked
/// list threaded through every `Capability` the session owns, via the
/// `item_session_caps` xlist item. This gives O(1) cap insertion/removal
/// and O(1) iteration when a session closes (bulk cap cleanup).
///
/// PowerFS doesn't have intrusive xlists; we use a `HashSet<(inode,
/// cap_id)>` as the reverse index. The cap records themselves live in
/// `CachedInode::client_caps` (mirroring `CInode::client_caps`), so this
/// set is purely an index — it doesn't own the `CapHolder` objects.
///
/// # Ceph alignment notes
///
/// | Ceph `Session` field         | PowerFS `ClientSession` field  |
/// |------------------------------|--------------------------------|
/// | `xlist<Capability*> caps`    | `caps: HashSet<(u64, u64)>`    |
/// | `last_cap_renew`             | `last_cap_renew_ms`            |
/// | `recall_caps` (DecayCounter) | `recall_caps_total` (AtomicU64)|
/// | `release_caps`               | `release_caps_total`           |
/// | `recall_limit`               | `recall_limit`                 |
/// | `state` (STATE_CLOSED/OPEN)  | `state` (SessionState)         |
/// | `connection`                 | (managed by net layer)         |
#[derive(Debug, Default)]
pub struct ClientSession {
    /// Reverse index: `(inode, cap_id)` for every cap this client holds.
    /// Mirrors Ceph's `xlist<Capability*> caps`. Used for O(1) cleanup
    /// on session close — iterate this set and remove each cap from its
    /// inode's `client_caps` map.
    pub caps: std::sync::Mutex<std::collections::HashSet<(u64, u64)>>,
    /// Monotonic ms of last cap renewal (Ceph: `last_cap_renew`).
    pub last_cap_renew_ms: std::sync::atomic::AtomicU64,
    /// Cumulative caps recalled from this session (Ceph: `recall_caps`).
    pub recall_caps_total: std::sync::atomic::AtomicU64,
    /// Cumulative caps released by this session (Ceph: `release_caps`).
    pub release_caps_total: std::sync::atomic::AtomicU64,
    /// Current recall limit (Ceph: `recall_limit`). Server caps the
    /// number of outstanding recalls per session to avoid flooding.
    pub recall_limit: std::sync::atomic::AtomicU32,
    /// Session state (Ceph: `state`).
    pub state: std::sync::Mutex<SessionState>,
}

/// Session state mirroring Ceph `Session::state_t`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SessionState {
    #[default]
    Closed,
    Opening,
    Active,
    Closing,
    Killing,
}

impl ClientSession {
    /// Record a new cap grant (Ceph: `Capability::item_session_caps`
    /// pushed to back of `Session::caps`).
    pub fn add_cap(&self, inode: u64, cap_id: u64) {
        self.caps.lock().unwrap().insert((inode, cap_id));
    }

    /// Remove a cap (Ceph: `cap_item.remove_myself()`).
    pub fn remove_cap(&self, inode: u64, cap_id: u64) {
        self.caps.lock().unwrap().remove(&(inode, cap_id));
    }

    /// Snapshot of `(inode, cap_id)` pairs — used on session close to
    /// drive bulk cap cleanup (Ceph: `Session::~Session` walks `caps`).
    pub fn snapshot_caps(&self) -> Vec<(u64, u64)> {
        self.caps.lock().unwrap().iter().copied().collect()
    }

    /// Number of caps held by this session.
    pub fn cap_count(&self) -> usize {
        self.caps.lock().unwrap().len()
    }
}

/// Pending recall state for force-reclaim timeout (§13.6.1).
#[allow(dead_code)]
#[derive(Clone)]
struct PendingRecall {
    sent_at: Instant,
    holder: String,
    token: String,
    /// The inode this recall is for (so `drain_expired_recalls` can route
    /// the force-reclaim back to the right `CapInodeState`).
    inode: u64,
}

/// Per-inode server state: current cap mode + holder list + waiter queue.
#[allow(dead_code)]
struct CapInodeState {
    /// Logical state — derived from the holder list but cached for O(1) checks.
    /// `ExclusiveWrite` ⇔ exactly 1 holder with `caps.is_exclusive()`.
    /// `SharedWrite` ⇔ ≥2 holders, none with `CAP_W`.
    /// `SharedRead` ⇔ ≥1 holder with `CAP_R`, none with `CAP_W`/`CAP_X`.
    /// `Free` ⇔ 0 holders.
    holders: Vec<CapHolder>,
    /// FIFO queue of open requests waiting for a cap upgrade (e.g. a reader
    /// wanting to upgrade to writer when no other writer exists). In the
    /// pure Cap model `open` never blocks, so this is only used for
    /// *upgrade* requests (reader→writer), not for initial open.
    upgrade_waiters: VecDeque<UpgradeWaiter>,
    /// Pending recall for force-reclaim timeout tracking. `None` if no
    /// recall is in flight.
    pending_recall: Option<PendingRecall>,
}

impl CapInodeState {
    fn new() -> Self {
        Self {
            holders: Vec::new(),
            upgrade_waiters: VecDeque::new(),
            pending_recall: None,
        }
    }

    #[allow(dead_code)]
    fn writer_count(&self) -> usize {
        self.holders
            .iter()
            .filter(|h| !h.is_expired() && (h.caps.has_w() || h.caps.has_x()))
            .count()
    }

    #[allow(dead_code)]
    fn reader_count(&self) -> usize {
        self.holders
            .iter()
            .filter(|h| !h.is_expired() && h.caps.has_r())
            .count()
    }

    fn active_holders(&self) -> impl Iterator<Item = &CapHolder> {
        self.holders.iter().filter(|h| !h.is_expired())
    }

    fn find_holder_mut(&mut self, token: &str) -> Option<&mut CapHolder> {
        self.holders.iter_mut().find(|h| h.token == token)
    }

    fn remove_holder(&mut self, token: &str) -> Option<CapHolder> {
        let idx = self.holders.iter().position(|h| h.token == token)?;
        Some(self.holders.swap_remove(idx))
    }

    /// Recompute logical state from the holder list.
    fn logical_state(&self) -> CapState {
        let active: Vec<&CapHolder> = self.active_holders().collect();
        if active.is_empty() {
            return CapState::Free;
        }
        // Classify each active holder:
        // - "active writer" = is_writer AND (has CAP_W or CAP_X, or caps is
        //   empty i.e. SHARED_WRITE participant). A writer downgraded to
        //   reader (caps=CAP_R only, is_writer=true) is NOT an active writer.
        // - "reader" = everyone else (has CAP_R, no W/X).
        let active_writers: Vec<&CapHolder> = active
            .iter()
            .copied()
            .filter(|h| h.is_writer && (h.caps.has_w() || h.caps.has_x() || h.caps.is_empty()))
            .collect();
        if !active_writers.is_empty() {
            // Exactly one writer with full caps (R+W+X) → ExclusiveWrite.
            if active_writers.len() == 1 && active_writers[0].caps.is_exclusive() {
                return CapState::ExclusiveWrite;
            }
            // Otherwise (≥2 writers, or writer without full caps) → SharedWrite.
            return CapState::SharedWrite;
        }
        // No active writers — only readers (may include downgraded writers
        // that now hold only CAP_R).
        CapState::SharedRead
    }
}

/// A queued upgrade request (reader → writer). Unlike initial open (which
/// never blocks), an upgrade may need to wait for other readers to release
/// `CAP_R` if strict write exclusion is desired. In the pure Cap model the
/// upgrade can also degrade to SHARED_WRITE (no blocking) — the waiter is
/// only used when the client explicitly requests exclusive upgrade.
#[allow(dead_code)]
struct UpgradeWaiter {
    client_id: String,
    sender: oneshot::Sender<OpenGrantResult>,
}

/// Server-side trait for pushing cap recall notifications to clients.
///
/// Implemented by the net layer (which has the gRPC push channel). The
/// `CapManager` calls `recall(...)` on cap degradation (e.g. a second writer
/// opens the file → recall the first writer's `CAP_W`+`CAP_X`).
///
/// The default `NoopCapRevoker` is a no-op so the manager works in tests
/// without a transport — in that mode recalls are silently dropped and
/// force-reclaim (via TTL + recall timeout) is the only progression path.
pub trait CapRevoker: Send + Sync {
    /// Push a recall notification to `holder` for `(inode, token)`.
    /// `caps_to_recall` tells the client what to release:
    /// - `CAP_W | CAP_X`: flush dirty + sync meta + ACK
    /// - `CAP_R`: invalidate read cache + ACK
    /// - `EXCLUSIVE`: flush + invalidate everything + ACK
    ///
    /// Returns `Ok(())` if the message was queued for delivery (not
    /// necessarily ACK'd). The server will wait up to `recall_timeout_ms`
    /// for the `RecallAck`; on timeout it force-reclaims the caps and
    /// bumps the epoch (fencing the stale client's subsequent IO).
    fn recall(
        &self,
        inode: u64,
        holder: &str,
        token: &str,
        caps_to_recall: CapSet,
        retained_caps: CapSet,
        new_epoch: u64,
    ) -> Result<(), String>;
}

/// A `CapRevoker` that does nothing — the default for unit tests.
#[derive(Debug, Default)]
pub struct NoopCapRevoker;

impl CapRevoker for NoopCapRevoker {
    fn recall(
        &self,
        _inode: u64,
        _holder: &str,
        _token: &str,
        _caps_to_recall: CapSet,
        _retained_caps: CapSet,
        _new_epoch: u64,
    ) -> Result<(), String> {
        Ok(())
    }
}

/// Hook for recording a health penalty when a client fails to ACK a recall
/// within the timeout (§13.6.1). Mirrors `RevokeTimeoutPenalty` from the
/// legacy lease model.
pub trait RecallTimeoutPenalty: Send + Sync {
    fn on_recall_ack_timeout(&self, client_id: &str);
}

#[derive(Debug, Default)]
pub struct NoopRecallPenalty;

impl RecallTimeoutPenalty for NoopRecallPenalty {
    fn on_recall_ack_timeout(&self, _client_id: &str) {}
}

/// Capability manager — server-side, per-Filer-leader. Replaces the role of
/// `InodeLeaseManager` for the Cap model (§13).
///
/// Thread-safe via a single `Mutex<HashMap<u64, CapInodeState>>`. The critical
/// sections are short (in-memory state transitions + token generation); recall
/// pushes are dispatched outside the lock via the `CapRevoker` trait.
#[derive(Clone)]
pub struct CapManager {
    /// Per-inode state. Lazily created on first `open_grant`.
    ///
    /// **Ceph alignment**: Ceph embeds cap state directly in `CInode::
    /// client_caps` (a `mempool_cap_map client -> Capability`). PowerFS
    /// mirrors this by also writing caps into `CachedInode::client_caps`
    /// via the optional `meta_cache` handle; this `inodes` map remains
    /// the fast-path authority for the cap state machine and is kept in
    /// sync with the inode-embedded copy. The inode-embedded copy is the
    /// "source of truth" for serialization (only `InodeInfo` is
    /// serialized — cap state is ephemeral, matching Ceph's behaviour
    /// where `CInode::client_caps` is not persisted to the backing store).
    inodes: Arc<Mutex<HashMap<u64, CapInodeState>>>,
    /// Per-client reverse index (Ceph: `SessionMap` keyed by `client_t`).
    ///
    /// Mirrors Ceph's `Session::caps` xlist: each `ClientSession` holds
    /// the set of `(inode, cap_id)` pairs the client owns, enabling O(1)
    /// cleanup on session close — we iterate the set and remove each cap
    /// from its inode's holder list, instead of scanning all inodes.
    sessions: Arc<Mutex<HashMap<String, Arc<ClientSession>>>>,
    /// Global cap_id allocator (Ceph: `MDCache::last_cap_id`). Monotonic
    /// across the Filer's lifetime; assigned to every new `CapHolder`.
    cap_id_alloc: Arc<AtomicU64>,
    /// SN allocator (§5.2 / §13.6.1) — leader-local, optimistic.
    sn: Arc<SnAllocator>,
    /// Global epoch counter — bumped on every recall/force-reclaim so stale
    /// IO from an unresponsive client is fenced off by the storage layer.
    epoch: Arc<AtomicU64>,
    /// Recall push transport. `NoopCapRevoker` in tests.
    revoker: Arc<dyn CapRevoker>,
    /// Recall timeout penalty hook. `NoopRecallPenalty` in tests.
    penalty: Arc<dyn RecallTimeoutPenalty>,
    /// Default cap duration (TTL).
    duration_ms: u64,
    /// Recall ACK timeout before force-reclaim.
    recall_timeout_ms: u64,
    /// Optional MetaCache reference for refcount tracking (Phase 3 lease
    /// recall parity). When set, `open_grant` bumps refcount on new grants
    /// and `release_cap` decrements it.
    meta_cache: Option<Arc<MetaCache>>,
}

impl Default for CapManager {
    fn default() -> Self {
        Self::new()
    }
}

impl CapManager {
    pub fn new() -> Self {
        Self {
            inodes: Arc::new(Mutex::new(HashMap::new())),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            cap_id_alloc: Arc::new(AtomicU64::new(1)),
            sn: Arc::new(SnAllocator::default()),
            epoch: Arc::new(AtomicU64::new(1)),
            revoker: Arc::new(NoopCapRevoker),
            penalty: Arc::new(NoopRecallPenalty),
            duration_ms: DEFAULT_CAP_DURATION_MS,
            recall_timeout_ms: DEFAULT_RECALL_TIMEOUT_MS,
            meta_cache: None,
        }
    }

    /// Get-or-create the `ClientSession` for `client_id` (Ceph:
    /// `SessionMap::get_or_existing`).
    pub fn session_for(&self, client_id: &str) -> Arc<ClientSession> {
        let mut sessions = self.sessions.lock().unwrap();
        sessions
            .entry(client_id.to_string())
            .or_insert_with(|| Arc::new(ClientSession::default()))
            .clone()
    }

    /// Drop a client session and bulk-clean all its caps (Ceph:
    /// `SessionMap::mark_session_dead` + `CInode::remove_client_cap`
    /// for each cap in `Session::caps`).
    ///
    /// Returns the list of `(inode, cap_id)` pairs that were removed,
    /// so the caller (net layer) can push any required recall/upgrade
    /// notifications to remaining holders.
    pub fn close_session(
        &self,
        client_id: &str,
    ) -> Vec<(u64, u64)> {
        let session = {
            let mut sessions = self.sessions.lock().unwrap();
            sessions.remove(client_id).map(|s| s.clone())
        };
        let Some(session) = session else {
            return Vec::new();
        };

        let removed = session.snapshot_caps();
        let mut inodes = self.inodes.lock().unwrap();
        for &(inode, cap_id) in &removed {
            if let Some(state) = inodes.get_mut(&inode) {
                // Remove the holder matching this cap_id (Ceph:
                // `CInode::remove_client_cap`).
                let token_to_remove: Option<String> = state
                    .holders
                    .iter()
                    .find(|h| h.cap_id == cap_id)
                    .map(|h| h.token.clone());
                if let Some(token) = token_to_remove {
                    state.remove_holder(&token);
                }
                if let Some(mc) = &self.meta_cache {
                    mc.decr_refcount(inode);
                    mc.cap_detach(inode, client_id);
                }
            }
        }
        removed
    }

    pub fn with_revoker(mut self, revoker: Arc<dyn CapRevoker>) -> Self {
        self.revoker = revoker;
        self
    }

    pub fn with_penalty(mut self, penalty: Arc<dyn RecallTimeoutPenalty>) -> Self {
        self.penalty = penalty;
        self
    }

    pub fn with_meta_cache(mut self, mc: Arc<MetaCache>) -> Self {
        self.meta_cache = Some(mc);
        self
    }

    pub fn with_duration_ms(mut self, ms: u64) -> Self {
        self.duration_ms = ms;
        self
    }

    pub fn with_recall_timeout_ms(mut self, ms: u64) -> Self {
        self.recall_timeout_ms = ms;
        self
    }

    /// Current global epoch. Used by the net layer to stamp storage IO
    /// requests for fencing.
    pub fn current_epoch(&self) -> u64 {
        self.epoch.load(Ordering::Relaxed)
    }

    /// Push a recall notification to a cap holder via the configured
    /// `CapRevoker`. Called by the net layer's `handle_cap_open_grant`
    /// to dispatch `RecallTask`s asynchronously (fire-and-forget — open
    /// does not wait for the recall ACK).
    pub fn recall_holder(
        &self,
        inode: u64,
        holder: &str,
        token: &str,
        caps_to_recall: CapSet,
        retained_caps: CapSet,
        new_epoch: u64,
    ) -> Result<(), String> {
        self.revoker.recall(
            inode,
            holder,
            token,
            caps_to_recall,
            retained_caps,
            new_epoch,
        )
    }

    /// Grant caps for an `open()` call. **Always succeeds** (open never
    /// blocks in the Cap model). Returns the granted caps + any recall tasks
    /// the net layer must dispatch asynchronously.
    ///
    /// `is_write_open` should be `true` for `O_WRONLY | O_RDWR`, `false` for
    /// `O_RDONLY`.
    ///
    /// # State transitions (§13.2.2)
    ///
    /// | Current state | open type | New state | Granted caps | Recall |
    /// |---------------|-----------|-----------|--------------|--------|
    /// | Free          | RDONLY    | SharedRead | CAP_R        | none |
    /// | Free          | RDWR      | ExclusiveWrite | EXCLUSIVE | none |
    /// | SharedRead    | RDONLY    | SharedRead | CAP_R        | none |
    /// | SharedRead    | RDWR      | SharedWrite | NONE        | (optional) recall readers' CAP_R |
    /// | ExclusiveWrite | RDONLY   | SharedRead | CAP_R        | recall holder's CAP_W+CAP_X |
    /// | ExclusiveWrite | RDWR     | SharedWrite | NONE        | recall holder's CAP_W+CAP_X |
    /// | SharedWrite   | RDONLY    | SharedWrite | CAP_R (or NONE) | none |
    /// | SharedWrite   | RDWR     | SharedWrite | NONE        | none |
    pub fn open_grant(&self, inode: u64, client_id: &str, is_write_open: bool) -> OpenGrantResult {
        let mut inodes = self.inodes.lock().unwrap();
        let state = inodes.entry(inode).or_insert_with(CapInodeState::new);

        // Idempotent re-open by the same client: return existing caps.
        // (Same client opening the same inode twice — e.g. dup(fd) — keeps
        // the original grant; we don't issue a new token.)
        if let Some(existing) = state
            .active_holders()
            .find(|h| h.client_id == client_id)
            .cloned()
        {
            return OpenGrantResult {
                granted_caps: existing.caps,
                token: existing.token.clone(),
                epoch: existing.epoch,
                duration_ms: self.duration_ms,
                sn: 0, // no new SN on idempotent re-open
                recall_tasks: Vec::new(),
            };
        }

        let current_state = state.logical_state();
        let now = Instant::now();
        let expire_at = now + Duration::from_millis(self.duration_ms);
        let sn = self.sn.next_sn();
        let epoch = self.epoch.load(Ordering::Relaxed);

        let (granted_caps, recall_tasks): (CapSet, Vec<RecallTask>) = match current_state {
            CapState::Free => {
                // No conflict — grant full caps based on open type.
                let caps = if is_write_open {
                    CapSet::EXCLUSIVE
                } else {
                    CapSet::CAP_R
                };
                (caps, Vec::new())
            }

            CapState::SharedRead => {
                if is_write_open {
                    // Reader→writer transition: degrade to SHARED_WRITE.
                    // The new writer gets NO CAP_W (synchronous IO). Existing
                    // readers keep CAP_R (their cache is still valid for
                    // reads; they'll see the writer's updates via sync RPC
                    // on the read path... actually no — once a writer exists,
                    // readers' CAP_R is stale. We should recall it.)
                    //
                    // Per §13.2.2: recall readers' CAP_R (optional but
                    // correct — readers' cached data may be stale once a
                    // writer is active). We choose to recall for correctness.
                    let mut recalls = Vec::new();
                    let new_epoch = self.epoch.fetch_add(1, Ordering::Relaxed) + 1;
                    for h in state.active_holders() {
                        if h.caps.has_r() {
                            recalls.push(RecallTask {
                                holder: h.client_id.clone(),
                                token: h.token.clone(),
                                caps_to_recall: CapSet::CAP_R,
                                retained_caps: CapSet::NONE,
                                new_epoch,
                            });
                        }
                    }
                    // Mark recall in flight + downgrade existing readers.
                    for h in state.holders.iter_mut() {
                        if !h.is_expired() && h.caps.has_r() {
                            h.caps = CapSet::NONE;
                            h.recall_in_flight = true;
                            h.epoch = new_epoch;
                        }
                    }
                    if let Some(r) = recalls.first() {
                        state.pending_recall = Some(PendingRecall {
                            sent_at: now,
                            holder: r.holder.clone(),
                            token: r.token.clone(),
                            inode,
                        });
                    }
                    (CapSet::NONE, recalls)
                } else {
                    // Reader + reader: compatible, grant CAP_R.
                    (CapSet::CAP_R, Vec::new())
                }
            }

            CapState::ExclusiveWrite => {
                // An exclusive writer (C1) holds CAP_R+W+X. A new open
                // (read or write) forces a recall of C1's CAP_W+CAP_X.
                // - open(RDONLY) by C2 → recall C1.W+X, C1 keeps CAP_R,
                //   C2 gets CAP_R → SHARED_READ.
                // - open(RDWR) by C2 → recall C1.W+X, C1 keeps nothing
                //   (degrade to NONE), C2 gets NONE → SHARED_WRITE.
                let new_epoch = self.epoch.fetch_add(1, Ordering::Relaxed) + 1;
                let mut recalls = Vec::new();
                for h in state.active_holders() {
                    if h.caps.is_exclusive() {
                        let retained = if is_write_open {
                            CapSet::NONE
                        } else {
                            // Downgrade writer to reader: keep CAP_R.
                            CapSet::CAP_R
                        };
                        let to_recall = if is_write_open {
                            CapSet::EXCLUSIVE
                        } else {
                            CapSet::CAP_W | CapSet::CAP_X
                        };
                        recalls.push(RecallTask {
                            holder: h.client_id.clone(),
                            token: h.token.clone(),
                            caps_to_recall: to_recall,
                            retained_caps: retained,
                            new_epoch,
                        });
                    }
                }
                // Apply the downgrade to existing holders immediately
                // (the recall ACK confirms, but the server-side state
                // transitions now so subsequent opens see the new state).
                for h in state.holders.iter_mut() {
                    if !h.is_expired() && h.caps.is_exclusive() {
                        h.caps = if is_write_open {
                            CapSet::NONE
                        } else {
                            CapSet::CAP_R
                        };
                        h.recall_in_flight = true;
                        h.epoch = new_epoch;
                    }
                }
                if let Some(r) = recalls.first() {
                    state.pending_recall = Some(PendingRecall {
                        sent_at: now,
                        holder: r.holder.clone(),
                        token: r.token.clone(),
                        inode,
                    });
                }
                let granted = if is_write_open {
                    CapSet::NONE
                } else {
                    CapSet::CAP_R
                };
                (granted, recalls)
            }

            CapState::SharedWrite => {
                // Already in shared-write mode — no caps to recall.
                // New writer: NONE. New reader: CAP_R (can cache reads
                // since all writes go through sync RPC).
                let caps = if is_write_open {
                    CapSet::NONE
                } else {
                    CapSet::CAP_R
                };
                (caps, Vec::new())
            }
        };

        // Insert the new holder.
        let token = generate_token(inode, client_id, sn);
        let cap_id = self.cap_id_alloc.fetch_add(1, Ordering::Relaxed);
        let holder = CapHolder {
            inode,
            client_id: client_id.to_string(),
            token: token.clone(),
            cap_id,
            caps: granted_caps,
            pending: granted_caps,
            wanted: granted_caps,
            revokes: VecDeque::new(),
            last_seq: sn,
            last_issue_seq: sn,
            mseq: 0,
            state_flags: CapStateFlags::NEW,
            is_writer: is_write_open,
            acquired_at: now,
            expire_at,
            epoch,
            recall_in_flight: false,
        };
        state.holders.push(holder);

        // Register the cap in the client's session reverse index
        // (Ceph: `Session::caps.push_back(&cap->item_session_caps)`).
        self.session_for(client_id).add_cap(inode, cap_id);

        // Mirror the cap into the inode-embedded `client_caps` map
        // (Ceph: `CInode::client_caps[client] = Capability`). The
        // inode-embedded copy is the authoritative in-memory state for
        // serialization; the `inodes` map above is the fast-path index
        // for the cap state machine.
        if let Some(mc) = &self.meta_cache {
            mc.incr_refcount(inode);
            mc.cap_attach(inode, client_id, cap_id, granted_caps);
        }

        log::debug!(
            "CapManager::open_grant inode={} client={} write_open={} state={:?} granted={:?} recalls={}",
            inode,
            client_id,
            is_write_open,
            current_state,
            granted_caps,
            recall_tasks.len()
        );

        OpenGrantResult {
            granted_caps,
            token,
            epoch,
            duration_ms: self.duration_ms,
            sn,
            recall_tasks,
        }
    }

    /// Client acknowledges a cap recall. The server clears the pending
    /// recall and, if the holder retained some caps (e.g. CAP_R after
    /// downgrade), updates its record. If the holder released all caps,
    /// it's removed from the holder list.
    ///
    /// Returns `Ok(retained_caps)` — the caps the holder still has after
    /// the ACK (may be `NONE`).
    pub fn recall_ack(&self, inode: u64, client_id: &str, token: &str) -> Result<CapSet, String> {
        let mut inodes = self.inodes.lock().unwrap();
        let state = inodes
            .get_mut(&inode)
            .ok_or_else(|| format!("inode {} not found in cap manager", inode))?;

        let (retained, h_is_writer) = {
            let h = state
                .find_holder_mut(token)
                .ok_or_else(|| format!("token {} not found for inode {}", token, inode))?;
            if h.client_id != client_id {
                return Err(format!(
                    "recall_ack holder mismatch: expected={} got={}",
                    h.client_id, client_id
                ));
            }
            h.recall_in_flight = false;
            (h.caps, h.is_writer)
        };

        // Clear pending recall if this ACK matches.
        if let Some(pr) = &state.pending_recall {
            if pr.token == token {
                state.pending_recall = None;
            }
        }

        // If the holder retained no caps AND it's a reader (not a writer),
        // remove it — pure readers with no caps serve no purpose. SHARED_WRITE
        // writers (caps=NONE, is_writer=true) must stay until `release_cap`
        // (close) so the upgrade-detection logic can count active writers.
        if retained.is_empty() && !h_is_writer {
            state.remove_holder(token);
            if let Some(mc) = &self.meta_cache {
                mc.decr_refcount(inode);
            }
        } else if retained.is_empty() {
            // SHARED_WRITE writer: keep in holder list but caps already NONE.
            // The holder's `release_cap` (close) will remove it and may
            // trigger an upgrade for the remaining writer.
        }

        log::debug!(
            "CapManager::recall_ack inode={} client={} retained={:?}",
            inode,
            client_id,
            retained
        );
        Ok(retained)
    }

    /// Release all caps for a client on `close()`. If the client held
    /// `CAP_W` (dirty data), the caller (net handler) must have already
    /// flushed + synced before calling this — `release_cap` does NOT
    /// flush (the flush barrier is the caller's responsibility, §13.5).
    ///
    /// **Upgrade detection (§13.4 场景 3):** after releasing, if exactly
    /// one writer remains and it currently has no `CAP_W` (i.e. we're in
    /// SHARED_WRITE), upgrade it back to `EXCLUSIVE_WRITE` by granting
    /// `CAP_W+CAP_X`. This restores high-performance local caching.
    ///
    /// Returns `Ok(Some(upgrade_task))` if an upgrade was triggered — the
    /// net layer must push the `GrantCap` notification to the upgraded
    /// client so it knows it can resume local caching.
    pub fn release_cap(
        &self,
        inode: u64,
        client_id: &str,
        token: &str,
    ) -> Result<Option<UpgradeTask>, String> {
        let mut inodes = self.inodes.lock().unwrap();
        let state = inodes
            .get_mut(&inode)
            .ok_or_else(|| format!("inode {} not found in cap manager", inode))?;

        let removed = state
            .remove_holder(token)
            .ok_or_else(|| format!("token {} not found for inode {}", token, inode))?;
        if removed.client_id != client_id {
            // Re-insert and error out — token/client mismatch.
            state.holders.push(removed.clone());
            return Err(format!(
                "release_cap holder mismatch: expected={} got={}",
                removed.client_id, client_id
            ));
        }

        // Remove from the client session reverse index (Ceph:
        // `cap->item_session_caps.remove_myself()`).
        let cap_id = removed.cap_id;
        if let Some(sess) = self.sessions.lock().unwrap().get(client_id) {
            sess.remove_cap(inode, cap_id);
            sess.release_caps_total
                .fetch_add(1, Ordering::Relaxed);
        }

        // Decr MetaCache refcount and detach the inode-embedded cap
        // (Ceph: `CInode::remove_client_cap`).
        if let Some(mc) = &self.meta_cache {
            mc.decr_refcount(inode);
            mc.cap_detach(inode, client_id);
        }

        // Upgrade detection: if we were in SHARED_WRITE and now exactly 1
        // active writer remains with no CAP_W, upgrade it to EXCLUSIVE.
        let mut upgrade = None;
        let all_active: Vec<CapHolder> = state.active_holders().cloned().collect();
        let active_writers: Vec<&CapHolder> = all_active.iter().filter(|h| h.is_writer).collect();

        // SHARED_WRITE → EXCLUSIVE_WRITE upgrade: exactly 1 active writer
        // remaining, and it currently has no CAP_W (was degraded).
        if active_writers.len() == 1 && !active_writers[0].caps.has_w() {
            let survivor = active_writers[0];
            // Upgrade: grant CAP_W+CAP_X (and CAP_R for local read cache).
            let new_caps = CapSet::EXCLUSIVE;
            let new_epoch = self.epoch.fetch_add(1, Ordering::Relaxed) + 1;
            let new_sn = self.sn.next_sn();

            // Update the holder in place.
            if let Some(h) = state.find_holder_mut(&survivor.token) {
                h.caps = new_caps;
                h.epoch = new_epoch;
                h.recall_in_flight = false;
            }

            upgrade = Some(UpgradeTask {
                holder: survivor.client_id.clone(),
                token: survivor.token.clone(),
                granted_caps: new_caps,
                epoch: new_epoch,
                sn: new_sn,
            });

            log::info!(
                "CapManager::release_cap upgrade inode={} survivor={} to EXCLUSIVE_WRITE",
                inode,
                survivor.client_id
            );
        }

        // If no holders remain, clean up the inode entry.
        if state.active_holders().next().is_none() {
            inodes.remove(&inode);
        }

        log::debug!(
            "CapManager::release_cap inode={} client={} upgrade={}",
            inode,
            client_id,
            upgrade.is_some()
        );
        Ok(upgrade)
    }

    /// Force-reclaim recalls that have timed out (§13.6.1). Called by a
    /// background sweep. Returns the list of `(inode, holder)` pairs that
    /// were force-reclaimed — the net layer may log / penalize health.
    pub fn drain_expired_recalls(&self) -> Vec<(u64, String)> {
        let now = Instant::now();
        let timeout = Duration::from_millis(self.recall_timeout_ms);
        let mut reclaimed = Vec::new();

        let mut inodes = self.inodes.lock().unwrap();
        let new_epoch = self.epoch.fetch_add(1, Ordering::Relaxed) + 1;

        for (inode, state) in inodes.iter_mut() {
            let should_reclaim = state
                .pending_recall
                .as_ref()
                .map(|pr| now.duration_since(pr.sent_at) > timeout)
                .unwrap_or(false);
            if should_reclaim {
                // Extract the token before mutable borrow via remove_holder.
                let token_to_reclaim = state.pending_recall.as_ref().unwrap().token.clone();
                // Force-reclaim: remove the holder, bump epoch, penalize.
                if let Some(h) = state.remove_holder(&token_to_reclaim) {
                    reclaimed.push((*inode, h.client_id.clone()));
                    self.penalty.on_recall_ack_timeout(&h.client_id);
                    // Clean up session reverse index (Ceph:
                    // `cap->item_session_caps.remove_myself()`).
                    if let Some(sess) =
                        self.sessions.lock().unwrap().get(&h.client_id)
                    {
                        sess.remove_cap(*inode, h.cap_id);
                    }
                    if let Some(mc) = &self.meta_cache {
                        mc.decr_refcount(*inode);
                        mc.cap_detach(*inode, &h.client_id);
                    }
                }
                state.pending_recall = None;
                // Bump epoch on remaining holders so they fence off
                // the stale client's IO.
                for h in state.holders.iter_mut() {
                    h.epoch = new_epoch;
                }
            }
        }

        // Clean up empty inodes.
        inodes.retain(|_, state| state.active_holders().next().is_some());

        reclaimed
    }

    /// Validate that a client holds a cap grant for an inode. Used by the
    /// Filer-side write/setattr handlers to check whether the client may
    /// use local cache (CAP_W / CAP_X) or must go through sync RPC.
    ///
    /// Returns `Ok(caps)` — the caps the client currently holds (may be
    /// `NONE` if the client is a SHARED_WRITE participant).
    pub fn validate_cap(&self, inode: u64, client_id: &str, token: &str) -> Result<CapSet, String> {
        let inodes = self.inodes.lock().unwrap();
        let state = inodes
            .get(&inode)
            .ok_or_else(|| format!("inode {} not found", inode))?;
        let h = state
            .holders
            .iter()
            .find(|h| h.token == token)
            .ok_or_else(|| format!("token {} not found", token))?;
        if h.client_id != client_id {
            return Err("holder mismatch".to_string());
        }
        if h.is_expired() {
            return Err("cap expired".to_string());
        }
        Ok(h.caps)
    }

    /// Snapshot the current logical state for an inode (observability /
    /// admin API). Returns `Free` if the inode has no cap state.
    pub fn logical_state(&self, inode: u64) -> CapState {
        let inodes = self.inodes.lock().unwrap();
        inodes
            .get(&inode)
            .map(|s| s.logical_state())
            .unwrap_or(CapState::Free)
    }

    /// Number of active cap holders for an inode (admin / metrics).
    pub fn holder_count(&self, inode: u64) -> usize {
        let inodes = self.inodes.lock().unwrap();
        inodes
            .get(&inode)
            .map(|s| s.active_holders().count())
            .unwrap_or(0)
    }
}

/// An upgrade notification to push to a client when it's promoted from
/// SHARED_WRITE participant back to EXCLUSIVE_WRITE (§13.4 场景 3).
#[derive(Debug, Clone)]
pub struct UpgradeTask {
    pub holder: String,
    pub token: String,
    pub granted_caps: CapSet,
    pub epoch: u64,
    pub sn: u64,
}

/// Generate a unique token for a cap grant. Format: `cap-{inode}-{client}-{sn}`.
fn generate_token(inode: u64, client_id: &str, sn: u64) -> String {
    // Trim client_id to 16 chars to keep tokens manageable.
    let short_client = if client_id.len() > 16 {
        &client_id[..16]
    } else {
        client_id
    };
    format!("cap-{}-{}-{}", inode, short_client, sn)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn t1_cap_set_bit_ops() {
        assert!(CapSet::CAP_R.has_r());
        assert!(!CapSet::CAP_R.has_w());
        assert!(CapSet::EXCLUSIVE.is_exclusive());
        assert!(CapSet::NONE.is_empty());

        let downgraded = CapSet::EXCLUSIVE.remove(CapSet::CAP_W | CapSet::CAP_X);
        assert!(downgraded.has_r());
        assert!(!downgraded.has_w());

        let upgraded = CapSet::NONE.union(CapSet::EXCLUSIVE);
        assert!(upgraded.is_exclusive());
    }

    #[test]
    fn t2_open_readonly_grants_cap_r() {
        let mgr = CapManager::new();
        let result = mgr.open_grant(100, "client-A", false);
        assert_eq!(result.granted_caps, CapSet::CAP_R);
        assert!(result.recall_tasks.is_empty());
        assert_eq!(mgr.logical_state(100), CapState::SharedRead);
    }

    #[test]
    fn t3_open_rdwr_grants_exclusive() {
        let mgr = CapManager::new();
        let result = mgr.open_grant(200, "client-A", true);
        assert!(result.granted_caps.is_exclusive());
        assert!(result.recall_tasks.is_empty());
        assert_eq!(mgr.logical_state(200), CapState::ExclusiveWrite);
    }

    #[test]
    fn t4_multiple_readers_compatible() {
        let mgr = CapManager::new();
        let _r1 = mgr.open_grant(300, "client-A", false);
        let r2 = mgr.open_grant(300, "client-B", false);

        assert_eq!(r2.granted_caps, CapSet::CAP_R);
        assert!(r2.recall_tasks.is_empty());
        assert_eq!(mgr.logical_state(300), CapState::SharedRead);
        assert_eq!(mgr.holder_count(300), 2);
    }

    #[test]
    fn t5_second_writer_triggers_recall_and_degrades_to_shared_write() {
        let mgr = CapManager::new();
        // C1 opens RDWR → EXCLUSIVE_WRITE
        let r1 = mgr.open_grant(400, "client-A", true);
        assert!(r1.granted_caps.is_exclusive());

        // C2 opens RDWR → recall C1's W+X, degrade to SHARED_WRITE
        let r2 = mgr.open_grant(400, "client-B", true);

        // C2 gets NO caps (SHARED_WRITE = sync IO)
        assert_eq!(r2.granted_caps, CapSet::NONE);
        // One recall task for C1
        assert_eq!(r2.recall_tasks.len(), 1);
        let recall = &r2.recall_tasks[0];
        assert_eq!(recall.holder, "client-A");
        assert_eq!(recall.caps_to_recall, CapSet::EXCLUSIVE);
        assert_eq!(recall.retained_caps, CapSet::NONE);

        // State is now SHARED_WRITE
        assert_eq!(mgr.logical_state(400), CapState::SharedWrite);
    }

    #[test]
    fn t6_reader_after_writer_downgrades_to_shared_read() {
        let mgr = CapManager::new();
        // C1 opens RDWR → EXCLUSIVE_WRITE
        let _r1 = mgr.open_grant(500, "client-A", true);

        // C2 opens RDONLY → recall C1's W+X, C1 keeps CAP_R, C2 gets CAP_R
        let r2 = mgr.open_grant(500, "client-B", false);

        assert_eq!(r2.granted_caps, CapSet::CAP_R);
        assert_eq!(r2.recall_tasks.len(), 1);
        let recall = &r2.recall_tasks[0];
        assert_eq!(recall.caps_to_recall, CapSet::CAP_W | CapSet::CAP_X);
        assert_eq!(recall.retained_caps, CapSet::CAP_R);

        assert_eq!(mgr.logical_state(500), CapState::SharedRead);
    }

    #[test]
    fn t7_recall_ack_clears_pending_recall() {
        let mgr = CapManager::new();
        let r1 = mgr.open_grant(600, "client-A", true);
        let _r2 = mgr.open_grant(600, "client-B", true);

        // C1 ACKs the recall (flush done, caps released)
        let retained = mgr.recall_ack(600, "client-A", &r1.token).unwrap();
        assert_eq!(retained, CapSet::NONE); // C1 retained nothing
    }

    #[test]
    fn t8_release_writer_triggers_upgrade() {
        let mgr = CapManager::new();
        // C1 and C2 both open RDWR → SHARED_WRITE
        let r1 = mgr.open_grant(700, "client-A", true);
        let _r2 = mgr.open_grant(700, "client-B", true);
        assert_eq!(mgr.logical_state(700), CapState::SharedWrite);

        // C1 ACKs its recall first
        mgr.recall_ack(700, "client-A", &r1.token).unwrap();

        // C1 releases → only C2 remains, should upgrade to EXCLUSIVE
        let upgrade = mgr.release_cap(700, "client-A", &r1.token).unwrap();
        assert!(upgrade.is_some(), "should trigger upgrade for C2");
        let u = upgrade.unwrap();
        assert_eq!(u.holder, "client-B");
        assert!(u.granted_caps.is_exclusive());

        assert_eq!(mgr.logical_state(700), CapState::ExclusiveWrite);
    }

    #[test]
    fn t9_force_reclaim_on_recall_timeout() {
        let mgr = CapManager::new().with_recall_timeout_ms(0); // immediate timeout
        let r1 = mgr.open_grant(800, "client-A", true);
        let _r2 = mgr.open_grant(800, "client-B", true);

        // Recall is pending for C1; with 0ms timeout, drain should reclaim it
        std::thread::sleep(Duration::from_millis(1));
        let reclaimed = mgr.drain_expired_recalls();
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].1, "client-A");
    }

    #[test]
    fn t10_idempotent_reopen_same_client() {
        let mgr = CapManager::new();
        let r1 = mgr.open_grant(900, "client-A", true);
        let r2 = mgr.open_grant(900, "client-A", true); // same client

        // Should return existing token, no new SN
        assert_eq!(r1.token, r2.token);
        assert_eq!(r2.sn, 0);
        assert_eq!(mgr.holder_count(900), 1);
    }

    #[test]
    fn t11_validate_cap_returns_held_caps() {
        let mgr = CapManager::new();
        let r = mgr.open_grant(1000, "client-A", true);

        let caps = mgr.validate_cap(1000, "client-A", &r.token).unwrap();
        assert!(caps.is_exclusive());
    }

    #[test]
    fn t12_shared_write_third_writer_no_recall() {
        let mgr = CapManager::new();
        let _r1 = mgr.open_grant(1100, "client-A", true);
        let _r2 = mgr.open_grant(1100, "client-B", true);
        assert_eq!(mgr.logical_state(1100), CapState::SharedWrite);

        // C3 opens RDWR — already SHARED_WRITE, no recall needed
        let r3 = mgr.open_grant(1100, "client-C", true);
        assert_eq!(r3.granted_caps, CapSet::NONE);
        assert!(r3.recall_tasks.is_empty());
        assert_eq!(mgr.logical_state(1100), CapState::SharedWrite);
    }
}
