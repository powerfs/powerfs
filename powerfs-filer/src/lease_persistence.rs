//! Raft-backed `LeasePersistence` for `InodeLeaseManager` (phase 5 §5.3).
//!
//! The lease manager's persistence backend is byte-keyed: a `LeaseEntry`
//! is serialized via `powerfs_lease::persistence::encode_entry` and stored
//! under its token string. This module wires those bytes through the filer's
//! Raft state machine (`ShardCommand::LeasePut` / `LeaseDelete` /
//! `LeaseSaveEpoch`), so a leader switch can recover lease state from the
//! Raft log and continue honoring leases already granted by the previous
//! leader — closing the "leader switch loses leases" correctness hole
//! (see `docs/lock-optimization-plan.md` §5.3 and the "Lessons Learned"
//! entry in `project_memory`).
//!
//! # Architecture
//!
//! ```text
//!  InodeLeaseManager::acquire
//!        │
//!        ▼
//!  MemoryLeaseStore::acquire  (in-memory)
//!        │
//!        ▼
//!  LeasePersistence::save     (this module — sync trait)
//!        │
//!        │  tokio::task::block_in_place(|| Handle::block_on(propose))
//!        ▼
//!  RaftGroupManagerV2::propose  (async)
//!        │
//!        ▼
//!  openraft log + replication → apply on all replicas
//!        │
//!        ▼
//!  ShardStore::lease_put       (writes CF_LEASES in RocksDB)
//! ```
//!
//! The sync→async bridge uses `tokio::task::block_in_place` because the
//! `LeasePersistence` trait is sync (it lives in `powerfs-lease`, which
//! doesn't depend on tokio). `block_in_place` is safe on a multi-threaded
//! runtime — the filer enables `features = ["full"]` so `rt-multi-thread`
//! is on. From a worker thread, `block_in_place` parks the current task
//! and runs the future on a spare thread; other worker tasks keep running.
//!
//! # Why synchronous (not async batching)
//!
//! The plan §5.3 mentions "leader 本地分配 SN(乐观),后台批量异步补 Raft 日志".
//! That design defers the Raft round-trip for SN allocation. Lease
//! **state** persistence is different: an acquire that returns to the
//! client without the entry being safely replicated is a correctness
//! regression — a leader switch before the batch flushes loses the
//! lease. Sync propose-on-save preserves the old `MemoryLeaseStore`
//! semantics ("if `acquire` returned Ok, the lease is durable") and
//! matches what the existing tests expect.
//!
//! Async batching can be layered on top later as an optimization (queue
//! → flusher → batch propose), trading a small window of potential
//! leader-switch lease loss for higher acquire throughput under high
//! contention. For phase 5 we choose correctness; the async path is a
//! profiling-driven follow-up.

use crate::raft_group_manager_v2::{RaftGroupManagerV2, ShardCommand, ShardId};
use crate::shard_store::ShardStore;
use powerfs_lease::persistence::LeasePersistence;
use powerfs_lease::LeaseError;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::task;

// Note: this module uses `tokio::task::block_in_place`. The filer enables
// `features = ["full"]` so `rt-multi-thread` is on and `block_in_place`
// is safe from any worker thread (the multi-threaded runtime parks the
// current task and runs the future on a spare thread). If the filer ever
// switches to `flavor = "current_thread"`, this module must be reworked
// to a worker-thread + channel pattern (see module-level TODOs).

/// `LeasePersistence` impl backed by the filer's Raft state machine +
/// per-shard RocksDB `CF_LEASES`.
///
/// One instance per filer. Cloning is cheap (Arc under the hood); the
/// lease manager takes ownership via `with_persistence(self, backend)`.
#[derive(Clone)]
pub struct RaftLeasePersistence {
    /// Raft manager — used to propose `LeasePut` / `LeaseDelete` /
    /// `LeaseSaveEpoch` to the shard owning the inode's lease. The
    /// lease manager calls `save`/`delete` synchronously from the
    /// acquire path; we bridge to the async `propose` via
    /// `block_in_place`.
    raft_mgr: Arc<RaftGroupManagerV2>,
    /// Shard store handle — used for local reads (`load_all` /
    /// `load_epoch`) which don't need to round-trip through Raft.
    /// The lease manager only loads on leader takeover, by which
    /// point Raft has applied all committed entries to the local
    /// RocksDB.
    shard_store: Arc<ShardStore>,
    /// Shard id where this persistence instance stores entries.
    /// Leases for inodes on shard A live on shard A's `CF_LEASES` —
    /// each `InodeLeaseManager` is per-shard in the long-term plan,
    /// but the current `InodeLeaseManager` is a single per-filer
    /// instance, so we route all entries to one shard (shard 0)
    /// for phase 5. A follow-up can shard by `calculate_shard(inode)`.
    shard_id: ShardId,
}

impl RaftLeasePersistence {
    /// Construct a new persistence backend rooted at `shard_id`.
    ///
    /// `shard_store` must be the store for that same shard — the
    /// caller (filer startup) holds the `MetaShardManager` and looks
    /// up the matching `Arc<ShardStore>`.
    pub fn new(
        raft_mgr: Arc<RaftGroupManagerV2>,
        shard_store: Arc<ShardStore>,
        shard_id: ShardId,
    ) -> Self {
        Self {
            raft_mgr,
            shard_store,
            shard_id,
        }
    }

    /// Propose a `ShardCommand` synchronously. Blocks the caller until
    /// the command is committed and applied on the local node.
    ///
    /// Uses `tokio::task::block_in_place` so the future can run on a
    /// spare thread without deadlocking the worker pool. Panics if
    /// called outside a tokio runtime — but every caller in the filer
    /// is inside `#[tokio::main]`'s multi-threaded context.
    fn propose_blocking(&self, cmd: ShardCommand) -> Result<u64, String> {
        let raft = self.raft_mgr.clone();
        let shard_id = self.shard_id;
        let payload = cmd.serialize();
        task::block_in_place(|| {
            let h = Handle::current();
            h.block_on(async move { raft.propose(shard_id, payload).await })
        })
    }

    /// Decode a `LeaseError` from a propose failure. The underlying
    /// error string is preserved as context so the lease manager can
    /// log it; we use `Internal` since `powerfs-lease`'s `LeaseError`
    /// doesn't distinguish network vs other transient failures.
    fn map_err(s: String) -> LeaseError {
        LeaseError::Internal(s)
    }
}

impl LeasePersistence for RaftLeasePersistence {
    fn save(&self, token: &str, data: &[u8]) -> Result<(), LeaseError> {
        let cmd = ShardCommand::LeasePut {
            token: token.to_string(),
            value: data.to_vec(),
        };
        self.propose_blocking(cmd)
            .map(|_| ())
            .map_err(Self::map_err)
    }

    fn delete(&self, token: &str) -> Result<(), LeaseError> {
        let cmd = ShardCommand::LeaseDelete {
            token: token.to_string(),
        };
        self.propose_blocking(cmd)
            .map(|_| ())
            .map_err(Self::map_err)
    }

    fn load_all(&self) -> Result<Vec<(String, Vec<u8>)>, LeaseError> {
        // Local read — no Raft round-trip. The lease manager calls
        // this on leader takeover, by which point openraft has applied
        // every committed log entry to the local RocksDB. Reads of
        // not-yet-committed entries would be racy, but Raft guarantees
        // the local RocksDB only reflects committed state.
        self.shard_store.lease_load_all().map_err(Self::map_err)
    }

    fn save_epoch(&self, epoch: u64) -> Result<(), LeaseError> {
        let cmd = ShardCommand::LeaseSaveEpoch { epoch };
        self.propose_blocking(cmd)
            .map(|_| ())
            .map_err(Self::map_err)
    }

    fn load_epoch(&self) -> Result<u64, LeaseError> {
        self.shard_store.lease_load_epoch().map_err(Self::map_err)
    }
}

#[cfg(test)]
mod tests {
    //! Tests live in `inode_lease_manager.rs` because they share the
    //! `PersistenceShim` mock infrastructure already wired there. The
    //! real Raft round-trip requires an initialized Raft cluster, which
    //! is exercised by the filer integration tests in `tests/`.
}
