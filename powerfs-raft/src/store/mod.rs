//! RocksDB-backed storage for openraft.
//!
//! 实现 [`openraft::storage::RaftLogStorage`] + [`openraft::storage::RaftStateMachine`]，
//! 泛型于 `C: RaftTypeConfig`，同一份代码服务 Master + Filer。
//!
//! ## Column Family 布局
//!
//! | CF | 用途 | key | value |
//! |---|---|---|---|
//! | `raft_log` | 日志条目 | `u64` big-endian (log index) | `serde_json(Entry<C>)` |
//! | `raft_log_meta` | 日志层元数据 | `"vote"` / `"last_purged"` | `serde_json(Vote/LogId)` |
//! | `raft_state_meta` | 状态机元数据 | `"last_applied_log"` / `"last_membership"` | `serde_json(LogId/StoredMembership)` |
//! | `raft_state_data` | 业务状态 | `u64` big-endian (log index) | `serde_json(D)` |
//!
//! 快照存放在独立的 `snapshot_dir` 目录，文件名按 `log_id.index` 排序（参考 sm-rocks）。
//!
//! 参考：
//! - `openraft/examples/log-rocks/src/lib.rs`（log 层范本）
//! - `openraft/examples/sm-rocks/src/state_machine.rs`（state machine 范本）

pub mod log_store;
pub mod state_machine;

pub use log_store::RocksLogStore;
pub use state_machine::RocksStateMachine;

use std::io;
use std::path::Path;
use std::sync::Arc;

use openraft::RaftTypeConfig;
use rocksdb::ColumnFamilyDescriptor;
use rocksdb::Options;
use rocksdb::DB;

/// CF：日志条目（key = u64 big-endian log index）。
pub const CF_LOG: &str = "raft_log";
/// CF：日志层元数据（vote / last_purged）。
pub const CF_LOG_META: &str = "raft_log_meta";
/// CF：状态机元数据（last_applied_log / last_membership）。
pub const CF_STATE_META: &str = "raft_state_meta";
/// CF：业务状态数据（key = u64 big-endian log index）。
pub const CF_STATE_DATA: &str = "raft_state_data";

/// 所有 CF 名（用于 `DB::open_cf_descriptors`）。
pub const ALL_CFS: &[&str] = &[CF_LOG, CF_LOG_META, CF_STATE_META, CF_STATE_DATA];

/// 快照子目录名（位于 db_path 下）。
pub const SNAPSHOT_DIR_NAME: &str = "snapshots";

/// 创建一对共享同一 RocksDB 实例的 `RocksLogStore<C>` + `RocksStateMachine<C>`。
///
/// 参考 `openraft/examples/sm-rocks/src/lib.rs::new`。
pub async fn new<C, P>(db_path: P) -> Result<(RocksLogStore<C>, RocksStateMachine), io::Error>
where
    C: RaftTypeConfig,
    P: AsRef<Path>,
{
    let mut db_opts = Options::default();
    db_opts.create_missing_column_families(true);
    db_opts.create_if_missing(true);

    let cf_descriptors = ALL_CFS
        .iter()
        .map(|name| ColumnFamilyDescriptor::new(*name, Options::default()))
        .collect::<Vec<_>>();

    let db_path = db_path.as_ref();
    let snapshot_dir = db_path.join(SNAPSHOT_DIR_NAME);

    let db =
        DB::open_cf_descriptors(&db_opts, db_path, cf_descriptors).map_err(io::Error::other)?;

    let db = Arc::new(db);
    Ok((
        RocksLogStore::new(db.clone()),
        RocksStateMachine::new(db, snapshot_dir).await?,
    ))
}
