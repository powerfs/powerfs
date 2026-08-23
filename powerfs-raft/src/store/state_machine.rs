//! RocksDB-backed [`RaftStateMachine`] + [`RaftSnapshotBuilder`]。
//!
//! 参考 `openraft/examples/sm-rocks/src/state_machine.rs`，泛型化于 `C: RaftTypeConfig`。
//! CF 布局：
//! - `raft_state_meta`：`last_applied_log` / `last_membership`
//! - `raft_state_data`：业务状态（Normal entry 的 payload，key = log index big-endian）
//!
//! 快照存放在 `snapshot_dir` 目录，文件名格式 `{index:020}-{leader_id}`，按字典序即按 log index 排序。
//!
//! **设计说明**：
//! - 通过 `AsRef<openraft::Entry<...>>` 访问 `entry.payload`，区分 Blank/Normal/Membership。
//! - Normal 条目：将 `C::D` 序列化后存入 `raft_state_data` CF（key = log index big-endian），
//!   并通过 `apply_notifier` channel 通知最新 applied index（供 MasterNode 拉取并 replay）。
//! - Membership 条目：更新 `last_membership`（与旧逻辑一致）。

use std::fs;
use std::io;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;

use futures::Stream;
use futures::TryStreamExt;
use openraft::alias::LogIdOf;
use openraft::alias::SnapshotMetaOf;
use openraft::alias::SnapshotOf;
use openraft::alias::StoredMembershipOf;
use openraft::entry::RaftEntry;
use openraft::storage::EntryResponder;
use openraft::storage::RaftStateMachine;
use openraft::type_config::TypeConfigExt;
use openraft::EntryPayload;
use openraft::OptionalSend;
use openraft::RaftSnapshotBuilder;
use tracing::debug;
use tracing::warn;
use openraft::RaftTypeConfig;
use rocksdb::DB;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::mpsc;

use crate::pb_impl::CommittedLidOf;
use crate::store::CF_STATE_DATA;
use crate::store::CF_STATE_META;

/// 快照中的 `sm_data` CF 条目列表：`(key, value)` 字节对。
type SnapshotDataEntries = Vec<(Vec<u8>, Vec<u8>)>;

/// 快照文件格式：metadata + data 一起存储。
///
/// `#[serde(bound = "")]` 让 derive 使用字段类型自身的 Serialize/Deserialize bounds，
/// 而非默认的 `C: Serialize`（`C` 是 TypeConfig marker，不可序列化）。
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
struct SnapshotFile<C>
where
    C: RaftTypeConfig,
{
    meta: SnapshotMetaOf<C>,
    /// `sm_data` CF 的 (key, value) 列表。
    data: SnapshotDataEntries,
}

/// RocksDB-backed 状态机 + 快照。
///
/// 结构体本身不泛型于 `C`（与 sm-rocks 一致），但 `RaftStateMachine<C>` impl 泛型于 `C`，
/// 因此同一份代码可服务 Master / Filer 两个 TypeConfig。
///
/// 可选携带 `apply_notifier`：当 `apply()` 处理完一批 entries 后，通过 channel 发送
/// 最新 applied log index，供上层（MasterNode）读取 `raft_state_data` CF 并 replay 业务命令。
#[derive(Debug, Clone)]
pub struct RocksStateMachine {
    db: Arc<DB>,
    snapshot_dir: PathBuf,
    /// 可选的 apply 通知 channel（发送最新 applied log index）。
    apply_notifier: Option<mpsc::Sender<u64>>,
}

impl RocksStateMachine {
    /// 从已打开的 RocksDB 实例构造。要求 CF `raft_state_meta` / `raft_state_data` 已存在。
    pub(crate) async fn new(db: Arc<DB>, snapshot_dir: PathBuf) -> Result<Self, io::Error> {
        db.cf_handle(CF_STATE_META).ok_or_else(|| {
            io::Error::other(format!("column family `{CF_STATE_META}` not found"))
        })?;
        db.cf_handle(CF_STATE_DATA).ok_or_else(|| {
            io::Error::other(format!("column family `{CF_STATE_DATA}` not found"))
        })?;

        fs::create_dir_all(&snapshot_dir)?;
        Ok(Self {
            db,
            snapshot_dir,
            apply_notifier: None,
        })
    }

    /// 设置 apply 通知 channel，返回 `self` 供链式调用。
    ///
    /// 当 `apply()` 处理完一批 entries 后，会通过此 channel 发送最新 applied log index。
    /// 上层（MasterNode）收到后可从 `raft_state_data` CF 读取 Normal entry payload 并 replay。
    pub fn with_apply_notifier(mut self, tx: mpsc::Sender<u64>) -> Self {
        self.apply_notifier = Some(tx);
        self
    }

    /// 返回内部 RocksDB 句柄（供上层读取 `raft_state_data` CF 中的 applied entries）。
    pub fn db(&self) -> &Arc<DB> {
        &self.db
    }

    fn cf_meta(&self) -> &rocksdb::ColumnFamily {
        self.db.cf_handle(CF_STATE_META).unwrap()
    }

    fn cf_data(&self) -> &rocksdb::ColumnFamily {
        self.db.cf_handle(CF_STATE_DATA).unwrap()
    }

    /// 读取状态机元数据：`(last_applied_log, last_membership)`。
    fn get_meta<C>(&self) -> Result<(Option<LogIdOf<C>>, StoredMembershipOf<C>), io::Error>
    where
        C: RaftTypeConfig,
    {
        let cf = self.cf_meta();

        let last_applied_log = self
            .db
            .get_cf(cf, "last_applied_log")
            .map_err(|e| io::Error::other(e.to_string()))?
            .map(|bytes| deserialize::<LogIdOf<C>>(&bytes))
            .transpose()?;

        let last_membership = self
            .db
            .get_cf(cf, "last_membership")
            .map_err(|e| io::Error::other(e.to_string()))?
            .map(|bytes| deserialize::<StoredMembershipOf<C>>(&bytes))
            .transpose()?
            .unwrap_or_default();

        Ok((last_applied_log, last_membership))
    }

    /// 快照文件名，按 log index 排序（参考 sm-rocks）。
    fn snapshot_filename<C>(meta: &SnapshotMetaOf<C>) -> String
    where
        C: RaftTypeConfig,
    {
        match &meta.last_log_id {
            Some(last) => format!("{:020}-{}", last.index(), last.committed_leader_id()),
            None => "--".to_string(),
        }
    }
}

fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>, io::Error> {
    serde_json::to_vec(value).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

fn deserialize<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, io::Error> {
    serde_json::from_slice(bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

// ---------------------------------------------------------------------------
// RaftSnapshotBuilder
// ---------------------------------------------------------------------------

impl<C> RaftSnapshotBuilder<C> for RocksStateMachine
where
    C: RaftTypeConfig,
    C::R: Default,
{
    type SnapshotData = Cursor<Vec<u8>>;

    async fn build_snapshot(&mut self) -> Result<SnapshotOf<C, Cursor<Vec<u8>>>, io::Error> {
        let (last_applied_log, last_membership) = self.get_meta::<C>()?;

        let meta = SnapshotMetaOf::<C> {
            last_log_id: last_applied_log,
            last_membership,
        };

        // 用 RocksDB snapshot 获取一致性视图。
        let db = self.db.clone();

        let data = C::spawn_blocking(move || -> Result<SnapshotDataEntries, io::Error> {
            let snapshot = db.snapshot();
            let cf_data = db
                .cf_handle(CF_STATE_DATA)
                .expect("column family not found");

            let mut snapshot_data = Vec::new();
            let iter = snapshot.iterator_cf(cf_data, rocksdb::IteratorMode::Start);

            for item in iter {
                let (key, value) = item.map_err(|e| io::Error::other(e.to_string()))?;
                snapshot_data.push((key.to_vec(), value.to_vec()));
            }
            Ok(snapshot_data)
        })
        .await??;

        // 完整快照（meta + data）写入文件，供 `get_current_snapshot` 读取。
        let snapshot_file = SnapshotFile::<C> {
            meta: meta.clone(),
            data: data.clone(),
        };
        let file_bytes = serialize(&snapshot_file)?;

        let snapshot_path = self.snapshot_dir.join(Self::snapshot_filename::<C>(&meta));
        fs::write(&snapshot_path, &file_bytes)?;

        // 返回的 snapshot 只含 data 部分（meta 单独返回）。
        let data_bytes = serialize(&data)?;
        Ok(SnapshotOf::<C, Cursor<Vec<u8>>> {
            meta,
            snapshot: Cursor::new(data_bytes),
        })
    }
}

// ---------------------------------------------------------------------------
// RaftStateMachine
// ---------------------------------------------------------------------------

impl<C> RaftStateMachine<C> for RocksStateMachine
where
    C: RaftTypeConfig,
    C::R: Default,
    C::D: Serialize,
    C::Entry:
        RaftEntry<D = C::D> + AsRef<openraft::Entry<CommittedLidOf<C>, C::D, C::NodeId, C::Node>>,
{
    type SnapshotData = Cursor<Vec<u8>>;

    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogIdOf<C>>, StoredMembershipOf<C>), io::Error> {
        self.get_meta::<C>()
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<C>, io::Error>> + Unpin + OptionalSend,
    {
        let mut batch = rocksdb::WriteBatch::default();
        let mut last_applied_log: Option<LogIdOf<C>> = None;
        let mut last_membership: Option<StoredMembershipOf<C>> = None;
        let mut last_applied_index: Option<u64> = None;
        let mut responses: Vec<(openraft::storage::ApplyResponder<C>, C::R)> = Vec::new();
        // Normal entry 的 (key, value) 对，循环结束后再写入 batch（避免跨 await 持有 ColumnFamily）。
        let mut normal_data: Vec<([u8; 8], Vec<u8>)> = Vec::new();
        // 诊断计数：本批 apply 处理的 entry 类型分布
        let mut normal_count: usize = 0;
        let mut membership_count: usize = 0;
        let mut blank_count: usize = 0;
        let mut first_index: Option<u64> = None;

        while let Some((entry, responder)) = entries.try_next().await? {
            let log_id = entry.log_id();
            let index = log_id.index();
            if first_index.is_none() {
                first_index = Some(index);
            }
            last_applied_log = Some(log_id.clone());
            last_applied_index = Some(index);

            // 通过 AsRef 访问 entry.payload，区分 Blank / Normal / Membership。
            let payload = &entry.as_ref().payload;

            match payload {
                EntryPayload::Normal(d) => {
                    // 将 C::D 序列化后暂存（key = index big-endian）。
                    let value_bytes = serialize(d)?;
                    normal_data.push((index.to_be_bytes(), value_bytes));
                    normal_count += 1;
                }
                EntryPayload::Membership(mem) => {
                    last_membership = Some(StoredMembershipOf::<C>::new(Some(log_id), mem.clone()));
                    membership_count += 1;
                }
                EntryPayload::Blank => {
                    blank_count += 1;
                }
            }

            // 发送默认响应。
            if let Some(responder) = responder {
                responses.push((responder, C::R::default()));
            }
        }

        let total_entries = normal_count + membership_count + blank_count;
        if total_entries > 0 {
            debug!(
                "RocksStateMachine::apply: processed {} entries (normal={}, membership={}, blank={}) \
                 first_index={:?} last_index={:?}",
                total_entries, normal_count, membership_count, blank_count,
                first_index, last_applied_index
            );
        }

        // 循环结束后获取 CF 句柄，写入 batch（此区域不跨 await）。
        let cf_data = self.cf_data();
        for (key, value) in &normal_data {
            batch.put_cf(cf_data, key, value);
        }

        let cf_meta = self.cf_meta();
        if let Some(ref log_id) = last_applied_log {
            batch.put_cf(cf_meta, "last_applied_log", serialize(log_id)?);
        }
        if let Some(ref membership) = last_membership {
            batch.put_cf(cf_meta, "last_membership", serialize(membership)?);
        }

        self.db
            .write(batch)
            .map_err(|e| io::Error::other(e.to_string()))?;

        // 持久化成功后再发送响应。
        for (responder, response) in responses {
            responder.send(response);
        }

        // 通知上层 MasterNode：有新的 applied entries 可供 replay。
        if let Some(tx) = &self.apply_notifier {
            if let Some(idx) = last_applied_index {
                // 非阻塞发送：channel 满时丢弃通知（下次 apply 会再发）。
                match tx.try_send(idx) {
                    Ok(()) => {
                        debug!(
                            "RocksStateMachine::apply: notified apply_index={} (entries={})",
                            idx, total_entries
                        );
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                        warn!(
                            "RocksStateMachine::apply: apply_notifier channel FULL, \
                             dropped notification for index {} (next apply will resend)",
                            idx
                        );
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                        warn!(
                            "RocksStateMachine::apply: apply_notifier channel CLOSED, \
                             no consumer is reading (index {})",
                            idx
                        );
                    }
                }
            }
        } else if total_entries > 0 {
            // 没有 apply_notifier：可能是 MasterNode 模式（无业务逻辑需要 replay）
            // 或 Filer 启动时未注入 channel。记录一次 DEBUG 帮助诊断。
            debug!(
                "RocksStateMachine::apply: no apply_notifier set, {} entries applied silently",
                total_entries
            );
        }

        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<C>,
        snapshot: Self::SnapshotData,
    ) -> Result<(), io::Error> {
        // 反序列化快照 data。
        let snapshot_data: SnapshotDataEntries = deserialize(snapshot.get_ref())?;
        let snapshot_data_clone = snapshot_data.clone();

        let last_applied_bytes = meta.last_log_id.as_ref().map(serialize).transpose()?;
        let last_membership_bytes = serialize(&meta.last_membership)?;

        let db = self.db.clone();

        C::spawn_blocking(move || -> Result<(), io::Error> {
            let cf_data = db
                .cf_handle(CF_STATE_DATA)
                .expect("column family not found");
            let cf_meta = db
                .cf_handle(CF_STATE_META)
                .expect("column family not found");

            let mut batch = rocksdb::WriteBatch::default();

            // 清空 sm_data。
            let iter = db.iterator_cf(cf_data, rocksdb::IteratorMode::Start);
            for item in iter {
                let (key, _) = item.map_err(|e| io::Error::other(e.to_string()))?;
                batch.delete_cf(cf_data, &key);
            }

            // 写入快照数据。
            for (key, value) in snapshot_data {
                batch.put_cf(cf_data, &key, &value);
            }

            // 写入元数据。
            if let Some(bytes) = last_applied_bytes {
                batch.put_cf(cf_meta, "last_applied_log", bytes);
            }
            batch.put_cf(cf_meta, "last_membership", last_membership_bytes);

            db.write(batch)
                .map_err(|e| io::Error::other(e.to_string()))?;
            db.flush_wal(true)
                .map_err(|e| io::Error::other(e.to_string()))
        })
        .await??;

        // 写入快照文件（供后续 get_current_snapshot 读取）。
        let snapshot_file = SnapshotFile::<C> {
            meta: meta.clone(),
            data: snapshot_data_clone,
        };
        let file_bytes = serialize(&snapshot_file)?;
        let snapshot_path = self.snapshot_dir.join(Self::snapshot_filename::<C>(meta));
        fs::write(&snapshot_path, &file_bytes)?;

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<SnapshotOf<C, Self::SnapshotData>>, io::Error> {
        // 找最新的快照文件（文件名按 log index 排序）。
        let mut latest_snapshot_id: Option<String> = None;

        for entry in fs::read_dir(&self.snapshot_dir)? {
            let entry = entry?;
            let path = entry.path();

            if !path.is_file() {
                continue;
            }

            if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                let snapshot_id = filename.to_string();
                if latest_snapshot_id
                    .as_ref()
                    .is_none_or(|current| snapshot_id > *current)
                {
                    latest_snapshot_id = Some(snapshot_id);
                }
            }
        }

        let Some(snapshot_id) = latest_snapshot_id else {
            return Ok(None);
        };

        let snapshot_path = self.snapshot_dir.join(&snapshot_id);
        let file_bytes = fs::read(&snapshot_path)?;
        let snapshot_file: SnapshotFile<C> = deserialize(&file_bytes)?;

        let data_bytes = serialize(&snapshot_file.data)?;
        Ok(Some(SnapshotOf::<C, Self::SnapshotData> {
            meta: snapshot_file.meta,
            snapshot: Cursor::new(data_bytes),
        }))
    }
}
