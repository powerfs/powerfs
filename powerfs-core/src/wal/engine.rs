//! WalEngine：WAL 引擎装配与恢复（方案 §5 / §7 / §10 / 附录 B S5 / C T4）。
//!
//! 装配流程（open，方案 §10）：
//! 1. 卷目录 flock 单写者锁（拒绝第二个写者）；
//! 2. superblock 加载（双副本取合法最大 seq；首次挂载初始化；双损坏拒绝
//!    挂载）；
//! 3. checkpoint 装载：从最高 seq 逐级回退尝试（损坏仅告警），得到索引
//!    初态与重放游标 `applied_lsn`；
//! 4. 扫描段清单 + 重放：lsn ≤ applied_lsn 的前缀由 checkpoint 承载，
//!    跳过 apply 但帧级 CRC/哈希链校验照常执行（加速不弱化完整性）；
//! 5. 重开活跃段（或无段/无活跃段时以 last_lsn+1 开新段）；
//! 6. 挂载 superblock（latest_ckpt_seq 回退后重写）+ 启动组提交；
//! 7. 后台调度线程（ckpt_interval / ckpt_lsn_distance 触发，方案 §7）。
//!
//! checkpoint 流程（[`WalEngine::checkpoint`]，方案 §7）：
//! 冻结索引快照（读锁内 build）→ flush 屏障保证 [1..=applied_lsn] 全部
//! durable → 原子写 `ckpt_<seq>.bin`（tmp→fsync→rename→fsync dir）→
//! CKPT_ANCHOR strict 入组提交 → superblock 轮换。任一步失败不影响在线
//! 路径：孤儿 ckpt 文件在恢复时装载亦安全（其状态 ⊆ durable 终态）。
//!
//! 写路径：DATA/DELETE 记录经组提交入队（page cache 落位即更新内存索引，
//! read-your-writes），strict 模式或 flush 屏障保证 durable。读路径：索引
//! O(1) 定位 → 段内 pread → 帧 CRC 重算校验（可配置）。
//!
//! 恢复语义（I1/I2）：重放终态 == 已完整落盘记录序列的终态；strict 已
//! ack 的记录必然完整落盘，async 窗口内未 fsync 的记录允许丢失。
//!
//! Drop 顺序约定：先停调度线程并 join，再做停机 checkpoint（§7 条件 4，
//! best-effort），最后释放 core（commit 排空 + 最终 fsync 完成）后释放
//! flock，避免第二个写者在排空期间抢到锁。

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use crate::wal::checkpoint::{self, CkptData, CkptHeader, CkptSegEntry};
use crate::wal::commit::{
    CommitConfig, CommitError, CommitMode, CommitQueue, CommitReceipt, CommitRequest,
};
use crate::wal::frame::{
    compute_frame_crc, DataPayload, DeletePayload, GcMigratePayload, RecordType, VolumeMetaPayload,
    FRAME_HEADER_SIZE,
};
use crate::wal::gc::{spawn_gc_worker, GcConfig, GcOutcome};
use crate::wal::index::{IndexError, IndexStats, WalIndex, NEEDLE_FLAG_MIGRATED};
use crate::wal::manifest::{SegManifest, SegmentState};
use crate::wal::replay::{replay_all, replay_with, RecoveryMode, ReplayError};
use crate::wal::segment::{seg_file_name, SegWriter, SegmentError, SEG_HEADER_SIZE};
use crate::wal::superblock::{self, SbError, Superblock};

/// DELETE tombstone 默认保留期（方案 §5.3：7 天，保留期内可 restore）。
pub const DEFAULT_TOMBSTONE_RETENTION_SECS: i64 = 7 * 24 * 3600;

/// superblock state：卷满（free ≤ 0，§11 OutOfSpace）；0 = 正常。
pub const VOLUME_STATE_FULL: u8 = 1;

/// 逻辑占用字节（§9.3）：used + staging + pinned。garbage 不计入——
/// 它是可由 GC 回收的死空间；free = volume_size − occupied。
fn occupied_bytes(st: &IndexStats) -> u64 {
    st.used_bytes + st.staging_bytes + st.pinned_bytes
}

/// 引擎配置。
#[derive(Debug, Clone)]
pub struct WalEngineConfig {
    /// 段大小（字节）。
    pub seg_size: u64,
    /// 段头携带的 volume 标识（S7 接线时由 volume server 注入）。
    pub volume_id: u64,
    /// 段创建时是否 fallocate 预分配。
    pub preallocate: bool,
    /// 恢复模式（方案 §10；尾撕裂三档均截断，中部断链按档位处理）。
    pub recovery_mode: RecoveryMode,
    /// 读路径是否重算帧 CRC 校验。
    pub verify_on_read: bool,
    /// 组提交配置。
    pub commit: CommitConfig,
    /// checkpoint 调度间隔（§7 条件 2；None 关闭后台调度，仅手动触发）。
    pub ckpt_interval: Option<Duration>,
    /// checkpoint 触发的日志距离（§7 条件 1，单位：记录条数；默认值近似
    /// 平均 4KiB 记录下的 1GiB 日志量）。
    pub ckpt_lsn_distance: u64,
    /// 优雅停机前是否强制一次 checkpoint（§7 条件 4；崩溃注入测试关闭，
    /// 保持「crash = page cache 丢失」盘面语义）。
    pub ckpt_on_close: bool,
    /// 卷大小（写入 superblock，容量伸缩 T7 的持久化基线）。
    pub volume_size: u64,
    /// 段 GC 配置（§8；T6）。
    pub gc: GcConfig,
    /// DELETE tombstone 保留期（秒；§5.3 默认 7 天，测试/管理可调短）。
    pub tombstone_retention_secs: i64,
}

impl Default for WalEngineConfig {
    fn default() -> Self {
        WalEngineConfig {
            seg_size: 64 << 20,
            volume_id: 0,
            preallocate: true,
            recovery_mode: RecoveryMode::TolerateTail,
            verify_on_read: true,
            commit: CommitConfig::default(),
            ckpt_interval: Some(Duration::from_secs(60)),
            ckpt_lsn_distance: 262_144,
            ckpt_on_close: true,
            volume_size: 0,
            gc: GcConfig::default(),
            tombstone_retention_secs: DEFAULT_TOMBSTONE_RETENTION_SECS,
        }
    }
}

/// 引擎错误。
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("segment error: {0}")]
    Segment(#[from] SegmentError),
    #[error("replay error: {0}")]
    Replay(#[from] ReplayError),
    #[error("index error: {0}")]
    Index(#[from] IndexError),
    #[error("commit error: {0}")]
    Commit(#[from] CommitError),
    #[error("checkpoint error: {0}")]
    Ckpt(#[from] checkpoint::CkptError),
    #[error("superblock error: {0}")]
    Superblock(#[from] SbError),
    #[error("volume id mismatch: existing {sb}, got {got}")]
    VolumeIdMismatch { sb: u64, got: u64 },
    #[error("volume lock held by another writer: {0}")]
    Locked(PathBuf),
    #[error("needle {0} not found")]
    NotFound(u64),
    #[error("frame crc mismatch on read for needle {needle_id}")]
    ReadCorrupt { needle_id: u64 },
    #[error("record too large for segment: need {need} bytes, seg_size {seg_size}")]
    RecordTooLarge { need: usize, seg_size: u64 },
    #[error("shrink rejected: requested {requested} < used+staging+pinned {required}")]
    ResizeTooSmall { requested: u64, required: u64 },
    #[error("volume full: capacity {volume_size}, occupied {occupied}, write needs {need} bytes")]
    VolumeFull {
        volume_size: u64,
        occupied: u64,
        need: u64,
    },
}

/// 引擎统计。
#[derive(Debug, Clone, Copy, Default)]
pub struct EngineStats {
    pub index: IndexStats,
    /// 卷逻辑容量（0 = 未限定）。
    pub volume_size: u64,
    /// 逻辑可用字节（未限定时为 u64::MAX）。
    pub free_bytes: u64,
    pub durable_lsn: u64,
    pub flushed_lsn: u64,
    pub segments: usize,
    pub commit_records: u64,
    pub commit_syncs: u64,
    /// 最近一次 checkpoint 的 applied_lsn（无则 0）。
    pub last_ckpt_lsn: u64,
    /// 最近一次 checkpoint 的 seq（无则 0）。
    pub last_ckpt_seq: u64,
}

/// checkpoint 执行回执。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CkptOutcome {
    pub ckpt_seq: u64,
    pub applied_lsn: u64,
}

/// 卷目录 flock 守卫（fd 存活期间持有排他锁，Drop 即释放）。
struct VolumeLock {
    _file: nix::fcntl::Flock<File>,
}

fn acquire_volume_lock(dir: &Path) -> Result<VolumeLock, EngineError> {
    use nix::fcntl::{Flock, FlockArg};
    let path = dir.join("volume.lock");
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .read(true)
        .open(&path)
        .map_err(EngineError::Io)?;
    match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
        Ok(locked) => Ok(VolumeLock { _file: locked }),
        Err(_) => Err(EngineError::Locked(path)),
    }
}

/// fsync 目录项（新建段文件后调用，保证崩溃后目录项可见）。
pub(super) fn fsync_dir(dir: &Path) -> std::io::Result<()> {
    File::open(dir)?.sync_all()
}

fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// 段 sink：组提交层与 SegWriter 的桥接，含段满自动换段。
///
/// 追加经 `Mutex<SegWriter>` 串行化（与队列锁共同保证单写者语义）；
/// sync 通过独立克隆的 fd 执行，不阻塞追加（两阶段流水）。换段时先
/// seal 旧段（数据 fsync + sealed 位回写），再开新段并 fsync 目录项。
pub(super) struct SegSink {
    writer: Mutex<SegWriter>,
    /// 当前 writer 底层 fd 的克隆（换段时同步替换）；sync 持读锁执行，
    /// 与追加互斥解耦。
    sync_file: RwLock<File>,
    /// 段清单（GC 候选扫描/整段回收共用；锁序约定 manifest → index）。
    pub(super) manifest: Mutex<SegManifest>,
    dir: PathBuf,
    volume_id: u64,
    seg_size: u64,
    preallocate: bool,
}

impl SegSink {
    fn new(
        writer: SegWriter,
        manifest: SegManifest,
        config: &WalEngineConfig,
    ) -> Result<Self, EngineError> {
        let dir = manifest.dir().to_path_buf();
        let volume_id = writer.volume_id();
        let sync_file = writer.try_clone_file()?;
        Ok(SegSink {
            writer: Mutex::new(writer),
            sync_file: RwLock::new(sync_file),
            manifest: Mutex::new(manifest),
            dir,
            volume_id,
            seg_size: config.seg_size,
            preallocate: config.preallocate,
        })
    }

    /// 封旧段 → 开新段（base_lsn = 首条待写记录的 lsn）→ 登记清单 →
    /// fsync 目录项 → 替换 writer 与 sync fd。
    fn roll(&self, w: &mut SegWriter, next_lsn: u64) -> Result<(), EngineError> {
        w.seal()?;
        let seg_id = self.manifest.lock().unwrap().next_seg_id();
        let new_w = SegWriter::create(
            &self.dir.join(seg_file_name(seg_id)),
            seg_id,
            self.volume_id,
            next_lsn,
            self.seg_size,
            self.preallocate,
        )?;
        self.manifest
            .lock()
            .unwrap()
            .register(seg_id, next_lsn, self.volume_id);
        *self.sync_file.write().unwrap() = new_w.try_clone_file()?;
        fsync_dir(&self.dir)?;
        *w = new_w;
        Ok(())
    }
}

impl crate::wal::commit::CommitSink for SegSink {
    fn append(
        &self,
        lsn: u64,
        rtype: RecordType,
        flags: u8,
        payload: &[u8],
    ) -> std::io::Result<crate::wal::commit::Placement> {
        use crate::wal::commit::Placement;
        let mut guard = self.writer.lock().unwrap();
        loop {
            match guard.append(rtype, flags, lsn, payload) {
                Ok(meta) => {
                    return Ok(Placement {
                        seg_id: guard.seg_id(),
                        offset: meta.offset,
                        crc: meta.crc,
                        payload_len: meta.payload_len,
                    });
                }
                Err(SegmentError::SegmentFull { .. }) => {
                    if guard.write_pos() == SEG_HEADER_SIZE as u64 {
                        // 全新段仍放不下：记录超过段容量，不可恢复。
                        return Err(std::io::Error::other(format!(
                            "record of {} bytes exceeds segment capacity {}",
                            FRAME_HEADER_SIZE + payload.len(),
                            self.seg_size
                        )));
                    }
                    self.roll(&mut guard, lsn)
                        .map_err(|e| std::io::Error::other(e.to_string()))?;
                }
                Err(e) => return Err(std::io::Error::other(e.to_string())),
            }
        }
    }

    /// 持久化屏障：经克隆 fd 执行 fsync，不阻塞追加。
    fn sync(&self) -> std::io::Result<()> {
        self.sync_file.read().unwrap().sync_data()
    }
}

/// checkpoint 段清单条目构造（方案 §4.4 segments 区）。
///
/// live/staging/dead 字节分桶与 per-seg 最大记录 lsn 均由索引推导；
/// 索引未涉及的段（空段）记 0 / base_lsn。
fn build_seg_entries(manifest: &SegManifest, index: &WalIndex) -> Vec<CkptSegEntry> {
    use crate::wal::checkpoint::{per_seg_bytes, SEG_STATE_ACTIVE, SEG_STATE_SEALED};
    let buckets = per_seg_bytes(index);
    let mut last_lsn: HashMap<u64, u64> = HashMap::new();
    for n in index.needles() {
        let m = last_lsn.entry(n.seg_id).or_insert(0);
        *m = (*m).max(n.version_lsn);
    }
    for t in index.tombstones() {
        let m = last_lsn.entry(t.seg_id).or_insert(0);
        *m = (*m).max(t.version_lsn);
    }
    for d in index.dead_copies() {
        let m = last_lsn.entry(d.seg_id).or_insert(0);
        *m = (*m).max(d.version_lsn);
    }
    manifest
        .segments()
        .iter()
        .map(|e| {
            let (live, staging, dead) = buckets.get(&e.seg_id).copied().unwrap_or((0, 0, 0));
            CkptSegEntry {
                seg_id: e.seg_id,
                state: if e.state == SegmentState::Active {
                    SEG_STATE_ACTIVE
                } else {
                    SEG_STATE_SEALED
                },
                live_bytes: live,
                staging_bytes: staging,
                dead_bytes: dead,
                base_lsn: e.base_lsn,
                last_lsn: last_lsn.get(&e.seg_id).copied().unwrap_or(e.base_lsn),
            }
        })
        .collect()
}

/// 引擎共享核心。调度线程（ckpt / GC）持有 `Arc` 克隆，`WalEngine` Drop
/// 时先停线程。字段对 wal 模块内子模块（gc.rs）可见；锁序全局约定：
/// commit 队列锁 → sink.writer → sink.manifest → index，任何嵌套获取都
/// 不得逆序（checkpoint/GC 的临界区按此方向取锁）。
pub(super) struct EngineCore {
    pub(super) dir: PathBuf,
    pub(super) config: WalEngineConfig,
    pub(super) sink: Arc<SegSink>,
    pub(super) commit: CommitQueue,
    pub(super) index: RwLock<WalIndex>,
    next_needle_id: AtomicU64,
    /// 下一个 checkpoint seq（open 时从既有文件接续）。
    next_ckpt_seq: AtomicU64,
    /// superblock 轮换 seq（单写者 flock 保证无并发写）。
    sb_seq: AtomicU64,
    /// 卷首次创建时间（superblock 沿用）。
    sb_created_ts: i64,
    /// 最近 checkpoint 的 applied_lsn（调度 distance 基线）。
    last_ckpt_lsn: AtomicU64,
    /// 最近 checkpoint 的 seq（无则 0）。
    last_ckpt_seq: AtomicU64,
    /// 最近 checkpoint 时间（unix ms，调度 interval 基线）。
    last_ckpt_ms: AtomicU64,
    /// 后台线程停止标志（ckpt / GC 共用）。
    pub(super) stop: AtomicBool,
    /// Drop 顺序约定：最后释放——commit 排空 + 最终 fsync 后 flock 才
    /// 释放。
    _volume_lock: VolumeLock,
}

impl EngineCore {
    /// superblock 轮换写入（seq 单调 +1，active_seg_id 取当前活跃段）。
    fn store_superblock(&self, latest_ckpt_seq: u64) -> Result<(), EngineError> {
        let seq = self.sb_seq.fetch_add(1, Ordering::SeqCst) + 1;
        let (volume_size, state) = {
            let idx = self.index.read().unwrap();
            let cap = idx.volume_size();
            let full = cap > 0 && occupied_bytes(&idx.stats()) >= cap;
            (cap, if full { VOLUME_STATE_FULL } else { 0 })
        };
        let sb = Superblock {
            seq,
            volume_id: self.config.volume_id,
            latest_ckpt_seq,
            active_seg_id: self.sink.writer.lock().unwrap().seg_id(),
            active_seg_size: self.config.seg_size,
            volume_size,
            state,
            created_ts: self.sb_created_ts,
            last_mount_ts: chrono::Utc::now().timestamp(),
            min_live_snapshot_lsn: 0, // P3 快照接入
        };
        superblock::store(&self.dir, &sb)?;
        Ok(())
    }

    /// checkpoint 执行（方案 §7 流程）。手动触发与后台调度共用。
    fn checkpoint_impl(&self) -> Result<CkptOutcome, EngineError> {
        // writer/manifest 信息先行（锁序 writer → manifest → index 与写
        // 路径一致，无环）。
        let (active_seg_id, next_seg_id) = {
            let w = self.sink.writer.lock().unwrap();
            let m = self.sink.manifest.lock().unwrap();
            (w.seg_id(), m.next_seg_id())
        };
        let ckpt_seq = self.next_ckpt_seq.fetch_add(1, Ordering::SeqCst);

        // 1. 短临界区：冻结索引快照 + 段清单分桶 + 重放边界（锁序
        //    manifest → index，与全局约定一致）。
        let (data, apply_lsn) = {
            let manifest = self.sink.manifest.lock().unwrap();
            let idx = self.index.read().unwrap();
            let apply_lsn = idx.last_lsn();
            let segs = build_seg_entries(&manifest, &idx);
            let header = CkptHeader {
                ckpt_seq,
                applied_lsn: apply_lsn,
                volume_id: self.config.volume_id,
                active_seg_id,
                next_seg_id,
                next_needle_id: self.next_needle_id.load(Ordering::SeqCst),
                next_snapshot_id: 1, // P3 快照表留位
                created_ts: chrono::Utc::now().timestamp(),
            };
            (checkpoint::build(header, segs, &idx), apply_lsn)
        };

        // 2. 屏障：快照覆盖的 [1..=apply_lsn] 全部 durable（ckpt 状态 ⊆
        //    durable 终态——I1/I2 的 checkpoint 侧）。
        self.commit.flush_barrier(apply_lsn)?;

        // 3. 原子写 ckpt 文件（失败只留下代 seq 空洞与可能的 tmp，无害）。
        checkpoint::write(&self.dir, &data)?;

        // 4. CKPT_ANCHOR strict 入组提交（恢复重放可见；durable 后才轮换
        //    superblock），并记录到内存索引。
        let mut payload = Vec::with_capacity(16);
        payload.extend_from_slice(&ckpt_seq.to_le_bytes());
        payload.extend_from_slice(&apply_lsn.to_le_bytes());
        let receipt = self
            .commit
            .enqueue(CommitRequest::new(RecordType::CkptAnchor, &payload).strict())?;
        self.index
            .write()
            .unwrap()
            .apply_ckpt_anchor(ckpt_seq, apply_lsn, receipt.lsn);

        // 5. superblock 轮换（anchor durable 后执行）。
        self.store_superblock(ckpt_seq)?;

        self.last_ckpt_lsn.store(apply_lsn, Ordering::SeqCst);
        self.last_ckpt_seq.store(ckpt_seq, Ordering::SeqCst);
        self.last_ckpt_ms.store(unix_millis(), Ordering::SeqCst);
        Ok(CkptOutcome {
            ckpt_seq,
            applied_lsn: apply_lsn,
        })
    }

    /// GC 搬移条件写（§8 case B）：向活跃段写入一条 GC_MIGRATE 记录，
    /// 在线索引仅当 needle 当前版本 == expect_version 且无更新的
    /// tombstone/purge 标记时应用。
    ///
    /// 并发正确性：搬移「读条目 → 重写」之间存在用户覆写/删除窗口。
    /// 普通 DATA 记录无法承载条件（其 lsn 一定更晚，会以旧数据覆盖新
    /// 用户更新），故搬移使用独立记录类型自包含 expect_version：
    /// - 在线 apply：版本不符/已有更新 tombstone → 跳过（用户胜出），
    ///   盘面记录成为无主副本随段 GC 回收；
    /// - 重放 apply（[`WalIndex::apply_gc_migrate`]）：needle 缺席且无
    ///   tombstone/purge 时应用——原段被 case A 删除后，本记录是该
    ///   needle 唯一可重放副本，据此重建条目，重放终态与在线一致。
    pub(super) fn migrate_write(
        &self,
        needle_id: u64,
        expect_version: u64,
        data: &[u8],
    ) -> Result<(CommitReceipt, bool), EngineError> {
        let need = FRAME_HEADER_SIZE + GcMigratePayload::FIXED + data.len();
        if need as u64 > self.config.seg_size - SEG_HEADER_SIZE as u64 {
            return Err(EngineError::RecordTooLarge {
                need,
                seg_size: self.config.seg_size,
            });
        }
        let payload = GcMigratePayload {
            needle_id,
            expect_version,
            data: bytes::Bytes::copy_from_slice(data),
        };
        let mut buf = Vec::with_capacity(GcMigratePayload::FIXED + data.len());
        payload.encode(&mut buf);

        let receipt = self
            .commit
            .enqueue(CommitRequest::new(RecordType::GcMigrate, &buf))?;
        let now = chrono::Utc::now().timestamp();
        let applied = self.index.write().unwrap().apply_gc_migrate(
            receipt.placement.seg_id,
            receipt.placement.offset,
            receipt.placement.crc,
            receipt.lsn,
            &buf,
            now,
        )?;
        Ok((receipt, applied))
    }

    /// tombstone 过期 purge 持久化（§8 case C）：向活跃段写入
    /// TOMB_PURGE 标记（durable 后旧副本所在段方可安全回收）。
    ///
    /// 标记必要性：DELETE 记录所在段可能先于含更早 DATA/GC_MIGRATE 副本
    /// 的段被 case A 回收；重放失去 DELETE 后孤儿副本会让已删 needle
    /// 复活。purge 标记以更大 lsn 驻留日志并由 checkpoint 持久化，重放
    /// 时压制该 needle 一切更早记录；当所有 base_lsn ≤ purge_lsn 的段都
    /// 已回收（不可能再有更老孤儿），标记可由 GC 清除。
    pub(super) fn purge_write(
        &self,
        needle_id: u64,
        delete_lsn: u64,
    ) -> Result<CommitReceipt, EngineError> {
        let mut buf = Vec::with_capacity(16);
        buf.extend_from_slice(&needle_id.to_le_bytes());
        buf.extend_from_slice(&delete_lsn.to_le_bytes());
        let receipt = self
            .commit
            .enqueue(CommitRequest::new(RecordType::TombPurge, &buf).strict())?;
        self.index
            .write()
            .unwrap()
            .apply_tomb_purge(receipt.lsn, &buf)?;
        Ok(receipt)
    }

    /// 容量伸缩（§11.1 VOLUME_META）：
    /// - shrink 前置校验 new_size ≥ used+staging+pinned（pinned 计入，
    ///   否则快照钉住的旧版本会被挤出盘）；不足返回 ResizeTooSmall。
    /// - grow 直接生效。VOLUME_META strict 组提交并 apply 后立即轮换
    ///   superblock（容量是 GC 回收 META 记录段时的持久化基线，不能只等
    ///   下次 ckpt 轮换）。
    pub fn resize_impl(&self, new_size: u64) -> Result<CommitReceipt, EngineError> {
        let required = occupied_bytes(&self.index.read().unwrap().stats());
        if new_size < required {
            return Err(EngineError::ResizeTooSmall {
                requested: new_size,
                required,
            });
        }
        let buf = VolumeMetaPayload::volume_size(new_size);
        let receipt = self
            .commit
            .enqueue(CommitRequest::new(RecordType::VolumeMeta, &buf).strict())?;
        let applied = self
            .index
            .write()
            .unwrap()
            .apply_volume_meta(receipt.lsn, &buf)?;
        debug_assert_eq!(applied, new_size);
        // superblock 立即同步容量与 Full 状态。
        self.store_superblock(self.last_ckpt_seq.load(Ordering::SeqCst))?;
        Ok(receipt)
    }
}

/// checkpoint 后台调度（§7 条件 1/2）：轮询 distance 与 interval，满足
/// 任一且有未 checkpoint 的写入即执行。失败仅告警，不影响在线写路径。
fn spawn_ckpt_worker(core: Arc<EngineCore>) -> std::thread::JoinHandle<()> {
    let interval = core.config.ckpt_interval.unwrap_or(Duration::from_secs(60));
    let poll = interval
        .min(Duration::from_secs(1))
        .max(Duration::from_millis(10));
    std::thread::Builder::new()
        .name("wal-ckpt".into())
        .spawn(move || {
            while !core.stop.load(Ordering::SeqCst) {
                std::thread::sleep(poll);
                if core.stop.load(Ordering::SeqCst) {
                    return;
                }
                let durable = core.commit.durable_lsn();
                let last = core.last_ckpt_lsn.load(Ordering::SeqCst);
                if durable <= last {
                    continue;
                }
                let dist_due = durable - last > core.config.ckpt_lsn_distance;
                let interval_due = unix_millis()
                    .saturating_sub(core.last_ckpt_ms.load(Ordering::SeqCst))
                    >= interval.as_millis() as u64;
                if dist_due || interval_due {
                    if let Err(e) = core.checkpoint_impl() {
                        log::warn!("wal: background checkpoint failed: {e}");
                    }
                }
            }
        })
        .expect("spawn wal ckpt worker")
}

/// WAL 引擎（v2）。单卷单实例；卷级 flock 保证同一时刻至多一个写者。
pub struct WalEngine {
    core: Arc<EngineCore>,
    /// 后台线程（ckpt 调度 / GC 扫描），Drop 时统一停止并 join。
    workers: Mutex<Vec<std::thread::JoinHandle<()>>>,
}

impl WalEngine {
    /// 装配引擎（方案 §10）：加锁 → superblock → ckpt 装载 → 重放 →
    /// 开活跃段/新段 → 挂载 superblock → 启动组提交与调度。
    pub fn open(dir: &Path, config: WalEngineConfig) -> Result<Self, EngineError> {
        std::fs::create_dir_all(dir).map_err(EngineError::Io)?;
        let volume_lock = acquire_volume_lock(dir)?;

        // ---- superblock：加载（双副本取合法最大 seq）或首次初始化 ----
        let now_ts = chrono::Utc::now().timestamp();
        let sb = match superblock::load(dir) {
            Ok(sb) => {
                if sb.volume_id != 0 && sb.volume_id != config.volume_id {
                    return Err(EngineError::VolumeIdMismatch {
                        sb: sb.volume_id,
                        got: config.volume_id,
                    });
                }
                sb
            }
            Err(SbError::NotFound { .. }) => Superblock {
                seq: 0,
                volume_id: config.volume_id,
                latest_ckpt_seq: 0,
                active_seg_id: 0,
                active_seg_size: config.seg_size,
                volume_size: config.volume_size,
                state: 0,
                created_ts: now_ts,
                last_mount_ts: now_ts,
                min_live_snapshot_lsn: 0,
            },
            Err(e) => return Err(EngineError::Superblock(e)), // 双损坏：拒绝挂载
        };

        // ---- checkpoint 装载：从最高 seq 逐级回退（§10 步骤 2）----
        // 写入前已 flush 屏障，任何完整落盘的 ckpt 状态 ⊆ durable 终态，
        // 因此装载不依赖 superblock 指针（ckpt 写后 anchor/superblock 未
        // 及轮换的崩溃窗口同样安全），按目录扫描取最高可用 seq。
        let mut ckpt_applied_lsn = 0u64;
        let mut loaded_ckpt_seq = 0u64;
        let mut next_needle_floor = 0u64;
        let mut index = WalIndex::new();
        let ckpt_seqs = checkpoint::list_ckpts(dir);
        if let Some(latest) = ckpt_seqs.last() {
            let mut loaded: Option<(u64, CkptData)> = None;
            for seq in ckpt_seqs.iter().rev() {
                match checkpoint::load(dir, *seq) {
                    Ok(data) => {
                        loaded = Some((*seq, data));
                        break;
                    }
                    Err(e) => log::warn!(
                        "wal: checkpoint {} unusable, falling back: {e}",
                        checkpoint::ckpt_file_name(*seq)
                    ),
                }
            }
            match loaded {
                Some((seq, data)) => {
                    let hdr = data.header.expect("loaded ckpt header");
                    if hdr.volume_id != config.volume_id {
                        return Err(EngineError::VolumeIdMismatch {
                            sb: hdr.volume_id,
                            got: config.volume_id,
                        });
                    }
                    ckpt_applied_lsn = hdr.applied_lsn;
                    loaded_ckpt_seq = seq;
                    next_needle_floor = hdr.next_needle_id;
                    index = checkpoint::into_index(&data);
                    index.advance_replay_cursor(ckpt_applied_lsn);
                    let _ = latest;
                }
                None => {
                    log::warn!(
                        "wal: all {} checkpoint files unusable, falling back to full replay",
                        ckpt_seqs.len()
                    );
                }
            }
        }

        // ---- 段清单 + 重放（§10 步骤 4：前缀由 ckpt 承载）----
        let mut manifest = SegManifest::load(dir, config.seg_size)?;
        let mut replay = replay_all(
            dir,
            &manifest,
            index,
            config.recovery_mode,
            ckpt_applied_lsn,
        )?;

        // 中部断链且已装载的 ckpt 锚点超前于恢复点：ckpt 索引引用了将被
        // 弃置的段，必须丢弃 ckpt 全量重放，并物理失效超前 ckpt 文件
        // （否则下次启动会重新装载超前状态）。
        let mut ckpt_seq_floor = 0u64;
        if replay.stop.is_some() && ckpt_applied_lsn > 0 {
            let forced = replay.stop;
            log::warn!(
                "wal: checkpoint {} anchored beyond mid-chain recovery point; \
                 discarding and falling back to full replay",
                loaded_ckpt_seq
            );
            // 首轮已物理截断损坏段：把停点传入全量重放，否则损坏证据消失，
            // 全量扫描会越过恢复点继续重放后继段。
            replay = replay_with(
                dir,
                &manifest,
                WalIndex::new(),
                config.recovery_mode,
                0,
                forced,
            )?;
            for seq in checkpoint::remove_future_ckpts(dir, replay.index.last_lsn()) {
                log::warn!("wal: removed post-recovery checkpoint seq {seq}");
                ckpt_seq_floor = ckpt_seq_floor.max(seq);
            }
            ckpt_applied_lsn = 0;
            loaded_ckpt_seq = checkpoint::latest_seq(dir).unwrap_or(0);
        }

        // 尾撕裂告警（三档均截断恢复，仅告警级别不同；中部停点段除外，
        // 由下方统一报告）。
        let stop_seg = replay.stop.map(|s| s.seg_id);
        for (&seg_id, &offset) in &replay.truncated {
            if Some(seg_id) == stop_seg {
                continue;
            }
            match config.recovery_mode {
                RecoveryMode::TolerateTail => {
                    log::info!("wal: torn tail truncated on seg {seg_id} at offset {offset}")
                }
                RecoveryMode::PointInTime => {
                    log::warn!("wal: torn tail truncated on seg {seg_id} at offset {offset}")
                }
                RecoveryMode::Absolute => log::error!(
                    "wal: torn tail truncated on seg {seg_id} at offset {offset} \
                     (absolute mode: incomplete write tolerated, any mid-log \
                     failure would reject mount)"
                ),
            }
        }

        // 中部断链：报告停点 → 弃置后继段（删文件 + fsync 目录 + 清单移除）
        // → 停点段物理封段（reopen+seal 幂等）。
        if let Some(sp) = replay.stop {
            log::error!(
                "wal: replay stopped at last complete record: seg {} offset {} \
                 reason {:?}; {} subsequent segment(s) abandoned",
                sp.seg_id,
                sp.offset,
                sp.reason,
                replay.abandoned.len()
            );
            for &seg_id in &replay.abandoned {
                let path = dir.join(seg_file_name(seg_id));
                if let Err(e) = std::fs::remove_file(&path) {
                    log::warn!("wal: failed to remove abandoned seg {seg_id}: {e}");
                }
                manifest.remove(seg_id);
            }
            fsync_dir(dir)?;
            let (mut sealer, _) =
                SegWriter::reopen(&manifest.seg_path(sp.seg_id), config.seg_size)?;
            sealer.seal()?;
            manifest.mark_sealed(sp.seg_id);
        }

        // 卷容量基线：VOLUME_META 记录 > checkpoint/superblock 装载值；
        // 均未设置（0）时回退本次挂载 config.volume_size。
        let mut index = replay.index;
        if index.volume_size() == 0 {
            let baseline = if sb.volume_size != 0 {
                sb.volume_size
            } else {
                config.volume_size
            };
            index.set_volume_size(baseline);
        }

        // 活跃段：中部停点后按 §10 步骤 5 封旧开新；否则重开既有活跃段
        // （哈希链从扫描摘要恢复）；无活跃段则开新段。
        let writer = if replay.stop.is_some() {
            let seg_id = manifest.next_seg_id();
            let base_lsn = index.last_lsn() + 1;
            let w = SegWriter::create(
                &dir.join(seg_file_name(seg_id)),
                seg_id,
                config.volume_id,
                base_lsn,
                config.seg_size,
                config.preallocate,
            )?;
            fsync_dir(dir)?;
            manifest.register(seg_id, base_lsn, config.volume_id);
            w
        } else {
            match manifest.active() {
                Some(entry) => {
                    let (w, _summary) =
                        SegWriter::reopen(&manifest.seg_path(entry.seg_id), config.seg_size)?;
                    w
                }
                None => {
                    let seg_id = manifest.next_seg_id();
                    let base_lsn = index.last_lsn() + 1;
                    let w = SegWriter::create(
                        &dir.join(seg_file_name(seg_id)),
                        seg_id,
                        config.volume_id,
                        base_lsn,
                        config.seg_size,
                        config.preallocate,
                    )?;
                    fsync_dir(dir)?;
                    // 新段登记进清单：后续换段依赖 next_seg_id() 接续。
                    manifest.register(seg_id, base_lsn, config.volume_id);
                    w
                }
            }
        };

        let next_needle = (index.max_needle_id() + 1).max(next_needle_floor);
        let next_lsn = index.last_lsn() + 1;
        let sink = Arc::new(SegSink::new(writer, manifest, &config)?);
        let commit = CommitQueue::new(sink.clone(), config.commit, next_lsn);

        let core = Arc::new(EngineCore {
            dir: dir.to_path_buf(),
            config,
            sink,
            commit,
            index: RwLock::new(index),
            next_needle_id: AtomicU64::new(next_needle),
            next_ckpt_seq: AtomicU64::new(loaded_ckpt_seq.max(ckpt_seq_floor) + 1),
            sb_seq: AtomicU64::new(sb.seq),
            sb_created_ts: sb.created_ts,
            last_ckpt_lsn: AtomicU64::new(ckpt_applied_lsn),
            last_ckpt_seq: AtomicU64::new(loaded_ckpt_seq),
            last_ckpt_ms: AtomicU64::new(unix_millis()),
            stop: AtomicBool::new(false),
            _volume_lock: volume_lock,
        });

        // ---- 挂载 superblock（§10 步骤 2 的回退重写：latest_ckpt_seq 对
        //      齐实际装载的 ckpt；新卷写首份）----
        core.store_superblock(loaded_ckpt_seq)?;

        // ---- 后台调度（ckpt §7 条件 1/2；GC §8；None 则仅手动触发）----
        let mut workers = Vec::new();
        if core.config.ckpt_interval.is_some() {
            workers.push(spawn_ckpt_worker(core.clone()));
        }
        if core.config.gc.interval.is_some() {
            workers.push(spawn_gc_worker(core.clone()));
        }

        Ok(WalEngine {
            core,
            workers: Mutex::new(workers),
        })
    }

    /// 手动触发一次 checkpoint（§7 条件 3）。
    pub fn checkpoint(&self) -> Result<CkptOutcome, EngineError> {
        self.core.checkpoint_impl()
    }

    /// 手动执行一轮 GC（§8：purge + 搬移 + 整段回收）。与后台调度共用
    /// 同一实现；失败返回错误由调用方处置。
    pub fn gc(&self) -> Result<GcOutcome, EngineError> {
        crate::wal::gc::run_gc_cycle(&self.core, &self.core.config.gc)
    }

    /// 容量伸缩（§11.1）：grow 直接生效；shrink 不满足
    /// new_size ≥ used+staging+pinned 时返回 ResizeTooSmall。
    /// VOLUME_META strict 落盘并同步 superblock 后返回。
    pub fn resize(&self, new_size: u64) -> Result<CommitReceipt, EngineError> {
        self.core.resize_impl(new_size)
    }

    /// 当前卷逻辑容量（字节；0 = 未限定）。
    pub fn volume_size(&self) -> u64 {
        self.core.index.read().unwrap().volume_size()
    }

    /// 逻辑可用字节：volume_size − (used+staging+pinned)；未限定容量
    /// （0）返回 u64::MAX。
    pub fn free_bytes(&self) -> u64 {
        let idx = self.core.index.read().unwrap();
        match idx.volume_size() {
            0 => u64::MAX,
            cap => cap.saturating_sub(occupied_bytes(&idx.stats())),
        }
    }

    /// 卷是否处于 Full 状态（free ≤ 0；删除经 purge/GC 释放后恢复）。
    pub fn is_full(&self) -> bool {
        let idx = self.core.index.read().unwrap();
        let cap = idx.volume_size();
        cap > 0 && occupied_bytes(&idx.stats()) >= cap
    }

    /// 单个 needle 的索引条目（适配层 read_needle_meta 用）。
    pub fn needle_entry(&self, needle_id: u64) -> Option<crate::wal::index::NeedleEntry> {
        self.core.index.read().unwrap().lookup(needle_id).cloned()
    }

    /// 全部活跃 needle 的索引条目（适配层 list/scrub 用）。
    pub fn needle_entries(&self) -> Vec<crate::wal::index::NeedleEntry> {
        self.core.index.read().unwrap().needles().cloned().collect()
    }

    /// I4 断言：索引统计与内容严格一致（GC/测试/scrub 校验用）。
    pub fn assert_index_consistent(&self) {
        self.core.index.read().unwrap().assert_consistent()
    }

    pub fn dir(&self) -> &Path {
        &self.core.dir
    }

    /// 分配一个新 needle id（从重放终态 max+1 起单调递增）。
    pub fn alloc_needle_id(&self) -> u64 {
        self.core.next_needle_id.fetch_add(1, Ordering::Relaxed)
    }

    /// 下一个待分配 needle id（不推进计数器）。
    pub fn next_needle_id(&self) -> u64 {
        self.core.next_needle_id.load(Ordering::Relaxed)
    }

    /// 写入/覆写一个 needle。返回回执（strict 模式下 durable=true）。
    ///
    /// 幂等去重（同 id+长度+checksum 跳过）在 enqueue 前执行——由 S7
    /// 接线 write-predict-dedup 时在此处衔接。
    pub fn write(
        &self,
        needle_id: u64,
        data: &[u8],
        mode: CommitMode,
    ) -> Result<CommitReceipt, EngineError> {
        let need = FRAME_HEADER_SIZE + 12 + data.len();
        if need as u64 > self.core.config.seg_size - SEG_HEADER_SIZE as u64 {
            return Err(EngineError::RecordTooLarge {
                need,
                seg_size: self.core.config.seg_size,
            });
        }
        // 容量准入（§11 free ≤ 0 → Full）：cap=0 表示未限定。逻辑占用按
        // used+staging+pinned（garbage 由 GC 回收，不占用逻辑额度）；
        // 物理超卖由段分配器兜底。
        {
            let idx = self.core.index.read().unwrap();
            let cap = idx.volume_size();
            if cap > 0 {
                let occupied = occupied_bytes(&idx.stats());
                if occupied + data.len() as u64 > cap {
                    return Err(EngineError::VolumeFull {
                        volume_size: cap,
                        occupied,
                        need: data.len() as u64,
                    });
                }
            }
        }
        let payload = DataPayload {
            needle_id,
            data: bytes::Bytes::copy_from_slice(data),
        };
        let mut buf = Vec::with_capacity(12 + data.len());
        payload.encode(&mut buf);

        let mut req = CommitRequest::new(RecordType::Data, &buf);
        if matches!(mode, CommitMode::Strict) {
            req = req.strict();
        }
        let receipt = self.core.commit.enqueue(req)?;
        let now = chrono::Utc::now().timestamp();
        self.core.index.write().unwrap().apply_data(
            receipt.placement.seg_id,
            receipt.placement.offset,
            receipt.placement.crc,
            receipt.lsn,
            &buf,
            now,
        )?;
        Ok(receipt)
    }

    /// 删除一个 needle（tombstone，保留期内可 restore——restore API 在
    /// P2 快照接入时暴露）。
    pub fn delete(&self, needle_id: u64, mode: CommitMode) -> Result<CommitReceipt, EngineError> {
        {
            let index = self.core.index.read().unwrap();
            if index.lookup(needle_id).is_none() && index.tombstone_of(needle_id).is_none() {
                return Err(EngineError::NotFound(needle_id));
            }
        }
        let now = chrono::Utc::now().timestamp();
        let payload = DeletePayload {
            needle_id,
            deleted_at: now,
            retention_until: now + self.core.config.tombstone_retention_secs,
        };
        let mut buf = Vec::with_capacity(DeletePayload::SIZE);
        payload.encode(&mut buf);

        let mut req = CommitRequest::new(RecordType::Delete, &buf);
        if matches!(mode, CommitMode::Strict) {
            req = req.strict();
        }
        let receipt = self.core.commit.enqueue(req)?;
        self.core
            .index
            .write()
            .unwrap()
            .apply_delete(receipt.lsn, &buf)?;
        Ok(receipt)
    }

    /// 读取一个 needle 的数据（索引定位 → pread → 帧 CRC 校验）。
    ///
    /// ENOENT 重试：GC 删段与读并发时，「索引快照后、打开文件前」原段可
    /// 能被整段回收。此时重查索引重试一次——条目已被搬移更新则读新位置
    /// 成功；条目未变却 ENOENT 意味着索引与盘面不一致，按错误返回。
    pub fn read(&self, needle_id: u64) -> Result<Vec<u8>, EngineError> {
        for attempt in 0..2 {
            let entry = {
                let index = self.core.index.read().unwrap();
                match index.lookup(needle_id) {
                    Some(e) => (
                        e.seg_id,
                        e.offset,
                        e.data_len,
                        e.crc,
                        e.version_lsn,
                        e.flags,
                    ),
                    None => return Err(EngineError::NotFound(needle_id)),
                }
            };
            let (seg_id, offset, data_len, crc, version_lsn, flags) = entry;
            let path = self.core.dir.join(seg_file_name(seg_id));
            let mut f = match File::open(&path) {
                Ok(f) => f,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound && attempt == 0 => {
                    continue; // GC 删段窗口：重查索引重试
                }
                Err(e) => return Err(EngineError::Io(e)),
            };
            f.seek(SeekFrom::Start(offset)).map_err(EngineError::Io)?;

            // 帧形态分派：DATA 帧 payload 为 needle_id|data_len|data（12B
            // 头）；GC_MIGRATE 帧 payload 为
            // needle_id|expect_version|data_len|data（20B 头）。CRC 按各
            // 自 rtype 重算比对（盘面即真相）。
            let migrated = flags & NEEDLE_FLAG_MIGRATED != 0;
            let header_len = if migrated { 20usize } else { 12usize };
            let rt = if migrated {
                RecordType::GcMigrate
            } else {
                RecordType::Data
            };

            let payload_len = header_len + data_len as usize;
            let mut payload = vec![0u8; payload_len];
            match f.read_exact(&mut payload) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof && attempt == 0 => {
                    continue; // 段被截断/删除窗口：重查索引重试
                }
                Err(e) => return Err(EngineError::Io(e)),
            }

            if self.core.config.verify_on_read {
                let got = compute_frame_crc(rt.to_u8(), 0, version_lsn, &payload);
                if got != crc {
                    return Err(EngineError::ReadCorrupt { needle_id });
                }
            }
            return Ok(payload[header_len..].to_vec());
        }
        Err(EngineError::NotFound(needle_id))
    }

    /// 持久化屏障：等待 `durable_lsn >= min_lsn`（FlushNeedles 语义锚点）。
    pub fn flush(&self, min_lsn: u64) -> Result<u64, EngineError> {
        Ok(self.core.commit.flush_barrier(min_lsn)?)
    }

    /// 屏障当前全部已入队记录。
    pub fn flush_all(&self) -> Result<u64, EngineError> {
        let target = self.core.commit.flushed_lsn();
        self.flush(target)
    }

    pub fn durable_lsn(&self) -> u64 {
        self.core.commit.durable_lsn()
    }

    pub fn flushed_lsn(&self) -> u64 {
        self.core.commit.flushed_lsn()
    }

    pub fn stats(&self) -> EngineStats {
        let index = self.core.index.read().unwrap();
        let cs = self.core.commit.stats();
        let cap = index.volume_size();
        let free = match cap {
            0 => u64::MAX,
            c => c.saturating_sub(occupied_bytes(&index.stats())),
        };
        EngineStats {
            index: index.stats(),
            volume_size: cap,
            free_bytes: free,
            durable_lsn: self.core.commit.durable_lsn(),
            flushed_lsn: self.core.commit.flushed_lsn(),
            segments: self.core.sink.manifest.lock().unwrap().segments().len(),
            commit_records: cs.records,
            commit_syncs: cs.syncs,
            last_ckpt_lsn: self.core.last_ckpt_lsn.load(Ordering::SeqCst),
            last_ckpt_seq: self.core.last_ckpt_seq.load(Ordering::SeqCst),
        }
    }

    /// 优雅关闭：排空组提交并最终 fsync（Drop 等价）。
    pub fn close(self) {
        drop(self);
    }
}

impl Drop for WalEngine {
    fn drop(&mut self) {
        // 1. 停后台线程（ckpt / GC）并等待其在途工作完成。
        self.core.stop.store(true, Ordering::SeqCst);
        for h in self.workers.lock().unwrap().drain(..) {
            let _ = h.join();
        }
        // 2. 优雅停机前强制一次（§7 条件 4，best-effort）：失败仅损失
        //    下次挂载的重放加速，不损失数据（组提交排空保证 durable）。
        if self.core.config.ckpt_on_close {
            if let Err(e) = self.core.checkpoint_impl() {
                log::warn!("wal: final checkpoint before close failed: {e}");
            }
        }
        // 3. core 的 Arc 随字段释放：commit 排空 + 最终 fsync 完成
        //    （_volume_lock 声明在最后）后 flock 才释放。
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

    fn open_default(dir: &Path) -> WalEngine {
        WalEngine::open(dir, WalEngineConfig::default()).expect("engine open")
    }

    /// 完全手动模式：关闭后台调度与停机 checkpoint，ckpt 只由显式
    /// checkpoint() 产生（测试内行为确定性）。
    fn manual_ckpt_cfg() -> WalEngineConfig {
        WalEngineConfig {
            ckpt_interval: None,
            ckpt_on_close: false,
            ..Default::default()
        }
    }

    fn no_sync_cfg() -> WalEngineConfig {
        WalEngineConfig {
            commit: CommitConfig {
                async_interval: std::time::Duration::from_secs(3600),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// 段的帧结束偏移表（lsn → 扫描到该帧后的 valid_end）。
    fn frame_ends(dir: &Path, seg_id: u64) -> Vec<(u64, u64)> {
        let mut r = crate::wal::segment::SegReader::open(&dir.join(seg_file_name(seg_id))).unwrap();
        let mut ends = Vec::new();
        while let Some(f) = r.next_frame().unwrap() {
            ends.push((f.meta.lsn, r.valid_end()));
        }
        ends
    }

    #[test]
    fn write_read_delete_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let eng = open_default(dir.path());

        let id = eng.alloc_needle_id();
        assert_eq!(id, 1);
        let r = eng
            .write(id, b"hello-needle-1", CommitMode::Strict)
            .unwrap();
        assert!(r.durable);
        assert_eq!(eng.read(id).unwrap(), b"hello-needle-1");

        // 覆写：读新版本，旧版本计垃圾。
        let r2 = eng
            .write(id, b"hello-needle-1-overwritten-longer", CommitMode::Async)
            .unwrap();
        assert!(!r2.durable);
        eng.flush_all().unwrap();
        assert_eq!(eng.read(id).unwrap(), b"hello-needle-1-overwritten-longer");

        let st = eng.stats();
        assert_eq!(st.index.active_count, 1);
        assert_eq!(st.index.garbage_bytes, b"hello-needle-1".len() as u64);
        assert_eq!(
            st.index.used_bytes,
            b"hello-needle-1-overwritten-longer".len() as u64
        );

        // 删除：读 ENOENT，统计迁移。
        let id2 = eng.alloc_needle_id();
        eng.write(id2, b"to-be-deleted", CommitMode::Strict)
            .unwrap();
        eng.delete(id2, CommitMode::Strict).unwrap();
        assert!(matches!(eng.read(id2), Err(EngineError::NotFound(2))));
        let st = eng.stats();
        assert_eq!(st.index.active_count, 1);
        assert_eq!(st.index.deleted_count, 1);
        assert_eq!(
            st.index.used_bytes,
            b"hello-needle-1-overwritten-longer".len() as u64
        );

        // 删除不存在 id：入队前拒绝。
        assert!(matches!(
            eng.delete(999, CommitMode::Async),
            Err(EngineError::NotFound(999))
        ));

        // strict 同步覆盖了之前的 async 记录：durable_lsn = 4 条全部记录。
        assert_eq!(eng.durable_lsn(), 4);
    }

    #[test]
    fn reopen_preserves_state_and_continues() {
        let dir = tempfile::tempdir().unwrap();
        // 关闭停机 ckpt：本测试验证纯重放路径的终态与 LSN 接续；
        // ckpt 装载路径由 T4 专项测试覆盖。
        let cfg = WalEngineConfig {
            ckpt_on_close: false,
            ..Default::default()
        };
        {
            let eng = WalEngine::open(dir.path(), cfg.clone()).unwrap();
            for i in 0..10u64 {
                eng.write(i + 1, format!("data-{i}").as_bytes(), CommitMode::Strict)
                    .unwrap();
            }
            eng.delete(3, CommitMode::Strict).unwrap();
        }

        // 重开：重放终态一致，LSN / needle id 接续。
        let eng = WalEngine::open(dir.path(), cfg.clone()).unwrap();
        assert_eq!(eng.stats().index.active_count, 9);
        for i in 0..10u64 {
            if i + 1 == 3 {
                assert!(matches!(eng.read(i + 1), Err(EngineError::NotFound(3))));
            } else {
                assert_eq!(eng.read(i + 1).unwrap(), format!("data-{i}").into_bytes());
            }
        }
        assert_eq!(eng.alloc_needle_id(), 11);
        // 10 write + 1 delete = 11 条记录 → next_lsn = 12。
        assert_eq!(eng.flushed_lsn(), 11);
        assert_eq!(eng.durable_lsn(), 11);

        // 续写：LSN 单调，重放可见。
        eng.write(11, b"post-restart", CommitMode::Strict).unwrap();
        assert_eq!(eng.read(11).unwrap(), b"post-restart");
        drop(eng);

        let eng2 = WalEngine::open(dir.path(), cfg).unwrap();
        assert_eq!(eng2.read(11).unwrap(), b"post-restart");
        assert_eq!(eng2.stats().index.active_count, 10);
    }

    /// kill -9 式崩溃（帧中）模拟：截断到最后一个 durable 帧 + 追加撕裂
    /// 半帧 → 重启恢复 == 已 ack（strict/durable）操作的重放结果（I1/I2）。
    #[test]
    fn crash_mid_frame_recovers_to_durable_prefix() {
        let dir = tempfile::tempdir().unwrap();
        {
            let eng = WalEngine::open(
                dir.path(),
                WalEngineConfig {
                    ckpt_on_close: false,
                    ..no_sync_cfg()
                },
            )
            .unwrap();
            // strict：ack 即 durable（r1..r5）。
            for i in 0..5u64 {
                eng.write(i + 1, format!("durable-{i}").as_bytes(), CommitMode::Strict)
                    .unwrap();
            }
            assert_eq!(eng.durable_lsn(), 5);
            // async 窗口：accepted 但未 durable（r6..r10）。
            for i in 5..10u64 {
                eng.write(i + 1, format!("window-{i}").as_bytes(), CommitMode::Async)
                    .unwrap();
            }
            assert_eq!(eng.durable_lsn(), 5);
            assert_eq!(eng.flushed_lsn(), 10);
            // Drop 触发最终 fsync；随后把盘面重建为"crash 时"状态 =
            // durable 前缀 + 撕裂半帧（等价于 page cache 丢失）。
        }
        let seg1 = dir.path().join(seg_file_name(1));
        let ends = frame_ends(dir.path(), 1);
        let lsn5_end = ends.iter().find(|(l, _)| *l == 5).unwrap().1;

        {
            let mut f = OpenOptions::new().write(true).open(&seg1).unwrap();
            f.set_len(lsn5_end).unwrap();
            // 撕裂半帧：非零残头（全零会被识别为预分配 slack）。
            f.seek(SeekFrom::Start(lsn5_end)).unwrap();
            f.write_all(&[0xaa_u8; 10]).unwrap();
            f.sync_data().unwrap();
        }

        // 重启恢复：恰好等于 strict ack 的 5 条记录。
        let eng = open_default(dir.path());
        assert_eq!(eng.stats().index.active_count, 5);
        for i in 0..5u64 {
            assert_eq!(
                eng.read(i + 1).unwrap(),
                format!("durable-{i}").into_bytes()
            );
        }
        for i in 5..10u64 {
            assert!(matches!(eng.read(i + 1), Err(EngineError::NotFound(_))));
        }
        // LSN / needle id 接续：新写入从 6 开始。
        assert_eq!(eng.alloc_needle_id(), 6);
        eng.write(6, b"post-crash", CommitMode::Strict).unwrap();
        assert_eq!(eng.read(6).unwrap(), b"post-crash");
        drop(eng);

        // 再次重启：恢复状态稳定（重放幂等 + 续写可见）。
        let eng2 = open_default(dir.path());
        assert_eq!(eng2.stats().index.active_count, 6);
        assert_eq!(eng2.read(6).unwrap(), b"post-crash");
    }

    #[test]
    fn flock_excludes_second_open() {
        let dir = tempfile::tempdir().unwrap();
        let eng = open_default(dir.path());
        match WalEngine::open(dir.path(), WalEngineConfig::default()) {
            Err(EngineError::Locked(p)) => {
                assert!(p.ends_with("volume.lock"));
            }
            Err(e) => panic!("expected Locked, got {e}"),
            Ok(_) => panic!("expected Locked, second open succeeded"),
        }
        drop(eng);
        let eng2 = open_default(dir.path());
        drop(eng2);
    }

    #[test]
    fn rolls_multiple_segments_and_recovers() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = WalEngineConfig {
            seg_size: 512,
            commit: CommitConfig {
                async_interval: std::time::Duration::from_secs(3600),
                ..Default::default()
            },
            ..Default::default()
        };
        {
            let eng = WalEngine::open(dir.path(), cfg.clone()).unwrap();
            for i in 0..20u64 {
                eng.write(i + 1, &[i as u8; 40], CommitMode::Strict)
                    .unwrap();
            }
            let st = eng.stats();
            assert!(
                st.segments > 1,
                "小段容量应触发换段：segments={}",
                st.segments
            );
            eng.flush_all().unwrap();
        }

        let eng = open_default(dir.path());
        assert!(eng.stats().segments > 1);
        for i in 0..20u64 {
            assert_eq!(
                eng.read(i + 1).unwrap(),
                vec![i as u8; 40],
                "needle {}",
                i + 1
            );
        }
        // 重开后活跃段续写。
        eng.write(21, &[0xff; 40], CommitMode::Strict).unwrap();
        assert_eq!(eng.read(21).unwrap(), vec![0xff; 40]);
    }

    #[test]
    fn flush_barrier_makes_async_durable() {
        let dir = tempfile::tempdir().unwrap();
        let eng = WalEngine::open(
            dir.path(),
            WalEngineConfig {
                ckpt_on_close: false,
                ..no_sync_cfg()
            },
        )
        .unwrap();
        for i in 0..5u64 {
            let r = eng.write(i + 1, b"async", CommitMode::Async).unwrap();
            assert!(!r.durable);
        }
        assert_eq!(eng.durable_lsn(), 0);
        eng.flush_all().unwrap();
        assert_eq!(eng.durable_lsn(), 5);
        assert!(eng.flush(3).is_ok(), "已 durable 的屏障走快路径");
        for i in 0..5u64 {
            assert_eq!(eng.read(i + 1).unwrap(), b"async");
        }
    }

    #[test]
    fn concurrent_writes_all_visible_and_consistent() {
        let dir = tempfile::tempdir().unwrap();
        let eng = Arc::new(open_default(dir.path()));
        const THREADS: usize = 8;
        const PER: usize = 20;

        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let eng = eng.clone();
                std::thread::spawn(move || {
                    let mut ids = Vec::with_capacity(PER);
                    for i in 0..PER {
                        let id = eng.alloc_needle_id();
                        let data = format!("t{t}-i{i:03}-{}", "y".repeat(30));
                        eng.write(id, data.as_bytes(), CommitMode::Strict).unwrap();
                        ids.push((id, data));
                    }
                    ids
                })
            })
            .collect();

        let mut all = Vec::new();
        for h in handles {
            all.extend(h.join().unwrap());
        }
        assert_eq!(all.len(), THREADS * PER);
        for (id, data) in &all {
            assert_eq!(&eng.read(*id).unwrap(), data.as_bytes(), "needle {id}");
        }
        let st = eng.stats();
        assert_eq!(st.index.active_count, (THREADS * PER) as u64);
        assert_eq!(
            st.index.used_bytes,
            all.iter().map(|(_, d)| d.len() as u64).sum::<u64>()
        );
    }

    /// 在位损坏（引擎存活期间位翻转）：读路径 CRC 重算检出。
    /// （重放阶段同样会拒绝此类损坏，这里专门验证 verify_on_read 路径。）
    #[test]
    fn read_detects_in_place_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let eng = open_default(dir.path());
        eng.write(1, b"payload-to-corrupt", CommitMode::Strict)
            .unwrap();

        // 定位 needle 1 的 payload 起点并翻转一个字节。
        let seg1 = dir.path().join(seg_file_name(1));
        let mut r = crate::wal::segment::SegReader::open(&seg1).unwrap();
        let f = r.next_frame().unwrap().unwrap();
        let corrupt_at = f.meta.offset + FRAME_HEADER_SIZE as u64 + 12;
        drop(r);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&seg1)
            .unwrap();
        file.seek(SeekFrom::Start(corrupt_at)).unwrap();
        let mut b = [0u8; 1];
        file.read_exact(&mut b).unwrap();
        file.seek(SeekFrom::Start(corrupt_at)).unwrap();
        b[0] ^= 0x01;
        file.write_all(&b).unwrap();
        drop(file);

        assert!(matches!(
            eng.read(1),
            Err(EngineError::ReadCorrupt { needle_id: 1 })
        ));
    }

    /// 记录超过段容量：入队前拒绝。
    #[test]
    fn record_too_large_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let eng = WalEngine::open(
            dir.path(),
            WalEngineConfig {
                seg_size: 4096,
                ..Default::default()
            },
        )
        .unwrap();
        let big = vec![0u8; 8192];
        assert!(matches!(
            eng.write(1, &big, CommitMode::Async),
            Err(EngineError::RecordTooLarge { .. })
        ));
    }

    // ---------------- P2.4：checkpoint 调度与恢复接线（C.1 T4）----------------

    /// checkpoint 写盘 → 重开装载：索引终态/统计/游标全部一致。
    #[test]
    fn checkpoint_reopen_state_consistent() {
        let dir = tempfile::tempdir().unwrap();
        {
            let eng = WalEngine::open(dir.path(), manual_ckpt_cfg()).unwrap();
            for i in 0..10u64 {
                eng.write(i + 1, format!("data-{i}").as_bytes(), CommitMode::Strict)
                    .unwrap();
            }
            eng.delete(3, CommitMode::Strict).unwrap();

            let out = eng.checkpoint().unwrap();
            assert_eq!(out.ckpt_seq, 1);
            assert_eq!(out.applied_lsn, 11); // 10 write + 1 delete
            assert!(dir.path().join(checkpoint::ckpt_file_name(1)).exists());
            let st = eng.stats();
            assert_eq!(st.last_ckpt_seq, 1);
            assert_eq!(st.last_ckpt_lsn, 11);
        }

        // 重开：从 ckpt 装载（前缀跳过）+ anchor 重放，终态一致。
        let eng = WalEngine::open(dir.path(), manual_ckpt_cfg()).unwrap();
        let st = eng.stats();
        assert_eq!(st.index.active_count, 9);
        assert_eq!(st.index.deleted_count, 1);
        assert_eq!(st.last_ckpt_seq, 1);
        assert_eq!(st.last_ckpt_lsn, 11);
        for i in 0..10u64 {
            if i + 1 == 3 {
                assert!(matches!(eng.read(i + 1), Err(EngineError::NotFound(3))));
            } else {
                assert_eq!(eng.read(i + 1).unwrap(), format!("data-{i}").into_bytes());
            }
        }
        // LSN / needle id 接续；anchor（ckpt 后第一条记录）经重放可见。
        assert_eq!(eng.alloc_needle_id(), 11);
        assert_eq!(eng.flushed_lsn(), 12);

        // 续写落在重放游标之后，LSN 全局单调。
        eng.write(11, b"post-ckpt", CommitMode::Strict).unwrap();
        assert_eq!(eng.read(11).unwrap(), b"post-ckpt");
    }

    /// checkpoint 后继续写入 → 重开：ckpt 状态 + 增量重放 = 全量重放终态。
    #[test]
    fn writes_after_checkpoint_recover() {
        let dir = tempfile::tempdir().unwrap();
        {
            let eng = WalEngine::open(dir.path(), manual_ckpt_cfg()).unwrap();
            for i in 0..5u64 {
                eng.write(i + 1, format!("pre-{i}").as_bytes(), CommitMode::Strict)
                    .unwrap();
            }
            eng.checkpoint().unwrap();
            for i in 5..8u64 {
                eng.write(i + 1, format!("post-{i}").as_bytes(), CommitMode::Strict)
                    .unwrap();
            }
            eng.delete(2, CommitMode::Strict).unwrap();
        }
        let eng = WalEngine::open(dir.path(), manual_ckpt_cfg()).unwrap();
        assert_eq!(eng.stats().index.active_count, 7);
        assert_eq!(eng.stats().index.deleted_count, 1);
        for i in 0..8u64 {
            let expect = if i < 5 {
                format!("pre-{i}")
            } else {
                format!("post-{i}")
            };
            if i + 1 == 2 {
                assert!(matches!(eng.read(i + 1), Err(EngineError::NotFound(2))));
            } else {
                assert_eq!(eng.read(i + 1).unwrap(), expect.into_bytes());
            }
        }
        assert_eq!(eng.alloc_needle_id(), 9);
    }

    /// 最新 checkpoint 文件损坏 → 逐级回退装载上一代 + 重放增量。
    #[test]
    fn corrupt_latest_ckpt_falls_back_to_previous() {
        let dir = tempfile::tempdir().unwrap();
        {
            let eng = WalEngine::open(dir.path(), manual_ckpt_cfg()).unwrap();
            for i in 0..5u64 {
                eng.write(i + 1, format!("c1-{i}").as_bytes(), CommitMode::Strict)
                    .unwrap();
            }
            eng.checkpoint().unwrap(); // seq 1
            for i in 5..10u64 {
                eng.write(i + 1, format!("c2-{i}").as_bytes(), CommitMode::Strict)
                    .unwrap();
            }
            eng.checkpoint().unwrap(); // seq 2（将被破坏的对象）
        }
        let latest = dir.path().join(checkpoint::ckpt_file_name(2));
        assert!(latest.exists());
        let mut raw = std::fs::read(&latest).unwrap();
        let mid = raw.len() / 2;
        raw[mid] ^= 0x01;
        std::fs::write(&latest, &raw).unwrap();

        // 重开：seq 2 损坏告警并回退 seq 1，重放其后的全部记录，终态一致。
        let eng = WalEngine::open(dir.path(), manual_ckpt_cfg()).unwrap();
        assert_eq!(eng.stats().index.active_count, 10);
        for i in 0..10u64 {
            let expect = if i < 5 {
                format!("c1-{i}")
            } else {
                format!("c2-{i}")
            };
            assert_eq!(eng.read(i + 1).unwrap(), expect.into_bytes());
        }
    }

    /// 崩溃窗口：ckpt 文件已完整落盘、superblock 未及轮换（或丢失）——
    /// 恢复按目录扫描 ckpt 装载，不依赖 superblock 的 ckpt 指针。
    #[test]
    fn ckpt_recovered_without_superblock_pointer() {
        let dir = tempfile::tempdir().unwrap();
        {
            let eng = WalEngine::open(dir.path(), manual_ckpt_cfg()).unwrap();
            for i in 0..5u64 {
                eng.write(i + 1, format!("win-{i}").as_bytes(), CommitMode::Strict)
                    .unwrap();
            }
            eng.checkpoint().unwrap();
            for i in 5..8u64 {
                eng.write(i + 1, format!("tail-{i}").as_bytes(), CommitMode::Strict)
                    .unwrap();
            }
        }
        // 删除 superblock 双副本（比 seq 回退更强的窗口模拟）。
        std::fs::remove_file(dir.path().join("superblock.a")).unwrap();
        std::fs::remove_file(dir.path().join("superblock.b")).unwrap();

        let eng = WalEngine::open(dir.path(), manual_ckpt_cfg()).unwrap();
        assert_eq!(eng.stats().index.active_count, 8);
        for i in 0..8u64 {
            let expect = if i < 5 {
                format!("win-{i}")
            } else {
                format!("tail-{i}")
            };
            assert_eq!(eng.read(i + 1).unwrap(), expect.into_bytes());
        }
    }

    /// 后台调度：interval 到期且有未 checkpoint 的写入 → 自动 checkpoint。
    #[test]
    fn background_scheduler_triggers_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = WalEngineConfig {
            ckpt_interval: Some(Duration::from_millis(50)),
            ..Default::default()
        };
        {
            let eng = WalEngine::open(dir.path(), cfg).unwrap();
            eng.write(1, b"sched", CommitMode::Strict).unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while eng.stats().last_ckpt_seq == 0 {
                assert!(
                    std::time::Instant::now() < deadline,
                    "调度线程未在期限内触发 checkpoint"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
            assert!(eng.stats().last_ckpt_lsn >= 1);
            assert!(dir
                .path()
                .join(checkpoint::ckpt_file_name(eng.stats().last_ckpt_seq))
                .exists());
        }
        // 调度产物在重开时可用。
        let eng = open_default(dir.path());
        assert_eq!(eng.read(1).unwrap(), b"sched");
    }

    /// superblock 卷标识校验：volume_id 不一致拒绝挂载（防错挂卷目录）。
    #[test]
    fn volume_id_mismatch_rejected() {
        let dir = tempfile::tempdir().unwrap();
        {
            let eng = WalEngine::open(
                dir.path(),
                WalEngineConfig {
                    volume_id: 1,
                    ckpt_interval: None,
                    ..Default::default()
                },
            )
            .unwrap();
            eng.write(1, b"x", CommitMode::Strict).unwrap();
        }
        let err = match WalEngine::open(
            dir.path(),
            WalEngineConfig {
                volume_id: 2,
                ckpt_interval: None,
                ..Default::default()
            },
        ) {
            Err(e) => e,
            Ok(_) => panic!("expected VolumeIdMismatch, got Ok"),
        };
        assert!(matches!(
            err,
            EngineError::VolumeIdMismatch { sb: 1, got: 2 }
        ));
    }

    /// 停机 checkpoint（§7 条件 4）：close 后 ckpt 文件与 superblock 轮换
    /// 已发生，重开直接从 ckpt 装载。
    #[test]
    fn close_forces_final_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        {
            let eng = WalEngine::open(
                dir.path(),
                WalEngineConfig {
                    ckpt_interval: None, // 无后台调度：ckpt 只能来自停机
                    ..Default::default()
                },
            )
            .unwrap();
            eng.write(1, b"final", CommitMode::Strict).unwrap();
            assert_eq!(eng.stats().last_ckpt_seq, 0);
        }
        assert!(
            checkpoint::latest_seq(dir.path()).is_some(),
            "close 必须生成停机 checkpoint"
        );
        let eng = WalEngine::open(dir.path(), manual_ckpt_cfg()).unwrap();
        assert_eq!(eng.read(1).unwrap(), b"final");
        assert!(eng.stats().last_ckpt_seq >= 1);
    }

    /// ckpt 前缀跳过不弱化完整性：装载后继续校验 ckpt 覆盖段的帧 CRC/链
    /// （损坏段即使全部落在 skip 区间，Absolute 档也拒绝挂载）。
    #[test]
    fn skipped_prefix_still_integrity_checked() {
        let dir = tempfile::tempdir().unwrap();
        {
            let eng = WalEngine::open(dir.path(), manual_ckpt_cfg()).unwrap();
            eng.write(1, b"will-corrupt", CommitMode::Strict).unwrap();
            eng.checkpoint().unwrap();
        }
        // ckpt 覆盖了 needle 1（skip 区间），但段内位翻转仍必须在重放
        // 扫描时被检出（CRC 校验不因跳过 apply 而弱化）。
        let seg1 = dir.path().join(seg_file_name(1));
        let mut r = crate::wal::segment::SegReader::open(&seg1).unwrap();
        let f = r.next_frame().unwrap().unwrap();
        let corrupt_at = f.meta.offset + FRAME_HEADER_SIZE as u64 + 12;
        drop(r);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&seg1)
            .unwrap();
        file.seek(SeekFrom::Start(corrupt_at)).unwrap();
        let mut b = [0u8; 1];
        file.read_exact(&mut b).unwrap();
        file.seek(SeekFrom::Start(corrupt_at)).unwrap();
        b[0] ^= 0x01;
        file.write_all(&b).unwrap();
        drop(file);

        // Absolute：段损坏 → 重放拒绝（不影响 ckpt 前缀本身的有效性）。
        let cfg = WalEngineConfig {
            recovery_mode: RecoveryMode::Absolute,
            ..manual_ckpt_cfg()
        };
        assert!(matches!(
            WalEngine::open(dir.path(), cfg),
            Err(EngineError::Replay(ReplayError::Corrupt { .. }))
        ));
    }

    /// 中段损坏恢复矩阵：512B 段 6 条 100B 写入跨 2 段；损坏 seg1 的第 3
    /// 帧后，Absolute 拒载，TolerateTail/PointInTime 停在损坏前，seg2
    /// 弃置删除，停点段封段开新段，续写与再次重开均正常。
    #[test]
    fn mid_chain_corruption_recovery_matrix() {
        for mode in [RecoveryMode::TolerateTail, RecoveryMode::PointInTime] {
            let dir = tempfile::tempdir().unwrap();
            let cfg = WalEngineConfig {
                seg_size: 512,
                preallocate: false,
                recovery_mode: mode,
                ckpt_interval: None,
                ckpt_on_close: false,
                gc: GcConfig {
                    interval: None,
                    ..GcConfig::default()
                },
                ..Default::default()
            };
            {
                let eng = WalEngine::open(dir.path(), cfg.clone()).unwrap();
                for i in 1..=6u64 {
                    eng.write(i, &[0xa0 + i as u8; 100], CommitMode::Strict)
                        .unwrap();
                }
            }
            // 至少两个段，损坏点在非尾段。
            let mf = SegManifest::load(dir.path(), cfg.seg_size).unwrap();
            assert!(mf.segments().len() >= 2, "{}", mf.segments().len());
            assert!(dir.path().join(seg_file_name(2)).exists());
            corrupt_frame_payload(dir.path(), 1, 3);

            // 同一盘面 Absolute 必须拒载（只读，不改动文件）。
            let abs_cfg = WalEngineConfig {
                recovery_mode: RecoveryMode::Absolute,
                ..cfg.clone()
            };
            assert!(matches!(
                WalEngine::open(dir.path(), abs_cfg),
                Err(EngineError::Replay(ReplayError::Corrupt { .. }))
            ));

            // 容忍档：停点前缀可见，后继段记录全部消失。
            let eng = WalEngine::open(dir.path(), cfg.clone()).unwrap();
            assert_eq!(eng.read(1).unwrap(), vec![0xa1; 100]);
            assert_eq!(eng.read(2).unwrap(), vec![0xa2; 100]);
            for i in 3..=6u64 {
                assert!(
                    matches!(eng.read(i), Err(EngineError::NotFound(_))),
                    "needle {i} must be abandoned in mode {mode:?}"
                );
            }
            assert_eq!(eng.stats().index.active_count, 2);
            // 旧 seg2（w4-6）已弃置；manifest 只余停点段 seg1（sealed）+
            // 恢复后新挂的活跃段（复用 seg_id 2，base_lsn 接续 = 3）。
            let mf_after = SegManifest::load(dir.path(), cfg.seg_size).unwrap();
            let segs = mf_after.segments();
            assert_eq!(segs.len(), 2);
            assert_eq!(segs[0].seg_id, 1);
            assert_eq!(segs[0].state, SegmentState::Sealed);
            assert_eq!(segs[1].seg_id, 2);
            assert_eq!(segs[1].state, SegmentState::Active);
            assert_eq!(segs[1].base_lsn, 3);

            // 续写落在新段，LSN 接续单调。
            let nid = eng.alloc_needle_id();
            eng.write(nid, b"post-recovery", CommitMode::Strict)
                .unwrap();
            assert_eq!(eng.read(nid).unwrap(), b"post-recovery");
            let lsn_after = eng.flushed_lsn();
            drop(eng);

            // 再次重开：盘面干净、无停点、续写持久。
            let eng2 = WalEngine::open(dir.path(), cfg).unwrap();
            assert_eq!(eng2.read(1).unwrap(), vec![0xa1; 100]);
            assert_eq!(eng2.read(2).unwrap(), vec![0xa2; 100]);
            assert_eq!(eng2.read(nid).unwrap(), b"post-recovery");
            assert_eq!(eng2.flushed_lsn(), lsn_after);
        }
    }

    /// 中部损坏且已有 checkpoint 锚点超前于恢复点：装载的 ckpt 状态被
    /// 丢弃（全量重放），超前 ckpt 文件物理失效，后续重启不会复活。
    #[test]
    fn mid_chain_corruption_invalidates_future_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = || WalEngineConfig {
            seg_size: 512,
            preallocate: false,
            ckpt_interval: None,
            ckpt_on_close: false,
            gc: GcConfig {
                interval: None,
                ..GcConfig::default()
            },
            ..Default::default()
        };
        {
            let eng = WalEngine::open(dir.path(), cfg()).unwrap();
            for i in 1..=3u64 {
                eng.write(i, &[i as u8; 100], CommitMode::Strict).unwrap();
            }
            let out = eng.checkpoint().unwrap();
            assert_eq!(out.applied_lsn, 3);
            for i in 4..=6u64 {
                eng.write(i, &[i as u8; 100], CommitMode::Strict).unwrap();
            }
        }
        assert!(!checkpoint::list_ckpts(dir.path()).is_empty());
        corrupt_frame_payload(dir.path(), 1, 3);

        let eng = WalEngine::open(dir.path(), cfg()).unwrap();
        // 全量重放恢复点 = lsn 2：needle3（损坏帧）与后继段 4..6 全部丢弃。
        assert_eq!(eng.read(1).unwrap(), vec![1u8; 100]);
        assert_eq!(eng.read(2).unwrap(), vec![2u8; 100]);
        for i in 3..=6u64 {
            assert!(matches!(eng.read(i), Err(EngineError::NotFound(_))));
        }
        assert!(
            checkpoint::list_ckpts(dir.path()).is_empty(),
            "checkpoints anchored beyond recovery point must be removed"
        );
        assert_eq!(eng.stats().last_ckpt_seq, 0);
        drop(eng);

        // 再开：不会重新装载已失效的超前状态。
        let eng2 = WalEngine::open(dir.path(), cfg()).unwrap();
        assert!(matches!(eng2.read(3), Err(EngineError::NotFound(_))));
        // 新 checkpoint 可正常落盘（已删除文件的序号复用是安全的：
        // 写路径 tmp + 原子 rename，且删除已 fsync 目录）。
        let seq = eng2.checkpoint().unwrap().ckpt_seq;
        assert!(seq >= 1);
        drop(eng2);

        // 第三次重开：ckpt 装载 + 重放后状态仍是恢复点终态。
        let eng3 = WalEngine::open(dir.path(), cfg()).unwrap();
        assert!(matches!(eng3.read(3), Err(EngineError::NotFound(_))));
        assert_eq!(eng3.read(1).unwrap(), vec![1u8; 100]);
        assert_eq!(eng3.read(2).unwrap(), vec![2u8; 100]);
        assert!(eng3.stats().last_ckpt_seq >= 1);
    }

    /// 翻转指定段内指定 lsn 帧的 payload 一个字节（造成帧 CRC 失败，
    /// 哈希链前缀保持完整）。
    fn corrupt_frame_payload(dir: &Path, seg_id: u64, lsn: u64) {
        let path = dir.join(seg_file_name(seg_id));
        let mut r = crate::wal::segment::SegReader::open(&path).unwrap();
        let target = loop {
            match r.next_frame().unwrap() {
                Some(f) if f.meta.lsn == lsn => break f.meta.offset,
                Some(_) => continue,
                None => panic!("frame lsn {lsn} not found in seg {seg_id}"),
            }
        };
        drop(r);
        let at = target + FRAME_HEADER_SIZE as u64 + 16;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.seek(SeekFrom::Start(at)).unwrap();
        let mut b = [0u8; 1];
        file.read_exact(&mut b).unwrap();
        file.seek(SeekFrom::Start(at)).unwrap();
        b[0] ^= 0x01;
        file.write_all(&b).unwrap();
        file.sync_all().unwrap();
    }

    fn resize_cfg(seg: u64) -> WalEngineConfig {
        WalEngineConfig {
            seg_size: seg,
            preallocate: false,
            ckpt_interval: None,
            ckpt_on_close: false,
            tombstone_retention_secs: 0,
            gc: GcConfig {
                interval: None,
                ..GcConfig::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn resize_enforces_capacity_full_state_and_gc_release() {
        let dir = tempfile::tempdir().unwrap();
        let eng = WalEngine::open(dir.path(), resize_cfg(4096)).unwrap();

        // 未限定容量：free 以 u64::MAX 表达，非 Full。
        assert_eq!(eng.volume_size(), 0);
        assert_eq!(eng.free_bytes(), u64::MAX);
        assert!(!eng.is_full());

        eng.resize(300).unwrap();
        assert_eq!(eng.volume_size(), 300);
        for i in 1..=3u64 {
            eng.write(i, &[i as u8; 100], CommitMode::Strict).unwrap();
        }
        let st = eng.stats();
        assert_eq!(st.volume_size, 300);
        assert_eq!(st.free_bytes, 0);
        assert_eq!(st.index.used_bytes, 300);
        assert!(eng.is_full());
        // superblock 轮换点（checkpoint；resize 本身也是轮换点）同步
        // 容量与 Full 标记。
        eng.checkpoint().unwrap();
        let sb = superblock::load(dir.path()).unwrap();
        assert_eq!(sb.volume_size, 300);
        assert_eq!(sb.state, VOLUME_STATE_FULL);

        // 超出容量：写拒绝。
        assert!(matches!(
            eng.write(4, &[4u8; 100], CommitMode::Strict),
            Err(EngineError::VolumeFull {
                volume_size: 300,
                occupied: 300,
                need: 100,
            })
        ));

        // shrink 到占用以下：拒绝并带缺口信息；恰好等于占用允许。
        assert!(matches!(
            eng.resize(200),
            Err(EngineError::ResizeTooSmall {
                requested: 200,
                required: 300,
            })
        ));
        eng.resize(300).unwrap();

        // 删除后字节转入 staging（保留期占额度）：仍 Full；
        // purge/GC 把 staging 转成可回收 garbage 后逻辑空间释放。
        eng.delete(1, CommitMode::Strict).unwrap();
        assert!(eng.is_full());
        assert!(matches!(
            eng.write(4, &[4u8; 100], CommitMode::Strict),
            Err(EngineError::VolumeFull { .. })
        ));
        let out = eng.gc().unwrap();
        assert!(out.purged >= 1, "purged tombstones: {:?}", out);
        assert!(!eng.is_full());
        assert_eq!(eng.free_bytes(), 100);
        eng.write(4, &[4u8; 100], CommitMode::Strict).unwrap();
        assert!(eng.is_full());

        // grow 立即解除 Full，并同步 superblock state。
        eng.resize(1000).unwrap();
        assert_eq!(eng.volume_size(), 1000);
        assert_eq!(eng.free_bytes(), 700);
        assert!(!eng.is_full());
        let sb = superblock::load(dir.path()).unwrap();
        assert_eq!(sb.volume_size, 1000);
        assert_eq!(sb.state, 0);
    }

    #[test]
    fn resize_persists_via_record_replay_and_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = || resize_cfg(4096);

        // 路径 1：无 checkpoint，VOLUME_META 记录经全量重放恢复。
        {
            let eng = WalEngine::open(dir.path(), cfg()).unwrap();
            eng.resize(4321).unwrap();
            eng.write(1, &[1u8; 100], CommitMode::Strict).unwrap();
        }
        let eng = WalEngine::open(dir.path(), cfg()).unwrap();
        assert_eq!(eng.volume_size(), 4321);
        assert_eq!(eng.free_bytes(), 4221);

        // 路径 2：checkpoint alloc_stats 携带 volume_size。
        eng.resize(8888).unwrap();
        eng.checkpoint().unwrap();
        drop(eng);
        let eng = WalEngine::open(dir.path(), cfg()).unwrap();
        assert_eq!(eng.volume_size(), 8888);
        assert_eq!(eng.stats().last_ckpt_seq, 1);

        // 路径 3：从未 resize 时以 superblock/config 容量为基线。
        let dir2 = tempfile::tempdir().unwrap();
        let base = WalEngineConfig {
            volume_size: 111,
            ..resize_cfg(4096)
        };
        {
            let eng = WalEngine::open(dir2.path(), base.clone()).unwrap();
            assert_eq!(eng.volume_size(), 111);
            eng.write(1, &[1u8; 10], CommitMode::Strict).unwrap();
        }
        let eng = WalEngine::open(dir2.path(), base).unwrap();
        assert_eq!(eng.volume_size(), 111);
        assert_eq!(eng.free_bytes(), 101);
    }
}
