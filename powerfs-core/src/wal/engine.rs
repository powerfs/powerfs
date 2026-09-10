//! WalEngine：WAL 引擎装配与恢复（方案 §5 / §6 / 附录 B S5）。
//!
//! 装配流程（open）：
//! 1. 卷目录 flock 单写者锁（拒绝第二个写者）；
//! 2. 扫描段清单（SegManifest）；
//! 3. 重放全部段得到内存索引（tolerate_tail 截断最后段撕裂尾）；
//! 4. 重开活跃段（或无段/无活跃段时以 last_lsn+1 开新段）；
//! 5. 以重放 last_lsn+1 为起始 LSN 启动组提交队列（跨重启 LSN 单调）。
//!
//! 写路径：DATA/DELETE 记录经组提交入队（page cache 落位即更新内存索引，
//! read-your-writes），strict 模式或 flush 屏障保证 durable。读路径：索引
//! O(1) 定位 → 段内 pread → 帧 CRC 重算校验（可配置）。
//!
//! 恢复语义（I1/I2）：重放终态 == 已完整落盘记录序列的终态；strict 已
//! ack 的记录必然完整落盘，async 窗口内未 fsync 的记录允许丢失。
//!
//! Drop 顺序约定：`_volume_lock` 声明在最后——先排空组提交（最终 fsync
//! 完成），再释放 flock，避免第二个写者在排空期间抢到锁。

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::wal::commit::{
    CommitConfig, CommitError, CommitMode, CommitQueue, CommitReceipt, CommitRequest,
};
use crate::wal::frame::{
    compute_frame_crc, DataPayload, DeletePayload, RecordType, FRAME_HEADER_SIZE, RT_DATA,
};
use crate::wal::index::{IndexError, IndexStats, WalIndex};
use crate::wal::manifest::SegManifest;
use crate::wal::replay::{replay_all, ReplayError};
use crate::wal::segment::{seg_file_name, SegWriter, SegmentError, SEG_HEADER_SIZE};

/// DELETE tombstone 默认保留期（方案 §5.3：7 天，保留期内可 restore）。
const TOMBSTONE_RETENTION_SECS: i64 = 7 * 24 * 3600;

/// 引擎配置。
#[derive(Debug, Clone)]
pub struct WalEngineConfig {
    /// 段大小（字节）。
    pub seg_size: u64,
    /// 段头携带的 volume 标识（S7 接线时由 volume server 注入）。
    pub volume_id: u64,
    /// 段创建时是否 fallocate 预分配。
    pub preallocate: bool,
    /// 恢复时是否容忍最后段撕裂尾（截断到最后完整帧）。
    pub tolerate_tail: bool,
    /// 读路径是否重算帧 CRC 校验。
    pub verify_on_read: bool,
    /// 组提交配置。
    pub commit: CommitConfig,
}

impl Default for WalEngineConfig {
    fn default() -> Self {
        WalEngineConfig {
            seg_size: 64 << 20,
            volume_id: 0,
            preallocate: true,
            tolerate_tail: true,
            verify_on_read: true,
            commit: CommitConfig::default(),
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
    #[error("volume lock held by another writer: {0}")]
    Locked(PathBuf),
    #[error("needle {0} not found")]
    NotFound(u64),
    #[error("frame crc mismatch on read for needle {needle_id}")]
    ReadCorrupt { needle_id: u64 },
    #[error("record too large for segment: need {need} bytes, seg_size {seg_size}")]
    RecordTooLarge { need: usize, seg_size: u64 },
}

/// 引擎统计。
#[derive(Debug, Clone, Copy, Default)]
pub struct EngineStats {
    pub index: IndexStats,
    pub durable_lsn: u64,
    pub flushed_lsn: u64,
    pub segments: usize,
    pub commit_records: u64,
    pub commit_syncs: u64,
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
fn fsync_dir(dir: &Path) -> std::io::Result<()> {
    File::open(dir)?.sync_all()
}

/// 段 sink：组提交层与 SegWriter 的桥接，含段满自动换段。
///
/// 追加经 `Mutex<SegWriter>` 串行化（与队列锁共同保证单写者语义）；
/// sync 通过独立克隆的 fd 执行，不阻塞追加（两阶段流水）。换段时先
/// seal 旧段（数据 fsync + sealed 位回写），再开新段并 fsync 目录项。
struct SegSink {
    writer: Mutex<SegWriter>,
    /// 当前 writer 底层 fd 的克隆（换段时同步替换）；sync 持读锁执行，
    /// 与追加互斥解耦。
    sync_file: RwLock<File>,
    manifest: Mutex<SegManifest>,
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

/// WAL 引擎（v2）。单卷单实例；卷级 flock 保证同一时刻至多一个写者。
pub struct WalEngine {
    dir: PathBuf,
    config: WalEngineConfig,
    sink: Arc<SegSink>,
    commit: CommitQueue,
    index: RwLock<WalIndex>,
    next_needle_id: AtomicU64,
    _volume_lock: VolumeLock,
}

impl WalEngine {
    /// 装配引擎：加锁 → 清单 → 重放 → 开活跃段/新段 → 启动组提交。
    pub fn open(dir: &Path, config: WalEngineConfig) -> Result<Self, EngineError> {
        std::fs::create_dir_all(dir).map_err(EngineError::Io)?;
        let volume_lock = acquire_volume_lock(dir)?;

        let mut manifest = SegManifest::load(dir, config.seg_size)?;
        let replay = replay_all(dir, &manifest, config.tolerate_tail)?;
        let index = replay.index;

        // 活跃段：重开既有段（哈希链从扫描摘要恢复）；无活跃段则开新段。
        let writer = match manifest.active() {
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
        };

        let next_needle = index.max_needle_id() + 1;
        let next_lsn = index.last_lsn() + 1;
        let sink = Arc::new(SegSink::new(writer, manifest, &config)?);
        let commit = CommitQueue::new(sink.clone(), config.commit, next_lsn);

        Ok(WalEngine {
            dir: dir.to_path_buf(),
            config,
            sink,
            commit,
            index: RwLock::new(index),
            next_needle_id: AtomicU64::new(next_needle),
            _volume_lock: volume_lock,
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// 分配一个新 needle id（从重放终态 max+1 起单调递增）。
    pub fn alloc_needle_id(&self) -> u64 {
        self.next_needle_id.fetch_add(1, Ordering::Relaxed)
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
        if need as u64 > self.config.seg_size - SEG_HEADER_SIZE as u64 {
            return Err(EngineError::RecordTooLarge {
                need,
                seg_size: self.config.seg_size,
            });
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
        let receipt = self.commit.enqueue(req)?;
        let now = chrono::Utc::now().timestamp();
        self.index.write().unwrap().apply_data(
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
            let index = self.index.read().unwrap();
            if index.lookup(needle_id).is_none() && index.tombstone_of(needle_id).is_none() {
                return Err(EngineError::NotFound(needle_id));
            }
        }
        let now = chrono::Utc::now().timestamp();
        let payload = DeletePayload {
            needle_id,
            deleted_at: now,
            retention_until: now + TOMBSTONE_RETENTION_SECS,
        };
        let mut buf = Vec::with_capacity(DeletePayload::SIZE);
        payload.encode(&mut buf);

        let mut req = CommitRequest::new(RecordType::Delete, &buf);
        if matches!(mode, CommitMode::Strict) {
            req = req.strict();
        }
        let receipt = self.commit.enqueue(req)?;
        self.index
            .write()
            .unwrap()
            .apply_delete(receipt.lsn, &buf)?;
        Ok(receipt)
    }

    /// 读取一个 needle 的数据（索引定位 → pread → 帧 CRC 校验）。
    pub fn read(&self, needle_id: u64) -> Result<Vec<u8>, EngineError> {
        let entry = {
            let index = self.index.read().unwrap();
            match index.lookup(needle_id) {
                Some(e) => (e.seg_id, e.offset, e.data_len, e.crc, e.version_lsn),
                None => return Err(EngineError::NotFound(needle_id)),
            }
        };
        let (seg_id, offset, data_len, crc, version_lsn) = entry;
        let path = self.dir.join(seg_file_name(seg_id));
        let mut f = File::open(&path).map_err(EngineError::Io)?;
        f.seek(SeekFrom::Start(offset)).map_err(EngineError::Io)?;

        let payload_len = 12 + data_len as usize;
        let mut payload = vec![0u8; payload_len];
        f.read_exact(&mut payload).map_err(EngineError::Io)?;

        if self.config.verify_on_read {
            let got = compute_frame_crc(RT_DATA, 0, version_lsn, &payload);
            if got != crc {
                return Err(EngineError::ReadCorrupt { needle_id });
            }
        }
        Ok(payload[12..].to_vec())
    }

    /// 持久化屏障：等待 `durable_lsn >= min_lsn`（FlushNeedles 语义锚点）。
    pub fn flush(&self, min_lsn: u64) -> Result<u64, EngineError> {
        Ok(self.commit.flush_barrier(min_lsn)?)
    }

    /// 屏障当前全部已入队记录。
    pub fn flush_all(&self) -> Result<u64, EngineError> {
        let target = self.commit.flushed_lsn();
        self.flush(target)
    }

    pub fn durable_lsn(&self) -> u64 {
        self.commit.durable_lsn()
    }

    pub fn flushed_lsn(&self) -> u64 {
        self.commit.flushed_lsn()
    }

    pub fn stats(&self) -> EngineStats {
        let index = self.index.read().unwrap();
        let cs = self.commit.stats();
        EngineStats {
            index: index.stats(),
            durable_lsn: self.commit.durable_lsn(),
            flushed_lsn: self.commit.flushed_lsn(),
            segments: self.sink.manifest.lock().unwrap().segments().len(),
            commit_records: cs.records,
            commit_syncs: cs.syncs,
        }
    }

    /// 优雅关闭：排空组提交并最终 fsync（Drop 等价）。
    pub fn close(self) {
        drop(self);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

    fn open_default(dir: &Path) -> WalEngine {
        WalEngine::open(dir, WalEngineConfig::default()).expect("engine open")
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
        {
            let eng = open_default(dir.path());
            for i in 0..10u64 {
                eng.write(i + 1, format!("data-{i}").as_bytes(), CommitMode::Strict)
                    .unwrap();
            }
            eng.delete(3, CommitMode::Strict).unwrap();
        }

        // 重开：重放终态一致，LSN / needle id 接续。
        let eng = open_default(dir.path());
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

        let eng2 = open_default(dir.path());
        assert_eq!(eng2.read(11).unwrap(), b"post-restart");
        assert_eq!(eng2.stats().index.active_count, 10);
    }

    /// kill -9 式崩溃（帧中）模拟：截断到最后一个 durable 帧 + 追加撕裂
    /// 半帧 → 重启恢复 == 已 ack（strict/durable）操作的重放结果（I1/I2）。
    #[test]
    fn crash_mid_frame_recovers_to_durable_prefix() {
        let dir = tempfile::tempdir().unwrap();
        {
            let eng = WalEngine::open(dir.path(), no_sync_cfg()).unwrap();
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
        let eng = WalEngine::open(dir.path(), no_sync_cfg()).unwrap();
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
}
