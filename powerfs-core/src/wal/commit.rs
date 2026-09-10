//! WAL 组提交与 fsync 屏障（方案 §5 / 附录 B S4）。
//!
//! 写路径分层（对应方案 §5.2 流程，队列锁承担 leader 聚批角色）：
//!
//! - **入队（enqueue）**：写者在队列锁内完成 LSN 分配（全局单调，从 1
//!   起）并把记录追加到 [`CommitSink`]（落 OS page cache 即返回），同步
//!   推进 `flushed_lsn`。索引可见性与落盘位置在入队时即成立，读路径
//!   read-your-writes。
//! - **持久化（fsync 调度）**：独立 worker 线程按条件聚合同步——strict
//!   写者 / barrier 等待者 / 背压触顶 / async 间隔到期任一成立，先在锁内
//!   快照 `target = flushed_lsn` 再解锁执行 `sync`。因此 **fsync 进行中
//!   写者仍可入队**（两阶段流水：fsync 不阻塞入队，只阻塞下一轮 sync）；
//!   快照在锁内完成，保证 target 覆盖的所有记录 append 先于 sync 调用
//!   （mutex happens-before），durable 语义严格成立。
//! - **唤醒**：sync 完成后推进 `durable_lsn`、释放脏字节预算、唤醒整批
//!   等待者（leader/follower 摊薄 fsync：同一快照覆盖的所有等待者共享
//!   一轮 sync）。
//!
//! 语义分层（方案 §5.1）：
//! - [`CommitMode::Async`]（默认）：入队返回 accepted，崩溃丢失窗口 ≤
//!   `async_interval`；
//! - [`CommitMode::Strict`]：入队在记录 durable 后返回，丢失窗口 0；
//! - 帧 flags 携带 [`FLAG_SYNC_BARRIER`] 的记录永远提升为真正的持久化
//!   屏障（文件系统 fsync/release 语义的锚点），屏障标记原样写入日志帧。
//!
//! 背压：未 durable 字节超过 `max_dirty_bytes` 时入队阻塞，由 fsync 推进
//! 释放预算。sticky 错误：sink append/sync 失败后队列进入不可恢复状态，
//! 后续入队与等待全部返回错误（引擎层负责卷级处置）。

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use crate::wal::frame::{RecordType, FLAG_SYNC_BARRIER, FRAME_HEADER_SIZE};

/// sink 追加的物理落位（索引侧 read-your-writes 定位与校验用）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Placement {
    /// 记录所在段。
    pub seg_id: u64,
    /// 帧起始在段内的偏移（含段头前缀）。
    pub offset: u64,
    /// 帧 CRC（覆盖 rtype|flags|lsn|payload）。
    pub crc: u32,
    pub payload_len: u32,
}

/// 组提交的底层追加/持久化接口（由 WalEngine 基于 SegWriter + 换段实现）。
///
/// 契约：
/// - `append` 只要求落 OS page cache 即可返回（追加由队列锁串行化，调用方
///   之间不并发），并返回物理落位；
/// - `sync` 返回 `Ok` 时，**在 sync 调用之前完成的全部 append 必须已
///   durable**。队列保证"sync 调用之前"以锁内快照 `flushed_lsn` 为界。
pub trait CommitSink: Send + Sync {
    fn append(
        &self,
        lsn: u64,
        rtype: RecordType,
        flags: u8,
        payload: &[u8],
    ) -> std::io::Result<Placement>;
    /// 持久化屏障：本调用返回后，此前 append 的记录全部 durable。
    fn sync(&self) -> std::io::Result<()>;
}

/// 组提交配置。
#[derive(Debug, Clone, Copy)]
pub struct CommitConfig {
    /// async 模式的 fsync 聚合间隔（方案 §5.1 默认 5ms）。
    pub async_interval: Duration,
    /// 未 durable 字节预算，超过后入队阻塞（方案 §5.2 默认 64MiB）。
    pub max_dirty_bytes: u64,
}

impl Default for CommitConfig {
    fn default() -> Self {
        CommitConfig {
            async_interval: Duration::from_millis(5),
            max_dirty_bytes: 64 << 20,
        }
    }
}

/// 提交模式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitMode {
    /// accepted 即返回，持久化由 fsync 调度器按间隔/预算聚合。
    Async,
    /// 本记录 durable 后才返回。
    Strict,
}

/// 入队请求。
#[derive(Debug, Clone, Copy)]
pub struct CommitRequest<'a> {
    pub rtype: RecordType,
    /// 帧级 flags 原样写入日志；含 [`FLAG_SYNC_BARRIER`] 时本记录提升为
    /// 持久化屏障。
    pub flags: u8,
    pub payload: &'a [u8],
    pub mode: CommitMode,
}

impl<'a> CommitRequest<'a> {
    pub fn new(rtype: RecordType, payload: &'a [u8]) -> Self {
        CommitRequest {
            rtype,
            flags: 0,
            payload,
            mode: CommitMode::Async,
        }
    }

    /// strict：durable 后返回。
    pub fn strict(mut self) -> Self {
        self.mode = CommitMode::Strict;
        self
    }

    /// 置 F_SYNC_BARRIER：强制 fsync 屏障并等待 durable。
    pub fn barrier(mut self) -> Self {
        self.flags |= FLAG_SYNC_BARRIER;
        self
    }
}

/// 入队回执：`durable` 为 false 表示 accepted-but-not-durable（async 窗口）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitReceipt {
    pub lsn: u64,
    pub durable: bool,
    /// 本记录的物理落位。
    pub placement: Placement,
}

/// 组提交错误。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CommitError {
    #[error("payload too large: {got} bytes (max {})", u32::MAX)]
    PayloadTooLarge { got: usize },
    #[error("commit queue is shutting down")]
    ShuttingDown,
    #[error("sink failure: {0}")]
    Sink(String),
}

/// 组提交统计。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CommitStats {
    /// 已追加记录数。
    pub records: u64,
    /// 已追加记录的物理日志字节（含帧头）。
    pub bytes: u64,
    /// 已执行的 sink.sync 轮数。
    pub syncs: u64,
}

/// 一条已入队但未 durable 的记录（FIFO 与 lsn 同序）。
struct Undurable {
    lsn: u64,
    bytes: u64,
    enqueued_at: Instant,
}

struct CommitCore {
    config: CommitConfig,
    sink: Arc<dyn CommitSink>,
    /// 下一个待分配 lsn（从 1 起，0 表示无记录）。
    next_lsn: u64,
    /// 已 append 到 page cache 的最大 lsn。
    flushed_lsn: u64,
    /// 已 fsync 的最大 lsn。
    durable_lsn: u64,
    dirty_bytes: u64,
    undurable: VecDeque<Undurable>,
    /// 强制下一轮 sync（strict 入队 / barrier / close 置位）。
    kick: bool,
    /// (token, target)：barrier/被动等待者，target > durable 且 <= flushed
    /// 时构成即时 sync 条件。
    waiters: Vec<(u64, u64)>,
    shutting_down: bool,
    error: Option<CommitError>,
    stats: CommitStats,
}

struct Shared {
    core: Mutex<CommitCore>,
    /// worker 等待 sync 条件。
    sync_cv: Condvar,
    /// strict/barrier 等待 durable。
    durable_cv: Condvar,
    /// 背压等待预算释放。
    space_cv: Condvar,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, CommitCore> {
        self.core.lock().unwrap()
    }
}

/// 组提交队列：多写者入队，单 worker 聚合 fsync。
pub struct CommitQueue {
    shared: Arc<Shared>,
    worker: Mutex<Option<std::thread::JoinHandle<()>>>,
    closed: AtomicBool,
}

static NEXT_WAITER_TOKEN: AtomicU64 = AtomicU64::new(1);

/// 等待者注销守卫。声明顺序约定：`_guard` 先于 core guard 声明，析构时
/// core 先释放、guard 再重新加锁清理（避免同线程重入死锁）。
struct WaitDereg<'a> {
    q: &'a CommitQueue,
    token: u64,
}

impl Drop for WaitDereg<'_> {
    fn drop(&mut self) {
        let mut core = self.q.shared.lock();
        core.waiters.retain(|&(t, _)| t != self.token);
    }
}

impl CommitQueue {
    /// 创建队列并启动 fsync worker 线程。`next_lsn` 为起始 LSN（恢复场景
    /// 传入重放 last_lsn + 1，保证跨重启单调；全新卷传 1）。
    pub fn new(sink: Arc<dyn CommitSink>, config: CommitConfig, next_lsn: u64) -> Self {
        assert!(config.max_dirty_bytes > 0, "max_dirty_bytes must be > 0");
        let core = CommitCore {
            config,
            sink,
            next_lsn,
            flushed_lsn: next_lsn.saturating_sub(1),
            durable_lsn: next_lsn.saturating_sub(1),
            dirty_bytes: 0,
            undurable: VecDeque::new(),
            kick: false,
            waiters: Vec::new(),
            shutting_down: false,
            error: None,
            stats: CommitStats::default(),
        };
        let shared = Arc::new(Shared {
            core: Mutex::new(core),
            sync_cv: Condvar::new(),
            durable_cv: Condvar::new(),
            space_cv: Condvar::new(),
        });
        let worker_shared = shared.clone();
        let handle = std::thread::Builder::new()
            .name("wal-commit".into())
            .spawn(move || worker_run(worker_shared))
            .expect("spawn wal commit worker");
        CommitQueue {
            shared,
            worker: Mutex::new(Some(handle)),
            closed: AtomicBool::new(false),
        }
    }

    /// 入队一条记录（方案 §5.2 步骤 1/2/6）。
    ///
    /// Async：append 到 page cache 后立即返回 accepted。Strict（或 flags
    /// 含 F_SYNC_BARRIER）：append 后等待 durable 再返回，同批等待者共享
    /// 一轮 fsync。
    pub fn enqueue(&self, req: CommitRequest<'_>) -> Result<CommitReceipt, CommitError> {
        if req.payload.len() > u32::MAX as usize {
            return Err(CommitError::PayloadTooLarge {
                got: req.payload.len(),
            });
        }
        let bytes = (FRAME_HEADER_SIZE + req.payload.len()) as u64;

        let mut core = self.shared.lock();
        if let Some(e) = &core.error {
            return Err(e.clone());
        }
        if core.shutting_down {
            return Err(CommitError::ShuttingDown);
        }
        // 背压：未 durable 字节超限时阻塞，由 fsync 推进释放预算。
        while core.dirty_bytes + bytes > core.config.max_dirty_bytes {
            if let Some(e) = &core.error {
                return Err(e.clone());
            }
            if core.shutting_down {
                return Err(CommitError::ShuttingDown);
            }
            core = self.shared.space_cv.wait(core).unwrap();
        }

        let lsn = core.next_lsn;
        core.next_lsn += 1;
        let placement = match core.sink.append(lsn, req.rtype, req.flags, req.payload) {
            Ok(p) => p,
            Err(e) => {
                // append 失败后 lsn 已消耗且盘上状态未知：sticky 错误。
                let err = CommitError::Sink(e.to_string());
                core.error = Some(err.clone());
                self.shared.sync_cv.notify_one();
                self.shared.durable_cv.notify_all();
                self.shared.space_cv.notify_all();
                return Err(err);
            }
        };
        core.flushed_lsn = lsn;
        core.dirty_bytes += bytes;
        core.undurable.push_back(Undurable {
            lsn,
            bytes,
            enqueued_at: Instant::now(),
        });
        core.stats.records += 1;
        core.stats.bytes += bytes;

        let need_durable =
            matches!(req.mode, CommitMode::Strict) || req.flags & FLAG_SYNC_BARRIER != 0;
        if need_durable {
            core.kick = true;
            self.shared.sync_cv.notify_one();
            loop {
                if let Some(e) = &core.error {
                    return Err(e.clone());
                }
                if core.durable_lsn >= lsn {
                    return Ok(CommitReceipt {
                        lsn,
                        durable: true,
                        placement,
                    });
                }
                core = self.shared.durable_cv.wait(core).unwrap();
            }
        }
        // 新记录可能恰好满足某个等待者的 target。
        self.shared.sync_cv.notify_one();
        Ok(CommitReceipt {
            lsn,
            durable: false,
            placement,
        })
    }

    /// 被动等待 `durable_lsn >= lsn`（不主动触发 fsync；需立即持久化请用
    /// [`Self::flush_barrier`]）。
    pub fn wait_durable(&self, lsn: u64) -> Result<u64, CommitError> {
        self.wait_for_durable(lsn, false)
    }

    /// 持久化屏障：强制 sync 并等待 `durable_lsn >= target`。FlushNeedles
    /// 等文件系统语义锚点走此路径（方案 §5.1）。目标已 durable 时立即返回，
    /// 不触发额外 fsync。
    pub fn flush_barrier(&self, target: u64) -> Result<u64, CommitError> {
        self.wait_for_durable(target, true)
    }

    fn wait_for_durable(&self, target: u64, force: bool) -> Result<u64, CommitError> {
        // 析构顺序：core 先释放，guard 再加锁清理。
        let _guard = WaitDereg {
            q: self,
            token: NEXT_WAITER_TOKEN.fetch_add(1, Ordering::Relaxed),
        };
        let mut core = self.shared.lock();
        if core.durable_lsn >= target {
            return Ok(core.durable_lsn);
        }
        if force {
            core.kick = true;
            self.shared.sync_cv.notify_one();
        }
        core.waiters.push((_guard.token, target));
        self.shared.sync_cv.notify_one();
        loop {
            if let Some(e) = &core.error {
                return Err(e.clone());
            }
            if core.durable_lsn >= target {
                return Ok(core.durable_lsn);
            }
            core = self.shared.durable_cv.wait(core).unwrap();
        }
    }

    pub fn durable_lsn(&self) -> u64 {
        self.shared.lock().durable_lsn
    }

    pub fn flushed_lsn(&self) -> u64 {
        self.shared.lock().flushed_lsn
    }

    pub fn dirty_bytes(&self) -> u64 {
        self.shared.lock().dirty_bytes
    }

    pub fn stats(&self) -> CommitStats {
        self.shared.lock().stats
    }

    /// 优雅关闭：拒绝新写入，排空既有记录并做最终 fsync，同步等待 worker
    /// 退出后返回。幂等；[`Drop`] 复用同一逻辑。
    pub fn close(&self) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        {
            let mut core = self.shared.lock();
            core.shutting_down = true;
            core.kick = true;
            self.shared.sync_cv.notify_one();
        }
        if let Some(h) = self.worker.lock().unwrap().take() {
            let _ = h.join();
        }
    }
}

impl Drop for CommitQueue {
    fn drop(&mut self) {
        self.close(); // 幂等：已 close 时仅跳过，worker 句柄已被取走
    }
}

/// fsync worker：等待 sync 条件 → 锁内快照 target → 解锁 sync → 推进
/// durable 并唤醒等待者。
fn worker_run(shared: Arc<Shared>) {
    let sink = shared.lock().sink.clone();
    loop {
        let mut core = shared.lock();
        let shutdown_round;
        loop {
            if core.error.is_some() {
                return; // sticky 错误：线程退出，等待者已由置错处唤醒
            }
            if core.shutting_down {
                shutdown_round = true;
                break;
            }
            let waiter_due = core
                .waiters
                .iter()
                .any(|&(_, t)| t > core.durable_lsn && t <= core.flushed_lsn);
            let interval_due = core
                .undurable
                .front()
                .is_some_and(|u| u.enqueued_at.elapsed() >= core.config.async_interval);
            let budget_due = core.dirty_bytes >= core.config.max_dirty_bytes;
            if core.kick || waiter_due || interval_due || budget_due {
                shutdown_round = false;
                break;
            }
            // 最早 undurable 记录的 interval 截止时间；无记录则无限等待。
            let timeout = core.undurable.front().map(|u| {
                core.config
                    .async_interval
                    .saturating_sub(u.enqueued_at.elapsed())
            });
            core = match timeout {
                Some(t) => shared.sync_cv.wait_timeout(core, t).unwrap().0,
                None => shared.sync_cv.wait(core).unwrap(),
            };
        }
        let target = core.flushed_lsn;
        core.kick = false;
        drop(core);

        // sync 不持锁：fsync 进行中写者仍可入队（两阶段流水）。
        if let Err(e) = sink.sync() {
            let mut core = shared.lock();
            core.error.get_or_insert(CommitError::Sink(e.to_string()));
            shared.durable_cv.notify_all();
            shared.space_cv.notify_all();
            return;
        }

        let mut core = shared.lock();
        if target > core.durable_lsn {
            core.durable_lsn = target;
            let mut freed = 0u64;
            while core.undurable.front().is_some_and(|u| u.lsn <= target) {
                freed += core.undurable.pop_front().unwrap().bytes;
            }
            core.dirty_bytes = core.dirty_bytes.saturating_sub(freed);
        }
        core.stats.syncs += 1;
        shared.durable_cv.notify_all();
        shared.space_cv.notify_all();
        if shutdown_round {
            return; // 最终 fsync 完成，排空退出
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::io::Error as IoError;

    /// 测试用 sink：内存记录 + durable 计数，可注入 append/sync 失败。
    #[derive(Default)]
    struct MockSink {
        records: Mutex<Vec<MockRecord>>,
        /// 已被 sync 覆盖的记录数（append 顺序 == lsn 序）。
        durable_upto: AtomicU64,
        sync_count: AtomicU64,
        fail_append_at: AtomicU64,
        fail_sync: AtomicBool,
    }

    #[derive(Debug, Clone)]
    struct MockRecord {
        lsn: u64,
        rtype: RecordType,
        flags: u8,
        payload: Vec<u8>,
    }

    impl MockSink {
        fn durable_upto(&self) -> u64 {
            self.durable_upto.load(Ordering::SeqCst)
        }
        fn sync_count(&self) -> u64 {
            self.sync_count.load(Ordering::SeqCst)
        }
        fn len(&self) -> usize {
            self.records.lock().unwrap().len()
        }
        fn record(&self, idx: usize) -> MockRecord {
            self.records.lock().unwrap()[idx].clone()
        }
        fn injected(msg: &str) -> IoError {
            IoError::other(msg.to_string())
        }
    }

    impl CommitSink for MockSink {
        fn append(
            &self,
            lsn: u64,
            rtype: RecordType,
            flags: u8,
            payload: &[u8],
        ) -> std::io::Result<Placement> {
            let fail_at = self.fail_append_at.load(Ordering::SeqCst);
            if fail_at != 0 && self.len() as u64 + 1 >= fail_at {
                return Err(Self::injected("injected append failure"));
            }
            let mut records = self.records.lock().unwrap();
            let placement = Placement {
                seg_id: 1,
                offset: records.len() as u64,
                crc: 0,
                payload_len: payload.len() as u32,
            };
            records.push(MockRecord {
                lsn,
                rtype,
                flags,
                payload: payload.to_vec(),
            });
            Ok(placement)
        }

        fn sync(&self) -> std::io::Result<()> {
            if self.fail_sync.load(Ordering::SeqCst) {
                return Err(Self::injected("injected sync failure"));
            }
            self.sync_count.fetch_add(1, Ordering::SeqCst);
            let n = self.len() as u64;
            self.durable_upto.fetch_max(n, Ordering::SeqCst);
            Ok(())
        }
    }

    fn cfg(interval: Duration, max_dirty: u64) -> CommitConfig {
        CommitConfig {
            async_interval: interval,
            max_dirty_bytes: max_dirty,
        }
    }

    /// 并发组提交：lsn 全局稠密唯一，段内容按 lsn 序还原 payload，
    /// 整批由一次 fsync 覆盖。
    #[test]
    fn concurrent_group_commit_correctness() {
        let sink = Arc::new(MockSink::default());
        let q = Arc::new(CommitQueue::new(
            sink.clone(),
            cfg(Duration::from_secs(3600), 1 << 20),
            1,
        ));
        const THREADS: usize = 8;
        const PER: usize = 50;

        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let q = q.clone();
                std::thread::spawn(move || {
                    let mut local = Vec::with_capacity(PER);
                    for i in 0..PER {
                        let payload = format!("t{t}-i{i:04}-{}", "x".repeat(20));
                        let r = q
                            .enqueue(CommitRequest::new(RecordType::Data, payload.as_bytes()))
                            .expect("enqueue");
                        assert!(!r.durable);
                        local.push((r.lsn, payload));
                    }
                    local
                })
            })
            .collect();

        let mut expected: HashMap<u64, String> = HashMap::new();
        for h in handles {
            for (lsn, p) in h.join().unwrap() {
                expected.insert(lsn, p);
            }
        }

        // lsn 全局稠密唯一。
        let mut lsns: Vec<u64> = expected.keys().copied().collect();
        lsns.sort_unstable();
        assert_eq!(lsns, (1..=(THREADS * PER) as u64).collect::<Vec<_>>());

        let last = q.flushed_lsn();
        q.flush_barrier(last).expect("barrier");
        assert_eq!(sink.sync_count(), 1, "整批应由一轮 fsync 覆盖");
        assert_eq!(sink.durable_upto() as usize, THREADS * PER);

        // 段内容：lsn 稠密按序、payload 一一对应。
        assert_eq!(sink.len(), THREADS * PER);
        for idx in 0..sink.len() {
            let rec = sink.record(idx);
            assert_eq!(rec.lsn, (idx + 1) as u64, "lsn 必须稠密按序");
            assert_eq!(rec.rtype, RecordType::Data);
            assert_eq!(rec.payload, expected[&rec.lsn].as_bytes());
        }
    }

    /// strict 模式：ack 返回时记录必须已 durable。
    #[test]
    fn strict_ack_is_durable() {
        let sink = Arc::new(MockSink::default());
        let q = Arc::new(CommitQueue::new(
            sink.clone(),
            cfg(Duration::from_secs(3600), 1 << 20),
            1,
        ));
        const THREADS: usize = 16;
        const PER: usize = 10;

        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let q = q.clone();
                let sink = sink.clone();
                std::thread::spawn(move || {
                    for i in 0..PER {
                        let payload = format!("s{t}-i{i:02}");
                        let r = q
                            .enqueue(
                                CommitRequest::new(RecordType::Data, payload.as_bytes()).strict(),
                            )
                            .expect("strict enqueue");
                        assert!(r.durable);
                        assert!(
                            sink.durable_upto() >= r.lsn,
                            "ack 时必须已 durable: lsn {} durable_upto {}",
                            r.lsn,
                            sink.durable_upto()
                        );
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(sink.durable_upto(), (THREADS * PER) as u64);
        assert!(sink.sync_count() >= 1);
        assert_eq!(sink.len(), THREADS * PER);
        for idx in 0..sink.len() {
            assert_eq!(sink.record(idx).lsn, (idx + 1) as u64);
        }
    }

    /// async 窗口语义：窗口内 accepted-but-not-durable；barrier 后 durable；
    /// interval 到期自动 fsync。
    #[test]
    fn async_window_semantics() {
        // 窗口内：无任何 fsync。
        let sink = Arc::new(MockSink::default());
        let q = CommitQueue::new(sink.clone(), cfg(Duration::from_secs(3600), 1 << 20), 1);
        let r = q
            .enqueue(CommitRequest::new(RecordType::Data, b"win"))
            .unwrap();
        assert!(!r.durable);
        assert_eq!(q.durable_lsn(), 0);
        assert_eq!(sink.durable_upto(), 0, "async 窗口内不得提前 fsync");
        assert_eq!(q.flushed_lsn(), r.lsn);

        q.flush_barrier(r.lsn).unwrap();
        assert_eq!(q.durable_lsn(), r.lsn);
        assert_eq!(sink.durable_upto(), 1);
        assert_eq!(sink.sync_count(), 1);
        drop(q);

        // interval 到期自动 sync。
        let sink2 = Arc::new(MockSink::default());
        let q2 = CommitQueue::new(sink2.clone(), cfg(Duration::from_millis(5), 1 << 20), 1);
        let r2 = q2
            .enqueue(CommitRequest::new(RecordType::Data, b"tick"))
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while q2.durable_lsn() < r2.lsn {
            assert!(Instant::now() < deadline, "async interval 内未完成 fsync");
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(sink2.durable_upto() >= 1);
        drop(q2);
    }

    /// barrier 按 LSN 等待：目标未入队也持续等待；目标覆盖后返回；
    /// 已 durable 的目标走快路径不触发额外 fsync。
    #[test]
    fn barrier_waits_for_target_lsn() {
        let sink = Arc::new(MockSink::default());
        let q = Arc::new(CommitQueue::new(
            sink.clone(),
            cfg(Duration::from_secs(3600), 1 << 20),
            1,
        ));
        for i in 0..5u64 {
            q.enqueue(CommitRequest::new(
                RecordType::Data,
                format!("r{i}").as_bytes(),
            ))
            .unwrap();
        }

        let done = Arc::new(AtomicBool::new(false));
        let q2 = q.clone();
        let done2 = done.clone();
        let h = std::thread::spawn(move || {
            q2.flush_barrier(7).expect("barrier future lsn");
            done2.store(true, Ordering::SeqCst);
        });
        std::thread::sleep(Duration::from_millis(200));
        assert!(
            !done.load(Ordering::SeqCst),
            "目标 lsn 未覆盖时必须继续等待"
        );
        assert_eq!(q.durable_lsn(), 5, "barrier 已强制同步既有记录");

        for i in 5..7u64 {
            q.enqueue(CommitRequest::new(
                RecordType::Data,
                format!("r{i}").as_bytes(),
            ))
            .unwrap();
        }
        h.join().unwrap();
        assert!(done.load(Ordering::SeqCst));
        assert!(q.durable_lsn() >= 7);

        let before = sink.sync_count();
        q.flush_barrier(5).unwrap();
        assert_eq!(
            sink.sync_count(),
            before,
            "已 durable 的 barrier 不触发额外 fsync"
        );
        assert_eq!(before, 2);
    }

    /// 背压：未 durable 字节超限阻塞入队，fsync 释放预算后解除。
    #[test]
    fn backpressure_blocks_until_fsync_releases() {
        let sink = Arc::new(MockSink::default());
        let q = Arc::new(CommitQueue::new(
            sink.clone(),
            cfg(Duration::from_secs(3600), 256),
            1,
        ));
        let _r1 = q
            .enqueue(CommitRequest::new(RecordType::Data, &[0u8; 100]))
            .unwrap();
        let r2 = q
            .enqueue(CommitRequest::new(RecordType::Data, &[1u8; 100]))
            .unwrap();
        assert_eq!(q.dirty_bytes(), (26 + 100 + 26 + 100) as u64);

        // 26 + 60 = 86，252 + 86 > 256 → 阻塞。
        let q2 = q.clone();
        let h = std::thread::spawn(move || {
            q2.enqueue(CommitRequest::new(RecordType::Data, &[2u8; 60]))
                .expect("blocked enqueue must unblock")
        });
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(q.flushed_lsn(), 2, "背压阻塞期间不得分配新 lsn");

        q.flush_barrier(r2.lsn).unwrap();
        let r3 = h.join().unwrap();
        assert_eq!(r3.lsn, 3);
        assert_eq!(q.dirty_bytes(), (26 + 60) as u64);
        drop(q);
    }

    /// FLAG_SYNC_BARRIER 记录：async 模式下也提升为持久化屏障，标记写入帧。
    #[test]
    fn sync_barrier_flag_forces_durable() {
        let sink = Arc::new(MockSink::default());
        let q = CommitQueue::new(sink.clone(), cfg(Duration::from_secs(3600), 1 << 20), 1);
        let r = q
            .enqueue(CommitRequest::new(RecordType::Data, b"anchor").barrier())
            .unwrap();
        assert!(r.durable, "F_SYNC_BARRIER 提升为持久化屏障");
        assert!(sink.durable_upto() >= 1);
        let rec = sink.record(0);
        assert_eq!(
            rec.flags & FLAG_SYNC_BARRIER,
            FLAG_SYNC_BARRIER,
            "屏障标记写入帧"
        );
        drop(q);
    }

    /// sink append 失败：sticky 错误，后续入队全部拒绝。
    #[test]
    fn append_error_is_sticky() {
        let sink = Arc::new(MockSink {
            fail_append_at: AtomicU64::new(3),
            ..Default::default()
        });
        let q = CommitQueue::new(sink, cfg(Duration::from_secs(3600), 1 << 20), 1);
        q.enqueue(CommitRequest::new(RecordType::Data, b"1"))
            .unwrap();
        q.enqueue(CommitRequest::new(RecordType::Data, b"2"))
            .unwrap();
        assert!(matches!(
            q.enqueue(CommitRequest::new(RecordType::Data, b"3")),
            Err(CommitError::Sink(_))
        ));
        assert!(
            matches!(
                q.enqueue(CommitRequest::new(RecordType::Data, b"4")),
                Err(CommitError::Sink(_))
            ),
            "sticky 错误必须拒绝后续入队"
        );
        drop(q);
    }

    /// sink sync 失败：等待者收到错误，队列进入不可恢复状态。
    #[test]
    fn sync_error_propagates_to_waiters() {
        let sink = Arc::new(MockSink {
            fail_sync: AtomicBool::new(true),
            ..Default::default()
        });
        let q = CommitQueue::new(sink, cfg(Duration::from_secs(3600), 1 << 20), 1);
        assert!(matches!(
            q.enqueue(CommitRequest::new(RecordType::Data, b"x").strict()),
            Err(CommitError::Sink(_))
        ));
        assert!(
            matches!(
                q.enqueue(CommitRequest::new(RecordType::Data, b"y")),
                Err(CommitError::Sink(_))
            ),
            "sticky 错误必须拒绝后续入队"
        );
        drop(q);
    }

    /// close：排空既有记录并最终 fsync，拒绝新写入；Drop join worker。
    #[test]
    fn close_drains_and_rejects_new_writes() {
        let sink = Arc::new(MockSink::default());
        let q = CommitQueue::new(sink.clone(), cfg(Duration::from_secs(3600), 1 << 20), 1);
        for i in 0..3u64 {
            q.enqueue(CommitRequest::new(
                RecordType::Data,
                format!("d{i}").as_bytes(),
            ))
            .unwrap();
        }
        q.close();
        assert_eq!(q.durable_lsn(), 3, "close 必须排空并最终 fsync");
        assert_eq!(sink.durable_upto(), 3);
        assert!(matches!(
            q.enqueue(CommitRequest::new(RecordType::Data, b"late")),
            Err(CommitError::ShuttingDown)
        ));
        drop(q);
    }
}
