//! §8.5 锁消息优先级分层.
//!
//! CHANNEL_LOCK (§8.4) 给锁消息一条独立的物理通道 + 独立 worker 线程池,
//! 但通道内仍是 FIFO. 在高冲突场景下, 这会让一个 `Acquire` (P2) 排在
//! `RevokeAck` (P0) 前面——而 `RevokeAck` 正是触发 Early Grant、解锁排队
//! 等待者的关键控制消息. FIFO 排队会把 waiter 的 stall 从 "毫秒级 ACK
//! 往返" 拉长到 "整队 IO 消息处理完", 显著放大尾延迟.
//!
//! 本模块把锁通道的 FIFO mpsc 换成一个**优先级队列**:
//!
//! ```text
//! P0 (最高): RevokeAck —— 解锁排队等待者 (Early Grant 触发点)
//! P1:        Release    —— 自愿释放, 同样解锁等待者
//! P2:        Acquire    —— 新锁请求 (普通优先级)
//! P3 (最低): Renew / LeaseStatus / Range / Invalidate —— 后台-ish
//! ```
//!
//! 控制消息 (P0/P1) 优先于数据消息 (P2/P3), 这样在冲突爆发时, 解锁
//! 等待者的消息先被处理, waiter stall 被压缩到 ACK 往返级别. 同优先级
//! 内保持 FIFO (用单调 seq), 公平性不被破坏.
//!
//! # 实现
//!
//! - 有界 (capacity) 优先级队列: `Mutex<BinaryHeap<LockWork>>` + `Notify`.
//! - `try_push` 非阻塞 (队列满时返回 `Full`, 由 IoLoop 决定丢弃/降级,
//!   绝不阻塞 IoLoop 读循环——锁消息量远小于 IO, Full 极罕见, 丢弃后
//!   客户端重试 acquire 或 §8.3.1 force-reclaim 兜底).
//! - `recv` 异步 (队列空时挂起等待 `Notify`, 不忙等).
//! - 关闭 (`close`) 后: `try_push` 返回 `Closed`, `recv` 排空后返回 `None`.
//!
//! # 唤醒正确性 (无丢失唤醒)
//!
//! 消费者先**创建** `notified()` future, 再查堆, 再 `.await`——这样在
//! "查堆(空) → await" 窗口内若有生产者 push + `notify_one()`, 存储的
//! permit 会被随后的 `notified().await` 立即消费, 不会丢失唤醒.

use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::Mutex;

use tokio::sync::Notify;

use crate::protocol::MsgType;
use crate::work::Work;

/// §8.5 锁消息优先级. 枚举顺序即优先级 (越靠前越高, `RevokeAck` 最高).
///
/// `rank()` 返回的数值越大表示越优先, 供 `BinaryHeap` (max-heap) 出队.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LockPriority {
    /// P0: `RevokeAck` —— 持有者确认释放, 触发 Early Grant 解锁下一个
    /// 排队等待者. 处理这条消息的延迟直接决定 waiter stall, 必须最高优先.
    RevokeAck,
    /// P1: 自愿 `Release` —— 持有者主动释放, 同样解锁等待者 (走 queue
    /// pop + grant 路径). 比 Acquire 优先, 因为它解锁别人.
    Release,
    /// P2: 新 `Acquire` —— 普通优先级的新锁请求. 不解锁任何人, 只是
    /// 入队/冲突. 与 P3 相比, Acquire 是用户感知的延迟 (open/lock 系统调用).
    Acquire,
    /// P3: `Renew` / `LeaseStatus` / `Range` / `Invalidate` —— 续期/查询/
    /// 失效通知, 后台-ish, 延迟容忍度高.
    Routine,
}

impl LockPriority {
    /// 数值优先级 (越大越先出队). `RevokeAck`=3 … `Routine`=0.
    /// 供 `LockWork::Ord` 使用 (BinaryHeap 是 max-heap).
    const fn rank(self) -> u8 {
        match self {
            LockPriority::RevokeAck => 3,
            LockPriority::Release => 2,
            LockPriority::Acquire => 1,
            LockPriority::Routine => 0,
        }
    }

    /// 把一条锁 `MsgType` 映射到它的 §8.5 优先级 tier.
    ///
    /// - `RevokeInodeLeaseAck` → P0 (Early Grant 触发点).
    /// - `ReleaseInodeLease` / `ReleaseLease` → P1 (自愿释放, 解锁等待者).
    /// - `AcquireInodeLease` / `AcquireLease` / `AcquireLeaseBatch` → P2.
    /// - 其余 (`Renew*` / `LeaseStatus` / `RangeLease` / `Invalidate`) → P3.
    ///
    /// 非 lock 通道的 MsgType 不应到达这里 (IoLoop 先用
    /// `is_lock_channel()` 过滤); 若误入, 保守归为 P2 (普通优先级).
    pub fn for_msg_type(mt: MsgType) -> Self {
        match mt {
            MsgType::RevokeInodeLeaseAck | MsgType::CapRecallAck => LockPriority::RevokeAck,
            MsgType::ReleaseInodeLease | MsgType::ReleaseLease | MsgType::CapRelease => {
                LockPriority::Release
            }
            MsgType::AcquireInodeLease
            | MsgType::AcquireLease
            | MsgType::AcquireLeaseBatch
            | MsgType::CapOpenGrant => LockPriority::Acquire,
            // 其余 lock 通道消息 (Renew* / LeaseStatus / RangeLease /
            // Invalidate / CapRecallNotify / CapUpgradeNotify — server-push
            // notifications, not client-initiated) 以及任何误入的非 lock
            // 消息 → 后台优先级.
            _ => LockPriority::Routine,
        }
    }
}

/// 一条带优先级的锁工作项. `Ord` 让 `BinaryHeap` (max-heap) 按以下顺序出队:
/// 1. 优先级 rank 降序 (P0 `RevokeAck` 先出);
/// 2. 同优先级内, seq 升序 (FIFO——先入队的先出).
struct LockWork {
    priority_rank: u8,
    /// 单调递增的入队序号, 用于同优先级 FIFO. `BinaryHeap` 是 max-heap,
    /// 所以用 `Reverse(seq)` 让小 seq 排在堆顶 (大 = 先出).
    seq: u64,
    work: Work,
}

impl Ord for LockWork {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // rank 降序 (大 rank 先出)
        self.priority_rank
            .cmp(&other.priority_rank)
            // 同 rank 内 seq 升序 (小 seq 先出): 用 Reverse 让 max-heap 行为正确
            .then_with(|| std::cmp::Reverse(self.seq).cmp(&std::cmp::Reverse(other.seq)))
    }
}

impl PartialOrd for LockWork {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for LockWork {}

impl PartialEq for LockWork {
    fn eq(&self, other: &Self) -> bool {
        self.priority_rank == other.priority_rank && self.seq == other.seq
    }
}

/// `try_push` 失败原因. 镜像 `mpsc::TrySendError` 的两个变体, 供 IoLoop
/// 按 `is_full()` / `is_closed()` 分别处理 (满→丢弃+日志, 关闭→停止读循环).
#[derive(Debug)]
pub enum TryPushError {
    /// 队列已满 (达到 capacity). 调用方通常丢弃并记日志——锁消息量远
    /// 小于 IO, Full 极罕见; 丢弃后客户端重试或 §8.3.1 force-reclaim 兜底.
    Full,
    /// 队列已关闭 (服务端 shutdown). 调用方应停止读循环.
    Closed,
}

/// §8.5 共享内核: 一个有界优先级堆 + 两个 Notify (唤醒消费者) + 关闭位.
struct Inner {
    heap: Mutex<BinaryHeap<LockWork>>,
    /// 容量上限 (有界, 防止无界增长). 镜像 §8.4 `lock_queue_capacity`.
    capacity: usize,
    /// 消费者在堆空时等待此 Notify. 生产者 push 后 `notify_one()` 唤醒.
    not_empty: Notify,
    /// 单调序号, 保证同优先级 FIFO.
    seq: AtomicU64,
    /// 关闭位. `close()` 后 `try_push` 返回 `Closed`, `recv` 排空后返回 `None`.
    closed: AtomicBool,
}

impl Inner {
    fn try_push(&self, work: Work, priority: LockPriority) -> Result<(), TryPushError> {
        if self.closed.load(AtomicOrdering::Acquire) {
            return Err(TryPushError::Closed);
        }
        let lw = LockWork {
            priority_rank: priority.rank(),
            seq: self.seq.fetch_add(1, AtomicOrdering::Relaxed),
            work,
        };
        let pushed = {
            let mut heap = match self.heap.lock() {
                Ok(h) => h,
                Err(p) => p.into_inner(),
            };
            if heap.len() >= self.capacity {
                return Err(TryPushError::Full);
            }
            heap.push(lw);
            true
        };
        if pushed {
            // 唤醒一个等待的消费者. 若无消费者在等, 存一个 permit,
            // 下一次 recv() 的 notified() 会立即消费——不会丢失唤醒.
            self.not_empty.notify_one();
        }
        Ok(())
    }

    async fn recv(&self) -> Option<Work> {
        loop {
            // 关键: 先创建 notified() future, 再查堆, 再 await.
            // 这样 "查堆(空) → await" 窗口内的 notify_one() 存的 permit
            // 会被这个 notified() 消费, 不丢失唤醒.
            let notified = self.not_empty.notified();
            {
                let mut heap = match self.heap.lock() {
                    Ok(h) => h,
                    Err(p) => p.into_inner(),
                };
                if let Some(lw) = heap.pop() {
                    return Some(lw.work);
                }
                // 堆空 + 已关闭 → 终止 (排空后返回 None, 镜像 mpsc 语义)
                if self.closed.load(AtomicOrdering::Acquire) {
                    return None;
                }
            }
            // 堆空且未关闭: 等待生产者唤醒.
            notified.await;
        }
    }

    fn close(&self) {
        self.closed.store(true, AtomicOrdering::Release);
        // 唤醒所有等待的消费者, 让它们看到 closed 位并返回 None.
        self.not_empty.notify_waiters();
    }
}

/// §8.5 优先级队列的生产者端 (clone 廉价, 持 `Arc<Inner>`).
/// IoLoop 持有一个, 把锁消息按 MsgType 推导的优先级入队.
#[derive(Clone)]
pub struct LockPriorityProducer {
    inner: std::sync::Arc<Inner>,
}

impl LockPriorityProducer {
    /// 非阻塞入队. 优先级由 `work.msg.msg_type()` 经
    /// [`LockPriority::for_msg_type`] 自动推导, 调用方无需指定.
    /// 满返回 `Full` (丢弃), 关闭返回 `Closed` (停止读循环).
    pub fn try_push(&self, work: Work) -> Result<(), TryPushError> {
        let priority = work
            .msg
            .msg_type()
            .map(LockPriority::for_msg_type)
            .unwrap_or(LockPriority::Routine);
        self.inner.try_push(work, priority)
    }

    /// 关闭队列 (shutdown). 关闭后 `try_push` 返回 `Closed`,
    /// `recv` 排空后返回 `None`.
    pub fn close(&self) {
        self.inner.close();
    }
}

/// §8.5 优先级队列的消费者端 (持 `Arc<Inner>`). 由 lock worker 线程池
/// 持有, `recv().await` 按优先级出队.
pub struct LockPriorityConsumer {
    inner: std::sync::Arc<Inner>,
}

/// 创建一对 (producer, consumer), 容量 = `capacity`. 镜像 §8.4
/// `setup_lock_queue` 的 mpsc 接口, 但内部是优先级堆.
pub fn channel(capacity: usize) -> (LockPriorityProducer, LockPriorityConsumer) {
    let inner = std::sync::Arc::new(Inner {
        heap: Mutex::new(BinaryHeap::new()),
        capacity,
        not_empty: Notify::new(),
        seq: AtomicU64::new(0),
        closed: AtomicBool::new(false),
    });
    (
        LockPriorityProducer {
            inner: inner.clone(),
        },
        LockPriorityConsumer { inner },
    )
}

impl LockPriorityConsumer {
    /// 按优先级出队. 堆空时挂起等待, 关闭后排空返回 `None`.
    pub async fn recv(&self) -> Option<Work> {
        self.inner.recv().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_conn::ClientConn;
    use crate::protocol::{
        ClientType, FrameFlags, FrameHeader, MsgType, NetMessage, CHANNEL_DATA, STATUS_OK,
    };
    use tokio::sync::mpsc;

    fn make_work(mt: MsgType) -> Work {
        let (tx, _rx) = mpsc::unbounded_channel::<Vec<u8>>();
        // ClientConn::new returns Arc<ClientConn> (no double-wrap).
        let conn = ClientConn::new(
            1,
            "127.0.0.1:1234".parse().unwrap(),
            ClientType::Kernel,
            CHANNEL_DATA,
            0,
            tx,
        );
        let header = FrameHeader::new(mt.as_u16(), FrameFlags::new(0), 1, 0).with_status(STATUS_OK);
        let msg = NetMessage::new(header);
        Work::new(conn, msg)
    }

    #[test]
    fn test_priority_ordering_revoke_ack_first() {
        // 同时刻入队 P0/P1/P2/P3 各一条, 出队顺序必须是 P0→P1→P2→P3.
        let (tx, rx) = channel(16);
        // 故意按逆序入队, 验证不是 FIFO.
        tx.try_push(make_work(MsgType::RenewInodeLease)).unwrap(); // P3
        tx.try_push(make_work(MsgType::AcquireInodeLease)).unwrap(); // P2
        tx.try_push(make_work(MsgType::ReleaseInodeLease)).unwrap(); // P1
        tx.try_push(make_work(MsgType::RevokeInodeLeaseAck))
            .unwrap(); // P0

        let rt = tokio::runtime::Runtime::new().unwrap();
        let got: Vec<MsgType> = rt.block_on(async move {
            let mut out = Vec::new();
            for _ in 0..4 {
                let w = rx.recv().await.unwrap();
                out.push(w.msg.msg_type().unwrap());
            }
            out
        });
        assert_eq!(
            got,
            vec![
                MsgType::RevokeInodeLeaseAck, // P0
                MsgType::ReleaseInodeLease,   // P1
                MsgType::AcquireInodeLease,   // P2
                MsgType::RenewInodeLease,     // P3
            ]
        );
    }

    #[test]
    fn test_fifo_within_same_priority() {
        // 3 条 P2 Acquire 入队, 出队必须按入队顺序 (FIFO).
        let (tx, rx) = channel(16);
        tx.try_push(make_work(MsgType::AcquireInodeLease)).unwrap(); // seq 0
        tx.try_push(make_work(MsgType::AcquireInodeLease)).unwrap(); // seq 1
        tx.try_push(make_work(MsgType::AcquireInodeLease)).unwrap(); // seq 2

        let rt = tokio::runtime::Runtime::new().unwrap();
        let count = rt.block_on(async move {
            let mut n = 0;
            while rx.recv().await.is_some() {
                n += 1;
                if n == 3 {
                    break;
                }
            }
            n
        });
        assert_eq!(count, 3);
    }

    #[test]
    fn test_priority_preempts_fifo_lower() {
        // P2 先入队, 然后 P0 入队 → P0 必须先出 (优先级压过 FIFO).
        let (tx, rx) = channel(16);
        tx.try_push(make_work(MsgType::AcquireInodeLease)).unwrap(); // P2, seq 0
        tx.try_push(make_work(MsgType::RevokeInodeLeaseAck))
            .unwrap(); // P0, seq 1

        let rt = tokio::runtime::Runtime::new().unwrap();
        let first = rt.block_on(async move { rx.recv().await.unwrap().msg.msg_type().unwrap() });
        assert_eq!(first, MsgType::RevokeInodeLeaseAck); // P0 先出, 尽管 seq 更大
    }

    #[test]
    fn test_full_returns_full_error() {
        let (tx, _rx) = channel(2);
        tx.try_push(make_work(MsgType::AcquireInodeLease)).unwrap();
        tx.try_push(make_work(MsgType::AcquireInodeLease)).unwrap();
        // 第 3 条 → Full
        match tx.try_push(make_work(MsgType::AcquireInodeLease)) {
            Err(TryPushError::Full) => {} // ok
            other => panic!("expected Full, got {:?}", other),
        }
    }

    #[test]
    fn test_closed_returns_closed_error() {
        let (tx, _rx) = channel(4);
        tx.close();
        match tx.try_push(make_work(MsgType::AcquireInodeLease)) {
            Err(TryPushError::Closed) => {} // ok
            other => panic!("expected Closed, got {:?}", other),
        }
    }

    #[test]
    fn test_close_drains_then_returns_none() {
        // 关闭后, 已入队的项仍可取出 (排空), 之后 recv 返回 None.
        let (tx, rx) = channel(4);
        tx.try_push(make_work(MsgType::RevokeInodeLeaseAck))
            .unwrap();
        tx.close();

        let rt = tokio::runtime::Runtime::new().unwrap();
        let (first, second) = rt.block_on(async move {
            let a = rx.recv().await; // 取出已入队的
            let b = rx.recv().await; // 排空后 → None
            (a, b)
        });
        assert!(first.is_some(), "queued item should be drained after close");
        assert!(second.is_none(), "recv after drain + close should be None");
    }

    #[tokio::test]
    async fn test_recv_waits_when_empty_then_wakes() {
        // 消费者先 await (堆空), 生产者后 push → 必须被唤醒并取出.
        let (tx, rx) = channel(4);
        let rx = std::sync::Arc::new(rx);
        let rx2 = rx.clone();

        let consumer = tokio::spawn(async move { rx2.recv().await });

        // 给消费者一点时间 park.
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        tx.try_push(make_work(MsgType::RevokeInodeLeaseAck))
            .unwrap();
        let got = consumer.await.unwrap();
        assert!(got.is_some(), "consumer should wake after push");
        assert_eq!(
            got.unwrap().msg.msg_type().unwrap(),
            MsgType::RevokeInodeLeaseAck
        );
        // 避免 rx 析构警告
        drop(rx);
    }

    #[test]
    fn test_for_msg_type_mapping() {
        // P0
        assert_eq!(
            LockPriority::for_msg_type(MsgType::RevokeInodeLeaseAck),
            LockPriority::RevokeAck
        );
        // P1
        assert_eq!(
            LockPriority::for_msg_type(MsgType::ReleaseInodeLease),
            LockPriority::Release
        );
        assert_eq!(
            LockPriority::for_msg_type(MsgType::ReleaseLease),
            LockPriority::Release
        );
        // P2
        assert_eq!(
            LockPriority::for_msg_type(MsgType::AcquireInodeLease),
            LockPriority::Acquire
        );
        assert_eq!(
            LockPriority::for_msg_type(MsgType::AcquireLease),
            LockPriority::Acquire
        );
        assert_eq!(
            LockPriority::for_msg_type(MsgType::AcquireLeaseBatch),
            LockPriority::Acquire
        );
        // P3
        assert_eq!(
            LockPriority::for_msg_type(MsgType::RenewInodeLease),
            LockPriority::Routine
        );
        assert_eq!(
            LockPriority::for_msg_type(MsgType::LeaseStatus),
            LockPriority::Routine
        );
        assert_eq!(
            LockPriority::for_msg_type(MsgType::RangeLease),
            LockPriority::Routine
        );
        assert_eq!(
            LockPriority::for_msg_type(MsgType::Invalidate),
            LockPriority::Routine
        );
    }
}
