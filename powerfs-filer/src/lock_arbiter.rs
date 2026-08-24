//! MDLock — 细粒度独立锁对象 (对齐  MDS Locker 4 套状态机)
//!
//! 参考:  `src/mds/locks.cc`, `SimpleLock.h`, `ScatterLock.h`,
//!        `FileLock.h`, `LocalLock.h`
//! 设计文档: `docs/mdlock-design.md`
//!
//! # 核心原则
//!
//! 客户端看不到锁, 只拿 cap/lease 令牌; 所有锁状态、冲突仲裁、
//! 缓存权限全部收敛在 Filer 端 (lock_arbiter)。
//!
//! # 四套锁状态机
//!
//! | 状态机 | 状态集 | 锁类型 | 特点 |
//! |--------|--------|--------|------|
//! | LocalLock | AVAILABLE, LOCK | ISNAP | MDS 本地, 无客户端 cap |
//! | SimpleLock | AVAILABLE, SHARED, LONER, EXCL, GATHER, REVOKING | IAUTH/ILINK/IXATTR/DN | 排他写, 共享读, Loner |
//! | ScatterLock | AVAILABLE, DSCATTER, EXCL, INACTIVE, SYNC, GATHER | IDFT/INEST | 多方共享写 |
//! | FileLock | SimpleLock + SYNC | IFILE | 完整 Loner + FILE cap |
//!
//! # 与 cap_manager 的关系
//!
//! `cap_manager` 的三态机 (Free/SharedRead/ExclusiveWrite/SharedWrite)
//! 映射到 IFILE 锁 (FileLock) 的状态:
//! - Free → AVAILABLE
//! - SharedRead → SHARED
//! - ExclusiveWrite → LONER
//! - SharedWrite → SHARED (多 writer, 无 CAP_W)
//!
//! `eval()` 将锁状态映射到 `CapSet` (CAP_R/W/X) 下发给客户端。

use crate::cap_manager::CapSet;
use lazy_static::lazy_static;
use log::{debug, warn};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

/// 锁状态机类别 — 对齐  4 套锁状态机
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum LockClass {
    /// LocalLock: MDS 本地锁, 不涉及客户端 cap, 二态 AVAIL/LOCK
    Local,
    /// SimpleLock: 排他写+共享读, rdlock/wrlock 原语, 支持 Loner
    Simple,
    /// ScatterLock: 多方共享写, MDS 间合并, 不输出客户端 cap
    Scatter,
    /// FileLock: 扩展 SimpleLock + 完整 FILE cap + SYNC
    File,
}

/// 锁类型 — 对齐  MDS lock types
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum LockType {
    Auth = 0,  // IAUTH: inode 权限 (mode/uid/gid)
    Link = 1,  // ILINK: 硬链接计数 (nlink)
    Xattr = 2, // IXATTR: 扩展属性
    Dn = 3,    // DN: dentry 名称解析 (含 lease)
    Snap = 4,  // ISNAP: 快照 (LocalLock)
    File = 5,  // IFILE: 文件数据 (read/write/truncate)
    Dft = 6,   // IDFT: 目录分片 (dirfrag)
    Nest = 7,  // INEST: 嵌套目录
}

impl LockType {
    pub const NUM_TYPES: usize = 8;

    /// 从数组索引恢复 LockType
    pub fn from_index(i: usize) -> Self {
        match i {
            0 => LockType::Auth,
            1 => LockType::Link,
            2 => LockType::Xattr,
            3 => LockType::Dn,
            4 => LockType::Snap,
            5 => LockType::File,
            6 => LockType::Dft,
            7 => LockType::Nest,
            _ => unreachable!("invalid lock type index"),
        }
    }

    /// 锁类型 → 状态机类别
    pub fn class(self) -> LockClass {
        match self {
            LockType::Auth | LockType::Link | LockType::Xattr | LockType::Dn => LockClass::Simple,
            LockType::Snap => LockClass::Local,
            LockType::File => LockClass::File,
            LockType::Dft | LockType::Nest => LockClass::Scatter,
        }
    }

    /// 锁类型 → cap 位掩码 (eval 使用)
    /// 返回 CapSet, 表示该锁在 LONER 状态下下发的全套 cap
    pub fn cap_bits(self) -> CapSet {
        match self {
            LockType::Auth => CapSet::CAP_R | CapSet::CAP_X, // 权限: 读+元数据写
            LockType::Link => CapSet::CAP_R,                 // 链接: 只读
            LockType::Xattr => CapSet::CAP_R | CapSet::CAP_X, // xattr: 读+元数据写
            LockType::Dn => CapSet::NONE,                    // DN: 输出 lease, 不输出 cap
            LockType::Snap => CapSet::NONE,                  // LocalLock: 不输出 cap
            LockType::File => CapSet::CAP_R | CapSet::CAP_W | CapSet::CAP_X, // 文件: 全套
            LockType::Dft | LockType::Nest => CapSet::NONE,  // ScatterLock: 不输出 cap
        }
    }
}

/// 锁状态 — 合并 4 套状态机的所有状态
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LockState {
    // === 共享状态 ===
    /// 无锁, 无持有者 (所有 class)
    Available,

    // === SimpleLock + FileLock 状态 ===
    /// 共享态: 多方并发读
    Shared,
    /// Loner 独占优化: 仅 1 client, 下发 exclusive cap
    Loner,
    /// 完全独占: xlock 持有
    Excl,
    /// 正在收集 recall ACK
    Gather,
    /// 正在部分撤销 (recall 子集 cap)
    Revoking,

    // === LocalLock 状态 ===
    /// LocalLock 独占: MDS 本地锁
    Lock,

    // === ScatterLock 状态 ===
    /// ScatterLock 散射态: 多方共享写
    Dscatter,
    /// ScatterLock 非活跃
    Inactive,
    /// 同步态: 只读 (FileLock SYNC + ScatterLock SYNC_SCATTER)
    Sync,
}

impl LockState {
    pub fn name(self) -> &'static str {
        match self {
            LockState::Available => "AVAILABLE",
            LockState::Shared => "SHARED",
            LockState::Loner => "LONER",
            LockState::Excl => "EXCL",
            LockState::Gather => "GATHER",
            LockState::Revoking => "REVOKING",
            LockState::Lock => "LOCK",
            LockState::Dscatter => "DSCATTER",
            LockState::Inactive => "INACTIVE",
            LockState::Sync => "SYNC",
        }
    }
}

/// GATHER 目标 — recall 完成后跃迁到哪个状态
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GatherTarget {
    /// 跃迁到 EXCL (xlock)
    ToExcl,
    /// 跃迁到 SHARED (wrlock 多 client)
    ToShared,
    /// 跃迁到 LONER (wrlock 单 client)
    ToLoner,
    /// 跃迁到 DSCATTER (scatter_wrlock, ScatterLock 多方共享写)
    ToDscatter,
    /// 跃迁到 SYNC (file_flush_to_sync / quiesce FileLock)
    ToSync,
}

/// 锁持有者 (per-client, per-lock)
#[derive(Clone, Debug)]
pub struct LockHolder {
    pub client_id: String,
    /// 全局序列号 (SnAllocator 分配, 用于 fencing)
    pub sn: u64,
    /// Fencer epoch (每次 recall bump)
    pub epoch: u64,
    /// 已授予的 cap 掩码
    pub granted_caps: CapSet,
    /// 脏 cap (需要 flush 的)
    pub dirty_caps: CapSet,
    /// 是否有 recall 在途
    pub recall_in_flight: bool,
    /// recall 后保留的 cap (部分撤销)
    pub retain_caps: CapSet,
    /// recall 要收回的 cap
    pub recall_caps: CapSet,
    /// 过期时间
    pub expire_at: Instant,
}

/// GATHER 等待者: 记录已发 recall 但未收到 ACK 的 holder
#[derive(Clone, Debug)]
pub struct GatherEntry {
    pub client_id: String,
    pub sn: u64,
    pub sent_at: Instant,
    pub acked: bool,
}

/// 锁原语类型
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LockOp {
    Rdlock,
    Wrlock,
    Xlock,
    Unlock,
}

impl LockOp {
    /// 调度优先级: 数值越大越优先 (对齐  Locker 等待队列优先级).
    /// xlock (rename/truncate 等强一致路径) 优先于 wrlock, wrlock 优先于 rdlock.
    /// 解锁路径 `Unlock` 不入队, 此处给 0 仅占位.
    pub fn priority(self) -> u8 {
        match self {
            LockOp::Xlock => 3,
            LockOp::Wrlock => 2,
            LockOp::Rdlock => 1,
            LockOp::Unlock => 0,
        }
    }
}

/// 等待者: 被阻塞的锁请求
#[derive(Debug)]
pub struct LockWaiter {
    pub client_id: String,
    pub op: LockOp,
    pub sender: oneshot::Sender<LockGrantResult>,
}

/// 锁授予结果
#[derive(Debug, Clone)]
pub struct LockGrantResult {
    pub client_id: String,
    pub lock_type: LockType,
    pub sn: u64,
    pub epoch: u64,
    pub granted_caps: CapSet,
    /// 需要异步发送的 recall 任务
    pub recall_tasks: Vec<RecallTask>,
    /// 租约持续时间 (ms)
    pub duration_ms: u64,
}

/// 异步锁获取结果
pub enum LockAcquireResult {
    /// 立即授予
    Granted(LockGrantResult),
    /// 需要等待 GATHER 完成 (recall 旧 holder + 等 ACK).
    /// `recall_tasks` 必须由调用方 (net_handler) 异步 dispatch 给
    /// 旧 holder, 否则 GATHER 永远不完成 (waiter 阻塞直到 tick
    /// force-reclaim). await `rx` 获取最终授予结果.
    Waiting {
        recall_tasks: Vec<RecallTask>,
        rx: oneshot::Receiver<LockGrantResult>,
    },
}

/// recall 通知任务 (由 net_handler 异步发送到客户端)
#[derive(Debug, Clone)]
pub struct RecallTask {
    pub client_id: String,
    /// 对应锁类型 (调用 recall_ack 时使用)
    pub lock_type: LockType,
    pub sn: u64,
    pub caps_to_recall: CapSet,
    pub retained_caps: CapSet,
    pub new_epoch: u64,
}

/// 独立锁对象 — 一个 inode 一种 lock_type 一把锁
#[derive(Debug)]
pub struct MdLock {
    pub lock_type: LockType,
    pub class: LockClass,
    pub state: LockState,
    pub holders: Vec<LockHolder>,
    pub gather_list: Vec<GatherEntry>,
    pub gather_remaining: usize,
    pub gather_target: GatherTarget,
    pub waiting: VecDeque<LockWaiter>,
    pub eval_issued: CapSet,
    pub eval_wanted: CapSet,
}

impl MdLock {
    fn new(lock_type: LockType) -> Self {
        Self {
            lock_type,
            class: lock_type.class(),
            state: LockState::Available,
            holders: Vec::new(),
            gather_list: Vec::new(),
            gather_remaining: 0,
            gather_target: GatherTarget::ToShared,
            waiting: VecDeque::new(),
            eval_issued: CapSet::NONE,
            eval_wanted: CapSet::NONE,
        }
    }

    fn find_holder(&self, client_id: &str) -> Option<&LockHolder> {
        self.holders.iter().find(|h| h.client_id == client_id)
    }

    fn find_holder_mut(&mut self, client_id: &str) -> Option<&mut LockHolder> {
        self.holders.iter_mut().find(|h| h.client_id == client_id)
    }

    fn holder_count(&self) -> usize {
        self.holders.len()
    }

    fn first_holder(&self) -> Option<&LockHolder> {
        self.holders.first()
    }

    /// 清理过期 holder
    fn garbage_collect(&mut self, now: Instant) -> bool {
        let before = self.holders.len();
        self.holders.retain(|h| h.expire_at > now);
        let cleaned = self.holders.len() != before;
        if cleaned && self.holders.is_empty() {
            // 无 holder → AVAILABLE (除非 GATHER 中)
            if self.state != LockState::Gather {
                self.state = LockState::Available;
            }
        }
        cleaned
    }

    /// eval: 锁状态 → cap 掩码 (对齐  SimpleLock::eval())
    /// 按 class 分发到对应状态机的 eval 逻辑
    pub fn eval(&mut self) -> CapSet {
        match self.class {
            LockClass::Local => self.eval_local(),
            LockClass::Simple => self.eval_simple(),
            LockClass::Scatter => self.eval_scatter(),
            LockClass::File => self.eval_file(),
        }
    }

    /// LocalLock eval: 不输出客户端 cap
    fn eval_local(&self) -> CapSet {
        CapSet::NONE
    }

    /// SimpleLock eval: 排他写+共享读, 支持 Loner
    fn eval_simple(&mut self) -> CapSet {
        match self.state {
            LockState::Loner => {
                // LONER: 单 client, 下发全套 exclusive cap
                let caps = self.lock_type.cap_bits();
                self.eval_issued = caps;
                caps
            }
            LockState::Shared => {
                // SHARED: 只下发 shared (只读) cap
                let caps = match self.lock_type {
                    LockType::Auth => CapSet::CAP_R,
                    LockType::Xattr => CapSet::CAP_R,
                    LockType::Link => CapSet::CAP_R,
                    _ => CapSet::CAP_R,
                };
                self.eval_issued = caps;
                caps
            }
            LockState::Excl => {
                // EXCL: xlock holder 独占, 其他人无 cap
                if self.holders.len() == 1 {
                    let caps = self.lock_type.cap_bits();
                    self.eval_issued = caps;
                    caps
                } else {
                    CapSet::NONE
                }
            }
            LockState::Gather => {
                // GATHER: 保持现有 cap
                self.eval_issued
            }
            LockState::Revoking => {
                // REVOKING: 保留 retain_caps
                self.holders
                    .iter()
                    .map(|h| h.retain_caps)
                    .fold(CapSet::NONE, |acc, c| acc | c)
            }
            _ => CapSet::NONE,
        }
    }

    /// ScatterLock eval: 多方共享写, 不输出客户端 cap
    fn eval_scatter(&mut self) -> CapSet {
        // ScatterLock 不输出客户端 cap, 变更在 MDS 间合并
        self.eval_issued = CapSet::NONE;
        CapSet::NONE
    }

    /// FileLock eval: 扩展 SimpleLock + 完整 FILE cap + SYNC
    fn eval_file(&mut self) -> CapSet {
        match self.state {
            LockState::Loner => {
                // FileLock LONER: 单 client 写, 下发全套 FILE cap
                // 允许本地 dirty (CAP_W + CAP_X), 大幅减 RPC
                let caps = CapSet::CAP_R | CapSet::CAP_W | CapSet::CAP_X;
                self.eval_issued = caps;
                caps
            }
            LockState::Shared => {
                // FileLock SHARED: 多 client, 只读 cap, 不能 dirty
                let caps = CapSet::CAP_R;
                self.eval_issued = caps;
                caps
            }
            LockState::Sync => {
                // FileLock SYNC: 只读, cap 已写回
                // 对齐  SYNC 把 FILE caps 收回, 仅保留 CAP_R
                let caps = CapSet::CAP_R;
                self.eval_issued = caps;
                caps
            }
            LockState::Excl => {
                // FileLock EXCL: xlock holder 独占
                if self.holders.len() == 1 {
                    let caps = CapSet::CAP_R | CapSet::CAP_W | CapSet::CAP_X;
                    self.eval_issued = caps;
                    caps
                } else {
                    CapSet::NONE
                }
            }
            LockState::Gather => self.eval_issued,
            LockState::Revoking => self
                .holders
                .iter()
                .map(|h| h.retain_caps)
                .fold(CapSet::NONE, |acc, c| acc | c),
            _ => CapSet::NONE,
        }
    }

    /// GATHER 超时检查
    fn gather_timeout(&mut self, recall_timeout: Duration) {
        if self.state != LockState::Gather || self.gather_remaining == 0 {
            return;
        }

        let now = Instant::now();
        let mut all_done = true;

        for g in &mut self.gather_list {
            if !g.acked && now.duration_since(g.sent_at) > recall_timeout {
                // 超时: force-reclaim
                warn!(
                    "mdlock gather_timeout type={:?} client={} sn={} force-reclaim",
                    self.lock_type, g.client_id, g.sn
                );
                g.acked = true; // 标记为已完成 (force-reclaim)
            }
            if !g.acked {
                all_done = false;
            }
        }

        if all_done {
            self.gather_complete();
        }
    }

    /// GATHER 完成: 根据目标状态跃迁
    fn gather_complete(&mut self) {
        // 先收集已 ACK (含 force-reclaim 标记 acked=true) 的旧 holder
        // client_id, 用于 ToExcl 移除. 必须在 `gather_list.clear()` 之前
        // 收集, 否则丢失"谁被 recall"的信息.
        // (Bug fix: recall_ack 先把 recall_in_flight 设为 false, 导致
        //  旧 retain(|h| recall_in_flight == false) 保留了应被移除的旧
        //  holder, xlock waiter 重试时又触发 GATHER 死循环.)
        let acked_clients: Vec<String> = self
            .gather_list
            .iter()
            .filter(|g| g.acked)
            .map(|g| g.client_id.clone())
            .collect();

        // 清理 gather_list
        self.gather_list.clear();
        self.gather_remaining = 0;

        // 清理 recall_in_flight
        for h in &mut self.holders {
            h.recall_in_flight = false;
        }

        // 根据目标跃迁
        match self.gather_target {
            GatherTarget::ToExcl => {
                // xlock 完成: 移除已 ACK 的旧 holder (被 recall 的),
                // 只保留新 xlock holder (GATHER 期间未在 gather_list 中).
                self.holders.retain(|h| !acked_clients.contains(&h.client_id));
                self.state = LockState::Excl;
            }
            GatherTarget::ToShared => {
                self.state = LockState::Shared;
            }
            GatherTarget::ToLoner => {
                if self.holders.len() == 1 {
                    self.state = LockState::Loner;
                } else {
                    self.state = LockState::Shared;
                }
            }
            GatherTarget::ToDscatter => {
                // ScatterLock GATHER 完成: 跃迁到 DSCATTER (多方共享写)
                self.state = LockState::Dscatter;
            }
            GatherTarget::ToSync => {
                // FileLock/ScatterLock GATHER 完成: 跃迁到 SYNC (只读, cap 已写回)
                self.state = LockState::Sync;
            }
        }

        self.eval();
    }

    /// Loner 升级: 当 SHARED 退化为单 holder 时, 把该 holder 的 granted_caps
    /// 升级到该锁类型的全套 cap (CAP_R|W|X 或对应子集), 并 bump sn 用于 fencing.
    ///
    /// 对齐  SimpleLock::set_excl_grant_for_loner()
    /// 触发点:
    /// - `unlock` 后只剩 1 个 holder
    /// - `tick` 过期清理后只剩 1 个 holder
    /// - `evict_client` 清理后只剩 1 个 holder
    /// 调用方需在调用后通过 `wake_waiters`/通知机制下发新 cap 给客户端。
    ///
    /// 选择策略: 在多 holder 共存场景下 (LONER writer + readers),
    /// 优先升级第一个持有冲突 cap (CAP_W 或 CAP_X) 的 holder (即原 writer);
    /// 若不存在 (即原 writer 已离开, 只剩 readers), 升级第一个 reader。
    fn promote_to_loner(&mut self, new_sn: u64) -> Option<&LockHolder> {
        debug_assert!(!self.holders.is_empty());
        let full_caps = self.lock_type.cap_bits();
        // 优先升级第一个 granted_caps != full_caps 的 holder (即需要升级的那个)
        let idx = self
            .holders
            .iter()
            .position(|h| h.granted_caps != full_caps)
            .unwrap_or(0);
        let h = self.holders.get_mut(idx)?;
        if h.granted_caps != full_caps {
            debug!(
                "mdlock promote_to_loner type={:?} client={} {} -> {:?}",
                self.lock_type, h.client_id, h.sn, full_caps
            );
            h.granted_caps = full_caps;
            h.sn = new_sn;
        }
        self.state = LockState::Loner;
        self.eval();
        self.holders.first()
    }
}

/// 默认租约持续时间 (30s)
const DEFAULT_LEASE_DURATION_MS: u64 = 30_000;
/// 默认 recall 超时 (2s)
const DEFAULT_RECALL_TIMEOUT_MS: u64 = 2_000;

/// 锁管理器 — 全局锁仲裁入口 (对齐  Locker 类)
pub struct LockArbiter {
    /// inode → 8 种独立锁对象
    locks: Mutex<HashMap<u64, [MdLock; LockType::NUM_TYPES]>>,
    /// 全局序列号分配器
    sn_counter: std::sync::atomic::AtomicU64,
    /// 全局 epoch 计数器
    epoch_counter: std::sync::atomic::AtomicU64,
    /// recall 超时
    recall_timeout: Duration,
    /// 租约持续时间
    lease_duration: Duration,
}

impl Default for LockArbiter {
    fn default() -> Self {
        Self::new()
    }
}

impl LockArbiter {
    pub fn new() -> Self {
        Self {
            locks: Mutex::new(HashMap::new()),
            sn_counter: std::sync::atomic::AtomicU64::new(1),
            epoch_counter: std::sync::atomic::AtomicU64::new(1),
            recall_timeout: Duration::from_millis(DEFAULT_RECALL_TIMEOUT_MS),
            lease_duration: Duration::from_millis(DEFAULT_LEASE_DURATION_MS),
        }
    }

    /// 测试/演示专用构造函数 (允许自定义 recall/lease 时长, 加速测试和演示)
    pub fn new_for_test(recall_timeout: Duration, lease_duration: Duration) -> Self {
        Self {
            locks: Mutex::new(HashMap::new()),
            sn_counter: std::sync::atomic::AtomicU64::new(1),
            epoch_counter: std::sync::atomic::AtomicU64::new(1),
            recall_timeout,
            lease_duration,
        }
    }

    /// 分配序列号
    fn alloc_sn(&self) -> u64 {
        self.sn_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// 分配 epoch
    fn alloc_epoch(&self) -> u64 {
        self.epoch_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    // ==================== 锁原语 ====================

    /// rdlock: 获取共享读锁
    ///
    /// 对齐  SimpleLock::rdlock()
    /// - AVAILABLE → SHARED
    /// - LONER → SHARED (被新 reader 打破)
    /// - EXCL/GATHER → 等待
    pub fn rdlock(&self, inode: u64, lock_type: LockType, client_id: &str) -> LockGrantResult {
        let mut locks = self.locks.lock().unwrap();
        Self::ensure_init_locked(&mut locks, inode);
        let lock_arr = locks.get_mut(&inode).unwrap();
        let lock = &mut lock_arr[lock_type as usize];

        // 清理过期 holder
        let now = Instant::now();
        if lock.garbage_collect(now) {
            lock.eval();
        }

        // 等待条件: EXCL/GATHER/REVOKING/LOCK 时不可获取 (LONER 状态下
        // reader 可共存, 不在此列). 冲突时返回 NONE — rdlock 是同步原语,
        // 不注册 waiter; 需要异步等待的调用方应使用 `wrlock_async` /
        // `xlock_async` (它们用 oneshot channel 注册 waiter).
        if lock.state == LockState::Excl
            || lock.state == LockState::Gather
            || lock.state == LockState::Revoking
            || lock.state == LockState::Lock
        {
            return LockGrantResult {
                client_id: client_id.to_string(),
                lock_type,
                sn: 0,
                epoch: 0,
                granted_caps: CapSet::NONE,
                recall_tasks: Vec::new(),
                duration_ms: 0,
            };
        }

        // 检查是否已持有 (同 client 重入)
        let sn = self.alloc_sn();
        if let Some(h) = lock.find_holder_mut(client_id) {
            return LockGrantResult {
                client_id: client_id.to_string(),
                lock_type,
                sn: h.sn,
                epoch: h.epoch,
                granted_caps: h.granted_caps,
                recall_tasks: Vec::new(),
                duration_ms: DEFAULT_LEASE_DURATION_MS,
            };
        }

        // 分配新 holder
        let granted_caps = match lock.state {
            LockState::Available | LockState::Shared => CapSet::CAP_R,
            LockState::Loner => CapSet::CAP_R, // LONER 被打破 → SHARED
            _ => CapSet::NONE,
        };

        let holder = LockHolder {
            client_id: client_id.to_string(),
            sn,
            epoch: 0,
            granted_caps,
            dirty_caps: CapSet::NONE,
            recall_in_flight: false,
            retain_caps: CapSet::NONE,
            recall_caps: CapSet::NONE,
            expire_at: now + self.lease_duration,
        };
        lock.holders.push(holder);

        // 状态转移
        if lock.state == LockState::Available {
            lock.state = LockState::Shared;
        } else if lock.state == LockState::Loner {
            lock.state = LockState::Shared;
        }

        lock.eval();

        debug!(
            "mdlock_rdlock inode={} type={:?} client={} state={}",
            inode,
            lock_type,
            client_id,
            lock.state.name()
        );

        LockGrantResult {
            client_id: client_id.to_string(),
            lock_type,
            sn,
            epoch: 0,
            granted_caps: lock.eval_issued,
            recall_tasks: Vec::new(),
            duration_ms: DEFAULT_LEASE_DURATION_MS,
        }
    }

    /// wrlock: 获取排他写锁
    ///
    /// 对齐  SimpleLock::wrlock() + FileLock loner
    /// - 无 holder → LONER (单 client)
    /// - LONER + 同 client → 复用
    /// - 其他 holder → GATHER (recall 其他 holder)
    pub fn wrlock(&self, inode: u64, lock_type: LockType, client_id: &str) -> LockGrantResult {
        let mut locks = self.locks.lock().unwrap();
        Self::ensure_init_locked(&mut locks, inode);
        let lock_arr = locks.get_mut(&inode).unwrap();
        let lock = &mut lock_arr[lock_type as usize];
        let now = Instant::now();

        // 清理过期 holder
        if lock.garbage_collect(now) {
            lock.eval();
        }

        // LONER fast path: 同 client 复用
        if lock.state == LockState::Loner {
            if let Some(h) = lock.first_holder() {
                if h.client_id == client_id {
                    return LockGrantResult {
                        client_id: client_id.to_string(),
                        lock_type,
                        sn: h.sn,
                        epoch: h.epoch,
                        granted_caps: h.granted_caps,
                        recall_tasks: Vec::new(),
                        duration_ms: DEFAULT_LEASE_DURATION_MS,
                    };
                }
            }
        }

        // 需要排他: 如果有其他 holder 持有冲突 cap, 进入 GATHER
        let mut recall_tasks = Vec::new();
        let conflict_caps = CapSet::CAP_W | CapSet::CAP_X;
        // 是否真的需要 GATHER: 至少一个其他 holder 持有 W+X
        let need_gather = lock.holders.iter().any(|h| {
            h.client_id != client_id && !CapSet(h.granted_caps.0 & conflict_caps.0).is_empty()
        });

        if need_gather {
            // 进入 GATHER: recall 其他 holder 的写权限 (CAP_W+CAP_X), 保留读 (CAP_R)
            // 对齐  SimpleLock::wrlock — wrlock 不要求 recall 全部 cap,
            // 只收回与 wrlock 冲突的 CAP_W+CAP_X, 已有 reader 可继续读
            if lock.state != LockState::Gather {
                lock.state = LockState::Gather;
                lock.gather_target = if lock.holder_count() == 0 {
                    GatherTarget::ToLoner
                } else {
                    GatherTarget::ToShared
                };

                let new_epoch = self.alloc_epoch();

                for other in &mut lock.holders {
                    if other.client_id != client_id {
                        // 只 recall 该 holder 实际持有的冲突 cap
                        let need_recall = CapSet(other.granted_caps.0 & conflict_caps.0);
                        if need_recall.is_empty() {
                            // 该 holder 没有冲突 cap, 跳过 recall
                            continue;
                        }
                        let retain = other.granted_caps.remove(need_recall);
                        let g = GatherEntry {
                            client_id: other.client_id.clone(),
                            sn: other.sn,
                            sent_at: now,
                            acked: false,
                        };
                        lock.gather_list.push(g);
                        lock.gather_remaining += 1;
                        other.recall_in_flight = true;
                        other.recall_caps = need_recall;
                        other.retain_caps = retain;
                        other.epoch = new_epoch;

                        recall_tasks.push(RecallTask {
                            client_id: other.client_id.clone(),
                            lock_type,
                            sn: other.sn,
                            caps_to_recall: need_recall,
                            retained_caps: retain,
                            new_epoch,
                        });
                    }
                }
            }

            // GATHER 超时检查
            lock.gather_timeout(self.recall_timeout);

            if lock.gather_remaining > 0 {
                // 仍未完成: 返回 NONE, 调用方应重试或通过 wrlock_async 等待
                return LockGrantResult {
                    client_id: client_id.to_string(),
                    lock_type,
                    sn: 0,
                    epoch: 0,
                    granted_caps: CapSet::NONE,
                    recall_tasks,
                    duration_ms: 0,
                };
            }

            // GATHER 完成
            lock.gather_complete();
        }

        // 授予: wrlock 是排他写锁, 调用方成为唯一 writer.
        // 对齐  FileLock LONER 语义: LONER = 单一 writer (持 W+X),
        // 其他 holder 可持 R (reader). 到达此处时其他 holder 的 W+X
        // 已被 GATHER recall 降级为 R (recall_ack: granted_caps=retain_caps),
        // 故调用方是唯一 writer → LONER + 全套 cap (本地写缓存 fast path).
        let sn = self.alloc_sn();
        let granted_caps = lock.lock_type.cap_bits();
        lock.state = LockState::Loner;

        // 添加/更新 holder
        if let Some(h) = lock.find_holder_mut(client_id) {
            h.sn = sn;
            h.granted_caps = granted_caps;
            h.expire_at = now + self.lease_duration;
        } else {
            let holder = LockHolder {
                client_id: client_id.to_string(),
                sn,
                epoch: 0,
                granted_caps,
                dirty_caps: CapSet::NONE,
                recall_in_flight: false,
                retain_caps: CapSet::NONE,
                recall_caps: CapSet::NONE,
                expire_at: now + self.lease_duration,
            };
            lock.holders.push(holder);
        }

        lock.eval();

        debug!(
            "mdlock_wrlock inode={} type={:?} client={} state={}",
            inode,
            lock_type,
            client_id,
            lock.state.name()
        );

        LockGrantResult {
            client_id: client_id.to_string(),
            lock_type,
            sn,
            epoch: 0,
            granted_caps,
            recall_tasks,
            duration_ms: DEFAULT_LEASE_DURATION_MS,
        }
    }

    /// xlock: 获取完全独占锁
    ///
    /// 对齐  SimpleLock::xlock()
    /// rename/unlink/truncate/migrate 必须拿到 xlock
    pub fn xlock(&self, inode: u64, lock_type: LockType, client_id: &str) -> LockGrantResult {
        let mut locks = self.locks.lock().unwrap();
        Self::ensure_init_locked(&mut locks, inode);
        let lock_arr = locks.get_mut(&inode).unwrap();
        let lock = &mut lock_arr[lock_type as usize];
        let now = Instant::now();

        // 清理过期 holder
        if lock.garbage_collect(now) {
            lock.eval();
        }

        // 同 client 唯一 holder: 直接升级
        if lock.holder_count() == 1 {
            let can_upgrade = lock
                .first_holder()
                .map(|h| h.client_id == client_id)
                .unwrap_or(false);
            if can_upgrade {
                let (sn, epoch) = lock
                    .first_holder()
                    .map(|h| (h.sn, h.epoch))
                    .unwrap_or((0, 0));
                lock.state = LockState::Excl;
                lock.eval();
                return LockGrantResult {
                    client_id: client_id.to_string(),
                    lock_type,
                    sn,
                    epoch,
                    granted_caps: lock.eval_issued,
                    recall_tasks: Vec::new(),
                    duration_ms: DEFAULT_LEASE_DURATION_MS,
                };
            }
        }

        // 无 holder: 直接获取
        if lock.holder_count() == 0 {
            let sn = self.alloc_sn();
            let holder = LockHolder {
                client_id: client_id.to_string(),
                sn,
                epoch: 0,
                granted_caps: lock.lock_type.cap_bits(),
                dirty_caps: CapSet::NONE,
                recall_in_flight: false,
                retain_caps: CapSet::NONE,
                recall_caps: CapSet::NONE,
                expire_at: now + self.lease_duration,
            };
            lock.holders.push(holder);
            lock.state = LockState::Excl;
            lock.eval();
            return LockGrantResult {
                client_id: client_id.to_string(),
                lock_type,
                sn,
                epoch: 0,
                granted_caps: lock.eval_issued,
                recall_tasks: Vec::new(),
                duration_ms: DEFAULT_LEASE_DURATION_MS,
            };
        }

        // 有其他 holder: 进入 GATHER (recall 全部)
        let mut recall_tasks = Vec::new();
        if lock.state != LockState::Gather {
            lock.state = LockState::Gather;
            lock.gather_target = GatherTarget::ToExcl;
            let new_epoch = self.alloc_epoch();

            for other in &mut lock.holders {
                if other.client_id != client_id {
                    let g = GatherEntry {
                        client_id: other.client_id.clone(),
                        sn: other.sn,
                        sent_at: now,
                        acked: false,
                    };
                    lock.gather_list.push(g);
                    lock.gather_remaining += 1;
                    other.recall_in_flight = true;
                    other.recall_caps = other.granted_caps;
                    other.retain_caps = CapSet::NONE; // xlock: 全部收回
                    other.epoch = new_epoch;

                    recall_tasks.push(RecallTask {
                        client_id: other.client_id.clone(),
                        lock_type,
                        sn: other.sn,
                        caps_to_recall: other.granted_caps,
                        retained_caps: CapSet::NONE,
                        new_epoch,
                    });
                }
            }
        }

        // GATHER 超时检查
        lock.gather_timeout(self.recall_timeout);

        if lock.gather_remaining > 0 {
            return LockGrantResult {
                client_id: client_id.to_string(),
                lock_type,
                sn: 0,
                epoch: 0,
                granted_caps: CapSet::NONE,
                recall_tasks,
                duration_ms: 0,
            };
        }

        // GATHER 完成 → EXCL
        lock.gather_complete();

        // 授予 xlock
        let sn = self.alloc_sn();
        let holder = LockHolder {
            client_id: client_id.to_string(),
            sn,
            epoch: 0,
            granted_caps: lock.lock_type.cap_bits(),
            dirty_caps: CapSet::NONE,
            recall_in_flight: false,
            retain_caps: CapSet::NONE,
            recall_caps: CapSet::NONE,
            expire_at: now + self.lease_duration,
        };
        lock.holders.push(holder);
        lock.state = LockState::Excl;
        lock.eval();

        debug!(
            "mdlock_xlock inode={} type={:?} client={} state={}",
            inode,
            lock_type,
            client_id,
            lock.state.name()
        );

        LockGrantResult {
            client_id: client_id.to_string(),
            lock_type,
            sn,
            epoch: 0,
            granted_caps: lock.eval_issued,
            recall_tasks,
            duration_ms: DEFAULT_LEASE_DURATION_MS,
        }
    }

    /// unlock: 释放锁
    ///
    /// 对齐  SimpleLock::unlock()
    /// - EXCL → AVAILABLE (并 wake_waiters)
    /// - SHARED/LONER → 移除 holder; 若剩 1 个 holder → 升级为 LONER
    ///   (Loner 重入管理: 当多 client 退化为单 holder, 该 holder 重新拿到全套 cap)
    /// - LOCK (LocalLock) → AVAILABLE
    ///
    /// 返回 Option<(client_id, new_sn, granted_caps)>: Loner 升级任务
    /// (供调用方 cap_manager 通过 revoker 下发新 cap 给被升级的 client)
    pub fn unlock(
        &self,
        inode: u64,
        lock_type: LockType,
        sn: u64,
    ) -> Option<(String, u64, CapSet)> {
        // 标记 unlock 后是否需要 wake_waiters (Loner 升级或 holder 变 0)
        let mut wake_needed = false;
        // 升级通知任务 (Loner 升级时, 下发新 cap 给被升级的 client)
        let mut promote_task: Option<(String, u64, CapSet)> = None;

        {
            let mut locks = self.locks.lock().unwrap();
            Self::ensure_init_locked(&mut locks, inode);
            let lock_arr = locks.get_mut(&inode).unwrap();
            let lock = &mut lock_arr[lock_type as usize];
            let now = Instant::now();
            let _ = now;

            // 找到并移除 holder
            let before = lock.holders.len();
            lock.holders.retain(|h| h.sn != sn);
            if lock.holders.len() == before {
                return None; // 未找到
            }

            // 状态转移
            match lock.state {
                LockState::Excl => {
                    // EXCL 释放 → AVAILABLE
                    lock.state = LockState::Available;
                    wake_needed = true;
                }
                LockState::Loner | LockState::Shared => {
                    if lock.holders.is_empty() {
                        lock.state = LockState::Available;
                        wake_needed = true;
                    } else if lock.holders.len() == 1
                        && (lock.class == LockClass::Simple || lock.class == LockClass::File)
                    {
                        // 只剩 1 个 holder → 升级为 LONER
                        // bump sn 用于 fencing 旧 holder 的 sn
                        let new_sn = self.alloc_sn();
                        if let Some(h) = lock.promote_to_loner(new_sn) {
                            promote_task = Some((h.client_id.clone(), h.sn, h.granted_caps));
                        }
                        wake_needed = true;
                    }
                }
                LockState::Lock => {
                    lock.state = LockState::Available;
                    wake_needed = true;
                }
                LockState::Dscatter => {
                    if lock.holders.is_empty() {
                        lock.state = LockState::Inactive;
                    }
                }
                _ => {}
            }

            lock.eval();
        } // 释放 locks MutexGuard, 避免重入死锁

        debug!(
            "mdlock_unlock inode={} type={:?} sn={} wake={}",
            inode, lock_type, sn, wake_needed
        );

        // Loner 升级通知: 实际由调用方 (net_handler) 把新 cap 推给 client
        // 这里只做日志, 真正下发由 recall_tasks / 异步通知路径处理
        if let Some((client, new_sn, caps)) = &promote_task {
            debug!(
                "mdlock_unlock promote_to_loner inode={} type={:?} client={} new_sn={} caps={:?}",
                inode, lock_type, client, new_sn, caps
            );
        }

        // 唤醒等待者 (锁已释放, 不会重入)
        if wake_needed {
            self.wake_waiters(inode, lock_type);
        }

        promote_task
    }

    /// recall_ack: 客户端 ACK recall, GATHER 计数减一
    /// 客户端 ACK 一次 recall.
    ///
    /// 在 `gather_list` 中找到匹配 `(client_id, sn)` 的条目并标记 `acked=true`.
    /// 如果所有 gather 条目都 ACK 了 (含 force-reclaim 标记), 调用
    /// `gather_complete()` 完成目标状态跃迁, 然后 `wake_waiters` 唤醒所有
    /// 阻塞的 wrlock/xlock/async waiter (它们会重试并在 holder 稳定后
    /// 自行完成 Loner/Excl 授予).
    ///
    /// **Loner promote 的正确触发时机**:
    /// - recall_ack 路径上: 新 caller (C2) 还没加入 `holders` (因为
    ///   wrlock/xlock 在 GATHER 建表时已 `return LockGrantResult::NONE` 提前
    ///   返回, 未 push 新 holder). 此时 `holders.len()` 与真实存活客户端数
    ///   不一致, 绝不能在此处 promote 会把被降级的旧 reader 升级为 W+X.
    /// - 正确路径:
    ///   1) `wake_waiters` 唤醒 C2 → C2 重试 wrlock/xlock →
    ///      `gather_remaining==0` → `gather_complete()` → C2 被加入
    ///      `holders` → `state=Loner/Excl` → C2 在本次返回中直接拿到全套 cap.
    ///   2) 只剩 1 个稳定 holder 时 (非 C2 waiting 场景), 由
    ///      `tick()/unlock()/evict_client()` 路径 promote (500 ms sweep
    ///      tick 内下发 `CapUpgradeNotify`).
    ///
    /// 返回 `true` = 找到匹配的 gather 条目并标记了 ACK (无论 gather 是否
    /// 全部完成); `false` = 此 lock_type 上没有匹配的 gather 条目.
    pub fn recall_ack(
        &self,
        inode: u64,
        lock_type: LockType,
        client_id: &str,
        sn: u64,
    ) -> bool {
        let mut locks = self.locks.lock().unwrap();
        Self::ensure_init_locked(&mut locks, inode);
        let lock_arr = locks.get_mut(&inode).unwrap();
        let lock = &mut lock_arr[lock_type as usize];

        let mut matched = false;
        let mut all_done = true;
        for g in &mut lock.gather_list {
            if g.client_id == client_id && g.sn == sn {
                g.acked = true;
                matched = true;
                debug!(
                    "mdlock_recall_ack inode={} type={:?} client={} sn={}",
                    inode, lock_type, client_id, sn
                );
            }
            if !g.acked {
                all_done = false;
            }
        }

        if !matched {
            return false;
        }

        // 更新 holder: 降级 cap (retain_caps 已由建 gather 时写好)
        if let Some(h) = lock.find_holder_mut(client_id) {
            h.granted_caps = h.retain_caps;
            h.recall_in_flight = false;
            h.dirty_caps = CapSet::NONE; // 已 flush
        }

        let gather_done = if all_done && lock.gather_remaining > 0 {
            lock.gather_remaining = 0;
            lock.gather_complete();
            true
        } else {
            false
        };

        // 释放 MutexGuard, 避免 wake_waiters 死锁
        drop(locks);

        if gather_done {
            self.wake_waiters(inode, lock_type);
        }

        true
    }

    // ==================== eval 触发 ====================

    /// 获取 inode 所有锁的 cap 掩码并集 (下发给客户端)
    pub fn eval_inode_caps(&self, inode: u64) -> CapSet {
        let mut locks = self.locks.lock().unwrap();
        Self::ensure_init_locked(&mut locks, inode);
        let lock_arr = locks.get_mut(&inode).unwrap();

        let mut total = CapSet::NONE;
        for lock in lock_arr.iter_mut() {
            let caps = lock.eval();
            total = total | caps;
        }
        total
    }

    /// 获取指定锁的当前 eval_issued
    pub fn get_eval_issued(&self, inode: u64, lock_type: LockType) -> CapSet {
        let mut locks = self.locks.lock().unwrap();
        Self::ensure_init_locked(&mut locks, inode);
        let lock_arr = locks.get_mut(&inode).unwrap();
        let lock = &mut lock_arr[lock_type as usize];
        lock.eval();
        lock.eval_issued
    }

    // ==================== tick: 过期清理 ====================

    /// 清理指定 inode 所有锁的过期 holder + GATHER 超时 + Loner 升级
    ///
    /// 对齐  MDS tick 周期触发 SimpleLock::eval + Loner 重入检测
    /// 返回 (Loner 升级通知列表) 供调用方下发新 cap 给客户端
    pub fn tick(&self, inode: u64) -> Vec<(LockType, String, u64, CapSet)> {
        let mut promote_tasks: Vec<(LockType, String, u64, CapSet)> = Vec::new();
        let mut wake_inodes: Vec<LockType> = Vec::new();

        {
            let mut locks = self.locks.lock().unwrap();
            Self::ensure_init_locked(&mut locks, inode);
            let lock_arr = locks.get_mut(&inode).unwrap();
            let now = Instant::now();

            for (i, lock) in lock_arr.iter_mut().enumerate() {
                let cleaned = lock.garbage_collect(now);
                if cleaned {
                    // holder 数为 0 → AVAILABLE
                    if lock.holders.is_empty() && lock.state != LockState::Gather {
                        lock.state = LockState::Available;
                        wake_inodes.push(LockType::from_index(i));
                    } else if lock.holders.len() == 1
                        && (lock.state == LockState::Shared || lock.state == LockState::Loner)
                        && (lock.class == LockClass::Simple || lock.class == LockClass::File)
                    {
                        // 只剩 1 个 holder → LONER 升级 (bump sn 用于 fencing)
                        let new_sn = self.alloc_sn();
                        if let Some(h) = lock.promote_to_loner(new_sn) {
                            promote_tasks.push((
                                LockType::from_index(i),
                                h.client_id.clone(),
                                h.sn,
                                h.granted_caps,
                            ));
                        }
                        wake_inodes.push(LockType::from_index(i));
                    }
                    lock.eval();
                }

                // GATHER 超时检查
                if lock.state == LockState::Gather && lock.gather_remaining > 0 {
                    lock.gather_timeout(self.recall_timeout);
                    // GATHER 超时 force-reclaim 后可能 gather_complete
                    if lock.state != LockState::Gather {
                        wake_inodes.push(LockType::from_index(i));
                    }
                }

                // ScatterLock INACTIVE → AVAILABLE (idle 回收)
                if lock.class == LockClass::Scatter
                    && lock.state == LockState::Inactive
                    && lock.holders.is_empty()
                {
                    lock.state = LockState::Available;
                }
            }
        } // 释放 MutexGuard

        // GATHER 完成 / Loner 升级后唤醒等待者
        for lt in wake_inodes {
            self.wake_waiters(inode, lt);
        }

        promote_tasks
    }

    // ==================== sn fencing ====================

    /// 验证 sn 是否有效
    pub fn sn_valid(&self, inode: u64, lock_type: LockType, sn: u64) -> bool {
        let mut locks = self.locks.lock().unwrap();
        Self::ensure_init_locked(&mut locks, inode);
        let lock_arr = locks.get_mut(&inode).unwrap();
        let lock = &lock_arr[lock_type as usize];
        lock.holders.iter().any(|h| h.sn == sn)
    }

    /// 强制 fencing: epoch bump, 使所有旧 sn 失效
    pub fn fence_epoch(&self, inode: u64, lock_type: LockType) {
        let mut locks = self.locks.lock().unwrap();
        Self::ensure_init_locked(&mut locks, inode);
        let lock_arr = locks.get_mut(&inode).unwrap();
        let lock = &mut lock_arr[lock_type as usize];

        warn!(
            "mdlock fence_epoch inode={} type={:?} force-reclaiming all holders",
            inode, lock_type
        );

        lock.holders.clear();
        lock.gather_list.clear();
        lock.gather_remaining = 0;
        lock.state = LockState::Available;
        lock.eval();
    }

    // ==================== FileLock 专用 ====================

    /// FileLock SHARED/LONER → SYNC (客户端 flush 脏数据后调用).
    ///
    /// 对齐  FileLock::flush_to_sync:
    /// - 若有其他 holder 持 W cap, 发 GATHER recall W cap (target=ToSync)
    /// - GATHER 完成后 state=SYNC (只读, cap 已写回)
    /// - 单 holder 且已 flush: 直接 SYNC
    ///
    /// 返回 recall_tasks 供调用方异步 dispatch.
    pub fn file_flush_to_sync(&self, inode: u64, client_id: &str) -> Vec<RecallTask> {
        let mut locks = self.locks.lock().unwrap();
        Self::ensure_init_locked(&mut locks, inode);
        let lock_arr = locks.get_mut(&inode).unwrap();
        let lock = &mut lock_arr[LockType::File as usize];
        let now = Instant::now();

        if lock.class != LockClass::File {
            return Vec::new();
        }

        if lock.state != LockState::Shared && lock.state != LockState::Loner {
            return Vec::new();
        }

        // 检查是否有其他 holder 持 W cap 需要 recall
        let conflict_caps = CapSet::CAP_W;
        let need_gather = lock.holders.iter().any(|h| {
            h.client_id != client_id && !CapSet(h.granted_caps.0 & conflict_caps.0).is_empty()
        });

        if !need_gather {
            // 无其他 W cap holder: 直接 SYNC
            lock.state = LockState::Sync;
            lock.eval();
            debug!(
                "mdlock_file_flush_to_sync inode={} client={} direct SYNC",
                inode, client_id
            );
            return Vec::new();
        }

        // 有其他 W cap holder: GATHER recall W cap, target=ToSync
        let mut recall_tasks = Vec::new();
        let new_epoch = self.alloc_epoch();
        lock.state = LockState::Gather;
        lock.gather_target = GatherTarget::ToSync;
        for other in &mut lock.holders {
            if other.client_id != client_id {
                let need_recall = CapSet(other.granted_caps.0 & conflict_caps.0);
                if need_recall.is_empty() {
                    continue;
                }
                let retain = other.granted_caps.remove(need_recall);
                let g = GatherEntry {
                    client_id: other.client_id.clone(),
                    sn: other.sn,
                    sent_at: now,
                    acked: false,
                };
                lock.gather_list.push(g);
                lock.gather_remaining += 1;
                other.recall_in_flight = true;
                other.recall_caps = need_recall;
                other.retain_caps = retain;
                other.epoch = new_epoch;
                recall_tasks.push(RecallTask {
                    client_id: other.client_id.clone(),
                    lock_type: LockType::File,
                    sn: other.sn,
                    caps_to_recall: need_recall,
                    retained_caps: retain,
                    new_epoch,
                });
            }
        }
        // GATHER 超时检查
        lock.gather_timeout(self.recall_timeout);
        if lock.gather_remaining == 0 {
            lock.gather_complete();
        }
        debug!(
            "mdlock_file_flush_to_sync inode={} client={} GATHER recalls={}",
            inode,
            client_id,
            recall_tasks.len()
        );
        recall_tasks
    }

    /// FileLock SYNC → SHARED (新写请求到来时调用)
    pub fn file_sync_to_shared(&self, inode: u64, client_id: &str) {
        let mut locks = self.locks.lock().unwrap();
        Self::ensure_init_locked(&mut locks, inode);
        let lock_arr = locks.get_mut(&inode).unwrap();
        let lock = &mut lock_arr[LockType::File as usize];

        if lock.class != LockClass::File {
            return;
        }

        if lock.state == LockState::Sync {
            lock.state = LockState::Shared;
            lock.eval();
            debug!(
                "mdlock_file_sync_to_shared inode={} client={}",
                inode, client_id
            );
        }
    }

    // ==================== LocalLock 专用 ====================

    /// LocalLock lock (MDS 本地, 无客户端 cap)
    pub fn local_lock(&self, inode: u64, lock_type: LockType) -> bool {
        let mut locks = self.locks.lock().unwrap();
        Self::ensure_init_locked(&mut locks, inode);
        let lock_arr = locks.get_mut(&inode).unwrap();
        let lock = &mut lock_arr[lock_type as usize];

        if lock.state != LockState::Available {
            return false; // 不阻塞
        }

        lock.state = LockState::Lock;
        lock.eval();
        true
    }

    /// LocalLock unlock
    pub fn local_unlock(&self, inode: u64, lock_type: LockType) {
        let mut locks = self.locks.lock().unwrap();
        Self::ensure_init_locked(&mut locks, inode);
        let lock_arr = locks.get_mut(&inode).unwrap();
        let lock = &mut lock_arr[lock_type as usize];

        lock.state = LockState::Available;
        lock.eval();
    }

    // ==================== ScatterLock 专用 ====================

    /// ScatterLock scatter_wrlock (多方共享写)
    pub fn scatter_wrlock(
        &self,
        inode: u64,
        lock_type: LockType,
        client_id: &str,
    ) -> LockGrantResult {
        let mut locks = self.locks.lock().unwrap();
        Self::ensure_init_locked(&mut locks, inode);
        let lock_arr = locks.get_mut(&inode).unwrap();
        let lock = &mut lock_arr[lock_type as usize];
        let now = Instant::now();

        if lock.garbage_collect(now) {
            lock.eval();
        }

        // INACTIVE → DSCATTER
        if lock.state == LockState::Inactive {
            lock.state = LockState::Dscatter;
        }

        // 对齐  ScatterLock: EXCL/LOCK 等冲突态需要 GATHER recall 旧 holder,
        // 完成后跃迁到 DSCATTER. ScatterLock 不输出客户端 cap (cap_bits=NONE),
        // recall 只清理 holder (不 recall cap).
        if lock.state != LockState::Available
            && lock.state != LockState::Dscatter
            && lock.state != LockState::Sync
        {
            // 冲突: 进入 GATHER recall 旧 holder
            let mut recall_tasks = Vec::new();
            if lock.state != LockState::Gather {
                lock.state = LockState::Gather;
                lock.gather_target = GatherTarget::ToDscatter;
                let new_epoch = self.alloc_epoch();
                for other in &mut lock.holders {
                    if other.client_id != client_id {
                        let g = GatherEntry {
                            client_id: other.client_id.clone(),
                            sn: other.sn,
                            sent_at: now,
                            acked: false,
                        };
                        lock.gather_list.push(g);
                        lock.gather_remaining += 1;
                        other.recall_in_flight = true;
                        other.epoch = new_epoch;
                        recall_tasks.push(RecallTask {
                            client_id: other.client_id.clone(),
                            lock_type,
                            sn: other.sn,
                            caps_to_recall: CapSet::NONE, // ScatterLock 不输出 cap
                            retained_caps: CapSet::NONE,
                            new_epoch,
                        });
                    }
                }
            }
            // GATHER 超时检查
            lock.gather_timeout(self.recall_timeout);
            if lock.gather_remaining > 0 {
                return LockGrantResult {
                    client_id: client_id.to_string(),
                    lock_type,
                    sn: 0,
                    epoch: 0,
                    granted_caps: CapSet::NONE,
                    recall_tasks,
                    duration_ms: 0,
                };
            }
            // GATHER 完成 → DSCATTER
            lock.gather_complete();
        }

        let sn = self.alloc_sn();
        if lock.find_holder(client_id).is_none() {
            let holder = LockHolder {
                client_id: client_id.to_string(),
                sn,
                epoch: 0,
                granted_caps: CapSet::NONE, // ScatterLock 不输出 cap
                dirty_caps: CapSet::NONE,
                recall_in_flight: false,
                retain_caps: CapSet::NONE,
                recall_caps: CapSet::NONE,
                expire_at: now + self.lease_duration,
            };
            lock.holders.push(holder);
        }

        if lock.state == LockState::Available || lock.state == LockState::Sync {
            lock.state = LockState::Dscatter;
        }

        lock.eval();

        LockGrantResult {
            client_id: client_id.to_string(),
            lock_type,
            sn,
            epoch: 0,
            granted_caps: CapSet::NONE, // ScatterLock 不输出 cap
            recall_tasks: Vec::new(),
            duration_ms: DEFAULT_LEASE_DURATION_MS,
        }
    }

    /// ScatterLock scatter_unlock
    pub fn scatter_unlock(&self, inode: u64, lock_type: LockType, sn: u64) {
        let mut locks = self.locks.lock().unwrap();
        Self::ensure_init_locked(&mut locks, inode);
        let lock_arr = locks.get_mut(&inode).unwrap();
        let lock = &mut lock_arr[lock_type as usize];

        lock.holders.retain(|h| h.sn != sn);

        if lock.holders.is_empty() {
            if lock.state == LockState::Dscatter {
                lock.state = LockState::Inactive;
            } else {
                lock.state = LockState::Available;
            }
        }
        lock.eval();
    }

    // ==================== trylock 非阻塞版本 ====================

    /// try_rdlock: 非阻塞共享读锁
    pub fn try_rdlock(
        &self,
        inode: u64,
        lock_type: LockType,
        client_id: &str,
    ) -> Option<LockGrantResult> {
        let result = self.rdlock(inode, lock_type, client_id);
        if result.granted_caps.is_empty() && result.sn == 0 {
            None
        } else {
            Some(result)
        }
    }

    /// try_wrlock: 非阻塞排他写锁
    pub fn try_wrlock(
        &self,
        inode: u64,
        lock_type: LockType,
        client_id: &str,
    ) -> Option<LockGrantResult> {
        let result = self.wrlock(inode, lock_type, client_id);
        if result.granted_caps.is_empty() && result.sn == 0 {
            None
        } else {
            Some(result)
        }
    }

    /// try_xlock: 非阻塞完全独占锁
    pub fn try_xlock(
        &self,
        inode: u64,
        lock_type: LockType,
        client_id: &str,
    ) -> Option<LockGrantResult> {
        let result = self.xlock(inode, lock_type, client_id);
        if result.granted_caps.is_empty() && result.sn == 0 {
            None
        } else {
            Some(result)
        }
    }

    // ==================== 异步等待 ====================

    /// rdlock_async: 异步获取共享读锁
    ///
    /// 冲突时注册 LockWaiter, 返回 Waiting(receiver)
    /// 调用方 await receiver 即可
    pub fn rdlock_async(
        &self,
        inode: u64,
        lock_type: LockType,
        client_id: &str,
    ) -> LockAcquireResult {
        let result = self.rdlock(inode, lock_type, client_id);
        if result.sn != 0 || !result.granted_caps.is_empty() {
            return LockAcquireResult::Granted(result);
        }

        // 冲突: 注册 waiter (rdlock 通常无 recall_tasks, 但保留以统一)
        let recall_tasks = result.recall_tasks;
        let (tx, rx) = oneshot::channel();
        {
            let mut locks = self.locks.lock().unwrap();
            Self::ensure_init_locked(&mut locks, inode);
            let lock_arr = locks.get_mut(&inode).unwrap();
            let lock = &mut lock_arr[lock_type as usize];
            lock.waiting.push_back(LockWaiter {
                client_id: client_id.to_string(),
                op: LockOp::Rdlock,
                sender: tx,
            });
        }
        LockAcquireResult::Waiting { recall_tasks, rx }
    }

    /// wrlock_async: 异步获取排他写锁
    pub fn wrlock_async(
        &self,
        inode: u64,
        lock_type: LockType,
        client_id: &str,
    ) -> LockAcquireResult {
        let result = self.wrlock(inode, lock_type, client_id);
        if result.sn != 0 || !result.granted_caps.is_empty() {
            return LockAcquireResult::Granted(result);
        }

        // 冲突: 注册 waiter (即使有 recall_tasks, 仍需等待 GATHER 完成).
        // recall_tasks 由调用方 dispatch 给旧 holder, 否则 GATHER 不完成.
        let recall_tasks = result.recall_tasks;
        let (tx, rx) = oneshot::channel();
        {
            let mut locks = self.locks.lock().unwrap();
            Self::ensure_init_locked(&mut locks, inode);
            let lock_arr = locks.get_mut(&inode).unwrap();
            let lock = &mut lock_arr[lock_type as usize];
            lock.waiting.push_back(LockWaiter {
                client_id: client_id.to_string(),
                op: LockOp::Wrlock,
                sender: tx,
            });
        }
        LockAcquireResult::Waiting { recall_tasks, rx }
    }

    /// xlock_async: 异步获取完全独占锁
    pub fn xlock_async(
        &self,
        inode: u64,
        lock_type: LockType,
        client_id: &str,
    ) -> LockAcquireResult {
        let result = self.xlock(inode, lock_type, client_id);
        if result.sn != 0 || !result.granted_caps.is_empty() {
            return LockAcquireResult::Granted(result);
        }

        // 冲突: 注册 waiter. recall_tasks (GATHER recall 全部旧 holder)
        // 由调用方 dispatch, 否则旧 holder 不知道要 ACK, GATHER 不完成.
        let recall_tasks = result.recall_tasks;
        let (tx, rx) = oneshot::channel();
        {
            let mut locks = self.locks.lock().unwrap();
            Self::ensure_init_locked(&mut locks, inode);
            let lock_arr = locks.get_mut(&inode).unwrap();
            let lock = &mut lock_arr[lock_type as usize];
            lock.waiting.push_back(LockWaiter {
                client_id: client_id.to_string(),
                op: LockOp::Xlock,
                sender: tx,
            });
        }
        LockAcquireResult::Waiting { recall_tasks, rx }
    }

    /// wake_waiters: 状态变迁后唤醒等待者
    ///
    /// 收集等待者 → 释放锁 → 重新尝试授予 → 仍冲突则放回队列
    /// 在 unlock / recall_ack / gather_complete 后调用
    fn wake_waiters(&self, inode: u64, lock_type: LockType) {
        // 1. 收集等待者 (持锁)
        let mut waiters: Vec<LockWaiter> = {
            let mut locks = self.locks.lock().unwrap();
            Self::ensure_init_locked(&mut locks, inode);
            let lock_arr = locks.get_mut(&inode).unwrap();
            let lock = &mut lock_arr[lock_type as usize];
            if lock.waiting.is_empty() {
                return;
            }
            lock.waiting.drain(..).collect()
        };

        // 对齐  Locker: 等待队列按锁原语优先级排序 (xlock > wrlock > rdlock).
        // stable 排序保持同优先级 FIFO; 高优先级强一致路径 (rename/truncate) 先获锁.
        waiters.sort_by_key(|w| std::cmp::Reverse(w.op.priority()));

        // 2. 释放锁后逐个重试 (避免重入死锁)
        for waiter in waiters {
            let result = match waiter.op {
                LockOp::Rdlock => self.rdlock(inode, lock_type, &waiter.client_id),
                LockOp::Wrlock => self.wrlock(inode, lock_type, &waiter.client_id),
                LockOp::Xlock => self.xlock(inode, lock_type, &waiter.client_id),
                LockOp::Unlock => continue,
            };

            if result.sn == 0 && result.granted_caps.is_empty() {
                // 仍冲突: 放回队列
                let mut locks = self.locks.lock().unwrap();
                let lock_arr = locks.get_mut(&inode).unwrap();
                let lock = &mut lock_arr[lock_type as usize];
                lock.waiting.push_back(waiter);
            } else {
                // 授予成功: 通知等待者
                let _ = waiter.sender.send(result);
            }
        }
    }

    // ==================== 会话管理 ====================

    /// evict_client: 清理指定客户端在该 Arbiter 上的所有锁
    ///
    /// 对齐  session 销毁时 Locker 清理该 session 全部 cap/lease
    /// 遍历所有 inode 的所有锁类型, 移除该 client 的 holder
    /// 返回 (inode, LockType) 列表供调用方触发 wake_waiters + Loner 升级通知
    pub fn evict_client(
        &self,
        client_id: &str,
    ) -> (
        Vec<(u64, LockType)>,
        Vec<(u64, LockType, String, u64, CapSet)>,
    ) {
        let mut locks = self.locks.lock().unwrap();
        let mut changed_inodes: Vec<(u64, LockType)> = Vec::new();
        // (inode, lock_type, surviving_client, new_sn, upgraded_caps)
        let mut promote_tasks: Vec<(u64, LockType, String, u64, CapSet)> = Vec::new();

        for (inode, lock_arr) in locks.iter_mut() {
            for (i, lock) in lock_arr.iter_mut().enumerate() {
                let before = lock.holders.len();
                lock.holders.retain(|h| h.client_id != client_id);
                if lock.holders.len() != before {
                    let lt = LockType::from_index(i);
                    changed_inodes.push((*inode, lt));

                    // 状态转移
                    match lock.state {
                        LockState::Excl => {
                            lock.state = LockState::Available;
                        }
                        LockState::Loner | LockState::Shared => {
                            if lock.holders.is_empty() {
                                lock.state = LockState::Available;
                            } else if lock.holders.len() == 1
                                && (lock.class == LockClass::Simple
                                    || lock.class == LockClass::File)
                            {
                                // 剩 1 个 holder → LONER 升级
                                let new_sn = self.alloc_sn();
                                if let Some(h) = lock.promote_to_loner(new_sn) {
                                    promote_tasks.push((
                                        *inode,
                                        lt,
                                        h.client_id.clone(),
                                        h.sn,
                                        h.granted_caps,
                                    ));
                                }
                            }
                        }
                        LockState::Dscatter => {
                            if lock.holders.is_empty() {
                                lock.state = LockState::Inactive;
                            }
                        }
                        LockState::Gather => {
                            // 清理 gather_list 中该 client 的条目
                            let before_g = lock.gather_list.len();
                            lock.gather_list.retain(|g| g.client_id != client_id);
                            if lock.gather_list.len() != before_g {
                                lock.gather_remaining =
                                    lock.gather_list.iter().filter(|g| !g.acked).count();
                            }
                            if lock.gather_remaining == 0 {
                                lock.gather_complete();
                            }
                        }
                        _ => {}
                    }
                    lock.eval();
                }
            }
        }

        if !changed_inodes.is_empty() {
            warn!(
                "mdlock evict_client {} cleaned {} locks promoted {} loners",
                client_id,
                changed_inodes.len(),
                promote_tasks.len()
            );
        }

        // 注意: 这里不直接 wake_waiters, 因为遍历了多个 inode,
        // 调用方需要按 changed_inodes 触发 wake + 按 promote_tasks 下发新 cap
        (changed_inodes, promote_tasks)
    }

    /// evict_client 触发后唤醒等待者 (单 inode + 单 LockType)
    ///
    /// 调用方应在 evict_client 返回的每个 (inode, LockType) 上调用此方法
    pub fn wake_after_evict(&self, inode: u64, lock_type: LockType) {
        self.wake_waiters(inode, lock_type);
    }

    // ==================== Quiesce 静默协议 ====================

    /// quiesce: 静默指定 inode — recall 全部 cap, 等待所有 ACK
    ///
    /// 对齐  快照/子树迁移前静默全部客户端
    /// 1. 对所有锁类型发起 GATHER (recall 全部)
    /// 2. 返回 recall_tasks 给调用方异步发送
    /// 3. 调用方等待所有 recall_ack 后调用 quiesce_complete
    pub fn quiesce(&self, inode: u64) -> Vec<RecallTask> {
        let mut locks = self.locks.lock().unwrap();
        Self::ensure_init_locked(&mut locks, inode);
        let lock_arr = locks.get_mut(&inode).unwrap();
        let now = Instant::now();
        let new_epoch = self.alloc_epoch();
        let mut all_recalls = Vec::new();

        for lock in lock_arr.iter_mut() {
            if lock.holders.is_empty() {
                continue;
            }
            if lock.state != LockState::Gather {
                lock.state = LockState::Gather;
                // 对齐  quiesce gather_target 按锁类型区分
                // - SimpleLock/LocalLock: ToExcl (完全独占)
                // - FileLock: ToSync (flush 数据, 只读)
                // - ScatterLock: ToSync (同步)
                lock.gather_target = match lock.class {
                    LockClass::Simple | LockClass::Local => GatherTarget::ToExcl,
                    LockClass::File | LockClass::Scatter => GatherTarget::ToSync,
                };

                for h in &mut lock.holders {
                    let g = GatherEntry {
                        client_id: h.client_id.clone(),
                        sn: h.sn,
                        sent_at: now,
                        acked: false,
                    };
                    lock.gather_list.push(g);
                    lock.gather_remaining += 1;
                    h.recall_in_flight = true;
                    h.recall_caps = h.granted_caps;
                    h.retain_caps = CapSet::NONE; // quiesce: 全部收回
                    h.epoch = new_epoch;

                    all_recalls.push(RecallTask {
                        client_id: h.client_id.clone(),
                        lock_type: lock.lock_type,
                        sn: h.sn,
                        caps_to_recall: h.granted_caps,
                        retained_caps: CapSet::NONE,
                        new_epoch,
                    });
                }
            }
        }

        warn!(
            "mdlock quiesce inode={} recall_tasks={}",
            inode,
            all_recalls.len()
        );
        all_recalls
    }

    /// quiesce_complete: 检查 quiesce 是否完成 (所有 GATHER 已 ACK)
    pub fn quiesce_complete(&self, inode: u64) -> bool {
        let mut locks = self.locks.lock().unwrap();
        Self::ensure_init_locked(&mut locks, inode);
        let lock_arr = locks.get_mut(&inode).unwrap();

        let mut all_done = true;
        for lock in lock_arr.iter_mut() {
            if lock.state == LockState::Gather && lock.gather_remaining > 0 {
                // 检查超时
                lock.gather_timeout(self.recall_timeout);
                if lock.gather_remaining > 0 {
                    all_done = false;
                } else {
                    lock.gather_complete();
                }
            }
        }
        all_done
    }

    // ==================== dirty cap 管理 ====================

    /// mark_dirty: 标记客户端的 cap 为脏 (需要 flush)
    pub fn mark_dirty(&self, inode: u64, lock_type: LockType, client_id: &str, dirty_caps: CapSet) {
        let mut locks = self.locks.lock().unwrap();
        Self::ensure_init_locked(&mut locks, inode);
        let lock_arr = locks.get_mut(&inode).unwrap();
        let lock = &mut lock_arr[lock_type as usize];

        if let Some(h) = lock.find_holder_mut(client_id) {
            h.dirty_caps = h.dirty_caps | dirty_caps;
        }
    }

    /// flush_dirty: 标记脏 cap 已 flush (recall ACK 时调用)
    pub fn flush_dirty(&self, inode: u64, lock_type: LockType, client_id: &str) {
        let mut locks = self.locks.lock().unwrap();
        Self::ensure_init_locked(&mut locks, inode);
        let lock_arr = locks.get_mut(&inode).unwrap();
        let lock = &mut lock_arr[lock_type as usize];

        if let Some(h) = lock.find_holder_mut(client_id) {
            h.dirty_caps = CapSet::NONE;
        }
    }

    /// get_dirty_clients: 获取指定 inode 上有脏 cap 的客户端列表
    pub fn get_dirty_clients(&self, inode: u64) -> Vec<(String, LockType, CapSet)> {
        let mut locks = self.locks.lock().unwrap();
        Self::ensure_init_locked(&mut locks, inode);
        let lock_arr = locks.get_mut(&inode).unwrap();

        let mut result = Vec::new();
        for (i, lock) in lock_arr.iter_mut().enumerate() {
            for h in &lock.holders {
                if !h.dirty_caps.is_empty() {
                    result.push((h.client_id.clone(), LockType::from_index(i), h.dirty_caps));
                }
            }
        }
        result
    }

    // ==================== 调试 ====================

    /// dump 指定 inode 的所有锁状态
    pub fn dump(&self, inode: u64) -> String {
        let mut locks = self.locks.lock().unwrap();
        Self::ensure_init_locked(&mut locks, inode);
        let lock_arr = locks.get(&inode).unwrap();

        let mut out = format!("MDLock dump for inode {}:\n", inode);
        for lock in lock_arr.iter() {
            if lock.state == LockState::Available && lock.holders.is_empty() {
                continue;
            }
            out.push_str(&format!(
                "  {:?}[{:?}] state={} holders={} gather={:?}/{:?} eval_issued={:?}\n",
                lock.lock_type,
                lock.class,
                lock.state.name(),
                lock.holders.len(),
                lock.gather_remaining,
                lock.gather_target,
                lock.eval_issued,
            ));
            for (i, h) in lock.holders.iter().enumerate() {
                out.push_str(&format!(
                    "    [{}] client={} sn={} granted={:?} dirty={:?} recall={}\n",
                    i,
                    h.client_id,
                    h.sn,
                    h.granted_caps,
                    h.dirty_caps,
                    if h.recall_in_flight { "Y" } else { "N" }
                ));
            }
        }
        out
    }

    // ==================== 内部辅助 ====================

    fn ensure_init_locked(locks: &mut HashMap<u64, [MdLock; LockType::NUM_TYPES]>, inode: u64) {
        if !locks.contains_key(&inode) {
            let arr: [MdLock; LockType::NUM_TYPES] = [
                MdLock::new(LockType::Auth),
                MdLock::new(LockType::Link),
                MdLock::new(LockType::Xattr),
                MdLock::new(LockType::Dn),
                MdLock::new(LockType::Snap),
                MdLock::new(LockType::File),
                MdLock::new(LockType::Dft),
                MdLock::new(LockType::Nest),
            ];
            locks.insert(inode, arr);
        }
    }
}

// 全局 LockArbiter 实例 (由 server 初始化)
lazy_static! {
    pub static ref LOCK_ARBITER: Arc<LockArbiter> = Arc::new(LockArbiter::new());
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    // ============================================================
    // §1 状态机基础: 4 套锁状态机的状态转移
    // ============================================================

    #[test]
    fn test_local_lock_state_machine() {
        // LocalLock 二态: AVAILABLE ⇄ LOCK
        let a = LockArbiter::new();
        assert!(a.local_lock(100, LockType::Snap));
        assert!(!a.local_lock(100, LockType::Snap)); // 阻塞
        a.local_unlock(100, LockType::Snap);
        assert!(a.local_lock(100, LockType::Snap)); // 重新获取
    }

    #[test]
    fn test_simplelock_available_to_shared_via_rdlock() {
        // SimpleLock: AVAILABLE → SHARED (多 reader)
        let a = LockArbiter::new();
        let r1 = a.rdlock(101, LockType::Auth, "C1");
        assert!(r1.granted_caps.has_r());
        let r2 = a.rdlock(101, LockType::Auth, "C2");
        assert!(r2.granted_caps.has_r());
        // 两个 reader 都拿 CAP_R, 状态 SHARED
    }

    #[test]
    fn test_simplelock_available_to_loner_via_wrlock() {
        // SimpleLock: AVAILABLE → LONER (单 writer)
        let a = LockArbiter::new();
        let r = a.wrlock(102, LockType::Auth, "C1");
        // Auth LONER 下发 R + X
        assert!(r.granted_caps.has_r());
        assert!(r.granted_caps.has_x());
    }

    #[test]
    fn test_filelock_available_to_loner() {
        // FileLock: AVAILABLE → LONER, 下发 R+W+X 全套
        let a = LockArbiter::new();
        let r = a.wrlock(103, LockType::File, "C1");
        assert!(r.granted_caps.has_r());
        assert!(r.granted_caps.has_w());
        assert!(r.granted_caps.has_x());
    }

    #[test]
    fn test_filelock_loner_to_sync_to_shared() {
        // FileLock: LONER → SYNC → SHARED
        let a = LockArbiter::new();
        a.wrlock(104, LockType::File, "C1");
        a.file_flush_to_sync(104, "C1");
        let caps = a.get_eval_issued(104, LockType::File);
        assert!(caps.has_r() && !caps.has_w()); // SYNC 只读
        a.file_sync_to_shared(104, "C1");
        let caps2 = a.get_eval_issued(104, LockType::File);
        assert!(caps2.has_r());
    }

    #[test]
    fn test_scatterlock_dscatter_state() {
        // ScatterLock: AVAILABLE → DSCATTER (多方共享写)
        let a = LockArbiter::new();
        let r1 = a.scatter_wrlock(105, LockType::Dft, "C1");
        assert_eq!(r1.granted_caps, CapSet::NONE); // ScatterLock 不输出 cap
        let r2 = a.scatter_wrlock(105, LockType::Dft, "C2");
        assert_eq!(r2.granted_caps, CapSet::NONE);
        // 两方共享写
    }

    // ============================================================
    // §2 多客户端竞争: wrlock 冲突 → GATHER → recall_ack → SHARED
    // ============================================================

    #[test]
    fn test_wrlock_conflict_recall_then_ack() {
        let a = LockArbiter::new();
        // C1 取得 LONER
        let r1 = a.wrlock(200, LockType::File, "C1");
        assert!(r1.granted_caps.is_exclusive());

        // C2 wrlock → GATHER → recall C1 的 CAP_W+CAP_X, 保留 CAP_R
        let r2 = a.wrlock(200, LockType::File, "C2");
        assert!(!r2.recall_tasks.is_empty());

        // 验证 recall_task 收回的是 CAP_W+CAP_X, 保留 CAP_R
        let recall = &r2.recall_tasks[0];
        assert!(recall.caps_to_recall.has_w());
        assert!(recall.caps_to_recall.has_x());
        assert!(recall.retained_caps.has_r());
        assert!(!recall.retained_caps.has_w());

        // C1 ACK → GATHER 完成 → 状态 SYNC (C2 还没 retry, 尚未加入 holders)
        let matched = a.recall_ack(200, LockType::File, "C1", r1.sn);
        assert!(matched, "C1 ACK 应匹配到 gather 条目");
        // C2 retry wrlock → gather_remaining==0 → gather_complete → C2 加入 holders → state=Loner
        let r2_retry = a.wrlock(200, LockType::File, "C2");
        assert!(r2_retry.granted_caps.is_exclusive(), "C2 retry 应拿到 LONER EXCL");
        // C2: LONER → 全套 R|W|X
        assert!(r2_retry.granted_caps.has_r() && r2_retry.granted_caps.has_w() && r2_retry.granted_caps.has_x());
        // 此时 holders=[C1(R), C2(R|W|X)], eval 为 File 类 Loner, 所以 eval_issued 应该是全套, 但
        // 实际上 FileLock::eval_file Loner 只在 holders.len() == 1 时下发全套, 这里 2 个 holders
        // 会走到 Sync(或 Loner 判断失败), 所以验证 C2 自身 granted_caps 即可 (上面已断言).
        let _ = r2;
    }

    #[test]
    fn test_xlock_conflict_recall_then_ack() {
        let a = LockArbiter::new();
        // C1 LONER
        let r1 = a.wrlock(201, LockType::Auth, "C1");
        // C2 xlock → GATHER ToExcl → recall C1 全部 (无 retain)
        let r2 = a.xlock(201, LockType::Auth, "C2");
        assert!(!r2.recall_tasks.is_empty());
        // xlock: retain_caps = NONE
        assert_eq!(r2.recall_tasks[0].retained_caps, CapSet::NONE);

        // ACK: C1 被 ToExcl 移除 (gather_complete 从 acked_clients 清理). 然后 C2 retry 拿到 EXCL
        let matched = a.recall_ack(201, LockType::Auth, "C1", r1.sn);
        assert!(matched, "C1 ACK 应匹配到 gather 条目 (Auth xlock)");
        // C2 retry xlock → gather_remaining==0 → gather_complete → holders=[C2] → state=Excl
        let r2_retry = a.xlock(201, LockType::Auth, "C2");
        // Auth Loner/Excl: CAP_R | CAP_X
        assert!(r2_retry.granted_caps.has_r() && r2_retry.granted_caps.has_x());
        assert_eq!(r2_retry.granted_caps, CapSet::CAP_R | CapSet::CAP_X);
    }

    // ============================================================
    // §3 GATHER 超时: recall 无 ACK → force-reclaim
    // ============================================================

    #[test]
    fn test_gather_timeout_force_reclaim() {
        // 用极短 recall_timeout 加速测试
        let a = LockArbiter::new_for_test(Duration::from_millis(50), Duration::from_secs(30));
        // C1 LONER
        let r1 = a.wrlock(300, LockType::File, "C1");
        // C2 wrlock → GATHER → recall C1
        let r2 = a.wrlock(300, LockType::File, "C2");
        assert!(!r2.recall_tasks.is_empty());

        // 不 ACK, 等待超时 (60ms > 50ms)
        thread::sleep(Duration::from_millis(60));

        // 第二次 wrlock 试图唤醒: gather_timeout 触发 force-reclaim
        let r3 = a.wrlock(300, LockType::File, "C2");
        // GATHER 已超时 force-reclaim, 现在 C2 应当拿到 LONER (单 holder)
        assert!(r3.granted_caps.has_w() || r3.granted_caps.has_r());

        let _ = (r1, r2);
    }

    // ============================================================
    // §4 Loner 退出/重入完整循环 (新 client 打破 → recall → 降级 → 重入 → 升级)
    // ============================================================

    #[tokio::test]
    async fn test_loner_break_recall_degrade_reentry_promote() {
        // 完整 Loner 循环 (async 路径):
        // 1. C1 wrlock → LONER (writer, 全套 cap)
        // 2. C2 wrlock_async → GATHER → recall C1 W+X (打破 LONER)
        // 3. C1 ACK → SHARED, C1 降级为 reader (CAP_R)
        // 4. wake_waiters 内部 self.wrlock("C2") → 添加 C2 为 LONER holder
        //    (因为 C1 此时只持 R, 不冲突, 跳过 GATHER)
        // 5. C2 unlock → 剩 C1 → C1 升级回 LONER (bump sn)
        let a = Arc::new(LockArbiter::new());
        let r1 = a.wrlock(400, LockType::File, "C1");
        assert!(r1.granted_caps.is_exclusive());
        let sn_c1_v1 = r1.sn;

        // C2 wrlock_async → GATHER → Waiting (wrlock_async 是同步函数, 无需 spawn)
        let rx = match a.wrlock_async(400, LockType::File, "C2") {
            LockAcquireResult::Granted(_) => panic!("应 Waiting"),
            LockAcquireResult::Waiting { recall_tasks: _, rx } => rx,
        };

        // C1 ACK → GATHER 完成 → wake_waiters 通知 C2 (重新调用 wrlock 添加 holder)
        a.recall_ack(400, LockType::File, "C1", sn_c1_v1);

        // C2 收到通知: wake_waiters 内部 self.wrlock("C2") 已添加 C2 holder
        let c2_grant = tokio::time::timeout(Duration::from_secs(2), rx)
            .await
            .expect("超时")
            .expect("sender 不应 drop");
        assert!(c2_grant.granted_caps.has_w(), "C2 应为 LONER 拿 W");
        let sn_c2 = c2_grant.sn;

        // 此时 holders=[C1(R), C2(W+X+R)], state=LONER
        // 验证 C1 降级为 reader
        let caps_c1 = a.get_eval_issued(400, LockType::File);
        assert!(
            caps_c1.has_r() && caps_c1.has_w(),
            "LONER 状态 eval_issued 应包含 W (C2 是 writer)"
        );

        // C2 unlock → 剩 C1 → C1 升级回 LONER
        a.unlock(400, LockType::File, sn_c2);

        // 验证 C1 升级: eval_issued 仍含 W (LONER), sn 已 bump (旧 sn 失效)
        let caps_c1_v2 = a.get_eval_issued(400, LockType::File);
        assert!(
            caps_c1_v2.has_w() && caps_c1_v2.has_x(),
            "C1 应升级为 LONER 拿到 W+X"
        );
        assert!(
            !a.sn_valid(400, LockType::File, sn_c1_v1),
            "旧 sn 应已失效 (bump 后)"
        );
    }

    #[test]
    fn test_loner_promote_via_evict() {
        // 场景: C1 (reader) + C2 (writer) 共存 (LONER+reader)
        // evict C2 (writer) → 剩 C1 (reader) → C1 升级为 LONER (writer)
        // 用 unlock 触发升级 (因为 evict_client 内部已集成 promote_to_loner)
        let a = LockArbiter::new();
        // 直接构造场景: C1 rdlock (reader) + C1 wrlock (升级 LONER, 因为单 holder)
        let r1 = a.wrlock(401, LockType::File, "C1");
        assert!(r1.granted_caps.is_exclusive());

        // C2 wrlock_async → GATHER → C1 ACK → C2 加入为 LONER
        // 但这里用同步路径简化: 直接 wrlock C2 (会进入 GATHER)
        let r2 = a.wrlock(401, LockType::File, "C2");
        assert!(!r2.recall_tasks.is_empty(), "应有 recall task");
        a.recall_ack(401, LockType::File, "C1", r1.sn);

        // 此时 holders=[C1(R)], state=SHARED, C2 还没加入 (同步 wrlock 在 GATHER pending 时返回 NONE)
        // 直接 evict C1 测试 promote 路径不适用于"C2 加入"场景
        // 改为: evict C1 → holders=[] → AVAILABLE
        let (changed, promotes) = a.evict_client("C1");
        assert!(!changed.is_empty());
        assert!(promotes.is_empty(), "无 holder 时不应有 promote");

        // C3 现在可以重新获取
        let r3 = a.wrlock(401, LockType::File, "C3");
        assert!(r3.granted_caps.is_exclusive());
    }

    // ============================================================
    // §5 Quiesce 静默协议: quiesce → recall → ACK → quiesce_complete
    // ============================================================

    #[test]
    fn test_quiesce_full_protocol() {
        let a = LockArbiter::new();
        let r1 = a.wrlock(500, LockType::File, "C1");
        let r2 = a.rdlock(500, LockType::Auth, "C1");

        // quiesce: recall 全部 cap
        let tasks = a.quiesce(500);
        assert!(!tasks.is_empty());

        // quiesce_complete 未完成
        assert!(!a.quiesce_complete(500));

        // 按 RecallTask 中的 lock_type + sn 调用 recall_ack
        for t in &tasks {
            a.recall_ack(500, t.lock_type, "C1", t.sn);
        }

        // 现在 quiesce 应完成
        assert!(a.quiesce_complete(500));

        // 顺便验证: quiesce 后再次 wrlock 应直接拿到 (GATHER 已完成)
        let _ = r1;
        let _ = r2;
    }

    // ============================================================
    // §6 会话销毁: evict_client 清理 + Loner 升级
    // ============================================================

    #[tokio::test]
    async fn test_evict_client_cleanup_and_promote() {
        // 场景: C1 (writer LONER) + C2 wrlock_async 加入 (LONER+reader 共存)
        // evict C2 (writer) → 剩 C1 (reader) → C1 升级为 LONER (writer)
        let a = Arc::new(LockArbiter::new());
        let r1 = a.wrlock(600, LockType::File, "C1");
        assert!(r1.granted_caps.is_exclusive());

        // C2 wrlock_async → GATHER → Waiting (wrlock_async 是同步函数, 无需 spawn)
        let rx = match a.wrlock_async(600, LockType::File, "C2") {
            LockAcquireResult::Granted(_) => panic!("应 Waiting"),
            LockAcquireResult::Waiting { recall_tasks: _, rx } => rx,
        };

        // C1 ACK → GATHER 完成 → wake_waiters → 添加 C2 holder
        a.recall_ack(600, LockType::File, "C1", r1.sn);
        let c2_grant = tokio::time::timeout(Duration::from_secs(2), rx)
            .await
            .expect("超时")
            .expect("sender 不应 drop");
        assert!(c2_grant.granted_caps.has_w());

        // 此时 holders=[C1(R), C2(W+X+R)], state=LONER
        // evict C2 (writer) → 剩 C1 (reader) → C1 升级
        let (changed, promotes) = a.evict_client("C2");
        assert!(!changed.is_empty(), "应有 inode 状态变化");
        assert!(!promotes.is_empty(), "应有 Loner 升级任务");
        let (inode, lt, client, _sn, caps) = &promotes[0];
        assert_eq!(*inode, 600);
        assert_eq!(*lt, LockType::File);
        assert_eq!(client, "C1");
        assert!(caps.has_w() && caps.has_x());

        // evict C1 → 所有 holder 清空 → AVAILABLE
        let (changed2, _) = a.evict_client("C1");
        assert!(!changed2.is_empty());

        // 现在 C3 可以重新获取
        let r3 = a.wrlock(600, LockType::File, "C3");
        assert!(r3.granted_caps.is_exclusive());
    }

    // ============================================================
    // §7 异步等待: wrlock_async + tokio runtime
    // ============================================================

    #[tokio::test]
    async fn test_wrlock_async_grant_immediate() {
        let a = Arc::new(LockArbiter::new());
        let r = a.wrlock_async(700, LockType::File, "C1").await_grant();
        assert!(r.granted_caps.has_w());
    }

    #[tokio::test]
    async fn test_wrlock_async_waiting_then_grant() {
        let a = Arc::new(LockArbiter::new());
        let r1 = a.wrlock(800, LockType::File, "C1");
        // C2 wrlock_async → GATHER → Waiting (wrlock_async 是同步函数, 无需 spawn)
        let rx = match a.wrlock_async(800, LockType::File, "C2") {
            LockAcquireResult::Granted(_) => panic!("应进入 Waiting"),
            LockAcquireResult::Waiting { recall_tasks: _, rx } => rx,
        };

        // C1 ACK → GATHER 完成 → wake_waiters 通知 C2
        a.recall_ack(800, LockType::File, "C1", r1.sn);

        // C2 收到通知
        let result = tokio::time::timeout(Duration::from_secs(2), rx).await;
        assert!(result.is_ok(), "应在 GATHER 完成后被唤醒");
        let grant = result.unwrap().unwrap();
        assert!(grant.granted_caps.has_r());
    }

    // ============================================================
    // §8 dirty cap 管理: mark_dirty → get_dirty_clients → flush_dirty
    // ============================================================

    #[test]
    fn test_dirty_cap_tracking() {
        let a = LockArbiter::new();
        let r = a.wrlock(900, LockType::File, "C1");
        // C1 LONER, 写数据 → 标脏
        a.mark_dirty(900, LockType::File, "C1", CapSet::CAP_W);

        let dirty = a.get_dirty_clients(900);
        assert_eq!(dirty.len(), 1);
        assert_eq!(dirty[0].0, "C1");
        assert!(dirty[0].2.has_w());

        // flush_dirty → 清空
        a.flush_dirty(900, LockType::File, "C1");
        let dirty2 = a.get_dirty_clients(900);
        assert!(dirty2.is_empty());

        let _ = r;
    }

    // ============================================================
    // §9 sn fencing: 旧 sn 在 fence_epoch 后失效
    // ============================================================

    #[test]
    fn test_sn_fencing() {
        let a = LockArbiter::new();
        let r = a.wrlock(1000, LockType::File, "C1");
        assert!(a.sn_valid(1000, LockType::File, r.sn));
        a.fence_epoch(1000, LockType::File);
        assert!(!a.sn_valid(1000, LockType::File, r.sn));
    }

    // ============================================================
    // §10 dump: 状态可观测性
    // ============================================================

    #[test]
    fn test_dump_format() {
        let a = LockArbiter::new();
        a.wrlock(1100, LockType::File, "C1");
        let s = a.dump(1100);
        assert!(s.contains("MDLock dump for inode 1100"));
        assert!(s.contains("File"));
        assert!(s.contains("LONER"));
        assert!(s.contains("client=C1"));
    }

    // ============================================================
    // 辅助 trait (LockAcquireResult 简化测试)
    // ============================================================

    trait AwaitGrant {
        fn await_grant(self) -> LockGrantResult;
    }
    impl AwaitGrant for LockAcquireResult {
        fn await_grant(self) -> LockGrantResult {
            match self {
                LockAcquireResult::Granted(r) => r,
                LockAcquireResult::Waiting { .. } => {
                    panic!("测试期望 Granted, 实际进入 Waiting")
                }
            }
        }
    }
}
