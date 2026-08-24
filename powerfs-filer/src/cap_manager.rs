//! Cap Manager — Stage 3 重构 (2026-08-23): lock_arbiter 驱动的 thin wrapper.
//!
//! 设计原则 (用户要求重构, 不留旧架构债):
//! - `CapManager` **不再** 维护 per-inode holder 状态 — `LockArbiter` 才是
//!   状态机的唯一 source of truth (4 套状态机 + GATHER/Loner/Quiesce).
//! - `CapManager` 仅作为 `LockArbiter` 与网络层 (`CapRevoker`) 之间的桥接:
//!   1. `open_grant` → `arbiter.wrlock/rdlock(File)` → 转 `OpenGrantResult`
//!   2. `release_cap` → `arbiter.unlock` → 处理 `promote_task` → `UpgradeTask`
//!   3. `recall_ack` → `arbiter.recall_ack` (GATHER 计数减一)
//!   4. `drain_expired_recalls` → 遍历 `active_inodes` 调 `arbiter.tick`,
//!      返回 promote_tasks 供 net_handler 下发 `CapUpgradeNotify`
//!      (tick 内部已处理 GATHER 超时 force-reclaim + Loner 升级)
//!   5. `evict_session_full` → `arbiter.evict_client` → 返回 promote_tasks
//!      供 net_handler `on_disconnect` 下发 `CapUpgradeNotify`
//! - `active_inodes` 仅是 `HashSet<u64>` 索引, 用于 drain 遍历, 不存 holder 数据
//! - `meta_cache` 的 refcount / `cap_attach` 跟踪仍由 `CapManager` 维护
//!   (`LockArbiter` 不感知 `MetaCache` — 保持锁模块独立性)
//!
//! # 已删除的旧架构债
//! - `CapState` (Free/SharedRead/ExclusiveWrite/SharedWrite 四态机)
//! - `CapHolder` / `RevokeRecord` / `CapStateFlags`
//! - `CapInodeState` / `PendingRecall` / `UpgradeWaiter`
//! - `ClientSession` / `SessionState`
//! - `SnAllocator` / 全局 `epoch` / 全局 `cap_id_alloc` (由 `LockArbiter` 内部维护)
//! - `logical_state` / `holder_count` / `validate_cap` (基于旧状态机的观察 API)

use crate::lock_arbiter::{LockArbiter, LockType};
use crate::meta_cache::MetaCache;
use std::collections::HashSet;
use std::ops::BitOr;
use std::sync::{Arc, Mutex};

/// Default lease duration (30s) — matches the inode lease default.
const DEFAULT_CAP_DURATION_MS: u64 = 30_000;

// ==================== CapSet ====================

/// A bit-set of capabilities granted to a client for an inode.
///
/// Encoded as `u8` so it can travel in protocol headers cheaply. Combinations:
/// - `CAP_R` alone: SHARED reader
/// - `CAP_R | CAP_W | CAP_X` (`EXCLUSIVE`): LONER writer (single-client fast path)
/// - empty set (`NONE`): SHARED writer participant (sync IO, no local cache)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct CapSet(pub u8);

impl CapSet {
    pub const NONE: CapSet = CapSet(0);
    pub const CAP_R: CapSet = CapSet(0b001);
    pub const CAP_W: CapSet = CapSet(0b010);
    pub const CAP_X: CapSet = CapSet(0b100);
    /// Full set granted to a single exclusive (LONER) writer.
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

// ==================== 对外契约类型 (字段不变) ====================

/// What `open_grant` returns to the caller (the net handler). The net handler
/// forwards `granted_caps` to the client in the open response; `recall_tasks`
/// are dispatched asynchronously (recall is fire-and-forget — `open` does not
/// wait for recall ACK).
#[derive(Debug, Clone)]
pub struct OpenGrantResult {
    /// Caps granted to the opener. May be `EXCLUSIVE` (LONER writer),
    /// `CAP_R` (reader), or `NONE` (SHARED writer — sync IO).
    pub granted_caps: CapSet,
    /// Server-issued token for this client's cap grant. Used in subsequent
    /// `release_cap` / `recall_ack` calls. Format: `cap-{inode}-{client}-{sn}`.
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
    /// from LONER to SHARED reader). `NONE` if fully revoked.
    pub retained_caps: CapSet,
    /// Epoch bump for fencing — the holder's subsequent IO with the old
    /// epoch will be rejected by the storage layer.
    pub new_epoch: u64,
}

/// An upgrade notification to push to a client when it's promoted to LONER
/// (single-writer fast path restored after `release_cap` / `evict_client`
/// leaves exactly one writer holding the lock).
///
/// Carries the new `(token, sn, epoch, caps)` the client must use for
/// subsequent IO; the old (token, sn, epoch) is fenced off.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpgradeTask {
    pub holder: String,
    pub token: String,
    pub granted_caps: CapSet,
    pub epoch: u64,
    pub sn: u64,
}

// ==================== CapRevoker / RecallTimeoutPenalty traits ====================

/// Server-side trait for pushing cap recall notifications to clients.
///
/// Implemented by the net layer (which has the gRPC push channel). The
/// `CapManager` calls `recall(...)` on cap degradation (e.g. a second writer
/// opens the file → recall the first writer's `CAP_W`+`CAP_X`).
///
/// The default `NoopCapRevoker` is a no-op so the manager works in tests
/// without a transport — in that mode recalls are silently dropped and
/// force-reclaim (via GATHER timeout) is the only progression path.
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
/// within the timeout (§13.6.1 fencing token). Implemented by the net layer
/// (`HealthCapPenaltyBridge`) to feed the health tracker.
pub trait RecallTimeoutPenalty: Send + Sync {
    fn on_recall_ack_timeout(&self, client_id: &str);
}

#[derive(Debug, Default)]
pub struct NoopRecallPenalty;

impl RecallTimeoutPenalty for NoopRecallPenalty {
    fn on_recall_ack_timeout(&self, _client_id: &str) {}
}

// ==================== CapManager ====================

/// Cap Manager — server-side, per-Filer-leader. Stage 3 重构后仅作为
/// `LockArbiter` 与网络层的桥接层.
///
/// Thread-safe via:
/// - `active_inodes: Mutex<HashSet<u64>>` — 仅用于 drain 遍历
/// - `arbiter: Arc<LockArbiter>` — 内部自维护锁 (互斥+oneshot)
///
/// 所有锁状态机逻辑 (GATHER/Loner/Quiesce/recall/promote) 全部由
/// `LockArbiter` 处理; `CapManager` 只做:
/// 1. 类型转换 (`LockGrantResult` ↔ `OpenGrantResult`, promote → `UpgradeTask`)
/// 2. token 编解码 (cap_manager 自有协议 `cap-{inode}-{client}-{sn}`)
/// 3. MetaCache refcount / cap_attach 跟踪
/// 4. Recall 推送 (通过 `CapRevoker` trait)
#[derive(Clone)]
pub struct CapManager {
    /// 活跃 inode 索引 — 仅记录哪些 inode 走过 `open_grant`, 用于
    /// `drain_expired_recalls` 遍历调 `arbiter.tick`. 不存 holder 数据.
    active_inodes: Arc<Mutex<HashSet<u64>>>,
    /// LockArbiter 引用 — 状态机唯一 source of truth.
    arbiter: Arc<LockArbiter>,
    /// Recall 推送 transport. `NoopCapRevoker` in tests.
    revoker: Arc<dyn CapRevoker>,
    /// Recall ACK 超时惩罚钩子. `NoopRecallPenalty` in tests.
    penalty: Arc<dyn RecallTimeoutPenalty>,
    /// 默认租期 (ms) — 仅用于 `OpenGrantResult.duration_ms` 字段,
    /// 实际过期由 `arbiter.lease_duration` 控制.
    duration_ms: u64,
    /// 可选 MetaCache 引用 — refcount + cap_attach 跟踪.
    meta_cache: Option<Arc<MetaCache>>,
}

impl Default for CapManager {
    fn default() -> Self {
        Self::new()
    }
}

impl CapManager {
    /// 创建一个 CapManager, 内部默认创建一个 LockArbiter.
    /// 可通过 `with_arbiter()` 注入自定义 arbiter (测试场景).
    pub fn new() -> Self {
        Self {
            active_inodes: Arc::new(Mutex::new(HashSet::new())),
            arbiter: Arc::new(LockArbiter::new()),
            revoker: Arc::new(NoopCapRevoker),
            penalty: Arc::new(NoopRecallPenalty),
            duration_ms: DEFAULT_CAP_DURATION_MS,
            meta_cache: None,
        }
    }

    /// 注入自定义 LockArbiter (用于测试场景指定 recall_timeout / lease_duration).
    pub fn with_arbiter(mut self, arbiter: Arc<LockArbiter>) -> Self {
        self.arbiter = arbiter;
        self
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

    /// 透传 LockArbiter 引用 (供 net_handler 直接调用高级锁原语如
    /// `xlock` / `scatter_wrlock` / `quiesce`).
    pub fn arbiter(&self) -> &Arc<LockArbiter> {
        &self.arbiter
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

    /// 授予 open 请求的 cap. 委托 `arbiter.wrlock` (写) / `arbiter.rdlock` (读),
    /// 然后把 `LockGrantResult` 转换为 `OpenGrantResult`.
    ///
    /// LockType 固定为 `File` (文件数据锁); 其他锁类型 (Auth/Xattr/Link 等)
    /// 由 net_handler 在 setattr / xattr 路径直接调 `self.arbiter()`.
    pub fn open_grant(&self, inode: u64, client_id: &str, is_write_open: bool) -> OpenGrantResult {
        // 注册 active_inode (用于 drain 遍历)
        self.active_inodes.lock().unwrap().insert(inode);

        let lock_type = LockType::File;
        let grant = if is_write_open {
            self.arbiter.wrlock(inode, lock_type, client_id)
        } else {
            self.arbiter.rdlock(inode, lock_type, client_id)
        };

        // MetaCache refcount + cap_attach (cap_id 用 sn 代替, 全局唯一)
        if grant.granted_caps != CapSet::NONE {
            if let Some(mc) = &self.meta_cache {
                mc.incr_refcount(inode);
                mc.cap_attach(
                    inode,
                    client_id,
                    grant.sn,
                    grant.granted_caps,
                    grant.epoch,
                    is_write_open,
                );
            }
        }

        // 转换 recall_tasks: arbiter.RecallTask → cap_manager.RecallTask
        let recall_tasks: Vec<RecallTask> = grant
            .recall_tasks
            .iter()
            .map(|t| RecallTask {
                holder: t.client_id.clone(),
                token: generate_token(inode, &t.client_id, t.sn),
                caps_to_recall: t.caps_to_recall,
                retained_caps: t.retained_caps,
                new_epoch: t.new_epoch,
            })
            .collect();

        let token = generate_token(inode, client_id, grant.sn);

        log::debug!(
            "CapManager::open_grant inode={} client={} write={} granted={:?} sn={} recalls={}",
            inode,
            client_id,
            is_write_open,
            grant.granted_caps,
            grant.sn,
            recall_tasks.len()
        );

        OpenGrantResult {
            granted_caps: grant.granted_caps,
            token,
            epoch: grant.epoch,
            duration_ms: self.duration_ms,
            sn: grant.sn,
            recall_tasks,
        }
    }

    /// 客户端 ACK recall. 遍历所有 `LockType` 调 `arbiter.recall_ack`,
    /// 命中匹配 (client_id, sn) 的 lock_type 即完成 GATHER 计数减一.
    ///
    /// **Stage 4 重构**: 旧版硬编码 `LockType::File`, 导致 setattr/
    /// setxattr 路径的 Auth/Xattr GATHER recall_ack 无法处理 (客户端
    /// ACK 后 arbiter 找不到匹配 lock_type, GATHER 不完成, waiter
    /// 阻塞到 tick force-reclaim 2s 超时). 现遍历全部 lock_type 解决.
    ///
    /// **Loner promote 策略**: 不在 recall_ack 内联 promote (详见
    /// `arbiter.recall_ack` 注释 — C2 还没加入 holders 时 promote
    /// 会错误升级旧 holder). promote 通过两条路径:
    ///   1) 被 `wake_waiters` 唤醒的新 caller 在重试 wrlock/xlock 时
    ///      直接作为新 holder 拿到 LONER/EXCL cap (在其自身 RPC 返回值里).
    ///   2) 稳定单 holder 由 `drain_expired_recalls()` 的 500ms sweep
    ///      tick 升级为 LONER, 并下发 `CapUpgradeNotify`.
    ///
    /// token → sn 解析依赖 cap_manager 自有 token 格式
    /// (`cap-{inode}-{client}-{sn}`), 不接受外部伪造 token.
    ///
    /// 返回 `Ok(None)` 总是成立 (不在此路径下发 upgrade); 仅 Err 表示
    /// token 解析失败.
    pub fn recall_ack(
        &self,
        inode: u64,
        client_id: &str,
        token: &str,
    ) -> Result<Option<UpgradeTask>, String> {
        let sn = parse_sn_from_token(token)
            .ok_or_else(|| format!("invalid token (no sn suffix): {}", token))?;

        let mut any_hit = false;
        for i in 0..LockType::NUM_TYPES {
            let lt = LockType::from_index(i);
            let matched = self.arbiter.recall_ack(inode, lt, client_id, sn);
            if matched {
                any_hit = true;
                log::debug!(
                    "CapManager::recall_ack inode={} client={} sn={} lt={:?} matched",
                    inode,
                    client_id,
                    sn,
                    lt
                );
            }
        }

        if !any_hit {
            log::warn!(
                "CapManager::recall_ack no matching gather entry inode={} client={} sn={}",
                inode,
                client_id,
                sn
            );
        }

        Ok(None)
    }

    /// 释放 cap (close). 委托 `arbiter.unlock(inode, File, sn)`.
    ///
    /// 如果 unlock 后只剩 1 个 writer holder, arbiter 自动 promote_to_loner
    /// (升级为 LONER, bump sn 用于 fencing), 返回 `promote_task`.
    /// 我们将其转换为 `UpgradeTask` 供 net_handler 下发 `CapUpgradeNotify`.
    ///
    /// **前置条件**: 调用方 (net_handler) 必须已 flush 脏数据 + sync 元数据
    /// 后再调用此方法 — `release_cap` 不做 flush (§13.5 flush barrier 是调用方
    /// 的责任).
    pub fn release_cap(
        &self,
        inode: u64,
        client_id: &str,
        token: &str,
    ) -> Result<Option<UpgradeTask>, String> {
        let sn = parse_sn_from_token(token)
            .ok_or_else(|| format!("invalid token (no sn suffix): {}", token))?;

        let lock_type = LockType::File;
        let promote = self.arbiter.unlock(inode, lock_type, sn);

        // MetaCache refcount -- + cap_detach
        if let Some(mc) = &self.meta_cache {
            mc.decr_refcount(inode);
            mc.cap_detach(inode, client_id);
        }

        // 转 promote_task → UpgradeTask
        let upgrade = promote.map(|(survivor_client, new_sn, caps)| {
            let new_token = generate_token(inode, &survivor_client, new_sn);
            // meta_cache 更新升级后的 holder (cap_id 用 new_sn)
            if let Some(mc) = &self.meta_cache {
                mc.cap_attach(inode, &survivor_client, new_sn, caps, 0, true);
            }
            log::info!(
                "CapManager::release_cap promote_to_loner inode={} survivor={} sn={} caps={:?}",
                inode,
                survivor_client,
                new_sn,
                caps
            );
            UpgradeTask {
                holder: survivor_client,
                token: new_token,
                granted_caps: caps,
                epoch: 0,
                sn: new_sn,
            }
        });

        log::debug!(
            "CapManager::release_cap inode={} client={} sn={} upgrade={}",
            inode,
            client_id,
            sn,
            upgrade.is_some()
        );

        Ok(upgrade)
    }

    /// 周期性扫描: 遍历所有活跃 inode 调 `arbiter.tick(inode)`.
    ///
    /// `arbiter.tick` 内部已处理:
    /// - 过期 holder garbage_collect
    /// - GATHER 超时 → force-reclaim (recall_timeout 触发)
    /// - Loner 升级 (只剩 1 个 holder 时 bump sn + 升级 caps)
    ///
    /// 返回 `promote_tasks: Vec<(u64, LockType, String, u64, CapSet)>` 供
    /// net_handler sweep loop 下发 `CapUpgradeNotify` 给被升级为 LONER 的客户端.
    /// - `(inode, lock_type, survivor_client, new_sn, upgraded_caps)`
    /// - 仅 `LockType::File` 类型的 promote 才下发 `CapUpgradeNotify`
    ///   (其余 LockType 是元数据锁, 客户端无 cap 状态需同步)
    pub fn drain_expired_recalls(&self) -> Vec<(u64, LockType, String, u64, CapSet)> {
        let inodes: Vec<u64> = self.active_inodes.lock().unwrap().iter().copied().collect();
        let mut all_promotes: Vec<(u64, LockType, String, u64, CapSet)> = Vec::new();
        for inode in inodes {
            let promotes = self.arbiter.tick(inode);
            for (lt, survivor, new_sn, caps) in promotes {
                log::debug!(
                    "CapManager::drain_expired_recalls promote inode={} lt={:?} survivor={} sn={} caps={:?}",
                    inode, lt, survivor, new_sn, caps
                );
                all_promotes.push((inode, lt, survivor, new_sn, caps));
            }
        }
        all_promotes
    }

    /// 客户端会话销毁. 委托 `arbiter.evict_client(client_id)`.
    ///
    /// arbiter 返回:
    /// - `changed_inodes: Vec<(u64, LockType)>` — 该 client 持有的所有锁
    /// - `promote_tasks: Vec<(u64, LockType, String, u64, CapSet)>` — 剩 1 个
    ///   holder 的 inode 升级为 LONER
    ///
    /// CapManager 唤醒所有 changed_inodes 的等待者 + 清理 MetaCache,
    /// 把 promote_tasks 透传给调用方 (net_handler) 以下发 `CapUpgradeNotify`.
    ///
    /// **Stage 4 重构**: 删除旧 `close_session(Vec<(u64,u64)>)` 占位契约,
    /// 只保留此完整版 — 调用方必须处理 promote_tasks (Stage 4 net_handler
    /// 的 `on_disconnect` 负责下发).
    pub fn evict_session_full(
        &self,
        client_id: &str,
    ) -> (
        Vec<(u64, LockType)>,
        Vec<(u64, LockType, String, u64, CapSet)>,
    ) {
        let (changed_inodes, promote_tasks) = self.arbiter.evict_client(client_id);

        for (inode, lt) in &changed_inodes {
            self.arbiter.wake_after_evict(*inode, *lt);
            if let Some(mc) = &self.meta_cache {
                mc.cap_detach(*inode, client_id);
                mc.decr_refcount(*inode);
            }
        }

        if !changed_inodes.is_empty() {
            log::info!(
                "CapManager::evict_session_full client={} cleaned={} promoted={}",
                client_id,
                changed_inodes.len(),
                promote_tasks.len()
            );
        }

        (changed_inodes, promote_tasks)
    }

    /// 生成 cap token (供 net_handler 在 sweep/evict 路径构造 upgrade
    /// 通知的 token, 与 `open_grant` / `release_cap` 返回的 token 格式一致).
    /// 格式: `cap-{inode}-{short_client(≤16)}-{sn}`.
    pub fn make_token(&self, inode: u64, client_id: &str, sn: u64) -> String {
        generate_token(inode, client_id, sn)
    }
}

// ==================== token 编解码 ====================

/// 生成 cap token. 格式: `cap-{inode}-{short_client}-{sn}`.
/// `short_client` 截断到 16 字符 (避免 client_id 含特殊字符时 token 过长).
fn generate_token(inode: u64, client_id: &str, sn: u64) -> String {
    let short_client = if client_id.len() > 16 {
        &client_id[..16]
    } else {
        client_id
    };
    format!("cap-{}-{}-{}", inode, short_client, sn)
}

/// 从 token 解析 sn (最后一段). 接受 `cap-{inode}-{client}-{sn}` 格式,
/// 用 `rsplit('-')` 取最后一段并 parse 为 u64.
fn parse_sn_from_token(token: &str) -> Option<u64> {
    token.rsplit('-').next().and_then(|s| s.parse().ok())
}
