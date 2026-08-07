//! 流量控制与连接管理 (Phase 1: 数据结构 + 统计 + 慢连接标记)
//!
//! 本模块集中所有流控逻辑, 调用方 (IoLoop/Worker) 只需调用:
//!   - `FlowController::on_request_start()` — 请求进入时
//!   - `FlowController::on_request_complete()` — 请求完成时
//!   - `FlowController::register_conn()` / `unregister_conn()` — 连接生命周期
//!
//! Phase 2 将在此之上增加 `FlowPolicy` trait (可插拔策略) 和 `admit()` 准入决策.
//!
//! 设计原则:
//!   1. 统计用 atomic, 无锁读, 热路径不加锁
//!   2. 慢连接用 EWMA 跟踪延迟, 双阈值标记/恢复 (防抖动)
//!   3. snapshot 接口返回可序列化数据, 供 HTTP API / Prometheus 使用

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::flow_policy::{AdmissionDecision, FlowCtx, FlowPolicy};

/// 通道类型 (与协议 CHANNEL_DATA / CHANNEL_META 对应)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Channel {
    Data,
    Meta,
}

impl Channel {
    /// 从协议层 channel 字节 (CHANNEL_DATA=0, CHANNEL_META=1) 转换
    pub fn from_u8(v: u8) -> Self {
        match v {
            crate::protocol::CHANNEL_META => Channel::Meta,
            _ => Channel::Data,
        }
    }
}

/// 单连接统计 (全原子, 可并发读写)
///
/// 字段语义:
///   - `bytes_sent` / `bytes_recv`: 服务端视角, sent=发往客户端的响应字节
///   - `reqs_recv`: 收到的请求数, `reqs_sent`: 已发响应数
///   - `active_reqs`: 当前在途请求数 (start++ / complete--)
///   - `lat_ewma_us`: 延迟指数加权移动平均 (微秒), 用于慢连接判定
///   - `slow`: 是否被标记为慢连接
#[derive(Debug)]
pub struct ConnStats {
    pub conn_id: u64,
    pub peer_addr: String,
    pub channel: Channel,

    // 流量计数
    pub bytes_sent: AtomicU64,
    pub bytes_recv: AtomicU64,
    pub reqs_sent: AtomicU64,
    pub reqs_recv: AtomicU64,
    pub reqs_err: AtomicU64,

    // 延迟统计
    pub lat_ewma_us: AtomicU64,
    pub lat_max_us: AtomicU64,
    pub slow_count: AtomicU64,

    // 慢连接标记
    pub slow: AtomicBool,
    pub slow_since: AtomicU64,
    pub recovery_counter: AtomicU32,

    // 在途请求
    pub active_reqs: AtomicU32,
}

impl ConnStats {
    pub fn new(conn_id: u64, peer_addr: String, channel: Channel) -> Self {
        Self {
            conn_id,
            peer_addr,
            channel,
            bytes_sent: AtomicU64::new(0),
            bytes_recv: AtomicU64::new(0),
            reqs_sent: AtomicU64::new(0),
            reqs_recv: AtomicU64::new(0),
            reqs_err: AtomicU64::new(0),
            lat_ewma_us: AtomicU64::new(0),
            lat_max_us: AtomicU64::new(0),
            slow_count: AtomicU64::new(0),
            slow: AtomicBool::new(false),
            slow_since: AtomicU64::new(0),
            recovery_counter: AtomicU32::new(0),
            active_reqs: AtomicU32::new(0),
        }
    }

    /// 重置所有计数器 (用于 reconnect 复用 conn_id 时)
    pub fn reset(&self) {
        self.bytes_sent.store(0, Ordering::Relaxed);
        self.bytes_recv.store(0, Ordering::Relaxed);
        self.reqs_sent.store(0, Ordering::Relaxed);
        self.reqs_recv.store(0, Ordering::Relaxed);
        self.reqs_err.store(0, Ordering::Relaxed);
        self.lat_ewma_us.store(0, Ordering::Relaxed);
        self.lat_max_us.store(0, Ordering::Relaxed);
        self.slow_count.store(0, Ordering::Relaxed);
        self.slow.store(false, Ordering::Relaxed);
        self.slow_since.store(0, Ordering::Relaxed);
        self.recovery_counter.store(0, Ordering::Relaxed);
        self.active_reqs.store(0, Ordering::Relaxed);
    }

    /// 生成可序列化快照 (供 HTTP API)
    pub fn snapshot(&self) -> ConnStatsSnapshot {
        ConnStatsSnapshot {
            conn_id: self.conn_id,
            peer_addr: self.peer_addr.clone(),
            channel: match self.channel {
                Channel::Data => "data",
                Channel::Meta => "meta",
            },
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            bytes_recv: self.bytes_recv.load(Ordering::Relaxed),
            reqs_sent: self.reqs_sent.load(Ordering::Relaxed),
            reqs_recv: self.reqs_recv.load(Ordering::Relaxed),
            reqs_err: self.reqs_err.load(Ordering::Relaxed),
            lat_ewma_us: self.lat_ewma_us.load(Ordering::Relaxed),
            lat_max_us: self.lat_max_us.load(Ordering::Relaxed),
            slow_count: self.slow_count.load(Ordering::Relaxed),
            slow: self.slow.load(Ordering::Relaxed),
            slow_since: self.slow_since.load(Ordering::Relaxed),
            active_reqs: self.active_reqs.load(Ordering::Relaxed),
        }
    }
}

/// ConnStats 的可序列化快照 (用于 HTTP API JSON 响应)
#[derive(Debug, Clone, serde::Serialize)]
pub struct ConnStatsSnapshot {
    pub conn_id: u64,
    pub peer_addr: String,
    pub channel: &'static str,
    pub bytes_sent: u64,
    pub bytes_recv: u64,
    pub reqs_sent: u64,
    pub reqs_recv: u64,
    pub reqs_err: u64,
    pub lat_ewma_us: u64,
    pub lat_max_us: u64,
    pub slow_count: u64,
    pub slow: bool,
    pub slow_since: u64,
    pub active_reqs: u32,
}

/// 全局统计汇总
#[derive(Debug, Default)]
pub struct GlobalStats {
    pub total_bytes_sent: AtomicU64,
    pub total_bytes_recv: AtomicU64,
    pub total_reqs: AtomicU64,
    pub total_errs: AtomicU64,
    pub active_reqs: AtomicU32,
    pub active_conns: AtomicU32,
    pub slow_conns: AtomicU32,
    /// 延迟直方图桶: [0]=<=1ms, [1]=<=10ms, [2]=<=100ms,
    /// [3]=<=1s, [4]=<=10s, [5]=>10s
    pub lat_buckets: [AtomicU64; 6],
}

impl GlobalStats {
    pub fn new() -> Self {
        Self::default()
    }

    /// 根据延迟返回桶索引
    pub fn lat_bucket_index(latency_us: u64) -> usize {
        match latency_us {
            x if x <= 1_000 => 0,
            x if x <= 10_000 => 1,
            x if x <= 100_000 => 2,
            x if x <= 1_000_000 => 3,
            x if x <= 10_000_000 => 4,
            _ => 5,
        }
    }

    /// 生成可序列化快照
    pub fn snapshot(&self) -> GlobalStatsSnapshot {
        GlobalStatsSnapshot {
            total_bytes_sent: self.total_bytes_sent.load(Ordering::Relaxed),
            total_bytes_recv: self.total_bytes_recv.load(Ordering::Relaxed),
            total_reqs: self.total_reqs.load(Ordering::Relaxed),
            total_errs: self.total_errs.load(Ordering::Relaxed),
            active_reqs: self.active_reqs.load(Ordering::Relaxed),
            active_conns: self.active_conns.load(Ordering::Relaxed),
            slow_conns: self.slow_conns.load(Ordering::Relaxed),
            lat_buckets: [
                self.lat_buckets[0].load(Ordering::Relaxed),
                self.lat_buckets[1].load(Ordering::Relaxed),
                self.lat_buckets[2].load(Ordering::Relaxed),
                self.lat_buckets[3].load(Ordering::Relaxed),
                self.lat_buckets[4].load(Ordering::Relaxed),
                self.lat_buckets[5].load(Ordering::Relaxed),
            ],
        }
    }
}

/// GlobalStats 的可序列化快照
#[derive(Debug, Clone, serde::Serialize)]
pub struct GlobalStatsSnapshot {
    pub total_bytes_sent: u64,
    pub total_bytes_recv: u64,
    pub total_reqs: u64,
    pub total_errs: u64,
    pub active_reqs: u32,
    pub active_conns: u32,
    pub slow_conns: u32,
    pub lat_buckets: [u64; 6],
}

/// 慢连接跟踪器配置
#[derive(Debug, Clone)]
pub struct SlowConnTrackerConfig {
    /// EWMA 超过此值标记为慢 (微秒, 默认 100ms)
    pub slow_threshold_us: u64,
    /// EWMA 平滑系数 (0-100, 表示 0.0-1.0, 默认 20 = 0.2)
    pub ewma_alpha_pct: u8,
    /// EWMA 低于此值开始累计恢复 (微秒, 默认 10ms)
    pub recovery_threshold_us: u64,
    /// 连续 N 次低于恢复阈值才解除慢标记 (默认 10)
    pub recovery_count: u32,
}

impl Default for SlowConnTrackerConfig {
    fn default() -> Self {
        Self {
            slow_threshold_us: 100_000,
            ewma_alpha_pct: 20,
            recovery_threshold_us: 10_000,
            recovery_count: 10,
        }
    }
}

/// 慢连接跟踪器 (无状态, 仅持有配置; 状态在 ConnStats 中)
#[derive(Debug, Clone)]
pub struct SlowConnTracker {
    cfg: SlowConnTrackerConfig,
}

impl SlowConnTracker {
    pub fn new(cfg: SlowConnTrackerConfig) -> Self {
        Self { cfg }
    }

    pub fn config(&self) -> &SlowConnTrackerConfig {
        &self.cfg
    }

    /// 更新 EWMA 延迟 (原子 CAS)
    ///
    /// 公式: new_ewma = alpha * sample + (1 - alpha) * old_ewma
    /// alpha = ewma_alpha_pct / 100
    pub fn update_ewma(&self, stats: &ConnStats, latency_us: u64) {
        let alpha = self.cfg.ewma_alpha_pct as u64;
        // 首次采样直接写入 (old=0 时 ewma = alpha*sample, 偏低; 改为直接写 sample)
        let old = stats.lat_ewma_us.load(Ordering::Relaxed);
        let new = if old == 0 {
            latency_us
        } else {
            // new = (alpha * sample + (100 - alpha) * old) / 100
            (alpha.saturating_mul(latency_us) + (100u64 - alpha) * old) / 100
        };
        // CAS 循环处理并发
        let mut prev = old;
        while let Err(actual) =
            stats
                .lat_ewma_us
                .compare_exchange(prev, new, Ordering::Relaxed, Ordering::Relaxed)
        {
            prev = actual;
            // 重新计算 new (基于 actual)
            let new_based = if prev == 0 {
                latency_us
            } else {
                (alpha.saturating_mul(latency_us) + (100u64 - alpha) * prev) / 100
            };
            if prev == new_based {
                break; // 无需更新
            }
            if stats
                .lat_ewma_us
                .compare_exchange(prev, new_based, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }

        // 更新 max (CAS)
        let mut cur_max = stats.lat_max_us.load(Ordering::Relaxed);
        while latency_us > cur_max {
            match stats
                .lat_max_us
                .compare_exchange(cur_max, latency_us, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(actual) => cur_max = actual,
            }
        }
    }

    /// 评估是否标记/解除慢连接 (原子操作, 无锁)
    ///
    /// 返回 true 表示状态发生翻转 (正常→慢 或 慢→正常), 调用方可据此打日志
    pub fn evaluate(&self, stats: &ConnStats) -> SlowStateChange {
        let ewma = stats.lat_ewma_us.load(Ordering::Relaxed);
        let was_slow = stats.slow.load(Ordering::Relaxed);
        let now_ns = current_ns();

        if !was_slow && ewma > self.cfg.slow_threshold_us {
            // 标记为慢
            if stats
                .slow
                .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                stats.slow_since.store(now_ns, Ordering::Relaxed);
                stats.slow_count.fetch_add(1, Ordering::Relaxed);
                stats.recovery_counter.store(0, Ordering::Relaxed);
                SlowStateChange::MarkedSlow
            } else {
                SlowStateChange::Unchanged
            }
        } else if was_slow && ewma < self.cfg.recovery_threshold_us {
            // 累计恢复计数
            let n = stats.recovery_counter.fetch_add(1, Ordering::Relaxed) + 1;
            if n >= self.cfg.recovery_count {
                // 连续 N 次低于恢复阈值, 解除慢标记
                if stats
                    .slow
                    .compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
                {
                    stats.recovery_counter.store(0, Ordering::Relaxed);
                    SlowStateChange::Recovered
                } else {
                    SlowStateChange::Unchanged
                }
            } else {
                SlowStateChange::Unchanged
            }
        } else if was_slow {
            // 仍慢, 重置恢复计数 (防止偶尔的低延迟累积误解除)
            stats.recovery_counter.store(0, Ordering::Relaxed);
            SlowStateChange::Unchanged
        } else {
            SlowStateChange::Unchanged
        }
    }
}

/// 慢连接状态变化 (供调用方打日志)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlowStateChange {
    Unchanged,
    MarkedSlow,
    Recovered,
}

/// 流控控制器 (统一入口, 持有全局统计 + per-conn 统计 + 慢连接跟踪器 + 策略)
///
/// Phase 1 S1: 统计收集 + 慢连接标记 (on_request_start/complete)
/// Phase 1 S2: 增加 FlowPolicy + admit() 准入决策
/// Phase 2: load_factor 负载反馈
pub struct FlowController {
    global: GlobalStats,
    /// conn_id → ConnStats (用 Arc 共享, 供 IoLoop/Worker 直接持有引用避免查表)
    conns: RwLock<HashMap<u64, Arc<ConnStats>>>,
    slow_tracker: SlowConnTracker,
    /// 可插拔准入策略 (None 时永远放行, 用于无流控场景)
    policy: RwLock<Option<Arc<dyn FlowPolicy>>>,
}

impl FlowController {
    pub fn new(slow_tracker: SlowConnTracker) -> Self {
        Self {
            global: GlobalStats::new(),
            conns: RwLock::new(HashMap::new()),
            slow_tracker,
            policy: RwLock::new(None),
        }
    }

    /// 用默认慢连接配置创建
    pub fn with_defaults() -> Self {
        Self::new(SlowConnTracker::new(SlowConnTrackerConfig::default()))
    }

    /// 注册新连接, 返回 Arc<ConnStats> 供调用方持有
    pub fn register_conn(
        &self,
        conn_id: u64,
        peer_addr: String,
        channel: Channel,
    ) -> Arc<ConnStats> {
        let stats = Arc::new(ConnStats::new(conn_id, peer_addr, channel));
        {
            let mut conns = self.conns.write();
            conns.insert(conn_id, stats.clone());
        }
        self.global.active_conns.fetch_add(1, Ordering::Relaxed);
        stats
    }

    /// 注销连接 (断连/关闭时调用)
    pub fn unregister_conn(&self, conn_id: u64) {
        let removed = {
            let mut conns = self.conns.write();
            conns.remove(&conn_id)
        };
        if let Some(stats) = removed {
            self.global.active_conns.fetch_sub(1, Ordering::Relaxed);
            if stats.slow.load(Ordering::Relaxed) {
                self.global.slow_conns.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }

    /// 请求进入 (在 IoLoop/Worker 开始处理前调用)
    ///
    /// 返回 Arc<ConnStats> 供调用方在 complete 时传入 (避免再次查表)
    pub fn on_request_start(&self, stats: &ConnStats) {
        stats.active_reqs.fetch_add(1, Ordering::Relaxed);
        stats.reqs_recv.fetch_add(1, Ordering::Relaxed);
        self.global.active_reqs.fetch_add(1, Ordering::Relaxed);
        self.global.total_reqs.fetch_add(1, Ordering::Relaxed);
    }

    /// 请求完成 (响应发送后调用)
    ///
    /// 参数:
    ///   - `stats`: register_conn 返回的 Arc (或 on_request_start 时持有的引用)
    ///   - `latency_us`: 请求处理耗时 (微秒)
    ///   - `bytes`: 响应字节数 (含帧头+body+data)
    ///   - `err`: 是否出错
    pub fn on_request_complete(&self, stats: &ConnStats, latency_us: u64, bytes: u64, err: bool) {
        // 递减在途计数
        stats.active_reqs.fetch_sub(1, Ordering::Relaxed);
        self.global.active_reqs.fetch_sub(1, Ordering::Relaxed);

        // 流量统计 (服务端视角: sent = 发给客户端的响应)
        stats.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
        stats.reqs_sent.fetch_add(1, Ordering::Relaxed);
        self.global
            .total_bytes_sent
            .fetch_add(bytes, Ordering::Relaxed);

        if err {
            stats.reqs_err.fetch_add(1, Ordering::Relaxed);
            self.global.total_errs.fetch_add(1, Ordering::Relaxed);
        }

        // EWMA 延迟更新
        self.slow_tracker.update_ewma(stats, latency_us);

        // 慢连接评估
        let change = self.slow_tracker.evaluate(stats);
        match change {
            SlowStateChange::MarkedSlow => {
                self.global.slow_conns.fetch_add(1, Ordering::Relaxed);
                log::warn!(
                    "FlowControl: conn {} ({}) marked SLOW (ewma={}us > {}us)",
                    stats.conn_id,
                    stats.peer_addr,
                    stats.lat_ewma_us.load(Ordering::Relaxed),
                    self.slow_tracker.cfg.slow_threshold_us
                );
            }
            SlowStateChange::Recovered => {
                self.global.slow_conns.fetch_sub(1, Ordering::Relaxed);
                log::info!(
                    "FlowControl: conn {} ({}) recovered from SLOW",
                    stats.conn_id,
                    stats.peer_addr
                );
            }
            SlowStateChange::Unchanged => {}
        }

        // 全局延迟直方图
        let bucket = GlobalStats::lat_bucket_index(latency_us);
        self.global.lat_buckets[bucket].fetch_add(1, Ordering::Relaxed);
    }

    /// 收到请求字节时累计 (可选, 用于精确统计 recv 流量)
    pub fn on_bytes_recv(&self, stats: &ConnStats, bytes: u64) {
        stats.bytes_recv.fetch_add(bytes, Ordering::Relaxed);
        self.global
            .total_bytes_recv
            .fetch_add(bytes, Ordering::Relaxed);
    }

    /// 查询某连接统计 (返回克隆的 Arc, 调用方可短时持有)
    pub fn get_conn(&self, conn_id: u64) -> Option<Arc<ConnStats>> {
        self.conns.read().get(&conn_id).cloned()
    }

    /// 全局快照 (HTTP API 用)
    pub fn snapshot_global(&self) -> GlobalStatsSnapshot {
        self.global.snapshot()
    }

    /// 所有连接快照 (HTTP API 用)
    pub fn snapshot_connections(&self) -> Vec<ConnStatsSnapshot> {
        let conns = self.conns.read();
        let mut out: Vec<ConnStatsSnapshot> = conns.values().map(|s| s.snapshot()).collect();
        // 按 conn_id 排序, 方便查看
        out.sort_by_key(|s| s.conn_id);
        out
    }

    /// 慢连接配置 (运行时只读; 调整配置需重建 controller, Phase 2 增加运行时调整)
    pub fn slow_tracker_config(&self) -> &SlowConnTrackerConfig {
        self.slow_tracker.config()
    }

    /// 当前慢连接数
    pub fn slow_conn_count(&self) -> u32 {
        self.global.slow_conns.load(Ordering::Relaxed)
    }

    // ----- Phase 1 S2: 准入策略集成 -----

    /// 安装准入策略 (运行时可替换)
    ///
    /// 传入 `None` 可禁用流控 (所有请求放行).
    /// 调用方在 `on_request_start` 之前应调用 `admit()` 检查准入.
    pub fn set_policy(&self, policy: Option<Arc<dyn FlowPolicy>>) {
        *self.policy.write() = policy;
    }

    /// 安装默认 AdaptiveConcurrencyPolicy (便捷方法)
    pub fn set_default_policy(&self) {
        self.set_policy(Some(Arc::new(
            crate::flow_policy::AdaptiveConcurrencyPolicy::with_defaults(),
        )));
    }

    /// 运行时调整 AdaptiveConcurrencyPolicy 的 max_active_global.
    /// 如果当前未安装 AdaptiveConcurrencyPolicy, 则忽略.
    pub fn set_max_active_global(&self, max: u32) -> bool {
        let guard = self.policy.read();
        if let Some(policy) = guard.as_ref() {
            if let Some(acp) = policy
                .as_any()
                .downcast_ref::<crate::flow_policy::AdaptiveConcurrencyPolicy>()
            {
                acp.set_max_active_global(max);
                return true;
            }
        }
        false
    }

    /// 运行时调整 AdaptiveConcurrencyPolicy 的 max_active_per_conn.
    pub fn set_max_active_per_conn(&self, max: u32) -> bool {
        let guard = self.policy.read();
        if let Some(policy) = guard.as_ref() {
            if let Some(acp) = policy
                .as_any()
                .downcast_ref::<crate::flow_policy::AdaptiveConcurrencyPolicy>()
            {
                acp.set_max_active_per_conn(max);
                return true;
            }
        }
        false
    }

    /// 准入决策: 是否允许新请求
    ///
    /// 返回 `Admit` 时, 调用方应调 `on_request_start` 并处理请求.
    /// 返回 `Reject(reason)` 时, 调用方应返回 BUSY / EAGAIN.
    ///
    /// 注意: `admit()` 只做决策, 不修改计数. admit 与 on_request_start 之间
    /// 的竞态是 best-effort (防雪崩, 非精确限流).
    pub fn admit(&self, conn_id: u64, msg_type: u16, est_bytes: usize) -> AdmissionDecision {
        // 先读 policy (read lock), 再读 conns (read lock)
        // 锁顺序: policy → conns (其他路径无反向获取, 不会死锁)
        let policy_guard = self.policy.read();
        let Some(policy) = policy_guard.as_ref() else {
            return AdmissionDecision::Admit; // 无策略, 放行
        };
        let conns = self.conns.read();
        let Some(stats) = conns.get(&conn_id) else {
            return AdmissionDecision::Admit; // 连接不存在, 放行 (后续 start 会失败)
        };
        let ctx = FlowCtx {
            conn: stats,
            global: &self.global,
            msg_type,
            est_bytes,
        };
        policy.admit(&ctx)
    }

    /// 当前负载因子 (0-3, Phase 2 用于响应帧 flags bit 6-7)
    ///
    /// Phase 2: 基于 global active_reqs 和策略的 max_active_global 计算比率.
    /// Worker 在发送响应前调用此方法, 将结果 stamp 到响应帧 flags bits 6-7.
    pub fn current_load_factor(&self) -> u8 {
        let policy_guard = self.policy.read();
        if let Some(policy) = policy_guard.as_ref() {
            let global_active = self.global.active_reqs.load(Ordering::Relaxed);
            return policy.load_factor(global_active);
        }
        0
    }

    /// 策略名称 (供 HTTP API / 日志)
    pub fn policy_name(&self) -> &'static str {
        let policy_guard = self.policy.read();
        match policy_guard.as_ref() {
            Some(p) => p.name(),
            None => "none",
        }
    }
}

/// 获取当前时间 (纳秒, 单调时钟)
fn current_ns() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flow_policy::{AdmissionDecision, FlowPolicy, RejectReason};

    fn make_stats(conn_id: u64) -> ConnStats {
        ConnStats::new(
            conn_id,
            format!("127.0.0.1:{}", 1000 + conn_id),
            Channel::Data,
        )
    }

    #[test]
    fn test_conn_stats_atomic_update() {
        let s = make_stats(1);
        assert_eq!(s.bytes_sent.load(Ordering::Relaxed), 0);
        s.bytes_sent.fetch_add(100, Ordering::Relaxed);
        s.bytes_sent.fetch_add(50, Ordering::Relaxed);
        assert_eq!(s.bytes_sent.load(Ordering::Relaxed), 150);
    }

    #[test]
    fn test_conn_stats_snapshot() {
        let s = make_stats(42);
        s.bytes_recv.fetch_add(1024, Ordering::Relaxed);
        s.active_reqs.fetch_add(3, Ordering::Relaxed);
        let snap = s.snapshot();
        assert_eq!(snap.conn_id, 42);
        assert_eq!(snap.bytes_recv, 1024);
        assert_eq!(snap.active_reqs, 3);
        assert_eq!(snap.channel, "data");
        assert!(!snap.slow);
    }

    #[test]
    fn test_lat_bucket_index() {
        assert_eq!(GlobalStats::lat_bucket_index(0), 0);
        assert_eq!(GlobalStats::lat_bucket_index(1_000), 0); // 边界 1ms
        assert_eq!(GlobalStats::lat_bucket_index(1_001), 1);
        assert_eq!(GlobalStats::lat_bucket_index(10_000), 1); // 10ms
        assert_eq!(GlobalStats::lat_bucket_index(100_000), 2); // 100ms
        assert_eq!(GlobalStats::lat_bucket_index(1_000_000), 3); // 1s
        assert_eq!(GlobalStats::lat_bucket_index(10_000_000), 4); // 10s
        assert_eq!(GlobalStats::lat_bucket_index(10_000_001), 5);
    }

    #[test]
    fn test_ewma_first_sample() {
        let tracker = SlowConnTracker::new(SlowConnTrackerConfig::default());
        let s = make_stats(1);
        tracker.update_ewma(&s, 5_000);
        // 首次采样直接写
        assert_eq!(s.lat_ewma_us.load(Ordering::Relaxed), 5_000);
    }

    #[test]
    fn test_ewma_convergence() {
        let tracker = SlowConnTracker::new(SlowConnTrackerConfig::default());
        let s = make_stats(1);
        // alpha=0.2, 连续 10 次 100us 采样, ewma 应趋向 100
        for _ in 0..10 {
            tracker.update_ewma(&s, 100);
        }
        let ewma = s.lat_ewma_us.load(Ordering::Relaxed);
        assert!(ewma > 90 && ewma <= 100, "ewma should converge to ~100, got {}", ewma);
    }

    #[test]
    fn test_ewma_max_update() {
        let tracker = SlowConnTracker::new(SlowConnTrackerConfig::default());
        let s = make_stats(1);
        tracker.update_ewma(&s, 1_000);
        tracker.update_ewma(&s, 500);
        tracker.update_ewma(&s, 5_000);
        assert_eq!(s.lat_max_us.load(Ordering::Relaxed), 5_000);
    }

    #[test]
    fn test_slow_marking_and_recovery() {
        let cfg = SlowConnTrackerConfig {
            slow_threshold_us: 100_000,    // 100ms
            ewma_alpha_pct: 50,             // alpha=0.5 加速收敛
            recovery_threshold_us: 10_000, // 10ms
            recovery_count: 3,
        };
        let tracker = SlowConnTracker::new(cfg);
        let s = make_stats(1);

        // 初始: 不慢
        assert!(!s.slow.load(Ordering::Relaxed));

        // 注入高延迟, 直到 ewma 超过 100ms
        // alpha=0.5: ewma_n = 0.5*sample + 0.5*prev
        // 持续 200us 采样, ewma 趋向 200
        for _ in 0..20 {
            tracker.update_ewma(&s, 200_000); // 200ms
        }
        let change = tracker.evaluate(&s);
        assert_eq!(change, SlowStateChange::MarkedSlow);
        assert!(s.slow.load(Ordering::Relaxed));
        assert_eq!(s.slow_count.load(Ordering::Relaxed), 1);

        // 再次 evaluate (仍慢), 不应重复标记
        let change = tracker.evaluate(&s);
        assert_eq!(change, SlowStateChange::Unchanged);
        assert_eq!(s.slow_count.load(Ordering::Relaxed), 1);

        // 注入低延迟, ewma 降到 10ms 以下
        // 持续 5ms 采样, ewma 趋向 5000
        for _ in 0..20 {
            tracker.update_ewma(&s, 5_000); // 5ms
        }
        // 连续 recovery_count=3 次 evaluate 才解除
        let c1 = tracker.evaluate(&s);
        assert_eq!(c1, SlowStateChange::Unchanged); // n=1
        let c2 = tracker.evaluate(&s);
        assert_eq!(c2, SlowStateChange::Unchanged); // n=2
        let c3 = tracker.evaluate(&s);
        assert_eq!(c3, SlowStateChange::Recovered); // n=3 触发解除
        assert!(!s.slow.load(Ordering::Relaxed));
        assert_eq!(s.recovery_counter.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_recovery_counter_reset_on_high_latency() {
        let cfg = SlowConnTrackerConfig {
            slow_threshold_us: 100_000,
            ewma_alpha_pct: 50,
            recovery_threshold_us: 10_000,
            recovery_count: 3,
        };
        let tracker = SlowConnTracker::new(cfg);
        let s = make_stats(1);

        // 标记为慢
        for _ in 0..20 {
            tracker.update_ewma(&s, 200_000);
        }
        tracker.evaluate(&s);
        assert!(s.slow.load(Ordering::Relaxed));

        // 累计 2 次低延迟 evaluate
        for _ in 0..20 {
            tracker.update_ewma(&s, 5_000);
        }
        tracker.evaluate(&s); // n=1
        tracker.evaluate(&s); // n=2
        assert_eq!(s.recovery_counter.load(Ordering::Relaxed), 2);

        // 突然又高延迟, 重置恢复计数
        for _ in 0..5 {
            tracker.update_ewma(&s, 200_000);
        }
        tracker.evaluate(&s);
        assert_eq!(s.recovery_counter.load(Ordering::Relaxed), 0);
        assert!(s.slow.load(Ordering::Relaxed)); // 仍慢
    }

    #[test]
    fn test_flow_controller_register_unregister() {
        let fc = FlowController::with_defaults();
        let snap = fc.snapshot_global();
        assert_eq!(snap.active_conns, 0);

        fc.register_conn(1, "127.0.0.1:1001".into(), Channel::Data);
        let snap = fc.snapshot_global();
        assert_eq!(snap.active_conns, 1);

        fc.unregister_conn(1);
        let snap = fc.snapshot_global();
        assert_eq!(snap.active_conns, 0);
        assert!(fc.get_conn(1).is_none());
    }

    #[test]
    fn test_flow_controller_request_lifecycle() {
        let fc = FlowController::with_defaults();
        let stats = fc.register_conn(1, "127.0.0.1:1001".into(), Channel::Data);

        // 请求开始
        fc.on_request_start(&stats);
        assert_eq!(stats.active_reqs.load(Ordering::Relaxed), 1);
        assert_eq!(stats.reqs_recv.load(Ordering::Relaxed), 1);
        let snap = fc.snapshot_global();
        assert_eq!(snap.active_reqs, 1);
        assert_eq!(snap.total_reqs, 1);

        // 请求完成
        fc.on_request_complete(&stats, 5_000, 1_024, false);
        assert_eq!(stats.active_reqs.load(Ordering::Relaxed), 0);
        assert_eq!(stats.bytes_sent.load(Ordering::Relaxed), 1_024);
        assert_eq!(stats.reqs_sent.load(Ordering::Relaxed), 1);
        let snap = fc.snapshot_global();
        assert_eq!(snap.active_reqs, 0);
        assert_eq!(snap.total_bytes_sent, 1_024);
        assert_eq!(snap.lat_buckets[1], 1); // 5ms 在 <=10ms 桶 (桶索引 1)
    }

    #[test]
    fn test_flow_controller_slow_marking_integration() {
        let cfg = SlowConnTrackerConfig {
            slow_threshold_us: 100_000,
            ewma_alpha_pct: 50,
            recovery_threshold_us: 10_000,
            recovery_count: 3,
        };
        let fc = FlowController::new(SlowConnTracker::new(cfg));
        let stats = fc.register_conn(1, "127.0.0.1:1001".into(), Channel::Data);

        // 高延迟请求, 触发慢标记
        for _ in 0..20 {
            fc.on_request_start(&stats);
            fc.on_request_complete(&stats, 200_000, 100, false); // 200ms
        }
        assert_eq!(fc.slow_conn_count(), 1);
        assert!(stats.slow.load(Ordering::Relaxed));

        let snap = fc.snapshot_global();
        assert_eq!(snap.slow_conns, 1);
    }

    #[test]
    fn test_flow_controller_unregister_slow_conn() {
        let cfg = SlowConnTrackerConfig {
            slow_threshold_us: 100_000,
            ewma_alpha_pct: 50,
            recovery_threshold_us: 10_000,
            recovery_count: 3,
        };
        let fc = FlowController::new(SlowConnTracker::new(cfg));
        let stats = fc.register_conn(1, "127.0.0.1:1001".into(), Channel::Data);

        // 标记为慢
        for _ in 0..20 {
            fc.on_request_start(&stats);
            fc.on_request_complete(&stats, 200_000, 100, false);
        }
        assert_eq!(fc.slow_conn_count(), 1);

        // 注销慢连接, slow_conns 应递减
        fc.unregister_conn(1);
        assert_eq!(fc.slow_conn_count(), 0);
    }

    #[test]
    fn test_connections_snapshot_sorted() {
        let fc = FlowController::with_defaults();
        fc.register_conn(3, "addr3".into(), Channel::Meta);
        fc.register_conn(1, "addr1".into(), Channel::Data);
        fc.register_conn(2, "addr2".into(), Channel::Data);

        let snaps = fc.snapshot_connections();
        assert_eq!(snaps.len(), 3);
        assert_eq!(snaps[0].conn_id, 1);
        assert_eq!(snaps[1].conn_id, 2);
        assert_eq!(snaps[2].conn_id, 3);
        assert_eq!(snaps[2].channel, "meta");
    }

    #[test]
    fn test_conn_stats_reset() {
        let s = make_stats(1);
        s.bytes_sent.fetch_add(500, Ordering::Relaxed);
        s.active_reqs.fetch_add(2, Ordering::Relaxed);
        s.slow.store(true, Ordering::Relaxed);

        s.reset();
        assert_eq!(s.bytes_sent.load(Ordering::Relaxed), 0);
        assert_eq!(s.active_reqs.load(Ordering::Relaxed), 0);
        assert!(!s.slow.load(Ordering::Relaxed));
    }

    // ----- Phase 1 S2: admit 集成测试 -----

    #[test]
    fn test_admit_no_policy_always_admit() {
        // 无策略时, admit 永远放行
        let fc = FlowController::with_defaults();
        let stats = fc.register_conn(1, "127.0.0.1:1001".into(), Channel::Data);
        // 填满 active_reqs
        for _ in 0..100 {
            stats.active_reqs.fetch_add(1, Ordering::Relaxed);
        }
        let d = fc.admit(1, 1, 1024);
        assert_eq!(d, AdmissionDecision::Admit);
        assert_eq!(fc.policy_name(), "none");
    }

    #[test]
    fn test_admit_with_default_policy_idle() {
        let fc = FlowController::with_defaults();
        fc.set_default_policy();
        fc.register_conn(1, "127.0.0.1:1001".into(), Channel::Data);

        let d = fc.admit(1, 1, 1024);
        assert_eq!(d, AdmissionDecision::Admit);
        assert_eq!(fc.policy_name(), "adaptive-concurrency");
    }

    #[test]
    fn test_admit_reject_conn_full() {
        let fc = FlowController::with_defaults();
        let policy = Arc::new(crate::flow_policy::AdaptiveConcurrencyPolicy::new(256, 4))
            as Arc<dyn FlowPolicy>;
        fc.set_policy(Some(policy));
        let stats = fc.register_conn(1, "127.0.0.1:1001".into(), Channel::Data);
        // 填满 per-conn (4)
        for _ in 0..4 {
            stats.active_reqs.fetch_add(1, Ordering::Relaxed);
        }
        let d = fc.admit(1, 1, 1024);
        assert_eq!(d, AdmissionDecision::Reject(RejectReason::ConnFull));
    }

    #[test]
    fn test_admit_reject_global_full() {
        let fc = FlowController::with_defaults();
        let policy = Arc::new(crate::flow_policy::AdaptiveConcurrencyPolicy::new(8, 64))
            as Arc<dyn FlowPolicy>;
        fc.set_policy(Some(policy));
        fc.register_conn(1, "127.0.0.1:1001".into(), Channel::Data);
        // 填满全局 (8)
        for _ in 0..8 {
            fc.global.active_reqs.fetch_add(1, Ordering::Relaxed);
        }
        let d = fc.admit(1, 1, 1024);
        assert_eq!(d, AdmissionDecision::Reject(RejectReason::GlobalFull));
    }

    #[test]
    fn test_admit_reject_slow_conn() {
        let fc = FlowController::with_defaults();
        let policy = Arc::new(crate::flow_policy::AdaptiveConcurrencyPolicy::new(256, 4))
            as Arc<dyn FlowPolicy>;
        fc.set_policy(Some(policy));
        let stats = fc.register_conn(1, "127.0.0.1:1001".into(), Channel::Data);
        // 标记为慢, 上限减半 = 2
        stats.slow.store(true, Ordering::Relaxed);
        for _ in 0..2 {
            stats.active_reqs.fetch_add(1, Ordering::Relaxed);
        }
        let d = fc.admit(1, 1, 1024);
        assert_eq!(d, AdmissionDecision::Reject(RejectReason::SlowConn));
    }

    #[test]
    fn test_admit_unknown_conn_admits() {
        // 连接不存在时放行 (后续 on_request_start 会失败)
        let fc = FlowController::with_defaults();
        fc.set_default_policy();
        let d = fc.admit(999, 1, 1024);
        assert_eq!(d, AdmissionDecision::Admit);
    }

    #[test]
    fn test_admit_does_not_mutate_counters() {
        // admit 只决策, 不递增 active_reqs (调用方负责 on_request_start)
        let fc = FlowController::with_defaults();
        fc.set_default_policy();
        let stats = fc.register_conn(1, "127.0.0.1:1001".into(), Channel::Data);
        let d = fc.admit(1, 1, 1024);
        assert_eq!(d, AdmissionDecision::Admit);
        assert_eq!(stats.active_reqs.load(Ordering::Relaxed), 0);
        assert_eq!(fc.snapshot_global().active_reqs, 0);
    }

    #[test]
    fn test_set_policy_replace_at_runtime() {
        let fc = FlowController::with_defaults();
        fc.set_default_policy();
        assert_eq!(fc.policy_name(), "adaptive-concurrency");

        // 运行时替换为 NullPolicy
        fc.set_policy(Some(Arc::new(crate::flow_policy::NullPolicy)));
        assert_eq!(fc.policy_name(), "null");

        // 禁用流控
        fc.set_policy(None);
        assert_eq!(fc.policy_name(), "none");
    }

    #[test]
    fn test_admit_full_lifecycle_with_policy() {
        // 完整流程: admit (Admit) → on_request_start → on_request_complete
        let fc = FlowController::with_defaults();
        fc.set_default_policy();
        let stats = fc.register_conn(1, "127.0.0.1:1001".into(), Channel::Data);

        // admit 放行
        let d = fc.admit(1, 1, 1024);
        assert_eq!(d, AdmissionDecision::Admit);

        // 调用方负责 on_request_start
        fc.on_request_start(&stats);
        assert_eq!(stats.active_reqs.load(Ordering::Relaxed), 1);

        // 完成请求
        fc.on_request_complete(&stats, 5_000, 1_024, false);
        assert_eq!(stats.active_reqs.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_current_load_factor_no_policy() {
        let fc = FlowController::with_defaults();
        // 无策略 → 0
        assert_eq!(fc.current_load_factor(), 0);
    }

    #[test]
    fn test_current_load_factor_with_policy() {
        let fc = FlowController::with_defaults();
        let policy = Arc::new(crate::flow_policy::AdaptiveConcurrencyPolicy::new(100, 64))
            as Arc<dyn FlowPolicy>;
        fc.set_policy(Some(policy));
        fc.register_conn(1, "127.0.0.1:1001".into(), Channel::Data);
        let stats = fc.get_conn(1).unwrap();

        // 0 active → lf=0
        assert_eq!(fc.current_load_factor(), 0);

        // 25 active (25%) → lf=1
        for _ in 0..25 {
            fc.on_request_start(&stats);
        }
        assert_eq!(fc.current_load_factor(), 1);

        // 50 active (50%) → lf=2
        for _ in 0..25 {
            fc.on_request_start(&stats);
        }
        assert_eq!(fc.current_load_factor(), 2);

        // 75 active (75%) → lf=3
        for _ in 0..25 {
            fc.on_request_start(&stats);
        }
        assert_eq!(fc.current_load_factor(), 3);

        // complete 1 → 74 active → lf=2
        fc.on_request_complete(&stats, 1_000, 100, false);
        assert_eq!(fc.current_load_factor(), 2);
    }
}
