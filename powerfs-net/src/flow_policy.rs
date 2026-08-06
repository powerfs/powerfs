//! 流控策略 (Phase 1 S2): 可插拔准入决策
//!
//! `FlowPolicy` trait 定义准入接口, `AdaptiveConcurrencyPolicy` 是默认实现.
//! `FlowController` 持有 `Option<Arc<dyn FlowPolicy>>`, 调用方在
//! `on_request_start` 之前调用 `FlowController::admit()` 获取准入决策.
//!
//! # 调用模式
//!
//! ```ignore
//! let decision = fc.admit(conn_id, msg_type, est_bytes);
//! if let AdmissionDecision::Admit = decision {
//!     let stats = fc.get_conn(conn_id).unwrap();
//!     fc.on_request_start(&stats);
//!     // ... 处理请求 ...
//!     fc.on_request_complete(&stats, latency_us, bytes, err);
//! } else {
//!     // 返回 BUSY / EAGAIN
//! }
//! ```
//!
//! # 设计说明
//!
//! - `admit()` 只做决策, 不修改计数 (调用方在 Admit 后调 `on_request_start`)
//! - admit 与 on_request_start 之间的竞态是 best-effort (防雪崩, 非精确限流)
//! - 慢连接的 per-conn 上限减半 (降级, 避免拖累全局)
//! - 上限可通过 AtomicU32 运行时调整 (HTTP API)

use std::sync::atomic::{AtomicU32, Ordering};

use crate::flow_control::{ConnStats, GlobalStats};

/// 准入决策
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionDecision {
    /// 允许, 调用方应调 `on_request_start` 并处理请求
    Admit,
    /// 拒绝, 调用方应返回 BUSY / EAGAIN
    Reject(RejectReason),
}

/// 拒绝原因 (供日志和 metrics 分类)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RejectReason {
    /// per-conn 并发上限
    ConnFull,
    /// 全局并发上限
    GlobalFull,
    /// 慢连接限流 (并发减半后仍满)
    SlowConn,
    /// 令牌桶耗尽 (预留, Phase 3)
    RateLimited,
}

impl RejectReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            RejectReason::ConnFull => "conn_full",
            RejectReason::GlobalFull => "global_full",
            RejectReason::SlowConn => "slow_conn",
            RejectReason::RateLimited => "rate_limited",
        }
    }
}

/// 准入上下文 (借用, 无分配)
pub struct FlowCtx<'a> {
    pub conn: &'a ConnStats,
    pub global: &'a GlobalStats,
    pub msg_type: u16,
    pub est_bytes: usize,
}

/// 流控策略 trait (可插拔)
///
/// Phase 1: 实现 `admit` (准入决策)
/// Phase 2: `load_factor` 用于服务器→客户端负载反馈 (flags bit 6-7)
pub trait FlowPolicy: Send + Sync {
    /// 准入决策: 是否允许新请求
    fn admit(&self, ctx: &FlowCtx) -> AdmissionDecision;

    /// 当前负载因子 (0-3, Phase 2 用; Phase 1 返回 0)
    ///
    /// 0=空闲(0-25%), 1=正常(25-50%), 2=较忙(50-75%), 3=满载(75-100%)
    fn load_factor(&self) -> u8;

    /// 策略名称
    fn name(&self) -> &'static str;
}

/// 默认策略: 自适应并发上限
///
/// 规则:
///   1. 慢连接的 per-conn 上限减半 (向上取整, 避免 max=1 变 0)
///   2. per-conn `active_reqs` 超过上限 → `Reject(ConnFull)` 或 `Reject(SlowConn)`
///   3. 全局 `active_reqs` 超过上限 → `Reject(GlobalFull)`
///   4. 否则 `Admit`
///
/// 上限可通过 `AtomicU32` 运行时调整 (HTTP API / 管理接口).
pub struct AdaptiveConcurrencyPolicy {
    max_active_global: AtomicU32,
    max_active_per_conn: AtomicU32,
}

impl AdaptiveConcurrencyPolicy {
    /// 创建策略, 指定全局和 per-conn 并发上限
    pub fn new(max_active_global: u32, max_active_per_conn: u32) -> Self {
        Self {
            max_active_global: AtomicU32::new(max_active_global),
            max_active_per_conn: AtomicU32::new(max_active_per_conn),
        }
    }

    /// 默认配置: global=256, per-conn=64
    pub fn with_defaults() -> Self {
        Self::new(256, 64)
    }

    pub fn max_active_global(&self) -> u32 {
        self.max_active_global.load(Ordering::Relaxed)
    }

    pub fn max_active_per_conn(&self) -> u32 {
        self.max_active_per_conn.load(Ordering::Relaxed)
    }

    /// 运行时调整全局上限
    pub fn set_max_active_global(&self, v: u32) {
        self.max_active_global.store(v, Ordering::Relaxed);
    }

    /// 运行时调整 per-conn 上限
    pub fn set_max_active_per_conn(&self, v: u32) {
        self.max_active_per_conn.store(v, Ordering::Relaxed);
    }

    /// 计算负载因子 (0-3)
    ///
    /// 基于全局 active/max 比率. Phase 2 由 FlowController 调用并写入响应帧 flags.
    pub fn compute_load_factor(&self, global_active: u32) -> u8 {
        let max = self.max_active_global.load(Ordering::Relaxed);
        if max == 0 {
            return 3; // 无上限配置视为满载 (异常防御)
        }
        // ratio = global_active / max, 映射到 0-3
        // 用乘法避免浮点: (global_active * 4) / max
        let scaled = (global_active as u64) * 4 / max as u64;
        match scaled {
            0 => 0,          // 0-25%
            1 => 1,          // 25-50%
            2 => 2,          // 50-75%
            _ => 3,          // 75-100%+
        }
    }
}

impl FlowPolicy for AdaptiveConcurrencyPolicy {
    fn admit(&self, ctx: &FlowCtx) -> AdmissionDecision {
        let max_per_conn = self.max_active_per_conn.load(Ordering::Relaxed);
        let max_global = self.max_active_global.load(Ordering::Relaxed);
        let is_slow = ctx.conn.slow.load(Ordering::Relaxed);

        // 1. 慢连接: per-conn 上限减半 (向上取整, max=1 时仍为 1)
        let effective_per_conn = if is_slow {
            max_per_conn.div_ceil(2)
        } else {
            max_per_conn
        };

        // 2. per-conn 并发上限
        let conn_active = ctx.conn.active_reqs.load(Ordering::Relaxed);
        if conn_active >= effective_per_conn {
            return AdmissionDecision::Reject(if is_slow {
                RejectReason::SlowConn
            } else {
                RejectReason::ConnFull
            });
        }

        // 3. 全局并发上限
        let global_active = ctx.global.active_reqs.load(Ordering::Relaxed);
        if global_active >= max_global {
            return AdmissionDecision::Reject(RejectReason::GlobalFull);
        }

        AdmissionDecision::Admit
    }

    fn load_factor(&self) -> u8 {
        // Phase 1: 无活跃统计引用, 返回 0
        // Phase 2: FlowController 会调 compute_load_factor(global_active)
        0
    }

    fn name(&self) -> &'static str {
        "adaptive-concurrency"
    }
}

/// Null 策略: 永远放行 (用于禁用流控, 调试场景)
pub struct NullPolicy;

impl FlowPolicy for NullPolicy {
    fn admit(&self, _ctx: &FlowCtx) -> AdmissionDecision {
        AdmissionDecision::Admit
    }
    fn load_factor(&self) -> u8 {
        0
    }
    fn name(&self) -> &'static str {
        "null"
    }
}

/// 策略快照 (可序列化, 供 HTTP API)
#[derive(Debug, Clone, serde::Serialize)]
pub struct PolicySnapshot {
    pub name: &'static str,
    pub max_active_global: u32,
    pub max_active_per_conn: u32,
}

impl AdaptiveConcurrencyPolicy {
    pub fn snapshot(&self) -> PolicySnapshot {
        PolicySnapshot {
            name: "adaptive-concurrency",
            max_active_global: self.max_active_global(),
            max_active_per_conn: self.max_active_per_conn(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flow_control::{Channel, ConnStats, GlobalStats};

    fn make_ctx<'a>(conn: &'a ConnStats, global: &'a GlobalStats) -> FlowCtx<'a> {
        FlowCtx {
            conn,
            global,
            msg_type: 1,
            est_bytes: 1024,
        }
    }

    fn make_conn(id: u64) -> ConnStats {
        ConnStats::new(id, format!("127.0.0.1:{}", id), Channel::Data)
    }

    #[test]
    fn test_admit_when_idle() {
        let policy = AdaptiveConcurrencyPolicy::new(256, 64);
        let conn = make_conn(1);
        let global = GlobalStats::new();
        let ctx = make_ctx(&conn, &global);
        assert_eq!(policy.admit(&ctx), AdmissionDecision::Admit);
    }

    #[test]
    fn test_reject_conn_full() {
        let policy = AdaptiveConcurrencyPolicy::new(256, 4);
        let conn = make_conn(1);
        let global = GlobalStats::new();
        // 填满 per-conn
        for _ in 0..4 {
            conn.active_reqs.fetch_add(1, Ordering::Relaxed);
        }
        let ctx = make_ctx(&conn, &global);
        assert_eq!(
            policy.admit(&ctx),
            AdmissionDecision::Reject(RejectReason::ConnFull)
        );
    }

    #[test]
    fn test_reject_global_full() {
        let policy = AdaptiveConcurrencyPolicy::new(8, 64);
        let conn = make_conn(1);
        let global = GlobalStats::new();
        // 填满全局
        for _ in 0..8 {
            global.active_reqs.fetch_add(1, Ordering::Relaxed);
        }
        let ctx = make_ctx(&conn, &global);
        assert_eq!(
            policy.admit(&ctx),
            AdmissionDecision::Reject(RejectReason::GlobalFull)
        );
    }

    #[test]
    fn test_slow_conn_halved_limit() {
        let policy = AdaptiveConcurrencyPolicy::new(256, 4);
        let conn = make_conn(1);
        let global = GlobalStats::new();
        // 标记为慢
        conn.slow.store(true, Ordering::Relaxed);
        // 慢连接上限 = (4+1)/2 = 2
        for _ in 0..2 {
            conn.active_reqs.fetch_add(1, Ordering::Relaxed);
        }
        let ctx = make_ctx(&conn, &global);
        // 第 3 个应被拒绝 (SlowConn)
        assert_eq!(
            policy.admit(&ctx),
            AdmissionDecision::Reject(RejectReason::SlowConn)
        );
    }

    #[test]
    fn test_slow_conn_ceil_division() {
        // max=1 时, (1+1)/2=1, 不会变 0
        let policy = AdaptiveConcurrencyPolicy::new(256, 1);
        let conn = make_conn(1);
        let global = GlobalStats::new();
        conn.slow.store(true, Ordering::Relaxed);
        let ctx = make_ctx(&conn, &global);
        assert_eq!(policy.admit(&ctx), AdmissionDecision::Admit);
        conn.active_reqs.fetch_add(1, Ordering::Relaxed);
        assert_eq!(
            policy.admit(&ctx),
            AdmissionDecision::Reject(RejectReason::SlowConn)
        );
    }

    #[test]
    fn test_runtime_adjust_limits() {
        let policy = AdaptiveConcurrencyPolicy::new(256, 64);
        assert_eq!(policy.max_active_global(), 256);
        assert_eq!(policy.max_active_per_conn(), 64);

        policy.set_max_active_global(512);
        policy.set_max_active_per_conn(128);
        assert_eq!(policy.max_active_global(), 512);
        assert_eq!(policy.max_active_per_conn(), 128);
    }

    #[test]
    fn test_load_factor_phase1() {
        let policy = AdaptiveConcurrencyPolicy::with_defaults();
        // Phase 1: load_factor() 始终返回 0
        assert_eq!(policy.load_factor(), 0);
    }

    #[test]
    fn test_compute_load_factor() {
        let policy = AdaptiveConcurrencyPolicy::new(100, 64);

        // 0% → 0
        assert_eq!(policy.compute_load_factor(0), 0);
        // 20% → 0
        assert_eq!(policy.compute_load_factor(20), 0);
        // 25% → 1
        assert_eq!(policy.compute_load_factor(25), 1);
        // 49% → 1
        assert_eq!(policy.compute_load_factor(49), 1);
        // 50% → 2
        assert_eq!(policy.compute_load_factor(50), 2);
        // 74% → 2
        assert_eq!(policy.compute_load_factor(74), 2);
        // 75% → 3
        assert_eq!(policy.compute_load_factor(75), 3);
        // 100% → 3
        assert_eq!(policy.compute_load_factor(100), 3);
        // 120% → 3
        assert_eq!(policy.compute_load_factor(120), 3);
    }

    #[test]
    fn test_compute_load_factor_max_zero_defense() {
        let policy = AdaptiveConcurrencyPolicy::new(0, 64);
        // max=0 视为满载 (异常防御)
        assert_eq!(policy.compute_load_factor(0), 3);
    }

    #[test]
    fn test_policy_name() {
        let policy = AdaptiveConcurrencyPolicy::with_defaults();
        assert_eq!(policy.name(), "adaptive-concurrency");
    }

    #[test]
    fn test_null_policy_always_admit() {
        let policy = NullPolicy;
        let conn = make_conn(1);
        let global = GlobalStats::new();
        // 即使满载也放行
        for _ in 0..100 {
            global.active_reqs.fetch_add(1, Ordering::Relaxed);
        }
        let ctx = make_ctx(&conn, &global);
        assert_eq!(policy.admit(&ctx), AdmissionDecision::Admit);
    }

    #[test]
    fn test_reject_reason_as_str() {
        assert_eq!(RejectReason::ConnFull.as_str(), "conn_full");
        assert_eq!(RejectReason::GlobalFull.as_str(), "global_full");
        assert_eq!(RejectReason::SlowConn.as_str(), "slow_conn");
        assert_eq!(RejectReason::RateLimited.as_str(), "rate_limited");
    }

    #[test]
    fn test_policy_snapshot() {
        let policy = AdaptiveConcurrencyPolicy::new(512, 128);
        let snap = policy.snapshot();
        assert_eq!(snap.name, "adaptive-concurrency");
        assert_eq!(snap.max_active_global, 512);
        assert_eq!(snap.max_active_per_conn, 128);
    }

    #[test]
    fn test_admit_does_not_mutate_counters() {
        // admit 只决策, 不修改 active_reqs (调用方负责 on_request_start)
        let policy = AdaptiveConcurrencyPolicy::new(256, 64);
        let conn = make_conn(1);
        let global = GlobalStats::new();
        let ctx = make_ctx(&conn, &global);
        assert_eq!(policy.admit(&ctx), AdmissionDecision::Admit);
        assert_eq!(conn.active_reqs.load(Ordering::Relaxed), 0);
        assert_eq!(global.active_reqs.load(Ordering::Relaxed), 0);
    }
}
