//! powerfs-lock-health: Client health scoring + adaptive throttle +
//! quarantine pool + Fencer token for PowerFS lock admission control.
//!
//! Implements the three-layer defense mandated by
//! `docs/lock-optimization-plan.md` §8.2 (constraint 8: "故障隔离必备"):
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │  Layer 1: 客户端健康评分 (ClientHealthScore)            │
//! │  - 故障次数、续期成功率、lease 持有时长 P99、churn 率   │
//! │  - 分数: 0-100, < 30 触发 Layer 2/3                     │
//! └────────┬────────────────────────────────────────────────┘
//!          │
//! ┌────────▼─────────────────────────────────────────────────┐
//! │  Layer 2: 自适应限流 (AdaptiveThrottle)                  │
//! │  - 低分客户端 lease 时长自动缩短 (30s → 5s → 1s)        │
//! │  - 高频 churn 客户端 acquire 限速 (令牌桶)              │
//! │  - 非阻塞,只是变慢,不直接拒绝                           │
//! └────────┬────────────────────────────────────────────────┘
//!          │
//! ┌────────▼─────────────────────────────────────────────────┐
//! │  Layer 3: 强制隔离 (Quarantine)                         │
//! │  - 分数 < 10 且持续 N 次 → 加入隔离池                   │
//! │  - 隔离期内 lease 请求直接拒绝 (LockError::Quarantined) │
//! │  - 隔离期可配置 (默认 60s), 期满后分数恢复到 50 重试    │
//! └─────────────────────────────────────────────────────────┘
//! ```
//!
//! Also implements §8.3:
//! - **Fencer token**: lease carries an `epoch`; client restart must
//!   re-register for a new epoch; stale-epoch lease requests are
//!   rejected. Prevents zombie-client writes after split-brain.
//! - **Blacklist**: 3 consecutive quarantine entries → permanent
//!   blacklist (admin-only release).
//!
//! # Integration
//!
//! The filer's `InodeLeaseManager` and the volume's
//! `RangeLeaseManager` consult `ClientHealth::check(client_id)` on
//! every `acquire`:
//! - `HealthDecision::Allow` → proceed normally.
//! - `HealthDecision::Throttle { lease_ms }` → cap the granted
//!   lease duration at `lease_ms`.
//! - `HealthDecision::Quarantine { until }` → reject with
//!   `LockError::Quarantined(client_id)`.
//! - `HealthDecision::Blacklisted` → reject with
//!   `LockError::Quarantined(client_id)` (permanent).
//!
//! Server-side event hooks (called by the lease store):
//! - `record_acquire(client_id)` — bumps acquire count, churn rate.
//! - `record_renew_success / record_renew_failure` — feeds renew
//!   success rate into Layer 1.
//! - `record_lease_failure(client_id)` — bumps failure count.
//! - `record_revoke_ack_timeout(client_id)` — strong penalty
//!   (slow client not responding to Early Revoke ACK, §8.3 point 1).
//! - `record_lease_held_duration(client_id, dur)` — feeds P99
//!   lease-hold duration into Layer 1 (long holds = good).

pub mod config;
pub mod fencer;
pub mod health;

pub use config::HealthConfig;
pub use fencer::Fencer;
pub use health::{ClientHealth, ClientHealthScore, HealthDecision, HealthSnapshot};
