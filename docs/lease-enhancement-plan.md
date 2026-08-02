# Lease 模块增强方案

## 背景与动机

当前 PowerFS 的 lease 实现分散在三处，且近期修复了多个 token 管理相关 bug：

- `powerfs-volume/src/range_lease.rs`：服务端 lease 管理（内存）
- `powerfs-fuse-core/src/lease.rs`：客户端 lease 缓存 + `LeaseManager` trait
- `powerfs-fuse-core/src/volume_client.rs`：`update_lease` / `get_valid_lease_token`

主要问题：
1. token 生命周期手动管理，易泄漏（已修复 `update_lease` token 不更新 bug）
2. lease 状态仅内存，Volume Server 崩溃后丢失
3. epoch counter 重启归零，存在 ABA 风险
4. 无 `remaining()` API，客户端无法在长操作前检查 lease 剩余时间

参考 `dynalock` 的 fence token + TTL 持久化设计，对 lease 模块进行增强改造，并抽离为独立 crate。

## 架构设计

### 独立 crate 分层

```
powerfs-lease (独立 crate，无 PowerFS 业务依赖)
├── 核心 trait
│   ├── LeaseStore: 服务端 lease 存储接口（sync）
│   ├── LeaseManager: 客户端 lease 管理接口（async + 缓存）
│   └── LeasePersistence: 可选持久化后端
├── 内存实现: MemoryLeaseStore<K> (泛化自 RangeLeaseManager)
├── Fence token 生成器: 持久化 epoch + UUID
├── LeaseGuard: RAII 自动释放
└── 测试套件: 纯单元测试，无网络/文件系统依赖

powerfs-volume (依赖 powerfs-lease)
├── StripeLeaseManager: 包装 MemoryLeaseStore<StripeKey>，添加 inode/stripe 语义
├── RocksDBLeaseStore: 实现 LeasePersistence trait (P1)
└── Net handler: TLV 协议适配

powerfs-fuse-core (依赖 powerfs-lease)
├── VolumeLeaseManager: 包装客户端缓存 + 异步 API
└── LeaseGuard 使用: open→release 期间持有 guard，Drop 自动释放
```

### 关键类型

```rust
// powerfs-lease: 泛化资源 key
pub trait LeaseKey: Clone + Eq + Hash + Send + Sync + 'static {
    /// 判断两个 key 是否冲突（范围重叠等）
    fn conflicts(&self, other: &Self) -> bool;
}

// powerfs-volume: stripe key 实现
pub struct StripeKey {
    pub inode: u64,
    pub stripe_start: u64,
    pub stripe_count: u64,
}
impl LeaseKey for StripeKey { /* ... */ }
```

## 增强项

### P0-1: 独立 crate 抽离

**目标**：将 lease 核心逻辑抽离到 `powerfs-lease` crate，不改变现有行为。

**改动**：
- 新建 `powerfs-lease` crate
- 迁移 `LeaseToken`、`LeaseMode`、`LeaseEntry`、`LeaseKey` trait 到新 crate
- `RangeLeaseManager` → `MemoryLeaseStore<K: LeaseKey>`（泛化 inode/stripe 为 K）
- `LeaseManager` trait 迁移到新 crate
- powerfs-volume 和 powerfs-fuse-core 改为依赖 powerfs-lease

**验收**：编译通过，现有测试全部通过，行为不变。

### P0-2: LeaseGuard RAII

**目标**：用 RAII 替代手动 token 管理，杜绝 token 泄漏。

**设计**：
```rust
pub struct LeaseGuard {
    token: LeaseToken,
    manager: Weak<dyn LeaseManager>,
    key_info: GuardKeyInfo, // volume_id, inode 等
    expire_at: Instant,
    released: bool,
}

impl LeaseGuard {
    pub fn token(&self) -> &LeaseToken { &self.token }
    pub fn remaining(&self) -> Duration { self.expire_at.saturating_duration_since(Instant::now()) }
    pub fn is_expired(&self) -> bool { Instant::now() >= self.expire_at }
    pub fn release(mut self) -> Result<()> { /* mark released + RPC */ }
}

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        if !self.released {
            // best-effort 异步释放，避免阻塞 Drop
            if let Some(mgr) = self.manager.upgrade() {
                let mgr = mgr;
                let token = self.token.clone();
                tokio::spawn(async move { let _ = mgr.release_by_token(&token).await; });
            }
        }
    }
}
```

**收益**：`update_lease` bug 类问题从根因消除——guard 持有期间 token 始终有效，guard drop 时自动释放。

### P1-1: Lease 持久化（RocksDB）

**目标**：Volume Server 崩溃后恢复 lease 状态。

**设计**：
- `LeasePersistence` trait：`save(entry)` / `delete(token)` / `load_all()` / `load_epoch()`
- `RocksDBLeaseStore` 实现：lease 存储在 `lease` CF，epoch 存储在 `meta` CF
- acquire/release 时异步 batch write，不阻塞主路径
- Volume Server 启动时 `load_all()` 恢复，跳过已过期 lease

### P1-2: Fence Token 持久化

**目标**：epoch counter 重启后不归零。

**设计**：
- `FenceTokenGenerator`：内存 counter + 持久化基数
- token 格式：`lease-{persisted_epoch_base + local_counter}-{uuid}`
- 启动时从 RocksDB 读取最大 epoch 作为基数
- 每 N 次 generate 或 shutdown 时 flush 基数

### P1-3: remaining() API

**目标**：客户端在长写操作前检查 lease 剩余时间。

**依赖**：P0-2 LeaseGuard

**设计**：
- `LeaseGuard::remaining(&self) -> Duration`
- `LeaseGuard::is_expired(&self) -> bool`
- fuse write 路径在 flush 前 check，剩余不足时先 renew

### P2-1: 批量 Stripe 操作

**目标**：大文件写多个 stripe 时一次获取所有 lease。

**设计**：
- `acquire_batch(keys: &[K], mode) -> Result<Vec<LeaseGuard>>`
- 服务端在一个锁范围内完成所有冲突检查 + 授权

### P2-2: 监控指标

**目标**：lease 相关指标可视化。

**设计**：
- `LeaseStats`：active_count, acquire_total, conflict_total, avg_latency, expired_total
- 通过 powerfs-monitor 暴露为 Prometheus metrics

## 实施计划

| 阶段 | 内容 | 优先级 | 状态 |
|------|------|--------|------|
| 阶段 1 | P0-1 独立 crate 抽离 + P0-2 LeaseGuard RAII | 高 | ✅ 完成 (commit ed9eb20a) |
| 阶段 2 | P1-1 持久化 + P1-2 fence token 持久化 | 高 | ✅ 完成 (commit 0dc0942b) |
| 阶段 2b | P1-3 remaining() 集成到写路径 | 中 | ⏳ API 已就绪，待集成 |
| 阶段 3 | P2-1 批量操作 + P2-2 监控指标 | 低 | ⏳ 待实施 |

### 已完成项详情

#### P0-1: 独立 crate 抽离 (commit ed9eb20a)
- 新建 `powerfs-lease` crate（无 PowerFS 业务依赖）
- `LeaseKey` trait + `MemoryLeaseStore<K>` 泛化自 RangeLeaseManager
- `LeaseManager` trait（客户端异步接口 + 缓存）
- `LeaseGuard` RAII（Drop 自动释放）
- 14 单元测试 + 1 doctest

#### P0-2: LeaseGuard RAII (commit ed9eb20a)
- `LeaseGuard::drop()` fire-and-forget tokio::spawn 释放
- `release()` 显式释放 + `mark_released()` 跳过 Drop
- `remaining()` / `is_expired()` 查询方法
- 4 个 guard 测试

#### P1-1: Lease 持久化 (commit 0dc0942b)
- `LeasePersistence` trait（byte-based，无泛型）
- `encode_entry` / `decode_entry` 序列化（Instant ↔ Unix millis）
- `MemoryLeaseStore` 可选持久化后端
- acquire/renew/release/disconnect_holder/cleanup_expired 全路径持久化
- `load_from_persistence()` 启动恢复
- `RocksDBLeasePersistence` 实现（leases + meta 两个 CF）
- VolumeServer::new 自动接入持久化
- 3 个 RocksDB 测试（basic CRUD、reopen、full roundtrip）

#### P1-2: Fence Token 持久化 (commit 0dc0942b)
- epoch counter 通过 `save_epoch` / `load_epoch` 持久化
- `load_from_persistence` 恢复时设置 epoch = max(current, stored) + 1
- 防止 ABA：重启后 epoch 不归零

## 约束

1. **协议不变**：TLV/gRPC 接口保持兼容，增强在内部
2. **性能不退化**：持久化用异步 batch write，不阻塞 acquire 主路径
3. **渐进迁移**：先抽离 crate（行为不变），再逐步添加增强
4. **测试覆盖**：新 crate 必须有独立单元测试，不依赖运行环境
