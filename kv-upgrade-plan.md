# PowerFS KV 系统生产级升级方案

## 1. 现状分析

### 1.1 当前架构

PowerFS 的 KV 系统目前是一个轻量级内存缓存，主要用于 ML 场景的 KV Cache：

| 组件 | 实现 | 位置 |
|------|------|------|
| 缓存核心 | `KVCache` - 内存 HashMap | [powerfs-core/src/kv_cache.rs](file:///home/portion/powerfs/powerfs-core/src/kv_cache.rs) |
| 持久化 | `KVCachePersist` - 简单文件持久化 | [powerfs-core/src/kv_cache_persist.rs](file:///home/portion/powerfs/powerfs-core/src/kv_cache_persist.rs) |
| CLI 客户端 | `KVClient` | [powerfs-cli/src/kv_client.rs](file:///home/portion/powerfs/powerfs-cli/src/kv_client.rs) |
| 监控指标 | `MetricStore::get_kv_metrics()` | [powerfs-monitor/src/metric_store.rs](file:///home/portion/powerfs/powerfs-monitor/src/metric_store.rs) |

### 1.2 核心数据结构

```rust
struct KVBlock {
    meta: KVBlockMeta,  // block_id, session_id, layer_id, dtype, fid, ...
    data: Vec<u8>,
}

struct KVSession {
    session_id: String,
    model_name: String,
    num_layers: u32,
    dtype: KVDtype,     // FP32/FP16/BF16/FP8/INT8
}
```

### 1.3 问题与差距

| 问题 | 影响 | 优先级 |
|------|------|--------|
| 无多租户隔离 | 不同用户数据相互可见，违反 RBAC 原则 | P0 |
| 纯内存存储 | 重启丢失数据，内存无上限 | P0 |
| 无驱逐策略 | 内存持续增长，可能导致 OOM | P0 |
| 无持久化保障 | 数据仅通过简单文件持久化，可靠性差 | P1 |
| 无数据 Pin 机制 | 无法保护关键数据不被驱逐 | P2 |
| 无分段写入 | 大对象写入效率低 | P3 |

---

## 2. 设计目标

### 2.1 核心原则

1. **向后兼容**：现有 API 消费者（CLI、监控）无需修改即可工作
2. **RBAC 集成**：利用现有的用户/角色/权限系统，不引入新的 `tenant_id` 概念
3. **渐进式升级**：分阶段实施，每阶段可独立测试和验证
4. **利用现有组件**：复用 RocksDB、MetricStore、AlertEngine 等已有基础设施

### 2.2 Mooncake 模式映射

| Mooncake 特性 | PowerFS 映射方案 |
|---------------|------------------|
| `tenant_id` | 映射到现有的 `user_id`（已在 RBAC 中实现） |
| `soft_pin/hard_pin` | 新增 `PinMode` 枚举，控制驱逐行为 |
| `eviction_ratio/high_watermark` | 利用现有 MetricStore 监控，集成 AlertEngine |
| `PutStart/PutEnd` | 新增分段写入 API |
| `GetReplicaListByRegex` | 新增前缀/正则查询 API |
| `Lease TTL` | 新增键值对过期机制 |
| Oplog + 快照 HA | 暂不实现（PowerFS Master 已有 Raft） |

---

## 3. 分阶段实施计划

### Phase 1: 多租户隔离与持久化基础

**目标**：实现基于用户的 KV 数据隔离，将内存 HashMap 替换为 RocksDB 持久化存储

#### 3.1.1 数据模型变更

```rust
// 新增：KV 命名空间概念（替代 Mooncake 的 tenant）
struct KVNamespace {
    id: String,
    name: String,
    owner_id: String,           // 关联到 RBAC 用户
    created_at: Instant,
    updated_at: Instant,
}

// 新增：KVBlock 扩展 owner_id
struct KVBlockMeta {
    block_id: u64,
    session_id: String,
    namespace_id: String,       // 新增：关联命名空间
    owner_id: String,           // 新增：关联用户
    layer_id: u32,
    num_tokens: u32,
    dtype: KVDtype,
    head_dim: u32,
    num_heads: u32,
    size_bytes: u64,
    created_at: Instant,
    last_accessed: Instant,
    ttl: Option<Duration>,
    fid: String,
    block_index: u32,
    pin_mode: PinMode,          // 新增：Pin 模式
}

enum PinMode {
    None,           // 可被驱逐
    Soft,           // 软 Pin，仅在高水位时可驱逐
    Hard,           // 硬 Pin，不可驱逐
}
```

#### 3.1.2 API 变更

| 原有 API | 新增/变更 | 说明 |
|----------|-----------|------|
| `put_block(session_id, block)` | `put_block(user_id, session_id, block)` | 新增 user_id 参数 |
| `get_block(session_id, block_id)` | `get_block(user_id, session_id, block_id)` | 新增 user_id 参数 |
| `batch_put(session_id, blocks)` | `batch_put(user_id, session_id, blocks)` | 新增 user_id 参数 |
| `batch_get(session_id, block_ids)` | `batch_get(user_id, session_id, block_ids)` | 新增 user_id 参数 |
| - | `create_namespace(user_id, name)` | 新增命名空间创建 |
| - | `list_namespaces(user_id)` | 新增命名空间列表 |
| - | `delete_namespace(user_id, namespace_id)` | 新增命名空间删除 |

#### 3.1.3 实现步骤

1. 在 `KVCache` 中添加 `owner_id` 参数验证
2. 使用 RocksDB 替换 HashMap，按 `owner_id` 组织数据
3. 实现命名空间 CRUD 操作
4. 更新 CLI 客户端传递用户信息

#### 3.1.4 测试验证

- 不同用户创建的 KV 数据相互隔离
- 用户无法访问其他用户的 KV 数据
- 重启后数据通过 RocksDB 恢复
- 现有 CLI 命令正常工作

---

### Phase 2: 驱逐策略与资源管理

**目标**：实现基于 LRU 的驱逐策略，配置高水位线，集成监控告警

#### 3.2.1 核心配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `kv_max_memory_mb` | 8192 | KV 缓存最大内存（MB） |
| `kv_high_watermark` | 0.95 | 高水位线比例 |
| `kv_eviction_ratio` | 0.05 | 每次驱逐比例 |
| `kv_soft_pin_ttl_minutes` | 30 | 软 Pin 默认过期时间 |

#### 3.2.2 驱逐流程

```
1. 写入数据 → 检查内存使用
2. 如果内存 > high_watermark * max_memory:
   a. 按 LRU 顺序收集可驱逐数据（排除 hard pin）
   b. 如果 soft pin 数据超过 soft_pin_ttl，加入驱逐列表
   c. 驱逐数据直到内存 < (1 - eviction_ratio) * high_watermark * max_memory
3. 触发告警（如果配置）
```

#### 3.2.3 集成监控

- 利用现有 `MetricStore` 收集 KV 内存使用、命中率、驱逐次数
- 利用现有 `AlertEngine` 触发内存告警
- 在 Monitor 前端展示 KV 资源使用面板

#### 3.2.4 测试验证

- 内存超过高水位时自动触发驱逐
- Hard pin 数据不被驱逐
- Soft pin 数据在 TTL 过期后可被驱逐
- 驱逐后内存恢复到安全水平
- 告警系统正确触发

---

### Phase 3: 高级功能增强

**目标**：实现分段写入、正则查询、Lease TTL 等高级功能

#### 3.3.1 分段写入 API

```rust
struct PutStartRequest {
    user_id: String,
    session_id: String,
    key: String,
    total_size: u64,
    num_slices: u32,
}

struct PutSliceRequest {
    user_id: String,
    session_id: String,
    key: String,
    slice_index: u32,
    data: Vec<u8>,
}

struct PutEndRequest {
    user_id: String,
    session_id: String,
    key: String,
}

// 流程：PutStart → N × PutSlice → PutEnd
```

#### 3.3.2 正则/前缀查询

```rust
fn list_keys_by_prefix(user_id: &str, session_id: &str, prefix: &str) -> Vec<String>;
fn list_keys_by_regex(user_id: &str, session_id: &str, pattern: &str) -> Vec<String>;
```

#### 3.3.3 Lease TTL 机制

```rust
// 写入时指定 TTL
fn put_with_ttl(user_id: &str, session_id: &str, block: KVBlock, ttl: Duration);

// 续期
fn renew_lease(user_id: &str, session_id: &str, block_id: u64, ttl: Duration);

// 后台清理过期数据（定期任务）
```

#### 3.3.4 测试验证

- 大对象分段写入正确性
- 前缀/正则查询准确性
- TTL 过期自动清理
- 续期功能正常工作

---

### Phase 4: 性能优化与稳定性

**目标**：优化性能，提高系统稳定性

#### 3.4.1 性能优化

| 优化项 | 方案 |
|--------|------|
| 批量操作 | 优化批量 put/get 的网络和存储开销 |
| 内存管理 | 使用高效的数据结构，减少内存碎片 |
| 读写分离 | 支持读多写少场景的性能优化 |
| 压缩存储 | 对 FP16/FP8 数据进行专用压缩 |

#### 3.4.2 稳定性增强

| 增强项 | 方案 |
|--------|------|
| 限流保护 | 对每个用户/命名空间设置 QPS 限制 |
| 错误处理 | 完善的错误码体系和重试机制 |
| 监控增强 | 增加延迟、吞吐量等关键指标 |
| 熔断机制 | 防止单个用户占用过多资源 |

#### 3.4.3 测试验证

- 性能基准测试（吞吐量、延迟）
- 压力测试（高并发场景）
- 稳定性测试（长时间运行）
- 故障恢复测试（模拟节点重启）

---

## 4. 向后兼容性

### 4.1 API 兼容策略

1. **可选参数**：新增的 `user_id` 参数在旧 API 中可选（默认使用 admin 用户）
2. **版本控制**：API 路径添加版本号（如 `/api/v1/kv/...`）
3. **迁移工具**：提供数据迁移脚本，将旧数据迁移到新的多租户结构

### 4.2 现有消费者影响

| 消费者 | 影响 | 处理方式 |
|--------|------|----------|
| `powerfs-cli kv` | 需要传递用户信息 | CLI 登录后自动携带 token |
| Monitor 指标 | 需要更新查询 | 修改 MetricStore 查询逻辑 |
| FUSE 挂载 | 间接使用 | 保持现有接口不变 |

---

## 5. 风险评估

| 风险 | 概率 | 影响 | 缓解措施 |
|------|------|------|----------|
| 性能回归 | 中 | 高 | 详细的性能测试和基准对比 |
| 数据丢失 | 低 | 高 | 完善的持久化和备份策略 |
| API 兼容性 | 低 | 中 | 向后兼容设计和迁移工具 |
| 内存泄漏 | 中 | 高 | 严格的驱逐策略和监控 |

---

## 6. 实施时间线

| 阶段 | 预估时长 | 主要交付物 |
|------|----------|------------|
| Phase 1 | 2-3 周 | 多租户隔离、RocksDB 持久化、命名空间管理 |
| Phase 2 | 2 周 | LRU 驱逐、高水位控制、告警集成 |
| Phase 3 | 2 周 | 分段写入、正则查询、Lease TTL |
| Phase 4 | 2 周 | 性能优化、限流保护、稳定性增强 |

---

## 7. 验证标准

### 7.1 功能验证

- ✅ 多租户数据隔离（不同用户数据相互不可见）
- ✅ 持久化（重启后数据恢复）
- ✅ 驱逐策略（内存自动控制）
- ✅ Pin 机制（soft/hard pin 行为正确）
- ✅ 分段写入（大对象正确组装）

### 7.2 性能验证

- ✅ 写入吞吐量：> 100 MB/s
- ✅ 读取延迟：< 10ms（99%）
- ✅ 驱逐耗时：< 1s（驱逐 5% 数据）

### 7.3 代码质量

- ✅ `cargo fmt --check --all` 通过
- ✅ `cargo clippy --all -- -D warnings` 通过
- ✅ 单元测试覆盖率 > 80%
- ✅ GitHub Actions 全部通过
