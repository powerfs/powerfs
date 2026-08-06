# PowerFS Lease 设计与一致性方案

> 状态：**已确认方案**
> 编写日期：2026-08-06
> 替代文档：lease-enhancement-plan.md、data-consistency-design.md、posix-metadata-service-design.md、strong-consistency-refactor-plan.md、cache-consistency-fix-plan.md、multi_client_consistency.md

## 1. 背景与问题

### 1.1 当前架构

PowerFS 数据一致性依赖 **per-stripe range lease**（由 Volume Server 管理）：

- **Stripe 粒度**：64MB，文件按 stripe 分布到多个 Volume
- **Lease 持有者**：FUSE 客户端写前向 Volume Server 申请 exclusive lease
- **Lease 校验**：Volume Server 在 write_needle 时校验 token 对应的 stripe range
- **元数据同步**：close 时 FUSE 客户端将 content_size + chunks 列表同步到 Filer（Raft 强一致）

### 1.2 发现的问题

| 问题 | 描述 | 影响 |
|------|------|------|
| **Lease 粒度不匹配** | 客户端缓存 key = `(volume_id, inode)`（per-inode），服务端 key = `StripeKey { inode, stripe_start, stripe_count }`（per-stripe） | 客户端 `has_valid_lease` 误判，写 stripe 1+ 时服务端校验失败 |
| **ensure_lease 只获取 stripe 0** | `build_range_lease_tlv(inode, 0, 1, ...)` 固定获取第一个 stripe | 大文件（>64MB）写 stripe 1+ 时无 lease |
| **isize 缺乏保护** | content_size 在客户端本地更新，close 时"最后写入者胜出"覆盖 Filer | 并发写同一文件时 isize 可能回退，导致数据丢失 |
| **Lease 缓存覆盖** | `acquire_lease` 每次覆盖 `(volume_id, inode)` 的 LeaseInfo | 多 stripe 写时 token 互相覆盖 |

### 1.3 适用场景

PowerFS 需要支持两种后端：

- **Volume Server 有 lease 功能**：标准 PowerFS Volume Server，支持 range lease 管理
- **NVMe-oF target 后端**：仅支持读写，不支持 lease，需要 Filer 提供元数据级一致性保护

## 2. 方案概述

提供两种可配置的 Lease 模式：

| 模式 | 名称 | Lease 管理方 | 适用场景 | isize 保护 |
|------|------|-------------|----------|-----------|
| **D（默认）** | Range Lease | Volume Server | 标准 Volume Server | max 合并 |
| **A** | Inode Metadata Lease | Filer | NVMe-oF target / 简单存储 | Filer 原子更新 |

### 2.1 方案 D：Range Lease + isize Max 合并

**数据一致性**：Volume Server 管理 per-stripe range lease
- 修复客户端 lease 缓存为 per-stripe key：`(volume_id, inode, stripe_start)`
- `ensure_lease` 根据实际写 offset 计算 stripe_start，获取对应 stripe 的 lease
- `has_valid_lease` 检查特定 stripe 的 lease

**isize 一致性**：close 时 content_size = max(filer_size, local_size)
- Filer 端 close 处理：`content_size = max(existing.content_size, request.content_size)`
- chunks 列表合并（union by chunk_index，后者覆盖前者）
- 适用于 append-only 或单客户端大文件场景

**优点**：per-stripe lease 允许并发写不同 stripe，性能高
**缺点**：isize 用 max 合并，并发写同一 stripe 仍需上层协调

### 2.2 方案 A：Inode Metadata Lease

**数据一致性**：无 Volume Server lease，直接写入
- FUSE 客户端直接 write_needle，Volume Server 不校验 lease
- 依赖 Filer 的 inode lease 保证写互斥

**元数据一致性**：Filer 管理 per-inode exclusive lease
- 写前向 Filer 申请 inode metadata lease（exclusive）
- 持有 lease 期间可以更新 content_size + chunks
- close 时原子提交 content_size + chunks 到 Raft
- 释放 lease 后其他客户端可获取新 lease

**isize 一致性**：Filer 原子更新
- close 操作在 Raft 日志中原子提交 content_size + chunks
- 不存在"最后写入者胜出"问题
- 其他客户端 getattr 通过 Raft read 获取最新 isize

**优点**：强一致性，isize 原子更新，不依赖 Volume Server lease 功能
**缺点**：per-inode lease 串行化写操作，并发度低于 range lease

## 3. 配置

### 3.1 配置文件格式

```toml
# powerfs-fuse 配置
[lease]
# Lease 模式：
# - "range" (方案 D，默认): Volume Server 管理 per-stripe range lease
# - "inode"  (方案 A):       Filer 管理 per-inode metadata lease
#                           适用于 NVMe-oF target 等不支持 lease 的后端
mode = "range"

# 通用配置
lease_duration_ms = 30000    # lease 有效期 30s
renew_interval_ms = 10000    # 续租间隔 10s
grace_period_ms = 5000       # 宽限期 5s（客户端崩溃后 lease 过期时间）

# Range lease 配置（mode = "range" 时生效）
[lease.range]
stripe_size = 67108864       # stripe 大小 64MB

# Inode lease 配置（mode = "inode" 时生效）
[lease.inode]
# 无额外配置，使用通用配置
```

### 3.2 配置优先级

```
CLI 参数 > 配置文件 > 默认值（range）
```

缺失配置项必须立即报错，不提供默认值（遵循项目硬约束）。

### 3.3 后端能力探测

FUSE 客户端在 mount 时探测 Volume Server 是否支持 lease：
- 发送 `PROBE` 请求，Volume Server 返回能力列表
- 如果 Volume Server 不支持 lease，自动降级到 inode 模式
- 也可通过配置强制指定模式

## 4. 方案 D 详细设计

### 4.1 Stripe 计算

```
stripe_size = 64MB (可配置)
stripe_index = offset / stripe_size
stripe_start = stripe_index * stripe_size
```

### 4.2 客户端 Lease 缓存修复

**修改前**（per-inode）：
```rust
leases: DashMap<(u64, u64), LeaseInfo>  // (volume_id, inode)
```

**修改后**（per-stripe）：
```rust
leases: DashMap<(u64, u64, u64), LeaseInfo>  // (volume_id, inode, stripe_start)
```

### 4.3 ensure_lease 修复

```rust
fn ensure_lease(&self, inode: u64, offset: u64, len: u64) -> Result<LeaseToken> {
    let stripe_start = (offset / self.stripe_size) * self.stripe_size;
    let stripe_end = ((offset + len - 1) / self.stripe_size + 1) * self.stripe_size;

    // 跨 stripe 时获取所有涉及的 stripe lease
    let mut s = stripe_start;
    while s < stripe_end {
        if !self.has_valid_lease(volume_id, inode, s) {
            let token = self.acquire_lease(inode, s, 1)?;
            self.update_lease(volume_id, inode, s, token, duration);
        }
        s += self.stripe_size;
    }
    Ok(())
}
```

### 4.4 isize Max 合并

Filer 端 close 处理：
```rust
fn handle_close(request: CloseRequest) {
    let existing = self.store.get(inode)?;
    let new_size = request.content_size.max(existing.content_size);

    // chunks 列表合并：union by chunk_index
    let mut chunks = existing.chunks.clone();
    for chunk in request.chunks {
        let idx = chunk.index;
        chunks.insert(idx, chunk);  // 后者覆盖前者
    }

    self.store.update(inode, |entry| {
        entry.content_size = new_size;
        entry.chunks = chunks;
    });
}
```

## 5. 方案 A 详细设计

### 5.1 Inode Metadata Lease

Filer 新增 inode lease 管理：
```rust
struct InodeLeaseManager {
    leases: HashMap<u64, InodeLease>,  // inode -> lease
}

struct InodeLease {
    holder: ClientId,
    expire_at: Instant,
    state: LeaseState,  // Exclusive / Shared / Free
}
```

### 5.2 写路径

```
1. FUSE 客户端 → Filer: ACQUIRE_INODE_LEASE(inode, exclusive)
2. Filer: 校验无其他 holder，授权 exclusive lease
3. FUSE 客户端 → Volume Server: write_needle（无 lease 校验）
4. FUSE 客户端 → Filer: CLOSE(inode, content_size, chunks)
   - Filer 在 Raft 日志中原子提交 content_size + chunks
5. FUSE 客户端 → Filer: RELEASE_INODE_LEASE(inode)
```

### 5.3 isize 原子更新

close 操作在 Raft 日志中原子提交：
```rust
// Filer Raft apply
fn apply_close(&mut state, entry: CloseEntry) {
    // 直接覆盖（持有 exclusive lease，无并发）
    state.inodes.get_mut(entry.inode).unwrap().content_size = entry.content_size;
    state.inodes.get_mut(entry.inode).unwrap().chunks = entry.chunks;
}
```

### 5.4 NVMe-oF target 兼容

- Volume Server 配置 `lease_enabled = false`
- write_needle 跳过 lease 校验
- 一致性完全由 Filer inode lease 保证

## 6. 客户端崩溃恢复

### 6.1 方案 D

- Volume Server lease TTL 过期后自动释放
- 宽限期内拒绝其他客户端的 lease 请求
- 宽限期后允许新 lease

### 6.2 方案 A

- Filer inode lease TTL 过期后自动释放
- 宽限期内拒绝其他客户端的 lease 请求
- 宽限期后允许新 lease
- 崩溃客户端的未提交 chunks 成为孤儿数据，由 GC 清理

## 7. getattr 一致性

两种方案都确保 getattr 获取最新 isize：

- **open 时 getattr**：绕过 TTL，直接从 Filer 获取最新元数据
- **非 open 文件 getattr**：绕过 TTL，从 Filer 获取最新
- **持有 lease 时 getattr**：使用本地 content_size（持有 exclusive lease，无并发）

## 8. 并发场景分析

### 8.1 单客户端大文件顺序写（IOR）

- **方案 D**：逐 stripe 获取 lease，性能好
- **方案 A**：持有一个 inode lease，顺序写，性能可接受

### 8.2 多客户端小文件并发写（mdtest）

- **方案 D**：每个文件 < 64MB，只涉及 stripe 0，无冲突
- **方案 A**：每个文件独立 inode lease，无冲突

### 8.3 多客户端并发写同一文件

- **方案 D**：不同 stripe 可并发，同 stripe 串行；isize 用 max 合并
- **方案 A**：inode lease 串行化所有写，isize 原子更新

### 8.4 推荐配置

| 场景 | 推荐模式 | 原因 |
|------|---------|------|
| 标准 Volume Server | D (range) | 并发性能好 |
| NVMe-oF target | A (inode) | Volume Server 不支持 lease |
| HPC 大文件 | D (range) | per-stripe 并发 |
| 对 isize 一致性要求极高 | A (inode) | 原子更新，无合并风险 |

## 9. 实施计划

### Phase 1：修复方案 D（当前模式）

1. 客户端 lease 缓存改为 per-stripe key
2. `ensure_lease` 按实际 stripe 获取 lease
3. `has_valid_lease` 检查特定 stripe
4. Filer close 处理添加 max 合并逻辑
5. 测试：fio 大文件写、IO500

### Phase 2：实现方案 A（inode lease）

1. Filer 新增 InodeLeaseManager
2. 新增 ACQUIRE_INODE_LEASE / RELEASE_INODE_LEASE 消息
3. FUSE 客户端 lease mode 配置
4. Volume Server `lease_enabled` 配置
5. 测试：NVMe-oF target 模拟、并发写同一文件

### Phase 3：后端能力探测

1. Volume Server PROBE 消息
2. FUSE 客户端自动降级逻辑
3. 集成测试
