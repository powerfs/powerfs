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

---

## 10. Lease 读写锁模型（客户端-服务端完整交互）

> 状态：**设计方案，待实施**
> 编写日期：2026-08-21
> 核心原则：**以 lease 锁为缓存权威性的唯一依据**

### 10.1 设计目标

当前实现中，inode lease **仅支持 Exclusive 模式**（`InodeLeaseManager::acquire` 硬编码 `LeaseMode::Exclusive`），读取路径不持有任何 lease。这导致：

- 多客户端并发读同一文件时，无 lease 保护 → 缓存权威性依赖 TTL + Invalidate 通知
- 客户端 A 读完后释放缓存，客户端 B 写入，客户端 A 再次读需 RPC 往返
- 无法实现"多读者共存、缓存共享"的语义

本方案引入 **Shared（读锁）+ Exclusive（写锁）** 双模式 lease，使缓存权威性直接由锁类型决定。

### 10.2 锁模式定义

| 模式 | 符号 | 持有者 | 缓存权威性 | 适用路径 |
|------|------|--------|-----------|---------|
| **Shared** | S | 多客户端同时持有 | 本地缓存有效，可直接读 | `open(O_RDONLY)` / `read` / `getattr` / `readdir` |
| **Exclusive** | X | 单客户端排他 | 本地缓存权威，可读可写 | `open(O_WRONLY/O_RDWR)` / `write` / `setattr` / `truncate` |
| **None** | - | 无 | 缓存无效，需 RPC | 首次访问 / lease 过期 / 未获取 |

**兼容矩阵：**

| 已持有 \ 请求 | Shared | Exclusive |
|--------------|--------|-----------|
| **Shared** | 兼容（多读者） | 不兼容（需撤销所有 S） |
| **Exclusive** | 不兼容（需降级/释放） | 不兼容（需释放后重新获取） |

### 10.3 客户端缓存权威性规则

```
客户端 getattr / read 时：
  if 持有 Shared lease(inode):
      → 本地缓存权威，直接返回（无 RPC）
  elif 持有 Exclusive lease(inode):
      → 本地缓存权威，直接返回（无 RPC）
  else:
      → 缓存可能过期，发 RPC 到 Filer
```

```
客户端 write / setattr / truncate 时：
  if 持有 Exclusive lease(inode):
      → 直接写本地缓存（标记 Dirty）
  else:
      → 必须先 acquire Exclusive lease，获取成功后方可写
```

### 10.4 客户端-服务端完整交互流程

#### 场景 1：客户端 A 读文件（无其他人持有锁）

```
Client A                          Filer
  |                                 |
  |-- ACQUIRE(inode, Shared) -----> |
  |                                 |-- 无冲突，授权 S lease
  |<-------- Grant(token, TTL) ---- |
  |                                 |
  |-- GETATTR(inode) -------------> |  (首次访问，缓存 MISS)
  |<-------- attr + data ---------- |
  |                                 |
  |  [本地缓存填充，标记 Clean]      |
  |  [后续 read/getattr 直接用缓存] |
  |                                 |
  |  (TTL 到期前续约 RENEW) -------> |
  |<-------- Renewed -------------- |
```

#### 场景 2：客户端 B 要写文件，客户端 A 持有 Shared lease

```
Client A (S holder)        Filer                   Client B (writer)
  |                          |                         |
  |                          |<- ACQUIRE(inode, X) --- |
  |                          |                         |
  |                          |--- 冲突：A 持有 S -------|
  |                          |    推送 Revoke 通知      |
  |<--- REVOKE(inode) ------|--- 排队 B 的请求 --------|
  |                          |                         |
  |  [A 无脏数据（S lease    |                         |
  |   只读），直接释放]       |                         |
  |                          |                         |
  |--- REVOKE_ACK ---------> |                         |
  |    (release S lease)     |                         |
  |                          |--- S 已释放，授权 X -----|
  |                          |------ Grant(X) -------->|
  |                          |                         |
  |  [A 的本地缓存标记 Stale] |                         | [B 持有 X lease]
  |  [后续 read 需重新 RPC]   |                         | [B 可写本地缓存]
```

**关键点：**
- Shared lease 持有者**无脏数据**（只读），Revoke 后可立即 ACK
- Filer 收到 ACK 后立即授予 Exclusive lease（Early Grant）
- Client A 的缓存被标记 Stale，下次访问需 RPC

#### 场景 3：客户端 C 要写文件，客户端 B 持有 Exclusive lease

```
Client B (X holder)        Filer                   Client C (writer)
  |                          |                         |
  |                          |<- ACQUIRE(inode, X) --- |
  |                          |                         |
  |                          |--- 冲突：B 持有 X -------|
  |                          |    推送 Revoke 通知      |
  |<--- REVOKE(inode) ------|--- 排队 C 的请求 --------|
  |                          |                         |
  |  [B 有脏数据！            |                         |
  |   必须先 flush]           |                         |
  |                          |                         |
  |--- flush dirty data ---> | (Raft propose)          |
  |--- sync metadata ------> | (Raft commit)           |
  |                          |                         |
  |  [脏数据已落盘]           |                         |
  |--- REVOKE_ACK ---------> |                         |
  |    (release X lease)     |                         |
  |                          |--- X 已释放，授权 X -----|
  |                          |------ Grant(X) -------->|
  |                          |                         |
  |  [B 的本地缓存标记 Stale] |                         | [C 持有 X lease]
  |                          |                         | [C 从 Filer 读取最新数据]
```

**关键点：**
- Exclusive lease 持有者**可能有脏数据**，Revoke 后必须先 flush 再 ACK
- flush 顺序：dirty chunks → Volume Server → sync metadata → Filer (Raft)
- Filer 收到 ACK 后授予新 X lease，C 从 Filer 获取最新数据（服务端权威）

#### 场景 4：客户端 A 要读文件，客户端 B 持有 Exclusive lease

```
Client A (reader)          Filer                   Client B (X holder)
  |                          |                         |
  |-- ACQUIRE(inode, S) ---> |                         |
  |                          |--- 冲突：B 持有 X -------|
  |                          |    推送 Revoke 通知      |
  |                          |<---- B flush + ACK -----|
  |                          |                         |
  |                          |--- X 已释放，授权 S -----|
  |<------- Grant(S) -------|                         |
  |                          |                         |
  |-- GETATTR(inode) ------> |  (读取最新数据)         |
  |<-------- attr + data --- |                         |
  |                          |                         |
  |  [本地缓存权威]           |                         | [B 缓存 Stale]
```

**关键点：**
- 即使 A 只请求 Shared lease，B 持有的 Exclusive lease 也必须先释放
- B 释放前必须 flush 脏数据，确保 Filer 有最新数据
- A 获取 S lease 后从 Filer 读取最新数据，后续读直接用缓存

#### 场景 5：多客户端并发读（Shared lease 共享）

```
Client A           Filer           Client B           Client C
  |                  |                |                  |
  |-- ACQUIRE(S) -> |                |                  |
  |<- Grant(S) ---- |                |                  |
  |                  |<- ACQUIRE(S) --|                  |
  |                  |-- Grant(S) --->|                  |
  |                  |                |<- ACQUIRE(S) ----|
  |                  |-- Grant(S) ----|----------------->|
  |                  |                |                  |
  |  [缓存权威]      |  [缓存权威]    |  [缓存权威]      |
  |  (无 RPC)        |  (无 RPC)      |  (无 RPC)        |
  |                  |                |                  |
  |  [任一客户端 write 触发 EX acquire → Filer Revoke 所有 S holders]
```

#### 场景 6：目录级 Shared lease（已有实现）

```
Client A                           Filer
  |                                  |
  |-- ACQUIRE(dir_inode, Shared) --> |  (readdir 时获取)
  |<------- Grant(S, TTL=30s) ------ |
  |                                  |
  |  [本地 dentry 缓存权威]           |
  |  [lookup / entry_exists 跳过 RPC]|
  |                                  |
  |  [其他客户端 mkdir/create 在该目录]
  |  → Filer 检测到 dir 修改          |
  |  → 推送 Invalidate(dir_inode)    |
  |<---- Invalidate(dir) ----------- |
  |  [本地 dir lease 失效]            |
  |  [下次 readdir 重新获取 lease]    |
```

### 10.5 Lease 释放规则（脏数据屏障）

**核心原则：lease 释放前，该锁保护的所有脏数据必须已成功刷新。**

```
release_lease(inode):
    if 持有 Exclusive lease:
        1. flush dirty chunks → Volume Server   (数据落盘)
        2. sync size/chunks → Filer (Raft)      (元数据落盘)
        3. 验证 flush + sync 均成功
        4. 成功 → release lease + mark cache Clean
        5. 失败 → 保留 lease + 保留 dirty flag + 后台重试
    elif 持有 Shared lease:
        1. 无脏数据（Shared lease 只读）
        2. 直接 release lease + mark cache Stale
    fi
```

**当前实现状态（release 路径）：**

| 步骤 | 当前代码 | 状态 |
|------|---------|------|
| 1. flush dirty chunks | `flush_dirty_chunks_impl(inode)` ([fuse.rs#L6311](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L6311)) | ✅ 已实现 |
| 2. sync metadata | `sync_size_chunks_on_close(inode)` ([fuse.rs#L6336](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L6336)) | ✅ 已实现 |
| 3. 验证成功 | `if sync_result.is_ok()` → clear_dirty ([fuse.rs#L6358](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L6358)) | ✅ 已实现 |
| 4. release lease | `release_inode_lease(inode, ...)` ([fuse.rs#L6449](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L6449)) | ✅ 已实现 |
| 5. flush 失败保留 | `if flush_result.is_ok()` else keep pinned ([fuse.rs#L6401](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L6401)) | ✅ 已实现 |

**结论：release 路径的脏数据屏障已正确实现。**

### 10.6 现有实现差距分析

| # | 差距 | 当前状态 | 目标状态 | 影响 |
|---|------|---------|---------|------|
| G1 | **inode lease 仅 Exclusive** | `acquire()` 硬编码 `LeaseMode::Exclusive` ([inode_lease_manager.rs#L458](file:///home/portion/powerfs/powerfs-filer/src/inode_lease_manager.rs#L458)) | 支持 Shared + Exclusive | 多读者无法共享缓存 |
| G2 | **read 路径不获取 lease** | `read()` 直接读缓存或 RPC，无 lease 获取 ([fuse.rs#L3991](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L3991)) | `open(O_RDONLY)` 时 acquire Shared lease | 读取无锁保护，缓存权威性依赖 TTL |
| G3 | **getattr 不检查 Shared lease** | 仅检查 `is_open`（隐含 EX）或 `EntryState::Clean` ([fuse.rs#L2570](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L2570)) | 检查是否持有 Shared/Exclusive lease | 非 open 文件 getattr 可能绕过有效缓存 |
| G4 | **服务端 acquire_or_wait 不区分锁模式** | `acquire_or_wait` 只处理 Exclusive 冲突 ([inode_lease_manager.rs#L600](file:///home/portion/powerfs/powerfs-filer/src/inode_lease_manager.rs#L600)) | Shared 请求与现有 Shared 兼容；Exclusive 请求撤销所有 Shared | 无法支持多读者 + 写者排队 |
| G5 | **Invalidate 通知不携带锁语义** | `notify_client(inode, version)` 仅通知 inode 失效 ([inode_notifier.rs#L98](file:///home/portion/powerfs/powerfs-filer/src/inode_notifier.rs#L98)) | Revoke 通知携带目标锁模式（S→释放/降级，X→flush+释放） | 客户端无法区分"释放"还是"降级" |

### 10.7 实施计划

#### Phase A：服务端 Shared lease 支持

1. `InodeLeaseManager::acquire` 新增 `mode: LeaseMode` 参数，不再硬编码 Exclusive
2. `MemoryLeaseStore` 支持多 Shared holder + 单 Exclusive holder 的兼容性检查
3. `acquire_or_wait` 根据请求模式决定冲突处理：
   - Shared 请求 vs Shared 持有 → 直接授予
   - Shared 请求 vs Exclusive 持有 → 排队 + Revoke X holder
   - Exclusive 请求 vs Shared 持有 → 排队 + Revoke 所有 S holders
   - Exclusive 请求 vs Exclusive 持有 → 排队 + Revoke X holder
4. Revoke 通知携带目标模式（`REVOKE_SHARED` / `REVOKE_EXCLUSIVE`）

#### Phase B：客户端 Shared lease 获取

1. `open(O_RDONLY)` → acquire Shared lease（TTL = lease_duration_ms）
2. `open(O_WRONLY/O_RDWR)` → acquire Exclusive lease（已有逻辑，保持不变）
3. `read()` 前检查 Shared lease 有效性，过期则续约
4. `getattr` 优先级链：
   ```
   持有 X lease → 本地权威
   持有 S lease → 本地权威
   is_open（兼容旧逻辑） → 本地权威
   EntryState::Clean → 本地权威
   else → RPC to Filer
   ```
5. `release` 时：Shared lease → 直接释放（无脏数据）；Exclusive lease → flush + sync + 释放（已有逻辑）

#### Phase C：Revoke 处理优化

1. 客户端收到 `REVOKE_SHARED` → 立即释放 S lease（无脏数据），ACK
2. 客户端收到 `REVOKE_EXCLUSIVE` → flush dirty → sync metadata → 释放 X lease → ACK
3. 可选：锁降级（X → S）避免完全释放后重新获取的 RPC 开销

#### Phase D：测试验证

1. 单客户端：read 后 getattr 无 RPC（S lease 缓存命中）
2. 双客户端并发读：两者同时持有 S lease，无冲突
3. 读-写切换：A 持 S → B 请求 X → A 被 revoke → B 获取 X → B 写入 → B 释放 → A 重新获取 S 读到最新
4. 写-读切换：B 持 X 写入 → A 请求 S → B flush + revoke ACK → A 获取 S 读到最新
5. lease 释放脏数据屏障：X lease 持有者 release 时 flush 失败 → lease 不释放 → 后台重试

### 10.8 与目录级 Lease 的关系

| 维度 | 目录级 Shared lease | 文件级 Shared lease | 文件级 Exclusive lease |
|------|--------------------|--------------------|----------------------|
| 粒度 | 父目录 inode | 文件 inode | 文件 inode |
| 获取时机 | `readdir` / `opendir` | `open(O_RDONLY)` | `open(O_WRONLY/O_RDWR)` |
| 缓存对象 | dentry 缓存（正/负） | inode 元数据 + 数据 | inode 元数据 + 数据 |
| 失效触发 | 其他客户端修改该目录 | 其他客户端请求 X lease | 其他客户端请求 S/X lease |
| 脏数据 | 无（目录 lease 只读） | 无（Shared 只读） | 有（write 标记 Dirty） |
| 释放条件 | 直接释放 | 直接释放 | flush + sync 成功后释放 |
| 已实现 | ✅ | ❌ (Phase B) | ✅ |

---

## 11. 多客户端-服务端联动：状态机与冲突仲裁详解

> 状态：**设计方案，待实施**
> 编写日期：2026-08-21
> 核心原则：**所有可能冲突的操作，底层核心都是 lease 锁机制**

### 11.1 服务端 Lease 状态机（per-inode）

服务端对每个 inode 维护一个 lease 状态，驱动客户端的缓存权威性。

```
                         ┌──────────┐
            acquire(S)   │          │  acquire(S) [兼容]
       ┌────────────────>│  Shared  │<──────────────┐
       │                 │ (N holders)│              │
       │                 └────┬─────┘              │
       │                      │                    │
       │     acquire(X)       │ acquire(X)         │
       │     [冲突→Revoke S]  │ [冲突→Revoke S]    │
       │     all S holders    │                    │
       │     ACK后授予 X      │                    │
       │                      ▼                    │
       │                 ┌──────────┐              │
       │     acquire(S)  │          │  release(X)  │
       │     [冲突→Revoke X]│Exclusive│─────────────┤
       │     flush+ACK后  │ (1 holder)│             │
       │     授予 S       └────┬─────┘             │
       │                      │                    │
       │                      │ release(X)         │
       │                      │ flush+sync成功      │
       │                      ▼                    │
       │                 ┌──────────┐              │
       │                 │   Free   │──────────────┘
       └────────────────>│ (0 holders)│  acquire(S)
                         └──────────┘
```

**状态定义：**

| 状态 | 持有者数量 | 缓存权威性 | 脏数据可能 |
|------|-----------|-----------|-----------|
| **Free** | 0 | 无缓存权威 | 无 |
| **Shared** | 1~N | 所有 S holder 本地缓存权威 | 无（只读） |
| **Exclusive** | 1 | X holder 本地缓存权威 | 有（write 标记 Dirty） |
| **Revoking** | 过渡态 | holder 缓存待失效 | flush 中 |

**状态转换条件表：**

| 当前状态 | 事件 | 目标状态 | 服务端动作 |
|---------|------|---------|-----------|
| Free | acquire(S) from C1 | Shared | 授予 S lease，返回 token |
| Free | acquire(X) from C1 | Exclusive | 授予 X lease，返回 token |
| Shared | acquire(S) from C2 [兼容] | Shared | 授予 S lease（多 holder） |
| Shared | acquire(X) from C2 [冲突] | Revoking→Exclusive | Revoke 所有 S holders → ACK 后授予 X |
| Shared | release(S) from Ci [最后一个] | Free | 释放 lease |
| Shared | TTL expire [最后一个] | Free | 自动过期 |
| Exclusive | acquire(S) from C2 [冲突] | Revoking→Shared | Revoke X holder → flush+ACK → 授予 S |
| Exclusive | acquire(X) from C2 [冲突] | Revoking→Exclusive | Revoke X holder → flush+ACK → 授予 X |
| Exclusive | release(X) from C1 | Free | flush+sync 验证 → 释放 lease |
| Exclusive | TTL expire + grace | Free | 自动过期，宽限期后可获取 |
| Revoking | holder ACK (S lease) | Shared/Exclusive | 授予排队 waiter |
| Revoking | holder ACK (X lease, flush done) | Shared/Exclusive | 授予排队 waiter |
| Revoking | revoke_timeout (2s) | Shared/Exclusive | 强制回收 + 罚分 + 授予 waiter |

### 11.2 客户端 Lease 状态机（per-inode）

客户端为每个 inode 维护本地 lease 状态，决定缓存是否权威。

```
                         ┌──────────┐
       open(O_RDONLY)    │          │  release / TTL expire
       acquire(S) OK     │   None   │─────────────────────┐
       ┌────────────────>│          │                     │
       │                 └──────────┘                     │
       │                                                   │
       │     ┌──────────────┐                              │
       │     │              │  REVOKE(S) 通知              │
       │     │    Shared    │  [无脏数据，立即ACK]          │
       ├────>│  (read OK)   │─────────────────────┐        │
       │     │              │                      │        │
       │     └──────────────┘                      │        │
       │                                           ▼        │
       │                                    ┌──────────┐   │
       │     open(O_WRONLY) acquire(X) OK   │Revoking  │   │
       │     ┌──────────────┐               │(flush中) │   │
       ├────>│              │  REVOKE(X)    │          │   │
       │     │  Exclusive   │  通知         └────┬─────┘   │
       │     │  (read/write)│──────────────┐     │ACK done │
       │     │              │              │     │         │
       │     └──────┬───────┘              │     ▼         │
       │            │                      │  ┌──────────┐ │
       │            │ release:             │  │  None    │<┘
       │            │ 1.flush dirty        │  └──────────┘
       │            │ 2.sync metadata      │
       │            │ 3.release lease      │
       │            ▼                      │
       │     ┌──────────────┐              │
       │     │  Flushing    │              │
       │     │  (release中)  │──────────────┘
       │     └──────────────┘
       │            │ flush+sync OK
       │            ▼
       │     ┌──────────┐
       └────>│   None   │
             └──────────┘
```

**客户端状态定义：**

| 状态 | 含义 | 缓存权威 | 允许操作 |
|------|------|---------|---------|
| **None** | 无 lease | 否（需 RPC） | 首次访问 / 已释放 |
| **Shared** | 持有 S lease | 是（只读） | read, getattr |
| **Exclusive** | 持有 X lease | 是（读写） | read, write, setattr, truncate |
| **Flushing** | release 中，flush 脏数据 | 是（过渡） | 等待 flush 完成 |
| **Revoking** | 收到 Revoke 通知 | 否（待失效） | flush（如 X）→ ACK → None |

**客户端状态转换条件表：**

| 当前状态 | 事件 | 目标状态 | 客户端动作 |
|---------|------|---------|-----------|
| None | open(O_RDONLY) + acquire(S) OK | Shared | 填充缓存，标记 Clean |
| None | open(O_WRONLY/WR) + acquire(X) OK | Exclusive | 填充缓存，标记 Clean |
| Shared | read / getattr | Shared | 直接返回本地缓存（无 RPC） |
| Shared | write 请求 | None→Exclusive | 释放 S → acquire(X) → 写缓存 |
| Shared | REVOKE 通知 | None | 释放 S lease → ACK → 缓存标记 Stale |
| Shared | release(fd) | None | 释放 S lease → 缓存标记 Stale |
| Shared | TTL expire | None | 本地 lease 失效 → 缓存标记 Stale |
| Exclusive | write / setattr | Exclusive | 写本地缓存，标记 Dirty |
| Exclusive | read / getattr | Exclusive | 直接返回本地缓存（无 RPC） |
| Exclusive | REVOKE 通知 | Flushing→None | flush dirty → sync meta → 释放 X → ACK |
| Exclusive | release(fd) | Flushing→None | flush dirty → sync meta → 释放 X → 缓存 Clean |
| Exclusive | TTL expire | None | 本地 lease 失效 → 缓存 Stale（脏数据由后台 flush） |
| Flushing | flush+sync OK | None | 释放 lease → 缓存 Clean（保留权威副本） |
| Flushing | flush+sync FAIL | Exclusive | 保留 lease + Dirty → 后台重试 |

### 11.3 多客户端联动列表：服务端仲裁表

服务端 `InodeLeaseManager` 维护 per-inode 的 holder 列表和 waiter 队列。

**服务端数据结构（per-inode）：**

```
InodeLeaseState {
    inode: u64,
    // 当前持有者列表（Shared 可多个，Exclusive 仅 1 个）
    holders: Vec<LeaseHolder>,
    // 等待队列（FIFO，冲突时排队）
    waiters: VecDeque<Waiter>,
    // 待处理的 Revoke（sent_at + holder + token）
    pending_revoke: Option<RevokeState>,
    // SN 分配器（Early Grant 的 IO 排序）
    sn_allocator: SnAllocator,
}

LeaseHolder {
    client_id: String,
    token: String,
    mode: LeaseMode,        // Shared or Exclusive
    acquired_at: Instant,
    expire_at: Instant,
    epoch: u64,             // Fencer epoch（zombie-client fencing）
}

Waiter {
    client_id: String,
    requested_mode: LeaseMode,  // Shared or Exclusive
    duration_ms: u64,
    sender: oneshot::Sender<AcquireResult>,  // Early Grant 回调
}

RevokeState {
    sent_at: Instant,
    holder: String,         // 被 Revoke 的 holder
    token: String,
    target_mode: LeaseMode, // 请求者的锁模式（决定 holder 如何 ACK）
}
```

**服务端仲裁矩阵（请求模式 × 当前持有模式）：**

| 请求模式 | 当前持有 | 仲裁结果 | 服务端动作 |
|---------|---------|---------|-----------|
| Shared | Free | ✅ 授予 | 返回 token，加入 holders |
| Shared | Shared (N holders) | ✅ 授予 | 返回 token，加入 holders（N+1） |
| Shared | Exclusive (C1) | ⏳ 排队 | Revoke C1 → C1 flush+ACK → 授予 S 给请求者 |
| Exclusive | Free | ✅ 授予 | 返回 token，加入 holders |
| Exclusive | Shared (C1,C2,...) | ⏳ 排队 | Revoke **所有** S holders → 全部 ACK → 授予 X 给请求者 |
| Exclusive | Exclusive (C1) | ⏳ 排队 | Revoke C1 → C1 flush+ACK → 授予 X 给请求者 |
| Shared (同 client) | Shared (同 client) | ✅ 幂等 | 返回已有 token |
| Exclusive (同 client) | Exclusive (同 client) | ✅ 幂等 | 返回已有 token |
| Shared (同 client) | Exclusive (同 client) | ✅ 降级 | 降级 X→S（可选优化） |
| Exclusive (同 client) | Shared (同 client) | ✅ 升级 | 升级 S→X（需检查无其他 S holder） |

### 11.4 所有可能冲突的操作及其 Lease 处理

#### 11.4.1 操作冲突矩阵

| 操作 \ 持有者 | C1 持 S | C1 持 X | 无人持锁 |
|--------------|---------|---------|---------|
| **C2 read** | ✅ C2 acquire(S) 兼容，直接授予 | ⏳ Revoke C1(X) → C1 flush+ACK → C2 acquire(S) | ✅ C2 acquire(S) |
| **C2 write** | ⏳ Revoke C1(S) → C1 ACK(无脏) → C2 acquire(X) | ⏳ Revoke C1(X) → C1 flush+ACK → C2 acquire(X) | ✅ C2 acquire(X) |
| **C2 getattr** | ✅ C2 acquire(S) 兼容，或直接读 Filer | ⏳ Revoke C1(X) → C1 flush+ACK → C2 acquire(S) | ✅ C2 acquire(S) 或直接读 Filer |
| **C2 setattr/truncate** | ⏳ Revoke C1(S) → C1 ACK → C2 acquire(X) | ⏳ Revoke C1(X) → C1 flush+ACK → C2 acquire(X) | ✅ C2 acquire(X) |
| **C2 unlink** | ⏳ Revoke C1(S) → C1 ACK → C2 acquire(X) | ⏳ Revoke C1(X) → C1 flush+ACK → C2 acquire(X) | ✅ C2 acquire(X) |
| **C2 rename** | ⏳ Revoke C1(S) → C1 ACK → C2 acquire(X) | ⏳ Revoke C1(X) → C1 flush+ACK → C2 acquire(X) | ✅ C2 acquire(X) |

**关键规则：**
- **读操作（read/getattr）**：请求 Shared lease，与现有 Shared 兼容
- **写操作（write/setattr/truncate/unlink/rename）**：请求 Exclusive lease，与任何现有 lease 冲突
- **所有修改操作必须先获取 X lease**，这是唯一的数据一致性保证

#### 11.4.2 冲突处理流程（服务端视角）

```
acquire(inode, client_id, mode):
    ┌─ Fast path: 无冲突 ──────────────────────────────┐
    │ if 当前无 holder 或 (mode=S 且 所有 holder=S):  │
    │     授予 lease, 返回 token                        │
    │     分配 SN (Early Grant 排序用)                  │
    │     incr MetaCache refcount                       │
    │     return Granted                                │
    └───────────────────────────────────────────────────┘
    │
    ├─ 幂等 path: 同 client 重获取 ─────────────────────┐
    │ if holder == client_id:                          │
    │     返回已有 token (SN=0, 复用原 SN)              │
    │     return Granted                                │
    └───────────────────────────────────────────────────┘
    │
    ├─ Grace period path: 过期但宽限期内 ────────────────┐
    │ if 旧 holder 过期 但 未超 grace_period:          │
    │     return Error("grace period")                  │
    └───────────────────────────────────────────────────┘
    │
    └─ Conflict path: 排队 + Early Revoke ──────────────┐
      │ 排入 waiters 队列 (FIFO)                        │
      │ if 是第一个 waiter:                              │
      │     推送 REVOKE 通知给所有冲突 holder:           │
      │       - S holder(s): "释放 S, 无脏数据"          │
      │       - X holder: "flush 脏数据, 释放 X"         │
      │     记录 pending_revoke (sent_at, holder, token)│
      │ return Queued(receiver)                          │
      └───────────────────────────────────────────────────┘

handle_revoke_ack(inode, token, client_id):
    ┌─ 释放 holder 的 lease ────────────────────────────┐
    │ 验证 token + holder 匹配                          │
    │ 从 holders 列表移除                               │
    │ decr MetaCache refcount                           │
    │ 清除 pending_revoke                               │
    └───────────────────────────────────────────────────┘
    │
    └─ Early Grant: 授予下一个 waiter ──────────────────┐
      │ pop waiters 队列 (FIFO)                         │
      │ if 队列空: return (inode 进入 Free)             │
      │                                                   │
      │ 检查 waiter 请求的 mode 是否与剩余 holders 兼容:  │
      │   - waiter 请求 S: 总是兼容 (S vs S)            │
      │   - waiter 请求 X: 需无任何剩余 holder           │
      │                                                   │
      │ if 兼容:                                          │
      │     授予 lease, 分配新 SN                        │
      │     通过 oneshot::Sender 通知 waiter             │
      │     if 队列还有 waiter 且新 holder 是 S:         │
      │         继续授予后续 S waiter (批量授予)          │
      │ else:                                             │
      │     继续等待 (可能需要 Revoke 更多 holder)        │
      └───────────────────────────────────────────────────┘
```

#### 11.4.3 Revoke 超时强制回收

```
force_reclaim_expired_revokes() [后台定时扫描]:
    ┌─ 扫描所有 pending_revoke ─────────────────────────┐
    │ for each (inode, revoke_state) in pending:       │
    │     if now - revoke_state.sent_at > revoke_timeout_ms (2s): │
    │         强制释放该 holder 的 lease                │
    │         记录健康罚分 (ClientHealth penalty)       │
    │         授予排队 waiter (Early Grant)             │
    │         清除 pending_revoke                       │
    └───────────────────────────────────────────────────┘
```

### 11.5 客户端 Revoke 处理流程

客户端收到服务端 Revoke 通知时的处理逻辑（区分 S/X 锁）：

```
on_revoke_notify(inode, current_mode):
    ┌─ Shared lease 被撤销 ─────────────────────────────┐
    │ if current_mode == Shared:                        │
    │     // S lease 只读，无脏数据                      │
    │     本地缓存标记 Stale (缓存失效，不删除)         │
    │     release_lease(inode, token) → 服务端          │
    │     发送 REVOKE_ACK                              │
    │     本地状态 → None                              │
    └───────────────────────────────────────────────────┘
    │
    ┌─ Exclusive lease 被撤销 ──────────────────────────┐
    │ if current_mode == Exclusive:                     │
    │     // X lease 可能有脏数据，必须先 flush          │
    │     1. flush dirty chunks → Volume Server         │
    │     2. sync size/chunks → Filer (Raft commit)     │
    │     3. 验证 flush + sync 成功                     │
    │     4. release_lease(inode, token) → 服务端       │
    │     5. 发送 REVOKE_ACK                           │
    │     6. 本地缓存标记 Stale                         │
    │     7. 本地状态 → None                            │
    │                                                   │
    │     // flush 失败时:                              │
    │     //   保留 lease, 保留 Dirty                   │
    │     //   不发 ACK → 服务端 2s 后强制回收           │
    │     //   客户端被罚分 (ClientHealth)              │
    └───────────────────────────────────────────────────┘
```

### 11.6 Lease 生命周期完整时序（含 SN 和 Epoch）

```
时间轴 ──────────────────────────────────────────────────────────────►

T0: C1 open(O_RDONLY) file F
    C1 → Filer: ACQUIRE(F, S, duration=30s)
    Filer: store.acquire(F, C1, Shared, 30s) → token=T1, SN=1, epoch=E1
    Filer → C1: Grant(token=T1, sn=1, expire=T0+30s)
    C1: 本地缓存权威 (Shared)

T1: C2 open(O_RDONLY) file F
    C2 → Filer: ACQUIRE(F, S, duration=30s)
    Filer: S vs S 兼容 → store.acquire(F, C2, Shared, 30s) → token=T2, SN=2
    Filer → C2: Grant(token=T2, sn=2, expire=T1+30s)
    C2: 本地缓存权威 (Shared)

T2: C3 open(O_WRONLY) file F  [冲突！]
    C3 → Filer: ACQUIRE(F, X, duration=30s)
    Filer: X vs S 冲突 → 排队 C3
    Filer → C1: REVOKE(F, T1)  ┐
    Filer → C2: REVOKE(F, T2)  ┘ 推送给所有 S holder
    Filer: record pending_revoke(sent_at=T2)

T3: C1 收到 REVOKE (S lease, 无脏数据)
    C1: 缓存标记 Stale
    C1 → Filer: REVOKE_ACK(F, T1)
    Filer: 释放 C1 的 S lease, refcount--
    Filer: 检查 waiter C3 请求 X, 但 C2 仍持有 S → 继续等待

T3+5ms: C2 收到 REVOKE (S lease, 无脏数据)
    C2: 缓存标记 Stale
    C2 → Filer: REVOKE_ACK(F, T2)
    Filer: 释放 C2 的 S lease, refcount--
    Filer: 无剩余 S holder → Early Grant C3
    Filer: store.acquire(F, C3, Exclusive, 30s) → token=T3, SN=3
    Filer → C3: Grant(token=T3, sn=3) [via oneshot channel]
    C3: 从 Filer 读取最新数据, 本地缓存权威 (Exclusive)

T4: C3 write "hello" to F
    C3: 本地缓存写入, 标记 Dirty (无需 RPC, 持有 X lease)

T5: C1 open(O_RDONLY) file F [冲突！]
    C1 → Filer: ACQUIRE(F, S, duration=30s)
    Filer: S vs X 冲突 → 排队 C1
    Filer → C3: REVOKE(F, T3)

T6: C3 收到 REVOKE (X lease, 有脏数据！)
    C3: flush dirty → Volume Server (data="hello")
    C3: sync size/chunks → Filer (Raft commit: size=5, chunks=[...])
    C3 → Filer: REVOKE_ACK(F, T3)
    Filer: 释放 C3 的 X lease
    Filer: Early Grant C1
    Filer: store.acquire(F, C1, Shared, 30s) → token=T4, SN=4
    Filer → C1: Grant(token=T4, sn=4)
    C1: 从 Filer 读取最新数据 (size=5, data="hello"), 本地缓存权威

T7: 如果 C3 未在 2s 内 ACK (T2+2s)
    Filer: force_reclaim_expired_revokes()
    Filer: 强制释放 C3 的 X lease
    Filer: 记录 C3 罚分 (ClientHealth)
    Filer: Early Grant C1 (C1 读取最新已 sync 的数据)
    [C3 的未 flush 脏数据成为孤儿, 由 GC 清理]
```

### 11.7 SN（序列号）与 Epoch 的作用

| 机制 | 作用 | 分配时机 | 使用场景 |
|------|------|---------|---------|
| **SN** | IO 操作排序，保证 Early Grant 后写入顺序正确 | 每次 Grant 分配 `sn = atomic_fetch_add(1)` | 客户端写入携带 SN，Volume Server 按 SN 排序应用 |
| **Epoch** | Fencer epoch，防止 zombie client 复活后用旧 token 写入 | 每次 leader 切换 `epoch = atomic_fetch_add(1)` | lease 携带 epoch，旧 epoch 的 token 被拒绝 |
| **Grace Period** | 客户端崩溃后的安全窗口，防止数据损坏 | lease 过期后 `grace = max(5s, 3*p99_renew_lateness)` | 宽限期内拒绝其他 client 的 acquire |

**SN 排序示例：**
```
C1 持 X lease, SN=1, 写 data_v1
C2 请求 X lease → Revoke C1 → C1 flush data_v1 → ACK
C2 获 X lease, SN=2, 写 data_v2
Volume Server: 按 SN 排序 → data_v1 先于 data_v2 应用
→ 即使 C2 的 write 网络先到, 也不会覆盖 data_v1
```

### 11.8 当前实现差距与改造点（更新）

基于代码审查，底层 `MemoryLeaseStore` **已支持 Shared/Exclusive 兼容性检查**（[store.rs#L381](file:///home/portion/powerfs/powerfs-lease/src/store.rs#L381)）：

```rust
// Conflict if either side is exclusive AND keys overlap
if (existing.mode.is_exclusive() || mode.is_exclusive())
    && existing.key.conflicts(&key)
```

但 `InodeLeaseManager` 层有以下差距需改造：

| # | 差距 | 当前代码 | 改造点 |
|---|------|---------|-------|
| G1 | acquire 硬编码 Exclusive | [inode_lease_manager.rs#L458](file:///home/portion/powerfs/powerfs-filer/src/inode_lease_manager.rs#L458) `.acquire(key, client_id, LeaseMode::Exclusive, ...)` | 新增 `mode: LeaseMode` 参数 |
| G2 | 幂等检查不区分 mode | [inode_lease_manager.rs#L421](file:///home/portion/powerfs/powerfs-filer/src/inode_lease_manager.rs#L421) `if entry.holder == client_id` | 需检查 `entry.mode == 请求 mode`，否则触发升级/降级 |
| G3 | acquire_or_wait 不区分 mode | [inode_lease_manager.rs#L600](file:///home/portion/powerfs/powerfs-filer/src/inode_lease_manager.rs#L600) | Shared 请求 vs Shared 持有 → 直接授予；Exclusive 请求 vs Shared 持有 → Revoke 所有 S |
| G4 | Revoke 不携带目标 mode | [early_grant.rs#L163](file:///home/portion/powerfs/powerfs-filer/src/early_grant.rs#L163) `RevokeState` 无 mode 字段 | 新增 `target_mode` 字段，客户端区分 S-revoke（立即ACK）vs X-revoke（flush后ACK） |
| G5 | Waiter 不记录请求 mode | [early_grant.rs#L180](file:///home/portion/powerfs/powerfs-filer/src/early_grant.rs#L180) `Waiter` 无 mode 字段 | 新增 `requested_mode` 字段，grant_to_waiter 时检查兼容性 |
| G6 | grant_to_waiter 不检查兼容性 | [inode_lease_manager.rs#L725](file:///home/portion/powerfs/powerfs-filer/src/inode_lease_manager.rs#L725) | 授予 X waiter 前需确认无剩余 S holder；授予 S waiter 时可批量授予 |
| G7 | 客户端 read 路径不获取 S lease | [fuse.rs#L3991](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L3991) `read()` | open(O_RDONLY) 时 acquire(S) |
| G8 | 客户端 getattr 不检查 S lease | [fuse.rs#L2570](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L2570) | 优先级链：持 X→持 S→is_open→Clean→RPC |

### 11.9 实施优先级（修订）

| 优先级 | Phase | 内容 | 依赖 |
|-------|-------|------|------|
| P0 | A1 | 服务端 `InodeLeaseManager::acquire` 新增 mode 参数（最小改动） | 无 |
| P0 | A2 | `Waiter` 和 `RevokeState` 新增 mode 字段 | A1 |
| P0 | A3 | `acquire_or_wait` 按 mode 分支冲突处理 | A1, A2 |
| P0 | A4 | `grant_to_waiter` 检查 mode 兼容性，支持批量 S 授予 | A3 |
| P1 | B1 | 客户端 `open(O_RDONLY)` acquire Shared lease | A1-A4 |
| P1 | B2 | 客户端 `getattr` 优先级链加入 S lease 检查 | B1 |
| P1 | B3 | 客户端 `read` 路径检查 S lease 有效性 | B1 |
| P2 | C1 | Revoke 通知携带 target_mode | A2 |
| P2 | C2 | 客户端 S-revoke 立即 ACK，X-revoke flush 后 ACK | C1, B1 |
| P3 | D1 | 锁降级 X→S（避免完全释放+重新获取） | A1-A4, B1 |
| P3 | D2 | 多客户端联动测试（读-写切换、写-读切换、并发读） | 全部 |

---

## 12. 逐路径 Lease 检查与完善计划

> 状态：**逐条检查清单，按路径递增实施**
> 编写日期：2026-08-21
> 核心原则：**每个 VFS 调用路径必须显式列出 lease 检查点**

### 12.0 请求扩展：lease 信息携带设计

在逐条分析前，先回答"是否要扩展请求"的问题。

**结论：是的，create/open 请求应携带 lease 需求，服务端直接授权。**

#### 12.0.1 当前请求 vs 扩展请求对比

| 操作 | 当前请求 | 扩展请求（新增字段） | 收益 |
|------|---------|-------------------|------|
| **create** | `CreateReq { parent, name, mode, uid, gid }` | `CreateReq { ..., lease_mode: LeaseMode, client_id: String }` | 服务端在 CreateInode Raft propose 中**原子附带 lease 授权**，响应返回 server token。客户端无需 lockify 异步同步，消除同步窗口内的冲突风险 |
| **mkdir** | `MkdirReq { parent, name, mode, uid, gid }` | `MkdirReq { ..., lease_mode: LeaseMode, client_id: String }` | 同上，新建目录的 dir lease 原子授权 |
| **open** | 无显式请求（FUSE open 仅传 inode+flags） | 客户端根据 flags 决定 `lease_mode`，调用 `acquire(S/X)` | open 时显式获取 S/X lease，缓存权威性由锁保证 |
| **read** | 无 lease 信息 | **不需要扩展**。read 通过 open 时获取的 lease 隐式持有 | 无额外 RPC |
| **write** | 无 lease 信息 | **不需要扩展**。write 通过 open 时获取的 X lease 隐式持有 | 无额外 RPC |
| **setattr** | `SetAttrReq { inode, ... }` | `SetAttrReq { ..., lease_token: Option<String> }` | 服务端验证 token 有效性，无需额外 acquire RPC |
| **unlink** | `UnlinkReq { parent, name }` | `UnlinkReq { ..., lease_token: Option<String> }` | 服务端验证父目录 lease，跳过冲突检查 |
| **rename** | `RenameReq { olddir, oldname, newdir, newname }` | `RenameReq { ..., lease_token: Option<String> }` | 服务端验证源/目标目录 lease |

#### 12.0.2 create 直接授权锁的方案（重点）

**当前流程（lockify 异步同步）：**
```
C1 create(file) → Filer: CreateInode
                  Filer: Raft propose CreateInode → commit
C1 ← CreateResp(attr)
C1: lockify_declare_new_inode(inode)  // 本地自宣告 X lease, token="local-..."
C1: async task → Filer: acquire(inode, X)  // 异步同步，可能冲突
                 Filer: 授予 server token
C1: CAS replace local token → server token
```

**问题：** 异步同步窗口内（create 返回 → acquire 完成），local token 未被服务端认可。如果另一个客户端同时访问该 inode，服务端不知道 C1 持有 lease。

**改进方案（create 直接授权）：**
```
C1 create(file) → Filer: CreateInode { lease_mode: Exclusive, client_id: C1 }
                  Filer: Raft propose CreateInode + LeaseGrant(C1, X)
                  Filer: 原子提交：inode 创建 + lease 授权
C1 ← CreateResp(attr, lease_token: "server-xxx", lease_expire: 30s)
C1: 缓存 lease(token="server-xxx", mode=X)  // 无需异步同步
```

**优势：**
- create + lease 原子绑定，无同步窗口
- 消除 lockify 异步同步的冲突风险
- 减少 1 次 acquire RPC（即使是异步的）
- 服务端 lease 状态立即一致

**实现改造点：**

| 层 | 文件 | 改动 |
|----|------|------|
| 协议 | `powerfs-coherence` CreateReq | 新增 `lease_mode: u8` + `client_id: String` 字段 |
| 服务端 | `net_handler.rs` handle_create | CreateInode 后调用 `lease_manager.acquire(inode, client_id, mode)` |
| 服务端 | `meta_shard_manager.rs` create_file | Raft propose 中附带 LeasePut 命令 |
| 客户端 | `fuse.rs` create | 请求携带 lease_mode=Exclusive + client_id；响应中提取 lease_token |
| 客户端 | `fuse.rs` create | 删除 `lockify_declare_new_inode`（被服务端授权取代） |

### 12.1 逐路径检查表

下表列出所有 VFS 路径，标注当前 lease 状态和目标状态。**按实施顺序排列（从简到复杂，递增推进）。**

| # | 路径 | 当前 lease | 目标 lease | 优先级 | 状态 |
|---|------|-----------|-----------|-------|------|
| 1 | **create** | lockify 本地自宣告 X（异步同步） | 服务端直接授权 X（create 请求携带 lease_mode） | P0 | ❌ 待实施 |
| 2 | **mkdir** | lockify 本地自宣告 X（异步同步） | 服务端直接授权 X（mkdir 请求携带 lease_mode） | P0 | ❌ 待实施 |
| 3 | **open(O_RDONLY)** | 不获取 lease（或获取 X，不分模式） | acquire Shared lease | P1 | ❌ 待实施 |
| 4 | **open(O_WRONLY/WR)** | acquire Exclusive lease（已有） | 保持，验证正确性 | P1 | ✅ 已实现 |
| 5 | **read** | 不检查 lease | 检查 S/X lease → 本地权威 | P1 | ❌ 待实施 |
| 6 | **getattr** | 检查 is_open + Clean | 检查 S/X lease → is_open → Clean → RPC | P1 | ❌ 待实施 |
| 7 | **write** | ensure_lease 获取 X | 验证 X lease 有效性，过期则续约 | P1 | ✅ 已实现 |
| 8 | **setattr** | 不检查 lease | 检查 X lease；无则 acquire X | P2 | ❌ 待实施 |
| 9 | **unlink** | 检查 dir lease（entry_exists） | 检查 dir lease + 目标 inode lease 释放 | P2 | ❌ 待实施 |
| 10 | **rmdir** | 检查 dir lease | 同 unlink | P2 | ❌ 待实施 |
| 11 | **rename** | 不检查 lease | 检查源/目标 dir lease + 目标 inode lease | P2 | ❌ 待实施 |
| 12 | **release(O_RDONLY)** | 释放 X lease（错误！应为 S） | 释放 S lease（无脏数据，直接释放） | P1 | ❌ 待实施 |
| 13 | **release(O_WRONLY/WR)** | flush + sync + 释放 X | 保持，验证正确性 | P1 | ✅ 已实现 |
| 14 | **lookup** | 检查 dir lease | 保持，验证正确性 | P1 | ✅ 已实现 |
| 15 | **readdir** | 获取 dir lease | 保持，验证正确性 | P1 | ✅ 已实现 |

### 12.2 逐条详细计划

---

#### 条目 1: create — 服务端直接授权 X lease

**当前代码：** [fuse.rs#L3177-3273](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L3177)

**当前流程：**
1. `acquire_dir_lease(parent)` — 获取父目录 S lease
2. `entry_exists(parent, name)` — 检查文件是否存在（dir lease 命中则跳过 RPC）
3. `meta_client.create(parent, name, mode, uid, gid, shard_id, None)` — RPC 创建
4. `lockify_declare_new_inode(inode)` — 本地自宣告 X lease + 异步同步
5. `invalidate_dir_entries(parent)` — 失效父目录缓存

**问题：**
- lockify 异步同步窗口内，服务端不知道客户端持有 lease
- 另一客户端同时 lookup 该 inode 时，服务端无 lease 记录 → 可能授予冲突 lease

**改造计划：**
1. **协议层**：`CreateReq` 新增 `lease_mode: u8`（0=None, 1=Shared, 2=Exclusive）+ `client_id: String`
2. **客户端**：create 请求携带 `lease_mode=Exclusive, client_id=self.client_id()`
3. **服务端**：`handle_create` 在 CreateInode Raft commit 后，调用 `lease_manager.acquire(inode, client_id, Exclusive)`
4. **服务端**：响应中新增 `lease_token: Option<String>` + `lease_expire_ms: u64`
5. **客户端**：从响应提取 lease_token，直接缓存（替代 lockify）
6. **客户端**：删除 `lockify_declare_new_inode(inode)` 调用

**验证点：**
- create 后立即 write，无额外 acquire RPC
- 另一客户端 lookup 该 inode 时，服务端返回 lease 冲突
- create 失败时不授予 lease

---

#### 条目 2: mkdir — 服务端直接授权 X lease（目录 lease）

**当前代码：** [fuse.rs#L2879-2940](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L2879)

**当前流程：**
1. `entry_exists(parent, name)` — 检查
2. `meta_client.mkdir(parent, name, mode, uid, gid, shard_id)` — RPC
3. `invalidate_dir_entries(parent)` — 失效父目录
4. `lockify_declare_new_inode(attr.inode)` — 本地自宣告 X lease

**改造计划：**
1. **协议层**：`MkdirReq` 新增 `lease_mode` + `client_id`
2. **客户端**：mkdir 请求携带 `lease_mode=Exclusive, client_id`
3. **服务端**：CreateInode(Raft) 后 `lease_manager.acquire(dir_inode, client_id, Exclusive)`
4. **客户端**：响应提取 lease_token，缓存
5. **客户端**：删除 `lockify_declare_new_inode`，改为**同时获取 S lease**（目录创建后客户端需要 readdir，S lease 更合适）

**注意：** mkdir 创建的是目录，后续操作是 readdir/lookup（读操作）。所以 mkdir 应授权 **Shared** lease 而非 Exclusive，除非客户端立即要修改目录内容。

**决策：** mkdir 授权 Shared lease（目录读操作为主），如需修改（rmdir/subdir create）再升级为 X。

---

#### 条目 3: open(O_RDONLY) — 获取 Shared lease

**当前代码：** [fuse.rs#L3509-3989](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L3509)

**当前流程：**
1. 检查 `is_dir` → EISDIR
2. `pin_inode(inode)` — 防止 TTL 过期
3. 如果 `is_inode_lease_mode && fid.is_some()`：`acquire_inode_lease(inode, client_id, X)` — **总是获取 X lease**
4. 否则：不获取 lease

**问题：**
- `open(O_RDONLY)` 也获取 Exclusive lease → 阻止其他客户端并发读
- 应该根据 flags 区分：O_RDONLY → Shared，O_WRONLY/O_RDWR → Exclusive

**改造计划：**
1. **客户端**：open 时检查 `flags & O_ACCMODE`
   - `O_RDONLY` → `acquire(inode, Shared)` → 缓存 S lease
   - `O_WRONLY/O_RDWR` → `acquire(inode, Exclusive)` → 缓存 X lease（已有）
2. **客户端**：`open_file_leases.bind(inode, token, expire_at)` 记录 lease mode
3. **服务端**：`acquire` 支持 mode 参数（Phase A1 改造）

**验证点：**
- `open(O_RDONLY)` 后，另一客户端 `open(O_RDONLY)` 不阻塞（S vs S 兼容）
- `open(O_RDONLY)` 后，另一客户端 `open(O_WRONLY)` 阻塞（S vs X 冲突→Revoke）
- `open(O_WRONLY)` 后，另一客户端 `open(O_RDONLY)` 阻塞（X vs S 冲突→Revoke）

---

#### 条目 4: open(O_WRONLY/O_RDWR) — 验证现有 X lease

**当前代码：** 同条目 3

**状态：** ✅ 已实现，但需验证：
- ensure_lease 缓存复用是否正确
- release 时释放 X lease 是否在 flush 之后

**验证点：**
- open(O_WRONLY) → write → release：lease 在 flush+sync 后释放
- open(O_WRONLY) → 无 write → release：lease 直接释放（无脏数据）

---

#### 条目 5: read — 检查 S/X lease

**当前代码：** [fuse.rs#L3991-4080](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L3991)

**当前流程：**
1. `cache.get_inode(inode)` — 获取缓存 entry
2. Inline 路径：直接从 `inline_buffers` 读取
3. Flat/Stripe 路径：从 chunk_cache 读取

**问题：**
- 不检查任何 lease，直接信任本地缓存
- 如果无 lease 且缓存过期，可能读到旧数据

**改造计划：**
1. **客户端**：read 前检查 lease 有效性
   ```
   if 持有 S lease(inode) → 本地缓存权威，直接读
   elif 持有 X lease(inode) → 本地缓存权威，直接读
   elif is_open（兼容旧逻辑）→ 本地缓存权威，直接读
   else → 检查 EntryState，Stale 则 RPC 刷新
   ```
2. **客户端**：lease 过期时续约（best-effort，不阻塞 read）

**验证点：**
- 持有 S lease 时 read 无 RPC
- 无 lease 且缓存 Stale 时 read 触发 RPC
- lease 过期后续约成功，read 无 RPC

---

#### 条目 6: getattr — 优先级链加入 S lease 检查

**当前代码：** [fuse.rs#L2548-2627](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L2548)

**当前优先级链：**
1. `is_open` → peek_inode（本地权威）
2. `is_dir` → get_inode（本地权威）
3. `EntryState::Clean` → peek_inode（本地权威）
4. else → RPC

**改造后优先级链：**
1. 持有 X lease → peek_inode（本地权威）
2. 持有 S lease → peek_inode（本地权威）
3. `is_open` → peek_inode（兼容旧逻辑）
4. `is_dir` → get_inode
5. `EntryState::Clean` → peek_inode
6. else → RPC

**改造计划：**
1. **客户端**：getattr 开头新增 lease 检查
   ```rust
   if self.lock_manager.state().get_inode(inode).is_some() {
       // 持有 S 或 X lease，本地权威
       if let Some(entry) = self.cache.peek_inode(inode) {
           return Ok((self.create_stat(&entry), ttl));
       }
   }
   ```
2. **验证**：持有 S lease 的非 open 文件 getattr 无 RPC

---

#### 条目 7: write — 验证现有 X lease

**当前代码：** [fuse.rs#L5106-5548](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L5106)

**状态：** ✅ 已实现（ensure_lease 获取 X lease + 缓存复用）

**验证点：**
- 首次 write 时 ensure_lease 获取 X lease
- 后续 write 复用缓存的 lease
- lease 过期时 ensure_lease 续约
- 无 X lease 时 write 失败（不应发生，open 时已获取）

---

#### 条目 8: setattr — 检查 X lease

**当前代码：** [fuse.rs#L2673-2879](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L2673)

**当前流程：**
1. 解析 attr 字段（mode/size/uid/gid/atime/mtime）
2. `meta_client.setattr(inode, ...)` — RPC
3. 更新本地缓存

**问题：**
- 不检查 X lease，直接 RPC
- 另一客户端可能持有 lease，setattr 应先获取 X lease

**改造计划：**
1. **客户端**：setattr 前检查 X lease
   ```
   if 持有 X lease → 直接 setattr RPC（持锁者优先）
   elif 持有 S lease → 释放 S → acquire X → setattr RPC
   else → acquire X → setattr RPC
   ```
2. **协议层**（可选）：`SetAttrReq` 新增 `lease_token`，服务端验证后跳过冲突检查
3. **服务端**：setattr 处理时验证 lease_token，无 token 则检查是否有人持锁

**验证点：**
- 持有 X lease 时 setattr 直接 RPC（无 acquire）
- 无 lease 时 setattr 先 acquire X
- 另一客户端持有 X lease 时 setattr 阻塞（Revoke → flush → ACK）

---

#### 条目 9: unlink — 检查 dir lease + 目标 inode lease

**当前代码：** [fuse.rs#L3031-3170](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L3031)

**当前流程：**
1. `lookup_in_cache` 或 dir lease 检查
2. `cache.dec_nlink(inode)` — 递减 nlink
3. 删除 chunks + cache
4. `meta_client.unlink(parent, name, shard_id)` — RPC
5. `invalidate_dir_entries(parent)`

**问题：**
- 不检查目标 inode 是否被其他客户端持有 lease
- 另一客户端可能正在写该文件（持有 X lease），unlink 会导致数据丢失

**改造计划：**
1. **客户端**：unlink 前检查目标 inode 的 lease
   ```
   if 其他客户端持有目标 inode 的 X lease:
       → Filer Revoke 该 lease → flush → ACK → unlink
   if 本客户端持有 X lease:
       → flush + sync → 释放 X → unlink
   ```
2. **服务端**：unlink 处理时检查目标 inode 是否有活跃 lease
   - 有活跃 lease → Revoke holder → 等待 ACK → unlink
   - 无活跃 lease → 直接 unlink
3. **客户端**：unlink 后释放本客户端持有的目标 inode lease（如有）

**验证点：**
- 另一客户端持 X lease 时 unlink 触发 Revoke
- 本客户端持 X lease 时 unlink 先 flush 再释放
- 无 lease 时 unlink 直接执行

---

#### 条目 10: rmdir — 同 unlink（目录版）

**当前代码：** [fuse.rs#L3001-3031](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L3001)

**改造计划：**
1. 检查目标目录是否有活跃 lease（S 或 X）
2. Revoke 所有 S holder → ACK → rmdir
3. 本客户端持有的目录 lease 释放后 rmdir

---

#### 条目 11: rename — 检查源/目标 dir lease + 目标 inode lease

**当前代码：** [fuse.rs#L6888-6944](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L6888)

**当前流程：**
1. `entry_exists(newdir, newname)` — 检查目标是否存在
2. `meta_client.rename(olddir, oldname, newdir, newname, shard_id)` — RPC
3. `cache.rename(...)` — 更新缓存

**问题：**
- 不检查任何 lease
- 源目录、目标目录、源 inode、目标 inode 都可能有 lease

**改造计划：**
1. **客户端**：rename 前检查：
   - 源目录 S lease（本客户端持有 → OK）
   - 目标目录 S lease（本客户端持有 → OK，否则 acquire）
   - 源 inode lease（如有其他客户端持有 → Revoke）
   - 目标 inode lease（如目标已存在，Revoke 其 lease）
2. **服务端**：rename 处理时验证 lease，Revoke 冲突 holder

**验证点：**
- 源/目标目录有其他客户端 lease 时 rename 触发 Revoke
- 目标文件存在且有 lease 时 Revoke

---

#### 条目 12: release(O_RDONLY) — 释放 S lease

**当前代码：** [fuse.rs#L6080-6500](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L6080)

**当前流程：**
1. Inline 路径：sync inline buffer
2. Flat/Stripe 路径：flush dirty → sync metadata → clear dirty
3. 递减 open_count
4. **释放 lease**（总是释放 X lease）

**问题：**
- `release(O_RDONLY)` 也走释放 X lease 的路径，但 O_RDONLY 应持有 S lease
- 当前不区分 S/X，都调用 `release_inode_lease`

**改造计划：**
1. **客户端**：`open_file_leases` 记录 lease mode（S 或 X）
2. **客户端**：release 时根据 mode 分支：
   ```
   if lease_mode == Shared:
       // S lease 无脏数据，直接释放
       release_inode_lease(inode, token)
       mark cache Stale
   elif lease_mode == Exclusive:
       // X lease 有脏数据，flush + sync + release（已有逻辑）
       flush dirty → sync metadata → release_inode_lease
       mark cache Clean
   ```
3. **验证**：O_RDONLY release 不触发 flush（无脏数据）

---

#### 条目 13: release(O_WRONLY/O_RDWR) — 验证现有 X lease 释放

**状态：** ✅ 已实现

**验证点：**
- flush + sync 成功后释放 X lease
- flush 失败保留 lease + Dirty
- 释放后缓存标记 Clean（保留权威副本）

---

#### 条目 14-15: lookup / readdir — 验证现有 dir lease

**状态：** ✅ 已实现

**验证点：**
- lookup 在 dir lease 有效期内跳过 RPC
- readdir 获取 dir lease
- 其他客户端修改目录后 dir lease 失效

### 12.3 实施顺序（递增推进）

```
阶段 1（P0）: 服务端 Shared lease 支持
  └─ 条目 1: create 直接授权 X lease（协议扩展 + 服务端 + 客户端）
  └─ 条目 2: mkdir 直接授权 S lease（协议扩展 + 服务端 + 客户端）

阶段 2（P1）: 客户端 S lease 获取 + 读路径
  └─ 条目 3: open(O_RDONLY) acquire S lease
  └─ 条目 4: open(O_WRONLY) 验证 X lease（已有）
  └─ 条目 5: read 检查 S/X lease
  └─ 条目 6: getattr 优先级链加入 S lease
  └─ 条目 12: release(O_RDONLY) 释放 S lease
  └─ 条目 13: release(O_WRONLY) 验证 X lease（已有）

阶段 3（P2）: 写路径 + 删除路径
  └─ 条目 7: write 验证 X lease（已有）
  └─ 条目 8: setattr 检查 X lease
  └─ 条目 9: unlink 检查目标 inode lease
  └─ 条目 10: rmdir 同 unlink
  └─ 条目 11: rename 检查源/目标 lease

阶段 4（P1 验证）: 已实现路径回归
  └─ 条目 14: lookup 验证 dir lease
  └─ 条目 15: readdir 验证 dir lease
```

### 12.4 每条验证清单

每条改造完成后，必须验证以下检查点：

| 检查点 | 验证方法 |
|-------|---------|
| **lease 获取** | strace/fuse 日志确认 acquire RPC 调用 |
| **lease 缓存命中** | 连续操作无重复 acquire RPC |
| **缓存权威性** | 持有 lease 时 getattr/read 无 RPC |
| **跨客户端冲突** | client2 操作时 client1 收到 Revoke |
| **脏数据屏障** | X lease release 时 flush 在 lease 释放前 |
| **S lease 快速释放** | S lease release 时无 flush（无脏数据） |
| **TTL 过期** | lease 过期后缓存标记 Stale |
| **Revoke 超时** | 2s 未 ACK → 强制回收 + 罚分 |

---

## 13. 设计纠正：Capability 模型取代互斥锁模型

> 状态：**⚠️ 重要纠正，取代 §10-§12 的互斥锁模型**
> 编写日期：2026-08-21
> 核心纠正：**Lease ≠ 互斥写锁；Lease = 缓存权限租约**

### 13.1 之前设计的根本错误

§10-§12 将 lease 设计为**互斥写锁**，存在致命问题：

| 错误设计 | 后果 |
|---------|------|
| `open(O_RDWR)` 必须 acquire Exclusive lease，冲突时排队 | **open 阻塞** → 违反 POSIX，xfstests generic/065、pjdfstest open 竞争用例全部失败 |
| `write/setattr 必须先获取 X lease` | 第二个写者 open 后无法写，必须等第一个释放 |
| `acquire(X) vs Exclusive 持有 → Revoke + 排队` | open 变成串行排队，多写者场景退化 |
| 把 lease 当成 open 的互斥锁 | HDFS 单写者模型，不兼容 POSIX |

**正确认知：** open 系统调用本身**永不阻塞**，多客户端 O_RDWR 同时打开必须全部成功。lease 控制的是**能否本地缓存**，不是**能否 open/write**。

### 13.2 Capability 三态模型

采用 CephFS 风格的 Capability（cap）模型，取代简单的 Shared/Exclusive 锁。

#### 13.2.1 Cap 能力集

| Cap | 含义 | 持有者可做 | 释放代价 |
|-----|------|-----------|---------|
| **CAP_R** | 本地读缓存许可 | read/getattr 走本地缓存，无 RPC | 无脏数据，直接释放 |
| **CAP_W** | 本地脏写缓存许可 | write 缓存到本地，延迟刷回 | 必须 flush 全部脏页 |
| **CAP_X** | 元数据修改许可 | setattr/truncate 本地修改 size/mtime | 必须 sync 元数据到 Filer |

#### 13.2.2 三态状态机（per-inode）

| 状态 | 场景 | Cap 能力 | IO 行为 | 性能 |
|------|------|---------|---------|------|
| **EXCLUSIVE_WRITE** | 单写者 | CAP_R + CAP_W + CAP_X | 本地缓存读写，write 不走 RPC | 🚀 高性能 |
| **SHARED_WRITE** | ≥2 写者 | 无 CAP_W（CAP_R 可选） | write/truncate 走同步 RPC 到后端 | 🐢 同步 IO |
| **SHARED_READ** | 多读者 | CAP_R | 本地读缓存，无写 | 🚀 高性能 |
| **FREE** | 无打开 | 无 | 首次访问需 RPC | - |

**状态转换（服务端驱动）：**

```
FREE ──open(RDWR) by C1──> EXCLUSIVE_WRITE (C1: R+W+X)
                                │
                                │ open(RDWR) by C2 [冲突！]
                                │ → recall C1 的 CAP_W+CAP_X
                                │ → C1 flush dirty + invalidate cache + ACK
                                │
                                ▼
                            SHARED_WRITE (C1,C2: 无 W, 同步 IO)
                                │
                                │ 所有写者 close
                                │ 剩余 0 个写者
                                │
                                ▼
                            FREE  (或剩 1 个读者 → SHARED_READ)
```

**关键转换规则：**

| 当前状态 | 事件 | 目标状态 | 服务端动作 |
|---------|------|---------|-----------|
| FREE | C1 open(RDONLY) | SHARED_READ | 授予 C1: CAP_R |
| FREE | C1 open(RDWR) | EXCLUSIVE_WRITE | 授予 C1: CAP_R+W+X |
| SHARED_READ | C2 open(RDONLY) | SHARED_READ | 授予 C2: CAP_R（多读者兼容） |
| SHARED_READ | C2 open(RDWR) | **SHARED_WRITE** | recall 所有读者的 CAP_R（可选），授予 C2 无 CAP_W |
| EXCLUSIVE_WRITE | C2 open(RDWR) | **SHARED_WRITE** | recall C1 的 CAP_W+CAP_X → C1 flush+ACK → 双方无 CAP_W |
| EXCLUSIVE_WRITE | C2 open(RDONLY) | SHARED_READ | recall C1 的 CAP_W+CAP_X → C1 flush+ACK → 授予双方 CAP_R |
| SHARED_WRITE | C3 open(RDWR) | SHARED_WRITE | 直接授予（本就同步 IO） |
| SHARED_WRITE | 所有写者 close | FREE | 释放 |
| SHARED_WRITE | 剩 1 个写者 | **EXCLUSIVE_WRITE** | 升级该写者：授予 CAP_W+CAP_X（恢复高性能） |

### 13.3 open 永不阻塞原则

```
open(inode, flags) 的服务端处理：
    ┌─ open 总是成功，永不阻塞 ─────────────────────────┐
    │                                                     │
    │ case 1: 无现有 lease                                 │
    │   open(RDONLY) → 授予 CAP_R → return OK            │
    │   open(RDWR)   → 授予 CAP_R+W+X → return OK        │
    │                                                     │
    │ case 2: 已有 EXCLUSIVE_WRITE (C1)                   │
    │   open(RDONLY) by C2 → recall C1.W+X → grant C2.R   │
    │                       → return OK (不阻塞 C2)       │
    │   open(RDWR)   by C2 → recall C1.W+X                │
    │                       → 降级到 SHARED_WRITE          │
    │                       → return OK (不阻塞 C2)       │
    │                                                     │
    │ case 3: 已有 SHARED_WRITE                           │
    │   open(RDWR) by C3 → 直接 return OK (本就同步 IO)   │
    │                                                     │
    │ case 4: 已有 SHARED_READ (多读者)                    │
    │   open(RDWR) by C2 → recall 读者的 CAP_R（可选）     │
    │                     → 降级到 SHARED_WRITE            │
    │                     → return OK                     │
    └─────────────────────────────────────────────────────┘
    
    注意：recall 是异步的，open 不等 recall 完成。
          recall 期间，旧 holder 的 cap 标记 "recalling"，
          新 open 立即返回，旧 holder 收到 recall 后 flush。
```

**与互斥锁模型的根本区别：**
- 互斥锁模型：open(RDWR) 冲突 → 阻塞/排队 → 等 holder 释放
- Cap 模型：open(RDWR) 冲突 → **立即返回 OK** → 异步 recall 旧 holder → 双方降级

### 13.4 完整场景时序图

#### 场景 1：单写者（EXCLUSIVE_WRITE，高性能）

```
Client A                          Filer
  |                                 |
  |-- open(F, O_RDWR) ------------>|
  |                                 |-- 无冲突，授予 CAP_R+W+X
  |<-------- OK (caps=R+W+X) ------|
  |                                 |
  |  [write "hello" 到本地缓存]     |  (无 RPC！CAP_W 允许)
  |  [setattr size=5 本地]          |  (无 RPC！CAP_X 允许)
  |                                 |
  |-- read(0, 5) ----------------->|  (本地缓存，无 RPC！CAP_R)
  |<-------- "hello" (from cache) -|
  |                                 |
  |-- close ----------------------->|
  |   [flush dirty → Volume Server]|
  |   [sync meta → Filer Raft]     |
  |   [release caps]                |
  |<-------- OK -------------------|
```

#### 场景 2：双写者冲突（降级到 SHARED_WRITE）

```
Client A (EX holder)     Filer                   Client B (new writer)
  |                         |                         |
  |  [持有 CAP_R+W+X]       |                         |
  |  [本地缓存有 dirty]      |                         |
  |                         |<- open(F, O_RDWR) ------|
  |                         |                         |
  |                         |--- open 立即返回 OK ---->|
  |                         |    (B 不阻塞，进入 SHARED_WRITE)
  |                         |                         |
  |<-- RECALL(W+X) ---------|                         |
  |    "降级：释放写缓存能力" |                         |
  |                         |                         |
  |  [flush dirty → Volume] |                         |
  |  [sync meta → Filer]    |                         |
  |  [invalidate local cache]|                        |
  |                         |                         |
  |-- RECALL_ACK ---------->|                         |
  |   (A 降级完成)           |                         |
  |                         |                         |
  |  [A 现在：无 CAP_W]      |                         | [B 现在：无 CAP_W]
  |  [A write 走同步 RPC] --→|←-- B write 走同步 RPC --|
  |                         |                         |
  |  [A read 走同步 RPC] --→|←-- B read 走同步 RPC ---|
  |                         |  (后端保证 POSIX 一致性) |
```

**关键点：**
- B 的 open **立即成功**，不等 A 的 recall
- A 收到 recall 后 flush + invalidate
- 之后双方 write 都走同步 RPC（无本地写缓存）
- POSIX read-after-write 由后端存储保证

#### 场景 3：写者全部 close，升级回 EXCLUSIVE_WRITE

```
Client A (SHARED_WRITE)    Filer
  |                           |
  |  [B 已 close，只剩 A]      |
  |                           |  (检测到写者数 1→0，A 是最后一个)
  |                           |
  |<-- GRANT(W+X) ------------|
  |    "升级：恢复写缓存能力"   |
  |                           |
  |  [A 持有 CAP_R+W+X]        |
  |  [A write 回到本地缓存]     |  (无 RPC，高性能恢复)
  |                           |
  |-- close ----------------->|
  |   [flush + sync + release]|
```

#### 场景 4：多读者（SHARED_READ，本地读缓存）

```
Client A           Filer           Client B           Client C
  |                  |                |                  |
  |-- open(RDONLY)->|                |                  |
  |<- OK (CAP_R) ---|                |                  |
  |                  |<- open(RDONLY)-|                  |
  |                  |- OK (CAP_R) -->|                  |
  |                  |                |<- open(RDONLY)---|
  |                  |- OK (CAP_R) --->-----------------|
  |                  |                |                  |
  |  [read 本地缓存] |  [read 本地]   |  [read 本地]    |
  |  (无 RPC)        |  (无 RPC)      |  (无 RPC)        |
  |                  |                |                  |
  |  [任一客户端 open(RDWR) → recall 所有 CAP_R → 降级 SHARED_WRITE]
```

### 13.5 逐路径 Cap 检查规则（取代 §12）

| # | 路径 | Cap 检查规则 | open 阻塞？ |
|---|------|-------------|------------|
| 1 | **open(RDONLY)** | 授予 CAP_R；与现有 CAP_R 兼容 | ❌ 不阻塞 |
| 2 | **open(RDWR)** | 若已有 EXCLUSIVE holder → recall 其 W+X，降级 SHARED_WRITE；open 立即返回 | ❌ 不阻塞 |
| 3 | **read** | 持有 CAP_R → 本地缓存；无 CAP_R → 同步 RPC | - |
| 4 | **write** | 持有 CAP_W → 本地缓存（延迟刷回）；无 CAP_W → 同步 RPC 到后端 | - |
| 5 | **setattr/truncate** | 持有 CAP_X → 本地修改；无 CAP_X → 同步 RPC | - |
| 6 | **getattr** | 持有 CAP_R → 本地缓存；无 → RPC | - |
| 7 | **create** | 新建文件直接授予 CAP_R+W+X（EXCLUSIVE_WRITE） | ❌ 不阻塞 |
| 8 | **unlink** | recall 目标 inode 的所有 cap → flush → unlink | - |
| 9 | **release(RDONLY)** | 释放 CAP_R（无脏数据） | - |
| 10 | **release(RDWR)** | flush dirty + sync meta → 释放所有 cap；若剩 1 写者则升级 | - |

**与 §12 互斥锁模型的对比：**

| 维度 | §12 互斥锁模型（错误） | §13 Cap 模型（正确） |
|------|---------------------|---------------------|
| open(RDWR) 冲突 | 排队 + Revoke + 等待 ACK | **立即返回 OK**，异步 recall |
| 第二个写者 | 阻塞直到第一个释放 | **不阻塞**，降级到同步 IO |
| write 权限 | 必须持有 X lease 才能 write | open 即可 write，无 CAP_W 走同步 RPC |
| 多写者并发 | 不允许（串行） | **允许**（SHARED_WRITE，同步 IO） |
| POSIX 兼容 | ❌ open 阻塞违反 POSIX | ✅ open 永不阻塞 |
| xfstests | ❌ generic/065 等失败 | ✅ 通过 |

### 13.6 工程关键点

#### 13.6.1 Recall 回调的时序与 fencing

```
recall 风险：客户端网络卡顿 / crash / 不回复

处理机制：
1. Lease 带 TTL（默认 30s）
2. Recall 带 2s 超时
3. 超时未 ACK → 强制回收 cap + fencing token 防护
4. 存储层 IO 必须携带 fencing token（cap epoch）
5. 过期 token 的 IO 直接拒绝（防止 zombie client 写脏数据）

fencing token 流程：
  C1 持有 CAP_W, epoch=5
  C1 网络卡顿，recall 超时
  Filer 强制回收，epoch→6
  Filer 授予 C2（或降级 SHARED_WRITE）
  C1 恢复后 write 携带 epoch=5 → Volume Server 拒绝（5 < 6）
```

#### 13.6.2 O_APPEND 语义

| Cap 状态 | O_APPEND 处理 |
|---------|--------------|
| EXCLUSIVE_WRITE（有 CAP_W） | 客户端本地 append（单写者，无冲突） |
| SHARED_WRITE（无 CAP_W） | **必须下发到 Filer 原子执行**，不能本地 append（多写者会互相覆盖） |

#### 13.6.3 mmap 写

| Cap 状态 | mmap(MAP_SHARED) 写处理 |
|---------|------------------------|
| EXCLUSIVE_WRITE | 正常使用 page cache 脏回写 |
| SHARED_WRITE | **必须作废 mmap 脏页**，msync 强制刷盘；后续 mmap 写走同步路径 |

#### 13.6.4 fcntl/flock 与 lease 的区别

| 机制 | 层级 | 阻塞行为 | 用途 |
|------|------|---------|------|
| **lease/cap** | 内核/DLM 内部 | **不阻塞 open**，控制缓存 | 缓存一致性优化 |
| **fcntl(F_SETLK)** | 应用层 | 阻塞（可超时） | 应用层字节范围锁 |
| **flock** | 应用层 | 阻塞 | 应用层全文件锁 |

**关键：** fcntl/flock 是应用主动调用的互斥锁，可以阻塞；lease/cap 是内核内部的缓存许可，不阻塞 open。两者独立，不要混淆。

### 13.7 修订实施计划

取代 §12.3 的实施顺序：

| 阶段 | 内容 | 核心改动 |
|------|------|---------|
| **P0-A** | 服务端 Cap 状态机 | `InodeLeaseManager` → `CapManager`；三态：EXCLUSIVE_WRITE / SHARED_WRITE / SHARED_READ |
| **P0-B** | open 永不阻塞 | open RPC 总是返回 OK；冲突时异步 recall，不阻塞 open |
| **P0-C** | Cap recall 机制 | recall CAP_W+CAP_X → 客户端 flush+invalidate → ACK |
| **P1-A** | 客户端 write 路径 | 持有 CAP_W → 本地缓存；无 CAP_W → 同步 RPC |
| **P1-B** | 客户端 read/getattr | 持有 CAP_R → 本地缓存；无 → RPC |
| **P1-C** | release 路径 | flush dirty + sync + 释放 cap；检测升级时机 |
| **P2-A** | fencing token | 存储层 IO 携带 epoch，拒绝过期 token |
| **P2-B** | O_APPEND 原子性 | SHARED_WRITE 模式 append 下发 Filer |
| **P2-C** | mmap 作废 | SHARED_WRITE 模式作废 mmap 脏页 |
| **P3** | 升级/降级测试 | 单写者→多写者降级、多写者→单写者升级 |

### 13.8 当前代码差距（基于 Cap 模型重新评估）

| # | 差距 | 当前代码 | Cap 模型目标 |
|---|------|---------|-------------|
| C1 | open 获取 X lease（互斥） | [fuse.rs#L3558](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L3558) `LockMode::Exclusive` | open 总是成功，授予 cap 而非互斥锁 |
| C2 | 无 SHARED_WRITE 状态 | `LeaseMode` 仅 Shared/Exclusive | 新增 SHARED_WRITE 态（无 CAP_W） |
| C3 | write 必须持 X lease | [fuse.rs#L5106](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L5106) ensure_lease | write 持 CAP_W 走本地；无 CAP_W 走同步 RPC |
| C4 | 无 cap recall 回调 | `LeaseRevoker` 仅 Revoke | 新增 recall CAP_W+CAP_X 的异步回调 |
| C5 | 无 fencing token | lease 无 epoch 字段 | IO 请求携带 epoch，存储层校验 |
| C6 | 无升级机制 | 无 | 写者数 1 时升级回 EXCLUSIVE_WRITE |
| C7 | O_APPEND 本地处理 | [fuse.rs#L5106](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L5106) | SHARED_WRITE 模式下发 Filer |
| C8 | mmap 未作废 | 无处理 | SHARED_WRITE 模式作废 mmap 脏页 |
