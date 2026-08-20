# 分片路由与跨分片操作设计原则

> 本文档定义 PowerFS 分片系统的核心设计原则，特别是**禁止服务间转发**的容错原则，
> 以及跨分片操作（子目录创建等）的正确处理方式。
>
> 状态：生效中（作为后续所有分片相关改动的强制约束）

---

## 1. 核心设计原则：禁止服务间转发

### 1.1 原则声明

**Filer 之间绝不允许互相转发请求。** 任何 Filer 收到不属于自己（非本节点 leader 的 shard）
的写入请求时，必须**立即返回 `STATUS_ERR_REDIRECT`**，由客户端更新路由表后重新发送到正确
的 leader 节点。

```
✗ 错误（禁止）：Filer-A → Filer-B → Raft
✓ 正确（要求）：Filer-A → 客户端(REDIRECT) → Filer-B → Raft
```

### 1.2 容错原理

此原则是为了**容错**，不是性能优化：

| 场景 | 服务间转发 | 客户端重定向 |
|------|-----------|-------------|
| 非 leader 节点正常 | 转发成功，但增加一次 RPC 跳数 | 客户端更新路由，直连 leader |
| 非 leader 节点已 down | 转发超时，请求失败，需重试 | 客户端路由表已知 leader 地址，直连成功 |
| 非 leader 节点慢 | 转发阻塞，拖慢整体 | 客户端直连 leader，不受影响 |
| Leader 频繁变更 | 转发目标反复变化，可能死循环 | 客户端路由表统一更新，无循环 |

**关键洞察**：非 leader 节点**可能就是 down 了**才不是 leader（例如刚崩溃、网络分区）。
向一个可能已故障的节点转发请求，会**加重故障**——请求被吞掉、超时、占用资源。
客户端重定向则把决策权交给持有全局路由视图的客户端，天然容错。

### 1.3 客户端路由表机制

客户端维护 `shard_router`（shard_id → leader_addr 映射），来源：

1. **Master topology 推送**：`GetTopology` 响应包含每个 shard 的 leader 信息
2. **重定向响应学习**：收到 `STATUS_ERR_REDIRECT` 时，从 `Owner` 字段更新对应 shard 的 leader
3. **cache_epoch 失效**：leader 变更时 bump epoch，触发 FUSE 层失效 MetadataCache

**这意味着**：服务端无需知道其他 shard 的 leader 在哪——它只需知道自己是不是某个 shard
的 leader。不是就返回 REDIRECT，让客户端决定。

### 1.4 实现约束

- `RaftGroupManagerV2::propose` / `propose_ff`：非 leader 时**直接返回 `not_leader` 错误**，
  绝不调用 `propose_forward`
- `net_handler`：捕获 `not_leader` 错误，返回 `STATUS_ERR_REDIRECT` + 正确 leader 地址
- 客户端 `ShardedRpcPool`：收到 REDIRECT 后更新 `shard_router` 并重试

**已删除的违规代码**：原 `propose` / `propose_ff` 中的 `propose_forward` 服务间转发逻辑
（含 5 次重试、leader 反查、gRPC 转发）已全部移除。

---

## 2. 分片策略：目录亲和 + 子目录换片

### 2.1 核心策略

| 操作对象 | 分片规则 | 是否跨分片 |
|---------|---------|-----------|
| **文件** | `shard = calculate_shard(parent_inode)` | 否（同父目录 shard） |
| **子目录** | `shard = pick_child_dir_shard(parent) = (parent_shard + 1) % shard_count` | 视 shard_count 而定（见 §2.2） |
| **根目录** | 固定 shard 0 | 否 |

**文件创建**：inode 从 `parent_shard` 分配（`alloc_inode_in_shard(parent_shard)`），
确保 `calculate_shard(inode) == calculate_shard(parent_inode)`，inode record 和 dir entry
**天然在同一 shard**，单 shard 完成，无跨分片。

**子目录创建**：子目录 inode 换 shard 分配（负载均衡）。客户端算
`target_shard = (parent_shard + 1) % shard_count`，inode record 在 `target_shard`，
dir entry 在 `parent_shard`。

### 2.2 目录创建的两种方式

子目录创建根据 `target_shard` 是否等于 `parent_shard` 分为两种方式：

| 方式 | 触发条件 | 通信次数 | 实现 |
|------|---------|---------|------|
| **单 shard 快速路径** | `target_shard == parent_shard`（shard_count ≤ 1） | 1 次 RPC | 客户端发 `Mkdir` 到 parent_shard，服务端原子完成 CreateInode + AddDirEntry |
| **跨 shard 两阶段** | `target_shard != parent_shard`（shard_count > 1，常态） | 3 次 RPC | 客户端协调 Phase A（target_shard）+ Phase B（parent_shard），见 §3 |

**单 shard 快速路径**（shard_count ≤ 1 或特殊场景）：
- 客户端发 `Mkdir`（`MsgType::Mkdir = 0x0014`）到 parent_shard leader
- 服务端 `handle_mkdir` 原子完成 CreateInode + AddDirEntry（同 shard 单次 Raft propose）
- 保留此路径是为了不增加单 shard 场景的通信开销

**跨 shard 两阶段**（shard_count > 1，子目录换片常态）：
- 客户端先 `alloc_inode_batch(target_shard)` 分配 inode
- Phase A: `MkdirPhaseA`（`0x003c`）→ target_shard（CreateInode only）
- Phase B: `MkdirPhaseB`（`0x003d`）→ parent_shard（AddDirEntry only）
- 每个 Phase 独立重定向（非 leader 返回 REDIRECT，客户端更新路由重试）

### 2.3 为什么子目录要换片

如果所有子目录都从 parent_shard 分配 inode（即 `target_shard == parent_shard`），
整个目录树会坍缩到 root 的 shard 0，3 shard × 3 filer 的集群中 shard 1/2 完全闲置
——违反水平扩展目标。

子目录换片让目录树**按层级分散**到多个 shard，实现负载均衡。代价是子目录创建需要
跨 shard 两阶段，但子目录创建频率远低于文件创建，可接受。

---

## 3. 跨分片子目录创建：两阶段客户端协调

### 3.1 设计原则

子目录创建的跨分片场景**绝不通过服务间转发**完成。采用**客户端协调的两阶段提交**：

```
┌────────┐     ┌──────────────┐     ┌──────────────┐
│ Client │     │ target_shard │     │ parent_shard │
│        │     │ (新子目录)    │     │ (父目录)      │
└───┬────┘     └──────┬───────┘     └──────┬───────┘
    │                 │                    │
    │  alloc_inode_batch (target_shard)    │
    │────────────────►│                   │
    │  ← ino          │                   │
    │                 │                    │
    │  Phase A: MkdirPhaseA (CreateInode)  │
    │  (route by target_shard)             │
    │────────────────►│                   │
    │                 │                   │
    │  ← attr         │  CreateInode OK   │
    │◄────────────────│                   │
    │                 │                    │
    │  Phase B: MkdirPhaseB (AddDirEntry) │
    │  (route by parent_shard)            │
    │─────────────────────────────────────►│
    │                                      │
    │                                      │  AddDirEntry OK
    │◄─────────────────────────────────────│
    │                                      │
    ▼                 ▼                    ▼
```

**关键**：三个步骤（alloc + Phase A + Phase B）由**客户端分别路由**到各自 shard
的 leader，各自独立重定向。服务端只处理发到自己手里的单 shard 请求，绝不转发。

### 3.2 服务端处理

| 消息 | 处理函数 | 检查的 leader | 执行操作 |
|------|---------|--------------|---------|
| `MkdirPhaseA` (0x003c) | `handle_mkdir_phase_a` | target_shard | `create_directory_phase_a`：仅 `CreateInode` |
| `MkdirPhaseB` (0x003d) | `handle_mkdir_phase_b` | parent_shard | `create_directory_phase_b`：仅 `AddDirEntry` + notify |

每个 handler 开头 `check_leader`，非 leader 直接返回 `STATUS_ERR_REDIRECT`，
客户端更新 `shard_router` 后重试该 Phase。

### 3.3 失败处理

| 失败点 | 状态 | 恢复策略 |
|--------|------|---------|
| alloc_inode_batch 失败 | 无副作用 | 客户端重试或返回错误 |
| Phase A 失败 | 无副作用（inode 已分配但未创建 record） | 客户端重试或返回错误（inode 号浪费，可接受） |
| Phase A 成功，Phase B 失败 | 孤儿 inode（target_shard 有 inode record，但无 dir entry 指向） | GC 扫描清理无引用的 inode record |

**孤儿 inode 容忍性**：少量孤儿 inode 不影响正确性（无 dir entry 指向则不可访问），
由后台 GC 定期清理。这比服务间转发的复杂性和故障风险可接受得多。

### 3.4 性能优化：父目录概要信息（可选增强）

为减少跨分片读放大（ls 父目录时需跨分片取子目录详细信息），可在父目录 shard 存储子目录的
**概要信息**：

| 存储位置 | 存储内容 | 用途 |
|---------|---------|------|
| `parent_shard` | 子目录 dir entry + 概要属性（inode, mode, size, mtime, **target_shard_id**） | `ls` / `stat` 子目录可直接返回，无需跨分片 |
| `target_shard` | 子目录完整 inode record + 子目录内部 dentry | 子目录内部操作（ls 子目录、创建文件） |

**lookup(parent, name) 流程**：
1. 查 `parent_shard` 的 dir entry → 得到 inode + 概要属性（含 target_shard_id）
2. 若概要属性满足请求（如 `getattr` 只需 mode/size/mtime），直接返回
3. 若需完整属性或操作子目录内部，按 target_shard_id 路由到目标 shard

**此优化为可选项**，初版实现基础两阶段（alloc + Phase A + Phase B），后续按性能需求添加概要信息。

---

## 4. 其他跨分片操作

### 4.1 跨目录重命名

沿用 [shard-optimization-design.md §5.1](shard-optimization-design.md) 的**乐观 2PC**，
由客户端协调：

```
Phase 1: Prepare
  - Client → old_shard: prepare_rename（写 redirect/tombstone）
  - Client → new_shard: prepare_rename（写新条目）
Phase 2: Commit
  - Client → old_shard: commit（删除源条目）
  - Client → new_shard: commit（确认新条目）
```

**绝不服务间转发**：每个 Phase 由客户端直连对应 shard 的 leader。

### 4.2 删除文件

`calculate_shard(inode) == calculate_shard(parent_inode)`（文件同分片），删除单 shard 完成，
无跨分片。

### 4.3 删除子目录

跨分片（子目录 inode 在 target_shard，dir entry 在 parent_shard），两阶段：
- Phase A: `RemoveDirEntry` → parent_shard
- Phase B: `DeleteInode` → target_shard

---

## 5. Leader 变更通知

### 5.1 客户端路由更新机制

客户端 `shard_router` 的 leader 信息更新来源：

1. **主动同步**：定期或事件触发调用 `GetTopology`，全量刷新路由表
2. **重定向学习**：收到 `STATUS_ERR_REDIRECT` 时增量更新单个 shard 的 leader
3. **cache_epoch 失效**：leader 变更时 FUSE 层 bump epoch，失效 MetadataCache

### 5.2 服务端职责

服务端**不主动推送** leader 变更通知（避免复杂的长连接管理），而是：
- 非 leader 收到写入请求时返回 `STATUS_ERR_REDIRECT` + 当前 leader 地址
- 客户端据此更新路由表并重试

**这是拉模型（pull-based）**，简单可靠，无需服务端维护客户端连接列表。

### 5.3 openraft 的 leader 变更检测

openraft 的 `Raft::metrics()` 提供 `WatchReceiver<ServerMetrics>`，可监听 `state` 字段变化
（Leader/Follower/Candidate）。服务端可用此机制预判自己是否是某 shard 的 leader，
避免无谓的 `client_write` 调用（openraft 在非 leader 时返回 `ForwardToLeader` Fatal 错误）。

当前 `propose` 实现已在调用 `client_write` 前检查 `metrics().state == Leader`，
非 leader 时直接返回 `not_leader` 错误。

---

## 6. 实现状态

### 6.1 已完成

- [x] 删除 `propose` / `propose_ff` 中的服务间转发逻辑
- [x] 非 leader 时返回 `not_leader` 错误，由 `net_handler` 转 `STATUS_ERR_REDIRECT`
- [x] 文件创建单 shard（`alloc_inode_in_shard(parent_shard)`）
- [x] 客户端 `shard_router` + 重定向处理（`meta_shard_client.rs`）
- [x] `cache_epoch` 失效机制
- [x] 子目录创建两阶段客户端协调（`MkdirPhaseA` + `MkdirPhaseB`）
- [x] 单 shard 快速路径保留（`target_shard == parent_shard` 时走原 `Mkdir`）
- [x] `ShardMap::shard_count()` 供客户端算 `pick_child_dir_shard`

### 6.2 待实现

- [ ] 跨目录重命名客户端协调 2PC
- [ ] 孤儿 inode GC 扫描
- [ ] 父目录概要信息优化（可选）

### 6.3 已知技术债

- `sharding-and-dot-fix-plan.md` 中提到的 `handle_propose` 转发已废弃（已在该文档标注）
- 原 `propose_create_inode_and_direntry` 仍保留用于文件 create（单 shard 场景），
  子目录 create 已改走客户端两阶段，不再调用此函数

---

## 7. 目录级 Lease：客户端目录内容缓存一致性

### 7.1 问题背景

文件创建路径中，FUSE `create` 回调会调用 `entry_exists(parent, name)` 检查文件是否已存在。
在缓存 MISS 时，`entry_exists` 会向 Filer 发 lookup RPC。对于 `cp 1000 files` 到同一目录
的场景，每个文件名都是新的 → 缓存 MISS → **每个 create 都附带一次 lookup RPC**，
使文件创建从 1 RPC 变成 2 RPC，IOPS 减半。

单客户端场景下，父目录的内容只被本客户端修改，lookup RPC 完全冗余。但现有 lease 机制
只针对**文件 inode**（写入路径），不覆盖**目录 inode**，导致目录内容缓存无法得到
一致性保证，客户端不敢跳过 lookup。

### 7.2 设计原则：目录 Shared Lease

引入**目录级 Shared lease**，让客户端在持有目录 lease 期间信任本地 dentry 缓存，
跳过 lookup RPC。

```
客户端持有 parent_dir 的 Shared lease:
  ├─ lookup(parent, name) → 命中缓存直接返回，MISS 时才发 RPC
  ├─ entry_exists(parent, name) → 只查缓存，不发 RPC（持有 lease 即认为缓存有效）
  ├─ create(parent, name) → 跳过 entry_exists RPC，直接发 create RPC
  └─ readdir(parent) → 命中缓存直接返回，MISS 时才发 RPC

客户端修改目录（create/mkdir/unlink/rmdir 成功后）:
  └─ 本地 invalidate 该目录的 dentry 缓存条目（但保持 lease）
     下次 readdir/lookup 重新从 Filer 拉取

其他客户端修改同一目录:
  └─ 服务端 lease 冲突 → 持有方 lease 失效（lockify CAS 冲突）
     下次 lookup 发现 lease 失效 → 重新 acquire + 发 RPC
```

### 7.3 Lease 模式选择

| 操作 | Lease 模式 | 原因 |
|------|-----------|------|
| `opendir` / `readdir` | **Shared** | 只读目录内容，多客户端可并发读 |
| `lookup` / `entry_exists` | 复用已持有的 Shared | 不单独 acquire |
| `create` / `mkdir` / `mknod` | **Exclusive**（目录 lease 升级） | 修改目录内容 |
| `unlink` / `rmdir` | **Exclusive**（目录 lease 升级） | 修改目录内容 |

**实现简化**：初始版本统一用 **Shared lease**。修改目录的操作（create/mkdir/unlink/rmdir）
不升级 lease，而是：
1. 执行修改 RPC（Filer 端 Raft 提交）
2. 本地 invalidate 该目录的 dentry 缓存
3. 保持 Shared lease（因为修改是自己发起的，缓存失效后重新拉取即可）

这样避免了 Shared→Exclusive 升级的死锁风险（两个客户端同时升级会死锁）。
Exclusive 升级留作后续优化，当前 Shared 模式已足够覆盖单客户端性能场景。

### 7.4 服务端：零改动

服务端 `InodeLeaseManager::acquire(inode, client_id, duration_ms)` **不区分文件/目录** ——
任何 inode 都能 acquire lease。目录 lease 完全是**客户端侧优化**，服务端无需任何修改。

冲突检测依赖现有机制：
- 客户端 A 持有 dir 的 Shared lease
- 客户端 B 对 dir 下文件执行 create → Filer 端 Raft 提交 AddDirEntry
- 客户端 A 的 lockify 后台同步 CAS 冲突 → 本地 lease 失效
- 客户端 A 下次 lookup 发现 lease 失效 → 重新 acquire + 发 RPC

### 7.5 客户端实现要点

**1. opendir / readdir 路径**：
```rust
fn opendir(...) {
    // 对父目录 acquire Shared lease（lockify self-declare，后台同步）
    let _ = self.lock_manager.acquire_local(
        inode, LockMode::Shared, self.lease_duration_ms,
    );
    // 继续原有 readdir 逻辑
}
```

**2. entry_exists 在持有目录 lease 时只查缓存**：
```rust
fn entry_exists(&self, parent: u64, name: &str) -> bool {
    // 持有目录 Shared lease 时，信任缓存，不发 RPC
    if self.has_valid_dir_lease(parent) {
        return self.lookup_in_cache(parent, name).is_some();
    }
    // 无 lease：原有逻辑（查缓存 + MISS 时发 RPC）
    if self.lookup_in_cache(parent, name).is_some() {
        return true;
    }
    // ... 发 lookup RPC
}
```

**3. create / mkdir / unlink / rmdir 成功后 invalidate 目录 dentry 缓存**：
```rust
fn create(...) {
    // ... 执行 create RPC 成功 ...
    // 修改了父目录内容，invalidate 该目录的 dentry 缓存
    self.invalidate_dir_entries(parent);
    // ... 返回新文件 Entry ...
}
```

**4. releasedir 释放目录 lease**：
```rust
fn releasedir(...) {
    self.lock_manager.release_inode_lease(inode, ...);
}
```

### 7.6 与文件 inode lease 的关系

目录 lease 和文件 inode lease 是**正交**的，互不影响：
- 文件 inode lease：写入路径独占（Exclusive），防止多客户端并发写同一文件
- 目录 lease：读取路径共享（Shared），加速目录内容查询

两者复用同一个 `FuseLockManager` 和 `InodeLeaseManager`，只是 LockMode 和 acquire 时机不同。

### 7.7 性能预期

| 场景 | 优化前 | 优化后（持有目录 lease） |
|------|--------|------------------------|
| `cp N files` 到同一目录 | N × (lookup + create) = 2N RPC | N × create = N RPC |
| `ls` 后再 `ls` | 每次都可能发 readdir RPC | 缓存命中，0 RPC |
| `stat` 已缓存文件 | 100ms 后 TTL 过期发 lookup | lease 有效期内 0 RPC |

单客户端 `cp 1000 files` 预期 IOPS 从 ~12 提升到 ~22（减少一半 RPC）。
