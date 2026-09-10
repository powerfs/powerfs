# PowerFS 元数据性能优化方案

> 分支: `lock-optimization` (private)
> 状态: 进行中
> 更新: 2026-08-20

## 目录

- [背景与问题](#背景与问题)
- [当前架构分析](#当前架构分析)
- [总体方案](#总体方案)
- [P0: 集中式调试控制](#p0-集中式调试控制)
- [P1: 容器映射 release 目录](#p1-容器映射-release-目录)
- [P2: Lease 管理与失效](#p2-lease-管理与失效)
- [P3: Filer 元数据缓存层（核心）](#p3-filer-元数据缓存层核心)
- [P4: Create 路径优化](#p4-create-路径优化)
- [P5: Open 缓存信任](#p5-open-缓存信任)
- [P6: Batch Unlink](#p6-batch-unlink)
- [测试验证计划](#测试验证计划)

---

## 背景与问题

小文件 IOPS 仅 18，根因:

1. **Raft 同步提交瓶颈**: 每个 CreateInode + AddDirEntry 走两次顺序 Raft propose，每次等待 quorum commit + apply (~40ms)，总计 ~80ms
2. **读路径依赖 Raft apply**: create 后立即 lookup/getattr 需等 Raft apply 完成，否则 "file not found"
3. **异步提交丢数据**: `propose_ff` (fire-and-forget) 提交后不等 commit，leader 切换或 lease 过期时日志被静默丢弃，导致 inode 丢失
4. **lookup RPC 过多**: 缺少目录级 lease，每次 create/open 都触发 lookup
5. **无批量机制**: unlink 逐条提交 Raft，高并发删除性能差

### 核心矛盾

> **性能 vs 可靠性**: Raft 同步提交保证强一致但慢 (~80ms/op)；异步提交快但丢数据。

### 解决思路

> **Filer 端引入元数据缓存层**: 创建的文件先写入本地缓存（立即可读），Raft propose 异步进行。只有 dirty（修改后）和 deleted（删除）的条目才同步走 Raft。这样既保证创建的高 IOPS，又通过缓存层桥接异步 Raft 的可见性间隙。

---

## 当前架构分析

### 写路径 (当前)

```
FUSE create()
  → MetaShardClient.create_inode_and_direntry()  [RPC]
    → Filer net_handler.handle_create()
      → MetaShardManager.propose_create_inode_and_direntry()
        → RaftGroupManagerV2.propose(ShardCommand::CreateInode)   # 等待 quorum commit (~40ms)
        → RaftGroupManagerV2.propose(ShardCommand::AddDirEntry)   # 等待 quorum commit (~40ms)
          → ShardStore.apply_command()                            # apply 到 RocksDB
        → 轮询 ShardStore.get_inode() 等待 apply 可见
      → 返回 attr
```

**问题**: 两次顺序 Raft commit = ~80ms，IOPS ≈ 12

### 读路径 (当前)

```
FUSE lookup/getattr()
  → MetaShardClient.lookup()  [RPC]
    → Filer net_handler.handle_lookup()
      → ShardStore.get_inode() / get_dir_entry()                 # 直接读 RocksDB
```

**问题**: create 后若 Raft 未 apply，lookup 返回 "not found"

### 异步模式 (当前, async_meta_persist=true)

```
propose_ff(ShardCommand::CreateInode)   # fire-and-forget, 不等 commit
propose_ff(ShardCommand::AddDirEntry)   # fire-and-forget
→ 立即返回
```

**问题**: leader 切换/lease 过期时日志丢失，inode 不存在

---

## 总体方案

### 架构目标

```
                     ┌─────────────────────────────────┐
                     │         Filer 节点               │
                     │                                  │
  FUSE create() ────►│  NetHandler                      │
                     │    │                             │
                     │    ▼                             │
                     │  MetaShardManager                │
                     │    │                             │
                     │    ├──► MetaCache (NEW)          │  ← 创建立即可见
                     │    │      inode cache + dir cache│     dirty/deleted 标记
                     │    │                             │
                     │    ├──► RaftGroupManagerV2       │  ← 异步 propose (创建)
                     │    │      propose / propose_ff   │     同步 propose (dirty/delete)
                     │    │             │               │
                     │    │             ▼               │
                     │    │      ShardStore.apply()     │  ← Raft commit 后落地 RocksDB
                     │    │             │               │     同时更新 MetaCache
                     │    │             ▼               │
                     │    │      RocksDB                │
                     │    │                             │
  FUSE lookup() ────►│  NetHandler                      │
                     │    │                             │
                     │    ▼                             │
                     │  MetaCache.get() ──hit──► 返回   │  ← 优先读缓存
                     │    │ miss                        │
                     │    ▼                             │
                     │  ShardStore.get() ──► 返回       │  ← 回退到 RocksDB
                     └─────────────────────────────────┘
```

### 分阶段实施

| 阶段 | 内容 | 状态 | 优先级 |
|------|------|------|--------|
| P0 | 集中式调试控制 (Master 配置中心 + 节点轮询) | ✅ 完成 | 高 |
| P1 | 容器映射 release 目录 | ✅ 完成 | 高 |
| P2 | Lease 管理与失效 (clear_all + leader 切换 + invalidate) | 🔧 进行中 | 高 |
| **P3** | **Filer 元数据缓存层 (核心)** | 📋 待实施 | **最高** |
| P4 | Create 路径优化 (合并 setattr 到 CreateInode) | 📋 待实施 | 高 |
| P5 | Open 缓存信任 (cache hit 信任本地缓存) | 📋 待实施 | 高 |
| P6 | Batch Unlink (批量删除) | 📋 待实施 | 中 |

---

## P0: 集中式调试控制

**状态**: ✅ 完成并验证

### 架构

```
Master (配置中心)
  ├── DebugConfigStore (DashMap<node_id, DebugConfig>)
  ├── HTTP /admin/debug (GET/PUT/DELETE)
  └── gRPC GetDebugConfig (节点轮询, 2s间隔)

节点 (fuse/filer/volume)
  ├── DebugConfigPoller (后台 tokio task, 每 2s 拉取)
  └── dynamic_log (运行时调整 level + target filter + flag)
```

### 验证结果

- ✅ GET/PUT/DELETE `/admin/debug` 正常
- ✅ 日志级别控制: all="error" 抑制日志, all="debug" 恢复, 2-5s 生效
- ✅ 节点级覆盖: all="error" + filer-1="debug" → filer-1 有 DEBUG 日志, fuse-1 无
- ✅ Target filter: "powerfs_common::debug_config_poller" → 只输出该 target
- ✅ Flag 控制: verbose_io 等
- ✅ 三类节点 (fuse/filer/volume) 均每 2s 轮询

### 关键文件

- `powerfs-common/src/dynamic_log.rs` — 日志原语
- `powerfs-common/src/debug_config_poller.rs` — 节点轮询
- `powerfs-master/src/debug_config.rs` — Master 配置存储
- `powerfs-master/src/metrics.rs` — HTTP 端点
- `powerfs-master/src/net_handler.rs` — gRPC GetDebugConfig
- `powerfs-net/src/protocol.rs` — `GetDebugConfig = 0x0089`
- `powerfs-net/src/serialize.rs` — 编解码
- 各 `main.rs` — `dynamic_log::init` + 启动 poller

---

## P1: 容器映射 release 目录

**状态**: ✅ 完成并验证

### 方案

将 host 的 `target/release/` 目录下的二进制直接映射到容器的 `/app` 路径，避免手动 `docker cp`。

```yaml
# docker-compose.yml (所有组件)
volumes:
  - /home/portion/powerfs/target/release/powerfs-master:/app/powerfs-master:ro
  - /home/portion/powerfs/target/release/powerfs-filer:/app/powerfs-filer:ro
  - /home/portion/powerfs/target/release/powerfs-fuse:/app/powerfs-fuse:ro
  - /home/portion/powerfs/target/release/powerfs-volume:/app/powerfs-volume:ro
```

### 覆盖范围

master-1/2/3, filer-1/2/3, fuse-1/2, volume-1~6, s3, monitor — 全部映射

### 效果

代码 `cargo build --release` 后，`docker compose restart <service>` 即可生效，无需重新构建镜像。

---

## P2: Lease 管理与失效

**状态**: 🔧 进行中

### 问题

1. Leader 切换后，旧 leader 的 lease 仍然有效，客户端信任过期缓存
2. Invalidate 通知到达时，未清除对应目录的 lease，导致 has_valid_dir_lease() 返回 true
3. 缺少 clear_all 机制

### 方案

#### P2.1 ClientLeaseState::clear_all()

```rust
// powerfs-lock-fuse/src/state.rs
pub fn clear_all(&self) {
    let inode_count = self.inode_leases.lock().unwrap().len();
    let range_count = self.range_leases.lock().unwrap().len();
    self.inode_leases.lock().unwrap().clear();
    self.range_leases.lock().unwrap().clear();
    if inode_count > 0 || range_count > 0 {
        log::warn!(
            "ClientLeaseState::clear_all: dropped {} inode leases + {} range leases (leader change)",
            inode_count, range_count
        );
    }
}
```

#### P2.2 check_cache_epoch() 清 lease

```rust
// powerfs-fuse/src/fuse.rs
fn check_cache_epoch(&self) {
    let current = self.client.facade().meta_shard_client().cache_epoch();
    let last = self.last_cache_epoch.load(...);
    if current != last {
        self.cache.invalidate_all();
        self.lock_manager.state().clear_all();  // ← NEW: 清所有 lease
        self.last_cache_epoch.store(current, ...);
    }
}
```

#### P2.3 InvalidateHandler 清 lease

```rust
// powerfs-fuse/src/invalidate_handler.rs
pub struct InvalidateHandler {
    // ...
    lease_state: RwLock<Option<Arc<ClientLeaseState>>>,  // ← NEW
}

// 处理 invalidate 通知时:
self.cache.invalidate_inode(inode);
self.chunk_cache.remove_inode_chunks(inode);
if let Some(lease_state) = self.lease_state.read().unwrap().as_ref() {
    lease_state.invalidate_inode(inode);  // ← 清除该 inode 的 lease
}
```

### 验证计划

- [ ] 模拟 leader 切换 (kill filer-1 leader), 确认 lease 清空日志
- [ ] 跨客户端创建文件, 确认 invalidate 通知到达后 lease 清除
- [ ] lease 清除后 lookup 走 RPC 而非缓存

---

## P3: Filer 元数据缓存层（核心）

**状态**: 📋 待实施
**优先级**: 最高

### 问题

> Filer 不能直接依赖 Raft 日志。创建的文件等要有缓存，dirty 和删除的才往 Raft 日志去。

当前每个创建操作直接走 Raft propose + 等待 commit/apply，导致:
- IOPS 低 (~12, 每次 ~80ms)
- 异步模式 (propose_ff) 丢数据
- 读路径依赖 Raft apply，创建后立即读可能 "not found"

### 设计目标

1. **创建立即可见**: CreateInode 写入本地缓存后立即返回，不等 Raft commit
2. **读优先走缓存**: lookup/getattr 优先从缓存读，miss 时回退 RocksDB
3. **dirty 同步 Raft**: 修改后的 inode (setattr/write size update) 同步走 Raft
4. **删除同步 Raft**: unlink/rmdir 同步走 Raft
5. **Raft apply 回写缓存**: Raft commit + apply 后，将结果同步到缓存，保持一致

### 架构设计

```
                     MetaShardManager
                     │
                     ├──► MetaCache (NEW: 内存缓存层)
                     │     │
                     │     ├── inode_cache: DashMap<Inode, CachedInode>
                     │     │     CachedInode {
                     │     │       info: InodeInfo,
                     │     │       state: Clean | Dirty | Deleted | Staging,
                     │     │       version: u64,  // Raft apply 版本
                     │     │     }
                     │     │
                     │     ├── dir_cache: DashMap<(ParentIno, Name), CachedEntry>
                     │     │     CachedEntry {
                     │     │       child_ino: u64,
                     │     │       state: Clean | Dirty | Deleted | Staging,
                     │     │     }
                     │     │
                     │     └── pending_raft: Vec<ShardCommand>  // 待异步提交的创建
                     │
                     ├──► RaftGroupManagerV2 (Raft 层)
                     │     ├── propose (同步, dirty/delete)
                     │     └── propose_ff (异步, 创建)
                     │
                     └──► ShardStore (RocksDB)
                           └── apply_command() → 同时更新 MetaCache
```

### 缓存条目状态机

```
                    create()
                       │
                       ▼
                  ┌─────────┐
                  │ Staging │  ← 内存可见, Raft propose 异步进行
                  └────┬────┘
                       │ Raft commit + apply
                       ▼
                  ┌─────────┐
          ┌───────│  Clean  │  ← 与 Raft/RocksDB 一致
          │       └────┬────┘
          │            │ setattr/write
          │            ▼
          │       ┌─────────┐
          │       │  Dirty  │  ← 同步 Raft propose, 等待 commit
          │       └────┬────┘
          │            │ Raft commit + apply
          │            ▼
          │       ┌─────────┐
          └───────│  Clean  │
                  └────┬────┘
                       │ unlink/rmdir
                       ▼
                  ┌──────────┐
                  │ Deleted  │  ← 同步 Raft propose, 等待 commit
                  └────┬─────┘
                       │ Raft commit + apply
                       ▼
                   [移除缓存]
```

### 写路径 (新)

#### CreateInode (异步, 走 Staging)

```
1. MetaShardManager.create_inode()
   → 分配 inode number
   → MetaCache.stage_inode(ino, info)           # Staging 状态, 立即可读
   → MetaCache.stage_direntry(parent, name, ino) # Staging 状态
   → RaftGroupManager.propose_ff(CreateInode)    # 异步, 不等 commit
   → RaftGroupManager.propose_ff(AddDirEntry)    # 异步
   → 返回 attr (从 MetaCache 读)                 # < 1ms

2. Raft commit + apply (后台)
   → ShardStore.apply_command(CreateInode)
     → 写入 RocksDB
     → MetaCache.confirm_inode(ino, raft_version) # Staging → Clean
   → ShardStore.apply_command(AddDirEntry)
     → 写入 RocksDB
     → MetaCache.confirm_direntry(parent, name)   # Staging → Clean
```

#### SetAttr / UpdateSize (同步, 走 Dirty)

```
1. MetaShardManager.setattr(ino, ...)
   → MetaCache.mark_dirty(ino)                   # Dirty 状态
   → RaftGroupManager.propose(SetAttr)            # 同步, 等待 commit
   → 等待 apply
   → MetaCache.confirm_inode(ino, raft_version)   # Dirty → Clean
   → 返回
```

#### Unlink / Rmdir (同步, 走 Deleted)

```
1. MetaShardManager.unlink(parent, name)
   → MetaCache.mark_deleted(parent, name, ino)   # Deleted 状态
   → RaftGroupManager.propose(RemoveDirEntry)     # 同步, 等待 commit
   → RaftGroupManager.propose(RemoveInode)        # 同步
   → 等待 apply
   → MetaCache.remove_entry(parent, name, ino)    # 移除缓存
   → 返回
```

### 读路径 (新)

```
1. lookup(parent, name)
   → MetaCache.get_direntry(parent, name)
     ├── hit (Clean/Staging): 返回 child_ino + attr  ← 缓存命中
     ├── hit (Deleted): 返回 ENOENT                  ← 已删除
     └── miss: ShardStore.get_dir_entry(parent, name)
               ├── hit: 返回 + 回填 MetaCache
               └── miss: 返回 ENOENT

2. getattr(ino)
   → MetaCache.get_inode(ino)
     ├── hit (Clean/Staging): 返回 attr             ← 缓存命中
     ├── hit (Dirty): 返回 dirty attr (最新)        ← 返回未提交的修改
     ├── hit (Deleted): 返回 ENOENT
     └── miss: ShardStore.get_inode(ino)
               ├── hit: 返回 + 回填 MetaCache
               └── miss: 返回 ENOENT
```

### 一致性保证

| 场景 | 保证 |
|------|------|
| 创建后立即读 | Staging 状态立即可读 (MetaCache hit) |
| 创建后 Filer 崩溃 | Staging 丢失, 但未 commit 的 Raft 日志也会被新 leader 截断; 客户端重试 |
| 修改后读 | Dirty 状态返回最新值 (MetaCache hit) |
| 删除后读 | Deleted 状态返回 ENOENT |
| Leader 切换 | check_cache_epoch → MetaCache.invalidate_all() (同 P2) |
| 跨客户端读 | Raft apply 后 RocksDB 有数据, 其他 Filer 的 MetaCache miss → 读 RocksDB |

### Follower 节点缓存

Follower 节点同样维护 MetaCache:
- Raft apply 时更新缓存 (Staging→Clean 不会发生在 follower, 因为 follower 不处理 create)
- Follower 的缓存全部从 Raft apply 填充, 状态为 Clean
- 读请求打到 follower 时, 缓存命中直接返回; miss 读 RocksDB

### 内存管理

- MetaCache 使用 LRU 淘汰策略
- 最大条目数可配置 (默认 100万 inode + 200万 direntry)
- 超过阈值时, 清理 Clean 状态的最久未访问条目
- Dirty/Staging/Deleted 状态不可淘汰

### 关键文件

- `powerfs-filer/src/meta_cache.rs` (NEW) — 缓存层实现
- `powerfs-filer/src/meta_shard_manager.rs` — 修改写/读路径
- `powerfs-filer/src/shard_store.rs` — apply_command 时更新缓存
- `powerfs-filer/src/net_handler.rs` — 读请求走缓存

### 预期性能

| 操作 | 当前 | 优化后 | 提升 |
|------|------|--------|------|
| create (同 shard) | ~80ms (2x Raft commit) | ~1ms (缓存 + async Raft) | 80x |
| create (跨 shard) | ~120ms | ~2ms (2x 缓存 + async Raft) | 60x |
| lookup (cache hit) | ~1ms (RocksDB) | ~0.01ms (内存) | 100x |
| lookup (cache miss) | ~1ms (RocksDB) | ~1ms (RocksDB) | — |
| setattr | ~40ms (Raft commit) | ~40ms (同步 Raft) | — |
| unlink | ~80ms (2x Raft commit) | ~80ms (同步 Raft) | — |

**预期 IOPS**: 12 → 500+ (创建路径)

---

## P4: Create 路径优化

**状态**: 📋 待实施

### 问题

当前 create 路径:
1. CreateInode (Raft propose)
2. AddDirEntry (Raft propose)
3. SetAttr mode/uid/gid (额外 Raft propose) ← 多余

### 方案

将 mode/uid/gid 合并到 CreateInode 命令中，消除额外的 SetAttr propose。

```rust
// ShardCommand::CreateInode 增加字段
Createode {
    ino: u64,
    parent_ino: u64,
    name: String,
    attr: InodeAttr,
    // NEW: 直接携带 mode/uid/gid, 避免额外 SetAttr
    mode: u32,
    uid: u32,
    gid: u32,
}
```

### 验证

- [ ] create 后 stat 验证 mode/uid/gid 正确
- [ ] IOPS 对比 (消除 1 次 Raft propose)

---

## P5: Open 缓存信任

**状态**: 📋 待实施

### 问题

当前 open 路径即使缓存命中也会发 getattr RPC 确认，增加延迟。

### 方案

open 时若缓存命中且未过期 (TTL 内), 信任缓存, 跳过 RPC:

```rust
fn open(ino) {
    if let Some(cached) = cache.get_inode(ino) {
        if cached.is_clean() && !cached.expired() {
            return cached.attr;  // 信任缓存, 不发 RPC
        }
    }
    // cache miss 或过期: 发 getattr RPC
    let attr = client.getattr(ino);
    cache.put(ino, attr);
    return attr;
}
```

### 验证

- [ ] open cache hit 不发 RPC (检查日志无 getattr)
- [ ] open cache miss 发 RPC
- [ ] 并发 open 正确性

---

## P6: Batch Unlink

**状态**: 📋 待实施

### 问题

unlink 逐条提交 Raft, 高并发删除 (如 `rm -rf`) 性能差。

### 方案

客户端批量收集 unlink 请求, 定期或达到阈值时发送 BatchUnlink RPC:

```
FUSE unlink()
  → BatchCollector.add(parent, name)    # 收集, 不立即发送
  → 定时器 10ms 或 达到 64 条
  → MetaShardClient.batch_unlink(entries)  [单次 RPC]
    → Filer net_handler.handle_batch_unlink()
      → MetaShardManager.propose_many([RemoveDirEntry, RemoveInode, ...])
      → 单次 Raft commit 批量提交
```

### 协议

- 新增 `BatchUnlink = 0x003e` 命令
- 请求: Count(u32) + [(ParentIno, Name)] * Count
- 响应: Count(u32) + [Status(u8)] * Count

### 参数

- flush_interval: 10ms
- batch_size: 64
- 参数可配置

### 验证

- [ ] `rm -rf` 1000 文件, IOPS 对比
- [ ] 批量删除正确性 (无遗漏)

---

## 测试验证计划

### 每阶段验证要求

> 每个阶段完成后必须验证通过才进入下一阶段, 防止问题积累。

| 阶段 | 验证内容 | 方法 |
|------|----------|------|
| P0 | 调试控制生效 | curl + 日志检查 |
| P1 | 二进制实时更新 | cargo build + restart + 验证新代码 |
| P2 | lease 清空 | 模拟 leader 切换 + invalidate |
| P3 | 缓存层正确性 | 创建后立即读 + 崩溃恢复 + 跨客户端 |
| P4 | create 无多余 setattr | 日志检查 + IOPS 对比 |
| P5 | open cache hit | 日志无 getattr + 正确性 |
| P6 | batch unlink | rm -rf 性能 + 正确性 |

### 端到端测试

- T1: VFS smoke (touch/cp/mv/rm/stat/chmod)
- T2: 批量文件 (1500 文件 cp -r, MD5 校验)
- T3: 并发 (12×200 请求)
- T4: 跨客户端 (fuse-1 写, fuse-2 读)
- T5: 性能 (IOPS, 吞吐)
- T6: 稳定性 (5min 无错误)
- T7: 持久性 (filer 重启后数据完整)
- T8: Leader 切换 (kill filer, 验证 failover)

---

## 附录

### 相关文档

- [shard-routing-no-forward-principle.md](shard-routing-no-forward-principle.md) — 分片路由与跨分片操作原则
- [dir-lease-design.md](dir-lease-design.md) — 目录级 lease 设计
- [lock-optimization-plan.md](lock-optimization-plan.md) — 锁优化方案
- [changelog-lease-dual-mode.md](changelog-lease-dual-mode.md) — Lease 双模式变更日志

### 约束

- 所有设计文档 (*.md) 不入 git, 仅本地参考 (docs/ 在 .gitignore)
- 所有代码改动仅推送到 private 库
- 禁止服务间转发, 非 leader 返回 STATUS_ERR_REDIRECT
- Docker 容器映射 host release 目录到 /app
