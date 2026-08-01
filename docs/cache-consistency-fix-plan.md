# PowerFS 元数据缓存一致性修复方案 —— 回归原设计

## 1. 问题本质：实现偏离原设计

### 1.1 原设计（docs/posix-metadata-service-design.md 0.1-0.3）
PowerFS 元数据本应是 **CRDT 弱一致 + Delta Sync 异步**：
- fuse 端持 `DirORSet` 本地副本（每目录一个 OR-Set）
- 写：本地 apply DirORSet + 更新 VectorClock，**立即返回**；异步 `push_delta` 给 filer
- 读：读本地 DirORSet（零开销）
- filer：CRDT `merge_from` + `pull_delta` 返回其他客户端变更
- `change_cache_flusher` 缓冲本地写 delta，批量/定时/close 触发 push
- `do_pull_and_apply_deltas` 接收 filer delta，merge 到本地 + 失效 MetadataCache

原设计分 4 Phase：① filer delta 计算/合并 ② 客户端 delta 接收/应用 ③ 写路径解耦（本地 apply）④ Cache 失效联动。

### 1.2 实现偏离（现状）
| 原设计组件 | 现状 | 位置 |
|-----------|------|------|
| fuse 端 DirORSet 本地副本 | **缺失**（用 MetadataCache 同步访问 filer） | powerfs-fuse/src/cache.rs |
| ShardedDirCache（DirORSet 容器） | **缺失** | — |
| change_cache_flusher | **缺失** | — |
| do_pull_and_apply_deltas | **缺失** | — |
| fuse 写路径本地 apply | **偏离**（同步 filer_create_entry 等调用） | powerfs-fuse/src/fuse.rs |
| `powerfs-orset::DirORSet` | **已实现**（apply_local_op/merge/compute_delta/lookup） | powerfs-orset/src/lib.rs:608 |
| `DirORSetCache` trait | **已实现**（客户端缓存接口） | powerfs-orset/src/lib.rs:1333 |
| filer push_delta/pull_delta | **已实现**（缺 leader 校验 + delta 广播） | meta_shard_manager.rs:1583-1757 |
| meta_shard_client push/pull 接口 | **有接口未接入** | meta_shard_client.rs |

### 1.3 偏离导致的 entry 空问题
fuse 用 MetadataCache（镜像需验证）而非 DirORSet（合法副本无需验证），叠加 MetadataCache 自身缺陷（readdir 永久信任 list_children 无 TTL、list_children 不过滤失效 entry、remove 依赖 inode_cache 命中），导致 readdir/lookup 双向不一致。

## 2. 修复目标：回归原设计
1. fuse 端落地 `DirORSet` 本地副本 + `ShardedDirCache` 容器
2. 落地 `change_cache_flusher`（本地写 delta 缓冲 + 异步 push）+ `do_pull_and_apply_deltas`（pull + merge）
3. fuse 写路径解耦：本地 apply + 异步 sync（移除同步 filer 调用）
4. DirORSet（CRDT 目录条目）与 MetadataCache（inode attr 缓存）**共存**：delta 应用后失效 MetadataCache（原设计 Phase 4）
5. filer 端补齐：push_delta/pull_delta 加 leader 校验 + on_write 广播 delta + create/delete 原子化
6. 封装为 `powerfs-coherence` crate，对外 trait，内部 CRDT 实现（epoch/lease 作为未来可选演进）

## 3. 模块化：`powerfs-coherence` crate

```
powerfs-coherence/
├── Cargo.toml
└── src/
    ├── lib.rs          # trait + 公共类型
    ├── crdt_client.rs  # CrdtReplicaCoherence（fuse 端，回归原设计）
    ├── crdt_server.rs  # CrdtCoherenceAuthority（filer 端）
    └── mock.rs         # 测试 mock
```

### 3.1 对外 trait
```rust
// 客户端侧（fuse 用）
pub trait CacheCoherence: Send + Sync {
    fn on_local_write(&self, parent_ino: u64, op: &WriteOp);
    fn validate_cache(&self, kind: CacheKind) -> ValidationResult; // CRDT 恒 Valid
    fn on_remote_delta(&self, parent_ino: u64, delta: DirDelta);
    fn record_version(&self, kind: CacheKind, version: u64);
}

// Filer 侧
pub trait CoherenceAuthority: Send + Sync {
    fn on_write_committed(&self, parent_ino: u64, op: &WriteOp) -> u64;
    fn pull_delta(&self, dir_ino: u64, client_vclock: &VectorClock) -> Vec<DirDelta>;
}
```

### 3.2 CRDT 实现（回归原设计，唯一主线实现）
```rust
pub struct CrdtReplicaCoherence {
    dir_cache: ShardedDirCache,             // dir_ino -> DirORSet（本地副本）
    change_cache: ChangeCache,              // 待 sync 的 delta 缓冲
    filer: Arc<dyn DeltaSyncChannel>,      // push/pull 通道（meta_shard_client）
    metadata_cache: Arc<MetadataCache>,     // inode attr 缓存（共存，delta 后失效）
    config: CrdtConfig,
}
```

epoch/lease 不在本方案实施，作为未来可选实现预留 trait 扩展点。

## 4. 实施步骤（对照原设计 4 Phase）

### Phase 1: filer 端 delta 补齐 + inode 授权接口
**目标**：filer 成为完整 CRDT 协作节点 + 数据源原子 + leader 校验 + inode 批量授权

**1.1 B6 原子化** — `powerfs-filer/src/shard_store.rs`
- 新增 `create_inode_atomic(info, parent_inode, name)`：WriteBatch 同时 put CF_INODES + CF_DIR_ENTRIES，再更新内存（参考已有 `batch_update_inodes` L1122 写法）
- 新增 `remove_inode_atomic(inode, parent_inode, name)`：WriteBatch 同时 delete CF_INODES + CF_DIR_ENTRIES
- 新增 `rename_dir_entry_atomic(old_parent, old_name, new_parent, new_name, inode)`：WriteBatch delete old + put new
- `meta_shard_manager.rs:1866-1894` push_delta 的 Add/Remove/Rename 改调原子版本

**1.2 B4 leader 校验** — `powerfs-filer/src/grpc_service.rs`
- push_delta/pull_delta handler 入口调 `is_shard_leader(shard_id)`，follower 返回 `not-leader + leader_addr`
- 不转发请求（遵循架构原则：Raft 服务不转发）

**1.3 delta 广播** — `powerfs-filer/src/net_handler.rs`
- create/unlink/mkdir/rmdir/rename 成功后调 `coherence.on_write_committed(parent_ino, op)`
- `CrdtCoherenceAuthority.on_write_committed`：compute_delta + `notifier.broadcast_delta(parent_ino, delta)`（复用 InodeNotifier 通道，推 DirDelta）
- fuse 端 `on_remote_delta` merge 到本地 DirORSet（Phase 2 接入）

**1.4 inode 批量授权接口** — `powerfs-filer/src/shard_store.rs` + `grpc_service.rs`
- `shard_store.rs`：新增 `next_inode: AtomicU64`（持久化到 RocksDB CF_META，Raft 复制）；`alloc_inode_batch(size) -> (start, end)` 原子自增
- `grpc_service.rs`：新增 `alloc_inode_batch` gRPC handler（leader only）
- `meta_shard_manager.rs`：暴露 `alloc_inode_batch` 给 gRPC 层

**1.5 size/chunks 强一致接口** — `powerfs-filer/src/shard_store.rs`
- 新增 `update_inode_size_chunks_atomic(inode, size, chunks)`：WriteBatch 更新 CF_INODES 的 size/chunks 字段（close sync 账本用，Phase 3 接入）
- 通过 Raft 提交（强一致）

**验证**：
- filer 单测：create_inode_atomic 后 list_directory/lookup 一致；崩溃重启不丢
- push_delta/pull_delta 发 follower 返回 not-leader
- alloc_inode_batch 并发调用返回不重叠区间
- update_inode_size_chunks_atomic 后所有 filer 节点可见

### Phase 2: fuse 端 CRDT 副本骨架
**目标**：fuse 端持 DirORSet 本地副本，能 pull + merge

**2.1 建 `powerfs-coherence` crate**
- `Cargo.toml`：依赖 powerfs-common, powerfs-orset, powerfs-net
- `src/lib.rs`：定义 `CacheCoherence`/`CoherenceAuthority` trait + WriteOp/ValidationResult/DirDelta 类型
- `src/crdt_client.rs`：`CrdtReplicaCoherence`（fuse 端）
- `src/crdt_server.rs`：`CrdtCoherenceAuthority`（filer 端，包装 meta_shard_manager）

**2.2 ShardedDirCache** — `powerfs-coherence/src/crdt_client.rs`
- `dir_ino -> Arc<RwLock<DirORSet>>` 容器（HashMap + RwLock）
- `ensure_replica(dir_ino)`：本地无副本时触发 pull
- 复用 `powerfs-orset::DirORSet` 的 apply_local_op/merge/lookup/get_entries

**2.3 do_pull_and_apply_deltas** — `powerfs-coherence/src/crdt_client.rs`
- 接入 `meta_shard_client.pull_delta(dir_ino, vclock)`
- merge 到本地 DirORSet
- merge 后对变更 inode 调 `metadata_cache.invalidate_inode`（联动失效）

**2.4 后台 puller** — `powerfs-coherence/src/crdt_client.rs`
- tokio task 定时 pull_delta（间隔可配）
- 首次访问目录/lookup miss 时触发即时 pull

**2.5 DeltaSyncChannel** — `powerfs-coherence/src/crdt_client.rs`
- 封装 meta_shard_client 的 push_delta/pull_delta
- leader 重试：收到 not-leader 后更新 leader_addr 重试

**验证**：
- fuse 启动后 pull 根目录副本，readdir 读本地 DirORSet 与 filer 一致
- filer 端 create 后，fuse pull_delta 能看到新条目

### Phase 3: fuse 写路径解耦 + lease 协调
**目标**：元数据写本地 apply + 异步 sync + inode 预留段 + close sync 账本

**3.1 InodeAllocator** — `powerfs-coherence/src/crdt_client.rs`
- 维护预留段 `[cursor, end)`
- `alloc_inode()`：cursor<end 返回 cursor++；耗尽调 filer `alloc_inode_batch` 获取新段
- 接入 meta_shard_client 的 alloc_inode_batch gRPC

**3.2 ChangeCache + change_cache_flusher** — `powerfs-coherence/src/crdt_client.rs`
- `ChangeCache`：dir_ino -> Vec<DirDelta> 缓冲
- `change_cache_flusher` tokio task：定时/批量 drain + push_delta
- **同一 dir_ino 的 delta 串行 push**（保序：create a→unlink a→create a 必须按序 apply）；不同 dir_ino 可并发
- `force_sync(dir_ino)`：同步 push + 等 filer 确认（close 用）

**3.3 写路径改造** — `powerfs-fuse/src/fuse.rs` create/unlink/mkdir/rmdir/rename
- create：`inode_allocator.alloc_inode()` → `DirORSet.apply_local(Create)` → `change_cache.push(delta)` → 立即返回
- unlink/mkdir/rmdir/rename：本地 apply + 缓冲 delta
- **移除**同步 filer_create_entry/net 调用（原设计 Phase 3 要求）
- **size/chunks 不走 ChangeCache**（见 3.4）

**3.4 close 时 size/chunks 强一致 sync** — `powerfs-fuse/src/fuse.rs` release/flush
- close 流程：flush 数据 → `filer.update_inode_size_chunks(inode, size, chunks)`（同步等 Raft，§5.1）→ 释放 lease
- 接入 Phase 1.5 的 update_inode_size_chunks_atomic
- **sync 失败处理**（数据/账本非原子问题）：sync 重试到成功或超时上限；超时 close 返回 EIO + 标记 inode "需 fsck"；lease 在 sync 成功前不释放（崩溃则 lease 超时回收 + fsck 修复孤儿 chunks）

**3.5 B1/B2/B3 止血** — `powerfs-fuse/src/cache.rs` + `fuse.rs`
- 过渡期 MetadataCache 仍用于 inode attr，修缺陷（readdir TTL/list_children 过滤 None/remove 强制清 path_map）
- Phase 4 切本地副本后 MetadataCache 仅 attr 用途

**验证**：
- create 本地立即返回；change_cache_flusher push 后 filer 可见
- 其他客户端 pull 后可见（窗口=sync_interval）
- close 后 filer size/chunks 全局一致；其他客户端 open 拿到最新账本
- inode 预留段耗尽自动续批，无重叠

### Phase 3.5: filer 端延迟删除 GC（§5.2）
**目标**：unlink/rmdir 物理删除由 GC 后台做，不阻塞 rm -rf

**3.5.1 delete_time + open_count** — `powerfs-filer/src/shard_store.rs` + `net_handler.rs`
- merge Remove delta 时：保留 tombstone + 记录 `delete_time`（CF_INODES 加字段或 CF_TOMBSTONES）
- open handler：`open_count` per-inode 计数（open++/close--），存内存 + 持久化
- 复用 DirORSet tombstone 机制（不额外删 inode/dir_entry）

**3.5.2 GC 任务** — `powerfs-filer/src/gc.rs`（新增）
- tokio task 定时扫描 tombstone（间隔可配）
- GC 条件全满足才物理删：`delete_time > grace_period` + `无活跃 lease` + `open_count == 0` + `nlink == 0`
- 物理删：`remove_inode_atomic`（复用 Phase 1.1）+ 清 tombstone
- 通知 volume server 回收数据块（复用已有 needle 回收接口）

**3.5.3 readdir/lookup 跳过 tombstone** — 已有（OR-Set 语义），验证即可

**验证**：
- rm -rf 大目录不阻塞（只产生 tombstone delta）
- grace_period 内 tombstone 可恢复（误删恢复）
- GC 后 inode/数据块物理释放
- GC 时有 open/lease 推迟

### Phase 4: 读路径切本地副本 + open lease 附带账本
**目标**：readdir/lookup 读本地 DirORSet；open lease 附带 size/chunks

**4.1 readdir** — `powerfs-fuse/src/fuse.rs:1855`
- 改读本地 `DirORSet.get_entries(parent_ino)`（零开销）
- 本地无副本时 `ensure_replica` 触发 pull

**4.2 lookup** — `powerfs-fuse/src/fuse.rs`
- 读本地 `DirORSet.lookup(parent_ino, name)`
- miss 时主动 pull_delta 再查（兜底）
- attr 走 MetadataCache（size/chunks 强一致，其他弱一致）

**4.3 getattr** — `powerfs-fuse/src/fuse.rs`
- size/chunks：MetadataCache 命中且 lease 有效用缓存，否则 filer 查询
- 其他 attr：MetadataCache + 短 TTL

**4.4 open lease 附带账本** — `powerfs-fuse/src/fuse.rs` open
- 获取 lease 时，filer 在 lease 响应附带最新 size/chunks
- fuse 用附带账本填 MetadataCache，省一次 getattr

**4.5 delta 应用联动** — `powerfs-coherence/src/crdt_client.rs`
- `do_pull_and_apply_deltas` merge 后，对变更 inode 调 `metadata_cache.invalidate_inode`
- size/chunks 不被 delta 失效（强一致，仅 lease 刷新）

**验证**：
- 单客户端 cp -prf/rm -rf/tar -czf/tar -xzf/md5 3 轮无 entry 空
- 读写全程读本地 DirORSet 副本
- open 拿到 lease 附带账本，读数据正确

### inode 批量授权设计说明（Phase 1.4 / 3.1 背景）

纯本地生成 inode（hash/`client_id<<48|seq`）有隐患：与 filer 现有 inode 分配可能冲突、空间分布不均、filer 作为 inode 权威的语义被绕过。采用 **filer 批量授权预留段**（对标 CephFS MDS inode 分配、SeaweedFS master fid 段分配）。

**filer 端**：
- 维护 `next_inode: AtomicU64`，持久化到 RocksDB + Raft 复制（崩溃不丢）
- `alloc_inode_batch(size) -> (start, end)`：原子 `next_inode += size`，返回区间
- 通过 gRPC/TLV 暴露 `alloc_inode_batch` 接口（leader only）

**fuse 端**：
- 持有 `InodeAllocator`，维护预留段 `[cursor, end)`
- `alloc_inode()`：`cursor < end` 直接返回 `cursor++`（零等待）；耗尽时向 filer 请求新批次（1 RTT，罕见）
- 批次大小可配（默认 1024，平衡 RTT 开销与 inode 浪费）

**写路径流程**：
```
create → alloc_inode() [预留段内,零等待]
       → DirORSet.apply_local(Create{ino, name, attr})
       → ChangeCache 缓冲 delta
       → 立即返回（不等 filer）
后台 change_cache_flusher → push_delta → filer merge（inode 已全局唯一）
```

**对比纯本地生成**：

| 维度 | 纯本地生成 | 批量授权(预留) |
|------|-----------|---------------|
| 全局唯一 | 算法保证（潜在冲突） | **filer 权威保证** |
| 写本地化 | 完全本地 | 预留段内本地，耗尽时 1 RTT |
| filer 兼容 | 需 filer 接受客户端段 | **filer 统一分配，兼容** |
| inode 空间 | 集中在 client_id 段 | 均匀 |
| 复杂度 | 低 | 中（allocator + alloc 接口） |

**选型**：批量授权。filer 仍是 inode 权威（架构一致），写路径几乎完全本地化，无冲突风险。

**配置**：
```toml
[coherence]
inode_batch_size = 1024  # 预留段大小
```

## 5. DirORSet 与 MetadataCache 共存关系（原设计 Phase 4）

元数据必须分级——与数据正确性相关的属性强一致，目录浏览弱一致：

| 缓存 | 职责 | 一致性 | 数据源 | 失效时机 |
|------|------|--------|--------|---------|
| `DirORSet`（本地副本） | 目录条目（name→inode） | **弱一致(CRDT)** | 本地 apply + delta sync | CRDT merge 自动收敛 |
| `MetadataCache` size/chunks | 数据账本 | **强一致** | filer Raft + lease 绑定 | lease 释放/获取时刷新 |
| `MetadataCache` mode/uid/gid/time | 其他 attr | 弱一致 | filer getattr + 短 TTL | delta 应用后 invalidate_inode |

- readdir/lookup 名字解析：DirORSet（CRDT 弱一致）
- getattr：size/chunks 强一致（MetadataCache 命中且 lease 有效用缓存，否则 filer 查询）；其他 attr 弱一致
- setattr：truncate(size) 走 lease + filer 强一致；chmod/chown 走 CRDT delta
- **size/chunks 不走 CRDT delta**——数据账本必须与 lease 绑定强一致

## 5.1 lease 与 delta 协调（数据强一致 vs 元数据弱一致）

### 问题
数据写用 lease lock（强一致），元数据用 CRDT delta（弱一致）。close 时数据已持久化但 size/chunks delta 未 sync，其他客户端 open 读到旧账本 → 数据正确性问题。

### 协调方案
**size/chunks 走 filer Raft 强一致，不走 CRDT**。close 时先 sync 账本再释放 lease。

```
close(file):
  1. flush 数据（lease 保护下，已线性化）
  2. 强制 sync 数据账本到 filer（size/chunks 变更，同步等 Raft 确认）—— 不走 CRDT delta
  3. 释放 lease
```
lease 释放前 size/chunks 全局强一致，B 获取 lease 时 filer 保证账本最新。

```
open(file):
  1. lookup（本地 DirORSet 弱一致，拿 inode）
  2. 获取 lease（filer 强一致）—— filer 在 lease 响应里附带最新 size/chunks（权威值）
  3. 用 lease 附带账本读数据
```

### setattr 分流
- truncate（size）：lease + filer 强一致（与 close 同理）
- chmod/chown/utimes：CRDT delta（弱一致）

## 5.2 端到端背压与限流

### 问题
一个 fuse 客户端大量操作（rm -rf 百万文件）产生海量 delta，change_cache_flusher 批量 push 冲垮 filer（noisy neighbor），所有客户端延迟飙升。需端到端背压。

### 核心原则
**filer 快速拒绝（不入队保护自己）→ fuse 缓冲消化 → 应用层感知背压慢下来**

```
应用 write/create → fuse 本地 apply + ChangeCache
                       ↓ ChangeCache 达上限,阻塞应用(等待,非错误)
change_cache_flusher → push_delta(inflight 信号量) → filer
                       ↓ 背压 RATE_LIMITED
                    filer per-client 限流 + 全局上限
```

### 分层策略（等待 vs 拒绝）
| 层 | 超限处理 | 理由 |
|----|---------|------|
| **filer 端** | 快速返回 `RATE_LIMITED + retry_after`（不入队） | 保护 filer 不被淹没 |
| **fuse ChangeCache** | 继续缓冲；达上限让应用 write/create **阻塞等待** | 背压传到应用层，慢下来 |
| **应用层** | write/create 慢（阻塞），不返回错误 | 透明降速，应用无需改 |

**关键**：filer 端拒绝（保护自己），fuse 端缓冲+应用等待（消化背压）。不是都入队列。

### 优先级队列
不同操作优先级不同，不能平等限流：
| 操作 | 优先级 | 理由 |
|------|--------|------|
| size/chunks sync（close） | **高** | 强一致，阻塞 close 影响应用 |
| pull_delta（读） | 中 | 影响 readdir/lookup 可见性 |
| push_delta（写） | 低 | 弱一致，可延迟 |

filer 端按优先级调度：高优先级不受低优先级限流影响。

### 与现有架构复用
| 现有组件 | 复用方式 |
|---------|---------|
| `powerfs-net client.inflight_sem` | fuse 端 push_delta inflight 限制 |
| `TransportChannel.max_concurrent` | 已有并发上限，复用 |
| `CircuitBreakerPool` | filer 过载熔断，复用 |
| 新增 | per-client 限流（filer）+ ChangeCache 背压（fuse）+ 优先级队列 |

### 配置
```toml
[coherence]
# fuse 端
change_cache_max_global = 100000   # ChangeCache 全局上限
change_cache_high_watermark = 0.8   # 高水位(减慢 apply)
push_inflight_max = 16              # push_delta 并发上限
# filer 端
filer_per_client_inflight = 32      # per-client 限流
filer_global_inflight = 256         # 全局上限
```

### 实施归属
- **Phase 2**：fuse 端 DeltaSyncChannel 加 inflight 信号量 + leader 重试
- **Phase 3**：ChangeCache 容量上限 + 水位背压（应用阻塞）+ 收到 RATE_LIMITED 退避
- **Phase 3.5/4**：filer 端 per-client 限流 + 全局上限 + 优先级队列（size/chunks sync 高优）

## 5.3 延迟删除 + 后端 GC

### 设计
CRDT OR-Set 的 remove 本就是 tombstone 标记（非物理删除）。利用这一原生语义：客户端删除只产生 tombstone delta（轻量），物理删除由 filer GC 后台做。

**核心收益**：解决 `rm -rf` 大目录卡住问题——rm -rf 只产生 tombstone delta（不产生大量物理 IO），GC 慢慢回收，不阻塞 rm -rf。

### 流程
```
fuse unlink/rmdir:
  → DirORSet.apply_local(Remove) 生成 tombstone delta（CRDT 原生）
  → 缓冲 push_delta
  → 立即返回（快）

filer merge Remove:
  → 保留 tombstone + 记录 delete_time（不物理删 inode/dir_entry）

filer GC 任务（后台定时）:
  → 扫描 tombstone，满足全部条件才物理删:
     1. tombstone 年龄 > gc_grace_period（如 1h，给误删恢复 + 并发收敛留窗口）
     2. 无活跃 lease（filer 是 lease 权威，无人正写）
     3. 无 open 文件（filer 维护 open count，无人正读）
  → 物理删 inode + dir_entry（WriteBatch 原子）
  → 通知 volume server 回收数据块
```

### GC 安全条件
- **无活跃 lease**：filer 是 lease 权威，直接查
- **无 open**：filer 维护 per-inode open_count（open++/close--），>0 推迟 GC
- **nlink == 0**：hardlink 场景，unlink 一个 name 后 nlink>0 不删 inode（inode 引用计数）
- **grace_period**：误删恢复窗口 + 并发操作收敛时间

### 与现有架构契合
| 组件 | 复用/新增 |
|------|----------|
| `DirORSet` tombstone 机制 | 复用（merge_from 已处理 tombstones） |
| filer delete_time | 新增字段（CF_INODES 或单独 CF_TOMBSTONES） |
| filer GC 任务 | 新增（tokio task 定时扫描） |
| filer open_count | 新增（per-inode 计数，open/close 维护） |
| readdir/lookup 跳过 tombstone | 已有（OR-Set 语义） |
| volume server 数据块回收 | 复用已有 needle 回收 |

### 挑战与解决
| 挑战 | 解决 |
|------|------|
| tombstone 膨胀影响 readdir | GC 定期清理；readdir 跳过 tombstone（OR-Set 已处理） |
| GC 时有人正打开 | open_count > 0 推迟 GC |
| 跨客户端 A 删 B 仍可见 | tombstone delta sync 后 B 也看不到；GC 前 B 可继续用已打开的句柄 |
| 数据块何时回收 | inode 物理删时通知 volume server 回收 needle |
| rename 跨目录的 tombstone | rename 产生 Remove(old)+Add(new) delta，tombstone 仅在 old 目录 |

### 配置
```toml
[coherence]
gc_grace_period_secs = 3600   # tombstone 保留时间（误删恢复窗口）
gc_interval_secs = 300        # GC 扫描间隔
```

### 实施归属
- **Phase 1**：filer 端 delete_time 字段 + open_count 维护（open/close handler）
- **Phase 3**：fuse 端 unlink/rmdir 只产生 tombstone delta（已是 CRDT 语义，无需额外改）
- **Phase 3.5（新增）**：filer GC 任务 + volume server 数据块回收通知

## 6. 典型场景处理

### 单客户端大目录操作
| 场景 | 流程 | 一致性 |
|------|------|--------|
| `cp -prf src dst` | create 本地 apply(inode 预留段) + 数据写 lease + close sync 账本；readdir 读本地 DirORSet | 目录条目立即可见；size/chunks close 时 sync |
| `rm -rf dir` | unlink 只产生 tombstone delta（轻量，不阻塞）；filer GC 延迟物理删 + 回收数据块（§5.2） | 条目本地立即消失；物理删 grace_period 后由 GC 做，不阻塞 rm -rf |
| `tar -czf` | 大量 create/write，inode 预留段批量化；close sync 账本 | 写本地化，close 强一致账本 |
| `tar -xzf` | create + write，同上 | 同上 |
| md5 校验 | open 获取 lease(附带 size/chunks) + 读数据 | 强一致账本，读正确数据 |

### 多客户端并发
| 场景 | 处理 |
|------|------|
| A 写 B 读同文件 | A close sync 账本+释放 lease；B open 获取 lease(filer 附带最新账本)。B 在 A 释放 lease 前拿不到 lease（强一致） |
| 并发 create 同名 | OR-Set union 两个 entry(不同 inode)；lookup 返回其一（按 vclock LWW 或报 EEXIST）。**待定：见 §6.1** |
| A create B 不可见 | B 本地 DirORSet 无副本，pull_delta 后可见（窗口=pull_interval）。lookup miss 触发主动 pull |
| rename 冲突 | rename delta 携带 old/new；filer merge 时若 new_name 已存在，按 vclock LWW 或报错。**待定** |
| 并发 truncate | lease 串行化（同一时刻一个 lease holder），size 变更顺序由 lease 序决定 |

### 崩溃恢复
| 场景 | 处理 |
|------|------|
| fuse 崩溃，未 push 的目录 delta 丢失 | 目录条目弱一致，丢失可接受（用户重做操作）。inode 预留段丢失（可接受） |
| fuse 崩溃，close 前 size/chunks 未 sync | lease 由 filer 超时回收（已有机制）；数据可能不一致（与现有 FS 同语义，close 未完成本就不可靠） |
| filer 崩溃 | Raft 选主，next_inode 持久化不丢；客户端 push/pull 重连 leader |
| filer leader 切换 | 客户端 push/pull 发 follower 被拒+重定向（B4），重试到新 leader |

### §6.1 待定决策点
- **并发 create 同名**：OR-Set union 两 entry，lookup 返回哪个？方案：a) vclock LWW（后写覆盖）b) 报 EEXIST（POSIX 语义）c) 两 entry 共存，应用层处理
- **rename 目标已存在**：a) 覆盖（LWW）b) 报 EEXIST

## 7. 配置

```toml
[coherence]
mode = "crdt"                 # 回归原设计，唯一实现
crdt_sync_interval_ms = 100   # change_cache_flusher 间隔
crdt_sync_batch = 64          # 批量 flush 阈值
crdt_force_sync_on_close = true  # close 触发 size/chunks 同步 push（强一致账本）
crdt_pull_interval_ms = 1000  # 后台 puller 间隔
inode_batch_size = 1024       # inode 预留段大小
attr_ttl_ms = 1000            # MetadataCache 其他 attr TTL（size/chunks 不走 TTL，lease 绑定）
```

## 8. 验证清单
- [ ] Phase 1: filer 原子性单测（create_inode_atomic 后 list_directory/lookup 一致）；push/pull 拒绝非 leader；alloc_inode_batch 区间不重叠
- [ ] Phase 2: fuse 启动 pull 副本，readdir 读本地 DirORSet 与 filer 一致
- [ ] Phase 3: create 本地立即返回；close 后 filer size/chunks 全局一致；其他客户端 open 拿到最新账本
- [ ] Phase 4: 单客户端 cp -prf/rm -rf/tar -czf/tar -xzf/md5 3 轮无 entry 空
- [ ] 多客户端：A 写 B pull 后可见（弱一致窗口 = sync_interval）；A close 后 B open 读到正确数据（lease + 账本强一致）
- [ ] 全部通过后跑 fio（`/tmp/fio_meta_test.fio`），CRDT 模式

## 9. 风险与回滚
- **数据/账本非原子（严重）**：close 时数据已持久化(volume server)但账本 sync(filer Raft)失败 → 孤儿 chunks。缓解：sync 重试+超时 EIO+标记 fsck；lease 不提前释放；**需 fsck 工具**扫描 volume server orphan chunks 比对 filer 账本修复（Phase 3.4）
- **GC 误删（严重）**：GC 条件缺 nlink → hardlink 误删。已修复：GC 加 `nlink==0` 检查（§5.3）
- **delta 乱序（严重）**：同目录 delta 乱序 apply → 状态错乱。已修复：同 dir_ino 串行 push（§4 Phase 3.2）
- **inode 预留段丢失**：fuse 崩溃丢失未用预留段（可接受，inode 空间 2^64）；filer next_inode 持久化不丢
- **写持久性**：`force_sync_on_close=true` 保证 close 前 size/chunks 已 Raft 确认；目录 delta 崩溃丢失可接受（弱一致）
- **弱一致不可接受**：调短 sync_interval；或未来加 epoch 模式（trait 预留）
- **close sync 失败**：见"数据/账本非原子"；sync 失败重试，持续失败 close 返回 EIO + fsck 标记
- **递交风暴**：见 §5.2 背压机制；filer 限流 + fuse 缓冲 + 应用等待
- **DirORSet 副本内存膨胀**：大目录副本占内存；ShardedDirCache 加 LRU 淘汰 + 容量上限（Phase 2）
- **GC 扫描性能**：百万 tombstone 扫描耗资源；分批+限流+按 shard 并行（Phase 3.5）
- **并发同名 create**：OR-Set union 两 entry，按 vclock LWW（§6.1）
- **过渡期 B1/B2/B3**：MetadataCache 缺陷先止血，Phase 4 切本地副本后 MetadataCache 仅 attr 用途

## 9.1 语义说明
- **size/chunks 强一致仅文件**：目录无 chunks，目录 attr（nlink/mtime）走 CRDT 弱一致
- **symlink target**：走 CRDT 弱一致（不影响数据正确性）
- **truncate 截断 chunks 回收**：延迟回收（标记待删 + GC），与 unlink 一致
- **size vs chunks 语义**：size=逻辑大小，chunks=物理块列表，`update_inode_size_chunks_atomic` 原子更新
- **crc32 校验**：读 chunk 时校验 `FileChunk.crc32`，不匹配报 EIO（复用已有机制）
- **配置一致性**：`gc_grace_period` 多 filer 必须一致（否则 GC 时机不同）；`sync_interval` fuse 各自配置
- **监控指标**：delta sync 延迟 / GC 积压 / lease 队列 / inode 耗尽率 / ChangeCache 水位（实施时补）

## 10. 待规划运维工具
- **fsck**：扫描 volume server orphan chunks，比对 filer 账本修复（解决数据/账本非原子）
- **监控仪表盘**：上述指标的 Prometheus exporter + Grafana

## 11. 工作量评估
| 模块 | 复用 | 新增 | 难度 |
|------|------|------|------|
| powerfs-orset DirORSet | 全复用 | — | 低 |
| filer push/pull + ServerDirORSet | 大部分复用 | leader 校验 + 广播 delta + WriteBatch | 中 |
| filer inode 批量授权 | — | next_inode + alloc_inode_batch + gRPC | 中 |
| filer size/chunks 强一致接口 | — | update_inode_size_chunks_atomic + Raft | 中 |
| filer 延迟删除 GC | DirORSet tombstone | delete_time + open_count + GC 任务 + nlink | 中高 |
| filer 端到端背压 | inflight_sem/CircuitBreaker | per-client 限流 + 全局上限 + 优先级队列 | 中 |
| fuse ShardedDirCache | 复用 DirORSetCache trait | 容器 + LRU 淘汰 | 中 |
| fuse change_cache_flusher | — | 全新 + 同 dir 串行 + 容量背压 | 中高 |
| fuse do_pull_and_apply_deltas | meta_shard_client 接口 | merge + 联动失效 | 中 |
| fuse InodeAllocator | — | 预留段 + 续批 | 中 |
| fuse 写路径解耦 | — | 改 fuse.rs create/unlink 等 + close sync 账本 | 中高 |
| powerfs-coherence crate | — | trait + 封装 | 低 |
| fsck 运维工具 | — | orphan chunks 扫描 + 账本比对修复 | 中（可后置） |

主线是落地原设计 Phase 2/3/4（fuse 端 CRDT 副本 + 写解耦 + 读本地），filer 端补齐 leader 校验/广播/原子性/inode 授权/GC/背压。fsck 可后置（Phase 3.4 标记 + 后续工具修复）。
