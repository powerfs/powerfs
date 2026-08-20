# PowerFS MetaCache 设计方案

> 参考: Ceph MDCache (src/mds/MDCache.{h,cc})
> 定位: 独立 crate `powerfs-meta-cache`
> 状态: 规划中
> 更新: 2026-08-20

## 目录

- [设计目标与定位](#设计目标与定位)
- [Ceph MDCache 核心启示](#ceph-mdcache-核心启示)
- [架构总览](#架构总览)
- [核心数据结构](#核心数据结构)
- [读写流程](#读写流程)
- [缓存淘汰策略](#缓存淘汰策略)
- [与 Raft/WAL 的交互](#与-raftwal-的交互)
- [客户端 Lease/Caps 联动](#客户端-leasecaps-联动)
- [故障恢复](#故障恢复)
- [大目录分片 DirFrag](#大目录分片-dirfrag)
- [crate 结构](#crate-结构)
- [与现有系统集成](#与现有系统集成)
- [分阶段实施计划](#分阶段实施计划)
- [与 Ceph MDCache 对比](#与-ceph-mdcache-对比)

---

## 设计目标与定位

### 定位

`powerfs-meta-cache` 是 Filer 端的**内存元数据缓存子系统**，参考 Ceph MDCache 设计，但适配 PowerFS 的 Raft + RocksDB 架构。

```
┌─────────────────────────────────────────────────┐
│ Filer 节点                                       │
│                                                  │
│  NetHandler ──► MetaCache ──► ShardStore         │
│                  (内存)        (RocksDB)          │
│                     │                             │
│                     │ stage                       │
│                     ▼                             │
│               RaftGroupManager                    │
│                  (WAL + 复制)                      │
│                     │                             │
│                     ▼ apply                       │
│               ShardStore (持久)                    │
│                     │                             │
│                     ▼ confirm                     │
│               MetaCache (清除 staging)              │
└─────────────────────────────────────────────────┘
```

**权威元数据在 RocksDB (ShardStore)**，MetaCache 是可被裁剪、可丢失的内存高速缓存 + 新建条目的 staging 层。

### 核心目标

1. **创建立即可见**: `create_inode()` 先写入 MetaCache staging，立即对读可见，Raft 异步提交
2. **读优先走缓存**: `lookup`/`getattr`/`readdir` 优先从 MetaCache 读，miss 时回退 ShardStore
3. **dirty 同步 Raft**: 修改 (`setattr`/`update_size_chunks`) 同步走 Raft，保证强一致
4. **删除同步 Raft**: `unlink`/`rmdir` 同步走 Raft，并在缓存中标记 Deleted
5. **目录分片粒度淘汰**: 参考 Ceph dirfrag，按目录分片整体淘汰，适配文件系统树形语义
6. **内存可控**: 按内存字节限制触发 trim，支持 caps recall 联动客户端释放缓存

---

## Ceph MDCache 核心启示

| 启示 | PowerFS 采纳方案 |
|------|------------------|
| 目录是树形结构，dentry 间强关联 | 引入 DirFrag 目录分片淘汰单元 |
| 缓存裁剪联动客户端 caps recall | Filer 内存压力时 recall 客户端 lease |
| WAL 优先写入，后端延迟刷写 | Raft log 作为 WAL，RocksDB 延迟 apply |
| 大目录必须分片 | DirFrag 支持百万级子项的流式 readdir |
| 区分 Auth/非 Auth 元数据 | Leader 节点缓存 Auth 元数据，Follower 缓存副本 |

---

## 架构总览

```
                         powerfs-meta-cache crate
                    ┌────────────────────────────────┐
                    │                                │
                    │  MetaCache                     │
                    │    │                           │
                    │    ├── inode_table             │
                    │    │    DashMap<Inode, CachedInode>
                    │    │                           │
                    │    ├── dirfrag_table           │
                    │    │    DashMap<DirFragId, DirFrag>
                    │    │      DirFrag {            │
                    │    │        dentries: BTreeMap<Name, Dentry>,
                    │    │        state: Clean|Dirty|Staging|Trimming,
                    │    │        lru_score: f64,    │
                    │    │      }                    │
                    │    │                           │
                    │    ├── staging_queue            │
                    │    │    VecDeque<(Inode, Instant)>
                    │    │    待 Raft apply 的创建    │
                    │    │                           │
                    │    ├── deleted_markers          │
                    │    │    DashMap<Inode, Instant> │
                    │    │    待 Raft apply 的删除    │
                    │    │                           │
                    │    └── trim_controller           │
                    │         memory_limit: usize,   │
                    │         current_usage: AtomicUsize,
                    │         reservation: usize,    │
                    │         mid: f64,              │
                    │                                │
                    │  DirFragId = (ParentInode, FragId)
                    │  FragId = u32 (0 = 完整目录, >0 = 分片)
                    │                                │
                    └────────────┬───────────────────┘
                                 │
                    ┌────────────┼───────────────────┐
                    │            │                   │
                    ▼            ▼                   ▼
              RaftGroupMgr   ShardStore        LeaseManager
              (propose_ff    (RocksDB          (recall
               propose)       get/put)          client lease)
```

---

## 核心数据结构

### CachedInode — 内存 inode 对象

参考 Ceph `CInode`，但适配 Rust：

```rust
/// 内存中的 inode 缓存条目。
///
/// 参考 Ceph CInode，但简化了锁管理（PowerFS 用 Raft leader lease 代替
/// 分布式锁）。
pub struct CachedInode {
    /// inode 元数据（与 RocksDB 中的 InodeInfo 同构）
    pub info: InodeInfo,

    /// 缓存条目状态
    pub state: CacheState,

    /// 引用计数：客户端持有的 lease/caps 数量。
    /// trim 时检查此值，>0 不可淘汰。
    pub refcount: AtomicU32,

    /// 最后访问时间（用于 LRU 排序）
    pub last_access: AtomicU64,

    /// 所属 DirFrag 的 ID（用于目录分片淘汰）
    pub dirfrag_id: DirFragId,

    /// 是否为 Auth 元数据（本节点是 leader 的 shard）
    pub is_auth: bool,

    /// Raft apply 版本号（Staging→Clean 时设置）
    pub raft_version: u64,
}

/// 缓存条目状态机
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheState {
    /// 与 RocksDB 一致，可被 trim 淘汰
    Clean,

    /// 新创建，待 Raft apply。不可淘汰，读命中直接返回。
    Staging,

    /// 已修改，待同步 Raft commit。不可淘汰。
    Dirty,

    /// 已删除，待 Raft apply。读命中返回 ENOENT。
    Deleted,

    /// 正在被 trim 写回，临时状态。
    Trimming,
}
```

### DirFrag — 目录分片

参考 Ceph `CDir`（dirfrag），是**淘汰的基本单元**：

```rust
/// 目录分片 ID。
///
/// 大目录会被切分为多个 frag，每个 frag 是独立的淘汰单元。
/// 小目录 FragId = 0（完整目录）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DirFragId {
    pub parent_inode: u64,
    pub frag_id: u32,
}

/// 目录分片缓存。
///
/// 参考 Ceph CDir。淘汰的基本单元：不能单独淘汰某个 dentry，
/// 必须整个 DirFrag 一起卸载。这适配了文件系统目录的树形语义
/// （同一目录的 dentry 通常一起被访问）。
pub struct DirFrag {
    /// 分片 ID
    pub id: DirFragId,

    /// 该分片下的所有目录项：name → child_inode
    pub dentries: RwLock<BTreeMap<String, u64>>,

    /// 分片状态
    pub state: RwLock<CacheState>,

    /// 分片内 dentry 数量（用于内存估算）
    pub dentry_count: AtomicU32,

    /// 最后访问时间
    pub last_access: AtomicU64,

    /// 该分片是否为 Auth（本节点 leader 的 shard）
    pub is_auth: bool,

    /// 是否为大目录分片（dentry_count > threshold）
    pub is_large: AtomicBool,
}
```

### CachedDentry — 目录项

```rust
/// 目录项缓存（嵌入在 DirFrag.dentries 中）。
///
/// 这里只存 child_inode 映射，完整的 child inode 元数据
/// 在 CachedInode 中。参考 Ceph CDentry。
pub struct CachedDentry {
    pub name: String,
    pub child_inode: u64,
    pub state: CacheState,
}
```

### MetaCache — 主结构

```rust
/// Filer 端元数据缓存子系统。
///
/// 参考 Ceph MDCache。负责：
/// 1. 新建条目的 staging（立即可见，Raft 异步提交）
/// 2. 读路径的缓存加速（命中返回，miss 回退 ShardStore）
/// 3. 内存控制与 trim（目录分片粒度淘汰）
/// 4. 删除标记（Deleted 状态，Raft apply 后清除）
pub struct MetaCache {
    /// inode 缓存表
    inode_table: DashMap<u64, Arc<CachedInode>>,

    /// 目录分片缓存表
    dirfrag_table: DashMap<DirFragId, Arc<DirFrag>>,

    /// 待 Raft apply 的创建队列（用于 sweep 已 apply 的 staging）
    staging_queue: Mutex<VecDeque<(u64, Instant)>>,

    /// 删除标记表（待 Raft apply）
    deleted_markers: DashMap<u64, Instant>,

    /// 内存控制
    trim_controller: TrimController,

    /// 配置
    config: MetaCacheConfig,
}

pub struct MetaCacheConfig {
    /// 内存上限（字节），默认 4GB
    pub memory_limit: usize,

    /// 保留余量（达到上限后优先回收 caps）
    pub reservation: usize,

    /// LRU 冷热分界点（0.0-1.0）
    pub mid: f64,

    /// 大目录分片阈值（dentry_count > 此值则分片）
    pub large_dir_threshold: usize,

    /// staging 超时（超过此时间仍未 Raft apply，视为丢失）
    pub staging_timeout: Duration,

    /// deleted 标记超时
    pub deleted_timeout: Duration,

    /// trim 检查间隔
    pub trim_interval: Duration,
}
```

---

## 读写流程

### 读路径

参考 Ceph MDCache 读流程：

```
lookup(parent, name):
  1. 查 DirFrag: dirfrag_table.get((parent, frag_id))
     ├── hit: 查 dentries.get(name)
     │   ├── hit: 返回 child_inode
     │   │         查 inode_table.get(child_inode) → 返回 CachedInode
     │   └── miss (Deleted): 返回 ENOENT
     └── miss: 回退 ShardStore.get_dir_entry(parent, name)
               ├── hit: 回填 DirFrag + CachedInode (Clean)
               └── miss: 返回 ENOENT

getattr(inode):
  1. 查 inode_table.get(inode)
     ├── hit (Clean/Staging/Dirty): 返回 info
     ├── hit (Deleted): 返回 ENOENT
     └── miss: 回退 ShardStore.get_inode(inode)
               ├── hit: 回填 CachedInode (Clean)
               └── miss: 返回 ENOENT

readdir(parent, offset, limit):
  1. 查 DirFrag: dirfrag_table.get((parent, frag_id))
     ├── hit: 迭代 dentries.range(offset..) → 流式返回
     └── miss: 从 ShardStore 加载完整 DirFrag → 回填 → 返回
```

### 跨 shard 子目录的两级信息缓存（ls 时 stat 子目录提速）

> 设计背景：目录可能在父目录的 shard（parent_shard），而每个子目录自己的
> inode/元数据在另一个 shard（child_shard，按 inode Hash）。如果 ls 后紧接着
> stat 每个子目录，会产生 **N 次跨 shard Filer RPC**。这是大型目录性能瓶颈。
>
> 本方案来自你明确提出的要求 —— "子目录分两部分，父目录那里有部分子目录的
> 信息，所在 shard 也有部分信息（目录下的 dentry 等详细信息）。**这样 ls dir
> 时 stat 子目录也可以从父目录 shard 获取**"。

#### 两级缓存模型

```
parent_shard (P):                       child_shard (C):
  ┌───────────────────────────┐          ┌───────────────────────────┐
  │ parent_inode = 100         │          │ child_inode = 2000        │
  │ DirFrag(parent=100, frag=0)│          │ InodeInfo(mode, uid, ...) │
  │  ├ entries:                │          │ DirFrag(parent=2000, ...) │
  │  │  "sub_a" → 2000         │          │  └ entries: ...           │
  │  │  "sub_b" → 2001         │          └───────────────────────────┘
  │  │  ...
  │  │
  │  └ child_summaries:        ◄── 新增 ── ShardCommand 写入时同步
  │       2000 → DirStatSummary(mode=0755, uid=0, gid=0, size=4096,
  │                                mtime=1700000000, ctime=1700000000,
  │                                child_shard_id=ShardId(17))
  │       2001 → DirStatSummary(...)
  └───────────────────────────┘
```

#### `DirStatSummary` 存什么

只存**足够在 ls 时做 stat** 的轻量摘要，避免拷贝 chunks/inline/replica 等大字段：

```rust
/// 存在 parent_shard 的 DirFrag.child_summaries 中。
/// 写路径：任何修改子目录 inode 的 SetAttr / CreateInode / DeleteInode 都在
/// Raft commit 后，通知 parent_shard 更新本 summary（通过 cross-shard
/// notify，见下文）。
pub struct DirStatSummary {
    pub child_inode:    u64,
    /// mode 的低 12 位（type + perms）。ls -l 需要判断 d/l/- 与 rwx
    pub mode_and_type:  u32,
    pub uid:            u32,
    pub gid:            u32,
    pub size:           u64,
    pub mtime:          u64,
    pub ctime:          u64,
    pub atime:          u64,
    pub nlink:          u32,
    /// Hash(child_inode) = child_shard。客户端拿到 summary 后如果还要
    /// 读 detailed metadata / open，可以直接路由到 child_shard 而无需
    /// 再问 parent_shard。
    pub child_shard_id: ShardId,
    /// 摘要的 LWW 时间戳（父目录 shard 用 max 合并跨 shard notify），
    /// 防止乱序更新覆盖新值。
    pub version_ts:     u64,
}
```

#### 写路径：摘要如何同步到 parent_shard

`CreateInode / SetAttr / DeleteInode` 在 child_shard 上 Raft apply 完成后，
追加一个轻量的**跨 shard 通知**（仍是客户端路由 —— **不做服务间转发**，
遵守 [shard-routing-no-forward-principle.md](shard-routing-no-forward-principle.md)）：

```
create / chmod / chown / truncate / unlink on child_shard(C):
  1. RaftGroupManager.propose() on C → commit + apply
  2. InodeInfo 最新值已在 ShardStore(C)
  3. (新) 把 (parent_inode, name, DirStatSummary{最新字段})
     打包成 ShardCommand::UpdateChildSummary(parent_inode, name, summary)
     由**同一**调用链路（meta_shard_manager）路由到 parent_shard(P) 再 propose
     —— 客户端原本就知道 parent_shard（算过 `calculate_shard(parent_inode)`），
     直接 propose，**不转发**。
  4. parent_shard(P) apply:
       DirFrag(parent_inode).child_summaries.insert(
         child_inode,
         summary, merge_by = max(version_ts)
       )
```

关键点：

| 原则 | 说明 |
|------|------|
| **绝不服务间转发** | UpdateChildSummary 仍由调用者（meta_shard_manager）按 `parent_shard = calculate_shard(parent_inode)` 直连 leader，与普通 setattr / unlink 一致。 |
| **最终一致，不阻塞主路径** | 摘要更新是"尽力而为"，失败不影响 CreateInode/SetAttr 的主流程返回（后台重试 + 下一次 ls miss 时用 inode 直接查 child_shard 兜底）。 |
| **一致性锚：version_ts LWW** | summary 里带 `max(apply_raft_version, timestamp)`，乱序到达时取 `max(ts)` 的胜出，防止"后写被先写覆盖"。 |
| **Deleted 清摘要** | RemoveDirEntry 在 parent_shard apply 时同步在本地 DirFrag 里把 `(parent,name)` 的 dentry + summary 一起删除。 |

#### 读路径：ls dir + stat 子目录如何提速

```
ls /dir_a (parent=100, shard=P):
  readdir(parent=100, shard=P):
    1. DirFrag(100) 从 parent_shard(P) 返回 dentries + child_summaries
       → 客户端本地缓存：EntryState=Clean
    ┌──────────────────────────────────────────────────────────────┐
    │ 传统实现（当前 summary=False）：                               │
    │   stat sub_a → Filer RPC → calculate_shard(2000)=C           │
    │   → lookup parent=100,name=sub_a (P)                          │
    │   → getattr 2000 (C) — 2 次 RPC / entry                       │
    └──────────────────────────────────────────────────────────────┘
    ┌──────────────────────────────────────────────────────────────┐
    │ 本方案（summary=True）：                                       │
    │   stat sub_a — FUSE 本地 EntryAttrCache 命中 summary         │
    │   → 直接返回 mode/uid/gid/size/mtime/ctime/nlink            │
    │   → 0 RPC / entry（ls -l 1000 子目录，RPC: N→1）             │
    └──────────────────────────────────────────────────────────────┘
```

客户端（FUSE `MetadataCache`）在收到 readdir 响应时，如果响应中携带了
`DirStatSummary[]`，就**直接在 EntryAttrCache 里 seed 这些条目**，不需要
再发一轮 per-file stat RPC。

#### 对 MetaCache 内部的影响（本文档的主范围）

- `DirFrag` 增加 `child_summaries: BTreeMap<u64, DirStatSummary>`。
- `ShardStore(parent_shard)` 增加列族 `CF_DIR_CHILD_SUMMARIES`：
  `key = (parent_inode, frag_id, child_inode)`，value = serialized summary。
- Raft `ShardCommand::UpdateChildSummary` 在 parent_shard 应用时：
  MetaCache.DirFrag 更新 `child_summaries[child_inode]`（LWW max ts），
  同 ShardStore.CF_DIR_CHILD_SUMMARIES。
- Trim：DirFrag 卸载时，child_summaries 随 dentries 整体释放。
- Leader 失效：和其它 MetaCache 条目一样，`invalidate_all()` 清空；
  下一次读从 RocksDB CF_DIR_CHILD_SUMMARIES 回填（Clean 状态）。

#### 与禁止转发原则的关系

本方案明确 **不依赖服务间转发**：
- 更新 parent_shard 的 summary 时，不是 child_shard"悄悄给 parent_shard 发 RPC"，
  而是**调用者（MetaShardManager / NetHandler 所在原线程）在完成 child_shard
  propose 后，再对 parent_shard 做一次 propose** —— 调用者有 parent_inode，
  直接 `calculate_shard(parent_inode)` 路由，和其它 two-phase 操作（
  CreateDirectory PhaseA/PhaseB）完全一致。
- 原 [shard-routing-no-forward-principle.md](shard-routing-no-forward-principle.md) §1 已强调
  "禁止服务间转发"，此处不破坏。

### 写路径 — Create（异步 staging）

```
create(parent, name, info):
  1. MetaCache.stage_create(info, parent, name)
     → inode_table.insert(ino, CachedInode { state: Staging })
     → dirfrag_table.get_or_insert(parent).dentries.insert(name, ino)
     → staging_queue.push_back((ino, now))

  2. RaftGroupManager.propose_ff(CreateInode + AddDirEntry)
     → 不等 commit，立即返回

  3. 返回 attr (从 MetaCache 读)  # < 1ms
```

### 写路径 — SetAttr（同步 Raft）

```
setattr(inode, mode, uid, gid):
  1. MetaCache.mark_dirty(inode)
     → inode_table.get(ino).state = Dirty
     → 更新 info 字段

  2. RaftGroupManager.propose(SetAttr)
     → 等待 quorum commit

  3. 等待 apply

  4. MetaCache.confirm_dirty(inode)
     → state = Clean
     → raft_version = current_version
```

### 写路径 — Unlink（同步 Raft）

```
unlink(parent, name, ino):
  1. MetaCache.stage_delete(parent, name, ino)
     → inode_table.get(ino).state = Deleted
     → dirfrag.dentries.remove(name)
     → deleted_markers.insert(ino, now)

  2. RaftGroupManager.propose(RemoveDirEntry + DeleteInode)
     → 等待 quorum commit

  3. 等待 apply

  4. MetaCache.confirm_delete(ino)
     → inode_table.remove(ino)
     → deleted_markers.remove(ino)
```

### Raft Apply 回调

```
on_raft_apply(cmd: ShardCommand):
  match cmd:
    CreateInode { info } →
      MetaCache.confirm_create(info.inode)
        → state = Clean, raft_version = current
        → staging_queue 移除

    AddDirEntry { parent, name, inode } →
      MetaCache.confirm_add_direntry(parent, name)
        → dentry state = Clean

    DeleteInode { inode } →
      MetaCache.confirm_delete(inode)
        → inode_table.remove(inode)
        → deleted_markers.remove(inode)

    RemoveDirEntry { parent, name } →
      MetaCache.confirm_remove_direntry(parent, name)
        → dirfrag.dentries.remove(name)
```

---

## 缓存淘汰策略

参考 Ceph MDCache trim 机制，PowerFS 采用**两级淘汰**：

### 第一级：客户端 Lease Recall

当内存接近上限时，优先 recall 客户端 lease，迫使客户端丢弃本地缓存：

```
trim():
  if current_usage < memory_limit - reservation:
    return  # 内存充足，无需 trim

  # Step 1: Recall 客户端 lease
  # 通知持有 lease 的客户端归还，降低 refcount
  for inode in inode_table.values():
    if inode.refcount > 0 && !inode.is_hot():
      lease_manager.recall(inode.inode)

  # 等待 recall 响应（异步，有超时）
  # 如果客户端不响应，标记为 "failing to respond to cache pressure"
```

### 第二级：DirFrag 卸载

如果 recall 后内存仍超限，卸载冷的 DirFrag：

```
  # Step 2: 筛选可裁剪的 DirFrag
  candidates = dirfrag_table.values()
    .filter(|frag| {
      frag.state == Clean           // 非 dirty/staging
      && frag.refcount == 0          // 无活跃引用
      && !frag.is_auth               // 优先淘汰非 Auth
      && frag.last_access < cold_threshold  // 冷
    })
    .sorted_by(|a, b| a.last_access.cmp(b.last_access))

  # Step 3: 写回 dirty 并释放
  for frag in candidates:
    if frag.state == Dirty:
      ShardStore.flush_dirfrag(frag)  // 写回 RocksDB

    # Step 4: 释放整个 DirFrag
    for (name, child_ino) in frag.dentries:
      inode_table.remove(child_ino)  // 引用计数下降
    dirfrag_table.remove(frag.id)

    if current_usage < memory_limit - reservation:
      break
```

### 关键原则

| 原则 | 说明 |
|------|------|
| 淘汰粒度是 DirFrag | 不能单独淘汰某个 dentry，整个目录分片一起卸载 |
| Staging/Dirty/Deleted 不可淘汰 | 只有 Clean 状态可被 trim |
| 优先淘汰非 Auth | Follower 缓存的副本优先淘汰 |
| refcount > 0 不可淘汰 | 有客户端 lease 引用的不可淘汰 |

### Trim 触发时机

- **定时器**: 每 `trim_interval`（默认 5s）检查一次
- **写入触发**: 每次 stage_create 后检查内存是否超限
- **手动触发**: admin API 可手动触发 trim

---

## 与 Raft/WAL 的交互

### 为什么必须走 Raft？

**核心原则：持久化必须走 Raft，否则没有容错接管。**

```
如果创建直接写 RocksDB 不走 Raft:
  Leader 写本地 RocksDB → 返回成功
  Leader 故障 → Follower 接管
  Follower 的 RocksDB 没有这条记录 → 数据丢失 → 无法接管

Raft 的作用:
  Leader 写 Raft log → 复制到 Follower → quorum commit
  Leader 故障 → Follower 有完整 Raft log → 重放 apply → 数据完整 → 容错接管
```

MetaCache staging **不是持久化**，只是加速读可见性的内存层。真正的持久化是 Raft commit → ShardStore (RocksDB)。

### 三种方案对比

| 方案 | 创建延迟 | 容错 | leader切换 | 适用场景 |
|------|---------|------|-----------|---------|
| A. 同步 Raft + 等 apply (strict) | ~80ms | ✅ 强一致 | 不丢数据 | 苛刻环境 |
| B. Staging + propose (等commit不等apply) | ~10ms | ✅ 强 | 不丢数据 | **推荐(默认)** |
| C. Staging + propose_ff (不等commit) | ~1ms | ⚠️ 弱 | 未commit丢失,客户端重试 | 不推荐 |

### 推荐方案：B (Staging + propose 等 commit 不等 apply)

```
创建流程:
  1. MetaCache.stage_create(info)        # 内存staging,立即可读
  2. Raft.propose(CreateInode + AddDirEntry)  # 等quorum commit,不等apply
  3. 返回客户端 (从staging读attr)         # ~10ms

  (后台 ~5ms)
  4. Raft commit → ShardStore.apply()     # 写RocksDB (leader+follower)
  5. MetaCache.confirm_create()           # 清除staging,ShardStore接管

容错保证:
  ✅ commit保证: 数据已复制到多数follower,leader故障不丢
  ✅ staging保证: commit后apply前,读命中staging
  ✅ apply后: staging清除,ShardStore接管读
  ✅ leader切换: 已commit的Raft log会被新leader重放apply

leader切换场景:
  场景1: commit后apply前leader故障
    → 新leader重放Raft log → apply到RocksDB ✅
    → staging丢失,但ShardStore有数据 ✅
    → 客户端读: miss staging → hit ShardStore ✅

  场景2: commit前leader故障 (propose未完成)
    → Raft log未commit,新leader截断未commit日志
    → 客户端收到错误(STATUS_ERR_REDIRECT) → 重试到新leader ✅
    → 不会出现"假成功"
```

### 方案 C (propose_ff) 的问题

```
propose_ff不等commit就返回:
  → 如果leader在commit前故障
  → 客户端收到的"成功"是假成功
  → 后续访问 "file not found"
  → 这就是之前遇到的 "UpdateInodeSizeChunks failed for inode not found" 根因

结论: propose_ff只适用于可丢失的非关键操作(如UpdateInodeSizeChunks)
      CreateInode/AddDirEntry必须用propose(等commit)
```

### Raft 作为 WAL

参考 Ceph MDLog，PowerFS 的 Raft log 充当 WAL：

```
写操作流程 (方案B):
  1. MetaCache.stage_create()     # 修改内存 (staging)
  2. Raft.propose()               # 写 WAL (Raft log), 等commit
  3. 返回客户端                    # ~10ms

  (后台)
  4. Raft apply → ShardStore.apply()  # 持久化到 RocksDB
  5. MetaCache.confirm_create()         # 清除 staging

关键区别 vs Ceph MDLog:
  - Ceph: MDLog先写RADOS,后台flush到元数据对象
  - PowerFS: Raft log先commit(复制),后台apply到RocksDB
  - 两者都是: WAL优先,持久化延迟,保证崩溃一致性
```

### 性能预期

| 操作 | 方案A(strict) | 方案B(推荐) | 方案C(不推荐) |
|------|--------------|------------|--------------|
| create(同shard) | ~80ms | ~10ms | ~1ms |
| create(跨shard) | ~120ms | ~20ms | ~2ms |
| 容错 | 强 | 强 | 弱(丢数据) |
| IOPS | ~12 | ~100 | ~1000但不可靠 |

**方案B在保证容错的前提下,比方案A快8倍,是性能与可靠性的最佳平衡。**

### Follower 节点的 MetaCache

Follower 节点也维护 MetaCache，但：
- **不处理 create**: Follower 不接收写请求，无 staging
- **从 Raft apply 填充**: 每次 apply 时更新缓存（Clean 状态）
- **可服务读请求**: 读打到 follower 时，缓存命中直接返回
- **优先淘汰**: trim 时优先淘汰 follower 上的非 Auth 缓存

---

## 客户端 Lease/Caps 联动

参考 Ceph caps recall 机制：

```
                    Filer (MetaCache)              FUSE Client
                    ─────────────────              ───────────
读请求 ──────────────────────────────────────────► lookup/getattr
                                                       │
    ◄────────── 返回 attr + 颁发 lease (Shared) ──────┘
    refcount++

内存压力:
    recall lease ─────────────────────────────────► 收到 recall
    refcount--                                      丢弃本地缓存
    ◄──────────────── 归还 lease ──────────────────────┘

如果客户端不响应 recall:
    标记 "failing to respond to cache pressure"
    超时后强制失效该客户端的所有 lease
```

### Recall 触发条件

1. MetaCache 内存超过 `memory_limit - reservation`
2. 单个 DirFrag 的 refcount 过高（热点目录）
3. admin API 手动触发

---

## 故障恢复

参考 Ceph MDS standby-replay 的 MDLog 回放：

### Filer 重启

```
1. ShardStore 从 RocksDB 加载持久化元数据
2. MetaCache 初始化为空（无 staging）
3. Raft 从日志回放未 apply 的条目
4. 每个 apply 更新 MetaCache（Clean 状态）
5. 对外提供服务

注意: MetaCache 是可丢失的缓存，重启后从 RocksDB 重建。
      staging 条目在重启时丢失，客户端需重试。
```

### Leader 切换

```
1. 新 leader 检测到 cache_epoch 变化
2. MetaCache.invalidate_all() — 清除所有 staging
3. 从 ShardStore 读取当前状态（Clean 状态填充）
4. 客户端重试失败的请求
```

---

## 大目录分片 DirFrag

参考 Ceph dirfrag，支持百万级子项的目录：

### 分片策略

```
初始: DirFragId(parent, frag_id=0)  // 完整目录

当 dentry_count > large_dir_threshold (默认 4096):
  分裂为:
    DirFragId(parent, frag_id=1)  // hash(name) & mask == 0
    DirFragId(parent, frag_id=2)  // hash(name) & mask == 1

readdir 时:
  按 frag_id 顺序流式迭代，不一次性加载全部子项
```

### 流式 readdir

```rust
fn readdir(&self, parent: u64, offset: &str, limit: usize) -> Vec<DirEntry> {
    let frag_id = self.calculate_frag(parent, offset);
    let frag = self.dirfrag_table.get_or_load(DirFragId(parent, frag_id));

    let entries = frag.dentries.range(offset.to_string()..)
        .take(limit)
        .collect();

    // 如果当前 frag 不够，继续加载下一个 frag
    if entries.len() < limit {
        let next_frag = frag_id + 1;
        // ... 加载下一个分片
    }

    entries
}
```

---

## crate 结构

```
powerfs-meta-cache/
├── Cargo.toml
├── src/
│   ├── lib.rs              # 公共 API 导出
│   ├── cache.rs            # MetaCache 主结构
│   ├── inode.rs            # CachedInode
│   ├── dirfrag.rs          # DirFrag, DirFragId
│   ├── dentry.rs           # CachedDentry
│   ├── state.rs            # CacheState 状态机
│   ├── trim.rs             # TrimController 淘汰控制
│   ├── staging.rs          # StagingQueue 创建暂存
│   ├── deleted.rs          # DeletedMarkers 删除标记
│   ├── config.rs           # MetaCacheConfig
│   ├── metrics.rs          # Prometheus 指标
│   └── sweep.rs            # 后台 sweep 线程
├── tests/
│   ├── cache_test.rs       # 基本缓存测试
│   ├── trim_test.rs        # 淘汰策略测试
│   ├── staging_test.rs     # Staging 正确性测试
│   ├── dirfrag_test.rs     # 大目录分片测试
│   └── concurrency_test.rs # 并发安全测试
└── benches/
    ├── lookup_bench.rs     # 读性能基准
    └── create_bench.rs     # 创建性能基准
```

### 依赖关系

```toml
[dependencies]
powerfs-common = { path = "../powerfs-common" }   # InodeInfo, 类型
dashmap = "5"
parking_lot = "0.12"
crossbeam-queue = "0.3"
prometheus = "0.13"
log = "0.4"

[dev-dependencies]
tokio = { version = "1", features = ["full"] }
tempfile = "3"
```

### 公共 API

```rust
// lib.rs
pub use cache::MetaCache;
pub use inode::CachedInode;
pub use dirfrag::{DirFrag, DirFragId};
pub use dentry::CachedDentry;
pub use state::CacheState;
pub use config::MetaCacheConfig;
pub use trim::TrimController;
```

---

## 与现有系统集成

### 集成点

```
powerfs-filer/src/meta_shard_manager.rs
  ├── 持有 Arc<MetaCache>
  ├── create_inode() → meta_cache.stage_create() + propose_ff
  ├── get_inode() → meta_cache.get_inode() → fallback ShardStore
  ├── lookup_entry() → meta_cache.get_direntry() → fallback ShardStore
  ├── setattr() → meta_cache.mark_dirty() + propose (同步)
  ├── unlink() → meta_cache.stage_delete() + propose (同步)
  └── on_raft_apply() → meta_cache.confirm_*()

powerfs-filer/src/shard_store.rs
  └── apply_command() 完成后回调 meta_cache.confirm_*()

powerfs-filer/src/net_handler.rs
  └── 读请求走 meta_cache

powerfs-filer/src/main.rs
  └── 启动 meta_cache 后台 sweep 线程

powerfs-filer/src/lease_persistence.rs
  └── trim 触发 recall client lease
```

### 替换现有 meta_cache.rs

当前 `powerfs-filer/src/meta_cache.rs` 是简单实现，将被替换为独立 crate：

```rust
// powerfs-filer/src/meta_shard_manager.rs
// 旧:
use crate::meta_cache::MetaCache;

// 新:
use powerfs_meta_cache::MetaCache;
```

---

## 分阶段实施计划

### 阶段 1: 基础缓存层 (MVP)

**目标**: 替换现有简单 meta_cache.rs，提供 staging + 读加速

- [ ] 创建 `powerfs-meta-cache` crate
- [ ] 实现 `MetaCache` 基本结构（inode_table + dirfrag_table）
- [ ] 实现 `stage_create()` / `get_inode()` / `get_direntry()`
- [ ] 实现 `stage_delete()` / `deleted_markers`
- [ ] 实现 `confirm_*()` 回调
- [ ] 实现 `invalidate_all()` (leader 切换)
- [ ] 集成到 `MetaShardManager`
- [ ] 验证: 创建后立即可读 + leader 切换后 staging 清除

### 阶段 2: 缓存淘汰

**目标**: 内存可控，支持大容量缓存

- [ ] 实现 `TrimController`（内存计数 + 水位）
- [ ] 实现 DirFrag 粒度淘汰
- [ ] 实现 staging/deleted 超时 sweep
- [ ] 实现定时 trim 线程
- [ ] 验证: 100万 inode 内存不爆 + trim 正确淘汰

### 阶段 3: 客户端 Lease Recall 联动

**目标**: 内存压力时 recall 客户端 lease

- [ ] 实现 lease recall 机制
- [ ] FUSE 客户端处理 recall 消息
- [ ] refcount 追踪
- [ ] 验证: recall 后客户端丢弃缓存 + refcount 下降

### 阶段 4: 大目录分片

**目标**: 支持百万级子项目录

- [ ] 实现 DirFrag 分裂逻辑
- [ ] 实现流式 readdir
- [ ] 验证: 100万子项目录 readdir 不 OOM

### 阶段 5: Follower 缓存优化

**目标**: Follower 服务读请求，优先淘汰非 Auth

- [ ] Follower 从 Raft apply 填充缓存
- [ ] trim 优先淘汰非 Auth
- [ ] 验证: Follower 读加速 + Auth/非Auth 淘汰优先级

---

## 与 Ceph MDCache 对比

| 维度 | Ceph MDCache | PowerFS MetaCache |
|------|-------------|-------------------|
| **内存对象** | CInode/CDir/CDentry (C++原生对象) | CachedInode/DirFrag/CachedDentry (Rust结构体) |
| **淘汰单元** | 完整 dirfrag 目录分片 | DirFrag 目录分片 (相同理念) |
| **持久化** | MDLog(WAL) + RADOS对象; 延迟刷盘 | Raft log(WAL) + RocksDB; 延迟 apply |
| **readdir** | 加载整个 dirfrag 对象 | RocksDB range scan + DirFrag 流式迭代 |
| **冷热分离** | 靠 trim 卸载整个分片 | 内存缓存淘汰，冷数据留在 RocksDB |
| **序列化** | 内存原生对象，无序列化 | 内存命中无序列化；miss 需反序列化 |
| **多节点缓存** | 每个 MDS 独立 MDCache | 每个 Filer 独立 MetaCache |
| **客户端联动** | Caps recall | Lease recall |
| **故障恢复** | MDLog 回放重建 | Raft log 回放 + RocksDB 重建 |
| **锁管理** | Locker (excl/shared 分布式锁) | Raft leader lease (无分布式锁) |
| **大目录** | dirfrag 分片 | DirFrag 分片 (相同理念) |

### PowerFS 的优势

1. **Rust 内存安全**: 无 C++ 的手动内存管理风险
2. **Raft 强一致**: leader lease 代替复杂分布式锁
3. **RocksDB LSM-Tree**: range scan 高效，天然支持流式 readdir
4. **DashMap 无锁读**: 高并发读性能

### PowerFS 的劣势

1. **序列化开销**: miss 时需反序列化（Ceph 内存原生对象无此开销）
2. **无实时 standby-replay**: Filer 故障切换依赖 Raft 重新选举，非热备

---

## 附录

### 配置示例

```toml
# filer.toml
[meta_cache]
memory_limit = "4GB"          # 内存上限
reservation = "512MB"         # 保留余量
mid = 0.7                     # LRU 冷热分界
large_dir_threshold = 4096    # 大目录分片阈值
staging_timeout = "30s"       # staging 超时
deleted_timeout = "10s"       # deleted 标记超时
trim_interval = "5s"          # trim 检查间隔
```

### Prometheus 指标

```
powerfs_metacache_inode_count{state="clean"}      # Clean inode 数
powerfs_metacache_inode_count{state="staging"}    # Staging inode 数
powerfs_metacache_inode_count{state="dirty"}      # Dirty inode 数
powerfs_metacache_inode_count{state="deleted"}    # Deleted inode 数
powerfs_metacache_dirfrag_count                   # DirFrag 数
powerfs_metacache_memory_bytes                    # 当前内存使用
powerfs_metacache_trim_total                      # trim 次数
powerfs_metacache_recall_total                    # lease recall 次数
powerfs_metacache_staging_timeout_total           # staging 超时次数
```

### 相关文档

- [meta-perf-optimization-plan.md](meta-perf-optimization-plan.md) — 元数据性能优化总方案
- [dir-lease-design.md](dir-lease-design.md) — 目录级 lease 设计
- [shard-routing-no-forward-principle.md](shard-routing-no-forward-principle.md) — 分片路由原则
- [lock-optimization-plan.md](lock-optimization-plan.md) — 锁优化方案
