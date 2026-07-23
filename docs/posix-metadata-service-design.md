# PowerFS POSIX 元数据服务改造方案

## 0. CRDT 弱一致性与 Delta Sync 设计

### 0.1 核心设计原则

PowerFS 采用 **CRDT (Conflict-free Replicated Data Type)** 实现元数据的弱一致性，与传统强一致性的 Lease 锁机制形成互补：

| 维度 | 元数据操作 (Metadata) | 数据操作 (Data) |
|------|-----------------------|-----------------|
| **操作类型** | mkdir, rmdir, create, unlink, lookup, readdir, symlink, rename | open, read, write, close, flush |
| **一致性模型** | CRDT 弱一致性 (最终一致) | 强一致性 (线性化) |
| **同步机制** | VectorClock + Delta Sync (异步) | Lease 锁 + 数据节点直连 |
| **锁机制** | **无锁** (CRDT Merge 解决冲突) | **Lease 锁** (Follower 向 Leader 申请) |
| **延迟特征** | 本地操作立即返回，后台异步同步 | 需要网络往返 (获取/释放 Lease) |

### 0.2 CRDT 架构设计

#### 0.2.1 数据结构

```rust
// VectorClock: 跟踪各客户端的操作序列
pub struct VectorClock {
    entries: HashMap<String, u64>,  // client_id -> sequence_number
}

// OR-Set: 无冲突复制数据类型
// 每个目录对应一个 DirORSet，存储该目录下的所有条目
pub struct DirORSet {
    dir_ino: u64,
    entries: HashMap<EntryId, DirEntry>,
    // entries 中每个 entry 带有 tag: {client_id, seq}
    // 用于 CRDT merge 时判断操作顺序
}

// EntryId: 条目的唯一标识
pub struct EntryId {
    name: String,
    client_id: u64,  // 创建者 ID
    seq: u64,         // 创建者的本地序列号
}
```

#### 0.2.2 Delta Sync 流程

```
FUSE Client A                    Filer Server                    FUSE Client B
    │                                │                                │
    │  1. 本地操作 (mkdir/file)      │                                │
    │──────────────────────────────▶│                                │
    │  应用到本地 DirORSet            │                                │
    │  更新本地 VectorClock           │                                │
    │                                │                                │
    │  2. 异步 push_delta             │                                │
    │──────────────────────────────▶│                                │
    │  (DeltaOp列表 + VectorClock)   │  3. CRDT Merge               │
    │                                │  合并到服务端 OR-Set            │
    │                                │  更新服务端 VectorClock          │
    │                                │                                │
    │                                │  4. 定时 pull_delta            │
    │◀──────────────────────────────│──────────────────────────────▶│
    │  返回 B 的变更 (DeltaOp列表)   │                                │
    │                                │  5. 应用到本地 DirORSet          │
    │                                │  更新本地 VectorClock           │
    │                                │  触发缓存失效                   │
```

#### 0.2.3 DeltaOp 类型

```rust
// 与 Master 的 DeltaOp 保持兼容
pub struct DeltaOp {
    op: Option<DeltaOpType>,
    vclock: Option<VectorClock>,
}

pub enum DeltaOpType {
    Add(DirEntryOrset),      // 新增条目
    Remove(EntryId),         // 删除条目
    Rename(RenameOp),        // 重命名
    SetAttr(SetAttrOp),      // 更新属性
}
```

### 0.3 实现方案分阶段

#### Phase 1: Filer 端 Delta 计算与合并

**目标**: 让 Filer 成为真正的 CRDT 协作节点

**关键实现**:
- `meta_shard_manager.pull_delta()`: 基于 VectorClock 差值计算增量操作
- `meta_shard_manager.push_delta()`: CRDT Merge 合并客户端推送的 Delta

#### Phase 2: 客户端 Delta 接收与应用

**目标**: 正确解析 Filer 返回的 Delta，更新本地 OR-Set

**关键实现**:
- `do_pull_and_apply_deltas()`: 实现 Filer DeltaOp 到本地 DirORSet 的转换
- 对 Add/Remove/Rename/SetAttr 四种操作分别实现 apply 逻辑

#### Phase 3: 元数据写路径解耦

**目标**: 元数据操作改为纯本地 CRDT + 异步 Delta Sync

**关键改动**:
- 移除 mkdir/create/symlink 中的同步 `filer_create_entry` 调用
- 改为仅写入本地 `ShardedDirCache` + `add_change` 到 ChangeCache
- `change_cache_flusher` 统一使用 `push_delta` 批量同步

#### Phase 4: Cache 失效联动与 Lease 隔离

**目标**: 完善缓存一致性，明确 Lease 使用边界

**关键实现**:
- Delta 应用后触发 `MetadataCache` 失效
- 审计 Lease 调用点，确保仅用于数据操作路径

### 0.4 CRDT 冲突合并引擎设计

#### 0.4.1 问题分析

当前实现存在以下缺陷：

| 问题 | 现状 | 影响 |
|------|------|------|
| **全局 VectorClock** | `server_vclock` 是全局单例 | 不同分片的 Delta 同步互相干扰 |
| **简单追加 DeltaLog** | `push_delta` 只是 append 操作 | 无冲突检测，直接覆盖写入 |
| **缺失 seq 追踪** | `extract_seq_from_delta` 仅处理 Add | Remove/Rename/SetAttr 同步不完整 |
| **无 OR-Set 数据结构** | 直接操作 ShardStore | 并发写入会导致数据丢失 |
| **FIFO 淘汰策略** | DeltaLog max_size=10000 后截断 | 新客户端无法获取完整历史 |

#### 0.4.2 核心数据结构

##### EntryTag: 操作唯一标识

```rust
/// CRDT 操作的唯一标签 (tag)，用于冲突检测和合并
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct EntryTag {
    client_id: String,      // 创建者/操作者 ID
    seq: u64,               // 客户端本地序列号
    operation_id: String,   // 操作唯一 ID (UUID)
}

impl EntryTag {
    pub fn new(client_id: &str, seq: u64) -> Self {
        Self {
            client_id: client_id.to_string(),
            seq,
            operation_id: format!("{}-{}", client_id, seq),
        }
    }
    
    /// 检查两个 tag 是否来自同一客户端的操作序列
    pub fn same_client(&self, other: &EntryTag) -> bool {
        self.client_id == other.client_id
    }
    
    /// 比较操作因果顺序 (同一客户端内 seq 越大越新)
    pub fn is_newer_than(&self, other: &EntryTag) -> bool {
        self.client_id == other.client_id && self.seq > other.seq
    }
}
```

##### DirEntryOrset: 带 CRDT tag 的目录项

```rust
/// 带 CRDT 标签的目录条目
#[derive(Debug, Clone)]
pub struct DirEntryOrset {
    tag: EntryTag,           // 创建/修改此条目的操作标签
    inode: u64,              // inode 号
    name: String,            // 文件名
    parent_ino: u64,         // 父目录 inode
    mode: u32,               // 文件模式
    file_type: FileType,     // File/Directory
    size: u64,               // 文件大小
    mtime: u64,              // 修改时间
    etag: Option<String>,    // ETag (S3 兼容)
}
```

##### Tombstone: 已删除条目的标记

```rust
/// CRDT Tombstone: 标记已删除的条目，用于正确处理并发 Remove 操作
#[derive(Debug, Clone)]
pub struct Tombstone {
    tag: EntryTag,           // 删除操作的标签
    entry_key: String,       // 条目键 (parent_ino + ":" + name)
    deleted_at: Instant,     // 删除时间
    gc_epoch: u64,           // 垃圾回收纪元 (用于清理)
}
```

##### ServerDirORSet: 服务端 CRDT 目录状态

```rust
/// 每个分片每个目录的 CRDT OR-Set 状态
pub struct ServerDirORSet {
    dir_ino: u64,                                    // 目录 inode
    entries: HashMap<String, DirEntryOrset>,         // name -> entry
    entry_tags: HashMap<String, HashSet<EntryTag>>,  // name -> 所有操作 tags (用于冲突检测)
    tombstones: HashMap<String, Vec<Tombstone>>,     // name -> 已删除的 tombstone 列表
    vclock: ServerVectorClock,                       // 此目录的 VectorClock
}

impl ServerDirORSet {
    pub fn new(dir_ino: u64) -> Self { ... }
    
    /// 合并一个 Add 操作
    pub fn merge_add(&mut self, entry: DirEntryOrset) -> MergeResult { ... }
    
    /// 合并一个 Remove 操作
    pub fn merge_remove(&mut self, entry_key: &str, tag: &EntryTag) -> MergeResult { ... }
    
    /// 合并一个 Rename 操作
    pub fn merge_rename(&mut self, old_key: &str, new_key: &str, tag: &EntryTag) -> MergeResult { ... }
    
    /// 合并一个 SetAttr 操作
    pub fn merge_setattr(&mut self, entry_key: &str, tag: &EntryTag, size: u64, mtime: u64) -> MergeResult { ... }
    
    /// 检查操作是否可以安全应用 (无并发冲突)
    pub fn is_causally_ready(&self, tag: &EntryTag) -> bool { ... }
    
    /// 清理过期的 tombstone
    pub fn cleanup_tombstones(&mut self, max_age: Duration) { ... }
}

#[derive(Debug, Clone, PartialEq)]
pub enum MergeResult {
    Applied,           // 操作已应用
    Idempotent,        // 幂等 (重复操作，已忽略)
    ConcurrentlyAdded, // 并发 Add (同名不同 tag，两个都保留)
    ConcurrentlyRemoved, // 并发 Remove (已标记删除)
    Conflict,          // 检测到冲突，需进一步处理
}
```

#### 0.4.3 冲突合并语义规则

##### Add + Add 冲突 (同名不同客户端)

```
场景: 客户端 A 和 B 并发创建同名文件 "foo.txt"

规则:
1. 如果两个 tag 相同 (同一客户端重试) → Idempotent，保留一个
2. 如果两个 tag 不同 (不同客户端) → 两个都保留，创建两个 inode
3. 最终结果: DirORSet 中存在两个 "foo.txt"，lookup 返回其中一个 (由实现定义)

示例:
  Client A: Create "foo.txt" (tag: A-1, inode: 100)
  Client B: Create "foo.txt" (tag: B-1, inode: 101)
  
  合并结果:
    entries["foo.txt"] = DirEntryOrset { tag: A-1, inode: 100, ... }
    entries["foo.txt#B-1"] = DirEntryOrset { tag: B-1, inode: 101, ... }
    entry_tags["foo.txt"] = {A-1, B-1}
```

##### Add + Remove 冲突 (并发创建和删除)

```
场景: 客户端 A 创建 "foo.txt"，客户端 B 并发删除 "foo.txt"

规则 (Add-Wins 语义):
1. 如果 Remove 的 tag 已在 entry_tags 中 → Remove 有效，Add 被 tombstone
2. 如果 Remove 的 tag 不在 entry_tags 中 (并发) → Add 优先，Remove 被忽略
3. 原因: 创建操作通常比删除操作更重要

示例:
  服务端已有: entries["foo.txt"] = {tag: A-1, inode: 100}
  Client B: Remove "foo.txt" (tag: B-1)
  
  合并结果 (Add-Wins):
    entries["foo.txt"] = {tag: A-1, inode: 100}  // 保留 Add
    tombstones["foo.txt"] = [Tombstone {tag: B-1, ...}]  // 记录 Remove
    下次 B pull_delta 时会收到 "foo.txt" 存在的通知
```

##### SetAttr + SetAttr 冲突 (并发属性修改)

```
场景: 客户端 A 和 B 并发修改同一文件的属性

规则 (Last-Writer-Wins 基于 VectorClock):
1. 如果两个操作是因果有序的 (一个在另一个之后) → 后者覆盖前者
2. 如果两个操作是并发的 → 基于 VectorClock 偏序关系决定
3. 决策逻辑:
   - 如果 vclock(A) > vclock(B) (A 因果依赖 B) → A 覆盖 B
   - 如果 vclock(B) > vclock(A) (B 因果依赖 A) → B 覆盖 A
   - 如果 vclock 不可比较 (真正并发) → 使用 (client_id, seq) 作为 tiebreaker

示例:
  服务端已有: entries["file.txt"] = {size: 100, mtime: T1, vclock: {A:1}}
  Client A: SetAttr size=200, vclock: {A:2}
  Client B: SetAttr size=300, vclock: {A:1, B:1}
  
  分析:
    - A 的 vclock {A:2} 依赖于服务端 {A:1} (因果有序)
    - B 的 vclock {A:1, B:1} 也依赖于服务端 {A:1}
    - A 和 B 并发 (互不依赖)
    
  合并结果:
    - 比较 (client_id, seq): A(1) vs B(1) → 假设按字典序 A < B
    - B 优先: entries["file.txt"] = {size: 300, mtime: T_B, ...}
```

##### Rename 冲突 (并发重命名)

```
场景: 客户端 A 将 "foo.txt" 重命名为 "bar.txt"，客户端 B 并发删除 "foo.txt"

规则:
1. Rename 本质是 Add(new) + Remove(old) 的复合操作
2. 与 Remove 冲突时: Rename 的 Add 部分优先 (同 Add-Wins)
3. 与 Rename 冲突时: 两个 rename 的 Add 部分都保留

示例 (Rename + Remove):
  服务端已有: entries["foo.txt"] = {tag: X-1, inode: 100}
  Client A: Rename "foo.txt" → "bar.txt" (tag: A-1)
  Client B: Remove "foo.txt" (tag: B-1)
  
  合并结果:
    entries["bar.txt"] = {tag: A-1, inode: 100}  // Rename 成功
    tombstones["foo.txt"] = [Tombstone {tag: B-1, ...}]  // Remove 被记录
```

#### 0.4.4 冲突检测与合并流程图

```
                    ┌─────────────────────────┐
                    │   收到客户端 DeltaOp     │
                    └───────────────┬─────────┘
                                    │
                                    ▼
                    ┌─────────────────────────┐
                    │  查找对应目录的 OR-Set   │
                    │  (dir_ino → DirORSet)   │
                    └───────────────┬─────────┘
                                    │
                                    ▼
                    ┌─────────────────────────┐
                    │  解析 DeltaOp 类型       │
                    │  (Add/Remove/Rename/    │
                    │   SetAttr)              │
                    └───────────────┬─────────┘
                                    │
                         ┌──────────┼──────────┬──────────┐
                         ▼          ▼          ▼          ▼
                    ┌─────────┐┌─────────┐┌─────────┐┌─────────┐
                    │  Add    ││ Remove  ││ Rename  ││SetAttr  │
                    └────┬────┘└────┬────┘└────┬────┘└────┬────┘
                         │          │          │          │
                         ▼          ▼          ▼          ▼
                    ┌─────────────────────────────────────────────┐
                    │           冲突检测                           │
                    │  1. tag 是否重复? (幂等检测)                  │
                    │  2. entry_tags 中是否有并发 tag?              │
                    │  3. VectorClock 偏序关系?                    │
                    └───────────────┬─────────────────────────────┘
                                    │
                         ┌──────────┴──────────┐
                         ▼                      ▼
                    ┌─────────────┐      ┌─────────────┐
                    │  无冲突/幂等 │      │  并发冲突   │
                    └──────┬──────┘      └──────┬──────┘
                           │                    │
                           ▼                    ▼
                    ┌─────────────┐      ┌─────────────────────┐
                    │ 直接应用    │      │ 按语义规则合并:      │
                    │ 更新 entries │      │ - Add+Add: 双保留  │
                    │ 更新 vclock  │      │ - Add+Remove: Add  │
                    └──────┬──────┘      │   优先              │
                           │             │ - SetAttr: LWW      │
                           │             └──────────┬──────────┘
                           │                        │
                           └────────────┬───────────┘
                                        │
                                        ▼
                            ┌─────────────────────────┐
                            │  同步到其他副本          │
                            │  (Raft 复制 or Delta)   │
                            └─────────────────────────┘
```

#### 0.4.5 Per-Shard VectorClock 设计

```
改造前 (全局 VClock):
  MetaShardManager {
    server_vclock: ServerVectorClock  // 全局单例
  }
  
  问题: Shard 0 和 Shard 1 的 Delta 同步互相干扰

改造后 (Per-Shard VClock):
  MetaShardManager {
    shard_vclocks: HashMap<ShardId, ServerVectorClock>  // 按分片管理
  }
  
  每个分片独立维护自己的 VectorClock:
  - Shard 0: 跟踪操作 shard 0 内 inode 的客户端序列
  - Shard 1: 跟踪操作 shard 1 内 inode 的客户端序列
  - ...
```

#### 0.4.6 持久化与清理策略

##### OR-Set 状态持久化

```rust
// 新增 RocksDB Column Family
const CF_ORSET_STATE: &str = "orset_state";     // 持久化 DirORSet 状态
const CF_TOMBSTONES: &str = "tombstones";       // 持久化 Tombstone 记录

// 持久化格式
// orset_state: {dir_ino → serialized DirORSet}
// tombstones: {entry_key → serialized Vec<Tombstone>}
```

##### Tombstone 清理策略

```
清理条件 (满足任一):
1. 时间过期: tombstone.deleted_at + TTL (默认 24h) < now
2. 所有客户端已知晓: 所有已知客户端的 vclock 都已包含此操作
3. 显式 GC: 管理员手动触发垃圾回收

清理流程:
1. 定期 (每小时) 扫描 tombstones
2. 检查每个 tombstone 的过期时间
3. 对于过期 tombstone，检查是否还有客户端需要此信息
4. 如果不需要，删除 tombstone 并更新 RocksDB
```

##### DeltaLog 滚动策略

```
改造前 (FIFO 截断):
  max_size = 10000 → 超出后删除最旧条目
  问题: 新客户端可能丢失历史

改造后 (Snapshot + Delta):
  1. 定期 (每 10000 操作) 创建 OR-Set snapshot
  2. 新客户端从 snapshot 开始，获取增量 delta
  3. Snapshot 之间的 delta 保留，支持增量同步
  4. 超过 snapshot 周期的 delta 可以安全删除
```

#### 0.4.7 实现分步计划

##### Phase A: ServerDirORSet 数据结构与 merge 方法

**目标**: 实现核心 CRDT 数据结构和合并逻辑

**关键实现**:
- [ ] `EntryTag` 结构体 (唯一标识 + 因果比较)
- [ ] `DirEntryOrset` 结构体 (带 tag 的目录项)
- [ ] `Tombstone` 结构体 (删除标记)
- [ ] `ServerDirORSet` 结构体 (per-shard per-dir CRDT 状态)
- [ ] `merge_add()` 方法 (Add 合并 + 冲突检测)
- [ ] `merge_remove()` 方法 (Remove 合并 + Add-Wins 语义)
- [ ] `merge_setattr()` 方法 (SetAttr 合并 + LWW 语义)
- [ ] `merge_rename()` 方法 (Rename 合并)
- [ ] `is_causally_ready()` 方法 (因果就绪检查)

##### Phase B: 重写 push_delta/pull_delta 使用 OR-Set merge

**目标**: 用 CRDT merge 替代简单追加

**关键实现**:
- [ ] 将 `server_vclock` 改为 `shard_vclocks: HashMap<ShardId, ServerVectorClock>`
- [ ] 添加 `orset_states: HashMap<(ShardId, u64), ServerDirORSet>` 存储
- [ ] 重写 `push_delta()`: 先 merge 到 OR-Set，再持久化到 ShardStore
- [ ] 重写 `pull_delta()`: 基于 OR-Set 计算增量变更
- [ ] 修复 `extract_seq_from_delta()`: 支持所有 DeltaOp 类型

##### Phase C: Tombstone 持久化与清理策略

**目标**: 完善 CRDT 正确性和存储管理

**关键实现**:
- [ ] 新增 `CF_ORSET_STATE` 和 `CF_TOMBSTONES` ColumnFamily
- [ ] `ServerDirORSet` 状态持久化到 RocksDB
- [ ] Tombstone 持久化与加载
- [ ] 实现 tombstone TTL 清理任务
- [ ] 实现 DeltaLog snapshot + delta 滚动策略

##### Phase D: 单元测试验证并发冲突场景

**目标**: 验证 CRDT 合并语义的正确性

**测试用例**:
- [ ] **Test 1**: 单客户端顺序操作 (Add → SetAttr → Remove)
- [ ] **Test 2**: 两客户端并发 Add 同名文件 (Add+Add 冲突)
- [ ] **Test 3**: 一客户端 Add + 另一客户端并发 Remove (Add-Wins 语义)
- [ ] **Test 4**: 两客户端并发 SetAttr (Last-Writer-Wins 语义)
- [ ] **Test 5**: 并发 Rename + Remove
- [ ] **Test 6**: 幂等检测 (重复推送相同操作)
- [ ] **Test 7**: VectorClock 因果顺序检测
- [ ] **Test 8**: Tombstone 过期清理
- [ ] **Test 9**: DeltaLog snapshot + delta 滚动

##### Phase E: 集成测试与部署验证

**目标**: 多容器部署下的 CRDT 功能验证

**测试场景**:
- [ ] **场景 1**: 3 Filer 容器启动，3 分片 leader 选举
- [ ] **场景 2**: FUSE 客户端挂载，基本元数据操作
- [ ] **场景 3**: 两个 FUSE 客户端并发操作，验证 CRDT 合并
- [ ] **场景 4**: 关闭一个 Filer 容器，验证 leader 切换
- [ ] **场景 5**: 恢复 Filer 容器，验证数据同步
- [ ] **场景 6**: 大量并发元数据操作压力测试

#### 0.4.8 预期改进效果

| 维度 | 改造前 | 改造后 |
|------|--------|--------|
| **冲突处理** | 直接覆盖，数据丢失 | CRDT Merge，无冲突合并 |
| **并发 Add** | 后者覆盖前者 | 双保留 (由实现决定可见性) |
| **并发 Remove** | 直接删除 | Add-Wins 语义，保护创建操作 |
| **并发 SetAttr** | 随机覆盖 | VectorClock 偏序决定 + LWW |
| **历史同步** | FIFO 截断，可能丢失 | Snapshot + Delta，完整同步 |
| **分片隔离** | 全局 VClock 互相干扰 | Per-Shard VClock 独立管理 |

---

## 1. 背景与目标

### 1.1 问题描述

原有 Filer 服务将 S3 桶服务与 POSIX 元数据服务混合在一起，导致：

- **概念混淆**：路径解析需要先查找桶根 inode，再查找实际路径
- **性能开销**：每次元数据操作需要多一次桶根查找
- **代码复杂度**：桶协议和 POSIX 协议交织，维护困难
- **初始化问题**：默认桶初始化存在时序竞争，导致 "bucket default not found" 错误

### 1.2 改造目标

将元数据服务从桶服务中彻底分离，实现：

1. **独立的 POSIX 元数据服务**：使用扁平路径，无需桶前缀
2. **简化的路径解析**：直接从系统根 inode(1) 开始解析
3. **固定根 inode**：POSIX 根固定为 inode 1，简化数据模型
4. **持久化初始化**：通过 `format` 命令预先初始化元数据

## 2. 架构设计

### 2.1 改造前架构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Filer 服务 (端口 8888/8889)                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌───────────────────────────────┐  ┌─────────────────────────────────┐    │
│  │    S3 服务 (HTTP/axum, 8888)   │  │   元数据服务 (gRPC/tonic, 8889)  │    │
│  ├───────────────────────────────┤  ├─────────────────────────────────┤    │
│  │ • CreateBucket                │  │ • GetEntry (path带桶)            │    │
│  │ • DeleteBucket                │  │ • CreateEntry (path带桶)        │    │
│  │ • HeadBucket                  │  │ • UpdateEntry                   │    │
│  │ • ListBuckets                 │  │ • DeleteEntry                   │    │
│  │ • PutObject                   │  │ • RenameEntry                   │    │
│  │ • GetObject                   │  │ • ListEntries                   │    │
│  │ • DeleteObject                │  │ • Lease 操作                    │    │
│  │ • ListObjects                 │  │ • Delta 同步                    │    │
│  └───────────────────────────────┘  └─────────────────────────────────┘    │
│                 │                              │                           │
│                 └──────────┬───────────────────┘                           │
│                            ↓                                               │
│              ┌─────────────────────────────┐                               │
│              │   MetaShardManager          │                               │
│              ├─────────────────────────────┤                               │
│              │   resolve_path("bucket/key")│                               │
│              │   先找桶根inode再找子节点    │                               │
│              └─────────────────────────────┘                               │
│                            ↓                                               │
│              ┌─────────────────────────────┐                               │
│              │   ShardStore (RocksDB)      │                               │
│              ├─────────────────────────────┤                               │
│              │   inodes: inode→InodeInfo   │                               │
│              │   dir_entries: parent:name  │                               │
│              │   metadata: root_inodes    │                               │
│              └─────────────────────────────┘                               │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 改造后架构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Filer 服务 (端口 8888/8889)                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌───────────────────────────────┐  ┌─────────────────────────────────┐    │
│  │    S3 桶服务 (HTTP, 8888)     │  │   POSIX 元数据 (gRPC, 8889)    │    │
│  ├───────────────────────────────┤  ├─────────────────────────────────┤    │
│  │ 路径模型: /bucket/key         │  │ 路径模型: /path/to/file        │    │
│  │ 解析: 桶根inode + 子节点      │  │ 解析: 根inode(1) + 子节点      │    │
│  │ FilerMetaServiceClient        │  │ PosixMetaServiceClient         │    │
│  └───────────────────────────────┘  └─────────────────────────────────┘    │
│                 │                              │                           │
│                 └──────────┬───────────────────┘                           │
│                            ↓                                               │
│              ┌─────────────────────────────┐                               │
│              │   MetaShardManager (双模式) │                               │
│              ├─────────────────────────────┤                               │
│              │   resolve_flat_path(path)   │ ← POSIX 模式                 │
│              │   resolve_path(bucket/key)  │ ← S3 模式                    │
│              └─────────────────────────────┘                               │
│                            ↓                                               │
│              ┌─────────────────────────────┐                               │
│              │   ShardStore (RocksDB)      │                               │
│              ├─────────────────────────────┤                               │
│              │   POSIX Root: inode=1       │                               │
│              │   S3 Buckets: 独立子树      │                               │
│              └─────────────────────────────┘                               │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 3. 数据模型

### 3.1 POSIX 元数据模型

```rust
// 系统根 (固定 inode = 1)
inode=1, name="/", parent=0, type=Directory

// POSIX 路径示例
inode=2, name="dir1", parent=1
inode=3, name="file1", parent=2
inode=4, name="file2", parent=2, size=1024
inode=5, name="subdir", parent=2

// 目录结构:
/ (inode=1)
├── dir1/ (inode=2)
│   ├── file1 (inode=3)
│   ├── file2 (inode=4, 1KB)
│   └── subdir/ (inode=5)
```

### 3.2 S3 桶模型（保留兼容）

```rust
// S3 桶作为根下的子树
inode=100, name="my-bucket", parent=1
inode=101, name="key1", parent=100
inode=102, name="key2", parent=100

// 快速查找映射
metadata CF: { "bucket:my-bucket": 100 }
```

## 4. 接口设计

### 4.1 Proto 定义

```protobuf
// POSIX 元数据服务 (扁平路径，FUSE 使用)
service PosixMetaService {
    // 基础 CRUD
    rpc GetEntry(GetEntryRequest) returns (GetEntryResponse);
    rpc GetEntryByInode(GetEntryByInodeRequest) returns (GetEntryByInodeResponse);
    rpc CreateEntry(CreateEntryRequest) returns (CreateEntryResponse);
    rpc CreateDirectory(CreateDirectoryRequest) returns (CreateDirectoryResponse);
    rpc UpdateEntry(UpdateEntryRequest) returns (UpdateEntryResponse);
    rpc DeleteEntry(DeleteEntryRequest) returns (DeleteEntryResponse);
    rpc RenameEntry(RenameEntryRequest) returns (RenameEntryResponse);
    rpc ListEntries(ListEntriesRequest) returns (ListEntriesResponse);
    rpc LookupDirectoryEntry(LookupDirectoryEntryRequest) returns (LookupDirectoryEntryResponse);
    
    // Delta sync API
    rpc PushDelta(PushDeltaRequest) returns (PushDeltaResponse);
    rpc PullDelta(PullDeltaRequest) returns (PullDeltaResponse);
    
    // Lease management
    rpc AcquireLease(LeaseRequest) returns (LeaseResponse);
    rpc ReleaseLease(LeaseReleaseRequest) returns (LeaseReleaseResponse);
    rpc RenewLease(LeaseRenewRequest) returns (LeaseRenewResponse);
    
    // Raft message exchange
    rpc SendRaftMessage(RaftMessageRequest) returns (RaftMessageResponse);
    
    // Shard management
    rpc GetShardStats(GetShardStatsRequest) returns (GetShardStatsResponse);
    rpc ListShards(ListShardsRequest) returns (ListShardsResponse);
}

// CreateDirectory 支持递归创建 (mkdir -p)
message CreateDirectoryRequest {
    string path = 1;       // 如: /dir1/subdir
    uint32 mode = 2;       // 权限模式
    string client_id = 3;  // 客户端标识
}

message CreateDirectoryResponse {
    bool success = 1;
    string error = 2;
    uint64 inode = 3;      // 创建的最后一个目录的 inode
}
```

### 4.2 路径格式

- **POSIX 元数据**: `/dir1/file1` (扁平，直接从根开始)
- **S3 桶操作**: `bucket1/key` (保持现状，使用 FilerMetaService)

## 5. 功能对照

### 5.1 Master vs 新 Filer POSIX 服务

| 功能 | Master (DirectoryTree) | 新 Filer POSIX 服务 | 状态 |
|------|----------------------|-------------------|------|
| `get_entry(path)` | ✓ | ✓ (`resolve_flat_path`) | ✅ |
| `get_entry_by_inode(ino)` | ✓ | ✓ | ✅ |
| `create_entry(entry)` | ✓ (文件+目录) | ✓ (通过 mode 判断) | ✅ |
| `update_entry(entry)` | ✓ | ✓ | ✅ |
| `delete_entry(ino, is_dir)` | ✓ | ✓ | ✅ |
| `rename_entry(...)` | ✓ | ✓ | ✅ |
| `list_entries(parent_ino)` | ✓ | ✓ | ✅ |
| `lookup(parent_ino, name)` | ✓ | ✓ | ✅ |
| `create_directory(path)` | ✓ (递归) | ✓ (递归 mkdir -p) | ✅ |
| `init_root()` | ✓ | ✓ (`format_posix_root`) | ✅ |
| Lease 管理 | ✓ | ✓ | ✅ |
| Delta 同步 | ✓ | ✓ | ✅ |
| Raft 消息 | - | ✓ (新增) | ✅ |
| 分片管理 | - | ✓ (新增) | ✅ |

## 6. 代码结构

### 6.1 新增/修改文件

| 文件 | 变更内容 |
|------|----------|
| `powerfs-filer/proto/filer.proto` | 新增 `PosixMetaService` 定义和 `CreateDirectory` 消息 |
| `powerfs-filer/src/posix_service.rs` | **新文件**: 实现 `PosixMetaServiceImpl` |
| `powerfs-filer/src/meta_shard_manager.rs` | 新增 `resolve_flat_path()`, `format_posix_root()`, `has_posix_root()` |
| `powerfs-filer/src/shard_store.rs` | `FileType` 添加 `PartialEq` derive |
| `powerfs-filer/src/lib.rs` | 导出 `posix_service` 模块 |
| `powerfs-filer/src/main.rs` | 初始化 POSIX 根 inode，同时启动两个 gRPC 服务 |
| `powerfs-fuse-core/src/client.rs` | 使用 `PosixMetaServiceClient`，移除桶前缀 |

### 6.2 关键代码示例

#### client.rs - 路径转换

```rust
// 改造前: 添加桶前缀
fn to_filer_path(&self, fuse_path: &str) -> String {
    format!("{}{}", self.collection, fuse_path)  // "default/dir1/file1"
}

// 改造后: 保持扁平路径
fn to_filer_path(&self, fuse_path: &str) -> String {
    fuse_path.to_string()  // "/dir1/file1"
}
```

#### meta_shard_manager.rs - 扁平路径解析

```rust
/// POSIX 根 inode (固定为 1)
pub const POSIX_ROOT_INODE: u64 = 1;

/// 解析扁平路径 (如 "/dir1/file1")
pub async fn resolve_flat_path(&self, path: &str) -> Result<u64, String> {
    let parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();
    
    // 根路径返回 POSIX 根 inode
    if parts.is_empty() {
        return Ok(POSIX_ROOT_INODE);
    }

    let mut current_inode = POSIX_ROOT_INODE;

    for part in parts.iter() {
        let shard_id = self.shard_strategy.calculate_shard(current_inode);
        let shard_store = self.get_shard_store(shard_id)?;
        
        let inode_info = shard_store
            .lookup(current_inode, part)
            .ok_or_else(|| format!("path component '{}' not found", part))?;

        current_inode = inode_info.inode;
    }

    Ok(current_inode)
}
```

## 7. 初始化与使用

### 7.1 初始化（类似 mkfs）

```bash
# 格式化 POSIX 根 + S3 默认桶
docker-compose exec filer-1 /app/powerfs-filer format --bucket default
```

### 7.2 启动服务

```bash
# 启动所有服务
docker-compose up -d

# 单独启动 Filer (同时运行 POSIX 和 S3 服务)
docker-compose up -d filer-1
```

### 7.3 验证

```bash
# 检查 Filer 服务状态
docker-compose logs filer-1

# FUSE 挂载后测试基本操作
docker-compose exec fuse-1 df -h /mnt/powerfs
docker-compose exec fuse-1 ls -la /mnt/powerfs
docker-compose exec fuse-1 mkdir -p /mnt/powerfs/test/dir
docker-compose exec fuse-1 touch /mnt/powerfs/test/file.txt
docker-compose exec fuse-1 cat /mnt/powerfs/test/file.txt
```

## 8. 测试计划

### 8.1 基础功能测试

| 测试项 | 命令 | 预期结果 |
|--------|------|----------|
| df 磁盘信息 | `df -h` | 显示挂载点信息 |
| ls 列出目录 | `ls -la` | 空目录或默认内容 |
| mkdir 创建目录 | `mkdir test` | 创建成功 |
| mkdir -p 递归创建 | `mkdir -p a/b/c` | 多级目录创建成功 |
| touch 创建文件 | `touch file.txt` | 创建成功 |
| cat 读取文件 | `cat file.txt` | 空文件 |
| echo 写入文件 | `echo "hello" > file.txt` | 写入成功 |
| cat 读取内容 | `cat file.txt` | 显示 "hello" |
| cp 复制文件 | `cp file.txt copy.txt` | 复制成功 |
| mv 重命名 | `mv file.txt new.txt` | 重命名成功 |
| rm 删除文件 | `rm new.txt` | 删除成功 |
| rmdir 删除目录 | `rmdir test` | 删除成功 |

### 8.2 压力测试

| 测试项 | 说明 |
|--------|------|
| 大量小文件创建 | 创建 10000 个小文件 |
| 大文件读写 | 写入/读取 1GB 文件 |
| 目录层级测试 | 深度 10 层嵌套目录 |
| 并发读写 | 多客户端并发操作 |

### 8.3 IO500 测试

使用标准 IO500 基准测试进行性能评估。

## 9. 风险与缓解

| 风险 | 等级 | 缓解措施 |
|------|------|----------|
| 数据迁移 | 低 | 双服务并行，无需迁移；仅新增根 inode |
| 兼容性 | 低 | `FilerMetaService`（桶模型）保留，S3 API 继续可用 |
| 性能退化 | 低 | 减少了桶根查找，理论上性能提升 |
| 功能缺失 | 低 | 已对照 Master 功能表，所有功能均已实现 |

## 10. 后续优化

1. **统计信息**：增加文件数、目录数、总容量等统计接口
2. **配额管理**：支持目录级别配额限制
3. **ACL 扩展**：支持 POSIX ACL
4. **快照功能**：基于 Raft 快照实现目录快照
5. **异步 API**：增加批量操作接口
