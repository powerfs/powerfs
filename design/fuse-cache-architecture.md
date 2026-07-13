# PowerFS FUSE 客户端架构设计（OR-Set 弱一致版）

> 版本：v2.0
> 状态：设计中
> 范围：FUSE 客户端 OR-Set CRDT 弱一致缓存 + 冲突合并 + 跨节点刷新
> 重大变更：从 POSIX 强一致 FS 重定位为弱一致分布式数据同步存储

---

## 一、产品定位与设计哲学

### 1.1 定位重定义

PowerFS 从"通用 POSIX 分布式文件系统"重定位为**弱一致分布式数据同步存储**。

| 维度 | 旧定位（POSIX FS） | 新定位（数据同步存储） |
|------|-------------------|---------------------|
| 一致性 | 全局线性一致 | 最终一致 + 冲突不丢失 |
| 文件名 | 唯一主键 | (name+client+seq) 唯一 |
| 并发写 | 排他（lease 保护） | 零阻塞，全部保留 |
| 冲突处理 | 报错/覆盖 | 分流/重命名/保留副本 |
| 广播失效 | 全局广播 | 增量 delta 同步 |
| 客户端扩容 | 广播风暴 | 无衰减 |
| 合并模式 | 无 | 自动/人工/AI 三级 |
| 适用场景 | 交互式协作 | AI 数据集/日志/边缘同步 |

### 1.2 核心设计哲学

1. **缺省弱一致**：默认最终一致，后期可扩展强一致模式（目录级配置）
2. **冲突是常态**：不试图阻止冲突，而是保留所有分支，智能合并
3. **零数据丢失**：所有并发冲突只分流/重命名/保留副本，绝不静默覆盖
4. **写零阻塞**：本地缓存直接返回成功，无跨机 RPC 等待
5. **无广播风暴**：仅增量 delta 同步，客户端无限扩容无性能衰减

### 1.3 核心能力承诺

- 所有冲突绝不静默丢数据
- 写操作零阻塞，本地即返回
- 无全局广播失效
- 支持人工/自动/AI 三级合并
- 支持文件属性/API 主动刷新收敛

---

## 二、整体架构

### 2.1 三层体系

```
┌──────────────────────────────────────────────────────────────┐
│                    FUSE 客户端层                               │
│                                                              │
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐ │
│  │ FUSE Handler    │  │ OR-Set 本地缓存  │  │ POSIX 投影层  │ │
│  │ (fuser_fs.rs)   │←→│ (长时效/无租约)  │←→│ (OR-Set→VFS) │ │
│  └─────────────────┘  └────────┬────────┘  └──────────────┘ │
│                                │ 异步 delta 同步              │
└────────────────────────────────┼─────────────────────────────┘
                                 ▼
┌──────────────────────────────────────────────────────────────┐
│                    Meta 集群层（RocksDB + Raft）               │
│                                                              │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────────┐  │
│  │ 全局 OR-Set  │  │ 冲突队列       │  │ 合并策略引擎        │  │
│  │ 存储         │  │ (持久化)      │  │ (Auto/Manual/AI)  │  │
│  └─────────────┘  └──────────────┘  └────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
                                 ▼
┌──────────────────────────────────────────────────────────────┐
│                    AI 调度层（未来扩展）                       │
│  Master AI 节点：智能内容合并、决策、分类、归档               │
└──────────────────────────────────────────────────────────────┘
```

### 2.2 模块划分

```
┌──────────────────────────────────────────────────────────────┐
│                    fuser_fs.rs（Handler 层）                   │
│  薄层：FUSE 回调入口、参数解析、reply 调度                     │
└─────────────┬──────────────────────────┬─────────────────────┘
              │                          │
              ▼                          ▼
┌──────────────────────┐   ┌──────────────────────────────────┐
│   MetadataManager    │   │          DataManager             │
│   （元数据管理层）    │   │          （数据管理层）           │
│                      │   │                                  │
│  ┌─ OR-Set 目录缓存 ─┐│   │  ┌─ ChunkMap 缓存（分片映射）──┐ │
│  ├─ POSIX 投影层     ┤│   │  ├─ ChunkData 缓存（分片数据）┤ │
│  ├─ 冲突标记管理     ┤│   │  ├─ Write Buffer（写缓冲）     ┤ │
│  ├─ 增量同步调度     ┤│   │  └─ Flush / 持久化调度         ┤ │
│  └─ 跨节点刷新       ┘│   │                                  │
│  ─────────────────── │   │  Volume 数据 RPC                │
│  Meta 元数据 RPC      │   │                                  │
└──────────┬───────────┘   └──────────────────┬───────────────┘
           │                                  │
           ▼                                  ▼
   Meta Raft KV 集群             Volume Server 集群
   （OR-Set + 冲突队列 + 合并）  （分片存储 + FID 寻址）
```

### 2.3 模块职责边界

| 模块 | 职责 | 不负责 |
|------|------|--------|
| **fuser_fs.rs** | FUSE 回调入口、POSIX 视图 reply、内核 VFS 交互 | OR-Set 逻辑、数据读写 |
| **MetadataManager** | OR-Set 目录缓存、POSIX 投影、冲突标记、增量同步、跨节点刷新、Meta RPC | chunk 数据、volume 交互 |
| **DataManager** | ChunkMap/Data 缓存、Write Buffer、Flush 调度、Volume RPC | 元数据缓存、目录列表 |

---

## 三、核心数据结构：目录 OR-Set + 冲突模型

### 3.1 目录 OR-Set（服务端持久化 + 客户端缓存同源）

```rust
/// 目录维度：OR-Set 保证新增/删除可交换、可合并、无丢失
struct DirORSet {
    /// 有效文件条目集合：(文件名, 客户端ID, 客户端序列号)
    entries: HashSet<DirEntry>,
    /// 删除墓碑：解决删除后并发新建复活问题
    tombstones: HashSet<EntryID>,
    /// 向量时钟：判定并发冲突/因果顺序
    vclock: VectorClock,
    /// 待合并冲突队列（持久化）
    conflicts: Vec<ConflictRecord>,
    /// 当前目录合并策略配置
    policy: MergePolicy,
    /// 合并模式：Auto / Manual / AI
    merge_mode: MergeMode,
}

/// 单文件条目（唯一标识由「name+client+seq」组成，彻底避免覆盖）
struct DirEntry {
    name: String,
    client_id: u64,
    seq: u64,
    inode_id: u64,
    mtime: u64,
    size: u64,
    hash: Hash256,
}

/// 条目唯一标识
type EntryID = (String, u64, u64);  // (name, client_id, seq)

/// 冲突记录
struct ConflictRecord {
    base: DirEntry,
    branches: Vec<DirEntry>,
    conflict_type: ConflictType,
    create_time: u64,
    resolved: bool,
}

/// 冲突类型枚举
enum ConflictType {
    CreateCreate,    // 并发新建同名
    WriteWrite,      // 并发修改同文件
    WriteUnlink,     // 一端写一端删
    DeleteCreate,    // 删除后并发新建
    RenameConflict,  // 并发 rename
}

/// 合并策略
enum MergePolicy {
    LwwTime,         // 最新 mtime 优先
    ContentHash,     // 内容哈希优先
    WeightBased,     // 权重优先
    KeepAll,         // 全部保留
    WritePriority,   // 写入优先（删除冲突）
    DeletePriority,  // 删除优先
    NewCreatePriority, // 新建优先
}

/// 合并模式
enum MergeMode {
    Auto,    // 自动按策略合并
    Manual,  // 人工确认
    AI,      // AI 智能合并（未来）
}
```

### 3.2 关键设计突破

> **传统存储**：文件名是唯一主键 → 必然覆盖冲突。
> **本方案**：文件名不唯一，`(name+client+seq)` 才唯一 → 并发全部保留，永不丢失。

### 3.3 向量时钟

```rust
struct VectorClock {
    /// client_id -> 该客户端已见的最大 seq
    counters: HashMap<u64, u64>,
}

impl VectorClock {
    /// 判定因果顺序：self < other 返回 Before，self > other 返回 After
    fn compare(&self, other: &Self) -> CausalOrder;

    /// 判定并发： neither before nor after
    fn is_concurrent(&self, other: &Self) -> bool;

    /// 合并：取各 client 的最大 seq
    fn merge(&self, other: &Self) -> Self;
}
```

---

## 四、FUSE POSIX 投影层（关键设计）

### 4.1 问题：OR-Set 与 VFS 的语义鸿沟

OR-Set 允许同名多份，但 FUSE 挂载后内核 VFS 期望 POSIX 语义（同名唯一）。需要**投影层**把 OR-Set 投影成 VFS 可接受的视图。

### 4.2 投影规则

```
OR-Set 真实存储                    FUSE 投影（VFS/应用看到）
─────────────                    ──────────────────────────
file1 (client1, seq1, 主版本) →   file1                 （可见）
file1 (client2, seq2, 冲突)   →   .conflicts/file1.client2.seq2  （隐藏）
file1 (client3, seq3, 冲突)   →   .conflicts/file1.client3.seq3  （隐藏）
```

**投影规则**：
1. **主版本选择**：按目录 MergePolicy 选出主版本，用原文件名
2. **冲突副本**：放入 `.conflicts/` 隐藏目录，命名格式 `{name}.{client_id}.{seq}`
3. **`.conflicts/` 目录**：自动创建，对 `ls` 默认隐藏（类似 lost+found）
4. **冲突状态查询**：通过 xattr `user.fs.conflict_count` 查询当前目录冲突数

### 4.3 投影层实现

```rust
impl MetadataManager {
    /// 将 OR-Set 投影为 POSIX 目录列表（readdir 用）
    fn project_dir_listing(&self, dir_ino: u64) -> Vec<VisibleEntry> {
        let orset = self.get_orset(dir_ino);
        let mut visible = Vec::new();

        // 1. 按文件名分组
        let mut groups: HashMap<String, Vec<&DirEntry>> = HashMap::new();
        for entry in &orset.entries {
            groups.entry(entry.name.clone()).or_default().push(entry);
        }

        // 2. 每组选主版本，其余进 .conflicts/
        for (name, entries) in &groups {
            if entries.len() == 1 {
                // 无冲突，直接显示
                visible.push(VisibleEntry {
                    name: name.clone(),
                    inode: entries[0].inode_id,
                    is_conflict: false,
                });
            } else {
                // 有冲突，按 policy 选主版本
                let primary = self.select_primary(entries, &orset.policy);
                visible.push(VisibleEntry {
                    name: name.clone(),
                    inode: primary.inode_id,
                    is_conflict: true,
                });
                // 冲突副本进 .conflicts/（通过专用 inode 暴露）
            }
        }

        visible
    }

    /// lookup 投影：按文件名查找主版本
    fn project_lookup(&self, dir_ino: u64, name: &str) -> Option<DirEntry> {
        let orset = self.get_orset(dir_ino);
        let candidates: Vec<&DirEntry> = orset.entries.iter()
            .filter(|e| e.name == name)
            .collect();

        if candidates.is_empty() {
            return None;
        }
        if candidates.len() == 1 {
            return Some(candidates[0].clone());
        }
        // 有冲突，返回主版本
        Some(self.select_primary(&candidates, &orset.policy).clone())
    }
}
```

### 4.4 `.conflicts/` 目录访问

- `ls /mnt/powerfs/dir/` — 默认不显示 `.conflicts/`
- `ls -a /mnt/powerfs/dir/` — 显示 `.conflicts/`
- `ls /mnt/powerfs/dir/.conflicts/` — 列出所有冲突副本
- 用户可手动处理冲突副本（删除/重命名/合并）

---

## 五、全部冲突场景处理规则

### 5.1 场景1：并发新建同名文件（Create vs Create）

**现象**：Client1、Client2 同时在 /dir 创建 file1

**系统标准行为**：
- 两端本地全部创建成功，无报错、无阻塞
- 服务端识别为「并发新建冲突」，进入冲突队列
- 不覆盖、不丢弃任何一份

**自动合并策略**（目录可配置）：
| 策略 | 行为 |
|------|------|
| LwwTime | 最新 mtime 保留原名，旧版本重命名为 file1.client2.timestamp |
| ContentHash | 内容一致则合并为单文件；内容不同保留差异副本 |
| WeightBased | 业务节点优先，边缘节点自动分流 |
| KeepAll | 全部分流，不自动取舍 |

**人工模式**：全部保留冲突标记，等待用户决策
**AI 合并**（未来）：识别文件类型，文本 diff 合并、二进制择优、数据集去重

### 5.2 场景2：并发修改同一已有文件（Write vs Write）

**现象**：file1 已存在，两端同时覆盖写/追加写

**标准行为**：
- 本地双写成功，异步同步服务端
- 服务端判定：同源 inode、并发修改 → 内容冲突
- 生成主版本 + 分支版本，不覆盖

**自动策略**：
| 策略 | 行为 |
|------|------|
| LwwTime | 新版原名、旧版分流 |
| DataIncrement | 变更更多字节为主版本 |
| StableBaseline | 哈希相似度高的作为基线 |

### 5.3 场景3：一端写、一端删（Write vs Unlink）

**现象**：Client1 修改 file1，Client2 删除 file1

**标准行为**：产生「更新-删除冲突」，不允许删除静默吞掉写入

**策略**：
| 策略 | 行为 | 适用 |
|------|------|------|
| WritePriority | 保留更新后文件，删除失效 | 科研/AI 数据默认 |
| DeletePriority | 目录显示已删除，写入变冲突副本 | 日志/临时文件 |
| KeepBoth | 更新版 + deleted 标记副本同时存在 | - |

### 5.4 场景4：并发删除同一文件（Unlink vs Unlink）

**行为**：OR-Set 天然合并墓碑，最终统一为删除状态，无冲突、无副本。无需人工/AI 干预。

### 5.5 场景5：删除后并发新建（Delete vs Create）

**现象**：A 删 file1，B 立刻新建 file1

**标准行为**：判定为「删除-新建冲突」，保留新建文件，标记删除墓碑冲突

**策略**：
| 策略 | 行为 |
|------|------|
| NewCreatePriority（默认） | 防止删除后业务重建数据丢失 |
| HistoryStable | 保留删除状态，新建分流为冲突文件 |

### 5.6 场景6：并发目录操作（mkdir/rmdir/rename）

| 场景 | 行为 |
|------|------|
| 并发 mkdir 同名目录 | 自动合并目录，子文件各自保留，无冲突丢失 |
| rename 与写冲突 | 原路径保留冲突副本，新路径正常生效 |
| 跨目录 rename 冲突 | 双目录分别收敛，冲突文件分流保留 |

---

## 六、三级合并模式

### 6.1 自动模式（高吞吐业务默认）

全部冲突后台自动按目录策略收敛，用户无感知。
- 适合：AI 数据集、日志、采集数据、边缘写入
- 策略由目录 MergePolicy 配置

### 6.2 人工确认模式（核心生产数据）

所有冲突不自动改名/删除，仅标记冲突状态。

**管理接口**（通过专用 API 或 xattr）：
- 保留 A、丢弃 B
- 保留 B、丢弃 A
- 全部保留
- 文本三路合并导出新文件

### 6.3 AI 智能合并模式（未来架构预留）

Master 节点接入 AI 决策引擎，针对冲突队列智能判断：

| 文件类型 | AI 策略 |
|---------|---------|
| 时序数据 | 按时间拼接合并 |
| 文本代码 | diff 智能合并 |
| 模型权重 | 择优保留最优版本 |
| 重复数据 | 自动去重合并 |
| 异常冲突 | 自动上报人工审核 |

AI 决策结果写入 Raft 元数据，全局统一收敛。

---

## 七、跨节点数据共享：主动刷新机制

弱一致架构下，大部分场景靠后台定时增量同步收敛；需要跨节点共享、强视图一致的场景，提供两种主动收敛方式。

### 7.1 机制1：特殊文件属性强制刷新（用户态无侵入）

扩展自定义 inode 属性（兼容 POSIX 扩展属性）：

| xattr | 含义 |
|-------|------|
| `user.fs.need_sync=1` | 设置后，下一次访问自动拉取服务端最新 OR-Set 全量快照 |
| `user.fs.sync_once=1` | 单次刷新，刷新后自动清零属性 |
| `user.fs.sync_force=1` | 强制合并本地缓存、覆盖脏视图 |

**使用场景**：跨节点共享配置、共享任务文件、共享标记文件、需要立刻可见对方写入的场景。

**执行逻辑**：
1. 客户端访问带强制同步属性的文件/目录
2. 暂停本地弱一致读取
3. 主动拉取服务端最新完整 OR-Set
4. 本地缓存全量合并、收敛
5. 返回最新全局视图

### 7.2 机制2：主动 API 刷新接口（程序可控）

提供两类接口，供业务/调度器/AI 框架主动调用：

| 接口 | 行为 | 适用 |
|------|------|------|
| 目录增量刷新 | 拉取最近变更 delta，快速收敛（轻量） | 任务运行中周期同步 |
| 目录全量刷新 | 丢弃本地脏缓存，对齐服务端权威视图（重度一致） | 任务切换、跨节点交接、快照加载 |

### 7.3 自动收敛兜底策略

| 触发条件 | 行为 |
|---------|------|
| 默认每 2s | 增量同步变更 delta |
| 默认每 30s | 全量对齐目录快照 |
| 客户端断连重连 | 强制清空本地缓存、全量同步 |

---

## 八、缓存整体一致性模型

### 8.1 三种业务模式

| 模式 | 一致性 | 适用 | 实现机制 |
|------|--------|------|---------|
| 普通业务（默认） | 弱一致 | 海量客户端高并发写入 | 本地长时效缓存 + 异步同步 + 冲突自动分流 |
| 跨节点共享 | 按需强一致 | 配置/任务/标记文件共享 | xattr / API 触发即时收敛 |
| 核心数据 | 人工/AI 管控 | 关键数据保护 | 关闭自动覆盖，冲突留存 |

### 8.2 同步机制总览

```
┌─────────────────────────────────────────────────┐
│ 客户端本地 OR-Set 缓存                          │
│  ├─ 写操作直接修改本地 OR-Set，立即返回成功     │
│  ├─ 异步 delta 推送到 Meta（默认 2s）           │
│  ├─ 后台增量拉取（默认 2s）                     │
│  └─ 全量对齐（默认 30s）                        │
└────────────────────┬────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────┐
│ Meta 集群 OR-Set 存储                           │
│  ├─ 接收客户端 delta，合并到全局 OR-Set         │
│  ├─ 冲突检测：vclock 判定并发                   │
│  ├─ 冲突入队列：按 MergePolicy/MergeMode 处理   │
│  └─ 增量 delta 推送到订阅客户端                 │
└─────────────────────────────────────────────────┘
```

---

## 九、模块详细设计

### 9.1 MetadataManager 接口

```rust
impl MetadataManager {
    // === 构造 ===
    pub fn new(client: Arc<SyncFuseClient>, client_id: u64) -> Self;

    // === 读路径（走本地 OR-Set 缓存，零 RPC） ===
    pub fn lookup(&self, dir_ino: u64, name: &str) -> Result<Option<DirEntry>, FsError>;
    pub fn list_dir(&self, dir_ino: u64) -> Result<Vec<VisibleEntry>, FsError>;
    pub fn get_entry_by_inode(&self, ino: u64) -> Result<Option<DirEntry>, FsError>;
    pub fn list_conflicts(&self, dir_ino: u64) -> Result<Vec<ConflictRecord>, FsError>;

    // === 写路径（本地立即成功，异步同步） ===
    pub fn create(&self, dir_ino: u64, name: &str, entry: NewEntryParams) -> Result<DirEntry, FsError>;
    pub fn mkdir(&self, dir_ino: u64, name: &str, mode: u32) -> Result<DirEntry, FsError>;
    pub fn unlink(&self, dir_ino: u64, name: &str) -> Result<(), FsError>;
    pub fn rmdir(&self, dir_ino: u64, name: &str) -> Result<(), FsError>;
    pub fn rename(&self, old_dir: u64, old_name: &str, new_dir: u64, new_name: &str) -> Result<(), FsError>;
    pub fn setattr(&self, ino: u64, params: SetAttrParams) -> Result<DirEntry, FsError>;

    // === 冲突管理 ===
    pub fn resolve_conflict(&self, conflict_id: &str, resolution: ConflictResolution) -> Result<(), FsError>;
    pub fn set_merge_policy(&self, dir_ino: u64, policy: MergePolicy) -> Result<(), FsError>;
    pub fn set_merge_mode(&self, dir_ino: u64, mode: MergeMode) -> Result<(), FsError>;

    // === 跨节点刷新 ===
    pub fn refresh_dir_incremental(&self, dir_ino: u64) -> Result<(), FsError>;
    pub fn refresh_dir_full(&self, dir_ino: u64) -> Result<(), FsError>;
    pub fn check_sync_xattr(&self, ino: u64) -> Result<bool, FsError>;

    // === 后台任务 ===
    pub fn start_background_tasks(&self, handle: tokio::runtime::Handle);
}
```

### 9.2 DataManager 接口

```rust
impl DataManager {
    // === 构造 ===
    pub fn new(client: Arc<SyncFuseClient>, chunk_size: u64, max_chunk_bytes: usize) -> Self;

    // === 读写 ===
    pub fn read(&self, ino: u64, offset: u64, size: usize) -> Result<Vec<u8>, FsError>;
    pub fn write(&self, ino: u64, offset: u64, data: &[u8]) -> Result<u64, FsError>;
    pub fn current_file_size(&self, ino: u64) -> u64;

    // === 持久化 ===
    pub fn flush(&self, ino: u64) -> Result<(), FsError>;
    pub fn fsync(&self, ino: u64) -> Result<(), FsError>;
    pub fn release_inode(&self, ino: u64) -> Result<(), FsError>;
    pub fn truncate(&self, ino: u64, new_size: u64) -> Result<(), FsError>;

    // === 预取 ===
    pub fn prefetch(&self, ino: u64, offset: u64, size: u64);
}
```

### 9.3 OR-Set 本地缓存

```rust
struct LocalORSetCache {
    /// dir_inode → DirORSet
    dirs: RwLock<HashMap<u64, DirORSet>>,
    /// inode → DirEntry（反向索引）
    inode_map: RwLock<HashMap<u64, DirEntry>>,
    /// 本地未同步的 delta 队列
    pending_deltas: RwLock<VecDeque<DeltaOp>>,
    /// 客户端序列号生成器
    seq_counter: AtomicU64,
    /// 客户端 ID
    client_id: u64,
}

/// Delta 操作（增量同步单元）
enum DeltaOp {
    Add(DirEntry),
    Remove(EntryID),
    Rename { old: EntryID, new: DirEntry },
    SetAttr { inode: u64, params: SetAttrParams },
}
```

### 9.4 POSIX 投影层

```rust
struct PosixProjection {
    /// 缓存的投影视图（dir_inode → 可见条目列表）
    view_cache: RwLock<HashMap<u64, Vec<VisibleEntry>>>,
    /// .conflicts/ 虚拟目录的 inode 分配
    conflict_dir_inodes: RwLock<HashMap<u64, u64>>,
}

struct VisibleEntry {
    name: String,
    inode: u64,
    file_type: FileType,
    is_conflict: bool,  // 是否有冲突副本
}
```

---

## 十、Handler 层改造（fuser_fs.rs）

### 10.1 结构体

```rust
struct PowerFsFuserFs {
    meta: Arc<MetadataManager>,
    data: Arc<DataManager>,
    notifier: Arc<Mutex<Option<fuser::Notifier>>>,
    client_id: u64,
}
```

### 10.2 各 Handler 改造要点

| Handler | 改造内容 | 关键调用 |
|---------|---------|---------|
| `lookup` | 走 POSIX 投影，返回主版本 | `meta.lookup()` |
| `getattr` | 从 OR-Set 取条目属性；size 取 max(meta, data) | `meta.get_entry_by_inode()`, `data.current_file_size()` |
| `setattr` | 本地 OR-Set 更新，异步同步；检查 sync xattr | `meta.setattr()`, `meta.check_sync_xattr()` |
| `readdir` | 走 POSIX 投影，返回可见条目 + . + .. | `meta.list_dir()` |
| `mkdir` | 本地 OR-Set 新增目录条目，立即返回 | `meta.mkdir()` |
| `create` | 本地 OR-Set 新增文件条目，立即返回 | `meta.create()` |
| `open` | 检查 sync xattr，需要时触发刷新 | `meta.check_sync_xattr()` |
| `read` | 从 data 层读 | `data.read()` |
| `write` | 本地写 + 更新 OR-Set size，立即返回 | `data.write()` |
| `flush` | data.flush() + 异步 delta 同步 | `data.flush()` |
| `release` | data.flush() + data.release_inode() | `data.flush()`, `data.release_inode()` |
| `unlink` | 本地 OR-Set 加墓碑，立即返回 | `meta.unlink()` |
| `rmdir` | 本地 OR-Set 加墓碑，立即返回 | `meta.rmdir()` |
| `rename` | 本地 OR-Set rename，立即返回 | `meta.rename()` |
| `getxattr` | 支持 user.fs.* 同步属性查询 | `meta.check_sync_xattr()` |
| `setxattr` | 支持 user.fs.need_sync 等设置触发刷新 | `meta.refresh_dir_full()` |

### 10.3 `.conflicts/` 目录处理

- `readdir` 遇到 `.conflicts/` 名称时正常返回（ls -a 可见）
- `lookup(".conflicts")` 返回虚拟目录 inode
- `readdir(.conflicts/)` 返回所有冲突副本
- 用户可对冲突副本执行 unlink/rename 操作解决冲突

---

## 十一、Master 端改造

### 11.1 存储模型重构

从 `path → Entry` 单一 KV 改为 OR-Set 模型：

| 旧模型 | 新模型 |
|--------|--------|
| `path:{full_path}` → Entry | `dir:{dir_inode}:entries` → OR-Set 序列化 |
| `inode:{ino}` → path | `dir:{dir_inode}:tombstones` → 墓碑集合 |
| - | `dir:{dir_inode}:conflicts` → 冲突队列 |
| - | `dir:{dir_inode}:policy` → 合并策略配置 |
| `inode:{ino}` → Entry | 保留，作为 inode → entry 查询 |

### 11.2 API 变更

| API | 变更 |
|-----|------|
| `Create` | 改为 OR-Set Add 操作，生成 (name+client+seq) |
| `Delete` | 改为 OR-Set Remove + 墓碑 |
| `Rename` | 改为 Remove + Add 原子操作 |
| `ListEntries` | 返回 OR-Set 投影后的可见列表 |
| `Subscribe` | 改为 delta 推送（非全量广播） |
| 新增 `GetConflicts` | 返回目录冲突队列 |
| 新增 `ResolveConflict` | 人工/AI 解决冲突 |
| 新增 `SetMergePolicy` | 配置目录合并策略 |
| 新增 `RefreshFull` | 全量 OR-Set 快照拉取 |
| 新增 `RefreshDelta` | 增量 delta 拉取 |

### 11.3 冲突检测引擎

```
Meta 收到客户端 delta:
  1. 合并到全局 OR-Set
  2. 用 vclock 判定是否并发
  3. 若并发且涉及同名/同 inode → 生成 ConflictRecord
  4. 按 MergeMode 处理:
     - Auto: 按 MergePolicy 自动合并
     - Manual: 入队列等待人工
     - AI: 入队列等待 AI 决策
  5. 推送 delta 给其他订阅客户端
```

---

## 十二、实施计划（5 Phase 细化）

### Phase 1：OR-Set 核心 + 本地缓存 + 异步同步

**目标**：建立 OR-Set 弱一致基础架构，客户端本地读写零阻塞。

**FUSE 端任务**：

| # | 任务 | 说明 |
|---|------|------|
| 1.1 | 创建 `metadata_manager.rs` | OR-Set 本地缓存、Delta 队列、client_id/seq 生成 |
| 1.2 | 创建 `data_manager.rs` | 封装 chunk_cache + write_buffer + dirty_chunks |
| 1.3 | OR-Set 数据结构实现 | DirORSet / DirEntry / EntryID / VectorClock |
| 1.4 | POSIX 投影层实现 | OR-Set → 可见列表，主版本选择，.conflicts/ 虚拟目录 |
| 1.5 | 写操作本地化 | create/mkdir/unlink/rmdir/rename/setattr 本地 OR-Set 即返回 |
| 1.6 | 读操作走投影 | lookup/readdir/getattr 走本地 OR-Set 投影 |
| 1.7 | Delta 同步后台任务 | 2s 增量推送 + 30s 全量对齐 |
| 1.8 | PowerFsFuserFs 结构体重构 | meta + data 双模块 |
| 1.9 | 编译通过 + 基本测试 | - |

**Master 端任务**：

| # | 任务 | 说明 |
|---|------|------|
| M1.1 | OR-Set 存储模型 | RocksDB schema 重构 |
| M1.2 | Delta 接收与合并 | 接收客户端 delta，合并到全局 OR-Set |
| M1.3 | Delta 推送 | 替换全量广播为增量 delta 推送 |
| M1.4 | VectorClock 实现 | 因果顺序判定 |

**验收标准**：
- 客户端本地读写零 RPC（写操作立即返回）
- 多客户端并发写不丢数据（全部保留为不同 EntryID）
- 异步 delta 同步后视图收敛

---

### Phase 2：冲突检测 + 自动合并策略

**目标**：自动识别并发冲突，按策略合并。

| # | 任务 | 说明 |
|---|------|------|
| 2.1 | 冲突检测引擎 | vclock 判定并发，生成 ConflictRecord |
| 2.2 | 五类冲突场景处理 | CreateCreate/WriteWrite/WriteUnlink/DeleteCreate/Rename |
| 2.3 | MergePolicy 实现 | LwwTime/ContentHash/WeightBased/KeepAll 等 |
| 2.4 | 主版本选择算法 | 投影层调用，按 policy 选主 |
| 2.5 | .conflicts/ 目录完整实现 | 冲突副本可见、可访问、可处理 |
| 2.6 | 编译通过 + 冲突场景测试 | - |

**Master 端任务**：

| # | 任务 | 说明 |
|---|------|------|
| M2.1 | 冲突队列持久化 | conflicts 持久化到 RocksDB |
| M2.2 | 自动合并引擎 | 按 MergeMode=Auto 自动处理 |
| M2.3 | MergePolicy 配置接口 | 目录级策略配置 |

**验收标准**：
- 并发新建同名文件：全部保留，主版本+冲突副本
- 并发修改：主版本+分支版本
- 写/删冲突：按策略保留
- .conflicts/ 可访问

---

### Phase 3：跨节点刷新 + 人工合并接口

**目标**：支持按需强一致视图 + 人工冲突解决。

| # | 任务 | 说明 |
|---|------|------|
| 3.1 | xattr 同步属性支持 | user.fs.need_sync/sync_once/sync_force |
| 3.2 | 增量刷新接口 | refresh_dir_incremental |
| 3.3 | 全量刷新接口 | refresh_dir_full，丢弃本地脏缓存 |
| 3.4 | 人工合并接口 | resolve_conflict（保留A/保留B/全部保留/三路合并） |
| 3.5 | 断连重连强制刷新 | 重连清空本地缓存，全量同步 |
| 3.6 | 编译通过 + 测试 | - |

**Master 端任务**：

| # | 任务 | 说明 |
|---|------|------|
| M3.1 | 全量 OR-Set 快照接口 | 全量拉取 |
| M3.2 | 增量 delta 接口 | 按版本号拉取 delta |
| M3.3 | 冲突解决接口 | 处理人工 resolution |

**验收标准**：
- 设置 user.fs.need_sync=1 后访问立即拉取最新
- 跨节点刷新后视图一致
- 人工可解决冲突

---

### Phase 4：优化、调优、监控

**目标**：性能调优，可观测性，边缘场景完善。

| # | 任务 | 说明 |
|---|------|------|
| 4.1 | 同步频率可配置 | 2s/30s 可调 |
| 4.2 | 冲突队列监控 | 冲突数量、解决率指标 |
| 4.3 | 同步延迟监控 | delta 推送/拉取延迟 |
| 4.4 | 内存占用优化 | OR-Set 缓存 LRU |
| 4.5 | 大目录优化 | OR-Set 分页加载 |
| 4.6 | 压力测试 | 大量客户端、高并发写 |

---

### Phase 5：AI 智能合并（未来扩展）

**目标**：Master 接入 AI 决策引擎。

| # | 任务 | 说明 |
|---|------|------|
| 5.1 | AI 决策引擎接入 | Master AI 节点 |
| 5.2 | 文件类型识别 | 文本/日志/二进制/数据集/模型权重 |
| 5.3 | 智能合并策略 | diff 合并/时间拼接/择优/去重 |
| 5.4 | 异常冲突上报 | AI 无法处理的转人工 |
| 5.5 | AI 决策持久化 | 写入 Raft 元数据全局收敛 |

---

## 十三、与现有代码的映射

### 现有文件 → 新模块映射

| 现有文件/结构 | 去向 | 说明 |
|-------------|------|------|
| [cache.rs](file:///home/portion/powerfs/powerfs-fuse/src/cache.rs) `MetadataCache` | `metadata_manager.rs` | 重构为 OR-Set 模型 |
| [cache.rs](file:///home/portion/powerfs/powerfs-fuse/src/cache.rs) `ChunkCache` | `data_manager.rs` | 增加 LRU 字节限制 |
| [fuser_fs.rs](file:///home/portion/powerfs/powerfs-fuse/src/fuser_fs.rs) `WriteBuffer` | `data_manager.rs` | 移入 |
| [fuser_fs.rs](file:///home/portion/powerfs/powerfs-fuse/src/fuser_fs.rs) `LeaseInfo` | **废弃** | 弱一致无需写保护租约 |
| [fuser_fs.rs](file:///home/portion/powerfs/powerfs-fuse/src/fuser_fs.rs) `dirty_chunks` | `data_manager.rs` | 移入 |
| [fuser_fs.rs](file:///home/portion/powerfs/powerfs-fuse/src/fuser_fs.rs) `metadata_subscription_loop` | `metadata_manager.rs` | 改为 delta 订阅 |
| [fuser_fs.rs](file:///home/portion/powerfs/powerfs-fuse/src/fuser_fs.rs) `handle_metadata_notification` | `metadata_manager.rs` | 改为 delta 合并 |
| [fuser_fs.rs](file:///home/portion/powerfs/powerfs-fuse/src/fuser_fs.rs) `lease_renewal_loop` | **废弃** | 无租约 |
| [directory_tree.rs](file:///home/portion/powerfs/powerfs-master/src/directory_tree.rs) | 重构 | path KV → OR-Set 存储 |
| [directory_tree.rs](file:///home/portion/powerfs/powerfs-master/src/directory_tree.rs) `Lease` | **废弃** | 无写保护租约 |

### 废弃的概念

- 写保护租约（acquire_lease/release_lease/renew_lease）
- 全局广播失效
- TTL=0 + Notifier 强一致模式（改为弱一致，Notifier 仍用于内核缓存失效）
- 同步提交 + 错误回滚（改为本地即成功，异步同步）

### 保留的概念

- Notifier API（用于内核 VFS dentry 失效，写操作后仍需通知内核）
- ChunkCache / WriteBuffer / dirty_chunks（数据层保留）
- FUSE TTL=0（VFS 不缓存，每次穿透到用户态）
- Master epoch 机制（用于断连重连检测）
- Volume Server 数据存储（不变）

---

## 十四、风险与缓解

| 风险 | 影响 | 概率 | 缓解措施 |
|------|------|------|---------|
| POSIX 工具行为异常 | 用户体验 | 高 | POSIX 投影层保证主版本可见；冲突副本隐藏在 .conflicts/ |
| 冲突副本积累过多 | 磁盘占用 | 中 | 自动合并策略 + .conflicts/ 清理工具 + 配额限制 |
| 弱一致窗口内读旧数据 | 数据新鲜度 | 高 | xattr/API 主动刷新；文档明确弱一致语义 |
| OR-Set 合并复杂度 | 性能 | 中 | 墓碑 GC 机制；大目录分页加载 |
| Master 重构引入 bug | 稳定性 | 高 | 分阶段迁移；旧 path KV 保留兼容期 |
| 客户端断连数据丢失 | 数据丢失 | 低 | 本地 delta 持久化；重连后重放 |

---

## 十五、测试策略

### 单元测试

- OR-Set 增删合并、vclock 因果判定
- POSIX 投影层主版本选择
- 五类冲突场景检测
- MergePolicy 各策略

### 集成测试

- 多客户端并发写同名文件，验证全部保留
- 冲突副本在 .conflicts/ 可访问
- xattr 触发刷新后视图一致
- 断连重连后全量同步

### 端到端测试

- 大量客户端高并发写
- 弱一致窗口验证
- 跨节点刷新一致性
- 冲突解决流程

---

## 附录：与旧方案（强一致租约）的关键差异

| 维度 | 旧方案（v1.0 强一致） | 新方案（v2.0 OR-Set 弱一致） |
|------|---------------------|---------------------------|
| 一致性 | 租约线性一致 | 最终一致 + 冲突保留 |
| 文件名 | 唯一 | (name+client+seq) 唯一 |
| 写操作 | 强制走 Master | 本地即成功，异步同步 |
| 冲突处理 | 报错/覆盖 | 分流/保留/合并 |
| 失效机制 | 全局广播 | 增量 delta |
| 租约 | 读租约+写保护租约 | 无租约 |
| POSIX 兼容 | 完全兼容 | 投影层兼容（主版本可见） |
| 客户端扩容 | 广播风暴 | 无衰减 |
| 合并模式 | 无 | 自动/人工/AI |
