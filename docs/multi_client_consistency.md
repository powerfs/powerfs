# 多客户端写冲突与一致性分析

## 一、冲突场景分类

### 1. 元数据冲突

#### 1.1 同名文件并发创建

**场景**：两个客户端同时在同一目录下创建同名文件 `file.txt`

```
Client A                    Client B
     |                          |
     v                          v
  lookup(file.txt)         lookup(file.txt)
     |                          |
   not found                  not found
     |                          |
     v                          v
  create(file.txt)         create(file.txt)
     |                          |
     v                          v
  write(data1)             write(data2)
```

**当前行为**：OR-Set 模型会保留两份条目，分别记录为 `(name="file.txt", client_id=A, seq=1)` 和 `(name="file.txt", client_id=B, seq=1)`，永不覆盖。

**问题**：POSIX 语义下用户只能看到一个文件，另一个被隐藏在 `.conflicts/` 目录中。

#### 1.2 并发删除 + 修改

**场景**：Client A 删除文件，Client B 同时修改该文件

```
Client A                    Client B
     |                          |
     v                          v
  unlink(file.txt)         write(file.txt, offset=0)
     |                          |
     v                          v
  flush_metadata()         flush_metadata()
```

**问题**：如果删除先到达 Master，修改操作可能失败；反之，文件可能被修改后又被删除，产生不一致状态。

#### 1.3 并发 Rename

**场景**：两个客户端同时将同一文件重命名为不同名称

```
Client A                    Client B
     |                          |
     v                          v
  rename(file.txt, a.txt)  rename(file.txt, b.txt)
```

**问题**：最终文件可能只存在一个名称，另一个 rename 操作被覆盖。

---

### 2. 数据冲突

#### 2.1 同一 Offset 并发写入

**场景**：两个客户端同时写入同一文件的相同偏移量

```
Client A                    Client B
     |                          |
     v                          v
  write(offset=0, data=AAAA)  write(offset=0, data=BBBB)
     |                          |
     v                          v
  flush_dirty_chunks()      flush_dirty_chunks()
```

**当前行为**：last-writer-wins，后到达的数据覆盖先到达的数据，导致数据丢失。

#### 2.2 Append 并发导致空洞

**场景**：两个客户端同时 append 写入同一文件

```
Client A                    Client B
     |                          |
     v                          v
  read_size() -> 1000       read_size() -> 1000
     |                          |
     v                          v
  write(offset=1000, data1)  write(offset=1000, data2)
```

**问题**：两个客户端都认为文件大小是 1000，都从 offset=1000 开始写入，导致后写入的数据覆盖先写入的数据。

#### 2.3 Stripe 文件并发写

**场景**：大文件使用 Stripe 模式，两个客户端并发写入不同 chunk

```
Client A                    Client B
     |                          |
     v                          v
  write(chunk=0, volume=0)  write(chunk=1, volume=1)
     |                          |
     v                          v
  flush_metadata()         flush_metadata()
```

**问题**：元数据中 chunk 与 volume 的映射可能不一致，导致数据无法正确读取。

---

### 3. 元数据-数据一致性

#### 3.1 本地缓存不可见性

**场景**：Client A 修改文件后，修改尚未 flush 到 Master

```
Client A                    Client B
     |                          |
     v                          v
  write(data)              lookup(file.txt)
  (cached locally)         (read from Master)
     |                          |
     v                          v
  flush()                  read(file.txt)
                          (gets old data)
```

**问题**：Client B 读取到的是旧数据，违反 read-after-write 一致性。

#### 3.2 Flat → Stripe 提升未同步

**场景**：Client A 将文件从 Flat 模式提升为 Stripe 模式

```
Client A                    Client B
     |                          |
     v                          v
  write(> 64MB)            read(file.txt)
  → promote to Stripe      (expects Flat layout)
     |                          |
     v                          v
  flush_layout()           read_chunk(0)
                          (layout mismatch)
```

**问题**：Client B 可能使用旧的 Flat 布局信息读取数据，导致读取失败或数据错误。

---

## 二、当前系统保护机制

### 1. OR-Set 最终一致性

| 特性 | 机制 | 效果 |
|------|------|------|
| 条目唯一标识 | `(name, client_id, seq)` | 永不覆盖，保留所有版本 |
| VectorClock | 因果顺序判定 | 确定更新顺序，处理并发 |
| `.conflicts/` 目录 | POSIX 投影层展示 | 用户可查看和手动解决冲突 |

**代码位置**：
- [powerfs-orset/src/lib.rs](file:///home/portion/powerfs/powerfs-orset/src/lib.rs) - OR-Set 核心数据结构
- [powerfs-fuse-enterprise/src/posix_projection.rs](file:///home/portion/powerfs/powerfs-fuse-enterprise/src/posix_projection.rs) - POSIX 投影层，支持冲突检测和展示

### 2. Master Lease 机制

**已实现**：Master 端支持文件级租约，提供 `acquire_lease`、`release_lease`、`renew_lease` API

**代码位置**：
- [powerfs-master/src/lock_manager/mod.rs](file:///home/portion/powerfs/powerfs-master/src/lock_manager/mod.rs) - LockManager 接口
- [powerfs-master/src/lock_manager/raft_lease_lock.rs](file:///home/portion/powerfs/powerfs-master/src/lock_manager/raft_lease_lock.rs) - Raft 租约实现
- [powerfs-fuse-core/src/client.rs](file:///home/portion/powerfs/powerfs-fuse-core/src/client.rs) - FUSE 客户端 Lease API

**当前问题**：FUSE 客户端尚未在写入路径中集成租约获取逻辑。

---

## 三、一致性方案对比

### 方案一：文件级租约（Lease）

**原理**：客户端写文件前向 Master 获取独占写租约，租约到期自动释放

```
Client A                    Client B
     |                          |
     v                          v
  acquire_lease(file.txt)  acquire_lease(file.txt)
     |                          |
   success                   blocked/wait/retry
     |                          |
     v                          v
  write(data)               (等待租约释放)
  release_lease()              |
     |                          v
     v                     acquire_lease() -> success
                         write(data)
```

**优势**：
- 简单直接，避免所有并发写冲突
- 已有 Master 端实现，只需 FUSE 客户端集成
- 租约到期自动释放，不怕客户端崩溃

**劣势**：
- 降低并发度，同一文件只能有一个写者
- 需要维护租约心跳，增加网络开销
- 可能出现租约饿死（某个客户端长期持有）

**适用场景**：
- 单文件写多读少场景
- 需要强一致性保证的业务

---

### 方案二：版本号乐观并发控制（Version CAS）

**原理**：Entry 增加 `version` 字段，更新时校验版本，版本不匹配则重试

```rust
pub struct DirEntry {
    pub id: EntryId,
    pub inode: u64,
    pub size: u64,
    pub mtime: u64,
    pub version: u64,  // 新增版本号
    // ...
}
```

**更新流程**：

```
Client A                    Client B
     |                          |
     v                          v
  get_entry(version=3)     get_entry(version=3)
     |                          |
     v                          v
  modify(data)             modify(data)
  put_entry(version=3→4)   put_entry(version=3→4)
     |                          |
   success                   conflict! retry
     |                          |
     v                          v
  (更新成功)               get_entry(version=4)
                          put_entry(version=4→5)
```

**优势**：
- 高并发，无锁等待
- 版本冲突时可自动重试
- 适合多读多写场景

**劣势**：
- 需要重试机制，增加代码复杂度
- 冲突频繁时性能下降
- 需要处理无限重试问题

**适用场景**：
- 高并发读写场景
- 元数据更新频繁但冲突概率低的场景

---

### 方案三：写时复制（Copy-on-Write）

**原理**：写操作不覆盖原有数据，而是创建新副本，通过指针切换实现原子更新

```
┌─────────────────────────────────────────────┐
│  Entry: inode=100, data_ptr=0x1000          │
└─────────────────────────────────────────────┘
                         │
                         ▼
              ┌─────────────────┐
              │ Data at 0x1000  │
              │ "original data" │
              └─────────────────┘

Client A writes:
                         │
                         ▼
              ┌─────────────────┐    ┌─────────────────┐
              │ Data at 0x1000  │    │ Data at 0x2000  │
              │ "original data" │    │ "new data"      │
              └─────────────────┘    └─────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────┐
│  Entry: inode=100, data_ptr=0x2000          │  ← 原子更新指针
└─────────────────────────────────────────────┘
```

**优势**：
- 无锁并发读，读操作不受写影响
- 数据版本可回溯，支持快照
- 适合读多写少场景

**劣势**：
- 写操作开销大（复制数据）
- 需要垃圾回收旧版本
- 内存占用增加

**适用场景**：
- 读多写少场景
- 需要数据快照/版本回溯的场景

---

## 四、推荐实施路径

### 第一阶段：文件级租约（短期）

**目标**：快速解决最严重的并发写冲突

**实施步骤**：

1. **FUSE 客户端集成租约**：在 `fuser_fs.rs` 的 `open`/`create` 路径中调用 `acquire_lease`，在 `release` 路径中调用 `release_lease`

2. **租约心跳机制**：后台线程定期调用 `renew_lease`，确保租约不失效

3. **租约过期处理**：客户端检测到租约过期时，重新获取租约并重试写入

**代码改动**：

```rust
// fuser_fs.rs - open/create 时获取租约
fn open(&mut self, _req: &Request<'_>, inode: u64, flags: u32) -> Result<ReplyOpen> {
    let path = self.get_path(inode);
    let (lease_id, epoch) = self.client.acquire_lease(&path, &self.client_id, 60000)?;
    // 存储 lease_id 到 inode 上下文
    self.lease_cache.insert(inode, lease_id);
    Ok(ReplyOpen { fh: inode, .. })
}

// fuser_fs.rs - release 时释放租约
fn release(&mut self, _req: &Request<'_>, _inode: u64, _fh: u64) -> Result<ReplyEmpty> {
    if let Some(lease_id) = self.lease_cache.remove(&_inode) {
        let _ = self.client.release_lease(&lease_id);
    }
    Ok(ReplyEmpty)
}
```

---

### 第二阶段：版本号乐观并发控制（中期）

**目标**：在保证一致性的同时提升并发度

**实施步骤**：

1. **Entry 添加 version 字段**：在 OR-Set 的 `DirEntry` 结构中添加 `version: u64`

2. **Master 端实现版本校验**：更新元数据时校验版本，版本不匹配返回错误

3. **FUSE 客户端实现重试逻辑**：捕获版本冲突错误，重新获取最新数据并重试

---

### 第三阶段：写时复制（长期）

**目标**：支持数据快照和高并发读

**实施步骤**：

1. **Chunk 存储层支持 COW**：数据写入时创建新 chunk，不覆盖旧 chunk

2. **元数据引用计数**：跟踪 chunk 引用，无引用时回收

3. **快照功能**：基于 COW 实现文件快照

---

## 五、冲突检测与处理流程

```
┌─────────────────────────────────────────────────────────────┐
│                     FUSE 客户端                              │
├─────────────────────────────────────────────────────────────┤
│  1. 写操作                                                   │
│     └─ acquire_lease(path)                                  │
│         ├─ 成功 → 执行写入                                   │
│         └─ 失败 → 返回 EBUSY 或等待重试                       │
│                                                              │
│  2. Flush 到 Master                                          │
│     └─ put_entry(entry, expected_version)                    │
│         ├─ version 匹配 → 更新成功                           │
│         └─ version 不匹配 → 获取最新版本重试                   │
│                                                              │
│  3. 冲突检测（POSIX 投影层）                                  │
│     └─ has_conflicts(orset)                                  │
│         ├─ 有冲突 → 显示 .conflicts/ 目录                    │
│         └─ 无冲突 → 正常展示                                  │
└─────────────────────────────────────────────────────────────┘
```

---

## 六、总结

| 方案 | 实现难度 | 并发度 | 一致性保证 | 推荐优先级 |
|------|----------|--------|------------|------------|
| 文件级租约 | 低 | 中 | 强一致性 | 第一阶段 |
| 版本号 CAS | 中 | 高 | 乐观并发 | 第二阶段 |
| 写时复制 | 高 | 高 | 快照一致性 | 第三阶段 |

**当前建议**：优先实施文件级租约机制，快速解决最严重的并发写冲突问题。该方案利用现有 Master 端 Lease 实现，只需在 FUSE 客户端集成，改动量最小，收益最大。