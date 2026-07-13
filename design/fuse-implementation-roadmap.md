# PowerFS FUSE 实施路线图（OR-Set 弱一致架构）

> 版本：v1.1
> 状态：待执行
> 关联设计：[fuse-cache-architecture.md](fuse-cache-architecture.md) v2.0
> 创建时间：2026-07-13
> 更新时间：2026-07-13（v1.1 调整：拆分 Phase 1 为 1A/1B，加入每步质量门控，优先单客户端功能正确性）

---

## 0. 关键决策记录

| # | 决策点 | 选择 | 说明 |
|---|--------|------|------|
| 1 | Inode 分配策略 | **方案 B：Master 启动时预分配** | 按客户端 ID 分配 inode 范围段，客户端本地从区间内分配，写操作零 RPC |
| 2 | Delta 同步方向 | **方案 B：Master 推 + 客户端推** | 客户端推自己的 delta；Master 推其他客户端 delta；带服务端负载平衡（聚合/批量/按需订阅） |
| 3 | `.conflicts/` inode 分配 | **预分配 + 独立段** | 每个客户端预留一段作为虚拟目录 inode，避免与真实文件冲突 |
| 4 | RocksDB schema 兼容期 | **缺省 1 个月** | 旧 path KV 保留 1 个月迁移期，可通过配置调整 |
| 5 | Master 端 OR-Set 改造时机 | **Phase 1B 同步进行** | Phase 1A 先保证单客户端功能正确（不依赖 Master 改造），Phase 1B 再做 Master OR-Set + 多客户端同步 |
| 6 | 实施原则 | **每步质量门控 + 优先单客户端功能** | 每个任务完成后必须通过 cargo check/fmt/clippy/test；先保证单客户端基本操作正确，再扩展多客户端 |

---

## 1. 总览

### 1.1 Phase 划分（调整后）

| Phase | 目标 | 预估工期 | 依赖 | 里程碑 |
|-------|------|---------|------|--------|
| **Phase 1A** | **单客户端功能正确**：本地 OR-Set + POSIX 投影 + DataManager + handler 重构 | 2-2.5 周 | 无 | **单客户端 ls/cp/rm/mv/cat/mkdir/readdir 全部通过** |
| Phase 1B | 多客户端同步：Master OR-Set + Delta 推送/拉取 + inode 预分配 | 2-3 周 | Phase 1A | 多客户端并发写不丢数据 |
| Phase 2 | 冲突检测 + 自动合并策略 + `.conflicts/` 完整实现 | 2-3 周 | Phase 1B | 五类冲突自动处理 |
| Phase 3 | 跨节点刷新 + 人工合并接口 + 断连重连 | 2 周 | Phase 2 | xattr/API 触发刷新 |
| Phase 4 | 优化调优 + 监控 + 压力测试 | 2 周 | Phase 3 | 性能达标 |
| Phase 5 | AI 智能合并（未来） | - | Phase 4 | - |

### 1.2 质量门控（每个任务必须执行）

每个任务完成后，必须依次执行以下质量检查，全部通过才能进入下一个任务：

```bash
# 1. 编译检查
cargo check --all 2>&1 | tail -20

# 2. 格式化检查
cargo fmt --all
cargo fmt --check --all 2>&1 | tail -20

# 3. Clippy 检查（-D warnings，警告视为错误）
cargo clippy --all -- -D warnings 2>&1 | tail -30

# 4. 单元测试
cargo test --lib 2>&1 | tail -30

# 5. 集成测试（如果该任务涉及 handler 改动）
cargo test --test fuse_basic_test 2>&1 | tail -30
```

**质量门控规则**：
- 任何一步失败，必须修复后才能进入下一个任务
- 每个任务的代码提交前必须跑完全部 5 步
- 测试失败不允许跳过，必须修复或记录为已知问题
- clippy 警告不允许忽略（除非有明确注释说明原因）

### 1.3 Phase 1A 验收标准（最高优先级）

**Phase 1A 完成后，单客户端必须能通过以下所有基本操作测试**：

| 操作 | 命令 | 验证点 |
|------|------|--------|
| 创建目录 | `mkdir /mnt/powerfs/testdir` | 目录存在，`ls` 可见 |
| 创建文件 | `echo "hello" > /mnt/powerfs/test.txt` | 文件存在，内容正确 |
| 读取文件 | `cat /mnt/powerfs/test.txt` | 输出 "hello" |
| 列目录 | `ls /mnt/powerfs/` | 显示 test.txt 和 testdir |
| 列目录含隐藏 | `ls -a /mnt/powerfs/` | 显示 . 和 .. |
| 复制文件 | `cp /mnt/powerfs/test.txt /mnt/powerfs/copy.txt` | copy.txt 内容与 test.txt 相同 |
| 移动/重命名 | `mv /mnt/powerfs/test.txt /mnt/powerfs/renamed.txt` | renamed.txt 存在，test.txt 不存在 |
| 删除文件 | `rm /mnt/powerfs/renamed.txt` | 文件不存在 |
| 删除目录 | `rmdir /mnt/powerfs/testdir` | 目录不存在 |
| 文件属性 | `stat /mnt/powerfs/copy.txt` | size/mtime/mode 正确 |
| 修改权限 | `chmod 0600 /mnt/powerfs/copy.txt` | 权限变更生效 |
| 大文件写入 | `dd if=/dev/zero of=/mnt/powerfs/big.bin bs=1M count=10` | size = 10MB |
| 文件截断 | `truncate -s 1M /mnt/powerfs/big.bin` | size = 1MB |
| find 命令 | `find /mnt/powerfs/ -name "*.txt"` | 正确列出 txt 文件 |
| grep 命令 | `grep "hello" /mnt/powerfs/copy.txt` | 正确匹配 |

**验收方式**：通过 `tests/fuse_basic_test.rs` 扩展后的测试套件全部通过。

---

## 2. Phase 1A：单客户端功能正确（最高优先级）

### 2.1 设计原则

Phase 1A **不依赖 Master 改造**，采用以下策略：

1. **启动时全量拉取**：客户端启动时通过现有 `list_entries` API 从 Master 拉取全量目录数据，填充本地 OR-Set
2. **写操作本地成功 + 简单同步**：写操作修改本地 OR-Set 立即返回，flush 时通过现有 `create_entry`/`update_entry`/`delete_entry` API 同步到 Master（兼容旧 path KV）
3. **无 Delta 推送**：Phase 1A 不实现 Delta 推送，多客户端同步留到 Phase 1B
4. **无冲突处理**：单客户端无并发冲突，POSIX 投影层只需处理单版本情况

这样 Phase 1A 可以独立验证单客户端功能正确性，不被 Master 改造阻塞。

### 2.2 任务清单

#### F1A.1 基础数据结构：VectorClock + DirORSet（客户端版）

| # | 任务 | 文件 | 代码量 | 质量门控 |
|---|------|------|--------|---------|
| F1A.1.1 | 实现 `VectorClock` | `powerfs-fuse/src/orset.rs` [新增] | ~150 行 | check + fmt + clippy + 单元测试 |
| F1A.1.2 | 实现 `EntryId` + `DirEntry` | `orset.rs` | ~80 行 | 同上 |
| F1A.1.3 | 实现 `DirORSet`（add/remove/get_by_name/get_by_inode） | `orset.rs` | ~200 行 | 同上 |
| F1A.1.4 | 实现 `DeltaOp` 枚举 + 序列化 | `orset.rs` | ~80 行 | 同上 |
| F1A.1.5 | 单元测试：VectorClock 因果判定 | `orset.rs` 内 | ~100 行 | cargo test --lib |
| F1A.1.6 | 单元测试：DirORSet 增删查 | `orset.rs` 内 | ~100 行 | cargo test --lib |

**数据结构**：

```rust
// powerfs-fuse/src/orset.rs

use std::collections::{HashMap, HashSet};
use serde::{Serialize, Deserialize};

/// 条目唯一标识：(name + client_id + seq)
#[derive(Hash, Eq, PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct EntryId {
    pub name: String,
    pub client_id: u64,
    pub seq: u64,
}

/// 目录条目
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DirEntry {
    pub id: EntryId,
    pub inode: u64,
    pub file_type: FileType,
    pub mode: u32,
    pub size: u64,
    pub mtime: u64,
    pub atime: u64,
    pub parent_ino: u64,
    pub chunks: Vec<ChunkInfo>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileType {
    RegularFile,
    Directory,
    Symlink,
}

/// 向量时钟
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct VectorClock {
    counters: HashMap<u64, u64>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum CausalOrder {
    Before,
    After,
    Equal,
    Concurrent,
}

impl VectorClock {
    pub fn new() -> Self { Self::default() }
    pub fn increment(&mut self, client_id: u64) -> u64 { /*...*/ }
    pub fn observe(&mut self, client_id: u64, seq: u64) { /*...*/ }
    pub fn compare(&self, other: &Self) -> CausalOrder { /*...*/ }
    pub fn is_concurrent(&self, other: &Self) -> bool { /*...*/ }
    pub fn merge(&mut self, other: &Self) { /*...*/ }
}

/// Delta 操作
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DeltaOp {
    Add { entry: DirEntry, vclock: VectorClock },
    Remove { id: EntryId, vclock: VectorClock },
    Rename { old_id: EntryId, new_entry: DirEntry, vclock: VectorClock },
    SetAttr { inode: u64, mode: Option<u32>, size: Option<u64>, mtime: Option<u64>, vclock: VectorClock },
}

/// 目录 OR-Set
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DirORSet {
    pub entries: HashMap<EntryId, DirEntry>,
    pub tombstones: HashSet<EntryId>,
    pub vclock: VectorClock,
}

impl DirORSet {
    pub fn add(&mut self, entry: DirEntry) { /*...*/ }
    pub fn remove(&mut self, id: &EntryId) { /*...*/ }
    pub fn get_by_name(&self, name: &str) -> Vec<&DirEntry> { /*...*/ }
    pub fn get_by_inode(&self, inode: u64) -> Option<&DirEntry> { /*...*/ }
    pub fn list_all(&self) -> Vec<&DirEntry> { /*...*/ }
    pub fn apply_delta(&mut self, delta: &DeltaOp) { /*...*/ }
}
```

**质量门控检查清单**（每个子任务完成后执行）：
- [ ] `cargo check --all` 通过
- [ ] `cargo fmt --all` + `cargo fmt --check --all` 通过
- [ ] `cargo clippy --all -- -D warnings` 通过
- [ ] `cargo test --lib` 相关测试通过

---

#### F1A.2 POSIX 投影层

| # | 任务 | 文件 | 代码量 | 质量门控 |
|---|------|------|--------|---------|
| F1A.2.1 | 实现 `PosixProjection` 主体 | `powerfs-fuse/src/posix_projection.rs` [新增] | ~200 行 | check + fmt + clippy |
| F1A.2.2 | 实现 `project_listing`（readdir 投影） | `posix_projection.rs` | ~60 行 | + 单元测试 |
| F1A.2.3 | 实现 `project_lookup`（单文件查找） | `posix_projection.rs` | ~40 行 | + 单元测试 |
| F1A.2.4 | 实现 `select_primary`（主版本选择，Phase 1A 仅 LwwTime） | `posix_projection.rs` | ~40 行 | + 单元测试 |
| F1A.2.5 | 单元测试：单版本投影 | `posix_projection.rs` 内 | ~80 行 | cargo test --lib |
| F1A.2.6 | 单元测试：多版本投影（模拟，为 Phase 2 准备） | `posix_projection.rs` 内 | ~60 行 | cargo test --lib |

**Phase 1A 简化**：单客户端场景下，每个 name 只有一个 DirEntry，`select_primary` 直接返回唯一版本。多版本逻辑为 Phase 2 预留。

```rust
// powerfs-fuse/src/posix_projection.rs

use crate::orset::{DirORSet, DirEntry, FileType};

#[derive(Clone, Debug)]
pub struct VisibleEntry {
    pub name: String,
    pub inode: u64,
    pub file_type: FileType,
    pub has_conflict: bool,
}

pub enum MergePolicy {
    LwwTime,
    KeepAll,  // Phase 1A 默认
}

pub struct PosixProjection {
    default_policy: MergePolicy,
}

impl PosixProjection {
    pub fn new() -> Self { /*...*/ }

    /// 投影目录列表（readdir 用）
    /// Phase 1A：每个 name 只有一个 entry，直接投影
    pub fn project_listing(&self, orset: &DirORSet) -> Vec<VisibleEntry> {
        let mut visible = Vec::new();
        let mut seen_names: HashSet<String> = HashSet::new();

        for entry in orset.entries.values() {
            if seen_names.contains(&entry.id.name) {
                // 同名多份（Phase 2 场景），Phase 1A 不会触发
                continue;
            }
            seen_names.insert(entry.id.name.clone());
            visible.push(VisibleEntry {
                name: entry.id.name.clone(),
                inode: entry.inode,
                file_type: entry.file_type.clone(),
                has_conflict: false,  // Phase 1A 无冲突
            });
        }
        visible
    }

    /// 投影单文件查找
    pub fn project_lookup(&self, orset: &DirORSet, name: &str) -> Option<DirEntry> {
        let candidates = orset.get_by_name(name);
        if candidates.is_empty() {
            return None;
        }
        if candidates.len() == 1 {
            return Some(candidates[0].clone());
        }
        // 多版本：选主（Phase 2 完整实现）
        Some(self.select_primary(&candidates).clone())
    }

    fn select_primary<'a>(&self, entries: &'a [&DirEntry]) -> &'a DirEntry {
        // LwwTime: 最新 mtime 优先
        entries.iter().max_by_key(|e| e.mtime).copied().unwrap()
    }
}
```

---

#### F1A.3 Inode 分配器（客户端本地版，简化）

| # | 任务 | 文件 | 代码量 | 质量门控 |
|---|------|------|--------|---------|
| F1A.3.1 | 实现 `InodeAllocator`（本地递增，Phase 1B 改为 Master 预分配） | `powerfs-fuse/src/inode_allocator.rs` [新增] | ~80 行 | check + fmt + clippy + 单元测试 |
| F1A.3.2 | 单元测试 | `inode_allocator.rs` 内 | ~40 行 | cargo test --lib |

**Phase 1A 简化**：先用本地递增 inode 分配（从 100 开始，避开根 inode=1），Phase 1B 改为 Master 预分配。

```rust
// powerfs-fuse/src/inode_allocator.rs

use std::sync::atomic::{AtomicU64, Ordering};

pub struct InodeAllocator {
    next_inode: AtomicU64,
}

impl InodeAllocator {
    pub fn new() -> Self {
        Self {
            next_inode: AtomicU64::new(100),  // 避开根 inode=1
        }
    }

    pub fn allocate(&self) -> u64 {
        self.next_inode.fetch_add(1, Ordering::SeqCst)
    }
}
```

---

#### F1A.4 MetadataManager（本地版）

| # | 任务 | 文件 | 代码量 | 质量门控 |
|---|------|------|--------|---------|
| F1A.4.1 | 实现 `MetadataManager` 主体 | `powerfs-fuse/src/metadata_manager.rs` [新增] | ~300 行 | check + fmt + clippy |
| F1A.4.2 | 实现启动时全量拉取（从 Master 填充本地 OR-Set） | `metadata_manager.rs` | ~100 行 | + 单元测试 |
| F1A.4.3 | 实现读路径（lookup/list_dir/get_entry_by_inode/get_parent_dir） | `metadata_manager.rs` | ~150 行 | + 单元测试 |
| F1A.4.4 | 实现写路径（create/mkdir/unlink/rmdir/rename/setattr） | `metadata_manager.rs` | ~200 行 | + 单元测试 |
| F1A.4.5 | 实现简单同步（flush 时通过现有 API 同步到 Master） | `metadata_manager.rs` | ~100 行 | + 集成测试 |
| F1A.4.6 | 单元测试：读路径 | `metadata_manager.rs` 内 | ~100 行 | cargo test --lib |
| F1A.4.7 | 单元测试：写路径 | `metadata_manager.rs` 内 | ~100 行 | cargo test --lib |

**Phase 1A 简化设计**：

```rust
// powerfs-fuse/src/metadata_manager.rs

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::orset::{DirORSet, DirEntry, EntryId, DeltaOp, VectorClock, FileType};
use crate::posix_projection::{PosixProjection, VisibleEntry};
use crate::inode_allocator::InodeAllocator;
use crate::client::SyncFuseClient;

pub struct MetadataManager {
    /// 本地 OR-Set 缓存：dir_inode -> DirORSet
    dir_cache: RwLock<HashMap<u64, Arc<RwLock<DirORSet>>>>,
    /// inode 反向索引：ino -> (dir_ino, EntryId)
    inode_index: RwLock<HashMap<u64, (u64, EntryId)>>,
    /// POSIX 投影层
    projection: PosixProjection,
    /// Inode 分配器
    inode_allocator: InodeAllocator,
    /// 客户端 ID
    client_id: u64,
    /// 本地 seq 计数器
    seq_counter: AtomicU64,
    /// gRPC 客户端
    client: Arc<SyncFuseClient>,
}

impl MetadataManager {
    pub fn new(client: Arc<SyncFuseClient>, client_id: u64) -> Self { /*...*/ }

    /// 启动时全量拉取（Phase 1A：用现有 list_entries API）
    pub fn init_from_master(&self) -> Result<(), FsError> {
        // 1. 从 Master 拉取根目录（inode=1）的 entries
        // 2. 递归拉取所有子目录
        // 3. 填充本地 dir_cache + inode_index
        // 注意：Phase 1A 只拉取一级目录，深层目录按需拉取
    }

    // === 读路径（本地 OR-Set，零 RPC） ===

    pub fn lookup(&self, dir_ino: u64, name: &str) -> Result<Option<DirEntry>, FsError> {
        let dir_cache = self.dir_cache.read().unwrap();
        if let Some(orset_arc) = dir_cache.get(&dir_ino) {
            let orset = orset_arc.read().unwrap();
            return Ok(self.projection.project_lookup(&orset, name));
        }
        // 本地未命中：从 Master 拉取该目录
        drop(dir_cache);
        self.fetch_dir_from_master(dir_ino)?;
        let dir_cache = self.dir_cache.read().unwrap();
        if let Some(orset_arc) = dir_cache.get(&dir_ino) {
            let orset = orset_arc.read().unwrap();
            return Ok(self.projection.project_lookup(&orset, name));
        }
        Ok(None)
    }

    pub fn list_dir(&self, dir_ino: u64) -> Result<Vec<VisibleEntry>, FsError> {
        let dir_cache = self.dir_cache.read().unwrap();
        if let Some(orset_arc) = dir_cache.get(&dir_ino) {
            let orset = orset_arc.read().unwrap();
            return Ok(self.projection.project_listing(&orset));
        }
        drop(dir_cache);
        self.fetch_dir_from_master(dir_ino)?;
        let dir_cache = self.dir_cache.read().unwrap();
        if let Some(orset_arc) = dir_cache.get(&dir_ino) {
            let orset = orset_arc.read().unwrap();
            return Ok(self.projection.project_listing(&orset));
        }
        Ok(vec![])
    }

    pub fn get_entry_by_inode(&self, ino: u64) -> Result<Option<DirEntry>, FsError> {
        let index = self.inode_index.read().unwrap();
        if let Some((dir_ino, entry_id)) = index.get(&ino) {
            let dir_cache = self.dir_cache.read().unwrap();
            if let Some(orset_arc) = dir_cache.get(dir_ino) {
                let orset = orset_arc.read().unwrap();
                return Ok(orset.entries.get(entry_id).cloned());
            }
        }
        // 未命中：回退 Master 查询
        drop(index);
        self.fetch_entry_by_inode_from_master(ino)
    }

    pub fn get_parent_dir(&self, dir_ino: u64) -> Result<Option<DirEntry>, FsError> {
        let entry = self.get_entry_by_inode(dir_ino)?;
        if let Some(e) = entry {
            if e.parent_ino == 0 || e.parent_ino == dir_ino {
                // 根目录，返回自身
                return Ok(Some(e));
            }
            return self.get_entry_by_inode(e.parent_ino);
        }
        Ok(None)
    }

    // === 写路径（本地即成功，简单同步） ===

    pub fn create(&self, dir_ino: u64, name: &str, mode: u32) -> Result<DirEntry, FsError> {
        let inode = self.inode_allocator.allocate();
        let seq = self.next_seq();
        let now = now_unix();

        let entry = DirEntry {
            id: EntryId { name: name.to_string(), client_id: self.client_id, seq },
            inode,
            file_type: FileType::RegularFile,
            mode,
            size: 0,
            mtime: now,
            atime: now,
            parent_ino: dir_ino,
            chunks: vec![],
        };

        // 写入本地 OR-Set
        self.apply_to_local_orset(dir_ino, entry.clone())?;

        // Phase 1A 简单同步：通过现有 create_entry API 同步到 Master
        // 注意：这里不阻塞返回，同步失败仅 warn
        if let Err(e) = self.sync_create_to_master(&entry) {
            warn!("sync_create_to_master failed: {}, local entry still valid", e);
        }

        Ok(entry)
    }

    pub fn mkdir(&self, dir_ino: u64, name: &str, mode: u32) -> Result<DirEntry, FsError> {
        // 类似 create，但 file_type = Directory
        // 同时为新目录创建空的 DirORSet
    }

    pub fn unlink(&self, dir_ino: u64, name: &str) -> Result<(), FsError> {
        // 1. 从本地 OR-Set 查找该 name 的 entry
        // 2. 移除 entry，加入 tombstones
        // 3. 同步到 Master（delete_entry API）
    }

    pub fn rmdir(&self, dir_ino: u64, name: &str) -> Result<(), FsError> {
        // 类似 unlink，但验证是空目录
    }

    pub fn rename(&self, old_dir: u64, old_name: &str, new_dir: u64, new_name: &str) -> Result<(), FsError> {
        // 1. 从 old_dir OR-Set 查找 old_name
        // 2. 创建新 entry（new_name，保留 inode）
        // 3. 从 old_dir 移除，加入 new_dir
        // 4. 同步到 Master
    }

    pub fn setattr(&self, ino: u64, mode: Option<u32>, size: Option<u64>, mtime: Option<u64>) -> Result<DirEntry, FsError> {
        // 1. 查找 entry
        // 2. 更新属性
        // 3. 更新本地 OR-Set
        // 4. 同步到 Master
    }

    // === 内部辅助 ===

    fn next_seq(&self) -> u64 { self.seq_counter.fetch_add(1, Ordering::SeqCst) }

    fn apply_to_local_orset(&self, dir_ino: u64, entry: DirEntry) -> Result<(), FsError> {
        // 1. 获取或创建该目录的 DirORSet
        // 2. add entry
        // 3. 更新 inode_index
    }

    fn fetch_dir_from_master(&self, dir_ino: u64) -> Result<(), FsError> {
        // 通过现有 list_entries API 拉取，转换为 DirORSet
    }

    fn fetch_entry_by_inode_from_master(&self, ino: u64) -> Result<Option<DirEntry>, FsError> {
        // 通过现有 get_entry_by_inode API 拉取
    }

    fn sync_create_to_master(&self, entry: &DirEntry) -> Result<(), FsError> {
        // 通过现有 create_entry API 同步（兼容旧 path KV）
    }
}
```

---

#### F1A.5 DataManager（修复历史问题）

| # | 任务 | 文件 | 代码量 | 质量门控 |
|---|------|------|--------|---------|
| F1A.5.1 | 实现 `DataManager` 主体 | `powerfs-fuse/src/data_manager.rs` [新增] | ~200 行 | check + fmt + clippy |
| F1A.5.2 | 移植现有 chunk_cache + write_buffer + dirty_chunks | `data_manager.rs` | ~100 行 | 同上 |
| F1A.5.3 | **修复 ChunkCache LRU**（当前 `_max_chunks` 被忽略） | `cache.rs` | ~50 行 | + 单元测试 |
| F1A.5.4 | **修复 write 文件大小维护**（本地维护 size，getattr 返回） | `data_manager.rs` | ~40 行 | + 单元测试 |
| F1A.5.5 | **修复 truncate 清理 chunks**（删除超过 new_size 的 chunks） | `data_manager.rs` | ~50 行 | + 单元测试 |
| F1A.5.6 | 实现 read/write/flush/release/truncate 接口 | `data_manager.rs` | ~200 行 | + 集成测试 |
| F1A.5.7 | 单元测试 | `data_manager.rs` 内 | ~100 行 | cargo test --lib |

**关键修复点**（来自历史问题）：

1. **ChunkCache LRU**：当前 [cache.rs:948](file:///home/portion/powerfs/powerfs-fuse/src/cache.rs#L948) 的 `_max_chunks` 参数被忽略，无容量限制导致内存泄漏。修复为按字节数 LRU 淘汰。

2. **write 文件大小**：当前 [fuser_fs.rs:1860-1897](file:///home/portion/powerfs/powerfs-fuse/src/fuser_fs.rs#L1860-L1897) 每次 write 都通过 `get_entry_by_inode` + `update_entry` 两次 gRPC 更新 size，存在竞态。改为 DataManager 本地维护 `file_sizes: HashMap<u64, u64>`，write 时本地更新，getattr 返回本地值。

3. **truncate 清理 chunks**：当前 [fuser_fs.rs:907-936](file:///home/portion/powerfs/powerfs-fuse/src/fuser_fs.rs#L907-L936) setattr 只改 size 不清 chunks，导致截断后数据残留。修复为清理超过 new_size 的 chunk 缓存和 dirty 标记。

```rust
// powerfs-fuse/src/data_manager.rs

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::cache::ChunkCache;
use crate::client::SyncFuseClient;

pub struct DataManager {
    chunk_cache: Arc<ChunkCache>,
    write_buffer: RwLock<HashMap<u64, Vec<WriteBufferEntry>>>,
    dirty_chunks: RwLock<HashMap<u64, Vec<u64>>>,
    /// 文件大小缓存（write 时本地维护，修复历史问题）
    file_sizes: RwLock<HashMap<u64, u64>>,
    client: Arc<SyncFuseClient>,
    chunk_size: u64,
    max_cache_bytes: usize,
}

impl DataManager {
    pub fn new(client: Arc<SyncFuseClient>, chunk_size: u64, max_cache_bytes: usize) -> Self { /*...*/ }

    pub fn read(&self, ino: u64, offset: u64, size: usize) -> Result<Vec<u8>, FsError> { /*...*/ }

    pub fn write(&self, ino: u64, offset: u64, data: &[u8]) -> Result<u64, FsError> {
        let end = offset + data.len() as u64;

        // 修复：本地维护文件大小，无需 RPC
        {
            let mut sizes = self.file_sizes.write().unwrap();
            let current = sizes.entry(ino).or_insert(0);
            if end > *current {
                *current = end;
            }
        }

        // 写入 chunk_cache + write_buffer（现有逻辑）
        // ...

        Ok(data.len() as u64)
    }

    pub fn current_file_size(&self, ino: u64) -> u64 {
        *self.file_sizes.read().unwrap().get(&ino).unwrap_or(&0)
    }

    pub fn flush(&self, ino: u64) -> Result<(), FsError> { /*...*/ }

    pub fn fsync(&self, ino: u64) -> Result<(), FsError> { /*...*/ }

    pub fn release_inode(&self, ino: u64) -> Result<(), FsError> { /*...*/ }

    /// 修复：truncate 清理 chunks
    pub fn truncate(&self, ino: u64, new_size: u64) -> Result<(), FsError> {
        // 1. 更新本地大小
        self.file_sizes.write().unwrap().insert(ino, new_size);

        // 2. 清理超过 new_size 的 chunk 缓存
        let max_chunk_offset = (new_size / self.chunk_size) * self.chunk_size;
        self.chunk_cache.remove_after(ino, max_chunk_offset);

        // 3. 清理 dirty_chunks
        let mut dirty = self.dirty_chunks.write().unwrap();
        if let Some(offsets) = dirty.get_mut(&ino) {
            offsets.retain(|&off| off < max_chunk_offset);
        }

        Ok(())
    }

    pub fn prefetch(&self, ino: u64, offset: u64, size: u64) { /*...*/ }
}
```

---

#### F1A.6 fuser_fs.rs 重构

| # | 任务 | 文件 | 代码量 | 质量门控 |
|---|------|------|--------|---------|
| F1A.6.1 | 重构 `PowerFsFuserFs` 结构体（meta + data 双模块） | `fuser_fs.rs` L80-L96 | ~40 行 | check + fmt + clippy |
| F1A.6.2 | 重构 `lookup`（含 `.` / `..` 特殊名称） | `fuser_fs.rs` L724 | ~50 行 | + 集成测试 |
| F1A.6.3 | 重构 `getattr`（size 取 max(meta, data)） | `fuser_fs.rs` L747 | ~40 行 | + 集成测试 |
| F1A.6.4 | 重构 `setattr`（truncate 清理 chunks） | `fuser_fs.rs` L793 | ~50 行 | + 集成测试 |
| F1A.6.5 | 重构 `readdir`（含 `.` / `..` + POSIX 投影） | `fuser_fs.rs` L2226 | ~70 行 | + 集成测试 |
| F1A.6.6 | 重构 `mkdir` | `fuser_fs.rs` L992 | ~30 行 | + 集成测试 |
| F1A.6.7 | 重构 `rmdir` | `fuser_fs.rs` L1130 | ~30 行 | + 集成测试 |
| F1A.6.8 | 重构 `unlink` | `fuser_fs.rs` L1205 | ~30 行 | + 集成测试 |
| F1A.6.9 | 重构 `create` | `fuser_fs.rs` L1284 | ~30 行 | + 集成测试 |
| F1A.6.10 | 重构 `rename` | `fuser_fs.rs` L2328 | ~40 行 | + 集成测试 |
| F1A.6.11 | 重构 `open`/`opendir` | `fuser_fs.rs` L1509, L1572 | ~30 行 | + 集成测试 |
| F1A.6.12 | 重构 `read`/`write` | `fuser_fs.rs` L1578, L1820 | ~40 行 | + 集成测试 |
| F1A.6.13 | 重构 `flush`/`release` | `fuser_fs.rs` L2100, L2124 | ~40 行 | + 集成测试 |
| F1A.6.14 | 移除 lease 相关代码 | `fuser_fs.rs` | -200 行 | 编译通过 |
| F1A.6.15 | 移除旧 `handle_metadata_notification` 逻辑 | `fuser_fs.rs` L2751 | 重构 | 编译通过 |
| F1A.6.16 | 全量编译通过 | - | - | cargo build --all |

**新结构体**：

```rust
struct PowerFsFuserFs {
    meta: Arc<MetadataManager>,
    data: Arc<DataManager>,
    notifier: Arc<Mutex<Option<fuser::Notifier>>>,
    client_id: u64,
}
```

**每步质量门控**（handler 重构期间）：

每个 handler 重构完成后，必须通过：
```bash
cargo check --all
cargo fmt --all && cargo fmt --check --all
cargo clippy --all -- -D warnings
cargo test --test fuse_basic_test  # 基础功能测试
```

---

#### F1A.7 扩展测试套件

| # | 任务 | 文件 | 代码量 | 质量门控 |
|---|------|------|--------|---------|
| F1A.7.1 | 扩展 `fuse_basic_test.rs`：mkdir + rmdir | `tests/fuse_basic_test.rs` | ~60 行 | 测试通过 |
| F1A.7.2 | 扩展 `fuse_basic_test.rs`：create + write + read | `tests/fuse_basic_test.rs` | ~80 行 | 测试通过 |
| F1A.7.3 | 扩展 `fuse_basic_test.rs`：readdir 含 . / .. | `tests/fuse_basic_test.rs` | ~60 行 | 测试通过 |
| F1A.7.4 | 扩展 `fuse_basic_test.rs`：rename | `tests/fuse_basic_test.rs` | ~60 行 | 测试通过 |
| F1A.7.5 | 扩展 `fuse_basic_test.rs`：unlink | `tests/fuse_basic_test.rs` | ~40 行 | 测试通过 |
| F1A.7.6 | 扩展 `fuse_basic_test.rs`：truncate + 文件大小 | `tests/fuse_basic_test.rs` | ~60 行 | 测试通过 |
| F1A.7.7 | 扩展 `fuse_basic_test.rs`：大文件写入（多 chunk） | `tests/fuse_basic_test.rs` | ~60 行 | 测试通过 |
| F1A.7.8 | 扩展 `fuse_basic_test.rs`：stat + chmod | `tests/fuse_basic_test.rs` | ~40 行 | 测试通过 |
| F1A.7.9 | 新增 Unix 工具兼容性测试（ls/cp/rm/mv/cat/find/grep） | `tests/posix_compat_test.rs` [新增] | ~150 行 | 测试通过 |

**Unix 工具兼容性测试示例**：

```rust
// tests/posix_compat_test.rs

#[test]
fn test_ls_command() {
    assert_powerfs_mounted();
    let test_dir = create_test_dir("ls_test");

    // 创建几个文件
    fs::write(test_dir.join("file1.txt"), "content1").unwrap();
    fs::write(test_dir.join("file2.txt"), "content2").unwrap();
    fs::create_dir(test_dir.join("subdir")).unwrap();

    // 运行 ls
    let output = Command::new("ls")
        .arg(&test_dir)
        .output()
        .expect("ls failed");

    assert!(output.status.success(), "ls should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("file1.txt"), "ls should show file1.txt");
    assert!(stdout.contains("file2.txt"), "ls should show file2.txt");
    assert!(stdout.contains("subdir"), "ls should show subdir");
}

#[test]
fn test_cp_command() {
    assert_powerfs_mounted();
    let test_dir = create_test_dir("cp_test");

    fs::write(test_dir.join("source.txt"), "Hello PowerFS").unwrap();

    let output = Command::new("cp")
        .arg(test_dir.join("source.txt"))
        .arg(test_dir.join("dest.txt"))
        .output()
        .expect("cp failed");

    assert!(output.status.success(), "cp should succeed");
    let content = fs::read_to_string(test_dir.join("dest.txt")).unwrap();
    assert_eq!(content, "Hello PowerFS");
}

#[test]
fn test_find_command() {
    assert_powerfs_mounted();
    let test_dir = create_test_dir("find_test");

    fs::write(test_dir.join("a.txt"), "a").unwrap();
    fs::write(test_dir.join("b.log"), "b").unwrap();
    fs::create_dir(test_dir.join("sub")).unwrap();
    fs::write(test_dir.join("sub").join("c.txt"), "c").unwrap();

    let output = Command::new("find")
        .arg(&test_dir)
        .arg("-name").arg("*.txt")
        .output()
        .expect("find failed");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("a.txt"), "find should find a.txt");
    assert!(stdout.contains("c.txt"), "find should find c.txt in subdir");
    assert!(!stdout.contains("b.log"), "find should not find b.log");
}

#[test]
fn test_grep_command() {
    assert_powerfs_mounted();
    let test_dir = create_test_dir("grep_test");

    fs::write(test_dir.join("data.txt"), "line1\nhello world\nline3\n").unwrap();

    let output = Command::new("grep")
        .arg("hello")
        .arg(test_dir.join("data.txt"))
        .output()
        .expect("grep failed");

    assert!(output.status.success(), "grep should find hello");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("hello world"));
}
```

---

#### F1A.8 端到端验收测试

| # | 任务 | 文件 | 代码量 | 质量门控 |
|---|------|------|--------|---------|
| F1A.8.1 | 手动验收：ls/cp/rm/mv/cat 全部通过 | 手动 | - | 全部成功 |
| F1A.8.2 | 手动验收：find/grep 兼容性 | 手动 | - | 全部成功 |
| F1A.8.3 | 手动验收：大文件 dd + truncate | 手动 | - | 全部成功 |
| F1A.8.4 | 自动化验收：运行完整测试套件 | `cargo test --all` | - | 全部通过 |

### 2.3 Phase 1A 任务依赖图

```
F1A.1 (OR-Set 数据结构) ──→ F1A.2 (POSIX 投影层) ──→ F1A.4 (MetadataManager)
                                                          ↑
F1A.3 (InodeAllocator) ───────────────────────────────────┘
                                                          ↑
F1A.5 (DataManager) ──────────────────────────────────────┘
                                                          ↓
                                                    F1A.6 (fuser_fs 重构)
                                                          ↓
                                                    F1A.7 (扩展测试套件)
                                                          ↓
                                                    F1A.8 (端到端验收)
```

### 2.4 Phase 1A 质量门控总结

**每个子任务完成后必须执行**：

```bash
# 必须全部通过才能进入下一个任务
cargo check --all && \
cargo fmt --all && \
cargo fmt --check --all && \
cargo clippy --all -- -D warnings && \
cargo test --lib
```

**涉及 handler 改动的任务额外执行**：

```bash
cargo test --test fuse_basic_test
```

**Phase 1A 完成时的最终验收**：

```bash
# 完整质量检查
cargo check --all
cargo fmt --check --all
cargo clippy --all -- -D warnings
cargo test --all

# 手动验收（在 FUSE 挂载点执行）
mkdir /mnt/powerfs/testdir
echo "hello" > /mnt/powerfs/test.txt
cat /mnt/powerfs/test.txt
ls /mnt/powerfs/
ls -a /mnt/powerfs/  # 应显示 . 和 ..
cp /mnt/powerfs/test.txt /mnt/powerfs/copy.txt
mv /mnt/powerfs/test.txt /mnt/powerfs/renamed.txt
rm /mnt/powerfs/renamed.txt
rmdir /mnt/powerfs/testdir
stat /mnt/powerfs/copy.txt
chmod 0600 /mnt/powerfs/copy.txt
dd if=/dev/zero of=/mnt/powerfs/big.bin bs=1M count=10
truncate -s 1M /mnt/powerfs/big.bin
find /mnt/powerfs/ -name "*.txt"
grep "hello" /mnt/powerfs/copy.txt
```

全部通过后，Phase 1A 完成，进入 Phase 1B。

---

## 3. Phase 1B：多客户端同步

### 3.1 前置条件

- Phase 1A 全部验收通过
- 单客户端功能正确性已确认

### 3.2 任务清单

#### M1B.1 Master 端：VectorClock + OR-Set 存储

| # | 任务 | 文件 | 代码量 | 质量门控 |
|---|------|------|--------|---------|
| M1B.1.1 | 实现 `VectorClock`（Master 版） | `powerfs-master/src/vclock.rs` [新增] | ~200 行 | check + fmt + clippy + 单元测试 |
| M1B.1.2 | 实现 `DirORSet` 存储层 | `powerfs-master/src/orset_store.rs` [新增] | ~400 行 | 同上 |
| M1B.1.3 | RocksDB schema 定义 + 双写兼容 | `orset_store.rs` | ~100 行 | 同上 |
| M1B.1.4 | 实现 `apply_delta` 合并逻辑 | `orset_store.rs` | ~150 行 | 同上 |
| M1B.1.5 | 单元测试 | `orset_store.rs` 内 | ~200 行 | cargo test --lib |

---

#### M1B.2 Master 端：directory_tree.rs 重构

| # | 任务 | 文件 | 代码量 | 质量门控 |
|---|------|------|--------|---------|
| M1B.2.1 | 重构 `create_entry` 为 OR-Set Add | `directory_tree.rs` | ~80 行 | check + fmt + clippy + 集成测试 |
| M1B.2.2 | 重构 `delete_entry` 为 OR-Set Remove | `directory_tree.rs` | ~60 行 | 同上 |
| M1B.2.3 | 重构 `rename_entry` 为 Remove+Add | `directory_tree.rs` | ~80 行 | 同上 |
| M1B.2.4 | 重构 `list_entries` 走 OR-Set 投影 | `directory_tree.rs` | ~50 行 | 同上 |
| M1B.2.5 | 重构 `lookup` 走 OR-Set | `directory_tree.rs` | ~40 行 | 同上 |
| M1B.2.6 | 旧 path KV 双写（兼容期） | `directory_tree.rs` | +30 行 | 编译通过 |
| M1B.2.7 | 废弃 lease 相关方法 | `directory_tree.rs` | 标注 | 编译通过 |
| M1B.2.8 | Master 端全量测试通过 | - | - | cargo test --all |

---

#### M1B.3 Master 端：Inode 范围预分配

| # | 任务 | 文件 | 代码量 | 质量门控 |
|---|------|------|--------|---------|
| M1B.3.1 | 实现 `InodeRangeAllocator` | `powerfs-master/src/inode_range.rs` [新增] | ~150 行 | check + fmt + clippy + 单元测试 |
| M1B.3.2 | gRPC `AllocateInodeRange` | `server.rs` + proto | ~80 行 | 同上 |
| M1B.3.3 | Master 启动时初始化 | `directory_tree.rs` | +20 行 | 编译通过 |

---

#### M1B.4 Master 端：Delta 推送机制

| # | 任务 | 文件 | 代码量 | 质量门控 |
|---|------|------|--------|---------|
| M1B.4.1 | 实现 `DeltaSubscriptionManager` | `powerfs-master/src/delta_subscription.rs` [新增] | ~250 行 | check + fmt + clippy |
| M1B.4.2 | 重构 `subscribe_metadata` 为 delta 推送 | `directory_tree.rs` + `server.rs` | ~100 行 | + 集成测试 |
| M1B.4.3 | gRPC 流式接口 | proto | ~40 行 | 编译通过 |
| M1B.4.4 | 负载平衡（聚合 + 批量 + 背压） | `delta_subscription.rs` | ~100 行 | + 单元测试 |

---

#### F1B.5 FUSE 端：升级 InodeAllocator 为 Master 预分配

| # | 任务 | 文件 | 代码量 | 质量门控 |
|---|------|------|--------|---------|
| F1B.5.1 | 升级 `InodeAllocator` 支持 Master 预分配 | `inode_allocator.rs` | +80 行 | check + fmt + clippy + 单元测试 |
| F1B.5.2 | 客户端启动时申请 inode 范围 | `main.rs` | +20 行 | 编译通过 |
| F1B.5.3 | 范围耗尽自动申请新范围 | `inode_allocator.rs` | +40 行 | + 单元测试 |

---

#### F1B.6 FUSE 端：Delta 同步后台任务

| # | 任务 | 文件 | 代码量 | 质量门控 |
|---|------|------|--------|---------|
| F1B.6.1 | 实现 delta 推送循环（2s） | `metadata_manager.rs` | ~80 行 | check + fmt + clippy |
| F1B.6.2 | 实现 delta 拉取循环（2s） | `metadata_manager.rs` | ~80 行 | + 集成测试 |
| F1B.6.3 | 实现全量对齐循环（30s） | `metadata_manager.rs` | ~60 行 | + 集成测试 |
| F1B.6.4 | 实现断连重连强制全量同步 | `metadata_manager.rs` | ~50 行 | + 集成测试 |

---

#### F1B.7 client.rs 扩展

| # | 任务 | 文件 | 代码量 | 质量门控 |
|---|------|------|--------|---------|
| F1B.7.1 | 新增 `allocate_inode_range` | `client.rs` | ~40 行 | check + fmt + clippy |
| F1B.7.2 | 新增 `push_deltas` | `client.rs` | ~50 行 | 同上 |
| F1B.7.3 | 新增 `pull_deltas` | `client.rs` | ~50 行 | 同上 |
| F1B.7.4 | 新增 `fetch_dir_orset` | `client.rs` | ~40 行 | 同上 |
| F1B.7.5 | 新增 `subscribe_delta_stream` | `client.rs` | ~60 行 | 同上 |

---

#### F1B.8 多客户端测试

| # | 任务 | 文件 | 代码量 | 质量门控 |
|---|------|------|--------|---------|
| F1B.8.1 | 多客户端并发写不丢数据测试 | `tests/orset_phase1b_test.rs` [新增] | ~100 行 | 测试通过 |
| F1B.8.2 | Delta 同步收敛测试 | `tests/orset_phase1b_test.rs` | ~80 行 | 测试通过 |
| F1B.8.3 | 断连重连全量同步测试 | `tests/orset_phase1b_test.rs` | ~80 行 | 测试通过 |
| F1B.8.4 | inode 预分配测试 | `tests/orset_phase1b_test.rs` | ~60 行 | 测试通过 |

### 3.3 Phase 1B 验收标准

| 验证项 | 通过标准 |
|--------|---------|
| 编译/clippy/fmt | 全部通过 |
| 多客户端并发写 | 2 客户端同时 create 同名文件，都成功，`.conflicts/` 可见 2 份 |
| Delta 同步收敛 | 写入后 2s 内另一客户端可见 |
| 断连重连 | Master 重启后客户端全量同步恢复 |
| inode 预分配 | 客户端启动时获取 inode 范围，本地分配无冲突 |

---

## 4. Phase 2-4 概要

（与 v1.0 相同，此处省略，详见 [fuse-cache-architecture.md](fuse-cache-architecture.md) 第十二章）

**Phase 2 前置条件**：Phase 1B 全部验收通过。

---

## 5. 代码量估算（调整后）

| Phase | FUSE 端 | Master 端 | 测试 | 总计 |
|-------|--------|----------|------|------|
| **Phase 1A** | ~2000 行 | 0（不依赖） | ~900 行 | ~2900 行 |
| Phase 1B | ~800 行 | ~1500 行 | ~320 行 | ~2620 行 |
| Phase 2 | ~250 行 | ~1100 行 | ~520 行 | ~1870 行 |
| Phase 3 | ~580 行 | ~350 行 | ~360 行 | ~1290 行 |
| Phase 4 | ~570 行 | - | ~200 行 | ~770 行 |
| **总计** | **~4200 行** | **~2950 行** | **~2300 行** | **~9450 行** |

---

## 6. 实施顺序（Phase 1A 详细）

### Week 1：数据结构 + 核心模块

```
Day 1-2: F1A.1 VectorClock + DirORSet（客户端版）
  ├─ F1A.1.1 VectorClock 实现 + 单元测试
  ├─ F1A.1.2 EntryId + DirEntry 实现
  ├─ F1A.1.3 DirORSet 实现 + 单元测试
  └─ F1A.1.4 DeltaOp 枚举 + 序列化
  质量门控：cargo check + fmt + clippy + test --lib

Day 3: F1A.2 POSIX 投影层 + F1A.3 InodeAllocator
  ├─ F1A.2.1-F1A.2.4 PosixProjection 实现
  ├─ F1A.2.5-F1A.2.6 单元测试
  ├─ F1A.3.1 InodeAllocator 实现
  └─ F1A.3.2 单元测试
  质量门控：cargo check + fmt + clippy + test --lib

Day 4-5: F1A.4 MetadataManager + F1A.5 DataManager
  ├─ F1A.4.1-F1A.4.4 MetadataManager 读/写路径
  ├─ F1A.4.5 简单同步
  ├─ F1A.5.1-F1A.5.2 DataManager 主体 + 移植
  ├─ F1A.5.3 修复 ChunkCache LRU
  ├─ F1A.5.4 修复 write 文件大小
  └─ F1A.5.5 修复 truncate 清理 chunks
  质量门控：cargo check + fmt + clippy + test --lib
```

### Week 2：Handler 重构 + 测试

```
Day 6-7: F1A.6 fuser_fs.rs 重构（核心 handler）
  ├─ F1A.6.1 结构体重构
  ├─ F1A.6.2 lookup（含 . / ..）
  ├─ F1A.6.3 getattr
  ├─ F1A.6.5 readdir（含 . / ..）
  ├─ F1A.6.6 mkdir
  ├─ F1A.6.9 create
  └─ F1A.6.11 open
  质量门控：每个 handler 后 cargo check + fmt + clippy + test --test fuse_basic_test

Day 8: F1A.6 剩余 handler
  ├─ F1A.6.4 setattr（truncate 修复）
  ├─ F1A.6.7 rmdir
  ├─ F1A.6.8 unlink
  ├─ F1A.6.10 rename
  ├─ F1A.6.12 read/write
  └─ F1A.6.13 flush/release
  质量门控：同上

Day 9: F1A.6 清理 + F1A.7 扩展测试
  ├─ F1A.6.14 移除 lease 代码
  ├─ F1A.6.15 移除旧通知逻辑
  ├─ F1A.6.16 全量编译
  ├─ F1A.7.1-F1A.7.8 扩展 fuse_basic_test.rs
  └─ F1A.7.9 新增 posix_compat_test.rs
  质量门控：cargo test --all

Day 10: F1A.8 端到端验收
  ├─ F1A.8.1 手动 ls/cp/rm/mv/cat
  ├─ F1A.8.2 手动 find/grep
  ├─ F1A.8.3 手动 dd/truncate
  └─ F1A.8.4 自动化 cargo test --all
  验收标准：全部通过 → Phase 1A 完成
```

---

## 7. 风险与缓解

| 风险 | 概率 | 影响 | 缓解措施 |
|------|------|------|---------|
| Phase 1A 简单同步导致 Master 数据不一致 | 中 | 多客户端数据丢失 | Phase 1A 仅验证单客户端，多客户端在 Phase 1B 解决 |
| 本地 inode 与 Master inode 冲突 | 低 | inode 重复 | Phase 1A 用本地递增（从 100 开始），Phase 1B 改为 Master 预分配 |
| ChunkCache LRU 修复引入回归 | 中 | 数据读取失败 | 单元测试覆盖 + 集成测试验证 |
| truncate 修复影响现有行为 | 低 | 截断后读取异常 | 单元测试 + 手动 dd/truncate 验收 |
| handler 重构批量进行编译错误多 | 高 | 开发效率低 | 每个 handler 单独重构 + 编译验证 |
| POSIX 投影层单版本简化导致 Phase 2 重构 | 低 | 返工 | 投影层接口预留多版本参数 |

---

## 8. 附录：质量门控脚本

创建 `scripts/quality-gate.sh`：

```bash
#!/bin/bash
set -e

echo "=== PowerFS Quality Gate ==="

echo "[1/5] cargo check..."
cargo check --all 2>&1 | tail -5

echo "[2/5] cargo fmt..."
cargo fmt --all
cargo fmt --check --all 2>&1 | tail -5

echo "[3/5] cargo clippy..."
cargo clippy --all -- -D warnings 2>&1 | tail -10

echo "[4/5] cargo test --lib..."
cargo test --lib 2>&1 | tail -10

echo "[5/5] cargo test --test fuse_basic_test..."
cargo test --test fuse_basic_test 2>&1 | tail -10

echo "=== Quality Gate PASSED ==="
```

每个任务完成后执行：`bash scripts/quality-gate.sh`
