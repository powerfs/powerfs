# 分片均衡与 `stat .` 修复方案

## 背景与问题

在多 Filer 分片集群中存在以下三类问题：

1. **inode 分配严重不均衡**：旧实现使用 `node_id * 1B + 1000` 作为 inode 基址，
   而 shard 范围为 1M。node_id ≥ 1 的节点分配的所有 inode 全部落入最后一个 shard，
   导致单 shard 过载、其余 shard 空闲。
2. **shard 路由策略不合理**：文件和目录都从全局生成器分配 inode，无法保证
   `calculate_shard(inode) == calculate_shard(parent_inode)`，导致 readdir 需要
   跨 shard 查询文件 inode 记录，性能差且易出错。
3. **`stat .` / `stat ..` 频繁返回 `-ENOENT`**：内核 dentry 缓存过期后（entry_timeout=100ms）
   会调用 `lookup(parent, ".")` 和 `lookup(parent, "..")`，而 Filer 不存储
   `.` / `..` 目录项，直接返回 ENOENT。

此外还伴随两个相关问题：
4. **跨 shard rename 不支持**：旧实现直接返回 `"cross-shard rename not supported yet"`。
5. **symlink target 在 rename 后丢失**：Filer 响应错误地把 `name` 当作 `symlink_target` 返回；
   客户端未从 inline_data 中提取 symlink target。

## 修复方案

### 方案 1：Per-shard inode 分配器（解决分配不均衡）

**核心思路**：每个 shard 拥有独立的分配器，每个 Filer 节点在 shard 范围内拥有
互不重叠的 slot，从根本上避免跨节点冲突和单 shard 过载。

**实现** ([meta_shard_manager.rs](file:///home/portion/powerfs/powerfs-filer/src/meta_shard_manager.rs))：

```rust
struct ShardAllocator {
    counter: AtomicU64,      // shard 内本节点 slot 的计数器
    shard_start: u64,        // shard 范围起始
    node_offset: u64,        // 本节点在 shard 内的偏移
}

const MAX_FILER_NODES: u64 = 64;

// 每个 shard 范围划分为 MAX_FILER_NODES 个 slot
// node_offset = node_id * (range_size / MAX_FILER_NODES)
// 实际 inode = shard_start + node_offset + counter
```

**分配接口**：
- `alloc_inode_in_shard(shard_id)` — 在指定 shard 内分配 inode
- `generate_inode()` — 兼容旧调用方，等价于 `alloc_inode_in_shard(ShardId(0))`
- `recover_inode_generator()` — 扫描 RocksDB 恢复每个 shard 的 counter

**Shard 范围修正** ([shard_map.rs](file:///home/portion/powerfs/powerfs-allocator/src/shard_map.rs))：

旧实现最后一个 shard 范围延伸到 `u64::MAX`，导致 per-node offset 产生
天文数字 inode。修正为**所有 shard 统一 1M 范围**，最后一个 shard 不再
延伸到 `u64::MAX`：

```rust
// 旧：最后一个 shard = [2M, u64::MAX)
// 新：最后一个 shard = [2M, 3M)
let end = start.saturating_add(inode_per_shard); // 所有 shard 统一
```

### 方案 2：文件随父目录、目录选新 shard（解决路由策略）

> **更新（与 [shard-routing-no-forward-principle.md](shard-routing-no-forward-principle.md) 对齐）**：
> 子目录创建已从"服务端单函数 `create_directory`"重构为**客户端协调的两阶段提交**。
> 服务端**绝不服务间转发**；非 leader 时返回 `STATUS_ERR_REDIRECT`，客户端更新路由后重试。
> 本节代码片段保留为分片策略说明（`pick_child_dir_shard` 仍用于选目标 shard），
> 实际的跨 shard 提交流程见下方"子目录创建的两种方式"。

**核心思路**：
- **文件**（含 symlink、S3 对象）：inode 分配在父目录所在 shard，保证
  `calculate_shard(inode) == calculate_shard(parent_inode)`，readdir 时
  可从同一 shard 获取目录项和 inode 记录，避免跨 shard 查询。
- **目录**：inode 分配在不同 shard，将目录树分散到各 shard 实现负载均衡。

**分片策略实现**：

```rust
// create_file / create_symlink / create_s3_object
let parent_shard = self.shard_strategy.calculate_shard(parent_inode);
let inode = self.alloc_inode_in_shard(parent_shard);

// create_directory（仅计算 target_shard，实际提交由客户端两阶段协调，见下表）
let parent_shard = self.shard_strategy.calculate_shard(parent_inode);
let target_shard = self.pick_child_dir_shard(parent_shard);
let inode = self.alloc_inode_in_shard(target_shard);

// pick_child_dir_shard: round-robin 到下一个 shard
pub fn pick_child_dir_shard(&self, parent_shard: ShardId) -> ShardId {
    let count = self.shard_strategy.get_shard_count();
    if count <= 1 { return parent_shard; }
    ShardId((parent_shard.0 + 1) % count)
}
```

> **说明**：用户曾质疑"所有子项都跟父目录同 shard 岂不是都在一个 shard 上"。
> 实际上**目录**会跳到新 shard，目录树深度增长时逐层分散到不同 shard；
> 只有**文件**留在父目录的 shard，这是为了 readdir 局部性，且文件数量
> 远大于目录，集中在父目录 shard 不会导致目录树本身倾斜。

#### 子目录创建的两种方式

子目录创建根据 `target_shard` 是否等于 `parent_shard` 分为两种方式（详见
[shard-routing-no-forward-principle.md §2.2-§3](shard-routing-no-forward-principle.md)）：

| 方式 | 触发条件 | 通信次数 | 实现 |
|------|---------|---------|------|
| **单 shard 快速路径** | `target_shard == parent_shard`（shard_count ≤ 1） | 1 次 RPC | 客户端发 `Mkdir`(0x0014) 到 parent_shard，服务端 `handle_mkdir` 原子完成 `CreateInode` + `AddDirEntry` |
| **跨 shard 两阶段** | `target_shard != parent_shard`（shard_count > 1，常态） | 3 次 RPC | 客户端协调 `alloc_inode_batch`(target_shard) → `MkdirPhaseA`(0x003c, target_shard, CreateInode) → `MkdirPhaseB`(0x003d, parent_shard, AddDirEntry) |

**关键**：两个 Phase 由**客户端分别路由**到各自 shard 的 leader，各自独立重定向。
服务端只处理发到本节点的单 shard 请求，**绝不转发**。Phase A 成功而 Phase B 失败时
产生孤儿 inode（target_shard 有 record 但无 dir entry 指向），由后台 GC 清理——
这比服务间转发的故障风险可接受得多。

**原 `create_directory` 服务端函数已废弃**：子目录 create 不再在服务端单函数内跨 shard 调用，
改由客户端两阶段协调。文件 create 仍走 `propose_create_inode_and_direntry`（单 shard）。

### 方案 3：`stat .` / `stat ..` 拦截（解决 ENOENT）

**核心思路**：在 FUSE `lookup` 回调中拦截 `.` 和 `..`，本地解析，
不转发给 Filer（Filer 根本不存储这两个目录项）。

**实现** ([fuse.rs](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs))：

```rust
fn lookup(&self, _ctx: &Context, parent: Self::Inode, name: &CStr) -> std::io::Result<Entry> {
    let name_str = name.to_str().unwrap_or("");
    match name_str {
        "." => return self.lookup_dot(parent),
        ".." => return self.lookup_dotdot(parent),
        _ => {}
    }
    // ... 正常 lookup 逻辑
}
```

- **`lookup_dot(parent)`**：返回目录自身属性。先查缓存，缓存未命中时
  通过 `get_entry_by_inode(parent)` 从 Filer 获取。
- **`lookup_dotdot(parent)`**：返回父目录属性。先从缓存获取 `parent.parent`，
  缓存未命中时从 Filer 查询并用路径反解父 inode。Root 的 `..` 返回 root 自身。
- **`resolve_parent_from_path`**：从 Filer 返回的完整路径反解父 inode
  （Entry proto 无 parent_ino 字段）。

**readdir 父目录 inode 回退**：readdir 时若缓存未命中导致无法获取
父目录 inode，回退到 Filer 查询，避免 `..` 指向自身导致 `cd ..` 失效。

### 方案 4：跨 shard rename 两阶段提交

> **设计原则更新**：此处的"由 `RaftGroup::handle_propose` 转发"已**废弃**。
> 详见 [shard-routing-no-forward-principle.md](shard-routing-no-forward-principle.md)。
> 所有跨 shard 操作改为**客户端协调的两阶段提交**，服务端绝不经 `propose_forward` 转发。
> 非 leader 时返回 `STATUS_ERR_REDIRECT`，客户端更新 `shard_router` 后直连正确 leader。

**核心思路**：跨 shard rename 分解为多条独立的 Raft 提案，由**客户端分别路由**到
各自 shard 的 leader（非 leader 返回 REDIRECT，客户端重定向后重试）。

**实现** ([meta_shard_manager.rs](file:///home/portion/powerfs/powerfs-filer/src/meta_shard_manager.rs))：

```
Phase A: 在 old_parent 的 shard 上解析 inode（get_dir_entry_inode）
Phase B: AddDirEntry  → new_parent 的 shard（新名字指向 inode）
Phase C: RemoveDirEntry → old_parent 的 shard（删除旧名字）
Phase D: RenameInode  → inode 自身的 shard（更新 name + parent_inode）
         + SetAttr    → 两个父目录（更新 mtime/ctime）
```

**新增 `RenameInode` 命令** ([raft_group_manager.rs](file:///home/portion/powerfs/powerfs-filer/src/raft_group_manager.rs) + [shard_store.rs](file:///home/portion/powerfs/powerfs-filer/src/shard_store.rs))：

```rust
ShardCommand::RenameInode { inode, new_name, new_parent_inode } => {
    if let Some(mut info) = self.get_inode(inode) {
        info.name = new_name;
        info.parent_inode = new_parent_inode;
        info.ctime = now;
        info.mtime = now;
        self.update_inode(info);
    }
}
```

### 方案 5：symlink target 保留

**问题**：
1. Filer `inode_to_entry_info` 错误地把 `info.name` 当作 `symlink_target` 返回。
2. 客户端 `attr_from_resp_with_layout` 未从 inline_data 提取 symlink target，
   导致 rename 后缓存项 `symlink_target=None`，readlink 返回空。

**修复**：

1. [net_handler.rs](file:///home/portion/powerfs/powerfs-filer/src/net_handler.rs)：
   ```rust
   symlink_target: if matches!(info.file_type, FileType::Symlink) {
       info.symlink_target.clone()  // 旧: info.name.clone()
   } else { None },
   ```

2. [meta_shard_client.rs](file:///home/portion/powerfs/powerfs-fuse-core/src/meta_shard_client.rs)：
   ```rust
   if attr.file_type == libc::DT_LNK {
       if let Some(data) = &attr.inline_data {
           if let Ok(target) = std::str::from_utf8(data) {
               attr.symlink_target = Some(target.to_string());
           }
       }
   }
   ```

## 涉及文件

| 文件 | 修改内容 |
|------|---------|
| `powerfs-allocator/src/shard_map.rs` | shard 范围统一 1M，不再延伸到 u64::MAX |
| `powerfs-filer/src/shard_strategy.rs` | 更新测试用例匹配新的范围 |
| `powerfs-filer/src/meta_shard_manager.rs` | per-shard 分配器 + 文件/目录路由策略 + 跨 shard rename |
| `powerfs-filer/src/raft_group_manager.rs` | 新增 `RenameInode` 命令 |
| `powerfs-filer/src/shard_store.rs` | 处理 `RenameInode` 命令 |
| `powerfs-filer/src/net_handler.rs` | 修复 symlink_target 序列化 |
| `powerfs-fuse-core/src/meta_shard_client.rs` | 从 inline_data 提取 symlink target |
| `powerfs-fuse/src/fuse.rs` | `.` / `..` 拦截 + readdir 父目录回退 |
| `powerfs-cli/src/commands/config_gen.rs` | 补充 `request_timeout_secs: 15` |

## 验证计划

1. `cargo check` 全工作区编译通过
2. `cargo test` 相关单元测试通过（shard_map、shard_strategy）
3. 运行 `scripts/tests/cross_shard_regression.sh` 全量回归
4. 重点验证：
   - inode 分布均衡（多节点分配落在不同 shard）
   - `stat .` / `stat ..` 不再返回 ENOENT
   - 跨 shard rename 后文件可见、内容正确
   - symlink 跨目录 rename 后 readlink 返回正确 target
