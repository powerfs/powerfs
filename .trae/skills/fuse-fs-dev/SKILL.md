---
name: "fuse-fs-dev"
description: "FUSE用户态文件系统开发规范与最佳实践。涵盖OR-Set弱一致缓存、POSIX投影层、VFS dentry失效、Notifier API、死锁避免、TTL策略、readdir的./..处理、冲突合并等。开发或修改FUSE(fuser)文件系统时调用。"
---

# FUSE 用户态文件系统开发规范

> **[架构更新 - 2026-07-13]** PowerFS 已采用 OR-Set CRDT 弱一致架构。
> - 默认弱一致：本地 OR-Set 缓存即返回，异步 delta 同步
> - POSIX 投影层：主版本可见 + 冲突副本进 `.conflicts/`
> - 废弃：写保护租约、全局广播失效、同步提交+错误回滚
> - 保留：Notifier API（内核 VFS 失效）、TTL=0、readdir 的 `.`/`..` 处理
>
> 详细方案：[design/fuse-cache-architecture.md](../../design/fuse-cache-architecture.md) v2.0

本skill记录基于 `fuser` crate (Rust) 开发FUSE文件系统的最佳实践，特别是与内核VFS层交互的关键注意事项。

## 0. 核心架构原则

### OR-Set 弱一致缓存模型（新架构）

PowerFS FUSE 客户端采用 **OR-Set CRDT 弱一致缓存**，写操作本地即返回，异步 delta 同步到 Master。

**核心数据结构：**
- 目录条目唯一标识：`(name + client_id + seq)`，并发全部保留不覆盖
- 本地 OR-Set 缓存：`dir_inode → DirORSet`，写操作直接修改本地
- Delta 同步队列：本地变更异步推送到 Meta，默认 2s 增量 + 30s 全量

**写操作流程（本地即成功）：**
```rust
// create/mkdir/unlink/rmdir/rename/setattr
// 1. 修改本地 OR-Set，生成 DeltaOp 加入队列
// 2. 立即返回成功给 FUSE
// 3. 异步 delta 同步到 Master（后台任务）
let entry = self.meta.create(dir_ino, name, params)?;  // 本地 OR-Set Add
reply.entry(&TTL, &attr, 0);  // 立即返回
// delta 异步同步
```

**读操作流程（POSIX 投影层）：**
```rust
// lookup/readdir/getattr 走本地 OR-Set 投影
// 1. 查本地 OR-Set，按 name 分组
// 2. 无冲突：直接返回
// 3. 有冲突：按 MergePolicy 选主版本，其余进 .conflicts/
let entry = self.meta.lookup(dir_ino, name)?;  // 本地投影
```

### Inode 分配由 Master 统一管理

FUSE 用户层**不应该**自行分配 inode。Inode 必须由 Master 统一分配和管理，确保全局唯一性。

> **注意**：新架构下，客户端本地 OR-Set 缓存会暂存 inode，但权威 inode 仍由 Master 分配。本地创建时生成临时 inode，delta 同步后由 Master 确认或重新分配。

### POSIX 投影层（OR-Set → VFS 视图）

OR-Set 允许同名多份，但 VFS 期望同名唯一。投影层负责转换：

```
OR-Set 真实存储                    FUSE 投影（VFS/应用看到）
file1 (client1, seq1, 主版本) →   file1                 （可见）
file1 (client2, seq2, 冲突)   →   .conflicts/file1.client2.seq2  （隐藏）
```

**投影规则：**
1. 按文件名分组，每组按 MergePolicy 选主版本，用原文件名
2. 冲突副本放入 `.conflicts/` 隐藏目录，命名 `{name}.{client_id}.{seq}`
3. `ls` 默认不显示 `.conflicts/`，`ls -a` 显示
4. 冲突状态通过 xattr `user.fs.conflict_count` 查询

### 跨节点刷新（按需强一致）

弱一致架构下，需要强视图一致时通过两种方式触发：

| 方式 | 触发 | 行为 |
|------|------|------|
| xattr | `setxattr("user.fs.need_sync", "1")` | 下次访问拉取最新 OR-Set |
| API | `refresh_dir_incremental()` / `refresh_dir_full()` | 增量/全量刷新 |

### [已废弃] 旧强一致模型

以下旧规范已废弃，仅作历史参考：
- ~~本地缓存回退到 Master 查询~~ → 改为本地 OR-Set 优先，miss 才查 Master
- ~~同步提交 + 错误回滚~~ → 改为本地即成功，异步 delta 同步
- ~~写保护租约~~ → 废弃，弱一致无需排他保护
- ~~全局广播失效~~ → 改为增量 delta 推送

## 1. 内核 VFS Dentry 缓存失效

### 背景
FUSE文件系统与内核VFS交互时，dentry `(parent_inode, name) -> inode` 在VFS层被缓存。`rename`、`unlink`、`rmdir`、`mkdir`、`create` 等操作后，如果不通知内核失效，`ls` 等操作会返回旧缓存。

### 解决方案
使用 `fuser::Notifier` API 向内核发送失效通知：
- `notifier.inval_entry(parent, name)` — 使指定 dentry 失效
- `notifier.inval_inode(ino, offset, len)` — 使指定 inode 的缓存失效

### Cargo.toml 配置
```toml
fuser = { version = "0.14", features = ["abi-7-11", "abi-7-12"] }
```
- `abi-7-11`：启用 `Notifier` 类型
- `abi-7-12`：启用 `inval_entry` 和 `inval_inode` 方法

### 获取 Notifier
`fuser::mount2` 不支持 Notifier。必须使用 `Session::new` + `session.run()`：

```rust
struct PowerFsFuserFs {
    // ... 其他字段 ...
    notifier: Arc<std::sync::Mutex<Option<fuser::Notifier>>>,
}

// 挂载时获取 Notifier
let notifier_clone = fs.notifier.clone();
let session_handle = std::thread::Builder::new()
    .name("fuse_server".to_string())
    .spawn(move || {
        match fuser::Session::new(fs_for_mount, Path::new(&mount_point), &options) {
            Ok(mut session) => {
                let notifier = session.notifier();
                {
                    let mut guard = notifier_clone.lock().unwrap();
                    *guard = Some(notifier);
                }
                session.run()?;
            }
            Err(e) => error!("Failed to create FUSE session: {}", e),
        }
    })?;
```

### 失效辅助方法
```rust
fn invalidate_kernel_dentry(&self, parent: u64, name: &str) {
    let notifier_guard = self.notifier.lock().unwrap();
    if let Some(notifier) = notifier_guard.as_ref() {
        if let Err(e) = notifier.inval_entry(parent, OsStr::new(name)) {
            debug!("Failed to invalidate kernel dentry (parent={}, name={}): {}", parent, name, e);
        }
    }
}

fn invalidate_kernel_inode(&self, inode: u64) {
    let notifier_guard = self.notifier.lock().unwrap();
    if let Some(notifier) = notifier_guard.as_ref() {
        if let Err(e) = notifier.inval_inode(inode, 0, -1) {
            debug!("Failed to invalidate kernel inode ({}): {}", inode, e);
        }
    }
}
```

## 2. 死锁避免（关键）

### 规则：Reply 必须在 Invalidation 之前

**绝对不能**在 `reply.ok()` / `reply.entry()` / `reply.created()` 之前调用 `invalidate_kernel_dentry` 或 `invalidate_kernel_inode`。

### 死锁原因
1. 内核发送 FUSE 请求（如 UNLINK）到用户态
2. 用户态处理 UNLINK，在 reply 之前调用 `notifier.inval_entry()`
3. 内核收到 inval_entry 通知，尝试使 dentry 失效
4. 但该 dentry 正被当前 UNLINK 请求持有锁
5. `writev` 阻塞等待内核处理通知，内核阻塞等待 reply
6. **死锁！**

### 正确模式
```rust
fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
    // ... 处理逻辑 ...
    match self.client.delete_entry(&entry_path, false, &self.client_id) {
        Ok(_) => {
            self.cache.remove(entry.inode);
            reply.ok();                              // 1. 先发送 reply
            self.invalidate_kernel_dentry(parent, name_str);  // 2. 再发送通知
            self.invalidate_kernel_inode(entry.inode);
        }
        Err(e) => {
            reply.error(libc::EIO);
        }
    }
}
```

### 错误模式（会导致死锁）
```rust
// 错误！通知在 reply 之前
self.invalidate_kernel_dentry(parent, name_str);
reply.ok();
```

### 需要遵守此规则的处理器
- `unlink` — reply.ok() 后再 inval_entry + inval_inode
- `rmdir` — reply.ok() 后再 inval_entry + inval_inode
- `mkdir` — reply.entry() 后再 inval_entry
- `create` — reply.created() 后再 inval_entry
- `rename` — reply.ok() 后再 inval_entry(旧名) + inval_entry(新名) + inval_inode

## 3. TTL 策略

```rust
const TTL: Duration = Duration::from_secs(0);
```

设置 TTL=0 让 VFS 不缓存 dentry 和属性，每次 lookup 都重新查询 FUSE daemon。与 Notifier API 配合使用，形成双重保障：
- TTL=0 防止 VFS 长期缓存
- Notifier 主动通知 VFS 失效已变更的条目

## 4. readdir 与 `.` / `..` 目录项

### 核心结论

`.`、`..` **不是手动创建的 inode 文件**，是 `readdir` 回调**必须主动返回的标准目录项**，由 POSIX 规范强制要求。内核 VFS 读取目录时，只会从 `readdir` 返回的列表里识别 `.`/`..`，不返回会导致 `ls` 异常、`cd` 报错。

### 两个特殊项含义

1. `.` （当前目录）：inode = 当前目录自身 inode
2. `..`（父目录）：inode = 该目录父目录的 inode

**根目录（inode=1 / ROOT_INODE）的 `..` 仍然指向根自身。**

### readdir 标准实现（带 offset 分页）

```rust
fn readdir(
    &mut self,
    _req: &Request<'_>,
    inode: u64,
    _fh: u64,
    offset: i64,
    mut reply: ReplyDirectory,
) {
    // 1. 获取当前目录元数据，拿到父 inode
    let entry = match self.cache.get_inode(inode) {
        Some(e) if e.is_dir => e,
        _ => {
            reply.error(libc::ENOTDIR);
            return;
        }
    };
    let parent_ino = entry.parent;

    let mut idx = offset as usize;

    // offset 0: 返回 "."
    if idx == 0 {
        if !reply.add(inode, 1, FileType::Directory, ".") {
            reply.ok();
            return;
        }
        idx = 1;
    }

    // offset 1: 返回 ".."
    if idx == 1 {
        if !reply.add(parent_ino, 2, FileType::Directory, "..") {
            reply.ok();
            return;
        }
        idx = 2;
    }

    // offset 2+: 返回子条目（. 和 .. 占了索引 0 和 1
    let child_offset = idx.saturating_sub(2);
    let children = self.cache.list_children(inode);
    for (child_ino, child_name, is_dir) in children.iter().skip(child_offset) {
        let dtype = if *is_dir { FileType::Directory } else { FileType::RegularFile };
        if !reply.add(*child_ino, 1, dtype, child_name) {
            reply.ok();
            return;
        }
        idx += 1;
    }

    reply.ok();
}
```

### reply.add 参数说明

```rust
reply.add(ino: u64, offset: u64, file_type: FileType, name: &str)
```

- `ino`：对应文件 inode 号
- `offset`：下一个条目的偏移量（从 1 开始递增，`. `..` 分别为 1 和 2）
- `file_type`：文件类型（Directory / RegularFile / Symlink 等）
- `name`：文件名（`.` / `..` / 普通文件名）

### 配套 lookup 回调（cd . / cd .. 依赖）

用户执行 `cd ..` / `stat .` 时，内核会调用 `lookup` 根据父目录 inode + 文件名查找子 inode，必须处理 `.` 和 `..` 两个特殊名称：

```rust
fn lookup(
    &mut self,
    _req: &Request<'_>,
    parent: u64,
    name: &OsStr,
    reply: ReplyEntry,
) {
    let name_str = name.to_str().unwrap_or("");

    // 处理特殊名称
    let target_ino = match name_str {
        "." => parent,                           // . 指向当前目录 inode
        ".." => {
            match self.cache.get_inode(parent) {
                Some(dir) => dir.parent,         // .. 指向父 inode
                None => { reply.error(libc::ENOENT); return; }
            }
        }
        // 普通文件/目录，从缓存或 Master 查找
        _ => {
            match self.cache.lookup_in_cache(parent, name_str) {
                Some(entry) => entry.inode,
                None => {
                    // 回退到 Master 查询...
                    reply.error(libc::ENOENT);
                    return;
                }
            }
        }
    };

    // 获取目标条目属性并返回
    // ...
}
```

### 根目录特殊规则

根目录 inode（一般固定为 1 / INODE_ROOT）的父 inode = 自身：
- `.` → root ino
- `..` → root ino

`cd /..` 不会跳出根目录，符合 Linux 规范。

### 常见问题

1. **readdir 不返回 `.` / `..`**
   - `ls -a` 看不到这两个隐藏项
   - `cd .` / `cd ..` 报错 `No such file or directory`

2. **lookup 未处理 `.` / `..`**
   就算 readdir 能列出，cd/stat 访问会失败

3. `.` / `..` 填错 inode
   比如 `..` 填成 0、填成子文件 inode，会导致文件系统错乱、crash

4. **offset 分页逻辑错误**
   readdir 是分页读取，offset=0 才推送 `.` 和 `..`；offset>0 直接跳过，避免重复返回。

### Stale Cache 清理

readdir 时对比 Master 返回的条目与本地缓存，移除已不存在的条目：

```rust
let master_names: HashSet<String> = entries.iter().map(|e| e.name.clone()).collect();
let children = self.cache.list_children(inode);
for (child_inode, child_name, _) in &children {
    if !master_names.contains(child_name) {
        debug!("readdir: removing stale cache entry '{}' (inode={})", child_name, child_inode);
        self.cache.remove(*child_inode);
    }
}
```

### 整体流程总结

1. **列举目录（ls）**：内核调用 `readdir`，手动 push `.`、`..` 两个目录项
2. **访问 `.`/`..`（cd/stat）**：内核调用 `lookup`，匹配特殊名称返回对应 inode
3. 两个接口配合，才能完整支持 `.`、`..` 标准目录语义

## 5. setattr 路径计算

### 规则：directory 字段必须是父目录路径，不能是完整 entry 路径

```rust
// 正确：从完整路径中提取父目录
let path = self.cache.get_path_by_parent_chain(inode).unwrap_or_else(|| "/".to_string());
let directory = if let Some(last_slash) = path.rfind('/') {
    if last_slash == 0 {
        "/".to_string()
    } else {
        path[..last_slash].to_string()
    }
} else {
    "/".to_string()
};

let filer_entry = FilerEntry {
    name: entry.name.clone(),
    directory,  // 正确：父目录路径
    // ...
};
```

### 错误模式（会创建幻影条目）
```rust
// 错误！directory 被设为完整路径
let filer_entry = FilerEntry {
    name: entry.name.clone(),
    directory: path.clone(),  // BUG: path 是 "/dir/file.txt"，不是 "/dir"
    // ...
};
```

这会导致 Master 在 RocksDB 中创建 `/dir/file.txt/file.txt` 幻影条目，使目录无法删除（rmdir 报 ENOTEMPTY）。

## 6. Rename 操作（OR-Set 模型）

> **[架构更新 - 2026-07-13]** Rename 从原子 delete+put 改为本地 OR-Set Remove+Add，异步 delta 同步。

### 新架构：本地 OR-Set Remove + Add

```rust
fn rename(&mut self, _req: &Request, parent: u64, name: &OsStr, new_parent: u64, new_name: &OsStr, reply: ReplyEmpty) {
    // 1. 本地 OR-Set 操作：Remove 旧条目 + Add 新条目
    match self.meta.rename(parent, name_str, new_parent, new_name_str) {
        Ok(_) => {
            reply.ok();  // 2. 立即返回成功（本地操作，无 RPC）
            // 3. 异步 delta 同步到 Master（后台任务自动处理）

            // 4. 失效内核 VFS dentry 缓存（保留）
            self.invalidate_kernel_dentry(parent, name_str);
            if parent != new_parent {
                self.invalidate_kernel_dentry(new_parent, new_name_str);
            }
        }
        Err(e) => {
            reply.error(libc::EIO);
        }
    }
}
```

### [已废弃] 旧强一致原子 Rename

~~Master 端直接在 RocksDB 中 delete + put 原子操作~~ → 改为本地 OR-Set Remove+Add，异步同步。

旧实现保留作历史参考，新开发请使用 OR-Set 模式。

## 7. MountOption 注意事项

- `AutoUnmount` 要求同时设置 `AllowOther` 或 `AllowRoot`，否则 fuser 会自动添加 `AllowOther`（需要 `/etc/fuse.conf` 中 `user_allow_other` 配置）
- `DefaultPermissions` 启用内核权限检查
- 建议组合：`FSName` + `AutoUnmount` + `DefaultPermissions`

## 8. 调试技巧

- FUSE 日志可通过 `RUST_LOG=debug` 或 `RUST_LOG=powerfs_fuse::fuser_fs=debug` 控制
- fuser 库日志前缀为 `fuser::session`、`fuser::request`
- 查看 FUSE 操作：`docker logs <container> 2>&1 | grep "FUSE("`
- 查看死锁：`docker exec <container> ps aux` — 如果有多个 `ls`/`rm` 进程 D 状态，说明 FUSE 死锁
- 查看挂载状态：`docker exec <container> cat /proc/mounts | grep powerfs`
