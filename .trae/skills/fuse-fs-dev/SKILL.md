---
name: "fuse-fs-dev"
description: "FUSE用户态文件系统开发规范与最佳实践。涵盖内核VFS dentry缓存失效、Notifier API、死锁避免、TTL策略、readdir实现、原子rename等。当开发或修改FUSE文件系统(fuser crate)代码时调用此skill。"
---

# FUSE 用户态文件系统开发规范

本skill记录基于 `fuser` crate (Rust) 开发FUSE文件系统的最佳实践，特别是与内核VFS层交互的关键注意事项。

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

## 4. readdir 实现

### `.` 和 `..` 条目
文件系统的 readdir 必须返回 `.`（当前目录）和 `..`（父目录）条目：

```rust
fn readdir(&mut self, _req: &Request, ino: u64, fh: u64, offset: i64, mut reply: ReplyDirectory) {
    let entry = self.cache.get_inode(ino);
    let parent_inode = entry.map(|e| e.parent).unwrap_or(1);

    let mut idx = offset as usize;

    // offset 0: 返回 "."
    if idx == 0 {
        if !reply.add(ino, 1, FileType::Directory, ".") {
            reply.ok();
            return;
        }
        idx = 1;
    }

    // offset 1: 返回 ".."
    if idx == 1 {
        if !reply.add(parent_inode, 2, FileType::Directory, "..") {
            reply.ok();
            return;
        }
        idx = 2;
    }

    // offset 2+: 返回子条目
    let child_offset = idx.saturating_sub(2);
    // ... 遍历 children.iter().skip(child_offset) ...
}
```

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

## 6. 原子性 Rename

### Master 端实现
直接在 RocksDB 中删除旧 key + 写入新 key，而非 delete-then-create：

```rust
pub fn rename_entry(&self, old_path: &str, new_directory: &str, new_name: &str, client_id: &str) -> Result<bool, rocksdb::Error> {
    if let Some(bytes) = self.db.get(old_path.as_bytes())? {
        let mut entry: Entry = prost::Message::decode(bytes.as_ref())?;
        entry.generation = self.allocate_generation();
        entry.name = new_name.to_string();
        entry.directory = new_directory.to_string();

        let new_key = Self::path_to_key(new_directory, new_name);
        let mut data = Vec::new();
        entry.encode(&mut data)?;

        self.db.delete(old_path.as_bytes())?;   // 删除旧 key
        self.db.put(&new_key, &data)?;            // 写入新 key

        self.publish_notification(EventType::Delete, old_path, None, client_id);
        self.publish_notification(EventType::Rename, &new_path, Some(entry), client_id);
        Ok(true)
    } else {
        Ok(false)
    }
}
```

### FUSE 端调用
```rust
fn rename(&mut self, _req: &Request, parent: u64, name: &OsStr, new_parent: u64, new_name: &OsStr, reply: ReplyEmpty) {
    // ... 获取 entry 和路径 ...

    match self.client.rename_entry(&old_path, &parent_path, name_str, &new_parent_path, new_name_str, &self.client_id) {
        Ok(_) => {
            reply.ok();  // 1. 先 reply
            self.invalidate_kernel_dentry(parent, name_str);           // 2. 失效旧 dentry
            if parent != new_parent {
                self.invalidate_kernel_dentry(new_parent, new_name_str); // 3. 失效新 dentry（跨目录时）
            }
            self.invalidate_kernel_inode(entry.inode);                  // 4. 失效 inode 缓存
        }
        Err(e) => {
            reply.error(libc::EIO);
        }
    }
}
```

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
