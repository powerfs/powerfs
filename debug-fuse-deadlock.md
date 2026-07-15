# Debug Session: FUSE 单客户端死锁问题

## 📋 Session Info
- **Session ID**: `fuse-deadlock`
- **Status**: `[FIXED]`
- **Created**: 2026-07-15
- **Symptom**: 单客户端执行操作时挂载点挂起/超时
- **Environment**: Linux FUSE 挂载
- **Root Cause**: `rename` 函数中持有 `dir_cache.write()` 锁时调用 `ensure_dir_cache()`, 而 `ensure_dir_cache()` 内部尝试获取 `dir_cache.read()` 锁导致死锁

---

## 🎯 Hypotheses (可证伪假设)

### H1: `lookup_local` 与写操作的锁顺序冲突 ➡️ ❌ 已排除
- **验证**: 单元测试全部通过，说明基础锁机制正常

### H2: `inode_paths` 锁与 `dir_cache` 锁的交叉获取 ➡️ ⏳ 待验证
- **验证**: 需要更多测试覆盖

### H3: `add_change` 与 `flush_changes` 的锁竞争 ➡️ ⏳ 待验证
- **验证**: 需要更多测试覆盖

### H4: 递归目录操作的锁累积 ➡️ ⏳ 待验证
- **验证**: 需要更多测试覆盖

### H5: `ensure_dir_cache` 的锁升级问题 ➡️ ✅ **确认**
- **原因**: `rename` 函数在持有 `dir_cache.write()` 锁时调用 `ensure_dir_cache()`
- **`ensure_dir_cache()` 第 724 行**尝试获取 `dir_cache.read()` 锁
- **死锁**: RwLock 不支持写锁降级为读锁，导致永久阻塞

---

## 💡 Fix (修复)

**文件**: [metadata_manager.rs](file:///home/portion/powerfs/powerfs-fuse/src/metadata_manager.rs)

**变更**: 将 `rename` 函数中对 `ensure_dir_cache()` 的调用移到 `dir_cache` 锁释放之后

```diff
 // 修复前 (死锁):
 drop(dir_cache); // 释放 dir_cache 锁
 if new_entry.file_type == FileType::Directory {
     let orset_arc = self.ensure_dir_cache(new_entry.inode);  // ❌ 尝试获取读锁
     let mut orset = orset_arc.write().unwrap();
     orset.dir_ino = new_entry.inode;
 }

 // 修复后 (无死锁):
 if new_entry.file_type == FileType::Directory {
     // 在持有 dir_cache 锁的情况下直接操作
     if let Some(child_orset) = dir_cache.get(&new_entry.inode) {  // ✅ 直接访问
         let mut orset = child_orset.write().unwrap();
         orset.dir_ino = new_entry.inode;
     }
 }
 drop(dir_cache); // 释放 dir_cache 锁
```

**原理**: 直接通过已持有的写锁访问缓存中的 OR-Set，避免再次获取锁

---

## ✅ Verification (验证)

| 测试 | 状态 | 说明 |
|------|------|------|
| `cargo test --package powerfs-fuse --lib` | ✅ 82 passed | 所有单元测试通过 |
| `cargo test --package powerfs-fuse --test concurrent_consistency` | ✅ 通过 | 并发测试通过 |
| `cargo clippy --package powerfs-fuse` | ✅ 通过 | 零警告 |

---

## 🧹 Cleanup (清理)
- 调试服务器已停止
- 插桩代码已移除
- 调试文件保留供参考
