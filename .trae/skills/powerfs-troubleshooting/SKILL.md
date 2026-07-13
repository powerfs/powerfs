---
name: "powerfs-troubleshooting"
description: "Troubleshooting guide for PowerFS FUSE client issues including OR-Set cache, delta sync, conflict resolution, mount points, and error handling. Invoke when debugging PowerFS FUSE related problems."
---

# PowerFS FUSE Troubleshooting Guide

> **[架构更新 - 2026-07-13]** PowerFS 已采用 OR-Set CRDT 弱一致架构。
> - 弱一致窗口：默认 2s 增量 + 30s 全量同步，窗口内可能读到旧数据
> - 冲突处理：并发写全部保留，主版本可见 + `.conflicts/` 冲突副本
> - 跨节点刷新：`user.fs.need_sync` xattr 或 API 触发即时收敛
>
> 详细方案：[design/fuse-cache-architecture.md](../../design/fuse-cache-architecture.md) v2.0

This document records common issues and their solutions encountered during PowerFS FUSE client development and testing.

## 1. CI Submodule Pull Failure

### Problem
`cargo fmt --all` failed in GitHub Actions because `rfs_tester` submodule was not pulled.

### Root Cause
- CI workflow did not include `submodules: true` in the checkout step
- Submodule URL used SSH which requires authentication in CI

### Solution
- Add `submodules: true` to the checkout action in `.github/workflows/rust.yml`
- Change submodule URL from SSH to HTTPS in `.gitmodules`

### Files Modified
- `.github/workflows/rust.yml`
- `.gitmodules`

## 2. Test Mount Path Mismatch

### Problem
Tests failed with "Mount path '/tmp/powerfs-test' is not a PowerFS FUSE mount!"

### Root Cause
- Tests used `/tmp/powerfs-test` but `test_harness` created mount at `/tmp/powerfs-posix-test`

### Solution
- Use `test_harness::get_fuse_mount()` to get the correct mount point path

### Files Modified
- `powerfs-fuse/tests/fs_call_validation_test.rs`

## 3. FUSE Process Error Output Discarded

### Problem
Could not see FUSE service debug logs during testing.

### Root Cause
- `spawn_fuse` redirected stderr to `Stdio::null()`

### Solution
- Change stderr from `Stdio::null()` to `Stdio::inherit()` in `test_harness.rs`

### Files Modified
- `powerfs-fuse/tests/test_harness.rs`

## 4. Directory Open Not Returning EISDIR

### Problem
`test_open_directory_fails` test failed because opening a directory did not return `EISDIR` error.

### Root Cause
- FUSE `open` method did not check if the inode is a directory
- Test used `File::open` which doesn't return `EISDIR` for directories

### Solution
- Add directory type check in `fuser_fs.rs` `open` method
- Use `libc::O_RDWR` flag with `libc::open` in the test

### Files Modified
- `powerfs-fuse/src/fuser_fs.rs`
- `powerfs-fuse/tests/fs_call_validation_test.rs`

## 5. FUSE Mount Permission Denied

### Problem
Tests failed with "Permission denied" when creating directories. `ls -la` showed root directory owned by `root:root`.

### Root Cause
- Root directory permissions were set incorrectly (0o755 instead of 0o777)
- UID/GID were hardcoded to 0 instead of current user
- Missing `AllowOther` mount option

### Solution
- Set root directory `perm` to 0o777 in `getattr` method
- Set `uid`/`gid` to current user using `libc::getuid()`/`libc::getgid()`
- Add `AllowOther` mount option

### Files Modified
- `powerfs-fuse/src/fuser_fs.rs`
- `powerfs-fuse/src/cache.rs`

## 6. FUSE Mount Options Conflict

### Problem
FUSE mount failed with "Conflicting mount options found: [AllowRoot, AllowOther]"

### Root Cause
- Both `AllowRoot` and `AllowOther` options were specified, which conflict

### Solution
- Remove `AllowRoot` option, keep only `AllowOther`

### Files Modified
- `powerfs-fuse/src/fuser_fs.rs`

## 7. Connection Pool Not Reusing Connections

### Problem
FUSE client created many new connections instead of reusing from pool.

### Root Cause
- Multiple issues:
  1. `MasterServiceClient::new(channel)` consumed channel ownership instead of cloning
  2. `return_master_channel` was missing in many method paths
  3. Connection pool semaphore was acquired in `get()` but never released in `put()`
  4. `subscribe_metadata` method acquired channel but never returned it

### Solution
- Change all `MasterServiceClient::new(channel)` to `MasterServiceClient::new(channel.clone())`
- Add `return_master_channel(channel).await` in all success and error paths
- Add `self.semaphore.add_permits(1)` in `put()` method
- Note: `subscribe_metadata` keeps channel for streaming, which is expected

### Files Modified
- `powerfs-fuse/src/client.rs`

## 8. Log System Enhancement

### Problem
FUSE is a background process, making it difficult to capture logs.

### Root Cause
- Logs only output to stdout/stderr
- No file-based logging option

### Solution
- Add `--log-file`, `--log-max-size-mb`, `--log-max-files` command-line arguments
- Configure `env_logger` to write to file when `--log-file` is specified
- Add structured log format with timestamp, level, and target

### Files Modified
- `powerfs-fuse/src/main.rs`
- `powerfs-monitor/src/main.rs`

## Connection Pool Implementation Notes

### Current State
- Connection pool is implemented in `powerfs-fuse/src/client.rs`
- Uses `ConnectionPool` struct with `channels: RwLock<Vec<Channel>>`
- Uses semaphore for concurrency control
- Supports both master and volume connection pools

### Key Methods
- `ensure_master_channel()`: Gets or creates master connection pool
- `get_volume_channel(addr)`: Gets or creates volume connection pool
- `return_master_channel(ch)`: Returns channel to master pool
- `return_volume_channel(addr, ch)`: Returns channel to volume pool
- `invalidate_master_channel()`: Clears and invalidates master pool
- `invalidate_volume_channel(addr)`: Clears and invalidates volume pool

### Important Notes
- Always use `channel.clone()` when creating gRPC clients to preserve channel ownership
- Always call `return_master_channel` or `return_volume_channel` after using a channel
- Connection reuse only works within the same async context
- Long-running streams (like `subscribe_metadata`) keep channels for their lifetime

## Debugging Tips

### Check FUSE Mount Status
```bash
cat /proc/mounts | grep powerfs
```

### Check Connection Count
```bash
netstat -an | grep 9334 | wc -l
```

### Enable Debug Logging
```bash
RUST_LOG=debug ./powerfs-fuse --master localhost:9334 --mount-point /mnt/powerfs --verbose
```

### Write Logs to File
```bash
./powerfs-fuse --master localhost:9334 --mount-point /mnt/powerfs --log-file /var/log/powerfs-fuse.log
```

## Testing Recommendations

1. Always use `test_harness.rs` utilities for consistent test setup
2. Add `ensure_fuse_mounted()` at the start of FUSE-related tests
3. Use `get_mount_path()` to get the correct mount point
4. Clean up test directories after tests complete
5. Run tests with `RUST_LOG=debug` to see detailed logs

## Common Error Codes

| Error | Description | Common Cause |
|-------|-------------|--------------|
| ENOENT | No such file or directory | Inode not found in OR-Set cache |
| EISDIR | Is a directory | Opening directory with file open flags |
| EACCES | Permission denied | Incorrect file/directory permissions |
| EEXIST | File exists | Creating file that already exists (note: OR-Set allows concurrent same-name) |
| ENODEV | No such device | FUSE mount not established |
| EPERM | Operation not permitted | Invalid operation for file type |

## OR-Set Weak Consistency Issues (New)

### Problem: Cross-client writes not visible immediately

**Symptom**: Client A writes file, Client B cannot see it immediately.

**Root Cause**: OR-Set weak consistency, default 2s incremental + 30s full sync window.

**Solution**:
- Wait for sync window (2-30s)
- Or set xattr `user.fs.need_sync=1` on the directory to force refresh on next access
- Or call `refresh_dir_incremental()` API for immediate delta sync

### Problem: `.conflicts/` directory appears with unexpected files

**Symptom**: `ls -a` shows `.conflicts/` directory containing files like `file1.client2.123`.

**Root Cause**: Concurrent writes from multiple clients created conflict copies. This is expected behavior — OR-Set preserves all concurrent versions.

**Solution**:
- Review conflict copies in `.conflicts/`
- Manually resolve: keep one, delete others
- Or configure directory MergePolicy to auto-merge (LwwTime/ContentHash/etc.)

### Problem: Same file name shows different content on different clients

**Symptom**: Client A sees file1 with content X, Client B sees file1 with content Y.

**Root Cause**: OR-Set allows concurrent same-name entries. POSIX projection layer selects primary version by MergePolicy, which may differ before full sync.

**Solution**:
- Trigger full refresh: `setxattr("user.fs.sync_force", "1")`
- Or call `refresh_dir_full()` API
- After convergence, all clients see same primary version

## Checklist for FUSE Issues

- [ ] Is the FUSE mount point correctly configured?
- [ ] Are the mount options correct (AllowOther, etc.)?
- [ ] Is the root directory permission set to 0o777?
- [ ] Are UID/GID correctly mapped to the current user?
- [ ] Is the OR-Set local cache properly initialized?
- [ ] Is the delta sync background task running?
- [ ] Are gRPC clients using `channel.clone()`?
- [ ] Is logging enabled to capture debug information?
- [ ] [OR-Set] Is the `.conflicts/` directory accessible for conflict resolution?
- [ ] [OR-Set] Is the sync window (2s/30s) acceptable for the use case?
- [ ] [OR-Set] Are xattr refresh attributes working for cross-node consistency?