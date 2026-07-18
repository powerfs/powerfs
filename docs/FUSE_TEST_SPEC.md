# PowerFS FUSE 测试规范

> **[架构更新 - 2026-07-13]** PowerFS 已重定位为弱一致分布式数据同步存储。
>
> **测试策略调整**：
> - POSIX 兼容性测试改为**投影层兼容性测试**（主版本可见 + `.conflicts/` 冲突副本）
> - 新增 OR-Set 冲突场景测试（并发新建/修改/删除同名文件，验证全部保留）
> - 新增跨节点刷新测试（xattr `user.fs.need_sync` + API 增量/全量刷新）
> - 弱一致窗口测试（验证 2s 增量 + 30s 全量同步收敛）
> - 一致性测试（coherence suite）相关用例基于旧强一致方案，需逐步迁移
>
> 详细架构方案：[design/fuse-cache-architecture.md](design/fuse-cache-architecture.md) v2.0

---

## 一、一键测试脚本（推荐）

### 1.1 使用 run-tests.sh 运行所有测试

```bash
# 运行所有测试（推荐）
docker/scripts/run-tests.sh

# 重建镜像并运行所有测试
docker/scripts/run-tests.sh --build

# 运行特定测试套件
docker/scripts/run-tests.sh --suite basic
docker/scripts/run-tests.sh --suite rfs
docker/scripts/run-tests.sh --suite volume
docker/scripts/run-tests.sh --suite posix
docker/scripts/run-tests.sh --suite concurrent
docker/scripts/run-tests.sh --suite mount
docker/scripts/run-tests.sh --suite coherence
docker/scripts/run-tests.sh --suite sync
docker/scripts/run-tests.sh --suite manual

# 详细日志模式
docker/scripts/run-tests.sh --verbose

# 跳过清理（保留测试环境用于调试）
docker/scripts/run-tests.sh --skip-cleanup
```

### 1.2 测试套件说明

| 测试套件 | 包含的测试 | 对应测试文件 | 说明 |
|---------|-----------|-------------|------|
| all | 所有测试 | - | 完整测试套件（默认） |
| mount | 挂载点验证 | mount_verification_test.rs | 验证测试环境正确性 |
| basic | FUSE 基础操作 | fuse_basic_test.rs | 文件/目录基本操作 |
| rfs | RFS 测试集成 | rfs_tester_fuse_test.rs | RFS 兼容性测试 |
| volume | Volume 服务测试 | volume_integration_test.rs, volume_verification_test.rs | Volume 服务集成验证 |
| posix | 投影层兼容性 | posix_tests.rs | POSIX 投影层兼容性（主版本可见 + .conflicts/） |
| concurrent | 并发冲突保留 | concurrent_consistency.rs | 多客户端并发写，验证 OR-Set 全部保留 |
| coherence | 一致性阶段 | coherence_phase0_test.rs, coherence_phase1_test.rs | [待迁移] 旧强一致方案测试 |
| orset | OR-Set 冲突场景 | orset_conflict_test.rs | [新增] 五类冲突场景 + 合并策略 |
| refresh | 跨节点刷新 | refresh_test.rs | [新增] xattr + API 增量/全量刷新 |
| fs | 文件系统核心 | fs_test.rs | 文件系统核心功能 |
| sync | 同步操作 | sync_test.rs | 文件同步操作 |
| minimal | 最小化测试 | fuse_minimal_test.rs | 最小功能验证 |
| manual | 手动验证 | - | 目录/文件操作手动验证 |

### 1.3 run-tests.sh 工作流程

1. **环境预检查**：验证 Docker、Docker Compose、Cargo 是否可用
2. **构建镜像**（可选）：构建 Rust 二进制和 Docker 镜像
3. **启动基础设施**：启动 Redis、Master、Volume、S3 服务
4. **启动 FUSE 客户端**：启动 fuse-1 容器并验证挂载
5. **运行测试**：按套件执行测试，显示详细结果
6. **清理环境**（可选）：停止并清理所有容器

## 二、测试环境准备

### 2.1 启动集群
```bash
docker/scripts/start-cluster.sh
```

### 2.2 启动 FUSE 客户端
```bash
docker/scripts/start-fuse.sh
```

### 2.3 验证环境
```bash
# 验证 FUSE 挂载（关键步骤）
docker exec fuse-1 mount | grep -E "on /mnt/powerfs type fuse"
# 预期输出: powerfs on /mnt/powerfs type fuse (rw,nosuid,nodev,relatime,user_id=0,group_id=0,default_permissions,allow_other)

# 验证目录操作
docker exec fuse-1 ls /mnt/powerfs
```

## 三、测试顺序（从简单到复杂）

### Phase 1: 文件基础操作（open/close/unlink）

#### Test 1.1: 创建文件
- 创建空文件
- 验证文件存在
- 删除文件
- 验证文件不存在

#### Test 1.2: 打开/关闭文件
- 创建文件
- 打开文件
- 关闭文件
- 删除文件

#### Test 1.3: 删除不存在的文件
- 尝试删除不存在的文件
- 验证返回错误

### Phase 2: 目录操作（mkdir/readdir/rmdir）

#### Test 2.1: 创建目录
- 创建目录
- 验证目录存在
- 删除目录
- 验证目录不存在

#### Test 2.2: 读取目录内容
- 创建目录
- 在目录中创建文件
- 读取目录内容
- 验证文件列表正确
- 删除文件和目录

#### Test 2.3: 删除非空目录
- 创建目录
- 在目录中创建文件
- 尝试删除非空目录
- 验证返回错误
- 删除文件后再删除目录

### Phase 3: 文件读写操作（write/read）

#### Test 3.1: 写入并读取小文件
- 创建文件
- 写入少量数据（< 16MB）
- 关闭文件
- 重新打开文件
- 读取文件内容
- 验证内容匹配

#### Test 3.2: 文件大小验证
- 创建文件
- 写入数据
- 获取文件大小
- 验证大小正确

#### Test 3.3: 追加写入
- 创建文件并写入初始数据
- 追加写入更多数据
- 验证总内容正确

### Phase 4: 多级目录结构

#### Test 4.1: 多级目录创建和遍历
- 创建目录结构: level1/level2/level3
- 在最底层目录创建文件
- 验证整个结构存在
- 逐层删除

#### Test 4.2: 多级目录文件操作
- 创建多级目录
- 在不同层级创建文件
- 读取所有文件内容
- 验证正确性

## 四、测试执行

### 4.1 使用一键脚本（推荐）
```bash
# 运行所有测试
docker/scripts/run-tests.sh

# 运行特定测试套件
docker/scripts/run-tests.sh --suite basic

# 重建镜像并运行测试
docker/scripts/run-tests.sh --build
```

### 4.2 手动运行测试
```bash
# 在 fuse-1 容器中运行
docker exec fuse-1 bash -c "cd /app && POWERFS_DOCKER_TEST=1 POWERFS_MOUNT=/mnt/powerfs cargo test --manifest-path powerfs-fuse/Cargo.toml --test fuse_basic_test -- --test-threads=1"
```

### 4.3 单独运行某个测试
```bash
docker exec fuse-1 bash -c "cd /app && POWERFS_DOCKER_TEST=1 POWERFS_MOUNT=/mnt/powerfs cargo test --manifest-path powerfs-fuse/Cargo.toml --test fuse_basic_test test_create_file_open_close_unlink -- --test-threads=1"
```

### 4.4 测试文件清单

| 测试文件 | 路径 | 说明 |
|---------|------|------|
| mount_verification_test.rs | powerfs-fuse/tests/ | 挂载点验证测试 |
| fuse_basic_test.rs | powerfs-fuse/tests/ | FUSE 基础操作测试 |
| rfs_tester_fuse_test.rs | powerfs-fuse/tests/ | RFS 测试集成 |
| volume_integration_test.rs | powerfs-fuse/tests/ | Volume 集成测试 |
| volume_verification_test.rs | powerfs-fuse/tests/ | Volume 验证测试 |
| posix_tests.rs | powerfs-fuse/tests/ | POSIX 兼容性测试 |
| concurrent_consistency.rs | powerfs-fuse/tests/ | 并发一致性测试 |
| coherence_phase0_test.rs | powerfs-fuse/tests/ | 一致性阶段 0 测试 |
| coherence_phase1_test.rs | powerfs-fuse/tests/ | 一致性阶段 1 测试 |
| fs_test.rs | powerfs-fuse/tests/ | 文件系统核心测试 |
| sync_test.rs | powerfs-fuse/tests/ | 同步操作测试 |
| fuse_minimal_test.rs | powerfs-fuse/tests/ | 最小化 FUSE 测试 |

## 五、故障排查

### 5.1 查看 FUSE 日志
```bash
docker logs fuse-1 2>&1 | tail -100
```

### 5.2 查看挂载状态
```bash
docker exec fuse-1 cat /proc/mounts | grep fuse
```

### 5.3 重启 FUSE 服务
```bash
docker compose -f docker/docker-compose.yml restart fuse-1
```

### 5.4 重建 Docker 镜像
```bash
docker/scripts/start-cluster.sh --build
docker/scripts/start-fuse.sh --build
```

### 5.5 清理卡住的容器
```bash
# 如果容器无法正常停止
docker rm -f $(docker ps -aq)

# 如果仍然卡住，重启 Docker
sudo systemctl restart docker
```

## 六、测试通过标准

所有测试用例必须：
1. 运行完成（不卡死）
2. 断言通过（无失败）
3. 资源正确清理（无残留文件/目录）

## 七、挂载点验证（强制要求）

### 6.1 测试前验证
所有 FUSE 集成测试必须在测试开始时验证挂载点是真正的 PowerFS FUSE 挂载：

```rust
fn assert_powerfs_mounted() {
    let mount_path = get_mount_path();
    if let Ok(content) = std::fs::read_to_string("/proc/mounts") {
        for line in content.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 3 && parts[1] == mount_path {
                return;
            }
        }
    }
    panic!(
        "Mount path '{}' is not a PowerFS FUSE mount! Tests must run against PowerFS.",
        mount_path
    );
}

#[test]
fn test_directory_operations() {
    assert_powerfs_mounted();  // 强制验证
    // ... 测试逻辑
}
```

### 6.2 验证原因
- **防止假阳性**：如果挂载点不存在或只是普通目录，测试会在本地文件系统上运行
- **确保测试有效性**：本地文件系统测试通过不代表 PowerFS FUSE 实现正确
- **避免测试误导**：防止开发人员误以为测试验证了 PowerFS 功能

### 6.3 测试文件列表

| 测试文件 | 位置 | 说明 |
|---------|------|------|
| mount_verification_test.rs | powerfs-fuse/tests/ | 挂载点验证测试 |
| fuse_basic_test.rs | powerfs-fuse/tests/ | FUSE 基础操作测试（已包含挂载验证） |
| rfs_tester_fuse_test.rs | powerfs-fuse/tests/ | RFS 测试集成（已包含挂载验证） |

## 八、错误处理标准

### 7.1 错误类型与错误码映射

| 错误类型 | 触发条件 | POSIX 错误码 | FUSE 操作 |
|---------|---------|-------------|----------|
| MasterNotConnected | Master 服务不可用 | ENOTCONN | mkdir/rmdir/readdir/lookup |
| MasterError | Master 操作失败 | EIO | mkdir/rmdir/readdir/lookup |
| VolumeNotConnected | Volume 服务不可用 | ENOTCONN | open/read/write/flush/release |
| VolumeError | Volume 操作失败 | EIO | open/read/write/flush/release |
| NotFound | 文件/目录不存在 | ENOENT | lookup/open/read/write/unlink |
| PermissionDenied | 权限不足 | EACCES | 所有操作 |
| InvalidArgument | 参数无效 | EINVAL | 所有操作 |

### 7.2 错误处理测试用例

#### Test 7.1: Master 不可用场景

```rust
#[test]
fn test_master_not_connected() {
    assert_powerfs_mounted();
    
    // 模拟 Master 断开连接
    // 验证目录创建返回 ENOTCONN
    let result = std::fs::create_dir("/mnt/powerfs/test_master_error");
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ENOTCONN),
               "Expected ENOTCONN when Master is not connected");
}
```

#### Test 7.2: Volume 不可用场景

```rust
#[test]
fn test_volume_not_connected() {
    assert_powerfs_mounted();
    
    // 模拟 Volume 断开连接
    // 验证文件写入返回 EIO
    let result = std::fs::write("/mnt/powerfs/test_volume_error.txt", b"test");
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::EIO),
               "Expected EIO when Volume is not connected");
}
```

#### Test 7.3: flush 错误传播

```rust
#[test]
fn test_flush_error_propagation() {
    assert_powerfs_mounted();
    
    // 创建文件并写入数据
    let file = std::fs::File::create("/mnt/powerfs/flush_test.txt").unwrap();
    
    // 模拟 flush 时 Volume 不可用
    // 验证 flush 返回 EIO
    let result = file.sync_all();
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::EIO),
               "Expected EIO when flush fails");
}
```

#### Test 7.4: release 错误传播

```rust
#[test]
fn test_release_error_propagation() {
    assert_powerfs_mounted();
    
    // 创建文件并写入数据
    let mut file = std::fs::File::create("/mnt/powerfs/release_test.txt").unwrap();
    file.write_all(b"test").unwrap();
    
    // 模拟 release 时 Volume 不可用
    // 验证 drop 返回错误
    drop(file);
    // 注意：drop 时的错误不会直接返回，但应记录到日志
}
```

### 7.3 错误处理实现要求

所有 FUSE 方法必须正确处理错误并返回对应错误码：

```rust
fn mkdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, mode: u32, reply: ReplyEntry) {
    match self.client.create_directory(parent, name.to_string_lossy().to_string()) {
        Ok(entry) => {
            let attrs = entry.attributes.as_ref().unwrap();
            reply.entry(&self.attr_timeout, &self.entry_timeout, attrs, 0);
        }
        Err(e) => {
            error!("mkdir: create_directory failed: {}", e);
            let errno = parse_master_error(&e);  // 区分 Master 错误类型
            reply.error(errno);
        }
    }
}
```

### 7.4 readdir 可见性验证

**目录条目必须能被 ls 正确显示**，这要求 readdir 的 offset 计算逻辑正确：

```rust
fn readdir_root(&self, mut reply: ReplyDirectory, offset: i64) {
    let idx = offset as usize;
    
    // "." 和 ".." 条目
    if idx == 0 {
        if !reply.add(1, 1, FileType::Directory, ".") {
            reply.ok();
            return;
        }
    }
    if idx <= 1 {
        if !reply.add(1, 2, FileType::Directory, "..") {
            reply.ok();
            return;
        }
    }
    
    // 目录条目
    match self.client.list_entries(1, 1000, "") {
        Ok(entries) => {
            for (i, entry) in entries.iter().enumerate() {
                let entry_idx = 2 + i;
                if entry_idx >= idx {
                    let child_ino = entry.attributes.as_ref().map(|a| a.ino).unwrap_or(0);
                    let mode_val = entry.attributes.as_ref().map(|a| a.mode).unwrap_or(0);
                    let file_type = mode_val & 0o170000;
                    
                    let kind = match file_type {
                        0o040000 => FileType::Directory,
                        0o120000 => FileType::Symlink,
                        _ => FileType::RegularFile,
                    };
                    
                    let next_offset = (entry_idx + 1) as i64;
                    if !reply.add(child_ino, next_offset, kind, &entry.name) {
                        break;
                    }
                }
            }
        }
        Err(e) => {
            error!("readdir_root: list_entries failed: {}", e);
        }
    }
    
    reply.ok();
}
```

### 7.5 测试验证步骤

1. **验证目录可见性**：创建目录后使用 `ls /mnt/powerfs/` 确认目录可见
2. **验证文件可见性**：在目录中创建文件后使用 `ls /mnt/powerfs/dir/` 确认文件可见
3. **验证错误码**：模拟服务异常场景，确认返回正确的 POSIX 错误码
4. **验证 flush/release**：确保数据写入失败时应用能收到错误通知