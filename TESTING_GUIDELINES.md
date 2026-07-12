# PowerFS 测试规范

## 1. 一键测试脚本

### 使用 run-tests.sh 运行所有测试

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

# 详细日志模式
docker/scripts/run-tests.sh --verbose

# 跳过清理（保留测试环境用于调试）
docker/scripts/run-tests.sh --skip-cleanup

# 跳过 Rust 构建（仅重建 Docker 镜像）
docker/scripts/run-tests.sh --build --skip-build
```

### 测试套件说明

| 测试套件 | 包含的测试 | 说明 |
|---------|-----------|------|
| all | 所有测试 | 完整测试套件（默认） |
| basic | fuse_basic_test | FUSE 基础操作测试 |
| rfs | rfs_tester_fuse_test | RFS 测试集成 |
| volume | volume_integration_test, volume_verification_test | Volume 服务测试 |
| posix | posix_tests | POSIX 兼容性测试 |
| concurrent | concurrent_consistency | 并发一致性测试 |
| mount | mount_verification_test | 挂载点验证测试 |
| coherence | coherence_phase0_test, coherence_phase1_test | 一致性阶段测试 |
| fs | fs_test | 文件系统核心测试 |
| sync | sync_test | 同步操作测试 |
| minimal | fuse_minimal_test | 最小化 FUSE 测试 |
| manual | 手动验证测试 | 目录/文件操作手动验证 |

### run-tests.sh 工作流程

1. **环境预检查**：验证 Docker、Docker Compose、Cargo 是否可用
2. **构建镜像**（可选）：构建 Rust 二进制和 Docker 镜像
3. **启动基础设施**：启动 Redis、Master、Volume、S3 服务
4. **启动 FUSE 客户端**：启动 fuse-1 容器并验证挂载
5. **运行测试**：按套件执行测试，显示详细结果
6. **清理环境**（可选）：停止并清理所有容器

## 2. 测试环境启动流程

### 标准流程

```bash
# 启动完整集群（包含 Redis、Master、Volume、S3、Monitor、Frontend）
docker/scripts/start-cluster.sh [--build]

# 启动 FUSE 客户端（必须在集群启动后执行）
docker/scripts/start-fuse.sh [--build]

# 停止集群
docker/scripts/stop-cluster.sh
```

### 环境验证

```bash
# 检查所有服务状态
docker/scripts/health-check.sh

# 验证 FUSE 挂载（关键步骤）
docker exec fuse-1 mount | grep -E "on /mnt/powerfs type fuse"
# 预期输出: powerfs on /mnt/powerfs type fuse (rw,nosuid,nodev,relatime,user_id=0,group_id=0,default_permissions,allow_other)

# 验证 FUSE 可写
docker exec fuse-1 bash -c "echo 'test' > /mnt/powerfs/test.txt && cat /mnt/powerfs/test.txt"

# 删除测试文件
docker exec fuse-1 rm /mnt/powerfs/test.txt
```

## 3. 测试类型分类

### 2.1 Unit Tests（单元测试）

```bash
# 运行所有单元测试
cargo test --workspace

# 运行特定模块的单元测试
cargo test --manifest-path powerfs-fuse/Cargo.toml
```

### 2.2 Integration Tests（集成测试）

```bash
# 运行 FUSE 集成测试（需在 Docker 环境中运行）
docker exec fuse-1 /app/target/debug/deps/rfs_tester_fuse_test-xxx

# 运行 Volume 验证测试
docker exec fuse-1 /app/target/debug/deps/volume_verification_test-xxx
```

### 2.3 Manual Tests（手动测试）

```bash
# 测试文件创建和读取
docker exec fuse-1 bash -c "echo 'Hello PowerFS' > /mnt/powerfs/test.txt"
docker exec fuse-2 cat /mnt/powerfs/test.txt

# 测试跨客户端一致性
docker exec fuse-1 bash -c "echo 'from fuse-1' > /mnt/powerfs/shared.txt"
docker exec fuse-2 cat /mnt/powerfs/shared.txt

# 测试目录操作
docker exec fuse-1 mkdir -p /mnt/powerfs/test_dir/subdir
docker exec fuse-1 touch /mnt/powerfs/test_dir/file.txt
docker exec fuse-2 ls -la /mnt/powerfs/test_dir/
```

## 4. 测试环境配置

### Docker Compose 文件

- **主配置**: `docker/docker-compose.yml` - 完整集群配置
- **测试配置**: `docker/docker-compose.test.yml` - 单节点测试配置（已废弃，使用主配置）

### 关键端口

| 服务 | 端口 | 说明 |
|------|------|------|
| Redis | 6379 | 缓存服务 |
| Master 1 | 9333 | 主节点 |
| Master 2 | 9334 | 从节点 |
| Master 3 | 9335 | 从节点 |
| Volume 1 | 8080 | 存储节点 |
| Volume 2 | 8081 | 存储节点 |
| Volume 3 | 8082 | 存储节点 |
| S3 Backend | 9000 | 对象存储 |
| Monitor | 8083 | 监控 API |
| Frontend | 8084 | 监控 UI |

### FUSE 挂载点

| 容器 | 挂载点 | 宿主机路径 |
|------|--------|-----------|
| fuse-1 | /mnt/powerfs | /tmp/powerfs/fuse1 |
| fuse-2 | /mnt/powerfs | /tmp/powerfs/fuse2 |

## 5. 测试数据验证

### 4.1 Volume 数据持久化验证

```bash
# 1. 写入测试数据
docker exec fuse-1 bash -c "echo 'persistent data' > /mnt/powerfs/persist.txt"

# 2. 验证写入成功
docker exec fuse-1 cat /mnt/powerfs/persist.txt

# 3. 重启 FUSE 客户端
docker compose -f docker/docker-compose.yml restart fuse-1

# 4. 验证数据持久化
docker exec fuse-1 cat /mnt/powerfs/persist.txt
```

### 4.2 跨客户端一致性验证

```bash
# 1. 在 fuse-1 写入
docker exec fuse-1 bash -c "echo 'shared content' > /mnt/powerfs/shared.txt"

# 2. 在 fuse-2 读取
docker exec fuse-2 cat /mnt/powerfs/shared.txt

# 3. 在 fuse-2 修改
docker exec fuse-2 bash -c "echo 'modified by fuse-2' > /mnt/powerfs/shared.txt"

# 4. 在 fuse-1 验证
docker exec fuse-1 cat /mnt/powerfs/shared.txt
```

## 6. 测试常见问题

### 5.1 FUSE 写入后读取为空

**原因**: content_size 未正确更新

**解决方案**: 
- 检查 FUSE 日志中的 `content_size` 值
- 确保 `flush_dirty_chunks` 正确调用并更新 metadata

### 5.2 端口占用

**原因**: 之前的集群未完全停止

**解决方案**:
```bash
docker/scripts/stop-cluster.sh
docker system prune -f
```

### 5.3 Volume 服务未注册

**原因**: Master 未就绪时 Volume 启动

**解决方案**:
- 等待 Master 就绪后再启动 Volume
- 检查 Redis 连接

### 5.4 测试二进制文件找不到

**原因**: Cargo 目标目录未挂载

**解决方案**:
```bash
# 确保 target 目录挂载到容器
docker run -v /home/portion/powerfs/target:/app/target ...
```

## 7. 测试开发规范

### 6.1 新增测试文件

所有测试文件应放在对应模块的 `tests/` 目录下：

```
powerfs-fuse/tests/
├── rfs_tester_fuse_test.rs       # RFS 测试集成
├── fuse_basic_test.rs            # FUSE 基础操作测试
├── mount_verification_test.rs    # 挂载点验证测试
└── test_harness.rs               # 测试框架
```

### 6.2 挂载点验证（强制要求）

**所有 FUSE 集成测试必须验证挂载点是真正的 PowerFS FUSE 挂载，而非本地文件系统。**

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
    let mount_path = get_mount_path();
    // ... 测试逻辑
}
```

**为什么需要这个验证？**

- 如果挂载点不存在或只是普通目录，测试会在本地文件系统上运行
- 本地文件系统测试通过不代表 PowerFS FUSE 实现正确
- 防止测试"假阳性"通过

### 6.3 测试命名规范

```rust
// 单元测试
#[test]
fn test_function_name() {}

// 集成测试（必须验证挂载点）
#[test]
fn test_integration_scenario_name() {
    assert_powerfs_mounted();
    // ... 测试逻辑
}

// 验证测试
#[test]
fn test_verification_behavior_name() {
    assert_powerfs_mounted();
    // ... 测试逻辑
}
```

### 6.4 测试环境变量

```bash
# Docker 测试环境（推荐）
export POWERFS_DOCKER_TEST=1
export POWERFS_MOUNT=/mnt/powerfs

# 宿主机测试环境（不推荐，需确保 FUSE 已挂载）
export POWERFS_MOUNT=/tmp/powerfs-test/mount
```

## 8. 错误处理标准

### 7.1 错误类型分类

| 错误类型 | 触发条件 | POSIX 错误码 | 说明 |
|---------|---------|-------------|------|
| MasterNotConnected | Master 服务不可用 | ENOTCONN | 连接 Master 失败或 Master 未就绪 |
| MasterError | Master 操作失败 | EIO | Master 服务异常，如目录创建失败 |
| VolumeNotConnected | Volume 服务不可用 | ENOTCONN | 连接 Volume 失败或 Volume 未注册 |
| VolumeError | Volume 操作失败 | EIO | Volume 服务异常，如文件读写失败 |
| NotFound | 文件/目录不存在 | ENOENT | 请求的资源不存在 |
| PermissionDenied | 权限不足 | EACCES | 操作被拒绝 |
| InvalidArgument | 参数无效 | EINVAL | 输入参数错误 |

### 7.2 错误码映射实现

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum FsError {
    MasterNotConnected(String),
    MasterError(String),
    VolumeNotConnected(String),
    VolumeError(String),
    NotFound(String),
    PermissionDenied(String),
    InvalidArgument(String),
    // ... 其他错误类型
}

impl FsError {
    pub fn to_errno(&self) -> i32 {
        match self {
            FsError::MasterNotConnected(_) => libc::ENOTCONN,
            FsError::MasterError(_) => libc::EIO,
            FsError::VolumeNotConnected(_) => libc::ENOTCONN,
            FsError::VolumeError(_) => libc::EIO,
            FsError::NotFound(_) => libc::ENOENT,
            FsError::PermissionDenied(_) => libc::EACCES,
            FsError::InvalidArgument(_) => libc::EINVAL,
            // ... 其他错误码映射
        }
    }
}
```

### 7.3 错误处理测试要求

所有涉及服务异常场景的测试必须验证正确的错误码返回：

```rust
#[test]
fn test_master_disconnect_error() {
    assert_powerfs_mounted();
    
    // 模拟 Master 不可用场景
    // 验证目录操作返回 ENOTCONN
    let result = std::fs::create_dir("/mnt/powerfs/test_dir");
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ENOTCONN));
}

#[test]
fn test_volume_disconnect_error() {
    assert_powerfs_mounted();
    
    // 模拟 Volume 不可用场景
    // 验证文件操作返回 EIO
    let result = std::fs::write("/mnt/powerfs/test.txt", b"test");
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::EIO));
}
```

### 7.4 flush/release 错误传播

**flush 和 release 操作必须传播错误**，确保数据写入失败时应用能收到错误：

```rust
fn flush(&mut self, _req: &Request<'_>, inode: u64, fh: u64, _lock_owner: Option<u64>, reply: ReplyEmpty) {
    debug!("flush: inode={}, fh={}", inode, fh);
    
    if let Err(e) = self.flush_dirty_chunks(inode) {
        error!("flush: flush_dirty_chunks failed: {}", e);
        reply.error(e.to_errno());  // 必须返回错误
        return;
    }
    
    reply.ok();
}

fn release(&mut self, _req: &Request<'_>, inode: u64, fh: u64, _flags: u32, _lock_owner: Option<u64>, _flush: bool, reply: ReplyEmpty) {
    debug!("release: inode={}, fh={}", inode, fh);
    
    if let Err(e) = self.flush_dirty_chunks(inode) {
        error!("release: flush_dirty_chunks failed: {}", e);
        reply.error(e.to_errno());  // 必须返回错误
        return;
    }
    
    self.file_handles.remove(&fh);
    reply.ok();
}
```

## 9. 测试检查清单

### 启动前检查

- [ ] Docker 和 Docker Compose 已安装
- [ ] 6379、9333-9335、8080-8084 端口未被占用
- [ ] 之前的集群已完全停止

### 测试中检查

- [ ] Redis 服务正常运行（`docker exec redis redis-cli ping`）
- [ ] Master 节点正常运行（`nc -z localhost 9333`）
- [ ] Volume 节点已注册（`nc -z localhost 8080`）
- [ ] FUSE 客户端已挂载（`docker exec fuse-1 ls /mnt/powerfs`）
- [ ] **挂载点验证**：确认 `/mnt/powerfs` 是 FUSE 挂载（`docker exec fuse-1 mount | grep fuse`）

### 测试后验证

- [ ] 文件写入后可正确读取
- [ ] 数据在 FUSE 重启后持久化
- [ ] 跨客户端数据一致性
- [ ] 测试日志无错误

### 错误处理验证

- [ ] Master 不可用时目录操作返回 ENOTCONN
- [ ] Master 异常时目录操作返回 EIO
- [ ] Volume 不可用时文件操作返回 ENOTCONN
- [ ] Volume 异常时文件操作返回 EIO
- [ ] flush/release 失败时返回 EIO
