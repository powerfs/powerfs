# PowerFS 测试规范

## 1. 测试环境启动流程

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

# 验证 FUSE 挂载
docker exec fuse-1 bash -c "echo 'test' > /mnt/powerfs/test.txt && cat /mnt/powerfs/test.txt"
```

## 2. 测试类型分类

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

## 3. 测试环境配置

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

## 4. 测试数据验证

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

## 5. 测试常见问题

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

## 6. 测试开发规范

### 6.1 新增测试文件

所有测试文件应放在对应模块的 `tests/` 目录下：

```
powerfs-fuse/tests/
├── rfs_tester_fuse_test.rs      # RFS 测试集成
├── volume_verification_test.rs  # Volume 验证测试
└── test_harness.rs              # 测试框架
```

### 6.2 测试命名规范

```rust
// 单元测试
#[test]
fn test_function_name() {}

// 集成测试
#[test]
fn test_integration_scenario_name() {}

// 验证测试
#[test]
fn test_verification_behavior_name() {}
```

### 6.3 测试环境变量

```bash
# Docker 测试环境
export POWERFS_DOCKER_TEST=1
export POWERFS_MOUNT=/mnt/powerfs

# 宿主机测试环境（已废弃）
export POWERFS_MOUNT=/tmp/powerfs-test/mount
```

## 7. 测试检查清单

### 启动前检查

- [ ] Docker 和 Docker Compose 已安装
- [ ] 6379、9333-9335、8080-8084 端口未被占用
- [ ] 之前的集群已完全停止

### 测试中检查

- [ ] Redis 服务正常运行（`docker exec redis redis-cli ping`）
- [ ] Master 节点正常运行（`nc -z localhost 9333`）
- [ ] Volume 节点已注册（`nc -z localhost 8080`）
- [ ] FUSE 客户端已挂载（`docker exec fuse-1 ls /mnt/powerfs`）

### 测试后验证

- [ ] 文件写入后可正确读取
- [ ] 数据在 FUSE 重启后持久化
- [ ] 跨客户端数据一致性
- [ ] 测试日志无错误
