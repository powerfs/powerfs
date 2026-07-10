# FUSE Coherence End-to-End Tests

端到端测试脚本用于验证 PowerFS FUSE 缓存一致性（Coherence）功能的三个阶段。

## 文件结构

```
scripts/
├── coherence_test_common.sh      # 公共测试库（服务启动/停止、断言函数等）
├── test_coherence_phase0.sh      # Phase 0: 同步提交 + 错误回滚测试
├── test_coherence_phase1.sh      # Phase 1: 服务器驱动缓存失效测试
├── test_coherence_phase2.sh      # Phase 2: 租约机制测试
└── test_coherence_all.sh         # 综合测试入口（运行所有阶段）
```

## 快速开始

### 运行所有测试
```bash
./scripts/test_coherence_all.sh
```

### 运行单个阶段
```bash
# 仅运行 Phase 0
./scripts/test_coherence_all.sh --phase0

# 仅运行 Phase 1
./scripts/test_coherence_all.sh --phase1

# 仅运行 Phase 2
./scripts/test_coherence_all.sh --phase2
```

### 运行指定阶段组合
```bash
./scripts/test_coherence_all.sh --phases "0,1"    # 运行 Phase 0 和 1
./scripts/test_coherence_all.sh --phases "02"     # 运行 Phase 0 和 2
```

## 各阶段测试内容

### Phase 0: 同步提交 + 错误回滚
验证元数据操作从 warn-and-continue 模式改为同步提交模式。

**测试用例:**
1. `mkdir synchronous creation` - 目录同步创建
2. `nested mkdir synchronous creation` - 嵌套目录同步创建
3. `file create synchronous` - 文件同步创建
4. `unlink synchronous deletion` - 文件同步删除
5. `rmdir synchronous deletion` - 目录同步删除
6. `rename synchronous` - 文件重命名
7. `rename directory synchronous` - 目录重命名
8. `setattr (chmod) synchronous` - 属性同步修改
9. `symlink synchronous creation` - 符号链接创建
10. `hard link synchronous creation` - 硬链接创建
11. `persistence across FUSE restart` - FUSE 重启后数据持久化
12. `multi-operation sequence consistency` - 多操作序列一致性

### Phase 1: 服务器驱动缓存失效
验证 Master 在元数据变更时主动推送失效通知，客户端订阅并更新本地缓存。

**测试用例:**
1. `cache invalidation - file creation` - 文件创建缓存失效
2. `cache invalidation - file deletion` - 文件删除缓存失效
3. `cache invalidation - directory creation` - 目录创建缓存失效
4. `cache invalidation - directory deletion` - 目录删除缓存失效
5. `cache invalidation - rename` - 重命名缓存失效
6. `cache invalidation - attribute change` - 属性变更缓存失效
7. `generation number increment` - Generation 号递增验证
8. `directory listing update after invalidation` - 目录列表更新
9. `multiple rapid changes handling` - 快速多次变更处理
10. `nested directory cache invalidation` - 嵌套目录缓存失效

### Phase 2: 租约机制
验证文件打开时申请租约，持有期间 Master 不推送失效通知，关闭时释放租约。

**测试用例:**
1. `lease acquisition on file open` - 文件打开时租约获取
2. `lease protection during file modification` - 文件修改期间租约保护
3. `lease release on file close` - 文件关闭时租约释放
4. `multiple files with leases` - 多文件同时持有租约
5. `lease with write operations` - 写操作期间的租约
6. `concurrent access with leases` - 带租约的并发访问
7. `directory listing with file leases` - 持有文件租约时的目录列表
8. `lease across multiple open/close cycles` - 多次打开/关闭循环的租约
9. `file size consistency with leases` - 租约期间文件大小一致性
10. `lease cleanup on FUSE unmount` - FUSE 卸载时租约清理

## 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `MOUNT_DIR` | `/tmp/powerfs-coherence-test` | 第一个 FUSE 挂载点 |
| `MOUNT2_DIR` | `/tmp/powerfs-coherence-test2` | 第二个 FUSE 挂载点（Phase 1/2） |
| `MASTER_DIR` | `/tmp/powerfs-coherence-master` | Master 数据目录 |
| `VOLUME_DIR` | `/tmp/powerfs-coherence-volume` | Volume 数据目录 |
| `MASTER_PORT` | `9460` | Master HTTP 端口 |
| `VOLUME_PORT` | `8197` | Volume gRPC 端口 |

## 日志

测试运行时，各服务的日志输出到以下文件：
- Master: `/tmp/coherence-test-master.log`
- Volume: `/tmp/coherence-test-volume.log`
- FUSE 1: `/tmp/coherence-test-fuse.log`
- FUSE 2: `/tmp/coherence-test-fuse2.log`

## 退出码

- `0`: 所有测试通过
- `1`: 有测试失败

## 注意事项

1. 需要 root 权限或 fuse 用户组权限来挂载 FUSE 文件系统
2. 测试会自动清理临时文件和进程
3. 某些测试用例可能因环境限制被跳过（SKIP），不影响整体结果
4. 建议在空闲系统上运行，避免资源竞争影响测试结果
