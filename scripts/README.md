# PowerFS Test Scripts

PowerFS 测试脚本集合，支持容器化和本地二进制两种测试模式。

## 目录结构

```
scripts/
├── env/                        # 环境管理
│   ├── start-env.sh           # 启动测试环境
│   ├── stop-env.sh            # 停止测试环境
│   └── cleanup.sh             # 清理环境
├── tests/                      # 测试脚本
│   ├── coherence/             # 一致性测试
│   │   ├── run_all.sh         # 运行所有阶段测试
│   │   └── phase0_sync.sh     # 同步提交测试
│   ├── posix/                 # POSIX 功能测试
│   │   └── run_tests.sh       # POSIX 操作测试
│   ├── perf/                  # 性能测试
│   │   └── run_bench.sh       # fio 性能基准测试
│   └── failover/              # 故障转移测试
│       └── run_e2e.sh         # 故障转移 E2E 测试
├── lib/                        # 公共库
│   └── common.sh              # 公共函数和配置
└── README.md                  # 本文档
```

## 快速开始

### 1. 启动测试环境

**使用本地二进制：**
```bash
# 启动环境（自动构建）
./scripts/env/start-env.sh

# 跳过构建（已编译时）
./scripts/env/start-env.sh --no-build
```

**使用 Docker：**
```bash
./scripts/env/start-env.sh --docker
```

### 2. 运行测试

**运行所有一致性测试：**
```bash
./scripts/tests/coherence/run_all.sh
```

**运行特定阶段：**
```bash
# 仅运行 Phase 0
./scripts/tests/coherence/run_all.sh --phase0

# 运行 Phase 0 和 2
./scripts/tests/coherence/run_all.sh --phases "0,2"
```

**运行 POSIX 功能测试：**
```bash
./scripts/tests/posix/run_tests.sh
```

**运行性能测试：**
```bash
# 使用默认 fio 引擎
./scripts/tests/perf/run_bench.sh

# 使用 libaio 引擎
./scripts/tests/perf/run_bench.sh --engine=libaio

# 带 fsync 测试
./scripts/tests/perf/run_bench.sh --engine=sync --fsync=1
```

**运行故障转移测试：**
```bash
./scripts/tests/failover/run_e2e.sh
```

### 3. 停止和清理

```bash
# 停止本地环境
./scripts/env/stop-env.sh

# 清理所有环境
./scripts/env/cleanup.sh

# 清理 Docker 环境
./scripts/env/cleanup.sh --docker

# 强制清理一切
./scripts/env/cleanup.sh --force --docker
```

## 测试阶段说明

### Phase 0: 同步提交 + 错误回滚
验证元数据操作的同步提交和错误传播机制。

测试项：
- mkdir 同步创建
- 嵌套 mkdir
- 文件创建
- 文件删除
- 目录删除
- 文件/目录重命名
- 属性修改 (chmod)
- 符号链接
- 硬链接
- 重启后数据持久化
- 多操作序列一致性

### Phase 1: 服务器驱动缓存失效
验证多客户端间的缓存失效机制（待实现）。

### Phase 2: Lease 机制
基于 Rust 集成测试的 Lease 一致性验证。

```bash
cargo test --package powerfs-master --test coherence_phase2_test
```

### Phase 3: Job 级强一致性
基于 Rust 集成测试的 Job 完成通知机制验证。

```bash
cargo test --package powerfs-master --test coherence_phase3_test
```

## 配置说明

### 默认端口
| 服务 | 端口 | 说明 |
|------|------|------|
| Master (HTTP) | 9460 | Master 服务端口 |
| Master (Net) | 9461 | Master 网络协议端口 |
| Volume (gRPC) | 8197 | Volume 服务端口 |
| Volume (HTTP) | 8198 | Volume HTTP 端口 |
| Filer (Net) | 8890 | Filer 网络协议端口 |

### 默认路径
| 路径 | 说明 |
|------|------|
| /tmp/powerfs-test | FUSE 挂载点 |
| /tmp/powerfs-test-master | Master 数据目录 |
| /tmp/powerfs-test-volume | Volume 数据目录 |
| /tmp/powerfs-test-filer | Filer 数据目录 |

### 自定义配置
通过环境变量覆盖默认值：

```bash
MOUNT_DIR=/my/mount \
MASTER_PORT=9000 \
./scripts/tests/posix/run_tests.sh
```

## 前置条件

### 本地测试
- Rust toolchain (stable)
- fuse3 开发库 (`libfuse3-dev`)
- `fio` (性能测试可选)
- `bc` (计算辅助)

### Docker 测试
- Docker Engine
- Docker Compose

### 安装依赖 (Ubuntu)
```bash
sudo apt update
sudo apt install -y fuse3 libfuse3-dev fio bc
```

## 故障排查

### 查看日志
```bash
# 最近的测试日志
cat /tmp/powerfs-test-master.log
cat /tmp/powerfs-test-fuse.log
cat /tmp/powerfs-test-volume.log
cat /tmp/powerfs-test-filer.log
```

### 环境问题
```bash
# 强制清理环境
./scripts/env/cleanup.sh --force

# 检查残留进程
ps aux | grep powerfs

# 检查挂载点
mount | grep powerfs
```

### 常见问题

**Q: FUSE 挂载失败？**
```bash
# 检查 /dev/fuse 权限
ls -la /dev/fuse
sudo chmod 666 /dev/fuse
```

**Q: 端口被占用？**
```bash
# 检查端口占用
ss -tlnp | grep 9460
# 或使用 lsof
lsof -i :9460
```

**Q: Docker 环境启动失败？**
```bash
# 重新构建镜像
cd docker && docker build -t powerfs:latest .
# 查看容器日志
docker compose -f docker-compose.test.yml logs
```

## 脚本依赖关系

```
scripts/
├── lib/common.sh (公共库)
│   ├── setup_test_env()      # 环境配置
│   ├── cleanup_test_env()    # 环境清理
│   ├── build_binaries()      # 构建二进制
│   ├── start_master/volume/filer/fuse()  # 启动各服务
│   └── test framework        # 测试函数
│
├── env/* (环境管理)
│   └── source lib/common.sh
│
└── tests/* (测试脚本)
    └── source lib/common.sh
```

## 与旧脚本的映射

| 旧脚本 | 新位置 | 状态 |
|--------|--------|------|
| scripts/start-cluster.sh | scripts/env/start-env.sh | 已更新 |
| scripts/stop-cluster.sh | scripts/env/stop-env.sh | 已更新 |
| scripts/force_cleanup.sh | scripts/env/cleanup.sh | 已更新 |
| scripts/test_coherence_all.sh | scripts/tests/coherence/run_all.sh | 已更新 |
| scripts/test_coherence_phase0.sh | scripts/tests/coherence/phase0_sync.sh | 已更新 |
| scripts/test_coherence_phase1.sh | - | 标记为待实现 |
| scripts/test_coherence_phase2.sh | Rust 集成测试 | 已整合 |
| scripts/test_coherence_phase3.sh | Rust 集成测试 | 已整合 |
| scripts/run_posix_test.sh | scripts/tests/posix/run_tests.sh | 已更新 |
| scripts/run_fio_test.sh | scripts/tests/perf/run_bench.sh | 已更新 |
| scripts/run_failover_e2e.sh | scripts/tests/failover/run_e2e.sh | 已更新 |
| scripts/perf_test.sh | scripts/tests/perf/run_bench.sh | 已更新 |
