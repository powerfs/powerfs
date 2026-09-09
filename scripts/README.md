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

### 硬件 RDMA kernel-VM 测试流程 (kernel/vm)

PowerFS 内核模块必须在 QEMU VM 里测试（VM 直通 SR-IOV VF RDMA）。
这是性能测试的主路径 — 完整的一键流程如下。

### 环境前置条件

| 项 | 值 |
|----|----|
| Host kernel | Linux 5.15+, GRUB: `intel_iommu=on iommu=pt` |
| 硬件 RDMA | mlx5_1 端口 ACTIVE (如 `ip link set ibp160s0f1 up`), IPoIB 配置 `192.168.100.3/24` |
| SR-IOV VF | mlx5_1 已创建 2 个 VF, 绑定 `vfio-pci` (见 kernel/vm/qemuctl2.sh setup-vf) |
| Rust 工具链 | nightly (rdma 特性需要), `cargo build --release -p powerfs-filer -p powerfs-volume -p powerfs-master --features rdma` |
| VM kernel | 6.17 (source at `/home/portion/powerfs/linux-6.17`) |

### 一键启动 (推荐)

```bash
cd /home/portion/powerfs

# 1. 编译 Rust 二进制 (只跑一次, 后续改动后重编)
cargo build --release -p powerfs-filer -p powerfs-volume -p powerfs-master --features rdma

# 2. 编译 ko + initramfs (WRITE_PREDICT=y 默认开启 dedup 方向②异步路径)
#    环境变量可覆盖: WRITE_PREDICT=n 编译无 dedup 的 ko
cd kernel/vm && bash qemuctl.sh build

# 3. 启动 Docker RDMA 服务 (master + 3 volume + filer + redis)
#    自动 generate-certs.sh + sync 到 VM share + restart 存储节点
bash qemuctl.sh service start --rdma

# 4. 启动 2 台 QEMU VM (各自一个 VF)
bash qemuctl2.sh start

# 5. VM1 mount (自动从 9p share 同步最新证书到 /etc/powerfs/)
bash qemuctl2.sh mount vm1

# 6. 验证
bash qemuctl2.sh exec vm1 "time dd if=/dev/zero of=/mnt/powerfs/t.bin bs=1M count=128 conv=fdatasync"
```

### 脚本清单

| 脚本 | 路径 | 说明 |
|------|------|------|
| `qemuctl.sh` | `kernel/vm/qemuctl.sh` | ko 编译, initramfs 打包, Docker 服务启停 (TCP + RDMA), RDMA 环境自检 |
| `qemuctl2.sh` | `kernel/vm/qemuctl2.sh` | QEMU VM 启停, VF 直通, SSH/EXEC, mount (含自动证书同步) |
| `generate-certs.sh` | `scripts/generate-certs.sh` | master CA 签发所有 leaf 证书 (filer, volume-1/2/3, kernel-client-1/2) |
| `build.sh` | `scripts/build.sh` | 全部 Rust 二进制编译 (替代手动 `cargo build`) |
| `build-rdma.sh` | `scripts/build-rdma.sh` | 带 RDMA 特性的 Rust 编译 |
| `docker-compose.rdma.yml` | `docker/docker-compose.rdma.yml` | 硬件 RDMA host-network compose (3 volume, 独立于 docker-compose.yml) |

### 架构图

```
  Host (192.168.100.3)                     QEMU VM1          QEMU VM2
  ┌──────────────────────┐                ┌──────────┐       ┌──────────┐
  │ mlx5_1 硬件 RDMA      │                │ ib0 VF   │       │ ib0 VF   │
  │ uverbs1 + rdma_cm     │                │ 192.168  │       │ 192.168  │
  │ /dev/infiniband/*     │                │ .100     │       │ .101     │
  └───────┬──────────────┘                └────┬─────┘       └────┬─────┘
          │ SR-IOV VF 直通                     │                  │
  ┌───────┴────────────────────────────────────┴──────────────────┴──────┐
  │                         RDMA 网络 (RoCE)                             │
  └──────────────────────────────────────────────────────────────────────┘
          │
  ┌───────┴──────────────────────────────────────────────────────────────┐
  │ Docker containers (network_mode: host, 共享 host netns)              │
  │   master-1   :9333 (TCP raft)  :9334 (RDMA net)   :9300 (metrics)   │
  │   filer-1    :8888 (HTTP)       :8889 (gRPC)       :9336 (RDMA net)   │
  │   volume-1   :8080 (gRPC)       :8901 (RDMA net)                     │
  │   volume-2   :8081 (gRPC)       :8902 (RDMA net)                     │
  │   volume-3   :8082 (gRPC)       :8903 (RDMA net)                     │
  └──────────────────────────────────────────────────────────────────────┘
          │ redis 172.30.0.50:6379 (bridge network)
```

### RDMA 队列参数表 (powerfs-net + kernel)

**ENOTCONN (-107) / RNR NAK / CQ overflow 通常是队列深度不足导致的.**
以下是 `powerfs-net/src/transport_rdma.rs` + `transport.rs` 中的调优参数.

| 参数 | 旧值 | 新值 | 公式 | 说明 |
|------|------|------|------|------|
| **Rust 客户端** (filer→volume) | | | | |
| `send_cq` / `recv_cq` | 32 | **128** | 2 × (64+64) | CQ 深度留 100% 余量防 overflow |
| `max_send_wr` | 16 | **64** | ≥ 2× kernel burst | send queue 足够 ACK + read 响应 |
| `max_recv_wr` | 16 | **64** | ≥ 2× kernel burst | recv queue 不触发 RNR NAK |
| `max_send_sge` / `max_recv_sge` | 1 | **2** | | 大帧多段 (inline 0→256 也配合) |
| `max_inline_data` | 0 | **256** | | 小数据 inline 零拷贝 |
| `pre_post_recv()` | 16 | **32** | ≥ kernel burst × 1.5 | 预投递 recv WR, 防 RQ 空 |
| **Rust 服务端** (accept 新连接) | | | | |
| `send_cq` / `recv_cq` | 64 | **256** | 2 × (64+64) | 客户端 CQ 也要对应用量 |
| `max_send_wr` | 16 | **64** | ≥ 2× kernel burst | 服务端 send 给客户端的 ACK/read |
| `max_recv_wr` | 32 | **64** | ≥ kernel burst × 2 | 接收 kernel write_needle 突发 |
| `PRE_POST_N` (服务端 RQ 预投递) | 24 | **48** | ≥ kernel burst × 1.5 | 48 对应 kernel 单连接 write burst cap=32 |
| **TransportConfig 默认** | | | | |
| `rdma_buf_num` (MR pool 大小) | 32 | **128** | ≥ PRE_POST_N + send_slack | 48 recv + 32 send + 48 slack |
| `rdma_buf_size` | 2MB | 2MB | | 与 kernel `PFS_RDMA_DATA_BUF_SIZE` 对齐 |
| **Kernel 客户端** (powerfs_net_rdma.h) | | | | |
| `CQ_SEND_SIZE` / `CQ_RECV_SIZE` | 128/64 | 128/64 | | 已足够, 无需调整 |
| `QP_MAX_SEND_WR` / `QP_MAX_RECV_WR` | 64/32 | 64/32 | | 已足够 |
| `PRE_POST_N` (kernel RQ 预投递) | 24 | 24 | | 服务端已扩, 客户端无需 |
| `WR_SLOT_PER_CONN` (kernel per-conn in-flight cap) | 16 | 16 | | 配合服务端 PRE_POST_N=48 (1.5× burst) |

**调参 checklist (改完必须做):**
```bash
# 改 Rust 参数后重编 + restart
cargo build --release -p powerfs-filer -p powerfs-volume -p powerfs-master --features rdma
cd docker && docker compose -f docker-compose.rdma.yml restart volume-1 volume-2 volume-3 filer-1

# 改 kernel 参数后重编 ko + rebuild initramfs + restart VM
cd kernel/vm && bash qemuctl.sh build && bash qemuctl2.sh stop && bash qemuctl2.sh start && bash qemuctl2.sh mount vm1
```

### WRITE_PREDICT 内核编译选项

`powerfs-net/src/powerfs_write_predict.c` 实现了异步 dedup 方向②:
热路径零阻塞 (lockless READ_ONCE 检查), SHA-256 + FingerprintLookup/Record 全部下沉到 workqueue.

```bash
# 开启 dedup (默认):
bash qemuctl.sh build                  # WRITE_PREDICT=y
ls -la powerfs.ko                      # 应是 16MB

# 关闭 dedup (对照实验):
WRITE_PREDICT=n bash qemuctl.sh build  # 15MB ko, dedup 字段 #ifdef out

# 验证 ko 是否包含 write_predict 符号:
nm powerfs.ko | grep write_predict
```

### 端口规划 (host-network 必须唯一)

| 服务 | TCP/gRPC | HTTP | RDMA net | 其他 |
|------|----------|------|----------|------|
| master-1 | 9333 (raft) | 9300 (metrics) | 9334 | — |
| volume-1 | 8080 | 8091 | 8901 | — |
| volume-2 | 8081 | 8092 | 8902 | — |
| volume-3 | 8082 | 8093 | 8903 | — |
| filer-1 | 8889 (gRPC) | 8888 | 9336 | 8900 (metrics) |
| redis | 172.30.0.50:6379 (bridge) | — | — | — |

> Docker TCP 模式 (docker-compose.yml) 端口不同, **RDMA compose 和 TCP compose 互斥**:
> 启动 RDMA 前必须 `docker compose -f docker-compose.yml down`, 反之亦然.

### 常见故障排查

| 现象 | 原因 | 修复 |
|------|------|------|
| `mount denied by master blacklist: client certificate rejected` | VM 用了 initramfs 里打包的旧证书, master CA 已变 | `qemuctl2.sh mount` 会自动从 9p share 同步. 如还不行: `qemuctl.sh service start --rdma` (自动 generate certs + restart 存储节点) |
| `Master filer discovery failed (-107)` | master 看不到已注册的 filer/volume, 通常是 filer/volume 重启后没带上新 leaf 证书 | 手动 `docker compose -f docker-compose.rdma.yml restart filer-1 volume-1 volume-2 volume-3`; 或看 master 日志确认无 `missing ClientCert TLV` |
| `ENOTCONN / Transport endpoint is not connected` (写路径) | RDMA CQ overflow 或 RNR NAK 耗尽 — Rust 侧 CQ/QP 深度不足 | 看上面的 "RDMA 队列参数表", 改完重编 Rust + restart 容器 |
| `write_needle failed: -107, needle_id=... leaked` | 同上, 服务端 RQ 空 → RNR NAK → 退避 → 超时 | 检查 PRE_POST_N 和 rdma_buf_num |
| `failed to read client cert /etc/powerfs/certs/filer-1.crt: No such file` | 容器启动时证书还没生成, 进程读过空文件 | `generate-certs.sh` 后必须 restart 对应容器 |
| Docker compose 报端口冲突 | TCP compose 和 RDMA compose 同时在跑 | `docker compose -f docker-compose.yml down` |
| `/dev/infiniband/uverbs2 不存在` (旧错误) | 硬件 RDMA 用 uverbs0/uverbs1, uverbs2 是 Soft-RoCE | `qemuctl.sh service start --rdma` 已改为探测 uverbs[0-4], 不再硬检查 |

### 验证命令速查

```bash
# Docker 服务状态
docker ps --format 'table {{.Names}}\t{{.Status}}'

# Master 注册状态 (3 volume + 1 filer 全部 OK 才算正常)
docker logs master-1 --tail 50 | grep -iE 'registered filer|node=volume-server'

# RDMA 队列参数生效验证
docker logs filer-1 2>&1 | grep -iE 'RdmaTransport.*buf_num|MrPool.*registered'
# 期望看到: buf_num=128, registered 128 buffers

# VM mount + RDMA 连接验证
qemuctl2.sh exec vm1 "dmesg | grep -iE 'rdma.*connected|handshake|RDMA_ERR'"

# DD 快速验证 (1M seqwrite ≥ 300 MB/s, 4K randread ≥ 300 MB/s)
qemuctl2.sh exec vm1 "time dd if=/dev/zero of=/mnt/powerfs/t.bin bs=1M count=128 conv=fdatasync"

# FIO 套件
qemuctl2.sh exec vm1 "bash /mnt/host/fio_pfs_rdma.sh vm1"
```

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
