# PowerFS 全面测试方案

## 概述

本文档基于对当前项目代码的审计，制定完整的分层测试方案。当前项目处于 **Phase 8**（与 SeaweedFS 功能对齐），已完成 Volume、Master/Filer、FUSE 客户端的 API 对齐和核心功能实现。

---

## 一、当前测试覆盖现状

### 1.1 测试文件汇总

| Crate | 测试文件 | 测试用例数 | 状态 |
|-------|---------|-----------|------|
| powerfs-common | 5 (utils_test, types_test, error_test, storage_keys_test, constants_test) | ~157 | ✓ 高覆盖率 (~97%) |
| powerfs-core | 5 (index_test, kv_cache_test, volume_test, storage_manager_test, needle_test) | ~74 | ✓ 高覆盖率 (~98%) |
| powerfs-master | 3 (raft_integration_test, e2e_test, cluster.rs) | ~11 | ⚠️ 仅 Raft 集成测试，Filer API 未测试 |
| powerfs-fuse | 1 (fs_test.rs) | 25 | ✅ MetadataCache 完整测试 |
| powerfs-volume | 0 | 0 | ❌ 无测试 |
| powerfs-server | 0 | 0 | ❌ 无测试 |
| powerfs-cli | 0 | 0 | ❌ 无测试 |

### 1.2 各 Crate 测试详情

**powerfs-fuse/tests/fs_test.rs**（25 个用例）：
- Inode 分配与管理（单调递增、唯一性）
- 文件/目录插入、查找、删除
- 目录子项列表（排除自身、多文件）
- 文件重命名（同目录、跨目录、不存在文件）
- 符号链接创建与读取
- 硬链接计数（递增、递减、零删除）
- LRU 缓存淘汰策略
- 扩展属性（set/get/list/remove）
- 属性更新（权限、大小、所有者、时间戳）
- ctime 自动更新

**powerfs-core**（5 个测试文件）：
- index_test.rs - MemoryIndex 操作测试（14 个用例）
- kv_cache_test.rs - KV 缓存并发测试（12 个用例）
- needle_test.rs - Needle 序列化/反序列化
- storage_manager_test.rs - 存储管理器测试
- volume_test.rs - Volume 读写测试（26 个用例）

**powerfs-master**（3 个测试文件）：
- raft_integration_test.rs - Raft 集成测试（5 个用例）：单节点 Leader、两节点选举、快照创建、三节点故障转移、状态机一致性
- e2e_test.rs - E2E 测试（6 个用例）：Raft gRPC 基础、集群信息端点、Raft 客户端、配置解析、带 peers 配置、Raft 节点生命周期
- cluster.rs - 测试集群基础设施（非测试用例）

---

## 二、测试金字塔总览

```
                    ┌──────────────────┐
                    │    E2E 测试       │  全链路：启停集群 → 挂载 → 读写 → 卸载
                    │   (~25 用例)      │
                    ├──────────────────┤
                    │    集成测试       │  gRPC 端到端、多节点 Raft、FUSE 文件操作
                    │   (~90 用例)      │
                    ├──────────────────┤
                    │    单元测试       │  每个 public 函数/方法、每个错误路径
                    │   (~350+ 用例)    │
                    └──────────────────┘
```

### 测试策略原则

| 维度 | 策略 |
|------|------|
| 覆盖率目标 | 行覆盖率 > 85%，分支覆盖率 > 75% |
| 测试独立性 | 每个测试独立创建 tempdir，测试后自动清理 |
| 确定性 | 不依赖 wall-clock 时间；通过 `tokio::time::pause` 控制异步时间 |
| 分层执行 | 单元测试 < 5s / 集成测试 < 30s / E2E < 120s |
| CI 集成 | 每次 push/PR 触发全量 `cargo test --all`；main 分支合并触发 benchmark |

---

## 三、文件系统测试方案讨论

### 3.1 是否需要针对安装点进行文件系统测试？

**是的，非常必要。** 原因如下：

1. **功能验证**：挂载后的文件系统行为与单元测试有本质区别，需要验证完整的 POSIX 语义
2. **边界场景**：真实挂载会触发更多内核路径和边界条件（如文件句柄管理、缓存一致性、权限检查）
3. **兼容性**：确保与标准 Unix 工具（ls、cp、rm、mv、cat、find、grep 等）兼容
4. **端到端验证**：验证从用户空间到存储后端的完整数据路径

### 3.2 标准文件系统测试工具

| 工具 | 用途 | 适用场景 | 优先级 |
|------|------|----------|--------|
| **xfstests** | Linux 文件系统标准测试套件（1000+ 用例） | 验证 POSIX 兼容性、崩溃恢复、权限管理 | P0 |
| **fsx** | 文件系统压力测试工具（随机文件操作） | 边界条件测试、随机故障注入 | P1 |
| **dd** | 数据写入/读取 | 大文件测试、IO 吞吐量验证 | P2 |
| **cp/rm/mv** | 标准文件操作 | 基本功能验证 | P2 |
| **rsync** | 同步测试 | 文件复制、增量同步 | P2 |
| **find/grep** | 文件查找 | 目录遍历、文件名匹配 | P2 |
| **fallocate** | 文件预分配 | 稀疏文件、空间预留 | P2 |
| **truncate** | 文件截断 | 大小调整、边界条件 | P2 |
| **getfattr/setfattr** | 扩展属性 | xattr 功能验证 | P2 |
| **chmod/chown/setfacl** | 权限管理 | ACL、权限位验证 | P2 |

### 3.3 xfstests 集成方案（推荐）

xfstests 是 Linux 内核社区维护的文件系统测试套件，包含 1000+ 测试用例，覆盖：
- POSIX 语义验证（rename、hardlink、symlink）
- 权限和 ACL
- 扩展属性（xattr）
- 文件系统一致性
- 崩溃恢复
- 性能基准

#### 集成步骤

**步骤 1：安装 xfstests**
```bash
# Ubuntu/Debian
sudo apt-get install xfstests

# 或从源码编译
git clone https://git.kernel.org/pub/scm/fs/xfs/xfstests-dev.git
cd xfstests-dev
make
```

**步骤 2：创建测试配置**
```bash
cat > /etc/xfstests/powerfs <<EOF
export TEST_DIR=/mnt/powerfs
export SCRATCH_MNT=/mnt/powerfs-scratch
export MKFS_OPTIONS=""
export FSTYP="fuse.powerfs"
export TEST_DEV="/dev/null"
EOF
```

**步骤 3：运行测试**
```bash
# 运行通用测试
sudo xfstests -c powerfs -g generic

# 运行特定测试组
sudo xfstests -c powerfs -g rename
sudo xfstests -c powerfs -g xattr
sudo xfstests -c powerfs -g permissions
```

### 3.4 挂载点测试实现方案

#### 方案 A：Rust 集成测试（推荐 - 立即执行）

使用 `fuse-backend-rs` 的测试模式，在内存中模拟挂载：

```rust
// tests/mount_test.rs
use fuse_backend_rs::channel::{Channel, ChannelReceiver};
use powerfs_fuse::fuse::PowerFsFs;
use std::path::Path;
use std::process::Command;
use tempfile::TempDir;

#[test]
fn test_mount_basic_ops() {
    let temp_dir = TempDir::new().unwrap();
    let mount_point = temp_dir.path().join("mount");
    std::fs::create_dir(&mount_point).unwrap();
    
    let fs = PowerFsFs::new();
    let session = fs.mount(&mount_point).unwrap();
    
    let result = Command::new("touch")
        .arg(mount_point.join("test.txt"))
        .status()
        .unwrap();
    assert!(result.success());
    
    let result = Command::new("sh")
        .arg("-c")
        .arg(format!("echo 'hello' > {}", mount_point.join("test.txt").display()))
        .status()
        .unwrap();
    assert!(result.success());
    
    let output = Command::new("cat")
        .arg(mount_point.join("test.txt"))
        .output()
        .unwrap();
    assert_eq!(String::from_utf8_lossy(&output.stdout), "hello\n");
    
    session.unmount().unwrap();
}
```

#### 方案 B：Shell 脚本集成测试（进阶）

编写独立的测试脚本，在 CI 环境中执行：

```bash
#!/bin/bash
# tests/fs_integration.sh

set -e

MOUNT_DIR=$(mktemp -d)
TMP_DIR=$(mktemp -d)

./target/debug/powerfs-master &
MASTER_PID=$!
sleep 2

./target/debug/powerfs-volume --master localhost:50051 &
VOLUME_PID=$!
sleep 2

./target/debug/powerfs-fuse --mount $MOUNT_DIR &
FUSE_PID=$!
sleep 3

echo "=== 测试文件创建 ==="
touch $MOUNT_DIR/test.txt
echo "test content" > $MOUNT_DIR/test.txt
cat $MOUNT_DIR/test.txt

echo "=== 测试目录操作 ==="
mkdir -p $MOUNT_DIR/subdir/nested
touch $MOUNT_DIR/subdir/file.txt

echo "=== 测试文件大小 ==="
dd if=/dev/urandom of=$MOUNT_DIR/large.bin bs=1M count=10
ls -lh $MOUNT_DIR/large.bin

echo "=== 测试删除 ==="
rm $MOUNT_DIR/test.txt
rm -rf $MOUNT_DIR/subdir

umount $MOUNT_DIR
kill $FUSE_PID $VOLUME_PID $MASTER_PID
rm -rf $MOUNT_DIR $TMP_DIR

echo "所有测试通过!"
```

#### 方案 C：xfstests 集成（长期目标）

集成 xfstests 测试套件，运行标准文件系统测试。

### 3.5 挂载点测试用例规划

#### 基础功能测试

| # | 测试用例 | 操作 | 验证点 |
|---|----------|------|--------|
| 1 | `test_mount_create_file` | touch → echo → cat | 文件创建、写入、读取 |
| 2 | `test_mount_create_dir` | mkdir -p | 嵌套目录创建 |
| 3 | `test_mount_list_dir` | ls -la | 目录列表完整 |
| 4 | `test_mount_rename` | mv | 文件重命名 |
| 5 | `test_mount_delete` | rm / rm -rf | 文件/目录删除 |
| 6 | `test_mount_symlink` | ln -s | 符号链接创建与解析 |
| 7 | `test_mount_hard_link` | ln | 硬链接计数 |
| 8 | `test_mount_copy` | cp | 文件复制 |

#### 性能测试

| # | 测试用例 | 操作 | 验证点 |
|---|----------|------|--------|
| 9 | `test_mount_large_file` | dd 1GB | 大文件写入/读取完整性 |
| 10 | `test_mount_many_small_files` | 创建 1000 个 4KB 文件 | 小文件 IOPS |
| 11 | `test_mount_sparse_file` | fallocate + dd | 稀疏文件处理 |

#### 并发测试

| # | 测试用例 | 操作 | 验证点 |
|---|----------|------|--------|
| 12 | `test_mount_concurrent_writes` | 多进程同时写入 | 数据一致性 |
| 13 | `test_mount_concurrent_reads` | 多进程同时读取 | 并发安全 |
| 14 | `test_mount_mixed_workload` | 读写混合 | 竞态条件 |

#### 权限测试

| # | 测试用例 | 操作 | 验证点 |
|---|----------|------|--------|
| 15 | `test_mount_permissions` | chmod / chown | 权限变更 |
| 16 | `test_mount_acl` | setfacl / getfacl | ACL 支持 |

#### xattr 测试

| # | 测试用例 | 操作 | 验证点 |
|---|----------|------|--------|
| 17 | `test_mount_xattr_set_get` | setfattr / getfattr | 扩展属性设置和获取 |
| 18 | `test_mount_xattr_list` | getfattr -d | 列出所有扩展属性 |
| 19 | `test_mount_xattr_remove` | setfattr -x | 扩展属性删除 |

---

## 四、各 Crate 测试方案

### 4.1 powerfs-fuse（FUSE 客户端）

**当前状态**：1 个测试文件（fs_test.rs），25 个用例，覆盖 MetadataCache

**待补充测试**：

#### FUSE 文件系统操作测试

| # | 测试用例 | 被测方法 | 说明 |
|---|----------|----------|------|
| 1 | `test_fuse_lookup` | `lookup()` | 根目录、存在文件、不存在文件 |
| 2 | `test_fuse_getattr` | `getattr()` | 文件、目录、不存在 inode |
| 3 | `test_fuse_create` | `create()` | 创建新文件、创建已存在文件 |
| 4 | `test_fuse_open` | `open()` | 读模式、写模式、不存在文件 |
| 5 | `test_fuse_read` | `read()` | 空文件、有内容文件、超出 EOF |
| 6 | `test_fuse_write` | `write()` | 创建写入、追加写入 |
| 7 | `test_fuse_release` | `release()` | 释放文件句柄 |
| 8 | `test_fuse_unlink` | `unlink()` | 删除文件、删除不存在 |
| 9 | `test_fuse_readdir` | `readdir()` | 空目录、含文件目录、嵌套目录 |
| 10 | `test_fuse_mkdir` | `mkdir()` | 创建目录 |
| 11 | `test_fuse_rmdir` | `rmdir()` | 删除目录、删除非空目录失败 |
| 12 | `test_fuse_rename` | `rename()` | 同目录重命名、跨目录移动 |

#### 挂载集成测试

| # | 测试用例 | 说明 |
|---|----------|------|
| 13 | `test_mount_basic` | 挂载 → 创建文件 → 写入 → 读取 → 卸载 |
| 14 | `test_mount_directory_ops` | 目录创建、嵌套、遍历、删除 |
| 15 | `test_mount_large_file` | 大文件（100MB）读写完整性 |

### 4.2 powerfs-master（Master/Filer）

**当前状态**：3 个测试文件，覆盖 Raft 共识和配置解析

**待补充测试**：

#### Filer API 测试

| # | 测试用例 | RPC | 说明 |
|---|----------|-----|------|
| 1 | `test_filer_lookup` | `LookupDirectoryEntry` | 查找目录条目 |
| 2 | `test_filer_get_entry` | `GetEntry` | 获取条目 |
| 3 | `test_filer_create_entry` | `CreateEntry` | 创建条目 |
| 4 | `test_filer_update_entry` | `UpdateEntry` | 更新条目 |
| 5 | `test_filer_delete_entry` | `DeleteEntry` | 删除条目 |
| 6 | `test_filer_list_entries` | `ListEntries` | 列出条目 |
| 7 | `test_filer_stream_mutate` | `StreamMutateEntry` | 流式变更 |
| 8 | `test_filer_subscribe_metadata` | `SubscribeMetadata` | 元数据订阅 |

### 4.3 powerfs-volume（Volume Server）

**当前状态**：0 个测试文件

**待补充测试**：

| # | 测试用例 | RPC | 说明 |
|---|----------|-----|------|
| 1 | `test_volume_create` | `CreateVolume` | 创建卷 |
| 2 | `test_volume_delete` | `DeleteVolume` | 删除卷 |
| 3 | `test_volume_write_needle` | `WriteNeedle` | 写入数据 |
| 4 | `test_volume_read_needle` | `ReadNeedle` | 读取数据 |
| 5 | `test_volume_delete_needle` | `DeleteNeedle` | 删除数据 |
| 6 | `test_volume_write_blob` | `WriteNeedleBlob` | 分块写入 |
| 7 | `test_volume_read_blob` | `ReadNeedleBlob` | 分块读取 |
| 8 | `test_volume_read_meta` | `ReadNeedleMeta` | 读取元数据 |

---

## 五、端到端（E2E）集成测试

### 5.1 单节点端到端测试

| # | 测试用例 | 流程 |
|---|----------|------|
| 1 | `e2e_single_node_full_flow` | 启动 1 Master + 1 Volume → 挂载 FUSE → 创建文件 → 写入 → 读取 → 校验 → 删除 → 卸载 |
| 2 | `e2e_single_node_large_file` | 单节点 1GB 文件写入/读取完整性（校验和验证） |
| 3 | `e2e_single_node_many_small_files` | 10000 个 4KB 小文件创建/读取/删除 |
| 4 | `e2e_single_node_directory_ops` | 目录创建/嵌套/遍历/删除 |
| 5 | `e2e_single_node_restart_recovery` | 写入 → 关闭 Master+Volume → 重启 → 数据可读 |

### 5.2 多节点集群端到端测试

| # | 测试用例 | 流程 |
|---|----------|------|
| 6 | `e2e_cluster_3node_basic` | 3 Master（Raft）+ 3 Volume → 分配卷 → 写入 → 读取 |
| 7 | `e2e_cluster_master_failover` | 3 Master → 写入数据 → kill Leader → 新 Leader 选举 → 继续读写 |
| 8 | `e2e_cluster_concurrent_clients` | 10 个并发客户端同时挂载读写 |

---

## 六、性能与基准测试（Benchmark）

| # | Benchmark | 被测组件 | 指标 | 目标值 |
|---|-----------|----------|------|--------|
| 1 | `bench_needle_serialize` | Needle | 序列化吞吐 | > 500 MB/s |
| 2 | `bench_memory_index_insert` | MemoryIndex | 插入 ops/s | > 1M ops/s |
| 3 | `bench_volume_write_4k` | Volume | 4KB IOPS | > 10K IOPS |
| 4 | `bench_volume_read_4k` | Volume | 4KB 读取 IOPS | > 50K IOPS |
| 5 | `bench_fuse_lookup` | FUSE | lookup ops/s | > 10K ops/s |

---

## 七、安全测试

| # | 测试用例 | 攻击向量 | 说明 |
|---|----------|----------|------|
| 1 | `test_security_path_traversal` | 路径穿越 | `../../etc/passwd` 被正确拒绝 |
| 2 | `test_security_oversized_request` | 资源耗尽 | 超大数据包被拒绝 |
| 3 | `test_security_checksum_tamper` | 数据篡改 | 篡改 Needle 校验和后读取被拒绝 |
| 4 | `test_security_special_chars` | 注入 | 文件名含 `\0`、`/`、换行符等特殊字符 |

---

## 八、测试加强实施计划

### 8.1 阶段划分

| 阶段 | 时间 | 目标 | 交付物 |
|------|------|------|--------|
| **P0：立即执行** | 本周 | 基础挂载点测试 + Volume/Filer API 测试 | 新增 30+ 测试用例 |
| **P1：进阶测试** | 下周 | E2E 单节点测试 + Shell 集成测试 | 新增 20+ 测试用例 |
| **P2：标准测试** | 下两周 | xfstests 集成 | 通过 80%+ xfstests 通用测试 |

### 8.2 P0：立即执行任务

| 优先级 | 任务 | 用例数 | 负责人 |
|--------|------|--------|--------|
| 1 | powerfs-fuse — 挂载集成测试（mount_test.rs） | 5 | Solo |
| 2 | powerfs-volume — VolumeServer gRPC 测试 | 8 | Solo |
| 3 | powerfs-master — Filer API 测试 | 8 | Solo |

### 8.3 P1：进阶测试任务

| 优先级 | 任务 | 用例数 | 负责人 |
|--------|------|--------|--------|
| 4 | E2E 单节点测试 | 5 | Solo |
| 5 | Shell 脚本集成测试（fs_integration.sh） | 10 | Solo |
| 6 | Benchmark 框架搭建（criterion） | 5 | Solo |

### 8.4 P2：标准测试任务

| 优先级 | 任务 | 说明 |
|--------|------|------|
| 7 | xfstests 集成 | 标准文件系统测试套件 |
| 8 | E2E 多节点集群测试 | 故障转移、并发测试 |

---

## 九、运行测试命令

```bash
# 全量测试
cargo test --all --verbose

# 仅单元测试（快）
cargo test --lib --all

# 仅集成测试
cargo test --test '*' --all

# 特定 crate
cargo test -p powerfs-fuse
cargo test -p powerfs-master --test raft_integration_test

# 带输出
cargo test -- --nocapture

# 覆盖率
cargo tarpaulin --all --out Html --output-dir ./coverage

# Benchmark
cargo bench -p powerfs-core
```

---

## 十、附录：测试命名规范

```
# 单元测试 — 函数名_场景[_边界]
test_<function>_<scenario>              # test_volume_write_needle_success
test_<function>_<scenario>_<edge>       # test_volume_write_needle_volume_full

# 集成测试 — 模块_操作_预期
test_<module>_<operation>_<expected>    # test_master_assign_volume_success
test_<module>_<operation>_<error>       # test_master_assign_volume_no_capacity

# E2E 测试
e2e_<scenario>                          # e2e_single_node_full_flow

# 性能测试
bench_<component>_<operation>           # bench_needle_serialize

# 安全测试
test_security_<vector>                  # test_security_path_traversal
```

---

## 十一、文件系统测试加强讨论

### 11.1 当前问题分析

1. **缺少挂载点测试**：当前测试仅覆盖 MetadataCache 单元测试，未验证真实挂载后的行为
2. **缺少端到端测试**：没有完整的从客户端到存储后端的链路测试
3. **缺少标准测试套件**：未集成 xfstests 等标准文件系统测试工具
4. **Volume Server 无测试**：powerfs-volume crate 完全没有测试覆盖

### 11.2 解决方案

**短期（1-2 周）**：
- 实现 Rust 集成测试，模拟挂载并使用标准 Unix 命令测试
- 为 powerfs-volume 添加 gRPC 端到端测试
- 为 powerfs-master 添加 Filer API 测试

**中期（2-4 周）**：
- 实现 Shell 脚本集成测试，启动真实进程进行测试
- 添加 E2E 单节点测试
- 搭建 Benchmark 框架

**长期（4-8 周）**：
- 集成 xfstests 测试套件
- 实现多节点集群 E2E 测试
- 添加故障注入测试

### 11.3 关键验证指标

| 指标 | 目标值 | 说明 |
|------|--------|------|
| 测试覆盖率 | > 85% | 行覆盖率 |
| 分支覆盖率 | > 75% | 条件分支覆盖 |
| xfstests 通过率 | > 80% | 通用测试组 |
| 测试执行时间 | < 120s | 全量测试 |
| E2E 测试覆盖 | 100% | 核心文件系统操作 |
