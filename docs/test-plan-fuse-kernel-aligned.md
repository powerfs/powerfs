# PowerFS 双客户端统一测试计划 (FUSE + Kernel)

> **对齐基准**：以 [kernel-test-plan.md](../kernel/kernel-test-plan.md) 为蓝本，在相同的 T1-T9 阶段结构下增加 **FUSE 客户端** 对应列与差异说明，确保两种文件系统客户端（用户态 fuse.powerfs / 内核态 powerfs.ko）在同一测试矩阵下完成验证。
>
> **文档状态**：实施中 — 当前阶段聚焦功能基线（T1-T4/T8），可靠性测试（T7）暂不执行
> **创建时间**：2026-08-20
> **更新时间**：2026-08-21 — 调整实施顺序，T7 可靠性测试移至最后且当前阶段暂不测试
> **原则**：
> 1. 同阶段同 ID 下两种客户端**通过标准完全一致**（MD5/stat/目录树一致等）
> 2. 环境差异与**不可用特性**明确标注 [FUSE-only]/[Kernel-only]/[N/A]
> 3. 脚本复用优先：`kernel/vm/test_t*.sh` 的 POSIX 侧逻辑抽取后，对 FUSE 同样适配挂载点即可运行

---

## 1. 双客户端环境对比

| 项目 | FUSE 客户端 (fuse.powerfs) | Kernel 客户端 (powerfs.ko) |
|------|------------------------------|-------------------------------|
| 运行环境 | Docker 容器 `fuse-1`、`fuse-2` （宿主机 docker compose） | QEMU 虚拟机 (`kernel/vm/qemuctl.sh`) |
| 挂载点 | fuse-1: `/mnt/powerfs` <br> fuse-2: `/mnt/fuse`  | `/mnt/pfs` |
| 状态检查 | `docker ps` + 容器日志 `/var/log/powerfs-fuse.log` | `dmesg` + `/proc/slabinfo` + `/proc/meminfo` + D状态 |
| 跨客户端 | `fuse-1` ↔ `fuse-2` (双容器，同一集群) | `QEMU-VM1` ↔ `fuse-1` (跨形态互通) |
| T5~T7 工具 | fio / mdtest (容器内安装) | fio / mdtest (VM内安装) |
| xfstests FSTYP | `FSTYP="fuse.powerfs"` (复用现有 `tests/xfstests/powerfs.conf`) | `FSTYP=powerfs` (新增 localconfig) |

---

## 2. 实施顺序（两客户端同节奏，先 FUSE 再 Kernel，同阶段一 PASS 才进下一）

```
T1(VFS基础) → T2(正确性) → T3(布局K1-K4) → T4(跨客户端/跨形态集成) → T8(持久化)
                                                                          ↓
                                                          T5(性能) → T6(稳定性)
                                                                          ↓
                                            T9(xfstests 可选补充，按需)
                                                                          ↓
                                            T7(可靠性 — 当前阶段暂不测试，最后执行)
```

> **当前阶段优先级**：最先确保**系统正常时**所有功能正确（T1→T2→T3→T4→T8），再做性能/稳定性（T5→T6），最后才是可靠性故障注入（T7）。T7 涉及 Volume/Filer/Master 故障切换、网络断连、CRC 注入等异常场景，当前阶段暂不测试，待功能与稳定性基线建立后再执行。

---

## 3. 阶段矩阵 (同 ID 两客户端对齐)

### 阶段 T1：VFS 基础操作

| 测试 ID | 内容 | FUSE 客户端通过标准 | Kernel 客户端通过标准 | FUSE 映射测试 |
|---------|------|---------------------|------------------------|--------------|
| T1.1 | 文件 CRUD：create/open/write/read/close/stat/truncate | MD5 一致，`stat size/mode` 正确 | 同左 + `dmesg` 无异常 | docker exec fuse-1/2 对 `/mnt/powerfs` 执行 touch/echo/cat/stat/truncate |
| T1.2 | 目录操作：mkdir/rmdir/readdir/rename/unlink/symlink/link/hardlink | 目录树正确，nlink 计数对 | 同左 + slab 无泄漏 | `mkdir -p` + `ls -laR` + `mv/ln/ln -s/unlink` |
| T1.3 | 权限测试：chmod/chown/utimes | 权限位 + 时间戳 stat 正确 | 同左 | `chmod 600` + `stat -c %a` |
| T1.4 | 特殊文件：mknod (fifo/sock) | 创建成功 + `test -p/-S` 可访问 | 同左 | `mkfifo` + `[[ -p ]]` |
| T1.5 | 边界测试：空文件 / 最大路径名 / 特殊字符文件名 | 无异常（ENAMETOOLONG/EINVAL 映射对） | 同左 + dmesg 无 Oops | 空文件 `>` + 255B 文件名 + 空格/中文文件名 |
| T1.6 | 并发读写：多进程同文件 / 不同文件 | 无 corruption，`md5sum` 与串行一致 | 同左 + 无 lockdep/RPC stall | `xargs -P4` 并行写入 60s |

**FUSE 适配脚本**：复用 `kernel/vm/test_t1_vfs_basic.sh` 的 T2~T6 断言，将 `vm()` 包装函数替换为 `docker exec fuse-1 / fuse-2`，挂载点改为 `/mnt/powerfs`，跳过 `dmesg/slab` 检查（FUSE 用户态不涉及），替换为检查 `docker logs fuse-1 2>&1 | grep -iE 'error|panic|deadlock'`。

---

### 阶段 T2：文件系统正确性

| 测试 ID | 内容 | FUSE 通过标准 | Kernel 通过标准 |
|---------|------|---------------|-----------------|
| T2.1 | 大目录树 `cp -r`（1000+ 文件） | `diff -r` 源/目标完全一致 | 同左 + slab/meminfo 稳定 |
| T2.2 | `tar czf` + `tar xzf` | MD5 清单完全一致 | 同左 |
| T2.3 | 源码编译：`make Linux`（简化为 `make powerfs-fuse` 或 `tar -xf linux-src && make defconfig`） | 编译成功无 IO 错误 | 同左 + 无 hung task |
| T2.4 | `rsync -a` 源码 → PowerFS | `rsync --checksum` 无增量项 | 同左 |
| T2.5 | `git clone` / `git commit` | `git status` 干净，操作无错误码 | 同左 |

---

### 阶段 T3：布局功能 (K1 Flat / K2 Inline / K3 Stripe / K4 Reliability)

> **说明**：布局由 Filer 端的 ShardStore 统一决定，两种客户端使用同一套 RPC 协议，FUSE 通过 `UPDATE_SIZE_CHUNKS` 携带 `inline_data` / `chunks`，内核端通过相同的 layout 结构（`powerfs_layout` 定义于 `powerfs-layout` crate）。**两客户端写入后互通必须完全一致**。

| 测试 ID | 内容 | FUSE 脚本/断言 | Kernel 脚本/断言 |
|---------|------|---------------|-----------------|
| T3.1 K1 | Flat 读写互通 (>chunk 大小，非 inline 非 stripe) | `dd bs=4M count=16` → MD5 跨客户端匹配 | `kernel/vm/test_k1_layout.sh` |
| T3.2 K2 | Inline 小文件 + 迁移阈值 (<=inline_max → >inline_max) | Filer 日志 `inline_len=N` + 迁移后 chunks>=1 + 大小 MD5 对 | `test_k2_inline.sh` |
| T3.3 K3 | Stripe 多卷读写（多 collection，stripe_width>1） | `collection="stripe3"` 挂载，`dd bs=4M count=32` → MD5 对 | `test_k3_stripe.sh` |
| T3.4 K4 | Reliability 布局正常读写（Replicated/EC 写入+读回，无故障注入） | `dd bs=4M count=16` 使用 Replicated collection 写入 → MD5 跨客户端匹配；EC collection 同理 | `test_k4_reliability.sh`（仅正常路径） |

> **T3.4 说明**：K4 在 T3 阶段**仅验证 Reliability 布局的正常读写功能**（Replicated/EC 写入后读回数据一致），不涉及 failover/CRC 注入/EC 降级读等故障场景。故障注入测试统一归入 T7 可靠性测试，当前阶段暂不执行。

---

### 阶段 T4：集成测试（两客户端互通，对齐核心交叉矩阵）

| 测试 ID | 内容 | FUSE 客户端（fuse-1 ↔ fuse-2） | Kernel ↔ FUSE 跨形态 |
|---------|------|-------------------------------|-----------------------|
| T4.1 | FUSE 创建 → Kernel 读取（Flat/Inline/Stripe 三类） | [fuse-1 → fuse-2 同构] MD5 完全一致 | **[异构]** fuse-1 写入 → VM 内 `md5sum /mnt/pfs/*` 相同 |
| T4.2 | Kernel 创建 → FUSE 读取（三类） | [fuse-2 → fuse-1 同构] MD5 完全一致 | **[异构]** VM 写入 → `docker exec fuse-1 md5sum` 相同 |
| T4.3 | remount 后数据一致性 | `docker restart fuse-1` 后文件清单/内容不变 | `umount + mount` 后不变 |
| T4.4 | 两客户端同时挂载并发读写 | fuse-1 写 A，fuse-2 写 B，交叉读无 corruption | VM 写 C，fuse-1 同时写 D，两路独立读一致 |

---

### 阶段 T8：数据持久化 (10 子类，1:1 对齐)

| 测试 ID | 内容 | FUSE 断言（`docker restart fuse-1`） | Kernel 断言（`umount → rmmod → insmod → mount`） |
|---------|------|----------------------------------|------------------------------------------------|
| T8.1 | 写入持久化：小(100B)/中(1MB)/大(10MB)/覆盖写/append | 重启后 MD5 一致 | remount 后 MD5 一致 |
| T8.2 | 创建/删除持久化：文件/目录/目录树 | 重启后存在性正确 + `rm` 后确实不存在 | 同左 |
| T8.3 | 硬链接持久化：nlink / 内容 / 原文件删除后存活 | `stat -c %h` 对，删除源后链接内容 MD5 不变 | 同左 |
| T8.4 | 软链接持久化：绝对路径/相对路径 | `readlink` 值正确 + `cat` 通过链接读内容正确 | 同左 |
| T8.5 | truncate 持久化：扩展/缩小/清零 | size 与内容 (后补 `\0`) 一致 | 同左 |
| T8.6 | 元数据持久化：chmod/chown/utimes/目录权限 | `stat` mode/uid/gid/mtime 重启后仍对 | 同左 |
| T8.7 | fsync 持久化：fsync+drop_caches；fsync+remount | MD5 对；容器内无 `/proc/sys/vm/drop_caches` 跳过，走 fsync+restart | `sync; echo 3 > drop_caches` 版本必须过 |
| T8.8 | rename 持久化：文件 rename / 目录 rename | 旧路径不存在 (`ENOENT`)，新路径内容可读 | 同左 |
| T8.9 | 综合场景：多操作混合 + 部分删除 | manifest (find + md5sum list) 一致；hardlink 存活 | 同左 |
| T8.10 | 完整客户端重载：kill+restart 全流程 | 相当于 `docker restart` 全量验证 | 完整 `rmmod powerfs + insmod powerfs.ko` 流程 |

---

### 阶段 T5：性能测试

| 测试 ID | 工具 | FUSE | Kernel |
|---------|------|------|--------|
| T5.1 | fio 顺序读 1KB~1GB | 在 fuse-1 容器内装 fio，输出 BW/IOPS | QEMU 内运行同参数 |
| T5.2 | fio 随机读写 4K~1M bs | 同上 | 同上 |
| T5.3 | Stripe vs Flat 性能对比 | 切换 collection 对比 | 同上 |
| T5.4 | Inline vs Flat 小文件性能 | mdtest 小文件创建吞吐 | 同上 |
| T5.5 | 元数据性能 create/stat/delete | mdtest | 同上 |
| T5.6 | 多线程并发 IO | `fio --numjobs=8` | 同上 |

---

### 阶段 T6：稳定性测试

| 测试 ID | 内容 | FUSE 通过标准 | Kernel 通过标准 |
|---------|------|---------------|-----------------|
| T6.1 | 持续顺序写 10 分钟 | 无 IO error；`docker logs` 无 error/panic；进程不退出 | 10min 后 `check_kernel_state` 全绿 |
| T6.2 | 持续随机读写混合 30 分钟 | 无 corruption；内存 RSS 稳定无泄漏 | slab 活跃对象稳定（对比基线 ±10%） |
| T6.3 | 高并发压力 32 线程 10 分钟 | 无 hang/deadlock；操作返回码全正 | 无 panic/deadlock；lockdep 无警告 |
| T6.4 | 内存泄漏检测 1 小时 | RSS 稳定 ±10%（`docker stats`） | `MemAvailable` ±5% + slab 无线性增长 |
| T6.5 | 长时间挂载 1 小时 | 无 hung FUSE 请求；心跳正常发送 | 无 `hung task` 警告，无 OOM |

---

### 阶段 T7：可靠性测试（当前阶段暂不测试，最后执行）

> **状态**：⏸️ **当前阶段暂不测试**。T7 为故障注入类测试，需在 T1-T4/T8 功能基线 + T5/T6 性能稳定性基线建立后再执行。当前优先保证系统正常时所有功能正确。

| 测试 ID | 内容 | FUSE 通过标准 | Kernel 通过标准 |
|---------|------|---------------|-----------------|
| T7.1 | 网络断连恢复 | 断 Master/Volume 期间写请求挂起/返回 EAGAIN，恢复后完成 / 重传成功 | VM 断网（tc netem）同左 |
| T7.2 | Volume Server failover | 主 volume `docker stop` → 从副本读成功；数据完整 | 同左 |
| T7.3 | Filer leader 切换 | `docker restart filer-<leader>`；写重试完成，读无错 | 同左 |
| T7.4 | CRC32 不匹配检测 | 注入损坏 → 返回 EIO，未返回坏数据 | 同左 + dmesg 有 `crc mismatch` 日志 |
| T7.5 | EC 降级读 | 1-2 分片丢失 → 降级重建成功，MD5 对 | 同左 |
| T7.6 | Reliability failover（从 T3.4 拆分） | Replicated 模式主副本 volume 故障 → 从副本读成功；EC 模式分片故障 → 降级读 MD5 对 | 同左 |
| T7.7 | 卸载排空 | `fusermount -u` 后 `docker exec fuse-1 ls /mnt/powerfs` 为本地空目录 | `umount` 后无残留 inode/dentry slab 对象 |

---

### 阶段 T9：xfstests 可选补充

**两客户端共用同一 powerfs.conf 框架，仅 FSTYP 不同**：

| 项目 | FUSE 客户端 | Kernel 客户端 |
|------|------------|--------------|
| 配置文件 | `tests/xfstests/powerfs.conf`：`FSTYP="fuse.powerfs"` （已有） | `tests/xfstests/powerfs-kernel.localconfig`（新增：FSTYP=powerfs，stub mkfs.powerfs） |
| 运行位置 | docker exec fuse-1 / xfstests-dev | QEMU VM 内 |
| 推荐用例 | `generic/001,002,005,011..015,020..022,031..033,068,076,080,113,125..127` 同左对齐 | 同左 + 额外 DIO/Cow 相关（若 powerfs.ko 支持） |
| 失败处理 | POSIX 兼容问题修复；不适用 quota/reflink/fiemap 排除 | 同左 + 若 VFS 回调缺实现则先补回调 |

---

## 4. 两客户端差异化测试 & 环境专属

### FUSE 专属

- [FUSE-only] **进程 RSS/FD 监控**：FUSE 用户态进程内存泄漏通过 `docker stats` 和 `/proc/<pid>/fd` 计数验证
- [FUSE-only] **tokio 线程池死锁**：通过长时间并发 `block_on()` 场景验证（inline 修复后附加回归项）

### Kernel 专属

- [Kernel-only] **dmesg/KASAN/slub_debug 检查**：每个阶段后强制 `check_kernel_state()`
- [Kernel-only] **lockdep/RCU stall 检查**：高并发阶段 (T1.6 / T6.3) 必查
- [Kernel-only] **模块 rmmod 后无泄漏**：T8.10 必查 `/proc/slabinfo` 无 `powerfs_*`

---

## 5. 执行记录模板

每次执行后在 `output/test-results/run_<TS>/` 产出：

| 文件 | 内容 |
|------|------|
| `summary.md` | 双客户端阶段 PASS/FAIL/SKIP 对比表 + 阻塞项 |
| `fuse-t1.log ~ fuse-t8.log` | FUSE 各阶段独立日志 |
| `kernel-t1.log ~ kernel-t8.log` | Kernel 各阶段独立日志（附 dmesg tail 50 行） |
| `perf-results/` | T5 fio 结果 JSON/CSV（两客户端对齐同参数） |
| `regression.md` | 相对上次运行新增 FAIL / FIX 的回归说明 |

---

## 6. 门禁条件 (Gate)

**当前阶段（功能基线）对外发布前必须满足**：
1. FUSE 客户端：T1 / T2 / T3 / T4 / T8 **全部 PASS**（无任何 FAIL）
2. Kernel 客户端：T1 / T2 / T3 / T4 / T8 **全部 PASS**，且 T4.1 + T4.2 跨形态互通 100% PASS
3. T7 可靠性测试：**当前阶段暂不测试**，待功能基线 + 性能稳定性基线建立后再执行

**后续阶段（完整发布）追加门禁**：
4. T5 / T6 在有发布窗口时通过（T5 性能指标同比不回归 ±15%）
5. T7 可靠性测试全部 PASS（网络断连/Volume failover/Filer leader 切换/CRC 注入/EC 降级读/卸载排空）
6. T9 xfstests 推荐用例 **通过率 ≥ 85%**（失败项需有分类说明）
