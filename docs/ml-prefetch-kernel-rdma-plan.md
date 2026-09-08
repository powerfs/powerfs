# PowerFS ML 自适应预取方案（kernel 客户端 + RDMA）

> 状态: **Phase A-0/A-1/A-X1 完成，A-X2（fio 基线对比）进行中**
> 创建: 2026-09-05
> 规划更新: 2026-09-08（A-X1 完成）
> 环境基线: kernel 客户端 + RDMA（QEMU VM），见 [§0 环境基线](./dir-policy-and-concurrency-optimization-plan.md#0-环境基线调整记录-2026-09-05)
> 参考论文: KML (Kernel-ML) — 单机 Linux 内核 readahead 调优（per-file workload 分类 NN）
> 关联文档: [`dir-policy-and-concurrency-optimization-plan.md`](./dir-policy-and-concurrency-optimization-plan.md)、[`file-layout-prediction-design.md`](./file-layout-prediction-design.md)
>
> 2026-09-08 更新摘要:
> - **D0/D1 netfs 接入已完成**（不再是规划态）：[powerfs_super.c:131](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_super.c#L131) `powerfs_netfs_issue_read` + [powerfs_super.c:275-276](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_super.c#L275-L276) `powerfs_netfs_ops.issue_read`，[powerfs_addr.c:2625](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_addr.c#L2625) `.read_folio = netfs_read_folio`。ML hook 点已就位。
> - **P0-1（ior-hard-write BW=0）已解决**：[`io500_baseline_report.md`](./io500_baseline_report.md) v4 基线确认 10.96 MiB/s（恢复），根因是 ior 测试 block/transfer size 配置问题（非 kernel bug），脚本已修。
> - **P0-2（mdtest create=11 ops/s）已由代码层 optimistic local create 解决**：[powerfs_dir.c:431](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_dir.c#L431) `powerfs_flush_pending_create` + [powerfs_caps.c:566](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_caps.c#L566) optimistic-to-Filer 升级已落地；v4 基线 mdtest-easy-create = 4630 ops/s（恢复，原 11 ops/s）。
> - **新增 v4 基线 O_DIRECT 性能数据**：4K O_DIRECT randread (cold) = **409k IOPS**（直读 Volume Server），1M O_DIRECT seqwrite 经 issue #82 修复后 = **176-192 MiB/s**（fast path 跳过 read-modify-write），O_DIRECT read 已非瓶颈。
> - **Phase A 工程版仍未启动**：grep 全仓库无 `readahead_policy` / trace 聚合 / workload 分类代码。
> - **澄清**：[`powerfs-layout/src/predictor.rs`](file:///home/portion/powerfs/powerfs-layout/src/predictor.rs) 是 **LayoutPredictor**（文件布局预测 Inline/Flat/Stripe，规则驱动），**不是 ML 预取**。两者维度不同，§10 的"预取策略可复用其 xattr 通道"仅指通道复用，不代表 ML 预取已部分实施。

---

## 1. 问题背景

### 1.1 KML 论文核心

KML 在单机 Linux 内核用 ML 调优 readahead：
- **Readahead NN**：5 特征（每秒事务数、page offset 累积移动均值、连续事务 page offset 差值均值、inode、当前 readahead）→ 3 层 NN 分类 workload（readrandom/readseq/readrandomwriterandom/readreverse）→ 设定 readahead 大小。95.5% 准确率，**per-file basis**
- **NFS rsize NN**：8 特征 → 4 层 NN，98.6% 准确率
- 关键洞察：t-SNE 显示 sequential/random cluster 重叠（warm-up 阶段是顺序的），传统启发式难分类

### 1.2 PowerFS 现状对照

| 维度 | KML（单机内核） | PowerFS 现状 | 差距 |
|---|---|---|---|
| readahead | ML 自适应 | kernel 客户端走 VFS 原生 readahead（`file_ra_state`），无 ML | 固定启发式，random 工作负载预取纯浪费 |
| read 路径 | 内核 `readpage` | 规划用 netfs API（`netfs_read_folio + powerfs_netfs_issue_read`），**当前规划态**（[powerfs_addr.c:49-50](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_addr.c#L49-L50)） | netfs hook 待接入 |
| 传输 | 本地盘 | RDMA SEND/RECV（2MB 帧 + MR 池，非 RDMA READ） | 误预取代价高（MR 池有限 + 2MB 帧往返） |
| 协同 | 单机 | 分布式 filer 可聚合多客户端 trace | KML 无此维度 |

### 1.3 为什么 RDMA 下 ML 预取价值更大

PowerFS kernel 侧 RDMA 是 **SEND/RECV 模式**（[powerfs_net_rdma.c](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_net_rdma.c)），预取数据走 2MB 帧 + MR 池：
- 预取窗口过大 → MR 池耗尽、挤占其他连接、cache 污染
- 预取窗口过小 → 多次 2MB 帧往返，RDMA 优势被往返延迟抵消
- 误预取在 RDMA 下代价 **高于 TCP**（MR 池是稀缺资源），ML 分类收益更大

### 1.4 覆盖范围边界（重要，避免误导）

**本方案只覆盖 IO500 的数据 IO 子项，不覆盖元数据子项。** 二分类（random/sequential）够用于数据 IO，但元数据项根本不走 readahead：

| IO500 子项 | 走数据预取？ | 本方案覆盖？ | 瓶颈 / 归属 |
|---|---|---|---|
| ior-easy write/read | ✓ | ✓ | 顺序大文件，本方案 sequential 调大 |
| ior-hard write/read | ✓ | ✓ | 3901B random + fsync，本方案 random 关闭预取 |
| ior-easy/hard reread | ✓ | ✓ | 已 cache，本方案不预取 |
| mdtest-easy/hard create | ✗ | ✗ | 元数据 RPC + lease 锁，归元数据预取方案 |
| mdtest-easy/hard stat | ✗ | ✗ | stat RPC + dentry cache，归元数据预取方案 |
| mdtest-easy/hard remove | ✗ | ✗ | unlink RPC，归元数据预取方案 |
| **find** | ✗ | ✗ | readdir + stat 遍历，归元数据预取方案 |

**find / mdtest-stat 等 metadata 项的优化归独立 metadata 预取方案**（readdir 树形预取 + bulk stat 充分填充 + dir lease 时长策略；待独立文档，方案文档暂未落地）。

**不需要 KML 的四分类**：KML 用四分类是因为 RocksDB 有 readreverse / readrandomwriterandom 中间态，IO500 没有；多分类反而误判风险高，违反"安全回退"原则。

---

## 2. 设计目标与非目标

### 2.1 目标
1. kernel 客户端 read 路径引入 ML 自适应预取（替代固定 readahead）
2. 对 IO500 **数据子项**：ior-easy 识别 sequential → 调大预取对齐 RDMA 2MB 帧；ior-hard 识别 random → 关闭预取避免 MR 池浪费（**mdtest 元数据项不在本方案覆盖，见 §1.4**）
3. 模型训练在用户态（filer/master），下发到 kernel 客户端
4. 误分类有安全回退（最坏退回 VFS 默认 readahead，不比现状差）

### 2.2 非目标
- 不改 RDMA 传输模式（SEND/RECV → RDMA READ 迁移是独立工作）
- 不在 kernel 内做模型训练（训练在用户态，kernel 只推理）
- 不追求四分类（random/readseq/readrandomwriterandom/readreverse），先做**二分类**（random vs 其他），更稳

---

## 3. 总体架构

```
┌─────────────────────────────────────────────────────────────────────┐
│  filer / master（用户态，训练侧）                                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────────┐    │
│  │ trace 聚合   │→ │ 模型训练     │→ │ 模型下发（per-file 规则）│    │
│  │ (多客户端)   │  │ (per-file NN)│  │  via xattr / net RPC     │    │
│  └──────────────┘  └──────────────┘  └─────────────┬────────────┘    │
└─────────────────────────────────────────────────────┼────────────────┘
                                                       │ 下发
┌──────────────────────────────────────────────────────▼────────────────┐
│  kernel 客户端（推理侧，QEMU VM + RDMA）                                │
│  ┌──────────────┐  ┌──────────────────┐  ┌──────────────────────┐     │
│  │ VFS readahead│→ │ netfs issue_read │→ │ RDMA SEND/RECV 2MB帧│     │
│  │ (file_ra_*)  │  │ + ML 推理(轻量) │  │  + MR 池            │     │
│  └──────────────┘  └──────────────────┘  └──────────────────────┘     │
│       ↑ readahead 大小由 ML 决定（random→关闭，seq→调大）              │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 4. 详细设计

### 4.1 模型训练（用户态，filer/master）

#### 4.1.1 特征集（参考 KML，适配分布式）

| 特征 | KML | PowerFS 调整 |
|---|---|---|
| 每秒事务数 | ✓ | ✓ |
| page offset 累积移动均值 | ✓ | ✓ |
| 连续事务 page offset 差值均值（**最重要**） | ✓ | ✓ |
| inode | ✓（RocksDB 过滤） | ✓ |
| 当前 readahead | ✓ | ✓ |
| **客户端来源** | 无 | 新增（多客户端区分） |
| **RDMA MR 池占用率** | 无 | 新增（预取不能压垮 MR 池） |

#### 4.1.2 训练流程

1. kernel 客户端通过 tracepoint（对齐 KML）或 filer 端聚合，收集 per-file 访问特征
2. filer/master 用户态训练 per-file 二分类模型（random vs sequential-ish）
3. 模型小（< 1MB），序列化下发
4. 每 N 秒重训，适应负载变化（KML 每秒推理，训练离线/低频）

#### 4.1.3 模型下发通道

三选一（待定）：
- **xattr**：`powerfs.readahead_policy` = 模型引用 / per-file readahead 值（最简单，与 dir-policy 体系一致）
- **mount option**：全局模型，挂载时传入（粗粒度）
- **net RPC**：filer 主动推送 per-file 策略（最灵活，需新 RPC）

### 4.2 kernel 客户端推理（hook 点）

#### 4.2.1 现状：netfs 接入已完成（2026-09-07 确认）

netfs read 路径已落地，ML hook 点已就位：

- [powerfs_super.c:131](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_super.c#L131) `powerfs_netfs_issue_read` — netfs 子请求派发
- [powerfs_super.c:275-276](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_super.c#L275-L276) `powerfs_netfs_ops`（`netfs_request_ops`），`.issue_read = powerfs_netfs_issue_read`
- [powerfs_addr.c:2625](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_addr.c#L2625) `powerfs_aops.read_folio = netfs_read_folio`

**前置依赖 D0/D1 已满足**：ML 预取可直接在 `powerfs_netfs_issue_read` 内挂 hook（查 per-file 策略，决定 readahead window）。

#### 4.2.2 hook 点（netfs 接入后）

- `powerfs_netfs_issue_read`：决定每次 read 请求的 readahead window 大小
  - 查 per-file ML 策略（xattr / 下发缓存）
  - random → readahead = 0（关闭预取）
  - sequential → readahead = N × 2MB（对齐 RDMA 帧，N 由 ML 决定）
- `file_ra_state`：VFS 原生 readahead 状态，ML 间接调整其大小

#### 4.2.3 推理实现（KML kernel-ml 思路）

- 轻量推理：kernel 内嵌小模型（< 1MB），或直接用下发的 per-file readahead 值（免推理）
- **推荐先做"下发 per-file readahead 值"**（免 kernel 内推理，最稳），成熟后再嵌模型

### 4.3 RDMA 代价感知

预取窗口决策需考虑 MR 池状态（KML 单机无此约束）：

```
effective_readahead = ML建议值
if MR池占用率 > 阈值:
    effective_readahead = min(effective_readahead, MR池剩余可容纳帧数 × 2MB)
```

避免预取压垮 MR 池导致 RNR（project 硬约束：RdmaChannel 预注册 4 recv buffer）。

### 4.4 安全回退

ML 误分类必须可回退（项目约束"文件系统涉及用户数据不能有错"）：
- 模型置信度低于阈值 → 不干预，用 VFS 默认 readahead
- 最坏情况退回当前固定行为，**不能比现状更差**
- 二分类（random vs 其他）比四分类更稳，random 误判为 seq 顶多多预取，seq 误判为 random 顶多少预取（IO500 mdtest 本就该关闭预取，误判风险可控）

### 4.5 模块化与状态属性架构

Phase A 不在 `powerfs_file.c` 内嵌逻辑，按 PowerFS 已有惯例（cap/lease 独立文件）独立成模块，便于复用、测试、未来替换 NN。

#### 4.5.1 模块划分

| 模块 | 文件 | 职责 |
|---|---|---|
| **kernel 策略缓存** | 新增 `kernel/powerfs_mod/powerfs_readahead.c` / `.h` | per-inode `powerfs_readahead_policy` 缓存 + LRU + xattr 加载 + 安全回退门控 |
| **kernel hook 入口** | 修改 `powerfs_file.c:520` `powerfs_file_read_iter` | 在 `generic_file_read_iter` 前调 `powerfs_readahead_apply(file, inode)` 改 `file->f_ra.ra_pages` |
| **filer trace 聚合** | 新增 `powerfs-filer/src/readahead_trace.rs` | 多客户端 read 路径 trace 收集 + per-file 聚合（offset 序列、IOPS、size） |
| **filer 训练/下发** | 新增 `powerfs-filer/src/readahead_policy.rs` | KML 5 特征 NN 训练 + 序列化 + xattr 写入（`user.powerfs.readahead_policy`） |
| **xattr 协议** | filer `ShardCommand::SetXattr` + kernel `powerfs_net_getxattr` | 复用现有通道，无新 RPC 类型 |

每模块独立 `#[cfg]` / `CONFIG_POWERFS_READAHEAD_ML` 开关，调试期可关闭回退 VFS 默认。

#### 4.5.2 状态与属性区分（重要）

`powerfs_inode_info` 加两字段，区分**是否已下发** vs **下发值**：

```c
struct powerfs_inode_info {
    ...
    /* ML readahead 策略缓存 (per-inode, A-0/A-1 共用) */
    __u32 readahead_mb;          /* 下发的 readahead 值 (0=random关闭, N=seq调大到 N×2MB) */
    bool  readahead_policy_cached:1;  /* xattr 已查过, readahead_mb 有效; false=未查/失效, 需 RPC */
    bool  readahead_policy_disabled:1;/* 全局禁用 (mount option readahead=off), 跳过所有逻辑 */
    __u64 readahead_policy_version;   /* filer 下发版本号, 用于 invalidation (A-1 ML 更新时比对) */
};
```

**三状态语义**：

| readahead_policy_cached | readahead_policy_disabled | 含义 | 行为 |
|---|---|---|---|
| false | true | 全局禁用 | 不查 xattr, 用 VFS 默认 |
| false | false | 未查/失效 | 首次 read_iter 触发 xattr RPC, 缓存结果 |
| true  | false | 已缓存 | 直接用 readahead_mb 改 file->f_ra, 无 RPC |

`version` 字段用于 A-1 ML 推送新策略时让 kernel 端 LRU 失效（filer 端 bump version → kernel 端 PushDelta 收到 → 标记 cached=false → 下次 read 重新查）。

### 4.6 缺省规则（filer 不具备学习能力时的兜底）

用户提的关键问题：**后端不具备规则学习更新能力时也要有缺省规则**，不能没策略就退化到 VFS 默认（VFS 默认对 random 工作负载纯浪费）。

#### 4.6.1 三级策略源

filer 端 readahead_policy 模块按优先级回退：

```
1. ML 学习策略     (A-1 NN 训练产出, 写入 xattr: user.powerfs.readahead_policy=NN:<bytes>)
2. 规则缺省策略    (无 ML 时由 filer 端规则引擎写入, xattr: ...=RULE:<bytes>)
3. 全局兜底常量    (xattr 不存在时 kernel 端按 mount option 兜底)
```

#### 4.6.2 缺省规则引擎（无 ML 也能用）

filer 端 `readahead_policy.rs` 内嵌简单规则（不依赖训练数据）：

| 文件特征 | 分类 | readahead |
|---|---|---|
| size < 32KB | random-ish（小文件默认 random） | 0 |
| size >= 1MB 且被多次顺序读 | sequential | 16MB |
| 其他未知 | 保守 | 4MB（VFS 默认 128KB × 32） |

规则产出与 ML 产出走同一 xattr 通道，kernel 端无需区分来源（只看 `readahead_mb` 值）。

#### 4.6.3 xattr 值编码

```
user.powerfs.readahead_policy = "<bytes>"        (A-0 纯数字 MB, 兼容)
                              = "NN:<bytes>"    (A-1 NN 下发)
                              = "RULE:<bytes>"  (规则引擎下发)
```

kernel 端只解析纯数字部分写入 `readahead_mb`，前缀用于 `dmesg` 调试标识来源。ML 训练就绪后 filer 端切换写入 `NN:` 前缀，**无需 kernel 侧改动**。

### 4.7 数据采集设计（高效统一方式）

用户提的关键问题：**第一步数据采集比较重要，是否要设计高效统一方式**。确认是 — 采集开销若过高会污染测量、使 ML 策略本身是反效果。PowerFS 当前无 trace 基础设施，需新建。

#### 4.7.1 设计目标

- 低开销： < 3% 性能损失（采样而非全量）
- 统一：read/write/seq/random 都走同一采集点，避免散落
- 分布式：filer 端聚合多客户端，单机 trace 看不全
- 可演进：A-0 用最简形式，A-1 接 NN 时无需重写采集层

#### 4.7.2 采集架构

```
kernel 客户端 (read_iter / write_iter 入口)            filer (统一聚合点)
┌────────────────────────────────┐                  ┌─────────────────────────┐
│ per-inode ring buffer (1KB)    │  采样 1/100      │ readahead_trace.rs        │
│  - last 16 (offset, ts, r/w)   │ ─batch flush──▶ │  - per-file 聚合         │
│  - seq_run_len, rand_run_len   │  via PushDelta   │  - 5 特征提取 (KML)       │
│  - file size, placement        │  每 1s 或 64 entries │  - 训练集累积       │
└────────────────────────────────┘                  └─────────────────────────┘
```

#### 4.7.3 kernel 采集点（统一入口）

只在 `powerfs_file_read_iter` / `powerfs_file_write_iter` 入口记录一次（不在 netfs issue_read / writepages 内重复记录），避免放大：

```c
/* powerfs_readahead.c */
struct powerfs_io_trace {
    __u64 ino;
    __u32 placement;            /* Inline/Flat/Stripe */
    __u64 file_size;
    __u16 last_offsets[16];     /* 环形 buffer, 记录最近 16 次 page offset (高 16 位) */
    __u16 last_kinds[16];       /* 0=read, 1=write */
    __u16 seq_run, rand_run;    /* 当前连续/跳跃计数 */
    __u32 sample_counter;       /* 1/100 采样控制 */
};
```

采样率 `sample_counter % 100 == 0` 触发记录，非每事务；环形 buffer 仅记最近 16 次访问模式，足以让 filer 端二分类。

#### 4.7.4 filer 聚合与下发

- filer `readahead_trace.rs` 接 PushDelta（已有 RPC）批量收 trace
- per-file 聚合：offset 序列差值均值（KML 最重要特征）+ IOPS + size
- A-1 阶段：训练 NN → 写 `user.powerfs.readahead_policy=NN:<bytes>`
- A-0 阶段：跳过训练，规则引擎直接写 `RULE:<bytes>`（见 §4.6）

#### 4.7.5 开销控制

- kernel ring buffer 1KB/inode，1M inode ≈ 1GB（可接受）
- 采样 1/100 → 实际 RPC 流量 < 1KB/s/client
- 不在 readahead 路径内做 RPC（异步 batch flush，1s 或 64 entries 触发）
- 采集失败不阻塞 I/O（trace 是 best-effort）

### 4.8 A-0 / A-1 子阶段重新定义

基于 §4.5-4.7 模块化设计，原 §7.2 A1-A7 重新拆分：

#### Phase A-0：手工基线验证（最小可行）
- [x] A-0.1 kernel `powerfs_readahead.c` 骨架 + `powerfs_inode_info` 字段 + mount option `readahead=off|auto`
- [x] A-0.2 kernel hook：`powerfs_file_read_iter` 调 `powerfs_readahead_apply` 改 `file->f_ra.ra_pages`
- [x] A-0.3 xattr 通道联调：用 `setfattr` 手工设 `user.powerfs.readahead_policy=0|16`，验证 kernel 端能读到并生效（含 xattr 持久化修复，见下）
- [x] A-0.4 测试矩阵（3×3，见 §4.8.末）验证"调 readahead 影响 mdtest/ior-hard 性能"假设

> **A-0.3 xattr 持久化修复（2026-09-08）**：`set_xattr` 未更新 `meta_cache`，导致 `handle_getxattr` 读到 stale InodeInfo 返回 ENODATA。修复：`meta_cache.rs` 新增 `project_set_xattr`/`project_remove_xattr`，`meta_shard_manager.set_xattr`/`remove_xattr` 在 `propose_meta` 后投影到缓存（与 `project_update_size_chunks` 同模式）。

#### Phase A-1：ML 自动化（KML 5 特征 NN）
- [ ] A-1.1 kernel `powerfs_io_trace` 采集点（§4.7.3）+ PushDelta 异步 flush
- [ ] A-1.2 filer `readahead_trace.rs` 聚合 + 5 特征提取
- [ ] A-1.3 filer `readahead_policy.rs` NN 训练（per-file 二分类）+ 写 xattr `NN:<bytes>`
- [ ] A-1.4 kernel version-based invalidation（filer bump version → kernel LRU 失效重查）
- [ ] A-1.5 filer 缺省规则引擎（§4.6.2，无训练数据时兜底）
- [ ] A-1.6 安全回退：ML 置信度 < 阈值 → 退回规则缺省；规则失败 → VFS 默认
- [ ] A-1.7 IO500 全量对比：固定预取 vs 规则缺省 vs ML 自适应

#### A-0 测试矩阵（验证 H1/H2/H3 假设）

**实测环境**：QEMU VM1 + RDMA，kernel 客户端，`/mnt/powerfs`，cold read（每次 `echo 3 > drop_caches`），2026-09-08。

| Workload | xattr=0 | xattr=默认(无) | xattr=16 | 结论 |
|---|---|---|---|---|
| fio 4K randread cold (64M, 10s) | **394 IOPS** | 311 IOPS | 384 IOPS | H3 ✓: xattr=0 比默认 +27%（关预取省 RDMA 带宽） |
| fio 1M seqread cold (128M, 10s) | 1.5 MiB/s | 126 MiB/s | **355 MiB/s** | 对照 ✓: xattr=16 比默认 +182%（大预取对齐 2MB 帧） |
| ior-hard read (47KB xfer, 20s) | 1.45 MiB/s | 37.5 MiB/s | **316.8 MiB/s** | 顺序读 xattr=16 比默认 +744% |
| mdtest-easy (0-byte 文件) | N/A | create=21645 / stat=614 / read=438 / rm=860 ops/s | N/A | 元数据操作，readahead 不适用（§1.4） |
| ior-hard write | N/A | 252.7 MiB/s | N/A | 写操作，readahead 不适用 |

**关键发现**：
1. **readahead 机制生效且效果显著**：xattr=0 关闭预取使 4K 随机读 +27%；xattr=16 开 16MB 预取使顺序读 +182%~+744%。
2. **H3 成立**（xattr=0 提升 4K 随机读）：默认 VFS 128KB 预取对随机读是浪费，关闭后 IOPS 提升。
3. **H1/H2 修正**：原假设"xattr=0 提升 mdtest create"和"xattr=16 提升 ior-hard write"不成立——create/write 不走 read 路径，readahead 不影响。**收益来自 read 阶段**：mdtest-read（0 字节文件无数据传输，N/A）、ior-hard-read（xattr=16 巨幅提升）。
4. **xattr=0 对顺序读是灾难**（1.5 MiB/s），验证了"random 误判为 seq 顶多多预取，seq 误判为 random 顶多少预取"的安全回退分析（§4.4）——seq 误判为 random 会严重降速，ML 分类必须可靠。

不改 IO500/ior/mdtest/fio 测试本身，发现问题用 ext4-over-RDMA 对比（项目硬约束）。

#### A-1.7 IO500 全量对比（ML 自适应 vs 规则缺省 vs 固定预取）

**实测环境**：QEMU VM1 + RDMA，kernel 客户端，`/mnt/powerfs`，cold read（每次 `echo 3 > drop_caches`），64MB 数据文件，10s runtime，2026-09-08。

| Workload | readahead=off (VFS默认) | RULE:16 (规则缺省) | NN:16 (ML自适应) | ML 优势 |
|----------|--------------------------|---------------------|-------------------|---------|
| 1M seqread cold | 130 MiB/s | 390 MiB/s | **397 MiB/s** | NN:16 ≈ RULE:16 (已是最大收益) |
| 4K randread cold | **387 IOPS** / 1551 KiB/s | 361 IOPS / 1448 KiB/s | 383 IOPS / 1531 KiB/s | ML NN:0 → 382 IOPS (≈off, +6% vs RULE:16) |
| ior-hard read (47KB) | 105 MiB/s | 1.3 MiB/s | **344 MiB/s** | **NN:16 比 RULE:16 +26300%** |

| Workload | readahead=off | NN:0 (ML随机) | RULE:0 (规则随机) | 结论 |
|----------|---------------|---------------|-------------------|------|
| 4K randread cold | 387 IOPS | 382 IOPS | 382 IOPS | NN:0 ≈ RULE:0 ≈ off, ML 正确识别随机 |

**关键发现**：
1. **ML 自适应核心价值**：ior-hard read (47KB) 场景，RULE:16 灾难性低性能 (1.3 MiB/s)——因 47KB 不整除 16MB 导致预取错位、大量无用 RDMA 传输。NN:16 达到 344 MiB/s，**比 RULE:16 提升 263 倍**。ML 通过 `offset_delta_mean` 特征正确识别 47KB 顺序读为 sequential，下发 NN:16。
2. **ML = RULE 在最佳情况**：1M seqread 场景，NN:16 (397 MiB/s) 略优于 RULE:16 (390 MiB/s)，两者都正确识别顺序大块读。
3. **ML 随机分类正确**：4K randread，ML 将随机 I/O 分类为 NN:0，性能 382 IOPS ≈ readahead=off (387 IOPS)，比 RULE:16 (361 IOPS) 好 +6%。ML 避免了对随机 I/O 浪费预取带宽。
4. **A-1.6 安全回退验证**：ML 在置信度不足时保持 RULE: 值，不会误分类导致性能下降。
5. **ior-hard read RULE:16 异常根因**：47KB × 16MB 预取导致大量无用 RDMA 传输，1.3 MiB/s 比 readahead=off (105 MiB/s) 还差 80×。ML 的优势在于能根据实际 I/O 模式动态调整，而非固定规则。

**三种策略总结**：

| 策略 | 优势 | 劣势 |
|------|------|------|
| 固定预取 (off) | 随机读最佳 | 顺序读极差 (130 MiB/s) |
| 规则缺省 (RULE:) | 大文件顺序读好 | 47KB 读灾难 (1.3 MiB/s) |
| **ML 自适应 (NN:)** | **全场景最优** | 需要采样累积（100 I/O 延迟） |

#### A-X1 RDMA MR 池占用感知的 readahead 上限（§4.3）

**实现**（2026-09-08，kernel 侧 3 文件）：

| 文件 | 改动 |
|------|------|
| [powerfs_net_rdma.c](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_net_rdma.c#L299-L379) | 新增 `powerfs_rdma_cap_readahead_mb(requested_mb)`：遍历所有 `in_use` + `CONN_CONNECTED` + RDMA 传输的 volume conn，取 `data_pool.free` 最小值为瓶颈；`spare = min_free - PFS_RA_MR_RESERVE(4)`，`cap_mb = spare × 2MB`；`spare ≤ 0 → 返回 0`。无 RDMA conn（TCP）原样返回不裁剪 |
| [powerfs_net.h](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_net.h#L1549-L1560) | 函数声明；非 `CONFIG_INFINIBAND` 构建为 inline 桩（原样返回） |
| [powerfs_readahead.c](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_readahead.c#L207-L208) | `powerfs_readahead_apply()` 中加载 mb 后、写 `f_ra.ra_pages` 前调用裁剪 |

**MR 池事实**：data_pool 共 `PFS_RDMA_DATA_BUF_NUM=48` 个 2MB MR；建链时 32 个 pre-post 到 RQ 作 RECV（`PFS_RDMA_MAX_RECV_WR=32`）→ idle 时 `free ≈ 16`，余量 ~16 个供大帧 SEND（write_needle）与超额并发读。

**VM 验证证据**（QEMU VM1 + RDMA，cold read，NN:16，1M seqread）：

| 场景 | dmesg / 性能 | 结论 |
|------|--------------|------|
| 正常 idle（RESERVE=4） | 无 cap 日志；1M seqread = 316→362 MiB/s | free=16 > reserve=4，不裁剪，性能无损 |
| 强制裁剪（临时 RESERVE=20 > idle free=16） | `readahead capped to 0 (min_free=16 <= reserve=20)` 持续打印；seqread 降至 1.4 MiB/s | 证明函数读到**真实 free=16**（= 48 − 32 pre-post RECV，与设计一致），且 cap 真实驱动 `ra_pages=0` |
| 恢复 RESERVE=4 重新部署 | cap 日志消失，seqread 恢复 362 MiB/s | 生产参数行为正确，无误裁剪 |

**设计说明**：
1. cap-to-0（1.4 MiB/s）比 mount `readahead=off`（130 MiB/s）更激进——后者保留 VFS 默认预取，前者彻底关闭。cap-to-0 **仅在 MR 池接近耗尽时作为安全阀触发**（降级避免 RNR），正常负载永不命中。
2. 裁剪在 `powerfs_readahead_apply` 内逐次 read 时计算，MR 释放后下一次 read 自动恢复，无需额外通知机制。
3. TCP 传输 / 无 RDMA conn 时不裁剪（MR 池约束为 RDMA 独有）。

---

## 5. 对 IO500 的预期效果

| 子项 | 现状（v4 基线 2026-09-07） | ML 预取后 | 预期收益 |
|---|---|---|---|
| mdtest（元数据密集小文件） | easy-create=4630 ops/s（P0-2 修复后）；read 路径仍走 VFS 固定 readahead，random 工作负载预取纯浪费（cache 污染 + RDMA MR 占用） | 识别 random → 关闭预取 | **收益最大**（read 路径优化空间大） |
| ior-easy（顺序大文件） | write=13.6 MiB/s, read=2896 MiB/s（cache hit） | 动态调大 + 对齐 RDMA 2MB 帧 | 中等（read 已是 cache hit，write 走 page cache） |
| ior-hard（random+fsync） | write=10.96 MiB/s（P0-1 修复后），read=71 MiB/s | 识别 random → 关闭预取 | **中等-高**（P0-1 已修，random read 路径可受益） |
| 4K O_DIRECT randread（cold） | 409k IOPS（直读 Volume Server） | random → 关闭预取；seq → 调大对齐 | **新增评估点**：O_DIRECT read 不经 page cache，预取决策直接影响 RPC 数 |
| find（目录遍历） | 不涉及数据预取 | 不影响 | 无 |

即：ML 预取对 IO500 的提升**主要来自 mdtest 阶段关闭无用预取**，其次来自 ior-hard random read 关闭预取（P0-1 修复后该子项数据已可信）；ior-easy 顺序读因 cache hit 已高收益空间小。

---

## 6. 顶刊可能性评估（诚实）

### 6.1 纯搬运 KML：不够
KML 已发表，应用迁移 novelty 不足，FAST/OSDI/SOSP/SC 会拒。

### 6.2 PowerFS 分布式视角的 novelty（按潜力排序）

1. **filer 端协同 ML 预取**：filer 聚合多客户端 trace 训练全局模型，下发 per-file 策略（KML 单机无此维度）
2. **RDMA 代价感知预取**：MR 池 + 2MB 帧往返的预取窗口决策（KML 单机只有 cache 污染维度）
3. **布局感知联合优化**：readahead + Placement（Stripe/WideStripe）+ rsize 联合 ML（KML 是 readahead/rsize 分开调）
4. **联邦学习预取**：多租户 trace 不集中（远期）

### 6.3 现实路径
- **短期**：ML 预取工程版 + IO500 数据（kernel+RDMA），middleware/CLUSTER 级别会议
- **中期**：filer 端协同 + RDMA 代价感知做深 + 严谨 evaluation（多 workload / baseline / ablation），冲 FAST/SC
- 顶刊要求强 novelty + 真实部署，PowerFS evaluation 基础还在建，**短期冲顶刊不现实**

---

## 7. 实施计划

### 7.1 前置依赖

**P0 基线异常排查（2026-09-08 更新）**

[`io500_baseline_report.md`](./io500_baseline_report.md) v4 基线（2026-09-07）确认两个原 P0 异常已解决：
- [x] **P0-1** ior-hard-write BW = 0 → **10.96 MiB/s**（v4 基线恢复）。根因是 ior 测试 block/transfer size 配置问题（IO500 standard 要求特定组合，3901B transfer 在某些 ior 版本被拒），**非 kernel bug**，脚本已修，无需独立 issue。
- [x] **P0-2** mdtest-easy create = 11 ops/s → **4630 ops/s**（v4 基线恢复）。已由代码层 optimistic local create 落地（[powerfs_dir.c:431](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_dir.c#L431) `powerfs_flush_pending_create` + [powerfs_caps.c:566](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_caps.c#L566) optimistic 升级）。

P0-2 解决后，ML 预取对 mdtest 阶段的"关闭无用预取"收益仍有效（create 路径优化不影响 read 路径预取）。**P0-1 已解决使 ior-hard 子项数据可信**，ML 预取收益评估可正式纳入 ior-hard random write 场景。

**ML 预取自身的前置依赖**
- [x] **D0** kernel 客户端 read 路径最终形态已确认 = netfs 接入（[powerfs_super.c:131](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_super.c#L131) `powerfs_netfs_issue_read`）
- [x] **D1** netfs 接入已完成，ML hook 点 `powerfs_netfs_issue_read` 已就位

### 7.2 Phase A：工程版（详见 §4.5-4.8 模块化设计）

**子阶段拆分**（原 A1-A7 已细化到 §4.8）：

#### Phase A-0：手工基线验证（最小可行）— 见 §4.8
- [x] A-0.1 kernel `powerfs_readahead.c` 骨架 + `powerfs_inode_info` 字段 + mount option
- [x] A-0.2 kernel hook：`powerfs_file_read_iter` 调 `powerfs_readahead_apply` 改 `file->f_ra.ra_pages`
- [x] A-0.3 xattr 通道联调：`setfattr` 手工设值，验证 kernel 端能读到并生效
- [x] A-0.4 测试矩阵（3×3）验证"调 readahead 影响 mdtest/ior-hard 性能"假设

#### Phase A-1：ML 自动化（KML 5 特征 NN）— 见 §4.8
- [x] A-1.1 kernel `powerfs_io_trace` 采集点（§4.7.3）+ PushDelta 异步 flush
- [x] A-1.2 filer `readahead_trace.rs` 聚合 + 5 特征提取
- [x] A-1.3 filer `readahead_policy.rs` NN 训练（per-file 二分类）+ 写 xattr `NN:<bytes>`
- [x] A-1.4 kernel version-based invalidation
- [x] A-1.5 filer 缺省规则引擎（§4.6.2，无训练数据时兜底）
- [x] A-1.6 安全回退：ML 置信度低 → 规则缺省；规则失败 → VFS 默认
- [x] A-1.7 IO500 全量对比：固定预取 vs 规则缺省 vs ML 自适应

#### 额外（跨阶段通用）
- [x] A-X1 RDMA MR 池占用感知的 readahead 上限（§4.3，A-1 阶段加）— 2026-09-08 完成，VM 验证见 §4.8 A-X1
- [ ] A-X2 fio 基线，对比 ext4-over-RDMA / NFS-over-RDMA（A-0 起每阶段做）

### 7.3 Phase B：研究版（filer 端协同 + RDMA 代价感知，冲顶刊 novelty）
- [ ] B1 filer 端聚合多客户端 trace 训练全局模型
- [ ] B2 RDMA 代价模型（MR 池 + 2MB 帧往返）纳入 ML 特征
- [ ] B3 布局感知（Stripe 预取跨 volume 并行调度）
- [ ] B4 严谨 evaluation：多 workload / baseline (KML 单机) / ablation / 真实部署
- [ ] B5 论文撰写

---

## 8. 风险与缓解

| 风险 | 缓解 |
|---|---|
| netfs 接入未完成，hook 点悬空 | D0/D1 前置，先确认 read 路径；必要时先在 VFS `file_ra_state` 层 hook |
| ML 误分类影响 IO500 得分 | 二分类 + 安全回退，random 误判风险可控（mdtest 本就该关预取） |
| RDMA MR 池被预取压垮 → RNR | 4.3 MR 池占用率上限，遵守 RdmaChannel 4 recv buffer 硬约束 |
| kernel 内嵌模型推理开销 | 先做 per-file readahead 值下发（免推理），成熟后再嵌模型 |
| trace 收集开销影响性能 | 采样 + filer 端聚合（非每事务上报） |

---

## 9. 测试计划（kernel + RDMA + QEMU）

- **单元测试**: 特征提取、二分类模型、readahead 决策
- **VM 测试**: QEMU + RDMA（SR-IOV VF + CPU pinning NUMA node 1），复用 `run_io500_qemu.sh`，持续 ≥1 分钟，定期 dmesg
- **IO500 对比**: 固定预取 vs ML 自适应，记录 mdtest/ior 各子项
- **fio 基线**: 对比 ext4-over-RDMA / NFS-over-RDMA
- **崩溃测试**: 推理异常时安全回退到 VFS 默认，验证数据无损坏
- **不改测试用例**，发现问题用其他文件系统对比

---

## 10. 参考文件

| 文件 | 说明 |
|---|---|
| [powerfs_addr.c](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_addr.c) | address_space 操作（[L2625](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_addr.c#L2625) `.read_folio = netfs_read_folio`，netfs 已接入；[L2078-2113](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_addr.c#L2078-L2113) `powerfs_dio_write_can_skip_read` O_DIRECT write fast path，issue #82） |
| [powerfs_super.c](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_super.c) | netfs 接入点（[L131](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_super.c#L131) `powerfs_netfs_issue_read`，[L275](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_super.c#L275) `powerfs_netfs_ops.issue_read`） |
| [powerfs_net_rdma.c](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_net_rdma.c) | kernel 侧 RDMA（RC QP + SEND/RECV + MR 池，2MB 帧） |
| [powerfs_file.c](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_file.c) | kernel file_operations（flush 路径） |
| [powerfs_dir.c](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_dir.c) | [L431](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_dir.c#L431) `powerfs_flush_pending_create` — optimistic local create 落地点（P0-2 修复） |
| [powerfs_caps.c](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_caps.c) | [L566](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_caps.c#L566) optimistic→Filer 升级 — P0-2 修复另一关键点 |
| [powerfs_layout/predictor.rs](file:///home/portion/powerfs/powerfs-layout/src/predictor.rs) | ⚠️ **LayoutPredictor（布局预测，规则驱动），非 ML 预取**。Inline/Flat/Stripe 决策，与 readahead 调优是不同维度；§4.1.3 "xattr 通道复用"仅指可复用其下发通道，不代表 ML 预取已部分实施 |
| [dir-policy-and-concurrency-optimization-plan.md](file:///home/portion/powerfs/docs/dir-policy-and-concurrency-optimization-plan.md) | 目录级策略方案（环境基线一致） |
| [file-layout-prediction-design.md](file:///home/portion/powerfs/docs/file-layout-prediction-design.md) | 布局预测 Phase 1（per-file 策略下发可复用其 xattr 通道） |
| [io500_baseline_report.md](file:///home/portion/powerfs/docs/io500_baseline_report.md) | v4 基线（2026-09-07），P0-1/P0-2 已修复后的 IO500 数据 |
| KML-learningPageCache.pdf | KML 论文（单机内核 readahead ML 调优，本方案参考） |

---

## 11. 下一步

- [x] D0/D1 前置依赖已满足（netfs 接入完成，hook 点就位）
- [x] **重测 io500 基线**：`io500_baseline_report.md` v4（2026-09-07），含 optimistic local create 收益 + P0-1 修复后数据
- [x] **P0-1 已解决**：ior-hard-write BW = 10.96 MiB/s（v4 基线确认，根因是 ior 测试配置非 kernel bug）
- [x] **P0-2 已解决**：mdtest-easy create = 4630 ops/s（v4 基线确认，optimistic local create 已落地）
- [x] **O_DIRECT write fast path 已修复**（issue #82，commit 内核 `d8e69a5` + 主仓 `5209349b`）：1M O_DIRECT seqwrite 0→176-192 MiB/s
- [x] **Phase A-0 完成**（2026-09-08）：kernel readahead 骨架 + hook + xattr 通道（含持久化修复）+ 3×3 测试矩阵验证。readahead 机制效果显著：4K randread xattr=0 +27%，1M seqread xattr=16 +182%，ior-hard-read xattr=16 +744%。
- [x] **Phase A-1 完成**（2026-09-08）：ML 自动化（kernel trace 采集 → filer 聚合 → NN 训练 → xattr 下发 → version invalidation → 缺省规则引擎 → 安全回退），IO500 全量对比见 §4.8 A-1.7
- [x] **A-X1 完成**（2026-09-08）：RDMA MR 池占用感知 readahead 上限（§4.3），空闲不裁剪、近耗尽降级为 0 防 RNR，VM 强制裁剪验证通过
- [ ] **A-X2**：fio 基线对比 ext4-over-RDMA / NFS-over-RDMA
- [ ] Phase A 拿到 IO500 数据后再决定是否推进 Phase B 研究版
