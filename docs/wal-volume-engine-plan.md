# Volume 存储引擎 v2 — 统一日志结构（WAL）方案

状态：方案讨论稿（已确认方向，待实施）
分支：feature/volume-wal（基于 feature/write-predict-dedup @ 259cb611）
日期：2026-09-10
客户端定位：Kernel 客户端为主，FUSE（Rust）客户端为辅

***

## 1. 背景与目标

### 1.1 现状问题（v1 needle 引擎）

| #  | 问题         | 根因                                                                                                   |
| -- | ---------- | ---------------------------------------------------------------------------------------------------- |
| P1 | 双写不一致窗口    | 数据文件 append 与 RocksDB index 更新是两次独立持久化，中间 crash 产生孤儿副本 / index 指向坏数据（`size mismatch` race 补丁是止血不是治本） |
| P2 | 回收代价失控     | compact = 停写自旋等待（最长 60s）→ 全量重写 → truncate；`garbage_bytes` 只有这一个回收出口                                  |
| P3 | fsync 语义模糊 | 数据文件 fsync 时机分散；RocksDB WAL 靠 30s 空闲线程；ack 返回时数据不保证落盘                                                |
| P4 | 无时间点一致性    | 无法回答"恢复到 T 时刻"；仅有 RocksDB checkpoint（元数据级），无数据快照                                                     |
| P5 | 空间管理原始     | append\_offset 单调 + truncate；无预分配段、无引用计数、无按段回收                                                       |

### 1.2 目标

1. **原子性**：写入/删除/索引变更作为单一日志事务，恢复后要么全部可见要么全部不可见。
2. **明确持久化语义**：ack 语义分层（accepted / durable），由参数开关控制（默认异步，strict 模式同步）。
3. **不停写回收**：垃圾回收按段粒度后台执行，无停写窗口、无限速可配。
4. **快照基础设施**：O(1) 快照创建、回滚、克隆；GC 与快照引用计数统一。
5. **可验证性**：记录哈希链 + 确定性故障注入测试，恢复结果可断言。
6. **客户端零改动**：Kernel / FUSE 客户端协议面（TLV 0x0066/0x006B/0x006C 等）保持不变，语义在服务端升级。

### 1.3 非目标（本期）

- sub-needle 细粒度随机覆写（延续 coalescer 合并语义，见 §12 局限）。
- 文件级时间点恢复（需 filer/metadata 配合，远期）。
- 跨 volume 事务。
- 数据压缩 / EC（现有 NeedleInfo 的 ec 字段语义保留位，不在本期实现）。

***

## 2. 设计取舍：为什么是"统一日志"而不是"WAL + 数据文件"

关键负载特征：FUSE/Kernel 客户端已把文件切成 **\~4MB 顺序 chunk**（对齐写，经 `WriteNeedleBlob`/`BatchWriteNeedleBlob` 下发）。在这个特征下：

- **双写无收益**：数据本身就是顺序大块追加，"先写 WAL 再异步物化到数据文件"（deferred 模式）意味着每字节两次写。统一日志把数据与索引变更放进同一条流，一次 append + 一次 fsync 即完成提交。
- **消除元数据引擎套娃**：v1 的 RocksDB-per-volume 是"元数据引擎自身又需要一层日志与压缩调度"。v2 的索引物化（checkpoint）是自写扁平不可变文件，无次级 compaction 抖动、无 block.db 容量规划、无 spillover。
- **快照天然成立**：段不可变 + 世代引用，快照 = 冻结引用，GC = 引用计数归零。

代价与对策：段不可变带来段内空洞，需要搬移式 GC（§8）； Needle 级索引全部驻内存（§10 内存估算）。

***

## 3. 总体架构

```text
┌────────────────────────────────────────────────────────────────┐
│ Volume Server                                                   │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ WalVolumeEngine (v2)                                      │  │
│  │                                                           │  │
│  │  写路径                内存状态             落盘形态        │  │
│  │  ┌──────────┐   ┌──────────────────┐   ┌────────────┐   │  │
│  │  │group     │──▶│ CommitQueue      │──▶│ active seg │   │  │
│  │  │commit    │   │ Index(HashMap)   │   │ (append)   │   │  │
│  │  └──────────┘   │ RefCnt/GenTable  │   └─────┬──────┘   │  │
│  │                 │ SnapTable        │         │ 封段      │  │
│  │  读路径 ────────▶│ SegManifest      │◀────────┘ sealed   │  │
│  │                 └────────┬─────────┘   seg_001..N       │  │
│  │                          │ 定期物化                      │  │
│  │                 ┌────────▼─────────┐   ┌────────────┐   │  │
│  │                 │ CheckpointWriter │──▶│ ckpt_<seq> │   │  │
│  │                 └──────────────────┘   └────────────┘   │  │
│  │  后台：GcWorker(搬移/整段删)  CheckpointScheduler  fsync  │  │
│  └──────────────────────────────────────────────────────────┘  │
│  兼容层：保留 Volume 公开 API，服务层协议不变                     │
└────────────────────────────────────────────────────────────────┘

Volume 目录布局：
  volume_<id>/
    superblock.a / superblock.b     # 双副本轮换，指向最新 checkpoint + active seg
    seg_0000000000000001.log        # 日志段（active → sealed 不可变）
    seg_0000000000000002.log
    ckpt_0000000000000010.bin       # 不可变 checkpoint
    staging/                        # tombstone 保留期数据引用（逻辑，非物理拷贝）
    gc_scratch/                     # GC 搬移临时文件
```

核心不变量（崩溃任意时刻断电，重启后必须成立）：

- **I1**：所有已 durable-ack 的操作在恢复后可见（strict 模式）/ 在持久化窗口内可能丢失（async 模式，显式声明的取舍）。
- **I2**：索引状态 == 从最新 checkpoint 重放其后全部完整记录的结果。
- **I3**：任何被活跃快照或 staging 保留期引用的物理副本不被回收。
- **I4**：空间统计（used/free/garbage/staging）与索引、段清单严格一致。

***

## 4. 磁盘格式

### 4.1 段文件（segment）

- 文件名：`seg_<seg_id:016x>.log`，seg\_id 单调递增。
- 默认段大小 256 MiB（可配 `wal_segment_size`）。段满 → sealed（不可变）→ 开新段。
- 段头 64 字节，一次写入：

```text
offset  size  field
0       8     magic        "PFWLSEG\0"
8       2     format_ver   u16 = 1
10      2     header_len   u16 = 64
12      8     seg_id       u64
20      8     volume_id    u64
28      8     base_lsn     u64   # 本段首条记录 LSN
36      8     created_ts   i64   # unix seconds
44      4     flags        u32   # bit0: sealed（写 sealed 记录后置位并回写）
48      16    reserved     零填充
64      4     header_crc   u32 (crc32c over [0..64))
```

### 4.2 记录帧（record frame）

每条记录 = 26B 定长头 + payload，段内紧密连续追加：

```text
offset  size  field
0       8     prev_crc   u64  # 前一条记录的 crc 字段（段首记录填链种子：段头不可变字段区 [0..44) 的 crc32c 的 u64 扩展，封段置位 flags 不改种子）
8       4     crc        u32  # crc32c(type|flags|lsn|payload)
12      4     len        u32  # payload 长度
16      1     rtype      u8
17      1     flags      u8   # bit0: F_SYNC_BARRIER（组提交 fsync 边界标记）
18      8     lsn        u64
26      len   payload
```

- **哈希链**：`prev_crc` 链接前一条记录 → 检测记录缺失/乱序/段内错位（不仅检单条损坏，还检"少了整条"）。借鉴 TigerBeetle 的 prepare 链。
- **PAD**：段尾剩余空间 < 所需记录长度时，写一条 `rtype=PAD` 的记录占满到段尾（`len` = 填充字节数），保证记录永远不跨段。
- 单条记录（含 DATA payload）无大小上限（除段大小约束）；4MB chunk 一条 DATA 记录直接放下，无需分片重组逻辑（对比 RocksDB 32KB 分块的设计动机：其记录是元数据、频繁小于块。我们的 DATA 是大块、一条一连续区间，跨段才有意义而跨段禁止）。

### 4.3 记录类型（rtype）

| rtype | 名称           | payload                                                             | 语义                                                                                       |
| ----- | ------------ | ------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| 0x01  | DATA         | `needle_id u64 \| data_len u32 \| data`                             | 写入/覆写 needle。重放时建立/更新索引 `needle_id → (seg_id, offset, len, version_lsn)`；同 id 重放取最后一条    |
| 0x02  | DELETE       | `needle_id u64 \| deleted_at i64 \| retention_until i64`            | tombstone。重放时将条目移入 tombstone 区（保留 7 天可 restore）                                          |
| 0x03  | ATTR         | `needle_id u64 \| attr_mask u32 \| values`                          | WORM 锁定等属性变更                                                                             |
| 0x04  | SNAP\_TAKE   | `snapshot_id u64 \| group_id u64 \| name_len u16 \| name \| ts i64` | 创建快照（O(1)，见 §9）。group\_id=0 为单卷快照；跨卷 EC 条带由协调器对 k+m 卷下发同一 snapshot\_id + group\_id（§9.4） |
| 0x05  | SNAP\_DROP   | `snapshot_id u64`                                                   | 删除快照，释放其引用                                                                               |
| 0x06  | CKPT\_ANCHOR | `ckpt_seq u64 \| applied_lsn u64`                                   | checkpoint 锚点：applied\_lsn 之前的段在满足引用条件后可回收                                               |
| 0x07  | VOLUME\_META | `field_mask u32 \| values`                                          | volume 级元数据（collection、state 变更等）                                                        |
| 0x7F  | PAD          | `len 即填充长度`                                                         | 段尾填充，重放时跳过                                                                               |

幂等性：重放是"按 LSN 顺序应用最后一态"，天然幂等；DATA 记录自带数据，无需 redo/undo 区分（借鉴"KV 化事务"思路：数据与索引变更同记录）。

### 4.4 Checkpoint 文件

- 文件名：`ckpt_<ckpt_seq:016x>.bin`，写完成后改名（tmp → final）+ fsync，**不可变**，同文件系统 hardlink 可实现近零成本克隆。
- 内容布局：

```text
[header]
  magic "PFWLCKPT\0" | format_ver | ckpt_seq | applied_lsn | volume_id
  | active_seg_id | next_seg_id | next_needle_id | next_snapshot_id
  | created_ts | counts...
[segments]      # 段清单，按 seg_id 升序
  per seg: seg_id u64 | state u8 (sealed/active/gc_pending) | live_bytes u64
           | dead_bytes u64 | record_range [base_lsn, last_lsn]
[needles]       # 活跃 needle 索引，按 needle_id 升序（支持二分）
  per entry: needle_id u64 | seg_id u64 | offset u64 | data_len u32
             | checksum u64 | checksum_algo u8 | version_lsn u64
             | refcnt u32 | created_at i64 | flags u32 (worm/ec/...)
[tombstones]    # 已删除待回收，按 retention_until 升序（GC 扫描友好）
  per entry: needle_id u64 | seg_id u64 | offset u64 | data_len u32
             | deleted_at i64 | retention_until i64
[snapshots]     # 活跃快照表
  per entry: snapshot_id u64 | root_lsn u64 | created_ts i64 | name
[alloc_stats]   # used_bytes / garbage_bytes / staging_bytes / counts
[footer]        | entry_count | crc32c(whole file)
```

- 全量物化（非增量）。索引量级：100 万 needle × 56B ≈ 56MB，写一次 < 1s（顺序写 + 后台线程），可接受；未来量大再演进增量 checkpoint。
- 快照引用的旧版本 needle 不在 `[needles]` 中单独罗列，而是**同 id 多版本条目**：`needle_id → [version list]`（通常 1 项；仅被快照钉住的 id 才 >1 项），见 §9.2。

### 4.5 Superblock（双副本轮换）

- 两个固定文件 `superblock.a` / `superblock.b`，各 128B，写入流程（barrier write）：
  1. 写副本 A（含 `seq+1`）→ fsync → 原子 rename 确认 →
  2. 下次更新写副本 B。
- 挂载时读两份，取 `seq` 更大且 CRC 合法的一份；两份都损坏 → 拒绝挂载（`recovery_mode=absolute` 语义）。

```text
magic "PFWLSB\0\0" | format_ver | seq u64 | volume_id u64
| latest_ckpt_seq u64 | active_seg_id u64 | active_seg_size u64
| volume_size u64 | state u8 | created_ts | last_mount_ts
| min_live_snapshot_lsn u64 | crc32c
```

***

## 5. 写路径与组提交

### 5.1 语义分层（对齐"参数开关控制"原则）

| 模式                   | ack 语义                                        | 崩溃丢失窗口                                  | 适用            |
| -------------------- | --------------------------------------------- | --------------------------------------- | ------------- |
| `wal_mode=async`（默认） | 记录已写入 active 段的 OS page cache 并入组，返回 accepted | ≤ `wal_async_interval`（默认 5ms）内已 ack 的写 | 常规业务（副本层另有兜底） |
| `wal_mode=strict`    | 该批 fsync 完成后返回 durable                        | 0（除盘故障）                                 | 关键场景          |

无论何种模式，**`FlushNeedles`（TLV 0x006C）永远提升为真正的持久化屏障**：对该批 needle 的记录强制 fsync 后返回 → Kernel 客户端 `fsync()`/`release()` 语义不变。这是 async 默认模式下文件系统正确性的锚点。

### 5.2 组提交流程

```text
写者 ──┐
写者 ──┼─▶ CommitQueue ──▶ leader 取批
写者 ──┘        │
                ▼
   1. 为每条写生成 DATA 记录（分配 lsn，计算 crc/prev_crc）
   2. 追加到 active 段 write buffer（顺序 write syscall）
   3. 末条记录置 F_SYNC_BARRIER
   4. async: 交给 fsync 调度器（按 interval/bytes 聚合）
      strict: 本批立即 fsync
   5. fsync 完成回调 → 唤醒整批写者（leader/follower 模式摊薄 fsync）
   6. 同步更新内存 Index（enqueue 时即更新 → read-your-writes）
```

- **两阶段流水**：fsync 进行中，下一批已可写 page cache（各自等自己的 fsync 轮次），fsync 不阻塞入队（借鉴 RocksDB pipelined write 双队列）。
- **索引可见性**：enqueue 时更新内存索引（可见即 accepted）；strict 模式下 ack 时已 durable，两者一致；async 模式下 accepted-but-not-durable 的写崩溃后丢失——文件系统语义由 fsync/FlushNeedles 屏障兜底。
- **背压**：`max_dirty_bytes`（默认 64MiB）超限时入队阻塞（等价 v1 coalescer 的 budget eviction，但阻塞点在持久化而非物化）。
- **覆写**：覆写 = 新 DATA 记录追加，旧版本条目按 §9.2 判定是否被快照钉住；未被钉住则旧版本记入 dead\_bytes（GC 回收）。
- **幂等去重**：保留 v1 的"同 id + 同长度 + 同 checksum 跳过"检查（enqueue 前查索引），防御 RDMA 重放/重试（与 write-predict-dedup 分支的预测去重在入队前衔接，逻辑不变）。

### 5.3 删除

- `DELETE` 记录入组提交；重放/实时路径将条目移入 tombstone 区（`retention_until = now + 7d`），`used_bytes` 立即扣减、`staging_bytes` 增加。
- restore（保留期内）= 索引条目移回活跃区，记录对称调整。
- 保留期过期 → GC purge（§8）。

***

## 6. 读路径

```text
read(needle_id, off, len):
  idx = Index.lookup(needle_id)          # HashMap，O(1)
  if idx.absent or tombstoned: ENOENT
  seg = SegManifest.get(idx.seg_id)      # sealed 段映射不变；active 段可读已写入区间
  data = pread(seg.fd, idx.offset+HDR+off, len)
  校验 checksum（可选启用读校验采样/全量）
```

- sealed 段只读 mmap 友好；Linux unlink-with-open-fd 语义 + 段级 refcount（epoch guard）双保险，GC 搬移时在途读安全。
- 读缓存：v2 不新增（page cache 天然承担，sealed 段不可变对 cache 极友好）。

***

## 7. Checkpoint 调度

触发条件（满足其一，后台线程执行，不阻塞写）：

1. `applied_lsn - last_ckpt_applied_lsn > ckpt_lsn_distance`（默认 1 GiB 日志量）；
2. 距上次 checkpoint > `ckpt_interval`（默认 60s）且有任何写入；
3. 管理命令手动触发；
4. 优雅停机前强制一次。

流程：冻结索引快照引用（短临界区拿 Arc/epoch）→ 顺序写 tmp 文件 → fsync → rename → 写 CKPT\_ANCHOR 记录（入组提交）→ 更新 superblock。失败则丢弃 tmp 重试，不影响在线路径。

WAL 段回收条件（三个同时成立）：

1. 段内所有记录 LSN ≤ 最新 CKPT\_ANCHOR 的 `applied_lsn`；
2. 段不被任何活跃快照引用（§9）；
3. 段内 tombstone 已过保留期或已被 GC 处理。

***

## 8. 垃圾回收（替代 v1 全量 compact）

段级引用计数（checkpoint 维护 `live_bytes/dead_bytes` per seg）：

```text
GC 周期扫描 sealed 段：
  case A: dead_bytes == seg 有效区且 tombstone 全部过期
          → 整段删除（确认无在途读 epoch guard）→ 段清单移除
  case B: dead_bytes / live_bytes > gc_ratio（默认 0.3）
          且 dead_bytes ≥ gc_min_bytes（默认 32MiB）
          → 搬移：把该段存活 needle 以 DATA 记录写入新段（顺序，复用组提交）
            → 原段变全死 → case A
  case C: tombstone 过期 purge：dead_bytes 增加，used 不变（delete 时已扣）
```

- **不停写**：搬移走正常写路径（新段是 active 段追加），无停写窗口；GC 线程限速（`gc_max_bytes_per_sec`，默认 100MiB/s）避免 IO 突发。
- 触发即 v1 `should_compact` 的超集（墓碑比例 + 物理垃圾比例），且额外拥有"段粒度增量"这一 v1 不具备的性质。
- 原段回收后 `[seg_id]` 不复用，SegManifest 收缩。

***

## 9. 快照

### 9.1 创建 / 删除 / 回滚 / 克隆

- `SNAP_TAKE(snapshot_id)`：O(1)。只记录一条日志 + 维护 `min_live_snapshot_lsn`（活跃快照中最小的 root\_lsn）。
- `SNAP_DROP(snapshot_id)`：O(1) 记录 + 触发受影响 needle 版本的引用释放（惰性，GC 时兑现）。
- **回滚**：`volume_rollback(snapshot_id)` = 以快照索引视图为准生成新世代写入（将快照可见版本重放为当前版本 DATA 记录）；当前世代数据按普通覆写规则处理。
- **克隆**：新 volume 引用源卷 checkpoint（hardlink 不可变文件 + 索引引用），写时 CoW，同 §9.2 机制。
- 快照只读挂载（远期）：以快照 checkpoint + 只读段集合构造只读引擎实例。

### 9.2 懒 CoW 判定（核心）

不为快照做 O(n) 引用标记；覆写时按 LSN 判定：

```text
write_needle(id, data):
  cur = Index.lookup(id)             # 当前版本 (version_lsn = V)
  if cur 存在 且 V < min_live_snapshot_lsn:
      # 当前版本被某个活跃快照引用 → 保留旧版本
      VersionTable.pin(id, cur)      # refcnt = 覆写时刻活跃快照数（对 root_lsn > V 的）
      记录新 DATA（新 version_lsn）
  else:
      旧版本 dead_bytes += size（无快照引用，GC 直接可回收）
  ...正常组提交
```

- `VersionTable`：仅对"存在活跃快照后被覆写/删除"的 needle 保存多版本条目（预期占比小）；checkpoint 按 `[needles]` 的 version-list 序列化。
- DELETE 同判定：版本被钉住则移入 tombstone 时保留物理副本（`staging_bytes` 计），drop 快照后由 GC 释放。
- 正确性锚点：**I3 不变量** —— GC 回收任何段前校验段内全部副本不被 VersionTable / tombstone(未过期) / 活跃快照 root\_lsn 覆盖。

### 9.3 与空间统计的关系

```text
used     = 活跃 needle 逻辑字节（当前版本）
staging  = tombstone 保留期内的物理字节（可 restore）
pinned   = 被活跃快照钉住的旧版本字节（新统计项）
garbage  = 段内死字节（GC 可回收）
free     = volume_size − used − staging − pinned
```

快照使"逻辑删除"不释放物理空间直到 drop —— 文档与指标必须显式暴露 `pinned`，避免"删了文件空间没回来"的误报（与 v1 garbage 语义同级，但归因清晰）。

### 9.4 跨卷 EC 条带的一致性域

引擎对 EC 无感知：分片即普通 blob（DATA 记录落到条带对应的 k+m 个 volume），`ec_enabled/ec_k/ec_m/ec_shards` 元数据留在上层（EC 规划层）。需要固化的约定：

1. **分片写入即普通写**：分片 blob（约 data\_len/k）以独立 needle 身份写入各目标卷；同 id + 同 checksum 幂等跳过 → 协调器重试/部分失败重发天然安全。
2. **快照必须整组**：volume 级快照只覆盖本卷。EC needle 的完整时间点一致性 = 对条带全部 k+m 卷做**组快照**（同一 snapshot\_id + group\_id，允许窗口内先后完成，组元数据由协调层记录）。**回滚与克隆禁止对 EC 卷单卷执行**，必须整组进行——单卷回滚会造成 stripe 永久不可解。
3. **删除/覆写**：协调器对各卷下发 DELETE，各卷独立 tombstone 保留期，本引擎机制不变。
4. **孤儿分片回收**：repair/重平衡 = 读分片 → 幂等写新卷 → 删旧卷；scrub + `ListNeedles` 支持协调层对账（对照 filer 侧 ec\_shards 元数据）。
5. **引擎侧改动仅为**：SNAP\_TAKE 携带 group\_id（格式已预留）、snapshot\_list 返回 group\_id、Full/ReadOnly 状态变更即时可查（协调器选卷依据）。

***

## 10. 崩溃恢复

```text
mount():
  1. 读 superblock.a/b → 取合法最大 seq → 得 latest_ckpt_seq / active_seg_id
  2. 加载 ckpt_<seq>（CRC 校验；损坏则回退上一个 ckpt_seq，superblock 重写）
  3. 重建内存 Index / SegManifest / VersionTable / SnapTable / alloc_stats
  4. 从 checkpoint 记录的重放起点（各段 base_lsn > ckpt 重放游标）顺序扫描
     seg_[checkpoint 时 active 段] .. 当前 superblock.active_seg_id：
       - 校验 prev_crc 哈希链 + crc
       - 尾部第一条非法记录起视为 crash tail → 按 recovery_mode 处理
       - 合法记录逐条重放（DATA→索引 / DELETE→tombstone / SNAP_*→SnapTable）
  5. 重放完成后：封段开新段，写新 checkpoint（异步）+ superblock
  6. 后台 GC 追平（回收 crash 前遗留的 dead/staging）
```

恢复模式（配置 `recovery_mode`）：

| 模式                  | 行为                | 场景                    |
| ------------------- | ----------------- | --------------------- |
| `tolerate_tail`（默认） | 仅截断尾部撕裂记录，继续重放    | 单机正常 crash（半条写入是宕机常态） |
| `point_in_time`     | 遇中部损坏停在最后完整记录处并告警 | 有副本环境，损坏段交给副本修复       |
| `absolute`          | 任何 CRC/链校验失败即拒绝挂载 | 审计/WORM 场景            |

哈希链的作用：`prev_crc` 断链 = 中间缺记录（不只是尾部截断），`tolerate_tail` 也只允许"从最后一条合法记录到段尾"的区间被丢弃；中部断链在 `tolerate_tail` 下告警 + 停在该点（等同 point\_in\_time），防止静默跳过造成状态机分叉。

***

## 11. 空间与容量

- 预分配：active 段文件 `fallocate` 预留段大小，避免尾段 ENOSPC 写入半途失败；段大小配置与 volume 剩余空间联动（剩余 < 2×seg\_size 时缩短新段）。
- `OutOfSpace` 判定：`free ≤ 0`（free 含义见 §9.3）→ `VolumeState::Full`，恢复条件与 v1 相同（删除释放）。
- 物理超卖保护：`sum(segments 有效区) ≤ volume_size` 由段分配器保证（v1 的"逻辑 free 高但物理 append\_offset 满"分裂问题在段模型下天然消失）。

### 11.1 容量伸缩（扩容 / 缩容）

**扩容（grow）**

- 管理命令 → 一条 `VOLUME_META` 记录（field\_mask 携带 new\_volume\_size）→ 组提交后原子生效；superblock 下次轮换同步。
- 生效后 free 立即重算（`free = new_size − used − staging − pinned`），不触碰已有段、无数据搬移。
- 物理前提仅为底层文件系统有空间：段是独立文件，天然跟随 backend/设备扩容，无 v1 一次性预分配大文件的限制。

**缩容（shrink）**

- 前置校验：`new_size ≥ used + staging + pinned`（pinned 必须计入，否则快照钉住的旧版本会被挤出盘），不满足则拒绝并返回缺口值。
- 通过后同样走 `VOLUME_META` 记录；已有段不回收（回收只由 GC 依引用驱动），仅影响 OutOfSpace 判定与新段创建。

**换盘 / 在线迁移（演进）**

- 段文件模型使 backend 接口退化为段文件 create/append/read/delete；v1 的 `allocate_volume`（全量预分配）与 `truncate_volume`（compact 专用）均不再需要。
- 在线搬卷（换设备/跨节点）= 段级搬运：目标侧逐段复制 → 源侧尾部增量重放 → superblock 原子切换，归入 §16 send/recv 演进。

***

## 12. 客户端协同（Kernel 为主 / FUSE 为辅）

### 12.1 协议面：零改动

Kernel 客户端（TLV 协议，ClientType=Kernel 0x02）使用的 volume 数据面命令不变：

| 命令                         | 码点          | v2 语义变化                                                                              |
| -------------------------- | ----------- | ------------------------------------------------------------------------------------ |
| `WriteNeedleBlob`          | 0x006B      | async 模式返回 accepted（≤5ms 持久化窗口）；strict 返回 durable。coalescer 保留（段内合并仍有价值：减少 DATA 记录数） |
| `ReadNeedleBlob`           | 0x0066      | 不变（读-your-writes 由内存索引 enqueue 时更新保证）                                                |
| `FlushNeedles`             | 0x006C      | **升级为持久化屏障**：覆盖目标 needle 的 LSN 强制 fsync 后返回。Kernel fsync/release 的正确性锚点              |
| `BatchWriteNeedle`         | 0x0065      | 同 0x006B 批量语义                                                                        |
| `RangeLease`               | 0x0067      | 不变（与存储引擎解耦）                                                                          |
| `DeleteNeedle/BatchDelete` | 0x0064/gRPC | → DELETE 记录，tombstone 保留期不变                                                          |

FUSE 客户端（Rust，ClientType=Fuse 0x01）走同一命令面，天然跟随，无需改动。

### 12.2 Kernel 语义对齐要点

1. **fsync 正确性**：Kernel `fsync(fd)` → `FlushNeedles` → v2 强制 fsync 屏障 → 返回后数据 durable。async 模式下"写后不 fsync 就断电可能丢"是显式取舍（与用户确认的可靠性-性能权衡一致），strict 配置可全局/按 volume 收紧。
2. **读-your-writes**：enqueue 即更新内存索引，Kernel 在 ack 后立即读必命中（含 coalescer 脏数据优先级，逻辑保留）。
3. **cap/lease 协同不变**：cap 授权、recall、mark\_dirty\_cap\_w/xp 等机制在客户端侧，与本引擎解耦；唯一接口是 FlushNeedles 屏障时机（release/last-close 路径已存在）。
4. **错误码**：`STATUS_ERR_REDIRECT`（not leader）、`OutOfSpace`、WORM 拒绝等映射不变。

### 12.3 快照的客户端暴露（P3 后）

- 控制面双通道（遵守既有约束）：CLI 经 Master 代理（§19.1 `VolumeAdminProxy`）；Web 前端仅经 Monitor。Kernel/FUSE 数据面无感知。
- 回滚在 volume server 执行前需安全条件：该 volume 无活跃写客户端（或先广播 recall）——沿用既有 lease 机制实现。

***

## 13. 上层接口（volume server ↔ 引擎）

```rust
pub trait WalEngine: Send + Sync {
    // 兼容 v1 语义（服务层零改动）
    fn write_needle(&self, key: u64, data: Bytes) -> Result<NeedleInfo>;
    fn read_needle(&self, id: &NeedleId) -> Result<Bytes>;
    fn write_needle_blob(&self, key: u64, off: i64, size: i32, data: Bytes) -> Result<()>;
    fn read_needle_blob(&self, key: u64, off: i64, size: i32) -> Result<Bytes>;
    fn delete_needle(&self, id: &NeedleId) -> Result<()>;
    fn restore_needle(&self, id: &NeedleId) -> Result<()>;
    fn worm_lock(&self, id: &NeedleId, days: i64) -> Result<()>;
    fn flush_specific_needles(&self, ids: &[NeedleId]) -> Result<usize>;  // → 持久化屏障
    fn get_stats(&self) -> EngineStats;  // used/free/staging/pinned/garbage/counts
    fn gc_pass(&self) -> Result<GcReport>;          // 替代 compact()
    fn scrub(&self) -> ScrubResult;                  // 基于 checkpoint 全量校验

    // v2 新增
    fn snapshot_take(&self, group_id: u64, name: &str) -> Result<SnapshotId>;  // group_id=0 单卷；EC 组快照用同一 id（§9.4）
    fn snapshot_drop(&self, id: SnapshotId) -> Result<()>;
    fn snapshot_list(&self) -> Result<Vec<SnapshotMeta>>;  // SnapshotMeta 含 group_id
    fn snapshot_rollback(&self, id: SnapshotId) -> Result<()>;
    fn clone_to(&self, id: SnapshotId, target_volume: VolumeId) -> Result<()>;
    fn resize(&self, new_size: u64) -> Result<()>;  // 扩/缩容，VOLUME_META 记录（§11.1）
}
```

`WriteCoalescer` 保留（DATA 记录合并仍降低记录数与 fsync 次数）；`VolumeMetadata`（RocksDB）退役，checkpoint 文件替代；`AllocationStats` 语义映射到 §9.3。

***

## 14. 可观测性

| 类别 | 指标                                                                                |
| -- | --------------------------------------------------------------------------------- |
| 提交 | commit\_latency 直方图、group\_size 直方图、fsync\_queue\_depth、accepted-vs-durable lag   |
| 日志 | wal\_bytes\_per\_sec、active\_seg\_fill\_ratio、segment\_count、checkpoint\_lag\_lsn |
| 空间 | used/staging/pinned/garbage 四项分列、gc\_ratio per seg TopN                           |
| GC | gc\_migration\_bytes\_per\_sec、segments\_reclaimed、gc\_pending\_count             |
| 快照 | snapshot\_count、oldest\_snapshot\_age、pinned\_bytes per snapshot                  |
| 恢复 | last\_recovery\_duration、tail\_truncated\_lsn、recovery\_mode                      |

对齐 Monitor 上报通道（心跳附带 EngineStats）。

***

## 15. 测试与验证策略

1. **确定性故障注入模拟器**（P1 交付，核心资产）：
   - 在 record 写入、fsync 前后、checkpoint 各阶段、superblock 轮换点注入 crash；
   - 注入位翻转、段截断、superblock 损坏、记录丢失；
   - 断言：恢复后状态 == 按序重放全部 durable-ack 操作的模型结果（strict）/ durable-ack 操作（async）；
   - 随机调度器 + 种子回放（VOPR 式）。
2. **不变量断言常开**：I1–I4 在模拟器与单测中断言。
3. **集成**：现有 volume 单测移植；kernel 客户端 fio（randwrite/seqwrite/fsync 密集）对比基线；`powerfs-ci-local` 全套。
4. **迁移正确性**：迁移工具输出与源 volume needle 级校验（checksum 全量比对）。

***

## 16. 已知局限与演进

| 局限             | 影响                                | 演进方向                                                                        |
| -------------- | --------------------------------- | --------------------------------------------------------------------------- |
| needle 为最小覆写单元 | needle 内高频随机小写依赖 coalescer；极端负载放大 | sub-needle extent（索引扩为 extent map，DATA 记录支持 range 版本）                       |
| 全量 checkpoint  | needle 数千万级时物化时间上升                | 增量 checkpoint（分层 sorted-run）                                                |
| 单 volume 单日志流  | 峰值写入受单 active 段限制                 | 并行段组（per-collection / 分带），组提交按带聚合                                           |
| 无压缩/EC         | 大容量成本；跨卷 EC 的快照/回滚需整组协调           | 段级压缩（sealed 段透明压缩，快照/引用计数兼容）；EC 组快照约定见 §9.4（group\_id 已在格式预留），组协调由 EC 规划层承担 |
| 增量复制           | 异地容灾需整卷复制                         | send/recv：基于 birth generation 的块级增量流                                        |

***

## 17. 分阶段实施

| 阶段                     | 内容                                                                                                                                              | 交付/验收                       | <br />                    |
| ---------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------- | :------------------------ |
| **P1 日志核心**            | 段管理 + 记录帧（哈希链）+ 组提交（async/strict）+ FlushNeedles 屏障 + 崩溃恢复（tolerate\_tail）+ `WalEngine` trait 与 v1 并行可切换（\`volume\_engine=needle                 | wal\` 启动参数）                 | 模拟器断言 I1/I2；kernel fio 回归 |
| **P2 Checkpoint + GC** | checkpoint 文件 + superblock 双副本 + 三档恢复模式 + 段 GC（整段删/搬移/限速）+ tombstone purge + 四项空间统计 + 容量伸缩（§11.1）+ 管理工具（本地只读命令 + gc/checkpoint/resize 双面，§19.5） | WAL 保留量有界；断电恢复时长可测；I3/I4 断言 | <br />                    |
| **P3 快照**              | SNAP\_TAKE/DROP/rollback/clone + 懒 CoW（VersionTable）+ pinned 统计 + GC 引用兑现 + 快照 CLI 双面与 EC 组操作（§9.4/§19.4）                                       | 快照 O(1) 创建；快照存在时覆写正确性模拟测试   | <br />                    |
| **P4 高级**              | 快照只读挂载、迁移工具 needle→WAL 与 CLI（Master 编排在线搬卷，§19.2）、确定性模拟器扩展（位翻转/段损坏矩阵）、压测调参                                                                      | 迁移全量校验通过；长稳运行               | <br />                    |

配置项汇总（volume server 配置新增）：`volume_engine`、`wal_mode`、`wal_segment_size`、`wal_async_interval`、`max_dirty_bytes`、`ckpt_interval`、`ckpt_lsn_distance`、`recovery_mode`、`gc_ratio`、`gc_min_bytes`、`gc_max_bytes_per_sec`。

***

## 18. 迁移（needle → WAL）

- 离线工具 `volume-migrate`：停写 → 扫描旧 needle（索引为准）→ 流式生成 DATA 记录写入新引擎 → 全量 checksum 校验 → 切换目录布局（旧数据转 `legacy/` 保留回退）。
- 在线双跑（可选，P4 后评估）：新引擎挂旁路，双写观察期，校验一致后切换。考虑实施成本，默认离线迁移。

***

## 19. 管理工具（本地 + CLI 双执行面）

### 19.1 双执行面定位

| 执行面     | 形态                                                                  | 适用场景                            | 路径                                                                                      |
| ------- | ------------------------------------------------------------------- | ------------------------------- | --------------------------------------------------------------------------------------- |
| **本地面** | `powerfs-volume admin <cmd>`（volume server 二进制子命令，直接打开本地 volume 目录） | 维护窗口、网络断裂、紧急恢复、离线迁移（§18 工具天然本地） | 本地文件系统，不经网络                                                                             |
| **远程面** | `powerfs-cli volume <cmd> -m <master:9333>`                         | 日常运维、批量操作、审计集中                  | CLI → Master `VolumeAdminProxy` gRPC → Master 以自身客户端身份调用目标 volume server 的 AdminService |

约束对齐：

- CLI 只跟 Master 交互（既有硬约束）→ 远程面一律经 Master；Master 依心跳拓扑定位 volume 所在节点。
- "server 不转发"原则不受影响：Master→volume 是 Master **主动发起的新请求**（代理），不是存储数据面的请求转发。
- admin 权限校验在 Master（远程面）；审计日志双写（Master 记 who/when/cmd/target，volume 记执行结果）。

### 19.2 命令树（映射 §13 引擎 API）

```text
powerfs-volume admin                      # 本地面
  stats       <volume_dir>                # 只读：四项空间统计 + 段/ckpt/快照概览
  list-needles <volume_dir> [--prefix N]  # 只读：索引枚举
  verify      <volume_dir> [--deep]       # 只读：checksum 校验（deep=全量读）
  scrub       <volume_dir> [--dry-run]
  gc          <volume_dir> trigger|status [--ratio R]
  checkpoint  <volume_dir> trigger|status
  snapshot    <volume_dir> take|list|drop|rollback|clone ...
  resize      <volume_dir> --size <bytes>            # §11.1
  migrate     <src_volume_dir> <dst_volume_dir>      # P4，仅本地（§18）

powerfs-cli volume                        # 远程面（-m 指定 Master）
  powerfs-cli volume status   -m <m> --volume <id>
  powerfs-cli volume stats    -m <m> --volume <id>
  powerfs-cli volume resize   -m <m> --volume <id> --size <bytes>
  powerfs-cli volume gc       -m <m> --volume <id> trigger|status
  powerfs-cli volume checkpoint -m <m> --volume <id> trigger|status
  powerfs-cli volume snapshot -m <m> take --volume <id> --name <n>
  powerfs-cli volume snapshot -m <m> take-group --ec-group <gid> --name <n>   # §9.4 组快照
  powerfs-cli volume snapshot -m <m> drop|rollback|clone ...
  powerfs-cli volume scrub    -m <m> --volume <id> [--dry-run]
  powerfs-cli volume migrate  -m <m> --volume <id> --to-node <node>            # P4：Master 编排（下发指令，实际搬运由两端 volume server 执行）
```

### 19.3 并发安全与单写者原则

- **引擎文件锁**：`<volume_dir>/lock`（flock）。运行中的 volume server 启动即持锁、退出释放——这是"单写者"的物理保证。
- 本地面写命令（gc/checkpoint/snapshot/resize/migrate）先取锁：取锁失败 → 明确报 EBUSY 并提示"server 运行中，走 powerfs-cli 或停服后重试"。
- 本地面只读命令（stats/list-needles/verify/scrub --dry-run）**不取锁、只读打开**：仅读 superblock + 最新 checkpoint + sealed 段，不触碰 active 段（与 server 并发安全，读到的可能是略旧的一致快照，符合检查用途）。
- 远程面命令全部经运行中的 server（gRPC），与写路径共用引擎内部并发控制，无锁冲突。

### 19.4 EC 组操作约定

- `take-group / rollback-group / clone-group`（组操作）仅远程面提供，由 Master 协调：对条带 k+m 卷并发下发同一 snapshot\_id + group\_id，汇总各卷结果；部分失败时组状态标记 partial 并支持重试幂等补齐（引擎幂等写/幂等 SNAP\_TAKE 保证）。
- 单卷面提交组回滚请求 → 拒绝（错误信息指明该卷属于 EC 组），`--force` 可越过但计入高危审计。

### 19.5 交付阶段

- P2：本地只读命令 + gc/checkpoint/resize 双面（引擎能力就绪即暴露）。
- P3：snapshot 双面 + 组操作（Master 协调器随 §9.4 落地）。
- P4：migrate 双面（本地工具 + Master 编排的在线搬卷）。

***

## 附录 A：调研结论摘要

成熟系统四骨架（本方案全部采纳）：

1. **WAL 与状态机分离，apply 幂等可重入**——本方案的"重放即状态"，无 redo/undo 区分。
2. **自校验记录帧 + 哈希链**——RocksDB frame（CRC/len/type）+ TigerBeetle 前序校验和链，§4.2。
3. **组提交 + 两阶段流水**——leader-follower 批量 fsync，WAL 队列与可见性分离，§5。
4. **快照 = 冻结世代引用，回收 = 引用计数归零**——ZFS/Btrfs 世代模型 + BlueStore 懒 CoW + JuiceFS 三阶段删除，§8/§9。

避开的坑（BlueStore 教训）：

- 元数据引擎套娃（RocksDB-on-BlueFS）→ 本方案 checkpoint 扁平文件；
- deferred 双写的 flush 债务失控 → 统一日志无双写；
- 双分配器 → 单一段分配器；
- per-4K 校验元数据放大 → per-needle checksum（沿用）。

与 BlueStore 的定位差异：其细粒度 extent 分配为任意随机覆写负载服务；本方案面向 4MB 顺序 chunk 负载取统一日志甜区，细粒度覆写列为 §16 演进项。

调研来源：RocksDB Wiki（WAL Format / Pipelined Write / WAL Recovery Modes / Track WAL in MANIFEST / Backup）、Ceph 官方文档（BlueStore Internals / RBD Layering）与 ;login: "File Systems Unfit as Distributed Storage Back Ends"、SeaweedFS volume/compaction、JuiceFS GC 深度文章、TiKV raftstore 配置与 snapshot 机制、SQLite WAL checkpoint、PostgreSQL FPW、OpenZFS CoW、TigerBeetle data\_file/VOPR 内部文档。

***

## 附录 B：P1 执行计划与进度记录

P1 范围（§17）：段管理 + 记录帧（哈希链）+ 组提交（async/strict）+ FlushNeedles 屏障 + 崩溃恢复（tolerate\_tail）+ `WalEngine` trait 与 v1 并行切换。快照/checkpoint/GC 属 P2/P3，本阶段接口留位但不实现。

### B.1 步骤分解

每步完成即运行测试验证，并在 B.2 回填状态与提交。

| #  | 步骤            | 内容                                                                                                                     | 新增文件（powerfs-core/src/wal/）                | 测试与验收                                                          |
| -- | ------------- | ---------------------------------------------------------------------------------------------------------------------- | ------------------------------------------ | -------------------------------------------------------------- |
| S1 | 记录帧与编解码       | 帧结构（prev\_crc/crc/len/type/flags/lsn，§4.2）、rtype 定义与 payload 编解码（DATA/DELETE/ATTR/SNAP\_\*/VOLUME\_META/PAD）、帧级读写器     | `frame.rs`                                 | 单测：编解码 roundtrip、哈希链校验、尾帧撕裂检出、PAD 填充                           |
| S2 | 段文件与段管理       | 段头（§4.1）读写、SegWriter（append/seal/fallocate/剩余空间 PAD 换段）、SegReader（顺序扫描+链校验）、SegManifest（内存段清单）                         | `segment.rs`, `manifest.rs`                | 单测：seal/roll、段尾边界 PAD、跨重启重扫一致性、torn tail 定位                    |
| S3 | 内存索引与重放器      | WalIndex（needle 索引 + tombstone + 统计）、Replayer（段序重放 → 索引，tolerate\_tail 截断）                                             | `index.rs`, `replay.rs`                    | 单测：重放幂等（重放两次结果一致）、DELETE/restore 语义、统计与索引严格一致（I4 的重放侧）         |
| S4 | 组提交与 fsync 屏障 | CommitQueue（leader 聚批、async 批量 fsync / strict 同步、max\_dirty\_bytes 背压、两阶段流水）、flush barrier（按 LSN 集合等待）                 | `commit.rs`                                | 单测：并发组提交正确性、strict ack 即 durable、async 窗口语义、barrier 等待指定 LSN   |
| S5 | 引擎装配与恢复       | WalEngine（open→load ckpt 位（P1 跳过）→重放→开新段；write/read/delete/flush/stats）、卷级 flock 单写者锁                                  | `engine.rs`                                | 单测：kill -9 式 crash 注入（帧中/fsync 前）→ 重启恢复 == 已 ack 操作重放结果（I1/I2） |
| S6 | 确定性故障模拟器      | 模拟 IO 层（可注入 crash 点/位翻转/截断）、随机操作序列 + 断言模型（shadow model 比对）                                                             | `sim/`（tests 或独立 crate）                    | 随机种子回放：数千 crash 点组合全部通过 I1/I2 断言                               |
| S7 | v1/v2 并行切换    | `volume_engine=needle\|wal` 配置贯通 volume server；`WalEngine` 适配现有 Volume API 面（§13 兼容方法）；本地面 admin 只读命令（stats/verify）最小集 | config 修改 + volume server 接线 + `wal_admin` | 集成测试：双引擎跑同一测试集；grpc\_test 回归                                   |

### B.2 进度记录

| #  | 状态  | 完成内容   | 验证结果   | 提交     |
| -- | --- | ------ | ------ | ------ |
| S1 | 完成 | `wal/frame.rs`：帧头 26B 编解码（prev_crc/crc/len/rtype/flags/lsn）、8 种 rtype 及全部 payload 编解码（DATA/DELETE/ATTR/SNAP_TAKE/SNAP_DROP/CKPT_ANCHOR/VOLUME_META/PAD）、`encode_frame`/`encode_pad_frame`（PAD 恰好填满剩余空间）、`scan_one` 逐帧扫描（CRC + 哈希链校验）；全零头部识别为预分配 slack，非零残头/声明长度越界识别为撕裂 | 9 个单测全绿：全 rtype roundtrip、payload roundtrip 与畸形拒收、哈希链缺帧检出、CRC 位翻转检出、撕裂尾检出（半头/半帧）、零 slack 判定、PAD 填充/跳过/最小形态、lsn+flags 极值 roundtrip；`cargo clippy -p powerfs-core --lib` 对 wal/ 无告警 | 8bcd142b |
| S2 | 完成 | `wal/segment.rs`：SegmentHeader 68B 编解码（CRC 覆盖 [0..64)）、SegWriter（create/reopen/append/pad_to_end/seal/sync、fallocate 预分配、seal 先 fsync 数据再改段头）、SegReader（流式扫描 + 链校验 + scan_summary 定位撕裂尾）；`wal/manifest.rs`：SegManifest 目录扫描构建、归一化规则（有后继段的段一律 Sealed）、next_seg_id/register/mark_sealed | 19 个单测全绿（frame 9 + segment 10）：段头损坏检出、append/scan roundtrip 与偏移连续、段尾 PAD 恰好填满 + 换段链重播种、跨重启 reopen 续链、撕裂尾定位与 reopen 截断续写、fallocate 预分配零读、manifest 归一化/文件名校验/seal 幂等。**设计修正**：链种子改为段头不可变字段区 [0..44) 的 crc32c（seal 改写 flags 不改种子，避免封段后链校验失败） | e9ac5553 |
| S3 | 完成 | `wal/index.rs`：WalIndex（needle 索引 + tombstone + **DeadCopy 死副本账本** + 统计）、apply_data/apply_delete/restore/apply_ckpt_anchor，version_lsn 单调防护实现重放幂等；`wal/replay.rs`：replay_all 按段序重放（LSN 跨段单调校验、PAD 跳过、SNAP_*/ATTR/VOLUME_META P1 留位）、tolerate_tail 截断最后段撕裂尾并 fsync、非尾段撕裂/段中损坏拒绝 | 32 个单测全绿：重放两次结果逐字段一致（幂等）、DELETE/restore/复活语义（复活旧副本转死副本账本）、覆写垃圾记账、I4 统计核对（garbage = 死副本账本 + tombstone，assert_consistent 逐字节验证）、跨段 LSN 回退检出、最后段撕裂截断后重放幂等、非尾段撕裂拒绝、CKPT_ANCHOR 重放。**设计补充**：DeadCopy 清单落账，garbage_bytes 可与索引内容核对（同时是 P2 GC 的回收依据） | b6aa0232 |
| S4 | 完成 | `wal/commit.rs`：CommitQueue 组提交（多写者入队、队列锁承担 leader 聚批、LSN 全局单调分配从 1 起）、`CommitSink` trait（append=page cache、sync=持久化屏障，S5 由 SegWriter+换段实现）、async/strict 双模式 + `FLAG_SYNC_BARRIER` 记录永久提升为屏障（标记原样写入帧）、`flush_barrier`/`wait_durable` 按 LSN 等待（快路径不触发额外 fsync）、`max_dirty_bytes` 背压阻塞、两阶段流水（worker 解锁执行 sync，fsync 期间写者照常入队；锁内快照 target 保证 append happens-before sync）、sticky 错误（append/sync 失败后拒绝全部后续写）、close 排空 + 最终 fsync + join | 9 个单测全绿：并发组提交正确性（8 线程×50、lsn 稠密唯一、整批一轮 fsync 覆盖、payload 按 lsn 还原）、strict ack 即 durable（16 线程逐笔断言 durable_upto ≥ lsn）、async 窗口语义（窗口内零 fsync、barrier 后 durable、5ms interval 到期自动 sync）、barrier 等待未来 LSN + 已 durable 快路径、背压阻塞与预算释放、barrier 标记写入帧、append/sync 失败 sticky 传播、close 排空拒新写；全库 `wal::` 41 测试全绿 | 0187309d |
| S5 | 完成 | `wal/engine.rs`：WalEngine 装配（flock 单写者锁 → SegManifest 扫描 → replay_all 重放 → 重开活跃段/以 last_lsn+1 开新段 → CommitQueue 以 last_lsn+1 起始 LSN 启动）；`SegSink` 桥接 CommitSink（Mutex\<SegWriter\> 串行化追加、克隆 fd 执行 sync 不阻塞追加、段满自动 seal→开新段→登记清单→fsync 目录项、记录超段容量拒绝）；write/read/delete/flush/flush_all/stats API（DATA/DELETE payload 编码、read 索引 O(1) 定位 + pread + 帧 CRC 重算校验、delete 前存在性检查、tombstone 7 天保留期）；nix Flock 守卫（替代已弃用 flock 调用，Drop 即释放，声明序保证排空先于解锁） | 50 个单测全绿（wal:: 全量）：write/read/delete/覆写 roundtrip 与垃圾记账、重开保持状态并接续 LSN/needle id、kill -9 式帧中崩溃恢复到 durable 前缀（strict 5 条可见 + async 窗口 5 条丢失 + 续写单调 + 二次重启稳定，I1/I2）、flock 拒绝第二个写者、小段容量多段换段 + 跨段重放恢复、flush barrier 提升 async durable、8 线程并发写全可见且统计一致、引擎存活期间位翻转读路径 CRC 检出、记录超段容量入队前拒绝；**修复**：open 新建段未登记 manifest 导致换段 seg_id 冲突（File exists）、在位损坏测试改为引擎存活期间注入（重开会在 replay 阶段拒绝，与设计一致） | (随本次提交) |
| S6 | 未开始 | <br /> | <br /> | <br /> |
| S7 | 未开始 | <br /> | <br /> | <br /> |

### B.3 执行约定

- 代码注释仅描述 PowerFS 自身设计，不引用外部参考系统名称。
- 每步独立英文提交（`feat(wal): ...`），提交前 `cargo check` + 该步测试必须绿。
- 与 write-predict-dedup 的衔接点：幂等去重检查（同 id+长度+checksum 跳过）在 enqueue 前执行，S7 接线时保留语义。

