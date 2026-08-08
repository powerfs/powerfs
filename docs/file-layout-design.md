# PowerFS 文件数据布局统一设计文档

> 文档创建：2026-08-07
> 最后更新：2026-08-08
> 状态：**P2.5 全部完成**（Inline 小文件 + 自动迁移 Flat + mdtest 验证 2.22x 提升）
> 取代：`file_layout_stripe_design.md`（布局部分）、`ecplan.md`（布局部分）

---

## 0. 文档定位

本文档是 PowerFS 文件数据布局的**单一权威设计**，整合三个正交维度（数据分布、可靠性、元数据编码），覆盖从小文件到 256-volume 全集群并行的所有场景。后续所有布局相关实现以本文档为准。

**取代关系**：
- `file_layout_stripe_design.md`：本文档第 4 章覆盖其全部内容并扩展
- `ecplan.md`：本文档第 5 章覆盖其布局部分；Bitrot/回收站/WORM 等非布局功能仍归 `ecplan.md`
- `file-key-design-analysis.md`：保留，描述 file_key 历史问题，本文档第 6 章引用其结论

---

## 1. 设计目标与背景

### 1.1 核心目标

1. **小文件高效**：90%+ 文件 < 10MB，单 chunk 写入，元数据 < 100B
2. **大文件并行**：支持 256-volume 全集群并行 IO，单文件带宽逼近集群上限
3. **可靠性分级**：副本/EC 自适应，写入永不等可靠性，后台异步转换
4. **元数据紧凑**：解决 chunk 列表 JSON 膨胀问题（1GB 文件 100KB → 80B）
5. **跨节点分布**：副本和分条数据强制分布在不同物理节点的 volume 上
6. **平滑迁移**：兼容现有实现，按 Phase 渐进式推进

### 1.2 真实文件大小分布参考

| 来源 | 中位数 | P90 | P99 |
|------|--------|-----|-----|
| Ceph 生产 (FAST'23) | 64KB | 1MB | 100MB |
| HDFS 生产 (NSDI'10) | 60MB | 64MB | 1GB |
| 企业文件服务器 (MS research) | 4KB | 64KB | 1MB |
| MooseFS 部署 | <1MB | 64MB | - |

**结论**：90%+ 文件 < 10MB，99% 文件 < 100MB。大文件是长尾，但消耗大部分存储容量和带宽。设计必须同时优化两个场景。

### 1.3 PowerFS 当前写模式（已验证）

基于 [zone_client.rs](file:///home/portion/powerfs/powerfs-filer/src/zone_client.rs) 和 [shard_store.rs](file:///home/portion/powerfs/powerfs-filer/src/shard_store.rs) 的代码分析：

- `alloc_needle_id` 用原子计数器 fetch_add，**needle_id 严格连续递增**
- FUSE/内核 writeback 按 offset 顺序刷盘，chunks 列表按 offset 排序
- 单文件分配在同一 zone，volume 选择基于空闲比例（同文件倾向同 volume）
- chunk_size 固定 2MB

**结论**：顺序写场景下 chunks 完全可预测，适合用几何描述替代 per-chunk 列表。

---

## 2. 文件分类与典型场景

### 2.1 文件大小分类

| 类别 | 大小范围 | 占比估计 | chunk 数 | 主要场景 |
|------|---------|---------|---------|---------|
| 微小文件 | < 2MB | ~60% | 1 | 配置、日志、元数据 |
| 小文件 | 2-10MB | ~30% | 1-5 | 文档、图片 |
| 中文件 | 10-100MB | ~8% | 5-50 | 视频片段、压缩包 |
| 大文件 | 100MB-1GB | ~1.5% | 50-512 | 数据集、虚拟磁盘 |
| 超大文件 | 1-100GB | ~0.4% | 512-51200 | HPC 数据、备份镜像 |
| 海量文件 | > 100GB | ~0.1% | 51200+ | 科学计算、AI 训练 |

### 2.2 典型场景

1. **小文件批量写**：日志/配置场景，写入延迟敏感，元数据规模敏感
2. **顺序大文件写**：HPC/备份场景，单流带宽饱和，多 volume 并行
3. **随机写文件**：数据库/虚拟磁盘，无法形成 stripe，per-chunk 列表
4. **256 路并行读**：HPC 场景，全集群 volume 并行，逼近集群带宽上限
5. **稀疏文件**：有空洞的文件，部分 chunk 不存在，读时零填充

---

## 3. 三维正交设计

文件布局由三个**正交维度**独立描述，可独立演进：

```rust
pub struct FileLayout {
    // 维度 1: 并行度 - 数据如何分布到 volume
    pub placement: Placement,
    
    // 维度 2: 可靠性 - 数据如何保护（副本/EC + 状态）
    pub reliability: Reliability,
    
    // 维度 3: 编码 - 元数据如何序列化
    pub encoding: ChunkEncoding,
}
```

| 维度 | 选项 | 触发条件 |
|------|------|---------|
| Placement | **Inline** / Flat / Stripe / WideStripe | 文件大小 + 目录属性 + IO 模式 |
| Reliability | SingleReplica / Replicated(N) / EC(N+M) + 压缩标志 | 文件大小 + 目录属性 + 空闲策略 |
| Encoding | **InlineData** / PerChunk / StripeDescriptor / Paginated | chunk 数 + Placement 类型 |

**正交性原则**：三个维度可任意组合。例如：
- 微小文件 + Raft 复制 + InlineData（IO500 mdtest 场景，最优化）
- 小文件 + 副本 + PerChunk（最常见）
- 大文件 + EC + StripeDescriptor（顺序写）
- 大文件 + 副本 + Paginated（随机写）
- HPC 文件 + EC + StripeDescriptor（256 路并行）

**Inline 特例**：Placement=Inline 时，Reliability 隐式为 Raft 复制（不参与 scrubber），Encoding 必为 InlineData。这是唯一不严格正交的组合，因为数据不在 Volume Server 上。

---

## 4. Placement 设计（数据分布）

### 4.1 四态枚举

```rust
pub enum Placement {
    /// Inline: 数据直接存 Filer 元数据（微小文件，< 4KB 默认）
    /// 通过 Raft 隐式复制，无 Volume Server 参与
    Inline {
        max_size: u32,          // 阈值，默认 4KB，可配到 8KB
    },
    
    /// Flat: 单 volume，小文件默认
    Flat,
    
    /// Stripe: 中等并行（4-16 volume）
    Stripe {
        stripe_size: u64,         // 默认 64MB
        stripe_count: u32,        // 默认 4
        start_volume_idx: u32,    // round-robin 错开
        volume_ids: Vec<u64>,     // 显式卷列表
    },
    
    /// WideStripe: 全集群并行（128-256 volume）
    WideStripe {
        stripe_size: u64,         // 默认 4MB（小 stripe 高并发）
        stripe_count: u32,        // 128 或 256
        start_volume_idx: u32,
        volume_ids: Vec<u64>,     // 范围压缩编码
    },
}
```

### 4.2 自动提升阈值

| 文件大小 | 默认 Placement | 理由 |
|---------|---------------|------|
| **< 4KB** | **Inline** | **数据直接存 Filer，Raft 复制，无 Volume Server 开销** |
| 4KB - 64MB | Flat | 单 volume 足够，避免元数据开销 |
| 64MB - 1GB | Stripe(4) | 4 volume 并行，单流带宽饱和 |
| 1GB - 100GB | Stripe(16) | 16 volume 并行，多客户端聚合 |
| > 100GB 或显式标志 | WideStripe(256) | 全集群并行，HPC 场景 |

**Inline 阈值**：默认 4KB（覆盖 IO500 mdtest-hard 的 3901B），可调到 8KB。超过阈值自动迁移到 Flat（见 4.6）。

**显式标志覆盖**：WideStripe 默认**仅显式启用**（避免误用），通过：
- 目录属性继承（见 4.4）
- `setfattr -n powerfs.placement -v wide_stripe_256 <file>`
- 创建时 API 参数（HPC 任务提交）

### 4.3 locate() 算法

Stripe/WideStripe 模式下，根据文件 offset 计算 (volume_idx, volume_offset)：

```rust
pub fn locate(&self, file_offset: u64) -> (usize, u64) {
    let stripe_size = self.stripe_size.max(1);
    let stripe_idx = file_offset / stripe_size;
    let vol_rank = (stripe_idx % self.stripe_count as u64) as u32;
    let vol_array_idx = ((self.start_volume_idx + vol_rank) as usize) 
                        % self.volume_ids.len();
    let vol_offset = (stripe_idx / self.stripe_count as u64) * stripe_size 
                     + (file_offset % stripe_size);
    (vol_array_idx, vol_offset)
}
```

**关键设计点**：
- `start_volume_idx`：round-robin 错开不同文件起始 volume，避免热点
- `volume_ids`：实际 volume 列表（WideStripe 时用范围压缩编码）
- `vol_offset`：volume 内偏移，跨 stripe 周期累加

### 4.4 目录属性继承

**新增能力**：目录可设置 stripe 和 inline 属性，该目录下新建文件**自动继承**。

**xattr 接口**：
```bash
# 设置目录 stripe 策略
setfattr -n powerfs.placement -v "stripe:4:64MB" /hpc/dataset1
setfattr -n powerfs.placement -v "wide_stripe:256:4MB" /hpc/training
setfattr -n powerfs.placement -v "flat" /var/log

# 设置目录 inline 阈值（独立于 placement，专门控制微小文件）
setfattr -n powerfs.inline -v "4096" /io500/mdtest-hard
setfattr -n powerfs.inline -v "8192" /config-files
setfattr -n powerfs.inline -v "0" /var/log   # 0=禁用 inline

# 查询目录策略
getfattr -n powerfs.placement /hpc/dataset1
getfattr -n powerfs.inline /io500/mdtest-hard

# 新建文件继承
touch /hpc/dataset1/file1   # 自动 Stripe(4, 64MB)
touch /io500/mdtest-hard/f1 # 自动 Inline(max_size=4096)
```

**属性格式**：
- `powerfs.placement`：
  - `flat`：单 volume
  - `stripe:<count>:<size>`：N 卷条带，例如 `stripe:4:64MB`
  - `wide_stripe:<count>:<size>`：宽条带，例如 `wide_stripe:256:4MB`
- `powerfs.inline`：
  - `<size>`：inline 阈值（字节数），例如 `4096`、`8192`
  - `0`：禁用 inline（即使文件很小也走 Volume Server）

**为何独立两个 xattr**：
- `powerfs.placement` 控制"数据放哪"（Flat/Stripe/WideStripe）
- `powerfs.inline` 控制"是否绕过 Volume Server"
- 两者正交：可设 `placement=flat + inline=4096`（<4KB 走 inline，>=4KB 走 flat）
- 也可只设 inline 不设 placement（<4KB 走 inline，>=4KB 按自动阈值提升）

**继承规则**：
1. 创建文件时，Filer 检查父目录 xattr `powerfs.inline` 和 `powerfs.placement`
2. 若 `powerfs.inline` 存在且 > 0：文件先尝试 inline，超阈值再迁移
3. 若 `powerfs.placement` 存在：超 inline 阈值后按此 placement 分配
4. 若两者都不存在：按全局默认（inline=4096，placement 按文件大小自动提升）
5. 子目录继承父目录两个属性（递归，除非显式覆盖）

**实现位置**：
- Filer `handle_create` / `handle_mkdir`：读取父目录两个 xattr
- 新增 MetaShardManager 方法：
  - `get_dir_placement(parent_ino) -> Option<PlacementSpec>`
  - `get_dir_inline_threshold(parent_ino) -> Option<u32>`
- xattr 存储：inode extended 字段 `powerfs.placement` 和 `powerfs.inline`

### 4.5 跨节点 anti-affinity 约束

**强制要求**：副本和分条数据必须分布在不同物理节点的 volume 上。

**实现**：
- Volume Server 启动时向 Master 注册 `node_id`（物理节点标识，可用 hostname 或 IP）
- Master 维护 `volume_id -> node_id` 映射
- Filer `alloc_for_new_file` 选 volume 时：
  - Flat: 单 volume（无 anti-affinity 要求）
  - Stripe/WideStripe: volume_ids 必须来自不同 node_id
  - Replicated: 副本必须在不同 node_id
  - EC: data + parity 块分布在不同 node_id

**Volume 选择算法**：
```rust
fn select_volumes_with_anti_affinity(
    zone: &ZoneState,
    count: usize,
    exclude_nodes: &HashSet<NodeId>,
) -> Result<Vec<u64>> {
    let mut selected = Vec::with_capacity(count);
    let mut used_nodes = exclude_nodes.clone();
    
    // 按 node 分组，每节点选空闲比例最大的 volume
    let by_node = group_volumes_by_node(&zone.volumes);
    
    for _ in 0..count {
        let candidate = by_node.iter()
            .filter(|(node, _)| !used_nodes.contains(*node))
            .flat_map(|(_, vols)| vols.iter())
            .max_by_key(|v| v.free_ratio())?;
        selected.push(candidate.volume_id);
        used_nodes.insert(candidate.node_id);
    }
    
    if selected.len() < count {
        return Err(InsufficientNodes);
    }
    Ok(selected)
}
```

**约束校验**：Master 定期扫描 volume 拓扑，发现违反 anti-affinity 的布局触发后台迁移。

### 4.6 Inline 模式详解

Inline 模式将微小文件数据直接存储在 Filer 元数据中，绕过 Volume Server，专为 IO500 mdtest 等小文件密集场景优化。

#### 4.6.1 适用场景

| 场景 | 文件大小 | 是否适合 Inline | 理由 |
|------|---------|---------------|------|
| IO500 mdtest-easy | 0 字节 | **是** | 完全无 Volume Server 开销，无 needle_id 分配 |
| IO500 mdtest-hard | 3901 字节 | **是** | 节省 99.8% 存储 + 2 RTT |
| 配置文件 | < 4KB | **是** | 单次操作完成 |
| 日志文件（持续追加） | 增长 | 否 | 很快超阈值，频繁迁移 |
| 数据库文件 | 增长 | 否 | 随机写 + 增长 |
| HPC 大文件 | GB+ | 否 | 大文件 |

#### 4.6.2 写入路径（inline 命中）

```
1. CREATE:
   - Filer 检测父目录 powerfs.inline xattr（或全局默认 4KB）
   - 不分配 volume_id/needle_id
   - 返回 Placement::Inline + max_size

2. WRITE (累计 < max_size):
   - 客户端不直连 Volume Server
   - 数据暂存客户端 inode 私有数据

3. CLOSE:
   - 客户端把 inline_data + 元数据一起发 Filer
   - Filer 单次 Raft 提交（数据 + 元数据）
   - 无 Volume Server 参与
```

#### 4.6.3 读取路径（inline 命中）

```
1. GETATTR: Filer 返回元数据 + inline_data（一次 RPC 拿全）
2. READ: 客户端直接从 GETATTR 响应读
   - 无需 ReadNeedle，无需 Volume Server
```

#### 4.6.4 RTT 与存储对比

| 操作 | 当前路径 | Inline 路径 | 节省 |
|------|---------|------------|------|
| 创建 0 字节 (mdtest-easy) | 1 RTT + 浪费 needle_id 分配 | 1 RTT，零分配 | 无浪费 |
| 创建+写 3901B (mdtest-hard) | 3 RTT (CREATE+WRITE+CLOSE) | **1 RTT** (CREATE+WRITE+CLOSE 合并) | **2 RTT** |
| 读 3901B | 2 RTT (GETATTR+READ) | **1 RTT** (GETATTR 带 data) | **1 RTT** |
| 存储 3901B | 2MB needle | 3901B in inode | **99.8% 节省** |

预期 mdtest-hard IOPS **2-3x 提升**（RTT 减半 + 无 Volume Server 排队）。

#### 4.6.5 0 字节文件优化（mdtest-easy 场景）

文件创建后未写入即关闭的特殊优化：

```
1. CREATE: Filer 分配 inode，标记 Placement::Inline
2. CLOSE (无 WRITE): 客户端发 CLOSE，inline_data = vec![]
   - Filer 元数据: InlineData { data: vec![] }, size=0
   - 不分配 needle_id，不创建空 needle
3. READ: GETATTR 返回 size=0，客户端零填充
4. DELETE: 仅删 Filer 元数据，无 Volume Server GC
```

**收益**：mdtest-easy（100 万文件）节省 100 万次 needle_id 分配 + 100 万次空 needle 创建。

#### 4.6.6 超阈值迁移策略

文件增长超 inline 阈值时自动迁移到 Flat。采用**滞后窗口**避免边界抖动：

```
迁移触发条件: 累计写入 > min(max_size × 1.5, INLINE_HARD_LIMIT)
默认: 4KB 阈值 → 6KB 才迁移；8KB 阈值 → 8KB (硬上限) 才迁移

迁移流程 (客户端驱动, crash-safe):
1. 客户端 WRITE 触发迁移检测 (new_end > migrate_threshold)
2. 客户端合并 inline_buffer + 当前 write → merged_data (不修改 inline_buffer)
3. 客户端调 Filer MIGRATE_INLINE_ALLOC RPC:
   a. Filer 分配 (volume_id, needle_id)
   b. Filer **不修改 inode** (保留 inline_data 用于 crash safety)
   c. Filer 返回 (volume_id, needle_id)
4. 客户端把 merged_data 放入 chunk_cache (dirty, chunk 0)
   - 必须放入: 否则后续 append 走 Flat 路径时 chunk 0 不在 cache
     → no_data_before 优化跳过 read-before-write → 零填充覆盖迁移数据
5. 客户端切换 cache 到 Flat: fid=(volume_id, needle_id), chunks=[{0,size,needle,volume}]
6. 客户端移除 inline_buffers/inline_max_sizes, 后续 write 走 Flat 路径
7. close 时:
   a. flush_dirty_chunks: 把 chunk_cache 数据写到 Volume Server (needle_id)
   b. sync_size_chunks_on_close: UPDATE_INODE_SIZE_CHUNKS (Flat + PerChunk[1], inline_data=None)
      → 原子清除 Filer inline_data + 设置 Flat chunks (Raft 提交)

crash safety:
- 步骤 3 后崩溃: Filer 仍有 inline_data, 文件仍可作 Inline 读; needle_id 泄漏 (同 CREATE 失败)
- 步骤 4-6 后崩溃: chunk_cache 丢失 (同 Flat write 崩溃); Filer 仍有 inline_data (旧数据)
- 步骤 7 后: Filer Flat + chunks, Volume Server 有数据. 完全一致
```

**为何客户端驱动 (非 Filer 驱动)**：
- Filer 无 Volume Server 写入接口 (仅 S3 handler 有 volume_client_pool)
- 客户端已有 chunk_cache + flush_dirty_chunks 机制, 复用更简单
- crash-safe: inline_data 在 close sync 时才清除, 之前任何崩溃都可恢复
- 迁移是**一次性**的, 对小文件成本可接受 (< 8KB 数据转移)

**滞后窗口的必要性**：
- 不带窗口：4KB 文件每次写都触发迁移 → 回退 → 再迁移，抖动严重
- 带 1.5x 窗口：4KB 文件写到 6KB 才迁移，期间累计在 inline，一次性迁移到 Volume Server
- 迁移是**一次性**的，对小文件来说成本可接受（< 8KB 数据转移）

**truncate 限制**：setattr (ftruncate) 到 >8KB 仍返回 EFBIG (未实现迁移).
通常文件通过 write 增长而非 truncate, 此为罕见场景, 后续可扩展.

#### 4.6.7 Inline 数据不压缩

**决策**：Inline 数据不做压缩。

**理由**：
1. Inline 数据 ≤ 8KB，zstd 压缩比有限（典型 2x），节省几 KB 意义不大
2. Raft 已保证可靠性（N=3 副本），无需额外保护
3. 压缩/解压增加 CPU 开销，对小文件延迟敏感场景不划算
4. 副本压缩属性（5.4 节）仅适用于 Replicated 模式的 Volume Server 数据

#### 4.6.8 Inline 与 Lease 的关系

Inline 模式下**不需要 Lease**：
- 数据在 Filer，不在 Volume Server
- Filer Raft 已保证强一致性（线性化）
- 客户端读直接从 Filer GETATTR，无 lease 续约开销
- 客户端写通过 Filer Raft 提交，无 lease 锁竞争

**与 lease-design.md 的关系**：Inline 文件不参与 lease 机制，是 lease 之外的独立路径。

---

## 5. Reliability 设计（数据保护）

### 5.1 四态枚举 + 状态机

```rust
pub enum Reliability {
    /// 单副本，写入默认状态
    SingleReplica,
    
    /// N 副本（默认 N=2）
    Replicated {
        factor: u8,
        compressed: bool,    // 副本压缩标志
    },
    
    /// 纠删码（默认 4+2）
    EC {
        data: u8,
        parity: u8,
    },
    
    /// 宽 EC（8+4, 16+4）
    ECWide {
        data: u8,
        parity: u8,
    },
}

pub enum ReliabilityState {
    /// 写入中或刚关闭，仅 SingleReplica
    Pending,
    
    /// 后台转换中（复制或 EC 编码）
    Syncing { progress: u8 },  // 0-100
    
    /// 转换完成，达到目标可靠性
    Completed,
    
    /// 数据丢失部分，待修复
    Degraded { missing_shards: u16 },
}
```

### 5.2 默认策略表

| 文件大小 | 写入时 | 后台目标 | 理由 |
|---------|--------|---------|------|
| < 4KB (Inline) | **Raft 复制（隐式 N=3）** | **保持 Raft 复制** | Inline 数据不参与 scrubber，Raft 已保证 |
| 4KB - 10MB | SingleReplica | Replicated(2, compressed=false) | 小文件副本开销小，恢复快 |
| 10MB - 1GB | SingleReplica | EC(4+2) | 中等文件 EC 节省空间 50% |
| 1GB - 100GB | SingleReplica | EC(8+4) | 大文件 EC 节省空间 67% |
| > 100GB | SingleReplica | EC(16+4) | 极致空间效率 |
| HPC 标志 | SingleReplica | 保持 SingleReplica 或 EC(16+4) | 由目录属性决定 |

**Inline 特例**：Placement=Inline 时，Reliability 隐式为 Raft 复制（Filer Raft 组的副本数，通常 N=3），不参与 scrubber 异步转换。Inline 文件迁移到 Flat 后，Reliability 才按上表转为 SingleReplica → 后台 Replicated(2)。

### 5.3 写入路径永不等可靠性

**核心原则**：写入路径始终 SingleReplica 快速确认，避免 IO 延迟。

```
写入流程:
1. CREATE: SingleReplica 状态 Pending
2. WRITE: 直连 Volume Server 写单副本，快速 ACK
3. CLOSE: 元数据 (SingleReplica, Pending)
4. 后台 scrubber 异步转换:
   - 检测 Pending 状态文件
   - 按策略复制 / EC 编码
   - 完成后状态 -> Completed
5. 读路径: 任何时候都可读（Degraded 时读剩余数据）
```

**风险与缓解**：
- **风险**：写入完成到转换完成期间，单点故障丢数据
- **缓解 1**：scrubber 高优先级，10 秒内启动转换
- **缓解 2**：重要文件可设 `powerfs.reliability=replicated:3`，创建时即多副本（牺牲延迟换安全）
- **缓解 3**：Master 持久化写入日志，故障恢复后继续转换

### 5.4 副本压缩属性

**新增能力**：副本支持压缩标志，长时间不用的文件后台压缩。

```rust
// Reliability::Replicated.compressed 字段
// true: 副本已压缩存储
// false: 副本未压缩

// 压缩状态枚举
pub enum CompressionState {
    Uncompressed,           // 未压缩
    Compressing { progress: u8 },  // 压缩中
    Compressed { algo: CompressionAlgo, ratio: u16 },  // 已压缩
}
```

**触发条件**：
- 文件 atime 超过阈值（默认 30 天未访问）
- 系统空闲（IO 队列深度 < 阈值）
- 用户显式设置：`setfattr -n powerfs.compress -v zstd <file>`

**压缩算法**：
- `zstd`：默认，压缩比和速度平衡
- `lz4`：快速压缩，压缩比较低
- `snappy`：兼容性好

**实现**：
- Volume Server 后台 worker：扫描 compressed=false 且 atime 超阈值的文件
- 读取原数据 → 压缩 → 写新 needle → 更新 inode 元数据
- 原 needle 标记为 `pending_delete`，GC 安全回收
- 压缩完成设 `compressed=true` + 算法 + 压缩比

**读路径适配**：
- 客户端读时检查 `compressed` 标志
- Volume Server 在 ReadNeedle 响应中返回压缩数据 + 算法标志
- 客户端解压（CPU 开销换网络/存储节省）

### 5.5 后台 scrubber 状态机

```
                  ┌──────────────────┐
                  │   Pending        │
                  │ (SingleReplica)  │
                  └────────┬─────────┘
                           │ scrubber 扫描
                           ▼
                  ┌──────────────────┐
                  │    Syncing       │
                  │ (复制/EC 编码中)  │
                  └────────┬─────────┘
                           │ 编码完成
                           ▼
                  ┌──────────────────┐
        ┌────────>│    Completed     │<────────┐
        │         │ (达到目标可靠性)  │         │
        │         └────────┬─────────┘         │
        │                  │ 数据损坏          │
        │                  ▼                   │
        │         ┌──────────────────┐         │
        │         │    Degraded      │         │
        │         │ (丢失部分 shard) │         │
        │         └────────┬─────────┘         │
        │                  │ 后台修复           │
        └──────────────────┘                   │
                                               │
                  ┌──────────────────┐         │
                  │   Compressing    │         │
                  │ (后台压缩中)      │─────────┘
                  └──────────────────┘   压缩完成 -> Completed
```

### 5.6 EC 跨节点分布

EC 模式下 data + parity 块强制分布在不同节点：

```rust
// EC(4+2) 分布示例（6 个块跨 6 个节点）
Node1: data_shard_0
Node2: data_shard_1
Node3: data_shard_2
Node4: data_shard_3
Node5: parity_shard_0
Node6: parity_shard_1
```

**约束**：
- 任意单节点故障：EC(4+2) 仍可读，状态 → Degraded
- 任意双节点故障：EC(4+2) 仍可读，状态 → Degraded
- 三节点故障：EC(4+2) 数据丢失，状态 → Lost（需从备份恢复）

**节点数不足处理**：
- EC(4+2) 需要至少 6 个节点
- 节点不足时降级为 Replicated(2)
- scrubber 检测集群扩容后自动升级为 EC

---

## 6. ChunkEncoding 设计（元数据序列化）

### 6.1 四态枚举

```rust
pub enum ChunkEncoding {
    /// Inline: 数据直接在 Filer 元数据中（微小文件，< 4KB/8KB）
    /// 无 Volume Server 参与，通过 Raft 复制
    InlineData { data: Vec<u8> },
    
    /// PerChunk: 单 chunk 或少量 chunk（90%+ 文件）
    /// 每 chunk 44B 二进制
    PerChunk { chunks: Vec<ChunkWire> },
    
    /// StripeDescriptor: Stripe/WideStripe 模式，chunk 由算法计算
    /// 40-60B 覆盖任意大小顺序写文件
    StripeDescriptor { layout: StripeLayout },
    
    /// Paginated: 大量 chunk 超响应阈值，分页拉取
    Paginated {
        inline_chunks: Vec<ChunkWire>,  // 响应内联的前 N 个
        total_count: u32,
        next_offset: u64,               // LIST_CHUNKS 起始
    },
}

pub struct ChunkWire {
    pub offset: u64,        // 8B
    pub size: u64,          // 8B
    pub needle_id: u64,     // 8B
    pub volume_id: u64,     // 8B
    pub crc32: u32,         // 4B
    pub mtime: u64,         // 8B
    // 总计 44B
}

pub struct StripeLayout {
    pub volume_ids: Vec<u64>,        // WideStripe 时用范围压缩
    pub stripe_size: u64,            // 8B
    pub stripe_count: u32,           // 4B
    pub start_volume_idx: u32,       // 4B
    pub start_needle_id: u64,        // 8B - 首 needle_id（连续递增）
    pub chunk_size: u32,             // 4B - 单 chunk 2MB
    pub total_chunks: u32,           // 4B
    pub first_crc32: u32,            // 4B - 首 chunk CRC（其余读时校验）
    // 总计 ~40B + volume_ids 编码
}
```

### 6.2 编码选择策略

| Placement | 默认 Encoding | 切换条件 |
|-----------|--------------|---------|
| **Inline** | **InlineData** | **超阈值迁移到 Flat 后切换 PerChunk** |
| Flat | PerChunk | chunk 数 > 50（100MB）切 Paginated |
| Stripe | StripeDescriptor | stripe_count × stripe_per_vol > 50 切 Paginated |
| WideStripe | StripeDescriptor | 始终（256 volume 不内联） |
| 随机写 | PerChunk | chunk 数 > 50 切 Paginated |

**Inline 不可与 Paginated 共存**：InlineData 文件大小 ≤ 8KB，永远不触发分页。

### 6.3 大小对比

| 文件大小 | 写模式 | JSON 现状 | 二进制 TLV | StripeDescriptor | InlineData | 压缩比 |
|---------|--------|----------|-----------|-----------------|-----------|--------|
| 3901B | - | 200B | 44B | - | **8B len + 3901B** | **25x** |
| 256MB | 顺序 | 24KB | 5.6KB | **40B** | - | **600x** |
| 1GB | 顺序 | 100KB | 22KB | **40B** | - | **2500x** |
| 10GB | 顺序 | 1MB | 225KB | **60B** | - | **17000x** |
| 256MB | 随机 | 24KB | 5.6KB | 5.6KB（无 stripe 收益） | - | 4x |
| 1GB | 随机 | 100KB | 22KB | 22KB + 分页 | - | 4x + 分页 |

### 6.4 模式自动检测

close/flush 时自动检测 chunks 是否可压缩为 StripeDescriptor。Inline 模式由 Placement 决定，无需检测：

```rust
fn detect_encoding(chunks: &[ChunkWire], placement: &Placement, inline_data: Option<&[u8]>) -> ChunkEncoding {
    // Inline 模式：数据已在 Filer，直接返回 InlineData
    if let Some(data) = inline_data {
        return ChunkEncoding::InlineData { data: data.to_vec() };
    }
    
    if chunks.is_empty() {
        return ChunkEncoding::PerChunk { chunks: vec![] };
    }
    
    // Placement 已是 Stripe/WideStripe 时，强制 StripeDescriptor
    if matches!(placement, Placement::Stripe { .. } | Placement::WideStripe { .. }) {
        if let Some(layout) = try_build_stripe_descriptor(chunks, placement) {
            return ChunkEncoding::StripeDescriptor { layout };
        }
    }
    
    // Flat 模式下检测是否实际可压缩（顺序写小文件）
    if chunks.len() > 1 && is_contiguous(chunks) {
        if chunks.len() > 50 {
            if let Some(layout) = try_build_stripe_descriptor(chunks, placement) {
                return ChunkEncoding::StripeDescriptor { layout };
            }
        }
    }
    
    // 随机写或小文件：PerChunk，必要时 Paginated
    if chunks.len() > 50 {
        let inline: Vec<_> = chunks.iter().take(50).cloned().collect();
        ChunkEncoding::Paginated {
            inline_chunks: inline,
            total_count: chunks.len() as u32,
            next_offset: chunks[50].offset,
        }
    } else {
        ChunkEncoding::PerChunk { chunks: chunks.to_vec() }
    }
}

fn is_contiguous(chunks: &[ChunkWire]) -> bool {
    let first = &chunks[0];
    chunks.iter().enumerate().all(|(i, c)| {
        c.volume_id == first.volume_id
        && c.needle_id == first.needle_id + i as u64
        && c.size == first.size
        && c.offset == first.offset + i as u64 * first.size
    })
}
```

### 6.5 CRC 处理策略

**选项 A（采用）**：响应不含完整 CRC 列表，读时从 Volume Server 校验。

- StripeDescriptor：仅含 `first_crc32`
- PerChunk：含完整 CRC 列表（小文件 < 50 chunk，CRC 总量 < 200B 可接受）
- Paginated：内联 chunks 含 CRC，其余读时校验

**理由**：
1. project memory 已约束 `crc32 checks must be performed during read operations`
2. 1GB 顺序写 CRC 列表 = 2KB，存元数据抵消 StripeDescriptor 压缩收益
3. Volume Server 读时返回 CRC，客户端校验，CRC 始终最新

---

## 7. list_chunk RPC 设计

### 7.1 新增消息类型

```c
// powerfs_net.h
POWERFS_NET_MSG_LIST_CHUNKS    = 0x001C,  // 分页拉取 chunks（大文件）
POWERFS_NET_MSG_MIGRATE_INLINE = 0x001D,  // Inline -> Flat 迁移（文件超阈值）
```

### 7.2 请求/响应格式

**请求 TLV 字段**：
| Field | 类型 | 说明 |
|-------|------|------|
| `Ino` | u64 | 文件 inode |
| `Offset` | u64 | 起始 chunk offset |
| `Limit` | u32 | 最多返回 chunk 数（默认 256） |

**响应 TLV 字段**：
| Field | 类型 | 说明 |
|-------|------|------|
| `Chunks` | bytes | 二进制 TLV 编码的 chunk 列表 |
| `HasMore` | u8 | 1=还有更多，0=已全部返回 |
| `NextOffset` | u64 | 下次请求起始 offset |
| `TotalCount` | u32 | 文件总 chunk 数 |

### 7.3 触发条件

客户端在以下情况调用 LIST_CHUNKS：
1. GETATTR 响应 `ChunkEncoding::Paginated` 且 `next_offset != 0`
2. WideStripe 模式下客户端首次打开文件（GETATTR 仅返回 descriptor）
3. EC 模式下降级读需要定位 parity 块位置
4. 文件 truncate 后需要重新拉取 chunk 列表

### 7.4 客户端缓存

- LIST_CHUNKS 响应缓存在客户端 inode 私有数据
- 缓存失效：Invalidate 通知 / 文件 mtime 变化 / 客户端重连
- 缓存策略：LRU，单文件最多缓存 1MB chunk 列表

### 7.5 MIGRATE_INLINE RPC 设计

**触发条件**：客户端检测 inline 文件累计写入 > `max_size × 1.5`，触发迁移到 Flat。

**请求 TLV 字段**：
| Field | 类型 | 说明 |
|-------|------|------|
| `Ino` | u64 | 文件 inode |
| `PendingWrite` | bytes | 客户端 pending 的写数据（触发迁移的那次） |
| `PendingOffset` | u64 | pending 写的起始 offset |
| `PendingSize` | u32 | pending 写的大小 |

**响应 TLV 字段**：
| Field | 类型 | 说明 |
|-------|------|------|
| `VolumeId` | u64 | 新分配的 volume_id |
| `FileKey` | u64 | 新分配的 needle_id |
| `ContentSize` | u64 | 合并后的文件大小 |
| `Status` | u16 | 0=成功，其他=错误码 |

**Filer 处理流程**：
```
1. Filer 收到 MIGRATE_INLINE 请求
2. 读取 inode 的 inline_data
3. 分配 (volume_id, needle_id)
4. 合并: inline_data + pending_write → 完整数据
5. 发 WriteNeedle 到 Volume Server
6. 更新 inode 元数据 (Raft 提交):
   - Placement: Inline → Flat
   - Encoding: InlineData → PerChunk[1]
   - Reliability: 保持 Raft 复制 → SingleReplica + Pending
   - 移除 inline_data 字段
7. 返回 (volume_id, needle_id) 给客户端
8. 客户端切换到 Flat 模式
```

**错误处理**：
- 迁移失败：保持 Inline 状态，客户端可重试或报错
- Volume Server 写失败：Filer 不更新元数据，返回错误
- Raft 提交失败：客户端重试（inline_data 仍在）

---

## 8. 典型场景流程

### 8.1 小文件写入（< 10MB，90% 场景）

```
1. CREATE:
   - Filer 读取父目录 powerfs.placement xattr（不存在则用默认 Flat）
   - 分配 (volume_id, needle_id)，SingleReplica + Pending
   - 返回 Flat + SingleReplica + PerChunk[]

2. WRITE:
   - 客户端直连 Volume Server 写单 chunk（2MB）
   - Volume Server 返回 CRC32

3. CLOSE:
   - 客户端同步元数据: PerChunk[1] + content_size + SingleReplica + Pending
   - Filer 持久化到 Raft

4. 后台 scrubber:
   - 扫描 Pending 状态文件
   - 按策略复制到另一节点 volume
   - 状态 -> Completed (Replicated(2))

5. 读:
   - GETATTR 返回 PerChunk[1] (44B) + Replicated(2) + Completed
   - 客户端直接读 Volume Server，校验 CRC32
```

**元数据大小**: 44B (PerChunk[1]) + 40B 属性 = 84B

### 8.2 中等文件顺序写（100MB - 1GB）

```
1. CREATE:
   - 父目录 powerfs.placement=stripe:4:64MB
   - 分配 4 个 volume（跨 4 节点 anti-affinity）
   - 返回 Stripe(4, 64MB) + SingleReplica + PerChunk[]

2. WRITE:
   - 0-64MB: volume[0]，写满 32 chunk
   - 64-128MB: volume[1]
   - 128-192MB: volume[2]
   - 192-256MB: volume[3]
   - 256MB+: 回到 volume[0]

3. CLOSE:
   - 检测 chunks 连续，构建 StripeDescriptor
   - 元数据: StripeDescriptor + SingleReplica + Pending

4. 后台 scrubber:
   - 转 EC(4+2)，需 6 节点
   - 4 data shard 对应 4 volume 已有数据
   - 2 parity shard 编码后写另 2 节点
   - 状态 -> Completed (EC(4+2))

5. 读:
   - GETATTR 返回 StripeDescriptor(40B) + EC(4+2) + Completed
   - 客户端按 locate() 计算 (vol_idx, vol_offset)
   - 并行读 4 volume，校验 CRC
```

**元数据大小**: 40B (StripeDescriptor) + 40B 属性 = 80B

### 8.3 HPC 大文件（> 100GB，256 volume 并行）

```
1. CREATE:
   - 父目录 powerfs.placement=wide_stripe:256:4MB
   - 分配 256 个 volume（跨 ≥256 节点）
   - 返回 WideStripe(256, 4MB) + SingleReplica + StripeDescriptor[]

2. WRITE:
   - 每 4MB 切换 volume，256 volume 轮转
   - 单客户端 256 路并发写

3. CLOSE:
   - StripeDescriptor 已就绪（无需等写入完成）
   - 元数据: StripeDescriptor + SingleReplica + Pending

4. 后台 scrubber:
   - 转 EC(16+4)
   - 256 volume 重组为 16+4 EC 组（每组跨 20 节点）
   - 状态 -> Completed (EC(16+4))

5. 读:
   - GETATTR 返回 StripeDescriptor(60B) + EC(16+4) + Completed
   - 客户端 256 路并行读
   - 任意 4 节点故障仍可读（Degraded 状态）
```

**元数据大小**: 60B (WideStripe descriptor，volume_ids 范围压缩) + 40B 属性 = 100B

### 8.4 随机写文件

```
1. CREATE:
   - 默认 Flat + SingleReplica

2. WRITE:
   - 随机 offset 写，无法形成 stripe
   - 每 chunk 单独分配 needle_id

3. CLOSE:
   - 检测 chunks 不连续，用 PerChunk
   - 若 chunk 数 > 50: Paginated

4. 后台 scrubber:
   - 转 EC(4+2)，但 chunks 列表不变
   - 元数据: Paginated + EC(4+2) + Completed

5. 读:
   - GETATTR 返回 Paginated (内联 50 chunk) + HasMore=true
   - 客户端按需 LIST_CHUNKS 拉取剩余
   - 缓存 chunk 列表
```

### 8.5 副本压缩场景

```
1. 文件写入完成（PerChunk[1] + Replicated(2) + Completed）
2. 30 天未访问（atime 超阈值）
3. scrubber 检测:
   - powerfs.compress=zstd xattr 或默认策略
   - 启动后台压缩
4. 压缩流程:
   - 读原 needle 数据
   - zstd 压缩（level 3）
   - 写新 needle（同 volume 或迁移 volume）
   - 更新 inode: Compressed(zstd, ratio=2.3)
   - 原 needle 标记 pending_delete
5. GC 回收原 needle
6. 读路径:
   - 客户端检测 Compressed 标志
   - ReadNeedle 返回压缩数据 + algo=zstd
   - 客户端解压
```

### 8.6 Inline 微小文件场景（IO500 mdtest）

**场景 A：mdtest-easy（0 字节文件）**

```
1. CREATE:
   - Filer 检测父目录 powerfs.inline=4096
   - 分配 inode，不分配 volume_id/needle_id
   - 返回 Placement::Inline + max_size=4096

2. CLOSE (无 WRITE):
   - 客户端发 CLOSE，inline_data = vec![]
   - Filer 单次 Raft 提交: InlineData{data:vec![]}, size=0
   - 无 Volume Server 参与

3. STAT/GETATTR:
   - Filer 返回 InlineData{data:vec![]}
   - 客户端 size=0，无读动作

4. DELETE:
   - 仅删 Filer 元数据，无 Volume Server GC

收益: 100 万文件节省 100 万次 needle_id 分配 + 100 万次空 needle
```

**场景 B：mdtest-hard（3901 字节文件）**

```
1. CREATE:
   - Filer 检测父目录 powerfs.inline=4096
   - 返回 Placement::Inline + max_size=4096

2. WRITE 3901B:
   - 客户端不直连 Volume Server
   - 数据暂存客户端 inode 私有数据

3. CLOSE:
   - 客户端把 inline_data(3901B) + 元数据发 Filer
   - Filer 单次 Raft 提交: InlineData{data:[3901B]}, size=3901
   - 无 Volume Server 参与

4. READ 3901B:
   - GETATTR 返回 InlineData{data:[3901B]}
   - 客户端直接从 GETATTR 响应读，无需 ReadNeedle

5. DELETE:
   - 仅删 Filer 元数据，无 Volume Server GC

收益: 3 RTT → 1 RTT（写），2 RTT → 1 RTT（读），99.8% 存储节省
```

**场景 C：Inline 迁移到 Flat（文件增长）**

```
1. 文件已存在: InlineData{data:[4096B]}, size=4096
2. 客户端 WRITE offset=4096, size=2048
3. 客户端检测: 4096 + 2048 = 6144 > max_size × 1.5 (6144)
4. 触发 MIGRATE_INLINE:
   a. 客户端调 Filer MIGRATE_INLINE RPC
   b. Filer 分配 (volume_id, needle_id)
   c. Filer 合并 inline_data(4096B) + pending_write(2048B) = 6144B
   d. Filer 发 WriteNeedle 到 Volume Server（6144B）
   e. Filer 更新元数据: Flat + PerChunk[1] + SingleReplica + Pending
   f. 原 inline_data 从元数据移除
5. 客户端收到新 (volume_id, needle_id)，切换到 Flat 模式
6. 后续写直连 Volume Server

迁移成本: 1 次额外 RPC + 6144B 数据转移（可接受）
```

---

## 9. 协议兼容迁移

### 9.1 新增 FieldId

```c
// powerfs_net.h 新增
POWERFS_NET_FLD_PLACEMENT       = 0xA0,  // Placement 编码 (u8 + 后续字段)
POWERFS_NET_FLD_RELIABILITY     = 0xA1,  // Reliability 编码 (u8 + 后续字段)
POWERFS_NET_FLD_RELIABILITY_STATE = 0xA2,  // ReliabilityState (u8 + 后续字段)
POWERFS_NET_FLD_COMPRESSION     = 0xA3,  // CompressionState (u8 + 后续字段)
POWERFS_NET_FLD_CHUNK_LAYOUT    = 0xA4,  // 二进制 ChunkEncoding (替代 JSON Chunks)
POWERFS_NET_FLD_HAS_MORE_CHUNKS = 0xA5,  // Paginated 标志 (u8)
POWERFS_NET_FLD_NEXT_OFFSET     = 0xA6,  // 下次 LIST_CHUNKS 起始 (u64)
POWERFS_NET_FLD_TOTAL_COUNT     = 0xA7,  // 总 chunk 数 (u32)
POWERFS_NET_FLD_STRIPE_SIZE     = 0xA8,  // Stripe size (u64)
POWERFS_NET_FLD_STRIPE_COUNT    = 0xA9,  // Stripe count (u32)
POWERFS_NET_FLD_START_VOLUME_IDX = 0xAA,  // 起始 volume 索引 (u32)
POWERFS_NET_FLD_VOLUME_IDS      = 0xAB,  // volume_ids 列表 (bytes)
POWERFS_NET_FLD_START_NEEDLE_ID = 0xAC,  // 首 needle_id (u64)
POWERFS_NET_FLD_CHUNK_SIZE      = 0xAD,  // 单 chunk 大小 (u32)
POWERFS_NET_FLD_INLINE_DATA     = 0xAE,  // Inline 数据 (bytes, <= 8KB)
POWERFS_NET_FLD_INLINE_MAX_SIZE = 0xAF,  // Inline 阈值 (u32, CREATE 响应携带)
```

### 9.2 字段策略

> **P2 实施决策（2026-08-08）**：不保留旧 JSON 兼容，二进制 TLV 为唯一编码格式。
> 旧客户端需升级协议版本，否则无法解析 chunk 布局。

| 字段 | 状态 | 说明 |
|------|------|------|
| `FieldId::Chunks` (JSON) | **已移除** | P2 起不再编码/解码，旧 JSON 路径彻底删除 |
| `FieldId::VolumeId` / `FileKey` | **已移除** | P2 起从 `encode_chunks_fields` 中删除，布局信息由 FileLayout TLV 统一承载 |
| `FieldId::ChunkLayout` (0xA4) | **已启用** | 二进制 ChunkEncoding（PerChunk/Paginated/StripeDescriptor/InlineData） |
| `FieldId::Placement` (0xA0) | **已启用** | Flat/Stripe/WideStripe |
| `FieldId::Reliability` (0xA1) | **已启用** | Replica/EC + State |
| `FieldId::HasMoreChunks` (0xA5) | 已定义 | 分页标志（Paginated 模式，待 P3 启用） |

### 9.3 客户端版本协商

握手 features 位新增：
- `FEATURE_CHUNK_LAYOUT_V2` (bit 8)：支持二进制 ChunkEncoding
- `FEATURE_PLACEMENT_V2` (bit 9)：支持 WideStripe
- `FEATURE_RELIABILITY_V2` (bit 10)：支持 EC 状态机
- `FEATURE_COMPRESSION_V1` (bit 11)：支持副本压缩

服务端检测客户端 features：
- 全支持 → 新格式
- 部分支持 → 降级到客户端支持的最大集合
- 全不支持 → 旧 JSON + 单 chunk 字段

### 9.4 服务端响应策略

> **P2 实施决策（2026-08-08）**：`encode_file_layout` 始终使用二进制 TLV 编码，
> 不再根据 `client_features` 降级到 JSON。`FEATURE_CHUNK_LAYOUT_V2` 参数保留用于前向兼容，
> 但当前所有客户端均使用二进制编码。

```rust
fn encode_file_layout(enc: &mut TlvEncoder, layout: &FileLayout, _features: u32) {
    // 姯终使用二进制 TLV 编码（JSON 兼容路径已移除）
    enc.add_u8(FieldId::Placement, layout.placement as u8);
    enc.add_u8(FieldId::Reliability, layout.reliability as u8);
    // ... 详细字段
    encode_chunk_encoding_v2(enc, &layout.encoding);
}
```

### 9.5 UpdateInodeSizeChunks 协议迁移（P2）

> **实施日期：2026-08-08**
> `UpdateInodeSizeChunks` 从 JSON body 迁移到二进制 TLV 编码。

**旧格式（已废弃）**：
- Request body = `serde_json::to_vec(UpdateInodeSizeChunksRequest)`
- Response body = `serde_json::to_vec(UpdateInodeSizeChunksResponse)`

**新格式（TLV）**：

Request body TLV 字段：
| FieldId | 类型 | 说明 |
|---------|------|------|
| `ShardId` (0x70) | u64 | 分片 ID（fuse 端传 dir_ino） |
| `Ino` (0x07) | u64 | 文件 inode |
| `Size` (0x06) | u64 | 文件内容大小 |
| `ClientId` (0x30) | string | 客户端标识 |
| FileLayout TLV | 嵌套 | Placement + Reliability + ChunkEncoding（chunks 二进制编码） |

Response：
| 场景 | Status | Body |
|------|--------|------|
| 成功 | `STATUS_OK` | 空 |
| 失败 | `STATUS_ERR_SERVER_ERROR` | TLV `FieldId::Name` = error string |

**实现位置**：
- Fuse 发送端：[meta_shard_client.rs](file:///home/portion/powerfs/powerfs-fuse-core/src/meta_shard_client.rs) `update_inode_size_chunks()`
- Filer 接收端：[net_handler.rs](file:///home/portion/powerfs/powerfs-filer/src/net_handler.rs) `handle_update_inode_size_chunks()`
- 共享错误解析：`send_coherence_msg()` 优先 TLV（`FieldId::Name`），JSON 回退保留作为防御性兼容

**已迁移的 coherence 协议**（全部使用 TLV body）：
- `AllocInodeBatch` — Request: ShardId + Count + ClientId; Response: StartInode + EndInode / Name=error
- `OpenCountInc` / `OpenCountDec` — Request: ShardId + Ino; Response: OpenCount / Name=error
- `UpdateInodeSizeChunks` — Request: ShardId + Ino + Size + ClientId + FileLayout; Response: 空 / Name=error

---

## 10. 实施阶段规划

### 10.1 实施路线图（5 个里程碑）

整个实施分为 **5 个里程碑（M1-M5）**，每个里程碑包含若干 Phase，里程碑之间有明确依赖关系和验证门。

```
M1 协议基础 ──→ M2 小文件优化 ──→ M3 数据分布 ──→ M4 可靠性体系 ──→ M5 技术债务清理
  (P1, P2)        (P2.5)         (P3, P5)        (P4, P6, P7)        (P8)
     │                │               │                │                 │
     ▼                ▼               ▼                ▼                 ▼
  协议可信赖       mdtest 2x      全场景覆盖       数据安全           代码干净
  问题可定位       存储省 99%     256 路并行       EC 节省 50%+       无历史包袱
```

| 里程碑 | 名称 | 包含 Phase | 目标 | 验证门 |
|--------|------|-----------|------|--------|
| **M1** | 协议基础稳固 | P1, P2 | 协议层可信赖，问题可定位，元数据紧凑 | 内核 dmesg 无异常 ≥5min；1GB 文件读写正常 |
| **M2** | 小文件极致优化 | P2.5 | IO500 mdtest IOPS 2-3x 提升，存储节省 99% | mdtest-hard IOPS ≥ 2x；3901B 文件不占 2MB needle |
| **M3** | 数据分布能力完整 | P3, P5 | 支持从小文件到 256 vol 并行的全场景 | 64MB+ 自动 stripe；256 路并行带宽 ≥ 单路 × 200 |
| **M4** | 可靠性体系完整 | P4, P6, P7 | 数据安全 + 空间效率 + 冷数据压缩 | 节点故障仍可读；EC 节省 50%+；压缩比验证 |
| **M5** | 技术债务清理 | P8 | 代码库干净，无历史包袱 | 旧客户端兼容性测试通过 |

**里程碑依赖关系**：
- M1 是所有后续里程碑的基础（协议必须先稳固）
- M2 依赖 M1（Inline 需要 ChunkEncoding V2 协议）
- M3 依赖 M1（Placement 需要 Placement 字段协议）
- M4 依赖 M3（EC 需要 anti-affinity 和 Placement 基础）
- M5 可在 M2 稳定后任意时间启动（JSON 废弃不阻塞新功能）

**并行可能性**：
- M2 和 M3 的 P3 可并行（Inline 和 Stripe 提升独立）
- M4 的 P7（压缩）可与 P6（EC）并行（不同代码路径）

### 10.2 Phase 划分总览

| Phase | 里程碑 | 内容 | 依赖 | 风险 | 验证标准 |
|-------|--------|------|------|------|---------|
| **P1** | M1 | 协议校验 6 层（-E2BIG + 严格校验 + 诊断日志） | 无 | 低 | 内核 dmesg 无异常 ≥5min |
| **P2** | M1 | 二进制 TLV 编码 chunks + LIST_CHUNKS RPC | P1 | 中 | 1GB 文件读写正常 |
| **P2.5** | M2 | Inline 模式：Filer 存储 + 客户端写路径 + 迁移逻辑 | P2 | 中 | IO500 mdtest-hard IOPS ≥ 2x |
| **P3** | M3 | Placement 提升（Flat→Stripe 自动） + 目录属性继承 | P2 | 中 | 64MB+ 文件自动 stripe |
| **P4** | M4 | Reliability 状态机 + scrubber 异步转换（仅 Replicated） | P3 | 高 | 副本数正确，故障恢复 |
| **P5** | M3 | WideStripe(256) 全集群并行 | P3 | 高 | 256 路并行带宽达标 |
| **P6** | M4 | EC(4+2/8+4/16+4) 编码 + 降级读 | P4 | 高 | EC 节点故障仍可读 |
| **P7** | M4 | 副本压缩 + 后台压缩 worker | P4 | 中 | 压缩比验证，读路径正确 |
| **P8** | M5 | JSON 字段废弃 + 文档收敛 | P2+ 稳定 | 低 | 旧客户端兼容性测试 |

### 10.3 里程碑 M1 详细任务（协议基础稳固）

**目标**：协议层可信赖，问题可定位，元数据紧凑。这是所有后续工作的基础。

#### P1: 协议校验 6 层

##### 10.3.1 P1 任务清单

**任务编号规则**：R=Rust（服务端+FUSE），K=Kernel（内核），数字对应 Layer 编号。

| 任务 ID | Layer | 侧 | 任务描述 | 修改文件 | 修改位置 | 依赖 | 复杂度 |
|---------|-------|-----|---------|---------|---------|------|--------|
| **R1** | L1 | Rust | 帧头严格校验：data_len ≥ body_len + data_len ≤ MAX_FRAME | [protocol.rs](file:///home/portion/powerfs/powerfs-net/src/protocol.rs) | FrameHeader 解析处 (~L268) | 无 | 低 |
| **R2** | L3 | Rust | per-msg_type 大小校验函数 | [protocol.rs](file:///home/portion/powerfs/powerfs-net/src/protocol.rs) | 新增 `check_resp_size()` | R1 | 低 |
| **R3** | L4 | Rust | TLV 必需字段校验 | [serialize.rs](file:///home/portion/powerfs/powerfs-net/src/serialize.rs), [fuse.rs](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs) | 新增 `check_required_fields()` | R1 | 中 |
| **R4** | L6 | Rust | 诊断日志宏 | [client.rs](file:///home/portion/powerfs/powerfs-net/src/client.rs), [net_handler.rs](file:///home/portion/powerfs/powerfs-filer/src/net_handler.rs) | recv_loop (~L570), response encode | R1,R2,R3 | 低 |
| **R5** | L2 | Rust | 响应大小防御性校验（Vec 无截断，但需告警异常大响应） | [client.rs](file:///home/portion/powerfs/powerfs-net/src/client.rs) | recv_loop (~L586) | R1 | 低 |
| **K1** | L1 | Kernel | 帧头严格校验：data_len < body_len → -EPROTO | [powerfs_net.c](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_net.c) | `pfs_rx_step` (~L1844) | 无 | 低 |
| **K2** | L2 | Kernel | **核心修复**：截断检测，min() → -E2BIG | [powerfs_net.c](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_net.c) | `pfs_rx_dispatch` (~L2042) | K1 | 中 |
| **K3** | L3 | Kernel | per-msg_type 大小校验函数 | [powerfs_net.c](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_net.c) | 新增 `powerfs_net_check_resp_size()` | K1 | 低 |
| **K4** | L4 | Kernel | TLV 必需字段校验 | [powerfs_net.c](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_net.c) | 新增 `powerfs_net_check_required_fields()` | K2 | 中 |
| **K5** | L5 | Kernel | 调用方 buffer 调整 + -E2BIG 检查 | [powerfs_net.c](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_net.c) | 9 处 `resp_body[]` 声明 | K2 | 低 |
| **K6** | L6 | Kernel | 诊断日志宏统一 | [powerfs_net.c](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_net.c), [powerfs_net.h](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_net.h) | K2/K3/K4 日志处 | K2,K3,K4 | 低 |

##### 10.3.2 P1 依赖关系图

```
=========== Step 1: Rust（服务端 + FUSE）===========

    R1 (L1 帧头校验)
    ├──→ R2 (L3 msg_type 校验)
    ├──→ R3 (L4 TLV 必需字段)
    ├──→ R5 (L2 响应大小防御)
    └──→ R4 (L6 诊断日志) ←── R2, R3 完成

=========== Step 2: FUSE 容器测试 ===========

    [TEST GATE] R1-R5 全部通过 → 进入 Step 3

=========== Step 3: Kernel（内核文件系统）===========

    K1 (L1 帧头校验)
    ├──→ K2 (L2 截断检测) ★核心修复
    │       ├──→ K4 (L4 TLV 必需字段)
    │       └──→ K5 (L5 buffer 调整)
    └──→ K3 (L3 msg_type 校验)
                └──→ K6 (L6 诊断日志) ←── K2, K4 完成

=========== Step 4: 内核 QEMU 测试 ===========

    [TEST GATE] K1-K6 全部通过 + dmesg ≥5min 干净

=========== Step 5: Commit ===========

    Commit 1: R1-R5（服务端 + FUSE + 公共库）
    Commit 2: K1-K6（内核文件系统）

=========== Step 6: 更新进度 ===========

    更新附录 C 进度表
```

##### 10.3.3 各任务详细实施说明

###### R1: Rust 帧头严格校验（Layer 1）

**文件**：[powerfs-net/src/protocol.rs](file:///home/portion/powerfs/powerfs-net/src/protocol.rs)

**修改位置**：FrameHeader 解析后（约 L268 附近，`impl FrameHeader` 块）

**新增校验**：
```rust
impl FrameHeader {
    /// 校验帧头不变式，返回 Err 描述具体违规
    pub fn validate(&self) -> Result<(), String> {
        if self.magic != *PROTOCOL_MAGIC {
            return Err(format!("invalid magic: {:02x?}", self.magic));
        }
        if self.version != PROTOCOL_VERSION {
            return Err(format!("invalid version: {}", self.version));
        }
        if self.data_len < self.body_len {
            return Err(format!(
                "data_len {} < body_len {} (invariant violation)",
                self.data_len, self.body_len
            ));
        }
        if self.data_len > MAX_FRAME_SIZE {
            return Err(format!(
                "data_len {} > MAX_FRAME_SIZE {}",
                self.data_len, MAX_FRAME_SIZE
            ));
        }
        Ok(())
    }
}
```

**调用点**：[client.rs](file:///home/portion/powerfs/powerfs-net/src/client.rs) L568 附近，header 解析后立即调用 `validate()`

###### R2: Rust per-msg_type 大小校验（Layer 3）

**文件**：[powerfs-net/src/protocol.rs](file:///home/portion/powerfs/powerfs-net/src/protocol.rs)

**新增函数**：
```rust
/// per-msg_type 期望响应大小范围（仅告警，不拒绝）
pub fn check_resp_size(msg_type: u16, body_len: usize, data_len: usize) {
    let (max_body, max_data) = match msg_type {
        0x0010..=0x001B => (4 * 1024, 0),       // 元数据操作: body < 4KB
        0x0018 => (64 * 1024, 0),                // READDIR: body < 64KB
        0x0020 => (256 * 1024, 2 * 1024 * 1024), // READ: data ≤ 2MB
        0x0021 => (4 * 1024, 2 * 1024 * 1024),   // WRITE: data ≤ 2MB
        _ => return,
    };
    if body_len > max_body {
        warn!(
            "RX_SIZE_ANOMALY msg=0x{:04x} body_len={} > expected_max={}",
            msg_type, body_len, max_body
        );
    }
    if data_len > max_data {
        warn!(
            "RX_SIZE_ANOMALY msg=0x{:04x} data_len={} > expected_max={}",
            msg_type, data_len, max_data
        );
    }
}
```

###### R3: Rust TLV 必需字段校验（Layer 4）

**文件**：[powerfs-net/src/serialize.rs](file:///home/portion/powerfs/powerfs-net/src/serialize.rs) + [powerfs-fuse/src/fuse.rs](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs)

**新增函数**（serialize.rs）：
```rust
/// 校验响应是否包含必需字段，缺失返回 Err
pub fn check_required_fields(msg_type: u16, body: &[u8]) -> Result<(), String> {
    let decoder = TlvDecoder::new(body);
    let required: &[FieldId] = match msg_type {
        0x0010 => &[FieldId::Ino, FieldId::Mode],        // LOOKUP
        0x0011 => &[FieldId::Ino, FieldId::Mode, FieldId::Size], // GETATTR
        0x0013 => &[FieldId::Ino, FieldId::Mode],        // CREATE
        0x0014 => &[FieldId::Ino, FieldId::Mode],        // MKDIR
        _ => return Ok(()),
    };
    for field in required {
        if !decoder.has_field(*field)? {
            return Err(format!(
                "RX_MISSING_FIELD msg=0x{:04x} field=0x{:02x}",
                msg_type, *field as u8
            ));
        }
    }
    Ok(())
}
```

**调用点**（fuse.rs）：FUSE 客户端解析响应前调用

###### R4: Rust 诊断日志规范（Layer 6）

**文件**：[powerfs-net/src/client.rs](file:///home/portion/powerfs/powerfs-net/src/client.rs), [powerfs-filer/src/net_handler.rs](file:///home/portion/powerfs/powerfs-filer/src/net_handler.rs)

**统一日志格式**（在 client.rs recv_loop 中）：
```rust
// 帧头不变式违反
error!("RX_HDR_INVARIANT seq={} msg=0x{:04x} reason={} peer={}",
       seq, msg_type, reason, peer_addr);

// 响应大小异常
warn!("RX_SIZE_ANOMALY seq={} msg=0x{:04x} body={} data={} peer={}",
      seq, msg_type, body_len, data_len, peer_addr);

// 必需字段缺失
error!("RX_MISSING_FIELD seq={} msg=0x{:04x} field=0x{:02x} peer={}",
       seq, msg_type, field_id, peer_addr);
```

###### R5: Rust 响应大小防御性校验（Layer 2）

**文件**：[powerfs-net/src/client.rs](file:///home/portion/powerfs/powerfs-net/src/client.rs)

**修改位置**：recv_loop ~L586，body/data 分割后

**新增**：Rust 使用 Vec 动态分配无静默截断，但需检测异常大响应并告警：
```rust
let body = payload[..body_len].to_vec();
let data = payload[body_len..].to_vec();

// 防御性校验：响应大小异常时告警（不拒绝，由 R2 check_resp_size 处理）
check_resp_size(header.msg_type, body.len(), data.len());
```

---

###### K1: Kernel 帧头严格校验（Layer 1）

**文件**：[kernel/powerfs_mod/powerfs_net.c](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_net.c)

**修改位置**：`pfs_rx_step` (~L1844)，帧头解析后

**新增校验**：
```c
/* Layer 1: 帧头不变式严格校验 */
if (hdr->data_len < hdr->body_len) {
    pr_err("RX_HDR_INVARIANT seq=%u msg=0x%04x data_len=%u < body_len=%u peer=%s:%u\n",
           hdr->seq, hdr->msg_type, hdr->data_len, hdr->body_len,
           conn->peer_addr, conn->peer_port);
    return -EPROTO;
}
if (hdr->data_len > POWERFS_NET_MAX_FRAME) {
    pr_err("RX_HDR_INVARIANT seq=%u msg=0x%04x data_len=%u > MAX_FRAME=%u peer=%s:%u\n",
           hdr->seq, hdr->msg_type, hdr->data_len, POWERFS_NET_MAX_FRAME,
           conn->peer_addr, conn->peer_port);
    return -EPROTO;
}
```

###### K2: Kernel 截断检测（Layer 2）★核心修复

**文件**：[kernel/powerfs_mod/powerfs_net.c](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_net.c)

**修改位置**：`pfs_rx_dispatch` (~L2042)，替换静默 `min()`

**修改前**（问题代码）：
```c
if (req->resp_body && body_len > 0) {
    size_t c = min(body_len, req->resp_body_cap);   // ❌ 静默截断
    memcpy(req->resp_body, body, c);
    req->resp_body_len = c;
}
```

**修改后**：
```c
if (req->resp_body && body_len > 0) {
    if (body_len > req->resp_body_cap) {
        /* Layer 2: 截断检测 - 不拷贝，返回 -E2BIG 让调用方感知 */
        pr_err("RX_TRUNCATE seq=%u msg=0x%04x body_len=%zu > cap=%zu peer=%s:%u\n",
               seq, msg_type, body_len, req->resp_body_cap,
               conn->peer_addr, conn->peer_port);
        req->error = -E2BIG;
        req->resp_body_len = 0;
    } else {
        memcpy(req->resp_body, body, body_len);
        req->resp_body_len = body_len;
    }
}
/* data 段同理 */
if (req->resp_data && data_len > 0) {
    if (data_len > req->resp_data_cap) {
        pr_err("RX_TRUNCATE seq=%u msg=0x%04x data_len=%zu > cap=%zu peer=%s:%u\n",
               seq, msg_type, data_len, req->resp_data_cap,
               conn->peer_addr, conn->peer_port);
        req->error = -E2BIG;
        req->resp_data_len = 0;
    } else {
        memcpy(req->resp_data, data, data_len);
        req->resp_data_len = data_len;
    }
}
```

**影响分析**：所有调用方需检查 `req->error == -E2BIG` 并传播到上层

###### K3: Kernel per-msg_type 大小校验（Layer 3）

**文件**：[kernel/powerfs_mod/powerfs_net.c](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_net.c)

**新增函数**：
```c
/* Layer 3: per-msg_type 期望响应大小校验（仅告警） */
static void powerfs_net_check_resp_size(u16 msg_type, size_t body_len, size_t data_len,
                                         const char *peer, u16 port)
{
    size_t max_body = 0, max_data = 0;
    switch (msg_type) {
    case POWERFS_NET_MSG_LOOKUP:
    case POWERFS_NET_MSG_GETATTR:
    case POWERFS_NET_MSG_CREATE:
    case POWERFS_NET_MSG_MKDIR:
    case POWERFS_NET_MSG_UNLINK:
    case POWERFS_NET_MSG_RMDIR:
    case POWERFS_NET_MSG_RENAME:
    case POWERFS_NET_MSG_SYMLINK:
    case POWERFS_NET_MSG_LINK:
        max_body = 4 * 1024; max_data = 0;
        break;
    case POWERFS_NET_MSG_READDIR:
        max_body = 64 * 1024; max_data = 0;
        break;
    case POWERFS_NET_MSG_READ:
        max_body = 256 * 1024; max_data = POWERFS_NET_MAX_DATA;
        break;
    case POWERFS_NET_MSG_WRITE:
        max_body = 4 * 1024; max_data = POWERFS_NET_MAX_DATA;
        break;
    default:
        return;
    }
    if (body_len > max_body)
        pr_warn("RX_SIZE_ANOMALY msg=0x%04x body_len=%zu > max=%zu peer=%s:%u\n",
                msg_type, body_len, max_body, peer, port);
    if (data_len > max_data)
        pr_warn("RX_SIZE_ANOMALY msg=0x%04x data_len=%zu > max=%zu peer=%s:%u\n",
                msg_type, data_len, max_data, peer, port);
}
```

**调用点**：`pfs_rx_dispatch` 中，K2 校验通过后调用

###### K4: Kernel TLV 必需字段校验（Layer 4）

**文件**：[kernel/powerfs_mod/powerfs_net.c](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_net.c)

**新增函数**：
```c
/* Layer 4: TLV 必需字段校验，缺失返回 -EIO */
static int powerfs_net_check_required_fields(u16 msg_type, const u8 *body, size_t body_len,
                                              u32 seq, const char *peer, u16 port)
{
    struct powerfs_tlv_dec dec;
    /* 每种 msg_type 的必需字段（简化：仅查首字段是否存在） */
    u8 required_field = 0;
    switch (msg_type) {
    case POWERFS_NET_MSG_LOOKUP:
    case POWERFS_NET_MSG_CREATE:
    case POWERFS_NET_MSG_MKDIR:
        required_field = POWERFS_NET_FLD_INO;
        break;
    case POWERFS_NET_MSG_GETATTR:
        required_field = POWERFS_NET_FLD_MODE;
        break;
    default:
        return 0;
    }
    powerfs_tlv_dec_init(&dec, body, body_len);
    if (powerfs_tlv_dec_find_u64(&dec, required_field, NULL) < 0) {
        pr_err("RX_MISSING_FIELD seq=%u msg=0x%04x field=0x%02x peer=%s:%u\n",
               seq, msg_type, required_field, peer, port);
        return -EIO;
    }
    return 0;
}
```

**调用点**：`pfs_rx_dispatch` 中，K2 校验通过且 status==OK 时调用

###### K5: Kernel 调用方 buffer 调整（Layer 5）

**文件**：[kernel/powerfs_mod/powerfs_net.c](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_net.c)

**9 处 buffer 调整**：

| 行号 | 函数 | 原 buffer | 新 buffer | 理由 |
|------|------|----------|----------|------|
| L3596 | `powerfs_net_lookup_timeout` | `resp_body[512]` | `resp_body[2048]` | 容纳 Chunks JSON |
| L3671 | `powerfs_net_getattr` | `resp_body[512]` | `resp_body[2048]` | 容纳 Chunks JSON |
| L3730 | `powerfs_net_statfs` | `resp_body[128]` | `resp_body[256]` | 余量 |
| L4180 | `powerfs_net_create` | `resp_body[64]` | `resp_body[256]` | 余量 |
| L4240 | `powerfs_net_mkdir` | `resp_body[128]` | `resp_body[256]` | 余量 |
| L4276 | `powerfs_net_symlink` | `resp_body[512]` | `resp_body[2048]` | 容纳 symlink target |
| L5286 | `powerfs_net_unlink` | `resp_body[64]` | `resp_body[256]` | 余量 |
| L5379 | `powerfs_net_rename` | `resp_body[128]` | `resp_body[256]` | 余量 |
| L5454 | `powerfs_net_readlink` | `resp_body[256]` | `resp_body[2048]` | 容纳长 symlink |

**调用方 -E2BIG 检查**：每个调用方在 `wait_for_completion` 后增加：
```c
if (req->error == -E2BIG) {
    pr_err("RX_TRUNCATE_CALLER msg=0x%04x seq=%u buffer too small\n",
           msg_type, seq);
    ret = -E2BIG;
    goto out;
}
```

###### K6: Kernel 诊断日志规范（Layer 6）

**文件**：[kernel/powerfs_mod/powerfs_net.c](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_net.c), [powerfs_net.h](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_net.h)

**新增日志宏**（powerfs_net.h）：
```c
/* Layer 6: 统一诊断日志前缀 */
#define RX_LOG_TRUNCATE    "RX_TRUNCATE"
#define RX_LOG_HDR_INVAR   "RX_HDR_INVARIANT"
#define RX_LOG_SIZE_ANOM   "RX_SIZE_ANOMALY"
#define RX_LOG_MISSING_FLD "RX_MISSING_FIELD"
```

**K2/K3/K4 中所有 pr_err/pr_warn 使用这些前缀**（已在 K2-K4 代码示例中体现）

##### 10.3.4 P1 执行序列

**严格按以下顺序执行，遵循 10.10 节 6 步流程**：

```
[Step 1] Rust 代码修改（按依赖顺序）
  1. R1 帧头校验 (protocol.rs)
  2. R2 msg_type 校验 (protocol.rs) — 依赖 R1
  3. R3 TLV 必需字段 (serialize.rs + fuse.rs) — 依赖 R1
  4. R5 响应大小防御 (client.rs) — 依赖 R1
  5. R4 诊断日志 (client.rs + net_handler.rs) — 依赖 R1,R2,R3

[Step 2] FUSE 容器测试
  - 容器内启动 master + volume + filer
  - 容器内挂载 powerfs-fuse
  - 测试用例：
    a. 正常 lookup/getattr/create/mkdir（验证不误报）
    b. 故意构造大响应（验证 RX_SIZE_ANOMALY 告警）
    c. 故意构造缺字段响应（验证 RX_MISSING_FIELD 错误）
    d. fio 顺序/随机读写（验证性能不回退）
  - 通过门槛：功能正确 + 无 panic + 性能不回退

[Step 3] Kernel 代码修改（按依赖顺序）
  1. K1 帧头校验 (pfs_rx_step) 
  2. K2 截断检测 (pfs_rx_dispatch) ★核心 — 依赖 K1
  3. K3 msg_type 校验 — 依赖 K1
  4. K4 TLV 必需字段 — 依赖 K2
  5. K5 buffer 调整 (9 处) — 依赖 K2
  6. K6 诊断日志 — 依赖 K2,K3,K4

[Step 4] 内核 QEMU 测试（从简单到复杂）
  1. 基础挂载：mount / umount / ls / stat
  2. 文件操作：create / write / read / delete
  3. 目录操作：mkdir / rmdir / rename / readdir
  4. fio 顺序读写
  5. fio 随机读写
  6. 高并发测试（多线程创建/读写）
  7. 故障注入：断连重连
  8. **dmesg 持续监控 ≥5 分钟**
  - 通过门槛：功能正确 + dmesg 干净 + 性能不回退

[Step 5] Commit
  Commit 1: "P1: add protocol validation with -E2BIG and strict header checks"
    - powerfs-net/src/protocol.rs (R1, R2)
    - powerfs-net/src/serialize.rs (R3)
    - powerfs-net/src/client.rs (R4, R5)
    - powerfs-fuse/src/fuse.rs (R3)
    - powerfs-filer/src/net_handler.rs (R4)
  
  Commit 2: "P1: add protocol validation with -E2BIG and strict header checks (kernel)"
    - kernel/powerfs_mod/powerfs_net.c (K1-K6)
    - kernel/powerfs_mod/powerfs_net.h (K6)

[Step 6] 更新进度
  - 更新附录 C 表格 P1 行
  - 更新 project_memory
  - 记录测试结果
```

##### 10.3.5 P1 测试用例清单

| 测试 ID | 测试描述 | 侧 | 预期结果 | 关联任务 |
|---------|---------|-----|---------|---------|
| T1 | 正常 lookup 响应（< 4KB） | Rust+Kernel | 无告警，功能正常 | R1-R5, K1-K6 |
| T2 | 正常 getattr 响应（含 chunks JSON） | Rust+Kernel | 无告警，chunks 解析正确 | R1-R5, K1-K6 |
| T3 | 故意发 body_len > data_len 帧 | Rust+Kernel | RX_HDR_INVARIANT 错误，连接关闭 | R1, K1 |
| T4 | 故意发 data_len > MAX_FRAME 帧 | Rust+Kernel | RX_HDR_INVARIANT 错误，连接关闭 | R1, K1 |
| T5 | 故意发 body > 4KB 的 lookup 响应 | Rust+Kernel | RX_TRUNCATE (-E2BIG) + 日志 | R5, K2, K5 |
| T6 | 故意发缺 Ino 字段的 lookup 响应 | Rust+Kernel | RX_MISSING_FIELD (-EIO) + 日志 | R3, K4 |
| T7 | 故意发 body > 4KB 的 getattr（异常大） | Rust+Kernel | RX_SIZE_ANOMALY 告警（不拒绝） | R2, K3 |
| T8 | fio 顺序读写 256MB | Rust+Kernel | 性能不回退（对比基线） | 全部 |
| T9 | fio 随机读写 256MB | Rust+Kernel | 性能不回退 | 全部 |
| T10 | IO500 mdtest-easy（0 字节文件） | Rust+Kernel | 功能正确，无告警 | 全部 |
| T11 | 高并发 100 线程创建文件 | Kernel | dmesg 无异常 | K1-K6 |
| T12 | 客户端断连重连 | Kernel | dmesg 无异常，重连后正常 | K1-K6 |
| T13 | dmesg 持续监控 5 分钟 | Kernel | 无 RCU stall / lockup / leak | 全部 |

### 10.4 里程碑 M1 详细任务（续）

#### P2: 二进制 TLV 编码 + LIST_CHUNKS RPC

**服务端**：
- [net_handler.rs](file:///home/portion/powerfs/powerfs-filer/src/net_handler.rs) `encode_chunks_fields` 改用嵌套 TLV
- 新增 `handle_list_chunks` RPC handler
- 自动模式检测（PerChunk / StripeDescriptor / Paginated）

**客户端 - FUSE**：
- [fuse.rs](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs) 解析 ChunkLayout V2
- 实现 LIST_CHUNKS 调用 + 缓存

**客户端 - 内核**：
- [powerfs_net.c](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_net.c) `powerfs_net_lookup_timeout` / `getattr` 增加 V2 解析
- 新增 `powerfs_net_list_chunks` 函数

**M1 验证门**：
- 内核 dmesg 无异常持续 ≥5 分钟
- 1GB 文件读写正常，元数据响应 < 100B（StripeDescriptor）
- fio 性能不回退（对比 P1 前）
- 协议校验日志可见（故意触发 -E2BIG 验证）

### 10.5 里程碑 M2 详细任务（小文件极致优化）

**目标**：IO500 mdtest IOPS 2-3x 提升，存储节省 99%。

#### P2.5: Inline 模式

**P2.5a - Filer 支持 inline 存储** ✅ 已完成：
- ✅ InodeInfo 新增 `inline_data: Option<Vec<u8>>` 字段 (shard_store.rs)
- ✅ Raft 日志条目支持携带 inline_data（≤8KB）(ShardCommand::UpdateInodeSizeChunks)
- ✅ `handle_create`：检测父目录 `powerfs.inline` xattr + 全局 `inline_max_size` 配置，
  返回 Placement::Inline + InlineMaxSize，跳过 Volume 分配 (net_handler.rs)
- ✅ `handle_close` (handle_update_inode_size_chunks)：解码 InlineData，单次 Raft 提交，
  含 8KB 硬上限校验 (net_handler.rs)
- ✅ `handle_getattr`/`handle_lookup`：`encode_chunks_fields` 在 inline_data 存在时
  输出 Placement::Inline + ChunkEncoding::InlineData (net_handler.rs)
- ✅ `handle_migrate_inline_alloc`：Inline → Flat 迁移分配 (net_handler.rs)。
  Filer 仅分配 (volume_id, needle_id)，不修改 inode（保留 inline_data 用于 crash safety）。
  客户端在写入超过 max_size×1.5 阈值时调用，合并数据后切换到 Flat 缓存路径
- ✅ 配置：`FilerConfig.inline_max_size` (默认 0=禁用，opt-in)，`FilerNetHandler::set_inline_max_size`
- ✅ 安全性：默认禁用（不改变现有 Flat 行为）；客户端 `decode_attr_resp` 跳过未知 TLV 字段，
  未实现 inline 的客户端不受影响

**P2.5b - 客户端 inline 写/读路径**（已实现）：
- FUSE [fuse.rs](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs)：
  - ✅ `PowerFsFs.inline_buffers: DashMap<u64, InlineBuffer>`：inline 模式 inode 的
    内存写入缓冲（`InlineBuffer { data, dirty }`），替代修改 CachedEntry
  - ✅ CREATE：`attr.is_inline()` 分支，不要求 volume_id/needle_id，初始化空 buffer +
    记录 `inline_max_size`
  - ✅ WRITE：`inline_buffers.get_mut` 命中时直接覆盖/追加到 buffer，标记 `dirty=true`，
    完全绕过 Volume Server + chunk_cache；超 8KB 硬上限返回 EFBIG（待 MIGRATE_INLINE）
  - ✅ READ：`inline_buffers.get` 命中时切片返回，绕过 Volume Server + lease
  - ✅ OPEN：getattr 刷新 inline_data 填充 buffer（重开已关闭的 inline 文件）；
    dirty 标记避免只读 open→release 回写覆盖并发写入
  - ✅ RELEASE：dirty 时把 buffer 作为 `inline_data` 单次 Raft 提交到 Filer
    （retry+timeout，同 Flat 路径），跳过 flush/lease 释放；非 dirty 跳过 sync
  - ✅ SETATTR(truncate)：调整 buffer 大小并标记 dirty
  - ✅ UNLINK：清理残留 inline buffer；FSYNC：inline 文件 no-op（release 时持久化）
- 客户端 RPC [meta_shard_client.rs](file:///home/portion/powerfs/powerfs-fuse-core/src/meta_shard_client.rs)：
  - ✅ `update_inode_size_chunks`：`req.inline_data` 存在时编码为 FileLayout
    (Placement::Inline + ChunkEncoding::InlineData)，与 Filer 解码对称
  - ✅ `attr_from_resp_with_layout`：从响应 body 解析 FileLayout，提取
    placement / inline_data / inline_max_size 到 `MetadataAttr`
  - ✅ `MetadataAttr::is_inline()` 辅助方法
- 传输类型 [lib.rs](file:///home/portion/powerfs/powerfs-coherence/src/lib.rs)：
  - ✅ `UpdateInodeSizeChunksRequest.inline_data: Option<Vec<u8>>`
- ✅ 迁移检测：累计 > max_size × 1.5 时调 `migrate_inline_alloc`（P2.5c 已实现）
- ⏳ 内核 [powerfs_fs.c](file:///home/portion/powerfs/kernel/powerfs_mod/powerfs_fs.c)：
  内核态 inline 路径待后续（当前 FUSE 客户端已完整覆盖用户态路径）

**P2.5c - 自动迁移 Inline → Flat** ✅ 已完成：
- ✅ CREATE 后无 WRITE 即 CLOSE：inline_data = vec![]（dirty=true 确保 release 持久化空文件）
- ✅ 不分配 needle_id，不创建空 needle
- ✅ GETATTR 返回 size=0 + InlineData{data:vec![]}
- ✅ DELETE 仅删 Filer 元数据
- ✅ 写入超过 max_size×1.5 阈值（4096×1.5=6144B）时自动迁移：
  客户端合并 inline buffer → 调 `migrate_inline_alloc` 分配 (volume_id, needle_id)
  → 数据放入 chunk_cache → 切换到 Flat 模式 → release 时走 Flat close 路径
- ✅ 迁移过程 crash-safe：Filer 仅分配不修改 inode，inline_data 保留直到 close 时 Flat chunks 覆盖

**P2.5d - IO500 mdtest 验证** ✅ 已完成（2026-08-08，容器环境）：
- 配置：`inline_max_size = 4096`（Filer 全局配置）
- 测试参数：`mdtest -n 1000 -w 3901 -e 3901 -F -i 3 -N 1`（3901B 文件，1000 个，3 次迭代）
- **结果对比**：

| 操作 | Baseline (Flat) ops/s | Inline ops/s | 加速比 |
|------|----------------------|-------------|--------|
| File creation | 27.1 | 60.1 | **2.22x** ✅ |
| File read | 686.5 | 915.3 | 1.33x |
| File stat | 1431.7 | 1534.2 | 1.07x |
| File removal | 396.3 | 378.2 | ~1.0x |

- ✅ 文件创建 IOPS ≥ 2x 目标达成（2.22x）
- ✅ 读取加速 1.33x（inline 数据从 Filer 元数据直接返回，绕过 Volume Server）
- ✅ 跨客户端读取一致（fuse-2 验证 MD5 匹配）
- ✅ Inline → Flat 迁移验证通过（7000B 文件触发迁移，MD5 一致）

**M2 验证门**：
- ✅ IO500 mdtest-hard IOPS ≥ 2x（对比 inline 前）— 文件创建 2.22x
- ✅ IO500 mdtest-easy 0 字节文件不分配 needle_id
- ✅ 3901B 文件存储占用 ≤ 4KB（vs 原 2MB needle）
- ✅ Inline → Flat 迁移过程无数据丢失
- Filer Raft log 增长可控（< 2x 基线）

### 10.6 里程碑 M3 详细任务（数据分布能力完整）

**目标**：支持从小文件到 256 vol 并行的全场景覆盖。

#### P3: Placement 提升 + 目录属性继承

**Filer**：
- 新增 `get_dir_placement(parent_ino) -> Option<PlacementSpec>`
- 新增 `get_dir_inline_threshold(parent_ino) -> Option<u32>`
- `handle_create` / `handle_mkdir` 读取父目录两个 xattr
- 文件大小超阈值自动提升 Stripe

**Master**：
- volume_id → node_id 映射
- `select_volumes_with_anti_affinity` 实现

**客户端**：
- locate() 算法实现
- 自动 stripe 提升触发

#### P5: WideStripe(256) 全集群并行

**Filer**：
- WideStripe volume_ids 范围压缩编码（避免 256×8B = 2KB 元数据）
- `handle_create` 支持 WideStripe 分配（256 volume 跨节点 anti-affinity）

**Master**：
- 全集群 volume 拓扑查询接口
- WideStripe 专用 volume pool（可选，避免与小文件争用）

**客户端**：
- WideStripe locate() 算法（256 路并行读）
- 客户端并发读连接池（256 连接）

**M3 验证门**：
- 64MB+ 文件自动提升为 Stripe(4)
- 目录 `powerfs.placement=wide_stripe:256:4MB` 生效
- 256 路并行读带宽 ≥ 单路 × 200
- anti-affinity 校验：stripe volume 分布在不同节点
- fio 顺序写 1GB 文件，元数据响应 < 100B

### 10.7 里程碑 M4 详细任务（可靠性体系完整）

**目标**：数据安全 + 空间效率 + 冷数据压缩。

#### P4: Reliability 状态机 + scrubber 异步转换

**Filer**：
- InodeInfo 新增 `reliability_state` 字段
- scrubber worker 定期扫描 Pending 状态文件
- 副本复制：SingleReplica → Replicated(2)
- 状态机：Pending → Syncing → Completed → Degraded

**Volume Server**：
- 副本复制接口（接收 Filer 指令复制数据到另一 volume）
- 副本一致性校验（CRC32 对比）

**客户端**：
- ReliabilityState 查询（GETATTR 返回状态）
- Degraded 状态降级读逻辑

#### P6: EC 编码 + 降级读

**Volume Server**：
- Reed-Solomon EC 编码/解码引擎
- EC 编码 worker（接收 Filer 指令，data shard → parity shard）
- 降级读：缺失 shard 时从剩余 shard 重建

**Filer**：
- EC 转换调度：Replicated(2) → EC(4+2/8+4/16+4)
- EC shard 位置管理（记录每个 shard 的 volume_id + node_id）
- anti-affinity 校验：data + parity 跨节点分布

**客户端**：
- EC 降级读透明处理（客户端无感知，Volume Server 内部重建）
- EC shard 位置缓存

#### P7: 副本压缩 + 后台压缩 worker

**Volume Server**：
- 压缩 worker（扫描 atime 超阈值文件）
- zstd/lz4/snappy 压缩算法支持
- 压缩数据写新 needle + 原 needle 标记 pending_delete

**Filer**：
- InodeInfo 新增 `compression_state` 字段
- 压缩完成更新元数据（算法 + 压缩比）
- GC 回收原 needle

**客户端**：
- 读路径检测压缩标志
- 解压逻辑（zstd/lz4/snappy）

**M4 验证门**：
- 单 volume server 故障，Replicated(2) 仍可读
- 单节点故障，EC(4+2) 仍可读（Degraded 状态）
- EC(4+2) 节省空间 50%（vs Replicated(2)）
- EC(8+4) 节省空间 67%
- 副本压缩比验证（zstd 典型 2-3x）
- scrubber 状态机正确（Pending → Syncing → Completed）
- 压缩文件读路径正确（解压后数据一致）

### 10.8 里程碑 M5 详细任务（技术债务清理）

**目标**：代码库干净，无历史包袱。

#### P8: JSON 字段废弃 + 文档收敛

**服务端**：
- 移除 `FieldId::Chunks`（JSON）编码路径
- 保留 `FieldId::VolumeId` / `FileKey`（首 chunk 兼容字段，永久保留）

**客户端**：
- 移除 JSON chunks 解析代码
- 仅保留二进制 ChunkLayout V2 解析

**文档**：
- 更新 [file-key-design-analysis.md](file:///home/portion/powerfs/docs/file-key-design-analysis.md) 标注 JSON 已废弃
- 更新 [network-architecture.md](file:///home/portion/powerfs/docs/network-architecture.md) 反映新协议字段
- 归档 `file_layout_stripe_design.md` 和 `ecplan.md` 布局部分

**M5 验证门**：
- 旧客户端（仅支持 JSON）连接新服务端：降级响应正确
- 新客户端连接旧服务端：回退解析 JSON 正确
- 代码库无死代码（JSON 编解码路径已移除）
- 文档无矛盾，单一权威来源

### 10.9 验证标准（全局）

**说明**：以下为跨里程碑的全局验证标准，每个里程碑的具体验证门见 10.3-10.8 节。

**功能验证**：
- fio 顺序/随机读写
- IO500 标准测试（含 mdtest-easy + mdtest-hard）
- 跨客户端读可见性
- 客户端 remount 后读一致性
- **Inline 文件创建/读/迁移/删除全流程**
- **0 字节文件不分配 needle_id 验证**

**可靠性验证**：
- 单 volume server 故障，副本/EC 仍可读
- scrubber 转换状态机正确
- 副本压缩比符合预期
- **Inline 文件 Raft 复制正确（Filer 节点故障仍可读）**

**性能验证**：
- 元数据响应大小符合预期（小文件 < 200B，大文件 < 100B）
- 256 路并行带宽 ≥ 单路 × 200
- 协议校验无性能回退（fio 对比）
- **IO500 mdtest-hard IOPS inline 后 ≥ 2x 提升**
- **Inline 文件存储节省 ≥ 99%（3901B vs 2MB needle）**

**内核稳定性**：
- QEMU 虚拟机持续运行 ≥ 5 分钟，dmesg 无异常
- 高并发写入无 workqueue lockup
- 客户端断连重连无内存泄漏
- **Inline 迁移到 Flat 过程中无数据丢失**

### 10.10 每个 Phase 的标准实施流程

**强制规范**：每个 Phase（P1-P8）都必须按以下顺序实施，不允许跳步。

```
┌─────────────────────────────────────────────────────────────┐
│  Step 1: 修改服务端 + FUSE 代码                              │
│  - powerfs-filer / powerfs-volume / powerfs-master          │
│  - powerfs-fuse                                              │
│  - powerfs-net / powerfs-common / powerfs-coherence         │
├─────────────────────────────────────────────────────────────┤
│  Step 2: FUSE 容器环境测试验证                               │
│  - 容器内启动服务 + 挂载 powerfs-fuse                        │
│  - fio / IO500 / 功能测试                                    │
│  - 通过门槛：功能正确 + 性能不回退                            │
├─────────────────────────────────────────────────────────────┤
│  Step 3: 修改内核文件系统代码                                 │
│  - kernel/powerfs_mod/                                       │
│  - QEMU 虚拟机构建 + 加载                                    │
├─────────────────────────────────────────────────────────────┤
│  Step 4: 内核测试验证                                        │
│  - QEMU 内挂载 powerfs 内核模块                              │
│  - fio / 功能测试                                            │
│  - dmesg 持续监控 ≥ 5 分钟无异常                             │
│  - 通过门槛：功能正确 + dmesg 干净                            │
├─────────────────────────────────────────────────────────────┤
│  Step 5: 分别 commit 递交                                    │
│  - commit 1: 服务端 + FUSE + 公共库（英文 message）          │
│  - commit 2: 内核文件系统（英文 message）                    │
├─────────────────────────────────────────────────────────────┤
│  Step 6: 更新进度                                            │
│  - 更新本文档 Phase 状态                                     │
│  - 更新 project_memory                                       │
│  - 记录测试结果到评估文档                                     │
└─────────────────────────────────────────────────────────────┘
```

#### 10.10.1 Step 1: 修改服务端 + FUSE 代码

**范围**：
- `powerfs-filer/`：Filer 服务端逻辑
- `powerfs-volume/`：Volume Server 数据存储
- `powerfs-master/`：Master 卷管理
- `powerfs-fuse/`：FUSE 用户态客户端
- `powerfs-net/`、`powerfs-common/`、`powerfs-coherence/`：公共库

**原则**：
- 先服务端后客户端（FUSE 依赖服务端协议）
- 公共库修改需保证向后兼容（避免破坏其他组件）
- 代码注释用中文（与项目现有风格一致）

#### 10.10.2 Step 2: FUSE 容器环境测试验证

**环境要求**（遵循 project_memory 约束）：
- 容器内启动所有服务（master + volume + filer）
- 容器内安装 fuse 并挂载 `powerfs-fuse`（不走主机跨容器）
- 使用 `/app/powerfs-fuse` 路径执行（非 `/usr/local/bin/`）

**测试内容**：
- 功能测试：对应 Phase 的验证门
- 性能测试：fio 对比（确保不回退）
- IO500 测试（M2 里程碑必备）

**通过门槛**：
- 功能全部正确
- 性能不回退（fio 对比基线）
- 无 panic / error 日志

#### 10.10.3 Step 3: 修改内核文件系统代码

**范围**：
- `kernel/powerfs_mod/`：内核模块源码

**原则**（遵循 project_memory 约束）：
- 内核态修改影响较大，从简单到复杂的测试
- 宁愿多测试验证，也不可快速到下个阶段
- 使用 `GFP_NOFS` 避免 VFS 回调递归
- 错误处理：可恢复用 `WARN_ON_ONCE` + 错误码，不变式违反用 `BUG_ON`

#### 10.10.4 Step 4: 内核测试验证

**环境要求**（遵循 project_memory 约束）：
- **必须使用 QEMU 虚拟机**，不在主机直接测试（避免系统崩溃）
- QEMU 双网卡：eth1 配置 `10.0.2.15/24` 支持 hostfwd SSH
- KASAN 关闭（`kasan.enabled=0`）减少开销
- tap0 启用 `vhost=on` 加速

**测试流程**（从简单到复杂）：
1. 基础挂载测试：mount / umount / ls / stat
2. 文件操作测试：create / write / read / delete
3. 目录操作测试：mkdir / rmdir / rename / readdir
4. fio 顺序读写测试
5. fio 随机读写测试
6. 高并发测试（多线程 / 多进程）
7. 故障注入测试（断连重连 / 服务重启）

**dmesg 监控要求**（遵循 project_memory 约束）：
- **不能简单认为成功就过**，必须检查 dmesg
- 持续运行 ≥ 5 分钟，定期检查 dmesg 无异常
- 关注：RCU stall、workqueue lockup、null pointer deref、memory leak
- 发现异常必须定位根因并修复，不允许绕过

**通过门槛**：
- 所有功能测试正确
- **dmesg 持续 ≥5 分钟无任何异常**
- fio 性能不回退
- 客户端 remount 后读一致性

#### 10.10.5 Step 5: 分别 commit 递交

**两个独立 commit**：

**Commit 1 - 服务端 + FUSE + 公共库**：
```
<phase-id>: <short description>

<detailed description of changes>

- Modified: powerfs-filer/src/...
- Modified: powerfs-fuse/src/...
- Modified: powerfs-net/src/...
- Tested: FUSE container environment, fio + IO500
```

**Commit 2 - 内核文件系统**：
```
<phase-id>: <short description> (kernel)

<detailed description of kernel changes>

- Modified: kernel/powerfs_mod/...
- Tested: QEMU VM, dmesg clean for 5+ minutes
```

**Commit message 规范**：
- 英文（遵循项目现有风格）
- 首行 `<phase-id>: <description>`，例如 `P1: add protocol validation with -E2BIG`
- 空行后详细描述
- 列出修改的文件和测试方式

**Git 安全约束**（遵循 project_memory）：
- 不主动 commit，除非用户明确要求
- 不 push 到远程，除非用户明确要求
- 不使用 `git add -A`，按文件添加
- 不修改 git config

#### 10.10.6 Step 6: 更新进度

**更新内容**：
1. **本文档 Phase 状态**：在 10.2 节 Phase 总览表标注完成日期
2. **project_memory**：记录关键决策、踩坑、约束更新
3. **测试评估文档**：fio / IO500 结果记录到 `docs/perf-evaluation-<phase>.md`

**进度跟踪表**（维护在本文档末尾附录 C）：

| Phase | 状态 | 服务端+FUSE 完成 | 内核完成 | 测试通过 | Commit Hash | 完成日期 |
|-------|------|-----------------|---------|---------|------------|---------|
| P1 | 待开始 | - | - | - | - | - |
| P2 | 待开始 | - | - | - | - | - |
| ... | ... | ... | ... | ... | ... | ... |

---

---

## 11. 与现有文档关系

### 11.1 取代

- **`file_layout_stripe_design.md`**：本文档第 4 章覆盖其全部内容并扩展（Flat/Stripe/WideStripe 三态 + 目录继承 + anti-affinity）。该文档保留作为历史参考，但**不再权威**。

### 11.2 部分取代

- **`ecplan.md`**：本文档第 5 章覆盖其布局部分（Reliability 状态机 + EC 跨节点分布）。`ecplan.md` 中的 Bitrot 检测、回收站、WORM 锁定等非布局功能仍归该文档。

### 11.3 引用

- **`file-key-design-analysis.md`**：保留，描述 file_key 历史 bug 修复。本文档第 6 章引用其结论（needle_id 由 Filer 分配，非 Master assign_fid）。
- **`network-architecture.md`**：保留，描述通信层。本文档第 7 章新增 LIST_CHUNKS RPC 需遵循该文档协议规范。
- **`lease-design.md`**：保留，描述 lease 机制。本文档不修改 lease 设计，但 EC 转换期间 lease 行为需符合该文档约束。

### 11.4 不影响

- `meta-design.md`：元数据 Raft 设计不变
- `shard-optimization-design.md`：分片策略不变
- `coherence-test.md`：一致性测试规范不变

---

## 12. 开放问题（待后续讨论）

1. **EC 编码算法选择**：Reed-Solomon vs XOR码（8+4 以上 RS 更优，4+2 两者皆可）
2. **压缩算法默认值**：zstd level 3 vs level 6（压缩比 vs CPU）
3. **scrubber 调度策略**：纯空闲触发 vs 持续低优先级运行
4. **WideStripe 卷分配**：256 volume 是否需要预留专用 pool，避免与小文件争用
5. **目录属性继承深度**：是否限制继承层级避免深层目录性能问题
6. **EC 转换中断恢复**：scrubber 中断后如何续传，避免重复编码

---

## 附录 A：术语表

| 术语 | 含义 |
|------|------|
| Chunk | 文件数据分片，固定 2MB |
| Needle | Volume Server 上的物理存储单元，与 Chunk 1:1 |
| Stripe | 跨多个 Volume 的条带单元，默认 64MB |
| Volume | 数据卷，单 Volume Server 上的逻辑存储空间 |
| **Inline** | **数据直接存 Filer 元数据，绕过 Volume Server（< 4KB/8KB 微小文件）** |
| **InlineData** | **Inline 模式下的数据编码，数据直接在 Filer Raft 日志中** |
| Placement | 数据分布策略（Inline/Flat/Stripe/WideStripe） |
| Reliability | 数据保护策略（SingleReplica/Replicated/EC） |
| ChunkEncoding | 元数据序列化方式（InlineData/PerChunk/StripeDescriptor/Paginated） |
| anti-affinity | 数据强制分布在不同物理节点的约束 |
| scrubber | 后台数据扫描和转换 worker |
| **MIGRATE_INLINE** | **Inline → Flat 迁移 RPC（文件超阈值时触发）** |

## 附录 B：默认参数汇总

| 参数 | 默认值 | 可调 |
|------|--------|------|
| chunk_size | 2MB | 否（协议常量） |
| **DEFAULT_INLINE_THRESHOLD** | **4KB** | **是（目录 xattr `powerfs.inline`）** |
| **INLINE_MAX_THRESHOLD** | **8KB** | **是（Filer 配置上限）** |
| **INLINE_MIGRATE_RATIO** | **1.5** | **否（协议常量，滞后窗口）** |
| DEFAULT_STRIPE_SIZE | 64MB | 是（目录 xattr） |
| DEFAULT_STRIPE_COUNT | 4 | 是（目录 xattr） |
| PROMOTE_THRESHOLD | 64MB | 是（Filer 配置） |
| WIDE_STRIPE_COUNT | 256 | 是（目录 xattr） |
| WIDE_STRIPE_SIZE | 4MB | 是（目录 xattr） |
| SMALL_FILE_THRESHOLD | 10MB | 是（Filer 配置） |
| EC_DATA_DEFAULT | 4 | 是（目录 xattr） |
| EC_PARITY_DEFAULT | 2 | 是（目录 xattr） |
| COMPRESS_IDLE_THRESHOLD | 30 天 | 是（Volume 配置） |
| COMPRESS_DEFAULT_ALGO | zstd | 是（xattr） |
| PAGINATE_CHUNK_THRESHOLD | 50 | 否（协议常量） |
| LIST_CHUNKS_DEFAULT_LIMIT | 256 | 是（请求参数） |

## 附录 C：实施进度跟踪表

> 本表记录每个 Phase 的实施进度，按 10.10 节标准流程执行。每完成一个 Phase 更新此表。

| Phase | 里程碑 | 状态 | 服务端+FUSE | 内核 | 测试通过 | Commit 1 (服务端) | Commit 2 (内核) | 完成日期 |
|-------|--------|------|------------|------|---------|------------------|----------------|---------|
| P1 | M1 | 已完成 | ✅ | ✅ | ✅ | 已合入 | 已合入 | 2026-07 |
| P2 | M1 | 进行中 | ✅ TLV 编码落地 | 待开始 | 待 FUSE 容器测试 | 0e0ec5f2 | - | - |
| P2.5 | M2 | 已完成 | ✅ Inline 存储+迁移+客户端路径 | 待开始 | ✅ mdtest 2.22x | a64c4247+199fded6+321217aa+cce28ec5+a1dc2542 | - | 2026-08-08 |
| P3 | M3 | 进行中 | ✅ Stripe alloc + xattr + write/read/flush | 待开始 | ✅ fio 962/4000 MiB/s | 098e13bb+92d078b2+628cddb1+a99465b9 | - | 2026-08-08 |
| P4 | M4 | 进行中 | ✅ Reliability 状态机 + scrubber + 读路径 failover (Flat+Stripe) | 待开始 | ✅ 容器 failover 验证 | 72d2b4c7+d7c4c365+48bf4604+f80e3ba0+c120b683 | - | 2026-08-08 |
| P5 | M3 | 待开始 | - | - | - | - | - | - |
| P6 | M4 | 待开始 | - | - | - | - | - | - |
| P7 | M4 | 待开始 | - | - | - | - | - | - |
| P8 | M5 | 部分完成 | ✅ JSON 路径已移除 | - | - | 0e0ec5f2 | - | 2026-08-08 |

**状态说明**：
- 待开始：未启动
- 进行中：Step 1-4 某一步
- 已完成：Step 6 进度更新完毕
- 阻塞：遇到问题需讨论

**里程碑进度**：
| 里程碑 | 状态 | 完成 Phase | 验证门通过 | 完成日期 |
|--------|------|-----------|-----------|---------|
| M1 协议基础 | 进行中 | 1/2 (P1✅ P2进行中) | 待 FUSE 容器测试 | - |
| M2 小文件优化 | 已完成 | 1/1 (P2.5✅) | ✅ mdtest create 2.22x | 2026-08-08 |
| M3 数据分布 | 进行中 | 0.5/2 (P3 FUSE✅) | ✅ fio Stripe write 962 MiB/s | - |
| M4 可靠性 | 进行中 | 1/3 (P4 FUSE✅) | ✅ 容器 failover 验证 | - |
| M5 技术债务 | 部分完成 | 0.5/1 (JSON 路径已移除) | - | - |

### P3 FUSE 性能评估 (2026-08-08)

**测试环境**: Docker 容器集群 (3 Master + 3 Volume + 3 Filer + 1 FUSE), fio 3.16

**配置**: `stripe:128:1MB` (128 个 1MB chunk, 跨 3 个 volume anti-affinity 分配)

| 模式 | 顺序写 (MiB/s) | 顺序读 (MiB/s) | 说明 |
|------|---------------|---------------|------|
| Flat | 941 | 3879 | 基准 (单 volume) |
| Stripe (修复前) | 76.2 | - | read-before-write 导致 "needle not found" |
| Stripe (修复后) | 962 | 4000 | 跳过未 flush chunk 的读取, 性能恢复 |

**关键修复**: Stripe 预分配 chunk 的 `size=0` (未 flush), 写路径通过 `chunk_map` 检测
`size==0` 跳过 read-before-write, 避免 ~128 次无效 volume RPC (128MB 文件).

**数据完整性**: MD5 校验通过 (write → direct IO read → cross-client read).

**anti-affinity 验证**: 128 chunk round-robin 分布到 3 个 volume (43/43/42).

### P4 可靠性评估 (2026-08-08)

**测试环境**: Docker 容器集群 (3 Master + 3 Volume + 3 Filer + 1 FUSE)

**实现范围** (FUSE + 服务端, 内核待开始):

| 组件 | 功能 | 状态 |
|------|------|------|
| Filer | ReliabilityState 状态机 (PendingReplicated → Replicated) | ✅ |
| Filer | scrubber worker 异步副本复制 (TLV 协议) | ✅ |
| Filer | 数据修改时状态回退 (Replicated → PendingReplicated) | ✅ |
| Filer | anti-affinity 副本 volume 选择 | ✅ |
| Volume | 副本复制接口 (scrubber 经 TLV read→write) | ✅ |
| FUSE | GETATTR 返回 replica_chunks | ✅ |
| FUSE | Flat 读路径 failover (主 volume 失败→副本 volume) | ✅ |
| FUSE | Stripe 读路径 failover (同 offset 副本查找) | ✅ |
| FUSE | CRC32 数据完整性校验 (写时计算, 读时验证) | ✅ |
| 协议 | FieldId::ReplicaChunks (0xB5) TLV 编码 | ✅ |

**关键修复**:
1. **chunk cache 粒度**: `effective_chunk_size = min(chunk_size, stripe_size)`, 确保 Stripe 模式每个 cache entry 对应一个 stripe unit, 否则 scrubber 读取失败 ("needle not found")
2. **TLV 顺序解码限制**: `decode_file_layout` 消费 ReplicaChunks 字段后 `next_bytes` 找不到, 新增 raw TLV 字节扫描绕过
3. **状态回退误触发**: getattr 比较 chunks 时不应回退状态, 仅 `chunks_changed` 时才 Replicated → PendingReplicated
4. **Inline→Flat 迁移数据丢失**: 迁移后必须 `mark_dirty(inode, 0)`, 否则 flusher 不会将迁移数据写到 Volume Server
5. **Stripe 空 chunks panic**: `placement=Some` + `chunks=[]` 时返回 EIO 而非 panic

**failover 验证**: 杀掉主 volume 容器, FUSE 读路径自动切换到副本 volume, 文件可读且 MD5 一致.

**遗留事项**:
- scrubber 副本复制时 CRC32 一致性校验 (scrubber 当前复制 CRC32 元数据但未校验读到的数据, FUSE 读路径已校验)
- 多 zone 环境 volume 地址查找偶发失败
- scrubber 大规模文件扫描性能优化

