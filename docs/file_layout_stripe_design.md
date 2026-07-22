# FileLayout 与 Stripe 设计文档

## 1. 设计背景

高性能计算存储场景下，单文件可能达到数十 GB 甚至 TB 级别。为了充分利用多台存储节点的并行 I/O 能力，需要引入条带化（Stripe）机制，将大文件的数据分布到多个 Volume 上，实现读写并行化。

### 1.1 核心目标

- **小文件高效存储**：单 Volume 顺序写入，无额外元数据开销
- **大文件并行 I/O**：自动提升为 Stripe 模式，数据条带化分布到多个 Volume
- **灵活扩展**：支持配置条带参数（大小、宽度）
- **热均衡**：通过 round-robin 起始索引避免热点 Volume

---

## 2. 布局模式

### 2.1 LayoutType 枚举

```rust
pub enum LayoutType {
    Flat = 0,   // 平铺模式：所有 chunk 写入同一个 volume（小文件）
    Stripe = 1, // 条带模式：数据按 stripe_size 轮流分布到多个 volume（大文件）
}
```

### 2.2 两种模式对比

| 特性 | Flat 模式 | Stripe 模式 |
|------|-----------|-------------|
| **适用场景** | 小文件（< 64MB） | 大文件（≥ 64MB） |
| **Volume 数量** | 1 | stripe_count（默认4） |
| **写入策略** | 顺序写入单个 Volume | 条带轮转写入多个 Volume |
| **并行度** | 1 | stripe_count |
| **元数据开销** | 无（不存储 Layout） | 存储 FileLayout 到 extended |

---

## 3. FileLayout 结构

```rust
pub struct FileLayout {
    pub layout_type: LayoutType,     // 布局类型
    pub stripe_size: u64,            // 单条带大小（字节），默认 64MB
    pub stripe_count: u32,           // 条带宽度（Volume 数量），默认 4
    pub start_volume_idx: u32,       // 起始 Volume 索引（round-robin 错开）
    pub volume_ids: Vec<u64>,        // 分配的 Volume ID 列表
}
```

### 3.1 默认参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `DEFAULT_STRIPE_SIZE` | 64MB | 单条带大小 |
| `DEFAULT_STRIPE_COUNT` | 4 | 条带宽度（并行 Volume 数） |
| `PROMOTE_THRESHOLD` | 64MB | 文件超过此大小自动提升为 Stripe |

---

## 4. Stripe 定位算法

### 4.1 数据分布模型

Stripe 模式采用类似 RAID0 的数据分布策略：

```text
文件 offset:  0 ──────── stripe_size ──────── stripe_size*2 ──────── stripe_size*3 ────── ...
                  ↓              ↓                  ↓                      ↓
volume[0]:    [0, s)      [s*4, s*5)        [s*8, s*9)              ...
volume[1]:    [s, s*2)    [s*5, s*6)        [s*9, s*10)             ...  
volume[2]:    [s*2, s*3)  [s*6, s*7)        [s*10, s*11)            ...
volume[3]:    [s*3, s*4)  [s*7, s*8)        [s*11, s*12)            ...
```

每个 Volume 连续写入 `stripe_size` 字节后轮转到下一个 Volume，形成条带循环。

### 4.2 locate() 算法

根据文件偏移计算目标 Volume 和 Volume 内偏移：

```rust
pub fn locate(&self, file_offset: u64) -> (usize, u64) {
    let stripe_size = self.stripe_size.max(1);
    let stripe_idx = file_offset / stripe_size;           // 条带序号
    let vol_rank = (stripe_idx % self.stripe_count as u64) as u32;  // 当前轮次中的 Volume 排名
    let vol_array_idx = ((self.start_volume_idx + vol_rank) as usize) % self.volume_ids.len();  // 实际 Volume 数组索引
    let vol_offset = (stripe_idx / self.stripe_count as u64) * stripe_size + (file_offset % stripe_size);  // Volume 内偏移
    (vol_array_idx, vol_offset)
}
```

**关键设计点**：
- `start_volume_idx`：通过 round-robin 错开不同文件的起始 Volume，避免热点
- `vol_array_idx`：考虑起始偏移后的实际 Volume 数组索引
- `vol_offset`：计算该数据在目标 Volume 内的物理偏移

### 4.3 locate_range() 算法

计算一个写入区间 `[offset, offset+size)` 跨越哪些 Volume：

```rust
pub fn locate_range(&self, file_offset: u64, size: u64) -> Vec<(usize, u64, u64, u64)>
```

返回值：`Vec<(volume_array_idx, vol_offset_start, vol_offset_end, file_offset_start)>`

**应用场景**：
- 大文件写入跨越条带边界时，拆分为多个子写入操作
- 并行写入多个 Volume，提升吞吐量

---

## 5. 存储与序列化

### 5.1 存储位置

FileLayout 信息存储在 `Entry.extended["file_layout"]` 中，无需修改 Proto 定义。

**设计优势**：
- 小文件不存储 Layout（Flat 模式默认），节省元数据空间
- 大文件提升为 Stripe 模式时才写入 Layout
- 向后兼容：老版本客户端忽略 extended 中的 file_layout 字段

### 5.2 序列化格式

采用紧凑的二进制格式：

| 字段 | 类型 | 大小 | 说明 |
|------|------|------|------|
| layout_type | u8 | 1 字节 | 0=Flat, 1=Stripe |
| stripe_size | u64 LE | 8 字节 | 条带大小 |
| stripe_count | u32 LE | 4 字节 | 条带宽度 |
| start_volume_idx | u32 LE | 4 字节 | 起始索引 |
| num_volumes | u32 LE | 4 字节 | Volume 数量 |
| volume_ids | [u64 LE] | N*8 字节 | Volume ID 列表 |

**示例**：Stripe 模式，4 个 Volume [1,2,3,4]，起始索引 2

```
0x01 0x00000040000000 0x00000004 0x00000002 0x00000004 0x00000001 0x00000002 0x00000003 0x00000004
 ^     ^                 ^          ^           ^          ^          ^          ^          ^
 type  64MB             4          start=2     4 vols    vol1      vol2      vol3      vol4
```

---

## 6. 自动提升机制

### 6.1 提升触发条件

当文件大小超过 `PROMOTE_THRESHOLD`（默认 64MB）且当前为 Flat 模式时，自动提升为 Stripe 模式：

```rust
let should_promote = layout.as_ref().is_none_or(|l| !l.is_stripe()) && file_size > PROMOTE_THRESHOLD;
```

### 6.2 提升流程

1. **批量分配 Stripe Volume**：调用 `assign_stripe_fids()` 获取多个 Volume 的 FID
2. **创建 Stripe Layout**：使用分配的 Volume ID 列表创建 FileLayout
3. **更新元数据**：将 FileLayout 序列化并存储到 Entry.extended
4. **按条带写入**：后续写入按 Stripe 模式分布到多个 Volume

### 6.3 提升时机

提升操作在 `flush_dirty_chunks_inner()` 中触发，即：
- 文件 dirty 数据达到阈值触发 flush 时
- 文件关闭（release）时

---

## 7. Round-Robin 起始索引

### 7.1 设计目的

避免多个大文件同时使用相同的 Volume 序列，导致热点问题。

### 7.2 分配策略

Master 在 `assign_stripe_volumes()` 中维护一个原子计数器：

```rust
let start_idx = self.stripe_round_robin.fetch_add(1, Ordering::Relaxed) % count;
```

每个新创建的 Stripe 文件获得递增的起始索引，循环覆盖。

### 7.3 效果示例

| 文件 | start_volume_idx | Volume 序列 |
|------|------------------|-------------|
| File A | 0 | [vol0, vol1, vol2, vol3] |
| File B | 1 | [vol1, vol2, vol3, vol0] |
| File C | 2 | [vol2, vol3, vol0, vol1] |
| File D | 3 | [vol3, vol0, vol1, vol2] |
| File E | 0 | [vol0, vol1, vol2, vol3] |

---

## 8. 写入流程

### 8.1 Flat 模式写入

```
┌─────────────────────────────────────────────────────────────┐
│  write(inode, offset, data)                                 │
│                          ↓                                  │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  ChunkCache: 缓存脏数据                               │  │
│  └───────────────────────────────────────────────────────┘  │
│                          ↓                                  │
│  flush_dirty_chunks()                                       │
│                          ↓                                  │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  batch_write_blob(): 批量写入单个 Volume              │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### 8.2 Stripe 模式写入

```
┌─────────────────────────────────────────────────────────────────────────┐
│  write(inode, offset, data)                                            │
│                          ↓                                             │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │  ChunkCache: 缓存脏数据                                           │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                          ↓                                             │
│  flush_dirty_chunks()                                                  │
│                          ↓                                             │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │  根据 FileLayout.locate() 按 Volume 分组脏数据                   │  │
│  │     ├── Volume 0: chunks[0, 4, 8, ...]                           │  │
│  │     ├── Volume 1: chunks[1, 5, 9, ...]                           │  │
│  │     ├── Volume 2: chunks[2, 6, 10, ...]                          │  │
│  │     └── Volume 3: chunks[3, 7, 11, ...]                          │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                          ↓                                             │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │  并行 batch_write_blob() 到多个 Volume                           │  │
│  │     ├── batch_write_blob(vol0_addr, fid0, chunks0)               │  │
│  │     ├── batch_write_blob(vol1_addr, fid1, chunks1)               │  │
│  │     ├── batch_write_blob(vol2_addr, fid2, chunks2)               │  │
│  │     └── batch_write_blob(vol3_addr, fid3, chunks3)               │  │
│  └───────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 9. 读取流程

### 9.1 单条带读取

```rust
let (vol_idx, vol_offset) = layout.locate(file_offset);
let volume_id = layout.volume_ids[vol_idx];
// 从对应 Volume 读取数据
```

### 9.2 跨条带读取

```rust
let ranges = layout.locate_range(file_offset, size);
for (vol_idx, vol_off_start, vol_off_end, file_off_start) in ranges {
    // 从对应 Volume 读取子区间数据
}
```

---

## 10. 配置参数

### 10.1 FUSE 客户端配置

通过 `flush_manager.rs` 中的 `FlushConfig` 控制：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `per_file_threshold` | 4MB | 单文件 dirty 数据触发 flush 阈值 |
| `global_threshold` | 64MB | 全局 dirty 数据触发 flush 阈值 |
| `max_dirty_age` | 5秒 | 脏数据最大存活时间 |
| `worker_count` | 2 | Flush worker 线程数 |

### 10.2 Stripe 参数

通过 `file_layout.rs` 中的常量控制：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `DEFAULT_STRIPE_SIZE` | 64MB | 单条带大小 |
| `DEFAULT_STRIPE_COUNT` | 4 | 条带宽度 |
| `PROMOTE_THRESHOLD` | 64MB | Flat→Stripe 提升阈值 |

---

## 11. 代码位置

| 组件 | 文件路径 | 核心功能 |
|------|----------|----------|
| FileLayout 定义 | `powerfs-fuse-enterprise/src/file_layout.rs` | LayoutType、FileLayout 结构、定位算法、序列化 |
| FUSE 集成 | `powerfs-fuse-enterprise/src/fuser_fs.rs` | flush_dirty_chunks_inner 中的 Stripe 提升和写入 |
| Master 分配 | `powerfs-master/src/master.rs` | assign_stripe_volumes、round-robin 起始索引 |
| gRPC 接口 | `powerfs-master/src/server.rs` | AssignFidRequest/Response 支持 stripe_count |

---

## 12. 性能预期

### 12.1 Stripe 模式并行度

| 参数配置 | 理论并行度 | 说明 |
|----------|------------|------|
| stripe_count=4 | 4x | 4 个 Volume 并行写入 |
| stripe_count=8 | 8x | 8 个 Volume 并行写入 |

### 12.2 IO500 测试数据（改造后）

| 测试项 | 改造前 | 改造后 | 提升幅度 |
|--------|--------|--------|----------|
| ior-easy-read | - | - | 19% |
| mdtest-easy-write | - | - | 37% |
| mdtest-easy-stat | - | - | 28% |

---

## 13. 未来优化方向

1. **动态 Stripe 参数**：支持按文件类型/大小配置不同的 stripe_size 和 stripe_count
2. **Volume 热迁移**：支持运行时将热 Volume 数据迁移到冷存储
3. **RAID1 冗余**：在 Stripe 基础上增加副本，提升可靠性
4. **自适应条带宽度**：根据集群规模自动调整 stripe_count