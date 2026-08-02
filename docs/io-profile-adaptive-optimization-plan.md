# PowerFS I/O 特征学习与自适应优化规划

> 状态：**独立规划方向**，设计为可插拔模块，未来独立接入
> 范围：应用 I/O 特征画像 + 自适应优化策略
> 编写日期：2026-08-02

---

## 一、概述

### 1.1 目标

通过学习应用 I/O 特征，系统自动调整优化参数，提升应用性能。分三阶段推进：

1. **阶段1（离线指纹 + 策略库）**：冷启动采集 trace → 生成指纹 → 匹配预定义策略
2. **阶段2（在线自适应）**：运行时检测模式切换 → 动态调整参数
3. **阶段3（ML 预测）**：研究探索，预测式预取

### 1.2 设计原则

- **可插拔**：作为独立 crate（`powerfs-profiler`），通过 trait 接入 FUSE 层，不影响核心架构
- **零侵入**：采集层通过 hook 接口接入，不修改 FUSE 回调主逻辑
- **可禁用**：默认关闭，通过配置/环境变量启用
- **可扩展**：策略库可扩展，支持自定义指纹和策略

---

## 二、模块架构

### 2.1 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│                    powerfs-fuse (FUSE 层)                    │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │ read/write  │  │ lookup/     │  │  IoProfileHook       │ │
│  │ 回调        │  │ readdir     │  │  (采集 hook)         │ │
│  └──────┬──────┘  └──────┬──────┘  └──────────┬──────────┘ │
│         │                │                     │            │
│         │    ┌───────────┴─────────────────────┘            │
│         │    │                                              │
│         ▼    ▼                                              │
│  ┌─────────────────────────┐  ┌──────────────────────────┐ │
│  │ 现有优化参数            │  │  powerfs-profiler (独立) │ │
│  │ (prefetch/lease/cache)  │◄─┤  ProfileAnalyzer         │ │
│  │                         │  │  StrategyMatcher         │ │
│  └─────────────────────────┘  └──────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 模块分层

| 层 | 职责 | 位置 |
|----|------|------|
| **采集层** | hook FUSE 回调，采集 I/O trace | `powerfs-fuse/src/io_profile_hook.rs` |
| **分析层** | trace → 特征向量 → 指纹 | `powerfs-profiler/src/analyzer.rs` |
| **策略层** | 指纹 → 优化参数 | `powerfs-profiler/src/strategy.rs` |
| **应用层** | 参数 → 系统调整 | `powerfs-fuse` 运行时配置接口 |

### 2.3 独立 crate 设计

`powerfs-profiler` 作为独立 crate，只依赖 `powerfs-common`，不依赖 `powerfs-fuse`：

```
powerfs-profiler/
├── Cargo.toml
├── src/
│   ├── lib.rs              # 模块入口 + 公共 trait
│   ├── analyzer.rs         # IoFingerprint 分析器
│   ├── strategy.rs         # OptimizationProfile 策略库
│   ├── trace.rs            # IoTrace 数据结构
│   ├── matcher.rs          # FingerprintMatcher trait + 实现
│   └── builtin_profiles.rs # 内置指纹策略（io500/fio/hadoop 等）
```

**依赖关系**：
```
powerfs-fuse → powerfs-profiler (可选，通过 feature flag)
powerfs-profiler → powerfs-common (仅基础类型)
```

---

## 三、接口设计

### 3.1 采集接口

```rust
// powerfs-profiler/src/trace.rs

/// 单条 I/O 事件（采集层记录）
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IoEvent {
    pub timestamp_ns: u64,
    pub op_type: IoOpType,
    pub inode: u64,
    pub offset: u64,
    pub size: u32,
    pub duration_us: u64,
    pub is_hit: bool,  // 缓存命中
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum IoOpType {
    Read, Write, Lookup, Readdir, Getattr, Create, Unlink, Mkdir, Rmdir, Rename, Open, Release,
}

/// I/O trace 收集器（采集层接口）
pub trait IoTraceCollector: Send + Sync {
    fn record(&self, event: IoEvent);
    fn flush(&self) -> Vec<IoEvent>;
    fn is_enabled(&self) -> bool;
}
```

### 3.2 hook 接口（FUSE 层接入点）

```rust
// powerfs-fuse/src/io_profile_hook.rs

/// FUSE 层的采集 hook，零侵入式接入
/// 通过 wrap 现有 FUSE 回调，记录 I/O 事件
pub struct IoProfileHook {
    collector: Arc<dyn IoTraceCollector>,
    enabled: Arc<std::sync::atomic::AtomicBool>,
}

impl IoProfileHook {
    pub fn new(collector: Arc<dyn IoTraceCollector>) -> Self { ... }

    /// 在 read/write/lookup 等回调前后调用
    pub fn record_op(&self, op: IoOpType, inode: u64, offset: u64, size: u32, duration: Duration, is_hit: bool) {
        if !self.enabled.load(Relaxed) { return; }
        self.collector.record(IoEvent { ... });
    }

    /// 启用/禁用采集
    pub fn set_enabled(&self, enabled: bool) { ... }
}
```

**接入方式**（零侵入，不修改 FUSE 回调主逻辑）：
- 在 `PowerFsFs` 结构体新增 `profile_hook: Option<IoProfileHook>` 字段
- read/write/lookup 等回调的入口/出口处调用 `profile_hook.record_op(...)`（Option 为 None 时零开销）

### 3.3 分析接口

```rust
// powerfs-profiler/src/analyzer.rs

/// 应用 I/O 指纹
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IoFingerprint {
    pub app_id: String,                    // 应用标识（如 "io500", "fio-seqwrite"）
    pub size_histogram: SizeHistogram,     // I/O 大小分布
    pub sequentiality_score: f64,          // 顺序性得分 0.0-1.0
    pub hotspot_concentration: f64,        // 热点集中度（Zipf α 参数）
    pub metadata_ratio: f64,               // 元数据操作占比 0.0-1.0
    pub read_write_ratio: f64,             // 读写比（>1 读为主）
    pub avg_concurrency: f64,              // 平均并发度
    pub phase_transitions: Vec<PhaseInfo>, // phase 切换信息
    pub access_stride: Option<u64>,        // stride 检测（随机/步长）
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SizeHistogram {
    pub buckets: Vec<(u32, u32)>,  // (size_threshold, count)，如 [(4096, 100), (65536, 50), ...]
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PhaseInfo {
    pub start_time_ns: u64,
    pub phase_type: PhaseType,
    pub duration_ms: u64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum PhaseType {
    SeqWrite, SeqRead, RandWrite, RandRead, Metadata, Mixed,
}

/// 指纹分析器
pub trait FingerprintAnalyzer: Send + Sync {
    /// 从 trace 生成指纹
    fn analyze(&self, trace: &[IoEvent]) -> IoFingerprint;
    /// 指纹相似度匹配（用于匹配策略库）
    fn similarity(&self, a: &IoFingerprint, b: &IoFingerprint) -> f64;
}
```

### 3.4 策略接口

```rust
// powerfs-profiler/src/strategy.rs

/// 优化参数集
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OptimizationProfile {
    pub prefetch_window: u64,           // 预取窗口（字节），0=禁用
    pub lease_duration_ms: u64,         // lease 时长
    pub chunk_cache_size_mb: u64,       // chunk 缓存大小
    pub inode_cache_ttl_s: u64,         // inode 缓存 TTL
    pub readdir_prefetch_depth: u32,    // 目录预加载深度
    pub writeback_flush_interval_ms: u64, // writeback flush 间隔
    pub read_ahead_aggressive: bool,    // 激进预读（顺序检测时翻倍窗口）
}

impl Default for OptimizationProfile {
    fn default() -> Self {
        // 默认参数（无优化）
        Self {
            prefetch_window: 2 * 1024 * 1024,  // 2MB
            lease_duration_ms: 30000,           // 30s
            chunk_cache_size_mb: 256,
            inode_cache_ttl_s: 2,
            readdir_prefetch_depth: 1,
            writeback_flush_interval_ms: 100,
            read_ahead_aggressive: false,
        }
    }
}

/// 策略匹配器
pub trait FingerprintMatcher: Send + Sync {
    /// 根据指纹匹配优化策略
    fn match_profile(&self, fp: &IoFingerprint) -> OptimizationProfile;
    /// 注册自定义策略（扩展点）
    fn register_strategy(&self, matcher: Box<dyn Fn(&IoFingerprint) -> Option<OptimizationProfile>>);
}
```

### 3.5 应用接口（FUSE 层参数调整）

```rust
// powerfs-fuse/src/runtime_config.rs

/// 运行时可调参数接口
/// powerfs-profiler 通过此接口调整 FUSE 运行时参数
pub trait RuntimeTunable: Send + Sync {
    fn set_prefetch_window(&self, window: u64);
    fn set_lease_duration(&self, ms: u64);
    fn set_chunk_cache_size(&self, mb: u64);
    fn set_inode_cache_ttl(&self, ttl: Duration);
    fn set_readdir_prefetch_depth(&self, depth: u32);
    fn set_writeback_flush_interval(&self, ms: u64);
    fn set_read_ahead_aggressive(&self, aggressive: bool);
    /// 批量应用优化参数
    fn apply_profile(&self, profile: &OptimizationProfile);
}
```

---

## 四、数据流与工作流

### 4.1 离线指纹模式（阶段1）

```
第一次运行（采集模式）:
  POWERFS_PROFILE=trace ./io500
    │
    ├─ FUSE 回调 → IoProfileHook.record_op()
    ├─ IoTraceCollector 累积事件
    └─ 退出时 flush → trace.json
         │
         ▼
  powerfs-profile-tool analyze trace.json
    ├─ FingerprintAnalyzer.analyze() → fingerprint.json
    └─ FingerprintMatcher.match_profile() → optimization.json

第二次运行（优化模式）:
  POWERFS_PROFILE=optimization.json ./powerfs-fuse
    │
    ├─ 启动时加载 optimization.json
    ├─ RuntimeTunable.apply_profile() 调整参数
    └─ 运行时使用优化后的参数
```

### 4.2 在线自适应模式（阶段2）

```
运行时:
  FUSE 回调 → IoProfileHook.record_op()
    │
    ├─ IoTraceCollector 滑动窗口累积（最近 N 秒）
    ├─ 定时分析（每 10s）→ 当前指纹
    ├─ FingerprintMatcher.match_profile() → 推荐策略
    ├─ 检测模式切换（相似度 < 阈值）→ 切换策略
    └─ RuntimeTunable.apply_profile() 动态调整
```

### 4.3 phase 切换检测

io500 等应用有明确 phase（write → find → read → stat），系统需识别切换：

```
检测条件：
  - I/O 大小分布突变（如 1M → 4K）
  - 读写比翻转（write-heavy → read-heavy）
  - 元数据比例突增（→ metadata phase）

切换动作：
  - SeqWrite phase → 长 lease + 大 writeback 间隔
  - SeqRead phase → 激进预读 + 大 prefetch 窗口
  - Metadata phase → 加深 readdir 预加载 + 延长 inode TTL
```

---

## 五、内置策略库

### 5.1 io500 策略

| 参数 | 默认值 | io500 优化值 | 理由 |
|------|--------|-------------|------|
| prefetch_window | 2MB | 4MB | 顺序读为主，大窗口减少 RPC |
| lease_duration_ms | 30000 | 120000 | 长 lease 减少续约 RPC |
| chunk_cache_size_mb | 256 | 512 | 大文件读写，需要更多缓存 |
| inode_cache_ttl_s | 2 | 10 | find/ls 阶段元数据密集 |
| readdir_prefetch_depth | 1 | 3 | 目录树预加载加速 find |
| writeback_flush_interval_ms | 100 | 50 | ior hard-write 需要快速持久化 |
| read_ahead_aggressive | false | true | 顺序读检测后翻倍窗口 |

### 5.2 fio 随机读写策略

| 参数 | 默认值 | fio-randrw 优化值 | 理由 |
|------|--------|-------------------|------|
| prefetch_window | 2MB | 0（禁用） | 随机读预取无效 |
| lease_duration_ms | 30000 | 10000 | 短 lease，频繁切换 |
| chunk_cache_size_mb | 256 | 512 | 随机访问需要大缓存 |
| read_ahead_aggressive | false | false | 禁用激进预读 |

### 5.3 小文件密集策略

| 参数 | 默认值 | 小文件优化值 | 理由 |
|------|--------|-------------|------|
| inode_cache_ttl_s | 2 | 30 | 元数据频繁访问 |
| readdir_prefetch_depth | 1 | 5 | 目录树深预加载 |
| chunk_cache_size_mb | 256 | 64 | 小文件数据小，减缓存让给元数据 |

---

## 六、与 PowerFS 架构的集成点

### 6.1 采集点（FUSE 回调 hook）

| FUSE 回调 | 采集字段 | 用途 |
|-----------|---------|------|
| read | inode/offset/size/duration/is_hit | 读模式分析 |
| write | inode/offset/size/duration | 写模式分析 |
| lookup | inode/duration | 元数据比例 |
| readdir | inode/duration/entry_count | 目录访问模式 |
| getattr | inode/duration | 元数据比例 |
| open/release | inode/duration | 文件生命周期 |

### 6.2 参数应用点

| 参数 | 应用位置 | 当前实现 |
|------|---------|---------|
| prefetch_window | [fuse.rs read 路径](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L1521) | 常量 `PREFETCH_CHUNKS=2`，改为可调 |
| lease_duration_ms | [fuse.rs PowerFsFs](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L162) | 字段 `lease_duration_ms`，已可调 |
| chunk_cache_size | [cache.rs ChunkCache](file:///home/portion/powerfs/powerfs-fuse/src/cache.rs#L1314) | `with_defaults()`，改为 `with_capacity()` |
| inode_cache_ttl | [cache.rs MetadataCache](file:///home/portion/powerfs/powerfs-fuse/src/cache.rs#L76) | 常量 `TTL=1s`，改为可调 |
| readdir_prefetch_depth | 新增 | 当前无预加载，需新增 |
| writeback_flush_interval | [fuse.rs flusher](file:///home/portion/powerfs/powerfs-fuse/src/fuse.rs#L173) | `sleep(100ms)`，改为可调 |

### 6.3 feature flag 控制

```toml
# powerfs-fuse/Cargo.toml
[features]
default = []
io-profiler = ["powerfs-profiler"]
```

```rust
// powerfs-fuse/src/lib.rs
#[cfg(feature = "io-profiler")]
pub mod io_profile_hook;
```

默认编译不含 profiler，启用 feature 后才接入采集 hook，零侵入。

---

## 七、实施阶段

### 阶段1：离线指纹 + 策略库（独立于强一致重构）

**前置条件**：强一致重构完成（参数应用点稳定）

**7.1.1 创建 powerfs-profiler crate**
- 定义 IoEvent / IoFingerprint / OptimizationProfile 数据结构
- 实现 FingerprintAnalyzer（trace → 指纹）
- 实现 FingerprintMatcher（指纹 → 策略）
- 内置 io500/fio-randrw/小文件 三个策略

**7.1.2 FUSE 层采集 hook**
- 新增 `io_profile_hook.rs`（feature flag 控制）
- 在 read/write/lookup/readdir/getattr 回调加 hook
- 采集模式：trace 写文件

**7.1.3 参数可调化**
- 将 `PREFETCH_CHUNKS`/`TTL`/`flush_interval` 等常量改为 `Arc<AtomicU64>` 可调
- 实现 `RuntimeTunable` trait
- 支持启动时加载 `optimization.json`

**7.1.4 profile-tool 命令行工具**
- `powerfs-profile-tool analyze trace.json` → 生成 fingerprint + optimization
- `powerfs-profile-tool list-profiles` → 列出内置策略
- `powerfs-profile-tool match fingerprint.json` → 匹配策略

**验证**：io500 跑两遍，第二遍对比第一遍性能提升

### 阶段2：在线自适应

**7.2.1 滑动窗口 trace 收集**
- IoTraceCollector 改为环形缓冲（最近 60s）
- 定时分析线程（每 10s 触发一次）

**7.2.2 phase 切换检测**
- 检测 I/O 模式突变（大小分布/读写比/元数据比例）
- 切换时动态 apply 新策略

**7.2.3 在线参数热调整**
- RuntimeTunable 支持运行时修改（不重启 FUSE）

**验证**：io500 单次运行中 phase 切换自动检测 + 参数调整

### 阶段3：ML 预测（研究探索）

**7.3.1 LSTM I/O 预测**
- 用 LSTM 预测下一个 I/O 请求的 offset/size
- 预测式预取（请求到达前预取）

**7.3.2 强化学习调度**
- RL agent 学习最优缓存替换策略
- 离线训练（模拟环境）+ 在线推理

**验证**：对比阶段1/2 的命中率提升

---

## 八、前沿研究方向对照

| 研究方向 | 代表工作 | PowerFS 适用性 | 阶段 |
|---------|---------|---------------|------|
| 应用指纹 | ISAAC (SC'17), Sherpa (FAST'21) | ✅ 阶段1 核心 | 阶段1 |
| 自适应预取 | C-Miner (FAST'04), SEER (FAST'18) | ✅ prefetch_window 动态调整 | 阶段1/2 |
| 学习型缓存 | Safari (FAST'21), ARC (USENIX'04) | ⚠️ ARC 可落地，Safari 研究风险 | 阶段2 |
| 学习型索引 | MINDS (SIGMOD'18), ALEX (SIGMOD'20) | ❌ Raft 元数据不适合替换 | 不采用 |
| RL 调度 | BatchRL (FAST'20), Capriccio (ATC'22) | ⚠️ 研究探索 | 阶段3 |
| I/O 延迟预测 | Parallax (FAST'21) | ⚠️ 研究探索 | 阶段3 |

---

## 九、配置与使用

### 9.1 配置文件

```toml
# powerfs.toml
[io_profiler]
enabled = false                    # 默认关闭
mode = "offline"                   # offline / online
trace_output = "/var/log/powerfs/trace.json"
optimization_file = "/etc/powerfs/optimization.json"
online_analysis_interval_s = 10    # 在线模式分析间隔
phase_change_threshold = 0.3       # phase 切换相似度阈值
```

### 9.2 环境变量（快捷控制）

```bash
# 采集模式
POWERFS_PROFILE=trace ./io500

# 优化模式
POWERFS_PROFILE=/etc/powerfs/optimization.json ./powerfs-fuse

# 在线自适应模式
POWERFS_PROFILE=online ./powerfs-fuse
```

### 9.3 命令行工具

```bash
# 分析 trace 生成指纹和优化策略
powerfs-profile-tool analyze trace.json --output optimization.json

# 列出内置策略
powerfs-profile-tool list-profiles

# 查看当前运行时参数
powerfs-profile-tool show-config --mount /mnt/powerfs

# 运行时调整参数（需在线模式）
powerfs-profile-tool tune --mount /mnt/powerfs --prefetch-window 4194304
```

---

## 十、与强一致重构的关系

- **独立模块**：powerfs-profiler 不依赖强一致重构，可独立开发
- **前置条件**：强一致重构完成后参数应用点才稳定（prefetch/lease/cache 接口稳定）
- **集成时机**：强一致重构 Step 7（测试验证）完成后，作为独立优化项接入
- **不冲突**：profiler 只调整参数，不修改一致性逻辑

---

## 十一、附录：代码位置索引（实施时参考）

| 模块 | 文件 | 说明 |
|------|------|------|
| 采集 hook | `powerfs-fuse/src/io_profile_hook.rs`（新增） | FUSE 回调 hook |
| 参数可调化 | `powerfs-fuse/src/fuse.rs` | PREFETCH_CHUNKS/TTL/flush_interval 改可调 |
| RuntimeTunable | `powerfs-fuse/src/runtime_config.rs`（新增） | 参数调整接口 |
| profiler crate | `powerfs-profiler/`（新增） | 分析 + 策略库 |
| profile-tool | `powerfs-profile-tool/`（新增） | 命令行工具 |
