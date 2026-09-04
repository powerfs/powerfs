# PowerFS 文件布局预测与智能放置策略设计

> 状态: **Phase 1 已实现并测试通过** — Phase 2 规划中
> 创建: 2026-09-05
> 更新: 2026-09-05
> 作者: PowerFS Team
> 分支: `feature/layout-prediction`

***

## 1. 问题背景

### 1.1 现有布局机制

当前 PowerFS 文件布局采用 **三维正交模型** (powerfs-layout/src/layout.rs):

```
FileLayout = Placement × Reliability × ChunkEncoding
```

**Placement 四态** (powerfs-layout/src/placement.rs):

* `Inline`: 数据存 Filer 元数据 (< 4KB)

* `Flat`: 单 Volume Server (< 64MB)

* `Stripe`: 4-16 Volume 并行 (< 100GB)

* `WideStripe`: 全集群并行 (>= 100GB)

**布局决策** (`auto_promote` 函数, placement.rs:238):

* **仅依据 file\_size** 分段选择, 无文件类型感知

* 阈值固定: 4KB → Inline, 64MB → Flat, 1GB → Stripe(4), 100GB → Stripe(16)

### 1.2 现有问题

| 问题                    | 影响                                                               | 现有代码位置                                            |
| --------------------- | ---------------------------------------------------------------- | ------------------------------------------------- |
| **Inline→Flat 运行时迁移** | 小文件增长超出 inline 阈值时, 需 Raft 协调迁移数据到 Volume Server, 产生额外 IO + 网络开销 | `net_handler.rs:3288 handle_migrate_inline_alloc` |
| **无文件类型感知**           | 文本文件和二进制文件同样大小可能选不同布局, 但当前无法区分                                   | `layout.rs:45 for_new_file`                       |
| **初始布局误判**            | 创建时空文件 → Inline, 后续写入大量数据 → 多次迁移                                 | `shard_store.rs:3057 storage_mode = Flat`         |
| **IO500 文件特征未利用**     | IO500 的 mdtest/ior 各有固定文件大小模式, 但系统无学习能力                          | `scripts/tests/perf/io500_test.sh`                |

### 1.3 迁移开销分析 (Inline→Flat)

```
当前路径:
  write_end (inline_data 满了)
    → sync_inodes_sb / writeback
      → filer detect inline overflow
        → handle_migrate_inline_alloc (Raft propose)
          → alloc volume + needle
          → copy inline_data → Volume Server
          → update inode storage_mode = Flat
          → clear inline_data
```

**开销**:

* 1 次 Raft propose (跨节点共识)

* 1 次 Volume Server needle 分配

* 1 次 inline\_data → Volume 网络传输

* inode 元数据更新 + Raft 日志

**如果能在创建时预测布局, 可完全避免此迁移.**

***

## 2. 设计目标

### 2.1 核心目标

1. **空文件状态**: 创建时为 `Empty` 状态, 不预分配任何布局
2. **内容感知布局**: 写入时根据内容特征决定 Inline/Flat/Stripe
3. **策略可配置**: 文件类型识别与放置策略独立, 支持自定义规则
4. **迁移最小化**: 预测准确率 > 90% 时, 运行时迁移次数 < 5%
5. **IO500 适配**: 针对 IO500 测试模式自动优化布局选择

### 2.2 非目标

* 不改变已有文件的布局 (仅影响新文件)

* 不引入同步阻塞 (布局预测异步进行)

* 不依赖外部 AI 服务 (模型内嵌或规则驱动)

***

## 3. 架构设计

### 3.1 新增 Empty 状态

```rust
// powerfs-layout/src/placement.rs

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum StorageMode {
    /// 新增: 空文件, 创建时未分配布局.
    /// 第一次 write 时由 LayoutPredictor 决定实际布局.
    #[default]
    Empty,
    Inline,
    Flat,
    Stripe,
    WideStripe,
    Ec,
}
```

**Empty 状态语义**:

* `content_size = 0`, `inline_data = None`, `chunks = []`, `fid = None`

* 读操作返回空数据 (0 bytes)

* 第一次 write 触发 `LayoutPredictor::predict()` 决定布局

* 预测后直接写入目标布局, 无需迁移

### 3.2 LayoutPredictor — 布局预测器

```rust
// powerfs-layout/src/predictor.rs (新增)

/// 布局预测输入
pub struct PredictContext {
    /// 文件名 (扩展名是强信号)
    pub filename: String,
    /// 父目录路径 (目录上下文是弱信号)
    pub parent_path: String,
    /// 父目录 placement xattr (显式策略)
    pub dir_placement: Option<PlacementSpec>,
    /// 父目录 inline 阈值
    pub dir_inline_threshold: Option<u32>,
    /// 创建标志 (O_CREAT, O_TRUNC, O_APPEND)
    pub create_flags: u32,
    /// 客户端类型 (FUSE kernel / FUSE userspace / S3)
    pub client_type: ClientType,
    /// 首次写入大小 (如果有, open 时已知)
    pub initial_write_size: Option<u64>,
}

/// 布局预测结果
pub enum PredictResult {
    /// 预测为 Inline (小文本/配置文件)
    Inline { max_size: u32 },
    /// 预测为 Flat (可执行文件/中等文件)
    Flat,
    /// 预测为 Stripe (数据文件/大文件)
    Stripe { stripe_count: u32, stripe_size: u64 },
    /// 无法预测, 回退到现有 auto_promote
    DeferToPolicy,
}

/// 布局预测器 trait (可插拔)
pub trait LayoutPredictor: Send + Sync {
    fn predict(&self, ctx: &PredictContext) -> PredictResult;
}
```

### 3.3 文件类型识别策略

#### 3.3.1 规则驱动 (Phase 1)

```rust
// powerfs-layout/src/predictor.rs

pub struct RuleBasedPredictor {
    rules: Vec<LayoutRule>,
}

pub struct LayoutRule {
    /// 规则名
    pub name: String,
    /// 匹配条件
    pub matcher: RuleMatcher,
    /// 预测布局
    pub placement: PredictResult,
    /// 优先级 (高优先级先匹配)
    pub priority: u32,
}

pub enum RuleMatcher {
    /// 扩展名匹配
    Extension { exts: Vec<String> },
    /// 文件名 glob
    FilenameGlob { pattern: String },
    /// 路径前缀
    PathPrefix { prefix: String },
    /// 父目录名
    ParentDir { names: Vec<String> },
    /// 大小区间
    SizeRange { min: u64, max: u64 },
    /// 客户端类型
    ClientType { ctype: ClientType },
    /// 组合条件 (AND)
    All { matchers: Vec<RuleMatcher> },
    /// 组合条件 (OR)
    Any { matchers: Vec<RuleMatcher> },
}
```

#### 3.3.2 默认规则集

| 文件类型         | 匹配规则                                     | 预测布局       | 理由         |
| ------------ | ---------------------------------------- | ---------- | ---------- |
| 配置文件         | `.conf .ini .cfg .toml .yaml .json .xml` | Inline     | 通常 < 4KB   |
| 脚本           | `.sh .py .rb .pl .lua`                   | Inline     | 通常 < 4KB   |
| 日志文件         | `.log`                                   | Flat       | 增长型, 预分配   |
| 可执行文件        | `.so .o .a .exe .bin`                    | Flat       | 中等大小, 单卷足够 |
| 压缩包          | `.tar .gz .zip .bz2 .xz .7z`             | Stripe(4)  | 通常较大, 并行读写 |
| 数据库          | `.db .sqlite .mdb`                       | Flat       | 随机读写, 单卷   |
| 媒体文件         | `.mp4 .mkv .avi .mov .mp3 .flac`         | Stripe(4)  | 大文件顺序读     |
| 深度学习模型       | `.pt .pth .ckpt .safetensors .bin`(ML目录) | Stripe(16) | 超大文件       |
| 临时文件         | `/tmp/*`                                 | Inline     | 通常小且短命     |
| IO500 mdtest | `mdtest.*`                               | Inline     | 固定 3901B   |
| IO500 ior    | `ior.*`                                  | Stripe(4)  | 固定大文件      |

#### 3.3.3 学习驱动 (Phase 2, 后续)

```rust
/// 基于历史统计的预测器
pub struct StatsBasedPredictor {
    /// 文件名模式 → 历史平均大小
    pattern_stats: DashMap<String, SizeStats>,
    /// 父目录 → 历史平均大小
    dir_stats: DashMap<String, SizeStats>,
}

pub struct SizeStats {
    pub count: u64,
    pub mean: f64,
    pub median: u64,
    pub p90: u64,
    pub p99: u64,
    pub placement_history: HashMap<StorageMode, u64>,
}
```

**学习机制**:

1. 每次 close 时记录 `(filename_pattern, parent_dir, final_size, final_placement)` 到统计
2. 下次创建同模式文件时, 用历史 P90 大小预测布局
3. 定期 (每小时) 持久化统计到 Filer 元数据

#### 3.3.4 深度学习 (Phase 3, 远期)

```
特征:
  - 文件名 (字符级 embedding)
  - 扩展名 (one-hot)
  - 父目录路径 (路径 embedding)
  - 创建时间 (周期特征)
  - 客户端类型 (embedding)
  - 历史同目录文件大小分布 (统计特征)

模型: LightGBM / 小型 MLP (推理 < 0.1ms)
训练: 离线训练 on Filer 统计数据, 模型 < 1MB
部署: 嵌入 Filer 进程, 不依赖外部服务
```

### 3.4 策略独立性设计

```rust
// powerfs-layout/src/policy.rs (扩展)

pub struct LayoutPolicy {
    /// 现有 auto_promote 阈值
    pub placement: PlacementPolicy,

    /// 布局预测器 (可插拔)
    pub predictor: Box<dyn LayoutPredictor>,

    /// 预测失败时的回退策略
    pub fallback: FallbackStrategy,

    /// 是否启用学习 (Phase 2)
    pub enable_learning: bool,

    /// 预测置信度阈值 (低于此值不预测, 回退 auto_promote)
    pub min_confidence: f32,
}

pub enum FallbackStrategy {
    /// 回退到现有 auto_promote (基于大小)
    AutoPromote,
    /// 默认 Flat (安全选择)
    Flat,
    /// 默认 Inline (保守选择, 允许后续迁移)
    Inline,
}
```

### 3.5 配置文件格式

```toml
# powerfs.toml

[layout]
# 启用布局预测
enable_prediction = true
# 预测失败回退策略: "auto_promote" | "flat" | "inline"
fallback = "auto_promote"
# 最小置信度 (0.0-1.0)
min_confidence = 0.6
# 启用统计学习
enable_learning = true

[layout.rules]
# 自定义规则 (按优先级排序)
[[layout.rules]]
name = "ml_checkpoints"
priority = 100
matcher = { extension = [".pt", ".pth", ".ckpt", ".safetensors"] }
placement = { type = "stripe", count = 16, size = 67108864 }

[[layout.rules]]
name = "config_files"
priority = 50
matcher = { extension = [".conf", ".ini", ".toml", ".yaml"] }
placement = { type = "inline", max_size = 4096 }

[[layout.rules]]
name = "io500_mdtest"
priority = 200
matcher = { filename_glob = "mdtest.*" }
placement = { type = "inline", max_size = 4096 }

[[layout.rules]]
name = "io500_ior"
priority = 200
matcher = { filename_glob = "ior*" }
placement = { type = "stripe", count = 4, size = 67108864 }
```

***

## 4. IO500 文件特征分析与测试方案

### 4.1 IO500 测试模式

| 测试阶段             | 文件特征         | 大小     | 当前布局        | 优化布局             |
| ---------------- | ------------ | ------ | ----------- | ---------------- |
| mdtest-hard      | 大量小文件        | 3901B  | Inline (正确) | Inline (预测命中)    |
| mdtest-easy      | 创建大量空文件      | 0B     | Inline→后续迁移 | Empty→按上下文预测     |
| ior-hard-write   | 4K block 随机写 | \~ TBD | Flat/Stripe | Stripe(4) (预测命中) |
| ior-easy-write   | 大文件顺序写       | \~ TBD | Stripe(4)   | Stripe(4) (预测命中) |
| find             | 遍历元数据        | -      | -           | -                |
| ior-easy-read    | 读回           | -      | -           | -                |
| ior-hard-read    | 读回           | -      | -           | -                |
| ior-easy-reread  | 再读           | -      | -           | -                |
| ior-hard-reread  | 再读           | -      | -           | -                |
| mdtest-hard-stat | stat         | -      | -           | -                |
| mdtest-easy-stat | stat         | -      | -           | -                |
| delete-easy      | 删除           | -      | -           | -                |
| delete-hard      | 删除           | -      | -           | -                |

### 4.2 IO500 布局判定策略

```
IO500 文件名模式:
  - mdtest-*          → Inline (3901B 固定)
  - ior*              → Stripe(4) (大文件)
  - stonewfile*       → 按大小判断 (通常大文件)
  - .scrub.*          → Inline (元数据)

判定逻辑:
  1. 文件名匹配 IO500 已知模式 → 直接预测
  2. 路径包含 /io500/ → 启用 IO500 模式
  3. 父目录有 io500.config → 读取配置中的文件大小列表
```

### 4.3 IO500 详细测试方案

#### 4.3.1 测试目标

1. **验证布局预测对 IO500 性能的影响**: 对比开启/关闭布局预测的 IO500 得分
2. **验证迁移次数降低**: 统计 Inline→Flat 迁移次数的变化
3. **验证元数据操作性能**: mdtest 的 create/stat/delete 性能
4. **验证数据吞吐量**: ior 的读写带宽

#### 4.3.2 测试环境

```
# 单节点 Docker 环境 (功能验证)
cd /home/portion/powerfs/docker
docker compose -f docker-compose-single.yml up -d

# 多节点集群环境 (性能验证, 需 3 master + 3 filer + 2+ volume)
cd /home/portion/powerfs/docker
docker compose -f docker-compose.yml up -d
```

#### 4.3.3 测试配置

```toml
# filer.toml — IO500 优化配置
[filer.layout]
enable_prediction = true
min_confidence = 0.6
fallback = "auto_promote"

# IO500 专用规则 (优先级 200, 高于通用规则)
[[filer.layout.rules]]
name = "io500_mdtest"
priority = 200
matcher = "glob"
values = "mdtest*"
placement = "inline"
max_size = 4096
confidence = 0.95

[[filer.layout.rules]]
name = "io500_ior"
priority = 200
matcher = "glob"
values = "ior*"
placement = "stripe"
stripe_count = 4
stripe_size = 67108864
confidence = 0.95

[[filer.layout.rules]]
name = "io500_stonewall"
priority = 200
matcher = "glob"
values = "stonewfile*"
placement = "stripe"
stripe_count = 4
stripe_size = 67108864
confidence = 0.85
```

#### 4.3.4 测试步骤

```bash
# Step 1: 准备 IO500 测试环境
# 参考 scripts/tests/perf/io500_test.sh

# Step 2: 基线测试 (关闭布局预测)
# 修改 filer.toml: enable_prediction = false
# 重启 filer, 运行 IO500
docker exec fuse-1 /app/io500 /mnt/powerfs --config /etc/io500/config.ini

# Step 3: 优化测试 (开启布局预测)
# 修改 filer.toml: enable_prediction = true
# 重启 filer, 运行 IO500 (同配置)
docker exec fuse-1 /app/io500 /mnt/powerfs --config /etc/io500/config.ini

# Step 4: 收集结果
# - IO500 得分 (mdtest + ior 综合分)
# - Filer 日志中的 LAYOUT_PREDICT 统计
# - 迁移次数 (grep "migrate_inline" filer logs)
# - fio 辅助验证 (4K/1M block, 顺序/随机读写)
```

#### 4.3.5 预期结果

| 指标 | 基线 (无预测) | 优化 (有预测) | 提升预期 |
|------|------------|------------|--------|
| IO500 总分 | `<<TBD_BASELINE>>` | `<<TBD_OPTIMIZED>>` | `<<TBD_IMPROVEMENT>>` |
| mdtest create (ops/s) | `<<TBD>>` | `<<TBD>>` | ≥5% (减少迁移) |
| ior-easy write (MB/s) | `<<TBD>>` | `<<TBD>>` | ≥0 (预测命中, 无额外开销) |
| Inline→Flat 迁移次数 | `<<TBD>>` | `<<TBD>>` | -90%+ |
| LAYOUT_PREDICT 命中率 | N/A | `<<TBD>>`% | ≥90% |

#### 4.3.6 fio 辅助验证

```bash
# 验证不同文件类型的布局正确性
# 配置文件 (Inline): 4K 顺序写
fio --name=config_test --filename=/mnt/powerfs/fio_test.conf \
    --ioengine=libaio --rw=write --bs=4k --size=4k --direct=1

# 可执行文件 (Flat): 1M 顺序写
fio --name=exec_test --filename=/mnt/powerfs/fio_test.so \
    --ioengine=libaio --rw=write --bs=1m --size=64m --direct=1

# 数据文件 (Stripe): 1M 顺序写, 大文件
fio --name=data_test --filename=/mnt/powerfs/fio_test.pt \
    --ioengine=libaio --rw=write --bs=1m --size=1g --direct=1

# 4K 随机写 (验证无迁移)
fio --name=randwrite --filename=/mnt/powerfs/fio_randwrite.bin \
    --ioengine=libaio --rw=randwrite --bs=4k --size=16m --direct=1 \
    --io_size=4g --runtime=60 --time_based
```

### 4.4 通用文件大小初始确定策略

```
布局决策流程 (写入时):

1. 显式 xattr 覆盖?
   - powerfs.placement xattr → 直接使用 (最高优先级)
   - powerfs.inline xattr → Inline/禁用

2. 预测器命中?
   - LayoutPredictor::predict() → 高置信度 → 直接使用
   - 低置信度 (< min_confidence) → 进入步骤 3

3. 统计学习命中?
   - 同模式历史 P90 大小 → auto_promote(P90)
   - 无历史数据 → 进入步骤 4

4. 回退策略
   - auto_promote(initial_write_size) (现有逻辑)
   - 或 Flat (安全默认)

5. Empty → 实际布局 转换
   - 第一次 write 时执行
   - 预测结果写入 inode storage_mode
   - 后续 write 直接按布局写入
```

***

## 5. 实施计划

### 5.1 分阶段实施

| 阶段      | 内容                          | 复杂度 | 依赖           | 状态 |
| ------- | --------------------------- | --- | ------------ | --- |
| Phase 1 | Empty 状态 + 规则预测器 + IO500 规则 | 中   | 无            | ✅ 已完成 |
| Phase 2 | 统计学习 + 历史模式匹配               | 中高  | Phase 1      | 规划中 |
| Phase 3 | 深度学习模型嵌入                    | 高   | Phase 2 数据积累 | 远期 |

### 5.2 Phase 1 详细任务 (已完成)

1. ✅ **新增 `StorageMode::Empty`**
   - placement.rs: 添加 Empty 变体 + is_empty() 方法
   - serde 默认改为 Empty (新文件)
   - 向后兼容: 旧 inode 反序列化为 Inline (现有行为)

2. ✅ **新增 `predictor.rs` 模块**
   - `LayoutPredictor` trait + `PredictContext` + `PredictResult`
   - `RuleBasedPredictor` 实现 + `from_config()` 构造
   - 默认规则集 (扩展名 + IO500 模式, 14 条规则)
   - 14 个单元测试

3. ✅ **修改 `for_new_file` / 新增 `for_empty_file`**
   - `for_empty_file()`: 创建空布局
   - `resolve_with_predictor()`: 预测器解析布局 (含置信度阈值)
   - `resolve_on_first_write()`: auto_promote 回退

4. ✅ **修改 filer 写入路径**
   - `meta_shard_manager.rs`: `create_file` + `create_file_with_shard` 调用预测器
   - `predict_storage_mode()` 方法
   - `net_handler.rs`: Empty → Inline 空数据 (wire protocol 兼容)
   - `shard_store.rs`: `create_file` → StorageMode::Empty

5. ✅ **配置文件支持**
   - `powerfs-common/src/config.rs`: `LayoutConfig` + `LayoutRuleConfig`
   - 手动 `Default` 实现 (enable_prediction=true, min_confidence=0.6)
   - TOML 格式规则定义
   - `TryFrom<LayoutRuleConfig> for LayoutRule` 转换

6. ✅ **IO500 规则集**
   - `io500_mdtest`: mdtest* → Inline (confidence=0.95)
   - `io500_ior`: ior* → Stripe(4) (confidence=0.95)
   - `io500_stonewall`: stonewfile* → Stripe(4) (confidence=0.85)

7. ✅ **测试**
   - 单元测试: 122 layout + 141 filer 测试全通过
   - VM 测试: 56 PASS, 0 FAIL, 0 WARN (test_layout_prediction.sh)
   - Clippy: 0 警告

### 5.3 分支策略

```
分支: feature/layout-prediction
基于: master (commit f598b2e3)

已完成提交:
  1. feat(layout): add StorageMode::Empty + predictor trait  ✅
  2. feat(layout): implement RuleBasedPredictor with default rules  ✅
  3. feat(filer): integrate predictor into write path  ✅
  4. feat(config): add layout prediction configuration  ✅
  5. feat(layout): add IO500-specific prediction rules  ✅
  6. test(layout): unit + VM tests for prediction accuracy  ✅
  7. docs: update design doc with implementation results  ✅ (本文档)
```

### 5.4 Phase 1 实测数据

#### VM 测试结果 (2026-09-05)

```
测试脚本: kernel/vm/test_layout_prediction.sh
环境: Docker single-node (master-1 + filer-1 + volume-server-1 + fuse-1)

结果: 56 PASS, 0 FAIL, 0 WARN

Filer LAYOUT_PREDICT 日志验证:
  - app.conf        → config_files   → Inline  (confidence=0.80) ✅
  - libpowerfs.so   → executables    → Flat    (confidence=0.70) ✅
  - model.pt        → ml_checkpoints → Stripe  (confidence=0.90) ✅
  - video.mp4       → video_files    → Stripe  (confidence=0.85) ✅
  - backup.tar.gz   → archives       → Stripe  (confidence=0.75) ✅
  - app.log         → logs           → Flat    (confidence=0.60) ✅
  - deploy.sh       → scripts        → Inline  (confidence=0.75) ✅
  - mdtest_hard.001 → io500_mdtest   → Inline  (confidence=0.95) ✅
  - ior_easy_write → io500_ior      → Stripe  (confidence=0.95) ✅
  - unknown_file    → defer          → Empty   (auto_promote) ✅

Inline→Flat 迁移次数: 0 (所有预测命中文件均无迁移)
```

***

## 6. 专利申请规划

### 6.1 专利核心创新点

1. **空文件状态延迟布局分配**
   - 创建时不确定布局, 第一次写入时根据内容决定
   - 避免初始布局误判导致的运行时迁移

2. **多层级文件布局预测**
   - 规则驱动 (扩展名/路径/模式) → 统计学习 → 深度学习
   - 三级递进, 准确率逐步提升

3. **IO500 测试模式自适应**
   - 根据 IO500 文件名模式自动选择最优布局
   - 可推广到其他基准测试 (fio, IOR)

4. **布局策略可插拔架构**
   - 预测器 trait 接口, 支持自定义实现
   - 配置文件定义规则, 无需修改代码

### 6.2 专利申请文档结构

```
1. 技术领域
   分布式文件系统 / 数据存储布局

2. 背景技术
   现有分布式文件系统的布局选择方法 (基于大小阈值)
   问题: 初始布局误判 → 运行时迁移开销

3. 发明内容
   3.1 空文件延迟布局分配方法
   3.2 基于文件类型特征的布局预测器
   3.3 多级递进式预测策略
   3.4 基准测试模式自适应

4. 附图说明
   图1: 延迟布局分配流程
   图2: 三级预测架构
   图3: IO500 自适应流程
   图4: 布局迁移次数对比

5. 具体实施方式
   实施例1: 规则驱动预测
   实施例2: 统计学习预测
   实施例3: 深度学习预测
   实施例4: IO500 自适应

6. 权利要求
   1. 一种分布式文件系统中文件布局延迟分配方法...
   2. 根据权利要求1, 其特征在于...
   ...
```

### 6.3 专利数据占位符

> 以下数据由专利准备人员填充。`<<PATENT_*>>` 为占位符, 标记需要补充的实验数据或法律文本。

#### 6.3.1 技术效果数据

| 数据项 | 占位符 | 说明 | 来源 |
|--------|------|------|------|
| 布局预测准确率 | `<<PATENT_ACCURACY>>` | Phase 1 VM 测试: 10/10 = 100% (小样本) | §5.4 |
| Inline→Flat 迁移减少率 | `<<PATENT_MIGRATION_REDUCTION>>` | Phase 1 VM 测试: 0 次 (对比基线 TBD) | §5.4 |
| IO500 总分提升 | `<<PATENT_IO500_SCORE>>` | 基线 vs 优化 (待 IO500 完整测试) | §4.3.5 |
| mdtest create 性能提升 | `<<PATENT_MDTEST_CREATE>>` | ops/s 对比 (待 IO500 完整测试) | §4.3.5 |
| ior-easy 写带宽 | `<<PATENT_IOR_EASY_WRITE>>` | MB/s 对比 (待 IO500 完整测试) | §4.3.5 |
| 预测器推理延迟 | `<<PATENT_PREDICT_LATENCY>>` | μs 级, 规则匹配 < 1μs (待 fio 精确测量) | TBD |
| 规则集覆盖率 | `<<PATENT_RULE_COVERAGE>>` | 常见文件类型覆盖率 (待统计) | TBD |

#### 6.3.2 对比实验数据

| 对比维度 | 现有技术 (auto_promote) | 本发明 (布局预测) | 占位符 |
|---------|---------------------|----------------|------|
| 初始布局正确率 | `<<PATENT_BASE_CORRECT>>`% | `<<PATENT_OUR_CORRECT>>`% | 基于文件大小阈值 |
| 运行时迁移次数 | `<<PATENT_BASE_MIGRATE>>` | `<<PATENT_OUR_MIGRATE>>` | Inline→Flat 迁移 |
| 元数据操作延迟 | `<<PATENT_BASE_META_LAT>>`ms | `<<PATENT_OUR_META_LAT>>`ms | mdtest create/stat |
| 数据吞吐量 | `<<PATENT_BASE_THROUGHPUT>>`MB/s | `<<PATENT_OUR_THROUGHPUT>>`MB/s | ior-easy write |
| IO500 综合分 | `<<PATENT_BASE_IO500>>` | `<<PATENT_OUR_IO500>>` | IO500 官方得分 |

#### 6.3.3 实施例数据

**实施例1: 规则驱动预测 (Phase 1, 已验证)**

```
测试环境: Docker single-node (1 master + 1 filer + 1 volume + 1 fuse)
测试样本: 10 种文件类型 (conf, so, pt, mp4, tar.gz, log, sh, mdtest, ior, unknown)
预测准确率: 10/10 = 100%
迁移次数: 0
推理延迟: <<PATENT_EXAMPLE1_LATENCY>>μs (待精确测量)
```

**实施例2: IO500 自适应 (待测试)**

```
测试环境: <<PATENT_EXAMPLE2_ENV>> (待确定: 单节点 or 集群)
测试配置: §4.3.3
IO500 基线总分: <<PATENT_EXAMPLE2_BASE_SCORE>>
IO500 优化总分: <<PATENT_EXAMPLE2_OPT_SCORE>>
提升幅度: <<PATENT_EXAMPLE2_IMPROVEMENT>>%
```

**实施例3: 统计学习预测 (Phase 2, 远期)**

```
训练数据: <<PATENT_EXAMPLE3_TRAIN_DATA>> 条文件记录
模型准确率: <<PATENT_EXAMPLE3_ACCURACY>>%
推理延迟: <<PATENT_EXAMPLE3_LATENCY>>μs
```

#### 6.3.4 附图数据

| 附图 | 内容 | 占位符 |
|------|------|--------|
| 图1 | 延迟布局分配流程图 | `<<PATENT_FIG1>>` (待绘制) |
| 图2 | 三级预测架构图 | `<<PATENT_FIG2>>` (待绘制) |
| 图3 | IO500 自适应流程图 | `<<PATENT_FIG3>>` (待绘制) |
| 图4 | 布局迁移次数对比柱状图 | `<<PATENT_FIG4_DATA>>` (待 IO500 测试) |

### 6.4 时间线

| 阶段   | 内容         | 时间            | 状态 |
| ---- | ---------- | ------------- | --- |
| 技术方案 | 本设计文档      | 已完成           | ✅ |
| Phase 1 实现 | Empty + 规则预测器 | 已完成 | ✅ |
| Phase 1 测试 | 单元 + VM 测试 | 已完成 | ✅ |
| IO500 完整测试 | 基线 vs 优化对比 | 待执行 | ⏳ |
| 专利草案 | 权利要求 + 实施例 | 待准备 (由专利人员) | ⏳ |
| 专利申请 | 正式提交       | IO500 测试后 | ⏳ |
| Phase 2 实现 | 统计学习 | 待规划 | 🔜 |
| 实施验证 | 代码 + 测试数据  | Phase 2 后 | 🔜 |

***

## 7. 风险与缓解

| 风险          | 影响         | 缓解措施                                        |
| ----------- | ---------- | ------------------------------------------- |
| 预测准确率低      | 仍需迁移       | 设置 min\_confidence 阈值, 低置信度回退 auto\_promote |
| Empty 状态兼容性 | 旧客户端不认识    | serde default = Empty, 旧 inode 反序列化为 Inline |
| 规则配置错误      | 布局不优       | 提供 reset 命令恢复默认规则                           |
| 学习数据不足      | Phase 2 无效 | Phase 1 规则兜底, 学习是增量优化                       |
| 深度学习推理延迟    | 写入变慢       | 模型 < 1MB, 推理 < 0.1ms, 异步预测                  |

***

## 8. 讨论要点 (待确认)

1. **Empty 状态是否影响现有 close 同步逻辑?**

   * close 时 Empty 状态文件 (0 bytes) 是否需要 sync?

   * 建议: Empty + 0 bytes → 不需要 sync, 直接保留 Empty

2. **预测器在 Filer 还是 FUSE 侧执行?**

   * Filer 侧: 集中预测, 可共享统计数据, 但增加 Filer 负载

   * FUSE 侧: 分布式预测, 低延迟, 但统计数据分散

   * 建议: Phase 1 在 Filer 侧, Phase 2 统计在 Filer + 预测缓存下发 FUSE

3. **是否需要支持运行时布局降级?**

   * 例如: 文件从 Stripe 截断到 1KB, 是否降级到 Inline?

   * 建议: 不降级, 避免反向迁移开销

4. **深度学习模型的训练数据来源?**

   * Filer 统计日志 → 离线训练

   * 需要标注? 不需要, 无监督聚类 + 历史大小回归

5. **专利申请范围?**

   * 中国专利? 国际 PCT?

   * 建议: 先中国发明专利, 视效果决定 PCT

***

## 9. 参考文件

| 文件 | 说明 |
|------|------|
| [layout.rs](file:///home/portion/powerfs/powerfs-layout/src/layout.rs) | FileLayout 三维正交定义 + for_empty_file + resolve_with_predictor |
| [placement.rs](file:///home/portion/powerfs/powerfs-layout/src/placement.rs) | Placement 四态 + StorageMode::Empty + auto_promote |
| [predictor.rs](file:///home/portion/powerfs/powerfs-layout/src/predictor.rs) | LayoutPredictor trait + RuleBasedPredictor + 默认规则集 |
| [policy.rs](file:///home/portion/powerfs/powerfs-layout/src/policy.rs) | PlacementPolicy 阈值配置 |
| [config.rs](file:///home/portion/powerfs/powerfs-common/src/config.rs) | LayoutConfig + LayoutRuleConfig 配置定义 |
| [meta_shard_manager.rs](file:///home/portion/powerfs/powerfs-filer/src/meta_shard_manager.rs) | create_file + create_file_with_shard + predict_storage_mode |
| [net_handler.rs](file:///home/portion/powerfs/powerfs-filer/src/net_handler.rs) | encode_chunks_fields (Empty→Inline wire protocol) |
| [shard_store.rs](file:///home/portion/powerfs/powerfs-filer/src/shard_store.rs) | create_file → StorageMode::Empty |
| [main.rs](file:///home/portion/powerfs/powerfs-filer/src/main.rs) | Filer 启动时初始化 LayoutPredictor |
| [test_layout_prediction.sh](file:///home/portion/powerfs/kernel/vm/test_layout_prediction.sh) | VM 测试脚本 (56 PASS) |
| [io500_test.sh](file:///home/portion/powerfs/scripts/tests/perf/io500_test.sh) | IO500 测试脚本 |

***

## 10. 下一步

- [x] 用户确认方案
- [x] 创建 `feature/layout-prediction` 分支
- [x] Phase 1 实施 (Empty + RuleBasedPredictor + IO500 规则)
- [x] Phase 1 测试 (单元 + VM)
- [x] 更新设计文档 (本文档)
- [ ] IO500 完整测试 (基线 vs 优化对比, 填充 `<<PATENT_*>>` 占位符)
- [ ] fio 辅助验证 (各种文件类型布局正确性 + 迁移次数对比)
- [ ] 专利草案撰写 (由专利人员, 使用本文档 §6 数据)
- [ ] Phase 2 规划 (统计学习)

