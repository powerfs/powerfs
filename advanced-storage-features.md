# PowerFS 高级存储功能实现方案

## 概述

本文档详细分析以下五个高级存储功能的实现方案：

1. **细纠删码（EC Erasure Coding）** - 数据冗余保护
2. **WORM 锁定（Write Once Read Many）** - 数据不可篡改
3. **回收站（Recycle Bin）** - 误删除恢复
4. **自动数据修复（Auto Repair）** - 数据一致性保障
5. **Bitrot 数据校验（Checksum Verification）** - 静默数据损坏检测

---

## 功能交互关系图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          高级存储功能交互关系                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐                │
│  │   Bitrot     │────▶│  Auto Repair │────▶│    EC/       │                │
│  │   检测       │     │   自动修复   │     │   Replica    │                │
│  │              │     │              │     │   重建       │                │
│  └──────────────┘     └──────────────┘     └──────────────┘                │
│        │                    │                                              │
│        │                    │                                              │
│        ▼                    ▼                                              │
│  ┌──────────────┐     ┌──────────────┐                                      │
│  │   WORM       │     │  Recycle Bin │                                      │
│  │   锁定       │     │   回收站     │                                      │
│  │              │     │              │                                      │
│  └──────────────┘     └──────────────┘                                      │
│        │                    │                                              │
│        │                    │                                              │
│        └────────────┬───────┘                                              │
│                     ▼                                                      │
│           ┌──────────────┐                                                 │
│           │  Needle/     │                                                 │
│           │  Volume      │                                                 │
│           │  元数据层    │                                                 │
│           └──────────────┘                                                 │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

交互说明：
• Bitrot检测 → 发现数据损坏 → 触发Auto Repair
• Auto Repair → 使用EC/副本信息 → 重建损坏数据
• WORM锁定 → 阻止删除操作 → 同时阻止进入回收站
• 回收站 → 软删除对象 → 过期后物理删除
```

---

## 1. 细纠删码（EC Erasure Coding）

### 1.1 设计原则

| 维度 | 方案 | 说明 |
|------|------|------|
| **EC粒度** | Needle级 | 每个Needle独立编码，避免Volume级编码的碎片问题 |
| **编解码参数** | Reed-Solomon (k=4, m=2) | 4份数据 + 2份校验，空间利用率66.7% |
| **策略互斥** | Collection级可选 | EC与副本机制互斥，同一Collection只能选一种 |
| **编码引擎** | reed-solomon-erasure crate | Rust成熟EC库，支持SIMD加速 |

### 1.2 数据结构变更

**VolumeInfo 扩展**：
```rust
pub struct VolumeInfo {
    // 已有字段...
    redundancy_type: RedundancyType,  // 新增：副本/EC
    ec_k: u32,                        // 新增：数据分片数
    ec_m: u32,                        // 新增：校验分片数
}

pub enum RedundancyType {
    Replica(u32),       // 副本数
    EC { k: u32, m: u32 }, // EC参数
}
```

**NeedleInfo 扩展**：
```rust
pub struct NeedleInfo {
    // 已有字段...
    ec_shards: Vec<EcShardInfo>,  // 新增：EC分片信息
}

pub struct EcShardInfo {
    shard_index: u32,           // 分片索引 (0..k+m-1)
    volume_id: VolumeId,        // 存储的Volume
    needle_id: NeedleId,        // 对应Needle ID
    is_parity: bool,            // 是否校验分片
}
```

### 1.3 核心流程

**写入流程（EC模式）**：
```
客户端写入数据
    │
    ▼
Master分配k个不同Volume
    │
    ▼
数据编码为k+m个分片
    │
    ├─▶ 分片0 → Volume-0
    ├─▶ 分片1 → Volume-1
    ├─▶ 分片2 → Volume-2
    ├─▶ 分片3 → Volume-3
    ├─▶ 校验0 → Volume-4
    └─▶ 校验1 → Volume-5
    │
    ▼
记录EC元数据到DirectoryTree
```

**读取流程（EC模式）**：
```
客户端读取数据
    │
    ▼
从任意k个分片读取
    │
    ├─▶ 分片0 + 分片1 + 分片2 + 分片3 → 直接解码
    ├─▶ 分片0 + 分片1 + 校验0 + 校验1 → 重建分片2/3后解码
    └─▶ 任意k个分片组合
    │
    ▼
返回完整数据
```

### 1.4 与副本机制的共存策略

| Collection配置 | 冗余策略 | 适用场景 |
|---------------|---------|---------|
| `redundancy=replica, copies=3` | 3副本 | 高性能、低延迟场景 |
| `redundancy=ec, k=4, m=2` | 4+2 EC | 大容量、低成本场景 |
| `redundancy=ec, k=8, m=3` | 8+3 EC | 超大规模存储场景 |

### 1.5 性能影响评估

| 指标 | 副本模式 | EC模式(k=4,m=2) | 差异 |
|------|---------|----------------|------|
| 写入带宽 | 100% | ~66.7% | 降低33% |
| 读取带宽 | 100% | ~100% | 持平 |
| 空间利用率 | 33.3% | 66.7% | 提升2倍 |
| 编码延迟 | 0 | ~10-20ms/MB | 额外开销 |
| 解码延迟 | 0 | ~5-10ms/MB | 额外开销 |

---

## 2. WORM 锁定（Write Once Read Many）

### 2.1 设计原则

| 维度 | 方案 | 说明 |
|------|------|------|
| **锁定粒度** | Needle级 | 支持单个对象的WORM锁定 |
| **锁定类型** | 时间锁定 | 指定保留期限，到期自动解锁 |
| **锁定层级** | Bucket级 + Object级 | Bucket级默认策略，Object级可覆盖 |
| **解锁方式** | 到期自动解锁 | 不支持手动提前解锁（合规要求） |

### 2.2 数据结构变更

**NeedleInfo 扩展**：
```rust
pub struct NeedleInfo {
    // 已有字段...
    worm_retention_until: Option<DateTime<Utc>>,  // 新增：WORM保留到期时间
}
```

**Entry 扩展（S3层）**：
```rust
pub struct Entry {
    // 已有字段...
    worm_config: Option<WormConfig>,  // 新增：WORM配置
}

pub struct WormConfig {
    enabled: bool,
    retention_period_days: u32,
    retention_until: DateTime<Utc>,
    mode: WormMode,
}

pub enum WormMode {
    Compliance,    // 合规模式：不可提前删除
    Governance,    // 管理模式：特权用户可删除
}
```

### 2.3 核心流程

**写入时锁定**：
```
客户端写入对象
    │
    ▼
检查Bucket WORM策略
    │
    ├─▶ Bucket启用WORM → 自动应用WORM锁定
    └─▶ Bucket未启用 → 检查Object级WORM配置
    │
    ▼
设置worm_retention_until = now + retention_period
    │
    ▼
写入Needle数据
```

**删除时拦截**：
```
客户端删除对象
    │
    ▼
检查worm_retention_until
    │
    ├─▶ 未过期 → 返回AccessDenied错误
    ├─▶ 已过期 → 允许删除
    └─▶ 未设置WORM → 允许删除
```

### 2.4 与其他功能的交互

| 功能 | 交互规则 |
|------|---------|
| **回收站** | WORM锁定期间，对象不能进入回收站，直接拒绝删除 |
| **自动修复** | WORM对象可以被自动修复（修复不改变数据内容） |
| **Bitrot检测** | WORM对象同样参与检测 |
| **EC编码** | WORM对象可以使用EC编码 |

---

## 3. 回收站（Recycle Bin）

### 3.1 设计原则

| 维度 | 方案 | 说明 |
|------|------|------|
| **删除策略** | 软删除 | 删除时标记而非物理删除 |
| **保留期限** | 可配置 | 默认7天，支持1-365天 |
| **恢复机制** | 元数据恢复 | 恢复时清除删除标记 |
| **清理机制** | 后台GC | 定期清理过期对象 |

### 3.2 数据结构变更

**NeedleInfo 扩展**：
```rust
pub struct NeedleInfo {
    // 已有字段...
    deleted_at: Option<DateTime<Utc>>,        // 新增：删除时间
    delete_retention_until: Option<DateTime<Utc>>,  // 新增：保留到期时间
}
```

**VolumeInfo 扩展**：
```rust
pub struct VolumeInfo {
    // 已有字段...
    recycle_bin_enabled: bool,           // 新增：是否启用回收站
    recycle_retention_days: u32,         // 新增：保留天数
}
```

### 3.3 核心流程

**删除流程**：
```
客户端删除对象
    │
    ▼
检查WORM状态
    │
    ├─▶ WORM锁定中 → 拒绝删除
    └─▶ 未锁定 → 继续
    │
    ▼
检查回收站配置
    │
    ├─▶ 启用回收站 → 软删除
    │   ├─▶ 设置deleted_at = now
    │   ├─▶ 设置delete_retention_until = now + retention_days
    │   └─▶ 标记index为deleted
    └─▶ 未启用 → 物理删除
```

**恢复流程**：
```
客户端恢复对象
    │
    ▼
检查delete_retention_until
    │
    ├─▶ 未过期 → 恢复
    │   ├─▶ 清除deleted_at
    │   ├─▶ 清除delete_retention_until
    │   └─▶ 恢复index状态
    └─▶ 已过期 → 对象已被清理，无法恢复
```

**后台清理流程**：
```
后台GC线程（每小时运行）
    │
    ▼
遍历所有Volume的Needle
    │
    ▼
检查delete_retention_until
    │
    ├─▶ 已过期 → 物理删除Needle数据
    │   ├─▶ 从index中移除
    │   └─▶ 释放磁盘空间
    └─▶ 未过期 → 保留
```

### 3.4 存储影响评估

| 配置 | 额外存储空间 | 清理频率 |
|------|------------|---------|
| 7天保留 | 最多7天数据量 | 每小时 |
| 30天保留 | 最多30天数据量 | 每小时 |
| 365天保留 | 最多365天数据量 | 每天 |

---

## 4. 自动数据修复（Auto Repair）

### 4.1 设计原则

| 维度 | 方案 | 说明 |
|------|------|------|
| **触发方式** | Bitrot检测 + 节点故障 | 两种场景触发修复 |
| **修复方式** | 副本重建 / EC解码 | 根据冗余策略选择 |
| **修复粒度** | Needle级 | 逐Needle修复 |
| **并发控制** | 限流器 | 避免修复任务过多影响正常业务 |

### 4.2 数据结构变更

**修复任务队列**：
```rust
pub struct RepairTask {
    task_id: String,
    volume_id: VolumeId,
    needle_id: NeedleId,
    repair_type: RepairType,
    status: RepairStatus,
    priority: RepairPriority,
    created_at: DateTime<Utc>,
    started_at: Option<DateTime<Utc>>,
    completed_at: Option<DateTime<Utc>>,
}

pub enum RepairType {
    BitrotCorruption,    // Bitrot检测到损坏
    NodeFailure,         // 节点故障导致数据不可用
    VolumeRebalance,     // Volume重新平衡
}

pub enum RepairStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
}

pub enum RepairPriority {
    High,    // 影响用户访问
    Medium,  // 部分副本丢失
    Low,     // 后台维护
}
```

### 4.3 核心流程

**Bitrot触发修复**：
```
Bitrot扫描发现损坏
    │
    ▼
创建RepairTask (priority=High)
    │
    ▼
检查冗余策略
    │
    ├─▶ 副本模式 → 从其他副本读取重建
    │   ├─▶ 从健康副本读取完整数据
    │   ├─▶ 写入损坏Volume
    │   └─▶ 更新checksum
    └─▶ EC模式 → 从k个健康分片解码重建
        ├─▶ 收集k个健康分片
        ├─▶ Reed-Solomon解码
        ├─▶ 写入损坏分片
        └─▶ 更新checksum
```

**节点故障触发修复**：
```
Master检测到节点故障
    │
    ▼
识别故障节点上的所有Volume
    │
    ▼
对每个Volume中的Needle创建RepairTask
    │
    ▼
调度修复任务（按优先级）
    │
    ▼
等待新节点上线后重建数据
```

### 4.4 修复调度策略

| 优先级 | 场景 | 并发数限制 |
|--------|------|-----------|
| High | 用户读取失败、Bitrot损坏 | 无限制 |
| Medium | 部分副本丢失、节点降级 | 最多5个任务 |
| Low | 后台巡检、Volume迁移 | 最多2个任务 |

---

## 5. Bitrot 数据校验（Checksum Verification）

### 5.1 设计原则

| 维度 | 方案 | 说明 |
|------|------|------|
| **校验算法** | CRC32C | 高性能、硬件加速支持 |
| **校验粒度** | Needle级 | 每个Needle独立校验 |
| **校验时机** | 写入时 + 后台扫描 | 双重保障 |
| **扫描策略** | 增量扫描 | 定期扫描未校验的数据 |

### 5.2 数据结构变更

**NeedleInfo 扩展**：
```rust
pub struct NeedleInfo {
    // 已有字段...
    checksum_algorithm: ChecksumAlgorithm,  // 新增：校验算法类型
    last_verified_at: Option<DateTime<Utc>>, // 新增：上次校验时间
    verification_count: u64,                 // 新增：校验次数
}

pub enum ChecksumAlgorithm {
    CRC32C,
    CRC64,
    Blake3,
}
```

**VolumeInfo 扩展**：
```rust
pub struct VolumeInfo {
    // 已有字段...
    bitrot_scan_enabled: bool,       // 新增：是否启用扫描
    bitrot_scan_interval_hours: u32, // 新增：扫描间隔
}
```

### 5.3 当前校验机制分析

**现有代码问题**：
- 当前 `calculate_checksum` 返回 `u64`（简单校验和）
- 校验算法强度不足，容易产生碰撞
- 没有后台扫描机制

**升级方案**：
```rust
pub enum ChecksumAlgorithm {
    CRC32C,
    CRC64,
    Blake3,
}

pub struct Checksum {
    algorithm: ChecksumAlgorithm,
    value: Vec<u8>,
}

impl Checksum {
    pub fn compute(data: &[u8], algorithm: ChecksumAlgorithm) -> Self {
        match algorithm {
            ChecksumAlgorithm::CRC32C => {
                let crc = crc32c::crc32c(data);
                Checksum {
                    algorithm: ChecksumAlgorithm::CRC32C,
                    value: crc.to_be_bytes().to_vec(),
                }
            }
            ChecksumAlgorithm::CRC64 => {
                let crc = crc64fast::crc64(data);
                Checksum {
                    algorithm: ChecksumAlgorithm::CRC64,
                    value: crc.to_be_bytes().to_vec(),
                }
            }
            ChecksumAlgorithm::Blake3 => {
                let hash = blake3::hash(data);
                Checksum {
                    algorithm: ChecksumAlgorithm::Blake3,
                    value: hash.as_bytes().to_vec(),
                }
            }
        }
    }
    
    pub fn verify(&self, data: &[u8]) -> bool {
        let computed = Self::compute(data, self.algorithm);
        computed.value == self.value
    }
}
```

### 5.4 核心流程

**写入时校验**：
```
客户端写入数据
    │
    ▼
计算Checksum（使用配置的算法）
    │
    ▼
将Checksum存储到Needle footer
    │
    ▼
写入Needle数据
```

**读取时校验**：
```
客户端读取数据
    │
    ▼
读取Needle数据和Checksum
    │
    ▼
重新计算Checksum
    │
    ▼
比对校验值
    │
    ├─▶ 一致 → 返回数据
    └─▶ 不一致 → 返回错误，触发修复
```

**后台扫描流程**：
```
后台扫描线程（每N小时运行）
    │
    ▼
遍历Volume中的Needle
    │
    ▼
检查last_verified_at
    │
    ├─▶ 超过扫描间隔 → 重新校验
    │   ├─▶ 读取Needle数据
    │   ├─▶ 重新计算Checksum
    │   ├─▶ 比对校验值
    │   ├─▶ 一致 → 更新last_verified_at
    │   └─▶ 不一致 → 触发自动修复
    └─▶ 未超过 → 跳过
```

### 5.5 校验算法对比

| 算法 | 输出大小 | 性能 | 碰撞概率 | 硬件加速 |
|------|---------|------|---------|---------|
| CRC32C | 4字节 | 最高 | 较高 | Intel SSE4.2 |
| CRC64 | 8字节 | 高 | 低 | 部分支持 |
| Blake3 | 32字节 | 中 | 极低 | 无 |

**推荐方案**：默认使用 CRC32C（平衡性能和安全性），支持通过配置切换到 Blake3。

---

## 6. 功能依赖关系与实施顺序

### 6.1 依赖关系图

```
Bitrot检测 ──▶ Auto Repair
    │                   │
    │                   ▼
    └───────▶ EC/副本重建
                          │
                          ▼
                     Volume管理
                          │
                    ┌─────┴─────┐
                    ▼           ▼
                WORM锁定    回收站
```

### 6.2 推荐实施顺序

| 阶段 | 功能 | 依赖 | 优先级 | 预估时间 |
|------|------|------|--------|---------|
| Phase 1 | Bitrot检测 | 无 | P0 | 2周 |
| Phase 2 | 回收站 | 无 | P1 | 2周 |
| Phase 3 | WORM锁定 | 无 | P1 | 1.5周 |
| Phase 4 | 自动数据修复 | Bitrot检测 | P0 | 3周 |
| Phase 5 | 细纠删码 | Auto Repair | P2 | 4周 |

### 6.3 实施策略

**Phase 1（基础保障）**：
- 升级校验算法（CRC32C）
- 实现后台扫描线程
- 在读取路径添加校验

**Phase 2（数据保护）**：
- 实现软删除机制
- 实现回收站管理API
- 实现后台GC清理

**Phase 3（合规要求）**：
- 实现WORM锁定元数据
- 在写入/删除路径添加拦截
- 实现Bucket级WORM策略

**Phase 4（自动恢复）**：
- 实现修复任务队列
- 实现副本重建逻辑
- 实现修复调度器

**Phase 5（高级冗余）**：
- 实现EC编解码
- 实现EC分片分配
- 实现EC数据重建

---

## 7. 性能影响综合评估

| 功能 | 写入性能影响 | 读取性能影响 | 存储空间影响 | CPU影响 |
|------|-------------|-------------|-------------|---------|
| Bitrot检测 | +5%（计算校验） | +2%（读取时校验） | +0% | 低 |
| 回收站 | 0% | +1%（检查删除状态） | +5-30%（保留数据） | 低 |
| WORM锁定 | +1%（检查锁定状态） | 0% | +0% | 极低 |
| Auto Repair | 0% | 0% | 0% | 中（修复时） |
| EC编码 | -33%（k=4,m=2） | 0% | -50%（空间节省） | 中 |

---

## 8. 关键设计决策总结

| 决策 | 方案 | 理由 |
|------|------|------|
| **EC粒度** | Needle级 | 避免Volume级编码的碎片问题，提高修复效率 |
| **EC参数** | (k=4, m=2) | 平衡空间利用率和容错能力 |
| **校验算法** | CRC32C | 硬件加速支持，性能与安全性平衡 |
| **WORM模式** | Compliance + Governance | 满足不同合规要求 |
| **回收站保留** | 可配置（默认7天） | 平衡数据恢复需求和存储成本 |
| **修复触发** | Bitrot检测 + 节点故障 | 全面覆盖数据损坏场景 |
| **修复调度** | 优先级队列 | 保障关键任务优先执行 |

---

*文档版本：v1.0*  
*创建日期：2026-07-07*  
*最后更新：2026-07-07*