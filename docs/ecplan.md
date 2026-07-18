# PowerFS 高级存储功能实施计划

## 概述

根据 [advanced-storage-features.md](advanced-storage-features.md) 的设计方案，本实施计划详细拆分五个高级存储功能的开发任务：

1. **Phase 1: Bitrot检测** - 数据完整性保障（基础）
2. **Phase 2: 回收站** - 误删除恢复
3. **Phase 3: WORM锁定** - 数据不可篡改
4. **Phase 4: 自动数据修复** - 数据一致性保障
5. **Phase 5: 细纠删码** - 高级冗余保护

---

## Phase 1: Bitrot检测（2周）

### 目标

升级校验算法，实现后台扫描机制，在读写路径添加校验。

### 任务拆分

#### P1-01: 升级校验算法（CRC32C）

**描述**：将当前简单的u64校验和升级为支持多种算法的Checksum结构

**涉及文件**：
- `powerfs-common/src/utils.rs` - 新增Checksum结构体和计算方法
- `powerfs-common/Cargo.toml` - 添加crc32c依赖

**代码变更**：
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
    pub fn compute(data: &[u8], algorithm: ChecksumAlgorithm) -> Self;
    pub fn verify(&self, data: &[u8]) -> bool;
}
```

**测试验证**：
- 单元测试：验证CRC32C/CRC64/Blake3计算正确性
- 兼容性测试：确保现有数据可正常读取

#### P1-02: 扩展Needle结构支持新校验算法

**描述**：修改Needle和NeedleInfo结构，支持新的校验算法

**涉及文件**：
- `powerfs-core/src/needle.rs` - 修改Needle结构
- `powerfs-common/src/types.rs` - 修改NeedleInfo结构

**代码变更**：
```rust
pub struct Needle {
    // 已有字段...
    checksum: Checksum,  // 从u64升级为Checksum
}

pub struct NeedleInfo {
    // 已有字段...
    checksum_algorithm: ChecksumAlgorithm,
    last_verified_at: Option<DateTime<Utc>>,
    verification_count: u64,
}
```

**测试验证**：
- 单元测试：Needle读写正常
- 集成测试：Volume写入/读取数据校验

#### P1-03: 在写入路径添加校验

**描述**：修改write_needle方法，使用新的Checksum计算

**涉及文件**：
- `powerfs-core/src/volume.rs` - 修改write_needle方法

**测试验证**：
- 写入数据后，校验和正确存储
- 写入性能回归测试

#### P1-04: 在读取路径添加校验

**描述**：修改read_needle方法，读取时验证校验和

**涉及文件**：
- `powerfs-core/src/volume.rs` - 修改read_needle方法

**测试验证**：
- 正常数据读取通过校验
- 损坏数据读取返回错误
- 读取性能回归测试

#### P1-05: 实现后台Bitrot扫描线程

**描述**：创建后台线程定期扫描Volume中的Needle，验证数据完整性

**涉及文件**：
- `powerfs-volume/src/bitrot_scanner.rs` - 新增扫描模块
- `powerfs-volume/src/server.rs` - 启动扫描线程

**代码变更**：
```rust
pub struct BitrotScanner {
    storage_manager: Arc<StorageManager>,
    scan_interval: Duration,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
}

impl BitrotScanner {
    pub async fn start(self);
    async fn scan_volume(&self, volume_id: VolumeId);
}
```

**测试验证**：
- 扫描线程正常启动和停止
- 扫描发现损坏数据并记录日志
- 扫描间隔配置生效

#### P1-06: 扩展Volume配置支持扫描参数

**描述**：修改VolumeInfo和Volume配置，支持Bitrot扫描开关和间隔

**涉及文件**：
- `powerfs-common/src/types.rs` - 修改VolumeInfo
- `powerfs-master/src/master.rs` - 支持配置传递

**测试验证**：
- 配置参数正确传递
- 扫描开关生效

### Phase 1 验证标准

| 指标 | 标准 |
|------|------|
| 单元测试 | 全部通过 |
| 校验正确性 | 损坏数据检测率100% |
| 写入性能 | 下降<10% |
| 读取性能 | 下降<5% |
| 扫描功能 | 后台线程正常运行 |

---

## Phase 2: 回收站（2周）

### 目标

实现软删除机制，支持对象恢复和后台GC清理。

### 任务拆分

#### P2-01: 扩展NeedleInfo支持软删除标记

**描述**：添加deleted_at和delete_retention_until字段

**涉及文件**：
- `powerfs-common/src/types.rs` - 修改NeedleInfo

**测试验证**：
- 数据结构序列化/反序列化正常

#### P2-02: 修改delete_needle实现软删除

**描述**：删除时标记而非物理删除

**涉及文件**：
- `powerfs-core/src/volume.rs` - 修改delete_needle方法

**测试验证**：
- 删除后数据仍存在（软删除）
- 删除标记正确设置

#### P2-03: 实现回收站恢复功能

**描述**：添加restore_needle方法恢复已删除对象

**涉及文件**：
- `powerfs-core/src/volume.rs` - 新增restore_needle方法

**测试验证**：
- 删除对象可恢复
- 过期对象无法恢复

#### P2-04: 实现后台GC清理线程

**描述**：定期清理过期的软删除对象

**涉及文件**：
- `powerfs-volume/src/gc_thread.rs` - 新增GC线程模块
- `powerfs-volume/src/server.rs` - 启动GC线程

**测试验证**：
- 过期对象自动清理
- GC线程正常启动和停止

#### P2-05: 扩展Volume配置支持回收站参数

**描述**：添加recycle_bin_enabled和recycle_retention_days

**涉及文件**：
- `powerfs-common/src/types.rs` - 修改VolumeInfo

**测试验证**：
- 配置参数正确传递
- 回收站开关生效

#### P2-06: 在S3层集成回收站功能

**描述**：修改S3 API支持回收站操作

**涉及文件**：
- `powerfs-master/src/s3/server.rs` - 添加回收站API

**API设计**：
| API | 方法 | 路径 | 说明 |
|-----|------|------|------|
| ListDeletedObjects | GET | /bucket?deleted | 列出已删除对象 |
| RestoreObject | POST | /bucket/object?restore | 恢复对象 |

**测试验证**：
- 使用AWS CLI测试回收站功能

### Phase 2 验证标准

| 指标 | 标准 |
|------|------|
| 单元测试 | 全部通过 |
| 软删除 | 删除后数据可恢复 |
| 恢复功能 | 未过期对象可恢复 |
| GC清理 | 过期对象自动清理 |
| S3兼容性 | AWS CLI操作正常 |

---

## Phase 3: WORM锁定（1.5周）

### 目标

实现WORM锁定机制，支持Bucket级和Object级策略。

### 任务拆分

#### P3-01: 扩展NeedleInfo支持WORM标记

**描述**：添加worm_retention_until字段

**涉及文件**：
- `powerfs-common/src/types.rs` - 修改NeedleInfo

**测试验证**：
- 数据结构序列化/反序列化正常

#### P3-02: 实现WORM配置结构

**描述**：创建WormConfig和WormMode枚举

**涉及文件**：
- `powerfs-common/src/types.rs` - 新增WormConfig

**测试验证**：
- 配置结构正确定义

#### P3-03: 在写入路径应用WORM策略

**描述**：根据Bucket配置自动应用WORM锁定

**涉及文件**：
- `powerfs-master/src/s3/server.rs` - 修改PutObject handler

**测试验证**：
- WORM策略正确应用
- retention_until正确设置

#### P3-04: 在删除路径拦截WORM对象

**描述**：拒绝删除WORM锁定期间的对象

**涉及文件**：
- `powerfs-core/src/volume.rs` - 修改delete_needle方法
- `powerfs-master/src/s3/server.rs` - 修改DeleteObject handler

**测试验证**：
- WORM锁定对象无法删除
- 锁定到期后可正常删除

#### P3-05: 实现Bucket级WORM策略配置

**描述**：添加Bucket WORM策略管理API

**涉及文件**：
- `powerfs-master/src/s3/server.rs` - 添加Bucket WORM API

**测试验证**：
- Bucket WORM策略可配置
- 策略正确应用到新对象

### Phase 3 验证标准

| 指标 | 标准 |
|------|------|
| 单元测试 | 全部通过 |
| 写入锁定 | WORM策略正确应用 |
| 删除拦截 | 锁定对象无法删除 |
| 到期解锁 | 到期后可正常删除 |
| S3兼容性 | AWS CLI操作正常 |

---

## Phase 4: 自动数据修复（3周）

### 目标

实现自动数据修复机制，支持副本重建和EC解码重建。

### 任务拆分

#### P4-01: 创建修复任务数据结构

**描述**：定义RepairTask、RepairType、RepairStatus、RepairPriority

**涉及文件**：
- `powerfs-common/src/types.rs` - 新增修复任务结构

**测试验证**：
- 数据结构序列化/反序列化正常

#### P4-02: 实现修复任务队列

**描述**：创建优先级队列管理修复任务

**涉及文件**：
- `powerfs-master/src/repair/queue.rs` - 新增任务队列模块

**测试验证**：
- 任务正确入队和出队
- 优先级排序正确

#### P4-03: 实现Bitrot触发修复

**描述**：Bitrot扫描发现损坏时创建修复任务

**涉及文件**：
- `powerfs-volume/src/bitrot_scanner.rs` - 触发修复任务

**测试验证**：
- Bitrot损坏触发High优先级任务
- 任务正确入队

#### P4-04: 实现节点故障触发修复

**描述**：Master检测到节点故障时创建修复任务

**涉及文件**：
- `powerfs-master/src/master.rs` - 节点故障处理

**测试验证**：
- 节点故障触发修复任务
- 任务正确分配到新节点

#### P4-05: 实现副本重建逻辑

**描述**：从健康副本读取数据重建损坏数据

**涉及文件**：
- `powerfs-master/src/repair/replica_repair.rs` - 副本重建模块

**测试验证**：
- 损坏数据成功重建
- 重建后数据校验通过

#### P4-06: 实现修复调度器

**描述**：管理修复任务的并发执行

**涉及文件**：
- `powerfs-master/src/repair/scheduler.rs` - 调度器模块

**测试验证**：
- 并发控制生效
- 优先级高的任务优先执行

#### P4-07: 实现修复状态管理API

**描述**：提供修复任务状态查询和管理API

**涉及文件**：
- `powerfs-master/src/api/server.rs` - 添加修复管理API

**测试验证**：
- 任务状态正确查询
- 任务可取消

### Phase 4 验证标准

| 指标 | 标准 |
|------|------|
| 单元测试 | 全部通过 |
| Bitrot触发 | 损坏检测后自动创建修复任务 |
| 节点故障触发 | 故障后自动创建修复任务 |
| 副本重建 | 数据成功恢复 |
| 并发控制 | 优先级策略生效 |

---

## Phase 5: 细纠删码（4周）

### 目标

实现Needle级EC编码，支持异步编解码，与副本机制互斥。

### 任务拆分

#### P5-01: 添加EC编解码依赖

**描述**：添加reed-solomon-erasure crate依赖

**涉及文件**：
- `powerfs-core/Cargo.toml` - 添加依赖

**测试验证**：
- 依赖正确添加
- 编解码基础测试通过

#### P5-02: 实现EC编解码模块

**描述**：创建异步EC编解码模块

**涉及文件**：
- `powerfs-core/src/ec/codec.rs` - 新增EC编解码模块

**代码变更**：
```rust
pub struct EcCodec {
    k: u32,
    m: u32,
}

impl EcCodec {
    pub async fn encode(&self, data: &[u8]) -> Vec<Vec<u8>>;
    pub async fn decode(&self, shards: &[Option<&[u8]>]) -> Result<Vec<u8>>;
    pub async fn reconstruct(&self, shards: &[Option<&[u8]>]) -> Vec<Vec<u8>>;
}
```

**测试验证**：
- 编码正确性：k+m分片正确生成
- 解码正确性：任意k个分片可恢复原始数据
- 重建正确性：损坏分片可重建

#### P5-03: 扩展VolumeInfo支持EC配置

**描述**：添加redundancy_type、ec_k、ec_m字段

**涉及文件**：
- `powerfs-common/src/types.rs` - 修改VolumeInfo

**测试验证**：
- 数据结构序列化/反序列化正常

#### P5-04: 扩展NeedleInfo支持EC分片信息

**描述**：添加ec_shards字段存储分片位置

**涉及文件**：
- `powerfs-common/src/types.rs` - 修改NeedleInfo

**测试验证**：
- 数据结构序列化/反序列化正常

#### P5-05: 修改Master分配逻辑支持EC模式

**描述**：EC模式下分配k个不同Volume

**涉及文件**：
- `powerfs-master/src/master.rs` - 修改assign_volume方法

**测试验证**：
- EC模式正确分配k个Volume
- 副本模式不受影响

#### P5-06: 实现EC模式写入流程

**描述**：编码数据并写入k+m个分片

**涉及文件**：
- `powerfs-core/src/volume.rs` - 修改write_needle方法
- `powerfs-master/src/write_handler.rs` - 修改写入逻辑

**测试验证**：
- EC模式数据正确写入
- 分片分布正确

#### P5-07: 实现EC模式读取流程

**描述**：从任意k个分片读取并解码

**涉及文件**：
- `powerfs-core/src/volume.rs` - 修改read_needle方法
- `powerfs-master/src/read_handler.rs` - 修改读取逻辑

**测试验证**：
- EC模式数据正确读取
- 部分分片损坏时仍可读取

#### P5-08: 实现EC模式数据修复

**描述**：使用EC解码重建损坏分片

**涉及文件**：
- `powerfs-master/src/repair/ec_repair.rs` - EC修复模块

**测试验证**：
- 损坏分片成功重建
- 重建后数据校验通过

#### P5-09: 实现Collection级冗余策略配置

**描述**：支持Collection级选择副本或EC策略

**涉及文件**：
- `powerfs-common/src/types.rs` - 修改CollectionConfig
- `powerfs-master/src/master.rs` - 支持策略配置

**测试验证**：
- 策略正确配置
- 不同Collection使用不同策略

### Phase 5 验证标准

| 指标 | 标准 |
|------|------|
| 单元测试 | 全部通过 |
| EC编码 | 数据正确编码为k+m分片 |
| EC解码 | 任意k个分片可恢复数据 |
| EC修复 | 损坏分片可重建 |
| 策略互斥 | EC与副本模式互斥 |
| 性能 | 写入下降<40%，读取下降<10% |

---

## 跨阶段依赖关系

```
Phase 1: Bitrot检测
    │
    ├──▶ Phase 4: 自动数据修复（依赖Bitrot触发）
    │
Phase 2: 回收站
    │
    ├──▶ Phase 3: WORM锁定（WORM对象不能进入回收站）
    │
Phase 4: 自动数据修复
    │
    ├──▶ Phase 5: 细纠删码（依赖修复机制）
    │
所有阶段
    │
    └──▶ 需要更新proto定义（如果涉及跨服务通信）
```

---

## 测试策略

### 单元测试

每个模块独立测试，覆盖核心逻辑：
- 校验算法正确性
- EC编解码正确性
- WORM锁定逻辑
- 回收站恢复逻辑
- 修复任务调度

### 集成测试

跨模块测试，验证端到端流程：
- 写入→读取→校验完整流程
- 删除→恢复完整流程
- WORM锁定→到期删除流程
- Bitrot损坏→修复完整流程
- EC编码→解码→修复完整流程

### 性能测试

使用benchmark测试性能影响：
- 写入/读取吞吐量
- EC编解码延迟
- Bitrot扫描IO开销
- 修复任务CPU/内存开销

### S3兼容性测试

使用s3tests验证S3 API兼容性：
- 回收站操作
- WORM锁定操作
- EC模式下的对象操作

---

## 风险评估

| 风险 | 概率 | 影响 | 缓解措施 |
|------|------|------|---------|
| EC编解码性能不足 | 中 | 高 | 使用异步编解码，选择高效库 |
| Bitrot扫描IO开销过大 | 低 | 中 | 增量扫描，限制扫描速率 |
| 修复任务过多影响业务 | 低 | 中 | 优先级队列+并发限制 |
| 数据结构变更兼容性 | 中 | 高 | 向后兼容设计，迁移工具 |
| S3 API兼容性问题 | 中 | 中 | 参考AWS文档，运行s3tests |

---

## 资源需求

| 角色 | 人数 | 主要职责 |
|------|------|---------|
| Rust后端开发 | 2人 | 核心功能实现 |
| 测试工程师 | 1人 | 单元测试、集成测试 |
| DevOps | 1人 | CI/CD、测试环境 |

---

*文档版本：v1.0*  
*创建日期：2026-07-07*  
*最后更新：2026-07-07*