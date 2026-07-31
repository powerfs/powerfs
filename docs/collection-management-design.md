# Collection 管理方案设计

## 1. 概述

### 1.1 什么是 Collection

Collection 是 PowerFS 中的**逻辑数据隔离单元**，类似于数据库中的"数据库"或对象存储中的"Bucket"。不同 Collection 的数据存储在不同的 Volume 组中，实现：

- **多租户隔离**：不同业务线/应用的数据物理分离
- **容量管理**：按 Collection 配额控制存储用量
- **策略差异化**：不同 Collection 可配置不同副本数、TTL、压缩策略
- **监控与计费**：按 Collection 统计读写量、容量、成本

### 1.2 当前状态

| 功能 | 状态 | 说明 |
|------|------|------|
| Volume 创建指定 collection | ❌ 硬编码 "default" | `CreateVolume` 未接收 collection 参数 |
| FUSE 挂载选择 collection | ✅ 参数传递 | 配置文件支持，但后端无区分 |
| Master 按 collection 匹配 Volume | ✅ 已实现 | `assign_volume` 会过滤 collection |
| Collection 管理界面 | ❌ 缺失 | 无前端页面 |
| Collection 管理 CLI | ❌ 缺失 | 无命令行工具 |
| Collection 级别配额 | ❌ 缺失 | 无容量限制 |
| Collection 级别监控 | ❌ 缺失 | 无指标聚合 |

### 1.3 设计目标

1. **最小改动原则**：复用现有 Volume 分配逻辑，仅增加 Collection 元数据管理
2. **运维友好**：提供 CLI 和 Web UI 两套管理界面
3. **向后兼容**：保留 "default" collection，现有配置无需修改
4. **可观测性**：Collection 级别的容量、I/O、文件数监控

---

## 2. 数据模型

### 2.1 Collection 定义

```rust
/// Collection 元数据（完整属性体系）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionInfo {
    // ═══════════════════════════════════════════════════════════
    // 基本属性
    // ═══════════════════════════════════════════════════════════

    /// Collection 名称（全局唯一）
    pub name: String,

    /// 创建时间
    pub created_at: i64,

    /// 更新时间
    pub updated_at: i64,

    /// 描述信息
    pub description: String,

    /// 标签（用于分类、检索）
    pub tags: HashMap<String, String>,

    /// 所有者（用户或团队）
    pub owner: String,

    /// 状态（active / readonly / archived / deleted）
    pub status: CollectionStatus,

    // ═══════════════════════════════════════════════════════════
    // 存储策略
    // ═══════════════════════════════════════════════════════════

    /// 存储策略（副本/EC）
    pub storage_policy: StoragePolicy,

    /// 磁盘类型（hdd / ssd / nvme / mixed）
    pub disk_type: DiskType,

    /// 压缩配置
    pub compression: CompressionConfig,

    /// 数据去重
    pub deduplication: DeduplicationConfig,

    // ═══════════════════════════════════════════════════════════
    // 容量与配额
    // ═══════════════════════════════════════════════════════════

    /// 容量配额（字节，0 表示无限制）
    pub capacity_quota_bytes: u64,

    /// 文件数量配额（0 表示无限制）
    pub file_count_quota: u64,

    /// 预分配 Volume 数量
    pub volume_count: u32,

    /// 单个 Volume 大小限制（字节）
    pub volume_size_limit: u64,

    // ═══════════════════════════════════════════════════════════
    // 数据生命周期
    // ═══════════════════════════════════════════════════════════

    /// TTL 秒数（0 表示永不过期）
    pub ttl_seconds: u32,

    /// 生命周期策略
    pub lifecycle: LifecycleConfig,

    // ═══════════════════════════════════════════════════════════
    // 安全与加密
    // ═══════════════════════════════════════════════════════════

    /// 加密配置
    pub encryption: EncryptionConfig,

    /// WORM（Write Once Read Many）模式
    pub worm_enabled: bool,

    /// WORM 保护天数
    pub worm_retention_days: u32,

    // ═══════════════════════════════════════════════════════════
    // 数据分布策略
    // ═══════════════════════════════════════════════════════════

    /// 副本放置策略
    pub placement: PlacementPolicy,

    // ═══════════════════════════════════════════════════════════
    // QoS 限流
    // ═══════════════════════════════════════════════════════════

    /// QoS 配置
    pub qos: QosConfig,

    // ═══════════════════════════════════════════════════════════
    // 数据分层
    // ═══════════════════════════════════════════════════════════

    /// 数据分层配置
    pub tiering: TieringConfig,

    // ═══════════════════════════════════════════════════════════
    // 审计与合规
    // ═══════════════════════════════════════════════════════════

    /// 审计日志级别
    pub audit_level: AuditLevel,

    // ═══════════════════════════════════════════════════════════
    // 文件限制
    // ═══════════════════════════════════════════════════════════

    /// 文件大小限制
    pub file_size_limit: FileSizeLimit,

    /// 目录深度限制
    pub max_directory_depth: u32,

    /// Volume 分配模式（自动 / 手动指定 / 混合）
    pub volume_allocation: VolumeAllocationMode,

    /// Volume 黑名单（这些 Volume 不会被分配给此 Collection）
    pub excluded_volume_ids: Vec<u64>,
}

/// Collection 状态
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum CollectionStatus {
    /// 正常可用
    Active,
    /// 只读（不允许写入）
    Readonly,
    /// 已归档（数据可能被压缩或移动到冷存储）
    Archived,
    /// 已删除（等待清理）
    Deleted,
}

/// 磁盘类型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DiskType {
    /// 机械硬盘
    Hdd,
    /// 固态硬盘
    Ssd,
    /// NVMe SSD
    Nvme,
    /// 混合（根据热度自动分层）
    Mixed,
}

// ═══════════════════════════════════════════════════════════════
// 存储策略（已有，保留）
// ═══════════════════════════════════════════════════════════════

/// 存储策略
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoragePolicy {
    pub name: String,
    pub redundancy: RedundancyMode,
    pub min_write_nodes: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RedundancyMode {
    Replication { copies: u32 },
    ErasureCoding {
        data_shards: u32,
        parity_shards: u32,
        algorithm: String,
    },
}

// ═══════════════════════════════════════════════════════════════
// 压缩配置
// ═══════════════════════════════════════════════════════════════

/// 压缩配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionConfig {
    /// 是否启用压缩
    pub enabled: bool,

    /// 压缩算法
    pub algorithm: CompressionAlgorithm,

    /// 压缩级别（1-9，数值越大压缩率越高但速度越慢）
    pub level: u32,

    /// 最小压缩大小（小于此大小的文件不压缩）
    pub min_size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompressionAlgorithm {
    /// 不压缩
    None,
    /// Snappy（快速，压缩率中等）
    Snappy,
    /// LZ4（最快，压缩率较低）
    Lz4,
    /// Zstd（平衡，推荐）
    Zstd,
    /// LZMA（高压缩率，速度慢）
    Lzma,
}

// ═══════════════════════════════════════════════════════════════
// 数据去重配置
// ═══════════════════════════════════════════════════════════════

/// 数据去重配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeduplicationConfig {
    /// 是否启用去重
    pub enabled: bool,

    /// 去重算法（chunk-based / fixed-size / variable-size）
    pub algorithm: DedupAlgorithm,

    /// 去重块大小（字节）
    pub chunk_size: u64,

    /// 去重哈希算法（sha256 / blake3）
    pub hash_algorithm: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DedupAlgorithm {
    /// 固定大小分块
    FixedSize,
    /// 变长分块（CDC，推荐）
    ContentDefined,
}

// ═══════════════════════════════════════════════════════════════
// 生命周期配置
// ═══════════════════════════════════════════════════════════════

/// 生命周期配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleConfig {
    /// 生命周期规则列表
    pub rules: Vec<LifecycleRule>,
}

/// 生命周期规则
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleRule {
    /// 规则 ID
    pub id: String,

    /// 是否启用
    pub enabled: bool,

    /// 筛选条件（前缀匹配）
    pub prefix: String,

    /// 转换动作
    pub transitions: Vec<LifecycleTransition>,

    /// 过期动作
    pub expiration: Option<LifecycleExpiration>,
}

/// 转换动作（数据分层）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleTransition {
    /// 创建后多少天执行
    pub days: u32,

    /// 目标存储类
    pub storage_class: StorageClass,
}

/// 过期动作
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleExpiration {
    /// 创建后多少天过期
    pub days: Option<u32>,

    /// 或指定过期日期
    pub date: Option<String>,
}

/// 存储类
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StorageClass {
    /// 热存储（SSD/NVMe）
    Hot,
    /// 温存储（HDD）
    Warm,
    /// 冷存储（压缩 + HDD）
    Cold,
    /// 归档存储（可能离线）
    Archive,
}

// ═══════════════════════════════════════════════════════════════
// 加密配置
// ═══════════════════════════════════════════════════════════════

/// 加密配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionConfig {
    /// 是否启用加密
    pub enabled: bool,

    /// 加密算法
    pub algorithm: EncryptionAlgorithm,

    /// 密钥管理方式
    pub key_management: KeyManagement,

    /// 密钥轮换周期（天，0 表示不轮换）
    pub key_rotation_days: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EncryptionAlgorithm {
    /// AES-256-GCM（推荐）
    Aes256Gcm,
    /// ChaCha20-Poly1305
    ChaCha20Poly1305,
    /// XChaCha20-Poly1305（更长 nonce）
    XChaCha20Poly1305,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KeyManagement {
    /// 系统管理（Master 集群密钥）
    SystemManaged,
    /// 用户管理（用户上传密钥）
    UserManaged,
    /// KMS 管理（外部密钥管理系统）
    KmsManaged { kms_key_id: String },
}

// ═══════════════════════════════════════════════════════════════
// 副本放置策略
// ═══════════════════════════════════════════════════════════════

/// 副本放置策略
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacementPolicy {
    /// 机架感知策略
    pub rack_aware: RackAwarePolicy,

    /// 数据中心策略（跨 DC 复制）
    pub data_center: DataCenterPolicy,

    /// 节点选择策略
    pub node_selector: NodeSelector,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RackAwarePolicy {
    /// 无机架感知
    None,
    /// 副本跨机架放置
    CrossRack,
    /// 副本跨机架 + 跨交换机放置
    CrossRackAndSwitch,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataCenterPolicy {
    /// 单数据中心
    Single,
    /// 跨数据中心同步复制
    SyncReplication { data_centers: Vec<String> },
    /// 跨数据中心异步复制
    AsyncReplication { target_data_centers: Vec<String> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSelector {
    /// 标签选择器
    pub match_labels: HashMap<String, String>,

    /// 反亲和性（避免同一节点）
    pub anti_affinity: bool,
}

// ═══════════════════════════════════════════════════════════════
// QoS 配置
// ═══════════════════════════════════════════════════════════════

/// QoS 配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QosConfig {
    /// 读 IOPS 限制（0 表示无限制）
    pub read_iops_limit: u64,

    /// 写 IOPS 限制
    pub write_iops_limit: u64,

    /// 读带宽限制（字节/秒）
    pub read_bandwidth_limit: u64,

    /// 写带宽限制
    pub write_bandwidth_limit: u64,

    /// 操作优先级（low / medium / high）
    pub priority: QosPriority,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QosPriority {
    Low,
    Medium,
    High,
}

// ═══════════════════════════════════════════════════════════════
// 数据分层配置
// ═══════════════════════════════════════════════════════════════

/// 数据分层配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TieringConfig {
    /// 是否启用自动分层
    pub enabled: bool,

    /// 分层策略
    pub policies: Vec<TieringPolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TieringPolicy {
    /// 热数据判定阈值（访问次数/天）
    pub hot_threshold: u64,

    /// 冷数据判定阈值（天未访问）
    pub cold_threshold: u64,

    /// 归档判定阈值
    pub archive_threshold: u64,
}

// ═══════════════════════════════════════════════════════════════
// 审计级别
// ═══════════════════════════════════════════════════════════════

/// 审计日志级别
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuditLevel {
    /// 不记录
    None,
    /// 仅记录元数据操作
    MetadataOnly,
    /// 记录所有操作（元数据 + 数据）
    All,
    /// 详细记录（包括读取操作）
    Verbose,
}

// ═══════════════════════════════════════════════════════════════
// 文件大小限制
// ═══════════════════════════════════════════════════════════════

/// 文件大小限制
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileSizeLimit {
    /// 最小文件大小（字节）
    pub min_size: u64,

    /// 最大文件大小（0 表示无限制）
    pub max_size: u64,

    /// 是否允许稀疏文件
    pub allow_sparse: bool,
}

// ═══════════════════════════════════════════════════════════════
// Volume 分配模式
// ═══════════════════════════════════════════════════════════════

/// Volume 分配模式
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VolumeAllocationMode {
    /// 自动分配（默认）
    /// Master 根据 collection 策略自动选择 Volume
    Auto {
        /// 预分配 Volume 数量
        count: u32,
        /// 单个 Volume 大小（字节）
        volume_size: u64,
    },

    /// 手动指定 Volume ID 列表
    /// 只有这些 Volume 会被用于该 Collection
    Manual {
        /// 指定的 Volume ID 列表
        volume_ids: Vec<u64>,
    },

    /// 混合模式
    /// 既有手动指定的 Volume，也允许自动分配补充
    Hybrid {
        /// 手动指定的 Volume ID
        fixed_volume_ids: Vec<u64>,
        /// 自动补充的 Volume 数量
        auto_count: u32,
    },
}

/// Collection 统计信息（运行时计算）
#[derive(Debug, Clone, Default)]
pub struct CollectionStats {
    /// 已用容量（字节）
    pub used_bytes: u64,

    /// 文件总数
    pub file_count: u64,

    /// Volume 总数
    pub volume_count: u32,

    /// 可写 Volume 数
    pub writable_volume_count: u32,

    /// 读 IOPS
    pub read_ops: u64,

    /// 写 IOPS
    pub write_ops: u64,

    /// 读取字节数（累计）
    pub read_bytes: u64,

    /// 写入字节数（累计）
    pub write_bytes: u64,
}
```

### 2.2 属性总览

| 类别 | 属性 | 类型 | 说明 | 实现优先级 |
|------|------|------|------|------------|
| **基本属性** | name | String | 全局唯一名称 | P0 |
| | owner | String | 所有者（用户/团队） | P1 |
| | status | Enum | active/readonly/archived/deleted | P0 |
| | tags | HashMap | 标签（分类、检索） | P1 |
| **存储策略** | storage_policy | StoragePolicy | 副本/EC 模式 | P0 |
| | disk_type | Enum | HDD/SSD/NVMe/Mixed | P0 |
| | compression | CompressionConfig | 压缩算法和级别 | P1 |
| | deduplication | DeduplicationConfig | 数据去重 | P2 |
| **容量配额** | capacity_quota_bytes | u64 | 容量上限 | P0 |
| | file_count_quota | u64 | 文件数上限 | P1 |
| | volume_count | u32 | 预分配 Volume 数量 | P0 |
| **生命周期** | ttl_seconds | u32 | 数据过期时间 | P0 |
| | lifecycle | LifecycleConfig | 自动转换/删除规则 | P1 |
| **安全加密** | encryption | EncryptionConfig | 加密算法和密钥管理 | P1 |
| | worm_enabled | bool | WORM 模式 | P2 |
| **数据分布** | placement | PlacementPolicy | 机架感知/跨DC | P1 |
| **QoS 限流** | qos | QosConfig | IOPS/带宽限制 | P2 |
| **数据分层** | tiering | TieringConfig | 热/温/冷自动分层 | P2 |
| **审计合规** | audit_level | Enum | 审计日志级别 | P2 |
| **文件限制** | file_size_limit | FileSizeLimit | 文件大小范围 | P1 |
| | max_directory_depth | u32 | 目录深度限制 | P2 |
| **Volume 分配** | volume_allocation | VolumeAllocationMode | 自动/手动/混合 | P0 |
| | excluded_volume_ids | Vec\<u64\> | Volume 黑名单 | P1 |

**实现优先级说明**：
- **P0**：核心功能，Phase 1 必须实现
- **P1**：重要功能，Phase 2-3 实现
- **P2**：增强功能，后续迭代

### 2.3 存储位置

- **Collection 元数据**：存储在 Master 的 RocksDB 中，key 为 `collection:{name}`
- **Collection-Volume 映射**：Volume 记录中已有 `collection` 字段，无需额外索引
- **统计信息**：Master 启动时从 Volume 列表聚合，定时更新

### 2.3 元数据存储隔离（推荐方案）

#### 问题背景

文件元数据（inode、目录结构、文件属性）存储在 Filer 的 RocksDB 中。当前所有 Collection 共享同一个 Filer 集群，数据文件混在一起，无法按 Collection 物理隔离。

#### 推荐方案：每个 Collection 独立 RocksDB 实例

```
/data/filer/
├── default/              # Collection: default 的元数据
│   ├── 000001.log
│   ├── 000002.sst
│   └── ...
├── user-uploads/         # Collection: user-uploads 的元数据
│   ├── 000001.log
│   ├── 000002.sst
│   └── ...
└── system-logs/          # Collection: system-logs 的元数据
    ├── 000001.log
    ├── 000002.sst
    └── ...
```

#### 实现设计

```rust
/// 管理多个 Collection 的 ShardStore 实例
pub struct CollectionShardStore {
    /// collection 名称 → ShardStore 实例
    stores: DashMap<String, Arc<ShardStore>>,

    /// 数据根目录
    base_path: String,

    /// 默认 collection（启动时预加载）
    default_collection: String,
}

impl CollectionShardStore {
    /// 获取指定 collection 的 ShardStore（懒加载）
    pub fn get_store(&self, collection: &str) -> Result<Arc<ShardStore>> {
        if let Some(store) = self.stores.get(collection) {
            return Ok(store.clone());
        }

        // 懒加载：第一次访问时打开
        let path = self.collection_path(collection);
        if !path.exists() {
            return Err(PowerFsError::CollectionNotFound(collection));
        }

        let store = Arc::new(ShardStore::open(&path)?);
        self.stores.insert(collection.to_string(), store.clone());
        Ok(store)
    }

    /// 创建新的 collection（Master 调用）
    pub fn create_collection(&self, collection: &str) -> Result<()> {
        let path = self.collection_path(collection);
        std::fs::create_dir_all(&path)?;

        let store = ShardStore::open(&path)?;
        self.stores.insert(collection.to_string(), Arc::new(store));

        info!("Created collection metadata store: {}", collection);
        Ok(())
    }

    /// 删除 collection（需要先清空数据）
    pub fn delete_collection(&self, collection: &str) -> Result<()> {
        // 1. 从内存移除
        self.stores.remove(collection);

        // 2. 删除数据目录
        let path = self.collection_path(collection);
        std::fs::remove_dir_all(&path)?;

        info!("Deleted collection metadata store: {}", collection);
        Ok(())
    }

    fn collection_path(&self, collection: &str) -> PathBuf {
        PathBuf::from(&self.base_path).join(collection)
    }
}
```

#### 请求路由

FUSE 客户端和 Filer API 请求需要携带 collection 参数：

```rust
// Filer API 请求
pub struct GetEntryRequest {
    pub collection: String,
    pub inode: u64,
}

// FUSE 客户端请求
impl MetaShardClient {
    pub async fn get_entry(&self, collection: &str, inode: u64) -> Result<Option<Entry>> {
        let store = self.collection_store.get_store(collection)?;
        store.get_entry(inode)
    }
}
```

#### 配置示例

```toml
# filer.toml
[data]
metadata_dir = "/data/filer"

[collections]
# 启动时预加载的 collection（加速首次访问）
preload = ["default"]

# collection 级别的 RocksDB 配置（可选）
[collections.user-uploads.rocksdb]
cache_size = "1GB"
max_open_files = 10000
compression = "zstd"
```

#### 方案优势

| 特性 | 说明 |
|------|------|
| **物理隔离** | 不同 Collection 的元数据文件完全分离 |
| **独立压缩** | 各 Collection 可配置不同的压缩策略 |
| **磁盘分层** | 热 Collection 放 SSD，冷 Collection 放 HDD |
| **快速删除** | 删除 Collection 直接删除目录，无需逐条清理 |
| **故障隔离** | 一个 Collection 的 RocksDB 损坏不影响其他 |

#### 改动文件

| 文件 | 改动类型 | 说明 |
|------|----------|------|
| `powerfs-filer/src/collection_store.rs` | 新增 | CollectionShardStore 实现 |
| `powerfs-filer/src/shard_store.rs` | 修改 | 增加从指定路径打开的接口 |
| `powerfs-filer/src/filer_server.rs` | 修改 | API 增加 collection 参数 |
| `powerfs-fuse-core/src/meta_shard_client.rs` | 修改 | 请求携带 collection |
| `docker/config/filer-*.toml` | 修改 | 增加 collection 配置 |

---

## 3. API 设计

### 3.1 Master HTTP API

| 端点 | 方法 | 说明 |
|------|------|------|
| `/api/collections` | GET | 列出所有 Collection |
| `/api/collections/{name}` | GET | 获取单个 Collection 详情 |
| `/api/collections` | POST | 创建 Collection |
| `/api/collections/{name}` | PUT | 更新 Collection 配置 |
| `/api/collections/{name}` | DELETE | 删除 Collection（需先清空数据） |
| `/api/collections/{name}/stats` | GET | 获取 Collection 统计信息 |
| `/api/collections/{name}/volumes` | GET | 列出 Collection 下的 Volume |
| `/api/collections/{name}/volumes` | POST | 绑定/解绑 Volume 到 Collection |

#### 创建 Collection — 自动分配 Volume

```json
POST /api/collections
{
  "name": "user-uploads",
  "storage_policy": { "name": "ec-4-2" },
  "disk_type": "ssd",
  "capacity_quota_bytes": 107374182400,
  "volume_allocation": {
    "mode": "auto",
    "count": 5,
    "volume_size": 21474836480
  },
  "description": "用户上传的图片和视频"
}
```

#### 创建 Collection — 手动指定 Volume

```json
POST /api/collections
{
  "name": "logs-archive",
  "storage_policy": { "name": "triple-replication" },
  "disk_type": "hdd",
  "volume_allocation": {
    "mode": "manual",
    "volume_ids": [101, 102, 103, 104, 105]
  },
  "description": "归档日志"
}
```

#### 创建 Collection — 混合模式

```json
POST /api/collections
{
  "name": "mixed-data",
  "storage_policy": { "name": "ec-4-2" },
  "disk_type": "mixed",
  "volume_allocation": {
    "mode": "hybrid",
    "fixed_volume_ids": [201, 202, 203],
    "auto_count": 2
  }
}
```

#### 动态绑定/解绑 Volume

```json
POST /api/collections/user-uploads/volumes
{
  "action": "bind",
  "volume_ids": [301, 302]
}
```

```json
POST /api/collections/user-uploads/volumes
{
  "action": "unbind",
  "volume_ids": [101]
}
```

#### 响应

```json
{
  "code": 200,
  "message": "success",
  "data": {
    "name": "user-uploads",
    "replication": "000",
    "volume_count": 5,
    "created_at": 1722412800,
    "status": "active"
  }
}
```

### 3.2 Volume 创建 API 修改

修改现有 `CreateVolumeRequest`，增加 `collection` 字段：

```protobuf
message CreateVolumeRequest {
  uint64 volume_id = 1;
  uint64 size = 2;
  string collection = 3;       // 新增：指定 collection
  string replication = 4;      // 新增：副本策略
  uint32 ttl = 5;              // 新增：TTL
}
```

### 3.3 gRPC 服务定义

```protobuf
service CollectionService {
  rpc ListCollections(ListCollectionsRequest) returns (ListCollectionsResponse);
  rpc GetCollection(GetCollectionRequest) returns (GetCollectionResponse);
  rpc CreateCollection(CreateCollectionRequest) returns (CreateCollectionResponse);
  rpc UpdateCollection(UpdateCollectionRequest) returns (UpdateCollectionResponse);
  rpc DeleteCollection(DeleteCollectionRequest) returns (DeleteCollectionResponse);
}
```

---

## 4. 核心流程

### 4.1 创建 Collection

```
┌─────────┐    POST /api/collections     ┌─────────┐
│  Admin  │ ──────────────────────────▶ │ Master  │
│  CLI/UI │                              │(Leader) │
└─────────┘                              └────┬────┘
                                              │
                    ┌─────────────────────────┴─────────────────────────┐
                    │ 1. 校验 name 唯一性                                │
                    │ 2. 创建 Collection 元数据                          │
                    │ 3. 根据 volume_allocation 模式选择 Volume:         │
                    │    a. Auto:    选择 Volume Server 节点并创建 Volume │
                    │    b. Manual:  校验指定 Volume ID 存在且可用        │
                    │    c. Hybrid:  校验指定 Volume + 自动补充创建       │
                    │ 4. 排除 excluded_volume_ids 中的 Volume            │
                    │ 5. 通过 Raft 共识持久化                             │
                    │ 6. 返回创建结果                                    │
                    └───────────────────────────────────────────────────┘
```

**关键点**：
- Volume 创建需要通过 Raft 共识，确保一致性
- 选择 Volume Server 节点时考虑：机架感知、磁盘类型、负载均衡
- Manual 模式下校验 Volume 是否存在、是否已绑定到其他 Collection
- 创建失败时需要清理已创建的 Volume

### 4.2 Volume 分配逻辑

```rust
impl Master {
    /// 为写入请求分配 Volume
    pub async fn assign_volume(
        &self,
        collection: &str,
    ) -> Result<(Fid, Vec<DataNodeInfo>)> {
        let coll = self.get_collection(collection)?;

        // 检查黑名单
        let excluded = &coll.excluded_volume_ids;

        match &coll.volume_allocation {
            // 自动模式：从可写 Volume 中选择
            VolumeAllocationMode::Auto { .. } => {
                self.find_writable_volume(collection, excluded)
            }

            // 手动模式：只在指定 Volume 中查找
            VolumeAllocationMode::Manual { volume_ids } => {
                self.find_in_specified_volumes(collection, volume_ids, excluded)
            }

            // 混合模式：先查指定的，不够再自动分配
            VolumeAllocationMode::Hybrid { fixed_volume_ids, .. } => {
                if let Some(v) = self.find_in_specified_volumes(
                    collection, fixed_volume_ids, excluded
                )? {
                    return Ok(v);
                }
                // 指定的 Volume 都满了，回退到自动分配
                self.find_writable_volume(collection, excluded)
            }
        }
    }

    /// 从指定 Volume ID 列表中查找可写 Volume
    fn find_in_specified_volumes(
        &self,
        collection: &str,
        volume_ids: &[u64],
        excluded: &[u64],
    ) -> Result<Option<(Fid, Vec<DataNodeInfo>)>> {
        for vid in volume_ids {
            // 跳过黑名单
            if excluded.contains(vid) {
                continue;
            }
            if let Some(vol) = self.volumes.get(vid) {
                if vol.collection == collection && vol.is_writable() {
                    return Ok(Some((Fid::new(*vid), vol.replica_nodes())));
                }
            }
        }
        Ok(None)
    }
}
```

### 4.3 FUSE 挂载选择 Collection

```
┌─────────────┐  mount --collection=user-uploads  ┌─────────┐
│ FUSE Client │ ───────────────────────────────▶ │ Master  │
└─────────────┘                                    └────┬────┘
       │                                                │
       │           ┌────────────────────────────────────┘
       │           │ assign_volume(replication, collection)
       │           ▼
       │    ┌─────────────────────────────────────────┐
       │    │ 1. 查找 collection=user-uploads 的 Volume │
       │    │ 2. 选择有可用空间的 Volume                │
       │    │ 3. 分配 file_key                         │
       │    │ 4. 返回 Fid 和 Volume Server 地址        │
       │    └─────────────────────────────────────────┘
       │
       └────────◀───────────────────────────────────────
                   Fid{volume_id, cookie, file_key}
                   locations: [Volume Server]
```

### 4.4 Collection 容量检查

在 `assign_volume` 时增加容量检查：

```rust
pub async fn assign_volume(&self, collection: &str) -> Result<(Fid, Vec<DataNodeInfo>)> {
    // 1. 检查 collection 是否存在
    let collection_info = self.get_collection(collection)?;

    // 2. 检查容量配额
    let stats = self.compute_collection_stats(collection)?;
    if collection_info.capacity_quota_bytes > 0 {
        if stats.used_bytes >= collection_info.capacity_quota_bytes {
            return Err(PowerFsError::CapacityExhausted(collection));
        }
    }

    // 3. 根据 volume_allocation 模式分配 Volume
    //    (见 4.2 Volume 分配逻辑)
}
```

---

## 5. 前端设计

### 5.1 Collection 管理页面

**菜单位置**：存储 → Collection 管理

**页面布局**：

```
┌────────────────────────────────────────────────────────────────────┐
│  Collection 管理                                    [+ 新建] [刷新] │
├────────────────────────────────────────────────────────────────────┤
│  ┌──────────────────────────────────────────────────────────────┐ │
│  │ 名称            │ 副本 │ 容量配额 │ 已用 │ 文件数 │ 状态    │ │
│  ├──────────────────────────────────────────────────────────────┤ │
│  │ default         │ 000  │ 无限制   │ 2.1GB│ 1,234 │ ✅ 正常 │ │
│  │ user-uploads    │ 000  │ 100GB    │ 45GB │ 89,123│ ✅ 正常 │ │
│  │ system-logs     │ 000  │ 50GB     │ 48GB │ 2,345 │ ⚠️ 即将满│ │
│  │ cache           │ 000  │ 200GB    │ 12GB │ 56,789│ ✅ 正常 │ │
│  └──────────────────────────────────────────────────────────────┘ │
│                                                                    │
│  [ 详情 ] [ Volume 列表 ] [ 监控 ] [ 编辑 ] [ 删除 ]               │
└────────────────────────────────────────────────────────────────────┘
```

### 5.2 Collection 详情页

点击"详情"进入：

```
┌────────────────────────────────────────────────────────────────────┐
│  Collection: user-uploads                                          │
├────────────────────────────────────────────────────────────────────┤
│  基本信息                                                          │
│  ┌────────────────────────────────────────────────────────────┐   │
│  │ 名称        │ user-uploads                                 │   │
│  │ 描述        │ 用户上传的图片和视频                          │   │
│  │ 副本策略    │ 000（单副本）                                 │   │
│  │ 容量配额    │ 100 GB                                       │   │
│  │ 预分配 Volume│ 5 个                                         │   │
│  │ 创建时间    │ 2026-07-31 10:30:00                          │   │
│  └────────────────────────────────────────────────────────────┘   │
│                                                                    │
│  容量统计                                                          │
│  ┌────────────────────────────────────────────────────────────┐   │
│  │ 已用容量    │ ████████████████░░░░░░░░ 45 GB / 100 GB (45%)│   │
│  │ 文件总数    │ 89,123                                        │   │
│  │ Volume 数量 │ 5（可写 5）                                   │   │
│  └────────────────────────────────────────────────────────────┘   │
│                                                                    │
│  I/O 统计（最近 1 小时）                                            │
│  ┌────────────────────────────────────────────────────────────┐   │
│  │ 读 IOPS     │ 1,234 ops/s                                   │   │
│  │ 写 IOPS     │ 567 ops/s                                     │   │
│  │ 读带宽      │ 12.3 MB/s                                     │   │
│  │ 写带宽      │ 5.6 MB/s                                      │   │
│  └────────────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────┘
```

### 5.3 新建 Collection 对话框

```
┌────────────────────────────────────────────────────────────────────┐
│  新建 Collection                                              [X] │
├────────────────────────────────────────────────────────────────────┤
│  名称 *           [                                          ]    │
│  描述             [                                          ]    │
│  副本策略 *       [ 000                          ▼]                 │
│  容量配额         [          ] GB（0 表示无限制）                   │
│  预分配 Volume 数量[    5     ] 个                                 │
│  单 Volume 大小   [    20    ] GB                                  │
│  磁盘类型         [ SSD      ▼]                                    │
│  TTL              [    0     ] 秒（0 表示永不过期）                 │
│                                                                    │
│                                              [ 取消 ] [ 创建 ]     │
└────────────────────────────────────────────────────────────────────┘
```

---

## 6. CLI 设计

### 6.1 命令列表

```bash
# 列出所有 collection
powerfs collection list

# 查看 collection 详情
powerfs collection info user-uploads

# 创建 collection
powerfs collection create user-uploads \
  --replication=000 \
  --capacity=100GB \
  --volume-count=5 \
  --volume-size=20GB \
  --description="用户上传的图片和视频"

# 更新 collection 配置
powerfs collection update user-uploads --capacity=200GB

# 删除 collection（需要先清空数据）
powerfs collection delete user-uploads --force

# 查看 collection 下的 volume
powerfs collection volumes user-uploads

# 查看 collection 统计
powerfs collection stats user-uploads
```

### 6.2 输出示例

```bash
$ powerfs collection list
NAME            REPLICATION  CAPACITY    USED      FILES    VOLUMES  STATUS
default         000          unlimited   2.1 GB    1,234    3        active
user-uploads    000          100 GB      45 GB     89,123   5        active
system-logs     000          50 GB       48 GB     2,345    2        warning
cache           000          200 GB      12 GB     56,789   4        active

$ powerfs collection create user-uploads --capacity=100GB --volume-count=5
Creating collection "user-uploads"...
  ✓ Created 5 volumes on nodes: volume-1, volume-2, volume-3
  ✓ Collection "user-uploads" created successfully

$ powerfs collection info user-uploads
Name:            user-uploads
Description:     用户上传的图片和视频
Replication:     000 (single replica)
Capacity Quota:  100 GB
Used:            45 GB (45%)
Files:           89,123
Volumes:         5 (5 writable)
Created:         2026-07-31 10:30:00
Status:          active

Read IOPS:       1,234 ops/s
Write IOPS:      567 ops/s
Read Bandwidth:  12.3 MB/s
Write Bandwidth: 5.6 MB/s
```

---

## 7. 监控指标

### 7.1 Prometheus 指标

```prometheus
# Collection 元数据
powerfs_collection_count{status="active"} 4

# Collection 容量
powerfs_collection_capacity_bytes{name="user-uploads"} 107374182400
powerfs_collection_used_bytes{name="user-uploads"} 48318382080
powerfs_collection_file_count{name="user-uploads"} 89123
powerfs_collection_volume_count{name="user-uploads"} 5

# Collection I/O
powerfs_collection_read_ops_total{name="user-uploads"} 12345678
powerfs_collection_write_ops_total{name="user-uploads"} 5678901
powerfs_collection_read_bytes_total{name="user-uploads"} 123456789012
powerfs_collection_write_bytes_total{name="user-uploads"} 56789012345
```

### 7.2 告警规则

```yaml
groups:
  - name: collection
    rules:
      - alert: CollectionCapacityWarning
        expr: |
          powerfs_collection_used_bytes / powerfs_collection_capacity_bytes > 0.8
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Collection {{ $labels.name }} capacity usage > 80%"
          description: "{{ $labels.name }} used {{ $value | humanize }}B / {{ $labels.capacity }}"

      - alert: CollectionCapacityCritical
        expr: |
          powerfs_collection_used_bytes / powerfs_collection_capacity_bytes > 0.95
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Collection {{ $labels.name }} capacity usage > 95%"

      - alert: CollectionNoWritableVolume
        expr: |
          powerfs_collection_volume_count{name="user-uploads"} - powerfs_collection_writable_volume_count{name="user-uploads"} == powerfs_collection_volume_count{name="user-uploads"}
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Collection {{ $labels.name }} has no writable volumes"
```

---

## 8. 实现计划

### Phase 1：基础设施（预计 3 天）

1. **数据结构定义**
   - `CollectionInfo` / `CollectionStats` 结构体
   - RocksDB 存储层

2. **Master API**
   - `/api/collections` CRUD 端点
   - `get_collection()` / `list_collections()` 方法

3. **Volume 创建修改**
   - `CreateVolumeRequest` 增加 `collection` 字段
   - Volume Server 解析并使用

### Phase 2：容量管理（预计 2 天）

1. **容量检查**
   - `assign_volume()` 增加配额检查
   - 配额超限错误处理

2. **统计聚合**
   - 定时任务聚合 Collection 统计
   - Prometheus 指标导出

### Phase 3：前端界面（预计 2 天）

1. **Collection 管理页**
   - 列表页 + 详情页
   - 新建/编辑对话框

2. **Volume 页关联**
   - Volume 详情显示所属 Collection
   - Collection 详情显示 Volume 列表

### Phase 4：CLI 工具（预计 1 天）

1. **命令实现**
   - `collection list/info/create/update/delete`
   - `collection volumes/stats`

### Phase 5：测试与文档（预计 1 天）

1. **单元测试**
   - Collection CRUD 测试
   - 容量检查测试

2. **集成测试**
   - 创建 Collection 并写入数据
   - 容量超限拒绝写入

3. **文档**
   - 用户文档：Collection 使用指南
   - 运维文档：Collection 管理手册

---

## 9. 风险与注意事项

### 9.1 兼容性

- **现有数据**：所有现有 Volume 属于 "default" collection
- **配置迁移**：启动时自动创建 "default" collection（如果不存在）
- **API 版本**：保持现有 API 向后兼容，新字段设为可选

### 9.2 性能考虑

- **统计聚合**：定时任务而非每次请求实时计算，避免锁竞争
- **容量检查**：在 `assign_volume` 时检查，而非每次写入，减少开销
- **索引优化**：Volume 按 collection 建立内存索引，加速查找

### 9.3 安全考虑

- **权限控制**：Collection 级别的 RBAC（后续迭代）
- **删除保护**：删除前检查是否还有数据
- **配额超限**：拒绝新写入但允许读取和删除

---

## 10. 存储策略推荐

### 10.1 冗余模式对比

| 模式 | 表示方式 | 存储开销 | 容错能力 | 适用场景 |
|------|----------|----------|----------|----------|
| **无冗余** | `Replication { copies: 1 }` | 1x | 0 节点故障 | 缓存、临时文件 |
| **双副本** | `Replication { copies: 2 }` | 2x | 1 节点故障 | 重要日志 |
| **三副本** | `Replication { copies: 3 }` | 3x | 2 节点故障 | 核心业务数据 |
| **EC 4+1** | `ErasureCoding { data: 4, parity: 1 }` | 1.25x | 1 节点故障 | 温数据、备份 |
| **EC 4+2** | `ErasureCoding { data: 4, parity: 2 }` | 1.5x | 2 节点故障 | 用户上传、文档 |
| **EC 8+2** | `ErasureCoding { data: 8, parity: 2 }` | 1.25x | 2 节点故障 | 大文件、视频 |
| **EC 8+4** | `ErasureCoding { data: 8, parity: 4 }` | 1.5x | 4 节点故障 | 归档数据 |

### 10.2 场景与策略推荐

| 场景 | 推荐策略 | 存储开销 | 容错 | 选择理由 |
|------|----------|----------|------|----------|
| 用户上传图片/文档 | EC 4+2 | 1.5x | 2 节点 | 数据重要但量不大，EC 效率高 |
| 视频存储 | EC 8+2 | 1.25x | 2 节点 | 大文件，数据块多，存储效率高 |
| 系统日志 | 双副本 | 2x | 1 节点 | 日志重要性中等，副本模式写入延迟低 |
| 缓存数据 | 无冗余 | 1x | 0 节点 | 可重新生成，成本优先 |
| 核心业务数据 | 三副本 | 3x | 2 节点 | 最重要数据，读取性能最好，修复最快 |
| 归档数据 | EC 8+4 | 1.5x | 4 节点 | 长期存储，高容错，读少写少 |

### 10.3 策略选择决策树

```
开始
  │
  ├── 数据是否可重新生成？
  │   ├── 是 → 缓存/临时 → 无冗余
  │   └── 否 → 继续
  │
  ├── 数据重要性？
  │   ├── 关键（丢失则业务中断）→ 三副本
  │   ├── 重要 → 继续
  │   └── 一般 → 双副本
  │
  ├── 数据量级？
  │   ├── 大文件（视频/备份）→ EC 8+2
  │   ├── 中等（文档/图片）→ EC 4+2
  │   └── 小文件 → 副本模式
  │
  └── 访问频率？
      ├── 热数据 → 副本模式（读性能好）
      ├── 温数据 → EC 4+2
      └── 冷数据 → EC 8+4（高容错）
```

### 10.4 EC 实现关键点

**写入流程**：
1. 根据策略分片（data_shards + parity_shards）
2. 计算校验块
3. 并行写入到不同 Volume Server 节点

**读取流程**：
- 正常：直接从数据分片读取
- 降级：从剩余健康分片重建数据
- 修复：后台触发分片重建

**关键参数**：
- `min_write_nodes`：写入时最少成功节点数
- `algorithm`：编码算法选择（reed-solomon 通用，isa-l Intel 优化）

---

## 11. 多接口统一 Collection 方案

### 11.1 设计原则

Collection 是**底层数据分布的唯一抽象**，FUSE / S3 / KV 只是不同的访问接口，共享同一个 Collection 的 Volume 池和存储策略。

```
┌─────────────────────────────────────────────────────────────┐
│                    Collection: user-data                     │
│              (EC 4+2, SSD, Volume 101-110)                  │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐                     │
│  │  FUSE   │  │   S3    │  │   KV    │  ← 访问接口层        │
│  │ Client  │  │ Gateway │  │ Service │                     │
│  └────┬────┘  └────┬────┘  └────┬────┘                     │
│       └────────────┼────────────┘                           │
│                    │                                         │
│            ┌───────▼────────┐                                │
│            │  Filer (inode) │  ← 统一元数据层                │
│            └───────┬────────┘                                │
│                    │                                         │
│            ┌───────▼────────┐                                │
│            │ Master          │  ← 统一 Volume 分配            │
│            │ assign_volume   │                                │
│            │ (collection)    │                                │
│            └───────┬────────┘                                │
│                    │                                         │
│            ┌───────▼────────┐                                │
│            │ Volume Server   │  ← 物理存储                    │
│            │ Vol 101-110     │                                │
│            └────────────────┘                                │
└─────────────────────────────────────────────────────────────┘
```

### 11.2 当前状态分析

#### S3 路径

| 环节 | 当前行为 | 问题 |
|------|----------|------|
| `S3Handler::create_bucket` | 硬编码 `replication="001"` | 不支持指定策略 |
| `BucketManager::create_bucket` | 硬编码 `collection="default"` | 不支持指定 collection |
| `BucketInfo` | 有 `collection` 字段 | 字段存在但永远为 "default" |
| `S3Handler::put_object` | 直接用 `bucket_info.volume_ids[0]` | 不动态分配 Volume，只用建桶时分配的第一个 |

#### KV 路径

| 环节 | 当前行为 | 问题 |
|------|----------|------|
| `KvCacheServiceImpl::put_block` | 硬编码 `assign_volume("001", "default")` | 不支持指定 collection |
| KV Session | 无 collection 概念 | Session 不关联 collection |

#### 关键发现

Master 的 `assign_volume(replication, collection)` **已经支持 collection 参数**，只是调用方都硬编码了 "default"。

S3 已有 inode 概念（通过 `MetaShardManager`），说明 S3 对象本质就是"文件"。KV 缺少这一层。

### 11.3 统一映射关系

```
Collection: "user-data"
  │
  ├── FUSE 挂载点: /mnt/powerfs
  │     └── mount --collection=user-data
  │
  ├── S3 Bucket: "photos"
  │     └── BucketInfo.collection = "user-data"
  │     └── S3 对象 = 文件系统中的文件 (inode 在 Filer 中)
  │
  └── KV Session: "inference-1"
        └── SessionMeta.collection = "user-data"
        └── KV block = 文件系统中的特殊文件 (inode 在 Filer 中)
```

数据分布示例：

```
Collection: "user-data" (EC 4+2, SSD, Volume 101-110)
├── FUSE 文件
│   ├── /photos/2024/01/cat.jpg     → Vol 101, inode=5001
│   └── /docs/readme.pdf            → Vol 103, inode=5002
│
├── S3 对象（同一 Collection 的文件）
│   ├── s3://photos/cat.jpg         → Vol 101, inode=5001  (同一个文件)
│   └── s3://docs/readme.pdf        → Vol 103, inode=5002
│
└── KV 数据块（同一 Collection 的特殊文件）
    ├── /kv/sess-1/block_0          → Vol 105, inode=6001
    └── /kv/sess-1/block_1          → Vol 107, inode=6002
```

### 11.4 S3 改动方案

S3 已经通过 `MetaShardManager` 管理 inode，只需小幅调整：

**a) 创建 Bucket 时指定 collection**：

```rust
// bucket_manager.rs — 修改 create_bucket
pub async fn create_bucket(
    &self,
    bucket: &str,
    replication: &str,
    collection: &str,  // 新增参数
) -> Result<BucketInfo> {
    // ...
    let (fid, nodes) = self
        .master_api
        .assign_volume(replication, collection)  // 使用指定 collection
        .await?;

    let bucket_info = BucketInfo {
        collection: collection.to_string(),  // 记录 collection
        // ...
    };
}
```

**b) put_object 用 collection 动态分配 Volume**：

```rust
// s3_handler.rs — 修改 put_object
pub async fn put_object(&self, bucket: &str, key: &str, data: &[u8]) -> Response {
    let bucket_info = self.bucket_manager.get_bucket(bucket).await?;

    // 用 bucket 关联的 collection 动态分配（不再固定 volume_ids[0]）
    let (fid, nodes) = self.master_api
        .assign_volume(&bucket_info.replication, &bucket_info.collection)
        .await?;

    let volume_id = fid.volume_id.0;
    let server_addr = format!("{}:{}", nodes[0].address, nodes[0].grpc_port);

    // 写入 Volume Server
    self.volume_client_pool
        .write_needle(&server_addr, volume_id, file_key, data)
        .await?;

    // 元数据写入 Filer（已有 inode 概念）
    self.meta_shard_manager
        .put_object_entry(root_inode, key, size, &fid_str, volume_id, &etag)
        .await?;
}
```

### 11.5 KV 改动方案

KV 需要增加文件层映射：

**a) CreateSession 关联 collection**：

```protobuf
// proto/powerfs.proto — CreateSessionRequest 增加 collection
message CreateSessionRequest {
    string session_id = 1;
    string namespace_id = 2;
    string owner_id = 3;
    string model_name = 4;
    // ... 现有字段 ...
    string collection = 10;  // 新增：指定 collection
}
```

```rust
// kv_cache_persist.rs — SessionMeta 增加 collection
pub struct SessionMeta {
    pub session_id: String,
    pub model_name: String,
    // ...
    pub collection: String,  // 新增
}
```

**b) PutBlock 用 session 关联的 collection**：

```rust
// kv_cache_service.rs — 修改 put_block
async fn put_block(&self, req: PutBlockRequest) -> Result<PutBlockResponse> {
    let session = self.engine.get_session(&req.session_id)?;
    let collection = &session.collection;

    // 1. 用 collection 分配 Volume
    let (fid, nodes) = self.master
        .assign_volume("001", collection)
        .await?;

    // 2. 写入 Volume Server（数据）
    self.volume_client_pool
        .write_needle(&server_addr, volume_id, file_key, &data)
        .await?;

    // 3. 在 Filer 中创建 inode 记录（元数据）
    //    KV block 作为特殊文件存储
    let kv_path = format!("/kv/{}/block_{}", session_id, block_id);
    self.filer.create_file(&kv_path, fid, size)?;
}
```

### 11.6 方案优势

| 优势 | 说明 |
|------|------|
| **统一存储策略** | 一个 Collection 的 EC/副本/压缩/加密对所有接口生效 |
| **统一 Volume 池** | 三个接口共享 Volume 101-110，提高利用率 |
| **数据互通** | FUSE 写的文件，S3 可以读；KV block 可以通过 FUSE 查看 |
| **统一监控** | Collection 级别的容量/IOPS 统计覆盖所有接口 |
| **简化实现** | 只需维护一套 Collection → Volume 映射 |

### 11.7 改动文件清单

| 文件 | 改动 | 优先级 |
|------|------|--------|
| `powerfs-filer/src/bucket_manager.rs` | `create_bucket` 增加 collection 参数 | P0 |
| `powerfs-filer/src/s3_handler.rs` | `create_bucket` 传递 collection | P0 |
| `powerfs-filer/src/s3_handler.rs` | `put_object` 用 collection 动态分配 Volume | P0 |
| `powerfs-master/src/kv_cache_service.rs` | `create_session` 增加 collection 参数 | P1 |
| `powerfs-master/src/kv_cache_service.rs` | `put_block` 用 session 的 collection | P1 |
| `powerfs-core/src/kv_cache_persist.rs` | SessionMeta 增加 collection | P1 |
| `proto/powerfs.proto` | CreateSessionRequest 增加 collection | P1 |
| `powerfs-filer/src/metadata_store.rs` | BucketInfo 已有 collection 字段（无需改） | - |

---

## 12. 附录：相关文件

| 文件 | 改动类型 | 说明 |
|------|----------|------|
| `powerfs-master/src/collection.rs` | 新增 | Collection 元数据和统计 |
| `powerfs-master/src/master.rs` | 修改 | 增加 Collection 管理 API |
| `powerfs-master/src/storage.rs` | 修改 | Collection 存储层 |
| `powerfs-volume/src/server.rs` | 修改 | CreateVolume 支持 collection 参数 |
| `powerfs-volume/src/proto/powerfs.proto` | 修改 | CreateVolumeRequest 增加 collection |
| `powerfs-cli/src/collection_cmd.rs` | 新增 | Collection CLI 命令 |
| `powerfs-monitor-frontend/src/pages/Collections/` | 新增 | 前端管理页面 |
| `docker/config/*.toml` | 修改 | Volume 启动配置增加 collection |