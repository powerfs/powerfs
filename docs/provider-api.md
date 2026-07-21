# PowerFS Provider 接口 API 文档

## 概述

PowerFS 通过 Provider 接口层实现模块解耦，应用层通过统一的 trait 接口访问底层服务。所有接口定义在 `powerfs-common/src/traits.rs`，返回类型统一为 `Result<T>`（即 `std::result::Result<T, PowerFsError>`）。

**设计原则**：
- 所有接口均为异步（`async_trait`），支持并发调用
- 所有方法返回 `Result<T>`，错误处理统一
- 接口实现需满足 `Send + Sync`，支持跨线程共享
- 通过 Feature Flags 支持按需启用模块

---

## 一、基础类型定义

### 1.1 标识类型

#### VolumeId

```rust
pub struct VolumeId(pub u32);
```

**用途**：Volume唯一标识

**特性**：
- 实现 `Display`、`From<u32>`、`FromStr`
- 支持序列化/反序列化

#### NodeId

```rust
pub struct NodeId(pub String);
```

**用途**：节点唯一标识

**命名规范**：`"node-{uuid}"` 或 `"master-{id}"`

#### NeedleId

```rust
pub struct NeedleId(pub u64);
```

**用途**：文件块（Needle）唯一标识

#### Collection

```rust
pub struct Collection(pub String);
```

**用途**：存储策略分组，区分不同的副本策略、TTL等

**默认值**：`"default"`

#### Fid

```rust
pub struct Fid {
    pub volume_id: VolumeId,   // VolumeID
    pub cookie: u64,           // 一致性校验值
    pub file_key: u64,         // 文件Key
}
```

**用途**：文件唯一标识符，包含定位所需的完整信息

**格式**：`"{volume_id},{cookie},{file_key}"`（如 `"1,123456789,987654321"`）

**解析方法**：`Fid::from_string(s: &str) -> Result<Self, String>`

---

### 1.2 状态类型

#### VolumeState

```rust
pub enum VolumeState {
    Creating,      // 创建中
    Available,     // 可用（默认）
    Full,          // 已满
    ReadOnly,      // 只读
    Deleting,      // 删除中
}
```

#### NodeState

```rust
pub enum NodeState {
    Init,           // 初始化中
    Ready,          // 就绪，等待心跳确认
    Healthy,        // 健康（默认）
    SoftError,      // 软错误（可恢复）
    FailSlow,       // 慢故障（响应延迟）
    Degraded,       // 降级（只读模式）
    Fault,          // 故障（不可用）
    Maintenance,    // 维护中
    Unavailable,    // 失联（心跳超时）
}
```

**状态判断方法**：

| 方法 | 返回 true 的状态 | 用途 |
|------|----------------|------|
| `is_assignable()` | Ready, Healthy, SoftError, FailSlow | 是否可分配新Volume |
| `is_readable()` | Ready, Healthy, SoftError, FailSlow, Degraded | 是否可服务读请求 |
| `is_writable()` | Ready, Healthy, SoftError, FailSlow | 是否可服务写请求 |
| `is_unhealthy()` | Init, Degraded, Fault, Unavailable, Maintenance | 是否健康（调度跳过） |

---

### 1.3 配置类型

#### ReplicaPlacement

```rust
pub struct ReplicaPlacement {
    pub copies: u32,           // 副本数量
    pub same_rack: bool,       // 是否同机架
    pub same_data_center: bool, // 是否同数据中心
}
```

**解析方法**：`ReplicaPlacement::from_string(s: &str) -> Result<Self, String>`

**SeaweedFS格式解析**：

| 输入 | 含义 |
|------|------|
| `"000"` | 1副本（原始） |
| `"001"` | 2副本（不同机架、不同数据中心） |
| `"010"` | 2副本（同机架、不同数据中心） |
| `"100"` | 2副本（同数据中心） |
| `"011"` | 3副本（1同机架 + 1不同数据中心） |
| `"111"` | 3副本（1同数据中心 + 1同机架 + 1不同数据中心） |
| `"3"` | 3副本（简单数字格式） |

---

## 二、错误类型

所有方法返回 `Result<T>`，错误类型定义在 [error.rs](file:///home/portion/powerfs/powerfs-common/src/error.rs)：

### 2.1 错误枚举

```rust
pub enum PowerFsError {
    Io(std::io::Error),
    SerdeJson(serde_json::Error),
    TonicTransport(tonic::transport::Error),
    TonicStatus(Box<tonic::Status>),
    ProstDecode(prost::DecodeError),
    ProstEncode(prost::EncodeError),
    UuidParse(uuid::Error),
    AddrParse(std::net::AddrParseError),
    Raft(Box<raft::Error>),
    
    VolumeNotFound(VolumeId),
    NeedleNotFound(NeedleId),
    VolumeExists(VolumeId),
    InvalidVolumeState(String),
    InvalidMasterState(String),
    InvalidRequest(String),
    FileNotFound(String),
    DirectoryNotFound(String),
    FileExists(String),
    PermissionDenied,
    ChecksumMismatch,
    OutOfSpace,
    NotLeader,
    Timeout,
    ConnectionRefused,
    QuorumNotReached,
    RateLimited,
    Internal(String),
    PathTooLong,
    InvalidPath(String),
    Storage(String),
}
```

### 2.2 错误分类与重试策略

| 错误类型 | 说明 | 是否可重试 | 重试策略 |
|---------|------|-----------|---------|
| `VolumeNotFound(VolumeId)` | Volume不存在 | 否 | 直接返回 |
| `NeedleNotFound(NeedleId)` | 文件块不存在 | 否 | 直接返回 |
| `VolumeExists(VolumeId)` | Volume已存在 | 否 | 直接返回 |
| `InvalidVolumeState(String)` | 无效的Volume状态 | 否 | 直接返回 |
| `InvalidMasterState(String)` | 无效的Master状态 | 否 | 直接返回 |
| `InvalidRequest(String)` | 无效请求 | 否 | 直接返回 |
| `FileNotFound(String)` | 文件不存在 | 否 | 直接返回 |
| `DirectoryNotFound(String)` | 目录不存在 | 否 | 直接返回 |
| `FileExists(String)` | 文件已存在 | 否 | 直接返回 |
| `PermissionDenied` | 权限拒绝 | 否 | 直接返回 |
| `ChecksumMismatch` | 校验和不匹配 | 否 | 直接返回 |
| `OutOfSpace` | 空间不足 | 否 | 直接返回 |
| `NotLeader` | 非Leader节点 | 是 | 切换Leader后重试 |
| `Timeout` | 操作超时 | 是 | 指数退避重试 |
| `ConnectionRefused` | 连接拒绝 | 是 | 指数退避重试 |
| `QuorumNotReached` | 未达到Quorum | 是 | 等待后重试 |
| `RateLimited` | 限流 | 是 | 等待5秒后重试 |
| `Internal(String)` | 内部错误 | 是 | 指数退避重试 |
| `PathTooLong` | 路径过长 | 否 | 直接返回 |
| `InvalidPath(String)` | 无效路径 | 否 | 直接返回 |
| `Storage(String)` | 存储错误 | 是 | 指数退避重试 |

### 2.3 错误分类方法

```rust
impl PowerFsError {
    pub fn error_kind(&self) -> ErrorKind {
        // 返回: NonRetryable / Retryable / LeaderChanged / RateLimited(Duration)
    }
}
```

---

## 三、VolumeProvider

**用途**：Volume分配、路由查询、节点心跳、Volume列表管理

**实现**：`MasterNode` ([powerfs-master/src/provider_impl.rs](file:///home/portion/powerfs/powerfs-master/src/provider_impl.rs))

**调用链**：`FUSE/KV/S3` → `FuseVolumeProvider` → gRPC → `MasterNode`

### 3.1 数据结构

#### Location

```rust
pub struct Location {
    pub url: String,           // Volume节点URL（内网）
    pub public_url: String,    // 公网URL（可选）
    pub grpc_port: u32,        // gRPC端口
    pub data_center: String,   // 数据中心标识
}
```

#### NodeStats

```rust
pub struct NodeStats {
    pub total_space: u64,      // 总空间（字节）
    pub used_space: u64,       // 已用空间（字节）
    pub cpu_usage: f64,        // CPU使用率（0-100）
    pub memory_usage: f64,     // 内存使用率（0-100）
    pub volume_count: u32,     // 管理的Volume数量
}
```

#### VolumeFilters

```rust
pub struct VolumeFilters {
    pub collection: Option<Collection>,  // 按Collection过滤
    pub state: Option<String>,           // 按状态过滤（如"available"）
    pub node_id: Option<NodeId>,         // 按节点ID过滤
}
```

#### VolumeInfo

```rust
pub struct VolumeInfo {
    pub id: VolumeId,
    pub node_id: NodeId,
    pub collection: Collection,
    pub size: u64,             // 容量限制（字节）
    pub used: u64,             // 已用空间（字节）
    pub replica_count: u32,    // 副本数量
    pub ttl: Ttl,              // 过期时间
    pub disk_type: DiskType,   // 磁盘类型
    pub state: VolumeState,    // 状态
    pub created_at: DateTime<Utc>,
    pub modified_at: DateTime<Utc>,
    pub next_file_key: u64,    // 下一文件Key
}
```

### 3.2 方法

#### assign_volume

```rust
async fn assign_volume(
    &self,
    collection: &str,       // Collection名称
    replication: &str,      // 副本策略（如"000"表示3副本）
) -> Result<(Fid, Vec<Location>)>;
```

**功能**：为指定Collection分配新Volume，返回文件ID和副本位置列表

**参数**：
- `collection`: Collection名称，用于区分不同存储策略
- `replication`: 副本策略，格式为"000"表示3副本，"00"表示2副本，或简单数字"3"

**返回值**：
- `Fid`: 文件ID（包含VolumeID、cookie、file_key）
- `Vec<Location>`: Volume节点位置列表（副本位置）

**错误场景**：
- `NotLeader`: 当前节点不是Leader
- `QuorumNotReached`: Raft未达到Quorum
- `OutOfSpace`: 集群空间不足
- `InvalidRequest`: 无效的副本策略格式

**使用示例**：
```rust
let (fid, locations) = volume_provider
    .assign_volume("default", "000")
    .await?;
println!("Assigned FID: {}, locations: {:?}", fid, locations);
// 输出: Assigned FID: 1,123456789,987654321, locations: [Location { url: "192.168.1.100", ... }]
```

---

#### lookup_volume

```rust
async fn lookup_volume(&self, volume_id: VolumeId) -> Result<Vec<Location>>;
```

**功能**：查询Volume的所有副本节点位置

**参数**：
- `volume_id`: VolumeID

**返回值**：
- `Vec<Location>`: Volume节点位置列表

**错误场景**：
- `VolumeNotFound`: Volume不存在
- `ConnectionRefused`: 无法连接到Master
- `NotLeader`: 当前节点不是Leader

**使用示例**：
```rust
let locations = volume_provider
    .lookup_volume(VolumeId(1))
    .await?;
for loc in locations {
    println!("Node: {}:{} (DC: {})", loc.url, loc.grpc_port, loc.data_center);
}
```

---

#### heartbeat

```rust
async fn heartbeat(&self, node_id: &NodeId, stats: &NodeStats) -> Result<()>;
```

**功能**：节点心跳上报，Master更新节点状态

**参数**：
- `node_id`: 节点ID
- `stats`: 节点状态统计

**返回值**：无

**错误场景**：
- `NotLeader`: 当前节点不是Leader
- `InvalidMasterState`: Master状态异常

**使用示例**：
```rust
let stats = NodeStats {
    total_space: 1024 * 1024 * 1024 * 100,  // 100GB
    used_space: 10 * 1024 * 1024 * 1024,     // 10GB
    cpu_usage: 25.5,
    memory_usage: 60.0,
    volume_count: 50,
};
volume_provider.heartbeat(&NodeId("node-1".to_string()), &stats).await?;
```

---

#### list_volumes

```rust
async fn list_volumes(&self, filters: &VolumeFilters) -> Result<Vec<VolumeInfo>>;
```

**功能**：列出Volume列表（支持多条件过滤）

**参数**：
- `filters`: 过滤条件（可选）

**返回值**：
- `Vec<VolumeInfo>`: Volume信息列表

**错误场景**：
- `ConnectionRefused`: 无法连接到Master

**使用示例**：
```rust
// 列出default collection下所有可用的Volume
let filters = VolumeFilters {
    collection: Some(Collection("default".to_string())),
    state: Some("available".to_string()),
    node_id: None,
};
let volumes = volume_provider.list_volumes(&filters).await?;

// 列出指定节点上的所有Volume
let node_filters = VolumeFilters {
    collection: None,
    state: None,
    node_id: Some(NodeId("node-1".to_string())),
};
let node_volumes = volume_provider.list_volumes(&node_filters).await?;
```

---

## 四、MetadataProvider

**用途**：文件/目录元数据的CRUD操作

**实现**：
- `DirectoryTree` ([powerfs-master/src/provider_impl.rs](file:///home/portion/powerfs/powerfs-master/src/provider_impl.rs)) — 树形目录结构
- `MetaShardManager` ([powerfs-filer/src/provider_impl.rs](file:///home/portion/powerfs/powerfs-filer/src/provider_impl.rs)) — 分片元数据管理

**调用链**：`FUSE/S3` → `FuseMetadataProvider` → gRPC → `DirectoryTree`/`MetaShardManager`

### 4.1 数据结构

#### EntryAttributes

```rust
pub struct EntryAttributes {
    pub ino: u64,             // inode号
    pub mode: u32,            // 文件权限（八进制，如0o644）
    pub uid: u32,             // 用户ID
    pub gid: u32,             // 组ID
    pub atime: DateTime<Utc>, // 访问时间
    pub mtime: DateTime<Utc>, // 修改时间
    pub ctime: DateTime<Utc>, // 变更时间
    pub crtime: DateTime<Utc>, // 创建时间
}
```

**权限位说明**：
- `0o755`: 目录（rwxr-xr-x）
- `0o644`: 文件（rw-r--r--）
- `0o777`: 全权限

#### FileChunk

```rust
pub struct FileChunk {
    pub offset: u64,          // 在文件中的偏移量
    pub size: u64,            // 块大小
    pub mtime: u64,           // 修改时间戳（毫秒）
    pub fid: String,          // 文件ID（Fid字符串格式）
    pub cookie: u32,          // Cookie（用于一致性校验）
    pub crc32: u32,           // CRC32校验值
}
```

#### Entry

```rust
pub struct Entry {
    pub name: String,                    // 文件名
    pub directory: String,               // 父目录路径
    pub attributes: Option<EntryAttributes>, // 文件属性
    pub chunks: Vec<FileChunk>,          // 文件块列表（文件类型）
    pub hard_link_id: String,            // 硬链接ID
    pub hard_link_counter: u32,          // 硬链接计数
    pub extended: HashMap<String, Vec<u8>>, // 扩展属性
    pub content_size: u64,               // 内容大小（字节）
    pub disk_size: u64,                  // 磁盘占用大小（字节）
    pub ttl: String,                     // TTL（过期时间，如"7d"）
    pub symlink_target: String,          // 符号链接目标（符号链接类型）
    pub owner: String,                   // 所有者
    pub generation: u64,                 // 版本号
}
```

**Entry类型判断**：
- 目录：`attributes.is_some()` 且 `mode & 0o40000 != 0`
- 文件：`attributes.is_some()` 且 `mode & 0o40000 == 0`
- 符号链接：`!symlink_target.is_empty()`

### 4.2 方法

#### get_entry

```rust
async fn get_entry(&self, path: &str) -> Result<Option<Entry>>;
```

**功能**：根据路径获取文件/目录元数据

**参数**：
- `path`: 文件路径（如"/bucket/key"）

**返回值**：
- `Option<Entry>`: 元数据（None表示不存在）

**错误场景**：
- `InvalidPath`: 无效路径格式
- `PermissionDenied`: 权限不足
- `NotLeader`: 非Leader节点

**使用示例**：
```rust
let entry = metadata_provider.get_entry("/my-bucket/my-file.txt").await?;
if let Some(e) = entry {
    println!("Size: {}, Mode: {:o}", e.content_size, e.attributes.as_ref().unwrap().mode);
} else {
    println!("File not found");
}
```

---

#### get_entry_by_inode

```rust
async fn get_entry_by_inode(&self, inode: u64) -> Result<Option<(Entry, String)>>;
```

**功能**：根据inode获取文件/目录元数据及路径

**参数**：
- `inode`: inode号

**返回值**：
- `Option<(Entry, String)>`: (元数据, 完整路径)（None表示不存在）

**错误场景**：
- `NotLeader`: 非Leader节点

**使用示例**：
```rust
let (entry, path) = metadata_provider.get_entry_by_inode(12345).await?.unwrap();
println!("Path: {}, Size: {}", path, entry.content_size);
```

---

#### create_entry

```rust
async fn create_entry(&self, entry: &Entry, client_id: &str) -> Result<u64>;
```

**功能**：创建文件/目录元数据

**参数**：
- `entry`: 元数据（需包含name、directory、attributes）
- `client_id`: 客户端ID（用于并发控制）

**返回值**：
- `u64`: 新分配的inode号

**错误场景**：
- `FileExists`: 文件已存在
- `DirectoryNotFound`: 父目录不存在
- `NotLeader`: 非Leader节点
- `QuorumNotReached`: Raft未达到Quorum

**使用示例**：
```rust
let entry = Entry {
    name: "new-file.txt".to_string(),
    directory: "/my-bucket".to_string(),
    attributes: Some(EntryAttributes {
        mode: 0o644,
        uid: 1000,
        gid: 1000,
        atime: Utc::now(),
        mtime: Utc::now(),
        ctime: Utc::now(),
        crtime: Utc::now(),
        ino: 0,  // 由系统分配，传入0即可
    }),
    content_size: 0,
    disk_size: 0,
    hard_link_id: String::new(),
    hard_link_counter: 1,
    extended: HashMap::new(),
    chunks: Vec::new(),
    ttl: String::new(),
    symlink_target: String::new(),
    owner: String::new(),
    generation: 1,
};
let inode = metadata_provider.create_entry(&entry, "client-1").await?;
```

---

#### update_entry

```rust
async fn update_entry(
    &self,
    entry: &Entry,            // 新的元数据
    client_id: &str,          // 客户端ID
    old_size: u64,            // 更新前的文件大小
    is_truncate: bool,        // 是否截断操作
) -> Result<u64>;
```

**功能**：更新文件/目录元数据

**参数**：
- `entry`: 新的元数据
- `client_id`: 客户端ID
- `old_size`: 更新前的文件大小（用于空间计算和配额检查）
- `is_truncate`: 是否为截断操作（影响chunks处理）

**返回值**：
- `u64`: 更新后的inode号

**错误场景**：
- `FileNotFound`: 文件不存在
- `InvalidRequest`: 无效请求
- `NotLeader`: 非Leader节点

**使用示例**：
```rust
let mut new_entry = entry.clone();
new_entry.content_size = 1024;
new_entry.attributes.as_mut().unwrap().mtime = Utc::now();

let inode = metadata_provider
    .update_entry(&new_entry, "client-1", old_size, false)
    .await?;
```

---

#### delete_entry

```rust
async fn delete_entry(&self, inode: u64, is_dir: bool, client_id: &str) -> Result<()>;
```

**功能**：删除文件/目录元数据

**参数**：
- `inode`: inode号
- `is_dir`: 是否为目录（目录删除需检查是否为空）
- `client_id`: 客户端ID

**返回值**：无

**错误场景**：
- `FileNotFound`: 文件不存在
- `DirectoryNotFound`: 目录不存在
- `InvalidRequest`: 非空目录删除失败
- `NotLeader`: 非Leader节点

**使用示例**：
```rust
// 删除文件
metadata_provider.delete_entry(file_inode, false, "client-1").await?;

// 删除目录（需为空）
metadata_provider.delete_entry(dir_inode, true, "client-1").await?;
```

---

#### list_entries

```rust
async fn list_entries(&self, inode: u64, limit: u32, client_id: &str) -> Result<Vec<Entry>>;
```

**功能**：列出目录下的文件/目录

**参数**：
- `inode`: 目录inode号
- `limit`: 返回数量限制
- `client_id`: 客户端ID

**返回值**：
- `Vec<Entry>`: 子项列表

**错误场景**：
- `DirectoryNotFound`: 目录不存在
- `InvalidRequest`: 非目录inode
- `NotLeader`: 非Leader节点

**使用示例**：
```rust
let entries = metadata_provider.list_entries(dir_inode, 100, "client-1").await?;
for entry in entries {
    let mode = entry.attributes.as_ref().map(|a| a.mode).unwrap_or(0);
    let is_dir = (mode & 0o40000) != 0;
    println!("{}: {}", if is_dir { "D" } else { "F" }, entry.name);
}
```

---

## 五、KvCacheProvider

**用途**：KV缓存块管理，支持大语言模型推理等场景的块级缓存

**实现**：`KVCacheEngine` ([powerfs-core/src/provider_impl.rs](file:///home/portion/powerfs/powerfs-core/src/provider_impl.rs))

**调用链**：`KV模块` → `KVCacheEngine` → `StorageManager` → Volume Server

### 5.1 数据结构

#### SessionInfo

```rust
pub struct SessionInfo {
    pub session_id: String,          // 会话ID
    pub block_count: u64,            // 块数量
    pub total_size: u64,             // 总大小（字节）
    pub created_at: DateTime<Utc>,   // 创建时间
    pub last_accessed_at: DateTime<Utc>, // 最后访问时间
}
```

#### SessionStats

```rust
pub struct SessionStats {
    pub session_id: String,   // 会话ID
    pub block_count: u64,     // 块数量
    pub total_size: u64,      // 总大小（字节）
    pub hit_count: u64,       // 命中次数
    pub miss_count: u64,      // 未命中次数
}
```

### 5.2 方法

#### put_block

```rust
async fn put_block(&self, session_id: &str, block_id: u64, data: &[u8]) -> Result<()>;
```

**功能**：写入缓存块

**参数**：
- `session_id`: 会话ID（如模型推理会话）
- `block_id`: 块ID（全局唯一）
- `data`: 块数据

**返回值**：无

**错误场景**：
- `Storage(String)`: 存储错误
- `OutOfSpace`: 空间不足

**使用示例**：
```rust
let block_data = model_layer.compute();
kv_cache_provider.put_block("session-1", 1, &block_data).await?;
```

---

#### get_block

```rust
async fn get_block(&self, session_id: &str, block_id: u64) -> Result<Option<Vec<u8>>>;
```

**功能**：读取缓存块

**参数**：
- `session_id`: 会话ID
- `block_id`: 块ID

**返回值**：
- `Option<Vec<u8>>`: 块数据（None表示未命中）

**错误场景**：
- `Storage(String)`: 存储错误

**使用示例**：
```rust
if let Some(data) = kv_cache_provider.get_block("session-1", 1).await? {
    println!("Cache hit, block size: {}", data.len());
} else {
    println!("Cache miss, need to compute");
}
```

---

#### list_sessions

```rust
async fn list_sessions(&self) -> Result<Vec<SessionInfo>>;
```

**功能**：列出所有活跃会话

**参数**：无

**返回值**：
- `Vec<SessionInfo>`: 会话列表

**错误场景**：
- `Storage(String)`: 存储错误

**使用示例**：
```rust
let sessions = kv_cache_provider.list_sessions().await?;
for session in sessions {
    println!("Session: {}, Blocks: {}, Size: {}MB", 
        session.session_id, 
        session.block_count,
        session.total_size / 1024 / 1024
    );
}
```

---

#### evict_session

```rust
async fn evict_session(&self, session_id: &str) -> Result<()>;
```

**功能**：驱逐会话缓存（释放空间）

**参数**：
- `session_id`: 会话ID

**返回值**：无

**错误场景**：
- `Storage(String)`: 存储错误

**使用示例**：
```rust
kv_cache_provider.evict_session("session-1").await?;
```

---

#### get_session_stats

```rust
async fn get_session_stats(&self, session_id: &str) -> Result<Option<SessionStats>>;
```

**功能**：获取会话统计信息（含命中率）

**参数**：
- `session_id`: 会话ID

**返回值**：
- `Option<SessionStats>`: 统计信息（None表示会话不存在）

**错误场景**：
- `Storage(String)`: 存储错误

**使用示例**：
```rust
if let Some(stats) = kv_cache_provider.get_session_stats("session-1").await? {
    let total = stats.hit_count + stats.miss_count;
    let hit_rate = if total > 0 {
        stats.hit_count as f64 / total as f64 * 100.0
    } else {
        0.0
    };
    println!("Hit rate: {:.2}%, Blocks: {}", hit_rate, stats.block_count);
}
```

---

## 六、EventProvider

**用途**：事件发布订阅，支持系统状态变更通知、监控告警等

**实现**：
- `RedisEventProvider` ([powerfs-common/src/event.rs](file:///home/portion/powerfs/powerfs-common/src/event.rs)) — 需要 `redis-event` feature
- `NullEventProvider` ([powerfs-common/src/event.rs](file:///home/portion/powerfs/powerfs-common/src/event.rs)) — 默认实现（空操作）

**调用链**：`各模块` → `EventProvider` → Redis Stream / Null

### 6.1 Feature Flag 依赖

| 实现 | Feature Flag | 说明 |
|-----|-------------|------|
| `RedisEventProvider` | `redis-event` | 使用Redis Stream作为事件总线 |
| `NullEventProvider` | 无 | 默认实现，所有操作不做实际处理 |

### 6.2 数据结构

#### EventEnvelope

```rust
pub struct EventEnvelope {
    pub event_id: String,     // 事件ID（UUID）
    pub event: Event,         // 事件内容（枚举）
    pub source: String,       // 事件来源（如"master"、"volume"）
    pub source_id: String,    // 来源ID（如节点ID）
    pub timestamp: DateTime<Utc>, // 时间戳
    pub version: String,      // 版本号（"1.0"）
}
```

#### EventStream

```rust
pub struct EventStream {
    pub receiver: tokio::sync::mpsc::Receiver<EventEnvelope>,
}
```

**说明**：事件流接收端，通过 `tokio::sync::mpsc::Receiver` 异步接收事件，缓冲区大小为100。

#### Event（事件类型枚举）

```rust
pub enum Event {
    NodeStatus(NodeStatusEvent),       // 节点状态变更
    VolumeStatus(VolumeStatusEvent),   // Volume状态变更
    KVSession(KVSessionEvent),         // KV会话操作
    KVBlock(KVBlockEvent),             // KV块操作
    MetricUpdate(MetricUpdateEvent),   // 指标更新
    AlertTrigger(AlertTriggerEvent),   // 告警触发
}
```

### 6.3 事件类型详细字段

#### NodeStatusEvent

```rust
pub struct NodeStatusEvent {
    pub node_id: String,     // 节点ID
    pub node_type: String,   // 节点类型（"master"、"volume"、"filer"）
    pub address: String,     // 节点地址
    pub grpc_port: u32,      // gRPC端口
    pub http_port: u32,      // HTTP端口
    pub status: String,      // 状态（"healthy"、"fault"等）
    pub cpu_usage: f64,      // CPU使用率
    pub mem_usage: f64,      // 内存使用率
    pub disk_usage: f64,     // 磁盘使用率
    pub network_rx: u64,     // 网络接收速率（字节/秒）
    pub network_tx: u64,     // 网络发送速率（字节/秒）
    pub uptime: u64,         // 运行时间（秒）
    pub volume_count: u32,   // Volume数量
    pub is_leader: bool,     // 是否为Leader
    pub raft_term: u64,      // Raft任期
}
```

#### VolumeStatusEvent

```rust
pub struct VolumeStatusEvent {
    pub volume_id: u32,      // VolumeID
    pub node_id: String,     // 节点ID
    pub size: u64,           // 容量（字节）
    pub used: u64,           // 已用（字节）
    pub file_count: u64,     // 文件数量
    pub status: String,      // 状态（"available"、"full"等）
    pub collection: String,  // Collection名称
}
```

#### KVSessionEvent

```rust
pub struct KVSessionEvent {
    pub session_id: String,  // 会话ID
    pub model_name: String,  // 模型名称
    pub layer_count: u32,    // 层数
    pub block_count: u64,    // 块数量
    pub memory_used: u64,    // 内存使用（字节）
    pub hit_ratio: f64,      // 命中率
    pub eviction_count: u64, // 驱逐次数
    pub event_type: String,  // 事件类型（"create"、"evict"、"update"）
}
```

#### KVBlockEvent

```rust
pub struct KVBlockEvent {
    pub block_id: u64,       // 块ID
    pub session_id: String,  // 会话ID
    pub layer_id: u32,       // 层ID
    pub event_type: String,  // 事件类型（"put"、"get"、"delete"）
    pub size_bytes: u64,     // 块大小（字节）
}
```

#### MetricUpdateEvent

```rust
pub struct MetricUpdateEvent {
    pub metric_name: String,          // 指标名称
    pub metric_type: String,          // 指标类型（"gauge"、"counter"、"histogram"）
    pub value: f64,                   // 指标值
    pub labels: HashMap<String, String>, // 标签
}
```

#### AlertTriggerEvent

```rust
pub struct AlertTriggerEvent {
    pub alert_id: String,    // 告警ID
    pub rule_id: String,     // 规则ID
    pub name: String,        // 告警名称
    pub severity: String,    // 严重级别（"critical"、"warning"、"info"）
    pub status: String,      // 状态（"firing"、"resolved"）
    pub message: String,     // 告警消息
    pub source: String,      // 告警来源
}
```

### 6.4 方法

#### publish

```rust
async fn publish(&self, event: Event, source_id: &str) -> Result<()>;
```

**功能**：发布事件到事件总线

**参数**：
- `event`: 事件类型（Event枚举）
- `source_id`: 事件来源ID（如节点ID）

**返回值**：无

**错误场景**：
- Redis实现：`Internal(String)` — Redis连接/写入失败
- Null实现：无错误

**使用示例**：
```rust
let event = Event::NodeStatus(NodeStatusEvent {
    node_id: "node-1".to_string(),
    node_type: "volume".to_string(),
    address: "192.168.1.100".to_string(),
    grpc_port: 8080,
    http_port: 8081,
    status: "healthy".to_string(),
    cpu_usage: 25.5,
    mem_usage: 60.0,
    disk_usage: 40.0,
    network_rx: 1024 * 1024,
    network_tx: 512 * 1024,
    uptime: 3600,
    volume_count: 50,
    is_leader: false,
    raft_term: 0,
});
event_provider.publish(event, "volume-manager").await?;
```

---

#### subscribe

```rust
async fn subscribe(&self, stream_key: &str) -> Result<EventStream>;
```

**功能**：订阅事件流

**参数**：
- `stream_key`: 流名称（如"powerfs_events"）

**返回值**：
- `EventStream`: 事件流接收端（`mpsc::Receiver`）

**错误场景**：
- Redis实现：`Internal(String)` — Redis连接失败
- Null实现：返回空的Receiver

**使用示例**：
```rust
let mut stream = event_provider.subscribe("powerfs_events").await?;
while let Some(envelope) = stream.receiver.recv().await {
    match envelope.event {
        Event::NodeStatus(e) => {
            println!("Node {} status changed to {}", e.node_id, e.status);
        }
        Event::VolumeStatus(e) => {
            println!("Volume {} used: {}%", e.volume_id, e.used * 100 / e.size);
        }
        _ => {}
    }
}
```

---

#### read_history

```rust
async fn read_history(
    &self,
    stream_key: &str,   // 流名称
    start: &str,        // 起始ID
    count: usize,       // 数量限制
) -> Result<Vec<EventEnvelope>>;
```

**功能**：读取历史事件

**参数**：
- `stream_key`: 流名称
- `start`: 起始ID，`"-"`表示最早，`"+"`表示最新，具体ID表示从该ID之后读取
- `count`: 返回数量限制

**返回值**：
- `Vec<EventEnvelope>`: 历史事件列表

**错误场景**：
- Redis实现：`Internal(String)` — Redis查询失败
- Null实现：返回空列表

**使用示例**：
```rust
// 读取最近100条事件
let events = event_provider
    .read_history("powerfs_events", "-", 100)
    .await?;

// 读取指定ID之后的事件
let events = event_provider
    .read_history("powerfs_events", "1620000000000-0", 50)
    .await?;
```

---

## 七、StorageProvider

**用途**：底层数据读写（Blob存储），直接操作Volume中的Needle

**实现**：`StorageManager` ([powerfs-core/src/provider_impl.rs](file:///home/portion/powerfs/powerfs-core/src/provider_impl.rs))

**调用链**：`FUSE/KV` → `FuseStorageProvider` → gRPC → `StorageManager` → Volume Server

### 7.1 方法

#### write_blob

```rust
async fn write_blob(
    &self,
    volume_id: u32,   // VolumeID
    file_key: u64,    // 文件Key
    offset: i64,      // 偏移量（相对文件起始）
    size: i32,        // 数据大小
    data: &[u8],      // 数据内容
) -> Result<()>;
```

**功能**：写入Blob数据到指定Volume

**参数**：
- `volume_id`: VolumeID
- `file_key`: 文件Key（来自Fid.file_key）
- `offset`: 写入偏移量（支持稀疏写入）
- `size`: 数据大小（应与data.len()一致）
- `data`: 数据内容

**返回值**：无

**错误场景**：
- `VolumeNotFound`: Volume不存在
- `OutOfSpace`: 空间不足
- `ChecksumMismatch`: 校验和错误
- `NotLeader`: 非Leader节点

**使用示例**：
```rust
let data = vec![0u8; 1024];
storage_provider.write_blob(1, 12345, 0, data.len() as i32, &data).await?;
```

---

#### batch_write_blob

```rust
async fn batch_write_blob(
    &self,
    volume_id: u32,
    file_key: u64,
    entries: &[(i64, i32, Vec<u8>, u32)],  // (offset, size, data, crc32)
) -> Result<()>;
```

**功能**：批量写入Blob数据（一次RPC写入多个块）

**参数**：
- `volume_id`: VolumeID
- `file_key`: 文件Key
- `entries`: 写入条目列表（offset, size, data, crc32）

**返回值**：无

**错误场景**：
- `VolumeNotFound`: Volume不存在
- `OutOfSpace`: 空间不足

**使用示例**：
```rust
let entries = vec![
    (0, 1024, data1.clone(), crc32(&data1)),
    (1024, 1024, data2.clone(), crc32(&data2)),
    (2048, 512, data3.clone(), crc32(&data3)),
];
storage_provider.batch_write_blob(1, 12345, &entries).await?;
```

---

#### read_blob

```rust
async fn read_blob(&self, volume_id: u32, file_key: u64, offset: i64, size: i32) -> Result<Vec<u8>>;
```

**功能**：读取Blob数据

**参数**：
- `volume_id`: VolumeID
- `file_key`: 文件Key
- `offset`: 读取偏移量
- `size`: 读取大小

**返回值**：
- `Vec<u8>`: 读取的数据

**错误场景**：
- `VolumeNotFound`: Volume不存在
- `NeedleNotFound`: 文件块不存在
- `ChecksumMismatch`: 校验和错误

**使用示例**：
```rust
let data = storage_provider.read_blob(1, 12345, 0, 1024).await?;
```

---

#### delete_blob

```rust
async fn delete_blob(&self, volume_id: u32, file_key: u64) -> Result<()>;
```

**功能**：删除Blob数据

**参数**：
- `volume_id`: VolumeID
- `file_key`: 文件Key

**返回值**：无

**错误场景**：
- `VolumeNotFound`: Volume不存在

**使用示例**：
```rust
storage_provider.delete_blob(1, 12345).await?;
```

---

## 八、Provider适配层

### 8.1 架构说明

Provider适配层（[powerfs-fuse-core/src/provider_adapter.rs](file:///home/portion/powerfs/powerfs-fuse-core/src/provider_adapter.rs)）将gRPC调用转换为标准Provider接口调用，实现应用层与底座层的解耦。

```
┌─────────────────────────────────────────────────────────┐
│                     FUSE应用层                          │
│              使用Provider trait接口                      │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│              Provider适配层                             │
│  ┌─────────────────────────────────────────────────┐   │
│  │ FuseVolumeProvider   → gRPC → Master服务        │   │
│  │ FuseMetadataProvider → gRPC → Master/Filer服务  │   │
│  │ FuseStorageProvider  → gRPC → Volume服务        │   │
│  └─────────────────────────────────────────────────┘   │
└──────────────────────┬──────────────────────────────────┘
                       │ gRPC (双向流 + 重试)
                       ▼
┌─────────────────────────────────────────────────────────┐
│                     底座服务                             │
│           Master Raft + Volume Server + Filer          │
└─────────────────────────────────────────────────────────┘
```

### 8.2 适配实现

#### FuseVolumeProvider

```rust
pub struct FuseVolumeProvider {
    client: Arc<PowerFuseClient>,  // gRPC客户端
}
```

**转换逻辑**：
| Provider方法 | gRPC方法 | 说明 |
|-------------|----------|------|
| `assign_volume` | `PowerFuseClient::assign_fid` | 分配Volume和Fid |
| `lookup_volume` | `PowerFuseClient::lookup_volume` | 查询Volume位置 |
| `heartbeat` | 空实现 | FUSE客户端不需要上报心跳 |
| `list_volumes` | 空列表 | FUSE客户端不需要列出Volume |

#### FuseMetadataProvider

```rust
pub struct FuseMetadataProvider {
    client: Arc<PowerFuseClient>,  // gRPC客户端
}
```

**转换逻辑**：
| Provider方法 | gRPC方法 | 说明 |
|-------------|----------|------|
| `get_entry` | `PowerFuseClient::get_entry` | 转换proto Entry到trait Entry |
| `get_entry_by_inode` | `PowerFuseClient::get_entry_by_inode` | 获取inode对应的Entry |
| `create_entry` | `PowerFuseClient::create_entry` | 创建元数据 |
| `update_entry` | `PowerFuseClient::update_entry` | 更新元数据 |
| `delete_entry` | `PowerFuseClient::delete_entry` | 删除元数据 |
| `list_entries` | `PowerFuseClient::list_entries` | 列出目录内容 |

#### FuseStorageProvider

```rust
pub struct FuseStorageProvider {
    client: Arc<PowerFuseClient>,  // gRPC客户端
}
```

**转换逻辑**：
| Provider方法 | gRPC方法 | 说明 |
|-------------|----------|------|
| `write_blob` | `PowerFuseClient::write_needle` | 写入Needle |
| `batch_write_blob` | `PowerFuseClient::batch_write_needle` | 批量写入 |
| `read_blob` | `PowerFuseClient::read_needle` | 读取Needle |
| `delete_blob` | `PowerFuseClient::delete_needle` | 删除Needle |

### 8.3 类型转换

适配层需要将gRPC proto类型转换为trait定义的类型：

| Proto类型 | Trait类型 | 转换方式 |
|----------|----------|---------|
| `powerfs_master::proto::Location` | `Location` | 字段映射 |
| `powerfs_master::proto::Entry` | `Entry` | `proto_entry_to_trait_entry()` |
| `powerfs_master::proto::EntryAttributes` | `EntryAttributes` | 字段映射 |
| `powerfs_master::proto::FileChunk` | `FileChunk` | 字段映射 |
| `powerfs_master::proto::Fid` | `Fid` | `Fid::from_string()` |

---

## 九、部署组合与Feature Flags

### 9.1 Feature Flags配置

| Feature | 启用模块 | 依赖 |
|---------|---------|------|
| `fuse` | powerfs-fuse | FUSE文件系统模块 |
| `kv` | powerfs-master/kv | KV缓存模块 |
| `s3` | powerfs-filer | S3兼容接口模块 |
| `monitor` | powerfs-monitor | 监控模块 |
| `redis-event` | Redis事件总线 | powerfs-common/redis-event |

### 9.2 部署组合示例

```bash
# 最小部署（仅FUSE）
cargo build --no-default-features --features fuse

# FUSE + KV缓存
cargo build --no-default-features --features fuse,kv

# FUSE + S3
cargo build --no-default-features --features fuse,s3

# 全功能（默认）
cargo build

# 全功能 + Redis事件总线
cargo build --features redis-event

# 仅KV模块（无FUSE）
cargo build --no-default-features --features kv
```

### 9.3 Provider实现可用性

| Provider | 默认可用 | 需要feature | 实现组件 |
|----------|---------|------------|---------|
| VolumeProvider | ✅ | 无 | MasterNode |
| MetadataProvider | ✅ | 无 | DirectoryTree / MetaShardManager |
| KvCacheProvider | ✅ | 无 | KVCacheEngine |
| StorageProvider | ✅ | 无 | StorageManager |
| EventProvider (Redis) | ❌ | `redis-event` | RedisEventProvider |
| EventProvider (Null) | ✅ | 无 | NullEventProvider（默认） |

### 9.4 模块依赖关系

```
                    ┌──────────────────┐
                    │   powerfs-common │  ← 公共类型、错误、Provider trait
                    └────────┬─────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
        ▼                    ▼                    ▼
┌───────────────┐    ┌───────────────┐    ┌───────────────┐
│ powerfs-master│    │ powerfs-core  │    │ powerfs-filer │
│ (Volume管理)  │    │ (存储引擎)    │    │ (S3接口)      │
└───────┬───────┘    └───────┬───────┘    └───────┬───────┘
        │                    │                    │
        └────────┬───────────┴────────────────────┘
                 │
                 ▼
        ┌───────────────┐
        │powerfs-fuse   │  ← FUSE文件系统（通过Provider适配器调用底座）
        │powerfs-fuse-core│ ← Provider适配器层
        └───────────────┘
```

---

## 十、接口调用流程示例

### 10.1 FUSE文件写入流程

```
1. FUSE层接收写入请求
       ↓
2. MetadataProvider::create_entry() 创建文件元数据
       ↓
3. VolumeProvider::assign_volume() 分配Volume
       ↓
4. StorageProvider::write_blob() 写入数据块
       ↓
5. MetadataProvider::update_entry() 更新文件大小和chunks
       ↓
6. EventProvider::publish() 发布文件创建事件
```

### 10.2 S3对象上传流程

```
1. S3 Handler接收PUT请求
       ↓
2. MetaShardManager路由到目标分片
       ↓
3. VolumeProvider::assign_volume() 分配Volume
       ↓
4. StorageProvider::write_blob() 写入对象数据
       ↓
5. MetaShardManager创建对象元数据（Entry）
       ↓
6. EventProvider::publish() 发布对象上传事件
```

### 10.3 KV缓存写入流程

```
1. KV模块接收缓存写入请求
       ↓
2. VolumeProvider::assign_volume() 分配KV专用Volume
       ↓
3. KvCacheProvider::put_block() 写入缓存块
       ↓
4. StorageProvider::write_blob() 持久化到Volume
       ↓
5. EventProvider::publish() 发布KV块写入事件
```

---

## 十一、最佳实践

### 11.1 重试策略

所有Provider方法返回的错误通过 `error_kind()` 方法判断是否可重试：

```rust
match provider.call().await {
    Ok(result) => result,
    Err(e) => match e.error_kind() {
        ErrorKind::Retryable(msg) => {
            // 指数退避重试
            tokio::time::sleep(Duration::from_secs(backoff)).await;
            provider.call().await
        }
        ErrorKind::LeaderChanged(_) => {
            // 切换Leader后重试
            switch_leader();
            provider.call().await
        }
        ErrorKind::RateLimited(duration) => {
            // 等待指定时间后重试
            tokio::time::sleep(duration).await;
            provider.call().await
        }
        ErrorKind::NonRetryable(msg) => {
            // 直接返回错误
            Err(e)
        }
    },
}
```

### 11.2 连接池管理

- gRPC客户端使用 `Arc<PowerFuseClient>` 共享
- Redis客户端使用 `Arc<Client>` 共享
- 避免每次请求创建新连接

### 11.3 并发安全

- 所有Provider实现需满足 `Send + Sync`
- 使用 `Arc<RwLock<T>>` 或 `Arc<Mutex<T>>` 保护共享状态
- 避免跨await持有锁（会导致future非Send）

### 11.4 日志记录

所有Provider方法调用应记录关键日志：
- 请求参数（脱敏后）
- 响应结果
- 耗时统计
- 错误信息

---

## 十二、扩展指南

### 12.1 新增Provider接口

1. 在 `powerfs-common/src/traits.rs` 中定义新trait
2. 在对应模块中实现trait（如 `powerfs-master/src/provider_impl.rs`）
3. 在适配层添加适配器（如 `powerfs-fuse-core/src/provider_adapter.rs`）
4. 更新Feature Flags（如需）

### 12.2 新增事件类型

1. 在 `powerfs-common/src/event.rs` 的 `Event` 枚举中添加新变体
2. 定义对应的事件结构体
3. 更新Redis实现的序列化/反序列化逻辑
4. 更新订阅者的事件处理逻辑

### 12.3 新增错误类型

1. 在 `powerfs-common/src/error.rs` 的 `PowerFsError` 枚举中添加新变体
2. 在 `error_kind()` 方法中添加分类逻辑
3. 更新相关Provider实现的错误返回
