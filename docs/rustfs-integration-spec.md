# RustFS 集成规格：S3 服务与管理面走 RustFS，PowerFS 提供 needle 存储

> 状态：P0 完成；P1 分阶段 A′ 已全部通过（P1a 注入缝、P1b 对象读写、P1c 真实后端与启动接线），见 [rustfs-p1-gate-report.md](rustfs-p1-gate-report.md) §9
> 修订：2026-10-26 改用「ECStore 内部委托」注入点（A′），不再改 `AppContext` 与 `rustfs/src/app/**`
> 日期：2026-10-26
> 关联规划：[conplan.md](conplan.md) 第 155-159 行
> 适用范围：PowerFS master / volume / 打包 / 配置单一源，以及 rustfs 子模块（fork）

## 0. 审批清单（Approval Checklist）

只有以下决策全部确认后才进入实施：

* [ ] D1：采用「方案 A：对象级存储契约适配」，RustFS 承担 S3 服务与管理面，PowerFS 只在 needle/blob 层提供数据持久化与其控制面元数据索引。

* [ ] D2：持久性由 PowerFS 卷复制/EC 负责；接入 RustFS 的 bucket 使用独立 collection，RustFS 自身 EC 在该模式下不启用，并在 capability 快照中显式标注。

* [ ] D3：对象元数据权威落在 Raft 一致的服务上。**P1c 直接复用 Filer 的 `FilerMetaService`**（master 同类 RPC 已全部 `UNIMPLEMENTED`）；P2 起再评估是否需要独立 `ObjectMetadataService`。不做「元数据也当普通 needle」的弱索引方案。

* [ ] D4：`rustfs` 由被忽略目录改为 git submodule，指向本项目维护的 fork；fork 上只保留「驱动注入 + capability 门控」两类补丁，其余保持可直接 rebase 上游。

* [ ] D5：在达到功能对齐后退役自研 `powerfs-s3`（删除 `powerfs-s3/` 与 `powerfs-master/src/s3/`），不做双实现长期并存。

* [ ] D6：P1 可行性门槛与回退判据（见 §10）接受。

* [ ] D7：P1c 具体决策（§4.4 D1–D10）接受，特别是：元数据走 Filer 新增的 S3 元数据 RPC；覆盖写为 `PutObjectMeta` 原地替换（原子）；powerfs 模式保留单目录控制面 ECStore；Admin 501 门控推迟到 T3.2。

***

## 1. 结论摘要

采用 RustFS 作为唯一的 S3 协议服务面与管理面（S3 API、IAM/STS、策略、控制台、Admin API、生命周期、复制、事件通知、审计、S3 Select），PowerFS 退回其擅长的层次：

* **数据面**：对象内容与 multipart part 以 needle/blob 形式写入 PowerFS volume（复用 `StorageBackend::write_needle/read_needle`、volume 分配与放置）。

* **元数据面**：新增 master 侧 `ObjectMetadataService`，保存 bucket、对象版本、删除标记、multipart 会话、ETag/校验和、用户元数据、标签、对象锁等 S3 语义所需的索引与记录；提供前缀有序列举。

* **控制面**：仍由 PowerFS master 负责卷分配、放置、卷复制/修复；RustFS 的 EC 与磁盘修复能力在该模式下不启用。

RustFS 侧不重写 S3 语义代码，只做两件事：

1. 把「存储实现」从写死的 `Arc<ECStore>` 抽象为可注入的驱动（native EC 与 powerfs 两种模式）；
2. 对依赖 ECStore 内部结构的 Admin/控制台路由做 capability 门控（不支持的路由返回 501，并展示 PowerFS 侧等价能力）。

***

## 2. 事实基线（已核实，不可再假设）

### 2.1 RustFS 现状

| 事实                                              | 位置                                                                                      |
| ----------------------------------------------- | --------------------------------------------------------------------------------------- |
| 版本 1.0.2，Apache-2.0，独立仓库（pinned `fea5a584c`）    | `rustfs/Cargo.toml:76-77`、`rustfs/LICENSE`                                              |
| App 上下文持有具体类型 `object_store: Arc<ECStore>`      | `rustfs/rustfs/src/app/context/global.rs:45`、`:141`                                     |
| 存储句柄解析返回具体 `Arc<ECStore>`                       | `rustfs/rustfs/src/app/context.rs:131-140`                                              |
| 启动直接构造 `ECStore::new_with_instance_ctx`         | `rustfs/rustfs/src/startup_storage.rs:167`、`:201`                                       |
| IAM 以 `IamSys<ObjectStore>` 绑定 ECStore          | `rustfs/rustfs/src/app/context.rs:90-119`                                               |
| 存储契约已独立成 crate                                  | `rustfs/crates/storage-api/src/lib.rs:100-103`                                          |
| 契约包含 Object/Bucket/Multipart/NamespaceLock/Heal | `storage-api/src/object.rs:402,431,674`、`bucket.rs:94`、`multipart.rs:100`               |
| 磁盘级 `DiskAPI` 方法多且绑定 RustFS 私有格式                | `rustfs/crates/ecstore/src/disk/mod.rs:1454-1566`                                       |
| 上游正在演进为 gateway + 可插拔存储方向                       | `docs/architecture/extension-schema`、`docs/testing/ci-gates.md`（gateway/integration 分支） |

### 2.2 PowerFS 现状

| 事实                                      | 位置                                                                 |
| --------------------------------------- | ------------------------------------------------------------------ |
| needle 格式与校验                            | `powerfs-core/src/needle.rs:12-118`                                |
| 存储后端抽象（read/write/sync/truncate needle） | `powerfs-core/src/storage_backend/mod.rs`                          |
| 卷服务、分配、租约                               | `powerfs-volume/src/server.rs`、`range_lease.rs`、`master_client.rs` |
| 自研 S3 为子集（无 IAM/策略/控制台）                 | `powerfs-s3/src/main.rs`、`powerfs-master/src/s3/server.rs`         |
| master 目录树与卷分配客户端                       | `powerfs-master/src/s3/directory_tree_api.rs`、`master_client.rs`   |

### 2.3 结论性约束

* 不能把 PowerFS 伪装成 RustFS 磁盘（方案 B）：需要完整复刻 `xl.meta`/版本/bitrot/remote-disk 语义，且与 PowerFS 卷复制叠加形成双层冗余，后续维护成本高于收益。

* 不能只做 S3 代理（方案 C）：双跳 + 双套鉴权/元数据，版本、生命周期、对象锁仍然无法统一。

* 因此选择在 **RustFS 存储契约层** 注入 PowerFS 驱动；这是唯一同时满足「S3 语义完整」与「PowerFS 只做 needle」的切口。

***

## 3. 目标架构

### 3.1 组件与职责

```
S3 客户端
   │ HTTPS (SigV4)
   ▼
[rustfs-server]  ← RustFS：S3 API / Admin API / Console / IAM / 策略 / 生命周期 / 通知 / 审计
   │  rustfs-storage-api 契约（Object/Bucket/Multipart/List/Lock/Heal/Capability）
   ▼
[powerfs-rustfs-store]  ← 新增适配 crate（本项目维护）
   │  gRPC
   ├──────────────► [powerfs-master] ObjectMetadataService（Raft 一致的对象索引与版本）
   └──────────────► [powerfs-volume] BlobService（needle 读写、分配、复制）
```

职责边界：

* RustFS：协议解析、鉴权、S3 语义、生命周期/复制/通知、控制台。

* 适配层：契约到 PowerFS API 的映射、错误码映射、能力快照、请求级锁。

* PowerFS master：bucket/object/version/multipart 元数据的唯一权威；前缀有序索引。

* PowerFS volume：blob（needle）持久化，卷分配与复制/EC 由其既有机制负责。

### 3.2 数据模型映射

| S3 概念     | PowerFS 落点                                                                                                               |
| --------- | ------------------------------------------------------------------------------------------------------------------------ |
| Bucket    | master `BucketMeta`（名称、创建时间、版本化状态、对象锁、配额、策略引用）；collection=`s3/<bucket>`                                                  |
| Object    | `ObjectRecord(bucket, key, version_id)`，含 size、etag、校验和、content-type、user metadata、tags、storage class、retention、parts\[] |
| Object 数据 | 每个 part 一个或多个 blob：`(volume_id, needle_id, offset, size, checksum)`                                                      |
| 版本 / 删除标记 | `ObjectRecord` 多版本链；delete marker 为无数据的特殊版本                                                                              |
| Multipart | `MultipartSession(upload_id, bucket, key, parts[])`；part 落 blob，完成时合并为对象版本                                               |
| 列举        | master 有序 key 索引（bucket + key + version 顺序），支持 prefix/delimiter/continuation                                             |
| 命名空间锁     | `NamespaceLocking` 映射到 master 租约（复用 `range_lease` 的租约语义）                                                                 |
| 对象锁 / 保留  | `ObjectRecord` 字段 + 生命周期任务校验                                                                                             |

一致性规则：

* Bucket/对象元数据写为线性一致（Raft 提交后才向客户端确认）。

* 数据先写 volume 并 `sync`，再提交元数据；未提交的 blob 由 GC 回收（带租约超时）。

* 覆盖写产生新版本；删除按版本化状态写入删除标记或直接删除版本记录 + 异步回收 blob。

* 列举分页使用不透明 continuation token，服务端保证快照一致性（token 内编码索引位置 + 版本纪元）。

***

## 4. RustFS fork 补丁设计

### 4.0 注入点修订（2026-10-26）

原设计把 `AppContext.object_store()` 改为 `Arc<dyn StorageEngine>`，实测需修改 33 个生产文件，超过门槛。改用 **A′：ECStore 内部委托**：

- `crates/ecstore/src/store/external.rs` 新增 `StorageDriver` 与 `ExternalBackend` 契约；
- 在 `BucketOperations`/`ObjectOperations` 等 trait 实现入口，若已安装后端则委托，否则原路径；
- `IamSys<ObjectStore>`、Admin、控制台、生命周期、通知全部零改动；
- 阶段划分与门槛见 [rustfs-p1-gate-report.md](rustfs-p1-gate-report.md) §8。

以下 4.1 保留原 A 方案记录，仅作历史参考，实际按 A′ 执行。

### 4.1 注入点（原方案 A，已弃用）

新增 `rustfs-storage-driver` 抽象（在 fork 内，尽量小的新文件）：

* 定义 `StorageEngine` 为契约 supertrait 组合，固定关联类型为本项目映射类型：

  * `ObjectIO<Error = EcstoreError, ...>`

  * `ObjectOperations<Error = EcstoreError, ObjectInfo = EcstoreObjectInfo, ...>`

  * `BucketOperations`、`MultipartOperations`、`ListOperations`、`NamespaceLocking`、`HealOperations`、`TopologySnapshotProvider`、`StorageAdminApi`

* `AppContext` 从 `Arc<ECStore>` 改为：

  * `engine: Arc<dyn StorageEngine>`

  * `native: Option<Arc<ECStore>>`（仅 native 模式用于依赖 ECStore 内部结构的 Admin 路由）

* 启动选择：`config.storage_driver = native | powerfs`（新增配置字段），构造对应 engine。

改动的确切文件（fork 分支 `powerfs/integration`）：

1. `rustfs/src/app/context/global.rs`：`AppContext` 字段与 `object_store()` → `engine()`；保留 `native_store()`。
2. `rustfs/src/app/context.rs`：`resolve_object_store_handle*` → `resolve_engine_handle*`，并保留 native 解析函数供 Admin 使用。
3. `rustfs/src/app/context/interfaces.rs`：`IamInterface::handle()` 等对 `ObjectStore` 的绑定改为泛型/句柄。
4. `rustfs/src/startup_storage.rs`：按 `storage_driver` 分支构造 ECStore 或 powerfs engine。
5. `rustfs/src/app/object/*`、`bucket/*`：把 `Arc<ECStore>` 依赖改为 `Arc<dyn StorageEngine>`（已泛型的用例仅改泛型边界）。
6. `rustfs/src/admin/*`：所有 `native_store()` 缺失时返回 `NotImplemented`（501），并在 `/minio/admin/v3/info` 暴露 `storage_driver=powerfs` 与 capability 快照。
7. `rustfs/Cargo.toml`（bin crate）：新增 path 依赖 `powerfs-rustfs-store`。
8. `rustfs/src/main.rs`：驱动选择与启动日志（不打印敏感内容）。

补丁约束：

* 不修改任何 S3 语义实现（签名校验、条件请求、分块编码、错误码）。

* 不修改 `crates/ecstore` 的行为；仅新增类型与 trait 边界。

* 所有新分支必须让 native 模式行为与上游一致（回归用 vanilla rustfs 测试集覆盖）。

### 4.2 适配 crate `powerfs-rustfs-store`

存放位置：PowerFS 仓库根目录（`powerfs-rustfs-store/`），通过 path 依赖 rustfs 子模块中的契约 crate（`rustfs/crates/storage-api`、`rustfs/crates/ecstore` 的类型与错误），以及 PowerFS 侧客户端。

文件与职责：

| 文件                  | 职责                                                         |
| ------------------- | ---------------------------------------------------------- |
| `src/lib.rs`        | 导出 `PowerFsStore`、`StoreConfig`、`capability()`             |
| `src/error.rs`      | PowerFS 错误 → `EcstoreError`/`StorageErrorCode` 映射表         |
| `src/bucket.rs`     | `make_bucket`/`delete_bucket`/`head_bucket`/`list_buckets` |
| `src/object.rs`     | `put/get/head/delete/copy`、条件请求、校验和、range                  |
| `src/multipart.rs`  | `create/put_part/list_parts/complete/abort`                |
| `src/list.rs`       | ListObjects v1/v2、ListObjectVersions、prefix/delimiter 分页   |
| `src/lock.rs`       | `NamespaceLocking` → master 租约                             |
| `src/heal.rs`       | 明确返回 `Unsupported`，或映射到 PowerFS 修复任务                       |
| `src/capability.rs` | 能力快照：EC/heal 不支持、版本化/多分块/生命周期支持情况                          |
| `src/config.rs`     | 从单一配置源读取 master/volume 端点与超时                               |

### 4.3 PowerFS 新增 API

`ObjectMetadataService`（master）：

* `CreateBucket/DeleteBucket/HeadBucket/ListBuckets`

* `BeginPut/CommitPut/AbortPut`（写路径 + 未提交回收）

* `HeadObject/GetObjectRecord/ListObjectsV2/ListObjectVersions`

* `BeginMultipart/PutPartRecord/ListParts/CompleteMultipart/AbortMultipart`

* `DeleteObject`（返回是否删除标记）

* `UpdateObjectMeta`（tags/retention/acl 引用）

* `TakeSnapshot`（列举分页快照 token）

`BlobService`（volume）：

* `ReserveBlob(volume_id, size) -> (needle_id, lease)`

* `PutBlob(volume_id, needle_id, stream) -> checksum`（流式，复用 needle 写入）

* `GetBlob(volume_id, needle_id, range) -> stream`

* `DeleteBlob(volume_id, needle_id)`（幂等）

* `StatBlob(volume_id, needle_id)`

协议扩展统一写入 `powerfs-volume` 与 master 的既有 proto（`powerfs-volume/src/proto.rs`、master proto），不新起 HTTP 端口。

### 4.4 P1c 实现规格（决策完成，2026-10-26）

> 本节是 P1c 的实施依据。P1c 不新建 master `ObjectMetadataService`，直接复用 Filer 现有 RPC；§4.3 的扩展 API 推迟到 P2。

#### 4.4.1 事实修正

| 事实 | 证据 |
| --- | --- |
| master 的 `get_entry/create_entry/update_entry/delete_entry/rename_entry/lookup_directory_entry/list_entries` 全部返回 `UNIMPLEMENTED("filesystem metadata operations moved to Filer Raft")` | [server.rs](file:///home/portion/powerfs/powerfs-master/src/server.rs#L1747-L1817) |
| 元数据唯一可用入口是 Filer 的 `FilerMetaService`，由 filer 进程在 gRPC 端口提供服务 | [filer.proto](file:///home/portion/powerfs/powerfs-filer/proto/filer.proto#L6-L44)、[main.rs](file:///home/portion/powerfs/powerfs-filer/src/main.rs#L550-L556) |
| `powerfs-master` 已通过 `build.rs` 生成同一份 filer proto 并公开为 `powerfs_master::filer_proto`，无需新增依赖或复制 proto | [build.rs](file:///home/portion/powerfs/powerfs-master/build.rs#L18-L23)、[lib.rs](file:///home/portion/powerfs/powerfs-master/src/lib.rs#L7) |
| Filer 的 `UpdateEntry` 只更新 size，不更新 chunks/extended；覆盖写不能依赖它 | [grpc_service.rs](file:///home/portion/powerfs/powerfs-filer/src/grpc_service.rs#L193-L225) |
| master `ListFilers` 可发现 filer 地址（P4 使用；P1c 用显式配置） | [master.proto](file:///home/portion/powerfs/powerfs-master/proto/master.proto#L841-L860)、[server.rs](file:///home/portion/powerfs/powerfs-master/src/server.rs#L1600-L1625) |

#### 4.4.2 决策

- **D1 元数据客户端**：使用 `powerfs_master::filer_proto::powerfs::filer_meta_service_client::FilerMetaServiceClient`；`Entry/FuseAttributes/FileChunk` 使用 `powerfs_master::filer_proto::powerfs` 下的同名类型。适配 crate 不再使用 `powerfs_master::s3::directory_tree_api::RemoteDirectoryTree`（它经 master 的 `MasterServiceClient` 走已废弃入口）。
- **D2 端点与失败策略**：`POWERFS_FILER_ENDPOINTS` 必填，逗号分隔 `host:port`。每请求轮询端点；遇到 `Unavailable`/`DeadlineExceeded` 换下一个端点。Filer 明确**不做服务端转发**：非 leader 对写类 RPC 返回 `not_leader: ...`（应用层错误，见 `raft_group_manager_v2::propose/propose_ff` 的 leader 预检查），因此写类 RPC（`EnsureBucketRoot/DeleteBucketRoot/PutObjectMeta/DeleteObjectMeta`）在每个端点各尝试一次直到命中 leader。此外，写类 RPC 处理端在 propose **之前**有本地读预检（`find_bucket_root`、`get_object_entry`、列子项），follower apply 滞后时会产生不带 `not_leader` 的过期 `bucket not found`/`bucket not empty`/`object not found`——这些同样换端点重试，直到命中 leader（其本地状态永远新鲜）。读类 RPC 对「未找到」同样换端点重查以容忍 follower apply 延迟（`GetBucketRoot/GetObjectMeta`）。超时由 `POWERFS_RPC_TIMEOUT_MS` 控制，默认 5000。P1c 不做自动发现（P4/T4.1 补 `ListFilers`）。
- **D3 客户端标识**：进程级 UUID v4，可用 `POWERFS_S3_CLIENT_ID` 覆盖；随 `PutObjectMeta` 传给 Filer（保留字段，P1c 服务端未做强校验）。
- **D4 命名空间**：桶根 = Filer inode 0 下的同名目录（`format_bucket_root` 语义），对象 = 该目录下的扁平 `name`，S3 key 中的 `/` 不创建中间目录。列举依赖 Filer `ListObjectMetas`（内部走 `list_directory_paginated`，BTreeMap 按 `name` 升序 + 真分页），升序为前提条件。
- **D5 桶操作映射**（Filer 侧新增 4 个 S3 元数据 RPC，避免在适配层拼底层 entry 语义）：
  - `make_bucket`：`EnsureBucketRoot{bucket}`；已存在且非 `force_create` → `BucketExists`；不存在则创建目录并返回 `created=true`。
  - `get_bucket_info`：`GetBucketRoot{bucket}`，`found=false` → `BucketNotFound`；`created` 取返回的 `mtime`。
  - `list_buckets`：`ListBucketRoots{}`，返回 `BucketRootInfo{bucket,inode,mtime}` 列表（仅桶根，不扫根目录全部条目）。
  - `delete_bucket`：`DeleteBucketRoot{bucket, force}`；目录非空且非 `force` → `BucketNotEmpty`；删除后同时清理 Filer 的 `root_inodes` 缓存与持久键。
  - 桶登记的权威性：桶根存在性以 **inode 0 下已 apply 的 Raft 目录项**为准。`find_bucket_root` 不再无条件信任本地 `root_inodes` 缓存（该缓存及其实体化副本不随 Raft 复制，跨节点删桶后会残留，导致 `GetBucketRoot/ListBucketRoots` 谎报桶仍存在）；目录项缺失时顺手清理本地残影。相应地，`format_bucket_root` 在异步持久化模式下也等待目录项 apply 后再返回（超时则仍返回已登记 inode，由读路径跨端点重试兜底），避免「建桶后立即 HeadBucket 得到 404」。
- **D6 对象操作映射**：
  - HEAD：`GetObjectMeta{bucket,key}` → `ObjectInfo`。`etag`/`content-type` 读自 `extended` 的 `s3.etag`/`s3.content-type`，其余 `s3.*` 键去前缀后作为用户元数据。
  - PUT（新建与覆盖写同一条路径）：`assign_volume(replication, collection)` → 取 `nodes[0]` 拼 `address:port` → `write_needle(volume_id, file_key, body)` → `PutObjectMeta{bucket,key,size,chunks,extended}`。**needle 数据面走 powerfs-net TLV 协议**（`powerfs_net::TlvVolumeClient`）：master 心跳处理把 volume 的 net_port 写入 `DataNodeInfo.grpc_port`（`net_handler.rs` 注释明确该字段承载数据面端口），volume 的 tonic gRPC `WriteNeedle` 仅供 admin/测试，旧 `VolumeClientPool`（tonic）拨到 net_port 会被 TLV 握手拒绝。TLV 单帧 DATA 段上限 2MB，P1c 单对象单 needle，超限明确报错，多分块在 P2 做。Filer 对已存在的 key **原地替换** size/chunks/extended，因此覆盖写是**单次原子 RPC**，不存在「删旧建新」窗口。ETag = 内容 **MD5** hex（S3 协议对单段对象的定义；powerfs-s3 旧接口用 SHA-256，属独立实现，不在本次对齐范围）。
  - mtime：`PutObjectMeta` 在 Filer 侧同时刷新对象 inode 的 mtime/atime，统一使用**秒级**时间戳（与 `ShardStore::current_time()` 和 FUSE 现有约定一致）；适配层不再写纳秒。
  - 0 字节对象：不写 needle，`chunks=[]`，读取返回空 body。
  - GET：`GetObjectMeta`，P1c 仅支持 `chunks.len() == 1` 的 range 切片；`chunks.len() > 1` 返回 `NotImplemented`（P2 做多段拼接）。`ObjectInfo.size` 取 `entry.content_size`，不取实际读到长度。读端点由 `S3MasterClient::get_volume_data_endpoint`（一次 `lookup_volume`）解析：IP 取自 `Location.url`、端口用 `Location.grpc_port`（拓扑节点的 powerfs-net 数据端口）。禁止用旧的 `get_node_info` 启发式——它按 IP 前缀硬编码 9333（master gRPC 端口），拨上去必然失败。
  - DELETE：`DeleteObjectMeta{bucket,key}` → 尽力删除 needle（失败只记 warn）。
- **D7 列举**：`ListObjectMetas{bucket,last_key,limit=1000}` 循环扫描直到 `has_more=false` 或空页；prefix/delimiter/max_keys 在适配层处理；continuation token 沿用「最后已消费 key 的 hex」。`list_object_versions` 返回单版本模型（`is_latest=true, num_versions=1`）。
- **D8 能力声明**：P1c `capability()` 全 false（ec/versioning/multipart/object_lock/lifecycle），仅用于日志与后续 Admin 快照；不扩展 fork 的 trait 面，Admin 501 门控在 P3.2 做。
- **D9 启动接线**：`RUSTFS_STORAGE_DRIVER=powerfs` 时在 `run()` 最前面安装后端；构造失败即进程退出，禁止静默回退 native。`AppContext` 仍持有具体 `Arc<ECStore>`，因此保留控制面 ECStore：`config.volumes` 为空时使用单一临时目录（`std::env::temp_dir()/rustfs-powerfs-control-<pid>`），单目录不触发同盘独立性检查，也不需要 `RUSTFS_UNSAFE_BYPASS_DISK_CHECK`。
  - **系统桶必须留在控制面 native ECStore**：`.rustfs.sys` / `.minio.sys` 的 IAM、`config/config.json`、scanner 状态在启动阶段即被读写，若一并委托给 PowerFS，外部存储没有该桶会导致 IAM 初始化卡死、S3 始终返回 `503 Service Unavailable`（`x-rustfs-readiness-pending: iam`）。`crates/ecstore/src/store/external.rs` 增加 `is_control_plane_bucket`/`backend_for_bucket`，所有委托点改用它；`list_bucket` 仍整体委托（系统桶不在外部桶列表中返回）。
  - `get_object_info_for_delete` 与写入端必须走同一后端，已纳入委托（否则 powerfs 桶的 DELETE 前置查询会按 native 判为桶不存在）。
- **D10 不做**：P1c 不实现版本化、multipart、对象锁、条件请求、Admin 门控、自动发现、多节点；这些按 P2–P4 推进。

#### 4.4.3 P1c 改动面

| 文件 | 改动 |
| --- | --- |
| `powerfs-filer/proto/filer.proto` | 新增 8 个 S3 元数据 RPC：`EnsureBucketRoot/GetBucketRoot/ListBucketRoots/DeleteBucketRoot/PutObjectMeta/GetObjectMeta/DeleteObjectMeta/ListObjectMetas` |
| `powerfs-filer/src/grpc_service.rs` | 实现上述 8 个 RPC；修正 `lookup_directory_entry` 的 shard 解析、`list_entries` 改为按名升序分页 |
| `powerfs-filer/src/meta_shard_manager.rs`、`shard_store.rs` | 增加 `find_bucket_root`/`unregister_root_inode`、`remove_root_inode` |
| `powerfs-rustfs-store/src/config.rs` | 增加 `filer_endpoints/client_id/rpc_timeout`；`POWERFS_FILER_ENDPOINTS` 必填 |
| `powerfs-rustfs-store/src/filer.rs`（新） | `FilerClient`：端点池、轮询+单次重试、8 个 S3 元数据 RPC 封装 |
| `powerfs-net/src/tlv_volume_client.rs`（自 `powerfs-filer` 迁入） | `TlvVolumeClient` 归属协议层，供 Filer 与适配器共用；filer 侧引用同步改为 `powerfs_net::TlvVolumeClient` |
| `powerfs-rustfs-store/src/backend.rs` | 用 `FilerClient` 替换 `RemoteDirectoryTree`；needle 读写改用 `TlvVolumeClient`（TLV），弃用 tonic `VolumeClientPool` |
| `powerfs-rustfs-store/src/bucket.rs`、`object.rs`、`list.rs` | 按 D4–D7 重写桶/对象/列举；覆盖写单次 `PutObjectMeta` |
| `powerfs-rustfs-store/src/error.rs` | 增加 `tonic::Status` 映射与桶错误分类 |
| `powerfs-rustfs-store/Cargo.toml` | 增加 `uuid`（workspace）、`powerfs-net`（TLV 数据面）；移除不再使用的目录树依赖引用 |
| `rustfs/rustfs/Cargo.toml` | 增加 path 依赖 `powerfs-rustfs-store` |
| `rustfs/crates/ecstore/src/store/external.rs` | 增加 `is_control_plane_bucket`/`backend_for_bucket`，系统桶不委托 |
| `rustfs/crates/ecstore/src/store/mod.rs` | 委托点改用 `backend_for_bucket`；`get_object_info_for_delete`、`put_object_with_old_current_size`（S3 PUT 直调的固有方法）纳入委托 |
| `rustfs/crates/ecstore/src/store/object.rs` | `delete_object_with_tier_delete_journal_and_guards`（S3 DELETE 直调的固有方法）与批量 `delete_objects_with_tier_delete_journal_and_accounting`（POST ?delete 直调）纳入委托 |
| `rustfs/rustfs/src/startup_entrypoint.rs` | `run()` 增加驱动选择、后端安装、powerfs 模式控制面卷；日志暴露 `storage_driver` |
| `rustfs/rustfs/src/storage_api.rs` 或 config | 仅当需要新增类型导出时改动（优先不改） |

预算：fork 侧 ≤5 个源码文件（external.rs、store/mod.rs、store/object.rs、startup_entrypoint.rs，另加 Cargo.toml），仍满足 ≤25 门槛；父仓库适配 crate ≤8 文件。

***

## 5. 关键流程

### 5.1 PUT Object（单分块）

1. RustFS 解析、鉴权、校验（内容长度、校验和、条件头）。
2. 适配层 `BeginPut` → master 返回占位版本与目标 collection；master 通过既有分配逻辑选卷。
3. 适配层流式写 `PutBlob`（可多段），volume 落 needle 并 `sync`。
4. `CommitPut(etag, size, checksums, user_meta)` → master 原子提交版本记录。
5. 返回 ETag/VersionId；失败时 `AbortPut` + 租约超时回收。

### 5.2 GET / HEAD

1. `GetObjectRecord`（含版本解析：`versionId`、`null`、delete marker 语义）。
2. 条件请求（If-Match/If-None-Match/If-Modified-Since）在适配层判定。
3. `GetBlob` + range 流式返回；校验失败返回 `BadDigest`/`XMinioInvalidObject` 等既有错误码。

### 5.3 ListObjectsV2 / ListObjectVersions

1. master 有序索引做前缀扫描；delimiter 折叠为 common prefixes。
2. 分页 token 编码 `(last_key, last_version, bucket_epoch)`；bucket\_epoch 变化时返回 `InvalidToken` 让客户端重试。
3. 只返回当前可见版本（未版本化/已删除语义按 S3 规则）。

### 5.4 Multipart

1. `BeginMultipart` 创建会话；part 写 blob 并登记 ETag/大小/校验和。
2. `CompleteMultipart` 校验 part 顺序与最小大小 → 合并为对象版本（记录 part 列表，不重写数据）。
3. `AbortMultipart` 标记会话废弃，blob 异步回收。

### 5.5 Delete / 版本化

* 未版本化 bucket：删除记录 + 回收 blob。

* 版本化：写 delete marker；指定 `versionId` 删除对应版本；`expiration` 由 RustFS 生命周期调用同样的删除 API。

***

## 6. 配置与部署

* 配置单一源继续使用 `.powerfs/cluster.toml`，新增：

```toml
[rustfs]
enable = true
image = "powerfs-rustfs"
listen = "0.0.0.0:9000"
console = "0.0.0.0:9001"
storage_driver = "powerfs"
```

* `powerfs-ctl config render` 生成 rustfs 配置与 compose 服务，沿用 `.powerfs/rendered/`，不新增硬编码 IP。

* rustfs 进程与 PowerFS master/volume 同生命周期管理：使用 `powerfs-packaging` 的 systemd 模板与启停脚本，禁止 `pkill`。

* 证书：复用 `generate-certs.sh`，rustfs 使用同一 CA；S3 端口必须 TLS。

P1c 临时环境变量（P5/T5.1 收敛到 `cluster.toml [rustfs]` 并渲染）：

| 变量 | 必填 | 默认 | 含义 |
| --- | --- | --- | --- |
| `RUSTFS_STORAGE_DRIVER` | 否 | `native` | `powerfs` 时安装外部后端 |
| `POWERFS_FILER_ENDPOINTS` | powerfs 模式必填 | 无 | Filer gRPC 端点，逗号分隔 `host:port` |
| `POWERFS_MASTER_ENDPOINTS` | powerfs 模式必填 | 无 | master gRPC 端点（volume 分配/定位） |
| `POWERFS_S3_COLLECTION` | 否 | `default` | 分配 volume 的 collection |
| `POWERFS_S3_REPLICATION` | 否 | `001` | 分配 volume 的 replication |
| `POWERFS_S3_CLIENT_ID` | 否 | 进程 UUID v4 | Filer 写请求的 `client_id` |
| `POWERFS_RPC_TIMEOUT_MS` | 否 | `5000` | Filer/volume RPC 超时 |

***

## 7. 测试与验证（每阶段都必须执行）

| 层     | 手段                                                               | 通过标准                                 |
| ----- | ---------------------------------------------------------------- | ------------------------------------ |
| 编译/静态 | `cargo check`、`cargo fmt --check`、`cargo clippy`（PowerFS 与 fork） | 零错误；fork 的 native 模式测试不回归            |
| 契约    | `powerfs-rustfs-store` 单测 + RustFS 契约兼容测试                        | 所有 trait 方法有行为用例与错误映射用例              |
| S3 功能 | `s3-tests`（对象、分块、版本化、列举、条件请求）                                    | 目标分组全绿；已知不支持项列入 capability 并有 501 用例 |
| 故障注入  | 杀 volume、master leader 切换、磁盘离线、进程重启                              | 读不返回错数据；写返回明确错误；恢复后修复完成              |
| 一致性   | 并发覆盖写/删除/列举、multipart 并发完成                                       | 无重复可见版本、无悬挂 blob（GC 后）               |
| 性能    | 1GB 小规模 → 专用 1TB 数据集；`s3bench`/`warp` + `fio`（必要时）               | 形成评估报告，与现 `powerfs-s3` 基线对比          |
| 容器    | 容器内完成安装与运行（禁止宿主机连容器内 fuse）                                       | 全流程脚本化可重复                            |
| CI    | `scripts/check-ip-literals.sh`、现有 quality job                    | 不新增 IP 字面量；文档路径为单一源                  |

P1c 具体验证（按顺序执行，任一步失败即停在当前步修复）：

```bash
# 1. 静态检查（父仓库与 fork 各自）
cargo check -p powerfs-rustfs-store
cargo clippy -p powerfs-rustfs-store --all-targets -- -D warnings
cargo fmt -p powerfs-rustfs-store -- --check
cd rustfs && cargo check -p rustfs --bins && cargo fmt --all --check
# 2. fork native 不回归
cargo test -p rustfs-ecstore --lib store::external::tests
# 3. native 模式 s3-tests 基线
#    启动 vanilla rustfs（1 个临时目录，无需 bypass）→ 预期 49/56
# 4. powerfs 模式启动（容器内，先确认进程列表无宿主机 fuse）
RUSTFS_STORAGE_DRIVER=powerfs \
POWERFS_MASTER_ENDPOINTS=<master:port> \
POWERFS_FILER_ENDPOINTS=<filer-grpc:port> \
  cargo run --bin rustfs -- /tmp/rustfs-powerfs-control
#    通过标准：日志含 storage_driver=powerfs；S3 端口可鉴权；无 panic
# 5. s3-tests 基础对象组（bucket 创建/删除、PUT/GET/HEAD/DELETE、0 字节、覆盖写、前缀列举）
#    通过标准：本组全绿；失败项必须给出根因并记录 issue，不得改测试
# 6. 数据核对：每个对象 GET 的 SHA-256 与 ETag 一致；删除后 needle 不存在
```

***

## 8. 风险与缓解

| 风险                                     | 缓解                                      |
| -------------------------------------- | --------------------------------------- |
| fork 与上游分叉、后续 rebase 成本                | 补丁限定在驱动注入与 Admin 门控；每月同步上游；记录补丁清单       |
| RustFS 契约面持续演进                         | 适配层单 crate 隔离；用契约编译测试锁定；升级前先跑契约测试       |
| 每对象一次 master 元数据写入成为瓶颈                 | 批量提交、目录索引分片、租约与元数据缓存；性能阶段专项评估           |
| S3 语义缺口（对象锁、Select、通知）                 | capability 快照显式列出；缺项在 P3 前补齐或明确不支持      |
| 许可证合规（RustFS 源自 MinIO 的 Apache-2.0 声明） | 发布前做一次来源与 NOTICE 合规核查，保留 LICENSE/NOTICE |
| 过渡期双 S3 实现                             | 只在开发分支并存；P5 到达标即删除自研实现                  |
| P1c 覆盖写后 mtime 与 chunks 两条 Raft 命令之间短暂不一致 | 先提 chunks 再刷 mtime；S3 客户端 HEAD 秒级精度，可接受；P2 合并为单命令 |
| P1c 依赖 Filer `ListObjectMetas` 按 name 升序        | 实测走 `list_directory_paginated`（BTreeMap 升序）；若乱序则视为阻塞缺陷，记录 issue 并停止，不在适配层掩盖    |

***

## 9. 交付物

1. `rustfs` git submodule（fork，pinned），父仓库 `.gitmodules` 更新、`.gitignore` 移除 `rustfs/`。
2. fork 分支 `powerfs/integration`：驱动注入 + capability 门控补丁，附补丁说明。
3. `powerfs-rustfs-store/` 适配 crate（含单测）。
4. master `ObjectMetadataService` + volume `BlobService` 及 proto。
5. 配置单一源新增 `[rustfs]` 段与渲染产物；systemd/deb/compose 集成。
6. 测试报告与性能评估 MD（按用户规范）。
7. 退役清单：删除 `powerfs-s3/`、`powerfs-master/src/s3/`、相关配置与文档入口。

***

## 10. 阶段计划与门槛

| 阶段        | 内容                                                                   | 退出条件                                            |
| --------- | -------------------------------------------------------------------- | ----------------------------------------------- |
| P0 基线     | 建 fork、转 submodule、原样构建并跑 vanilla rustfs；s3-tests 基线                 | 原样 rustfs 在本机 4 目录模式通过基线用例；许可证核查完成              |
| P1a/P1b 注入缝 | `ExternalBackend` 契约 + ECStore 委托；对象读写/list 已委托                            | 3/3 单测通过；native 行为不变；提交 `3609164f8`、`55b988a53` |
| P1c 真实后端    | 适配 crate 指向 Filer；启动接线 `RUSTFS_STORAGE_DRIVER=powerfs`；最小 CRUD + 前缀列举  | §7 的 6 步验证全部通过；native s3-tests 仍 49/56；powerfs 基础对象组全绿 |
| P2 元数据与语义 | 版本化、删除标记、分块、条件请求、对象元数据/标签、分页一致性                                      | s3-tests 版本化/分块组通过；故障注入无悬挂数据                    |
| P3 管理面    | IAM/STS、控制台、Admin capability 门控、生命周期、通知/审计；复制能力声明                    | 控制台可登录并展示正确能力；策略与生命周期用例通过                       |
| P4 集群与性能  | 多节点、leader 切换、卷故障、GC/修复、1GB→1TB 基准                                   | 连续运行 ≥1 分钟无异常（按既有规范）；性能报告完成                     |
| P5 交付与退役  | 配置/compose/systemd/deb/CI/文档；删除自研 S3；提交 submodule+父仓库                | 端到端容器化脚本通过；文档与单一源一致；无遗留旧配置                      |

**P1 回退判据（已执行）**：判定条件为驱动注入补丁文件数 ≤25 且不含 S3 语义改动。

实测结果（`powerfs/integration` @ `20ca42399`）：引用 `AppContext::object_store()` 的生产文件 **33 个**（另有 2 个测试文件），`Arc<ECStore>` 出现于 **118 个文件**，`storage/` 层调用 ECStore 的不同方法 **36 个**。方案 A 原注入点必然修改这 33 个文件，**门槛不通过**。

同时核查确认：方案 C 并不存在可用的「网关/代理」模式（RustFS 中 "gateway" 指 S3 协议栈，非存储网关），实现代理仍需同一注入点，改动面与方案 A 同级。

详细测量、根因与备选方案见 [rustfs-p1-gate-report.md](rustfs-p1-gate-report.md)。**在 A′（ECStore 内部委托）/ A″（仅 FS 数据面注入）/ C / D（回到自研）中重新选择并审批后，方可继续 P1。**

***

## 11. 明确不做的事

* 不保留自研 `powerfs-s3` 与新 S3 服务长期双活。

* 不在 PowerFS 侧实现 S3 语义（签名、策略、版本规则、生命周期）。

* 不把 RustFS 的 EC 与 PowerFS 卷复制叠加为默认部署（避免容量翻倍）。

* 不为旧配置文件/旧路径提供兼容层。

