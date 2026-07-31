# Collection 实施进度跟踪

> 本文档跟踪 Collection 管理方案的分步实施进度。
> 设计文档参见：[collection-management-design.md](./collection-management-design.md)

## 实施步骤总览

| Step | 描述 | 优先级 | 状态 | Commit |
|------|------|--------|------|--------|
| 1 | 核心数据结构与 Master Collection API | P0 | ✅ Done | (已完成) |
| 2 | Volume 创建支持 collection 参数 | P0 | ✅ Done | feat: volume create accepts collection param |
| 3 | Volume 分配模式 Auto/Manual/Hybrid | P0 | ✅ Done | feat: collection volume allocation modes |
| 4 | S3 接口支持 collection | P0 | ✅ Done | feat: step 4 S3 support for collection |
| 5 | KV 接口支持 collection | P1 | ✅ Done | feat: step 5 KV support for collection |
| 6 | 前端 Collection 管理页面 | P1 | ✅ Done | feat: step 6 collection management UI |
| 7 | CLI Collection 管理命令 | P1 | ✅ Done | feat: step 7 CLI collection management commands |

---

## Step 1: 核心数据结构与 Master Collection API ✅

**交付物**:
- `powerfs-master/src/collection.rs`: `CollectionInfo`、`CollectionStats`、`CollectionManager` 等核心结构
- `CollectionManager` 提供 CRUD、容量检查、统计聚合、序列化/恢复
- 已集成到 `MasterNode`，提供 `create_collection_ext`、`get_collection_info`、`list_collection_infos`、`get_collection_stats` 等 API
- `assign_volume` 中已加入容量检查

**验证**: 单元测试覆盖 CRUD、容量检查、序列化往返、状态矩阵等。

---

## Step 2: Volume 创建支持 collection 参数 ✅

**目标**: 让 Volume Server 创建 Volume 时接收并使用 collection 参数，替代硬编码的 "default"。

**改动范围**:
- `powerfs-volume/proto/powerfs.proto`: `CreateVolumeRequest` 增加 `collection` 字段
- `powerfs-core/src/volume.rs`: `Volume` 增加 `set_collection` 方法
- `powerfs-core/src/storage.rs`: `StorageManager` 增加 `create_volume_with_collection` 方法
- `powerfs-volume/src/server.rs`: `create_volume` 使用请求中的 collection，并发布带正确 collection 的 VolumeStatusEvent
- `powerfs-master/src/volume_client.rs`: `create_volume_with_retry` 增加 collection 参数
- `powerfs-master/src/master.rs`: `apply_assign_volume` 传递 collection
- `powerfs-cli/src/volume_client.rs`: 适配新 proto 字段
- `powerfs-volume/tests/grpc_test.rs`: 适配新 proto 字段

**验证**:
- `cargo build` 通过（proto 自动重新生成）
- `cargo fmt` / `cargo clippy` 通过（无新增 warning）
- `cargo test -p powerfs-core --lib`: 4 个新测试覆盖 default collection、自定义 collection、空 collection 归一化、重复创建拒绝
- `cargo test -p powerfs-master --lib`: 77 测试通过
- `cargo test -p powerfs-volume --lib`: 26 测试通过
- `cargo test -p powerfs-volume --test grpc_test`: 8 个集成测试通过

---

## Step 3: Volume 分配模式 Auto/Manual/Hybrid ✅

**目标**: 在 Master 的 `assign_volume` 中根据 `VolumeAllocationMode` 选择 Volume（自动/手动/混合），并应用 `excluded_volume_ids` 黑名单。

**改动范围**:
- `powerfs-master/src/master.rs`:
  - `assign_volume` 解析 collection 的 `volume_allocation` 与 `excluded_volume_ids`
  - 新增 `select_writable_volume` 方法 + 纯函数 `select_writable_volume_from`（可单测）
  - Auto 模式扫描全部匹配卷；Manual 仅在 pinned 列表内查找且不回退；Hybrid 先查 pinned 再回退 Auto
  - 黑名单在所有模式下生效，且优先于 Manual pin
  - `assign_stripe_volumes` 同步应用黑名单

**验证**:
- `cargo build` / `cargo fmt` / `cargo clippy` 通过（无新增 warning）
- 8 个新单测覆盖：Auto 选取匹配卷、Manual 仅选 pinned、Manual 不回退、Hybrid 回退 Auto、黑名单排除、黑名单覆盖 Manual pin、节点离开拓扑跳过、ReadOnly 状态跳过
- powerfs-master 85 个 lib 测试全部通过

---

## Step 4: S3 接口支持 collection ✅

**目标**: S3 Bucket 创建与 put_object 支持 collection 参数，动态分配 Volume。

**改动范围**:
- `powerfs-filer/src/bucket_manager.rs`: `create_bucket` 增加 `collection` 参数（空值归一化为 "default"），存入 `BucketInfo`；新增 `assign_volume_for_object` 方法用 bucket 的 collection 动态分配 Volume
- `powerfs-filer/src/s3_handler.rs`: `create_bucket` 接收 collection；`put_object` 改用 `assign_volume_for_object` 动态分配（替代固定写 `volume_ids[0]` 的旧逻辑）
- `powerfs-filer/src/server.rs`: `create_bucket` 处理函数从 HTTP 头 `x-powerfs-collection` 提取 collection

**验证**:
- `cargo fmt` / `cargo clippy -p powerfs-filer --all-targets` 通过（无新增 warning）
- `cargo build -p powerfs-filer` 通过
- `cargo test -p powerfs-filer --lib`: 49 测试通过
- `cargo test -p powerfs-master --lib`: 85 测试通过（含 Step 1-3 的 collection 测试）

---

## Step 5: KV 接口支持 collection ✅

**目标**: KV Session 关联 collection，put_block 使用 session 的 collection 分配 Volume。

**改动范围**:
- `powerfs-master/proto/master.proto`: `CreateSessionRequest` 增加 `collection` 字段 (field 10)
- `powerfs-core/src/kv_cache.rs`: `KVSession` 增加 `collection` 字段；`create_session` 增加 `collection` 参数
- `powerfs-core/src/kv_cache_persist.rs`: `SessionMeta` 增加 `collection` 字段（`#[serde(default)]` 兼容旧记录）；`PersistentKVCache::create_session` 透传 collection
- `powerfs-master/src/kv_cache_service.rs`: `create_session` 将 collection 传给 engine 并归一化后存入 SessionMeta；`put_block` 改用 session 的 collection 调用 `assign_volume`（替代硬编码 "default"）
- `powerfs-master/src/master.rs`: `restore_kv_sessions` 恢复时传入 `meta.collection`
- `powerfs-kv-client/src/client.rs`、`powerfs-cli/src/commands/kv.rs`: 适配 proto 新字段
- `powerfs-core/tests/kv_cache_test.rs`: 适配新签名并新增 `test_session_collection_stored` 测试

**验证**:
- `cargo fmt` / `cargo build --workspace` 通过
- `cargo clippy --workspace --all-targets` 无新增 warning（仅 2 个预存 warning 在未改动文件中）
- `cargo test -p powerfs-core --lib`: 43 测试通过
- `cargo test -p powerfs-core --test kv_cache_test`: 18 测试通过（含新增 collection 测试）
- `cargo test -p powerfs-master --lib`: 85 测试通过

---

## Step 6: 前端 Collection 管理页面 ✅

**目标**: Collection 列表/详情/新建/删除页面。

**改动范围**:
- `powerfs-monitor/src/main.rs`: 新增 4 个 HTTP 代理端点（GET/POST `/api/collections`、GET/DELETE `/api/collections/:name`），通过 gRPC 调用 Master 的 ListCollections/GetCollection/CreateCollection/DeleteCollection；新增 `CollectionDetail` DTO 与 `CreateCollectionBody` 请求体；create/delete 要求 admin 权限
- `powerfs-monitor-frontend/src/types/index.ts`: 新增 `CollectionInfo` 接口
- `powerfs-monitor-frontend/src/services/api.ts`: 新增 `getCollections`/`getCollection`/`createCollection`/`deleteCollection` API 函数与 `CreateCollectionParams` 类型
- `powerfs-monitor-frontend/src/pages/Collections/index.tsx`: 新建管理页面（表格 + 新建 Modal + 详情 Modal + 删除确认）
- `powerfs-monitor-frontend/src/components/Layout/index.tsx`: 在「存储」分组下新增「Collection 管理」菜单项
- `powerfs-monitor-frontend/src/App.tsx`: 注册 `/collections` 路由（requireAdmin）

**验证**:
- `cargo build -p powerfs-monitor` 通过
- `cargo clippy -p powerfs-monitor --all-targets` 无 warning
- `npm run build`（tsc + vite）通过
- 列表/详情/新建/删除 UI 完整，权限校验在监控后端完成

---

## Step 7: CLI Collection 管理命令 ✅

**目标**: `powerfs-cli collection list/info/create/delete/stats` 命令。

**改动范围**:
- `powerfs-cli/src/commands/collection.rs`: 新建命令模块，提供 5 个子命令（list/info/create/delete/stats），通过 gRPC 调用 Master 的 ListCollections/GetCollection/CreateCollection/DeleteCollection/GetStatistics
- `powerfs-cli/src/commands/mod.rs`: 注册 collection 模块并导出 `CollectionArgs`
- `powerfs-cli/src/main.rs`: 新增 `Commands::Collection` 变体与 dispatch

**用法示例**:
```bash
powerfs-cli collection list
powerfs-cli collection info <name>
powerfs-cli collection create <name> -r 001 -d hdd --max-volume-count 10
powerfs-cli collection delete <name>
powerfs-cli collection stats <name>
```

**验证**:
- `cargo fmt` / `cargo clippy -p powerfs-cli --all-targets` 通过（无 warning）
- `cargo build -p powerfs-cli` 通过
- `powerfs-cli collection --help` 与 `collection create --help` 输出正确，5 个子命令均注册
