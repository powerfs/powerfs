# Collection 实施进度跟踪

> 本文档跟踪 Collection 管理方案的分步实施进度。
> 设计文档参见：[collection-management-design.md](./collection-management-design.md)

## 实施步骤总览

| Step | 描述 | 优先级 | 状态 | Commit |
|------|------|--------|------|--------|
| 1 | 核心数据结构与 Master Collection API | P0 | ✅ Done | (已完成) |
| 2 | Volume 创建支持 collection 参数 | P0 | ✅ Done | feat: volume create accepts collection param |
| 3 | Volume 分配模式 Auto/Manual/Hybrid | P0 | ⏳ Pending | - |
| 4 | S3 接口支持 collection | P0 | ⏳ Pending | - |
| 5 | KV 接口支持 collection | P1 | ⏳ Pending | - |
| 6 | 前端 Collection 管理页面 | P1 | ⏳ Pending | - |
| 7 | CLI Collection 管理命令 | P1 | ⏳ Pending | - |

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

## Step 3: Volume 分配模式 Auto/Manual/Hybrid ⏳

**目标**: 在 Master 的 `assign_volume` 中根据 `VolumeAllocationMode` 选择 Volume（自动/手动/混合）。

---

## Step 4: S3 接口支持 collection ⏳

**目标**: S3 Bucket 创建与 put_object 支持 collection 参数，动态分配 Volume。

---

## Step 5: KV 接口支持 collection ⏳

**目标**: KV Session 关联 collection，put_block 使用 session 的 collection 分配 Volume。

---

## Step 6: 前端 Collection 管理页面 ⏳

**目标**: Collection 列表/详情/新建/编辑/删除页面。

---

## Step 7: CLI Collection 管理命令 ⏳

**目标**: `powerfs collection list/info/create/update/delete/volumes/stats` 命令。
