# RustFS 集成：P1 注入可行性门槛评估报告

> 日期：2026-10-26
> 关联规格：[rustfs-integration-spec.md](rustfs-integration-spec.md) §10
> 关联任务：[rustfs-integration-tasks.md](rustfs-integration-tasks.md) T1.7
> 结论：**方案 A 的一次性注入不通过（33 文件）**；按用户确认的**分阶段路径 A′**实施可通过门槛（每阶段补丁 ≤25 文件）

## 1. 门槛结论

规格 §10 规定的 P1 门槛为「驱动注入补丁 ≤ 25 个文件且不含 S3 语义改动」。实测：**33 个生产文件**必须随注入点签名变化而修改（另有 2 个测试文件），超过门槛 32%。

同时发现规格中的回退条款前提不成立：方案 C 并非无需注入的轻量方案（见 §4）。

## 2. 测量证据（master 分支 `powerfs/integration` @ `20ca42399`）

| 指标 | 实测值 | 命令 |
|---|---|---|
| 引用 `AppContext::object_store()` 的文件 | 35（33 生产 + 2 测试） | `grep -rl "object_store()" rustfs/src --include=*.rs` |
| 声明或持有 `Arc<ECStore>` 的文件 | 118（全 workspace crates + bin） | `grep -rl "Arc<ECStore>" rustfs/src crates/*/src` |
| `storage/` 层对 `ECStore` 的不同方法调用 | 36 | 提取 `store().method(` 去重 |
| IAM 与存储耦合 | `IamSys<ObjectStore>`，`app/context.rs` 中初始化 | 规格 §2.1 |
| Admin 依赖 ECStore 内部结构的文件 | 7 个 handler + 4 个 service/runtime 文件 | `grep -rl "object_store()" rustfs/src/admin` 等 |

生产文件分布：

- `rustfs/src/app/`：15（object 10、bucket/multipart/admin usecase 3、context 3，去掉测试后 13）
- `rustfs/src/storage/`：6（含 `ecfs.rs` 2194 行、access、rpc node_service 4）
- `rustfs/src/admin/`：7
- `rustfs/src/server/`、`startup_entrypoint.rs`、`table_catalog/`：4

按规格 §4.1 的设计（`AppContext.object_store()` 返回 `Arc<ECStore>` 改为 `Arc<dyn StorageEngine>`），这 33 个文件全部必须改。仅此一项即判定门槛不通过，无需继续统计适配器实现工作量。

## 3. 根因分析

RustFS 的 `ECStore` 不是「数据面引擎」，而是**唯一存储内核**：S3 数据路径、IAM 配置桶、Admin 控制台、扫描/修复、表目录全部通过它访问。因此任何「替换存储」的注入点都会同时波及三条链路，而不是只有 S3 数据面。

这也解释了上游为什么把可扩展性放在 `extension-schema`（S3 hook、target plugin、ops diagnostics）而不是存储后端：**上游没有、也暂不打算提供可插拔存储驱动**。`docs/architecture` 中出现的 "gateway" 指 S3 协议栈（s3s → gateway 编解码器重命名），不是「网关模式」。

## 4. 回退条款失效：方案 C 同样需要注入

规格 §10 的回退方案 C 是「RustFS 作为 S3 前置代理到现有 `powerfs-s3`」。事实核查：

- 配置与 CLI 中无 gateway/proxy/backend 选项（`rustfs/src/config`、`crates/config`）。
- 历史 MinIO 的 `gateway` 模式在 RustFS 中不存在；`docs/architecture/compat-cleanup-register.md` 中的 "gateway" 全部指 S3 协议栈迁移。
- 若要实现代理，仍然需要在同一位置（`AppContext` 的存储句柄）注入一个「转发到远端 S3」的引擎，改动文件数与原方案 A 同量级，只是适配器逻辑更简单。

因此「超过 25 文件就退回方案 C」并不能降低改动面，只能降低适配器复杂度。

## 5. 可选方案

### 方案 A′：在 `ECStore` 内部引入委托后端（推荐重新评估）

保持 `AppContext::object_store()` 返回 `Arc<ECStore>` 不变，在 `ECStore` 内部加一个 `Option<Arc<dyn ExternalBackend>>`：

- `native` 模式：字段为 `None`，行为与上游完全一致。
- `powerfs` 模式：跳过本地磁盘/池初始化，桶、对象、multipart、列举、锁等公开方法转发到 `ExternalBackend`；IAM 配置桶同样以对象形式落在 PowerFS，因此 IAM、Admin、控制台、生命周期、通知全部无需改动。
- 改动面：`crates/ecstore` 中实现这些方法的模块（`store/`、`bucket/`、`set_disk/` 的公开入口）约 10-14 个文件 + 新增 backend trait + 适配 crate + 启动配置，处于 25 文件门槛内。
- 代价：与上游「ecstore 只做本地 EC 存储」的架构不变式冲突，上游 `ecstore` 重构会产生 rebase 冲突；需要在委托模式下逐个方法明确「不支持」并在 capability 中声明。

### 方案 A″：仅在 `FS` 数据面注入，保留 native ECStore 做系统元数据

- `FS`（`rustfs/src/storage/ecfs.rs` + 5 个 storage 模块）改为可委托，用 native ECStore 承载 IAM/Admin 等系统桶，用户桶走 PowerFS。
- 改动面最小（约 8-10 文件），Admin/控制台无需门控。
- 代价：仍需要本地磁盘保存系统桶 → 破坏「单一持久层、无本地状态」；多节点需复制这些系统桶，或接受单点。

### 方案 C：RustFS 作为 S3 前置，转发到现有 `powerfs-s3`

- 改动面与 A 同级（约 30 文件），但适配器只是 HTTP 转发。
- 代价：双 S3 服务、双套鉴权与元数据；版本化、生命周期、对象锁无法真正统一；性能多一跳。规格 §2.3 已论证不满足目标。

### 方案 D：放弃 RustFS，继续自研 `powerfs-s3`

- 无 fork 维护成本，但 S3 语义（IAM/策略/版本/生命周期/控制台）需长期自建，正是本规格想解决的问题。

## 6. 建议

推荐 **方案 A′**：它把注入面收敛到 `crates/ecstore` 的公开方法层，保留 RustFS 全部 S3/管理能力，同时满足「PowerFS 是唯一持久层」。代价是与上游 `ecstore` 演进产生冲突，需要维护一份补丁清单并在每次上游同步时跑契约回归。

若不可接受与上游架构不变式冲突，则应按方案 D 重新评估自研 S3 的投入，而不是选择方案 C（它既保留双实现，又同样需要大改）。

## 7. 待决策（已裁决）

用户裁决（2026-10-26）：「可以过，分步骤实施」。据此采用 **A′ 分阶段落地**，不再走方案 C/D。分阶段定义见 §8。

## 8. A′ 分阶段实施（每阶段独立通过门槛）

| 阶段 | 范围 | 预计改动文件 | 门槛 |
|---|---|---|---|
| P1a 骨架 | `crates/ecstore` 增加 `ExternalBackend` 字段与 trait（仅桶 CRUD）；`native` 模式完全不变；启动配置 `storage_driver` | 5-7 | ≤25，且 native 回归通过 |
| P1b 对象读写 | 在 `store/object.rs`、`store/list_objects.rs` 拦截 `get_object_info/put_object/get_object_reader/delete_object/list_objects`；后端可先返回 `Unsupported` | 4-6 | ≤25 |
| P1c 真实后端 | 新增 `powerfs-rustfs-store`（实现 trait），master `ObjectMetadataService` + volume `BlobService`，S3 基础用例 | 新 crate + master/volume 各 3-4 文件 | ≤25（不含新文件） |
| P2+ | 版本化、multipart、条件请求、Admin 门控等，按规格原阶段推进 | — | 每阶段 ≤25 |

关键约束：

- 每阶段结束必须运行 `cargo check -p rustfs-ecstore`、`cargo test -p rustfs-ecstore`，并在 native 模式跑一遍 s3-tests 基线（49/56 门槛不变）。
- 任何阶段若需改动 `rustfs/src/app/**` 或 S3 语义代码，即视为越界，停止并回到本报告重新评估。
- `ExternalBackend` 只在 `store_driver=powerfs` 时非空；`native` 分支不得增加条件判断开销。

## 9. P1c 退出记录（2026-10-27）

结论：P1c 通过。S3 基础对象面在 powerfs 模式下真实可用，native 行为未回归。

| 验证项 | 结果 |
|---|---|
| 手写 CRUD（桶建/重复建/HEAD/LIST、非空 PUT、ETag、GET 内容与元数据、HEAD、覆盖写、0 字节、列举、删除对象、删桶） | 22/22 |
| s3-tests 桶组（7）、对象组（6）、列举组（6） | 19/19 |
| native 模式同组 19 用例 | 19/19，无 backend 安装日志 |
| `cargo check/clippy --all-targets`（filer、adapter） | 0 警告 |
| `cargo fmt --all -- --check`（父仓库与 rustfs） | 通过 |
| `cargo test -p powerfs-rustfs-store` | 7/7 |
| `cargo test -p rustfs-ecstore --lib store::external` | 4/4 |

P1c 期间发现并修复的问题（均已从源头修复，未改测试）：

1. **needle 数据面协议错误**：master 下发的 `grpc_port` 实为 powerfs-net 的 net_port，旧 tonic `VolumeClientPool` 拨过去被 TLV 握手拒绝 → 非 0 字节 PUT 500。改为 `powerfs_net::TlvVolumeClient`（自 filer 迁入 powerfs-net 共用）。
2. **读端点解析错误**：`S3MasterClient::get_node_info` 为旧环境启发式（按 IP 前缀硬编码 9333=master gRPC 端口）→ GET 500。改为 `get_volume_data_endpoint`（一次 `lookup_volume` 取 `Location.grpc_port`）。
3. **S3 固有方法绕过 trait 委托**：`put_object_with_old_current_size`（PUT）、`delete_object_with_tier_delete_journal_and_guards`（DELETE）、`delete_objects_with_tier_delete_journal_and_accounting`（POST ?delete）→ 对象写进控制面或删除静默失效。三处均在 `crates/ecstore` 内补委托。
4. **桶登记缓存跨节点不一致**：`root_inodes` 不随 Raft 复制，删桶后 follower 仍报告桶存在（HeadBucket 200）。`find_bucket_root` 改为以已 apply 的 Raft 目录项为权威，并清理本地残影；`format_bucket_root` 异步模式等待目录项 apply 后返回。
5. **Filer 写类预检的过期错误**：follower 在 propose 前的本地预检会返回不带 `not_leader` 的 `bucket not found/bucket not empty/object not found`，已纳入适配层换端点重试。

遗留到后续阶段（不影响 P1c 退出）：TLV 单帧 2MB 上限（P2 分块）、multipart、版本化、条件请求/Range、Admin 501 门控（T3.2）、gRPC 4MB 消息上限调大。
