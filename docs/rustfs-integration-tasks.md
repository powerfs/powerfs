# RustFS 集成任务清单

> 关联规格：[rustfs-integration-spec.md](rustfs-integration-spec.md)
> 状态：P1c 决策完成（spec §4.4），待审批；未获批准前不得修改代码
> 约定：每阶段结束必须完成该阶段的验证并通过退出条件，未通过时停在当前阶段修复，不进入下一阶段
>
> **当前构建方式（已变更）**：`powerfs-rustfs-store` 已移出主 Cargo workspace，
> 默认 deploy 路径不再需要 rustfs 子模块。构建该适配器请用
> `git submodule update --init rustfs` + `cargo +1.99 build --manifest-path powerfs-rustfs-store/Cargo.toml`
> （rustfs 使用 edition 2024，需要 Rust 1.99+）。下文 T0/T1 的验证记录保留为当时状态。

## P0 基线：fork 与 submodule

- [x] T0.1 在本项目组织下创建 rustfs fork（`powerfs/rustfs`），基于上游 `fea5a584c` 建立 `powerfs/integration` 分支；保留 `upstream` remote。
  - 验收：`origin` = `git@github.com:powerfs/rustfs.git`，`upstream` = 原上游；本地分支 `powerfs/integration` 已推送，HEAD `3609164f8`
- [x] T0.2 父仓库将 `rustfs/` 由被忽略目录改为 submodule，并从 `.gitignore` 移除 `rustfs/`。
  - 文件：`.gitignore`、`.gitmodules`
  - 验收：`git submodule status rustfs` 指向 `3609164f8`（fork `origin/powerfs/integration`）；`upstream` 与 `origin` 均已配置
- [x] T0.3 原样构建与运行 vanilla rustfs（4 本地目录模式），跑 `s3-tests` 基线。
  - 验收：`cargo build --release --bin rustfs` 通过（264 MB 二进制）；本机启动需 `RUSTFS_UNSAFE_BYPASS_DISK_CHECK=true`（四目录同盘）；`s3-tests` 选取 56 个基础用例，49 通过 / 7 失败；失败均为测试配置（alt 用户并入 root）或未实现特性（`PutObjectAcl` XML grants、`PutBucketOwnershipControls`），与集成无关
- [x] T0.4 许可证与来源合规核查（Apache-2.0、NOTICE、与 MinIO 的衍生声明）。
  - 验收：`rustfs/LICENSE` 为 Apache-2.0；`README.md` 明确说明「结合 MinIO 的简洁性」且 `rio-v2`（MinIO 磁盘格式兼容）为默认关闭的 preview feature；仓库无独立 `NOTICE` 文件，分发时需在发布清单中补充 RustFS 版权与 Apache-2.0 声明

## P1 注入可行性（分阶段通过）

> 状态：方案 A 一次性注入 33 文件不通过；改为 A′ 分阶段。评估见 [rustfs-p1-gate-report.md](rustfs-p1-gate-report.md) §8。
> 阶段门槛：每阶段补丁 ≤25 文件、`native` 行为不变、不得改动 `rustfs/src/app/**`。

### P1a 注入缝（已完成）

- [x] T1a.1 在 `crates/ecstore` 新增 `store::external`（`StorageDriver` + `ExternalBackend` 桶操作契约 + 进程级安装点），并通过 `lib.rs` 仅导出该缝。
  - 文件：`crates/ecstore/src/store/external.rs`（新）、`crates/ecstore/src/store/mod.rs`、`crates/ecstore/src/lib.rs`
  - 验收：`cargo check -p rustfs-ecstore --lib` 零警告；`cargo test -p rustfs-ecstore --lib store::external::tests` 3/3 通过；`cargo fmt -p rustfs-ecstore -- --check` 通过；`cargo check -p rustfs --bins` 通过
- [x] T1a.2 `BucketOperations::make_bucket/get_bucket_info/list_bucket/delete_bucket` 在已安装后端时委托，否则走原 ECStore 路径。
  - 验收：native 分支无额外状态依赖；未安装时行为与上游一致（由 T1a.1 单测 `native_mode_has_no_backend` 覆盖）

### P1b 对象读写（已完成切换，待启动接线）

- [x] T1b.1 扩展 `ExternalBackend`：`get_object_info/put_object/get_object_reader/delete_object/list_objects_v2/list_object_versions`（签名沿用 `storage_api_contracts`）。
  - 文件：`crates/ecstore/src/store/external.rs`、`crates/ecstore/src/store/mod.rs`
  - 验收：`cargo check -p rustfs-ecstore --lib` 零警告；`cargo test -p rustfs-ecstore --lib store::external::tests` 3/3 通过；提交 `55b988a53` 已推送
- [ ] T1b.2 EC 专属方法（rebalance/decommission/heal/scanner）的显式 501 门控。
  - 调整：P1c 不再要求改 Admin 路由；powerfs 模式的控制面只有一个空目录，EC 专属后台任务无数据可操作。显式 501 与 capability 快照统一在 **T3.2** 实现，避免本阶段扩大 fork 改动面。

### P1c 真实后端与启动接线（已决策，待实施）

> 决策依据：[rustfs-integration-spec.md](rustfs-integration-spec.md) §4.4（D1–D10）。
> 关键事实：master 的 entry RPC 全部 `UNIMPLEMENTED`；元数据必须走 Filer `FilerMetaService`，客户端已由 `powerfs_master::filer_proto` 提供。

- [x] T1c.1 适配 crate 改为使用 Filer 元数据客户端（替换 `RemoteDirectoryTree`）。
  - 文件：`powerfs-rustfs-store/src/{config,filer,backend,bucket,object,list,error}.rs`、`Cargo.toml`
  - 要点：`POWERFS_FILER_ENDPOINTS` 必填；`FilerMetaServiceClient` 端点轮询；Filer 不做服务端转发，写类 RPC 的 `not_leader`/apply 超时与读类的「未找到」均换端点重试（每端点一次）；`client_id` 为进程 UUID v4；桶/对象/列举按 spec §4.4 D4–D7 映射；类型统一为 `powerfs_master::filer_proto::powerfs::{Entry,FuseAttributes,FileChunk}`；needle 数据面走 `powerfs_net::TlvVolumeClient`（TLV；master 下发的 `grpc_port` 实为 net_port，tonic `VolumeClientPool` 拨过去会被握手拒绝），`TlvVolumeClient` 随之从 `powerfs-filer` 迁入 `powerfs-net` 供两处共用；TLV 单帧 DATA 上限 2MB，超限明确报错（P2 分块）
  - 验收：`cargo check/clippy -D warnings/fmt --check` 全绿；`filer.rs` 带端点轮询与错误映射单测（不连真实服务）——7/7 单测通过
- [x] T1c.2 启动接线：`RUSTFS_STORAGE_DRIVER=powerfs` 时安装后端并保留控制面 ECStore。
  - 文件：`rustfs/rustfs/Cargo.toml`、`rustfs/rustfs/src/startup_entrypoint.rs`（`run()`）、`rustfs/crates/ecstore/src/store/external.rs`（`backend_for_bucket` 系统桶豁免）、`rustfs/crates/ecstore/src/store/mod.rs`（委托点）
  - 要点：native 分支零改动；powerfs 模式 `config.volumes` 为空时用单临时目录（不触发同盘检查）；构造失败 fail-fast，不静默回退；系统桶（`.rustfs.sys`/`.minio.sys`）留在控制面 native ECStore，仅用户桶委托；`make_bucket`/`delete_bucket` 同时维护控制面桶元数据（`BucketMetadataSys` 依赖）；`get_object_info_for_delete` 与写入端同后端；S3 PUT 走 `put_object_with_old_current_size`，必须同样委托；S3 DELETE 走固有方法 `delete_object_with_tier_delete_journal_and_guards`、批量删除走 `delete_objects_with_tier_delete_journal_and_accounting`，均已委托；日志打印 `storage_driver/filer_endpoints/collection/replication`
  - 验收：native 模式启动日志无 backend 安装行、s3-tests 同组 19/19 通过；powerfs 模式启动日志 `external storage backend installed` 且无 panic
- [x] T1c.3 跑 s3-tests 基础对象组并记录通过清单。
  - 覆盖：桶创建/删除/重复创建、PUT/GET/HEAD/DELETE、0 字节对象、覆盖写后再读、前缀列举、delimiter 列举
  - 验收：本组全绿；每个对象 MD5 与 ETag 一致；删除后 needle 不存在；失败项记录 issue 且不改测试
  - 结果（2026-10-27）：手写 CRUD 22/22；s3-tests 19/19 通过
    - 桶组 7/7：`test_bucket_create_delete`、`test_bucket_create_exists`、`test_bucket_delete_notexist`、`test_bucket_delete_nonempty`、`test_bucket_head`、`test_bucket_head_notexist`、`test_bucket_list_empty`
    - 对象组 6/6：`test_object_write_check_etag`、`test_object_write_read_update_read_delete`、`test_object_head_zero_bytes`、`test_object_write_to_nonexist_bucket`、`test_object_read_not_exist`、`test_object_write_cache_control`
    - 列举组 6/6：`test_bucket_list_distinct`、`test_bucket_list_many`、`test_basic_key_count`、`test_bucket_listv2_prefix_basic`、`test_bucket_list_delimiter_basic`、`test_bucket_list_maxkeys_one`
    - native 模式同组 19/19，行为未回归


## P2 元数据与 S3 语义

- [ ] T2.1 版本化与删除标记（`versionId`、`null`、delete marker、多版本列举）。
  - 文件：`powerfs-master/src/object_meta/store.rs`、`powerfs-rustfs-store/src/object.rs`、`src/list.rs`
  - 验收：`s3-tests` versioning 组通过；覆盖写与删除语义正确
- [ ] T2.2 Multipart 全流程与最小分块约束。
  - 文件：`powerfs-rustfs-store/src/multipart.rs`、master service、volume blob
  - 验收：分块组通过；Abort 后无残留 blob（GC 后）
- [ ] T2.3 条件请求、Range、校验和（CRC32C/CRC64NVME/SHA256 按上游能力）。
  - 验收：条件请求与 Range 用例通过；校验失败错误码与上游一致
- [ ] T2.4 对象元数据：content-type、user metadata、tags、storage class、对象锁字段。
  - 验收：HEAD 元数据与 S3 语义一致；对象锁/保留用例通过
- [ ] T2.5 列举分页一致性：continuation token + bucket epoch。
  - 验收：并发写删场景下分页不重不漏；epoch 变化返回可重试用错误
- [ ] T2.6 命名空间锁映射到 master 租约。
  - 文件：`powerfs-rustfs-store/src/lock.rs`
  - 验收：同键并发 PUT/COPY 串行化；租约超时与续约用例通过

## P3 管理面与生命周期

- [ ] T3.1 IAM/STS 与策略持久化走 `StorageEngine`，不依赖 ECStore 内部桶语义。
  - 文件：`rustfs/rustfs/src/app/context/interfaces.rs`、`powerfs-rustfs-store/src/object.rs`
  - 验收：策略读写、用户/角色、临时凭证用例通过
- [ ] T3.2 Admin/Console capability 门控：ECStore 专属路由返回 501，暴露 `storage_driver` 与能力快照。
  - 文件：`rustfs/rustfs/src/admin/*`、`powerfs-rustfs-store/src/capability.rs`
  - 验收：控制台可登录；不支持项有明确提示与 501 用例
- [ ] T3.3 生命周期与过期删除接入同一删除 API。
  - 验收：生命周期配置生效；过期对象按版本规则删除
- [ ] T3.4 复制、通知、审计能力声明与接线（支持则实现，不支持则 capability 标注）。
  - 验收：事件通知可投递到测试目标；审计不泄漏敏感信息

## P4 集群、故障与性能

- [ ] T4.1 多节点 PowerFS 后端与 master leader 切换。
  - 验收：leader 切换期间写返回明确错误或重试成功；读不返回错数据
- [ ] T4.2 卷故障/离线/恢复：detect → 元数据可用性 → blob 修复或明确不可用。
  - 验收：故障注入脚本可重复；恢复后对象可读且校验通过
- [ ] T4.3 GC 与悬挂 blob 清理（租约超时、Abort、失败 Put）。
  - 验收：扫描前后空间一致；无误删在用 blob
- [ ] T4.4 性能评估：1GB 小规模验证 → 专用 1TB 数据集；`s3bench`/`warp`，必要时 `fio`。
  - 验收：输出评估报告 MD（吞吐、延迟、P99、对比基线）
- [ ] T4.5 连续运行稳定性（按既有规范：≥1 分钟无异常，定期检查日志/指标）。

## P5 交付与退役

- [ ] T5.1 配置单一源：`.powerfs/cluster.toml` 增加 `[rustfs]`；`powerfs-ctl config render` 生成配置与 compose。
  - 验收：无硬编码 IP；`scripts/check-ip-literals.sh` 通过
- [ ] T5.2 systemd/deb/compose 集成与启停脚本。
  - 文件：`powerfs-packaging/systemd/*`、`powerfs-packaging/scripts/*`、`docker/*`
  - 验收：容器内安装、启动、停止全流程脚本化通过
- [ ] T5.3 删除自研 S3：`powerfs-s3/`、`powerfs-master/src/s3/`、相关配置与文档入口。
  - 验收：工作区无残留引用；构建与测试通过
- [ ] T5.4 文档更新：README 能力说明、部署文档、能力矩阵（支持/不支持）。
- [ ] T5.5 提交：fork 补丁提交 + 父仓库 submodule 指针更新 + 父仓库改动；记录 commit 与验证结果。

## 验证命令基线（按阶段取用）

```bash
# 编译与静态检查
cargo check -p powerfs-rustfs-store
cargo clippy -p powerfs-rustfs-store --all-targets
cargo fmt --all --check

# rustfs fork
cargo build --release --bin rustfs
cargo test -p rustfs-ecstore      # 确认 native 模式未回归
make pre-commit                   # 仅在跨模块改动时

# S3 功能
# s3-tests：按对象/版本化/分块/列举/条件请求分组分批执行

# 配置单一源与 lint
powerfs-ctl config render
scripts/check-ip-literals.sh
```
