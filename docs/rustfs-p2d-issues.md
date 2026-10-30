# P2d 期间发现的问题清单（待录入私有仓库 Issue）

> 记录时间：2026-10-28。本文件仅作待建 issue 的草稿清单，内容与 `docs/rustfs-integration-tasks.md` 的 P2d 节对应。
> 环境：6 volume / 3 master / 3 filer 容器 + rustfs-p1c gateway。

## 1. [已修复] Filer scrubber 副本复制跨卷复用 needle_id，覆写目标卷既有数据

- **严重级别**：致命（用户数据损坏）
- **现象**：`test_multipart_upload_resend_part` 在 powerfs 后端间歇失败（native 稳定通过）；重传探针可复现 30MiB 对象 9–30MiB 区间被他人数据覆盖。
- **根因**：Master 的 file_key 是**按卷独立计数器**（`allocate_file_key`，步长 1MiB = `FILE_KEY_BLOCK_SIZE`），同一数值会在不同卷上分别分配给不相关的文件；
  `powerfs-filer/src/scrubber.rs::replicate_inode` 直接复用源 `chunk.needle_id` 写入目标卷，一旦该 key 在目标卷已被占用即覆写对方数据。
- **修复**：启用 `MsgType::AssignNeedle`（指定卷分配新 file_key，复用 `allocate_file_key` + Raft 持久化），复制改为向 Master 申请目标卷新 key；`replica_chunks` 记录新 key。
- **同批加固**：scrubber 仅 leader 执行复制；复制失败清理未提交的孤儿副本；GC 回收纳入 `replica_chunks`；scrubber 地址解析改用 Master 拓扑（回退 Zone 视图）。
- **验证**：副本 key 实测全新分配且与源不同；multipart 全组差异仍只有已声明非目标；探针 102/102 无错乱（EC 关闭状态下）。

## 2. [已修复] EC 转换后 S3 读路径返回校验分片垃圾（P6 EC Read）

- **严重级别**：致命（用户数据损坏，修复前通过配置规避）
- **状态**：已由 **P6 EC Read** 修复（`.trae/specs/p6-ec-read/`，7 个 Task 全部完成），设计与验证见
  [rustfs-ec-read.md](./rustfs-ec-read.md)。
- **现象**：EC(4+2) 转换完成后，30MiB 对象 S3 GET 前 4MiB 正确、其后至文件尾全部错乱；`HEAD Content-Length`、ETag 均正常，属持久性损坏。
- **根因**：`ShardStore::update_to_ec` 将 `info.chunks` 整体替换为 EC 分片列表（数据分片 `offset = group_start + i*shard_size`，校验分片 `offset = group_start`），
  但 S3 读路径（`powerfs-rustfs-store/src/object.rs::read_object`、master 内置 S3）按平铺 chunk 顺序拼接，不知道对象是 EC，且把校验分片 offset 当数据读。
  另：gRPC `Entry` 未暴露 EC 布局；`update_inode_size_chunks` 在数据变更后未复位 `reliability`（陈旧 EC 标记）。
- **修复**：`filer.proto`/`master.proto` 增 `EcLayout`（data/parity/shard_size，以 `ReliabilityState::EC` 为权威填充），
  `powerfs-rustfs-store/src/ec.rs` 按 group 读数据分片（CRC 校验）+ 缺失时 RS 重建 + Range 支持；
  master 内置 S3 对 EC 对象返回 501；`update_inode_size_chunks` 复位 `reliability`。
- **同批修复（回归中发现的新缺陷）**：EC 开启后 scrubber 会把 S3 multipart 的 `.powerfs.multipart`
  分块也转成 EC，`complete_multipart_upload` 按原偏移拼接后写出错乱对象（复现：
  `test_multipart_upload_resend_part`、`test_multipart_upload_overwrite_existing_object`）。
  已引入 `powerfs.ec-exempt` 元数据豁免标记（gateway 写分块时打标，filer 待转换队列跳过），
  `complete` 对仍带 `ec_layout` 的分块明确报错。修复后 EC 开/关的 s3-tests 失败清单逐条相同。
- **验证**：尺寸矩阵 1MiB/4MiB+1B/8MiB/30MiB/100MiB 全部逐字节一致；Range 跨 shard/跨 group/后缀/末字节
  全部正确；停 1/2 个数据分片可重建，停 3 个返回 500 且无错数据；日志无 `decode_missing`/CRC 刷屏。

## 3. [待建] master 内置 S3 的 EC 守卫在现有链路不触发

- **严重级别**：低（历史 demo 面，真正的读能力在 RustFS gateway）
- **现象**：`powerfs-master/src/s3/server.rs::get_object` 对 `ec_layout.is_some()` 返回 501，但生产链路
  `powerfs-s3` 的 `RemoteDirectoryTree` 走 master gRPC `get_entry`，该 RPC 当前返回 `UNIMPLEMENTED`
  （元数据已迁至 Filer），因此守卫拿不到 `ec_layout`，实际不会触发。
- **建议**：要么让 master 的 `get_entry` 真正代理 Filer 元数据（含 `ec_layout`），要么明确下线这条
  历史 S3 面，避免出现「以为有保护、实际没有」的错觉。

## 4. [待清理] 历史被转成 EC 的 multipart 分块

- **严重级别**：中（脏数据，会导致 complete 明确报错而非错数据）
- **现象**：修复前（EC 曾开启）写入的 `.powerfs.multipart` 分块已被 scrubber 转为 group-major EC 分片，
  这些分块与所属 upload 会话已失效；`complete_multipart_upload` 现在对这类分块报错。
- **建议**：随第 7 条一起提供一次性清理脚本，按「MPU 分块 + 带 `ec_layout` + 无活跃会话」条件删除。

## 5. [待修] volume 本地 file_key 自增分配器与 Master 块分配器并存

- **严重级别**：高（长期必然撞 key）
- **现象**：`powerfs-core/src/volume.rs`（`file_key == 0` 时本地 `+1` 自增）与 Master 的 `1 + n*1048576` 分配并存；本地小 key（1、2、3…）与 Master 的 `n=0` 键 1 直接重合。
- **影响**：任何走本地分配的写入路径都会与 Master 已分配 key 冲突，造成覆写。
- **建议**：收敛为单一权威分配器（Master 或按卷块分配），本地分配器删除或退化为「仅测试桩」并加编译期隔离。

## 6. [待查] holder=2 小 id 空间覆写

- **严重级别**：中
- **现象**：排查过程中观察到 holder id 为 2 的客户端写入落到小 key 空间，与主对象区域重叠。
- **建议**：复核 volume 侧 holder/lease 校验与 file_key 来源，确认是否为同第 5 条的另一个入口。

## 7. [待补] 既有非目标缺口（P2d 已声明，非本次引入）

- 校验和 `x-amz-checksum-*` 未实现（`test_multipart_checksum_sha256`、`test_multipart_reupload_checksum_and_etag`）。
- `ExternalBackend` 无 `delete_objects`：S3 批量删除落回 native 路径，`test_delete_objects_if_match` 缺 `Errors`。
- HEAD 的 `PartsCount` 头、Multipart 列举 `Owner.Id` 为上游实现缺口（native 同样失败）。
- 重启后注册时序：冷启动瞬间 GET 偶发 500（注册未完成即接流量）。
- 版本化、SSE-KMS/SSE-C、bucket policy、checksum helper 等：s3-tests 对应用例在 native 与 powerfs
  后端同样失败，属上游能力缺口（见 `rustfs-ec-read.md` §2.4 的失败清单对照）。

## 8. [待清理] 环境遗留脏数据

- 集群存在 ~876 个早期残留 `PendingReplicated` inode，引用已销毁卷 UUID（`read_needle status=1`），scrubber 每轮重复告警。
- 另有一批历史对象元数据声明 size=1MiB、实际 needle 仅 3 字节（`read_needle` 长度不符），scrubber
  每轮重试并跳过，形成噪声。
- 建议：提供一次性清理脚本（按「卷不存在」「needle 长度与元数据不符」条件批量置终态或删除），
  避免长期噪声与误判；与第 4 条的历史 EC multipart 分块一并处理。

## 9. [待建] EC 自愈缺失：Degraded 检测与重编码

- **严重级别**：中（读路径已可降级重建，但丢失的分片永不自愈）
- **现象**：P6 EC Read 只在**读**时按需重建（`powerfs-rustfs-store/src/ec.rs::assemble_group`，
  读满 `total` 个分片后 RS 重建并返回正确数据）。scrubber 没有「分片缺失 → 重新编码补副本」的闭环：
  一旦某组丢失 1~2 个分片，该对象会**永久**停留在降级读状态，每次读都要多读全组分片并解码，
  且若再丢一个分片就直接不可读。
- **建议**：scrubber 增加 Degraded 检测（按 `ec_layout` 校验每个 group 的在位分片数与 CRC），
  可用数 `< data` 时告警/置不可用状态，`>= data` 且 `< total` 时用现成 `EcEncoder` 重编码缺失分片，
  写入新 needle 并更新 `chunks`；需要与 Master 的 `AssignNeedle` 配合避免跨卷 key 冲突（见第 1 条）。

## 10. [待建] EC 读无缓存，小 Range 放大为整分片读取

- **严重级别**：低（性能，非正确性）
- **现象**：`read_ec_range` 快路径按与区间相交的数据分片逐个 `read_needle`，1 字节 Range 也会
  拉取整个 `shard_size`（1MiB）。降级路径更会读满该组 `total` 个分片。当前无任何分片缓存，
  高频小 Range 场景（如 S3 分段读）会明显放大 IO。
- **建议**：在 gateway 侧加分片级 LRU 缓存（键：needle_id + CRC/版本），或把 `shard_size`
  按访问模式调小；同时评估「一组的全部分片并行读」以降低降级路径延迟
  （P6 当前为串行读，30MiB 降级读已验证正确但未优化延迟）。

## 11. [技术债] FUSE 客户端与 rustfs-store 的 EC 读逻辑重复

- **严重级别**：中（长期维护风险：两套实现容易语义漂移）
- **现象**：`powerfs-rustfs-store/src/ec.rs` 实现了 group 读取、CRC 校验、RS 重建与末组截断；
  FUSE 客户端（`powerfs-fuse` / 内核态路径）另有独立的 EC 读逻辑（按 `ec_layout` 判断与重建）。
  两处各自处理分片身份（`i < data` 为数据分片、**不能按 offset 判定**）、零填充截断、CRC 规则，
  任一处修正都需同步另一处，否则会出现「S3 读对、FUSE 读错」一类难查问题。
- **建议**：抽出共享 EC 读模块（放 `powerfs-common` 或 `powerfs-core`，只依赖布局与编码器，
  不含传输细节），两侧以「分片读取器」回调注入；补一组共享对拍用例（同 shard 集合 → 同字节输出），
  防止再次漂移。
