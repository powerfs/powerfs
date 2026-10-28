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

## 2. [待修] EC 转换后 S3 读路径返回校验分片垃圾（EC 转换默认已关闭）

- **严重级别**：致命（用户数据损坏，当前通过配置规避）
- **现象**：EC(4+2) 转换完成后，30MiB 对象 S3 GET 前 4MiB 正确、其后至文件尾全部错乱；`HEAD Content-Length`、ETag 均正常，属持久性损坏。
- **根因**：`ShardStore::update_to_ec` 将 `info.chunks` 整体替换为 EC 分片列表（数据分片 `offset = group_start + i*shard_size`，校验分片 `offset = group_start`），
  但 S3 读路径（`powerfs-rustfs-store/src/object.rs::read_object`、master 内置 S3）按平铺 chunk 顺序拼接，不知道对象是 EC，且把校验分片 offset 当数据读。
- **规避**：`POWERFS_EC_MIN_FILE_SIZE` 默认值改为 1TiB（`powerfs-ctl` 渲染模板），EC 专项测试脚本显式 `=0`。
- **建议修复方向**：在 Filer 元数据/`Entry` 中暴露 `reliability`（data/parity）与 EC 布局，S3 读路径按 group 取 data shard 拼接；缺失 data shard 时走 RS 解码。需与 kernel EC 布局语义对齐并做降级读测试。

## 3. [待修] volume 本地 file_key 自增分配器与 Master 块分配器并存

- **严重级别**：高（长期必然撞 key）
- **现象**：`powerfs-core/src/volume.rs`（`file_key == 0` 时本地 `+1` 自增）与 Master 的 `1 + n*1048576` 分配并存；本地小 key（1、2、3…）与 Master 的 `n=0` 键 1 直接重合。
- **影响**：任何走本地分配的写入路径都会与 Master 已分配 key 冲突，造成覆写。
- **建议**：收敛为单一权威分配器（Master 或按卷块分配），本地分配器删除或退化为「仅测试桩」并加编译期隔离。

## 4. [待查] holder=2 小 id 空间覆写

- **严重级别**：中
- **现象**：排查过程中观察到 holder id 为 2 的客户端写入落到小 key 空间，与主对象区域重叠。
- **建议**：复核 volume 侧 holder/lease 校验与 file_key 来源，确认是否为同 #3 的另一个入口。

## 5. [待补] 既有非目标缺口（P2d 已声明，非本次引入）

- 校验和 `x-amz-checksum-*` 未实现（`test_multipart_checksum_sha256`、`test_multipart_reupload_checksum_and_etag`）。
- `ExternalBackend` 无 `delete_objects`：S3 批量删除落回 native 路径，`test_delete_objects_if_match` 缺 `Errors`。
- HEAD 的 `PartsCount` 头、Multipart 列举 `Owner.Id` 为上游实现缺口（native 同样失败）。
- 重启后注册时序：冷启动瞬间 GET 偶发 500（注册未完成即接流量）。

## 6. [待清理] 环境遗留脏数据

- 集群存在 ~876 个早期残留 `PendingReplicated` inode，引用已销毁卷 UUID（`read_needle status=1`），scrubber 每轮重复告警。
- 建议：提供一次性清理脚本（按「卷不存在」条件批量置终态或删除），避免长期噪声与误判。
