# P6 EC Read：RustFS gateway 的 EC 对象读路径

> 目标：scrubber 把对象转成 EC(4+2) 分片后，S3（RustFS gateway）仍能正确读、正确 Range、正确降级
> 重建，且绝不返回错数据。对应问题条目见 [rustfs-p2d-issues.md](./rustfs-p2d-issues.md) 第 2 条。

## 1. 设计

### 1.1 元数据暴露

- `powerfs-filer/proto/filer.proto` 与 `powerfs-master/proto/master.proto`（后者为 master 内置 S3 独立
  生成的副本）新增 `EcLayout { data_shards; parity_shards; shard_size; }`，`Entry` 追加
  `EcLayout ec_layout = 15`（纯 additive，1..14 不变）。
- `powerfs-filer/src/grpc_service.rs::ec_layout_from_inode`：**只以 `reliability_state == EC` 为权威**，
  `reliability` 仅用于取 data/parity；二者不一致、chunks 为空或 `len % (data+parity) != 0` 时记 `warn!`
  并返回 `None`（退回平铺读），绝不按猜测布局解析用户数据。
- `shard_size` 复用 `scrubber::EC_SHARD_SIZE`（`pub(crate)`），读路径不再出现第二份 1MiB 字面量。

### 1.2 读路径（`powerfs-rustfs-store/src/ec.rs`）

`object.rs::read_object` 按 `entry.ec_layout` 分叉：`Some` → `read_ec_range`；`None` → 原平铺循环（零改动）。

分片身份**只按 group 内索引**判定（数据分片 `i < data`），不使用 `offset`——校验分片的 offset 与数据
分片 0 重叠（都是 group 基址）。

每个 group 覆盖 `[g*data*shard_size, (g+1)*data*shard_size)`，末组不足一 stripe 时零填充，
`entry.content_size` 恒为对象真实长度，读出后按真实长度截断。

1. **快路径**：只读与请求区间相交的数据分片（小 Range 只打 1 个 needle），逐分片 CRC32 校验
   （`crc32 != 0` 才校验）后按索引拼接裁剪。
2. **降级路径**：任一数据分片读失败/长度不符/CRC 不符即整组降级，读满 `data+parity` 个分片，
   CRC 不符视为缺失，用 `EcEncoder::decode_missing`（Reed-Solomon）重建。
3. **不可恢复**：可用分片 `< data` 时返回错误（S3 500），**不返回任何部分数据**。

### 1.3 元数据自洽

- `shard_store.rs::update_inode_size_chunks_atomic`：chunks 变化且原状态为 `Replicated | EC` 时，
  复位状态为 `PendingReplicated`，同时复位 `reliability = Reliability::default()` 并清空
  `replica_chunks`，避免陈旧 EC 标记让读路径用错布局解析新写入的平铺数据。
- `PutObjectMeta` 中 extended 属性（含 EC 豁免标记）写入提前到 chunks 更新之前，保证 scrubber
  不会出现「先看到数据、后看到标记」的窗口。

### 1.4 EC 豁免：multipart 分块不得转换

`complete_multipart_upload` 只按 chunk 顺序平移偏移拼接分块，不知道分块已被转成 group-major 分片。

- gateway 写 MPU 分块时在 extended 里打 `powerfs.ec-exempt`（常量在 `powerfs-common`，
  `EC_EXEMPT_EXT_KEY`）；
- filer `ShardStore::list_pending_ec` 跳过带该键的 inode；
- `complete_multipart_upload` 兜底：若分块仍带 `ec_layout`（历史脏数据）直接报错，绝不静默拼坏。

### 1.5 master 内置 S3（历史 demo 面）

`powerfs-master/src/s3/server.rs::get_object` 对 `ec_layout.is_some()` 返回 `501` +
`x-powerfs-note: ec read unsupported on legacy S3`。已知限制：生产链路 `powerfs-s3` 的元数据经
master gRPC `get_entry` 获取，而该 RPC 当前返回 `UNIMPLEMENTED`，故该守卫在现有链路不会触发；
真正的读能力在 RustFS gateway。

## 2. 验证（容器集成，EC 开启）

环境：`powerfs-master-{1,2,3}`、`powerfs-volume-{1..6}`、`powerfs-filer-{1,2,3}`、
`rustfs-p1c`（gateway，`localhost:19000`）。验证时以 `POWERFS_EC_MIN_FILE_SIZE=0` 开启转换
（Task 7 已把模板默认改为 `0`，即 EC 默认开启），`POWERFS_SCRUBBER_SCAN_INTERVAL=30`。

### 2.1 尺寸矩阵（PUT → 等 EC 转换 → GET 逐字节比对）

判定 EC 已生效的方式：直连 filer gRPC `GetObjectMeta`，确认 `Entry` 字段 15（`ec_layout`）存在
且 `chunks` 为 `6 的倍数`、等长分片——不依赖 scrubber 日志正则。

| 对象 | 转换后 chunks | 分组 | 转换后 HEAD | GET 逐字节 |
|------|--------------|------|-------------|-----------|
| 1MiB | 6 | 1 | 一致 | 一致 |
| 4MiB+1B | 12 | 2 | 一致（`Content-Length=4194305`、ETag 不变） | 一致 |
| 8MiB | 12 | 2 | 一致 | 一致 |
| 30MiB | 48 | 8 | 一致 | 一致 |
| 100MiB | 150 | 25 | 一致 | 一致 |

`groups == ceil(size / (data*shard_size))` 全部成立，末组零填充被正确截断（4MiB+1B 用例）。

### 2.2 Range 矩阵（30MiB EC 对象，`group_data_size=4MiB`）

| Range | 场景 | 结果 |
|-------|------|------|
| `bytes=0-99` | 头 100B | 逐字节一致，`Content-Range` 正确 |
| `bytes=1048570-1048580` | 跨 shard 边界（1MiB） | 一致 |
| `bytes=4194204-4194504` | 跨 group 边界（4MiB） | 一致（201B，跨两组各取一段） |
| `bytes=-100` | 后缀 100B | 一致 |
| `bytes=31457279-31457279` | 末字节 | 一致 |

### 2.3 降级读（4MiB 单 group EC(4+2)，真实停容器）

`volume_id → 容器` 通过 `/data/volume_<id>` 目录反查，避免日志串扰误判。

| 场景 | 结果 |
|------|------|
| 基线（全部 volume 在线） | 逐字节一致 |
| stop 1 个数据分片所在 volume | 降级重建，逐字节一致 |
| stop 2 个 | 降级重建，逐字节一致 |
| stop 3 个（可用 3 < data 4） | HTTP 500，**未返回任何错误数据** |
| 三个 volume 重新 start | 恢复后逐字节一致 |

### 2.4 回归（s3-tests，EC 开/关对照）

| 组 | EC 开 | EC 关 | 结论 |
|----|-------|-------|------|
| `-k multipart` | 32 failed / 32 passed | 32 failed / 32 passed | 失败清单**逐条相同** |
| `-k "(if_match or if_none_match or get_object_range)"` | 25 failed | 25 failed | 失败清单**逐条相同** |

- 失败项均为既有未实现能力（checksum 头、SSE-KMS/SSE-C、版本化、bucket policy、`delete_objects`
  批量错误体、`PartCount`/`Owner.Id` 等），与 EC 无关。
- 修复前 EC 开启时 multipart 多出 2 条真实失败：
  `test_multipart_upload_resend_part`、`test_multipart_upload_overwrite_existing_object`；
  打 EC 豁免标记后两条转为 PASS，且 EC 开/关失败清单完全一致。

### 2.5 重传探针

`/tmp/resend_probe.py` 120s、40 轮、每轮重传 1~6 个分块：`failures=0`（EC 开启）。

### 2.6 日志

- filer ×3、近 60 分钟：`decode_missing` 0 次、`CRC32 mismatch` 0 次。
- gateway 近 60 分钟：`EC 快路径失败` 0 次（正常读全部命中快路径）；`not enough EC shards` 3 次，
  恰好对应降级测试中「停 3 个数据分片」的负向用例。

### 2.7 性能（100MiB 顺序 GET，各 3 次取中位数）

| 模式 | TTFB | 总耗时 | 吞吐 |
|------|------|--------|------|
| EC 关闭（平铺 chunk） | 493.9 ms | 546.7 ms | 182.9 MiB/s |
| EC 开启（1MiB shard 快路径） | 645.6 ms | 692.4 ms | 144.4 MiB/s |

EC 读吞吐约为平铺的 79%，主要开销是逐分片 RPC + CRC32 校验（25 个 group × 4 个数据分片）。
后续如需优化可考虑合并相邻分片 RPC 或批量读，但不属于本任务范围。

## 3. 已知限制与遗留

1. master 内置 S3 的 501 守卫在现有链路不触发（`get_entry` UNIMPLEMENTED），见 issues 文档。
2. 修复前被转成 EC 的 `.powerfs.multipart` 历史分块属脏数据：`complete_multipart_upload` 现在会
   明确报错而不是拼错；需要一次性清理脚本（与 issues 第 4、8 条一并处理）。
3. 历史残留对象存在「元数据 size=1MiB、实际 needle 仅 3 字节」的不一致（issues 第 8 条），
   scrubber 每轮会重试它们，但不影响新建对象。
4. EC 分片丢失后**不会自愈**（读时重建但 scrubber 不重编码）、EC 读无分片级缓存、
   与 FUSE 侧 EC 读逻辑重复，见 issues 文档第 9~11 条。
