# 配置去硬编码优化方案

> 状态：P1 已完成；P2 已完成（含 cold-start 端到端验证）；P3（脚本接入单源）、P4（防回归）待实施
> 日期：2026-10-26
> 关联 issue：private 库「config: eliminate hardcoded IPs and duplicated config files」

## 1. 问题背景

用户反馈：仓库内存在大量写死的配置文件、IP 地址和拓扑参数，修改一个节点需要改动十几到几十个文件，且副本之间已经发生漂移。

## 2. 现状盘点（扫描于 master @ 1b92c1e8）

### 2.1 配置副本与漂移

| 目录 | 文件数 | 行数 | 网段 | 用途 |
|---|---|---|---|---|
| `docker/config/` | 35（含 `rdma/`、`rdma-host/`） | 646 | 172.30.0.x | 主 compose、test compose、rdma compose |
| `config/` | 14 | 198 | 172.20.0.x | 仅 kernel-test 使用，其余为死副本 |
| `powerfs-packaging/config-templates/` | 3 | 178 | 混合 | 打包模板 |
| `.powerfs/rendered/`（生成物，未入库） | 15 | 305 | 172.30.0.x | `powerfs-ctl` 渲染产物 |

**已验证的漂移**：`config/master-1.toml` 与 `docker/config/master-1.toml` 差异 94 行——网段不同（172.20 vs 172.30）、`redis_url` 不同（`redis:6379` vs `172.30.0.50:6379`）、`ca_dir` 一个被注释、一个启用。

### 2.2 IP 字面量分布

| 区域 | 出现次数 | 涉及文件数 |
|---|---|---|
| `docker/config/` | 346 | 35 |
| `config/` | 50 | 14 |
| `powerfs-packaging/` | 52 | 2 |
| `scripts/` | 50 | 15 |
| `powerfs-monitor-frontend/` | 18 | 4 |
| `kernel/vm/`（子模块） | ~173 | 18+（共 54 个脚本） |

### 2.3 单 IP 变更的爆炸半径

| IP | 角色 | 引用文件数 |
|---|---|---|
| 172.30.0.11 | master-1 | 46 |
| 172.30.0.50 | redis | 40 |
| 172.30.0.31 | filer-1 | 25 |
| 172.30.0.21 | volume-1 | 23 |
| 172.30.0.41 | fuse-1 | 9 |

新增一个 volume 节点需要同时改：`docker/docker-compose.yml`（服务段 + IP + 卷挂载）、`docker/config/volume-N.toml`、`scripts/generate-certs.sh` 的 `THREE_VOLUME_IPS` 数组、FUSE 启动脚本、相关测试脚本。

### 2.4 已有能力

`powerfs-ctl` 已具备正确的单源机制：

- 单源文件：`.powerfs/cluster.toml`（`[cluster]` / `[network]` / `[nodes.*]`）
- 渲染命令：`powerfs-ctl config render` → `.powerfs/rendered/{docker-compose.yml, config/*.toml}`
- 一键部署：`powerfs-ctl bootstrap --profile ha`
- README 已将 `powerfs-ctl` 列为推荐路径

但该机制**不是唯一路径**：

1. 渲染产物（343 行 compose）与仓库手工 compose（625 行）并存，互不同步
2. `docker compose` 仍挂载静态 `./config/master-1.toml`，不读渲染结果
3. `generate-certs.sh` 自维护 IP 数组，不读 `cluster.toml`
4. VM 54 个脚本各自带 `:-` 默认值，未集中
5. 无防回归检查，新增 IP 字面量不会被拦截

### 2.5 已发现的坏引用

- `docker/docker-compose.crdt-test.yml` 挂载 `../config/volume-crdt-1.toml`，该文件**不存在**（CRDT 测试环境当前无法启动）
- 进一步核查：master/filer 已改为 `--config` **必填**，而 crdt-test compose 仍传 `--port/--dir/--peer` 等旧式参数 → 即使补齐配置，该环境仍会在参数解析阶段失败；需单独评估“重写为配置化”或“删除”
- `config/` 下 12 个文件无任何引用（死副本）

## 3. 根因

1. **拓扑信息重复编码在 5 处**：compose、每节点 TOML、证书脚本、FUSE 启动脚本、VM/测试脚本
2. **legacy 路径未退役**：手工 compose 与渲染 compose、`config/` 与 `docker/config/` 并存并已漂移
3. **无防回归机制**：仓库允许随时新增 IP 字面量，CI 无检查

## 4. 方案（分阶段）

### P1：收敛副本、修复坏引用（本次）

1. 删除 `config/` 下 12 个无引用文件
2. 将 kernel-test 实际使用的 `master-1.toml`、`volume-1.toml` 迁入 `docker/config/kernel-test/`，更新 compose 挂载路径（保留 172.20 独立测试拓扑）
3. 补齐 `docker/config/crdt-test/volume-1.toml`（172.31 拓扑），修复 crdt-test 坏引用
4. 验收：全仓库无悬空配置引用；`docker compose config` 校验通过

### P1-2：渲染模板对齐（下一步）

- 对齐 `powerfs-ctl render` 与仓库 compose 的差异：logging 锚点已具备；dedicated fuse 服务按设计**不进入**渲染 compose（由 `client enroll` + 启动脚本管理），需在文档中明确
- 主 compose 与静态 config 的挂载路径改为 rendered 产物前，必须先适配 CI nightly 的“后端恢复”路径（`cd docker && docker compose -f docker-compose.yml up -d`），否则 runner 上无 rendered 目录会直接失败

### P2：主部署改为渲染产物 ✅ 已完成（commit `78fe585b` + `cbb6bfaa` + `a0af91a6`）

仓库内手工 compose / 静态 config 全部清零，唯一路径 = `cluster.toml → powerfs-ctl bootstrap`。

- 删除 `docker/docker-compose.yml`、`docker-compose.test.yml`、`docker/config/` 主拓扑 17 份副本、8 个手写 compose 驱动脚本、`scripts/start-fuse-clients.sh`（已被 `docker/run-fuse-client.sh` 取代）
- 重写 `docker/scripts/start-cluster.sh` / `stop-cluster.sh` 为薄封装：调 `powerfs-ctl bootstrap` / `powerfs-ctl down`
- 模板暴露 `POWERFS_EC_DATA_SHARDS / PARITY_SHARDS`（master）和 `POWERFS_SCRUBBER_SCAN_INTERVAL / MAX_INODES / EC_MIN_FILE_SIZE`（filer）环境变量——docker/tests EC 场景 export 后 bootstrap 即可
- CI `check-topology-sync.sh` + `ci-cd.yml` quality job：从 `docker/topology.env` 派生 fuse 客户端声明 → 临时 home render → 校验 rendered compose + cluster.toml 与 topology.env 一致
- 全部 `docker/tests/*.sh` 容器名前缀适配（parse cluster.toml → PFIX，`docker compose -f "$COMPOSE_FILE"` 显式指定渲染路径）

保留独立测试拓扑（按 plan）：`docker-compose.{kernel-test,rdma,crdt-test,simple}.yml` + `docker/config/{kernel-test,crdt-test,rdma,rdma-host}/` 四个子目录。

**Cold-start 端到端验证**（2026-10-26，self-hosted runner 本机）：
- 停集群 → 删 15 个 docker volumes → 删 fuse 容器 → 删 powerfs_powerfs-network
- `docker/restore-backend.sh` 从零 bootstrap：term=1, commit_index=11, 15 证书全签, fuse-1/2 启动, CA 匹配 certs-default
- `docker exec fuse-1`: dd 4MiB + sync + md5 ✅
- `docker exec fuse-2`: echo → 写 fuse-2 mount → cat fuse-1 mount → 一致 ✅（跨容器一致性）
- `check-topology-sync.sh`: 17 addresses, 172.30.0.0/16 ✅

### P3：脚本接入单源

- `scripts/generate-certs.sh` 从 `cluster.toml` / rendered 证书清单读取 IP，删除硬编码数组
  - **已完成部分**：新增 `docker/topology.env` 作为 legacy Docker 拓扑唯一来源，`generate-certs.sh` 优先加载该文件（缺省值保留为兜底）；改节点 IP 只需改这一处
  - **待完成**：`MASTER_API`、VM 脚本（54 个）尚未接入
- `docker/run-fuse-client.sh` 已参数化（name/ip 由参数传入），无需改动
- VM 54 个脚本统一 `kernel/vm/cluster.env`，删除各自默认值

### P1-3：端到端验证（已完成，2026-10-26）

验证方式：simple profile 测试 home（`.powerfs-uni-test/`，未入库），**只改 `cluster.toml` 两处**——`[network].subnet` 172.41.0.0/16 → 172.42.0.0/16、`[client.fuse-uni].ip` 172.41.0.100 → 172.42.0.100，然后重跑 `powerfs-ctl bootstrap`。

| 检查项 | 结果 |
|---|---|
| rendered compose 的 subnet/gateway/全部 `ipv4_address` 随单源变化 | 通过（172.41 残留 0） |
| rendered 节点配置的 `redis_url`/`master_addresses`/`advertise_addr`/`node_id` | 通过（全部 172.42） |
| bootstrap 全链路（render → leader 门禁 → CA → filer/volume/fuse 重签 → 全量 up） | 通过（term=1, commit_index=4） |
| FUSE 挂载 4 MiB 随机写 + sync + md5 回读 | 通过（校验一致），小文件读写正常 |
| `rendered/certs/client_registry.json` 历史签发记录 | 仍含旧地址（仅审计记录，不影响运行） |

验证中修复的阻塞 bug：`spawn_health_monitor` 在 `has_peers == false` 时直接 return，单节点集群从不发布 `powerfs_is_leader`/`powerfs_raft_*`，`/metrics` 为空导致健康门禁 120s 超时。已改为单节点照常发布指标、仅跳过假 Leader探测（commit `2dd3adfd`）。

### P2 前置阻塞（实施中发现）

1. ~~**证书命名两套不统一**~~ **已完成**：统一为 `volume-N`（与 `powerfs-ctl` 声明式路径一致）。改动覆盖 `docker/topology.env`、`generate-certs.sh`、`docker/config/**`（含 rdma/rdma-host/kernel-test 及 master 参考段）、`powerfs-ctl` 之外的 CLI 生成器/测试/文档，并重新签发 `docker/certs-default/` 整套证书（CA 不变，registry 重建为干净夹具）。已实测：legacy 三 master + 6 volume 以新名注册、mTLS 通过、FUSE IO 校验一致；旧 `volume-server-N` 僵尸节点经 `manage node-remove` 清理；声明式 bootstrap + FUSE IO 回归通过
2. ~~**FUSE 每客户端参数**~~ **已完成**：`[client.<name>]` schema 扩展 `admin_port`/`verbose`/`container`/`log_file`/`force_mount`/`request_timeout_secs` 六个字段，`fuse.toml.j2` 按客户端渲染（`force_mount=true` 时自动带出 filer/volume 兜底地址），`client enroll`/`bootstrap` 产出的 `client-<name>.toml` 覆盖静态 `fuse-1/2.toml` 全部键。已实测：uni 集群 bootstrap 渲染出全部字段、FUSE 挂载 + 4MiB IO md5 一致、`request_timeout_secs=5` 生效。发现两项运行时惰键（另行跟踪，不影响渲染对齐）：`admin_port` 仅打警告日志（客户端已无 HTTP 端点，统计走 master `fuse-stats`）；`log_file` 读入后未接日志后端（`_log_file` 未使用）
3. ~~**主 compose 含 fuse-1/fuse-2 服务**~~ **已完成（退役方式确定并演练）**：渲染 compose 永不包含 fuse 服务（客户端生命周期独立于后端，避免 `compose down -v` 强杀挂载进程）；fuse 客户端经 cluster.toml `[client.*]` 声明（`powerfs-ctl init` 模板已加带注释示例）→ bootstrap 自动 enroll + 渲染 `client-<name>.toml` → `docker/run-fuse-client.sh <name> <ip>` 启动同名容器。`docker exec fuse-1` / `docker restart fuse-1` 类测试脚本零改动。README legacy compose 段已加退役说明；默认 home `.powerfs/cluster.toml` 已声明 fuse-1/2（172.30.0.41/42，verbose/container/force_mount 与静态配置对齐，惰键 admin_port/log_file 不带入新单源）。已实测：uni 集群声明 `[client.fuse-1]` 后 bootstrap 自动签发+渲染，`run-fuse-client.sh fuse-1` 启动、`docker exec fuse-1` 4MiB IO md5 一致、`docker restart` 后重挂载正常
4. ~~**CI runner 恢复路径**~~ **已完成**：新增 `docker/restore-backend.sh` 作为唯一权威恢复路径——外来集群守卫（拒绝覆盖运行中的异名 `*-master-1`）→ 清理空闲 legacy 网络（bridge 冲突）→ 从 `docker/certs-default` 种子化 master CA（保持 kernel/fuse 客户端证书有效，含 root 属主目录处理）→ `powerfs-ctl bootstrap`（幂等）→ 启动 cluster.toml 声明的 fuse 客户端 → 校验 master 容器健康且 CA 指纹匹配。nightly workflow「Check Docker services」改为：后端缺失时自动执行恢复脚本自愈（r760 事件类问题根除），CA 不符仍硬失败（保护调试现场）。容器名检查从 `^(master-1|master-1-test)$` 改为从 cluster.toml 派生的 `<cluster>-master-1`。已在本机（即 self-hosted runner）演练：全量 bootstrap（3 master raft 选主、6 volume + 3 filer + 2 fuse 客户端注册）、CA 匹配、fuse-1 4MiB IO md5 一致、down 后自愈重放通过（二次运行跳过种子化）
5. **compose 项目名与容器名固定**：`DockerComposeDriver` 未传 `-p`，项目名取目录名（恒为 `rendered`），模板 `container_name` 也写死 `master-1`/`redis`/…。两个 home 并存时会共享 `rendered_*` 卷/网络并撞容器名——首轮 live 验证即因复用旧 3 节点 raft 卷而选不出 leader。~~需由 `cluster.name` 派生项目名与容器前缀~~ **已完成**：渲染 compose 声明 top-level `name: <cluster.name>`，容器名统一 `<cluster>-<svc>` 前缀，`cluster.name` 增加 compose 项目名规则校验（`[a-z0-9][a-z0-9_-]*`），`run-fuse-client.sh` 默认网络名改为从 cluster.toml 派生；monitor 宿主端口改为可选 `cluster.monitor_host_port`（缺省不发布，避免多集群撞 8083）。已实测 powerfs(HA) 与 powerfs-uni(simple) 双集群并存，FUSE IO 正常

### P4：防回归

- **已完成部分**：`scripts/check-topology-sync.sh` 校验 `docker/topology.env` 与主 compose 的地址集合、网段、网关一致，已接入 CI `quality` job
- CI 增加 IP 字面量 lint（白名单：rendered 产物、测试 fixtures）
- 文档只保留单源路径；删除“手工改 compose/config”的指引

## 5. 风险与回滚

| 风险 | 缓解 |
|---|---|
| 删除 `config/` 影响 kernel-test | 先迁移到 `docker/config/kernel-test/` 并校验挂载路径 |
| 主 compose 改 rendered 后 CI 恢复路径断裂 | 放到 P2，与 workflow 同批修改并演练 |
| 不同测试拓扑无法共用一份 cluster.toml | 测试 compose 保留独立文件，仅参数化 IP |
| 回滚 | 每阶段独立提交，`git revert` 即可回退 |

## 6. 验收标准

- P1：`git grep` 无 `../config/` 悬空引用；`docker compose config -q` 对受影响的 compose 全部通过；CI 全绿
- P2：删除静态 `docker/config/` 后，`powerfs-ctl bootstrap` 与 CI nightly 均能正常起集群
- P3：修改 `cluster.toml` 中任一节点 IP 后，无需改动任何脚本即可完成部署
- P4：新增 IP 字面量的 PR 被 CI 拦截
