# 配置去硬编码优化方案

> 状态：P1 实施中
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

### P2：主部署改为渲染产物

- 删除 `docker/config/` 静态副本，`docker/docker-compose.yml` 由 `powerfs-ctl config render` 生成
- CI/运维脚本先执行 render，再 compose up
- 保留 test/rdma/simple/kernel-test/crdt-test 独立 compose（不同拓扑），但参数化 IP 并共享基础片段

### P3：脚本接入单源

- `scripts/generate-certs.sh` 从 `cluster.toml` / rendered 证书清单读取 IP，删除硬编码数组
  - **已完成部分**：新增 `docker/topology.env` 作为 legacy Docker 拓扑唯一来源，`generate-certs.sh` 优先加载该文件（缺省值保留为兜底）；改节点 IP 只需改这一处
  - **待完成**：`MASTER_API`、VM 脚本（54 个）尚未接入
- `docker/run-fuse-client.sh` 已参数化（name/ip 由参数传入），无需改动
- VM 54 个脚本统一 `kernel/vm/cluster.env`，删除各自默认值

### P2 前置阻塞（实施中发现）

1. **证书命名两套不统一**：legacy 夹具为 `volume-server-N.crt`，`powerfs-ctl` 声明式路径为 `volume-N.crt`；模板改指向任一命名前，需先统一（否则 mTLS 失败）
2. **FUSE 每客户端参数**：静态 `fuse-1/2.toml` 含 `admin_port=9998`、`verbose/container/log_file`，渲染模板未覆盖；测试 compose 依赖这些值
3. **主 compose 含 fuse-1/fuse-2 服务**：渲染模板不含（设计上由 `client enroll` + `run-fuse-client.sh` 管理），直接替换需同步 README 与脚本
4. **CI runner 恢复路径**：nightly 用 `cd docker && docker compose up -d` 恢复后端；若静态配置改为渲染产物，runner 需先生成 `.powerfs/rendered`，须与 workflow 同批修改并演练

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
