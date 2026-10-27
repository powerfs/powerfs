# 配置去硬编码优化方案

> 状态：P1 已完成；P2 已完成（含 cold-start 端到端验证）；P3 已完成（含双 VM 运行时回归）；P4 已完成（IP lint + 文档清理）
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

### P3：脚本接入单源（已完成，2026-10-26）

- `scripts/generate-certs.sh` **✅ 已完成** — 新增 `--from-cluster` 模式（commit `684a8bd8`）：
  - 自动启用条件：cluster.toml + rendered compose 同时存在 **且** 用户未显式传 `--topology`/`--master-api`
  - ADMIN_TOKEN ← cluster.toml `admin_token`
  - MASTER_API ← rendered compose 首 master ipv4_address + 9300
  - 存储节点 id+ip ← rendered compose awk 扫 `role-N:` + 首个 `ipv4_address:`（`found_ip` flag 防 monitor/s3 污染）
  - fuse 客户端 ← cluster.toml `[client.*]` 段 type="fuse" 的 name + ip
  - legacy 路径（host/three 显式传参）**完全保留**，不受影响
  - topology.env 继续是 legacy 路径来源
- `docker/topology.env` 注释已更新：P3 后 from-cluster 优先 cluster.toml，legacy 才读 topology.env
- `kernel/vm/` **✅ 核心 launcher + test 脚本统一 source share/powerfs.env**（kernel submodule `ee811cd`，父仓库 `ec755151`，均已推送）：
  - **扩展 share/powerfs.env**（已被 4 个脚本 source，扩成完整单源）：
    - 顶部加 from-cluster 自动覆盖块（扫描 cluster.toml + rendered compose → awk 派生 MASTER/FILER/VOLUME 地址 + cluster.toml 取 SUBNET/GATEWAY/SHARD_COUNT）
    - 补 POWERFS_FILER_ADDR / POWERFS_VOLUME_ADDR / POWERFS_API_PORT / POWERFS_SUBNET / POWERFS_GATEWAY / VM1_ETH0_IP / VM2_ETH0_IP 等缺失字段
    - 用户 env 显式设置优先覆盖 from-cluster（`${VAR+set}` guard）
    - 默认值保持 host RDMA 拓扑（不破坏 VM 原生场景）
  - **核心 launcher 脚本 10 个**：run_qemu.sh / run_qemu_vm2.sh / run_qemu_kernel_ssh.sh / run_qemu_kernel.sh / run-vm.sh / qemuctl.sh / qemuctl2.sh / setup_network.sh / setup_network_dual.sh / build_initramfs.sh — 加 source powerfs.env，删所有 `:-172.30.0.11,...` / `:-9334` 硬编码 fallback，改用 `: "${VAR:?not set}"` 引用
  - **修正漂移 bug**：filer IP 从旧单节点 `172.30.0.35,172.30.0.36,172.30.0.37` 统一到 HA 集群正确值 `31/32/33`（from-cluster 自动覆盖）
  - **test_*.sh + fault_injection.sh**：批量加 source 块 + sed 替换硬编码 mount 命令里的 `master_addr=172.30.0.11,...` → `${POWERFS_MASTER_ADDR}`
  - **bash -n 全量通过**（kernel/vm/ + kernel/vm/share/ 共 54+107 个脚本）
- `generate-certs.sh` **✅ kernel client from-cluster 已打通**：
  - `[client.<name>] type="kernel"` 现按段名签发（`KERNEL_CLIENT_IDS` + IP 成对入参），不再只打印 INFO
  - 实测：临时 home 声明 `[client.kernel-test] ip=172.30.0.200` → `kernel-test.crt/key` 成功签发，SAN-IP 正确；无声明时输出 `(no kernel clients declared)`
  - 限制：`ClientSpec` 目前只有单 `ip` 字段，VM 双 SAN（ib0+eth0）仍需显式 `cert sign-client --san-ip` 或多声明段；与 P3 验收无冲突
- `kernel/vm/share/powerfs.env` **✅ 恢复 POSIX sh 兼容**（修复 P3 改造引入的两个 bashism）：
  - `BASH_SOURCE[0]` 数组引用 → `${BASH_SOURCE:-$0}`（busybox ash/dash 下回退 `$0`）
  - 进程替换 `done < <(awk …)` → here-doc 命令替换（`sh -n`/`dash -n` 通过）
  - 逐级上溯查找仓库根（`share → vm → kernel → repo/.powerfs`），不再依赖固定相对层级
  - 新增 `POWERFS_MASTER_PRIMARY`（HA 逗号列表首项）供 admin API / ping 等单主机语义使用；修正 `qemuctl.sh` 证书脚本 `--master-api`、`qemuctl2.sh` ib0 ping 的取址
  - `setup_network.sh` 尾部回显、`run_qemu_kernel_ssh.sh` 的 VM_IP/GATEWAY 改为 env 派生（删除 172.20/172.30 字面量）
- VM 运行时回归 **✅ 已完成**（复用标准脚本，无临时新建脚本）：
  - 流程：`qemuctl2.sh stop` → `setup_network_dual.sh` → `run_qemu_kernel_ssh.sh` / `run_qemu_vm2.sh`，SSH 各 4s 就绪
  - 单源生效证据：VM `/proc/cmdline` 为 `powerfs_master_addr=172.30.0.11,172.30.0.12,172.30.0.13`、`powerfs_volume_addr=172.30.0.21…26`、`vm_ip=172.30.0.100/101`，全部来自 cluster.toml/render，脚本内无字面量
  - 网络：tap0/tap1 → `powerfs-br0`，VM ping master 0.163ms
  - 挂载：双 VM `transport=tcp` + `master_addr` 列表 + `shard_count=3`，`MOUNT_RC=0`
  - 跨 VM：VM1 写 4MiB → VM2 md5 一致；VM2 删除 → VM1 不可见
  - 稳定性：30 轮 4MiB 写/读/删，63s，FAIL=0；`check_dmesg_anomaly.sh` VM1/VM2 均通过（860/610 行，无 BUG/Oops/KASAN）
  - 观察项（非 P3 缺陷）：VM2 对未创建过的文件收到删除通知时打印 `remove_dir_entry NOT_FOUND`，属日志噪音，另记 issue 跟踪

### P3 补漏：HA 全链路换网段实测（已完成，2026-10-26）

P3 回归只验证了默认 172.30 网段，未覆盖"改 subnet 后 kernel VM 链路"。本次将 HA 集群 `subnet` 改为 172.31.0.0/16 做端到端实测，发现并修复 4 处遗漏的写死：

| 位置 | 问题 | 修复 |
|---|---|---|
| `kernel/vm/share/powerfs.env` | `POWERFS_GATEWAY`/`VM1_ETH0_IP`/`VM2_ETH0_IP` 是 `:-172.30.x` 字面量默认值，cluster.toml 未显式写 `gateway` 时不随 SUBNET 派生 | 三者统一从 `POWERFS_SUBNET` 前两段派生（`<a.b>.0.1` / `.100` / `.101`），与 render auto-derive 语义一致；用户显式 export 仍优先 |
| `kernel/vm/run_qemu_vm2.sh` | cmdline 写死 `vm_ip=172.30.0.101` | 改 `vm_ip=${VM2_ETH0_IP}` |
| `kernel/vm/run_qemu_kernel_ssh.sh` | `VM_IP` 只用于 echo，从未传入 cmdline，VM1 实际靠 init 脚本内写死 fallback 拿 IP | cmdline 补 `vm_ip=${VM_IP}` |
| `kernel/vm/build_initramfs.sh`（init） | VM 内 `ip route add default via 172.30.0.1` 字面量 | 从 `VM_IP` 派生 `${VM_IP%.*.*}.0.1` |

另修 `docker/scripts/stop-cluster.sh` bug：`REMOVE_VOLUMES=1` 传递的 `--remove-volumes` 不是合法 flag，改为 `-v`。

换网段实测结论（172.31.0.0/16）：

- docker 侧 rendered compose/config 零 172.30 残留，bootstrap、FUSE 写读、跨客户端一致 ✅
- kernel VM smoke 13/13 ✅（证书经 `[client.kernel-client-N]` 声明自动重签，SAN=172.31.0.100/101）
- **改子网必须先清数据卷**：旧 raft log 的 membership 记录旧 IP，带卷重建会出现 zombie-leader 脑裂（master-1 term=1 自认 leader，其余 term 持续增长互投；TCP 连通但 raft RPC `Elapsed` 超时）。正确顺序：`powerfs-ctl down --purge`（或 `compose down -v`）→ 改 `cluster.toml` → `bootstrap`
- **bridge 重建后 QEMU 必须重启**：`compose down` 删 `powerfs-br0` 会使 tap0/tap1 孤立、QEMU 持有死 fd（表现为 VM 内 EHOSTUNREACH/EINPROGRESS）。正确顺序：compose up → `setup_network_dual.sh` → 重启 VM
- `client enroll --ip` 对已声明但无 `ip` 的 `[client.*]` 段不补写 ip（只 skip append），bootstrap 会反复跳过该客户端——待修，见 improvement-plan

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
- **✅ CI IP 字面量 lint 已接入**（`scripts/check-ip-literals.sh` + `scripts/ip-literal-baseline.txt`）：
  - 扫描父仓库 git 索引中的文本文件（`docs/`、`*.md` 允许示例地址；`kernel/` 为 submodule 不在范围）
  - 正则只匹配 RFC1918 私网段（10/8、172.16/12、192.168/16），统计**出现次数**而非行数
  - 基线存量 89 文件 / 560 次；基线外新文件或超基线出现次数 → CI 失败并输出 `::error file=…` 注解
  - 出现次数减少或文件删除不失败，仅提示可 `--update` 刷新基线（防止基线长期失真）
  - 已做负向测试：新增文件含 IP → 失败；`docker/run-fuse-client.sh` 增 1 个 IP（2→3）→ 失败；`docs/` 新增 IP → 通过；无变更 → 89/560 通过
- **✅ 文档清理已完成**（只保留单源路径，删除手工改 compose/config 指引）：
  - `README.md`：新增 `Single-source configuration` 章节（cluster.toml → render 产物树 + 示例）；`docker-compose.test.yml` / `build_powerfs_image.sh` / `config/` 的部署与测试指引全部替换为 `powerfs-ctl bootstrap` + `.powerfs/rendered/*`；逐组件启动命令改为指向渲染配置并注明“不要就地编辑”；端口表、部署 wave 表删除（已由渲染配置承载）；测试指引改为 `scripts/tests/run_all.sh --track fuse`；仓库目录树中标明 `.powerfs/` 与生成的 `config/*.toml`
  - `docs/TESTING_GUIDELINES.md`：删除 deprecated `docker-compose.test.yml`，改为渲染 compose + 四个独立测试拓扑
  - `docs/collection-management-design.md`、`docs/config-consistency-fix-plan.md`：改动文件表中的 `docker/config/*.toml` / `config/fuse-*.toml` 改为 `.powerfs/cluster.toml`（渲染生成）
  - `docs/config-dehardcode-plan.md` 内历史分析段保留旧路径（作为迁移记录，不再作为操作指引）
  - 校验：README 引用的 7 个路径全部存在；`check-ip-literals.sh` 仍通过（md 豁免不影响）

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
