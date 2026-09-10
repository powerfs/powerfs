# PowerFS Agent API 契约（Step 0 冻结版 v0.1）

> 对齐 `powerfs-agent.md` §5/§6 Step 0。接口先行冻结：Mock 后端与前端并行开发，真实后端后续替换 Mock 实现，契约不变。
> 状态：**契约定义**，未实现。变更需在本文档记录版本。

---

## 1. 通用约定

### 1.1 部署与路径

- 所有 Agent API 挂在 `/api/agent/*` 命名空间
- 真实部署：Monitor 反向代理到 powerfs-agent（与管理面"一切经 Monitor"原则一致）；Mock 阶段：agent 服务直接暴露同路径（或由 nginx 转发）
- 鉴权：JWT Bearer Token（复用 Monitor 登录体系签发）；经 Monitor 代理时由 Monitor 校验后转发

### 1.2 响应包装（对齐 Monitor `ApiResponse<T>`）

```json
{ "code": 200, "message": "success", "data": { } }
{ "code": 500, "message": "error detail", "data": null }
```

| code | 语义 |
|---|---|
| 200 | 成功 |
| 400 | 参数错误 |
| 401 | 未认证 |
| 403 | 无权限（需要 admin） |
| 404 | 资源不存在 |
| 409 | 状态冲突（如对已批准的建议重复 approve） |
| 500 | 服务内部错误 |

### 1.3 其他约定

- 时间格式：RFC3339 UTC 字符串（如 `2026-09-01T08:00:00Z`）
- 分页：请求 `?page=1&page_size=20`，响应 data 内含 `items` + `total` + `page` + `page_size`
- 枚举值全部小写下划线风格
- 写操作（approve/reject/execute/cancel/config 修改）要求 admin 权限；查询对所有登录用户开放

---

## 2. 数据模型

### 2.1 Suggestion（建议卡片）

```json
{
  "id": "sgt-20260901-000001",
  "type": "tiering_migration",
  "title": "冷数据归档：/datasets/imagenet/raw",
  "description": "该目录 30 天无访问，共 2.3TB，建议从副本(3)降级为 EC(4+2) 并迁移至 cold 层",
  "severity": "info",
  "autonomy_level": 2,
  "status": "pending",
  "source": "rule_engine",
  "scope": {
    "volumes": ["vol-1"],
    "nodes": [],
    "paths": ["/datasets/imagenet/raw"],
    "estimated_bytes": 2469606195200,
    "estimated_file_count": 1286752
  },
  "evidence": [
    {
      "kind": "io_profile",
      "ref": "heatstat:/datasets/imagenet/raw",
      "summary": "30 天访问计数为 0，最近访问 2026-07-30",
      "snapshot": { "window_days": 30, "read_cnt": 0, "write_cnt": 0, "last_access": "..." }
    }
  ],
  "plan": {
    "dry_run": true,
    "estimated_duration_s": 3600,
    "impact": { "bandwidth_mbps": 200, "concurrent_tasks": 1, "p99_impact": "none" },
    "rollback": "反向迁移：EC(4+2) → 副本(3)，步骤与正向对称",
    "steps": [
      { "seq": 1, "action": "ec_convert", "target": { "path": "/datasets/imagenet/raw" }, "params": { "from": "replica3", "to": "ec42" } },
      { "seq": 2, "action": "tier_move", "target": { "path": "/datasets/imagenet/raw" }, "params": { "to_tier": "cold" } }
    ]
  },
  "created_at": "2026-09-01T08:00:00Z",
  "expires_at": "2026-09-08T08:00:00Z",
  "decision": null
}
```

**字段说明**

| 字段 | 说明 |
|---|---|
| `type` | `tiering_migration`（分层迁移）· `ec_adjust`（EC/副本调整）· `capacity_expansion`（扩容）· `root_cause`（根因处置）· `scrub_trigger`（校验触发）· `node_maintenance`（节点维护）· `param_tuning`（参数调优）· `alert_denoise`（告警去噪，通常自动）· `transfer_leader`（leader 切换） |
| `severity` | `info` · `warning` · `critical` |
| `autonomy_level` | 生成时建议的级别 L0-L3（实际执行资格以 config.autonomy 为准） |
| `status` | `pending` → `approved` → `executing` → `completed` / `failed`；或 `pending` → `rejected`；`executing` 可 → `cancelled`；超时 → `expired` |
| `source` | `rule_engine` · `llm` · `manual` |
| `evidence[]` | 证据链；`kind`: `metric` · `alert` · `topology` · `io_profile` · `scrub_result` · `capacity_projection` |
| `plan` | L2/L3 动作必填（含 dry-run 结果）；L1 建议可省略 |
| `decision` | 已决策时：`{ "by": "admin", "at": "...", "comment": "..." }` |

### 2.2 Decision（审计记录）

```json
{
  "id": "dec-20260901-000001",
  "suggestion_id": "sgt-20260901-000001",
  "trigger": { "kind": "schedule", "ref": "inspection:2026-09-01" },
  "input_snapshot": { "alerts": 12, "cluster_overview": { }, "topology_version": 42 },
  "reasoning": {
    "engine": "llm",
    "summary": "12 条告警中 9 条源于 node-3 磁盘劣化（SMART 197 增长），聚合为单一根因",
    "evidence_refs": ["alert:881", "alert:882", "smart:node-3"]
  },
  "action": { "type": "scrub_trigger", "params": { "volume_id": "vol-1" }, "autonomy_level": 2, "mode": "dry_run" },
  "verification": { "status": "pending", "checked_metrics": ["scrub.errors_found"], "result": null },
  "created_at": "2026-09-01T08:00:00Z",
  "duration_ms": 1450
}
```

| 字段 | 说明 |
|---|---|
| `trigger.kind` | `alert` · `schedule` · `event` · `manual` |
| `reasoning.engine` | `rule` · `llm` |
| `action.mode` | `dry_run` · `execute` |
| `verification.status` | `pending` · `passed` · `failed` · `skipped` |

### 2.3 AgentConfig

```json
{
  "enabled": true,
  "mock_mode": true,
  "llm": {
    "backend": "local",
    "local": { "endpoint": "http://127.0.0.1:8000/v1", "model": "qwen2.5-32b" },
    "cloud": { "endpoint": "", "api_key_masked": "sk-***" },
    "timeout_ms": 30000
  },
  "autonomy": {
    "default_level": 0,
    "overrides": {
      "alert_denoise": 3,
      "param_tuning": 3,
      "tiering_migration": 3,
      "scrub_trigger": 2,
      "ec_adjust": 1,
      "node_maintenance": 1,
      "transfer_leader": 1
    }
  },
  "guardrails": {
    "max_concurrent_migrations": 2,
    "max_migration_bandwidth_mbps": 200,
    "maintenance_window": { "enabled": true, "daily": "02:00-06:00" },
    "protected_patterns": ["/system/*"],
    "max_actions_per_hour": { "tiering_migration": 10, "scrub_trigger": 4 }
  },
  "updated_at": "2026-09-01T08:00:00Z",
  "updated_by": "admin"
}
```

- `enabled=false`：全局一键降级 L0（紧急止血），只观察不执行
- `autonomy_level` 生效规则：`min(suggestion.autonomy_level, config.autonomy.overrides[type] ?? default_level)`；`enabled=false` 时一律按 L0
- `mock_mode=true`：数据源为 Mock 生成器（Step 0）；切换 false 需重启或热重载（实现细节后定）

---

## 3. 接口清单

### 3.1 面板汇总

```
GET /api/agent/overview
```

响应 data：

```json
{
  "enabled": true,
  "mock_mode": true,
  "llm_backend": "local",
  "pending_suggestions": 3,
  "critical_suggestions": 1,
  "executing_actions": 1,
  "decisions_today": 27,
  "autonomy_summary": { "L0": 0, "L1": 2, "L2": 1, "L3": 4 },
  "last_inspection": { "id": "rep-...", "finished_at": "...", "issues_found": 5 },
  "health": "normal"
}
```

### 3.2 建议卡片

```
GET    /api/agent/suggestions?status=pending&type=tiering_migration&page=1&page_size=20
GET    /api/agent/suggestions/:id
POST   /api/agent/suggestions/:id/approve    { "comment": "..." }
POST   /api/agent/suggestions/:id/reject     { "comment": "..." }
POST   /api/agent/suggestions/:id/execute    { "mode": "execute" }        // mode 可选 dry_run|execute，默认 execute；仅 approved 状态可调
POST   /api/agent/suggestions/:id/cancel     { "comment": "..." }          // 仅 executing 状态可调
GET    /api/agent/suggestions/:id/progress
```

`progress` 响应 data：

```json
{
  "suggestion_id": "sgt-...",
  "status": "executing",
  "current_step": 2,
  "total_steps": 5,
  "progress_percent": 38,
  "started_at": "...",
  "eta_s": 1800,
  "throughput_mbps": 185,
  "errors": [],
  "step_history": [
    { "seq": 1, "action": "ec_convert", "status": "completed", "started_at": "...", "finished_at": "..." }
  ]
}
```

状态机约束：

| 操作 | 前置状态 | 结果状态 |
|---|---|---|
| approve | pending | approved（L1：同时视为"已采纳"，生成 decision；L2：进入待执行） |
| reject | pending | rejected |
| execute | approved | executing |
| cancel | executing | cancelled |
| （自动） | executing | completed / failed |
| （超时） | pending | expired |

### 3.3 决策审计

```
GET /api/agent/decisions?suggestion_id=&engine=&page=1&page_size=20
GET /api/agent/decisions/:id
```

### 3.4 配置

```
GET /api/agent/config
PUT /api/agent/config        // 请求体为 AgentConfig 全量；admin 权限；返回更新后配置
```

### 3.5 巡检报告

```
GET /api/agent/reports?page=1&page_size=20
GET /api/agent/reports/:id
POST /api/agent/reports/trigger          // 手动触发一次巡检（admin）
```

报告 data 结构（详情）：

```json
{
  "id": "rep-20260901-01",
  "kind": "inspection",
  "status": "completed",
  "started_at": "...", "finished_at": "...",
  "summary": { "issues_found": 5, "critical": 0, "warning": 2, "info": 3 },
  "sections": [
    { "title": "容量水位", "findings": [ { "severity": "warning", "message": "vol-2 使用率 87%，90 天预计耗尽", "suggestion_id": "sgt-..." } ] }
  ]
}
```

### 3.6 只读问答

```
POST /api/agent/chat   { "question": "当前集群容量最多还能撑多久？", "session_id": "..." }
```

响应 data：

```json
{
  "session_id": "chs-...",
  "answer": "按最近 7 天增长曲线，vol-2 预计 62 天后耗尽；建议……",
  "references": [
    { "kind": "suggestion", "id": "sgt-..." },
    { "kind": "metric", "ref": "capacity_projection:vol-2" }
  ]
}
```

约束：只读，不产生任何动作/状态变更；回答必须附 `references`；`enabled=false` 时仍可用（只读能力不降级）。

### 3.7 事件流（SSE）

```
GET /api/agent/events/stream          // text/event-stream，需 JWT（query 参数 token 或 header）
```

事件类型：

| event | data 载荷 | 说明 |
|---|---|---|
| `suggestion.created` | Suggestion 摘要（id/type/title/severity/autonomy_level） | 新建议产生 |
| `suggestion.status_changed` | `{ id, from, to, by }` | 状态流转 |
| `suggestion.progress` | progress 对象（节流：每 5s 或步进变化） | 执行进度 |
| `decision.created` | Decision 摘要 | 决策留痕 |
| `config.changed` | `{ updated_by, enabled }` | 配置变更 |
| `agent.health` | `{ health, message }` | Agent 自身状态（LLM 不可用/降级等） |
| `mock.scenario` | `{ name, action }` | Mock 场景启停通知（仅 mock_mode） |

心跳：每 15s 发送 `: ping` 注释行保活；断线客户端按 Last-Event-ID 补发（服务端保留最近 1000 条）。

### 3.8 Mock 控制接口（仅 `mock_mode=true` 可用）

```
GET  /api/agent/mock/scenarios
POST /api/agent/mock/scenarios     { "name": "alert_storm", "action": "start" }
```

内置场景：

| name | 演示内容 |
|---|---|
| `alert_storm` | 10 分钟内注入 30+ 条关联告警 → 聚合去噪 → 产出 1 条根因建议卡片 |
| `capacity_surge` | 容量快速增长 → 产生扩容建议 + 容量预测图数据 |
| `cold_data_pileup` | 冷数据堆积 → 产生分层迁移建议（含 dry-run 计划）→ 批准后模拟执行进度 |
| `bitrot_found` | scrub 发现静默损坏 → 产生自愈建议（scrub_trigger）|
| `quiet` | 清空一切模拟活动，回到空闲态 |

场景可叠加运行；`action: stop` 停止对应场景（已产生的 suggestion/decision 保留，供审计演示）。

---

## 4. 错误处理补充

| 场景 | code | message 示例 |
|---|---|---|
| 重复 approve | 409 | `suggestion sgt-... already approved` |
| execute 未 approve | 409 | `suggestion sgt-... is pending, execute requires approved` |
| guardrail 拒绝（预算耗尽） | 409 | `guardrail: max_actions_per_hour(tiering_migration) exceeded` |
| enabled=false 时 execute | 403 | `agent disabled: actions downgraded to L0` |
| mock 接口在非 mock 模式 | 404 | `mock control unavailable: mock_mode=false` |

---

## 5. 前端页面 ↔ 接口映射

| 页面/组件 | 使用接口 |
|---|---|
| Agent 面板首页 | overview + events/stream |
| 建议卡片列表（待处理/历史 tab） | suggestions 列表 + suggestion.created/status_changed 事件 |
| 建议详情抽屉（evidence/plan/审批按钮） | suggestions/:id + approve/reject/execute/cancel |
| 执行进度条 | suggestions/:id/progress 轮询 或 suggestion.progress 事件 |
| 决策历史页 | decisions 列表 |
| 配置页（自治级别/护栏/LLM 后端） | config GET/PUT |
| 巡检报告页 | reports 列表/详情 + trigger |
| 问答输入框 | chat |
| Mock 演示控制（演示模式下隐藏或标注） | mock/scenarios |

---

## 6. 版本记录

| 版本 | 日期 | 变更 |
|---|---|---|
| v0.1 | 2026-09-01 | 初稿：数据模型 + 全部接口契约冻结，供 Step 0 前后端并行开发 |
