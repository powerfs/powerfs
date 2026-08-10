# PowerFS 告警系统设计

## 概述

告警系统分为两类：

- **指标阈值告警**：基于周期性采样的指标值与阈值比较（CPU、磁盘、KV 命中率）
- **事件驱动告警**：基于状态变化或外部探测（节点离线、Filer 不可达、Raft 无 Leader、均衡迁移失败）

事件驱动告警在 `start_alert_evaluator` 中每 15s 执行一次，直接调用 `trigger_event_alert` 触发，不经过 pending/duration 阶段。

## 告警规则

### 1. 服务状态（Service Status）

| ID | 名称 | 触发条件 | 级别 | 类型 |
|---|---|---|---|---|
| `rule-node-offline` | 节点离线 | 任意节点心跳超 30s 未上报，状态变为 offline | critical | 事件 |
| `rule-no-raft-leader` | Raft 组无 Leader | 某 shard 所有副本 `is_leader=false` | critical | 事件 |

### 2. 均衡迁移（Balancer）

| ID | 名称 | 触发条件 | 级别 | 类型 |
|---|---|---|---|---|
| `rule-shard-imbalance` | 分片不均衡 | leader 分布 max - min > 1 | warning | 事件 |
| `rule-filer-no-leader` | Filer 无 Leader 分片 | 某 filer `leader_count=0` 且其他节点 >1 | warning | 事件 |

### 3. 故障（Failures）

| ID | 名称 | 触发条件 | 级别 | 类型 |
|---|---|---|---|---|
| `rule-filer-unreachable` | Filer 不可达 | Monitor 调 filer admin API 超时/失败 | critical | 事件 |
| `rule-shard-insufficient` | Shard 副本不足 | shard 副本数 < 2 | warning | 事件 |

### 4. 资源指标（Resource Metrics）— 已有

| ID | 名称 | 触发条件 | 级别 | 类型 |
|---|---|---|---|---|
| `rule-1` | 节点 CPU 使用率过高 | CPU > 80%，持续 30s | warning | 指标 |
| `rule-2` | 节点磁盘使用率过高 | 磁盘 > 90%，持续 60s | critical | 指标 |
| `rule-3` | KV 命中率过低 | 命中率 < 50%，持续 60s | warning | 指标 |

## 架构

```
start_alert_evaluator (15s tick)
├── alert_engine.evaluate_rules()          — 指标阈值告警 (pending/duration)
└── evaluate_event_alerts(app_state)       — 事件驱动告警 (直接触发)
    ├── 检查节点离线 (metric_store.get_nodes)
    ├── 检查 Filer 可达性 (filer_admin.get_json /admin/status)
    ├── 检查 Shard 健康 (fetch_cluster_shards)
    └── 检查分片均衡 (leader_distribution)
```

### 事件驱动告警生命周期

1. **触发**：`trigger_event_alert(rule_id, name, severity, source, message)` 直接创建 `firing` 状态告警
2. **恢复**：下一轮检查中条件不再满足时，`resolve_alerts_by_source(rule_id)` 自动标记为 `resolved`
3. **去重**：同一 `rule_id + source` 的告警已存在且 `firing` 时，不重复触发

### 前端展示

Alerts 页面增加 `category` 列（服务状态 / 均衡迁移 / 故障 / 资源指标），支持按分类筛选。
