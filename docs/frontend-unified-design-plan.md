# PowerFS Frontend Unified Design Plan

> Created: 2026-08-09
> Status: DRAFT — awaiting review
> Owner: PowerFS Team
> Supersedes: `frontend-improvement-plan.md` (Phase A/B done, this plan covers Phase C/D + i18n + node redesign)
> Architecture baseline: **Filer Raft strong consistency** (CRDT deprecated, see `strong-consistency-refactor-plan.md` 2026-08-02)

---

## 0. Architecture Baseline (Frontend must follow this)

PowerFS adopted **Filer Raft strong consistency**. CRDT code has been removed (-3244 lines, commit `d04c5fe1`). The frontend must reflect this — "conflict" UI is a health indicator (norm = 0), not a daily operation.

| Layer | Role | Consistency | Monitoring focus |
|---|---|---|---|
| Master | Raft scheduling (Volume routing / topology / resource allocation) | Raft | leader/term, Volume allocation, node heartbeat |
| Filer | Raft metadata (mkdir/create/unlink/rename/lookup/readdir/setattr) | Raft strong | Raft commit latency, shard distribution, leader_count, inode cache hit rate |
| Volume | Data (Needle + Range Lease Lock) | Range Lease | IOPS/throughput/p99, lease occupancy, bitrot |

Cross-client cache coherence: **Callback Invalidation** (Filer pushes targeted invalidate to subscribed clients, not broadcast).

---

## 1. Goals

This plan integrates three user requirements + gaps found via runtime verification:

1. **i18n: default English, with Chinese toggle** — currently zero i18n, all Chinese hardcoded.
2. **Node management redesign** per latest backend interfaces — current NodeInfo type drifts from backend.
3. **Unified design from a storage expert's perspective** — benchmark against Ceph Dashboard / BeeGFS admintools.

Plus runtime-verified gaps (Section 2) to fix the "UI exists, backend missing" issues.

---

## 2. Runtime-Verified Gaps (curl on live cluster)

### Gap 1 — Frontend calls, backend missing entirely (404) or method not registered (405): dead calls

| Frontend fn | Path | Runtime | Backend status |
|---|---|---|---|
| `getDevices` / `getDevice` | `GET /storage/devices` | **404** | Route absent |
| `excludeDevice`/`restoreDevice`/`drainDevice` | `POST /storage/devices/:id/*` | **404** | Absent |
| `getMigrationTasks`/`cancel`/`pause`/`resume` | `/storage/migrations/*` | **404** | Absent |
| `deleteNode` | `DELETE /metrics/nodes/:id` | **405** | Only GET registered |
| `deleteVolume` | `DELETE /metrics/volumes/:id` | **405** | Only GET registered |
| `deleteKVSession` | `DELETE /metrics/kv/sessions/:id` | **405** | Only GET registered |

**Impact**: StorageDevices page + device migration is an empty shell; Nodes/Volumes/KV "delete" buttons error on click. Currently masked by `useMock`.

### Gap 2 — Backend implemented, frontend not wired: idle capability

| Backend route | Runtime | Frontend status |
|---|---|---|
| `GET /api/master/status` | **200** | Not called — Master Raft status/leader invisible |
| `POST /api/master/transfer-leader` | 422(needs param) | Not called — leader transfer has no UI |
| `GET /api/metrics/volumes/:id/io` | **200** | Not called — C2 IO performance page not done |
| `GET /api/fuse/clients` (list) | **404** | B3 required but unimplemented; frontend only uses `/fuse/clients/:id/stats` |
| `GET /ws/metrics` | 400(needs WS handshake) | **Frontend has no WebSocket client** — real-time push unused, relies on axios polling |

### Gap 3 — Config dynamization half-done (A5/B3 marked Done but incomplete)

[Optimizations/index.tsx](file:///home/portion/powerfs/powerfs-monitor-frontend/src/pages/Optimizations/index.tsx): `circuitBreakerConfig`/`coalescerConfig` are **hardcoded constants** (L27-53), file comment self-admits "Phase B will support dynamic via backend API".

| Endpoint | GET | PUT (modify) |
|---|---|---|
| `/api/config/circuit-breaker` | **200** ✅ | **405** ❌ unimplemented |
| `/api/config/coalescer` | 200 ✅ | **405** ❌ unimplemented |

Backend has read-only snapshot; frontend not even calling it (still hardcoded); modify not implemented. "Runtime config observable+editable" is ~30% done.

### Type hygiene issues

- [types/index.ts:90](file:///home/portion/powerfs/powerfs-monitor-frontend/src/types/index.ts) `VolumeInfo.status` has both `'readonly'` and `'read_only'` (redundant, error-prone).
- `FuseMount` retains ghost `client_type` field (L340) — plan A2 said remove, still there.
- `Optimizations` route still in App.tsx (L155) but no Layout menu entry → **orphan page**.

---

## 3. i18n: Default English + Chinese Toggle

### Current state
- **Zero i18n framework**. [Layout/index.tsx](file:///home/portion/powerfs/powerfs-monitor-frontend/src/components/Layout/index.tsx) all Chinese hardcoded ("总览"/"仪表盘"/"告警中心"...).
- No i18n dependency in package.json.

### Solution
Adopt `react-i18next` + `i18next` (most mature ecosystem, AntD ConfigProvider native integration).

**Implementation points**:
1. **Default English**: `i18n.use(initReactI18next).init({ lng: 'en', fallbackLng: 'en' })`. Persist user choice in localStorage.
2. **Toggle button**: Header right side `🇺🇸 EN / 🇨🇳 中文` (Dropdown or Segmented), `i18n.changeLanguage(lng)` + persist on click.
3. **AntD sync**: `<ConfigProvider locale={lng==='zh'?zhCN:enUS}>` — calendar/pagination builtin copy syncs.
4. **Resource files**: `src/locales/{en,zh}/common.json` + `src/locales/{en,zh}/nav.json` (navigation) + `src/locales/{en,zh}/{page}.json` (per-page, lazy-loaded).
5. **Migration strategy**: Extract nav/Layout/Dashboard first (high-frequency visible), page-level copy migrated in batches. Replace hardcoded with `t('nav.dashboard')`.
6. **Key naming**: `namespace.scope.key`, e.g. `nav.clusterTopology`, `nodes.status.online`, `common.confirm`.

### Terminology table (English canonical, avoid ambiguity)

| 中文 | English | Note |
|---|---|---|
| 仪表盘 | Dashboard | |
| 集群拓扑 | Cluster Topology | |
| 容量规划 | Capacity Planning | |
| Volume 管理 | Volumes | Don't translate "Volume" (proper noun) |
| Filer 管理 | Filer Status | Don't translate "Filer" |
| 分片管理 | Shards | |
| 分片均衡 | Shard Balancing | |
| 冲突检测 | Conflict Detection | Downgrade to health sub-page |
| 性能测试 | Benchmark | |
| 存储设备 | Storage Devices | **P0 TBD: backend no endpoint** |
| Master Raft 健康 | Master Raft Health | New page |
| 运行时配置 | Runtime Config | Merge Optimizations here |

---

## 4. Node Management Redesign (per latest interfaces)

### 4.1 Actual backend interfaces (verified by reading [main.rs:463-694](file:///home/portion/powerfs/powerfs-monitor/src/main.rs))

| Endpoint | Data source | Exposed fields |
|---|---|---|
| `GET /api/metrics/nodes` | metric_store (heartbeat) | node_type/address/grpc_port/http_port/status/cpu/mem/disk/network_rx/network_tx/uptime/volume_count/is_leader/raft_term |
| `GET /api/topology` | Aggregate: masters(filter) + filers(**gRPC ListFilers**) + volume_servers(join volumes) | masters: NodeInfo[] / filers: **FilerNodeInfo** (leader_count/total_shards/is_healthy) / volume_servers: {node, volumes[]} |
| `GET /api/master/status` | Filter master nodes | nodes[]/leader/raft_term/total_masters/healthy_masters |
| `POST /api/master/transfer-leader` | admin only, gRPC | target_node_id |

### 4.2 Key gaps (frontend type vs backend reality)

| Frontend types/index.ts | Backend actual | Action |
|---|---|---|
| Has `device_count?` | **No such field** | Remove (backend has no device mgmt) |
| Has `raft_role?` | **No such field** (only is_leader + raft_term) | Remove, use is_leader + raft_term |
| Has `node_type: 'master'\|'volume'\|'filer'` | String, but filer data sparse in nodes | Use `/api/topology` as authoritative source |
| No `raft_term` | **Backend has it** | Frontend add raft_term field |

### 4.3 Redesign: nodes are not "one table", they are a **role-layered topology**

Three node types have completely different monitoring dimensions; mixing in one NodeInfo table is wrong. Use separate Tabs/cards per role.

#### Page 1: Cluster Topology (upgrade to node mgmt main entry)

Use reactflow to draw a 3-layer hierarchy:
```
Master Raft (3 nodes, leader highlighted)
   ├─ Filer Raft (3 nodes, each with shard leader_count)
   └─ Volume Servers (N nodes, each with volume count)
```
- Click node → right drawer shows node detail
- **Data source**: `GET /api/topology` (aggregated, one pull)
- Upgrade of current [ClusterTopology](file:///home/portion/powerfs/powerfs-monitor-frontend/src/pages/ClusterTopology)

#### Page 2: Nodes (refactor to role-based Tabs)

Three Tabs, each shows that role's nodes with role-specific fields:

**Tab 1 — Master Nodes**
- Data source: `GET /api/master/status` (has leader/raft_term/healthy stats)
- Fields: node_id / address / **is_leader** / **raft_term** / status / cpu/mem / uptime
- Action: **Transfer Leader button** (admin only, calls `/api/master/transfer-leader`, double-confirm)
- Top stat cards: total_masters / healthy_masters / current leader / raft_term

**Tab 2 — Filer Nodes**
- Data source: `GET /api/topology` filers (has leader_count/total_shards/is_healthy)
- Fields: node_id / address / grpc_port / **is_healthy** / **leader_count** / **total_shards**
- Top stats: filer total / healthy / total shards / leader distribution balance
- Sub-entry: click filer → jump to Shards page (filtered to that filer)

**Tab 3 — Volume Nodes**
- Data source: `GET /api/topology` volume_servers
- Fields: node_id / address / status / cpu/mem/disk / **volume_count** / total used/size
- Sub-entry: click → expand that node's volumes list

**Remove**: frontend `deleteNode`/`deleteVolume`/`deleteKVSession` dead calls (backend has no DELETE; semantically nodes shouldn't be frontend-deletable).

#### Page 3: Master Raft Health (new, P1)

Currently **completely missing** Raft cluster ops view. `get_master_status` returns 200 but frontend never calls it.

- Cluster health overview: total/healthy/leader/term
- Node list + leader highlight
- **Transfer Leader operation** (admin only, double-confirm + impact warning)
- (Optional) Raft commit latency trend chart (needs backend metric)

---

## 5. Unified Design — Storage Expert Perspective

### Design principles (benchmark vs Ceph Dashboard / BeeGFS admintools)

1. **Topology first**: the #1 ops question for distributed storage is "what does the cluster look like, who's down". Cluster Topology should be the **2nd highest-frequency entry** after Dashboard; node mgmt converges here, not a flat table.
2. **Role-separated**: Master/Filer/Volume have different duties; monitoring metrics must not be mixed. Filer → Raft metadata perf, Volume → IO throughput, Master → scheduling health.
3. **Strong-consistency context**: architecture is Raft strong-consistent; all "conflict/consistency" UI must convey "this is a health metric, norm = 0", not "high-frequency ops". Conflicts downgrade, Raft health upgrades.
4. **Data authenticity**: `useMock` default false is done, but Optimizations page still hardcodes constants — from a storage expert view this is "fake data", must wire to real API or take offline.
5. **Capacity & performance are storage core KPIs**: Capacity Planning + Volume IO Performance deserve dedicated slots, and must be based on real time-series (currently `get_metric_history` may be mock — verify).

### Information architecture (unified navigation)

Based on [Layout/index.tsx](file:///home/portion/powerfs/powerfs-monitor-frontend/src/components/Layout/index.tsx) current (already grouped, minor adjustment):

```
Overview
  - Dashboard
  - Cluster Topology ← node mgmt main entry
  - Master Raft Health ← new (P1)
Storage
  - Volumes (with IO Performance tab) ← wire /metrics/volumes/:id/io
  - Collections
  - Bitrot Scrub
  - Capacity Planning ← verify data authenticity
Metadata
  - Filer Status
  - Shards
  - Shard Balancing
  - Conflict Detection ← downgrade to health sub-page (norm 0)
Client & Performance
  - FUSE Clients (B4 done, wire /fuse/clients list endpoint)
  - S3
  - KV
  - Benchmark
Operations
  - Alerts
  - Runtime Config ← wire real /api/config/* (P1)
Security
  - AccessKeys / Users / Roles
```

**Remove/adjust**:
- Storage Devices entry → **interim hide** (backend has no `/storage/devices`); decision 1 confirms backend supplement is in-scope — restore entry once Master device mgmt gRPC + routes land.
- Optimizations → merge into Runtime Config; decision 2 confirms it becomes an **editable form** (wire real CB/Coalescer GET + add PUT hot-modify, remove hardcoded constants).

---

## 6. Priority & Sequencing

Integrates the three user requirements + Section 2 gaps.

| Priority | Task | Status | Rationale |
|---|---|---|---|
| **P0** | i18n framework + default English + EN/ZH resources | ✅ done (commit 62a1938c) | User explicit requirement; foundational change, cheaper the earlier |
| **P0** | Node type cleanup: remove device_count/raft_role, add raft_term | ✅ done (commit 62a1938c) | Align types with backend; basis for all node pages |
| **P0** | Hide Storage Devices entry + remove delete* dead calls (interim) | ✅ done (commit 62a1938c) | Avoid click-errors until backend DELETE/device routes land (decisions 1,3) |
| **P1** | Cluster Topology upgrade (reactflow 3-layer graph + node detail drawer) | ✅ done (commit 62a1938c) | Storage expert core entry |
| **P1** | Nodes page → 3 role Tabs (Master/Filer/Volume) | ✅ done (commit 62a1938c) | Node mgmt redesign core |
| **P1** | Master Raft Health page + Transfer Leader (decision 5) | ✅ done (commit 62a1938c) | Wire already-200 `/api/master/status`; high-risk op with double-confirm |
| **P1** | Optimizations/Runtime Config → editable form via PUT (decision 2) | ✅ done (commit 62a1938c) | Wire real GET + add PUT hot-modify; remove hardcoded constants |
| **P1** | WebSocket `/ws/metrics` integration (decision 4, promoted from P2) | ✅ done (commit a9387206) | Real-time push, in-scope this round; backend snapshot-on-connect added |
| **P1** | StorageDevices backend supplement (decision 1) | ✅ done | Master device mgmt gRPC + `/storage/devices` + `/storage/migrations` routes; frontend entry restored |
| **P1** | Volume IO Performance tab (wire `/metrics/volumes/:id/io`) | ✅ done (commit 4470d2c9) | C2, backend already 200 |
| **P1** | Header WS status badge | ✅ done (commit 38d61691) | Bonus: visibility into real-time stream health |
| **P2** | FUSE Clients list wire `/fuse/clients` (currently 404, backend must add) | ✅ done | `/api/fuse/clients` route + frontend wired with real data |
| **P2** | Conflicts fully downgrade to Filer sub-page health indicator | ✅ done | CRDT deprecated; 顶级导航入口移除, /conflicts 路由保留通过 Filer Tab 链接访问 |
| **P2** | Backend DELETE routes for nodes/volumes/kv-sessions (decision 3) | ✅ done (commit 62a1938c) | Unblocks frontend delete buttons |
| **P3** | Capacity Planning data authenticity verify (possibly mock) | ✅ done | Backend `/api/metrics/history/:metric` + frontend ECharts real time-series |

---

## 7. Decisions (confirmed 2026-08-09)

1. **StorageDevices / device migration** → **Supplement backend**. Implement Master device mgmt gRPC + monitor `/storage/devices` + `/storage/migrations` routes. Frontend entry restored once backend lands. This is now a tracked backend+frontend work item (not hidden indefinitely).
2. **Config dynamic modify** → **Allow hot-modify**. Backend adds PUT to `/api/config/circuit-breaker` and `/api/config/coalescer`; frontend Optimizations/Runtime Config page becomes an editable form. Hot-modify is permitted at runtime (no reboot required).
3. **Delete operation semantics** → **Missed (was missing)**. Add backend DELETE routes for `deleteNode`/`deleteVolume`/`deleteKVSession` (currently 405 — only GET registered). Frontend dead calls become real.
4. **Real-time push** → **Do this round**. WebSocket `/ws/metrics` integration is in-scope for this plan (promoted from P2 to P1), not deferred.
5. **Master Raft Health page** → **Build it**. `transfer-leader` is high-risk; frontend implements it with double-confirm + RBAC (admin only, already enforced backend-side).

### Updated execution order (post-decision)

```
P0: i18n + node type cleanup + hide dead calls (interim, until backend DELETE/device routes land)
P1: Cluster Topology upgrade → Nodes 3 Tabs → Master Raft Health (with Transfer Leader)
    + Optimizations/Runtime Config editable form (PUT hot-modify) + StorageDevices backend supplement
    + Volume IO Performance + WebSocket /ws/metrics
P2: FUSE Clients list (needs backend /fuse/clients) + Conflicts downgrade
P3: Capacity Planning data authenticity
```

Backend work items (block some frontend P1/P2):
- Add PUT `/api/config/circuit-breaker` + `/api/config/coalescer` (decision 2)
- Add DELETE `/metrics/nodes/:id`, `/metrics/volumes/:id`, `/metrics/kv/sessions/:id` (decision 3)
- Implement Master device mgmt gRPC + `/storage/devices` + `/storage/migrations` routes (decision 1)
- Add `/api/fuse/clients` list endpoint (currently 404, B3 residual)

---

## 8. Validation Plan

1. **i18n**: switch EN↔ZH, verify all visible copy switches + AntD locale (calendar/pagination) syncs + persists across reload.
2. **Node redesign**: verify Cluster Topology graph renders 3 layers; Nodes 3 Tabs show role-specific fields; Transfer Leader works (on test cluster) with double-confirm.
3. **Runtime config**: verify Optimizations/Runtime Config shows real CB/Coalescer values from `GET /api/config/*`, not hardcoded.
4. **Dead call removal**: click Storage Devices (hidden), Nodes/Volumes/KV delete buttons (removed) — no 404/405 errors.
5. **Container test env**: `docker compose up` master+filer+volume+monitor, run fio/IO500 to generate load, verify frontend panels update.

---

## 9. References

- [frontend-improvement-plan.md](file:///home/portion/powerfs/docs/frontend-improvement-plan.md) — Phase A/B done (this plan supersedes for C/D + new requirements)
- [strong-consistency-refactor-plan.md](file:///home/portion/powerfs/docs/strong-consistency-refactor-plan.md) — architecture baseline (Filer Raft, CRDT removed)
- [filer-architecture-design.md](file:///home/portion/powerfs/docs/filer-architecture-design.md) — Filer role
- [Layout/index.tsx](file:///home/portion/powerfs/powerfs-monitor-frontend/src/components/Layout/index.tsx) — current nav
- [types/index.ts](file:///home/portion/powerfs/powerfs-monitor-frontend/src/types/index.ts) — type drift source
- [api.ts](file:///home/portion/powerfs/powerfs-monitor-frontend/src/services/api.ts) — dead calls + idle backend
- [main.rs:463-694](file:///home/portion/powerfs/powerfs-monitor/src/main.rs) — topology/master_status/transfer_leader handlers
- [Optimizations/index.tsx](file:///home/portion/powerfs/powerfs-monitor-frontend/src/pages/Optimizations/index.tsx) — hardcoded config constants
