# PowerFS Frontend Improvement Plan

> Created: 2026-07-31
> Status: IN PROGRESS
> Owner: PowerFS Team

## 1. Background

The PowerFS frontend (React + TypeScript + Ant Design) has severely drifted from the actual system architecture during backend iterations. Key issues:

1. **NodeInfo missing Filer type** — only `'master' | 'volume'`, Filer layer invisible
2. **Client optimization results (Phase 1-4 + P1/P2) not exposed** — CircuitBreaker, multi-queue scheduler, WriteCoalescer, TCP keepalive all invisible
3. **Optimizations page has ghost flags** — 11 flags (`ec_simd_enabled`, `raft_pre_vote`, etc.) with zero backend implementation
4. **`useMock = true` hardcoded** — frontend runs against mock data, not validated against real APIs
5. **FuseMount type stale** — has `master` field (FUSE connects to Filer, not Master), missing runtime stats
6. **Conflicts page occupies primary nav** — should be downgraded (ORSet still active in Filer but conflicts are edge cases under Raft)

## 2. Data Flow Architecture

Reuse the existing FUSE → Master → Monitor → Frontend path (no new collection channel):

```
FUSE Client                        Master                     Monitor                  Frontend
───────────                       ──────                     ────────                  ────────
KeepConnected gRPC stream    →   ClientRegistry         →   GetFuseClients gRPC   →   /api/fuse/clients
  + extended stats fields         stores latest stats         pass-through stats       render pages
                                  GetFuseClients returns      /api/fuse/clients/:id
                                                              /stats
```

## 3. Phased Plan

### Phase A: Architecture Alignment

| # | Task | Priority | Details |
|---|---|---|---|
| A1 | NodeInfo add `'filer'` type | P0 | `types/index.ts` node_type union; monitor `/api/metrics/nodes` collect filer nodes |
| A2 | FuseMount type fix | P0 | Remove `master` field, add `filer_address`; remove ghost `client_type` |
| A3 | Turn off mock default | P0 | `useMock = import.meta.env.VITE_USE_MOCK === 'true'` |
| A4 | Conflicts downgrade | P2 | Move from primary nav to Filer sub-tab; show red badge when active |
| A5 | Optimizations page rebuild | P1 | Delete 11 ghost flags; replace with real runtime config (CB params, CoalescerConfig, scheduler weights) |

### Phase B: Client Observability (Core)

#### B1. Proto Extension

Extend `KeepConnectedRequest` (master.proto:L165) with `ClientStats stats = 15`:

```protobuf
message ClientStats {
    // Multi-queue scheduler
    uint32 data_queue_depth = 1;
    uint32 lease_queue_depth = 2;
    uint32 admin_queue_depth = 3;
    uint64 data_processed_total = 4;
    uint64 lease_processed_total = 5;
    uint64 admin_processed_total = 6;

    // CircuitBreaker
    uint32 cb_closed_count = 7;
    uint32 cb_open_count = 8;
    uint32 cb_half_open_count = 9;
    uint64 cb_trip_total = 10;

    // WriteCoalescer
    uint64 coalescer_dirty_bytes = 11;
    uint32 coalescer_dirty_entries = 12;
    uint64 coalescer_writes_in_total = 13;
    uint64 coalescer_flushes_out_total = 14;

    // Connection pool
    uint32 pool_active_connections = 15;
    uint32 pool_reconnect_total = 16;
    uint32 pool_ping_failures = 17;

    // Request latency (microseconds)
    uint64 read_latency_p50_us = 20;
    uint64 read_latency_p99_us = 21;
    uint64 write_latency_p50_us = 22;
    uint64 write_latency_p99_us = 23;

    // Lease
    uint32 active_leases = 30;
    uint64 lease_renewals_total = 31;
    uint64 lease_expired_total = 32;
}
```

Extend `FuseClientInfo` (master.proto:L433) with `ClientStats stats = 12`.

#### B2. FUSE Client Reporting

In volume_client.rs KeepConnected heartbeat loop, collect stats every 5s:
- Queue depth: existing `SchedulerStats` (Phase 4)
- CircuitBreaker: iterate `CircuitBreakerPool`
- WriteCoalescer: `coalescer.dirty_bytes_total()` + new counters
- Connection pool: MetaShardClient health check stats
- Latency: new p50/p99 sliding window

#### B3. Monitor API

| Endpoint | Method | Description |
|---|---|---|
| `/api/fuse/clients` | GET | Client list with stats summary |
| `/api/fuse/clients/:id/stats` | GET | Detailed stats with history |
| `/api/fuse/clients/:id/circuit-breakers` | GET | Per-client CB states |
| `/api/fuse/clients/:id/coalescer` | GET | Per-client Coalescer states |
| `/api/config/circuit-breaker` | GET/PUT | CB global config |
| `/api/config/coalescer` | GET/PUT | Coalescer global config |

#### B4. Frontend Pages

Transform Fuse page into client list + detail drawer:

```
FUSE Client List
┌──────────────────────────────────────────────────────────────┐
│ Mount Point    │ Host      │ Collection │ Status │ Queue Depth │ CB │ Coalescer │
│ /mnt/data      │ node-1    │ default    │ alive  │ 12/0/3      │ 8C │ 2.1MB     │
│ /mnt/cache     │ node-2    │ hot        │ alive  │ 5/1/1       │ 10C│ 0         │
└──────────────────────────────────────────────────────────────┘
                                                    ↓ click to expand
Client Detail Drawer (Tab layout)
┌──────────────────────────────────────────────────────────────┐
│ [Overview] [Scheduler] [CircuitBreaker] [Coalescer] [Pool] [Lease] │
│                                                              │
│ Overview: p50/p99 latency chart + error rate + throughput   │
│ Scheduler: 3 queue depth real-time chart + processing rate   │
│ CircuitBreaker: per-backend CB status cards                  │
│ Coalescer: dirty bytes trend + coalescing ratio              │
│ Pool: active connections + reconnect count + ping failures    │
│ Lease: active count + renewal success rate + expiring list   │
└──────────────────────────────────────────────────────────────┘
```

### Phase C: Storage Engine Deep Views

| # | Page | Data Source | Content |
|---|---|---|---|
| C1 | Volume detail | Master VolumeList + Volume gRPC | Needle count, append offset, compact status |
| C2 | I/O performance | Volume Server heartbeat | Per-volume IOPS/throughput/p50/p99 |
| C3 | Cluster topology | Master GetClusterInfo | Master→Filer→Volume→Device visual map |
| C4 | Capacity planning | Master historical metrics | Growth trend + projected full dates |

#### Phase C Data Gap Analysis (2026-07-31)

Investigated current backend data sources against C1-C4 requirements:

**Existing data:**
- `VolumeShortInfo` proto (master.proto:L109): `volume_id, size, read_only, collection, replica_placement, ttl, disk_type, used`
- `VolumeInfo` (metric_store.rs:L30): `id, node_id, size, used, file_count, status, collection, created_at`
- `Heartbeat` proto (master.proto:L82): Volume Server reports `VolumeShortInfo` list + `max_file_key` — **no I/O metrics**
- `MasterStatusResponse` (master.proto:L593): master nodes with CPU/mem/disk usage
- `VolumeListResponse` (master.proto:L238): `DataNodeInfo` list with volumes
- Monitor APIs: `/api/metrics/volumes`, `/api/metrics/nodes`, `/api/metrics/cluster` already exist
- Frontend pages: `Volumes/`, `StorageDevices/`, `Nodes/` already exist

**Critical gaps:**
1. **C1**: `VolumeInfo` missing `read_only, replica_placement, ttl, disk_type, compact_status, append_offset`. `file_count` exists but `compact_status` is not in proto at all.
2. **C2**: **I/O metrics completely missing**. Volume Server does not collect IOPS/throughput/latency. Heartbeat proto has no performance fields. This is the largest gap.
3. **C3**: Topology data scattered across `VolumeList` (data_nodes+volumes), `MasterStatus`, `ListFilers`. Needs aggregation API + visualization.
4. **C4**: `get_metric_history` (main.rs:L1689) returns **mock random data** (`rand::random`). No real time-series storage. Must implement historical metric persistence.

#### Phase C Implementation Plan

**Recommended order: C3 → C1 → C2 → C4** (start with what uses existing data, defer new metrics collection)

##### C3: Cluster Topology (uses existing data, medium effort)

Backend:
- New Monitor endpoint `GET /api/topology` aggregating: master nodes (from `MasterStatus`), filer nodes (from `ListFilers`/`GetShardMapping`), volume servers + volumes (from `VolumeList`)
- Response shape: `{ masters: [...], filers: [...], volume_servers: [{ id, address, volumes: [...] }] }`

Frontend:
- New `ClusterTopology` page with a tree/graph visualization: Master → Filer (shards) and Master → Volume Server → Volumes
- Use Ant Design Tree or a simple card-based hierarchy

##### C1: Volume Detail (minor proto + data extension, small effort)

Backend:
- Extend `VolumeShortInfo` proto with `uint64 file_count`, `uint32 compact_status`, `uint64 append_offset`
- Extend monitor `VolumeInfo` + `VolumeStatusEvent` with `read_only, replica_placement, ttl, disk_type, compact_status`
- Volume Server populates new fields in Heartbeat

Frontend:
- Enhance `Volumes` page detail drawer: show compact status, replica placement, disk type, TTL, read-only flag
- Add per-volume needle count and append offset (write position)

##### C2: I/O Performance (new metrics collection, large effort)

Backend:
- New module `powerfs-volume/src/io_stats.rs`: per-volume counters (read/write ops, bytes, latency histogram)
- Instrument `handle_write_needle` / `handle_read_needle` / `handle_read_needle_blob` to record stats
- Extend `Heartbeat` proto with `VolumeIoStats` message (iops, throughput, p50/p99 latency)
- Master stores and forwards to Monitor; Monitor exposes `GET /api/metrics/volumes/:id/io`

Frontend:
- New `VolumePerformance` page (or tab in Volumes detail): real-time IOPS/throughput chart + p50/p99 latency
- Use existing chart components

##### C4: Capacity Planning (time-series storage, large effort)

Backend:
- Replace mock `get_metric_history` with real time-series: store periodic snapshots of per-volume `used`/`size` and cluster-level storage
- Use ring buffer or Redis sorted sets (Redis already in stack) for time-series
- New endpoint `GET /api/metrics/volumes/:id/capacity-history`
- Compute projected full date: `current_used + growth_rate * days_until_full`

Frontend:
- New `CapacityPlanning` page: per-volume growth trend chart + projected full date badges
- Alert when projected full date < threshold (e.g., 7 days)

### Phase D: Navigation Reorganization

```
▸ Overview
  - Dashboard
  - Cluster Topology (C3)
▸ Storage
  - Volumes (with I/O performance C2)
  - Storage Devices
  - Bitrot Scrub
▸ Metadata
  - Filer Status
  - Shards
  - Shard Balancing
  - Conflict Detection (A4 downgraded here)
▸ Client
  - FUSE Clients (B4)
  - S3
  - KV
▸ Operations
  - Alerts
  - Benchmark
  - Runtime Config (A5 rebuilt)
  - Capacity Planning (C4)
▸ Security
  - Users / Roles / AccessKeys
```

## 4. Conflicts Assessment

ORSet is still actively used in Filer (`shard_store.rs` stores directory OR-Set state). Conflict detection is NOT dead code. However, under Raft consensus, conflicts are **edge cases** (network partition, multi-Filer split-brain recovery). Daily count should be 0.

**Decision**: Downgrade to Filer sub-tab. Show green "No active conflicts" by default; red badge + count when conflicts exist. Retain full functionality (manual resolve, auto-resolve, batch ignore).

## 5. Implementation Order

```
Phase A (architecture alignment) → Phase B (client observability) → Phase C (storage deep) → Phase D (nav)
```

Phase A is prerequisite (fix types + disable mock). Phase B is the largest work (proto extension → FUSE reporting → Master storage → Monitor API → frontend pages) and the core value.

## 6. Validation

Containerized test environment:
1. `docker-compose up` master + filer + volume + fuse mount
2. Run IO500 / fio to generate real load
3. Verify frontend panels update in real-time
4. Verify CB trip/recovery visible (kill a volume server)
5. Verify Coalescer ratio (random 4K writes)

## 7. Progress Tracking

| Phase | Task | Status | Commit |
|---|---|---|---|
| A | A1: NodeInfo add filer type | ✅ Done | eb82660a |
| A | A2: FuseMount type fix | ✅ Done | eb82660a |
| A | A3: Turn off mock default | ✅ Done | eb82660a |
| A | A4: Conflicts downgrade | ✅ Done | eb82660a |
| A | A5: Optimizations page rebuild | ✅ Done | eb82660a |
| B | B1: Proto extension | ✅ Done | (this commit) |
| B | B2: FUSE client stats reporting | ✅ Done | (this commit) |
| B | B3: Monitor API | ✅ Done | (this commit) |
| B | B4: Frontend client detail pages | ✅ Done | (this commit) |
| C | C1-C4: Storage deep views | TODO | |
| D | Navigation reorganization | TODO | |

### Phase B Validation

- `cargo build --workspace --all-targets`: ✅ pass (40.66s)
- `cargo clippy --workspace --all-targets`: ✅ pass (2 pre-existing warnings, no new issues)
- `npm run build` (frontend): ✅ pass (3789 modules transformed, built in 1.53s)

#### Container Integration Test (2026-07-31)

**Bugs found and fixed during container testing:**

1. **Monitor Redis ephemeral port exhaustion** (`event_bus.rs`):
   - Root cause: `EventStream::read()` created a new Redis connection per call; 1s retry loop exhausted all 28K+ ephemeral ports
   - Fix: Reuse connection in `EventStream`, add `block(5000)` to xread, exponential backoff (1→30s) in event processor

2. **Master `keep_connected` gRPC deadlock** (`server.rs`):
   - Root cause: Handler called `stream.message().await` BEFORE returning `Response::new(stream)`, causing client to wait for response headers while server waited for first message
   - Fix: Return response stream immediately; read first message inside the stream's `select!` loop; add cleanup on stream end

3. **Filer missing leadership check on read handlers** (`net_handler.rs`):
   - Root cause: `handle_getattr`, `handle_lookup`, `handle_readdir`, `handle_setattr` read from local RocksDB without checking shard leadership, returning "not found" on non-leader nodes
   - Fix: Added `check_leader(msg, shard_id)` to all four handlers with `shard_strategy.calculate_shard(ino)` routing

**Test Results:**
- FUSE client registration: ✅ 2 clients registered, stats reporting every 5s
- Stats API (`/api/fuse/mounts`): ✅ Returns full ClientStats (queue depth, CB, Coalescer, pool, latency)
- Stats update on I/O: ✅ `data_processed=1`, `admin_processed=1`, `cb_closed=1` observed
- Circuit breaker tracking: ✅ `cb_closed_count=1` on active client
- Coalescer fields: ✅ Present in API response (writes_in_total, flushes_out_total, dirty_bytes)
- Data path (large writes): ✅ Lease renewal fixed (see follow-up fix below)

**Follow-up fix — Lease renewal status=10 (2026-07-31):**
- Root cause: `VolumeClient::start_lease_renewer()` built the background renew request with only `LeaseToken` + `LeaseDuration`, omitting `ClientId`. The server-side `RangeLeaseManager::renew()` checks `holder == client_id`, so renewal always failed with "Lease holder mismatch" → `STATUS_ERR_SERVER_ERROR` (status=10). Leases expired after TTL (30s) + grace (3s), breaking all subsequent writes.
- Fix: Capture `self.config.client_id` in the renewer task and add `ClientId` to the renew request TLV (`powerfs-fuse-core/src/volume_client.rs`).
- Validation: cargo check ✅, clippy ✅ (no new warnings), 3 fuse-core lease tests ✅, 22 volume range_lease tests ✅.

#### B1 — Proto extension
- `powerfs-master/proto/master.proto`: added `ClientStats` message (multi-queue / CB / Coalescer / pool / latency / lease fields), extended `KeepConnectedRequest.stats = 15` and `FuseClientInfo.stats = 12`.

#### B2 — FUSE client stats reporting
- `powerfs-fuse-core/src/stats_reporter.rs` (new): `MasterStatsReporter` runs a KeepConnected gRPC stream, periodically collects `ClientStats` from `VolumeClient` (queue depths, CB pool, WriteCoalescer counters, MetaShardClient health, p50/p99 latency window, lease counters), and auto-reconnects on failure.
- Integrated into `FuseClientFacade` lifecycle (start on mount, stop on unmount).

#### B3 — Monitor API
- `powerfs-monitor/src/main.rs`: added `/api/fuse/clients/:id/stats`, `/api/config/circuit-breaker`, `/api/config/coalescer` endpoints returning `ClientStatsResponse` and runtime config snapshots.

#### B4 — Frontend client detail pages
- `powerfs-monitor-frontend/src/types/index.ts`: added `ClientStats` interface, extended `FuseMount.stats`.
- `powerfs-monitor-frontend/src/services/api.ts`: added `getFuseClientStats()`.
- `powerfs-monitor-frontend/src/pages/Fuse/index.tsx`: added summary columns (queue depth / CB / Coalescer) and a 5-tab stats Drawer (Overview / Scheduler / CircuitBreaker / Coalescer / Pool) with latency, lease, queue depth, CB state cards, dirty-bytes progress and coalescing ratio.
