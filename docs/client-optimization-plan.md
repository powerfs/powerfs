# PowerFS Client Optimization Plan

## Overview

This document outlines the optimization plan for PowerFS client-side components (FUSE client, Volume client, MetaShard client). The focus is on **precision fault isolation**, **lock-free concurrency**, and **server-side resource cleanup**.

## Current State Analysis

### 1. Circuit Breaker: Global Singleton (No Per-Server Isolation)

**File**: `powerfs-fuse-core/src/circuit_breaker.rs`

**Problem**: Both `VolumeClient` and `MetaShardClient` use a **single global circuit breaker** for all server connections. If one Volume Server fails, all requests to other healthy Volume Servers are also rejected.

```
VolumeClient {
    breaker: Arc<CircuitBreaker>  // ONE breaker for ALL 3 Volume Servers
}
```

**Impact**: A single server failure cascades to all other servers, violating the principle of fault isolation.

### 2. Request Queue: Mutex + VecDeque (Lock-Based)

**File**: `powerfs-fuse-core/src/meta_shard_client.rs` (L131-L163)

**Problem**: `RequestQueue` uses `VecDeque` protected by `Mutex`. High-concurrency scenarios cause lock contention.

```rust
pub struct RequestQueue {
    pub queue: VecDeque<PendingRequest>,  // Lock-based
    pub max_size: usize,
}
```

**Note**: `crossbeam` is in `Cargo.toml` dependencies but is NOT used in client code.

### 3. Connection Pool: Global Mutex HashMap

**Problem**: Both `volume_connections` and `filer_connections` use `Arc<Mutex<HashMap<String, Arc<PowerFsNetClient>>>>`. Every connection lookup/creation acquires the global lock.

### 4. Server-Side Lease Cleanup: Partial Implementation

**File**: `powerfs-volume/src/range_lease.rs` (L342-L380), `net_handler.rs` (L693-L710)

**Status**: `disconnect_holder()` is implemented and `on_disconnect` callback is wired. However:
- Only releases `session-{client_id}` format leases
- Client-provided holder (UUID-based `client_id`) leases rely on background expired cleanup
- No per-client rate limiting
- Filer side lacks `on_disconnect` resource cleanup

---

## Optimization Phases

### Phase 1: Per-Server Precision Circuit Breaker (HIGH PRIORITY)

**Goal**: One server failure should NOT affect requests to other servers.

#### 1.1 Implement `CircuitBreakerPool`

**File**: `powerfs-fuse-core/src/circuit_breaker.rs`

```rust
/// A pool of circuit breakers, one per backend server address.
/// Provides precise fault isolation: only the failed server's requests are rejected.
pub struct CircuitBreakerPool {
    breakers: DashMap<String, Arc<CircuitBreaker>>,
    default_config: CircuitBreakerConfig,
}

impl CircuitBreakerPool {
    /// Check if the circuit for the given server address is available.
    /// Creates a new breaker if none exists for this address.
    pub fn check(&self, addr: &str) -> bool;

    /// Record a success for the given server address.
    pub fn record_success(&self, addr: &str);

    /// Record a failure for the given server address.
    pub fn record_failure(&self, addr: &str);

    /// Get the current circuit state for the given server.
    pub fn state(&self, addr: &str) -> CircuitState;

    /// Reset the circuit for the given server.
    pub fn reset(&self, addr: &str);

    /// Remove the circuit for the given server (when server is decommissioned).
    pub fn remove(&self, addr: &str);
}
```

**Key Design**:
- Uses `DashMap` for lock-free per-server breaker lookup
- Each server address (e.g., `"172.20.0.21:8080"`) gets its own `CircuitBreaker`
- Automatically creates breakers on first access

#### 1.2 Integrate into VolumeClient

**File**: `powerfs-fuse-core/src/volume_client.rs`

```rust
// BEFORE
breaker: Arc<CircuitBreaker>,

// AFTER
breakers: Arc<CircuitBreakerPool>,
```

**Changes**:
- Replace all `self.breaker.is_available()` with `self.breakers.check(&addr)`
- Replace all `self.breaker.record_success()` with `self.breakers.record_success(&addr)`
- Replace all `self.breaker.record_failure()` with `self.breakers.record_failure(&addr)`
- All submission functions need to know the target `addr` for proper breaker routing

#### 1.3 Integrate into MetaShardClient

**File**: `powerfs-fuse-core/src/meta_shard_client.rs`

Same pattern as VolumeClient. Each Filer node address gets its own breaker.

#### 1.4 Add Unit Tests

**File**: `powerfs-fuse-core/src/circuit_breaker.rs` (add to existing `#[cfg(test)] mod tests`)

Test scenarios:
- `test_pool_isolation`: Server A failure does NOT affect Server B
- `test_pool_independent_recovery`: Server A recovery does NOT affect Server B
- `test_pool_auto_create`: New address automatically creates a breaker
- `test_pool_remove`: Removing a server clears its breaker

#### 1.5 Add `dashmap` Dependency

**File**: `powerfs-fuse-core/Cargo.toml`

```toml
dashmap = "5"
```

---

### Phase 2: Lock-Free Queue + DashMap Connection Pool (MEDIUM PRIORITY)

**Goal**: Eliminate Mutex bottlenecks in high-concurrency scenarios.

#### 2.1 Replace RequestQueue with Lock-Free Alternative

**Approach**: Keep `RequestQueue` API (`enqueue`/`dequeue`/`len`/`is_empty`) but use `crossbeam-queue::ArrayQueue` internally.

```rust
pub struct RequestQueue {
    queue: crossbeam_queue::ArrayQueue<PendingRequest>,  // Lock-free MPMC
    max_size: usize,
}
```

**Key Changes**:
- `ArrayQueue` is bounded (fixed capacity), so `max_size` is set at creation
- `enqueue` returns `Err` on full (matches current behavior)
- `dequeue` returns `None` on empty (matches current behavior)
- Remove all `Mutex<RequestQueue>` wrappers → `Arc<RequestQueue>` directly

#### 2.2 Replace Connection Pool HashMap with DashMap

```rust
// BEFORE
volume_connections: Arc<Mutex<HashMap<String, Arc<PowerFsNetClient>>>>,

// AFTER
volume_connections: Arc<DashMap<String, Arc<PowerFsNetClient>>>,
```

Same for `filer_connections`, `volume_router`, and `leases`.

#### 2.3 Replace RwLock with DashMap for Router and Lease Tables

```rust
// BEFORE
volume_router: Arc<RwLock<HashMap<u64, VolumeInfo>>>,
leases: Arc<RwLock<HashMap<(u64, u64), LeaseInfo>>>,

// AFTER
volume_router: Arc<DashMap<u64, VolumeInfo>>,
leases: Arc<DashMap<(u64, u64), LeaseInfo>>,
```

**Note**: `DashMap` provides better concurrent read performance than `RwLock`, but write operations still acquire per-shard locks. For the lease renewal loop that iterates all leases, we may need to use `DashMap::iter()` or convert to `DashMap::into_iter()` for safe iteration.

#### 2.4 Add `crossbeam-queue` Dependency

**File**: `powerfs-fuse-core/Cargo.toml`

```toml
crossbeam-queue = "0.3"
dashmap = "5"
```

---

### Phase 3: Server-Side Per-Client Lease Cleanup (HIGH PRIORITY)

**Goal**: When a FUSE client disconnects, the server must immediately release ALL leases held by that client, preventing lease leaks that block other clients.

#### 3.1 Unify Client Holder Identity

**Problem**: Currently leases are registered under two forms:
1. `session-{client_id}` (auto-generated by server for direct connections)
2. Client-provided holder (e.g., UUID-based `client_id`)

**Solution**: Clients MUST use a consistent, stable `client_id` (UUID) as the lease holder. The server should register leases under this ID AND also under `session-{numeric_id}` for backward compatibility.

#### 3.2 Enhance on_disconnect in Volume Server

**File**: `powerfs-volume/src/net_handler.rs`

```rust
async fn on_disconnect(&self, client_id: u64) {
    // 1. Release session-scoped leases (existing behavior)
    let session_holder = format!("session-{}", client_id);
    self.lease_mgr.disconnect_holder(&session_holder);

    // 2. NEW: Also release leases registered under the client's UUID-based holder
    // The client sends its client_id during handshake; we map numeric_id → UUID
    if let Some(uuid_holder) = self.client_id_map.get(&client_id) {
        self.lease_mgr.disconnect_holder(uuid_holder);
    }

    // 3. NEW: Clean up any per-client rate limiters
    self.rate_limiters.remove(&client_id);
}
```

#### 3.3 Add Client ID Mapping on Server Side

**File**: `powerfs-volume/src/net_handler.rs`

```rust
struct VolumeNetHandler {
    // ...
    client_id_map: Arc<Mutex<HashMap<u64, String>>>,  // numeric_id → UUID holder
    rate_limiters: Arc<DashMap<u64, RateLimiter>>,    // per-client rate limiters
}
```

#### 3.4 Add on_disconnect to Filer Server

**File**: `powerfs-filer/src/net_handler.rs`

Filer needs similar `on_disconnect` to clean up:
- Invalidation subscriptions
- Per-client metadata cache entries
- Delta sync state

#### 3.5 Add Per-Client Rate Limiting

**File**: `powerfs-net/src/server_connection.rs`

```rust
pub struct ClientSession {
    // ...
    rate_limiter: RateLimiter,  // token bucket per client
}
```

---

### Phase 4: Multi-Queue Priority Scheduling (LOW PRIORITY)

**Goal**: Ensure lease renewal and other critical operations are never blocked by data requests.

#### 4.1 Dedicated Background Threads Per Queue

**Current**: All queues share background processors.
**Optimization**: Each queue type has its own dedicated processor thread.

```rust
impl VolumeClient {
    async fn start_background_processors(&self) {
        // High priority: lease renewal
        tokio::spawn(self.lease_processor_loop());
        // Medium priority: management
        tokio::spawn(self.mgmt_processor_loop());
        // Low priority: data
        tokio::spawn(self.data_processor_loop());
    }
}
```

#### 4.2 Priority-Based Dispatch

When multiple queues have pending requests, process in order:
1. Lease queue (highest priority) — lease renewals, acquisitions
2. Management queue — topology updates, health checks
3. Data queue (lowest priority) — read/write operations

---

## Progress Tracking

| Phase | Description | Status | Files Changed | Tests | Commit |
|-------|-------------|--------|---------------|-------|--------|
| **1.1** | CircuitBreakerPool implementation | ✅ Completed | `circuit_breaker.rs` | Unit tests (4) | `2d7acbcb` |
| **1.2** | VolumeClient per-server breaker | ✅ Completed | `volume_client.rs` | Integration tests | `2d7acbcb` |
| **1.3** | MetaShardClient per-server breaker | ✅ Completed | `meta_shard_client.rs` | Integration tests | `2d7acbcb` |
| **1.4** | Phase 1 quality check & test | ✅ Completed | - | `cargo clippy` (0 warnings), `cargo test` (87 passed) | `2d7acbcb` |
| **2.1** | Lock-free RequestQueue | ⬜ Pending | `meta_shard_client.rs` | Unit tests | TBD |
| **2.2** | DashMap connection pool | ⬜ Pending | `volume_client.rs`, `meta_shard_client.rs` | Unit tests | TBD |
| **2.3** | DashMap router & lease tables | ⬜ Pending | `volume_client.rs` | Unit tests | TBD |
| **2.4** | Phase 2 quality check & test | ⬜ Pending | - | `cargo clippy`, `cargo test` | TBD |
| **3.1** | Unified client holder identity | ⬜ Pending | `net_handler.rs` | Integration tests | TBD |
| **3.2** | Volume on_disconnect enhancement | ⬜ Pending | `net_handler.rs` | Integration tests | TBD |
| **3.3** | Client ID mapping | ⬜ Pending | `net_handler.rs` | Unit tests | TBD |
| **3.4** | Filer on_disconnect | ⬜ Pending | `net_handler.rs` | Integration tests | TBD |
| **3.5** | Per-client rate limiting | ⬜ Pending | `server_connection.rs` | Unit tests | TBD |
| **3.6** | Phase 3 quality check & test | ⬜ Pending | - | `cargo clippy`, `cargo test` | TBD |
| **4.1** | Dedicated queue threads | ⬜ Pending | `volume_client.rs` | Integration tests | TBD |
| **4.2** | Priority-based dispatch | ⬜ Pending | `volume_client.rs` | Integration tests | TBD |
| **4.3** | Phase 4 quality check & test | ⬜ Pending | - | `cargo clippy`, `cargo test` | TBD |

### Phase 1 Summary (Completed)

**Key Changes:**
- Added `CircuitBreakerPool` struct in `circuit_breaker.rs` using `DashMap<String, Arc<CircuitBreaker>>`
- Removed global `breaker` from `MetaShardClient` and `VolumeClient`
- All circuit breaker checks now use per-server address routing
- Added `resolve_filer_addr()` and `resolve_volume_addr()` methods for address resolution
- Updated `record_success()` and `record_failure()` to accept server address parameter
- Added `test_circuit_breaker_per_server_isolation` test

**Test Results:**
- 71 unit tests (circuit_breaker, meta_shard_client, volume_client)
- 13 integration tests (client initialization, request submission, cleanup)
- 3 mock server tests (metadata provider, volume provider, end-to-end)
- Clippy: 0 warnings
- Cargo check: passed

---

## Risk Assessment

| Phase | Risk | Mitigation |
|-------|------|------------|
| Phase 1 | DashMap iteration safety | Use `DashMap::iter()` for read-only access; use `retain()` for filtering |
| Phase 2 | ArrayQueue bounded capacity | Set capacity to match `queue_max_size`; handle `PushError` gracefully |
| Phase 3 | Client ID mapping inconsistency | Use UUID from `client_id` field during handshake; fall back to `session-{id}` |
| Phase 4 | Thread overhead | Use `tokio::spawn` for async tasks; test with 3+ servers |

## References

- [data-consistency-design.md](data-consistency-design.md) - Data consistency architecture
- [distributed-communication-architecture.md](distributed-communication-architecture.md) - Communication architecture
- [improvement-plan.md](improvement-plan.md) - General improvement plan
- Known Issues in README.md - Lessons learned from past issues
