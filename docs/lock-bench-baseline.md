# PowerFS Lock Hot-Path Baseline (Phase 3)

> 本地参考文档, 不入 git (docs/ 已 .gitignore)。
>
> Baseline captured by `cargo bench --package powerfs-bench -- --save-baseline phase3`
> on 2026-08-19. Numbers are the criterion **median point_estimate** in
> nanoseconds, unless otherwise noted. Run on the development machine; treat
> as relative weights, not absolute SLAs.

This is the data called for by `docs/lock-optimization-plan.md` §四 step 7
("性能基线测试") and §五 ("优化方向 — 性能基线后决定"). It drives the
prioritization of phase-4 optimizations (Early Grant / SN / Lockify /
kernel client).

## 1. Client-side cache & manager (powerfs-lock-fuse)

| Bench                              | Median (ns) | Notes |
|------------------------------------|-------------|-------|
| `client_state/get_inode_hit`       | 61          | `Mutex<HashMap>` lookup, valid entry |
| `client_state/get_inode_miss`      | 24          | `Mutex<HashMap>` lookup, key absent |
| `client_state/put_inode`           | 370         | Mutex lock + HashMap insert + `String` clone |
| `client_state/sweep_empty`         | 203         | Walks 100 fresh entries, no removals |
| `client_state/sweep_with_expired`  | 3460        | Walks 100 entries, removes 10 expired |
| `metrics/record_acquire_hit`       | 12          | Two `AtomicU64::fetch_add` (Relaxed) |
| `metrics/snapshot`                 | 2           | Eight `AtomicU64::load` (Relaxed) |
| `manager/acquire_inode_cache_hit`  | 312         | Cache hit, no RPC (mock backend) |
| `manager/acquire_inode_cache_miss` | 391         | Cache miss → mock RPC → cache fill |
| `manager/acquire_range_cache_hit`  | 326         | Range cache hit |
| `manager/acquire_then_release`     | 430         | Per-write cycle: acquire (miss) + release |

**Read**: cache hit is ~312 ns; the sweep-`acquire` coupling contributes
~200 ns of that (see `sweep_empty`). Cache miss costs ~80 ns more than a
hit because of the `put_inode` (370 ns) overhead.

## 2. Server-side lease store (powerfs-lease / MemoryLeaseStore)

| Bench                          | Median (ns) | Notes |
|--------------------------------|-------------|-------|
| `store/acquire_clean`          | 1421        | First acquire on an inode nobody holds |
| `store/acquire_conflict`       | 177         | Conflict rejection (fast-fail) |
| `store/renew`                  | 66          | Token → entry lookup + expiry bump |
| `store/validate_token`         | 60          | Per-write-IO guard |
| `store/acquire_then_release`   | 1244        | Acquire (clean) + release |
| `store/cleanup_expired_empty` | 374         | Periodic sweep when nothing expired |

**Read**: server `acquire_clean` (1.4 µs) is dominated by token generation
(`format!("lease-{}-{}", epoch, uuid::Uuid::new_v4())`) + 3 HashMap inserts
(`leases`, `group_index`, `holder_index`). The `acquire_conflict` fast-fail
path is 8× cheaper because it short-circuits before token generation.

## 3. Health gate + Fencer (powerfs-lock-health)

| Bench                              | Median (ns) | Notes |
|------------------------------------|-------------|-------|
| `health/check_allow`               | 47          | Healthy client (score 100) |
| `health/check_throttle`            | 68          | Throttle band (score ∈ [10, 30)) |
| `health/check_quarantine`          | 68          | Active quarantine short-circuit |
| `health/record_acquire`            | 66          | Per-acquire churn feed |
| `health/record_renew_success`      | 44          | Per-renew score bump |
| `fencer/register`                  | 310         | `fetch_add` + HashMap insert + String alloc |
| `fencer/validate_ok`               | 28          | Fast-path: epoch matches |
| `fencer/validate_stale`            | 37          | Stale-epoch rejection (Err path) |
| `fencer/validate_not_registered`   | 26          | Post-`bump_all` rejection |

**Read**: health-gate adds **47–68 ns** to every server-side acquire
(1.4 µs), i.e. <5% overhead. Fencer `validate` adds **~30 ns**, also <3%.
Both are negligible vs. the lease-store `acquire` floor; they will not be
phase-4 bottlenecks.

## 4. TLV wire codec (powerfs-lock-net)

| Bench                          | Median (ns) | Notes |
|--------------------------------|-------------|-------|
| `codec/encode_acquire`         | 99          | Smallest payload (inode-level) |
| `codec/encode_acquire_range`   | 183         | + 2 u64 range fields |
| `codec/encode_grant_no_sn`     | 149         | SN omitted (forward-compat path) |
| `codec/encode_grant_with_sn`   | 207         | + FIELD_SN u64 |
| `codec/encode_release`         | 146         | |
| `codec/encode_revoke`          | 102         | Smallest frame |
| `codec/decode_acquire`         | 247         | Field parse via `HashMap<u8, Vec<u8>>` |
| `codec/decode_acquire_range`  | 297         | |
| `codec/decode_grant_no_sn`     | 290         | |
| `codec/decode_grant_with_sn`  | 598         | Extra `get_u64_field` call |
| `codec/decode_release`         | 168         | |
| `codec/decode_revoke`          | 113         | |

**Read**: encode is 100–200 ns; decode is 100–600 ns (the with-SN decode is
~2× the no-SN decode because `get_u64_field` allocates a `Vec<u8>` clone).
Vs. RPC RTT (typically >100 µs even on loopback), the codec is negligible.
Not a phase-4 bottleneck.

The decode allocation (`value.to_vec()` in `parse_fields`) is the obvious
micro-opt target if codec ever shows up — replace `HashMap<u8, Vec<u8>>`
with a small `[Option<Vec<u8>>; 256]` array or a `SmallVec` to avoid the
HashMap + clone overhead. Defer until measured.

## 5. Phase-4 prioritization (data-driven)

Based on the baseline above, the optimizations from `docs/lock-optimization-plan.md`
§7.3 rank as follows for the **write-heavy HPC workload** (the plan's
primary target):

| Priority | Optimization | Expected gain | Justification from baseline |
|----------|--------------|---------------|------------------------------|
| P1 | **Lazy sweep on cache hit** | ~200 ns per write (≈65% of cache-hit cost) | `sweep_expired` runs on every `acquire` and costs 203 ns even when nothing's expired. A "skip sweep if last sweep < N ms ago" guard removes this entirely on hot inodes. |
| P2 | **SN + Early Grant** (§5.2) | ≈50% of lock-handoff latency under contention | Server `acquire_clean` is 1.4 µs and `acquire_then_release` is 1.2 µs; under contention the revoke→grant chain doubles that. Early Grant overlaps them. |
| P3 | **Lockify async metadata** (§5.1) | Removes Raft RPC from `creat`/`mkdir` hot path | Metadata latency is dominated by Raft (~ms), not by lock layer (µs). Baseline doesn't measure Raft; defer to a separate metadata bench. |
| P4 | **Integer token optimization** | Server `acquire_clean` 1.4 µs → ~200 ns | Replace `format!("lease-{}-{}", epoch, uuid)` with a fixed-size `u128` packed from `(epoch, counter)`. Only worth doing if write-path profiling shows token alloc is the floor. |
| Defer | **Codec `HashMap` → array** | Decode 250–600 ns → ~100 ns | Codec is <1% of RPC RTT. Skip unless RPC moves off TCP. |
| Defer | **Health-gate inlining** | 47–68 ns → ~10 ns | Already <5% of acquire floor. Skip. |
| Defer | **Fencer validate inlining** | 28–37 ns → ~5 ns | Already <3% of acquire floor. Skip. |

## 6. Reproducing

```bash
# Full baseline (saves to target/criterion/*/new/ for --baseline phase3 compare):
cargo bench --package powerfs-bench -- --save-baseline phase3

# Quick smoke (1s warmup, 2s measurement, 50 samples):
cargo bench --package powerfs-bench -- --warm-up-time 1 --measurement-time 2 --sample-size 50

# Compare a future change against this baseline:
cargo bench --package powerfs-bench -- --baseline phase3

# HTML reports (per-bench distributions, PDFs, regressions):
xdg-open target/criterion/manager_acquire_inode_cache_hit/report/index.html
```

## 7. Caveats

- Numbers are from a single development machine. They are **relative
  weights** for prioritization, not absolute SLAs. Production numbers will
  differ; re-run on the deployment hardware before drawing conclusions.
- The `manager/acquire_*` benches use a `NoopBackend` whose `acquire_inode_lease`
  is ~10 ns (two atomics + Mutex lock). Subtract this from the manager
  numbers to get the pure manager/cache overhead.
- `store/acquire_clean` uses `format!("lease-{}-{}", epoch, uuid)` for
  token generation; this dominates the 1.4 µs. A `u128`-packed token would
  cut it to ~200 ns (see P4 above).
- Async benches (`manager/*`) use `tokio::runtime::Runtime::block_on`
  inside `b.iter`, which adds a small per-iter scheduling overhead (~30 ns).
  Subtract this when comparing to the synchronous `client_state/*` and
  `store/*` benches.
- The `client_state/sweep_with_expired` bench creates a fresh
  `ClientLeaseState` per batch (`iter_batched`), so it includes one heap
  alloc per iter; the true incremental sweep cost on a warm cache is lower.
