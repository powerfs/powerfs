#!/bin/bash
# Since-aware monitor (v2). Fixes:
#  - Does NOT grep across the entire container history: filters each log
#    line individually by timestamp >= ($LOG_DIR mtime - 60s) so we only
#    count diagnostics that happened during the current test run.
#  - Fixes set -u unbound var on empty ERR_msg awk captures.
#  - Does NOT spawn 3-4 docker logs calls per snapshot: caches per-run
#    log tails to /tmp for speed.
set -uo pipefail

E2E_PREFIX="kernel_e2e_"
MOUNT="/mnt/powerfs"
FUSE_C="fuse-1"
FILER_C="filer-1"
MASTER_C="master-1"
MODE="${2:-once}"
INTERVAL="${E2E_MON_INTERVAL:-10}"

pick_latest_test_id() {
    docker exec "$FUSE_C" bash -c "ls -d $MOUNT/${E2E_PREFIX}* 2>/dev/null" 2>/dev/null \
        | sort -t_ -k3 -n | tail -1 | sed "s|.*${E2E_PREFIX}||"
}

TEST_ID="${1:-$(pick_latest_test_id)}"
if [ -z "$TEST_ID" ]; then
    echo "ERROR: no running $E2E_PREFIX test dir found under $MOUNT in $FUSE_C"
    exit 1
fi

TEST_ROOT="$MOUNT/${E2E_PREFIX}${TEST_ID}"
LOG_DIR="/tmp/${E2E_PREFIX}${TEST_ID}_logs"

FULL_KERNEL_FILES=96000
declare -A PHASE_EXPECT=(
    ["copy"]=10
    ["unpack"]=900
    ["defconfig"]=60
    ["build"]=1800
    ["rm"]=600
)

# Lower-bound timestamp: test start minus a 60s fuzz, so we include any
# container output just before the test harness launched.
LOG_DIR_MTIME=$(stat -c %Y "$LOG_DIR" 2>/dev/null || date +%s)
SINCE_EPOCH=$((LOG_DIR_MTIME - 60))
SINCE_DATE_UTC=$(date -u -d "@$SINCE_EPOCH" '+[%Y-%m-%dT%H:%M:%SZ')

# Extract log lines >= SINCE_DATE_UTC. Works because powerfs log timestamps
# sort lexicographically in ISO-8601 UTC.
logs_since_test() {
    local c="$1"
    local tmpf="/tmp/${c}_logs_since_${TEST_ID}.txt"
    # Recompute at most every 5s to avoid re-pulling gigabyte logs.
    if [ ! -f "$tmpf" ] || [ "$(( $(date +%s) - $(stat -c %Y "$tmpf" 2>/dev/null || echo 0) ))" -gt 5 ]; then
        docker logs "$c" 2>&1 | awk -v thresh="$SINCE_DATE_UTC" '$0 >= thresh' > "$tmpf" 2>/dev/null || true
    fi
    cat "$tmpf" 2>/dev/null || true
}

count_regex() {
    # Grep for regex on stdin; return decimal count (no trailing newlines).
    # Using `|| true` then normalising empty→0 via bash param expansion.
    local pat="$1" out
    out=$(grep -cE "$pat" 2>/dev/null)
    [ -z "$out" ] && out=0
    # strip any whitespace/newlines to keep bash arithmetic happy.
    printf '%s' "${out//[^0-9]/}" | head -c 20
}

snapshot() {
    local now_ts epoch_start
    now_ts=$(date +%s)

    # --- Phase identification from log dir ---
    local phase="?"
    local sub=""
    if [ -f "$LOG_DIR/phase5_delete.log" ]  && [ -s "$LOG_DIR/phase5_delete.log" ];  then phase="rm";        sub="$(( $(wc -l < "$LOG_DIR/phase5_delete.log"  2>/dev/null || echo 0) )) lines";
    elif [ -f "$LOG_DIR/phase4_build.log" ] && [ -s "$LOG_DIR/phase4_build.log" ]; then phase="build";     sub="$(( $(wc -l < "$LOG_DIR/phase4_build.log"   2>/dev/null || echo 0) )) lines";
    elif [ -f "$LOG_DIR/phase3_defconfig.log" ] && [ -s "$LOG_DIR/phase3_defconfig.log" ]; then phase="defconfig"; sub="$(( $(wc -l < "$LOG_DIR/phase3_defconfig.log" 2>/dev/null || echo 0) )) lines";
    elif [ -f "$LOG_DIR/phase2_unpack.log" ]; then phase="unpack"; sub="";
    elif [ -f "$LOG_DIR/phase1_copy.log" ];   then phase="copy";   sub="";
    fi

    # --- Elapsed wall time (approx) from test dir mtime ---
    local dir_mtime=$LOG_DIR_MTIME
    local elapsed=$((now_ts - dir_mtime))
    local elapsed_h=$((elapsed/3600))
    local elapsed_m=$(((elapsed%3600)/60))
    local elapsed_s=$((elapsed%60))

    # --- Live file/dir counts inside fuse container ---
    local fcount dcount
    fcount=$(docker exec "$FUSE_C" bash -c "find $TEST_ROOT -type f 2>/dev/null | wc -l" 2>/dev/null || echo "?")
    dcount=$(docker exec "$FUSE_C" bash -c "find $TEST_ROOT -type d 2>/dev/null | wc -l" 2>/dev/null || echo "?")
    local unpack_pct="?"
    if [ "$fcount" != "?" ] && [ "$phase" = "unpack" ]; then
        unpack_pct=$(awk -v n="$fcount" 'BEGIN{printf "%.1f%%", 100.0*n/96000}')
    fi
    local obj_count=0
    if [ "$phase" = "build" ] && [ -d "$TEST_ROOT" 2>/dev/null ]; then
        obj_count=$(docker exec "$FUSE_C" bash -c "find $TEST_ROOT/linux-6.17 -name '*.o' 2>/dev/null | wc -l" 2>/dev/null || echo 0)
    fi

    # --- Capture per-container logs ONCE, reuse for all metrics ---
    local fuse_logs filer_logs
    fuse_logs=$(logs_since_test "$FUSE_C")
    filer_logs=$(logs_since_test "$FILER_C")

    # --- FUSE-1 key health indicators ---
    local bp_enter bp_fail md_corr md_benign md_bad evict_count
    bp_enter=$(printf '%s' "$fuse_logs" | count_regex 'BACKPRESSURE\[ENTER\]')
    bp_fail=$(printf  '%s' "$fuse_logs" | count_regex 'BACKPRESSURE FAILURE')
    md_corr=$(printf  '%s' "$fuse_logs" | count_regex 'METADATA_CORRUPTION')
    md_benign=$(printf '%s' "$fuse_logs" | count_regex 'METADATA_CORRUPTION.*inode=1 ')
    [ "$md_benign" -gt "$md_corr" ] && md_benign=$md_corr
    md_bad=$((md_corr - md_benign))
    evict_count=$(printf '%s' "$fuse_logs" | count_regex 'InvalidateHandler EVICT')
    local flush_dirty_warn=$(printf '%s' "$fuse_logs" | count_regex 'post-flush dirty NOT cleared')
    local rpc_fail_setattr=$(printf '%s' "$fuse_logs" | count_regex 'setattr RPC failed')
    local rpc_fail_create=$(printf  '%s' "$fuse_logs" | count_regex 'create RPC failed')
    local rpc_fail_mkdir=$(printf   '%s' "$fuse_logs" | count_regex 'mkdir RPC failed')
    local rpc_fail_unlink=$(printf  '%s' "$fuse_logs" | count_regex 'unlink RPC failed')
    local rpc_fail_rmdir=$(printf   '%s' "$fuse_logs" | count_regex 'rmdir RPC failed')

    # --- Phase-log EIO counts (authoritative for user-visible failures) ---
    local phase2_eio=0 phase3_eio=0 phase4_eio=0 phase5_eio=0
    local e_regex='input/output error|EIO|no space left|read-only|stale file|transport endpoint'
    [ -f "$LOG_DIR/phase2_unpack.log"    ] && phase2_eio=$(grep -ciE "$e_regex" "$LOG_DIR/phase2_unpack.log"    || true)
    [ -f "$LOG_DIR/phase3_defconfig.log" ] && phase3_eio=$(grep -ciE "$e_regex" "$LOG_DIR/phase3_defconfig.log" || true)
    [ -f "$LOG_DIR/phase4_build.log"     ] && phase4_eio=$(grep -ciE "$e_regex" "$LOG_DIR/phase4_build.log"     || true)
    [ -f "$LOG_DIR/phase5_delete.log"    ] && phase5_eio=$(grep -ciE "$e_regex" "$LOG_DIR/phase5_delete.log"    || true)
    [ -z "${phase2_eio:-}" ] && phase2_eio=0; [ -z "${phase3_eio:-}" ] && phase3_eio=0
    [ -z "${phase4_eio:-}" ] && phase4_eio=0; [ -z "${phase5_eio:-}" ] && phase5_eio=0
    local total_eio=$((phase2_eio+phase3_eio+phase4_eio+phase5_eio))

    # --- Filer-1 key health indicators ---
    # Cross-shard race = NOT FOUND log line whose target name also appears
    # inside its own dir_entries preview (that's when the inode record is
    # lagging even though the dir entry has already been listed).
    local nf_total nf_in_entries retry_hit retry_exceeded unlink_count rmdir_count setattr_timeout
    nf_total=$(printf '%s' "$filer_logs" | count_regex 'FILER_NET_LOOKUP: NOT FOUND')
    nf_in_entries=$(printf '%s' "$filer_logs" | python3 - <<'PYEOF' 2>/dev/null || echo 0
import re,sys
cnt=0
for line in sys.stdin:
    if 'NOT FOUND' not in line:
        continue
    m1 = re.search(r"name='([^']+?)'\(len=", line)
    if not m1:
        continue
    tgt = m1.group(1)
    m2 = re.search(r"dir_entries=\[(.*)\]", line)
    if not m2:
        continue
    prev = m2.group(1)
    if f"'{tgt}'(len=" in prev:
        cnt += 1
print(cnt)
PYEOF
)
    retry_hit=$(printf     '%s' "$filer_logs" | count_regex 'inode record not yet visible for inode=')
    retry_exceeded=$(printf '%s' "$filer_logs" | count_regex 'cross-shard apply lag exceeded retry budget')
    unlink_count=$(printf   '%s' "$filer_logs" | count_regex 'FILER_NET_UNLINK: deleted inode=')
    rmdir_count=$(printf    '%s' "$filer_logs" | count_regex 'FILER_NET_RMDIR:')
    setattr_timeout=$(printf '%s' "$filer_logs" | count_regex 'setattr timeout waiting for apply')
    local create_rpc_fail_filer=$(printf '%s' "$filer_logs" | count_regex 'CREATE_FAIL|create.*STATUS_ERR_SERVER_ERROR')

    local phase_exp=${PHASE_EXPECT[$phase]:-60}
    local hline
    hline=$(printf '=%.0s' {1..95}); hline=${hline:0:95}
    printf '\033[H\033[2J'
    echo "$hline"
    printf " PowerFS kernel E2E monitor  —  TEST_ID=%s  —  now=%s UTC\n" "$TEST_ID" "$(date -u '+%Y-%m-%d %H:%M:%S')"
    echo "$hline"
    printf " Window start:        logs >= %s (test started ~%s)\n" "$SINCE_DATE_UTC" "$(date -u -d @$LOG_DIR_MTIME '+%Y-%m-%d %H:%M:%S')"
    printf " Elapsed:             %02d:%02d:%02d\n" "$elapsed_h" "$elapsed_m" "$elapsed_s"
    printf " Current phase:       %-12s  [%s]\n" "$phase" "$sub"
    printf " Phase baseline:      ~%ds  (reference ETA)\n" "$phase_exp"
    echo ""
    echo " Workload progress inside $FUSE_C :"
    printf "   Files:             %s / ~%d     unpack=%s\n" "$fcount" "$FULL_KERNEL_FILES" "$unpack_pct"
    printf "   Dirs:              %s\n" "$dcount"
    [ "$phase" = "build" ] && printf "   .o objects:        %s\n" "$obj_count"
    echo ""
    echo " Phase-log EIO counters (0 = PASS baseline, >0 = user-visible failure) :"
    printf "   unpack:%-4d  defconfig:%-4d  build:%-4d  rm:%-4d  ─── TOTAL: %d\n" \
        "$phase2_eio" "$phase3_eio" "$phase4_eio" "$phase5_eio" "$total_eio"
    echo ""
    echo " $FUSE_C health (windowed to this run):"
    printf "   BACKPRESSURE ENTER : %-8d (backpressure active; expected under heavy writes)\n" "$bp_enter"
    printf "   BACKPRESSURE FAIL  : %-8d (!!! returns EIO to writes — must stay 0)\n" "$bp_fail"
    printf "   flush re-mark race : %-8d (warnings, not fatal)\n" "$flush_dirty_warn"
    printf "   METADATA_CORRUPTION: %-8d (real=%d, root-inode-1 benign=%d)\n" "$md_corr" "$md_bad" "$md_benign"
    printf "   cache EVICT events : %-8d (cross-client invalidations — expected)\n" "$evict_count"
    echo ""
    echo " FUSE RPC failures (windowed to this run; 0 is target):"
    printf "   setattr : %-6d   create: %-6d   mkdir: %-6d\n" "$rpc_fail_setattr" "$rpc_fail_create" "$rpc_fail_mkdir"
    printf "   unlink  : %-6d   rmdir : %-6d\n" "$rpc_fail_unlink" "$rpc_fail_rmdir"
    echo ""
    echo " $FILER_C health (windowed to this run):"
    printf "   lookup NOT FOUND total: %-7d (mostly tar pre-lookups, expected)\n" "$nf_total"
    printf "     target present in dir_entries but NOT FOUND: %s (cross-shard race)\n" "$nf_in_entries"
    printf "   apply-lag spin retries: hits=%-6d exceeded=%-6d (exceeded>0 means retry budget too small)\n" "$retry_hit" "$retry_exceeded"
    printf "   setattr apply timeout : %-8d (should be 0 once CRDT mode+uid+gid kicks in)\n" "$setattr_timeout"
    printf "   unlink OK=%-7d  rmdir OK=%-7d\n" "$unlink_count" "$rmdir_count"
    echo "$hline"

    local danger=0
    [ "$total_eio" -gt 0 ]      && { danger=1; echo "!! DANGER: phase-log EIO count > 0"; }
    [ "$bp_fail" -gt 0 ]        && { danger=1; echo "!! DANGER: BACKPRESSURE FAILURE > 0 — cache flusher unhealthy"; }
    [ "$md_bad" -gt 0 ]         && { danger=1; echo "!! DANGER: METADATA_CORRUPTION (non-root) detected"; }
    [ "$retry_exceeded" -gt 0 ] && { danger=1; echo "!! DANGER: cross-shard apply retries exceeded — NOT FOUND spurious open EIO"; }
    [ "$rpc_fail_setattr" -gt 0 ] && { danger=1; echo "!! DANGER: setattr RPC failures propagated EIO to userspace"; }
    [ "$rpc_fail_create" -gt 0 ] && { danger=1; echo "!! DANGER: create RPC failures propagated EIO to userspace"; }
    [ "$danger" = "0" ] && echo "Status: CLEAN — no data-integrity red flags so far."
    echo ""
}

if [ "$MODE" = "watch" ]; then
    while true; do
        snapshot
        sleep "$INTERVAL"
    done
else
    snapshot
fi
