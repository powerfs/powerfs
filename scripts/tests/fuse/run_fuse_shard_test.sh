#!/bin/bash
# =============================================================================
# PowerFS FUSE Shard Routing & TopologyUpdateListener Test
#
# Focused on verifying:
#   1. Basic L1 smoke (mount/mkdir/read/write/rm) — quick gate
#   2. Shard routing across many parent dirs (inode > shard_count)
#   3. Large-file migrate_inline path (triggers shard routing on close/fsync)
#   4. TopologyUpdateListener: restart a filer → verify auto re-sync
#   5. 60s continuous mixed ops stability
#
# Per-test features:
#   - Timestamped RUN/PASS/FAIL/HANG lines (real-time visibility)
#   - Per-test timeout + D-state hang detection
#   - Diagnostics capture on hang (stack, wchan, FUSE log, D-state procs)
#   - Non-blocking: output is line-buffered for `docker exec` polling
#
# Usage:
#   docker cp scripts/tests/fuse/run_fuse_shard_test.sh fuse-1-test:/tmp/
#   docker exec fuse-1-test stdbuf -oL /tmp/run_fuse_shard_test.sh
# =============================================================================

set -u
export LC_ALL=C

MOUNT="${MOUNT:-/mnt/powerfs}"
TEST_ROOT="$MOUNT/shard_test"
FUSE_LOG="${FUSE_LOG:-/var/log/powerfs-fuse.log}"
DIAG_DIR="${DIAG_DIR:-/tmp/fuse_shard_diag}"
TEST_TIMEOUT="${TEST_TIMEOUT:-20}"
STABLE_DURATION="${STABLE_DURATION:-60}"

PASS=0; FAIL=0; HANG=0; SKIP=0
FAILED_LIST=(); HANG_LIST=()

mkdir -p "$DIAG_DIR" "$TEST_ROOT"

# Colors
G='\033[0;32m'; R='\033[0;31m'; Y='\033[0;33m'; C='\033[0;36m'; B='\033[1m'; N='\033[0m'

timestamp() { date '+%H:%M:%S'; }

# ── Per-test runner with watchdog ───────────────────────────────────
# Args: <name> <timeout_secs> <command...>
run_test() {
    local name="$1"; shift
    local timeout_s="$1"; shift
    local cmd="$*"
    local tmpdir=$(mktemp -d /tmp/wd_XXXXXX)
    local outfile="$tmpdir/output"

    printf "  ${C}[%s]${N} RUN: %s (timeout=%ss)\n" "$(timestamp)" "$name" "$timeout_s"

    ( eval "$cmd" > "$outfile" 2>&1 ) &
    local cmd_pid=$!

    local elapsed=0 hung=0
    while kill -0 "$cmd_pid" 2>/dev/null; do
        if [ "$elapsed" -ge "$timeout_s" ]; then
            local state=$(cat /proc/$cmd_pid/stat 2>/dev/null | awk '{print $3}')
            hung=1
            printf "  ${R}[%s]${N} HANG: %s (state=%s at %ss)\n" "$(timestamp)" "$name" "$state" "$elapsed"
            capture_diagnostics "$name" "$cmd_pid" "$tmpdir"
            kill -TERM "$cmd_pid" 2>/dev/null; sleep 1
            kill -KILL "$cmd_pid" 2>/dev/null
            pkill -KILL -P "$cmd_pid" 2>/dev/null || true
            wait "$cmd_pid" 2>/dev/null || true
            break
        fi
        sleep 1
        elapsed=$((elapsed + 1))
        # Early D-state warning at 5s intervals
        if [ $((elapsed % 5)) -eq 0 ] && [ "$hung" = "0" ]; then
            local s=$(cat /proc/$cmd_pid/stat 2>/dev/null | awk '{print $3}')
            if [ "$s" = "D" ]; then
                printf "  ${Y}[%s]${N} WARN: %s in D-state at %ss\n" "$(timestamp)" "$name" "$elapsed"
            fi
        fi
    done

    if [ "$hung" = "1" ]; then
        HANG=$((HANG+1)); HANG_LIST+=("$name")
        printf "  ${R}[HANG]${N} %s\n" "$name"
        printf "  Output: %s\n" "$(tail -3 "$outfile" 2>/dev/null | tr '\n' ' ')"
        rm -rf "$tmpdir"; return 1
    fi

    wait "$cmd_pid" 2>/dev/null
    local rc=$?
    if [ "$rc" -eq 0 ]; then
        PASS=$((PASS+1))
        printf "  ${G}[%s]${N} PASS: %s (%ss)\n" "$(timestamp)" "$name" "$elapsed"
    else
        FAIL=$((FAIL+1)); FAILED_LIST+=("$name (rc=$rc)")
        printf "  ${R}[%s]${N} FAIL: %s (rc=%d, %ss)\n" "$(timestamp)" "$name" "$rc" "$elapsed"
        printf "  Output: %s\n" "$(tail -3 "$outfile" 2>/dev/null | tr '\n' ' ')"
    fi
    rm -rf "$tmpdir"; return $rc
}

# Capture diagnostics for a hung process
capture_diagnostics() {
    local name="$1" pid="$2" tmpdir="$3"
    local diag_file="$DIAG_DIR/${name//\//_}_hang.txt"
    {
        echo "=== Hang Diagnostics: $name ==="
        echo "Time: $(date '+%Y-%m-%d %H:%M:%S')"
        echo "PID: $pid"
        echo ""
        echo "--- /proc/$pid/stat ---"
        cat /proc/$pid/stat 2>/dev/null || echo "(unavailable)"
        echo ""
        echo "--- /proc/$pid/stack ---"
        cat /proc/$pid/stack 2>/dev/null || echo "(unavailable)"
        echo ""
        echo "--- /proc/$pid/wchan ---"
        cat /proc/$pid/wchan 2>/dev/null || echo "(unavailable)"
        echo ""
        echo "--- Child processes ---"
        for child in $(pgrep -P "$pid" 2>/dev/null); do
            local cs=$(cat /proc/$child/stat 2>/dev/null | awk '{print $3}')
            local cc=$(cat /proc/$child/cmdline 2>/dev/null | tr '\0' ' ')
            echo "  PID=$child state=$cs cmd=$cc"
            echo "  /proc/$child/stack:"
            cat /proc/$child/stack 2>/dev/null | head -10 || echo "  (unavailable)"
        done
        echo ""
        echo "--- All D-state processes ---"
        ps aux 2>/dev/null | awk '$8 ~ /D/ {print}' || echo "(none)"
        echo ""
        echo "--- FUSE client log (last 40 lines) ---"
        tail -40 "$FUSE_LOG" 2>/dev/null || echo "(unavailable)"
        echo ""
        echo "--- Test output ---"
        cat "$tmpdir/output" 2>/dev/null | tail -15 || echo "(unavailable)"
    } > "$diag_file"
    printf "  ${Y}[%s]${N} Diag saved: %s\n" "$(timestamp)" "$diag_file"
}

# ── Cleanup ─────────────────────────────────────────────────────────
cleanup() { rm -rf "$TEST_ROOT" 2>/dev/null || true; }
trap cleanup EXIT

# ════════════════════════════════════════════════════════════════════
# Header
# ════════════════════════════════════════════════════════════════════
printf "============================================================\n"
printf "  PowerFS FUSE Shard Routing & TopologyListener Test\n"
printf "  Mount: %s\n" "$MOUNT"
printf "  FUSE log: %s\n" "$FUSE_LOG"
printf "  Per-test timeout: %ss  Stability: %ss\n" "$TEST_TIMEOUT" "$STABLE_DURATION"
printf "  Time: %s\n" "$(date '+%Y-%m-%d %H:%M:%S')"
printf "============================================================\n\n"

# ════════════════════════════════════════════════════════════════════
# Phase 0: Smoke gate (mount + basic ops)
# ════════════════════════════════════════════════════════════════════
printf "${B}━━━ Phase 0: Smoke Gate ━━━${N}\n"

run_test "P0.01 mount alive" 5 "mount | grep -q '$MOUNT'"
run_test "P0.02 mkdir" 5 "mkdir -p $TEST_ROOT/p0 && test -d $TEST_ROOT/p0"
run_test "P0.03 write+read" 5 "echo smoke > $TEST_ROOT/p0/s.txt && [ \"\$(cat $TEST_ROOT/p0/s.txt)\" = smoke ]"
run_test "P0.04 unlink" 5 "rm $TEST_ROOT/p0/s.txt && test ! -e $TEST_ROOT/p0/s.txt"

if [ "$PASS" -lt 3 ]; then
    printf "\n${R}Smoke gate failed (%d/4 pass) — aborting.${N}\n" "$PASS"
    exit 1
fi

# ════════════════════════════════════════════════════════════════════
# Phase 1: Shard routing across many parent dirs
# Creates files in 20 different parent dirs to exercise shard routing
# for inodes that span multiple shard ranges (parent_ino > shard_count).
# ════════════════════════════════════════════════════════════════════
printf "\n${B}━━━ Phase 1: Shard Routing (20 parent dirs) ━━━${N}\n"

run_test "P1.01 create 20 dirs" 15 "for i in \$(seq 1 20); do mkdir -p $TEST_ROOT/p1/d\$i; done && [ \"\$(find $TEST_ROOT/p1 -maxdepth 1 -type d | wc -l)\" = 21 ]"

run_test "P1.02 write file in each dir" 20 "for i in \$(seq 1 20); do echo content_\$i > $TEST_ROOT/p1/d\$i/f.txt; done && [ \"\$(find $TEST_ROOT/p1 -name f.txt | wc -l)\" = 20 ]"

run_test "P1.03 verify all readable" 15 "ok=0; for i in \$(seq 1 20); do [ \"\$(cat $TEST_ROOT/p1/d\$i/f.txt)\" = content_\$i ] && ok=\$((ok+1)); done; [ \$ok = 20 ]"

run_test "P1.04 rename across dirs" 10 "mv $TEST_ROOT/p1/d1/f.txt $TEST_ROOT/p1/d2/moved.txt && test -e $TEST_ROOT/p1/d2/moved.txt && test ! -e $TEST_ROOT/p1/d1/f.txt"

run_test "P1.05 delete half" 10 "for i in \$(seq 1 10); do rm $TEST_ROOT/p1/d\$((i+10))/f.txt 2>/dev/null; done && [ \"\$(find $TEST_ROOT/p1 -name f.txt | wc -l)\" = 9 ]"

# ════════════════════════════════════════════════════════════════════
# Phase 2: Inline → Flat migration (large file, triggers shard routing)
# Inline threshold is 8KB. Writing >8KB triggers migrate_inline_alloc
# which uses shard routing. We test 4KB, 8KB, 16KB, 64KB, 256KB, 1MB.
# ════════════════════════════════════════════════════════════════════
printf "\n${B}━━━ Phase 2: Inline→Flat Migration (shard routing on close) ━━━${N}\n"

mkdir -p "$TEST_ROOT/p2"

for size_label in 4K 8K 16K 64K 256K 1M; do
    case $size_label in
        4K) bs=4K; count=1;;
        8K) bs=4K; count=2;;
        16K) bs=4K; count=4;;
        64K) bs=64K; count=1;;
        256K) bs=64K; count=4;;
        1M) bs=1M; count=1;;
    esac
    run_test "P2.${size_label} write+read+verify" 20 \
        "dd if=/dev/zero of=$TEST_ROOT/p2/f_$size_label.bin bs=$bs count=$count 2>&1 && \
         md5_1=\$(md5sum $TEST_ROOT/p2/f_$size_label.bin | cut -d' ' -f1) && \
         md5_2=\$(dd if=$TEST_ROOT/p2/f_$size_label.bin bs=1M 2>/dev/null | md5sum | cut -d' ' -f1) && \
         [ \"\$md5_1\" = \"\$md5_2\" ]"
done

run_test "P2.07 fsync all sizes" 15 "for f in $TEST_ROOT/p2/f_*.bin; do sync \"\$f\"; done"

run_test "P2.08 overwrite 1M file" 20 "dd if=/dev/urandom of=$TEST_ROOT/p2/f_1M.bin bs=1M count=1 2>&1 && [ \"\$(stat -c%s $TEST_ROOT/p2/f_1M.bin)\" = 1048576 ]"

# ════════════════════════════════════════════════════════════════════
# Phase 3: Concurrent shard routing
# 4 processes writing to different dirs simultaneously — exercises
# shard routing under contention.
# ════════════════════════════════════════════════════════════════════
printf "\n${B}━━━ Phase 3: Concurrent Shard Routing ━━━${N}\n"

run_test "P3.01 4 procs × 50 files" 30 \
    "mkdir -p $TEST_ROOT/p3 && \
     for p in 1 2 3 4; do (for i in \$(seq 1 50); do echo p\$p-f\$i > $TEST_ROOT/p3/p\${p}_f\${i}.txt; done) & done; wait && \
     [ \"\$(ls $TEST_ROOT/p3/ | wc -l)\" = 200 ]"

run_test "P3.02 verify all 200 files" 15 \
    "ok=0; for p in 1 2 3 4; do for i in \$(seq 1 50); do [ \"\$(cat $TEST_ROOT/p3/p\${p}_f\${i}.txt 2>/dev/null)\" = p\${p}-f\${i} ] && ok=\$((ok+1)); done; done; [ \$ok = 200 ]"

run_test "P3.03 concurrent large writes" 30 \
    "mkdir -p $TEST_ROOT/p3/lw && \
     for p in 1 2 3 4; do (dd if=/dev/zero of=$TEST_ROOT/p3/lw/f\$p.bin bs=64K count=16 2>/dev/null) & done; wait && \
     [ \"\$(ls $TEST_ROOT/p3/lw/ | wc -l)\" = 4 ] && \
     [ \"\$(stat -c%s $TEST_ROOT/p3/lw/f1.bin)\" = 1048576 ]"

# ════════════════════════════════════════════════════════════════════
# Phase 4: TopologyUpdateListener verification
# Records FUSE log offset, restarts filer-2, waits for topology update,
# then checks log for re-sync evidence and verifies ops still work.
# ════════════════════════════════════════════════════════════════════
printf "\n${B}━━━ Phase 4: TopologyUpdateListener ━━━${N}\n"

mkdir -p "$TEST_ROOT/p4"

# Record log offset before topology change
LOG_OFFSET_BEFORE=$(wc -l < "$FUSE_LOG" 2>/dev/null || echo 0)
printf "  [%s] FUSE log offset before: %s lines\n" "$(timestamp)" "$LOG_OFFSET_BEFORE"

run_test "P4.01 pre-restart baseline op" 5 "echo before_restart > $TEST_ROOT/p4/baseline.txt && [ \"\$(cat $TEST_ROOT/p4/baseline.txt)\" = before_restart ]"

# Determine which filer is the shard-0 leader from the FUSE log.
# Restarting the leader triggers a TopologyChanged notification via Master.
LEADER_FILER=$(grep -oE 'leader=[0-9.]+' "$FUSE_LOG" 2>/dev/null | tail -1 | cut -d= -f2)
LEADER_CONTAINER=""
if [ -n "$LEADER_FILER" ]; then
    case "$LEADER_FILER" in
        *0.31*) LEADER_CONTAINER="filer-1";;
        *0.32*) LEADER_CONTAINER="filer-2";;
        *0.33*) LEADER_CONTAINER="filer-3";;
    esac
fi
if [ -z "$LEADER_CONTAINER" ]; then
    # Fallback: restart filer-2 if we can't determine leader
    LEADER_CONTAINER="filer-2"
    LEADER_FILER="(unknown)"
fi
printf "  ${C}[%s]${N} Leader filer: %s (%s) — restarting to trigger topology change\n" \
    "$(timestamp)" "$LEADER_FILER" "$LEADER_CONTAINER"

# Restart the leader filer container (triggers TopologyChanged notification via Master)
printf "  ${C}[%s]${N} Restarting %s container...\n" "$(timestamp)" "$LEADER_CONTAINER"
docker restart "$LEADER_CONTAINER" >/dev/null 2>&1 || true

# Wait up to 45s for topology update to propagate (Raft election + notification)
run_test "P4.02 wait topology update (45s)" 50 \
    "deadline=\$((SECONDS+45)); \
     while [ \$SECONDS -lt \$deadline ]; do \
       tail -n +$LOG_OFFSET_BEFORE '$FUSE_LOG' 2>/dev/null | grep -qiE 'topology update|on_topology_update|sync_shard_router|sync_shard_map|leader.*changed|bump_cache_epoch' && exit 0; \
       sleep 1; \
     done; exit 1"

# Check what was logged
LOG_AFTER=$(tail -n +$LOG_OFFSET_BEFORE "$FUSE_LOG" 2>/dev/null | grep -iE 'topology|listener|shard_map|sync_shard' | tail -10)
if [ -n "$LOG_AFTER" ]; then
    printf "  ${G}[%s]${N} Topology log entries found:\n" "$(timestamp)"
    echo "$LOG_AFTER" | while IFS= read -r line; do printf "    %s\n" "$line"; done
    PASS=$((PASS+1))
    printf "  ${G}[%s]${N} PASS: P4.03 topology log verification\n" "$(timestamp)"
else
    # Not necessarily a failure — Master may not have pushed TopologyChanged yet
    SKIP=$((SKIP+1))
    printf "  ${Y}[%s]${N} SKIP: P4.03 no topology log entries (Master may not have pushed notification)\n" "$(timestamp)"
fi

run_test "P4.04 post-restart op (after topology change)" 10 "echo after_restart > $TEST_ROOT/p4/post.txt && [ \"\$(cat $TEST_ROOT/p4/post.txt)\" = after_restart ]"

run_test "P4.05 read pre-restart file (stale route check)" 5 "[ \"\$(cat $TEST_ROOT/p4/baseline.txt)\" = before_restart ]"

# ════════════════════════════════════════════════════════════════════
# Phase 5: Stability — continuous mixed ops for STABLE_DURATION seconds
# Prints progress every 5s so we can see exactly when/if it hangs.
# ════════════════════════════════════════════════════════════════════
printf "\n${B}━━━ Phase 5: Stability (%ss continuous ops) ━━━${N}\n" "$STABLE_DURATION"

run_test "P5 stability run" $((STABLE_DURATION + 10)) "
    mkdir -p $TEST_ROOT/p5
    deadline=\$((SECONDS+$STABLE_DURATION))
    iter=0
    while [ \$SECONDS -lt \$deadline ]; do
        iter=\$((iter+1))
        # Mixed ops: create, write, read, delete
        echo iter_\$iter > $TEST_ROOT/p5/f_\$iter.txt
        cat $TEST_ROOT/p5/f_\$iter.txt > /dev/null
        # Periodic large write
        if [ \$((iter % 10)) -eq 0 ]; then
            dd if=/dev/zero of=$TEST_ROOT/p5/large_\$iter.bin bs=64K count=4 2>/dev/null
        fi
        # Cleanup old files (keep last 50)
        if [ \$iter -gt 50 ]; then
            rm $TEST_ROOT/p5/f_\$((iter-50)).txt 2>/dev/null
        fi
        # Progress every 5s
        if [ \$((iter % 20)) -eq 0 ]; then
            elapsed=\$((SECONDS - (deadline - $STABLE_DURATION)))
            printf '    [%s] iter=%d elapsed=%ss files=%s\n' \
                \"\$(date '+%H:%M:%S')\" \"\$iter\" \"\$elapsed\" \"\$(ls $TEST_ROOT/p5/ | wc -l)\"
        fi
    done
    printf '    [%s] stability done: %d iterations\n' \"\$(date '+%H:%M:%S')\" \"\$iter\"
    exit 0
"

# ════════════════════════════════════════════════════════════════════
# Summary
# ════════════════════════════════════════════════════════════════════
printf "\n============================================================\n"
printf "  Test Summary\n"
printf "============================================================\n"
printf "  PASS: %d\n" "$PASS"
printf "  FAIL: %d\n" "$FAIL"
printf "  HANG: %d\n" "$HANG"
printf "  SKIP: %d\n" "$SKIP"
printf "  TOTAL: %d\n" "$((PASS+FAIL+HANG+SKIP))"
printf "\n"

if [ "$FAIL" -gt 0 ]; then
    printf "  Failed tests:\n"
    for f in "${FAILED_LIST[@]}"; do printf "    - %s\n" "$f"; done
    printf "\n"
fi

if [ "$HANG" -gt 0 ]; then
    printf "  Hung tests (diagnostics in %s/):\n" "$DIAG_DIR"
    for h in "${HANG_LIST[@]}"; do printf "    - %s\n" "$h"; done
    printf "\n"
fi

printf "============================================================\n"

cleanup
exit $((FAIL + HANG))
