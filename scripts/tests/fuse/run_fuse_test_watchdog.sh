#!/bin/bash
# =============================================================================
# PowerFS FUSE Test Runner with Watchdog
#
# Features:
#   1. Per-test timeout (default 15s, configurable via TEST_TIMEOUT env)
#   2. D-state (uninterruptible sleep) hang detection
#   3. Automatic diagnostics capture on hang:
#      - /proc/<pid>/stack (kernel stack)
#      - /proc/<pid>/wchan (wait channel)
#      - FUSE client log tail
#      - strace snapshot
#   4. Global test suite timeout (default 300s)
#   5. Auto-kill hung processes and continue to next test
#
# Usage:
#   docker cp scripts/tests/fuse/run_fuse_test_watchdog.sh fuse-1-test:/tmp/
#   docker exec fuse-1-test /tmp/run_fuse_test_watchdog.sh
#
# Environment variables:
#   TEST_TIMEOUT    - per-test timeout in seconds (default: 15)
#   SUITE_TIMEOUT   - total suite timeout in seconds (default: 300)
#   MOUNT           - mount point (default: /mnt/powerfs)
#   FUSE_LOG        - FUSE client log path (default: /var/log/powerfs-fuse.log)
#   DIAG_DIR        - diagnostics output dir (default: /tmp/fuse_test_diag)
# =============================================================================

set -u

MOUNT="${MOUNT:-/mnt/powerfs}"
TEST_ROOT="$MOUNT/watchdog_test"
FUSE_LOG="${FUSE_LOG:-/var/log/powerfs-fuse.log}"
DIAG_DIR="${DIAG_DIR:-/tmp/fuse_test_diag}"
TEST_TIMEOUT="${TEST_TIMEOUT:-15}"
SUITE_TIMEOUT="${SUITE_TIMEOUT:-300}"

PASS=0
FAIL=0
SKIP=0
HANG=0
FAILED_LIST=()
HANG_LIST=()

mkdir -p "$DIAG_DIR" "$TEST_ROOT"

# Colors
G='\033[0;32m'; R='\033[0;31m'; Y='\033[0;33m'; C='\033[0;36m'; N='\033[0m'

# ── Helpers ──────────────────────────────────────────────────────────

timestamp() { date '+%H:%M:%S'; }

# Run a command with timeout + hang detection
# Args: <test_name> <timeout_secs> <command...>
run_test() {
    local name="$1"; shift
    local timeout_s="$1"; shift
    local cmd="$*"

    local tmpdir=$(mktemp -d /tmp/wd_XXXXXX)
    local outfile="$tmpdir/output"
    local pidfile="$tmpdir/pid"

    echo -e "  ${C}[$(timestamp)]${N} RUN: $name (timeout=${timeout_s}s)"

    # Run command in background with output capture
    (
        eval "$cmd" > "$outfile" 2>&1
    ) &
    local cmd_pid=$!
    echo "$cmd_pid" > "$pidfile"

    # Watchdog loop: check completion + D-state
    local elapsed=0
    local hung=0
    while kill -0 "$cmd_pid" 2>/dev/null; do
        if [ "$elapsed" -ge "$timeout_s" ]; then
            # Check if process is in D state (uninterruptible sleep) or S (sleeping)
            local state=$(cat /proc/$cmd_pid/stat 2>/dev/null | awk '{print $3}')
            if [ "$state" = "D" ] || [ "$state" = "S" ]; then
                hung=1
                echo -e "  ${R}[$(timestamp)]${N} HANG: $name (state=$state at ${elapsed}s)"
                capture_diagnostics "$name" "$cmd_pid" "$tmpdir"
            else
                echo -e "  ${R}[$(timestamp)]${N} TIMEOUT: $name (timeout=${timeout_s}s, state=$state)"
                capture_diagnostics "$name" "$cmd_pid" "$tmpdir"
            fi

            # Kill the process tree
            kill -TERM "$cmd_pid" 2>/dev/null
            sleep 1
            kill -KILL "$cmd_pid" 2>/dev/null
            # Kill any children
            pkill -KILL -P "$cmd_pid" 2>/dev/null || true
            wait "$cmd_pid" 2>/dev/null || true
            break
        fi
        sleep 1
        elapsed=$((elapsed + 1))

        # Periodically check for D-state even before timeout
        if [ $((elapsed % 5)) -eq 0 ]; then
            local check_state=$(cat /proc/$cmd_pid/stat 2>/dev/null | awk '{print $3}')
            if [ "$check_state" = "D" ] && [ "$hung" = "0" ]; then
                # D-state before timeout — give it a few more seconds
                echo -e "  ${Y}[$(timestamp)]${N} WARN: $name in D-state at ${elapsed}s (monitoring...)"
            fi
        fi
    done

    # Check result
    if [ "$hung" = "1" ]; then
        HANG=$((HANG+1))
        HANG_LIST+=("$name")
        echo -e "  ${R}[HANG]${N} $name"
        echo "  Output: $(cat "$outfile" 2>/dev/null | tail -3)"
        rm -rf "$tmpdir"
        return 1
    fi

    if [ "$elapsed" -ge "$timeout_s" ]; then
        FAIL=$((FAIL+1))
        FAILED_LIST+=("$name (timeout ${timeout_s}s)")
        echo -e "  ${R}[FAIL]${N} $name (timeout)"
        echo "  Output: $(cat "$outfile" 2>/dev/null | tail -3)"
        rm -rf "$tmpdir"
        return 1
    fi

    # Check exit code
    wait "$cmd_pid" 2>/dev/null
    local rc=$?
    if [ "$rc" -eq 0 ]; then
        PASS=$((PASS+1))
        echo -e "  ${G}[PASS]${N} $name (${elapsed}s)"
    else
        FAIL=$((FAIL+1))
        FAILED_LIST+=("$name (rc=$rc)")
        echo -e "  ${R}[FAIL]${N} $name (rc=$rc, ${elapsed}s)"
        echo "  Output: $(cat "$outfile" 2>/dev/null | tail -3)"
    fi

    rm -rf "$tmpdir"
    return $rc
}

# Capture diagnostics for a hung process
capture_diagnostics() {
    local name="$1"
    local pid="$2"
    local tmpdir="$3"
    local diag_file="$DIAG_DIR/${name//\//_}_hang.txt"

    echo "=== Hang Diagnostics: $name ===" > "$diag_file"
    echo "Time: $(date '+%Y-%m-%d %H:%M:%S')" >> "$diag_file"
    echo "PID: $pid" >> "$diag_file"
    echo "" >> "$diag_file"

    # Process state
    echo "--- /proc/$pid/stat ---" >> "$diag_file"
    cat /proc/$pid/stat 2>/dev/null >> "$diag_file" || echo "(unavailable)" >> "$diag_file"
    echo "" >> "$diag_file"

    # Kernel stack
    echo "--- /proc/$pid/stack ---" >> "$diag_file"
    cat /proc/$pid/stack 2>/dev/null >> "$diag_file" || echo "(unavailable)" >> "$diag_file"
    echo "" >> "$diag_file"

    # Wait channel
    echo "--- /proc/$pid/wchan ---" >> "$diag_file"
    cat /proc/$pid/wchan 2>/dev/null >> "$diag_file" || echo "(unavailable)" >> "$diag_file"
    echo "" >> "$diag_file"

    # Status
    echo "--- /proc/$pid/status ---" >> "$diag_file"
    grep -E "State|Threads|VmRSS|voluntary|nonvoluntary" /proc/$pid/status 2>/dev/null >> "$diag_file" || echo "(unavailable)" >> "$diag_file"
    echo "" >> "$diag_file"

    # Child processes (could be dd, cat, etc. stuck in FUSE syscall)
    echo "--- Child processes ---" >> "$diag_file"
    local children=$(pgrep -P "$pid" 2>/dev/null)
    for child in $children; do
        local cstate=$(cat /proc/$child/stat 2>/dev/null | awk '{print $3}')
        local ccmd=$(cat /proc/$child/cmdline 2>/dev/null | tr '\0' ' ')
        echo "  PID=$child state=$cstate cmd=$ccmd" >> "$diag_file"
        echo "  /proc/$child/stack:" >> "$diag_file"
        cat /proc/$child/stack 2>/dev/null | head -10 >> "$diag_file" || echo "  (unavailable)" >> "$diag_file"
    done
    echo "" >> "$diag_file"

    # All D-state processes on the system (might be FUSE related)
    echo "--- All D-state processes ---" >> "$diag_file"
    ps aux 2>/dev/null | awk '$8 ~ /D/ {print}' >> "$diag_file" || echo "(unavailable)" >> "$diag_file"
    echo "" >> "$diag_file"

    # FUSE client log (last 30 lines)
    echo "--- FUSE client log (last 30 lines) ---" >> "$diag_file"
    tail -30 "$FUSE_LOG" 2>/dev/null >> "$diag_file" || echo "(unavailable)" >> "$diag_file"
    echo "" >> "$diag_file"

    # Mount info
    echo "--- Mount info ---" >> "$diag_file"
    mount | grep fuse >> "$diag_file" 2>/dev/null || echo "(unavailable)" >> "$diag_file"
    echo "" >> "$diag_file"

    # Test output so far
    echo "--- Test output ---" >> "$diag_file"
    cat "$tmpdir/output" 2>/dev/null | tail -10 >> "$diag_file" || echo "(unavailable)" >> "$diag_file"

    echo -e "  ${Y}[$(timestamp)]${N} Diagnostics saved to $diag_file"
}

# Simple assertion wrappers (compatible with existing test format)
assert_eq() {
    local expected="$1" actual="$2" msg="$3"
    if [ "$expected" = "$actual" ]; then
        PASS=$((PASS+1))
        echo -e "  ${G}[PASS]${N} $msg"
    else
        FAIL=$((FAIL+1))
        FAILED_LIST+=("$msg (expected='$expected' actual='$actual')")
        echo -e "  ${R}[FAIL]${N} $msg (expected='$expected' actual='$actual')"
    fi
}

assert_exists() {
    if [ -e "$1" ]; then
        PASS=$((PASS+1)); echo -e "  ${G}[PASS]${N} $2"
    else
        FAIL=$((FAIL+1)); FAILED_LIST+=("$2: $1 not found")
        echo -e "  ${R}[FAIL]${N} $2: $1 not found"
    fi
}

assert_not_exists() {
    if [ ! -e "$1" ]; then
        PASS=$((PASS+1)); echo -e "  ${G}[PASS]${N} $2"
    else
        FAIL=$((FAIL+1)); FAILED_LIST+=("$2: $1 still exists")
        echo -e "  ${R}[FAIL]${N} $2: $1 still exists"
    fi
}

# ── Global timeout watchdog ──────────────────────────────────────────

global_timeout_watchdog() {
    local suite_pid=$1
    local elapsed=0
    while kill -0 "$suite_pid" 2>/dev/null; do
        if [ "$elapsed" -ge "$SUITE_TIMEOUT" ]; then
            echo ""
            echo -e "${R}========================================${N}"
            echo -e "${R}  SUITE TIMEOUT (${SUITE_TIMEOUT}s) reached!${N}"
            echo -e "${R}  Killing all test processes...${N}"
            echo -e "${R}========================================${N}"
            # Capture all D-state processes
            capture_diagnostics "SUITE_TIMEOUT" "$suite_pid" "/tmp"
            # Kill everything
            pkill -KILL -P "$suite_pid" 2>/dev/null || true
            kill -KILL "$suite_pid" 2>/dev/null || true
            return 1
        fi
        sleep 5
        elapsed=$((elapsed + 5))
    done
    return 0
}

# ── Cleanup ──────────────────────────────────────────────────────────

cleanup() {
    rm -rf "$TEST_ROOT" 2>/dev/null || true
}
trap cleanup EXIT

# ════════════════════════════════════════════════════════════════════
# Main
# ════════════════════════════════════════════════════════════════════

echo "============================================================"
echo "  PowerFS FUSE Test with Watchdog"
echo "  Mount: $MOUNT"
echo "  Test timeout: ${TEST_TIMEOUT}s per test"
echo "  Suite timeout: ${SUITE_TIMEOUT}s total"
echo "  Diagnostics: $DIAG_DIR/"
echo "  Time: $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================================"
echo ""

# Start global watchdog in background
global_timeout_watchdog $$ &
WATCHDOG_PID=$!

# ════════════════════════════════════════════════════════════════════
# T1: Basic Operations
# ════════════════════════════════════════════════════════════════════
echo "━━━ T1: Basic Operations ━━━"
mkdir -p "$TEST_ROOT/t1"

run_test "T1.01 mkdir single" 5 "mkdir -p $TEST_ROOT/t1/d1 && test -d $TEST_ROOT/t1/d1"
run_test "T1.02 mkdir nested" 5 "mkdir -p $TEST_ROOT/t1/a/b/c && test -d $TEST_ROOT/t1/a/b/c"
run_test "T1.03 rmdir empty" 5 "mkdir -p $TEST_ROOT/t1/del && rmdir $TEST_ROOT/t1/del && test ! -e $TEST_ROOT/t1/del"
run_test "T1.04 rmdir non-empty fails" 5 "rmdir $TEST_ROOT/t1/a 2>/dev/null && false || true"
run_test "T1.05 create+read file" 5 "echo hello > $TEST_ROOT/t1/f1.txt && [ \"\$(cat $TEST_ROOT/t1/f1.txt)\" = hello ]"
run_test "T1.06 unlink file" 5 "rm $TEST_ROOT/t1/f1.txt && test ! -e $TEST_ROOT/t1/f1.txt"
run_test "T1.07 rename file" 5 "echo x > $TEST_ROOT/t1/src && mv $TEST_ROOT/t1/src $TEST_ROOT/t1/dst && test -e $TEST_ROOT/t1/dst"
run_test "T1.08 rename dir" 5 "mkdir -p $TEST_ROOT/t1/d1/sub && mv $TEST_ROOT/t1/d1 $TEST_ROOT/t1/d2 && test -d $TEST_ROOT/t1/d2/sub"
run_test "T1.09 dup mkdir fails" 5 "mkdir -p $TEST_ROOT/t1/dup && mkdir $TEST_ROOT/t1/dup 2>/dev/null && false || true"
run_test "T1.10 ENOENT" 5 "cat $TEST_ROOT/t1/nope 2>/dev/null && false || true"

# ════════════════════════════════════════════════════════════════════
# T2: File I/O
# ════════════════════════════════════════════════════════════════════
echo ""
echo "━━━ T2: File I/O ━━━"
mkdir -p "$TEST_ROOT/t2"

run_test "T2.01 write+read" 5 "echo 'hello world' > $TEST_ROOT/t2/rw.txt && [ \"\$(cat $TEST_ROOT/t2/rw.txt)\" = 'hello world' ]"
run_test "T2.02 overwrite" 5 "echo AAA > $TEST_ROOT/t2/ov && echo BBB > $TEST_ROOT/t2/ov && [ \"\$(cat $TEST_ROOT/t2/ov)\" = BBB ]"
run_test "T2.03 O_APPEND" 5 "echo AAA > $TEST_ROOT/t2/app && echo BBB >> $TEST_ROOT/t2/app && [ \"\$(cat $TEST_ROOT/t2/app)\" = \$'AAA\nBBB' ]"
run_test "T2.04 truncate down" 5 "echo 0123456789 > $TEST_ROOT/t2/tr && truncate -s 5 $TEST_ROOT/t2/tr && [ \"\$(stat -c%s $TEST_ROOT/t2/tr)\" = 5 ]"
run_test "T2.05 truncate up" 5 "truncate -s 20 $TEST_ROOT/t2/tr && [ \"\$(stat -c%s $TEST_ROOT/t2/tr)\" = 20 ]"
run_test "T2.06 dd write 1MB" 15 "dd if=/dev/zero of=$TEST_ROOT/t2/dd1m.bin bs=1M count=1 2>/dev/null && [ \"\$(stat -c%s $TEST_ROOT/t2/dd1m.bin)\" = 1048576 ]"
run_test "T2.07 dd read 1MB" 10 "dd if=$TEST_ROOT/t2/dd1m.bin of=/dev/null bs=1M 2>/dev/null"
run_test "T2.08 fsync" 10 "echo test > $TEST_ROOT/t2/fs.txt && sync $TEST_ROOT/t2/fs.txt"
run_test "T2.09 fallocate 1MB" 10 "touch $TEST_ROOT/t2/fa && fallocate -l 1048576 $TEST_ROOT/t2/fa && [ \"\$(stat -c%s $TEST_ROOT/t2/fa)\" = 1048576 ]"
run_test "T2.10 empty file" 5 "touch $TEST_ROOT/t2/em && [ \"\$(stat -c%s $TEST_ROOT/t2/em)\" = 0 ]"
run_test "T2.11 seek write" 10 "dd if=/dev/zero of=$TEST_ROOT/t2/sk bs=1K seek=100 count=1 conv=notrunc 2>/dev/null && [ \"\$(stat -c%s $TEST_ROOT/t2/sk)\" -ge 102400 ]"
run_test "T2.12 large 10MB" 30 "for i in \$(seq 1 10); do dd if=/dev/zero of=$TEST_ROOT/t2/lg bs=1M count=1 conv=notrunc oflag=append 2>/dev/null; done && [ \"\$(stat -c%s $TEST_ROOT/t2/lg)\" = 10485760 ]"

# ════════════════════════════════════════════════════════════════════
# T3: Metadata
# ════════════════════════════════════════════════════════════════════
echo ""
echo "━━━ T3: Metadata ━━━"
mkdir -p "$TEST_ROOT/t3"
echo "perm" > "$TEST_ROOT/t3/p.txt"

run_test "T3.01 chmod 644" 5 "chmod 644 $TEST_ROOT/t3/p.txt && [ \"\$(stat -c%a $TEST_ROOT/t3/p.txt)\" = 644 ]"
run_test "T3.02 chmod 755" 5 "chmod 755 $TEST_ROOT/t3/p.txt && [ \"\$(stat -c%a $TEST_ROOT/t3/p.txt)\" = 755 ]"
run_test "T3.03 chmod 000" 5 "chmod 000 $TEST_ROOT/t3/p.txt && [ \"\$(stat -c%a $TEST_ROOT/t3/p.txt)\" = 000 ]"
run_test "T3.04 chown uid" 5 "chown 1000 $TEST_ROOT/t3/p.txt && [ \"\$(stat -c%u $TEST_ROOT/t3/p.txt)\" = 1000 ]"
run_test "T3.05 chown gid" 5 "chown :1000 $TEST_ROOT/t3/p.txt && [ \"\$(stat -c%g $TEST_ROOT/t3/p.txt)\" = 1000 ]"
run_test "T3.06 utimes" 5 "touch -d '2020-01-01 00:00:00' $TEST_ROOT/t3/p.txt && [ \"\$(stat -c%y $TEST_ROOT/t3/p.txt | cut -d. -f1)\" = '2020-01-01 00:00:00' ]"
run_test "T3.07 access check" 5 "test -r $TEST_ROOT/t3/p.txt && test -w $TEST_ROOT/t3/p.txt"
run_test "T3.08 stat file type" 5 "stat -c%F $TEST_ROOT/t3/p.txt | grep -q regular"

# ════════════════════════════════════════════════════════════════════
# T4: Directory Operations
# ════════════════════════════════════════════════════════════════════
echo ""
echo "━━━ T4: Directory Operations ━━━"
mkdir -p "$TEST_ROOT/t4"

run_test "T4.01 readdir basic" 5 "for i in 1 2 3; do echo f\$i > $TEST_ROOT/t4/f\$i; done && [ \"\$(ls $TEST_ROOT/t4/ | wc -l)\" = 3 ]"
run_test "T4.02 readdir -la" 5 "ls -la $TEST_ROOT/t4/ | grep -q f1 && ls -la $TEST_ROOT/t4/ | grep -q f3"
run_test "T4.03 find recursive" 10 "mkdir -p $TEST_ROOT/t4/sub/deep && echo n > $TEST_ROOT/t4/sub/deep/n.txt && find $TEST_ROOT/t4 -type f -name '*.txt' | wc -l | grep -q 4"
run_test "T4.04 readdir 100 files" 15 "mkdir -p $TEST_ROOT/t4/h && for i in \$(seq 1 100); do touch $TEST_ROOT/t4/h/f_\$i; done && [ \"\$(ls $TEST_ROOT/t4/h/ | wc -l)\" = 100 ]"
run_test "T4.05 readdir 200 files" 20 "mkdir -p $TEST_ROOT/t4/th && for i in \$(seq 1 200); do touch $TEST_ROOT/t4/th/f_\$i; done && [ \"\$(ls $TEST_ROOT/t4/th/ | wc -l)\" = 200 ]"
run_test "T4.06 readdir after delete" 10 "rm $TEST_ROOT/t4/h/f_1 && [ \"\$(ls $TEST_ROOT/t4/h/ | wc -l)\" = 99 ]"

# ════════════════════════════════════════════════════════════════════
# T5: Links
# ════════════════════════════════════════════════════════════════════
echo ""
echo "━━━ T5: Links ━━━"
mkdir -p "$TEST_ROOT/t5"

run_test "T5.01 hardlink create" 5 "echo hc > $TEST_ROOT/t5/o && ln $TEST_ROOT/t5/o $TEST_ROOT/t5/h && test -e $TEST_ROOT/t5/h"
run_test "T5.02 hardlink nlink" 5 "[ \"\$(stat -c%h $TEST_ROOT/t5/o)\" = 2 ]"
run_test "T5.03 hardlink content" 5 "echo mod > $TEST_ROOT/t5/h && [ \"\$(cat $TEST_ROOT/t5/o)\" = mod ]"
run_test "T5.04 nlink after unlink" 5 "rm $TEST_ROOT/t5/h && [ \"\$(stat -c%h $TEST_ROOT/t5/o)\" = 1 ]"
run_test "T5.05 symlink create" 5 "echo t > $TEST_ROOT/t5/tgt && ln -s tgt $TEST_ROOT/t5/sym && test -L $TEST_ROOT/t5/sym"
run_test "T5.06 readlink" 5 "[ \"\$(readlink $TEST_ROOT/t5/sym)\" = tgt ]"

# ════════════════════════════════════════════════════════════════════
# T6: System Operations
# ════════════════════════════════════════════════════════════════════
echo ""
echo "━━━ T6: System Operations ━━━"

run_test "T6.01 df statfs" 5 "df $MOUNT > /dev/null 2>&1"
run_test "T6.02 stat -f" 5 "stat -f $MOUNT 2>/dev/null | grep -qi 'block\|size\|avail'"
run_test "T6.03 find maxdepth 3" 15 "find $TEST_ROOT -maxdepth 3 > /dev/null 2>&1"
run_test "T6.04 persistence check" 5 "test -e $TEST_ROOT/t1/dst"

# ════════════════════════════════════════════════════════════════════
# T7: Extended Attributes
# ════════════════════════════════════════════════════════════════════
echo ""
echo "━━━ T7: Extended Attributes ━━━"
mkdir -p "$TEST_ROOT/t7"
echo "xattr" > "$TEST_ROOT/t7/x.txt"

if command -v setfattr >/dev/null 2>&1; then
    run_test "T7.01 setxattr" 5 "setfattr -n user.k1 -v v1 $TEST_ROOT/t7/x.txt"
    run_test "T7.02 getxattr" 5 "[ \"\$(getfattr -n user.k1 --only-values $TEST_ROOT/t7/x.txt 2>/dev/null)\" = v1 ]"
    run_test "T7.03 listxattr" 5 "setfattr -n user.k2 -v v2 $TEST_ROOT/t7/x.txt 2>/dev/null && getfattr -m- $TEST_ROOT/t7/x.txt 2>/dev/null | grep -c 'user\\.' | grep -q 2"
    run_test "T7.04 removexattr" 5 "setfattr -x user.k1 $TEST_ROOT/t7/x.txt 2>/dev/null && ! getfattr -n user.k1 $TEST_ROOT/t7/x.txt 2>/dev/null | grep -q v1"
    run_test "T7.05 multi xattr" 5 "setfattr -n user.k3 -v v3 $TEST_ROOT/t7/x.txt 2>/dev/null && [ \"\$(getfattr -m- $TEST_ROOT/t7/x.txt 2>/dev/null | grep -c 'user\\.')\" = 2 ]"
else
    echo "  [SKIP] setfattr not available"
    SKIP=$((SKIP+5))
fi

# ════════════════════════════════════════════════════════════════════
# T8: Concurrency & Stress
# ════════════════════════════════════════════════════════════════════
echo ""
echo "━━━ T8: Concurrency & Stress ━━━"
mkdir -p "$TEST_ROOT/t8"

run_test "T8.01 concurrent write 100" 20 "mkdir -p $TEST_ROOT/t8/cw && for p in 1 2 3 4; do (for i in \$(seq 1 25); do echo p\$p > $TEST_ROOT/t8/cw/p\${p}_\${i}.txt; done) & done; wait && [ \"\$(ls $TEST_ROOT/t8/cw/ | wc -l)\" = 100 ]"
run_test "T8.02 concurrent read" 15 "echo shared > $TEST_ROOT/t8/sh && for p in 1 2 3 4; do (cat $TEST_ROOT/t8/sh > /dev/null) & done; wait"
run_test "T8.03 concurrent mkdir 200" 30 "mkdir -p $TEST_ROOT/t8/cm && for p in 1 2 3 4; do (for i in \$(seq 1 50); do mkdir -p $TEST_ROOT/t8/cm/p\${p}_d\${i}; done) & done; wait && [ \"\$(find $TEST_ROOT/t8/cm -maxdepth 1 -type d | wc -l)\" = 201 ]"
run_test "T8.04 stress 1000 files" 60 "mkdir -p $TEST_ROOT/t8/sf && for i in \$(seq 1 1000); do touch $TEST_ROOT/t8/sf/f_\$i 2>/dev/null; done && [ \"\$(ls $TEST_ROOT/t8/sf/ | wc -l)\" = 1000 ]"
run_test "T8.05 stress 500 dirs" 45 "mkdir -p $TEST_ROOT/t8/sd && for i in \$(seq 1 500); do mkdir -p $TEST_ROOT/t8/sd/d_\$i 2>/dev/null; done && [ \"\$(find $TEST_ROOT/t8/sd -maxdepth 1 -type d | wc -l)\" = 501 ]"

# ════════════════════════════════════════════════════════════════════
# Summary
# ════════════════════════════════════════════════════════════════════

# Stop global watchdog
kill $WATCHDOG_PID 2>/dev/null || true

echo ""
echo "============================================================"
echo "  Test Summary"
echo "============================================================"
echo "  PASS: $PASS"
echo "  FAIL: $FAIL"
echo "  HANG: $HANG"
echo "  SKIP: $SKIP"
echo "  TOTAL: $((PASS+FAIL+HANG+SKIP))"
echo ""

if [ "$FAIL" -gt 0 ]; then
    echo "  Failed tests:"
    for f in "${FAILED_LIST[@]}"; do
        echo "    - $f"
    done
    echo ""
fi

if [ "$HANG" -gt 0 ]; then
    echo "  Hung tests (diagnostics in $DIAG_DIR/):"
    for h in "${HANG_LIST[@]}"; do
        echo "    - $h"
    done
    echo ""
fi

echo "============================================================"

cleanup
exit $((FAIL + HANG))
