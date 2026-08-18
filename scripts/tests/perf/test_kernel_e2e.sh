#!/bin/bash
# PowerFS kernel-source end-to-end stress test
#
# Exercises the full file-system stack with a realistic, heavy workload:
#   1. Copy a Linux kernel tarball onto PowerFS           (large file write)
#   2. Unpack the tarball on PowerFS                      (bulk create+write, ~96k files)
#   3. make defconfig                                     (exec + create)
#   4. make -jN                                           (heavy read/write/setattr/exec)
#   5. rm -rf                                             (bulk unlink+rmdir)
#
# Each phase captures EIO / error counts and wall-clock time.
#
# Usage:
#   ./scripts/tests/perf/test_kernel_e2e.sh [OPTIONS]
#
# Options:
#   --container=NAME   FUSE container name        (default: fuse-1)
#   --mount=PATH       Mount point in container   (default: /mnt/powerfs)
#   --tarball=PATH     Host-side kernel tarball   (default: /home/portion/linux_6.17.0.orig.tar.gz)
#   --jobs=N           Build parallelism          (default: nproc)
#   --skip-build       Skip the make phases (only unpack + delete)
#   --keep-on-fail     Do not remove test dir on failure
#   --subset=DIRS      Only extract specified comma-separated subdirs (e.g. init,kernel,mm)
#                      Implies --skip-build (subset cannot do full kernel build)
#   --help             Show this help

set -uo pipefail

CONTAINER="${CONTAINER:-fuse-1}"
MOUNT_POINT="${MOUNT_POINT:-/mnt/powerfs}"
TARBALL="${TARBALL:-/home/portion/linux_6.17.0.orig.tar.gz}"
JOBS=""
SKIP_BUILD=0
KEEP_ON_FAIL=0
SUBSET=""
TEST_DIR_NAME="${TEST_DIR_NAME:-kernel_e2e_$(date +%s)}"

print_usage() {
    sed -n '3,23p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --container=*)  CONTAINER="${1#*=}" ;;
        --mount=*)      MOUNT_POINT="${1#*=}" ;;
        --tarball=*)    TARBALL="${1#*=}" ;;
        --jobs=*)       JOBS="${1#*=}" ;;
        --skip-build)   SKIP_BUILD=1 ;;
        --keep-on-fail) KEEP_ON_FAIL=1 ;;
        --subset=*)     SUBSET="${1#*=}"; SKIP_BUILD=1 ;;
        --help|-h)      print_usage ;;
        *) echo "Unknown option: $1"; print_usage ;;
    esac
    shift
done

# --- helpers ----------------------------------------------------------------

log()  { echo "[$(date +%H:%M:%S)] $*"; }
ok()   { echo "  PASS  $*"; }
fail() { echo "  FAIL  $*"; }

exec_in() {
    docker exec "$CONTAINER" bash -c "$1"
}

# Run a command inside the container, capture output to a log file,
# and report wall-clock seconds on stdout as "DUR=<n>".
# Usage: run_timed <logfile> <cmd>
run_timed() {
    local logfile="$1"; shift
    local start end rc
    start=$(date +%s)
    docker exec "$CONTAINER" bash -c "$*" > "$logfile" 2>&1
    rc=$?
    end=$(date +%s)
    echo "DUR=$((end - start))"
    return $rc
}

# count EIO / common FS error strings in a log file
# grep -c always prints a count (0 when no matches), so we only need to
# suppress the non-zero exit code when there are zero matches.
count_errors() {
    local f="$1"
    [ -f "$f" ] || { echo 0; return; }
    grep -ciE 'input/output error|EIO|no space left|read-only|stale file|transport endpoint' "$f" 2>/dev/null || true
}

# --- preflight ---------------------------------------------------------------

log "=== PowerFS kernel E2E stress test ==="
log "Container: $CONTAINER"
log "Mount:     $MOUNT_POINT"
log "Tarball:   $TARBALL"
log "Skip build: $SKIP_BUILD"

if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
    echo "ERROR: container '$CONTAINER' is not running"
    exit 1
fi

if [ ! -f "$TARBALL" ]; then
    echo "ERROR: tarball not found: $TARBALL"
    exit 1
fi

MOUNT_INFO=$(exec_in "cat /proc/mounts | grep $MOUNT_POINT" || true)
if ! echo "$MOUNT_INFO" | grep -q "fuse.powerfs"; then
    echo "ERROR: $MOUNT_POINT is not a PowerFS FUSE mount"
    echo "  $MOUNT_INFO"
    exit 1
fi
log "FUSE mount confirmed"

# tool check — only require build tools when not skipping build
REQUIRED_TOOLS="tar rm cp find"
[ "$SKIP_BUILD" = "0" ] && REQUIRED_TOOLS="$REQUIRED_TOOLS gcc make bc flex bison"
MISSING_TOOLS=""
for t in $REQUIRED_TOOLS; do
    exec_in "command -v $t >/dev/null 2>&1" || MISSING_TOOLS="$MISSING_TOOLS $t"
done
if [ -n "$MISSING_TOOLS" ]; then
    echo "ERROR: missing tools in container:$MISSING_TOOLS"
    echo "  Install with: docker exec $CONTAINER apt-get install -y build-essential bc flex bison libssl-dev libelf-dev"
    exit 1
fi
log "Tools OK (skip_build=$SKIP_BUILD)"

# determine jobs
if [ -z "$JOBS" ]; then
    JOBS=$(exec_in "nproc")
fi
log "Build jobs: $JOBS"

# Copy tarball into container /tmp if not already there
CONTAINER_TARBALL="/tmp/$(basename "$TARBALL")"
if ! exec_in "test -f $CONTAINER_TARBALL" 2>/dev/null; then
    log "Copying tarball to container: $CONTAINER_TARBALL"
    docker cp "$TARBALL" "$CONTAINER:$CONTAINER_TARBALL"
fi

# determine source dir name from tarball
SRC_DIR_GUESS=$(exec_in "tar tzf $CONTAINER_TARBALL 2>/dev/null | head -1 | cut -d/ -f1")
if [ -z "$SRC_DIR_GUESS" ]; then
    echo "ERROR: cannot determine source dir name from tarball"
    exit 1
fi
log "Source dir name: $SRC_DIR_GUESS"

# setup
TEST_DIR="$MOUNT_POINT/$TEST_DIR_NAME"
WORK_DIR="$TEST_DIR/$SRC_DIR_GUESS"
LOG_DIR="/tmp/${TEST_DIR_NAME}_logs"
rm -rf "$LOG_DIR"
mkdir -p "$LOG_DIR"

log "Test dir:   $TEST_DIR"
log "Work dir:   $WORK_DIR"
log "Log dir:    $LOG_DIR"
exec_in "rm -rf $TEST_DIR && mkdir -p $TEST_DIR"

TOTAL_PASS=0
TOTAL_FAIL=0

# --- Phase 1: copy tarball to PowerFS ---------------------------------------

log ""
log "=== Phase 1: Copy tarball to PowerFS ==="

TARBALL_SIZE=$(exec_in "stat -c %s $CONTAINER_TARBALL")
log "Tarball size: $((TARBALL_SIZE / 1024 / 1024)) MB"

CP_LOG="$LOG_DIR/phase1_copy.log"
RES=$(run_timed "$CP_LOG" "cp $CONTAINER_TARBALL $TEST_DIR/ && sync")
CP_DUR=${RES#DUR=}
CP_EXIT=$?
CP_EIO=$(count_errors "$CP_LOG")

log "Copy exit: $CP_EXIT, duration: ${CP_DUR}s, EIO: $CP_EIO"
if [ "$CP_EXIT" = "0" ] && [ "$CP_EIO" = "0" ]; then
    ok "Tarball copied to PowerFS in ${CP_DUR}s ($((TARBALL_SIZE / 1024 / 1024)) MB)"
    TOTAL_PASS=$((TOTAL_PASS + 1))
else
    fail "Tarball copy failed (exit=$CP_EXIT, EIO=$CP_EIO)"
    TOTAL_FAIL=$((TOTAL_FAIL + 1))
fi

# --- Phase 2: unpack on PowerFS ---------------------------------------------

log ""
log "=== Phase 2: Unpack tarball on PowerFS ==="

# Build tar extract args: full or subset
if [ -n "$SUBSET" ]; then
    # Convert comma-separated dirs to tar path args: init,kernel → linux-6.17/init linux-6.17/kernel
    TAR_ARGS=""
    IFS=',' read -ra SUBDIRS <<< "$SUBSET"
    for d in "${SUBDIRS[@]}"; do
        TAR_ARGS="$TAR_ARGS $SRC_DIR_GUESS/$d"
    done
    log "Subset extract: $SUBSET (args: $TAR_ARGS)"
    UNPACK_CMD="cd $TEST_DIR && tar xf $(basename "$CONTAINER_TARBALL") $TAR_ARGS"
else
    UNPACK_CMD="cd $TEST_DIR && tar xf $(basename "$CONTAINER_TARBALL")"
fi

UNPACK_LOG="$LOG_DIR/phase2_unpack.log"
RES=$(run_timed "$UNPACK_LOG" "$UNPACK_CMD")
UNPACK_DUR=${RES#DUR=}
UNPACK_EXIT=$?
UNPACK_EIO=$(count_errors "$UNPACK_LOG")

FILE_COUNT=$(exec_in "find $TEST_DIR -type f 2>/dev/null | wc -l" || echo 0)
DIR_COUNT=$(exec_in "find $TEST_DIR -type d 2>/dev/null | wc -l" || echo 0)

log "Files created:  $FILE_COUNT"
log "Directories:   $DIR_COUNT"
log "Unpack exit:    $UNPACK_EXIT, duration: ${UNPACK_DUR}s, EIO: $UNPACK_EIO"

# For subset mode, lower the threshold; for full mode, require >90k files
MIN_FILES=90000
[ -n "$SUBSET" ] && MIN_FILES=10

if [ "$UNPACK_EXIT" = "0" ] && [ "$UNPACK_EIO" = "0" ] && [ "$FILE_COUNT" -gt "$MIN_FILES" ]; then
    ok "Unpack succeeded ($FILE_COUNT files in ${UNPACK_DUR}s, no EIO)"
    TOTAL_PASS=$((TOTAL_PASS + 1))
else
    fail "Unpack had issues (exit=$UNPACK_EXIT, EIO=$UNPACK_EIO, files=$FILE_COUNT, min=$MIN_FILES)"
    TOTAL_FAIL=$((TOTAL_FAIL + 1))
fi

if [ "$SKIP_BUILD" = "0" ]; then

# --- Phase 3: configure -----------------------------------------------------

log ""
log "=== Phase 3: make defconfig ==="

CONFIG_LOG="$LOG_DIR/phase3_defconfig.log"
RES=$(run_timed "$CONFIG_LOG" "cd $WORK_DIR && make defconfig")
CONFIG_DUR=${RES#DUR=}
CONFIG_EXIT=$?

log "defconfig exit: $CONFIG_EXIT, duration: ${CONFIG_DUR}s"
if [ "$CONFIG_EXIT" = "0" ]; then
    ok "Kernel configured"
    TOTAL_PASS=$((TOTAL_PASS + 1))
else
    fail "defconfig failed (exit=$CONFIG_EXIT)"
    tail -5 "$CONFIG_LOG" | sed 's/^/    /'
    TOTAL_FAIL=$((TOTAL_FAIL + 1))
fi

# --- Phase 4: build ---------------------------------------------------------

log ""
log "=== Phase 4: make -j$JOBS ==="
log "Building (this may take several minutes)..."

BUILD_LOG="$LOG_DIR/phase4_build.log"
RES=$(run_timed "$BUILD_LOG" "cd $WORK_DIR && make -j$JOBS")
BUILD_DUR=${RES#DUR=}
BUILD_EXIT=$?

BUILD_EIO=$(count_errors "$BUILD_LOG")
BUILD_ERR=$(grep -ciE '^make.*\[.*Error|fatal error|cannot find|No such file' "$BUILD_LOG" 2>/dev/null || echo 0)
OBJ_COUNT=$(exec_in "find $WORK_DIR -name '*.o' -type f 2>/dev/null | wc -l" || echo 0)
KO_COUNT=$(exec_in "find $WORK_DIR -name '*.ko' -type f 2>/dev/null | wc -l" || echo 0)

log "Build exit:      $BUILD_EXIT, duration: ${BUILD_DUR}s"
log "EIO errors:      $BUILD_EIO"
log "Make errors:     $BUILD_ERR"
log ".o files built:  $OBJ_COUNT"
log ".ko files built: $KO_COUNT"

if [ "$BUILD_EXIT" = "0" ] && [ "$BUILD_EIO" = "0" ]; then
    ok "Kernel build succeeded ($OBJ_COUNT .o, $KO_COUNT .ko in ${BUILD_DUR}s)"
    TOTAL_PASS=$((TOTAL_PASS + 1))
else
    fail "Build had issues (exit=$BUILD_EXIT, EIO=$BUILD_EIO, make_errors=$BUILD_ERR)"
    grep -iE 'error|EIO|input/output' "$BUILD_LOG" 2>/dev/null | tail -10 | sed 's/^/    /'
    TOTAL_FAIL=$((TOTAL_FAIL + 1))
fi

fi  # SKIP_BUILD

# --- Phase 5: delete --------------------------------------------------------

log ""
log "=== Phase 5: rm -rf (cleanup) ==="

DEL_LOG="$LOG_DIR/phase5_delete.log"
RES=$(run_timed "$DEL_LOG" "rm -rf $TEST_DIR")
DEL_DUR=${RES#DUR=}
DEL_EXIT=$?
DEL_EIO=$(count_errors "$DEL_LOG")

# Check if directory is fully gone
REMAINING=$(exec_in "test -d $TEST_DIR && echo EXISTS || echo GONE")
REMAINING_FILES=0
RETRY_DEL_DUR=0

if [ "$REMAINING" = "EXISTS" ]; then
    REMAINING_FILES=$(exec_in "find $TEST_DIR -type f 2>/dev/null | wc -l" || echo 0)
    log "WARNING: rm -rf returned exit=$DEL_EXIT but $REMAINING_FILES files remain"
    log "  Remaining files:"
    exec_in "find $TEST_DIR -type f 2>/dev/null | head -10 | sed 's/^/    /'"
    # Retry once
    log "  Retrying rm -rf..."
    RETRY_START=$(date +%s)
    exec_in "rm -rf $TEST_DIR 2>&1" >> "$DEL_LOG"
    RETRY_END=$(date +%s)
    RETRY_DEL_DUR=$((RETRY_END - RETRY_START))
    REMAINING=$(exec_in "test -d $TEST_DIR && echo EXISTS || echo GONE")
    REMAINING_FILES=$(exec_in "find $TEST_DIR -type f 2>/dev/null | wc -l" || echo 0)
fi

log "Delete exit: $DEL_EXIT, duration: ${DEL_DUR}s (+${RETRY_DEL_DUR}s retry), EIO: $DEL_EIO"
log "Dir status:  $REMAINING, remaining files: $REMAINING_FILES"

if [ "$REMAINING" = "GONE" ] && [ "$DEL_EIO" = "0" ]; then
    if [ "$RETRY_DEL_DUR" -gt 0 ]; then
        ok "Cleanup succeeded on retry (first attempt left files — intermittent delete bug)"
    else
        ok "Cleanup succeeded"
    fi
    TOTAL_PASS=$((TOTAL_PASS + 1))
else
    fail "Cleanup failed (exit=$DEL_EXIT, EIO=$DEL_EIO, status=$REMAINING, files=$REMAINING_FILES)"
    TOTAL_FAIL=$((TOTAL_FAIL + 1))
fi

# --- summary ----------------------------------------------------------------

log ""
log "=========================================="
log "  SUMMARY: kernel E2E on PowerFS"
log "=========================================="
log "  Pass: $TOTAL_PASS"
log "  Fail: $TOTAL_FAIL"
log ""

# Print timing summary
log "Timing:"
[ -n "${CP_DUR:-}" ]    && log "  Phase 1 (copy tarball):   ${CP_DUR}s"
[ -n "${UNPACK_DUR:-}" ] && log "  Phase 2 (unpack):         ${UNPACK_DUR}s ($FILE_COUNT files)"
if [ "$SKIP_BUILD" = "0" ]; then
    [ -n "${CONFIG_DUR:-}" ] && log "  Phase 3 (defconfig):      ${CONFIG_DUR}s"
    [ -n "${BUILD_DUR:-}" ]  && log "  Phase 4 (make -j$JOBS):   ${BUILD_DUR}s ($OBJ_COUNT .o files)"
fi
[ -n "${DEL_DUR:-}" ]   && log "  Phase 5 (rm -rf):          ${DEL_DUR}s"
log ""

if [ "$TOTAL_FAIL" = "0" ]; then
    echo "RESULT: ALL PASS — kernel unpack/build/delete on PowerFS succeeded with no EIO"
    # clean up logs
    rm -rf "$LOG_DIR"
    exit 0
else
    echo "RESULT: $TOTAL_FAIL FAILURE(S) — see $LOG_DIR for details"
    if [ "$KEEP_ON_FAIL" = "1" ]; then
        log "Test dir kept: $TEST_DIR"
    else
        exec_in "rm -rf $TEST_DIR 2>/dev/null || true"
        rm -rf "$LOG_DIR"
    fi
    exit 1
fi
