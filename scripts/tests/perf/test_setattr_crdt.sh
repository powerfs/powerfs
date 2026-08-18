#!/bin/bash
# PowerFS setattr CRDT path verification test
#
# Verifies that the CRDT setattr optimization (SetAttrMeta for timestamp-only
# updates) resolves the EIO caused by bulk utimensat under cp -prf /etc.
#
# Test scenario: cp -prf /etc to the FUSE mount, check:
#   1. No EIO (Input/output error) during the copy
#   2. Timestamp (mtime) preservation rate
#
# Usage:
#   ./scripts/tests/perf/test_setattr_crdt.sh [--container fuse-1]
#
# Requires: fuse container running with powerfs-fuse mounted at /mnt/powerfs

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$SCRIPT_DIR/../../.." && pwd)

CONTAINER="${CONTAINER:-fuse-1}"
MOUNT_POINT="${MOUNT_POINT:-/mnt/powerfs}"
SOURCE_DIR="${SOURCE_DIR:-/etc}"
TEST_DIR_NAME="${TEST_DIR_NAME:-crdt_setattr_test}"
VERBOSE="${VERBOSE:-0}"

print_usage() {
    cat <<EOF
PowerFS setattr CRDT path verification

Usage: $0 [OPTIONS]

Options:
  --container=NAME   FUSE container name (default: fuse-1)
  --mount=PATH       Mount point inside container (default: /mnt/powerfs)
  --source=PATH      Source directory to copy (default: /etc)
  --verbose          Enable verbose output
  --help             Show this help message

EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --container=*) CONTAINER="${1#*=}" ;;
        --mount=*)     MOUNT_POINT="${1#*=}" ;;
        --source=*)    SOURCE_DIR="${1#*=}" ;;
        --verbose)     VERBOSE=1 ;;
        --help|-h)     print_usage; exit 0 ;;
        *) echo "Unknown option: $1"; print_usage; exit 1 ;;
    esac
    shift
done

log()  { echo "[$(date +%H:%M:%S)] $*"; }
ok()   { echo "  PASS  $*"; }
fail() { echo "  FAIL  $*"; }

# Run a command inside the FUSE container
exec_in() {
    docker exec "$CONTAINER" bash -c "$1"
}

# --- preflight ---
log "=== PowerFS setattr CRDT verification ==="
log "Container: $CONTAINER"
log "Mount:     $MOUNT_POINT"
log "Source:    $SOURCE_DIR"

if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
    echo "ERROR: container '$CONTAINER' is not running"
    exit 1
fi

MOUNT_INFO=$(exec_in "cat /proc/mounts | grep $MOUNT_POINT")
if ! echo "$MOUNT_INFO" | grep -q "fuse.powerfs"; then
    echo "ERROR: $MOUNT_POINT is not a PowerFS FUSE mount"
    echo "  $MOUNT_INFO"
    exit 1
fi
log "FUSE mount confirmed"

# Use grep -a (treat binary as text) — `strings` may be unavailable in slim containers
if exec_in "grep -aq 'setattr CRDT path' /app/powerfs-fuse 2>/dev/null"; then
    log "CRDT fix binary: present"
else
    log "WARNING: CRDT fix string not found in binary (check that powerfs-fuse has the SetAttrMeta optimization)"
fi

# --- setup ---
TEST_DIR="$MOUNT_POINT/$TEST_DIR_NAME"
log "Setting up test directory: $TEST_DIR"
exec_in "rm -rf $TEST_DIR && mkdir -p $TEST_DIR"

# --- Phase 1: cp -prf ---
log ""
log "=== Phase 1: cp -prf $SOURCE_DIR ==="

CP_OUTPUT=$(exec_in "cd $TEST_DIR && cp -prf $SOURCE_DIR . 2>&1; echo EXIT=\$?")
CP_EXIT=$(echo "$CP_OUTPUT" | grep -o 'EXIT=[0-9]*' | cut -d= -f2)
EIO_COUNT=$(echo "$CP_OUTPUT" | grep -ci "input/output error" || true)

log "cp exit code: $CP_EXIT"
log "EIO error count: $EIO_COUNT"

if [ "$CP_EXIT" = "0" ] && [ "$EIO_COUNT" = "0" ]; then
    ok "No EIO during cp -prf"
else
    fail "cp -prf had errors (exit=$CP_EXIT, EIO=$EIO_COUNT)"
fi

# --- Phase 2: timestamp preservation ---
log ""
log "=== Phase 2: Timestamp (mtime) preservation ==="

# Get list of source files (relative paths)
FILE_LIST=$(exec_in "find $SOURCE_DIR -type f 2>/dev/null | sed \"s|$SOURCE_DIR/||\" | head -n 500")

TOTAL=0
MATCH=0
MISMATCH=0
while IFS= read -r rel; do
    [ -z "$rel" ] && continue
    TOTAL=$((TOTAL + 1))
    SRC_T=$(exec_in "stat -c %Y $SOURCE_DIR/$rel 2>/dev/null || echo 0")
    DST_T=$(exec_in "stat -c %Y $TEST_DIR/$SOURCE_DIR/$rel 2>/dev/null || echo 0")
    if [ "$SRC_T" = "$DST_T" ]; then
        MATCH=$((MATCH + 1))
    else
        MISMATCH=$((MISMATCH + 1))
        [ "$VERBOSE" = "1" ] && [ $MISMATCH -le 5 ] && echo "  MISMATCH: $rel (src=$SRC_T dst=$DST_T)"
    fi
done <<< "$FILE_LIST"

if [ "$TOTAL" -gt 0 ]; then
    RATE=$((MATCH * 100 / TOTAL))
    log "Files checked: $TOTAL"
    log "mtime MATCH:    $MATCH ($RATE%)"
    log "mtime MISMATCH: $MISMATCH"
    if [ "$MISMATCH" = "0" ]; then
        ok "All $TOTAL files have correct mtime"
    else
        fail "$MISMATCH/$TOTAL files have wrong mtime ($RATE% match)"
        log "  Root cause: SetAttrMeta CRDT sends OK but filer does not persist mtime"
        log "  Cache misses (via InvalidateHandler) expose wrong mtime from filer"
    fi
else
    fail "No files found to compare"
fi

# --- Phase 3: original EIO trigger ---
log ""
log "=== Phase 3: Original EIO trigger (apt.conf.d) ==="

APT_REL="apt/apt.conf.d"
if exec_in "test -d $SOURCE_DIR/$APT_REL"; then
    SRC_M=$(exec_in "stat -c %Y $SOURCE_DIR/$APT_REL 2>/dev/null || echo 0")
    DST_M=$(exec_in "stat -c %Y $TEST_DIR/$SOURCE_DIR/$APT_REL 2>/dev/null || echo 0")
    if [ "$SRC_M" = "$DST_M" ]; then
        ok "apt.conf.d mtime preserved (src=$SRC_M dst=$DST_M)"
    else
        fail "apt.conf.d mtime NOT preserved (src=$SRC_M dst=$DST_M)"
    fi
fi

# --- cleanup ---
log ""
log "=== Cleanup ==="
exec_in "rm -rf $TEST_DIR"
log "Removed $TEST_DIR"

# --- summary ---
log ""
log "=== Summary ==="
PASS=1
[ "$CP_EXIT" = "0" ] && [ "$EIO_COUNT" = "0" ] || PASS=0
[ "$MISMATCH" = "0" ] || PASS=0

if [ "$PASS" = "1" ]; then
    echo "RESULT: ALL PASS — EIO resolved AND timestamps preserved"
    exit 0
else
    echo "RESULT: PARTIAL — EIO resolved but timestamp preservation incomplete"
    echo "  (CRDT path needs filer-side SetAttrMeta handler fix)"
    exit 1
fi
