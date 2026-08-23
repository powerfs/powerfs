#!/bin/bash
# PowerFS FUSE Feature Gap Tests
#
# Tests implemented features (xattr, fallocate, mknod) for correctness.
#
# Usage:
#   docker exec fuse-1-test /tmp/test_fuse_features.sh
#
# Expected results:
#   xattr:        PASS (FUSE implements setxattr/getxattr/listxattr/removexattr)
#   fallocate:    PASS (FUSE implements fallocate with size sync to Filer)
#   mknod:        PASS (FUSE implements mknod for fifo/block/char devices)

set -e

MOUNT="${MOUNT_DIR:-/mnt/powerfs}"
TEST_DIR="$MOUNT/feature_gap_test"
PASS=0
FAIL=0
SKIP=0

rm -rf "$TEST_DIR" 2>/dev/null || true
mkdir -p "$TEST_DIR"

# Colors
G='\033[0;32m'; R='\033[0;31m'; Y='\033[0;33m'; N='\033[0m'

assert_pass() {
    local name="$1"
    local cmd="$2"
    if eval "$cmd" >/dev/null 2>&1; then
        echo -e "  ${G}[PASS]${N} $name"
        PASS=$((PASS + 1))
    else
        echo -e "  ${R}[FAIL]${N} $name"
        FAIL=$((FAIL + 1))
    fi
}

assert_fail() {
    local name="$1"
    local cmd="$2"
    if eval "$cmd" >/dev/null 2>&1; then
        echo -e "  ${R}[FAIL]${N} $name (expected failure but succeeded)"
        FAIL=$((FAIL + 1))
    else
        echo -e "  ${G}[PASS]${N} $name (correctly returned error)"
        PASS=$((PASS + 1))
    fi
}

assert_eq() {
    local name="$1"
    local expected="$2"
    local actual="$3"
    if [ "$expected" = "$actual" ]; then
        echo -e "  ${G}[PASS]${N} $name"
        PASS=$((PASS + 1))
    else
        echo -e "  ${R}[FAIL]${N} $name (expected '$expected', got '$actual')"
        FAIL=$((FAIL + 1))
    fi
}

echo "========================================"
echo "  PowerFS FUSE Feature Gap Tests"
echo "========================================"
echo ""

# ── xattr tests (FUSE implements these) ──
echo "--- xattr tests ---"

# setxattr
assert_pass "setxattr user.test" \
    "setfattr -n user.test -v hello '$TEST_DIR/xattr_file' 2>/dev/null || { touch '$TEST_DIR/xattr_file'; setfattr -n user.test -v hello '$TEST_DIR/xattr_file'; }"

# Create file first if setfattr failed
touch "$TEST_DIR/xattr_file" 2>/dev/null || true
setfattr -n user.test -v hello "$TEST_DIR/xattr_file" 2>/dev/null || true

# getxattr
GETVAL=$(getfattr -n user.test --only-values "$TEST_DIR/xattr_file" 2>/dev/null || echo "")
assert_eq "getxattr user.test" "hello" "$GETVAL"

# listxattr
setfattr -n user.second -v world "$TEST_DIR/xattr_file" 2>/dev/null || true
LISTOUT=$(getfattr -m- "$TEST_DIR/xattr_file" 2>/dev/null | grep -c "user\." || echo "0")
if [ "$LISTOUT" -ge 2 ]; then
    echo -e "  ${G}[PASS]${N} listxattr (found $LISTOUT user xattrs)"
    PASS=$((PASS + 1))
else
    echo -e "  ${R}[FAIL]${N} listxattr (expected >=2, got $LISTOUT)"
    FAIL=$((FAIL + 1))
fi

# removexattr
setfattr -x user.second "$TEST_DIR/xattr_file" 2>/dev/null || true
REMCHECK=$(getfattr -n user.second --only-values "$TEST_DIR/xattr_file" 2>&1 || echo "REMOVED")
if echo "$REMCHECK" | grep -q "No such attribute\|REMOVED"; then
    echo -e "  ${G}[PASS]${N} removexattr user.second"
    PASS=$((PASS + 1))
else
    echo -e "  ${R}[FAIL]${N} removexattr (attribute still exists)"
    FAIL=$((FAIL + 1))
fi

# xattr on directory
mkdir -p "$TEST_DIR/xattr_dir" 2>/dev/null || true
assert_pass "setxattr on directory" \
    "setfattr -n user.dir_attr -v dirval '$TEST_DIR/xattr_dir'"
DIRVAL=$(getfattr -n user.dir_attr --only-values "$TEST_DIR/xattr_dir" 2>/dev/null || echo "")
assert_eq "getxattr on directory" "dirval" "$DIRVAL"

echo ""

# ── fallocate tests (FUSE implements this) ──
echo "--- fallocate tests ---"

# fallocate preallocate
touch "$TEST_DIR/falloc_file"
assert_pass "fallocate 1MB" \
    "fallocate -l 1048576 '$TEST_DIR/falloc_file'"

FALLOC_SIZE=$(stat -c "%s" "$TEST_DIR/falloc_file")
assert_eq "fallocate size" "1048576" "$FALLOC_SIZE"

# fallocate with offset
fallocate -o 1048576 -l 1048576 "$TEST_DIR/falloc_file" 2>/dev/null || true
FALLOC_SIZE2=$(stat -c "%s" "$TEST_DIR/falloc_file")
if [ "$FALLOC_SIZE2" -ge 2097152 ]; then
    echo -e "  ${G}[PASS]${N} fallocate with offset (size=$FALLOC_SIZE2)"
    PASS=$((PASS + 1))
else
    echo -e "  ${R}[FAIL]${N} fallocate with offset (size=$FALLOC_SIZE2, expected >=2097152)"
    FAIL=$((FAIL + 1))
fi

# fallocate punch hole (FALLOC_FL_PUNCH_HOLE)
fallocate -p -o 0 -l 4096 "$TEST_DIR/falloc_file" 2>/dev/null && \
    echo -e "  ${G}[PASS]${N} fallocate punch hole" && PASS=$((PASS + 1)) || \
    { echo -e "  ${Y}[SKIP]${N} fallocate punch hole (mode not supported)"; SKIP=$((SKIP + 1)); }

echo ""

# ── mknod tests (FUSE implements mknod for special files) ──
echo "--- mknod tests ---"

# mknod create fifo (mkfifo uses mknod syscall)
assert_pass "mkfifo" \
    "mkfifo '$TEST_DIR/test_fifo'"

# Verify fifo exists and is a fifo
if [ -p "$TEST_DIR/test_fifo" ]; then
    echo -e "  ${G}[PASS]${N} fifo is a FIFO (S_IFIFO)"
    PASS=$((PASS + 1))
else
    echo -e "  ${R}[FAIL]${N} fifo is not a FIFO"
    FAIL=$((FAIL + 1))
fi

# mknod create block device (requires root; may fail with EPERM in container)
mknod "$TEST_DIR/mknod_block" b 1 1 2>/dev/null && \
    echo -e "  ${G}[PASS]${N} mknod block device" && PASS=$((PASS + 1)) || \
    { echo -e "  ${Y}[SKIP]${N} mknod block device (requires root/CAP_MKNOD)"; SKIP=$((SKIP + 1)); }

# mknod create char device (requires root; may fail with EPERM in container)
mknod "$TEST_DIR/mknod_char" c 1 3 2>/dev/null && \
    echo -e "  ${G}[PASS]${N} mknod char device" && PASS=$((PASS + 1)) || \
    { echo -e "  ${Y}[SKIP]${N} mknod char device (requires root/CAP_MKNOD)"; SKIP=$((SKIP + 1)); }

echo ""

# ── Summary ──
echo "========================================"
echo "  FUSE Feature Gap Test Summary"
echo "========================================"
echo "  PASS: $PASS"
echo "  FAIL: $FAIL"
echo "  SKIP: $SKIP"
echo "  Total: $((PASS + FAIL + SKIP))"
echo ""

if [ "$FAIL" -eq 0 ]; then
    echo "  Result: ALL PASSED"
    rm -rf "$TEST_DIR"
    exit 0
else
    echo "  Result: HAS FAILURES"
    rm -rf "$TEST_DIR"
    exit 1
fi
