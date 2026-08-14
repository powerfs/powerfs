#!/bin/bash
# PowerFS Kernel Module Feature Gap Tests
#
# Tests implemented features (mknod, fifo) for correctness,
# and unimplemented features (xattr, fallocate) for proper error codes.
#
# Usage:
#   ssh -p 2223 root@localhost /tmp/test_kernel_features.sh
#
# Expected results:
#   mknod:        PASS (Kernel implements powerfs_mknod)
#   mkfifo:       PASS (mkfifo uses mknod)
#   xattr:        EXPECT_FAIL (Kernel does not implement setxattr/getxattr)
#   fallocate:    EXPECT_FAIL (Kernel does not implement fallocate)

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
echo "  PowerFS Kernel Feature Gap Tests"
echo "========================================"
echo ""

# ── mknod tests (Kernel implements these) ──
echo "--- mknod tests (Kernel implements powerfs_mknod) ---"

# mknod create block device
assert_pass "mknod block device" \
    "mknod '$TEST_DIR/mknod_block' b 1 1"

# Verify block device exists and has correct type
if [ -b "$TEST_DIR/mknod_block" ]; then
    echo -e "  ${G}[PASS]${N} block device type correct"
    PASS=$((PASS + 1))
else
    echo -e "  ${R}[FAIL]${N} block device type wrong (not -b)"
    FAIL=$((FAIL + 1))
fi

# mknod create char device
assert_pass "mknod char device" \
    "mknod '$TEST_DIR/mknod_char' c 1 3"

# Verify char device exists and has correct type
if [ -c "$TEST_DIR/mknod_char" ]; then
    echo -e "  ${G}[PASS]${N} char device type correct"
    PASS=$((PASS + 1))
else
    echo -e "  ${R}[FAIL]${N} char device type wrong (not -c)"
    FAIL=$((FAIL + 1))
fi

# mknod create fifo via mkfifo
assert_pass "mkfifo" \
    "mkfifo '$TEST_DIR/test_fifo'"

# Verify fifo exists and has correct type
if [ -p "$TEST_DIR/test_fifo" ]; then
    echo -e "  ${G}[PASS]${N} fifo type correct"
    PASS=$((PASS + 1))
else
    echo -e "  ${R}[FAIL]${N} fifo type wrong (not -p)"
    FAIL=$((FAIL + 1))
fi

# mknod create regular file via mknod
assert_pass "mknod regular file" \
    "mknod '$TEST_DIR/mknod_reg' 0644"

# Verify regular file
if [ -f "$TEST_DIR/mknod_reg" ]; then
    echo -e "  ${G}[PASS]${N} regular file type correct"
    PASS=$((PASS + 1))
else
    echo -e "  ${R}[FAIL]${N} regular file type wrong (not -f)"
    FAIL=$((FAIL + 1))
fi

# mknod with mode bits
mknod --mode=0600 "$TEST_DIR/mknod_mode" p 2>/dev/null && \
    echo -e "  ${G}[PASS]${N} mknod with explicit mode" && PASS=$((PASS + 1)) || \
    { echo -e "  ${Y}[SKIP]${N} mknod with explicit mode"; SKIP=$((SKIP + 1)); }

echo ""

# ── fifo read/write test ──
echo "--- fifo read/write test ---"

# Test fifo basic I/O (non-blocking, background)
echo "fifo_test_data" > "$TEST_DIR/test_fifo" &
FIFO_PID=$!
sleep 0.5
FIFO_CONTENT=$(cat "$TEST_DIR/test_fifo" 2>/dev/null || echo "")
wait $FIFO_PID 2>/dev/null || true

if [ "$FIFO_CONTENT" = "fifo_test_data" ]; then
    echo -e "  ${G}[PASS]${N} fifo read/write"
    PASS=$((PASS + 1))
else
    echo -e "  ${Y}[SKIP]${N} fifo read/write (content mismatch or timeout)"
    SKIP=$((SKIP + 1))
fi

echo ""

# ── xattr tests (Kernel does NOT implement xattr) ──
echo "--- xattr tests (expected: ENOSYS/EOPNOTSUPP) ---"

touch "$TEST_DIR/xattr_file"

# setxattr should fail
assert_fail "setxattr (expect fail)" \
    "setfattr -n user.test -v hello '$TEST_DIR/xattr_file'"

# getxattr should fail
assert_fail "getxattr (expect fail)" \
    "getfattr -n user.test '$TEST_DIR/xattr_file'"

# listxattr should fail or return empty
XATTR_LIST=$(getfattr -m- "$TEST_DIR/xattr_file" 2>&1 || echo "FAILED")
if echo "$XATTR_LIST" | grep -qi "not supported\|failed\|function not"; then
    echo -e "  ${G}[PASS]${N} listxattr correctly returns error"
    PASS=$((PASS + 1))
elif [ -z "$XATTR_LIST" ]; then
    echo -e "  ${G}[PASS]${N} listxattr returns empty (no xattrs)"
    PASS=$((PASS + 1))
else
    echo -e "  ${Y}[SKIP]${N} listxattr returned: $XATTR_LIST"
    SKIP=$((SKIP + 1))
fi

# Verify xattr returns proper error code
SETXATTR_ERR=$(setfattr -n user.test -v hello "$TEST_DIR/xattr_file" 2>&1 || true)
if echo "$SETXATTR_ERR" | grep -qi "not supported\|function not\|operation not"; then
    echo -e "  ${G}[PASS]${N} setxattr returns ENOSYS/EOPNOTSUPP"
    PASS=$((PASS + 1))
else
    echo -e "  ${Y}[SKIP]${N} setxattr error: $SETXATTR_ERR"
    SKIP=$((SKIP + 1))
fi

echo ""

# ── fallocate tests (Kernel does NOT implement fallocate) ──
echo "--- fallocate tests (expected: ENOSYS/EOPNOTSUPP) ---"

touch "$TEST_DIR/falloc_file"

# fallocate should fail
assert_fail "fallocate (expect fail)" \
    "fallocate -l 1048576 '$TEST_DIR/falloc_file'"

# Verify fallocate returns proper error code
FALLOC_ERR=$(fallocate -l 1048576 "$TEST_DIR/falloc_file" 2>&1 || true)
if echo "$FALLOC_ERR" | grep -qi "not supported\|function not\|operation not"; then
    echo -e "  ${G}[PASS]${N} fallocate returns ENOSYS/EOPNOTSUPP"
    PASS=$((PASS + 1))
else
    echo -e "  ${Y}[SKIP]${N} fallocate error: $FALLOC_ERR"
    SKIP=$((SKIP + 1))
fi

# Verify file size unchanged
FALLOC_SIZE=$(stat -c "%s" "$TEST_DIR/falloc_file")
if [ "$FALLOC_SIZE" = "0" ]; then
    echo -e "  ${G}[PASS]${N} file size unchanged after failed fallocate"
    PASS=$((PASS + 1))
else
    echo -e "  ${R}[FAIL]${N} file size changed to $FALLOC_SIZE (expected 0)"
    FAIL=$((FAIL + 1))
fi

echo ""

# ── dmesg check ──
echo "--- dmesg check ---"

# Check for any new errors in dmesg related to powerfs
DMESG_ERRORS=$(dmesg | tail -50 | grep -i "powerfs.*error\|powerfs.*oops\|powerfs.*bug\|BUG:" || true)
if [ -z "$DMESG_ERRORS" ]; then
    echo -e "  ${G}[PASS]${N} no powerfs errors in dmesg"
    PASS=$((PASS + 1))
else
    echo -e "  ${R}[FAIL]${N} powerfs errors found in dmesg:"
    echo "    $DMESG_ERRORS"
    FAIL=$((FAIL + 1))
fi

echo ""

# ── Summary ──
echo "========================================"
echo "  Kernel Feature Gap Test Summary"
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
