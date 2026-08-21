#!/usr/bin/env bash
# ================================================================
# T3: 布局功能 — 正常路径读写验证（无故障注入）
#
# 覆盖：K1 Flat / K2 Inline / K3 Stripe / K4 Replicated
# 核心断言：写入+读回 MD5 一致 + 跨客户端 MD5 一致
# 注意：仅验证正常路径，故障注入测试归入 T7（当前阶段暂不测试）
#
# 运行: bash tests/functional/t3_layout.sh
# ================================================================
set -u
cd "$(dirname "$0")/../.."
source tests/lib/assertions.sh

CONTAINER="fuse-1"
TSTAMP=$(date +%s)
TDIR="/mnt/powerfs/func_${TSTAMP}_t3"

preflight

section "T3: Layout Functional Tests (Normal Path)"
docker exec "$CONTAINER" mkdir -p "$TDIR"
trap 'docker exec "$CONTAINER" rm -rf "$TDIR" 2>/dev/null' EXIT

# ================================================================
# T3.1 K1: Flat layout — basic write + read + cross-client
# ================================================================
echo ""
echo "  [T3.1] K1 Flat: write + read + cross-client MD5"

# Small file (likely inline or flat)
f="$TDIR/k1_small.bin"
src_md5=$(docker exec "$CONTAINER" sh -c \
    "dd if=/dev/urandom bs=512 count=1 2>/dev/null | tee '$f' | md5sum | awk '{print \$1}'")
assert_stat "K1 small size" "$f" '%s' "512"
assert_md5_match "K1 small MD5" "$f" "$src_md5"
drop_cache fuse-2
assert_md5_cross "K1 small cross-client" "$f" "fuse-1" "fuse-2"

# Medium file (64KB — should use chunks, not inline)
f="$TDIR/k1_64k.bin"
src_md5=$(docker exec "$CONTAINER" sh -c \
    "dd if=/dev/urandom bs=64K count=1 2>/dev/null | tee '$f' | md5sum | awk '{print \$1}'")
assert_stat "K1 64K size" "$f" '%s' "65536"
assert_md5_match "K1 64K MD5" "$f" "$src_md5"
drop_cache fuse-2
assert_md5_cross "K1 64K cross-client" "$f" "fuse-1" "fuse-2"

# Large file (1MB)
f="$TDIR/k1_1m.bin"
src_md5=$(docker exec "$CONTAINER" sh -c \
    "dd if=/dev/urandom bs=1M count=1 2>/dev/null | tee '$f' | md5sum | awk '{print \$1}'")
assert_stat "K1 1M size" "$f" '%s' "1048576"
assert_md5_match "K1 1M MD5" "$f" "$src_md5"
drop_cache fuse-2
assert_md5_cross "K1 1M cross-client" "$f" "fuse-1" "fuse-2"

# ================================================================
# T3.2 K2: Inline layout — very small file (should be inline)
# ================================================================
echo ""
echo "  [T3.2] K2 Inline: very small files"

# 1-byte file
f="$TDIR/k2_1byte.bin"
docker exec "$CONTAINER" sh -c "printf 'X' > '$f'"
assert_stat "K2 1-byte size" "$f" '%s' "1"
content=$(docker exec "$CONTAINER" cat "$f" 2>/dev/null | tr -d '\r\n')
assert_eq "K2 1-byte content" "X" "$content"
drop_cache fuse-2
assert_md5_cross "K2 1-byte cross-client" "$f" "fuse-1" "fuse-2"

# 16-byte file
f="$TDIR/k2_16byte.bin"
docker exec "$CONTAINER" sh -c "printf '0123456789ABCDEF' > '$f'"
assert_stat "K2 16-byte size" "$f" '%s' "16"
assert_md5_match "K2 16-byte content" "$f" \
    "$(docker exec "$CONTAINER" sh -c "printf '0123456789ABCDEF' | md5sum | awk '{print \$1}'")"
drop_cache fuse-2
assert_md5_cross "K2 16-byte cross-client" "$f" "fuse-1" "fuse-2"

# Empty file
f="$TDIR/k2_empty.bin"
docker exec "$CONTAINER" touch "$f"
assert_stat "K2 empty size" "$f" '%s' "0"
drop_cache fuse-2
assert_md5_cross "K2 empty cross-client" "$f" "fuse-1" "fuse-2"

# ================================================================
# T3.3 K3: Stripe layout — multi-block file
# ================================================================
echo ""
echo "  [T3.3] K3 Stripe: multi-block write"

# 256KB file (should span multiple chunks/blocks)
f="$TDIR/k3_256k.bin"
src_md5=$(docker exec "$CONTAINER" sh -c \
    "dd if=/dev/urandom bs=256K count=1 2>/dev/null | tee '$f' | md5sum | awk '{print \$1}'")
assert_stat "K3 256K size" "$f" '%s' "262144"
assert_md5_match "K3 256K MD5" "$f" "$src_md5"
drop_cache fuse-2
assert_md5_cross "K3 256K cross-client" "$f" "fuse-1" "fuse-2"

# 4MB file (large, definitely multi-chunk)
f="$TDIR/k3_4m.bin"
src_md5=$(docker exec "$CONTAINER" sh -c \
    "dd if=/dev/urandom bs=4M count=1 2>/dev/null | tee '$f' | md5sum | awk '{print \$1}'")
assert_stat "K3 4M size" "$f" '%s' "4194304"
assert_md5_match "K3 4M MD5" "$f" "$src_md5"
drop_cache fuse-2
assert_md5_cross "K3 4M cross-client" "$f" "fuse-1" "fuse-2"

# ================================================================
# T3.4 K4: Replicated layout — write + read back (normal path)
# ================================================================
echo ""
echo "  [T3.4] K4 Replicated: normal write + read (no failover)"

# Write 128KB file
f="$TDIR/k4_128k.bin"
src_md5=$(docker exec "$CONTAINER" sh -c \
    "dd if=/dev/urandom bs=128K count=1 2>/dev/null | tee '$f' | md5sum | awk '{print \$1}'")
assert_stat "K4 128K size" "$f" '%s' "131072"
assert_md5_match "K4 128K MD5" "$f" "$src_md5"
drop_cache fuse-2
assert_md5_cross "K4 128K cross-client" "$f" "fuse-1" "fuse-2"

# Note: failover/CRC/EC degraded read tests are in T7 (not tested now)

# ================================================================
# T3.5: Multiple files in same directory (stress layout)
# ================================================================
echo ""
echo "  [T3.5] 50 files in same directory"

d="$TDIR/multi50"
docker exec "$CONTAINER" mkdir -p "$d"

# Create 50 files of different sizes
for i in $(seq 1 50); do
    size=$((i * 100))
    docker exec "$CONTAINER" sh -c "dd if=/dev/urandom bs=$size count=1 2>/dev/null > '$d/file_$i'"
done

# Verify count
count=$(docker exec "$CONTAINER" find "$d" -type f | wc -l)
assert_eq "50 files created" "50" "$count"

# Verify each file size
all_sizes_ok=true
for i in $(seq 1 50); do
    expected=$((i * 100))
    actual=$(docker exec "$CONTAINER" stat -c '%s' "$d/file_$i" 2>/dev/null | tr -d '\r')
    if [[ "$actual" != "$expected" ]]; then
        fail "file_$i size" "$expected" "$actual"
        all_sizes_ok=false
    fi
done
if $all_sizes_ok; then
    pass "all 50 files have correct sizes"
fi

# Cross-client verify a few files
for i in 5 15 30 45; do
    drop_cache fuse-2
    assert_md5_cross "file_$i cross-client" "$d/file_$i" "fuse-1" "fuse-2"
done

# ================================================================
# T3.6: overwrite + verify size and content change
# ================================================================
echo ""
echo "  [T3.6] overwrite changes size and content"

f="$TDIR/overwrite_layout.bin"
# Write 100 bytes
docker exec "$CONTAINER" sh -c "dd if=/dev/urandom bs=100 count=1 of='$f' 2>/dev/null"
md5_100=$(docker exec "$CONTAINER" md5sum "$f" 2>/dev/null | awk '{print $1}')

# Overwrite with 200 bytes
docker exec "$CONTAINER" sh -c "dd if=/dev/urandom bs=200 count=1 of='$f' 2>/dev/null"
assert_stat "overwrite: new size 200" "$f" '%s' "200"
md5_200=$(docker exec "$CONTAINER" md5sum "$f" 2>/dev/null | awk '{print $1}')

# MD5 must be different (different random data)
if [[ "$md5_100" != "$md5_200" ]]; then
    pass "overwrite: MD5 changed after resize"
else
    fail "overwrite: MD5 changed" "different" "same"
fi

# Overwrite with smaller (50 bytes)
docker exec "$CONTAINER" sh -c "dd if=/dev/urandom bs=50 count=1 of='$f' 2>/dev/null"
assert_stat "overwrite: shrink to 50" "$f" '%s' "50"

# ================================================================
# T3.7: read from different offsets
# ================================================================
echo ""
echo "  [T3.7] read from different offsets"

f="$TDIR/offset_read.bin"
# Write known pattern: 0x00 for first 100, 0xFF for next 100, 0x55 for next 100
docker exec "$CONTAINER" sh -c "
dd if=/dev/zero bs=100 count=1 of='$f' 2>/dev/null
printf '\xff%.0s' {1..100} >> '$f'
"

# Read first 100 bytes — should be 0x00
hex_part1=$(docker exec "$CONTAINER" sh -c "dd if='$f' bs=1 skip=0 count=10 2>/dev/null | od -A n -t x1 | tr -d ' \n'")
assert_eq "offset read: bytes 0-9 are 0x00" "00000000000000000000" "$hex_part1"

# Read bytes 100-109 — should be 0xFF
hex_part2=$(docker exec "$CONTAINER" sh -c "dd if='$f' bs=1 skip=100 count=10 2>/dev/null | od -A n -t x1 | tr -d ' \n'")
assert_eq "offset read: bytes 100-109 are 0xff" "ffffffffffffffffffff" "$hex_part2"

# ================================================================
# 日志检查 + 汇总
# ================================================================
echo ""
section "Log Check"
check_logs_clean "T3" "fuse-1"
check_logs_clean "T3" "filer-1"

print_summary
