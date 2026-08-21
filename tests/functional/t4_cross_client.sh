#!/usr/bin/env bash
# ================================================================
# T4: 跨客户端集成 — FUSE↔FUSE 数据一致性
#
# 覆盖：FUSE→FUSE 创建/读取/删除/元数据 + 并发读写 + remount 一致性
# 核心断言：跨客户端 MD5 一致 + 缓存清理后可见性正确
# 注意：每次跨客户端读取前必须 drop_cache，避免读到本地页缓存假象
#
# 运行: bash tests/functional/t4_cross_client.sh
# ================================================================
set -u
cd "$(dirname "$0")/../.."
source tests/lib/assertions.sh

CONTAINER="fuse-1"
TSTAMP=$(date +%s)
TDIR="/mnt/powerfs/func_${TSTAMP}_t4"

preflight

section "T4: Cross-Client Integration (FUSE ↔ FUSE)"
docker exec fuse-1 mkdir -p "$TDIR"
trap 'docker exec fuse-1 rm -rf "$TDIR" 2>/dev/null' EXIT

# ================================================================
# T4.1: fuse-1 create → fuse-2 read (MD5 match)
# ================================================================
echo ""
echo "  [T4.1] fuse-1 create → fuse-2 read"

# Small file
f="$TDIR/t41_small.bin"
src_md5=$(docker exec fuse-1 sh -c \
    "dd if=/dev/urandom bs=256 count=1 2>/dev/null | tee '$f' | md5sum | awk '{print \$1}'")
drop_cache fuse-2
assert_md5_cross "T4.1 small: fuse-1→fuse-2" "$f" "fuse-1" "fuse-2"

# Large file (1MB)
f="$TDIR/t41_large.bin"
src_md5=$(docker exec fuse-1 sh -c \
    "dd if=/dev/urandom bs=1M count=1 2>/dev/null | tee '$f' | md5sum | awk '{print \$1}'")
drop_cache fuse-2
assert_md5_cross "T4.1 large: fuse-1→fuse-2" "$f" "fuse-1" "fuse-2"

# ================================================================
# T4.2: fuse-2 create → fuse-1 read (reverse direction)
# ================================================================
echo ""
echo "  [T4.2] fuse-2 create → fuse-1 read"

f="$TDIR/t42_file.bin"
src_md5=$(docker exec fuse-2 sh -c \
    "dd if=/dev/urandom bs=512 count=1 2>/dev/null | tee '$f' | md5sum | awk '{print \$1}'")
drop_cache fuse-1
assert_md5_cross "T4.2: fuse-2→fuse-1" "$f" "fuse-2" "fuse-1"

# ================================================================
# T4.3: fuse-1 overwrite → fuse-2 sees new content
# ================================================================
echo ""
echo "  [T4.3] fuse-1 overwrite → fuse-2 sees new content"

f="$TDIR/t43_overwrite.bin"
# Initial write on fuse-1
docker exec fuse-1 sh -c "echo version1 > '$f'"
drop_cache fuse-2
content_v1_f2=$(docker exec fuse-2 cat "$f" 2>/dev/null | tr -d '\r\n')
assert_eq "T4.3 initial: fuse-2 sees version1" "version1" "$content_v1_f2"

# Overwrite on fuse-1
docker exec fuse-1 sh -c "echo version2 > '$f'"
drop_cache fuse-2
content_v2_f2=$(docker exec fuse-2 cat "$f" 2>/dev/null | tr -d '\r\n')
assert_eq "T4.3 overwrite: fuse-2 sees version2" "version2" "$content_v2_f2"

# ================================================================
# T4.4: fuse-1 delete → fuse-2 sees deletion
# ================================================================
echo ""
echo "  [T4.4] fuse-1 delete → fuse-2 sees deletion"

f="$TDIR/t44_delete.txt"
docker exec fuse-1 sh -c "echo deletetest > '$f'"
drop_cache fuse-2
assert_exists "T4.4: fuse-2 sees file before delete" "$f"

docker exec fuse-1 rm "$f"
drop_cache fuse-2
assert_not_exists "T4.4: fuse-2 sees file gone after delete" "$f"

# ================================================================
# T4.5: fuse-1 chmod → fuse-2 sees new mode
# ================================================================
echo ""
echo "  [T4.5] fuse-1 chmod → fuse-2 sees new mode"

f="$TDIR/t45_chmod.txt"
docker exec fuse-1 sh -c "echo mode_test > '$f' && chmod 600 '$f'"
drop_cache fuse-2
mode_f2=$(docker exec fuse-2 stat -c '%a' "$f" 2>/dev/null | tr -d '\r')
assert_eq "T4.5: fuse-2 sees mode 600" "600" "$mode_f2"

# Change on fuse-2, verify on fuse-1
docker exec fuse-2 chmod 644 "$f"
drop_cache fuse-1
mode_f1=$(docker exec fuse-1 stat -c '%a' "$f" 2>/dev/null | tr -d '\r')
assert_eq "T4.5: fuse-1 sees mode 644" "644" "$mode_f1"

# ================================================================
# T4.6: fuse-1 mkdir → fuse-2 ls
# ================================================================
echo ""
echo "  [T4.6] fuse-1 mkdir → fuse-2 ls"

d="$TDIR/t46_dir"
docker exec fuse-1 mkdir "$d"
docker exec fuse-1 sh -c "echo content1 > '$d/file1' && echo content2 > '$d/file2'"

drop_cache fuse-2
entries=$(docker exec fuse-2 ls -1 "$d" 2>/dev/null | sort | tr '\n' ' ')
assert_eq "T4.6: fuse-2 sees dir contents" "file1 file2 " "$entries"

# ================================================================
# T4.7: fuse-1 rename → fuse-2 sees new path
# ================================================================
echo ""
echo "  [T4.7] fuse-1 rename → fuse-2 sees new path"

old="$TDIR/t47_old.txt"
new="$TDIR/t47_new.txt"
docker exec fuse-1 sh -c "echo renamedata > '$old'"
src_md5=$(docker exec fuse-1 md5sum "$old" 2>/dev/null | awk '{print $1}')

docker exec fuse-1 mv "$old" "$new"
drop_cache fuse-2
assert_not_exists "T4.7: fuse-2 old path gone" "$old"
assert_exists "T4.7: fuse-2 new path exists" "$new"
assert_md5_match "T4.7: fuse-2 new path MD5" "$new" "$src_md5"

# ================================================================
# T4.8: concurrent writes from both clients (different files)
# ================================================================
echo ""
echo "  [T4.8] concurrent writes from both clients"

d="$TDIR/t48_concurrent"
docker exec fuse-1 mkdir -p "$d"

# fuse-1 writes 5 files, fuse-2 writes 5 files simultaneously
docker exec fuse-1 sh -c "for i in 1 2 3 4 5; do dd if=/dev/urandom bs=256 count=1 2>/dev/null > '$d/f1_'\$i; done" &
docker exec fuse-2 sh -c "for i in 1 2 3 4 5; do dd if=/dev/urandom bs=256 count=1 2>/dev/null > '$d/f2_'\$i; done" &
wait

# Total files should be 10
count=$(docker exec fuse-1 find "$d" -type f | wc -l)
assert_eq "T4.8: 10 files from both clients" "10" "$count"

# Cross-client verify all files
all_ok=true
for i in 1 2 3 4 5; do
    drop_cache fuse-2
    md5_f1=$(docker exec fuse-1 md5sum "$d/f1_$i" 2>/dev/null | awk '{print $1}')
    md5_f2=$(docker exec fuse-2 md5sum "$d/f1_$i" 2>/dev/null | awk '{print $1}')
    if [[ "$md5_f1" != "$md5_f2" ]]; then
        fail "T4.8: f1_$i cross-client" "match" "f1=$md5_f1 f2=$md5_f2"
        all_ok=false
    fi
done
if $all_ok; then
    pass "T4.8: all fuse-1 files visible and correct on fuse-2"
fi

# ================================================================
# T4.9: directory listing consistency across clients
# ================================================================
echo ""
echo "  [T4.9] directory listing consistency"

d="$TDIR/t49_lsdir"
docker exec fuse-1 mkdir -p "$d"
# Create files from both clients
docker exec fuse-1 sh -c "for i in 1 2 3; do touch '$d/from_f1_'\$i; done"
docker exec fuse-2 sh -c "for i in 1 2 3; do touch '$d/from_f2_'\$i; done"

drop_cache fuse-1
drop_cache fuse-2
ls_f1=$(docker exec fuse-1 ls -1 "$d" 2>/dev/null | sort)
ls_f2=$(docker exec fuse-2 ls -1 "$d" 2>/dev/null | sort)

if [[ "$ls_f1" == "$ls_f2" ]]; then
    pass "T4.9: directory listing identical on both clients"
    # Verify all 6 files present
    count=$(echo "$ls_f1" | wc -l)
    assert_eq "T4.9: 6 files in listing" "6" "$count"
else
    fail "T4.9: directory listing identical" "match" "mismatch"
    echo "    fuse-1 listing:"; echo "$ls_f1" | sed 's/^/      /'
    echo "    fuse-2 listing:"; echo "$ls_f2" | sed 's/^/      /'
fi

# ================================================================
# T4.10: append from one client, read from other
# ================================================================
echo ""
echo "  [T4.10] append from fuse-1, read from fuse-2"

f="$TDIR/t410_append.txt"
docker exec fuse-1 sh -c "echo line1 > '$f'"
docker exec fuse-1 sh -c "echo line2 >> '$f'"
drop_cache fuse-2
content=$(docker exec fuse-2 cat "$f" 2>/dev/null)
expected=$'line1\nline2'
if [[ "$content" == "$expected" ]]; then
    pass "T4.10: fuse-2 reads appended content correctly"
else
    fail "T4.10: fuse-2 reads appended content" "$expected" "$content"
fi

# ================================================================
# 日志检查 + 汇总
# ================================================================
echo ""
section "Log Check"
check_logs_clean "T4" "fuse-1"
check_logs_clean "T4" "fuse-2"
check_logs_clean "T4" "filer-1"

print_summary
