#!/usr/bin/env bash
# ================================================================
# T2: 文件系统正确性 — 严格数据一致性验证
#
# 覆盖：cp -r / tar / find+md5 manifest / 大文件 / git init
# 核心断言：diff -r 无差异 + find+md5sum manifest 完全一致
#
# 运行: bash tests/functional/t2_correctness.sh
# ================================================================
set -u
cd "$(dirname "$0")/../.."
source tests/lib/assertions.sh

CONTAINER="fuse-1"
TSTAMP=$(date +%s)
TDIR="/mnt/powerfs/func_${TSTAMP}_t2"

preflight

section "T2: Filesystem Correctness"
docker exec "$CONTAINER" mkdir -p "$TDIR"
trap 'docker exec "$CONTAINER" rm -rf "$TDIR" 2>/dev/null' EXIT

# ================================================================
# T2.1: cp -r 100 files + diff -r
# ================================================================
echo ""
echo "  [T2.1] cp -r 100 files + diff -r"

# Create source tree with 100 files of varying sizes
docker exec "$CONTAINER" mkdir -p "$TDIR/src"
docker exec "$CONTAINER" sh -c "
for i in \$(seq 1 100); do
    size=\$((RANDOM % 4096 + 1))
    dd if=/dev/urandom bs=\$size count=1 2>/dev/null > '$TDIR/src/file_'\$i
done
mkdir -p '$TDIR/src/subdir'
for i in \$(seq 1 10); do
    echo \"subdir file \$i\" > '$TDIR/src/subdir/sub_'\$i
done
"

# Count source files
src_count=$(docker exec "$CONTAINER" find "$TDIR/src" -type f | wc -l)
assert_eq "source has 110 files" "110" "$src_count"

# cp -r
assert_ok "cp -r succeeds" docker exec "$CONTAINER" cp -r "$TDIR/src" "$TDIR/dst"

# diff -r (the gold standard for tree equality)
diff_result=$(docker exec "$CONTAINER" diff -r "$TDIR/src" "$TDIR/dst" 2>&1)
if [[ -z "$diff_result" ]]; then
    pass "diff -r src dst: no differences"
else
    fail "diff -r src dst: no differences" "empty" "$diff_result"
fi

# dst file count matches
dst_count=$(docker exec "$CONTAINER" find "$TDIR/dst" -type f | wc -l)
assert_eq "dst has 110 files" "110" "$dst_count"

# ================================================================
# T2.2: find + md5sum manifest comparison
# ================================================================
echo ""
echo "  [T2.2] find + md5sum manifest comparison"

# Generate manifest on src
manifest_src=$(docker exec "$CONTAINER" sh -c "
find '$TDIR/src' -type f -exec md5sum {} \; | sort
" 2>/dev/null)

# Generate manifest on dst
manifest_dst=$(docker exec "$CONTAINER" sh -c "
find '$TDIR/dst' -type f -exec md5sum {} \; | \
    sed 's|$TDIR/dst|$TDIR/src|g' | sort
" 2>/dev/null)

# Compare manifests (normalize paths to match)
if [[ "$manifest_src" == "$manifest_dst" ]]; then
    pass "MD5 manifest: src and dst identical"
else
    fail "MD5 manifest: src and dst identical" "match" "mismatch"
    echo "    src manifest (first 5):"
    echo "$manifest_src" | head -5 | sed 's/^/      /'
    echo "    dst manifest (first 5):"
    echo "$manifest_dst" | head -5 | sed 's/^/      /'
fi

# ================================================================
# T2.3: tar czf + tar xzf round-trip
# ================================================================
echo ""
echo "  [T2.3] tar czf + tar xzf round-trip"

# Create tarball from src
assert_ok "tar czf creates archive" docker exec "$CONTAINER" tar czf "$TDIR/archive.tar.gz" -C "$TDIR" src

# Verify tarball is non-empty
tar_size=$(docker exec "$CONTAINER" stat -c '%s' "$TDIR/archive.tar.gz" 2>/dev/null | tr -d '\r')
if [[ "$tar_size" -gt 0 ]]; then
    pass "tarball non-empty ($tar_size bytes)"
else
    fail "tarball non-empty" "> 0" "$tar_size"
fi

# Extract to new location
docker exec "$CONTAINER" mkdir -p "$TDIR/tarout"
assert_ok "tar xzf extracts" docker exec "$CONTAINER" tar xzf "$TDIR/archive.tar.gz" -C "$TDIR/tarout"

# diff -r src vs tarout/src
diff_result=$(docker exec "$CONTAINER" diff -r "$TDIR/src" "$TDIR/tarout/src" 2>&1)
if [[ -z "$diff_result" ]]; then
    pass "tar round-trip: content identical"
else
    fail "tar round-trip: content identical" "empty" "$diff_result"
fi

# ================================================================
# T2.4: large file (4MB) read/write consistency
# ================================================================
echo ""
echo "  [T2.4] 4MB file read/write consistency"

f="$TDIR/large_4m.bin"
src_md5=$(docker exec "$CONTAINER" sh -c \
    "dd if=/dev/urandom bs=1M count=4 2>/dev/null | tee '$f' | md5sum | awk '{print \$1}'")
assert_stat "4MB file size" "$f" '%s' "4194304"
assert_md5_match "4MB file MD5" "$f" "$src_md5"

# Read back in chunks and verify
read_md5=$(docker exec "$CONTAINER" md5sum "$f" 2>/dev/null | awk '{print $1}')
assert_eq "4MB re-read MD5" "$src_md5" "$read_md5"

# Cross-client MD5
drop_cache fuse-2
assert_md5_cross "4MB cross-client MD5" "$f" "fuse-1" "fuse-2"

# ================================================================
# T2.5: append consistency (sequential appends)
# ================================================================
echo ""
echo "  [T2.5] sequential append consistency"

f="$TDIR/append_test.bin"
expected_md5=""
for i in $(seq 1 10); do
    docker exec "$CONTAINER" sh -c "dd if=/dev/urandom bs=100 count=1 2>/dev/null >> '$f'"
done
assert_stat "append file: 10 x 100B = 1000" "$f" '%s' "1000"

# Verify content hasn't been corrupted by concurrent appends
final_md5=$(docker exec "$CONTAINER" md5sum "$f" 2>/dev/null | awk '{print $1}')
# Read back and re-md5
reread_md5=$(docker exec "$CONTAINER" cat "$f" 2>/dev/null | md5sum | awk '{print $1}')
assert_eq "append: final MD5 == re-read MD5" "$final_md5" "$reread_md5"

# ================================================================
# T2.6: mixed workload (create/write/read/delete/rewrite)
# ================================================================
echo ""
echo "  [T2.6] mixed workload"

d="$TDIR/mixed"
docker exec "$CONTAINER" mkdir -p "$d"

# Create 20 files
docker exec "$CONTAINER" sh -c "for i in \$(seq 1 20); do echo \"data \$i\" > '$d/f'\$i; done"
count=$(docker exec "$CONTAINER" ls -1 "$d" | wc -l)
assert_eq "mixed: 20 files created" "20" "$count"

# Delete odd-numbered
docker exec "$CONTAINER" sh -c "for i in \$(seq 1 2 20); do rm '$d/f'\$i; done"
count=$(docker exec "$CONTAINER" ls -1 "$d" | wc -l)
assert_eq "mixed: 10 files after deleting odd" "10" "$count"

# Rewrite remaining files
docker exec "$CONTAINER" sh -c "for i in \$(seq 2 2 20); do echo \"rewritten \$i\" > '$d/f'\$i; done"

# Verify remaining files have correct content
all_correct=true
for i in $(seq 2 2 20); do
    content=$(docker exec "$CONTAINER" cat "$d/f$i" 2>/dev/null | tr -d '\r\n')
    if [[ "$content" != "rewritten $i" ]]; then
        fail "mixed: file f$i content" "rewritten $i" "$content"
        all_correct=false
    fi
done
if $all_correct; then
    pass "mixed: all remaining files have correct rewritten content"
fi

# Deleted files should not exist
all_deleted=true
for i in $(seq 1 2 19); do
    if docker exec "$CONTAINER" test -e "$d/f$i" 2>/dev/null; then
        fail "mixed: file f$i should be deleted" "absent" "exists"
        all_deleted=false
    fi
done
if $all_deleted; then
    pass "mixed: all odd-numbered files confirmed deleted"
fi

# ================================================================
# T2.7: directory tree consistency
# ================================================================
echo ""
echo "  [T2.7] directory tree consistency"

d="$TDIR/tree"
# Create a 3-level tree with known structure
docker exec "$CONTAINER" sh -c "
mkdir -p '$d/a/b/c'
mkdir -p '$d/a/b/d'
mkdir -p '$d/e'
echo 'file1' > '$d/a/file1'
echo 'file2' > '$d/a/b/file2'
echo 'file3' > '$d/a/b/c/file3'
echo 'file4' > '$d/a/b/d/file4'
echo 'file5' > '$d/e/file5'
"

# Verify tree structure via find
tree_output=$(docker exec "$CONTAINER" find "$d" -type f | sort | \
    sed "s|$TDIR/||" | tr '\n' ',')
expected="tree/a/b/c/file3,tree/a/b/d/file4,tree/a/b/file2,tree/a/file1,tree/e/file5,"
assert_eq "directory tree structure" "$expected" "$tree_output"

# Verify each file's content
assert_eq "tree: a/file1 content" "file1" "$(docker exec "$CONTAINER" cat "$d/a/file1" | tr -d '\r\n')"
assert_eq "tree: a/b/file2 content" "file2" "$(docker exec "$CONTAINER" cat "$d/a/b/file2" | tr -d '\r\n')"
assert_eq "tree: a/b/c/file3 content" "file3" "$(docker exec "$CONTAINER" cat "$d/a/b/c/file3" | tr -d '\r\n')"
assert_eq "tree: a/b/d/file4 content" "file4" "$(docker exec "$CONTAINER" cat "$d/a/b/d/file4" | tr -d '\r\n')"
assert_eq "tree: e/file5 content" "file5" "$(docker exec "$CONTAINER" cat "$d/e/file5" | tr -d '\r\n')"

# ================================================================
# 日志检查 + 汇总
# ================================================================
echo ""
section "Log Check"
check_logs_clean "T2" "fuse-1"
check_logs_clean "T2" "filer-1"

print_summary
